#include "TestHelpers.hpp"
#include <lattice/sync.hpp>
#include <chrono>
#include <exception>
#include <map>
#include <set>

namespace {
constexpr size_t workers = 8;

// Expose the actual protected helper, without a test-only generator or seed.
// Worker threads call only that helper in the first test; no SQLite calls race
// on this owner. This exercises the process-wide state used by ordinary adds.
struct identity_access : lattice::lattice_db {
    identity_access() : lattice_db(lattice::configuration(":memory:")) {}
    using lattice_db::generate_global_id;
};

template<class Body> void concurrently(Body body) {
    std::mutex mutex;
    std::condition_variable condition;
    size_t ready = 0;
    bool released = false;
    std::vector<std::exception_ptr> errors(workers);
    std::vector<std::thread> threads;
    threads.reserve(workers);
    const auto release_and_join = [&] {
        { std::lock_guard<std::mutex> lock(mutex); released = true; }
        condition.notify_all();
        for (auto& thread : threads) if (thread.joinable()) thread.join();
    };
    try {
        for (size_t worker = 0; worker < workers; ++worker) {
            threads.emplace_back([&, worker] {
                try {
                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        ++ready; condition.notify_all();
                        condition.wait(lock, [&] { return released; });
                    }
                    body(worker);
                } catch (...) { errors[worker] = std::current_exception(); }
            });
        }
        bool all_ready;
        {
            std::unique_lock<std::mutex> lock(mutex);
            all_ready = condition.wait_for(lock, std::chrono::seconds(10), [&] { return ready == workers; });
        }
        release_and_join();
        if (!all_ready) throw std::runtime_error("identity workers did not reach their start gate");
    } catch (...) { release_and_join(); throw; }
    for (const auto& error : errors) if (error) std::rethrow_exception(error);
}

bool canonical_v4(const std::string& value) {
    if (value.size() != 36 || value[14] != '4' || std::string("89ab").find(value[19]) == std::string::npos)
        return false;
    for (size_t index = 0; index < value.size(); ++index) {
        if (index == 8 || index == 13 || index == 18 || index == 23) {
            if (value[index] != '-') return false;
        } else if (!((value[index] >= '0' && value[index] <= '9') ||
                     (value[index] >= 'a' && value[index] <= 'f'))) return false;
    }
    return true;
}

lattice::configuration config(const TempDB& file) {
    lattice::configuration result(file.str());
    result.audit_retention_seconds = 0;
    return result;
}
} // namespace

TEST(GlobalIdentityConcurrency, ConcurrentGeneratorPreservesUniqueCanonicalV4Identities) {
    identity_access owner;
    constexpr size_t per_worker = 4096;
    std::vector<std::vector<std::string>> values(workers, std::vector<std::string>(per_worker));
    concurrently([&](size_t worker) {
        for (auto& value : values[worker]) value = owner.generate_global_id();
    });
    std::set<std::string> distinct;
    for (const auto& batch : values) for (const auto& value : batch) {
        ASSERT_TRUE(canonical_v4(value)) << value;
        ASSERT_TRUE(distinct.insert(value).second) << "concurrent generator repeated " << value;
    }
    EXPECT_EQ(distinct.size(), workers * per_worker);
    // This is bounded concurrency coverage, not a statistical uniqueness proof
    // or a deterministic reproduction of every possible data-race schedule.
}

TEST(GlobalIdentityConcurrency, IndependentStoreInsertsSurviveRelayReplayAndReopen) {
    constexpr size_t per_worker = 32;
    // TempDB's own fixture RNG is not under test: allocate paths and open every
    // store serially before the concurrent public writes begin.
    std::vector<std::unique_ptr<TempDB>> files;
    std::vector<std::unique_ptr<lattice::lattice_db>> owners;
    for (size_t worker = 0; worker < workers; ++worker) {
        files.push_back(std::make_unique<TempDB>("identity-writer"));
        owners.push_back(std::make_unique<lattice::lattice_db>(config(*files.back())));
    }
    std::vector<std::vector<std::string>> identities(workers, std::vector<std::string>(per_worker));
    const auto name = [](size_t worker, size_t sequence) {
        return "identity/" + std::to_string(worker) + "/" + std::to_string(sequence);
    };
    concurrently([&](size_t worker) {
        auto& owner = *owners[worker];
        owner.begin_transaction();
        try {
            for (size_t sequence = 0; sequence < per_worker; ++sequence) {
                auto row = owner.add(TestPerson{name(worker, sequence), static_cast<int>(worker), std::nullopt});
                identities[worker][sequence] = row.global_id();
            }
            owner.commit();
        } catch (...) { owner.rollback(); throw; }
    });
    std::map<std::string, std::string> expected;
    std::vector<std::vector<lattice::audit_log_entry>> batches(workers);
    for (size_t worker = 0; worker < workers; ++worker) {
        for (size_t sequence = 0; sequence < per_worker; ++sequence) {
            const auto& identity = identities[worker][sequence];
            ASSERT_TRUE(canonical_v4(identity));
            ASSERT_TRUE(expected.emplace(identity, name(worker, sequence)).second)
                << "independent inserts shared global identity " << identity;
        }
        batches[worker] = lattice::events_after(owners[worker]->db(), std::nullopt);
        ASSERT_EQ(batches[worker].size(), per_worker);
        std::set<std::string> source_identities;
        for (const auto& entry : batches[worker]) {
            ASSERT_EQ(entry.operation, "INSERT");
            ASSERT_TRUE(source_identities.insert(entry.global_row_id).second);
            const auto found = expected.find(entry.global_row_id);
            ASSERT_NE(found, expected.end());
            EXPECT_EQ(std::get<std::string>(entry.changed_fields.at("name").value), found->second);
        }
        EXPECT_EQ(source_identities, std::set<std::string>(identities[worker].begin(), identities[worker].end()));
    }
    TempDB relay_file{"identity-relay"};
    const auto verify = [&](lattice::lattice_db& relay) {
        const auto rows = relay.db().query("SELECT globalId,name,age FROM TestPerson ORDER BY globalId");
        ASSERT_EQ(rows.size(), expected.size());
        for (const auto& row : rows) {
            const auto& identity = std::get<std::string>(row.at("globalId"));
            const auto found = expected.find(identity);
            ASSERT_NE(found, expected.end());
            EXPECT_EQ(std::get<std::string>(row.at("name")), found->second);
            const auto worker = std::stoll(found->second.substr(std::string("identity/").size()));
            EXPECT_EQ(std::get<int64_t>(row.at("age")), worker);
        }
    };
    {
        lattice::lattice_db relay(config(relay_file));
        lattice::ensure_cursor_column(relay.db());
        lattice::register_replication_slot(relay.db(), "identity-receive");
        for (size_t worker = workers; worker-- > 0;) {
            const auto accepted = lattice::apply_remote_changes_for(relay, batches[worker], "identity-receive");
            std::vector<std::string> wanted;
            for (const auto& entry : batches[worker]) wanted.push_back(entry.global_id);
            EXPECT_EQ(accepted, wanted);
        }
        verify(relay);
        for (const auto& batch : batches) {
            const auto accepted = lattice::apply_remote_changes_for(relay, batch, "identity-receive");
            std::vector<std::string> wanted;
            for (const auto& entry : batch) wanted.push_back(entry.global_id);
            EXPECT_EQ(accepted, wanted);
        }
        verify(relay);
    }
    lattice::lattice_db reopened(config(relay_file));
    verify(reopened);
}
