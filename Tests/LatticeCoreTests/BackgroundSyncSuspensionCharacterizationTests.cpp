#include "TestHelpers.hpp"
#include <lattice/sync.hpp>
#include <deque>
#include <mutex>

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <fcntl.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
// Deliberately queued: receiving a frame and executing its apply are separate
// deterministic steps. This characterizes the existing disconnect/destruction
// boundary before the background-suspension API is added.
class suspension_queue final : public lattice::scheduler {
    std::mutex mutex_;
    std::deque<std::function<void()>> jobs_;
    std::atomic<bool> accepting_{true};
public:
    void invoke(std::function<void()>&& fn) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (accepting_) jobs_.push_back(std::move(fn));
    }
    bool is_on_thread() const noexcept override { return false; }
    bool is_same_as(const lattice::scheduler* other) const noexcept override {
        return other == this;
    }
    bool can_invoke() const noexcept override { return accepting_; }
    void shutdown() override {
        std::deque<std::function<void()>> cancelled;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            accepting_ = false;
            cancelled.swap(jobs_);
        }
    }
    void drain() {
        for (int i = 0; i < 100; ++i) {
            std::function<void()> job;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (jobs_.empty()) return;
                job = std::move(jobs_.front());
                jobs_.pop_front();
            }
            job();
        }
        FAIL() << "suspension characterization queue did not settle";
    }
};

struct recorded_sends {
    std::mutex mutex;
    std::vector<std::string> frames;
};

class suspension_transport final : public lattice::mock_sync_transport {
    std::shared_ptr<recorded_sends> sends_;
public:
    explicit suspension_transport(std::shared_ptr<recorded_sends> sends)
        : sends_(std::move(sends)) {}
    void send(const lattice::transport_message& message) override {
        {
            std::lock_guard<std::mutex> lock(sends_->mutex);
            sends_->frames.push_back(message.as_string());
        }
        lattice::mock_sync_transport::send(message);
    }
};

lattice::audit_log_entry suspension_remote_row() {
    lattice::audit_log_entry entry;
    entry.global_id = "background-probe-audit";
    entry.table_name = "TestPerson";
    entry.operation = "INSERT";
    entry.row_id = 88;
    entry.global_row_id = "background-probe-person";
    entry.changed_fields = {{"name", lattice::any_property("Queued remote")},
                            {"age", lattice::any_property(42)}};
    entry.changed_fields_names = {"name", "age"};
    entry.timestamp = "2026-11-01T00:00:00Z";
    return entry;
}

struct suspension_fixture {
    TempDB file{"background_suspension"};
    lattice::lattice_db reader{lattice::configuration(file.str())};
    std::shared_ptr<suspension_queue> queue = std::make_shared<suspension_queue>();
    std::shared_ptr<recorded_sends> sends = std::make_shared<recorded_sends>();
    lattice::mock_sync_transport* transport = nullptr;
    std::unique_ptr<lattice::synchronizer> sync;

    suspension_fixture() {
        // Install the model schema before the private sync handle is opened.
        (void)reader.objects<TestPerson>();
        auto db = std::make_unique<lattice::lattice_db>(
            lattice::configuration(file.str(), queue));
        lattice::sync_config config;
        config.websocket_url = "ws://fixture.invalid/background";
        config.sync_id = "background-characterization";
        config.all_active_sync_ids = {config.sync_id};
        config.upload_coalesce_ms = 0;
        config.checkpoint_passive_interval_ms = 0;
        auto wire = std::make_unique<suspension_transport>(sends);
        transport = wire.get();
        sync = std::make_unique<lattice::synchronizer>(
            std::move(db), config, std::move(wire));
        sync->connect();
        queue->drain();
        transport->clear_sent_messages();
        std::lock_guard<std::mutex> lock(sends->mutex);
        sends->frames.clear();
    }

    void enqueue_remote() {
        transport->simulate_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_audit_log({suspension_remote_row()}).to_json()));
    }
    bool has_remote() {
        return reader.find_by_global_id<TestPerson>("background-probe-person").has_value();
    }
};

// A fresh process attempts a fresh file description. It does not inherit any
// database operations and never uses SQLite after fork.
int child_lock_result(const std::string& path) {
    const char* file = path.c_str();
    const auto child = fork();
    if (child == 0) {
        const int fd = open(file, O_RDWR);
        if (fd < 0) _exit(2);
        const int result = flock(fd, LOCK_EX | LOCK_NB);
        if (result == 0) flock(fd, LOCK_UN);
        close(fd);
        _exit(result == 0 ? 0 : 1);
    }
    if (child < 0) return 3;
    int status = 0;
    if (waitpid(child, &status, 0) != child || !WIFEXITED(status)) return 4;
    return WEXITSTATUS(status);
}
} // namespace

TEST(BackgroundSyncCharacterization, DisconnectDoesNotCancelAlreadyQueuedRemoteApply) {
    suspension_fixture fixture;
    fixture.enqueue_remote();
    ASSERT_FALSE(fixture.has_remote());
    fixture.sync->disconnect();
    ASSERT_FALSE(fixture.sync->is_connected());
    fixture.queue->drain();
    EXPECT_TRUE(fixture.has_remote());
}

TEST(BackgroundSyncCharacterization, DestructionCancelsQueuedApplyAndPreservesLocalStore) {
    suspension_fixture fixture;
    fixture.enqueue_remote();
    ASSERT_FALSE(fixture.has_remote());
    ASSERT_TRUE(fixture.transport->get_sent_messages().empty());
    fixture.sync.reset();
    fixture.queue->drain();
    EXPECT_FALSE(fixture.has_remote());
    const auto row = fixture.reader.add(TestPerson{"Local still usable", 29, std::nullopt});
    EXPECT_TRUE(fixture.reader.find_by_global_id<TestPerson>(row.global_id()).has_value());
    // An unapplied queued delivery must have no durable audit/cursor progress.
    EXPECT_TRUE(fixture.reader.db().query(
        "SELECT id FROM AuditLog WHERE globalId='background-probe-audit'").empty());
    EXPECT_TRUE(fixture.reader.db().query(
        "SELECT sync_id FROM _lattice_replication_slots "
        "WHERE last_received_event_id='background-probe-audit'").empty());
    std::lock_guard<std::mutex> lock(fixture.sends->mutex);
    EXPECT_TRUE(fixture.sends->frames.empty());
}

TEST(BackgroundSyncCharacterization, DisconnectRetainsWSSOwnershipUntilClose) {
    struct restore_factory {
        std::shared_ptr<lattice::network_factory> old = lattice::get_network_factory();
        ~restore_factory() { lattice::set_network_factory(old); }
    } restore;
    lattice::set_network_factory(std::make_shared<lattice::mock_network_factory>());
    TempDB file{"background_owner"};
    const std::string lock_path = file.str() + ".sync.lock";
    struct remove_lock {
        const std::string& path;
        ~remove_lock() { std::error_code ignored; std::filesystem::remove(path, ignored); }
    } cleanup{lock_path};
    lattice::configuration config(file.str(), "ws://fixture.invalid/background-owner", "test");
    lattice::lattice_db owner(config);
    ASSERT_TRUE(owner.is_sync_agent());
    EXPECT_EQ(child_lock_result(lock_path), 1);
    owner.disconnect_sync();
    EXPECT_FALSE(owner.is_sync_connected());
    EXPECT_TRUE(owner.is_sync_agent());
    EXPECT_EQ(child_lock_result(lock_path), 1);
    owner.close();
    EXPECT_EQ(child_lock_result(lock_path), 0);
}
#endif
