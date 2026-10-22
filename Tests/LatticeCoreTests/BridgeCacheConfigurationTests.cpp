// sqlite-vec.h is transitively imported by the bridge on Linux; this is an executable, not an extension.
#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <dynamic_object.hpp>

// Exercise the cached bridge constructor Swift uses, on Linux as well as Apple platforms.
namespace {
lattice::SchemaVector cache_schemas() {
    lattice::property_descriptor value;
    value.name = "value";
    value.type = lattice::column_type::text;
    value.kind = lattice::property_kind::primitive;
    lattice::swift_schema_entry table;
    table.table_name = "CacheConfigurationProbe";
    table.properties["value"] = value;
    return {table};
}

void cache_insert(lattice::swift_lattice& store, const lattice::SchemaVector& schemas, const std::string& value) {
    lattice::swift_dynamic_object row;
    row.table_name = schemas[0].table_name;
    row.properties = schemas[0].properties;
    row.values["value"] = value;
    lattice::dynamic_object object(row);
    store.add(object);
}
}

TEST(BridgeCacheConfiguration, ReadOnlyAndWritableOrdersKeepAccessObserversAndCloseIndependent) {
    for (bool reader_first : {false, true}) {
        for (bool close_reader_first : {false, true}) {
            SCOPED_TRACE(reader_first ? "reader-first open" : "writer-first open");
            SCOPED_TRACE(close_reader_first ? "reader-first close" : "writer-first close");
            TempDB tmp{"bridge_cache_access"};
            const auto schemas = cache_schemas();
            lattice::swift_configuration writable_config(tmp.str());
            auto& cache = lattice::detail::LatticeCache::instance();
            {
                auto seed = cache.get_or_create(writable_config, schemas);
                cache_insert(*seed, schemas, "seed");
                seed->close();
            }
            auto readonly_config = writable_config;
            readonly_config.read_only = true;
            std::shared_ptr<lattice::swift_lattice> reader, writer;
            if (reader_first) {
                reader = cache.get_or_create(readonly_config, schemas);
                writer = cache.get_or_create(writable_config, schemas);
            } else {
                writer = cache.get_or_create(writable_config, schemas);
                reader = cache.get_or_create(readonly_config, schemas);
            }
            ASSERT_NE(reader.get(), writer.get());
            EXPECT_TRUE(reader->config().read_only);
            EXPECT_FALSE(writer->config().read_only);
            std::atomic<int> reader_fires{0}, writer_fires{0};
            const auto reader_token = static_cast<lattice::lattice_db&>(*reader).add_table_observer(schemas[0].table_name,
                [&](const auto&) { ++reader_fires; });
            const auto writer_token = static_cast<lattice::lattice_db&>(*writer).add_table_observer(schemas[0].table_name,
                [&](const auto&) { ++writer_fires; });

            EXPECT_ANY_THROW(cache_insert(*reader, schemas, "forbidden"));
            ASSERT_EQ(writer->objects(schemas[0].table_name).size(), 1u);
            cache_insert(*writer, schemas, "writer-one");
            EXPECT_EQ(reader->objects(schemas[0].table_name).size(), 2u);
            // Same-path commits intentionally fan out to both instances, including read-only ones.
            // Removing one instance's observer must not remove the other instance's registration.
            EXPECT_EQ(reader_fires.load(), 1);
            EXPECT_EQ(writer_fires.load(), 1);
            reader->remove_table_observer(schemas[0].table_name, reader_token);
            EXPECT_ANY_THROW(cache_insert(*reader, schemas, "still-forbidden"));
            cache_insert(*writer, schemas, "writer-two");
            EXPECT_EQ(reader->objects(schemas[0].table_name).size(), 3u);
            EXPECT_EQ(reader_fires.load(), 1);
            EXPECT_EQ(writer_fires.load(), 2);
            writer->remove_table_observer(schemas[0].table_name, writer_token);

            if (close_reader_first) {
                reader->close();
                cache_insert(*writer, schemas, "writer-survives");
                EXPECT_EQ(writer->objects(schemas[0].table_name).size(), 4u);
                writer->close();
            } else {
                writer->close();
                EXPECT_EQ(reader->objects(schemas[0].table_name).size(), 3u);
                EXPECT_ANY_THROW(cache_insert(*reader, schemas, "closed-writer-does-not-grant-write"));
                reader->close();
            }
        }
    }
}

TEST(BridgeCacheConfiguration, AuthorityFilterAndMaintenanceSettingsNeverAlias) {
    TempDB tmp{"bridge_cache_configuration"};
    const auto schemas = cache_schemas();
    lattice::swift_configuration base(tmp.str());
    auto& cache = lattice::detail::LatticeCache::instance();
    auto original = cache.get_or_create(base, schemas);
    EXPECT_EQ(original.get(), cache.get_or_create(base, schemas).get());

    // No endpoint is configured: these are local constructor/config tests, not network authentication tests.
    std::vector<lattice::swift_configuration> variants;
    auto token_a = base; token_a.authorization_token = "fixture-authority-A"; variants.push_back(token_a);
    auto token_b = base; token_b.authorization_token = "fixture-authority-B"; variants.push_back(token_b);
    auto observer = base; observer.sync_is_observer = true; variants.push_back(observer);
    auto busy = base; busy.busy_timeout_ms = 111; variants.push_back(busy);
    auto retention = base; retention.audit_retention_seconds = 3600; variants.push_back(retention);
    auto empty_filter = base; empty_filter.sync_filter = std::vector<lattice::sync_filter_entry>{}; variants.push_back(empty_filter);
    auto all_rows = base; all_rows.sync_filter = std::vector<lattice::sync_filter_entry>{{"CacheConfigurationProbe", std::nullopt}};
    variants.push_back(all_rows);
    auto empty_predicate = base; empty_predicate.sync_filter = std::vector<lattice::sync_filter_entry>{{"CacheConfigurationProbe", std::string{}}};
    variants.push_back(empty_predicate);
    auto selected_rows = base; selected_rows.sync_filter = std::vector<lattice::sync_filter_entry>{{"CacheConfigurationProbe", "value = 'visible'"}};
    variants.push_back(selected_rows);
    auto delay_a = base; delay_a.tuning.base_delay_seconds = 1.0000001; variants.push_back(delay_a);
    auto delay_b = base; delay_b.tuning.base_delay_seconds = 1.0000002; variants.push_back(delay_b);

    std::vector<std::shared_ptr<lattice::swift_lattice>> stores{original};
    for (const auto& config : variants) {
        auto opened = cache.get_or_create(config, schemas);
        for (const auto& previous : stores) EXPECT_NE(opened.get(), previous.get());
        EXPECT_EQ(opened.get(), cache.get_or_create(config, schemas).get());
        EXPECT_EQ(opened->config().sync_is_observer, config.sync_is_observer);
        EXPECT_EQ(opened->config().busy_timeout_ms, config.busy_timeout_ms);
        EXPECT_EQ(opened->config().audit_retention_seconds, config.audit_retention_seconds);
        EXPECT_TRUE(opened->config().authorization_token == config.authorization_token);
        EXPECT_EQ(opened->config().sync_filter.has_value(), config.sync_filter.has_value());
        stores.push_back(std::move(opened));
    }
    for (auto& store : stores) store->close();
}

TEST(BridgeCacheConfiguration, FilterAndIPCEncodingPreserveOptionalBoundariesAndNarrowingPolicy) {
    // Pure key evidence: no IPC socket or synchronizer is started by these cases.
    TempDB tmp{"bridge_cache_filter_delimiters"};
    lattice::swift_configuration a(tmp.str()), b(tmp.str());
    a.sync_filter = std::vector<lattice::sync_filter_entry>{{"a:b", "c"}};
    b.sync_filter = std::vector<lattice::sync_filter_entry>{{"a", "b:c"}};
    auto& cache = lattice::detail::LatticeCache::instance();
    auto first = cache.get_or_create(a, cache_schemas());
    auto second = cache.get_or_create(b, cache_schemas());
    EXPECT_NE(first.get(), second.get());
    first->close(); second->close();

    lattice::configuration::ipc_target one, two;
    one.channel = "a@b"; one.socket_path = "c";
    two.channel = "a"; two.socket_path = "b@c";
    a.ipc_targets = {one}; b.ipc_targets = {two};
    EXPECT_FALSE(lattice::detail::ipc_targets_fingerprint(a) == lattice::detail::ipc_targets_fingerprint(b));
    b.ipc_targets = a.ipc_targets;
    EXPECT_TRUE(lattice::detail::ipc_targets_fingerprint(a) == lattice::detail::ipc_targets_fingerprint(b));
    b.ipc_targets[0].narrowing_emits_removals = false;
    EXPECT_FALSE(lattice::detail::ipc_targets_fingerprint(a) == lattice::detail::ipc_targets_fingerprint(b));
    b.ipc_targets = a.ipc_targets;
    b.ipc_targets[0].sync_filter = std::vector<lattice::sync_filter_entry>{};
    EXPECT_FALSE(lattice::detail::ipc_targets_fingerprint(a) == lattice::detail::ipc_targets_fingerprint(b));
    b.ipc_targets = a.ipc_targets;
    a.ipc_targets[0].socket_path = std::nullopt;
    b.ipc_targets[0].socket_path = "";
    EXPECT_FALSE(lattice::detail::ipc_targets_fingerprint(a) == lattice::detail::ipc_targets_fingerprint(b));
}

TEST(BridgeCacheConfiguration, TuningFingerprintPreservesEveryDoubleBitAndOptionalAbsence) {
    // Pure fingerprint checks: non-finite tuning is never supplied to a running synchronizer.
    const uint64_t representations[] = {
        0x0000000000000000ULL, 0x8000000000000000ULL, // positive and negative zero
        0x3ff0000000000000ULL, 0x3ff0000000000001ULL, // adjacent finite values
        0x7ff0000000000000ULL,                       // infinity
        0x7ff8000000000001ULL, 0x7ff8000000000002ULL, // distinct quiet NaN payloads
    };
    for (bool base_delay : {true, false}) {
        lattice::configuration config;
        std::vector<std::string> fingerprints{lattice::detail::sync_tuning_fingerprint(config)};
        auto& delay = base_delay ? config.tuning.base_delay_seconds : config.tuning.max_delay_seconds;
        delay.emplace(0.0);
        for (auto bits : representations) {
            std::memcpy(&*delay, &bits, sizeof(bits));
            EXPECT_EQ(lattice::detail::cache_double_bits(*delay), bits);
            const auto fingerprint = lattice::detail::sync_tuning_fingerprint(config);
            EXPECT_EQ(fingerprint, lattice::detail::sync_tuning_fingerprint(config));
            for (const auto& previous : fingerprints) EXPECT_NE(fingerprint, previous);
            fingerprints.push_back(fingerprint);
        }
    }
}
