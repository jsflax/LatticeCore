// LatticeCAPITests — behavioral tests against the C ABI (C1 slice 2).
//
// Unlike CAPISurfaceTests (which pins the DECLARED surface by parsing
// sources), this target LINKS the C surface and exercises it end-to-end:
// slice 2's additions (sync options, sync progress + observers, detach,
// row cache, increment, statement counters, checkpoint, upload floor) plus
// behavioral pins for slice 1's fixes (the formerly-phantom cross-process
// observers actually fire across instances; receive_sync_data applies with
// per-entry error isolation).
//
// This file includes ONLY lattice.h from the product — everything
// observable must be observable through the C ABI, exactly as a
// Kotlin/Python binding would see it. (nlohmann/json.hpp is included as a
// TEST-SIDE verifier only, to parse lattice_object_to_json output — JSON
// object key order is unspecified, so string comparison would overpin.)
// (SwiftPM: macOS-only executable target `LatticeCAPITests`; CMake: built
// everywhere — see docs/capi-gap-audit.md V1 for why the SwiftPM Linux
// closure must not grow the C API library.)

#include <gtest/gtest.h>

#include "lattice.h"

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

// ============================================================================
// Helpers
// ============================================================================

struct TempPath {
    std::filesystem::path path;

    explicit TempPath(const std::string& name)
        : path(std::filesystem::temp_directory_path()
               / (name + "_" + random_suffix() + ".sqlite")) {}

    ~TempPath() {
        std::error_code ec;
        std::filesystem::remove(path, ec);
        std::filesystem::remove(path.string() + "-wal", ec);
        std::filesystem::remove(path.string() + "-shm", ec);
    }

    TempPath(const TempPath&) = delete;
    TempPath& operator=(const TempPath&) = delete;

    std::string str() const { return path.string(); }

private:
    static std::string random_suffix() {
        static std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<uint32_t> dist;
        return std::to_string(dist(rng));
    }
};

// Person schema: name TEXT, age INTEGER (+ optional extra column for
// schema-mismatch fixtures).
struct PersonSchema {
    lattice_property_t props[3];
    lattice_schema_t schema;

    explicit PersonSchema(bool with_extra_column = false) {
        std::memset(props, 0, sizeof(props));
        props[0].name = "name";
        props[0].type = LATTICE_TYPE_TEXT;
        props[0].kind = LATTICE_KIND_PRIMITIVE;
        props[0].nullable = true;
        props[1].name = "age";
        props[1].type = LATTICE_TYPE_INTEGER;
        props[1].kind = LATTICE_KIND_PRIMITIVE;
        props[1].nullable = true;
        props[2].name = "extra";
        props[2].type = LATTICE_TYPE_TEXT;
        props[2].kind = LATTICE_KIND_PRIMITIVE;
        props[2].nullable = true;
        std::memset(&schema, 0, sizeof(schema));
        schema.table_name = "Person";
        schema.properties = props;
        schema.property_count = with_extra_column ? 3u : 2u;
    }
};

// Adds a Person and returns the managed object handle (caller releases once).
lattice_object_t* add_person(lattice_db_t* db, const char* name, int64_t age) {
    lattice_object_t* obj = lattice_db_create_object(db, "Person");
    EXPECT_NE(obj, nullptr) << lattice_last_error();
    if (!obj) return nullptr;
    lattice_object_set_string(obj, "name", name);
    lattice_object_set_int(obj, "age", age);
    lattice_object_t* managed = lattice_db_add(db, obj);
    EXPECT_NE(managed, nullptr) << lattice_last_error();
    if (managed) {
        // lattice_db_add returns the same handle with an extra retain.
        lattice_object_release(managed);
    }
    return obj;
}

bool wait_for(const std::function<bool()>& cond,
              std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (cond()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return cond();
}

size_t count_json_strings(const char* json_array) {
    if (!json_array) return 0;
    size_t quotes = 0;
    for (const char* p = json_array; *p; p++) {
        if (*p == '"' && (p == json_array || p[-1] != '\\')) quotes++;
    }
    return quotes / 2;
}

// Inline scheduler for lattice_db_create_with_scheduler in tests.
void inline_invoke(void* /*context*/, void (*callback)(void*), void* callback_context) {
    callback(callback_context);
}

}  // namespace

// ============================================================================
// Feature flags (slice 2 flips)
// ============================================================================

TEST(CAPIFeatures, Slice2FeatureStringsAreTrue) {
    for (const char* f : {"checkpoint", "detach", "row_cache", "statement_counters",
                          "sync_progress", "sync_tuning", "to_json"}) {
        EXPECT_TRUE(lattice_capi_has_feature(f)) << f;
    }
    // Still reserved. unified_open deliberately stays false at 1.0 — the
    // drop-gate decision is documented in docs/design-unified-open.md.
    for (const char* f : {"read_generations", "unified_open"}) {
        EXPECT_FALSE(lattice_capi_has_feature(f)) << f;
    }
    EXPECT_FALSE(lattice_capi_has_feature("no_such_feature"));
    EXPECT_FALSE(lattice_capi_has_feature(nullptr));
}

// ============================================================================
// Sync options
// ============================================================================

TEST(CAPISyncOptions, InitFillsDefaults) {
    lattice_sync_options_t opts;
    std::memset(&opts, 0xAB, sizeof(opts));  // poison
    lattice_sync_options_init(&opts);
    EXPECT_EQ(opts.struct_size, sizeof(lattice_sync_options_t));
    EXPECT_EQ(opts.chunk_size, 0);
    EXPECT_EQ(opts.max_reconnect_attempts, -1);
    EXPECT_EQ(opts.base_delay_seconds, 0.0);
    EXPECT_EQ(opts.max_delay_seconds, 0.0);
    EXPECT_EQ(opts.stable_connection_ms, -1);
    EXPECT_EQ(opts.upload_coalesce_ms, -1);
    EXPECT_EQ(opts.checkpoint_passive_interval_ms, -1);
    EXPECT_EQ(opts.checkpoint_truncate_interval_ms, -1);
    EXPECT_EQ(opts.use_upload_floor, -1);
    EXPECT_EQ(opts.sync_filter_json, nullptr);
    EXPECT_EQ(opts.sync_id, nullptr);
    lattice_sync_options_init(nullptr);  // documented no-op
}

TEST(CAPISyncOptions, RoundTripThroughRealOpen) {
    TempPath path("capi_syncopts");
    PersonSchema ps;

    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    opts.chunk_size = 512;
    opts.max_reconnect_attempts = 7;
    opts.base_delay_seconds = 0.5;
    opts.max_delay_seconds = 30.0;
    opts.stable_connection_ms = 4000;
    opts.upload_coalesce_ms = 25;
    opts.checkpoint_passive_interval_ms = 1000;
    opts.checkpoint_truncate_interval_ms = 60000;
    opts.use_upload_floor = 1;
    opts.sync_filter_json = R"([{"table":"Person","predicate":"age > 10"}])";

    // No WSS endpoint: the tuning overlays configuration::sync_tuning and the
    // open must succeed and behave as a fully functional database.
    lattice_db_t* db = lattice_db_create_with_sync_options(
        path.str().c_str(), &ps.schema, 1, nullptr, nullptr, nullptr, &opts);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    lattice_object_t* p = add_person(db, "tuned", 42);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 1u);
    lattice_object_release(p);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPISyncOptions, NullOptionsBehavesLikeCreateWithSync) {
    TempPath path("capi_syncopts_null");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_sync_options(
        path.str().c_str(), &ps.schema, 1, nullptr, nullptr, nullptr, nullptr);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    lattice_object_t* p = add_person(db, "plain", 1);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 1u);
    lattice_object_release(p);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPISyncOptions, NonsenseValuesAreIgnoredNotFatal) {
    TempPath path("capi_syncopts_junk");
    PersonSchema ps;

    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    // Mirror of the bridge setters: all of these are IGNORED (knob keeps the
    // library default) — the open must still succeed.
    opts.chunk_size = -5;
    opts.max_reconnect_attempts = -3;
    opts.base_delay_seconds = -1.0;
    opts.max_delay_seconds = -2.0;
    opts.stable_connection_ms = -100;
    opts.upload_coalesce_ms = -7;
    opts.checkpoint_passive_interval_ms = -1;
    opts.checkpoint_truncate_interval_ms = -9;

    lattice_db_t* db = lattice_db_create_with_sync_options(
        path.str().c_str(), &ps.schema, 1, nullptr, nullptr, nullptr, &opts);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    lattice_object_t* p = add_person(db, "junk", 2);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 1u);
    lattice_object_release(p);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPISyncOptions, ZeroStructSizeFailsTheOpen) {
    TempPath path("capi_syncopts_zsz");
    PersonSchema ps;
    lattice_sync_options_t opts;
    std::memset(&opts, 0, sizeof(opts));  // struct_size == 0: init was skipped
    lattice_db_t* db = lattice_db_create_with_sync_options(
        path.str().c_str(), &ps.schema, 1, nullptr, nullptr, nullptr, &opts);
    EXPECT_EQ(db, nullptr);
    ASSERT_NE(lattice_last_error(), nullptr);
    EXPECT_NE(std::string(lattice_last_error()).find("struct_size"), std::string::npos);
}

TEST(CAPISyncOptions, ReservedSyncIdFailsTheOpen) {
    TempPath path("capi_syncopts_sid");
    PersonSchema ps;
    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    opts.sync_id = "wss:custom";
    lattice_db_t* db = lattice_db_create_with_sync_options(
        path.str().c_str(), &ps.schema, 1, nullptr, nullptr, nullptr, &opts);
    EXPECT_EQ(db, nullptr);
    ASSERT_NE(lattice_last_error(), nullptr);
    EXPECT_NE(std::string(lattice_last_error()).find("sync_id"), std::string::npos);
}

TEST(CAPISyncOptions, MalformedFilterJsonFailsTheOpen) {
    TempPath path("capi_syncopts_badjson");
    PersonSchema ps;
    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    opts.sync_filter_json = "{\"table\": \"Person\"}";  // object, not array
    lattice_db_t* db = lattice_db_create_with_sync_options(
        path.str().c_str(), &ps.schema, 1, nullptr, nullptr, nullptr, &opts);
    EXPECT_EQ(db, nullptr);
    EXPECT_NE(lattice_last_error(), nullptr);
}

TEST(CAPISyncOptions, ShorterCallerStructIsAccepted) {
    TempPath path("capi_syncopts_short");
    PersonSchema ps;
    // Simulate an old caller compiled against a struct that ended after the
    // tuning knobs: the impl must read only the declared prefix and default
    // the rest (the pointers land at their NULL defaults).
    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    opts.chunk_size = 64;
    opts.struct_size = offsetof(lattice_sync_options_t, sync_filter_json);
    // Poison the tail: a size-respecting impl must never read past struct_size.
    opts.sync_filter_json = reinterpret_cast<const char*>(0x1);
    opts.sync_id = reinterpret_cast<const char*>(0x1);

    lattice_db_t* db = lattice_db_create_with_sync_options(
        path.str().c_str(), &ps.schema, 1, nullptr, nullptr, nullptr, &opts);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    lattice_db_close(db);
    lattice_db_release(db);
}

// ============================================================================
// Sync progress
// ============================================================================

TEST(CAPISyncProgress, ZeroedOnSyncLessDb) {
    TempPath path("capi_progress");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    // A live db with neither WSS nor IPC sync: LATTICE_OK, all counters zero.
    lattice_sync_progress_t progress;
    std::memset(&progress, 0xCD, sizeof(progress));  // poison
    progress.struct_size = sizeof(progress);
    ASSERT_EQ(lattice_db_get_sync_progress(db, &progress), LATTICE_OK)
        << lattice_last_error();
    EXPECT_EQ(progress.struct_size, sizeof(progress));
    EXPECT_EQ(progress.pending_upload, 0);
    EXPECT_EQ(progress.total_upload, 0);
    EXPECT_EQ(progress.acked, 0);
    EXPECT_EQ(progress.received, 0);

    // Writes don't invent progress without a synchronizer.
    lattice_object_t* p = add_person(db, "noprog", 3);
    ASSERT_NE(p, nullptr);
    lattice_object_release(p);
    progress.struct_size = sizeof(progress);
    ASSERT_EQ(lattice_db_get_sync_progress(db, &progress), LATTICE_OK);
    EXPECT_EQ(progress.pending_upload, 0);
    EXPECT_EQ(progress.received, 0);

    // Size-prefix contract: a shorter caller struct gets only its prefix.
    lattice_sync_progress_t small;
    std::memset(&small, 0, sizeof(small));
    small.struct_size = offsetof(lattice_sync_progress_t, acked);
    small.acked = -99;  // beyond declared size: must NOT be written
    ASSERT_EQ(lattice_db_get_sync_progress(db, &small), LATTICE_OK);
    EXPECT_EQ(small.struct_size, offsetof(lattice_sync_progress_t, acked));
    EXPECT_EQ(small.acked, -99);

    // Invalid args.
    EXPECT_EQ(lattice_db_get_sync_progress(nullptr, &progress), LATTICE_ERROR_NULL_POINTER);
    EXPECT_EQ(lattice_db_get_sync_progress(db, nullptr), LATTICE_ERROR_NULL_POINTER);
    lattice_sync_progress_t zero_size;
    std::memset(&zero_size, 0, sizeof(zero_size));
    EXPECT_EQ(lattice_db_get_sync_progress(db, &zero_size), LATTICE_ERROR_INVALID_ARGUMENT);

    lattice_db_close(db);
    lattice_db_release(db);
}

// ============================================================================
// Sync observers (registration surface; callbacks need a live transport)
// ============================================================================

namespace {
struct ObserverProbe {
    std::atomic<int> destroyed{0};
};
void probe_destroy(void* ctx) { static_cast<ObserverProbe*>(ctx)->destroyed++; }
void noop_progress(void*, const lattice_sync_progress_t*) {}
void noop_state(void*, bool) {}
void noop_error(void*, const char*) {}
}  // namespace

TEST(CAPISyncObservers, RegisterRemoveAndDestroySemantics) {
    TempPath path("capi_syncobs");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    ObserverProbe p1, p2, p3;
    uint64_t t1 = lattice_db_observe_sync_progress(db, &p1, noop_progress, probe_destroy);
    uint64_t t2 = lattice_db_observe_sync_state(db, &p2, noop_state, probe_destroy);
    uint64_t t3 = lattice_db_observe_sync_error(db, &p3, noop_error, probe_destroy);
    ASSERT_NE(t1, 0u);
    ASSERT_NE(t2, 0u);
    ASSERT_NE(t3, 0u);
    // Tokens are shared across the three kinds: all distinct.
    EXPECT_NE(t1, t2);
    EXPECT_NE(t2, t3);
    EXPECT_NE(t1, t3);

    // Removal fires destroy exactly once, for the removed observer only.
    lattice_db_remove_sync_observer(db, t2);
    EXPECT_EQ(p2.destroyed.load(), 1);
    EXPECT_EQ(p1.destroyed.load(), 0);
    EXPECT_EQ(p3.destroyed.load(), 0);

    // Unknown token / double-remove: documented no-op.
    lattice_db_remove_sync_observer(db, t2);
    lattice_db_remove_sync_observer(db, 424242);
    EXPECT_EQ(p2.destroyed.load(), 1);

    // Invalid registrations return 0.
    EXPECT_EQ(lattice_db_observe_sync_progress(nullptr, &p1, noop_progress, nullptr), 0u);
    EXPECT_EQ(lattice_db_observe_sync_progress(db, &p1, nullptr, nullptr), 0u);

    // close() removes remaining observers and fires their destroys.
    lattice_db_close(db);
    EXPECT_EQ(p1.destroyed.load(), 1);
    EXPECT_EQ(p3.destroyed.load(), 1);
    EXPECT_EQ(p2.destroyed.load(), 1);  // not double-destroyed

    lattice_db_release(db);
}

TEST(CAPISyncObservers, MultipleObserversPerKind) {
    TempPath path("capi_syncobs_multi");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    ObserverProbe a, b;
    uint64_t ta = lattice_db_observe_sync_progress(db, &a, noop_progress, probe_destroy);
    uint64_t tb = lattice_db_observe_sync_progress(db, &b, noop_progress, probe_destroy);
    ASSERT_NE(ta, 0u);
    ASSERT_NE(tb, 0u);
    EXPECT_NE(ta, tb);

    lattice_db_remove_sync_observer(db, ta);
    EXPECT_EQ(a.destroyed.load(), 1);
    EXPECT_EQ(b.destroyed.load(), 0);
    lattice_db_remove_sync_observer(db, tb);
    EXPECT_EQ(b.destroyed.load(), 1);

    lattice_db_close(db);
    lattice_db_release(db);
}

// ============================================================================
// Detach + last_attach_error
// ============================================================================

TEST(CAPIAttachDetach, RoundTripAndIdempotence) {
    TempPath a_path("capi_att_a"), b_path("capi_att_b");
    PersonSchema ps;
    lattice_db_t* a = lattice_db_create_with_schemas(a_path.str().c_str(), &ps.schema, 1);
    lattice_db_t* b = lattice_db_create_with_schemas(b_path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(a, nullptr) << lattice_last_error();
    ASSERT_NE(b, nullptr) << lattice_last_error();

    lattice_object_t* pa = add_person(a, "in-a", 1);
    lattice_object_t* pb = add_person(b, "in-b", 2);
    ASSERT_NE(pa, nullptr);
    ASSERT_NE(pb, nullptr);

    EXPECT_EQ(lattice_db_last_attach_error(a), nullptr);

    ASSERT_EQ(lattice_db_attach(a, b), LATTICE_OK) << lattice_last_error();
    EXPECT_EQ(lattice_db_count(a, "Person", nullptr), 2u);  // union view

    ASSERT_EQ(lattice_db_detach(a, b), LATTICE_OK) << lattice_last_error();
    EXPECT_EQ(lattice_db_count(a, "Person", nullptr), 1u);  // main only

    // Idempotent: detaching a non-attached database succeeds.
    EXPECT_EQ(lattice_db_detach(a, b), LATTICE_OK) << lattice_last_error();
    EXPECT_EQ(lattice_db_last_attach_error(a), nullptr);

    // Invalid args.
    EXPECT_EQ(lattice_db_detach(nullptr, b), LATTICE_ERROR_NULL_POINTER);
    EXPECT_EQ(lattice_db_detach(a, nullptr), LATTICE_ERROR_NULL_POINTER);

    lattice_object_release(pa);
    lattice_object_release(pb);
    lattice_db_close(a);
    lattice_db_close(b);
    lattice_db_release(a);
    lattice_db_release(b);
}

TEST(CAPIAttachDetach, SchemaMismatchSurfacesAttachError) {
    TempPath a_path("capi_att_mm_a"), c_path("capi_att_mm_c");
    PersonSchema ps;
    PersonSchema ps_extra(/*with_extra_column=*/true);
    lattice_db_t* a = lattice_db_create_with_schemas(a_path.str().c_str(), &ps.schema, 1);
    lattice_db_t* c = lattice_db_create_with_schemas(c_path.str().c_str(), &ps_extra.schema, 1);
    ASSERT_NE(a, nullptr) << lattice_last_error();
    ASSERT_NE(c, nullptr) << lattice_last_error();

    lattice_object_t* pa = add_person(a, "main", 1);
    ASSERT_NE(pa, nullptr);

    // Same table name, diverged columns: attach must FAIL LOUDLY (slice 1
    // fixed the silent-LATTICE_OK regression) and expose the reason.
    EXPECT_EQ(lattice_db_attach(a, c), LATTICE_ERROR_DATABASE);
    EXPECT_NE(lattice_last_error(), nullptr);
    const char* reason = lattice_db_last_attach_error(a);
    ASSERT_NE(reason, nullptr);
    EXPECT_GT(std::strlen(reason), 0u);

    // Main database unaffected by the failed attach.
    EXPECT_EQ(lattice_db_count(a, "Person", nullptr), 1u);

    lattice_object_release(pa);
    lattice_db_close(a);
    lattice_db_close(c);
    lattice_db_release(a);
    lattice_db_release(c);
}

// ============================================================================
// Row cache + increment
// ============================================================================

TEST(CAPIRowCache, LifecycleOnManagedObject) {
    TempPath path("capi_rowcache");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    lattice_object_t* p = add_person(db, "cached", 30);
    ASSERT_NE(p, nullptr);

    EXPECT_FALSE(lattice_object_is_row_cache_enabled(p));
    ASSERT_EQ(lattice_object_enable_row_cache(p), LATTICE_OK) << lattice_last_error();
    EXPECT_TRUE(lattice_object_is_row_cache_enabled(p));

    // Snapshot reads are correct.
    EXPECT_STREQ(lattice_object_get_string(p, "name"), "cached");
    EXPECT_EQ(lattice_object_get_int(p, "age"), 30);

    // Write-through: read-your-writes holds while cached.
    lattice_object_set_int(p, "age", 31);
    EXPECT_EQ(lattice_object_get_int(p, "age"), 31);

    // Refresh (staleness escape hatch) keeps values consistent.
    ASSERT_EQ(lattice_object_refresh_row_cache(p), LATTICE_OK) << lattice_last_error();
    EXPECT_EQ(lattice_object_get_int(p, "age"), 31);

    ASSERT_EQ(lattice_object_disable_row_cache(p), LATTICE_OK);
    EXPECT_FALSE(lattice_object_is_row_cache_enabled(p));
    EXPECT_EQ(lattice_object_get_int(p, "age"), 31);  // live path agrees

    // Null-arg contract.
    EXPECT_EQ(lattice_object_enable_row_cache(nullptr), LATTICE_ERROR_NULL_POINTER);
    EXPECT_FALSE(lattice_object_is_row_cache_enabled(nullptr));

    lattice_object_release(p);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPIRowCache, NoOpOnUnmanagedObject) {
    PersonSchema ps;
    lattice_object_t* obj =
        lattice_object_create_with_schema("Person", ps.props, ps.schema.property_count);
    ASSERT_NE(obj, nullptr) << lattice_last_error();
    // Unmanaged objects are already value snapshots: enable is a documented
    // no-op and the flag stays false.
    EXPECT_EQ(lattice_object_enable_row_cache(obj), LATTICE_OK);
    EXPECT_FALSE(lattice_object_is_row_cache_enabled(obj));
    lattice_object_release(obj);
}

TEST(CAPIIncrement, SqlSideIncrementPersists) {
    TempPath path("capi_incr");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    lattice_object_t* p = add_person(db, "counter", 40);
    ASSERT_NE(p, nullptr);
    int64_t id = lattice_object_get_id(p);
    ASSERT_GT(id, 0);

    ASSERT_EQ(lattice_object_increment_int(p, "age", 2), LATTICE_OK) << lattice_last_error();
    ASSERT_EQ(lattice_object_increment_int(p, "age", -1), LATTICE_OK) << lattice_last_error();
    EXPECT_EQ(lattice_object_get_int(p, "age"), 41);

    // The increment is SQL-side: a FRESH handle reads the same value.
    lattice_object_t* fresh = lattice_db_find(db, "Person", id);
    ASSERT_NE(fresh, nullptr) << lattice_last_error();
    EXPECT_EQ(lattice_object_get_int(fresh, "age"), 41);
    lattice_object_release(fresh);

    EXPECT_EQ(lattice_object_increment_int(nullptr, "age", 1), LATTICE_ERROR_NULL_POINTER);
    EXPECT_EQ(lattice_object_increment_int(p, nullptr, 1), LATTICE_ERROR_NULL_POINTER);

    lattice_object_release(p);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPIIncrement, InMemoryIncrementOnUnmanagedObject) {
    PersonSchema ps;
    lattice_object_t* obj =
        lattice_object_create_with_schema("Person", ps.props, ps.schema.property_count);
    ASSERT_NE(obj, nullptr) << lattice_last_error();
    lattice_object_set_int(obj, "age", 5);
    ASSERT_EQ(lattice_object_increment_int(obj, "age", 3), LATTICE_OK);
    EXPECT_EQ(lattice_object_get_int(obj, "age"), 8);
    lattice_object_release(obj);
}

// ============================================================================
// Statement counters
// ============================================================================

TEST(CAPIStatementCounters, CountersAdvanceWithQueries) {
    TempPath path("capi_counters");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    uint64_t total0 = lattice_db_total_statement_count();
    uint64_t thread0 = lattice_db_thread_statement_count();

    lattice_object_t* p = add_person(db, "count-me", 7);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 1u);

    EXPECT_GT(lattice_db_total_statement_count(), total0);
    EXPECT_GT(lattice_db_thread_statement_count(), thread0);
    // The process-global counter includes at least everything this thread did.
    EXPECT_GE(lattice_db_total_statement_count() - total0,
              lattice_db_thread_statement_count() - thread0);

    lattice_object_release(p);
    lattice_db_close(db);
    lattice_db_release(db);
}

// ============================================================================
// Checkpoint
// ============================================================================

TEST(CAPICheckpoint, PassiveAndTruncateModes) {
    TempPath path("capi_ckpt");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    for (int i = 0; i < 10; i++) {
        lattice_object_t* p = add_person(db, "walrow", i);
        ASSERT_NE(p, nullptr);
        lattice_object_release(p);
    }

    EXPECT_EQ(lattice_db_checkpoint(db, 0), LATTICE_OK) << lattice_last_error();  // passive
    EXPECT_EQ(lattice_db_checkpoint(db, 1), LATTICE_OK) << lattice_last_error();  // truncate
    // Data intact after both.
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 10u);

    EXPECT_EQ(lattice_db_checkpoint(db, 2), LATTICE_ERROR_INVALID_ARGUMENT);
    EXPECT_EQ(lattice_db_checkpoint(db, -1), LATTICE_ERROR_INVALID_ARGUMENT);
    EXPECT_EQ(lattice_db_checkpoint(nullptr, 0), LATTICE_ERROR_NULL_POINTER);

    lattice_db_close(db);
    lattice_db_release(db);
}

// ============================================================================
// Upload floor
// ============================================================================

TEST(CAPIUploadFloor, ZeroWithoutSlotAndInvalidArgs) {
    TempPath path("capi_floor");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    // No synchronizer has ever registered this slot: floor is 0.
    EXPECT_EQ(lattice_db_read_upload_floor(db, "wss:wss://nowhere.example/sync"), 0);
    EXPECT_EQ(lattice_db_read_upload_floor(db, "ipc:none"), 0);

    EXPECT_EQ(lattice_db_read_upload_floor(nullptr, "wss:x"), -1);
    EXPECT_EQ(lattice_db_read_upload_floor(db, nullptr), -1);

    lattice_db_close(db);
    lattice_db_release(db);
}

// ============================================================================
// Slice-1 behavioral pins
// ============================================================================

namespace {
struct XprocProbe {
    std::atomic<int> fired{0};
    std::string last_op;
    std::mutex op_mutex;
};

void xproc_callback(void* ctx, const char* operation, int64_t /*row_id*/,
                    const char* /*global_id*/) {
    auto* probe = static_cast<XprocProbe*>(ctx);
    {
        std::lock_guard<std::mutex> lock(probe->op_mutex);
        probe->last_op = operation ? operation : "";
    }
    probe->fired++;
}
}  // namespace

// Slice 1 implemented the two formerly-phantom symbols. Pin the BEHAVIOR:
// an observer registered through lattice_db_observe_cross_process fires when
// a DIFFERENT database instance on the same path commits a write (the same
// shared dispatch path cross-process delivery rides; a sibling in-process
// instance is the deterministic way to exercise it in one test process).
TEST(CAPICrossProcessObservation, ObserverFiresForSiblingInstanceWrites) {
    TempPath path("capi_xproc");
    PersonSchema ps;

    // Instance A: plain open (immediate scheduler → callbacks fire inline).
    lattice_db_t* a = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(a, nullptr) << lattice_last_error();

    // Instance B: same path, DIFFERENT cache key (explicit scheduler) →
    // a genuinely distinct instance whose writes reach A only through the
    // shared change-dispatch path.
    lattice_scheduler_t* sched = lattice_scheduler_create(nullptr, inline_invoke, nullptr);
    ASSERT_NE(sched, nullptr);
    lattice_db_t* b = lattice_db_create_with_scheduler(path.str().c_str(), &ps.schema, 1, sched);
    ASSERT_NE(b, nullptr) << lattice_last_error();

    XprocProbe probe;
    uint64_t token = lattice_db_observe_cross_process(a, "Person", &probe, xproc_callback);
    ASSERT_NE(token, 0u) << lattice_last_error();

    lattice_object_t* p = add_person(b, "from-b", 11);
    ASSERT_NE(p, nullptr);

    ASSERT_TRUE(wait_for([&] { return probe.fired.load() > 0; }))
        << "cross-process observer never fired for a sibling-instance write";
    {
        std::lock_guard<std::mutex> lock(probe.op_mutex);
        EXPECT_EQ(probe.last_op, "INSERT");
    }

    // Removal stops delivery. Settle any in-flight dispatch first.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    lattice_db_remove_cross_process_observer(a, token);
    int fired_at_removal = probe.fired.load();

    lattice_object_t* p2 = add_person(b, "after-remove", 12);
    ASSERT_NE(p2, nullptr);
    std::this_thread::sleep_for(std::chrono::milliseconds(750));
    EXPECT_EQ(probe.fired.load(), fired_at_removal)
        << "observer fired after removal";

    lattice_object_release(p);
    lattice_object_release(p2);
    lattice_db_close(b);
    lattice_db_release(b);
    lattice_scheduler_release(sched);
    lattice_db_close(a);
    lattice_db_release(a);
}

// Slice 1 rerouted lattice_db_receive_sync_data through the core
// apply_remote_changes path. Pin the per-entry isolation contract: one bad
// entry in a batch is skipped, the good entries apply, and ONLY the applied
// ids come back (unapplied entries must not be acked).
TEST(CAPIReceiveSyncData, NackReturnsErrorInsteadOfEmptySuccess) {
    TempPath path("capi_receive_nack");
    PersonSchema schema;
    auto* db = lattice_db_create_with_schemas(path.str().c_str(), &schema.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    auto* person = add_person(db, "Local pending value", 7);
    ASSERT_NE(person, nullptr);

    char* raw_before = lattice_db_events_after(db, nullptr);
    ASSERT_NE(raw_before, nullptr) << lattice_last_error();
    const auto before = nlohmann::json::parse(raw_before);
    lattice_string_free(raw_before);
    ASSERT_FALSE(before.empty());
    const nlohmann::json nack = {
        {"kind", "nack"}, {"nack", {before.at(0).at("globalId")}},
        {"nackReason", "relay refused this upload"}
    };
    const auto wire = nack.dump();
    char* result = lattice_db_receive_sync_data(
        db, reinterpret_cast<const uint8_t*>(wire.data()), wire.size());
    EXPECT_EQ(result, nullptr);
    ASSERT_NE(lattice_last_error(), nullptr);
    EXPECT_STREQ(lattice_last_error(), "Sync upload rejected: relay refused this upload");
    if (result) lattice_string_free(result);

    // The error must not acknowledge or delete the local audit entry.
    char* raw_after = lattice_db_events_after(db, nullptr);
    ASSERT_NE(raw_after, nullptr) << lattice_last_error();
    EXPECT_EQ(nlohmann::json::parse(raw_after), before);
    lattice_string_free(raw_after);
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 1u);

    const std::string ack = R"({"kind":"ack","ack":[]})";
    result = lattice_db_receive_sync_data(
        db, reinterpret_cast<const uint8_t*>(ack.data()), ack.size());
    ASSERT_NE(result, nullptr) << lattice_last_error();
    EXPECT_EQ(nlohmann::json::parse(result), nlohmann::json::array());
    lattice_string_free(result);
    lattice_object_release(person);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPIReceiveSyncData, PerEntryErrorIsolation) {
    TempPath src_path("capi_recv_src"), dst_path("capi_recv_dst");
    PersonSchema ps;
    lattice_db_t* src = lattice_db_create_with_schemas(src_path.str().c_str(), &ps.schema, 1);
    lattice_db_t* dst = lattice_db_create_with_schemas(dst_path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(src, nullptr) << lattice_last_error();
    ASSERT_NE(dst, nullptr) << lattice_last_error();

    lattice_object_t* p1 = add_person(src, "good-1", 1);
    lattice_object_t* p2 = add_person(src, "good-2", 2);
    ASSERT_NE(p1, nullptr);
    ASSERT_NE(p2, nullptr);

    char* entries = lattice_db_events_after(src, nullptr);
    ASSERT_NE(entries, nullptr) << lattice_last_error();
    std::string entries_json(entries);
    lattice_string_free(entries);

    // Tamper the FIRST entry to target a table the receiver doesn't have —
    // that entry must fail to apply while the second still lands.
    //
    // The name is deliberately underscore-prefixed (an INTERNAL table). A3's
    // schema-skew tolerance gates on `is_model_table` (first char != '_'), so
    // an unknown MODEL table is now a defined, acked no-op — that contract is
    // pinned separately by SkewTolerance.UnknownTableSkippedAndAcked. An
    // unknown internal table bypasses both skew gates and still hard-fails,
    // which is the failure mode this test needs to keep pinning per-entry
    // isolation. Do not "simplify" this back to a bare table name.
    const std::string needle = "\"tableName\":\"Person\"";
    auto pos = entries_json.find(needle);
    ASSERT_NE(pos, std::string::npos) << entries_json;
    entries_json.replace(pos, needle.size(), "\"tableName\":\"_NoSuchTable\"");
    ASSERT_NE(entries_json.find("\"tableName\":\"Person\""), std::string::npos)
        << "expected a second, untampered entry";

    char* message = lattice_create_sync_message(entries_json.c_str());
    ASSERT_NE(message, nullptr) << lattice_last_error();

    char* applied = lattice_db_receive_sync_data(
        dst, reinterpret_cast<const uint8_t*>(message), std::strlen(message));
    ASSERT_NE(applied, nullptr)
        << "one bad entry must not fail the whole batch: "
        << (lattice_last_error() ? lattice_last_error() : "(no error)");

    // Exactly ONE id applied (the untampered entry), and exactly one Person
    // row landed in the receiver.
    EXPECT_EQ(count_json_strings(applied), 1u) << applied;
    EXPECT_EQ(lattice_db_count(dst, "Person", nullptr), 1u);

    lattice_string_free(applied);
    lattice_string_free(message);
    lattice_object_release(p1);
    lattice_object_release(p2);
    lattice_db_close(src);
    lattice_db_close(dst);
    lattice_db_release(src);
    lattice_db_release(dst);
}

// ============================================================================
// lattice_object_to_json (C1 slice 3) — contract pinned to Swift's
// DynamicObject.jsonObject(maxDepth:) shape (see lattice.h for the full
// contract). Output is PARSED, never string-compared: key order within JSON
// objects is explicitly unspecified.
// ============================================================================

namespace {

// Author {name, bio, note TEXT; score REAL; photo BLOB; pet -> Pet;
// friends -> [Author]} / Pet {name TEXT; owner -> Author}.
struct GraphSchema {
    lattice_property_t author_props[7];
    lattice_property_t pet_props[2];
    lattice_schema_t schemas[2];

    GraphSchema() {
        std::memset(author_props, 0, sizeof(author_props));
        std::memset(pet_props, 0, sizeof(pet_props));
        auto text = [](lattice_property_t& p, const char* name) {
            p.name = name;
            p.type = LATTICE_TYPE_TEXT;
            p.kind = LATTICE_KIND_PRIMITIVE;
            p.nullable = true;
        };
        text(author_props[0], "name");
        text(author_props[1], "bio");
        text(author_props[2], "note");
        author_props[3].name = "score";
        author_props[3].type = LATTICE_TYPE_REAL;
        author_props[3].kind = LATTICE_KIND_PRIMITIVE;
        author_props[3].nullable = true;
        author_props[4].name = "photo";
        author_props[4].type = LATTICE_TYPE_BLOB;
        author_props[4].kind = LATTICE_KIND_PRIMITIVE;
        author_props[4].nullable = true;
        author_props[5].name = "pet";
        author_props[5].type = LATTICE_TYPE_TEXT;
        author_props[5].kind = LATTICE_KIND_LINK;
        author_props[5].target_table = "Pet";
        author_props[5].nullable = true;
        author_props[6].name = "friends";
        author_props[6].type = LATTICE_TYPE_TEXT;
        author_props[6].kind = LATTICE_KIND_LINK_LIST;
        author_props[6].target_table = "Author";
        author_props[6].nullable = true;

        text(pet_props[0], "name");
        pet_props[1].name = "owner";
        pet_props[1].type = LATTICE_TYPE_TEXT;
        pet_props[1].kind = LATTICE_KIND_LINK;
        pet_props[1].target_table = "Author";
        pet_props[1].nullable = true;

        std::memset(schemas, 0, sizeof(schemas));
        schemas[0].table_name = "Author";
        schemas[0].properties = author_props;
        schemas[0].property_count = 7;
        schemas[1].table_name = "Pet";
        schemas[1].properties = pet_props;
        schemas[1].property_count = 2;
    }
};

// to_json + parse, freeing the C string.
nlohmann::json parsed_json(lattice_object_t* obj, int32_t max_depth) {
    char* json = lattice_object_to_json(obj, max_depth);
    EXPECT_NE(json, nullptr) << (lattice_last_error() ? lattice_last_error() : "");
    if (!json) return nlohmann::json::value_t::discarded;
    auto parsed = nlohmann::json::parse(json, nullptr, /*allow_exceptions=*/false);
    EXPECT_FALSE(parsed.is_discarded()) << json;
    lattice_string_free(json);
    return parsed;
}

std::string global_id_of(lattice_object_t* obj) {
    const char* gid = lattice_object_get_global_id(obj);
    return gid ? gid : "";
}

}  // namespace

TEST(CAPIToJson, ScalarTypesBlobBase64AndEmbeddedJson) {
    TempPath path("capi_tojson_scalars");
    GraphSchema gs;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), gs.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    lattice_object_t* a = lattice_db_create_object(db, "Author");
    ASSERT_NE(a, nullptr) << lattice_last_error();
    lattice_object_set_string(a, "name", "Ada");
    lattice_object_set_string(a, "bio", "[1,2,3]");        // embedded JSON -> inlined
    lattice_object_set_string(a, "note", "{not json");     // '{' but unparseable -> raw
    lattice_object_set_double(a, "score", 2.5);
    const uint8_t photo[] = {0x00, 0x01, 0xFF};
    lattice_object_set_blob(a, "photo", photo, sizeof(photo));
    ASSERT_NE(lattice_db_add(db, a), nullptr) << lattice_last_error();
    lattice_object_release(a);  // drop the add's extra retain

    nlohmann::json j = parsed_json(a, 0);
    EXPECT_EQ(j["name"], "Ada");
    EXPECT_EQ(j["bio"], nlohmann::json::parse("[1,2,3]"));  // inlined, not a string
    EXPECT_EQ(j["note"], "{not json");                       // raw string preserved
    EXPECT_EQ(j["score"], 2.5);
    EXPECT_EQ(j["photo"], "AAH/");                           // base64, matching Swift
    EXPECT_TRUE(j.contains("globalId"));
    EXPECT_EQ(j["globalId"], global_id_of(a));
    EXPECT_TRUE(j.contains("id"));
    EXPECT_EQ(j["id"], lattice_object_get_id(a));
    // Unset link/columns: keys omitted, not null.
    EXPECT_FALSE(j.contains("pet"));

    lattice_object_release(a);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPIToJson, LinkDepthAndStubs) {
    TempPath path("capi_tojson_links");
    GraphSchema gs;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), gs.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    lattice_object_t* author = lattice_db_create_object(db, "Author");
    lattice_object_set_string(author, "name", "Mary");
    ASSERT_NE(lattice_db_add(db, author), nullptr) << lattice_last_error();
    lattice_object_release(author);

    lattice_object_t* pet = lattice_db_create_object(db, "Pet");
    lattice_object_set_string(pet, "name", "Rex");
    ASSERT_NE(lattice_db_add(db, pet), nullptr) << lattice_last_error();
    lattice_object_release(pet);

    lattice_object_set_object(author, "pet", pet);
    const std::string pet_gid = global_id_of(pet);
    const std::string author_gid = global_id_of(author);
    ASSERT_FALSE(pet_gid.empty());
    ASSERT_FALSE(author_gid.empty());

    // Depth 0: the link collapses to a {"globalId"} stub.
    nlohmann::json j0 = parsed_json(author, 0);
    ASSERT_TRUE(j0.contains("pet"));
    EXPECT_EQ(j0["pet"], nlohmann::json({{"globalId", pet_gid}}));

    // Depth 1: the link expands one hop.
    nlohmann::json j1 = parsed_json(author, 1);
    ASSERT_TRUE(j1.contains("pet"));
    EXPECT_EQ(j1["pet"]["name"], "Rex");
    EXPECT_EQ(j1["pet"]["globalId"], pet_gid);

    lattice_object_release(author);
    lattice_object_release(pet);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPIHandleLifetime, RetainedHandlesAndQueryObjectsSurviveOwnerHandleRelease) {
    TempPath path("capi_handle_lifetime");
    PersonSchema schema;
    auto* db = lattice_db_create_with_schemas(path.str().c_str(), &schema.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    lattice_db_retain(db);
    lattice_db_release(db);

    auto* original = add_person(db, "retained", 41);
    ASSERT_NE(original, nullptr);
    lattice_object_retain(original);
    lattice_object_release(original);
    const auto id = lattice_object_get_id(original);
    auto* found = lattice_db_find(db, "Person", id);
    ASSERT_NE(found, nullptr) << lattice_last_error();
    auto* results = lattice_db_query(db, "Person", nullptr, nullptr, 0, 0);
    ASSERT_NE(results, nullptr) << lattice_last_error();
    ASSERT_EQ(lattice_results_count(results), 1u);
    auto* queried = lattice_results_get(results, 0);
    ASSERT_NE(queried, nullptr);
    lattice_results_free(results);
    lattice_object_release(original);
    // Release the database wrapper, without closing its underlying store.
    // Both returned managed objects must independently keep that store alive.
    lattice_db_release(db);

    EXPECT_EQ(lattice_object_get_int(found, "age"), 41);
    lattice_object_set_int(found, "age", 42);
    EXPECT_EQ(lattice_object_get_int(queried, "age"), 42);
    lattice_object_release(found);
    EXPECT_STREQ(lattice_object_get_string(queried, "name"), "retained");
    lattice_object_release(queried);
}

TEST(CAPILinks, UnlinkAfterPresenceCheckReturnsNull) {
    TempPath path("capi_link_unlinked_after_check");
    GraphSchema schema;
    auto* db = lattice_db_create_with_schemas(path.str().c_str(), schema.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    auto* author = lattice_db_create_object(db, "Author");
    auto* pet = lattice_db_create_object(db, "Pet");
    ASSERT_NE(author, nullptr);
    ASSERT_NE(pet, nullptr);
    ASSERT_EQ(lattice_db_add(db, author), author);
    ASSERT_EQ(lattice_db_add(db, pet), pet);
    lattice_object_release(author);
    lattice_object_release(pet);
    auto* absent = lattice_object_get_object(author, "pet");
    EXPECT_EQ(absent, nullptr);
    lattice_object_release(absent);
    lattice_object_set_object(author, "pet", pet);
    ASSERT_TRUE(lattice_object_has_value(author, "pet"));

    auto* other = lattice_db_create_with_schemas(path.str().c_str(), schema.schemas, 2);
    ASSERT_NE(other, nullptr) << lattice_last_error();
    const auto authorID = global_id_of(author);
    auto* otherAuthor = lattice_db_find_by_global_id(other, "Author", authorID.c_str());
    ASSERT_NE(otherAuthor, nullptr);
    lattice_object_set_object(otherAuthor, "pet", nullptr);
    EXPECT_FALSE(lattice_object_has_value(otherAuthor, "pet"));
    auto* missing = lattice_object_get_object(author, "pet");
    EXPECT_EQ(missing, nullptr);
    lattice_object_release(missing);

    lattice_object_release(otherAuthor);
    lattice_db_release(other);
    lattice_object_release(author);
    lattice_object_release(pet);
    lattice_db_release(db);
}

TEST(CAPILinks, UnmanagedStagedChildRemainsReadableWithoutPersisting) {
    TempPath path("capi_link_staged_child");
    GraphSchema schema;
    auto* db = lattice_db_create_with_schemas(path.str().c_str(), schema.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    auto* author = lattice_db_create_object(db, "Author");
    auto* pet = lattice_db_create_object(db, "Pet");
    ASSERT_NE(author, nullptr);
    ASSERT_NE(pet, nullptr);
    auto* absent = lattice_object_get_object(author, "pet");
    EXPECT_EQ(absent, nullptr);
    lattice_object_release(absent);
    lattice_object_set_string(pet, "name", "staged pet");
    lattice_object_set_object(author, "pet", pet);
    auto* linked = lattice_object_get_object(author, "pet");
    ASSERT_NE(linked, nullptr);
    EXPECT_STREQ(lattice_object_get_table_name(linked), "Pet");
    EXPECT_STREQ(lattice_object_get_string(linked, "name"), "staged pet");
    EXPECT_EQ(lattice_db_count(db, "Author", nullptr), 0u);
    EXPECT_EQ(lattice_db_count(db, "Pet", nullptr), 0u);
    lattice_object_release(linked);
    lattice_object_release(author);
    lattice_object_release(pet);
    lattice_db_release(db);
}

TEST(CAPILinks, RemoteTargetDeleteWithoutLinkCascadeReturnsNull) {
    TempPath path("capi_link_remote_target_delete");
    GraphSchema schema;
    auto* db = lattice_db_create_with_schemas(path.str().c_str(), schema.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    auto* author = lattice_db_create_object(db, "Author");
    auto* pet = lattice_db_create_object(db, "Pet");
    ASSERT_NE(author, nullptr);
    ASSERT_NE(pet, nullptr);
    ASSERT_EQ(lattice_db_add(db, author), author);
    ASSERT_EQ(lattice_db_add(db, pet), pet);
    lattice_object_release(author);
    lattice_object_release(pet);
    lattice_object_set_object(author, "pet", pet);

    // Apply only the target deletion, as a remote batch can omit a link
    // cascade. The surviving junction must not produce a phantom object.
    const auto entries = nlohmann::json::array({{
        {"globalId", "remote-pet-delete"},
        {"tableName", "Pet"}, {"operation", "DELETE"},
        {"globalRowId", global_id_of(pet)},
        {"changedFields", nlohmann::json::object()},
        {"changedFieldsNames", nlohmann::json::array()}
    }}).dump();
    char* message = lattice_create_sync_message(entries.c_str());
    ASSERT_NE(message, nullptr) << lattice_last_error();
    char* applied = lattice_db_receive_sync_data(
        db, reinterpret_cast<const uint8_t*>(message), std::strlen(message));
    ASSERT_NE(applied, nullptr) << lattice_last_error();
    EXPECT_EQ(nlohmann::json::parse(applied),
              nlohmann::json::array({"remote-pet-delete"}));
    lattice_string_free(applied);
    lattice_string_free(message);
    ASSERT_EQ(lattice_db_count(db, "Pet", nullptr), 0u);
    ASSERT_TRUE(lattice_object_has_value(author, "pet"));
    auto* missing = lattice_object_get_object(author, "pet");
    EXPECT_EQ(missing, nullptr);
    lattice_object_release(missing);
    lattice_object_release(author);
    lattice_object_release(pet);
    lattice_db_release(db);
}

TEST(CAPIHandleLifetime, LinkedObjectGetterReturnsOneOwnedReference) {
    TempPath path("capi_link_handle_lifetime");
    GraphSchema schema;
    auto* db = lattice_db_create_with_schemas(path.str().c_str(), schema.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    auto* author = lattice_db_create_object(db, "Author");
    auto* pet = lattice_db_create_object(db, "Pet");
    ASSERT_NE(author, nullptr);
    ASSERT_NE(pet, nullptr);
    lattice_object_set_string(pet, "name", "owned link");
    ASSERT_EQ(lattice_db_add(db, author), author);
    ASSERT_EQ(lattice_db_add(db, pet), pet);
    lattice_object_release(author);  // Release add's extra reference.
    lattice_object_release(pet);
    lattice_object_set_object(author, "pet", pet);

    auto* linked = lattice_object_get_object(author, "pet");
    ASSERT_NE(linked, nullptr) << lattice_last_error();
    // A getter-owned reference plus this explicit retain must survive ONE
    // release. A fresh zero-count FRT getter would delete it here instead.
    lattice_object_retain(linked);
    lattice_object_release(linked);
    lattice_object_release(author);
    lattice_object_release(pet);
    lattice_db_release(db);
    EXPECT_STREQ(lattice_object_get_string(linked, "name"), "owned link");
    lattice_object_release(linked);
}

TEST(CAPIHandleLifetime, ListElementsOwnTheirHandleAfterListAndParentRelease) {
    TempPath path("capi_list_handle_lifetime");
    GraphSchema schema;
    auto* db = lattice_db_create_with_schemas(path.str().c_str(), schema.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    auto* author = lattice_db_create_object(db, "Author");
    auto* friend_object = lattice_db_create_object(db, "Author");
    ASSERT_NE(author, nullptr);
    ASSERT_NE(friend_object, nullptr);
    lattice_object_set_string(friend_object, "name", "list member");
    ASSERT_EQ(lattice_db_add(db, author), author);
    ASSERT_EQ(lattice_db_add(db, friend_object), friend_object);
    lattice_object_release(author);
    lattice_object_release(friend_object);
    auto* friends = lattice_object_get_link_list(author, "friends");
    ASSERT_NE(friends, nullptr);
    lattice_link_list_push_back(friends, friend_object);
    ASSERT_EQ(lattice_link_list_size(friends), 1u);
    auto* member = lattice_link_list_get(friends, 0);
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(lattice_link_list_get(friends, 1), nullptr);
    lattice_link_list_release(friends);
    lattice_object_release(author);
    lattice_object_release(friend_object);
    lattice_db_release(db);
    EXPECT_STREQ(lattice_object_get_string(member, "name"), "list member");
    lattice_object_release(member);
}

TEST(CAPIToJson, CycleCollapsesToStubAndTerminates) {
    TempPath path("capi_tojson_cycle");
    GraphSchema gs;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), gs.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    lattice_object_t* author = lattice_db_create_object(db, "Author");
    lattice_object_set_string(author, "name", "Ouro");
    ASSERT_NE(lattice_db_add(db, author), nullptr) << lattice_last_error();
    lattice_object_release(author);
    lattice_object_t* pet = lattice_db_create_object(db, "Pet");
    lattice_object_set_string(pet, "name", "Boros");
    ASSERT_NE(lattice_db_add(db, pet), nullptr) << lattice_last_error();
    lattice_object_release(pet);

    // Author -> Pet -> Author cycle.
    lattice_object_set_object(author, "pet", pet);
    lattice_object_set_object(pet, "owner", author);

    // Generous depth: must terminate, with the back-link collapsing to the
    // visited author's stub (cycle guard), not recursing.
    nlohmann::json j = parsed_json(author, 16);
    ASSERT_TRUE(j.contains("pet"));
    EXPECT_EQ(j["pet"]["name"], "Boros");
    ASSERT_TRUE(j["pet"].contains("owner"));
    EXPECT_EQ(j["pet"]["owner"],
              nlohmann::json({{"globalId", global_id_of(author)}}));

    lattice_object_release(author);
    lattice_object_release(pet);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPIToJson, ListsExpandAtDepthAndSummarizeAtBoundary) {
    TempPath path("capi_tojson_lists");
    GraphSchema gs;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), gs.schemas, 2);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    lattice_object_t* a = lattice_db_create_object(db, "Author");
    lattice_object_set_string(a, "name", "Hub");
    ASSERT_NE(lattice_db_add(db, a), nullptr) << lattice_last_error();
    lattice_object_release(a);

    lattice_link_list_t* friends = lattice_object_get_link_list(a, "friends");
    ASSERT_NE(friends, nullptr) << lattice_last_error();
    for (const char* name : {"F1", "F2"}) {
        lattice_object_t* f = lattice_db_create_object(db, "Author");
        lattice_object_set_string(f, "name", name);
        ASSERT_NE(lattice_db_add(db, f), nullptr) << lattice_last_error();
        lattice_object_release(f);
        lattice_link_list_push_back(friends, f);
        lattice_object_release(f);
    }
    ASSERT_EQ(lattice_link_list_size(friends), 2u);

    // Depth 0: the list is a {"count"} summary.
    nlohmann::json j0 = parsed_json(a, 0);
    ASSERT_TRUE(j0.contains("friends"));
    EXPECT_EQ(j0["friends"], nlohmann::json({{"count", 2}}));

    // Depth 1: elements expand; THEIR lists summarize at the boundary.
    nlohmann::json j1 = parsed_json(a, 1);
    ASSERT_TRUE(j1["friends"].is_array());
    ASSERT_EQ(j1["friends"].size(), 2u);
    std::set<std::string> names;
    for (const auto& el : j1["friends"]) {
        ASSERT_TRUE(el.contains("name"));
        names.insert(el["name"].get<std::string>());
        ASSERT_TRUE(el.contains("friends"));
        EXPECT_EQ(el["friends"], nlohmann::json({{"count", 0}}));
    }
    EXPECT_EQ(names, (std::set<std::string>{"F1", "F2"}));

    lattice_link_list_release(friends);
    lattice_object_release(a);
    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPIToJson, UnmanagedObjectAndErrorContract) {
    GraphSchema gs;
    lattice_object_t* obj = lattice_object_create_with_schema(
        "Author", gs.author_props, gs.schemas[0].property_count);
    ASSERT_NE(obj, nullptr) << lattice_last_error();
    lattice_object_set_string(obj, "name", "Loose");
    lattice_object_set_double(obj, "score", 1.0);

    // Unmanaged objects serialize their staged scalars; no identity keys.
    nlohmann::json j = parsed_json(obj, 1);
    EXPECT_EQ(j["name"], "Loose");
    EXPECT_EQ(j["score"], 1.0);
    EXPECT_FALSE(j.contains("globalId"));
    EXPECT_FALSE(j.contains("id"));
    lattice_object_release(obj);

    EXPECT_EQ(lattice_object_to_json(nullptr, 5), nullptr);
    ASSERT_NE(lattice_last_error(), nullptr);
}

// ============================================================================
// Per-property unique constraints (conformance: cross-SDK divergence fix)
// ============================================================================

// A property flagged `is_unique` over the C ABI must produce the SAME schema
// object the Swift bridge's constraint pass creates for `@Unique` on the same
// field — a unique index named `unique_<table>_<ordinal>`, ordinals assigned
// in property declaration order. Identical names are load-bearing: a database
// created by Swift and reopened through the C ABI (or vice versa) must see
// the index already present instead of growing a second one.
TEST(CAPIUniqueConstraint, IsUniqueCreatesBridgeNamedIndexAndEnforces) {
    TempPath path("capi_unique");

    lattice_property_t props[3];
    std::memset(props, 0, sizeof(props));
    props[0].name = "email";
    props[0].type = LATTICE_TYPE_TEXT;
    props[0].kind = LATTICE_KIND_PRIMITIVE;
    props[0].nullable = true;
    props[0].is_unique = true;
    props[1].name = "age";
    props[1].type = LATTICE_TYPE_INTEGER;
    props[1].kind = LATTICE_KIND_PRIMITIVE;
    props[1].nullable = true;
    props[2].name = "code";
    props[2].type = LATTICE_TYPE_TEXT;
    props[2].kind = LATTICE_KIND_PRIMITIVE;
    props[2].nullable = true;
    props[2].is_unique = true;

    lattice_schema_t schema;
    std::memset(&schema, 0, sizeof(schema));
    schema.table_name = "UniquePerson";
    schema.properties = props;
    schema.property_count = 3;

    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    // Index names: ordinal 0 = email (first unique property), 1 = code.
    EXPECT_EQ(lattice_db_count(db, "sqlite_master",
                  "type = 'index' AND tbl_name = 'UniquePerson' AND "
                  "name = 'unique_UniquePerson_0'"),
              1u);
    EXPECT_EQ(lattice_db_count(db, "sqlite_master",
                  "type = 'index' AND tbl_name = 'UniquePerson' AND "
                  "name = 'unique_UniquePerson_1'"),
              1u);
    // ... and nothing else — exactly the two.
    EXPECT_EQ(lattice_db_count(db, "sqlite_master",
                  "type = 'index' AND tbl_name = 'UniquePerson' AND "
                  "name LIKE 'unique_%'"),
              2u);

    // Enforcement: duplicate email is rejected.
    lattice_object_t* first = lattice_db_create_object(db, "UniquePerson");
    ASSERT_NE(first, nullptr) << lattice_last_error();
    lattice_object_set_string(first, "email", "a@example.com");
    lattice_object_set_string(first, "code", "c-1");
    lattice_object_t* first_managed = lattice_db_add(db, first);
    ASSERT_NE(first_managed, nullptr) << lattice_last_error();
    lattice_object_release(first_managed);

    lattice_object_t* dup = lattice_db_create_object(db, "UniquePerson");
    ASSERT_NE(dup, nullptr) << lattice_last_error();
    lattice_object_set_string(dup, "email", "a@example.com");
    lattice_object_set_string(dup, "code", "c-2");
    lattice_object_t* dup_managed = lattice_db_add(db, dup);
    EXPECT_EQ(dup_managed, nullptr) << "duplicate unique value must be rejected";
    ASSERT_NE(lattice_last_error(), nullptr);
    EXPECT_EQ(lattice_db_count(db, "UniquePerson", nullptr), 1u);
    lattice_object_release(dup);

    // Distinct values still insert.
    lattice_object_t* other = lattice_db_create_object(db, "UniquePerson");
    ASSERT_NE(other, nullptr) << lattice_last_error();
    lattice_object_set_string(other, "email", "b@example.com");
    lattice_object_set_string(other, "code", "c-2");
    lattice_object_t* other_managed = lattice_db_add(db, other);
    ASSERT_NE(other_managed, nullptr) << lattice_last_error();
    lattice_object_release(other_managed);
    EXPECT_EQ(lattice_db_count(db, "UniquePerson", nullptr), 2u);

    lattice_object_release(first);
    lattice_object_release(other);
    lattice_db_close(db);
    lattice_db_release(db);

    // Reopen: the pass is idempotent — still exactly two unique indexes.
    lattice_db_t* db2 = lattice_db_create_with_schemas(path.str().c_str(), &schema, 1);
    ASSERT_NE(db2, nullptr) << lattice_last_error();
    EXPECT_EQ(lattice_db_count(db2, "sqlite_master",
                  "type = 'index' AND tbl_name = 'UniquePerson' AND "
                  "name LIKE 'unique_%'"),
              2u);
    lattice_db_close(db2);
    lattice_db_release(db2);
}

// ============================================================================
// Transaction read visibility (conformance: cross-SDK divergence fix)
// ============================================================================

// Under WAL the dedicated read-only connection cannot see another
// connection's open transaction. lattice_db_count/lattice_db_query issued
// between lattice_db_begin_transaction and commit/rollback must therefore
// route through the write connection — uncommitted writes are visible inside
// the transaction and gone after rollback.
TEST(CAPITransactionVisibility, ReadsInsideOpenTransactionSeeUncommittedWrites) {
    TempPath path("capi_txn_vis");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    // Committed baseline.
    lattice_object_t* base = add_person(db, "committed", 1);
    ASSERT_NE(base, nullptr);
    lattice_object_release(base);
    ASSERT_EQ(lattice_db_count(db, "Person", nullptr), 1u);

    ASSERT_EQ(lattice_db_begin_transaction(db), LATTICE_OK) << lattice_last_error();

    lattice_object_t* staged = lattice_db_create_object(db, "Person");
    ASSERT_NE(staged, nullptr) << lattice_last_error();
    lattice_object_set_string(staged, "name", "uncommitted");
    lattice_object_set_int(staged, "age", 99);
    lattice_object_t* managed = lattice_db_add(db, staged);
    ASSERT_NE(managed, nullptr) << lattice_last_error();
    lattice_object_release(managed);

    // Visible to count and query on the same handle while the txn is open.
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 2u)
        << "count missed uncommitted writes inside an open transaction";
    EXPECT_EQ(lattice_db_count(db, "Person", "age = 99"), 1u);

    lattice_results_t* res = lattice_db_query(db, "Person", "age = 99", nullptr, 0, 0);
    ASSERT_NE(res, nullptr) << lattice_last_error();
    ASSERT_EQ(lattice_results_count(res), 1u)
        << "query missed uncommitted writes inside an open transaction";
    lattice_object_t* row = lattice_results_get(res, 0);
    ASSERT_NE(row, nullptr);
    EXPECT_STREQ(lattice_object_get_string(row, "name"), "uncommitted");
    lattice_object_release(row);
    lattice_results_free(res);

    ASSERT_EQ(lattice_db_rollback(db), LATTICE_OK) << lattice_last_error();

    // After rollback the staged row is gone from both read routes.
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 1u);
    EXPECT_EQ(lattice_db_count(db, "Person", "age = 99"), 0u);
    lattice_results_t* res2 = lattice_db_query(db, "Person", "age = 99", nullptr, 0, 0);
    ASSERT_NE(res2, nullptr) << lattice_last_error();
    EXPECT_EQ(lattice_results_count(res2), 0u);
    lattice_results_free(res2);

    lattice_object_release(staged);
    lattice_db_close(db);
    lattice_db_release(db);
}

// The in-transaction read routing is THREAD-SCOPED: only the thread that
// opened the transaction reads through the write connection. Every other
// thread keeps the dedicated reader and must see ONLY committed state —
// leaking uncommitted rows to concurrent readers would break reader
// isolation (caught by the Swift suite against 1.0.0, fixed in 1.0.1).
TEST(CAPITransactionVisibility, OtherThreadsNeverSeeTheOpenTransaction) {
    TempPath path("capi_txn_thread_scope");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    lattice_object_t* base = add_person(db, "committed", 1);
    ASSERT_NE(base, nullptr);
    lattice_object_release(base);

    ASSERT_EQ(lattice_db_begin_transaction(db), LATTICE_OK) << lattice_last_error();
    lattice_object_t* staged = lattice_db_create_object(db, "Person");
    ASSERT_NE(staged, nullptr);
    lattice_object_set_string(staged, "name", "uncommitted");
    lattice_object_set_int(staged, "age", 99);
    lattice_object_t* managed = lattice_db_add(db, staged);
    ASSERT_NE(managed, nullptr) << lattice_last_error();
    lattice_object_release(managed);

    // Owning thread: read-your-writes.
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 2u);

    // Foreign thread: committed state only.
    uint64_t other_count = 0, other_filtered = ~0ull;
    std::thread([&] {
        other_count = lattice_db_count(db, "Person", nullptr);
        other_filtered = lattice_db_count(db, "Person", "age = 99");
    }).join();
    EXPECT_EQ(other_count, 1u)
        << "a foreign thread observed another thread's open transaction";
    EXPECT_EQ(other_filtered, 0u);

    ASSERT_EQ(lattice_db_rollback(db), LATTICE_OK) << lattice_last_error();
    lattice_object_release(staged);
    lattice_db_close(db);
    lattice_db_release(db);
}

// Commit is the positive control: the same routing must not break the
// ordinary committed-visibility contract.
TEST(CAPITransactionVisibility, CommitKeepsTransactionWrites) {
    TempPath path("capi_txn_commit");
    PersonSchema ps;
    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &ps.schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    ASSERT_EQ(lattice_db_begin_transaction(db), LATTICE_OK) << lattice_last_error();
    lattice_object_t* p = add_person(db, "kept", 7);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 1u);
    ASSERT_EQ(lattice_db_commit(db), LATTICE_OK) << lattice_last_error();

    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 1u);
    EXPECT_EQ(lattice_db_count(db, "Person", "age = 7"), 1u);
    lattice_object_release(p);
    lattice_db_close(db);
    lattice_db_release(db);
}

// ============================================================================
// Geo-bounds on the dynamic add path (conformance: cross-SDK divergence fix)
// ============================================================================

// An `is_geo_bounds` property is stored as four expanded REAL columns
// (`<name>_minLat/_maxLat/_minLon/_maxLon`). Objects created through
// lattice_object_create_with_schema used to lose the flag, so the add path
// built its INSERT against the raw property name and SQLite rejected it with
// "table has no column named <name>". The add must succeed and the four
// columns must round-trip.
TEST(CAPIGeoBounds, DynamicAddExpandsGeoBoundsColumns) {
    TempPath path("capi_geo_add");

    lattice_property_t props[2];
    std::memset(props, 0, sizeof(props));
    props[0].name = "name";
    props[0].type = LATTICE_TYPE_TEXT;
    props[0].kind = LATTICE_KIND_PRIMITIVE;
    props[0].nullable = true;
    props[1].name = "bounds";
    props[1].type = LATTICE_TYPE_REAL;
    props[1].kind = LATTICE_KIND_PRIMITIVE;
    props[1].nullable = false;
    props[1].is_geo_bounds = true;

    lattice_schema_t schema;
    std::memset(&schema, 0, sizeof(schema));
    schema.table_name = "GeoPlace";
    schema.properties = props;
    schema.property_count = 2;

    lattice_db_t* db = lattice_db_create_with_schemas(path.str().c_str(), &schema, 1);
    ASSERT_NE(db, nullptr) << lattice_last_error();

    // The table has the four expanded columns and NOT the raw property.
    EXPECT_EQ(lattice_db_count(db, "pragma_table_info('GeoPlace')",
                               "name = 'bounds_minLat'"), 1u);
    EXPECT_EQ(lattice_db_count(db, "pragma_table_info('GeoPlace')",
                               "name = 'bounds_maxLat'"), 1u);
    EXPECT_EQ(lattice_db_count(db, "pragma_table_info('GeoPlace')",
                               "name = 'bounds_minLon'"), 1u);
    EXPECT_EQ(lattice_db_count(db, "pragma_table_info('GeoPlace')",
                               "name = 'bounds_maxLon'"), 1u);
    EXPECT_EQ(lattice_db_count(db, "pragma_table_info('GeoPlace')",
                               "name = 'bounds'"), 0u);

    // Standalone with-schema creation — the path bindings use and the one
    // that used to drop the geo flag.
    lattice_object_t* obj = lattice_object_create_with_schema("GeoPlace", props, 2);
    ASSERT_NE(obj, nullptr) << lattice_last_error();
    lattice_object_set_string(obj, "name", "sf");
    lattice_object_set_double(obj, "bounds_minLat", 37.70);
    lattice_object_set_double(obj, "bounds_maxLat", 37.83);
    lattice_object_set_double(obj, "bounds_minLon", -122.52);
    lattice_object_set_double(obj, "bounds_maxLon", -122.35);

    lattice_object_t* managed = lattice_db_add(db, obj);
    ASSERT_NE(managed, nullptr)
        << "dynamic add with geo bounds failed: " << lattice_last_error();
    lattice_object_release(managed);

    // Round-trip the four columns through a fresh query.
    lattice_results_t* res = lattice_db_query(db, "GeoPlace", nullptr, nullptr, 0, 0);
    ASSERT_NE(res, nullptr) << lattice_last_error();
    ASSERT_EQ(lattice_results_count(res), 1u);
    lattice_object_t* row = lattice_results_get(res, 0);
    ASSERT_NE(row, nullptr);
    EXPECT_STREQ(lattice_object_get_string(row, "name"), "sf");
    EXPECT_DOUBLE_EQ(lattice_object_get_double(row, "bounds_minLat"), 37.70);
    EXPECT_DOUBLE_EQ(lattice_object_get_double(row, "bounds_maxLat"), 37.83);
    EXPECT_DOUBLE_EQ(lattice_object_get_double(row, "bounds_minLon"), -122.52);
    EXPECT_DOUBLE_EQ(lattice_object_get_double(row, "bounds_maxLon"), -122.35);
    lattice_object_release(row);
    lattice_results_free(res);

    lattice_object_release(obj);
    lattice_db_close(db);
    lattice_db_release(db);
}

// ============================================================================
// CAPI 1.1.0: Schema v2 (@Union, @NoHistory, Compound @Unique) & Sync Options
// ============================================================================

TEST(CAPIFeatures, V110FeatureStringsAreTrue) {
    for (const char* f : {"certificate_pin", "no_history", "schema_v2", "union"}) {
        EXPECT_TRUE(lattice_capi_has_feature(f)) << f;
    }
    EXPECT_GE(lattice_capi_version(), 1001000u);
    EXPECT_FALSE(lattice_capi_has_feature("unified_open"));
}

TEST(CAPISchemaV2, UnionRoundTripAndSyncReceive) {
    TempPath writer_path("capi_union_writer");
    TempPath reader_path("capi_union_reader");

    // 7-case RoutingSignal union matching OrbitalStore v31 (including unlabeled single-value verdict)
    lattice_union_case_value_t handoff_vals[2];
    lattice_union_case_value_init(&handoff_vals[0]);
    handoff_vals[0].param_name = "target";
    handoff_vals[0].type = LATTICE_TYPE_TEXT;
    lattice_union_case_value_init(&handoff_vals[1]);
    handoff_vals[1].param_name = "note";
    handoff_vals[1].type = LATTICE_TYPE_TEXT;

    lattice_union_case_value_t verdict_val;
    lattice_union_case_value_init(&verdict_val);
    verdict_val.param_name = ""; // unlabeled single value -> column "verdict", key "_0"
    verdict_val.type = LATTICE_TYPE_TEXT;

    lattice_union_case_t cases[3];
    lattice_union_case_init(&cases[0]);
    cases[0].case_name = "none";
    lattice_union_case_init(&cases[1]);
    cases[1].case_name = "handoff";
    cases[1].values = handoff_vals;
    cases[1].value_count = 2;
    lattice_union_case_init(&cases[2]);
    cases[2].case_name = "verdict";
    cases[2].values = &verdict_val;
    cases[2].value_count = 1;

    lattice_union_descriptor_t routing_desc;
    lattice_union_descriptor_init(&routing_desc);
    routing_desc.union_table_name = "_RoutingSignal";
    routing_desc.cases = cases;
    routing_desc.case_count = 3;

    lattice_property_ext_t action_props[2];
    lattice_property_ext_init(&action_props[0]);
    action_props[0].base.name = "tool";
    action_props[0].base.type = LATTICE_TYPE_TEXT;
    action_props[0].base.kind = LATTICE_KIND_PRIMITIVE;
    lattice_property_ext_init(&action_props[1]);
    action_props[1].base.name = "signal";
    action_props[1].base.type = LATTICE_TYPE_TEXT;
    action_props[1].base.kind = LATTICE_KIND_UNION;
    action_props[1].base.target_table = "_RoutingSignal";
    action_props[1].union_desc = &routing_desc;

    lattice_schema_ext_t action_schema;
    lattice_schema_ext_init(&action_schema);
    action_schema.table_name = "AgentAction";
    action_schema.properties = action_props;
    action_schema.property_count = 2;

    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    opts.target_schema_version = 31;
    opts.busy_timeout_ms = 5000;

    lattice_db_t* writer = lattice_db_open_v2(
        writer_path.str().c_str(), &action_schema, 1, sizeof(action_schema),
        nullptr, nullptr, nullptr, &opts, nullptr, 0, nullptr, nullptr);
    ASSERT_NE(writer, nullptr) << lattice_last_error();

    // Verify unlabeled single-value column is named "verdict" (not "verdict___0")
    EXPECT_EQ(lattice_db_count(writer, "pragma_table_info('_RoutingSignal')", "name = 'verdict'"), 1u);
    EXPECT_EQ(lattice_db_count(writer, "pragma_table_info('_RoutingSignal')", "name = 'handoff__target'"), 1u);

    // Create unmanaged AgentAction with v2 schema, set union value, add to writer
    lattice_object_t* obj = lattice_object_create_with_schema_v2(
        "AgentAction", action_props, 2, sizeof(lattice_property_ext_t));
    ASSERT_NE(obj, nullptr) << lattice_last_error();
    lattice_object_set_string(obj, "tool", "route");

    lattice_union_value_t* uv = lattice_union_value_create("verdict");
    ASSERT_NE(uv, nullptr);
    lattice_union_value_set_string(uv, "_0", "{\"pass\":true,\"notes\":\"ok\"}");
    ASSERT_EQ(lattice_object_set_union(obj, "signal", uv), LATTICE_OK);
    lattice_union_value_release(uv);

    lattice_object_t* managed = lattice_db_add(writer, obj);
    ASSERT_NE(managed, nullptr) << lattice_last_error();
    lattice_object_release(managed);
    lattice_object_release(obj);

    // Export writer audit events as sync message and apply into reader_db (must NOT fail on _RoutingSignal!)
    char* events = lattice_db_events_after(writer, nullptr);
    ASSERT_NE(events, nullptr);
    char* sync_msg = lattice_create_sync_message(events);
    ASSERT_NE(sync_msg, nullptr);

    opts.uploads_enabled = 0; // download-only replica
    lattice_db_t* reader = lattice_db_open_v2(
        reader_path.str().c_str(), &action_schema, 1, sizeof(action_schema),
        nullptr, nullptr, nullptr, &opts, nullptr, 0, nullptr, nullptr);
    ASSERT_NE(reader, nullptr) << lattice_last_error();

    char* applied = lattice_db_receive_sync_data(
        reader, reinterpret_cast<const uint8_t*>(sync_msg), std::strlen(sync_msg));
    ASSERT_NE(applied, nullptr) << "union sync receive failed: " << lattice_last_error();
    lattice_string_free(applied);
    lattice_string_free(sync_msg);
    lattice_string_free(events);

    lattice_results_t* res = lattice_db_query(reader, "AgentAction", nullptr, nullptr, 0, 0);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(lattice_results_count(res), 1u);
    lattice_object_t* row = lattice_results_get(res, 0);
    lattice_union_value_t* r_uv = lattice_object_get_union(row, "signal");
    ASSERT_NE(r_uv, nullptr);
    EXPECT_STREQ(lattice_union_value_get_case(r_uv), "verdict");
    EXPECT_STREQ(lattice_union_value_get_string(r_uv, "_0"), "{\"pass\":true,\"notes\":\"ok\"}");
    lattice_union_value_release(r_uv);

    // Mutate to bare case "none" and clear
    lattice_union_value_t* none_uv = lattice_union_value_create("none");
    ASSERT_EQ(lattice_object_set_union(row, "signal", none_uv), LATTICE_OK);
    lattice_union_value_release(none_uv);
    r_uv = lattice_object_get_union(row, "signal");
    ASSERT_NE(r_uv, nullptr);
    EXPECT_STREQ(lattice_union_value_get_case(r_uv), "none");
    lattice_union_value_release(r_uv);

    ASSERT_EQ(lattice_object_clear_union(row, "signal"), LATTICE_OK);
    EXPECT_EQ(lattice_object_get_union(row, "signal"), nullptr);

    lattice_object_release(row);
    lattice_results_free(res);
    lattice_db_close(reader);
    lattice_db_release(reader);
    lattice_db_close(writer);
    lattice_db_release(writer);
}

TEST(CAPISchemaV2, NoHistoryStreamingAndSyncNullGuard) {
    TempPath writer_path("capi_nohist_writer");
    TempPath reader_path("capi_nohist_reader");

    lattice_property_ext_t msg_props[2];
    lattice_property_ext_init(&msg_props[0]);
    msg_props[0].base.name = "status";
    msg_props[0].base.type = LATTICE_TYPE_TEXT;
    msg_props[0].base.kind = LATTICE_KIND_PRIMITIVE;
    lattice_property_ext_init(&msg_props[1]);
    msg_props[1].base.name = "text";
    msg_props[1].base.type = LATTICE_TYPE_TEXT;
    msg_props[1].base.kind = LATTICE_KIND_PRIMITIVE;
    msg_props[1].base.nullable = false;
    msg_props[1].no_history = true;

    lattice_schema_ext_t msg_schema;
    lattice_schema_ext_init(&msg_schema);
    msg_schema.table_name = "ChatMessage";
    msg_schema.properties = msg_props;
    msg_schema.property_count = 2;

    lattice_db_t* writer = lattice_db_open_v2(
        writer_path.str().c_str(), &msg_schema, 1, sizeof(msg_schema),
        nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr, nullptr);
    ASSERT_NE(writer, nullptr) << lattice_last_error();

    // Verify _lattice_meta records trigger_flags:ChatMessage = 'text'
    EXPECT_EQ(lattice_db_count(
        writer, "_lattice_meta",
        "key = 'trigger_flags:ChatMessage' AND value = 'text'"), 1u);

    lattice_object_t* msg = lattice_object_create_with_schema_v2(
        "ChatMessage", msg_props, 2, sizeof(lattice_property_ext_t));
    lattice_object_set_string(msg, "status", "streaming");
    lattice_object_set_string(msg, "text", "tok1");
    lattice_object_t* managed = lattice_db_add(writer, msg);
    ASSERT_NE(managed, nullptr);
    lattice_object_set_string(managed, "text", "tok1 tok2 tok3");
    lattice_object_release(managed);
    lattice_object_release(msg);

    // Late-bound events_after must carry the final streamed text to reader cleanly
    char* events = lattice_db_events_after(writer, nullptr);
    ASSERT_NE(events, nullptr);
    char* sync_msg = lattice_create_sync_message(events);
    ASSERT_NE(sync_msg, nullptr);

    lattice_db_t* reader = lattice_db_open_v2(
        reader_path.str().c_str(), &msg_schema, 1, sizeof(msg_schema),
        nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr, nullptr);
    ASSERT_NE(reader, nullptr);

    char* applied = lattice_db_receive_sync_data(
        reader, reinterpret_cast<const uint8_t*>(sync_msg), std::strlen(sync_msg));
    ASSERT_NE(applied, nullptr) << lattice_last_error();
    lattice_string_free(applied);
    lattice_string_free(sync_msg);
    lattice_string_free(events);

    lattice_results_t* res = lattice_db_query(reader, "ChatMessage", nullptr, nullptr, 0, 0);
    ASSERT_EQ(lattice_results_count(res), 1u);
    lattice_object_t* r_msg = lattice_results_get(res, 0);
    EXPECT_STREQ(lattice_object_get_string(r_msg, "text"), "tok1 tok2 tok3");
    lattice_object_release(r_msg);
    lattice_results_free(res);

    lattice_db_close(reader);
    lattice_db_release(reader);
    lattice_db_close(writer);
    lattice_db_release(writer);
}

// ============================================================================
// CAPI 1.2.0 (3.1.3): Managed Background Sync & Finite `sync_once`
// ============================================================================

namespace {

struct MockCapiWsRelay {
    struct ClientSlot {
        MockCapiWsRelay* relay = nullptr;
        lattice_websocket_client_t* client = nullptr;
        std::atomic<lattice_websocket_state_t> state{LATTICE_WS_CLOSED};
        std::atomic<bool> destroyed{false};
    };

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<std::function<void()>> tasks;
    bool stopping = false;
    std::thread dispatcher;

    bool send_catch_up_complete = true;
    bool auto_ack_uploads = true;
    std::vector<std::string> replay_frames;
    std::vector<std::string> uploaded_frames;
    std::vector<std::shared_ptr<ClientSlot>> slots;
    lattice_network_factory_t* factory = nullptr;

    MockCapiWsRelay() {
        dispatcher = std::thread([this]() {
            while (true) {
                std::function<void()> fn;
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    cv.wait(lock, [this] { return stopping || !tasks.empty(); });
                    if (stopping && tasks.empty()) return;
                    fn = std::move(tasks.front());
                    tasks.pop_front();
                }
                fn();
            }
        });
        factory = lattice_network_factory_create(this, &create_ws_cb, nullptr);
        lattice_set_network_factory(factory);
    }

    ~MockCapiWsRelay() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopping = true;
        }
        cv.notify_all();
        if (dispatcher.joinable()) dispatcher.join();
        lattice_set_network_factory(nullptr);
        if (factory) lattice_network_factory_release(factory);
    }

    void post(std::function<void()> fn) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (stopping) return;
            tasks.push_back(std::move(fn));
        }
        cv.notify_one();
    }

    static lattice_websocket_client_t* create_ws_cb(void* user_data) {
        auto* self = static_cast<MockCapiWsRelay*>(user_data);
        auto slot = std::make_shared<ClientSlot>();
        slot->relay = self;
        {
            std::lock_guard<std::mutex> lock(self->mutex);
            self->slots.push_back(slot);
        }
        auto* client = lattice_websocket_client_create(
            slot.get(),
            &ws_connect_cb,
            &ws_disconnect_cb,
            &ws_state_cb,
            &ws_send_cb);
        slot->client = client;
        lattice_websocket_client_set_on_destroy(client, slot.get(), &ws_destroy_cb);
        return client;
    }

    static void ws_destroy_cb(void* user_data) {
        auto* slot = static_cast<ClientSlot*>(user_data);
        slot->destroyed.store(true, std::memory_order_release);
        slot->state.store(LATTICE_WS_CLOSED, std::memory_order_release);
    }

    static void ws_connect_cb(void* user_data, const char* /*url*/, const char* /*headers_json*/) {
        auto* slot = static_cast<ClientSlot*>(user_data);
        slot->state.store(LATTICE_WS_CONNECTING, std::memory_order_release);
        std::vector<std::string> frames;
        bool emit_marker = true;
        {
            std::lock_guard<std::mutex> lock(slot->relay->mutex);
            frames = slot->relay->replay_frames;
            emit_marker = slot->relay->send_catch_up_complete;
        }
        slot->relay->post([slot, frames = std::move(frames), emit_marker]() {
            if (slot->destroyed.load(std::memory_order_acquire)) return;
            slot->state.store(LATTICE_WS_OPEN, std::memory_order_release);
            lattice_websocket_client_trigger_on_open(slot->client);
            for (const auto& frame : frames) {
                if (slot->destroyed.load(std::memory_order_acquire)) return;
                lattice_websocket_client_trigger_on_message(
                    slot->client,
                    LATTICE_WS_MSG_TEXT,
                    reinterpret_cast<const uint8_t*>(frame.data()),
                    frame.size());
            }
            if (emit_marker && !slot->destroyed.load(std::memory_order_acquire)) {
                const std::string marker = R"({"catchUpComplete":true})";
                lattice_websocket_client_trigger_on_message(
                    slot->client,
                    LATTICE_WS_MSG_TEXT,
                    reinterpret_cast<const uint8_t*>(marker.data()),
                    marker.size());
            }
        });
    }

    static void ws_disconnect_cb(void* user_data) {
        auto* slot = static_cast<ClientSlot*>(user_data);
        slot->state.store(LATTICE_WS_CLOSED, std::memory_order_release);
    }

    static lattice_websocket_state_t ws_state_cb(void* user_data) {
        auto* slot = static_cast<ClientSlot*>(user_data);
        return slot->state.load(std::memory_order_acquire);
    }

    static void ws_send_cb(void* user_data, lattice_websocket_msg_type_t /*type*/,
                           const uint8_t* data, size_t data_size) {
        auto* slot = static_cast<ClientSlot*>(user_data);
        std::string payload(reinterpret_cast<const char*>(data), data_size);
        bool ack_uploads = true;
        {
            std::lock_guard<std::mutex> lock(slot->relay->mutex);
            slot->relay->uploaded_frames.push_back(payload);
            ack_uploads = slot->relay->auto_ack_uploads;
        }
        if (!ack_uploads) return;
        try {
            auto parsed = nlohmann::json::parse(payload);
            if (parsed.contains("auditLog") && parsed["auditLog"].is_array()) {
                nlohmann::json ids = nlohmann::json::array();
                for (const auto& item : parsed["auditLog"]) {
                    if (item.contains("globalId") && item["globalId"].is_string()) {
                        ids.push_back(item["globalId"]);
                    }
                }
                if (!ids.empty()) {
                    const std::string ids_str = ids.dump();
                    char* ack_raw = lattice_create_ack_message(ids_str.c_str());
                    if (ack_raw) {
                        std::string ack_frame(ack_raw);
                        lattice_string_free(ack_raw);
                        slot->relay->post([slot, ack_frame = std::move(ack_frame)]() {
                            if (slot->destroyed.load(std::memory_order_acquire)) return;
                            lattice_websocket_client_trigger_on_message(
                                slot->client,
                                LATTICE_WS_MSG_TEXT,
                                reinterpret_cast<const uint8_t*>(ack_frame.data()),
                                ack_frame.size());
                        });
                    }
                }
            }
        } catch (...) {}
    }
};

}  // namespace

TEST(CAPIBackgroundSync, V120FeatureStringAndVersion) {
    EXPECT_TRUE(lattice_capi_has_feature("background_sync"));
    EXPECT_GE(lattice_capi_version(), 1002000u);
    EXPECT_STREQ(lattice_capi_version_string(), "1.2.0");
}

TEST(CAPIBackgroundSync, ManagedFiniteSyncOnceRoundTripAndSettlement) {
    MockCapiWsRelay relay;
    PersonSchema schema;

    // Build an initial server catch-up payload from a seed database.
    TempPath seed_path("capi_bgsync_seed");
    lattice_db_t* seed = lattice_db_create_with_schemas(seed_path.str().c_str(), &schema.schema, 1);
    ASSERT_NE(seed, nullptr);
    lattice_object_t* remote_person = lattice_db_create_object(seed, "Person");
    lattice_object_set_string(remote_person, "name", "RemotePeerRow");
    lattice_object_set_int(remote_person, "age", 42);
    lattice_object_t* managed_remote = lattice_db_add(seed, remote_person);
    ASSERT_NE(managed_remote, nullptr);
    lattice_object_release(managed_remote);
    lattice_object_release(remote_person);

    char* seed_events = lattice_db_events_after(seed, nullptr);
    ASSERT_NE(seed_events, nullptr);
    char* replay_msg = lattice_create_sync_message(seed_events);
    ASSERT_NE(replay_msg, nullptr);
    relay.replay_frames.emplace_back(replay_msg);
    lattice_string_free(replay_msg);
    lattice_string_free(seed_events);
    lattice_db_close(seed);
    lattice_db_release(seed);

    // Open a managed background sync database; verify it starts suspended.
    TempPath client_path("capi_bgsync_client");
    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    opts.background_sync_managed = 1;

    lattice_db_t* db = lattice_db_create_with_sync_options(
        client_path.str().c_str(), &schema.schema, 1, nullptr,
        "ws://relay.invalid/sync", "test-token", &opts);
    ASSERT_NE(db, nullptr) << lattice_last_error();
    EXPECT_TRUE(lattice_db_is_background_sync_managed(db));
    EXPECT_EQ(lattice_db_current_background_sync_generation(db), 0u);
    EXPECT_FALSE(lattice_db_is_sync_connected(db));

    // Stage a local write while suspended.
    lattice_object_t* local_person = lattice_db_create_object(db, "Person");
    lattice_object_set_string(local_person, "name", "LocalOfflineRow");
    lattice_object_set_int(local_person, "age", 19);
    lattice_object_t* managed_local = lattice_db_add(db, local_person);
    ASSERT_NE(managed_local, nullptr);
    lattice_object_release(managed_local);
    lattice_object_release(local_person);

    // Run finite sync_once: uploads LocalOfflineRow, replays RemotePeerRow, settles to generation 0.
    lattice_sync_once_result_t result = LATTICE_SYNC_ONCE_UNAVAILABLE;
    ASSERT_EQ(lattice_db_sync_once(db, 5.0, nullptr, &result), LATTICE_OK) << lattice_last_error();
    EXPECT_EQ(result, LATTICE_SYNC_ONCE_COMPLETED);
    EXPECT_EQ(lattice_db_current_background_sync_generation(db), 0u);
    EXPECT_FALSE(lattice_db_is_sync_connected(db));

    // Both rows exist locally and pending upload queue is drained.
    EXPECT_EQ(lattice_db_count(db, "Person", nullptr), 2u);
    EXPECT_EQ(lattice_db_count(db, "Person", "name = 'RemotePeerRow' AND age = 42"), 1u);
    EXPECT_EQ(lattice_db_count(db, "Person", "name = 'LocalOfflineRow' AND age = 19"), 1u);

    char* pending = lattice_db_get_pending_audit_log(db);
    ASSERT_NE(pending, nullptr);
    EXPECT_EQ(nlohmann::json::parse(pending).size(), 0u);
    lattice_string_free(pending);

    lattice_db_close(db);
    lattice_db_release(db);
}

TEST(CAPIBackgroundSync, ResumeSuspendAlreadyRunningAndUnavailableContracts) {
    MockCapiWsRelay relay;
    PersonSchema schema;
    TempPath client_path("capi_bgsync_lifecycle");

    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    opts.background_sync_managed = 1;

    lattice_db_t* db = lattice_db_create_with_sync_options(
        client_path.str().c_str(), &schema.schema, 1, nullptr,
        "ws://relay.invalid/lifecycle", "test-token", &opts);
    ASSERT_NE(db, nullptr);

    // Foreground resume acquires a continuous generation; sync_once returns ALREADY_RUNNING without disturbing it.
    ASSERT_EQ(lattice_db_resume_background_sync(db), LATTICE_OK);
    const uint64_t fg_gen = lattice_db_current_background_sync_generation(db);
    EXPECT_GT(fg_gen, 0u);

    lattice_sync_once_result_t result = LATTICE_SYNC_ONCE_UNAVAILABLE;
    ASSERT_EQ(lattice_sync_once(db, 5.0, &result), LATTICE_OK);
    EXPECT_EQ(result, LATTICE_SYNC_ONCE_ALREADY_RUNNING);
    EXPECT_EQ(lattice_db_current_background_sync_generation(db), fg_gen);

    // Suspend settles generation back to 0, permitting finite sync_once again.
    ASSERT_EQ(lattice_db_suspend_background_sync(db), LATTICE_OK);
    EXPECT_EQ(lattice_db_current_background_sync_generation(db), 0u);

    ASSERT_EQ(lattice_sync_once(db, 5.0, &result), LATTICE_OK);
    EXPECT_EQ(result, LATTICE_SYNC_ONCE_COMPLETED);
    EXPECT_EQ(lattice_db_current_background_sync_generation(db), 0u);

    // Invalid durations are rejected with INVALID_ARGUMENT.
    EXPECT_EQ(lattice_sync_once(db, 0.0, &result), LATTICE_ERROR_INVALID_ARGUMENT);
    EXPECT_EQ(lattice_sync_once(db, -2.0, &result), LATTICE_ERROR_INVALID_ARGUMENT);

    // Closed database returns UNAVAILABLE.
    lattice_db_close(db);
    ASSERT_EQ(lattice_sync_once(db, 5.0, &result), LATTICE_OK);
    EXPECT_EQ(result, LATTICE_SYNC_ONCE_UNAVAILABLE);
    lattice_db_release(db);

    // Unsupported in-memory database returns INVALID_ARGUMENT + error message.
    lattice_db_t* mem = lattice_db_create_in_memory();
    ASSERT_NE(mem, nullptr);
    EXPECT_EQ(lattice_sync_once(mem, 5.0, &result), LATTICE_ERROR_INVALID_ARGUMENT);
    EXPECT_EQ(result, LATTICE_SYNC_ONCE_UNAVAILABLE);
    EXPECT_NE(lattice_last_error(), nullptr);
    lattice_db_close(mem);
    lattice_db_release(mem);
}

TEST(CAPIBackgroundSync, BudgetExhaustedAndCancellationSettleCleanly) {
    MockCapiWsRelay relay;
    relay.send_catch_up_complete = false;  // Keep finite cycle waiting for catch-up completion
    PersonSchema schema;
    TempPath client_path("capi_bgsync_cancel");

    lattice_sync_options_t opts;
    lattice_sync_options_init(&opts);
    opts.background_sync_managed = 1;

    lattice_db_t* db = lattice_db_create_with_sync_options(
        client_path.str().c_str(), &schema.schema, 1, nullptr,
        "ws://relay.invalid/cancel", "test-token", &opts);
    ASSERT_NE(db, nullptr);

    // Short deadline expires cleanly with BUDGET_EXHAUSTED and settles generation to 0.
    lattice_sync_once_result_t result = LATTICE_SYNC_ONCE_UNAVAILABLE;
    ASSERT_EQ(lattice_sync_once(db, 0.08, &result), LATTICE_OK);
    EXPECT_EQ(result, LATTICE_SYNC_ONCE_BUDGET_EXHAUSTED);
    EXPECT_EQ(lattice_db_current_background_sync_generation(db), 0u);

    // External cancellation token interrupts a long-budget sync_once immediately and settles cleanly.
    lattice_sync_cancellation_t* cancel_tok = lattice_sync_cancellation_create();
    ASSERT_NE(cancel_tok, nullptr);
    EXPECT_FALSE(lattice_sync_cancellation_is_cancelled(cancel_tok));

    lattice_status_t worker_status = LATTICE_ERROR_DATABASE;
    lattice_sync_once_result_t worker_result = LATTICE_SYNC_ONCE_UNAVAILABLE;
    std::thread worker([&]() {
        worker_status = lattice_db_sync_once(db, 15.0, cancel_tok, &worker_result);
    });

    const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (lattice_db_current_background_sync_generation(db) == 0 &&
           std::chrono::steady_clock::now() < wait_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_GT(lattice_db_current_background_sync_generation(db), 0u);

    const auto cancelled_at = std::chrono::steady_clock::now();
    lattice_sync_cancellation_cancel(cancel_tok);
    EXPECT_TRUE(lattice_sync_cancellation_is_cancelled(cancel_tok));
    worker.join();

    EXPECT_LT(std::chrono::steady_clock::now() - cancelled_at, std::chrono::seconds(2));
    EXPECT_EQ(worker_status, LATTICE_OK);
    EXPECT_EQ(worker_result, LATTICE_SYNC_ONCE_BUDGET_EXHAUSTED);
    EXPECT_EQ(lattice_db_current_background_sync_generation(db), 0u);

    lattice_sync_cancellation_release(cancel_tok);
    lattice_db_close(db);
    lattice_db_release(db);
}
