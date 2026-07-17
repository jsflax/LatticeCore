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
// This file includes ONLY lattice.h — everything observable must be
// observable through the C ABI, exactly as a Kotlin/Python binding would
// see it. (SwiftPM: macOS-only executable target `LatticeCAPITests`;
// CMake: built everywhere — see docs/capi-gap-audit.md V1 for why the
// SwiftPM Linux closure must not grow the C API library.)

#include <gtest/gtest.h>

#include "lattice.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <functional>
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
                          "sync_progress", "sync_tuning"}) {
        EXPECT_TRUE(lattice_capi_has_feature(f)) << f;
    }
    // Still reserved.
    for (const char* f : {"read_generations", "to_json", "unified_open"}) {
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
    const std::string needle = "\"tableName\":\"Person\"";
    auto pos = entries_json.find(needle);
    ASSERT_NE(pos, std::string::npos) << entries_json;
    entries_json.replace(pos, needle.size(), "\"tableName\":\"NoSuchTable\"");
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
