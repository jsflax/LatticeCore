#include "lattice.h"
#include <lattice.hpp>
#include <dynamic_object.hpp>
#include <list.hpp>
#include <lattice/sync.hpp>
#include <lattice/network.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <cstring>
#include <cstdlib>

extern "C" void lattice_set_log_level(int level) {
    lattice::set_log_level(static_cast<lattice::log_level>(level));
}

// Thread-local error message storage
static thread_local std::string g_last_error;

static void set_error(const std::string& msg) {
    g_last_error = msg;
}

// =============================================================================
// Opaque Type Definitions (internal)
// =============================================================================

// The C API types are just typedefs to void, but internally we cast to these
using lattice_db_internal = lattice::swift_lattice_ref;
using lattice_object_internal = lattice::dynamic_object_ref;
using lattice_link_list_internal = lattice::link_list_ref;

// Results wrapper - holds vector of managed objects
struct lattice_results_internal {
    std::vector<lattice::managed<lattice::swift_dynamic_object>> objects;
    std::atomic<int> ref_count{1};
};

// Defined with the sync-observer registry near the end of this file; called
// from lattice_db_close so observers' destroy callbacks fire on close.
static void capi_teardown_sync_observers(lattice::swift_lattice* impl) noexcept;

// =============================================================================
// Error Handling
// =============================================================================

extern "C" const char* lattice_last_error(void) {
    return g_last_error.empty() ? nullptr : g_last_error.c_str();
}

// =============================================================================
// Versioning & Introspection
// =============================================================================

extern "C" uint32_t lattice_capi_version(void) {
    return LATTICE_CAPI_VERSION;
}

extern "C" const char* lattice_capi_version_string(void) {
    return LATTICE_CAPI_VERSION_STRING;
}

extern "C" int32_t lattice_schema_format_epoch(void) {
    return lattice::lattice_db::schema_format_epoch();
}

extern "C" bool lattice_capi_has_feature(const char* feature) {
    if (!feature) return false;
    // Keep sorted; the reserved (currently-false) strings are documented in
    // lattice.h and docs/CAPI-STABILITY.md.
    static const char* const kFeatures[] = {
        "attach",
        "checkpoint",
        "cross_process_observation",
        "detach",
        "fts",
        "geo_query",
        "ipc",
        "knn",
        "migration",
        "observation",
        "rollback",
        "row_cache",
        "statement_counters",
        "sync",
        "sync_filter",
        "sync_progress",
        "sync_tuning",
        "to_json",
        "transactions",
    };
    for (const char* const f : kFeatures) {
        if (std::strcmp(feature, f) == 0) return true;
    }
    return false;
}

// =============================================================================
// Database Lifecycle
// =============================================================================

extern "C" lattice_db_t* lattice_db_create_in_memory(void) {
    try {
        auto* ref = lattice::swift_lattice_ref::create_in_memory();
        ref->retain();  // Start with ref_count = 1
        return reinterpret_cast<lattice_db_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_db_t* lattice_db_create_at_path(const char* path) {
    if (!path) {
        set_error("path is null");
        return nullptr;
    }
    try {
        auto* ref = lattice::swift_lattice_ref::create_with_path(std::string(path));
        ref->retain();  // Start with ref_count = 1
        return reinterpret_cast<lattice_db_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" void lattice_db_retain(lattice_db_t* db) {
    if (!db) return;
    auto* ref = reinterpret_cast<lattice_db_internal*>(db);
    ref->retain();
}

extern "C" void lattice_db_release(lattice_db_t* db) {
    if (!db) return;
    auto* ref = reinterpret_cast<lattice_db_internal*>(db);
    if (ref->release()) {
        delete ref;
    }
}

extern "C" bool lattice_db_is_sync_connected(lattice_db_t* db) {
    if (!db) return false;
    auto* ref = reinterpret_cast<lattice_db_internal*>(db);
    try {
        return ref->get()->is_sync_connected();
    } catch (...) {
        return false;
    }
}

// Explicitly close database connections and stop background services.
// Safe to call before release. After calling close(), the db handle
// should only be released (no further operations).
extern "C" void lattice_db_close(lattice_db_t* db) {
    if (!db) return;
    auto* ref = reinterpret_cast<lattice_db_internal*>(db);
    // Tear down C sync observers first so their destroy callbacks fire and
    // no callback lands mid-close (documented in lattice.h).
    try {
        capi_teardown_sync_observers(ref->get());
    } catch (...) {}
    try {
        ref->get()->close();
    } catch (...) {}
}

// =============================================================================
// Helper: Convert C schemas to C++ SchemaVector
// =============================================================================

static lattice::SchemaVector convert_schemas(const lattice_schema_t* schemas, size_t schema_count) {
    lattice::SchemaVector schema_vec;
    if (!schemas || schema_count == 0) return schema_vec;

    schema_vec.reserve(schema_count);
    for (size_t i = 0; i < schema_count; i++) {
        const lattice_schema_t& s = schemas[i];
        if (!s.table_name) continue;

        lattice::SwiftSchema props;
        for (size_t j = 0; j < s.property_count; j++) {
            const lattice_property_t& p = s.properties[j];
            if (!p.name) continue;

            lattice::property_descriptor desc;
            desc.name = p.name;
            desc.type = static_cast<lattice::column_type>(p.type);
            desc.kind = static_cast<lattice::property_kind>(p.kind);
            desc.nullable = p.nullable;
            if (p.target_table) desc.target_table = p.target_table;
            if (p.link_table) desc.link_table = p.link_table;
            desc.is_indexed = p.is_indexed;
            desc.is_unique = p.is_unique;
            desc.is_full_text = p.is_full_text;
            desc.is_vector = p.is_vector;
            desc.is_geo_bounds = p.is_geo_bounds;
            if (p.column_name) desc.column_name = p.column_name;

            props[p.name] = desc;
        }

        schema_vec.emplace_back(s.table_name, props);
    }
    return schema_vec;
}

extern "C" lattice_db_t* lattice_db_create_with_schemas(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count
) {
    try {
        std::string db_path = path ? std::string(path) : ":memory:";
        auto schema_vec = convert_schemas(schemas, schema_count);

        lattice::configuration config(db_path);
        auto* ref = lattice::swift_lattice_ref::create(config, schema_vec);
        ref->retain();  // Start with ref_count = 1
        return reinterpret_cast<lattice_db_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

// =============================================================================
// Scheduler
// =============================================================================

// Internal scheduler wrapper that bridges C callbacks to C++ scheduler interface
struct lattice_scheduler_internal : public lattice::scheduler {
    void* context_;
    lattice_invoke_fn invoke_fn_;
    void (*destroy_fn_)(void*);

    lattice_scheduler_internal(void* ctx, lattice_invoke_fn invoke_fn, void (*destroy_fn)(void*))
        : context_(ctx), invoke_fn_(invoke_fn), destroy_fn_(destroy_fn) {}

    ~lattice_scheduler_internal() override {
        if (destroy_fn_ && context_) {
            destroy_fn_(context_);
        }
    }

    void invoke(std::function<void()>&& fn) override {
        // Wrap the std::function in a static callback that can be called from C
        auto* fn_ptr = new std::function<void()>(std::move(fn));
        invoke_fn_(context_,
            [](void* ctx) {
                auto* f = static_cast<std::function<void()>*>(ctx);
                if (*f) (*f)();
                delete f;
            },
            fn_ptr);
    }

    [[nodiscard]] bool is_on_thread() const noexcept override { return true; }
    [[nodiscard]] bool is_same_as(const lattice::scheduler* other) const noexcept override {
        auto* o = dynamic_cast<const lattice_scheduler_internal*>(other);
        return o && o->context_ == context_;
    }
    [[nodiscard]] bool can_invoke() const noexcept override { return true; }
};

extern "C" lattice_scheduler_t* lattice_scheduler_create(
    void* context,
    lattice_invoke_fn invoke_fn,
    void (*destroy_fn)(void*)
) {
    if (!invoke_fn) {
        set_error("invoke_fn is required");
        return nullptr;
    }
    try {
        auto* sched = new lattice_scheduler_internal(context, invoke_fn, destroy_fn);
        return reinterpret_cast<lattice_scheduler_t*>(sched);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" void lattice_scheduler_release(lattice_scheduler_t* scheduler) {
    if (!scheduler) return;
    auto* sched = reinterpret_cast<lattice_scheduler_internal*>(scheduler);
    delete sched;
}

extern "C" lattice_db_t* lattice_db_create_with_scheduler(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler
) {
    try {
        std::string db_path = path ? std::string(path) : ":memory:";
        auto schema_vec = convert_schemas(schemas, schema_count);

        // Create configuration with scheduler
        lattice::configuration config(db_path);
        if (scheduler) {
            auto* sched = reinterpret_cast<lattice_scheduler_internal*>(scheduler);
            // Create a shared_ptr that doesn't delete (scheduler lifecycle managed by caller)
            config.sched = std::shared_ptr<lattice::scheduler>(sched, [](lattice::scheduler*) {});
        }

        auto* ref = lattice::swift_lattice_ref::create(config, schema_vec);
        ref->retain();
        return reinterpret_cast<lattice_db_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_db_t* lattice_db_create_with_sync(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler,
    const char* wss_endpoint,
    const char* authorization_token
) {
    try {
        std::string db_path = path ? std::string(path) : ":memory:";
        auto schema_vec = convert_schemas(schemas, schema_count);

        // Create configuration with sync settings
        std::string ws_url = wss_endpoint ? std::string(wss_endpoint) : "";
        std::string auth_token = authorization_token ? std::string(authorization_token) : "";

        lattice::configuration config(db_path, ws_url, auth_token);

        if (scheduler) {
            auto* sched = reinterpret_cast<lattice_scheduler_internal*>(scheduler);
            config.sched = std::shared_ptr<lattice::scheduler>(sched, [](lattice::scheduler*) {});
        }

        auto* ref = lattice::swift_lattice_ref::create(config, schema_vec);
        ref->retain();
        return reinterpret_cast<lattice_db_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

// =============================================================================
// Object Lifecycle
// =============================================================================

extern "C" lattice_object_t* lattice_object_create(const char* table_name) {
    if (!table_name) {
        set_error("table_name is null");
        return nullptr;
    }
    try {
        auto* ref = lattice::dynamic_object_ref::create(std::string(table_name));
        ref->retain();  // Start with ref_count = 1
        return reinterpret_cast<lattice_object_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_object_t* lattice_object_create_with_schema(
    const char* table_name,
    const lattice_property_t* properties,
    size_t property_count
) {
    if (!table_name) {
        set_error("table_name is null");
        return nullptr;
    }
    try {
        std::string table_str(table_name);

        // Convert C properties to C++ property map
        std::unordered_map<std::string, lattice::property_descriptor> props;
        if (properties && property_count > 0) {
            for (size_t i = 0; i < property_count; i++) {
                const lattice_property_t& p = properties[i];
                if (!p.name) continue;

                lattice::property_descriptor desc;
                desc.name = p.name;
                desc.type = static_cast<lattice::column_type>(p.type);
                desc.kind = static_cast<lattice::property_kind>(p.kind);
                desc.nullable = p.nullable;
                if (p.target_table) desc.target_table = p.target_table;
                if (p.link_table) desc.link_table = p.link_table;

                props[p.name] = desc;
            }
        }

        // Create swift_dynamic_object with table name and schema
        lattice::swift_dynamic_object swift_obj(table_str, props);

        // Wrap in dynamic_object and then dynamic_object_ref
        auto dyn_obj = std::make_shared<lattice::dynamic_object>(swift_obj);
        auto* ref = lattice::dynamic_object_ref::wrap(dyn_obj);
        ref->retain();
        return reinterpret_cast<lattice_object_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_object_t* lattice_db_create_object(lattice_db_t* db, const char* table_name) {
    if (!db || !table_name) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        std::string table_str(table_name);

        // Get schema from registered schemas (like Swift does)
        const auto* props = db_ref->get()->get_properties_for_table(table_str);
        if (!props) {
            set_error("No schema registered for table");
            return nullptr;
        }

        // Create swift_dynamic_object with table name and schema
        lattice::swift_dynamic_object swift_obj(table_str, *props);

        // Wrap in dynamic_object and then dynamic_object_ref
        auto dyn_obj = std::make_shared<lattice::dynamic_object>(swift_obj);
        auto* ref = lattice::dynamic_object_ref::wrap(dyn_obj);
        ref->retain();
        return reinterpret_cast<lattice_object_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" void lattice_object_retain(lattice_object_t* obj) {
    if (!obj) return;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    ref->retain();
}

extern "C" void lattice_object_release(lattice_object_t* obj) {
    if (!obj) return;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    if (ref->release()) {
        delete ref;
    }
}

// =============================================================================
// Object Field Access - Getters
// =============================================================================

extern "C" int64_t lattice_object_get_int(lattice_object_t* obj, const char* field) {
    if (!obj || !field) return 0;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        return ref->get_int(field);
    } catch (...) {
        return 0;
    }
}

extern "C" double lattice_object_get_double(lattice_object_t* obj, const char* field) {
    if (!obj || !field) return 0.0;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        return ref->get_double(field);
    } catch (...) {
        return 0.0;
    }
}

// String storage for returned strings (per-object, last returned string)
static thread_local std::string g_returned_string;

extern "C" const char* lattice_object_get_string(lattice_object_t* obj, const char* field) {
    if (!obj || !field) return nullptr;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        g_returned_string = ref->get_string(field);
        return g_returned_string.c_str();
    } catch (...) {
        return nullptr;
    }
}

extern "C" size_t lattice_object_get_blob(lattice_object_t* obj, const char* field,
                                          uint8_t* buffer, size_t buffer_size) {
    if (!obj || !field) return 0;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        auto data = ref->get_data(field);
        if (buffer && buffer_size > 0) {
            size_t copy_size = std::min(buffer_size, data.size());
            std::memcpy(buffer, data.data(), copy_size);
        }
        return data.size();
    } catch (...) {
        return 0;
    }
}

extern "C" bool lattice_object_has_value(lattice_object_t* obj, const char* field) {
    if (!obj || !field) return false;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        return ref->has_value(field);
    } catch (...) {
        return false;
    }
}

// =============================================================================
// Object Field Access - Setters
// =============================================================================

extern "C" void lattice_object_set_int(lattice_object_t* obj, const char* field, int64_t value) {
    if (!obj || !field) return;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        ref->set_int(field, value);
    } catch (...) {}
}

extern "C" void lattice_object_set_double(lattice_object_t* obj, const char* field, double value) {
    if (!obj || !field) return;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        ref->set_double(field, value);
    } catch (...) {}
}

extern "C" void lattice_object_set_string(lattice_object_t* obj, const char* field, const char* value) {
    if (!obj || !field) return;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        ref->set_string(field, value ? std::string(value) : std::string());
    } catch (...) {}
}

extern "C" void lattice_object_set_blob(lattice_object_t* obj, const char* field,
                                        const uint8_t* data, size_t size) {
    if (!obj || !field) return;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        std::vector<uint8_t> vec(data, data + size);
        ref->set_data(field, vec);
    } catch (...) {}
}

extern "C" void lattice_object_set_null(lattice_object_t* obj, const char* field) {
    if (!obj || !field) return;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        ref->set_nil(field);
    } catch (...) {}
}

// =============================================================================
// Object Identity
// =============================================================================

extern "C" int64_t lattice_object_get_id(lattice_object_t* obj) {
    if (!obj) return 0;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        if (!ref->has_value("id")) return 0;
        return ref->get_int("id");
    } catch (...) {
        return 0;
    }
}

extern "C" const char* lattice_object_get_global_id(lattice_object_t* obj) {
    if (!obj) return nullptr;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        if (!ref->has_value("globalId")) return nullptr;
        g_returned_string = ref->get_string("globalId");
        return g_returned_string.empty() ? nullptr : g_returned_string.c_str();
    } catch (...) {
        return nullptr;
    }
}

extern "C" const char* lattice_object_get_table_name(lattice_object_t* obj) {
    if (!obj) return nullptr;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        g_returned_string = ref->get()->get_table_name();
        return g_returned_string.c_str();
    } catch (...) {
        return nullptr;
    }
}

// =============================================================================
// CRUD Operations
// =============================================================================

extern "C" lattice_object_t* lattice_db_add(lattice_db_t* db, lattice_object_t* obj) {
    if (!db || !obj) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* obj_ref = reinterpret_cast<lattice_object_internal*>(obj);

        // Add the object to the database
        db_ref->get()->add(*obj_ref->get());

        // Object is now managed - return the same reference
        obj_ref->retain();
        return obj;
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_object_t* lattice_db_find(lattice_db_t* db, const char* table_name, int64_t id) {
    if (!db || !table_name) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto result = db_ref->get()->object(id, std::string(table_name));

        if (!result) {
            return nullptr;
        }

        // Wrap in dynamic_object_ref
        auto* ref = new lattice::dynamic_object_ref(*result);
        ref->retain();
        return reinterpret_cast<lattice_object_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_object_t* lattice_db_find_by_global_id(lattice_db_t* db, const char* table_name,
                                                          const char* global_id) {
    if (!db || !table_name || !global_id) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto result = db_ref->get()->object_by_global_id(std::string(global_id), std::string(table_name));

        if (!result) {
            return nullptr;
        }

        auto* ref = new lattice::dynamic_object_ref(*result);
        ref->retain();
        return reinterpret_cast<lattice_object_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_status_t lattice_db_remove(lattice_db_t* db, lattice_object_t* obj) {
    if (!db || !obj) {
        set_error("null argument");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* obj_ref = reinterpret_cast<lattice_object_internal*>(obj);

        bool success = db_ref->get()->remove(*obj_ref);
        return success ? LATTICE_OK : LATTICE_ERROR_NOT_FOUND;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

// =============================================================================
// Query Operations
// =============================================================================

extern "C" lattice_results_t* lattice_db_query(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause,
    const char* order_by,
    int64_t limit,
    int64_t offset
) {
    if (!db || !table_name) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);

        auto where_opt = where_clause ? std::optional<std::string>(where_clause) : std::nullopt;
        auto order_opt = order_by ? std::optional<std::string>(order_by) : std::nullopt;

        // SQLite requires LIMIT when using OFFSET
        // If offset is set but limit is not, use -1 (unlimited) for limit
        std::optional<int64_t> limit_opt = std::nullopt;
        std::optional<int64_t> offset_opt = std::nullopt;

        if (limit > 0) {
            limit_opt = limit;
        } else if (offset > 0) {
            // OFFSET without LIMIT: use -1 (unlimited) for LIMIT
            limit_opt = -1;
        }
        if (offset > 0) {
            offset_opt = offset;
        }

        auto results = db_ref->get()->objects(table_name, where_opt, order_opt, limit_opt, offset_opt);

        auto* wrapper = new lattice_results_internal();
        wrapper->objects = std::move(results);
        return reinterpret_cast<lattice_results_t*>(wrapper);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" size_t lattice_db_count(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause
) {
    if (!db || !table_name) return 0;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto where_opt = where_clause ? std::optional<std::string>(where_clause) : std::nullopt;
        return db_ref->get()->count(table_name, where_opt);
    } catch (...) {
        return 0;
    }
}

extern "C" size_t lattice_db_delete_where(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause
) {
    if (!db || !table_name) return 0;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto where_opt = where_clause ? std::optional<std::string>(where_clause) : std::nullopt;

        // Get count before delete
        size_t count = db_ref->get()->count(table_name, where_opt);
        db_ref->get()->delete_where(table_name, where_opt);
        return count;
    } catch (...) {
        return 0;
    }
}

// =============================================================================
// Results Iteration
// =============================================================================

extern "C" size_t lattice_results_count(lattice_results_t* results) {
    if (!results) return 0;
    auto* wrapper = reinterpret_cast<lattice_results_internal*>(results);
    return wrapper->objects.size();
}

extern "C" lattice_object_t* lattice_results_get(lattice_results_t* results, size_t index) {
    if (!results) return nullptr;
    auto* wrapper = reinterpret_cast<lattice_results_internal*>(results);
    if (index >= wrapper->objects.size()) return nullptr;

    try {
        auto* ref = new lattice::dynamic_object_ref(wrapper->objects[index]);
        ref->retain();
        return reinterpret_cast<lattice_object_t*>(ref);
    } catch (...) {
        return nullptr;
    }
}

extern "C" void lattice_results_free(lattice_results_t* results) {
    if (!results) return;
    auto* wrapper = reinterpret_cast<lattice_results_internal*>(results);
    delete wrapper;
}

// =============================================================================
// Transactions
// =============================================================================

extern "C" lattice_status_t lattice_db_begin_transaction(lattice_db_t* db) {
    if (!db) return LATTICE_ERROR_NULL_POINTER;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        db_ref->get()->begin_transaction();
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

extern "C" lattice_status_t lattice_db_commit(lattice_db_t* db) {
    if (!db) return LATTICE_ERROR_NULL_POINTER;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        db_ref->get()->commit();
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

extern "C" lattice_status_t lattice_db_rollback(lattice_db_t* db) {
    if (!db) return LATTICE_ERROR_NULL_POINTER;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        db_ref->get()->db().execute("ROLLBACK");
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

// =============================================================================
// Observation
// =============================================================================

extern "C" uint64_t lattice_db_observe_table(
    lattice_db_t* db,
    const char* table_name,
    void* context,
    lattice_table_observer_fn callback
) {
    if (!db || !table_name || !callback) return 0;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        // Use lattice_db::add_table_observer directly (bypass swift_lattice overloads)
        auto cb = callback;
        auto ctx = context;
        return static_cast<lattice::lattice_db*>(db_ref->get())->add_table_observer(
            std::string(table_name),
            [cb, ctx](const std::vector<lattice::lattice_db::change_event>& events) {
                for (const auto& ev : events) {
                    const auto& op = std::get<1>(ev);
                    int64_t row_id = std::get<2>(ev);
                    const auto& global_id = std::get<3>(ev);
                    cb(ctx, op.c_str(), row_id, global_id.c_str());
                }
            });
    } catch (...) {
        return 0;
    }
}

extern "C" uint64_t lattice_db_observe_object(
    lattice_db_t* db,
    const char* table_name,
    int64_t row_id,
    void* context,
    lattice_object_observer_fn callback
) {
    if (!db || !table_name || !callback) return 0;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        struct obj_observer_ctx {
            void* user_context;
            lattice_object_observer_fn user_callback;
        };
        auto* obs_ctx = new obj_observer_ctx{context, callback};
        return db_ref->get()->add_object_observer(std::string(table_name), row_id, obs_ctx,
            [](const char*, void* ctx) {
                auto* oc = static_cast<obj_observer_ctx*>(ctx);
                oc->user_callback(oc->user_context);
            },
            [](void* ctx) {
                delete static_cast<obj_observer_ctx*>(ctx);
            });
    } catch (...) {
        return 0;
    }
}

extern "C" void lattice_db_remove_table_observer(lattice_db_t* db, const char* table_name, uint64_t token) {
    if (!db || !table_name) return;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        db_ref->get()->remove_table_observer(table_name, token);
    } catch (...) {}
}

extern "C" void lattice_db_remove_object_observer(lattice_db_t* db, const char* table_name,
                                                   int64_t row_id, uint64_t token) {
    if (!db || !table_name) return;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        db_ref->get()->remove_object_observer(table_name, row_id, token);
    } catch (...) {}
}

// =============================================================================
// Object Relationships - wrapping dynamic_object_ref (same as Swift)
// =============================================================================

extern "C" void lattice_object_set_object(lattice_object_t* obj, const char* field, lattice_object_t* value) {
    if (!obj || !field) return;
    try {
        auto* obj_ref = reinterpret_cast<lattice_object_internal*>(obj);
        if (value) {
            auto* value_ref = reinterpret_cast<lattice_object_internal*>(value);
            obj_ref->set_object(std::string(field), *value_ref);
        } else {
            obj_ref->set_nil(std::string(field));
        }
    } catch (...) {}
}

extern "C" lattice_object_t* lattice_object_get_object(lattice_object_t* obj, const char* field) {
    if (!obj || !field) return nullptr;
    try {
        auto* obj_ref = reinterpret_cast<lattice_object_internal*>(obj);
        auto* linked = obj_ref->get_object(std::string(field));
        return reinterpret_cast<lattice_object_t*>(linked);
    } catch (...) {
        return nullptr;
    }
}

// =============================================================================
// Link Lists - wrapping link_list_ref (same as Swift's List<T>)
// =============================================================================

extern "C" lattice_link_list_t* lattice_object_get_link_list(lattice_object_t* obj, const char* field) {
    if (!obj || !field) return nullptr;
    try {
        auto* obj_ref = reinterpret_cast<lattice_object_internal*>(obj);
        auto* list_ref = obj_ref->get_link_list(std::string(field));
        if (list_ref) {
            list_ref->retain();
        }
        return reinterpret_cast<lattice_link_list_t*>(list_ref);
    } catch (...) {
        return nullptr;
    }
}

extern "C" void lattice_link_list_release(lattice_link_list_t* list) {
    if (!list) return;
    auto* list_ref = reinterpret_cast<lattice_link_list_internal*>(list);
    if (list_ref->release()) {
        delete list_ref;
    }
}

extern "C" size_t lattice_link_list_size(lattice_link_list_t* list) {
    if (!list) return 0;
    auto* list_ref = reinterpret_cast<lattice_link_list_internal*>(list);
    return list_ref->size();
}

extern "C" lattice_object_t* lattice_link_list_get(lattice_link_list_t* list, size_t index) {
    if (!list) return nullptr;
    try {
        auto* list_ref = reinterpret_cast<lattice_link_list_internal*>(list);
        if (index >= list_ref->size()) return nullptr;

        auto proxy = (*list_ref)[index];
        auto* obj_ref = lattice::dynamic_object_ref::wrap(proxy.object);
        obj_ref->retain();
        return reinterpret_cast<lattice_object_t*>(obj_ref);
    } catch (...) {
        return nullptr;
    }
}

extern "C" void lattice_link_list_push_back(lattice_link_list_t* list, lattice_object_t* obj) {
    if (!list || !obj) return;
    try {
        auto* list_ref = reinterpret_cast<lattice_link_list_internal*>(list);
        auto* obj_ref = reinterpret_cast<lattice_object_internal*>(obj);
        list_ref->push_back(*obj_ref);
    } catch (...) {}
}

extern "C" void lattice_link_list_erase(lattice_link_list_t* list, size_t index) {
    if (!list) return;
    try {
        auto* list_ref = reinterpret_cast<lattice_link_list_internal*>(list);
        if (index < list_ref->size()) {
            list_ref->erase(index);
        }
    } catch (...) {}
}

extern "C" void lattice_link_list_clear(lattice_link_list_t* list) {
    if (!list) return;
    try {
        auto* list_ref = reinterpret_cast<lattice_link_list_internal*>(list);
        list_ref->clear();
    } catch (...) {}
}

// =============================================================================
// Sync Operations
// =============================================================================

extern "C" char* lattice_db_receive_sync_data(
    lattice_db_t* db,
    const uint8_t* data,
    size_t data_size
) {
    if (!db || !data || data_size == 0) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);

        // Validate the payload up front so a malformed message keeps the
        // documented NULL-on-error contract (the bridge call below maps a
        // parse failure to an empty result without setting an error).
        std::string json_str(reinterpret_cast<const char*>(data), data_size);
        if (!lattice::server_sent_event::from_json(json_str)) {
            set_error("Failed to parse sync data");
            return nullptr;
        }

        // Route through the same core path the Swift bridge uses
        // (swift_lattice::receive_sync_data -> lattice::apply_remote_changes):
        // per-entry error isolation, returns only the successfully-applied
        // ids. Replaces a divergent all-or-nothing reimplementation that
        // left already-executed entries unreported when a later entry threw
        // (docs/capi-gap-audit.md B-2).
        std::vector<uint8_t> bytes(data, data + data_size);
        auto applied_ids = db_ref->get()->receive_sync_data(bytes);

        if (auto err = db_ref->get()->last_receive_error()) {
            set_error(*err);
            return nullptr;
        }

        // Return JSON array of applied IDs
        nlohmann::json result = applied_ids;
        std::string result_str = result.dump();

        char* ret = static_cast<char*>(malloc(result_str.size() + 1));
        if (ret) {
            std::memcpy(ret, result_str.c_str(), result_str.size() + 1);
        }
        return ret;
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" char* lattice_db_get_pending_audit_log(lattice_db_t* db) {
    if (!db) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);

        // Query unsynchronized audit log entries
        auto entries = lattice::query_audit_log(db_ref->get()->db(), true, std::nullopt);

        // Convert to JSON array
        nlohmann::json result = nlohmann::json::array();
        for (const auto& entry : entries) {
            result.push_back(nlohmann::json::parse(entry.to_json()));
        }

        std::string result_str = result.dump();
        char* ret = static_cast<char*>(malloc(result_str.size() + 1));
        if (ret) {
            std::memcpy(ret, result_str.c_str(), result_str.size() + 1);
        }
        return ret;
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" char* lattice_db_events_after(lattice_db_t* db, const char* checkpoint_global_id) {
    if (!db) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);

        std::optional<std::string> checkpoint = std::nullopt;
        if (checkpoint_global_id && strlen(checkpoint_global_id) > 0) {
            checkpoint = std::string(checkpoint_global_id);
        }

        auto entries = lattice::events_after(db_ref->get()->db(), checkpoint);

        // Convert to JSON array
        nlohmann::json result = nlohmann::json::array();
        for (const auto& entry : entries) {
            result.push_back(nlohmann::json::parse(entry.to_json()));
        }

        std::string result_str = result.dump();
        char* ret = static_cast<char*>(malloc(result_str.size() + 1));
        if (ret) {
            std::memcpy(ret, result_str.c_str(), result_str.size() + 1);
        }
        return ret;
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" char* lattice_create_sync_message(const char* entries_json) {
    if (!entries_json) {
        set_error("null argument");
        return nullptr;
    }
    try {
        nlohmann::json entries = nlohmann::json::parse(entries_json);
        if (!entries.is_array() || entries.empty()) {
            return nullptr;  // No entries, return null (not an error)
        }

        // Parse entries into audit_log_entry objects
        std::vector<lattice::audit_log_entry> logs;
        for (const auto& entry_json : entries) {
            auto entry = lattice::audit_log_entry::from_json(entry_json.dump());
            if (entry) {
                logs.push_back(*entry);
            }
        }

        if (logs.empty()) {
            return nullptr;
        }

        // Create server_sent_event and serialize to JSON
        auto event = lattice::server_sent_event::make_audit_log(std::move(logs));
        std::string result_str = event.to_json();

        char* ret = static_cast<char*>(malloc(result_str.size() + 1));
        if (ret) {
            std::memcpy(ret, result_str.c_str(), result_str.size() + 1);
        }
        return ret;
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" char* lattice_create_ack_message(const char* global_ids_json) {
    if (!global_ids_json) {
        set_error("null argument");
        return nullptr;
    }
    try {
        nlohmann::json ids = nlohmann::json::parse(global_ids_json);
        if (!ids.is_array()) {
            set_error("expected JSON array");
            return nullptr;
        }

        std::vector<std::string> global_ids;
        for (const auto& id : ids) {
            global_ids.push_back(id.get<std::string>());
        }

        // Create server_sent_event and serialize to JSON
        auto event = lattice::server_sent_event::make_ack(std::move(global_ids));
        std::string result_str = event.to_json();

        char* ret = static_cast<char*>(malloc(result_str.size() + 1));
        if (ret) {
            std::memcpy(ret, result_str.c_str(), result_str.size() + 1);
        }
        return ret;
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" void lattice_db_mark_synced(lattice_db_t* db, const char* global_ids_json) {
    if (!db || !global_ids_json) return;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);

        // Parse JSON array of globalIds
        nlohmann::json ids = nlohmann::json::parse(global_ids_json);
        std::vector<std::string> global_ids;
        for (const auto& id : ids) {
            global_ids.push_back(id.get<std::string>());
        }

        lattice::mark_audit_entries_synced(*db_ref->get(), global_ids);
    } catch (...) {}
}

extern "C" int64_t lattice_db_compact_audit_log(lattice_db_t* db) {
    if (!db) return 0;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        return db_ref->get()->compact_audit_log();
    } catch (...) {
        return 0;
    }
}

extern "C" int64_t lattice_db_generate_history(lattice_db_t* db) {
    if (!db) return 0;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        return db_ref->get()->generate_history();
    } catch (...) {
        return 0;
    }
}

extern "C" void lattice_string_free(char* str) {
    if (str) {
        free(str);
    }
}

// =============================================================================
// WebSocket Client - C API wrapper around generic_websocket_client
// =============================================================================

// Internal wrapper for C callback-based WebSocket client
struct lattice_websocket_client_internal : public lattice::sync_transport {
    void* user_data_;
    lattice_ws_connect_fn connect_fn_;
    lattice_ws_disconnect_fn disconnect_fn_;
    lattice_ws_state_fn state_fn_;
    lattice_ws_send_fn send_fn_;

    // Event handlers (C++ side calls these)
    void* on_open_user_data_ = nullptr;
    void* on_message_user_data_ = nullptr;
    void* on_error_user_data_ = nullptr;
    void* on_close_user_data_ = nullptr;

    lattice_ws_on_open_fn on_open_fn_ = nullptr;
    lattice_ws_on_message_fn on_message_fn_ = nullptr;
    lattice_ws_on_error_fn on_error_fn_ = nullptr;
    lattice_ws_on_close_fn on_close_fn_ = nullptr;

    // C++ handlers from sync layer (set via set_on_* methods)
    on_open_handler cpp_on_open_handler_;
    on_message_handler cpp_on_message_handler_;
    on_error_handler cpp_on_error_handler_;
    on_close_handler cpp_on_close_handler_;

    lattice_websocket_client_internal(
        void* user_data,
        lattice_ws_connect_fn connect_fn,
        lattice_ws_disconnect_fn disconnect_fn,
        lattice_ws_state_fn state_fn,
        lattice_ws_send_fn send_fn
    ) : user_data_(user_data)
      , connect_fn_(connect_fn)
      , disconnect_fn_(disconnect_fn)
      , state_fn_(state_fn)
      , send_fn_(send_fn)
    {}

    ~lattice_websocket_client_internal() override = default;

    void connect(const std::string& url,
                 const std::map<std::string, std::string>& headers = {}) override {
        if (connect_fn_) {
            // Convert headers to JSON
            nlohmann::json headers_json = headers;
            std::string headers_str = headers_json.dump();
            connect_fn_(user_data_, url.c_str(), headers_str.c_str());
        }
    }

    void disconnect() override {
        if (disconnect_fn_) {
            disconnect_fn_(user_data_);
        }
    }

    lattice::transport_state state() const override {
        if (state_fn_) {
            auto s = state_fn_(const_cast<void*>(user_data_));
            return static_cast<lattice::transport_state>(s);
        }
        return lattice::transport_state::closed;
    }

    void send(const lattice::transport_message& message) override {
        if (send_fn_) {
            auto type = (message.msg_type == lattice::transport_message::type::text)
                ? LATTICE_WS_MSG_TEXT : LATTICE_WS_MSG_BINARY;
            send_fn_(user_data_, type, message.data.data(), message.data.size());
        }
    }

    void set_on_open(on_open_handler handler) override {
        cpp_on_open_handler_ = handler;
    }

    void set_on_message(on_message_handler handler) override {
        cpp_on_message_handler_ = handler;
    }

    void set_on_error(on_error_handler handler) override {
        cpp_on_error_handler_ = handler;
    }

    void set_on_close(on_close_handler handler) override {
        cpp_on_close_handler_ = handler;
    }

    // Called by platform to trigger events into C++
    void trigger_on_open() {
        // Call C++ handler (sync layer)
        if (cpp_on_open_handler_) {
            cpp_on_open_handler_();
        }
        // Call C callback (for platform notification)
        if (on_open_fn_) {
            on_open_fn_(on_open_user_data_);
        }
    }

    void trigger_on_message(lattice_websocket_msg_type_t type, const uint8_t* data, size_t size) {
        // Call C++ handler (sync layer) - this is the important one!
        if (cpp_on_message_handler_) {
            lattice::transport_message msg;
            msg.msg_type = (type == LATTICE_WS_MSG_TEXT)
                ? lattice::transport_message::type::text
                : lattice::transport_message::type::binary;
            msg.data.assign(data, data + size);
            cpp_on_message_handler_(msg);
        }
        // Call C callback (for platform notification)
        if (on_message_fn_) {
            on_message_fn_(on_message_user_data_, type, data, size);
        }
    }

    void trigger_on_error(const char* error) {
        // Call C++ handler (sync layer)
        if (cpp_on_error_handler_) {
            cpp_on_error_handler_(error ? error : "");
        }
        // Call C callback (for platform notification)
        if (on_error_fn_) {
            on_error_fn_(on_error_user_data_, error);
        }
    }

    void trigger_on_close(int code, const char* reason) {
        // Call C++ handler (sync layer)
        if (cpp_on_close_handler_) {
            cpp_on_close_handler_(code, reason ? reason : "");
        }
        // Call C callback (for platform notification)
        if (on_close_fn_) {
            on_close_fn_(on_close_user_data_, code, reason);
        }
    }
};

extern "C" lattice_websocket_client_t* lattice_websocket_client_create(
    void* user_data,
    lattice_ws_connect_fn connect_fn,
    lattice_ws_disconnect_fn disconnect_fn,
    lattice_ws_state_fn state_fn,
    lattice_ws_send_fn send_fn
) {
    if (!connect_fn || !disconnect_fn || !state_fn || !send_fn) {
        set_error("All WebSocket callbacks are required");
        return nullptr;
    }
    try {
        auto* client = new lattice_websocket_client_internal(
            user_data, connect_fn, disconnect_fn, state_fn, send_fn
        );
        return reinterpret_cast<lattice_websocket_client_t*>(client);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" void lattice_websocket_client_release(lattice_websocket_client_t* client) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    delete ws;
}

extern "C" void lattice_websocket_client_set_on_open(
    lattice_websocket_client_t* client,
    void* user_data,
    lattice_ws_on_open_fn fn
) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    ws->on_open_user_data_ = user_data;
    ws->on_open_fn_ = fn;
}

extern "C" void lattice_websocket_client_set_on_message(
    lattice_websocket_client_t* client,
    void* user_data,
    lattice_ws_on_message_fn fn
) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    ws->on_message_user_data_ = user_data;
    ws->on_message_fn_ = fn;
}

extern "C" void lattice_websocket_client_set_on_error(
    lattice_websocket_client_t* client,
    void* user_data,
    lattice_ws_on_error_fn fn
) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    ws->on_error_user_data_ = user_data;
    ws->on_error_fn_ = fn;
}

extern "C" void lattice_websocket_client_set_on_close(
    lattice_websocket_client_t* client,
    void* user_data,
    lattice_ws_on_close_fn fn
) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    ws->on_close_user_data_ = user_data;
    ws->on_close_fn_ = fn;
}

extern "C" void lattice_websocket_client_trigger_on_open(lattice_websocket_client_t* client) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    ws->trigger_on_open();
}

extern "C" void lattice_websocket_client_trigger_on_message(
    lattice_websocket_client_t* client,
    lattice_websocket_msg_type_t type,
    const uint8_t* data,
    size_t data_size
) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    ws->trigger_on_message(type, data, data_size);
}

extern "C" void lattice_websocket_client_trigger_on_error(
    lattice_websocket_client_t* client,
    const char* error
) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    ws->trigger_on_error(error ? error : "Unknown error");
}

extern "C" void lattice_websocket_client_trigger_on_close(
    lattice_websocket_client_t* client,
    int code,
    const char* reason
) {
    if (!client) return;
    auto* ws = reinterpret_cast<lattice_websocket_client_internal*>(client);
    ws->trigger_on_close(code, reason ? reason : "");
}

// =============================================================================
// Network Factory - C API wrapper
// =============================================================================

// Internal wrapper for C callback-based network factory
struct lattice_network_factory_internal : public lattice::network_factory {
    void* user_data_;
    lattice_create_websocket_fn create_ws_fn_;
    void (*destroy_fn_)(void*);

    lattice_network_factory_internal(
        void* user_data,
        lattice_create_websocket_fn create_ws_fn,
        void (*destroy_fn)(void*)
    ) : user_data_(user_data)
      , create_ws_fn_(create_ws_fn)
      , destroy_fn_(destroy_fn)
    {}

    ~lattice_network_factory_internal() override {
        if (destroy_fn_ && user_data_) {
            destroy_fn_(user_data_);
        }
    }

    std::unique_ptr<lattice::http_client> create_http_client() override {
        // Not implemented in C API yet
        return nullptr;
    }

    std::unique_ptr<lattice::sync_transport> create_sync_transport() override {
        if (create_ws_fn_) {
            auto* c_client = create_ws_fn_(user_data_);
            if (c_client) {
                // The C API returns a raw pointer, wrap it in unique_ptr
                auto* internal = reinterpret_cast<lattice_websocket_client_internal*>(c_client);
                return std::unique_ptr<lattice::sync_transport>(internal);
            }
        }
        return nullptr;
    }
};

extern "C" lattice_network_factory_t* lattice_network_factory_create(
    void* user_data,
    lattice_create_websocket_fn create_ws_fn,
    void (*destroy_fn)(void* user_data)
) {
    if (!create_ws_fn) {
        set_error("create_ws_fn is required");
        return nullptr;
    }
    try {
        auto* factory = new lattice_network_factory_internal(user_data, create_ws_fn, destroy_fn);
        return reinterpret_cast<lattice_network_factory_t*>(factory);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" void lattice_network_factory_release(lattice_network_factory_t* factory) {
    if (!factory) return;
    auto* f = reinterpret_cast<lattice_network_factory_internal*>(factory);
    delete f;
}

extern "C" void lattice_set_network_factory(lattice_network_factory_t* factory) {
    if (!factory) {
        lattice::set_network_factory(nullptr);
        return;
    }
    auto* f = reinterpret_cast<lattice_network_factory_internal*>(factory);
    // Create shared_ptr that doesn't delete (lifecycle managed by caller)
    auto shared = std::shared_ptr<lattice::network_factory>(f, [](lattice::network_factory*) {});
    lattice::set_network_factory(shared);
}

extern "C" void lattice_db_set_network_factory(lattice_db_t* db, lattice_network_factory_t* factory) {
    if (!db) return;
    auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);

    if (!factory) {
        // Clear the network factory
        return;
    }

    auto* f = reinterpret_cast<lattice_network_factory_internal*>(factory);
    // Create shared_ptr that doesn't delete (lifecycle managed by caller)
    auto shared = std::shared_ptr<lattice::network_factory>(f, [](lattice::network_factory*) {});

    // Store on the database (need to add this method to swift_lattice if not present)
    // For now, use global factory
    lattice::set_network_factory(shared);
}

// =============================================================================
// Query Extensions
// =============================================================================

// Thread-local storage for KNN result counts
static thread_local size_t g_last_knn_count = 0;

extern "C" lattice_knn_result_t* lattice_db_query_nearest(
    lattice_db_t* db,
    const char* table_name,
    const char* column_name,
    const uint8_t* query_vector,
    size_t vector_size,
    int k,
    lattice_distance_metric_t metric,
    const char* where_clause
) {
    if (!db || !table_name || !column_name || !query_vector || vector_size == 0 || k <= 0) {
        set_error("Invalid arguments to lattice_db_query_nearest");
        g_last_knn_count = 0;
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* impl = db_ref->get();

        std::vector<uint8_t> vec(query_vector, query_vector + vector_size);
        auto cpp_metric = static_cast<lattice::lattice_db::distance_metric>(metric);
        std::optional<std::string> where_opt;
        if (where_clause) where_opt = where_clause;

        auto results = impl->knn_query(table_name, column_name, vec, k, cpp_metric, where_opt);

        if (results.empty()) {
            g_last_knn_count = 0;
            return nullptr;
        }

        auto* c_results = static_cast<lattice_knn_result_t*>(
            malloc(sizeof(lattice_knn_result_t) * results.size()));
        for (size_t i = 0; i < results.size(); i++) {
            c_results[i].global_id = strdup(results[i].global_id.c_str());
            c_results[i].distance = results[i].distance;
        }
        g_last_knn_count = results.size();
        return c_results;
    } catch (const std::exception& e) {
        set_error(e.what());
        g_last_knn_count = 0;
        return nullptr;
    }
}

extern "C" size_t lattice_knn_results_count(lattice_knn_result_t* results) {
    (void)results;
    return g_last_knn_count;
}

extern "C" void lattice_knn_results_free(lattice_knn_result_t* results, size_t count) {
    if (!results) return;
    for (size_t i = 0; i < count; i++) {
        free(const_cast<char*>(results[i].global_id));
    }
    free(results);
}

extern "C" lattice_results_t* lattice_db_query_fts(
    lattice_db_t* db,
    const char* table_name,
    const char* column_name,
    const char* match_expression,
    const char* order_by,
    int64_t limit
) {
    if (!db || !table_name || !column_name || !match_expression) {
        set_error("Invalid arguments to lattice_db_query_fts");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* impl = db_ref->get();

        // Build FTS5 WHERE clause using subquery
        std::string fts_table = "_" + std::string(table_name) + "_" + column_name + "_fts";
        std::string fts_where = "id IN (SELECT rowid FROM " + fts_table +
                                " WHERE " + fts_table + " MATCH '" +
                                std::string(match_expression) + "')";

        std::optional<std::string> order_opt;
        if (order_by && std::string(order_by).length() > 0) {
            order_opt = order_by;
        }
        std::optional<int64_t> limit_opt;
        if (limit > 0) limit_opt = limit;

        auto managed_results = impl->objects(table_name, fts_where, order_opt, limit_opt);

        auto* results = new lattice_results_internal();
        results->objects = std::move(managed_results);
        return reinterpret_cast<lattice_results_t*>(results);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_results_t* lattice_db_query_within_bounds(
    lattice_db_t* db,
    const char* table_name,
    const char* column_name,
    double min_lat,
    double max_lat,
    double min_lon,
    double max_lon,
    const char* where_clause,
    int64_t limit
) {
    if (!db || !table_name || !column_name) {
        set_error("Invalid arguments to lattice_db_query_within_bounds");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* impl = db_ref->get();

        // Build WHERE clause using R*Tree subquery
        std::string rtree_table = "_" + std::string(table_name) + "_" + column_name + "_rtree";
        std::string bounds_where = "id IN (SELECT id FROM " + rtree_table +
                          " WHERE minLat >= " + std::to_string(min_lat) +
                          " AND maxLat <= " + std::to_string(max_lat) +
                          " AND minLon >= " + std::to_string(min_lon) +
                          " AND maxLon <= " + std::to_string(max_lon) + ")";
        if (where_clause) {
            bounds_where += " AND " + std::string(where_clause);
        }

        std::optional<int64_t> limit_opt;
        if (limit > 0) limit_opt = limit;

        auto managed_results = impl->objects(table_name, bounds_where, std::nullopt, limit_opt);

        auto* results = new lattice_results_internal();
        results->objects = std::move(managed_results);
        return reinterpret_cast<lattice_results_t*>(results);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" lattice_results_t* lattice_db_query_distinct(
    lattice_db_t* db,
    const char* table_name,
    const char* distinct_by,
    const char* where_clause,
    const char* order_by,
    int64_t limit,
    int64_t offset
) {
    if (!db || !table_name) {
        set_error("Invalid arguments to lattice_db_query_distinct");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* impl = db_ref->get();

        std::optional<std::string> where_opt, order_opt, distinct_opt;
        std::optional<int64_t> limit_opt, offset_opt;

        if (where_clause) where_opt = where_clause;
        if (order_by) order_opt = order_by;
        if (distinct_by) distinct_opt = distinct_by;
        if (limit > 0) limit_opt = limit;
        if (offset > 0) offset_opt = offset;

        auto managed_results = impl->objects(table_name, where_opt, order_opt, limit_opt, offset_opt,
                                              std::nullopt, distinct_opt);

        auto* results = new lattice_results_internal();
        results->objects = std::move(managed_results);
        return reinterpret_cast<lattice_results_t*>(results);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

extern "C" size_t lattice_db_count_distinct(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause,
    const char* group_by,
    const char* distinct_by
) {
    if (!db || !table_name) {
        set_error("Invalid arguments");
        return 0;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* impl = db_ref->get();

        std::optional<std::string> where_opt, group_opt, distinct_opt;
        if (where_clause) where_opt = where_clause;
        if (group_by) group_opt = group_by;
        if (distinct_by) distinct_opt = distinct_by;

        return impl->count(table_name, where_opt, group_opt, distinct_opt);
    } catch (const std::exception& e) {
        set_error(e.what());
        return 0;
    }
}

// =============================================================================
// Database Attachment
// =============================================================================

extern "C" lattice_status_t lattice_db_attach(lattice_db_t* db, lattice_db_t* other) {
    if (!db || !other) {
        set_error("Invalid arguments to lattice_db_attach");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* other_ref = reinterpret_cast<lattice_db_internal*>(other);
        // Since ATT-3 the bridge attach reports failure via bool +
        // last_attach_error() instead of throwing — discarding the bool made
        // attach failures return LATTICE_OK with no error set (P0, see
        // docs/capi-gap-audit.md B-1). The catch stays for older/other paths.
        if (!db_ref->get()->attach(*other_ref->get())) {
            auto reason = db_ref->get()->last_attach_error();
            set_error(reason ? reason->c_str() : "attach failed");
            return LATTICE_ERROR_DATABASE;
        }
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

// =============================================================================
// Migration Support
// =============================================================================

extern "C" lattice_db_t* lattice_db_create_with_migration(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler,
    int32_t target_schema_version,
    void* migration_context,
    lattice_migration_row_fn migration_fn,
    const char* migration_table
) {
    try {
        std::string db_path = path ? std::string(path) : ":memory:";
        auto schema_vec = convert_schemas(schemas, schema_count);

        lattice::configuration config(db_path);
        config.target_schema_version = target_schema_version;

        if (scheduler) {
            auto* sched = reinterpret_cast<lattice_scheduler_internal*>(scheduler);
            config.sched = std::shared_ptr<lattice::scheduler>(sched, [](lattice::scheduler*) {});
        }

        if (migration_fn && migration_table) {
            // B-8 (docs/capi-gap-audit.md): the JSON row round-trip below
            // cannot represent BLOB values - std::vector<uint8_t> variants
            // were silently skipped when serializing old_row and blob values
            // cannot be expressed in new_values_json, so a C-driven migration
            // of a blob-bearing table lost data silently. Refuse loudly
            // instead. (Vector columns are BLOB-typed and are refused too.)
            // Full BLOB-capable migration rides the unified-open ABI work.
            for (const auto& entry : schema_vec) {
                if (entry.table_name != migration_table) continue;
                for (const auto& [prop_name, desc] : entry.properties) {
                    if (desc.kind == lattice::property_kind::primitive &&
                        desc.type == lattice::column_type::blob) {
                        set_error(std::string("lattice_db_create_with_migration: table '") +
                                  migration_table + "' has BLOB column '" + prop_name +
                                  "' - the C migration callback's JSON row format cannot "
                                  "represent BLOB values, and migrating this table would "
                                  "silently drop them. BLOB-table row migration via the C "
                                  "ABI is unsupported (see lattice.h).");
                        return nullptr;
                    }
                }
            }
            std::string table_name = migration_table;
            config.migration_block = [migration_context, migration_fn, table_name](
                lattice::migration_context& ctx) {
                ctx.enumerate_objects(table_name, [&](const lattice::migration_row& old_row,
                                                       lattice::migration_row& new_row) {
                    // Serialize old row to JSON
                    nlohmann::json old_json;
                    for (const auto& [k, v] : old_row) {
                        if (std::holds_alternative<std::nullptr_t>(v)) {
                            old_json[k] = nullptr;
                        } else if (std::holds_alternative<int64_t>(v)) {
                            old_json[k] = std::get<int64_t>(v);
                        } else if (std::holds_alternative<double>(v)) {
                            old_json[k] = std::get<double>(v);
                        } else if (std::holds_alternative<std::string>(v)) {
                            old_json[k] = std::get<std::string>(v);
                        } else if (std::holds_alternative<std::vector<uint8_t>>(v)) {
                            // B-8: a BLOB value from the OLD on-disk schema
                            // (column absent from the new schema, so the
                            // up-front check could not see it). Silently
                            // dropping it loses data - fail the open instead.
                            throw std::runtime_error(
                                "lattice_db_create_with_migration: row in table '" +
                                table_name + "' carries BLOB column '" + k +
                                "' - the C migration callback's JSON row format cannot "
                                "represent BLOB values (see lattice.h)");
                        }
                    }

                    char* new_json_str = nullptr;
                    migration_fn(migration_context, old_json.dump().c_str(), &new_json_str);

                    if (new_json_str) {
                        try {
                            auto new_json = nlohmann::json::parse(new_json_str);
                            for (auto& [key, val] : new_json.items()) {
                                if (val.is_null()) {
                                    new_row[key] = nullptr;
                                } else if (val.is_number_integer()) {
                                    new_row[key] = val.get<int64_t>();
                                } else if (val.is_number_float()) {
                                    new_row[key] = val.get<double>();
                                } else if (val.is_string()) {
                                    new_row[key] = val.get<std::string>();
                                }
                            }
                        } catch (...) {}
                        free(new_json_str);
                    }
                });
            };
        }

        auto* ref = lattice::swift_lattice_ref::create(config, schema_vec);
        ref->retain();
        return reinterpret_cast<lattice_db_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

// =============================================================================
// Add with preserved global ID
// =============================================================================

extern "C" lattice_object_t* lattice_db_add_with_global_id(
    lattice_db_t* db,
    lattice_object_t* obj,
    const char* global_id
) {
    if (!db || !obj || !global_id) {
        set_error("Invalid arguments to lattice_db_add_with_global_id");
        return nullptr;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* obj_ref = reinterpret_cast<lattice_object_internal*>(obj);

        db_ref->get()->add_preserving_global_id(*obj_ref, std::string(global_id));

        obj_ref->retain();
        return obj;
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

// =============================================================================
// IPC Sync Configuration
// =============================================================================

extern "C" lattice_db_t* lattice_db_create_with_ipc(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler,
    const char** ipc_channels,
    size_t ipc_channel_count
) {
    try {
        std::string db_path = path ? std::string(path) : ":memory:";
        auto schema_vec = convert_schemas(schemas, schema_count);

        lattice::configuration config(db_path);

        if (scheduler) {
            auto* sched = reinterpret_cast<lattice_scheduler_internal*>(scheduler);
            config.sched = std::shared_ptr<lattice::scheduler>(sched, [](lattice::scheduler*) {});
        }

        for (size_t i = 0; i < ipc_channel_count; i++) {
            if (ipc_channels[i]) {
                lattice::configuration::ipc_target target;
                target.channel = ipc_channels[i];
                config.ipc_targets.push_back(std::move(target));
            }
        }

        auto* ref = lattice::swift_lattice_ref::create(config, schema_vec);
        ref->retain();
        return reinterpret_cast<lattice_db_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

// =============================================================================
// Sync Filter
// =============================================================================

// Shared by lattice_db_set_sync_filter and
// lattice_db_create_with_sync_options (lattice_sync_options_t.sync_filter_json).
// Throws on malformed JSON.
static std::vector<lattice::sync_filter_entry> parse_sync_filter_json(const char* filter_json) {
    auto json = nlohmann::json::parse(filter_json);
    if (!json.is_array()) {
        throw std::runtime_error(
            "sync filter JSON must be an array of {\"table\", \"predicate\"} objects");
    }
    std::vector<lattice::sync_filter_entry> filters;
    for (const auto& entry : json) {
        lattice::sync_filter_entry f;
        f.table_name = entry.value("table", "");
        // The header documents {"table", "predicate"}; the impl
        // historically parsed only the core field name "where_clause",
        // silently dropping documented-form predicates (the table then
        // synced ALL rows - docs/capi-gap-audit.md B-3). Accept both,
        // preferring the documented key.
        std::string where = entry.value("predicate", "");
        if (where.empty()) where = entry.value("where_clause", "");
        if (!where.empty()) f.where_clause = where;
        filters.push_back(std::move(f));
    }
    return filters;
}

extern "C" lattice_status_t lattice_db_set_sync_filter(lattice_db_t* db, const char* filter_json) {
    if (!db || !filter_json) {
        set_error("Invalid arguments");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* impl = db_ref->get();
        impl->update_sync_filter(parse_sync_filter_json(filter_json));
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

extern "C" lattice_status_t lattice_db_clear_sync_filter(lattice_db_t* db) {
    if (!db) {
        set_error("Invalid arguments");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        db_ref->get()->clear_sync_filter();
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

// =============================================================================
// Cross-Process Observation
// =============================================================================
//
// These two functions were declared in lattice.h since 2026-03-27 but never
// implemented (docs/capi-gap-audit.md B-9 - "phantom symbols"). Cross-process
// changes are delivered by the core cross-process notifier: writes committed
// by other processes are detected by tailing AuditLog
// (lattice_db::handle_cross_process_notification) and dispatched through
// notify_changes_batched() - the SAME batched table-observer pipeline local
// flushes use. So a cross-process observer is a table observer registered on
// that shared dispatch path; it also fires for this handle's own writes
// (documented in lattice.h).
//
// lattice_db_remove_cross_process_observer() takes only {db, token} while the
// core unregister needs the table name, so keep a token->table registry keyed
// by {db handle, token} (tokens are only unique per db instance). Entries for
// observers never removed before the db is released are retired lazily when
// the same {db, token} key is reused; the strings involved are tiny.

namespace {
std::mutex g_xproc_observer_mutex;
std::map<std::pair<void*, uint64_t>, std::string> g_xproc_observer_tables;
}

extern "C" uint64_t lattice_db_observe_cross_process(
    lattice_db_t* db,
    const char* table_name,
    void* context,
    lattice_table_observer_fn callback
) {
    if (!db || !table_name || !callback) return 0;
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto cb = callback;
        auto ctx = context;
        uint64_t token = static_cast<lattice::lattice_db*>(db_ref->get())->add_table_observer(
            std::string(table_name),
            [cb, ctx](const std::vector<lattice::lattice_db::change_event>& events) {
                for (const auto& ev : events) {
                    const auto& op = std::get<1>(ev);
                    int64_t row_id = std::get<2>(ev);
                    const auto& global_id = std::get<3>(ev);
                    cb(ctx, op.c_str(), row_id, global_id.c_str());
                }
            });
        if (token != 0) {
            std::lock_guard<std::mutex> lock(g_xproc_observer_mutex);
            g_xproc_observer_tables[{db, token}] = table_name;
        }
        return token;
    } catch (...) {
        return 0;
    }
}

extern "C" void lattice_db_remove_cross_process_observer(lattice_db_t* db, uint64_t token) {
    if (!db || token == 0) return;
    std::string table;
    {
        std::lock_guard<std::mutex> lock(g_xproc_observer_mutex);
        auto it = g_xproc_observer_tables.find({db, token});
        if (it == g_xproc_observer_tables.end()) return;  // unknown token: no-op
        table = it->second;
        g_xproc_observer_tables.erase(it);
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        db_ref->get()->remove_table_observer(table, token);
    } catch (...) {}
}

// =============================================================================
// Sync Options (C1 slice 2)
// =============================================================================

extern "C" void lattice_sync_options_init(lattice_sync_options_t* options) {
    if (!options) return;
    std::memset(options, 0, sizeof(*options));
    options->struct_size = sizeof(lattice_sync_options_t);
    // "Keep the library default" sentinels — values the bridge setters ignore.
    options->chunk_size = 0;
    options->max_reconnect_attempts = -1;
    options->base_delay_seconds = 0.0;
    options->max_delay_seconds = 0.0;
    options->stable_connection_ms = -1;
    options->upload_coalesce_ms = -1;
    options->checkpoint_passive_interval_ms = -1;
    options->checkpoint_truncate_interval_ms = -1;
    options->use_upload_floor = -1;
    options->sync_filter_json = nullptr;
    options->sync_id = nullptr;
}

extern "C" lattice_db_t* lattice_db_create_with_sync_options(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler,
    const char* wss_endpoint,
    const char* authorization_token,
    const lattice_sync_options_t* options
) {
    try {
        // Size-prefixed copy: read min(caller_size, sizeof) bytes onto a
        // defaults-initialized local, so callers built against an older
        // (shorter) struct keep working as fields are appended
        // (docs/CAPI-STABILITY.md rule 3).
        lattice_sync_options_t opts;
        lattice_sync_options_init(&opts);
        if (options) {
            if (options->struct_size == 0) {
                set_error("lattice_sync_options_t.struct_size is 0 - "
                          "initialize the struct with lattice_sync_options_init()");
                return nullptr;
            }
            std::memcpy(&opts, options,
                        std::min(options->struct_size, sizeof(lattice_sync_options_t)));
        }
        if (opts.sync_id) {
            set_error("lattice_sync_options_t.sync_id is reserved and must be NULL "
                      "(sync ids are derived by the library; see lattice.h)");
            return nullptr;
        }

        std::string db_path = path ? std::string(path) : ":memory:";
        auto schema_vec = convert_schemas(schemas, schema_count);
        std::string ws_url = wss_endpoint ? std::string(wss_endpoint) : "";
        std::string auth_token = authorization_token ? std::string(authorization_token) : "";

        lattice::swift_configuration config(db_path, ws_url, auth_token);

        if (scheduler) {
            auto* sched = reinterpret_cast<lattice_scheduler_internal*>(scheduler);
            config.sched = std::shared_ptr<lattice::scheduler>(sched, [](lattice::scheduler*) {});
        }

        // Wire the knobs through configuration::sync_tuning via the bridge
        // setters, so the validation semantics are literally the bridge's:
        // nonsensical values are ignored and the knob keeps sync.hpp's
        // default (0 stays meaningful where it means "disabled").
        config.set_sync_chunk_size(opts.chunk_size);
        config.set_sync_max_reconnect_attempts(opts.max_reconnect_attempts);
        config.set_sync_base_delay_seconds(opts.base_delay_seconds);
        config.set_sync_max_delay_seconds(opts.max_delay_seconds);
        config.set_sync_stable_connection_ms(opts.stable_connection_ms);
        config.set_sync_upload_coalesce_ms(opts.upload_coalesce_ms);
        config.set_sync_checkpoint_passive_interval_ms(opts.checkpoint_passive_interval_ms);
        config.set_sync_checkpoint_truncate_interval_ms(opts.checkpoint_truncate_interval_ms);
        if (opts.use_upload_floor >= 0) {
            config.set_sync_use_upload_floor(opts.use_upload_floor != 0);
        }

        if (opts.sync_filter_json) {
            // Same JSON contract as lattice_db_set_sync_filter. NOTE: an
            // empty array is an explicit empty whitelist (sync nothing).
            config.sync_filter = parse_sync_filter_json(opts.sync_filter_json);
        }

        auto* ref = lattice::swift_lattice_ref::create(config, schema_vec);
        ref->retain();
        return reinterpret_cast<lattice_db_t*>(ref);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

// =============================================================================
// Sync Progress & Sync Observers (C1 slice 2)
// =============================================================================

extern "C" lattice_status_t lattice_db_get_sync_progress(lattice_db_t* db,
                                                         lattice_sync_progress_t* progress) {
    if (!db || !progress) {
        set_error("null argument");
        return LATTICE_ERROR_NULL_POINTER;
    }
    if (progress->struct_size == 0) {
        set_error("lattice_sync_progress_t.struct_size is 0 - "
                  "set it to sizeof(lattice_sync_progress_t) before calling");
        return LATTICE_ERROR_INVALID_ARGUMENT;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto p = db_ref->get()->get_sync_progress();

        lattice_sync_progress_t full;
        std::memset(&full, 0, sizeof(full));
        size_t written = std::min(progress->struct_size, sizeof(full));
        full.struct_size = written;
        full.pending_upload = p.pending_upload;
        full.total_upload = p.total_upload;
        full.acked = p.acked;
        full.received = p.received;
        std::memcpy(progress, &full, written);
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

// Multi-observer fan-out over the bridge's single sync-callback slot per
// kind. Registries are keyed by the underlying swift_lattice instance (NOT
// the C handle) so multiple handles sharing a cached instance share one
// registry and one installed trampoline — installing per-handle would
// silently clobber the other handle's observers at the bridge slot.
//
// Locking: entries_mutex (recursive) guards the token map and is held during
// dispatch, so removing an observer from another thread blocks until an
// in-flight callback completes — after lattice_db_remove_sync_observer
// returns, the context is never touched again and destroy(context) is safe.
// Removing from within the callback (same thread) re-enters via the
// recursive mutex; dispatch re-looks each token up so erased entries are
// skipped. install_mutex serializes install/uninstall of the bridge slots.
namespace {

struct capi_sync_observer_registry {
    enum class kind : int { progress = 0, state = 1, error = 2 };
    struct entry {
        kind k;
        void* context;
        void* fn;
        void (*destroy)(void*);
    };

    std::recursive_mutex entries_mutex;
    std::mutex install_mutex;
    uint64_t next_token = 1;
    std::map<uint64_t, entry> entries;

    size_t count_of(kind k) {
        size_t n = 0;
        for (const auto& [tok, e] : entries) {
            if (e.k == k) n++;
        }
        return n;
    }
};

std::mutex g_sync_registry_map_mutex;
// Keyed by swift_lattice*. An entry lives while it has observers (or until
// close); it is erased when its last observer is removed, which also bounds
// the stale-pointer window to caller-leaked observers (documented in
// lattice.h: remove observers or close before the final release).
std::map<const void*, std::shared_ptr<capi_sync_observer_registry>> g_sync_registries;

std::shared_ptr<capi_sync_observer_registry> sync_registry_for(
    lattice::swift_lattice* impl, bool create_if_missing) {
    std::lock_guard<std::mutex> lock(g_sync_registry_map_mutex);
    auto it = g_sync_registries.find(impl);
    if (it != g_sync_registries.end()) return it->second;
    if (!create_if_missing) return nullptr;
    auto reg = std::make_shared<capi_sync_observer_registry>();
    g_sync_registries.emplace(impl, reg);
    return reg;
}

void drop_sync_registry_if_empty(lattice::swift_lattice* impl,
                                 const std::shared_ptr<capi_sync_observer_registry>& reg) {
    std::lock_guard<std::mutex> map_lock(g_sync_registry_map_mutex);
    std::lock_guard<std::recursive_mutex> lock(reg->entries_mutex);
    if (reg->entries.empty()) {
        auto it = g_sync_registries.find(impl);
        if (it != g_sync_registries.end() && it->second == reg) {
            g_sync_registries.erase(it);
        }
    }
}

// Context handed to the bridge trampolines. Holds the registry weakly: if
// the registry is torn down (close) while the bridge still owns the
// trampoline, dispatch becomes a no-op instead of dangling.
struct sync_trampoline_ctx {
    std::weak_ptr<capi_sync_observer_registry> registry;
};

template <typename Invoke>
void dispatch_sync_observers(void* raw_ctx,
                             capi_sync_observer_registry::kind k,
                             Invoke&& invoke) {
    auto* t = static_cast<sync_trampoline_ctx*>(raw_ctx);
    if (!t) return;
    auto reg = t->registry.lock();
    if (!reg) return;
    std::lock_guard<std::recursive_mutex> lock(reg->entries_mutex);
    std::vector<uint64_t> tokens;
    tokens.reserve(reg->entries.size());
    for (const auto& [tok, e] : reg->entries) {
        if (e.k == k) tokens.push_back(tok);
    }
    for (uint64_t tok : tokens) {
        auto it = reg->entries.find(tok);
        if (it == reg->entries.end() || it->second.k != k) continue;  // removed mid-dispatch
        invoke(it->second);
    }
}

using sync_kind = capi_sync_observer_registry::kind;

// Install the bridge trampoline for `k` on `impl` (call with install_mutex held).
void install_sync_trampoline(lattice::swift_lattice* impl,
                             const std::shared_ptr<capi_sync_observer_registry>& reg,
                             sync_kind k) {
    switch (k) {
        case sync_kind::progress:
            impl->set_on_sync_progress(
                new sync_trampoline_ctx{reg},
                [](void* ctx, int64_t pending_upload, int64_t total_upload,
                   int64_t acked, int64_t received) {
                    lattice_sync_progress_t p;
                    std::memset(&p, 0, sizeof(p));
                    p.struct_size = sizeof(p);
                    p.pending_upload = pending_upload;
                    p.total_upload = total_upload;
                    p.acked = acked;
                    p.received = received;
                    dispatch_sync_observers(ctx, sync_kind::progress,
                        [&](const capi_sync_observer_registry::entry& e) {
                            reinterpret_cast<lattice_sync_progress_fn>(e.fn)(e.context, &p);
                        });
                },
                [](void* ctx) { delete static_cast<sync_trampoline_ctx*>(ctx); });
            break;
        case sync_kind::state:
            impl->set_on_sync_state_change(
                new sync_trampoline_ctx{reg},
                [](void* ctx, bool connected) {
                    dispatch_sync_observers(ctx, sync_kind::state,
                        [&](const capi_sync_observer_registry::entry& e) {
                            reinterpret_cast<lattice_sync_state_fn>(e.fn)(e.context, connected);
                        });
                },
                [](void* ctx) { delete static_cast<sync_trampoline_ctx*>(ctx); });
            break;
        case sync_kind::error:
            impl->set_on_sync_error(
                new sync_trampoline_ctx{reg},
                [](void* ctx, const char* error, int64_t /*len*/) {
                    dispatch_sync_observers(ctx, sync_kind::error,
                        [&](const capi_sync_observer_registry::entry& e) {
                            reinterpret_cast<lattice_sync_error_fn>(e.fn)(
                                e.context, error ? error : "");
                        });
                },
                [](void* ctx) { delete static_cast<sync_trampoline_ctx*>(ctx); });
            break;
    }
}

// Clear the bridge slot for `k` on `impl` (call with install_mutex held).
// The bridge's null-callback path releases the old trampoline context.
void clear_sync_trampoline(lattice::swift_lattice* impl, sync_kind k) {
    switch (k) {
        case sync_kind::progress:
            impl->set_on_sync_progress(nullptr, nullptr, nullptr);
            break;
        case sync_kind::state:
            impl->set_on_sync_state_change(nullptr, nullptr, nullptr);
            break;
        case sync_kind::error:
            impl->set_on_sync_error(nullptr, nullptr, nullptr);
            break;
    }
}

uint64_t observe_sync_common(lattice_db_t* db, sync_kind k, void* context,
                             void* fn, void (*destroy)(void*)) {
    if (!db || !fn) return 0;
    try {
        auto* impl = reinterpret_cast<lattice_db_internal*>(db)->get();
        auto reg = sync_registry_for(impl, /*create_if_missing=*/true);

        std::lock_guard<std::mutex> install_lock(reg->install_mutex);
        bool first_of_kind;
        uint64_t token;
        {
            std::lock_guard<std::recursive_mutex> lock(reg->entries_mutex);
            first_of_kind = reg->count_of(k) == 0;
            token = reg->next_token++;
            reg->entries[token] = capi_sync_observer_registry::entry{k, context, fn, destroy};
        }
        if (first_of_kind) {
            install_sync_trampoline(impl, reg, k);
        }
        return token;
    } catch (const std::exception& e) {
        set_error(e.what());
        return 0;
    } catch (...) {
        return 0;
    }
}

}  // namespace

static void capi_teardown_sync_observers(lattice::swift_lattice* impl) noexcept {
    std::shared_ptr<capi_sync_observer_registry> reg;
    {
        std::lock_guard<std::mutex> lock(g_sync_registry_map_mutex);
        auto it = g_sync_registries.find(impl);
        if (it == g_sync_registries.end()) return;
        reg = it->second;
        g_sync_registries.erase(it);
    }
    std::vector<capi_sync_observer_registry::entry> taken;
    {
        std::lock_guard<std::mutex> install_lock(reg->install_mutex);
        {
            std::lock_guard<std::recursive_mutex> lock(reg->entries_mutex);
            taken.reserve(reg->entries.size());
            for (const auto& [tok, e] : reg->entries) taken.push_back(e);
            reg->entries.clear();
        }
        try {
            clear_sync_trampoline(impl, sync_kind::progress);
            clear_sync_trampoline(impl, sync_kind::state);
            clear_sync_trampoline(impl, sync_kind::error);
        } catch (...) {}
    }
    for (const auto& e : taken) {
        if (e.destroy && e.context) {
            try { e.destroy(e.context); } catch (...) {}
        }
    }
}

extern "C" uint64_t lattice_db_observe_sync_progress(
    lattice_db_t* db,
    void* context,
    lattice_sync_progress_fn callback,
    void (*destroy)(void*)
) {
    return observe_sync_common(db, sync_kind::progress, context,
                               reinterpret_cast<void*>(callback), destroy);
}

extern "C" uint64_t lattice_db_observe_sync_state(
    lattice_db_t* db,
    void* context,
    lattice_sync_state_fn callback,
    void (*destroy)(void*)
) {
    return observe_sync_common(db, sync_kind::state, context,
                               reinterpret_cast<void*>(callback), destroy);
}

extern "C" uint64_t lattice_db_observe_sync_error(
    lattice_db_t* db,
    void* context,
    lattice_sync_error_fn callback,
    void (*destroy)(void*)
) {
    return observe_sync_common(db, sync_kind::error, context,
                               reinterpret_cast<void*>(callback), destroy);
}

extern "C" void lattice_db_remove_sync_observer(lattice_db_t* db, uint64_t token) {
    if (!db || token == 0) return;
    try {
        auto* impl = reinterpret_cast<lattice_db_internal*>(db)->get();
        auto reg = sync_registry_for(impl, /*create_if_missing=*/false);
        if (!reg) return;

        capi_sync_observer_registry::entry taken{};
        bool found = false;
        bool kind_now_empty = false;
        {
            std::lock_guard<std::mutex> install_lock(reg->install_mutex);
            {
                std::lock_guard<std::recursive_mutex> lock(reg->entries_mutex);
                auto it = reg->entries.find(token);
                if (it == reg->entries.end()) return;  // unknown token: no-op
                taken = it->second;
                found = true;
                reg->entries.erase(it);
                kind_now_empty = reg->count_of(taken.k) == 0;
            }
            if (kind_now_empty) {
                clear_sync_trampoline(impl, taken.k);
            }
        }
        if (found && taken.destroy && taken.context) {
            taken.destroy(taken.context);
        }
        drop_sync_registry_if_empty(impl, reg);
    } catch (...) {}
}

// =============================================================================
// Database Detachment & Attach Errors (C1 slice 2; audit A-29/A-30)
// =============================================================================

extern "C" lattice_status_t lattice_db_detach(lattice_db_t* db, lattice_db_t* other) {
    if (!db || !other) {
        set_error("Invalid arguments to lattice_db_detach");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        auto* db_ref = reinterpret_cast<lattice_db_internal*>(db);
        auto* other_ref = reinterpret_cast<lattice_db_internal*>(other);
        // Same contract as attach since ATT-3: bool return + reason in
        // last_attach_error(); idempotent (detaching a non-attached db is
        // success), never throws across the boundary.
        if (!db_ref->get()->detach(*other_ref->get())) {
            auto reason = db_ref->get()->last_attach_error();
            set_error(reason ? reason->c_str() : "detach failed");
            return LATTICE_ERROR_DATABASE;
        }
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

extern "C" const char* lattice_db_last_attach_error(lattice_db_t* db) {
    if (!db) return nullptr;
    try {
        auto err = reinterpret_cast<lattice_db_internal*>(db)->get()->last_attach_error();
        if (!err) return nullptr;
        g_returned_string = *err;  // thread-safe copy (core accessor is mutex-guarded)
        return g_returned_string.c_str();
    } catch (...) {
        return nullptr;
    }
}

// =============================================================================
// Row Cache & Atomic Field Increment (C1 slice 2; audit A-8/A-9)
// =============================================================================

extern "C" lattice_status_t lattice_object_enable_row_cache(lattice_object_t* obj) {
    if (!obj) {
        set_error("null argument");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        reinterpret_cast<lattice_object_internal*>(obj)->enable_row_cache();
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

extern "C" lattice_status_t lattice_object_disable_row_cache(lattice_object_t* obj) {
    if (!obj) {
        set_error("null argument");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        reinterpret_cast<lattice_object_internal*>(obj)->disable_row_cache();
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

extern "C" lattice_status_t lattice_object_refresh_row_cache(lattice_object_t* obj) {
    if (!obj) {
        set_error("null argument");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        reinterpret_cast<lattice_object_internal*>(obj)->refresh_row_cache();
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

extern "C" bool lattice_object_is_row_cache_enabled(lattice_object_t* obj) {
    if (!obj) return false;
    try {
        return reinterpret_cast<lattice_object_internal*>(obj)->is_row_cache_enabled();
    } catch (...) {
        return false;
    }
}

extern "C" lattice_status_t lattice_object_increment_int(lattice_object_t* obj,
                                                         const char* field,
                                                         int64_t delta) {
    if (!obj || !field) {
        set_error("null argument");
        return LATTICE_ERROR_NULL_POINTER;
    }
    try {
        reinterpret_cast<lattice_object_internal*>(obj)->increment_int_field(field, delta);
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

// =============================================================================
// Statement Counters (C1 slice 2; audit A-10)
// =============================================================================

extern "C" uint64_t lattice_db_total_statement_count(void) {
    return lattice::swift_lattice::total_sql_statement_count();
}

extern "C" uint64_t lattice_db_thread_statement_count(void) {
    return lattice::swift_lattice::thread_sql_statement_count();
}

// =============================================================================
// WAL Checkpoint (C1 slice 2; audit A-15)
// =============================================================================

extern "C" lattice_status_t lattice_db_checkpoint(lattice_db_t* db, int32_t mode) {
    if (!db) {
        set_error("null argument");
        return LATTICE_ERROR_NULL_POINTER;
    }
    if (mode != 0 && mode != 1) {
        set_error("invalid checkpoint mode (0 = passive, 1 = truncate)");
        return LATTICE_ERROR_INVALID_ARGUMENT;
    }
    try {
        auto* impl = reinterpret_cast<lattice_db_internal*>(db)->get();
        if (mode == 1) {
            // TRUNCATE via the bridge member — logged outcome (partial
            // checkpoints under concurrent readers are how multi-GB WAL
            // files accumulate silently).
            impl->checkpoint();
        } else {
            // PASSIVE: checkpoint what can be done without blocking
            // readers/writers. PRAGMA for the same reason the bridge uses it
            // (sqlite3ext.h macro shadowing on Linux TUs).
            impl->db().query("PRAGMA wal_checkpoint(PASSIVE)", {});
        }
        return LATTICE_OK;
    } catch (const std::exception& e) {
        set_error(e.what());
        return LATTICE_ERROR_DATABASE;
    }
}

// =============================================================================
// Sync Upload Floor (C1 slice 2; audit A-32)
// =============================================================================

extern "C" int64_t lattice_db_read_upload_floor(lattice_db_t* db, const char* sync_id) {
    if (!db || !sync_id) {
        set_error("null argument");
        return -1;
    }
    try {
        auto* impl = reinterpret_cast<lattice_db_internal*>(db)->get();
        return lattice::read_upload_floor(impl->db(), std::string(sync_id));
    } catch (const std::exception& e) {
        set_error(e.what());
        return -1;
    }
}

// =============================================================================
// Object-graph -> JSON (C1 slice 3; audit A-11)
// =============================================================================

extern "C" char* lattice_object_to_json(lattice_object_t* obj, int32_t max_depth) {
    if (!obj) {
        set_error("null argument");
        return nullptr;
    }
    try {
        auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
        // Contract pinned to Swift's DynamicObject.jsonObject — implemented
        // once in the bridge (dynamic_object::to_json) so every consumer
        // (Swift, C, Kotlin/Python via this ABI) shares the walker.
        const std::string json = ref->to_json(max_depth);
        char* ret = static_cast<char*>(malloc(json.size() + 1));
        if (ret) {
            std::memcpy(ret, json.c_str(), json.size() + 1);
        }
        return ret;
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    } catch (...) {
        set_error("unknown error in lattice_object_to_json");
        return nullptr;
    }
}
