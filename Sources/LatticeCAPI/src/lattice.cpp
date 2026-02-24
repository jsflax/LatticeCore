#include "lattice.h"
#include <lattice.hpp>
#include <dynamic_object.hpp>
#include <list.hpp>
#include <lattice/sync.hpp>
#include <lattice/network.hpp>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <map>
#include <cstring>
#include <cstdlib>

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

// =============================================================================
// Error Handling
// =============================================================================

extern "C" const char* lattice_last_error(void) {
    return g_last_error.empty() ? nullptr : g_last_error.c_str();
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
        return ref->get_int("id");
    } catch (...) {
        return 0;
    }
}

extern "C" const char* lattice_object_get_global_id(lattice_object_t* obj) {
    if (!obj) return nullptr;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        g_returned_string = ref->get_string("globalId");
        return g_returned_string.c_str();
    } catch (...) {
        return nullptr;
    }
}

extern "C" const char* lattice_object_get_table_name(lattice_object_t* obj) {
    if (!obj) return nullptr;
    auto* ref = reinterpret_cast<lattice_object_internal*>(obj);
    try {
        // Access underlying dynamic_object to get table name
        auto* dyn = ref->get();
        if (dyn->lattice) {
            // Managed object - need to get from managed_
            g_returned_string = ref->get()->debug_description();  // TODO: expose table_name properly
        } else {
            // Unmanaged - get directly from unmanaged_.table_name
            // This requires friend access or a public method
        }
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

        bool success = db_ref->get()->remove(obj_ref);
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
#ifdef __BLOCKS__
        return db_ref->get()->add_table_observer(std::string(table_name), context,
            ^(void* ctx, const std::string& op, int64_t row_id, const std::string& gid) {
                callback(ctx, op.c_str(), row_id, gid.c_str());
            });
#else
        return db_ref->get()->add_table_observer(std::string(table_name), context,
            [callback](void* ctx, const std::string& op, int64_t row_id, const std::string& gid) {
                callback(ctx, op.c_str(), row_id, gid.c_str());
            });
#endif
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
        // Cast to base class to avoid name hiding from swift_lattice's block-based overload
        return static_cast<lattice::lattice_db*>(db_ref->get())->add_object_observer(std::string(table_name), row_id,
            [context, callback](const std::string&) { callback(context); });
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
        list_ref->push_back(obj_ref);
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

        // Parse the JSON data as ServerSentEvent
        std::string json_str(reinterpret_cast<const char*>(data), data_size);
        auto event = lattice::server_sent_event::from_json(json_str);

        if (!event) {
            set_error("Failed to parse sync data");
            return nullptr;
        }

        std::vector<std::string> applied_ids;

        if (event->event_type == lattice::server_sent_event::type::audit_log) {
            // Apply each audit log entry
            for (const auto& entry : event->audit_logs) {
                // Get schema for table
                const auto* props = db_ref->get()->get_properties_for_table(entry.table_name);
                std::unordered_map<std::string, lattice::column_type> schema;
                if (props) {
                    for (const auto& [name, desc] : *props) {
                        schema[name] = desc.type;
                    }
                }

                // Generate and execute SQL
                auto [sql, params] = entry.generate_instruction(schema);
                db_ref->get()->db().execute(sql, params);

                applied_ids.push_back(entry.global_id);
            }
        } else if (event->event_type == lattice::server_sent_event::type::ack) {
            // Mark entries as synchronized
            lattice::mark_audit_entries_synced(*db_ref->get(), event->acked_ids);
            applied_ids = event->acked_ids;
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
struct lattice_websocket_client_internal : public lattice::websocket_client {
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

    lattice::websocket_state state() const override {
        if (state_fn_) {
            auto s = state_fn_(const_cast<void*>(user_data_));
            return static_cast<lattice::websocket_state>(s);
        }
        return lattice::websocket_state::closed;
    }

    void send(const lattice::websocket_message& message) override {
        if (send_fn_) {
            auto type = (message.msg_type == lattice::websocket_message::type::text)
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
            lattice::websocket_message msg;
            msg.msg_type = (type == LATTICE_WS_MSG_TEXT)
                ? lattice::websocket_message::type::text
                : lattice::websocket_message::type::binary;
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

    std::unique_ptr<lattice::websocket_client> create_websocket_client() override {
        if (create_ws_fn_) {
            auto* c_client = create_ws_fn_(user_data_);
            if (c_client) {
                // The C API returns a raw pointer, wrap it in unique_ptr
                auto* internal = reinterpret_cast<lattice_websocket_client_internal*>(c_client);
                return std::unique_ptr<lattice::websocket_client>(internal);
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
