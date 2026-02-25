#pragma once

#ifdef __cplusplus

#include <LatticeCore.hpp>
#include <bridging.hpp>
#include <concepts>

#include <dynamic_object.hpp>
#include <list.hpp>

#if defined(__BLOCKS__) && defined(__APPLE__) && !defined(__swift__)
#include <Block.h>
#endif

// Forward declarations
namespace lattice {
class swift_lattice_ref;
class swift_migration_context_ref;
}

// Global retain/release functions for swift_lattice_ref
void retainSwiftLatticeRef(lattice::swift_lattice_ref*);
void releaseSwiftLatticeRef(lattice::swift_lattice_ref*);

// Global retain/release functions for swift_migration_context_ref
void retainSwiftMigrationContextRef(lattice::swift_migration_context_ref*);
void releaseSwiftMigrationContextRef(lattice::swift_migration_context_ref*);

// Log level control
void lattice_set_log_level(lattice::log_level level);
lattice::log_level lattice_get_log_level();

namespace lattice {

// ============================================================================
// swift_dynamic_object - A type-erased model for Swift interop
// Swift models map to this, with schema/properties set dynamically
// ============================================================================

using SwiftSchema = std::unordered_map<std::string, property_descriptor>;
using OptionalString = std::optional<std::string>;
using OptionalInt64 = std::optional<int64_t>;
using ByteVector = std::vector<uint8_t>;

// Constraint definition for Swift interop
struct swift_constraint {
    std::vector<std::string> columns;
    bool allows_upsert = false;

    swift_constraint() = default;
    swift_constraint(const std::vector<std::string>& cols, bool upsert = false)
        : columns(cols), allows_upsert(upsert) {}
};

using ConstraintVector = std::vector<swift_constraint>;

// Schema entry for passing from Swift: table_name + properties + constraints
struct swift_schema_entry {
    std::string table_name;
    SwiftSchema properties;
    ConstraintVector constraints;

    swift_schema_entry() = default;
    swift_schema_entry(const std::string& name, const SwiftSchema& props)
        : table_name(name), properties(props), constraints() {}
    swift_schema_entry(const std::string& name, const SwiftSchema& props, const ConstraintVector& constrs)
        : table_name(name), properties(props), constraints(constrs) {}
};

// Vector of schemas to pass from Swift at init time
using SchemaVector = std::vector<swift_schema_entry>;

// ============================================================================
// Combined Query Constraints - For multi-constraint nearest searches
// ============================================================================

/// Bounding box constraint for R*Tree spatial filtering
struct bounds_constraint {
    std::string column;
    double min_lat, max_lat, min_lon, max_lon;

    bounds_constraint() = default;
    bounds_constraint(const std::string& col, double minLat, double maxLat, double minLon, double maxLon)
        : column(col), min_lat(minLat), max_lat(maxLat), min_lon(minLon), max_lon(maxLon) {}
};

/// Vector similarity constraint for vec0 k-NN search
struct vector_constraint {
    std::string column;
    ByteVector query_vector;
    int k;
    int metric;  // 0=L2, 1=cosine, 2=L1

    vector_constraint() = default;
    vector_constraint(const std::string& col, const ByteVector& vec, int k_val, int met)
        : column(col), query_vector(vec), k(k_val), metric(met) {}
};

/// Geographic proximity constraint for R*Tree + Haversine filtering
struct geo_constraint {
    std::string column;
    double center_lat, center_lon;
    double radius_meters;

    geo_constraint() = default;
    geo_constraint(const std::string& col, double lat, double lon, double radius)
        : column(col), center_lat(lat), center_lon(lon), radius_meters(radius) {}
};

/// Full-text search constraint for FTS5
struct text_constraint {
    std::string column;
    std::string search_text;
    int limit;

    text_constraint() = default;
    text_constraint(const std::string& col, const std::string& text, int lim)
        : column(col), search_text(text), limit(lim) {}
};

/// Sort descriptor for nearest results
struct sort_descriptor {
    enum class Type : int32_t {
        none = 0,
        geo_distance = 1,    // Sort by geo distance column
        vector_distance = 2, // Sort by vector distance column
        property = 3,        // Sort by a property column
        text_rank = 4        // Sort by FTS5 rank column
    };

    Type type = Type::none;
    std::string column;      // Column name for distance or property
    bool ascending = true;   // true = ASC, false = DESC

    sort_descriptor() = default;
    sort_descriptor(Type t, const std::string& col, bool asc)
        : type(t), column(col), ascending(asc) {}
};

using BoundsConstraintVector = std::vector<bounds_constraint>;
using VectorConstraintVector = std::vector<vector_constraint>;
using GeoConstraintVector = std::vector<geo_constraint>;
using TextConstraintVector = std::vector<text_constraint>;

/// Distance entry for combined query result
struct distance_entry {
    std::string column;
    double distance;

    distance_entry() = default;
    distance_entry(const std::string& col, double dist) : column(col), distance(dist) {}
};

using DistanceEntryVector = std::vector<distance_entry>;

/// Result from combined nearest query with multiple distances
struct combined_query_result {
    managed<swift_dynamic_object> object;
    DistanceEntryVector distances;  // vector of (column, distance) pairs

    combined_query_result() = default;
    combined_query_result(managed<swift_dynamic_object> obj, DistanceEntryVector dists)
        : object(std::move(obj)), distances(std::move(dists)) {}
};

using CombinedQueryResultVector = std::vector<combined_query_result>;

// ============================================================================
// column_value_t helper functions for Swift interop
// These provide clean Swift APIs for working with variant values
// ============================================================================

/// Create column_value_t from double
inline column_value_t column_value_from_double(double v) {
    return v;
}

/// Create column_value_t from int64
inline column_value_t column_value_from_int(int64_t v) {
    return v;
}

/// Create column_value_t from string
inline column_value_t column_value_from_string(const std::string& v) {
    return v;
}

/// Create column_value_t from blob
inline column_value_t column_value_from_blob(const std::vector<uint8_t>& v) {
    return v;
}

/// Check if column_value_t is null (nullptr_t)
inline bool column_value_is_null(const column_value_t& v) {
    return std::holds_alternative<std::nullptr_t>(v);
}

/// Get double from column_value_t (returns nullopt if wrong type)
inline std::optional<double> column_value_as_double(const column_value_t& v) {
    if (std::holds_alternative<double>(v)) {
        return std::get<double>(v);
    }
    if (std::holds_alternative<int64_t>(v)) {
        return static_cast<double>(std::get<int64_t>(v));
    }
    return std::nullopt;
}

/// Get int64 from column_value_t (returns nullopt if wrong type)
inline std::optional<int64_t> column_value_as_int(const column_value_t& v) {
    if (std::holds_alternative<int64_t>(v)) {
        return std::get<int64_t>(v);
    }
    if (std::holds_alternative<double>(v)) {
        return static_cast<int64_t>(std::get<double>(v));
    }
    return std::nullopt;
}

/// Get string from column_value_t (returns nullopt if wrong type)
inline std::optional<std::string> column_value_as_string(const column_value_t& v) {
    if (std::holds_alternative<std::string>(v)) {
        return std::get<std::string>(v);
    }
    return std::nullopt;
}

/// Get blob from column_value_t (returns nullopt if wrong type)
inline std::optional<std::vector<uint8_t>> column_value_as_blob(const column_value_t& v) {
    if (std::holds_alternative<std::vector<uint8_t>>(v)) {
        return std::get<std::vector<uint8_t>>(v);
    }
    return std::nullopt;
}

// ============================================================================
// Row Migration Callback - For Swift-driven per-row migration
// (Defined early so swift_configuration can use it)
// ============================================================================

/// Swift-specific configuration that extends the core configuration.
struct swift_configuration : public configuration {
    struct SchemaPair { SwiftSchema from, to; };

    // Pre-populate migration schema pairs (eliminates block callback).
    // Called from Swift for each table+version that has a schema change.
    void addMigrationSchema(size_t version, const std::string& table_name,
                           const SwiftSchema& from, const SwiftSchema& to) {
        migration_schemas_[version][table_name] = SchemaPair{from, to};
    }

    // C function pointer callback for row migration (Swift-visible).
    // Follows the same pattern as generic_scheduler: void* context + C fn pointers.
    // During the callback, call migration_get_old_row() / migration_get_new_row()
    // to access the current row refs.
    void setRowMigrationCallback(
        void* context,
        void (*callback)(const char* table_name, void* ctx),
        void (*destroy)(void*) = nullptr
    ) {
        auto shared_ctx = std::shared_ptr<void>(context, destroy ? destroy : [](void*){});
        auto cb = callback;
        row_migration_fn_ = [shared_ctx, cb](const std::string& table_name,
                                              dynamic_object_ref*,
                                              dynamic_object_ref*) {
            cb(table_name.c_str(), shared_ctx.get());
        };
    }

#if defined(__BLOCKS__) && !defined(__swift__)
    // Block-based setters (C++ callers on Apple only — hidden from Swift)
    void setGetOldAndNewSchemaForVersionBlock(
        std::optional<SchemaPair> (^block)(const std::string& table_name, size_t version_number)
    ) {
        get_schema_pair_fn_ = [block](const std::string& table_name, size_t version_number) {
            return block(table_name, version_number);
        };
    }

    void setRowMigrationBlock(
        void (^block)(const std::string& table_name,
                      dynamic_object_ref* old_row,
                      dynamic_object_ref* new_row)
    ) SWIFT_COMPUTED_PROPERTY {
        row_migration_fn_ = [block](const std::string& table_name,
                                     dynamic_object_ref* old_row,
                                     dynamic_object_ref* new_row) {
            block(table_name, old_row, new_row);
        };
    }
#endif

    // Internal schema lookup (used by C++ migration code)
    std::optional<SchemaPair> lookupMigrationSchema(const std::string& table_name, size_t version) const {
        // Check pre-populated data first
        auto vit = migration_schemas_.find(version);
        if (vit != migration_schemas_.end()) {
            auto tit = vit->second.find(table_name);
            if (tit != vit->second.end()) {
                return tit->second;
            }
        }
        // Fall back to callback (C++ callers)
        if (get_schema_pair_fn_) {
            return get_schema_pair_fn_(table_name, version);
        }
        return std::nullopt;
    }

    // Default - in-memory
    swift_configuration() = default;

    // Path only
    explicit swift_configuration(const std::string& p) : configuration(p){}

    // Path + scheduler
    swift_configuration(const std::string& p, std::shared_ptr<lattice::scheduler> s)
        : configuration(p, std::move(s)) {}

    // Full configuration with sync
    swift_configuration(const std::string& p,
                       const std::string& ws_url,
                       const std::string& auth_token,
                       std::shared_ptr<lattice::scheduler> s = nullptr)
        : configuration(p, ws_url, auth_token, std::move(s)) {}

    // From base configuration
    swift_configuration(const configuration& base) : configuration(base) {}

private:
    std::function<void(const std::string& table_name,
                       dynamic_object_ref* old_row,
                       dynamic_object_ref* new_row)> row_migration_fn_;
    std::function<std::optional<SchemaPair>(const std::string& table_name,
                                            size_t version_number)> get_schema_pair_fn_;
    std::unordered_map<size_t, std::unordered_map<std::string, SchemaPair>> migration_schemas_;

    friend class swift_lattice;
};

// Internal implementation - inherits from lattice_db
class swift_lattice : public lattice_db {
public:
    // Construct with full configuration (including optional sync)
    explicit swift_lattice(swift_configuration&& config)
        : lattice_db(config),
    swift_config_(std::move(config))
    {
    }
    
    // Construct with swift_configuration (includes row migration callback)
    swift_lattice(const swift_configuration& config, const SchemaVector& schemas);
    swift_lattice(swift_configuration&& config, const SchemaVector& schemas);

    // Get properties for a table (used when hydrating objects)
    const SwiftSchema* get_properties_for_table(const std::string& table_name) const {
        auto it = schemas_.find(table_name);
        if (it != schemas_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    // Get constraints for a table (used for upsert logic)
    const ConstraintVector* get_constraints_for_table(const std::string& table_name) const {
        auto it = constraints_.find(table_name);
        if (it != constraints_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    // Check if any constraint allows upsert for a table
    bool has_upsert_constraint(const std::string& table_name) const {
        auto* constraints = get_constraints_for_table(table_name);
        if (!constraints) return false;
        for (const auto& c : *constraints) {
            if (c.allows_upsert) return true;
        }
        return false;
    }

    // Get columns for upsert ON CONFLICT clause
    std::vector<std::string> get_upsert_columns(const std::string& table_name) const {
        auto* constraints = get_constraints_for_table(table_name);
        if (!constraints) return {};
        for (const auto& c : *constraints) {
            if (c.allows_upsert) return c.columns;
        }
        return {};
    }

    std::string path_copy() const {
        return swift_config_.path;
    }
    
private:
    // Stored schemas for hydration
    std::unordered_map<std::string, SwiftSchema> schemas_;
    // Stored constraints per table
    std::unordered_map<std::string, ConstraintVector> constraints_;
    // Swift-specific configuration (stores row migration callback)
    swift_configuration swift_config_;

    void ensure_swift_tables(const SchemaVector& schemas);

public:
    // Add with schema from the object instance
    void add(dynamic_object& obj);
    void add(dynamic_object_ref* ref) {
        add(*ref->impl_);
    }

    void add_bulk(std::vector<dynamic_object>& objects);
    void add_bulk(std::vector<dynamic_object*>& objects);
    void add_bulk(std::vector<dynamic_object_ref*>& objects);
    
    // Bulk add with schema from the first object instance
    std::vector<managed<swift_dynamic_object>> add_bulk(std::vector<swift_dynamic_object>&& objects) {
        if (objects.empty()) {
            return {};
        }
        // Use schema from first object (all objects in the batch should have the same schema)
        auto schema = objects[0].instance_schema();
        return lattice_db::add_bulk_with_schema(std::move(objects), schema);
    }

    // Remove using the object's table name
    bool remove(dynamic_object&& obj);
    bool remove(dynamic_object_ref* obj);
    
    bool remove(managed<swift_dynamic_object>& obj) {
        if (!obj.is_valid()) return false;
        lattice_db::remove(obj, obj.table_name());
        return true;
    }
    
    std::optional<managed<swift_dynamic_object>> object(int64_t primary_key, const std::string& table_name);

    std::optional<managed<swift_dynamic_object>> object_by_global_id(const std::string& global_id, const std::string& table_name) {
        auto result = lattice_db::find_by_global_id<swift_dynamic_object>(global_id, table_name);
        if (result) {
            if (auto* props = get_properties_for_table(table_name)) {
                result->properties_ = *props;
                result->source.properties = *props;
            }
        }
        return result;
    }

    /// Get all objects from a table, optionally filtered and sorted
    std::vector<managed<swift_dynamic_object>> objects(
        const std::string& table_name,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt) {

        auto rows = query_rows(table_name, where_clause, order_by, limit, offset, group_by);
        std::vector<managed<swift_dynamic_object>> results;
        results.reserve(rows.size());

        // Get properties for this table
        const SwiftSchema* props = get_properties_for_table(table_name);

        for (const auto& row : rows) {
            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            // Populate properties_ from stored schema
            if (props) {
                obj.properties_ = *props;
                obj.source.properties = *props;
            }
            results.push_back(std::move(obj));
        }
        return results;
    }

    std::vector<managed<swift_dynamic_object>> union_objects(
        const std::vector<std::string>& table_names,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt) {

        auto rows = query_union_rows(table_names, where_clause, order_by, limit, offset);
        std::vector<managed<swift_dynamic_object>> results;
        results.reserve(rows.size());
        // TODO: need to get type from rows
        // Get properties for this table
        
        for (const auto& row : rows) {
            auto type_it = row.find("_type");
            if (type_it == row.end()) {
                LOG_ERROR("swift_lattice", "Bad query: row missing '_type' column");
                throw std::runtime_error("Bad query");
            }
            auto table_name = std::get<std::string>(type_it->second);
            const SwiftSchema* props = get_properties_for_table(table_name);
            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            // Populate properties_ from stored schema
            if (props) {
                obj.properties_ = *props;
                obj.source.properties = *props;
            }
            results.push_back(std::move(obj));
        }
        return results;
    }
    
    size_t count(const std::string& table_name,
                 OptionalString where_clause,
                 OptionalString group_by) {
        return lattice_db::count(table_name, where_clause, group_by);
    }

    size_t count(const std::string& table_name,
                 OptionalString where_clause) {
        return lattice_db::count(table_name, where_clause, std::nullopt);
    }

    size_t count(const std::string& table_name) {
        return lattice_db::count(table_name, std::nullopt, std::nullopt);
    }

    bool delete_where(const std::string& table_name, std::optional<std::string> where_clause) {
        return lattice_db::delete_where(table_name, where_clause);
    }

    bool delete_where(const std::string& table_name) {
        return lattice_db::delete_where(table_name, std::nullopt);
    }

    int64_t compact_audit_log() {
        return lattice_db::compact_audit_log();
    }

    int64_t generate_history() {
        return lattice_db::generate_history();
    }

    /// Checkpoint the WAL file, flushing all changes to the main database file.
    void checkpoint() {
        sqlite3_wal_checkpoint_v2(db().handle(), nullptr, SQLITE_CHECKPOINT_TRUNCATE, nullptr, nullptr);
    }

    /// Explicitly close all database connections and stop background services.
    /// Call before deleting database files to avoid "vnode unlinked while in use".
    void close() { lattice_db::close(); }
    bool is_sync_connected() const { return lattice_db::is_sync_connected(); }

    /// Rebuild the database file, reclaiming unused space.
    /// Closes the read connection before vacuuming and reopens it after.
    void vacuum() {
        close_read_db();
        try {
            db().execute("VACUUM");
        } catch (...) {
            reopen_read_db();
            throw;
        }
        reopen_read_db();
    }

    const std::string& path() const { return config().path; }

    void begin_transaction() { lattice_db::begin_transaction(); }
    void commit() { lattice_db::commit(); }
    void write(void (*fn)()) {
        begin_transaction();
        fn();
        commit();
    }
    
    void attach(swift_lattice& lattice);

    // ========================================================================
    // MARK: Observation API
    // ========================================================================

    // C function pointer observers (Swift-visible).
    // Same pattern as generic_scheduler: void* context + C fn pointers.
    uint64_t add_table_observer(const std::string& table_name,
                                 void* context,
                                 void (*callback)(void* ctx,
                                                  const char* operation,
                                                  int64_t row_id,
                                                  const char* global_row_id),
                                 void (*destroy)(void*) = nullptr) {
        auto shared_ctx = std::shared_ptr<void>(context, destroy ? destroy : [](void*){});
        auto cb = callback;
        return lattice_db::add_table_observer(table_name,
            [shared_ctx, cb](const std::string& op, int64_t row_id, const std::string& global_id) {
                cb(shared_ctx.get(), op.c_str(), row_id, global_id.c_str());
            });
    }

    uint64_t add_object_observer(const std::string& table_name,
                                  int64_t row_id,
                                  void* context,
                                  void (*callback)(const char* changed_field_names, void* ctx),
                                  void (*destroy)(void*) = nullptr) {
        auto shared_ctx = std::shared_ptr<void>(context, destroy ? destroy : [](void*){});
        auto cb = callback;
        return lattice_db::add_object_observer(table_name, row_id,
            [shared_ctx, cb](const std::string& changed_fields_names) {
                cb(changed_fields_names.c_str(), shared_ctx.get());
            });
    }

#if defined(__BLOCKS__) && !defined(__swift__)
    // Block-based observers (C++ callers on Apple only — hidden from Swift)
    using swift_table_observer_callback = void (^)(void* context,
                                                   const std::string& operation,
                                                   int64_t row_id,
                                                   const std::string& global_row_id);

    uint64_t add_table_observer(const std::string& table_name,
                                 void* context,
                                 swift_table_observer_callback callback) {
        return lattice_db::add_table_observer(table_name,
            [context, callback](const std::string& op, int64_t row_id, const std::string& global_id) {
                callback(context, op, row_id, global_id);
            });
    }

    using swift_object_observer_callback = void (^)(const std::string& changed_fields_names);

    uint64_t add_object_observer(const std::string& table_name,
                                  int64_t row_id,
                                  swift_object_observer_callback callback) {
        return lattice_db::add_object_observer(table_name, row_id,
            [callback](const std::string& changed_fields_names) {
                callback(changed_fields_names);
            });
    }
#endif

    void remove_table_observer(const std::string& table_name, uint64_t observer_id) {
        lattice_db::remove_table_observer(table_name, observer_id);
    }

    void remove_object_observer(const std::string& table_name, int64_t row_id, uint64_t observer_id) {
        lattice_db::remove_object_observer(table_name, row_id, observer_id);
    }

    void remove_all_object_observers(const std::string& table_name, int64_t row_id) {
        lattice_db::remove_all_object_observers(table_name, row_id);
    }

    // ========================================================================
    // MARK: Swift-compatible Vector Search API
    // ========================================================================

    /// Update or insert a vector into the vec0 table for a model's vector column
    void upsert_vector(const std::string& table_name,
                       const std::string& column_name,
                       const std::string& global_id,
                       const std::vector<uint8_t>& vector_data) {
        lattice_db::upsert_vec0(table_name, column_name, global_id, vector_data);
    }

    /// Delete a vector from the vec0 table
    void delete_vector(const std::string& table_name,
                       const std::string& column_name,
                       const std::string& global_id) {
        lattice_db::delete_vec0(table_name, column_name, global_id);
    }

    /// Perform a KNN query and return matching objects with distances
    /// metric: 0 = L2 (default), 1 = cosine, 2 = L1
    /// where_clause: optional filter on main model table (e.g., "category = 'foo'")
    std::vector<std::pair<managed<swift_dynamic_object>, double>> nearest_neighbors(
        const std::string& table_name,
        const std::string& column_name,
        const std::vector<uint8_t>& query_vector,
        int k,
        int metric = 0,
        const std::optional<std::string>& where_clause = std::nullopt) {

        auto dm = static_cast<distance_metric>(metric);
        auto knn_results = lattice_db::knn_query(table_name, column_name, query_vector, k, dm, where_clause);

        std::vector<std::pair<managed<swift_dynamic_object>, double>> results;
        results.reserve(knn_results.size());

        for (const auto& r : knn_results) {
            auto obj = object_by_global_id(r.global_id, table_name);
            if (obj) {
                results.push_back({std::move(*obj), r.distance});
            }
        }
        return results;
    }

    /// Perform a KNN query and return just globalIds with distances (lighter weight)
    std::vector<knn_result> nearest_neighbors_ids(
        const std::string& table_name,
        const std::string& column_name,
        const std::vector<uint8_t>& query_vector,
        int k,
        int metric = 0) {

        auto dm = static_cast<distance_metric>(metric);
        return lattice_db::knn_query(table_name, column_name, query_vector, k, dm);
    }
    
    // ========================================================================
    // Spatial Query API - geo_bounds with R*Tree
    // ========================================================================

    /// Query objects within a bounding box using R*Tree spatial index
    /// Returns objects where the geo_bounds property intersects the query bbox
    std::vector<managed<swift_dynamic_object>> objects_within_bbox(
        const std::string& table_name,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt)
        SWIFT_NAME(objectsWithinBBox(table:geoColumn:minLat:maxLat:minLon:maxLon:where:orderBy:limit:offset:groupBy:)) {

        // Build spatial query SQL using R*Tree
        std::string list_table = "_" + table_name + "_" + geo_column;
        std::string list_rtree = list_table + "_rtree";
        std::string single_rtree = "_" + table_name + "_" + geo_column + "_rtree";

        // Check if this is a geo_bounds list (separate table) or single geo_bounds (inline columns)
        bool is_list = db().table_exists(list_table);

        std::string sql;
        if (is_list) {
            // Geo bounds list: join through list table's R*Tree
            sql = "SELECT DISTINCT " + table_name + ".* FROM " + table_name +
                  " JOIN " + list_table + " lt ON " + table_name + ".globalId = lt.parent_id" +
                  " JOIN " + list_rtree + " r ON lt.id = r.id" +
                  " WHERE r.minLat <= " + std::to_string(max_lat) +
                  " AND r.maxLat >= " + std::to_string(min_lat) +
                  " AND r.minLon <= " + std::to_string(max_lon) +
                  " AND r.maxLon >= " + std::to_string(min_lon);
        } else {
            // Single geo_bounds: join main table's R*Tree directly
            sql = "SELECT " + table_name + ".* FROM " + table_name +
                  " JOIN " + single_rtree + " r ON " + table_name + ".id = r.id" +
                  " WHERE r.minLat <= " + std::to_string(max_lat) +
                  " AND r.maxLat >= " + std::to_string(min_lat) +
                  " AND r.minLon <= " + std::to_string(max_lon) +
                  " AND r.maxLon >= " + std::to_string(min_lon);
        }

        // Add additional where clause if provided
        if (where_clause.has_value() && !where_clause.value().empty()) {
            sql += " AND (" + where_clause.value() + ")";
        }

        // Add group by if provided
        if (group_by.has_value() && !group_by.value().empty()) {
            sql += " GROUP BY " + group_by.value();
        }

        // Add order by if provided
        if (order_by.has_value() && !order_by.value().empty()) {
            sql += " ORDER BY " + order_by.value();
        }

        // Add limit/offset if provided
        if (limit.has_value()) {
            sql += " LIMIT " + std::to_string(limit.value());
        }
        if (offset.has_value()) {
            sql += " OFFSET " + std::to_string(offset.value());
        }

        // Execute query
        auto rows = db().query(sql);
        std::vector<managed<swift_dynamic_object>> results;
        results.reserve(rows.size());

        // Get properties for this table
        const SwiftSchema* props = get_properties_for_table(table_name);

        for (const auto& row : rows) {
            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            if (props) {
                obj.properties_ = *props;
                obj.source.properties = *props;
            }
            results.push_back(std::move(obj));
        }
        return results;
    }

    /// Count objects within a bounding box
    int64_t count_within_bbox(
        const std::string& table_name,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt)
        SWIFT_NAME(countWithinBBox(table:geoColumn:minLat:maxLat:minLon:maxLon:where:)) {

        std::string list_table = "_" + table_name + "_" + geo_column;
        std::string list_rtree = list_table + "_rtree";
        std::string single_rtree = "_" + table_name + "_" + geo_column + "_rtree";

        bool is_list = db().table_exists(list_table);

        std::string sql;
        if (is_list) {
            sql = "SELECT COUNT(DISTINCT " + table_name + ".id) FROM " + table_name +
                  " JOIN " + list_table + " lt ON " + table_name + ".globalId = lt.parent_id" +
                  " JOIN " + list_rtree + " r ON lt.id = r.id" +
                  " WHERE r.minLat <= " + std::to_string(max_lat) +
                  " AND r.maxLat >= " + std::to_string(min_lat) +
                  " AND r.minLon <= " + std::to_string(max_lon) +
                  " AND r.maxLon >= " + std::to_string(min_lon);
        } else {
            sql = "SELECT COUNT(*) FROM " + table_name +
                  " JOIN " + single_rtree + " r ON " + table_name + ".id = r.id" +
                  " WHERE r.minLat <= " + std::to_string(max_lat) +
                  " AND r.maxLat >= " + std::to_string(min_lat) +
                  " AND r.minLon <= " + std::to_string(max_lon) +
                  " AND r.maxLon >= " + std::to_string(min_lon);
        }

        if (where_clause.has_value() && !where_clause.value().empty()) {
            sql += " AND (" + where_clause.value() + ")";
        }

        auto rows = db().query(sql);
        if (!rows.empty()) {
            auto& row = rows[0];
            for (const auto& [key, value] : row) {
                if (std::holds_alternative<int64_t>(value)) {
                    return std::get<int64_t>(value);
                }
            }
        }
        return 0;
    }

    /// Find objects nearest to a point within a radius.
    /// Uses bounding box + R*Tree for efficient filtering.
    /// When sortByDistance is true, computes Haversine distance and sorts.
    /// Returns (object, distance_meters) pairs.
    std::vector<std::pair<managed<swift_dynamic_object>, double>> geo_nearest(
        const std::string& table_name,
        const std::string& geo_column,
        double center_lat, double center_lon,
        double radius_meters,
        int32_t limit,
        bool sort_by_distance,
        OptionalString where_clause = std::nullopt)
        SWIFT_NAME(geoNearest(table:geoColumn:lat:lon:radius:limit:sortByDistance:where:)) {

        // Convert radius to bounding box
        constexpr double METERS_PER_DEGREE = 111000.0;
        constexpr double DEG_TO_RAD = 3.14159265358979323846 / 180.0;
        double delta_lat = radius_meters / METERS_PER_DEGREE;
        double delta_lon = radius_meters / (METERS_PER_DEGREE * std::cos(center_lat * DEG_TO_RAD));

        double min_lat = center_lat - delta_lat;
        double max_lat = center_lat + delta_lat;
        double min_lon = center_lon - delta_lon;
        double max_lon = center_lon + delta_lon;

        std::string list_table = "_" + table_name + "_" + geo_column;
        std::string list_rtree = list_table + "_rtree";
        std::string single_rtree = "_" + table_name + "_" + geo_column + "_rtree";

        bool is_list = db().table_exists(list_table);

        std::string sql;
        std::string distance_col;

        if (sort_by_distance) {
            // Haversine distance for sorting
            std::string lat_str = std::to_string(center_lat);
            std::string lon_str = std::to_string(center_lon);
            distance_col = "6371000.0 * 2.0 * ASIN(SQRT("
                "POWER(SIN(RADIANS(((r.minLat + r.maxLat) / 2.0) - " + lat_str + ") / 2.0), 2) + "
                "COS(RADIANS(" + lat_str + ")) * "
                "COS(RADIANS((r.minLat + r.maxLat) / 2.0)) * "
                "POWER(SIN(RADIANS(((r.minLon + r.maxLon) / 2.0) - " + lon_str + ") / 2.0), 2)"
                "))";
        } else {
            distance_col = "0.0";
        }

        if (is_list) {
            sql = "SELECT DISTINCT " + table_name + ".*, " + distance_col + " AS _distance FROM " + table_name +
                  " JOIN " + list_table + " lt ON " + table_name + ".globalId = lt.parent_id" +
                  " JOIN " + list_rtree + " r ON lt.id = r.id" +
                  " WHERE r.minLat <= " + std::to_string(max_lat) +
                  " AND r.maxLat >= " + std::to_string(min_lat) +
                  " AND r.minLon <= " + std::to_string(max_lon) +
                  " AND r.maxLon >= " + std::to_string(min_lon);
        } else {
            sql = "SELECT " + table_name + ".*, " + distance_col + " AS _distance FROM " + table_name +
                  " JOIN " + single_rtree + " r ON " + table_name + ".id = r.id" +
                  " WHERE r.minLat <= " + std::to_string(max_lat) +
                  " AND r.maxLat >= " + std::to_string(min_lat) +
                  " AND r.minLon <= " + std::to_string(max_lon) +
                  " AND r.maxLon >= " + std::to_string(min_lon);
        }

        if (where_clause.has_value() && !where_clause.value().empty()) {
            sql += " AND (" + where_clause.value() + ")";
        }

        if (sort_by_distance) {
            sql += " ORDER BY _distance ASC";
        }

        sql += " LIMIT " + std::to_string(limit);

        auto rows = db().query(sql);
        std::vector<std::pair<managed<swift_dynamic_object>, double>> results;
        results.reserve(rows.size());

        const SwiftSchema* props = get_properties_for_table(table_name);

        for (const auto& row : rows) {
            double distance = 0.0;
            auto dist_it = row.find("_distance");
            if (dist_it != row.end() && std::holds_alternative<double>(dist_it->second)) {
                distance = std::get<double>(dist_it->second);
            }

            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            if (props) {
                obj.properties_ = *props;
                obj.source.properties = *props;
            }
            results.push_back(std::make_pair(std::move(obj), distance));
        }
        return results;
    }

    /// Combined query: spatial filtering + vector search + where clause.
    /// Uses R*Tree for spatial filtering and vec0 for vector similarity.
    /// Returns objects ordered by vector distance within the spatial region.
    ///
    /// Parameters:
    /// - table_name: The model table name
    /// - vector_column: Column name for vector embeddings
    /// - query_vector: The query vector bytes
    /// - k: Maximum number of results
    /// - metric: Distance metric (0=L2, 1=cosine, 2=L1)
    /// - geo_column: Column name for geo bounds (optional, empty string to skip)
    /// - bounds: Geographic bounding box (minLat, maxLat, minLon, maxLon)
    /// - where_clause: Additional SQL WHERE conditions
    std::vector<std::pair<managed<swift_dynamic_object>, double>> combined_query(
        const std::string& table_name,
        const std::string& vector_column,
        const ByteVector& query_vector,
        int k,
        int metric,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt)
        SWIFT_NAME(combinedQuery(table:vectorColumn:queryVector:k:metric:geoColumn:minLat:maxLat:minLon:maxLon:where:)) {

        // Determine distance function based on metric
        std::string distance_func;
        switch (metric) {
            case 1: distance_func = "vec_distance_cosine"; break;
            case 2: distance_func = "vec_distance_L1"; break;
            default: distance_func = "vec_distance_L2"; break;
        }

        // Vec0 table name
        std::string vec_table = "_" + table_name + "_" + vector_column + "_vec";

        // Build query vector as blob literal
        std::ostringstream blob_ss;
        blob_ss << "X'";
        for (uint8_t b : query_vector) {
            blob_ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(b);
        }
        blob_ss << "'";
        std::string query_blob = blob_ss.str();

        std::ostringstream sql;
        sql << "SELECT " << table_name << ".*, "
            << distance_func << "(v.embedding, " << query_blob << ") AS _distance "
            << "FROM " << table_name << " "
            << "JOIN " << vec_table << " v ON " << table_name << ".globalId = v.global_id";

        // Add spatial join if geo_column is provided
        bool has_geo = !geo_column.empty();
        if (has_geo) {
            std::string rtree_table = "_" + table_name + "_" + geo_column + "_rtree";
            sql << " JOIN " << rtree_table << " r ON " << table_name << ".id = r.id";
        }

        sql << " WHERE 1=1";

        // Add spatial filter
        if (has_geo) {
            sql << " AND r.minLat <= " << std::to_string(max_lat)
                << " AND r.maxLat >= " << std::to_string(min_lat)
                << " AND r.minLon <= " << std::to_string(max_lon)
                << " AND r.maxLon >= " << std::to_string(min_lon);
        }

        // Add user where clause
        if (where_clause.has_value() && !where_clause.value().empty()) {
            sql << " AND (" << where_clause.value() << ")";
        }

        sql << " ORDER BY _distance ASC LIMIT " << k;

        auto rows = db().query(sql.str());
        std::vector<std::pair<managed<swift_dynamic_object>, double>> results;
        results.reserve(rows.size());

        const SwiftSchema* props = get_properties_for_table(table_name);

        for (const auto& row : rows) {
            double distance = 0.0;
            auto dist_it = row.find("_distance");
            if (dist_it != row.end() && std::holds_alternative<double>(dist_it->second)) {
                distance = std::get<double>(dist_it->second);
            }

            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            if (props) {
                obj.properties_ = *props;
                obj.source.properties = *props;
            }
            results.push_back(std::make_pair(std::move(obj), distance));
        }
        return results;
    }

    /// Combined nearest query: intersects multiple bounds, vector, and geo constraints.
    /// All constraints act as filters (must satisfy ALL).
    /// Returns results unordered with distances keyed by column name.
    ///
    /// Parameters:
    /// - table_name: The model table name
    /// - bounds: Vector of bounding box constraints (R*Tree intersection)
    /// - vectors: Vector of vector similarity constraints (vec0 k-NN)
    /// - geos: Vector of geo proximity constraints (R*Tree + Haversine)
    /// - where_clause: Additional SQL WHERE conditions
    /// - sort: Sort descriptor (by geo distance, vector distance, or property)
    /// - limit: Maximum number of results
    CombinedQueryResultVector combined_nearest_query(
        const std::string& table_name,
        const BoundsConstraintVector& bounds,
        const VectorConstraintVector& vectors,
        const GeoConstraintVector& geos,
        const TextConstraintVector& texts,
        OptionalString where_clause,
        const sort_descriptor& sort,
        int64_t limit,
        OptionalString group_by = std::nullopt)
        SWIFT_NAME(combinedNearestQuery(table:bounds:vectors:geos:texts:where:sort:limit:groupBy:)) {

        // Constants for geo calculations
        constexpr double METERS_PER_DEGREE = 111000.0;
        constexpr double DEG_TO_RAD = 3.14159265358979323846 / 180.0;

        // If no constraints, just return objects with limit
        if (bounds.empty() && vectors.empty() && geos.empty() && texts.empty()) {
            auto rows = db().query("SELECT * FROM " + table_name +
                (where_clause.has_value() && !where_clause.value().empty()
                    ? " WHERE " + where_clause.value() : "") +
                " LIMIT " + std::to_string(limit));

            CombinedQueryResultVector results;
            results.reserve(rows.size());
            const SwiftSchema* props = get_properties_for_table(table_name);

            for (const auto& row : rows) {
                auto obj = hydrate<swift_dynamic_object>(row, table_name);
                if (props) {
                    obj.properties_ = *props;
                    obj.source.properties = *props;
                }
                results.emplace_back(std::move(obj), DistanceEntryVector{});
            }
            return results;
        }

        // Build the SQL query with CTEs for each constraint type
        std::ostringstream sql;
        std::vector<std::string> cte_names;
        int cte_index = 0;

        sql << "WITH ";

        // ====================================================================
        // Step 1: R*Tree pre-filter for bounds constraints
        // ====================================================================
        for (const auto& bc : bounds) {
            std::string cte_name = "bounds_" + std::to_string(cte_index++);
            cte_names.push_back(cte_name);

            std::string rtree_table = "_" + table_name + "_" + bc.column + "_rtree";

            sql << cte_name << " AS ("
                << "SELECT id FROM " << rtree_table
                << " WHERE minLat <= " << std::to_string(bc.max_lat)
                << " AND maxLat >= " << std::to_string(bc.min_lat)
                << " AND minLon <= " << std::to_string(bc.max_lon)
                << " AND maxLon >= " << std::to_string(bc.min_lon)
                << "), ";
        }

        // ====================================================================
        // Step 2: R*Tree pre-filter + Haversine for geo constraints
        // ====================================================================
        std::vector<std::string> geo_cte_names;
        for (const auto& gc : geos) {
            std::string cte_name = "geo_" + std::to_string(cte_index++);
            cte_names.push_back(cte_name);
            geo_cte_names.push_back(cte_name);

            // Expand radius to bounding box for R*Tree pre-filter
            double delta_lat = gc.radius_meters / METERS_PER_DEGREE;
            double delta_lon = gc.radius_meters / (METERS_PER_DEGREE * std::cos(gc.center_lat * DEG_TO_RAD));

            double min_lat = gc.center_lat - delta_lat;
            double max_lat = gc.center_lat + delta_lat;
            double min_lon = gc.center_lon - delta_lon;
            double max_lon = gc.center_lon + delta_lon;

            std::string rtree_table = "_" + table_name + "_" + gc.column + "_rtree";
            std::string lat_str = std::to_string(gc.center_lat);
            std::string lon_str = std::to_string(gc.center_lon);
            std::string radius_str = std::to_string(gc.radius_meters);

            // Haversine distance calculation
            std::string haversine =
                "6371000.0 * 2.0 * ASIN(SQRT("
                "POWER(SIN(RADIANS(((r.minLat + r.maxLat) / 2.0) - " + lat_str + ") / 2.0), 2) + "
                "COS(RADIANS(" + lat_str + ")) * "
                "COS(RADIANS((r.minLat + r.maxLat) / 2.0)) * "
                "POWER(SIN(RADIANS(((r.minLon + r.maxLon) / 2.0) - " + lon_str + ") / 2.0), 2)"
                "))";

            sql << cte_name << " AS ("
                << "SELECT r.id, " << haversine << " AS distance"
                << " FROM " << rtree_table << " r"
                << " WHERE r.minLat <= " << std::to_string(max_lat)
                << " AND r.maxLat >= " << std::to_string(min_lat)
                << " AND r.minLon <= " << std::to_string(max_lon)
                << " AND r.maxLon >= " << std::to_string(min_lon)
                << " AND " << haversine << " <= " << radius_str
                << "), ";
        }

        // ====================================================================
        // Step 3: Intersect all spatial constraints to get candidate IDs
        // ====================================================================
        std::string candidates_cte = "spatial_candidates";
        if (!cte_names.empty()) {
            sql << candidates_cte << " AS (";
            for (size_t i = 0; i < cte_names.size(); ++i) {
                if (i > 0) sql << " INTERSECT ";
                sql << "SELECT id FROM " << cte_names[i];
            }
            sql << "), ";
        }

        // ====================================================================
        // Step 4: Vector constraints with pre-filtering
        // ====================================================================
        std::vector<std::string> vec_cte_names;
        for (const auto& vc : vectors) {
            std::string vec_table = "_" + table_name + "_" + vc.column + "_vec";

            // vec0 table is created lazily on first vector insert (needs dimensions).
            // If no data has been inserted yet, the table won't exist — return empty.
            if (!db().table_exists(vec_table)) {
                return CombinedQueryResultVector{};
            }

            std::string cte_name = "vec_" + std::to_string(cte_index++);
            vec_cte_names.push_back(cte_name);

            // Determine distance function
            std::string distance_func;
            switch (vc.metric) {
                case 1: distance_func = "vec_distance_cosine"; break;
                case 2: distance_func = "vec_distance_L1"; break;
                default: distance_func = "vec_distance_L2"; break;
            }

            // Build query vector as blob literal
            std::ostringstream blob_ss;
            blob_ss << "X'";
            for (uint8_t b : vc.query_vector) {
                blob_ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(b);
            }
            blob_ss << "'";
            std::string query_blob = blob_ss.str();

            // Always join with main table to get id (vec0 uses global_id, not rowid)
            sql << cte_name << " AS ("
                << "SELECT m.id AS id, " << distance_func << "(v.embedding, " << query_blob << ") AS distance"
                << " FROM " << vec_table << " v"
                << " JOIN " << table_name << " m ON m.globalId = v.global_id";

            // Build WHERE conditions
            std::vector<std::string> where_conditions;

            // Pre-filter by spatial candidates if we have them
            if (!cte_names.empty()) {
                where_conditions.push_back("m.id IN (SELECT id FROM " + candidates_cte + ")");
            }

            // Apply user's WHERE clause to vector CTE
            if (where_clause.has_value() && !where_clause.value().empty()) {
                where_conditions.push_back("(" + where_clause.value() + ")");
            }

            if (!where_conditions.empty()) {
                sql << " WHERE ";
                for (size_t i = 0; i < where_conditions.size(); ++i) {
                    if (i > 0) sql << " AND ";
                    sql << where_conditions[i];
                }
            }

            sql << " ORDER BY distance ASC LIMIT " << vc.k
                << "), ";
        }

        // ====================================================================
        // Step 4b: FTS5 full-text search constraints
        // ====================================================================
        std::vector<std::string> fts_cte_names;
        for (const auto& tc : texts) {
            std::string fts_table = "_" + table_name + "_" + tc.column + "_fts";

            std::string cte_name = "fts_" + std::to_string(cte_index++);
            fts_cte_names.push_back(cte_name);

            // Escape single quotes in search text
            std::string escaped_text = tc.search_text;
            size_t pos = 0;
            while ((pos = escaped_text.find('\'', pos)) != std::string::npos) {
                escaped_text.replace(pos, 1, "''");
                pos += 2;
            }

            sql << cte_name << " AS ("
                << "SELECT fts.rowid AS id, fts.rank AS distance"
                << " FROM " << fts_table << " fts"
                << " WHERE " << fts_table << " MATCH '" << escaped_text << "'";

            // Pre-filter by spatial candidates if we have them
            if (!cte_names.empty()) {
                sql << " AND fts.rowid IN (SELECT id FROM " << candidates_cte << ")";
            }

            // Apply user's WHERE clause to FTS CTE
            if (where_clause.has_value() && !where_clause.value().empty()) {
                sql << " AND fts.rowid IN (SELECT id FROM " << table_name
                    << " WHERE " << where_clause.value() << ")";
            }

            sql << " ORDER BY fts.rank LIMIT " << tc.limit
                << "), ";
        }

        // ====================================================================
        // Step 5: Final intersection of all candidates (including vectors and FTS)
        // ====================================================================
        std::string final_candidates_cte = "final_candidates";
        sql << final_candidates_cte << " AS (";

        // Collect all candidate sources
        std::vector<std::string> all_candidate_ctes;
        for (const auto& v : vec_cte_names) all_candidate_ctes.push_back(v);
        for (const auto& f : fts_cte_names) all_candidate_ctes.push_back(f);

        if (!all_candidate_ctes.empty()) {
            // Intersect all vector and FTS candidates
            for (size_t i = 0; i < all_candidate_ctes.size(); ++i) {
                if (i > 0) sql << " INTERSECT ";
                sql << "SELECT id FROM " << all_candidate_ctes[i];
            }
            // Also intersect with spatial candidates if present
            if (!cte_names.empty()) {
                sql << " INTERSECT SELECT id FROM " << candidates_cte;
            }
        } else if (!cte_names.empty()) {
            // No vector/FTS constraints, just use spatial candidates
            sql << "SELECT id FROM " << candidates_cte;
        } else {
            // No constraints at all - select all IDs
            sql << "SELECT id FROM " << table_name;
        }
        sql << ") ";

        // ====================================================================
        // Step 6: Final SELECT with all distances
        // ====================================================================
        sql << "SELECT " << table_name << ".*";

        // Add geo distance columns
        for (size_t i = 0; i < geos.size(); ++i) {
            sql << ", g" << i << ".distance AS _dist_" << geos[i].column;
        }

        // Add vector distance columns
        for (size_t i = 0; i < vectors.size(); ++i) {
            sql << ", v" << i << ".distance AS _dist_" << vectors[i].column;
        }

        // Add FTS rank columns
        for (size_t i = 0; i < texts.size(); ++i) {
            sql << ", f" << i << ".distance AS _dist_" << texts[i].column;
        }

        sql << " FROM " << table_name;

        // Join final candidates
        sql << " JOIN " << final_candidates_cte << " fc ON " << table_name << ".id = fc.id";

        // Join geo CTEs for distance values
        for (size_t i = 0; i < geo_cte_names.size(); ++i) {
            sql << " LEFT JOIN " << geo_cte_names[i] << " g" << i
                << " ON " << table_name << ".id = g" << i << ".id";
        }

        // Join vector CTEs for distance values
        for (size_t i = 0; i < vec_cte_names.size(); ++i) {
            sql << " LEFT JOIN " << vec_cte_names[i] << " v" << i
                << " ON " << table_name << ".id = v" << i << ".id";
        }

        // Join FTS CTEs for rank values
        for (size_t i = 0; i < fts_cte_names.size(); ++i) {
            sql << " LEFT JOIN " << fts_cte_names[i] << " f" << i
                << " ON " << table_name << ".id = f" << i << ".id";
        }

        // WHERE clause
        if (where_clause.has_value() && !where_clause.value().empty()) {
            sql << " WHERE " << where_clause.value();
        }

        // GROUP BY clause
        if (group_by.has_value() && !group_by.value().empty()) {
            sql << " GROUP BY " << group_by.value();
        }

        // ORDER BY clause (based on sort descriptor)
        if (sort.type != sort_descriptor::Type::none && !sort.column.empty()) {
            std::string order_col;
            switch (sort.type) {
                case sort_descriptor::Type::geo_distance:
                case sort_descriptor::Type::vector_distance:
                case sort_descriptor::Type::text_rank:
                    // Sort by distance/rank column (alias is _dist_{column})
                    order_col = "_dist_" + sort.column;
                    break;
                case sort_descriptor::Type::property:
                    // Sort by property on the table
                    order_col = table_name + "." + sort.column;
                    break;
                default:
                    break;
            }
            if (!order_col.empty()) {
                sql << " ORDER BY " << order_col << (sort.ascending ? " ASC" : " DESC");
            }
        }

        // Apply limit
        sql << " LIMIT " << limit;

        // Execute query
        auto rows = db().query(sql.str());
        CombinedQueryResultVector results;
        results.reserve(rows.size());

        const SwiftSchema* props = get_properties_for_table(table_name);

        for (const auto& row : rows) {
            DistanceEntryVector distances;

            // Extract geo distances
            for (const auto& gc : geos) {
                std::string dist_col = "_dist_" + gc.column;
                auto it = row.find(dist_col);
                if (it != row.end() && std::holds_alternative<double>(it->second)) {
                    distances.emplace_back(gc.column, std::get<double>(it->second));
                }
            }

            // Extract vector distances
            for (const auto& vc : vectors) {
                std::string dist_col = "_dist_" + vc.column;
                auto it = row.find(dist_col);
                if (it != row.end() && std::holds_alternative<double>(it->second)) {
                    distances.emplace_back(vc.column, std::get<double>(it->second));
                }
            }

            // Extract FTS rank scores
            for (const auto& tc : texts) {
                std::string dist_col = "_dist_" + tc.column;
                auto it = row.find(dist_col);
                if (it != row.end() && std::holds_alternative<double>(it->second)) {
                    distances.emplace_back(tc.column, std::get<double>(it->second));
                }
            }

            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            if (props) {
                obj.properties_ = *props;
                obj.source.properties = *props;
            }
            results.emplace_back(std::move(obj), std::move(distances));
        }

        return results;
    }

    // ========================================================================
    // Sync-server helpers
    // ========================================================================
    AuditLogEntryVector events_after(const OptionalString& checkpoint_global_id) {
        return ::lattice::events_after(this->db(), checkpoint_global_id);
    }
    
    /// Receive sync data from a client (server-side)
    /// data: JSON bytes containing ServerSentEvent
    /// Returns list of acknowledged global IDs
    std::vector<std::string> receive_sync_data(const ByteVector& data) {
        std::string json_str(data.begin(), data.end());
        auto event = server_sent_event::from_json(json_str);
        if (!event) {
            return {};  // Parse failed
        }
        std::vector<std::string> result;
        
        if (event->event_type == server_sent_event::type::audit_log) {
            // Apply remote changes to this database (model SQL + AuditLog records)
            ::lattice::apply_remote_changes(*this, event->audit_logs);
            result.reserve(event->audit_logs.size());
            for (const auto& entry : event->audit_logs) {
                result.push_back(entry.global_id);
            }
        } else if (event->event_type == server_sent_event::type::ack) {
            // Mark entries as synchronized (includes observer notification)
            ::lattice::mark_audit_entries_synced(*this, event->acked_ids);
            result = event->acked_ids;
        }
        
        return result;
    }
} SWIFT_UNSAFE_REFERENCE;

// ============================================================================
// swift_migration_context_ref - Wrapper for migration_context
// Exposes migration APIs to Swift during schema migration
// ============================================================================

/// Swift-friendly table changes info
struct swift_table_changes {
    std::string table_name;
    std::vector<std::string> added_columns;
    std::vector<std::string> removed_columns;
    std::vector<std::string> changed_columns;

    bool has_changes() const {
        return !added_columns.empty() || !removed_columns.empty() || !changed_columns.empty();
    }

    swift_table_changes() = default;
    swift_table_changes(const table_changes& tc)
        : table_name(tc.table_name)
        , added_columns(tc.added_columns)
        , removed_columns(tc.removed_columns)
        , changed_columns(tc.changed_columns) {}
};

/// Swift-friendly migration row (key-value pairs)
/// Uses column_value_t which Swift can work with via bridging
class swift_migration_context_ref {
public:
    /// Create a migration context ref wrapping an existing context
    explicit swift_migration_context_ref(migration_context* ctx)
        : ctx_(ctx) {}

    /// Get all pending schema changes
    std::vector<swift_table_changes> pending_changes() const SWIFT_NAME(pendingChanges()) {
        std::vector<swift_table_changes> result;
        for (const auto& tc : ctx_->pending_changes()) {
            result.emplace_back(tc);
        }
        return result;
    }

    /// Check if a specific table has pending changes
    bool has_changes_for(const std::string& table_name) const SWIFT_NAME(hasChanges(for:)) {
        return ctx_->has_changes_for(table_name);
    }

    /// Get pending changes for a specific table (returns empty if none)
    swift_table_changes changes_for(const std::string& table_name) const SWIFT_NAME(changes(for:)) {
        auto* tc = ctx_->changes_for(table_name);
        return tc ? swift_table_changes(*tc) : swift_table_changes();
    }
#ifdef __BLOCKS__
    /// Enumerate all objects in a table for migration.
    /// Swift callback receives each row as a dictionary for transformation.
    /// Use get_row_value/set_row_value for type-safe access.
    void enumerate_objects(
        const std::string& table_name,
        void(^callback)(int64_t row_id, const std::unordered_map<std::string, column_value_t>& old_row)
    ) SWIFT_NAME(enumerateObjects(table:callback:)) {
        ctx_->enumerate_objects(table_name, [&](const migration_row& old_row, migration_row& new_row) {
            // Get row_id
            int64_t row_id = 0;
            auto id_it = old_row.find("id");
            if (id_it != old_row.end() && std::holds_alternative<int64_t>(id_it->second)) {
                row_id = std::get<int64_t>(id_it->second);
            }

            // Call Swift callback with old row data
            callback(row_id, old_row);
        });
    }
#else
    /// Enumerate all objects in a table for migration.
    /// Swift callback receives each row as a dictionary for transformation.
    /// Use get_row_value/set_row_value for type-safe access.
    void enumerate_objects(
        const std::string& table_name,
        std::function<void(int64_t row_id, const std::unordered_map<std::string, column_value_t>& old_row)> callback
    ) SWIFT_NAME(enumerateObjects(table:callback:)) {
        ctx_->enumerate_objects(table_name, [&](const migration_row& old_row, migration_row& new_row) {
            // Get row_id
            int64_t row_id = 0;
            auto id_it = old_row.find("id");
            if (id_it != old_row.end() && std::holds_alternative<int64_t>(id_it->second)) {
                row_id = std::get<int64_t>(id_it->second);
            }

            // Call Swift callback with old row data
            callback(row_id, old_row);
        });
    }
#endif
    /// Set a value for a row during migration
    /// Call this from within your enumeration callback to transform data
    void set_row_value(const std::string& table_name, int64_t row_id,
                       const std::string& column_name, const column_value_t& value)
        SWIFT_NAME(setRowValue(table:rowId:column:value:)) {
        // Queue the update for application after migration
        pending_row_updates_[table_name][row_id][column_name] = value;
    }

    /// Rename a property (copies old column value to new column name)
    void rename_property(const std::string& table_name,
                         const std::string& old_name,
                         const std::string& new_name) SWIFT_NAME(renameProperty(table:from:to:)) {
        ctx_->rename_property(table_name, old_name, new_name);
    }

    /// Delete all objects in a table
    void delete_all(const std::string& table_name) SWIFT_NAME(deleteAll(table:)) {
        ctx_->delete_all(table_name);
    }

    /// Execute raw SQL for complex migrations
    void execute_sql(const std::string& sql) SWIFT_NAME(executeSQL(_:)) {
        ctx_->execute_sql(sql);
    }

    /// Query raw SQL for reading data
    std::vector<std::unordered_map<std::string, column_value_t>> query_sql(const std::string& sql)
        SWIFT_NAME(querySQL(_:)) {
        return ctx_->query_sql(sql);
    }

    // For SWIFT_SHARED_REFERENCE
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }

    // Apply pending row updates to the context
    // Called internally before migration context goes out of scope
    void apply_row_updates() {
        for (const auto& [table_name, rows] : pending_row_updates_) {
            ctx_->enumerate_objects(table_name, [&](const migration_row& old_row, migration_row& new_row) {
                int64_t row_id = 0;
                auto id_it = old_row.find("id");
                if (id_it != old_row.end() && std::holds_alternative<int64_t>(id_it->second)) {
                    row_id = std::get<int64_t>(id_it->second);
                }

                auto row_it = rows.find(row_id);
                if (row_it != rows.end()) {
                    for (const auto& [col, val] : row_it->second) {
                        new_row[col] = val;
                    }
                }
            });
        }
    }

private:
    migration_context* ctx_;
    std::atomic<int> ref_count_{0};
    // Pending row updates: table_name -> row_id -> column_name -> value
    std::unordered_map<std::string, std::unordered_map<int64_t, std::unordered_map<std::string, column_value_t>>> pending_row_updates_;
} SWIFT_SHARED_REFERENCE(retainSwiftMigrationContextRef, releaseSwiftMigrationContextRef);

#ifdef __BLOCKS__
/// Migration block type for Swift callbacks
/// Takes a swift_migration_context_ref* that Swift can use
using swift_migration_block_t = void(^)(swift_migration_context_ref*);
#else
/// Migration block type for Swift callbacks
/// Takes a swift_migration_context_ref* that Swift can use
using swift_migration_block_t = std::function<void(swift_migration_context_ref*)>;
#endif

// ============================================================================
// swift_lattice_ref - Wrapper that holds shared_ptr<swift_lattice>
// This is what Swift sees via SWIFT_SHARED_REFERENCE
// Caches instances by configuration path for connection reuse
// ============================================================================

// Cache key for swift_lattice_ref - defined at namespace scope to avoid
// Swift-C++ interop issues with nested types
namespace detail {
    struct LatticeRefCacheKey {
        std::string path;
        std::shared_ptr<scheduler> sched;
        std::string websocket_url;  // Include sync config in cache key
        std::string schema_hash;    // Hash of table names to detect schema changes

        bool operator<(const LatticeRefCacheKey& other) const {
            if (path != other.path) return path < other.path;
            if (websocket_url != other.websocket_url) return websocket_url < other.websocket_url;
            if (schema_hash != other.schema_hash) return schema_hash < other.schema_hash;
            // Compare schedulers: both null, or use is_same_as
            if (!sched && !other.sched) return false;
            if (!sched) return true;  // null < non-null
            if (!other.sched) return false;  // non-null > null
            // Use pointer comparison for ordering (is_same_as is for equality)
            return sched.get() < other.sched.get();
        }

        bool operator==(const LatticeRefCacheKey& other) const {
            if (path != other.path) return false;
            if (websocket_url != other.websocket_url) return false;
            if (schema_hash != other.schema_hash) return false;
            if (!sched && !other.sched) return true;
            if (!sched || !other.sched) return false;
            return sched->is_same_as(other.sched.get());
        }
    };

    // Cache for swift_lattice instances - provides both key-based and pointer-based lookups
    class LatticeCache {
    public:
        static LatticeCache& instance();

        // Get or create a swift_lattice by configuration key
        // Template to preserve swift_configuration type for constructor overload resolution
        template<typename ConfigT>
        std::shared_ptr<swift_lattice> get_or_create(const ConfigT& config, const SchemaVector& schemas) {
            LOG_DEBUG("LatticeCache", "get_or_create() path=%s", config.path.c_str());
            std::lock_guard<std::mutex> lock(mutex_);

            // Build schema hash from table names and properties (sorted for consistency)
            std::string schema_hash;
            {
                std::vector<std::string> schema_strings;
                schema_strings.reserve(schemas.size());
                for (const auto& s : schemas) {
                    std::string entry = s.table_name + "{";
                    // Collect property names
                    std::vector<std::string> prop_names;
                    for (const auto& [name, _] : s.properties) {
                        prop_names.push_back(name);
                    }
                    std::sort(prop_names.begin(), prop_names.end());
                    for (size_t i = 0; i < prop_names.size(); ++i) {
                        if (i > 0) entry += ",";
                        entry += prop_names[i];
                    }
                    entry += "}";
                    schema_strings.push_back(entry);
                }
                std::sort(schema_strings.begin(), schema_strings.end());
                for (const auto& s : schema_strings) {
                    if (!schema_hash.empty()) schema_hash += ";";
                    schema_hash += s;
                }
            }

            // Skip cache when migration is needed (target_schema_version > 1 indicates migration)
            // Migration requires a fresh connection to detect and apply schema changes
            bool skip_cache = config.target_schema_version > 1;

            LatticeRefCacheKey key{config.path, config.sched, config.websocket_url, schema_hash};

            // Find existing entry (unless migration requires fresh connection)
            if (!skip_cache) {
                for (auto it = key_cache_.begin(); it != key_cache_.end(); ++it) {
                    if (it->first == key) {
                        if (auto existing = it->second.lock()) {
                            return existing;
                        }
                        // Expired, remove from both caches
                        key_cache_.erase(it);
                        break;
                    }
                }
            } else {
                // For migration: remove any existing entry for this key
                // This ensures the old connection is invalidated
                for (auto it = key_cache_.begin(); it != key_cache_.end(); ++it) {
                    if (it->first == key) {
                        if (auto existing = it->second.lock()) {
                            ptr_cache_.erase(existing.get());
                        }
                        key_cache_.erase(it);
                        break;
                    }
                }
            }

            // Create new instance - ConfigT selects the right constructor
            LOG_DEBUG("LatticeCache", "Creating new swift_lattice for path=%s", config.path.c_str());
            auto inst = std::make_shared<swift_lattice>(config, schemas);
            LOG_DEBUG("LatticeCache", "swift_lattice created, ptr=%p", inst.get());
            key_cache_.emplace_back(key, inst);
            ptr_cache_[inst.get()] = inst;
            return inst;
        }

        // Look up swift_lattice by raw pointer (reverse lookup from lattice_db*)
        std::shared_ptr<swift_lattice> get_by_pointer(swift_lattice* ptr) {
            std::lock_guard<std::mutex> lock(mutex_);

            auto it = ptr_cache_.find(ptr);
            if (it != ptr_cache_.end()) {
                if (auto shared = it->second.lock()) {
                    return shared;
                }
                ptr_cache_.erase(it);
            }
            return nullptr;
        }

    private:
        LatticeCache() = default;

        std::mutex mutex_;
        std::vector<std::pair<LatticeRefCacheKey, std::weak_ptr<swift_lattice>>> key_cache_;
        std::unordered_map<swift_lattice*, std::weak_ptr<swift_lattice>> ptr_cache_;
    };
} // namespace detail

class swift_lattice_ref {
public:
    // Factory method - creates or reuses instance based on config path, with schemas
    static swift_lattice_ref* create(const configuration& config, const SchemaVector& schemas) {
        auto ref = new swift_lattice_ref();
        ref->impl_ = get_or_create_shared(config, schemas);
        return ref;
    }

    // Factory for path-only config
    static swift_lattice_ref* create_with_path(const std::string& path) {
        return create(configuration(path), {});
    }

    // Factory for in-memory
    static swift_lattice_ref* create_in_memory() {
        return create(configuration(), {});
    }

    /// Factory method with Swift migration block.
    /// The migration block is called when schema changes are detected.
    /// Use `ctx.pendingChanges()` to see what's changing and
    /// `ctx.enumerateObjects()` to transform data during migration.
    ///
    /// Example in Swift:
    /// ```swift
    /// let lattice = SwiftLatticeRef.create(config: config, schemas: schemas) { ctx in
    ///     if ctx.hasChanges(for: "Place") {
    ///         ctx.enumerateObjects(table: "Place") { rowId, oldRow in
    ///             if let lat = oldRow["latitude"], let lon = oldRow["longitude"] {
    ///                 ctx.setRowValue(table: "Place", rowId: rowId,
    ///                                 column: "location_minLat", value: lat)
    ///                 // ... etc
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    static swift_lattice_ref* create_with_migration(
        const configuration& config,
        const SchemaVector& schemas,
        swift_migration_block_t migration_block
    ) SWIFT_NAME(create(config:schemas:migration:)) {
        // Create a modified config with the migration block wrapped
        configuration new_config = config;
        new_config.migration_block = [migration_block](migration_context& ctx) {
            // Wrap the C++ migration_context in a Swift-friendly ref
            auto* swift_ctx = new swift_migration_context_ref(&ctx);
            swift_ctx->retain();  // Keep alive during callback

            // Call the Swift migration block
            migration_block(swift_ctx);

            // Apply any row updates that were queued during enumeration
            swift_ctx->apply_row_updates();

            // Release (Swift may have retained, so check before delete)
            if (swift_ctx->release()) {
                delete swift_ctx;
            }
        };

        auto ref = new swift_lattice_ref();
        ref->impl_ = get_or_create_shared(new_config, schemas);
        return ref;
    }

    /// Factory method with swift_configuration (supports row migration callback).
    /// Called for each row in tables with schema changes.
    /// The callback receives (table_name, old_row, new_row) where:
    /// - old_row: populated with current row data
    /// - new_row: should be filled with transformed data
    static swift_lattice_ref* create(const swift_configuration& config, const SchemaVector& schemas)
        SWIFT_NAME(create(swiftConfig:schemas:)) {
        LOG_DEBUG("swift_lattice_ref", "create() start path=%s schemas=%zu", config.path.c_str(), schemas.size());
        auto ref = new swift_lattice_ref();
        LOG_DEBUG("swift_lattice_ref", "create() calling get_or_create_shared");
        ref->impl_ = get_or_create_shared(config, schemas);
        LOG_DEBUG("swift_lattice_ref", "create() done, impl=%p", ref->impl_.get());
        return ref;
    }

    // Access the underlying swift_lattice (returns pointer for Swift interop)
    swift_lattice* get() { return impl_.get(); }
    const swift_lattice* get() const { return impl_.get(); }

    // For SWIFT_SHARED_REFERENCE
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }

    int64_t hash_value() {
        return reinterpret_cast<intptr_t>(impl_.get());
    }
    
    std::string path() const {
        return impl_.get()->path();
    }
private:
    swift_lattice_ref() = default;

    std::shared_ptr<swift_lattice> impl_;
    std::atomic<int> ref_count_{0};

public:
    // Get or create a swift_lattice_ref for an existing lattice_db pointer
    // Returns nullptr if the lattice was not created via swift_lattice_ref
    static swift_lattice_ref* get_ref_for_lattice(lattice_db* lattice) {
        if (!lattice)
            return nullptr;
        auto* swift_lat = static_cast<swift_lattice*>(lattice);
        auto shared = detail::LatticeCache::instance().get_by_pointer(swift_lat);
        if (!shared)
            return nullptr;

        auto ref = new swift_lattice_ref();
        ref->impl_ = shared;
        return ref;
    }

private:
    // Delegate to LatticeCache - template to preserve config type
    static std::shared_ptr<swift_lattice> get_or_create_shared(const swift_configuration& config, const SchemaVector& schemas) {
        return detail::LatticeCache::instance().get_or_create(config, schemas);
    }
} SWIFT_SHARED_REFERENCE(retainSwiftLatticeRef, releaseSwiftLatticeRef);

// ============================================================================
// Migration Lookup Functions
// Only valid during a migration callback. Used to look up existing objects
// by primary key or globalId for FK-to-Link migration.
// ============================================================================

/// Look up an object by primary key during migration. Returns true if found.
/// Call migration_take_lookup_result() to get the result.
bool migration_lookup(const std::string& table_name, int64_t primary_key)
    SWIFT_NAME(migrationLookup(table:primaryKey:));

/// Look up an object by globalId during migration. Returns true if found.
/// Call migration_take_lookup_result() to get the result.
bool migration_lookup_by_global_id(const std::string& table_name, const std::string& global_id)
    SWIFT_NAME(migrationLookupByGlobalId(table:globalId:));

/// Take the result of the last successful migration_lookup call.
/// Returns an empty ref if no lookup result is available.
dynamic_object_ref* migration_take_lookup_result()
    SWIFT_NAME(migrationTakeLookupResult());

/// Get the old row ref during a row migration callback.
/// Only valid inside a setRowMigrationCallback callback.
dynamic_object_ref* migration_get_old_row()
    SWIFT_NAME(migrationGetOldRow());

/// Get the new row ref during a row migration callback.
/// Only valid inside a setRowMigrationCallback callback.
dynamic_object_ref* migration_get_new_row()
    SWIFT_NAME(migrationGetNewRow());

} // namespace lattice

namespace swift_lattice = lattice;

#endif // __cplusplus
