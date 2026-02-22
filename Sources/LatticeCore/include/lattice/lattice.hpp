#pragma once

#include "log.hpp"
#include "types.hpp"
#include "db.hpp"
#include "schema.hpp"
#include "managed.hpp"
#include "scheduler.hpp"
#include "observation.hpp"
#include "cross_process_notifier.hpp"
#include <vector>
#include <memory>
#include <functional>
#include <random>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <map>
#include <array>
#include <stdio.h>
#include <iostream>
#include <concepts>

namespace lattice {

// Forward declarations
template<typename T> class query;
template<typename T> class results;
class lattice_db;
class synchronizer;

// Type trait to detect if T has a 'source' member (for swift_dynamic_object)
template<typename T, typename = void>
struct has_source_member : std::false_type {};

template<typename T>
struct has_source_member<T, std::void_t<decltype(std::declval<T>().source.values)>> : std::true_type {};

// A general template to help detect the validity of an expression
template <typename T, typename = void>
struct has_instance_schema : std::false_type {};

// Specialization will only compile if the expression inside decltype is valid
template <typename T>
struct has_instance_schema<T, std::void_t<decltype(std::declval<T>().instance_schema())>> : std::true_type {};

// Type trait to detect if T has collect_geo_bounds_lists method (for geo_bounds list persistence)
template<typename T, typename = void>
struct has_geo_bounds_lists : std::false_type {};

template<typename T>
struct has_geo_bounds_lists<T, std::void_t<decltype(std::declval<T>().collect_geo_bounds_lists())>> : std::true_type {};

// ============================================================================
// Multi-instance registry - allows multiple lattice_db instances to share a DB
// Similar to Swift's latticeIsolationRegistrar
// ============================================================================

class instance_registry {
public:
    // Singleton accessor - defined in lattice.cpp to avoid ODR violations
    static instance_registry& instance();

    void register_instance(const std::string& path, lattice_db* db) {
        std::lock_guard<std::mutex> lock(mutex_);
        instances_[path].push_back(db);
    }

    void unregister_instance(const std::string& path, lattice_db* db) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = instances_.find(path);
        if (it != instances_.end()) {
            auto& vec = it->second;
            vec.erase(std::remove(vec.begin(), vec.end(), db), vec.end());
            if (vec.empty()) {
                instances_.erase(it);
            }
        }
    }

    std::vector<lattice_db*> get_instances(const std::string& path) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = instances_.find(path);
        if (it != instances_.end()) {
            return it->second;
        }
        return {};
    }

private:
    instance_registry() = default;
    std::mutex mutex_;
    std::map<std::string, std::vector<lattice_db*>> instances_;
};

// ============================================================================
// Observation token - see observation.hpp for notification_token
// Backward compatibility alias
// ============================================================================

using observation_token = notification_token;

// ============================================================================
// Query builder for type-safe queries
// ============================================================================

template<typename T>
class query {
public:
    explicit query(lattice_db& db) : db_(db) {}

    query& where(const std::string& predicate) {
        where_clause_ = predicate;
        return *this;
    }

    query& order_by(const std::string& column, bool ascending = true) {
        order_clause_ = column + (ascending ? " ASC" : " DESC");
        return *this;
    }

    query& limit(size_t count) {
        limit_ = count;
        return *this;
    }

    query& offset(size_t count) {
        offset_ = count;
        return *this;
    }

    /// Filter by geo_bounds property within a bounding box using R*Tree index.
    /// This is the fast spatial query path.
    query& within_bbox(const std::string& geo_column,
                       double minLat, double maxLat,
                       double minLon, double maxLon) {
        geo_column_ = geo_column;
        geo_bbox_ = geo_bounds(minLat, maxLat, minLon, maxLon);
        return *this;
    }

    /// Filter by geo_bounds property within a bounding box.
    query& within_bbox(const std::string& geo_column, const geo_bounds& bbox) {
        geo_column_ = geo_column;
        geo_bbox_ = bbox;
        return *this;
    }

    std::vector<managed<T>> execute();
    size_t count();
    std::optional<managed<T>> first();

private:
    lattice_db& db_;
    std::string where_clause_;
    std::string order_clause_;
    size_t limit_ = 0;
    size_t offset_ = 0;
    std::string geo_column_;                    // For R*Tree spatial queries
    std::optional<geo_bounds> geo_bbox_;        // Bounding box for spatial filter
};

// ============================================================================
// Results container with observation support (matches realm-cpp pattern)
// ============================================================================

template<typename T>
class results {
public:
    using value_type = managed<T>;

    // Legacy callback type (receives full snapshot)
    using observer_t = std::function<void(const std::vector<managed<T>>&)>;

    // ========================================================================
    // results_change - Describes changes to this collection (realm-cpp style)
    // ========================================================================
    struct results_change {
        /// Pointer to the results collection that changed
        results<T>* collection;

        /// Indices of objects that were deleted
        std::vector<uint64_t> deletions;

        /// Indices of objects that were inserted
        std::vector<uint64_t> insertions;

        /// Indices of objects that were modified
        std::vector<uint64_t> modifications;

        /// True if the collection root was deleted
        bool collection_root_was_deleted = false;

        /// Returns true if no changes occurred
        [[nodiscard]] bool empty() const noexcept {
            return deletions.empty() && insertions.empty() && modifications.empty() &&
                   !collection_root_was_deleted;
        }
    };

    // New callback type (receives change info like realm-cpp)
    using change_observer_t = std::function<void(results_change)>;

    /// Construct from pre-fetched items (legacy)
    explicit results(std::vector<managed<T>> items, lattice_db* db = nullptr)
        : items_(std::move(items)), db_(db), table_name_(managed<T>::schema().table_name) {}

    /// Construct with query state (for chained queries)
    results(lattice_db* db, std::string table_name,
            std::string where_clause = "", std::string order_clause = "",
            size_t limit = 0, size_t offset = 0)
        : db_(db), table_name_(std::move(table_name)),
          where_clause_(std::move(where_clause)), order_clause_(std::move(order_clause)),
          limit_(limit), offset_(offset) {
        execute_query();
    }

    auto begin() { return items_.begin(); }
    auto end() { return items_.end(); }
    auto begin() const { return items_.begin(); }
    auto end() const { return items_.end(); }

    size_t size() const { return items_.size(); }
    bool empty() const { return items_.empty(); }
    managed<T>& operator[](size_t index) { return items_[index]; }
    const managed<T>& operator[](size_t index) const { return items_[index]; }

    /// Access first element (throws if empty)
    managed<T>& first() {
        if (items_.empty()) throw std::out_of_range("Results is empty");
        return items_.front();
    }

    /// Access last element (throws if empty)
    managed<T>& last() {
        if (items_.empty()) throw std::out_of_range("Results is empty");
        return items_.back();
    }

    // ========================================================================
    // Query API - returns new results with filter/sort applied
    // ========================================================================

    /// Filter results with SQL WHERE predicate
    /// Usage: auto adults = results.where("age > 18");
    results<T> where(const std::string& predicate) const {
        std::string new_where = where_clause_.empty() ? predicate
            : "(" + where_clause_ + ") AND (" + predicate + ")";
        return results<T>(db_, table_name_, new_where, order_clause_, limit_, offset_);
    }

    /// Sort results by column
    /// Usage: auto sorted = results.sort("name", true);
    results<T> sort(const std::string& column, bool ascending = true) const {
        std::string new_order = column + (ascending ? " ASC" : " DESC");
        return results<T>(db_, table_name_, where_clause_, new_order, limit_, offset_);
    }

    /// Limit number of results
    results<T> limit(size_t count) const {
        return results<T>(db_, table_name_, where_clause_, order_clause_, count, offset_);
    }

    /// Skip first N results
    results<T> offset(size_t count) const {
        return results<T>(db_, table_name_, where_clause_, order_clause_, limit_, count);
    }

    // ========================================================================
    // Observation API
    // ========================================================================

    /// Observe changes with full snapshot (legacy API)
    /// Returns a token that must be retained - observation stops when token is destroyed
    /// Callback is dispatched on the scheduler associated with the database
    notification_token observe(observer_t callback);

    /// Observe changes with detailed change info (realm-cpp style)
    /// Callback receives results_change with insertions/deletions/modifications
    notification_token observe(change_observer_t callback);

#if LATTICE_HAS_COROUTINES
    /// Create a coroutine-based change stream for async iteration
    /// Usage: for co_await (auto& change : results.changes()) { ... }
    change_stream<collection_change> changes();
#endif

private:
    std::vector<managed<T>> items_;
    lattice_db* db_ = nullptr;

    // Query state for chaining
    std::string table_name_;
    std::string where_clause_;
    std::string order_clause_;
    size_t limit_ = 0;
    size_t offset_ = 0;

    /// Execute the query and populate items_
    void execute_query();
};

// ============================================================================
// Configuration for lattice_db (matches Lattice.Configuration in Swift)
// ============================================================================

// Forward declarations for migration
class migration_context;
class lattice_db;

/// Migration block type. Called when schema changes are detected, BEFORE auto-migration.
/// Use this to transform data before columns are removed or types change.
/// @param context Migration context with pending changes and enumeration methods
using migration_block_t = std::function<void(migration_context& context)>;

struct configuration {
    /// Database file path. Use ":memory:" for in-memory database.
    std::string path = ":memory:";

    /// Scheduler for dispatching callbacks. nullptr = immediate_scheduler.
    std::shared_ptr<scheduler> sched = nullptr;

    /// WebSocket URL for sync. Empty string = sync disabled.
    /// Example: "ws://localhost:8080/sync"
    std::string websocket_url;

    /// Authorization token for sync. Required if websocket_url is set.
    std::string authorization_token;

    /// Target schema version. Default is 1 (initial schema).
    /// If the database is at a lower version, migrations will run.
    /// If the database is at a higher version, an error is thrown.
    int32_t target_schema_version = 1;

    /// Migration block. Called when schema changes are detected, BEFORE auto-migration.
    /// This allows you to transform data before columns are removed or types change.
    /// Example: copying lat/lon to new geo_bounds columns before lat/lon are dropped.
    migration_block_t migration_block;

    /// Read-only mode. When true:
    /// - Database is opened with SQLITE_OPEN_READONLY
    /// - No WAL mode (uses existing journal mode)
    /// - No table creation or schema changes
    /// - No sync, no change hooks
    /// Use this for bundled template databases in app resources.
    bool read_only = false;

    // Default constructor - in-memory, no sync
    configuration() = default;

    // Path only - file-based, no sync
    explicit configuration(const std::string& p) : path(p) {}

    // Path + scheduler - file-based, no sync, custom scheduler
    configuration(const std::string& p, std::shared_ptr<lattice::scheduler> s)
        : path(p), sched(std::move(s)) {}

    // Full configuration with sync
    configuration(const std::string& p,
                  const std::string& ws_url,
                  const std::string& auth_token,
                  std::shared_ptr<lattice::scheduler> s = nullptr)
        : path(p), sched(std::move(s)), websocket_url(ws_url), authorization_token(auth_token) {}

    /// Returns true if sync is configured (websocket_url is not empty)
    bool is_sync_enabled() const {
        return !websocket_url.empty() && !authorization_token.empty();
    }
};

// Backwards compatibility alias
using db_config = configuration;

// ============================================================================
// Migration Context - Passed to migration block for data transformation
// ============================================================================

/// Row data for migration - maps column name to value
using migration_row = std::unordered_map<std::string, column_value_t>;

/// Describes pending schema changes for a single table
struct table_changes {
    std::string table_name;
    std::vector<std::string> added_columns;    // New columns being added
    std::vector<std::string> removed_columns;  // Columns being removed
    std::vector<std::string> changed_columns;  // Columns with type changes

    bool has_changes() const {
        return !added_columns.empty() || !removed_columns.empty() || !changed_columns.empty();
    }
};

/// Context passed to migration block. Provides methods to enumerate and
/// transform objects during schema migration.
///
/// The migration block is called BEFORE auto-migration, so:
/// - Old columns still exist (you can read from them)
/// - New columns may not exist yet (auto-migration adds them after)
///
/// Typical flow:
/// 1. Check pending_changes() to see what's changing
/// 2. Use enumerate_objects() to read old data and prepare new values
/// 3. After your block returns, auto-migration runs (adds/removes columns)
class migration_context {
public:
    explicit migration_context(database& db) : db_(db) {}

    /// Get all pending schema changes across all tables.
    /// Use this to decide which tables need data transformation.
    const std::vector<table_changes>& pending_changes() const {
        return pending_changes_;
    }

    /// Check if a specific table has pending changes.
    bool has_changes_for(const std::string& table_name) const {
        for (const auto& tc : pending_changes_) {
            if (tc.table_name == table_name && tc.has_changes()) {
                return true;
            }
        }
        return false;
    }

    /// Get pending changes for a specific table (nullptr if none).
    const table_changes* changes_for(const std::string& table_name) const {
        for (const auto& tc : pending_changes_) {
            if (tc.table_name == table_name) {
                return &tc;
            }
        }
        return nullptr;
    }

    /// Enumerate all objects in a table for migration.
    /// The callback receives:
    /// - old_row: The original row data (read-only, includes columns being removed)
    /// - new_row: Mutable row data to write (pre-populated with old values)
    ///
    /// Note: Write to columns that exist in the current schema. For new columns
    /// that don't exist yet, the values will be applied after auto-migration
    /// adds them (stored temporarily and applied via UPDATE).
    ///
    /// Example: Migrating lat/lon to geo_bounds
    /// ```cpp
    /// ctx.enumerate_objects("Place", [](const migration_row& old_row, migration_row& new_row) {
    ///     double lat = std::get<double>(old_row.at("latitude"));
    ///     double lon = std::get<double>(old_row.at("longitude"));
    ///     new_row["location_minLat"] = lat;
    ///     new_row["location_maxLat"] = lat;
    ///     new_row["location_minLon"] = lon;
    ///     new_row["location_maxLon"] = lon;
    /// });
    /// ```
    void enumerate_objects(
        const std::string& table_name,
        std::function<void(const migration_row& old_row, migration_row& new_row)> block) {
        auto rows = db_.query("SELECT * FROM " + table_name);
        LOG_INFO("migration_context", "migrating %zu rows for table %s", rows.size(), table_name.c_str());
        for (const auto& old_row : rows) {
            migration_row new_row = old_row;
            block(old_row, new_row);
            int64_t row_id = 0;
            auto id_it = old_row.find("id");
            if (id_it != old_row.end() && std::holds_alternative<int64_t>(id_it->second)) {
                row_id = std::get<int64_t>(id_it->second);
            }
            if (row_id > 0) {
                pending_updates_[table_name][row_id] = std::move(new_row);
            }
        }
    }

    /// Rename a property (copies old column value to new column name).
    /// Useful when renaming without type change.
    void rename_property(const std::string& table_name,
                         const std::string& old_name,
                         const std::string& new_name) {
        std::string sql = "UPDATE " + table_name + " SET " + new_name + " = " + old_name;
        db_.execute(sql);
    }

    /// Delete all objects in a table.
    void delete_all(const std::string& table_name) {
        db_.execute("DELETE FROM " + table_name);
    }

    /// Execute raw SQL for complex migrations.
    void execute_sql(const std::string& sql) {
        db_.execute(sql);
    }

    /// Query raw SQL for reading data.
    std::vector<migration_row> query_sql(const std::string& sql) {
        return db_.query(sql);
    }

    // -- Internal methods (used by lattice_db) --

    /// Add pending changes for a table (called by lattice_db during schema diff)
    void add_table_changes(table_changes changes) {
        pending_changes_.push_back(std::move(changes));
    }

    /// Queue a row update for application after schema migration
    void queue_row_update(const std::string& table_name, int64_t row_id, migration_row row_data) {
        pending_updates_[table_name][row_id] = std::move(row_data);
    }

    /// Apply pending updates after auto-migration has added new columns
    void apply_pending_updates() {
        for (const auto& [table_name, rows] : pending_updates_) {
            // Get current table columns to filter out columns that no longer exist
            auto existing_cols = db_.get_table_info(table_name);

            for (const auto& [row_id, new_row] : rows) {
                // Build UPDATE statement - only include columns that exist in the table
                std::ostringstream sql;
                std::vector<column_value_t> params;

                sql << "UPDATE " << table_name << " SET ";
                bool first = true;

                for (const auto& [col, val] : new_row) {
                    if (col == "id" || col == "globalId") continue;

                    // Only include columns that exist in the current schema
                    if (existing_cols.find(col) == existing_cols.end()) continue;

                    if (!first) sql << ", ";
                    first = false;
                    sql << col << " = ?";
                    params.push_back(val);
                }

                if (!params.empty()) {
                    sql << " WHERE id = ?";
                    params.push_back(row_id);

                    try {
                        db_.execute(sql.str(), params);
                    } catch (const std::exception& e) {
                        // Log error but continue
                        LOG_ERROR("migration", "Update failed for %s row %lld: %s",
                                  table_name.c_str(), (long long)row_id, e.what());
                    }
                }
            }
        }
        pending_updates_.clear();
    }

    /// Check if there are any pending changes
    bool has_any_changes() const {
        for (const auto& tc : pending_changes_) {
            if (tc.has_changes()) return true;
        }
        return false;
    }

private:
    database& db_;
    std::vector<table_changes> pending_changes_;
    // Pending row updates: table_name -> row_id -> new_row_data
    std::unordered_map<std::string, std::unordered_map<int64_t, migration_row>> pending_updates_;
};

// ============================================================================
// Main database interface
// ============================================================================

class lattice_db {
public:
    // Construct with path (uses default scheduler, no sync)
    explicit lattice_db(const std::string& path)
        : config_(path)
        , db_(std::make_unique<database>(path, database::open_mode::read_write))
        , read_db_(path != ":memory:" ? std::make_unique<database>(path, database::open_mode::read_only) : nullptr)
        , scheduler_(std::make_shared<immediate_scheduler>()) {
        ensure_tables();
        setup_change_hook();
        instance_registry::instance().register_instance(config_.path, this);
        setup_cross_process_notifier();
    }

    // Construct in-memory (uses default scheduler, no sync)
    lattice_db()
        : config_()
        , db_(std::make_unique<database>(":memory:", database::open_mode::read_write))
        , read_db_(nullptr)  // In-memory DB can't have separate read connection
        , scheduler_(std::make_shared<immediate_scheduler>()) {
        ensure_tables();
        setup_change_hook();
        instance_registry::instance().register_instance(config_.path, this);
        setup_cross_process_notifier();
    }

    // Construct with full configuration (including optional sync)
    explicit lattice_db(const configuration& config)
        : config_(config)
        , db_(std::make_unique<database>(config.path,
              config.read_only ? database::open_mode::read_only_immutable : database::open_mode::read_write))
        , read_db_(config.read_only ? nullptr :
                   (config.path != ":memory:" ? std::make_unique<database>(config.path, database::open_mode::read_only) : nullptr))
        , scheduler_(config.sched ? config.sched : std::make_shared<immediate_scheduler>()) {
        if (!config.read_only) {
            ensure_tables();
            setup_change_hook();
            setup_sync_if_configured();
        }
        instance_registry::instance().register_instance(config_.path, this);
        setup_cross_process_notifier();
    }

    ~lattice_db();

    // Non-copyable and non-moveable (due to mutex and sqlite hooks)
    lattice_db(const lattice_db&) = delete;
    lattice_db& operator=(const lattice_db&) = delete;
    lattice_db(lattice_db&&) = delete;
    lattice_db& operator=(lattice_db&&) = delete;

    // ========================================================================
    // Add API - matches Swift's lattice.add(object) pattern
    // Takes unmanaged object, inserts into DB, returns managed object
    // ========================================================================
    // MARK: Add

    /// Add an unmanaged object to the database
    /// Returns a managed object bound to this database
    /// Usage: auto trip = db.add(Trip{"Costa Rica", 10});
    template<typename T>
    managed<std::decay_t<T>> add(T&& obj) {
        using U = std::decay_t<T>;
        managed<U> m(std::forward<T>(obj));
        bind_managed(m, managed<U>::schema());
        return m;
    }

    /// Add an unmanaged object with explicit schema (for dynamic objects)
    template<typename T>
    managed<std::decay_t<T>> add(T&& obj, const model_schema& schema,
                                  const std::vector<std::string>& conflict_columns = {}) {
        using U = std::decay_t<T>;
        managed<U> m(std::forward<T>(obj));
        bind_managed(m, schema, conflict_columns);
        return m;
    }
    
    // MARK: Add Bulk
    /// Add multiple objects in a single transaction (bulk insert)
    /// More efficient than calling add() in a loop
    template<typename T>
    std::vector<managed<std::decay_t<T>>> add_bulk(std::vector<T>&& objects) {
        using U = std::decay_t<T>;
        const auto& schema = managed<U>::schema();
        return add_bulk_with_schema(std::move(objects), schema);
    }

    /// Add multiple objects with explicit schema (for dynamic objects)
    /// conflict_columns: if non-empty, uses ON CONFLICT DO UPDATE for upsert
    template<typename T>
    std::vector<managed<std::decay_t<T>>> add_bulk_with_schema(std::vector<T>&& objects, const model_schema& schema,
                                                               const std::vector<std::string>& conflict_columns = {}) {
        using U = std::decay_t<T>;

        if (objects.empty()) {
            return {};
        }

        // Ensure table exists
        std::vector<column_def> columns;
        columns.reserve(schema.properties.size());
        for (const auto& prop : schema.properties) {
            columns.push_back({prop.name, prop.type, prop.nullable, false, false});
        }
        db_->ensure_table({schema.table_name, columns});

        // Ensure vec0 tables exist for any vector columns (with triggers)
        // This must happen BEFORE the inserts so triggers can fire
        // Use the first object to infer dimensions
        for (const auto& prop : schema.properties) {
            if (prop.is_vector && prop.type == column_type::blob) {
                // Find vector data in first object to infer dimensions
                managed<U> temp(objects[0]);
                auto values = temp.collect_values();
                for (const auto& [name, value] : values) {
                    if (name == prop.name && std::holds_alternative<std::vector<uint8_t>>(value)) {
                        const auto& vec_data = std::get<std::vector<uint8_t>>(value);
                        if (!vec_data.empty()) {
                            int dimensions = static_cast<int>(vec_data.size() / sizeof(float));
                            ensure_vec0_table(schema.table_name, prop.name, dimensions);
                        }
                        break;
                    }
                }
            }
        }

        // Build the SQL: INSERT INTO table (globalId, col1, col2, ...) VALUES (?, ?, ?, ...)
        std::ostringstream sql;
        sql << "INSERT INTO " << schema.table_name << " (globalId";
        for (const auto& prop : schema.properties) {
            if (prop.kind == property_kind::primitive) {
                if (prop.is_geo_bounds) {
                    // geo_bounds expands to 4 columns (matches CREATE TABLE pattern)
                    sql << ", " << prop.name << "_minLat";
                    sql << ", " << prop.name << "_maxLat";
                    sql << ", " << prop.name << "_minLon";
                    sql << ", " << prop.name << "_maxLon";
                } else {
                    sql << ", " << prop.name;
                }
            }
        }
        sql << ") VALUES (?";
        size_t param_count = 1;
        for (const auto& prop : schema.properties) {
            if (prop.kind == property_kind::primitive) {
                if (prop.is_geo_bounds) {
                    // geo_bounds needs 4 placeholders
                    sql << ", ?, ?, ?, ?";
                    param_count += 4;
                } else {
                    sql << ", ?";
                    ++param_count;
                }
            }
        }
        sql << ")";

        // Add ON CONFLICT clause for upsert if conflict_columns provided
        if (!conflict_columns.empty()) {
            sql << " ON CONFLICT (";
            bool first = true;
            for (const auto& col : conflict_columns) {
                if (!first) sql << ", ";
                sql << col;
                first = false;
            }
            sql << ") DO UPDATE SET ";
            first = true;
            for (const auto& prop : schema.properties) {
                if (prop.kind != property_kind::primitive) continue;
                // Skip conflict columns and globalId
                bool is_conflict = false;
                for (const auto& cc : conflict_columns) {
                    if (cc == prop.name) { is_conflict = true; break; }
                }
                if (is_conflict) continue;
                if (!first) sql << ", ";
                sql << prop.name << " = excluded." << prop.name;
                first = false;
            }
        }

        // Prepare once
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_->handle(), sql.str().c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare bulk insert: " + std::string(sqlite3_errmsg(db_->handle())));
        }

        std::vector<managed<U>> results;
        results.reserve(objects.size());

        // Use a transaction for efficiency
        bool was_in_transaction = false;
        try {
            if (!db_->is_in_transaction()) {
                db_->begin_transaction();
            } else {
                was_in_transaction = true;
            }

            for (auto&& obj : objects) {
                managed<U> m(std::forward<T>(obj));

                // Generate globalId
                auto gid = generate_global_id();

                // Bind parameters
                int idx = 1;
                sqlite3_bind_text(stmt, idx++, gid.c_str(), -1, SQLITE_TRANSIENT);

                // Collect and bind primitive values
                auto values = m.collect_values();
                for (const auto& prop : schema.properties) {
                    if (prop.kind == property_kind::primitive) {
                        if (prop.is_geo_bounds) {
                            // geo_bounds: bind 4 expanded columns
                            std::array<std::string, 4> suffixes = {"_minLat", "_maxLat", "_minLon", "_maxLon"};
                            for (const auto& suffix : suffixes) {
                                std::string col_name = prop.name + suffix;
                                bool found = false;
                                for (const auto& [name, val] : values) {
                                    if (name == col_name) {
                                        db_->bind_value(stmt, idx++, val);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found) {
                                    sqlite3_bind_null(stmt, idx++);
                                }
                            }
                        } else {
                            // Find the value for this property
                            bool found = false;
                            for (const auto& [name, val] : values) {
                                if (name == prop.name) {
                                    db_->bind_value(stmt, idx++, val);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                sqlite3_bind_null(stmt, idx++);
                            }
                        }
                    }
                }

                // Execute
                if (sqlite3_step(stmt) != SQLITE_DONE) {
                    throw std::runtime_error("Failed to insert: " + std::string(sqlite3_errmsg(db_->handle())));
                }

                // Get the new ID (or existing ID for upsert)
                auto id = sqlite3_last_insert_rowid(db_->handle());
                primary_key_t actual_id = id;
                global_id_t actual_gid = gid;

                // For upsert, last_insert_rowid returns 0 if only update happened
                if (!conflict_columns.empty() && id == 0) {
                    // Query back to get actual id/globalId
                    std::ostringstream where;
                    std::vector<column_value_t> params;
                    bool first = true;
                    for (const auto& col : conflict_columns) {
                        for (const auto& [name, val] : values) {
                            if (name == col) {
                                if (!first) where << " AND ";
                                where << col << " = ?";
                                params.push_back(val);
                                first = false;
                                break;
                            }
                        }
                    }
                    auto rows = read_db().query("SELECT id, globalId FROM " + schema.table_name +
                                                " WHERE " + where.str(), params);
                    if (!rows.empty()) {
                        actual_id = std::get<int64_t>(rows[0].at("id"));
                        actual_gid = std::get<std::string>(rows[0].at("globalId"));
                    }
                }

                // Bind the managed object
                m.db_ = db_.get();
                m.lattice_ = this;
                m.table_name_ = schema.table_name;
                m.id_ = actual_id;
                m.global_id_ = actual_gid;
                m.bind_to_db();

                results.push_back(std::move(m));

                // Reset for next iteration
                sqlite3_reset(stmt);
                sqlite3_clear_bindings(stmt);
            }

            if (!was_in_transaction) {
                db_->commit();
            }
        } catch (...) {
            if (db_->is_in_transaction()) {
                db_->rollback();
            }
            auto msg = sqlite3_errmsg(db_->handle());
            sqlite3_finalize(stmt);
            throw;
        }

        sqlite3_finalize(stmt);
        return results;
    }

    // MARK: Bind Managed
    /// Internal: Bind a managed object to the database with explicit schema
    /// Inserts row, assigns IDs, and binds properties so writes go to DB
    /// Used by swift_lattice for dynamic objects where schema comes from the instance
    /// conflict_columns: if non-empty, uses ON CONFLICT DO UPDATE for upsert
    template<typename T>
    void bind_managed(managed<T>& obj, const model_schema& schema,
                      const std::vector<std::string>& conflict_columns = {}) {
        // Convert property_descriptors to column_defs for table creation
        std::vector<column_def> columns;
        columns.reserve(schema.properties.size());
        for (const auto& prop : schema.properties) {
            columns.push_back({prop.name, prop.type, prop.nullable, false, false});
        }

        // Ensure table exists
        db_->ensure_table({schema.table_name, columns});

        // Collect values and add globalId
        auto values = obj.collect_values();
        auto gid = generate_global_id();
        values.insert(values.begin(), {"globalId", gid});

        // Ensure vec0 tables exist for any vector columns (with triggers)
        // This must happen BEFORE the insert so triggers can fire
        for (const auto& prop : schema.properties) {
            if (prop.is_vector && prop.type == column_type::blob) {
                // Find the vector data in values to infer dimensions
                for (const auto& [name, value] : values) {
                    if (name == prop.name && std::holds_alternative<std::vector<uint8_t>>(value)) {
                        const auto& vec_data = std::get<std::vector<uint8_t>>(value);
                        if (!vec_data.empty()) {
                            int dimensions = static_cast<int>(vec_data.size() / sizeof(float));
                            ensure_vec0_table(schema.table_name, prop.name, dimensions);
                        }
                        break;
                    }
                }
            }
        }

        // Insert row (triggers will handle vec0 sync)
        auto id = db_->insert(schema.table_name, values, conflict_columns);

        // For upsert, last_insert_rowid returns 0 if only update happened
        // Query back to get the actual id/globalId
        primary_key_t actual_id = id;
        global_id_t actual_gid = gid;
        if (!conflict_columns.empty() && id == 0) {
            // Build WHERE clause from conflict columns
            std::ostringstream where;
            std::vector<column_value_t> params;
            bool first = true;
            for (const auto& col : conflict_columns) {
                for (const auto& [name, val] : values) {
                    if (name == col) {
                        if (!first) where << " AND ";
                        where << col << " = ?";
                        params.push_back(val);
                        first = false;
                        break;
                    }
                }
            }
            auto rows = read_db().query("SELECT id, globalId FROM " + schema.table_name +
                                        " WHERE " + where.str(), params);
            if (!rows.empty()) {
                actual_id = std::get<int64_t>(rows[0].at("id"));
                actual_gid = std::get<std::string>(rows[0].at("globalId"));
            }
        }

        // Bind the managed object to the database
        obj.db_ = db_.get();
        obj.lattice_ = this;
        obj.table_name_ = schema.table_name;
        obj.id_ = actual_id;
        obj.global_id_ = actual_gid;

        // Bind all properties so writes go to DB
        obj.bind_to_db();

        // Persist geo_bounds lists (must happen after bind_to_db so properties are bound)
        persist_geo_bounds_lists(obj, schema.table_name, actual_gid);
    }

    // Persist geo_bounds lists if the managed type supports them
    // Uses the global has_geo_bounds_lists trait defined at namespace level
    template<typename ManagedT>
    void persist_geo_bounds_lists(ManagedT& obj, const std::string& table_name, const std::string& global_id) {
        if constexpr (has_geo_bounds_lists<ManagedT>::value) {
            auto geo_lists = obj.collect_geo_bounds_lists();
            for (const auto& [prop_name, bounds_list] : geo_lists) {
                if (!bounds_list.empty()) {
                    // Ensure list table exists
                    ensure_geo_bounds_list_table(table_name, prop_name);

                    // Insert each bounds entry
                    std::string list_table = "_" + table_name + "_" + prop_name;
                    for (const auto& bounds : bounds_list) {
                        std::string sql = "INSERT INTO " + list_table +
                            " (parent_id, minLat, maxLat, minLon, maxLon) VALUES (?, ?, ?, ?, ?)";
                        db_->execute(sql, {global_id, bounds.min_lat, bounds.max_lat, bounds.min_lon, bounds.max_lon});
                    }
                }
            }
        }
    }

    // ========================================================================
    // Query API - matches Swift's lattice.objects(T.self) pattern
    // Returns results<T> directly (not query builder)
    // ========================================================================

    /// Get all objects of type T
    /// Usage: auto trips = db.objects<Trip>();
    template<typename T>
    results<T> objects() {
        const auto& schema = managed<T>::schema();
        std::string sql = "SELECT * FROM " + schema.table_name;
        // Use read connection for queries (concurrent reads)
        auto rows = read_db().query(sql);

        std::vector<managed<T>> items;
        items.reserve(rows.size());
        for (const auto& row : rows) {
            items.push_back(hydrate<T>(row));
        }

        return results<T>(std::move(items), this);
    }

    /// Get a query builder for type T (supports spatial queries)
    /// Usage: db.query<Place>().within_bbox("location", ...).execute()
    template<typename T>
    query<T> query() {
        return ::lattice::query<T>(*this);
    }

    // Get the scheduler for this database
    std::shared_ptr<scheduler> get_scheduler() const { return scheduler_; }

    // Get the configuration
    const configuration& config() const { return config_; }

    /// Flag indicating if this connection is the sync coordinator
    /// When true, audit logs are generated but not uploaded (they come from remote)
    bool is_synchronizer() const { return is_synchronizer_; }
    void set_is_synchronizer(bool value) { is_synchronizer_ = value; }

    /// Append a change to the buffer (called from update hook)
    void append_to_change_buffer(const std::string& table, const std::string& op,
                                  int64_t row_id, const std::string& global_id) {
        std::lock_guard<std::mutex> lock(change_buffer_mutex_);
        change_buffer_.emplace_back(table, op, row_id, global_id);
    }

    /// Flush buffered changes and notify observers (called from WAL hook)
    /// Broadcasts to all instances sharing this database path
    void flush_changes() {
        LOG_DEBUG("flush_changes", "Called");
        std::vector<std::tuple<std::string, std::string, int64_t, std::string>> changes;
        {
            std::lock_guard<std::mutex> lock(change_buffer_mutex_);
            LOG_DEBUG("flush_changes", "buffer_empty=%d is_flushing=%d", change_buffer_.empty(), is_flushing_);
            if (change_buffer_.empty() || is_flushing_) return;
            is_flushing_ = true;
            changes = std::move(change_buffer_);
            change_buffer_.clear();
        }

        LOG_DEBUG("flush_changes", "Processing %zu changes", changes.size());

        // Get all instances sharing this database path
        auto instances = instance_registry::instance().get_instances(config_.path);
        LOG_DEBUG("flush_changes", "Found %zu instances for path: %s", instances.size(), config_.path.c_str());

        // Advance cross-process cursor BEFORE notifying observers.
        // A stale GCD callback from a previous post_notification() may fire on
        // a background thread during our synchronous observer dispatch below.
        // If the cursor isn't bumped yet, that callback would find the new audit
        // entries and duplicate the notifications.
        if (xproc_notifier_) {
            auto max_rows = read_db().query("SELECT MAX(id) AS max_id FROM AuditLog");
            if (!max_rows.empty()) {
                auto it = max_rows[0].find("max_id");
                if (it != max_rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                    last_seen_audit_id_ = std::get<int64_t>(it->second);
                }
            }
        }

        // Notify observers on all instances for each buffered change
        for (const auto& [table, op, row_id, global_id] : changes) {
            LOG_DEBUG("flush_changes", "Notifying model change: table=%s op=%s", table.c_str(), op.c_str());
            for (auto* instance : instances) {
                instance->notify_change(table, op, row_id, global_id);
            }
        }

        // Also notify AuditLog observers for entries created by SQL triggers
        // (The update hook skips AuditLog to avoid infinite loops, but observers still need notification)
        for (const auto& [table, op, row_id, global_id] : changes) {
            // Skip if this is already an AuditLog change (shouldn't happen, but be safe)
            if (table == "AuditLog") continue;

            LOG_DEBUG("flush_changes", "Querying AuditLog for table=%s rowId=%lld op=%s", table.c_str(), (long long)row_id, op.c_str());

            // Query for AuditLog entry created by trigger for this model change
            auto audit_rows = read_db().query(
                "SELECT id, globalId FROM AuditLog WHERE tableName = ? AND rowId = ? AND operation = ? ORDER BY id DESC LIMIT 1",
                {table, row_id, op}
            );

            LOG_DEBUG("flush_changes", "AuditLog query returned %zu rows", audit_rows.size());

            if (!audit_rows.empty()) {
                const auto& row = audit_rows[0];
                auto id_it = row.find("id");
                auto gid_it = row.find("globalId");
                if (id_it != row.end() && gid_it != row.end() &&
                    std::holds_alternative<int64_t>(id_it->second) &&
                    std::holds_alternative<std::string>(gid_it->second)) {
                    int64_t audit_row_id = std::get<int64_t>(id_it->second);
                    std::string audit_global_id = std::get<std::string>(gid_it->second);

                    LOG_DEBUG("flush_changes", "Notifying AuditLog observers: rowId=%lld", (long long)audit_row_id);

                    // Notify AuditLog observers on all instances
                    for (auto* instance : instances) {
                        instance->notify_change("AuditLog", "INSERT", audit_row_id, audit_global_id);
                    }
                }
            } else {
                LOG_DEBUG("flush_changes", "No AuditLog entry found!");
            }
        }

        // Post cross-process notification (cursor already advanced above)
        if (xproc_notifier_ && !config_.read_only) {
            xproc_notifier_->post_notification();
        }

        {
            std::lock_guard<std::mutex> lock(change_buffer_mutex_);
            is_flushing_ = false;
        }
        LOG_DEBUG("flush_changes", "Done");
    }

    // ========================================================================
    // Sync API (matches Lattice.swift)
    // ========================================================================

    /// Returns true if sync is configured and enabled
    bool is_sync_enabled() const { return config_.is_sync_enabled(); }

    /// Returns true if currently connected to sync server
    bool is_sync_connected() const;

    /// Manually trigger sync (uploads pending changes)
    void sync_now();

    /// Connect to sync server (called automatically if configured)
    void connect_sync();

    /// Disconnect from sync server
    void disconnect_sync();

    /// Set callback for sync state changes
    void set_on_sync_state_change(std::function<void(bool connected)> handler);

    /// Set callback for sync errors
    void set_on_sync_error(std::function<void(const std::string& error)> handler);

    // ========================================================================
    // Observation API
    // ========================================================================

    // Observer ID type
    using observer_id = uint64_t;

    // Register a table observer - called when any row in the table changes
    // Returns an ID that can be used to unregister
    observer_id add_table_observer(const std::string& table_name,
                                    std::function<void(const std::string& operation,
                                                      int64_t row_id,
                                                      const std::string& global_row_id)> callback) {
        std::lock_guard<std::mutex> lock(observers_mutex_);
        auto id = next_observer_id_++;
        table_observers_[table_name][id] = std::move(callback);
        return id;
    }

    // Remove a table observer
    void remove_table_observer(const std::string& table_name, observer_id id) {
        std::lock_guard<std::mutex> lock(observers_mutex_);
        auto it = table_observers_.find(table_name);
        if (it != table_observers_.end()) {
            it->second.erase(id);
        }
    }

    // ========================================================================
    // Per-Object Observation API (matches Swift's observationRegistrar)
    // ========================================================================

    /// Register an observer for a specific object (by table and row ID)
    /// Returns an ID that can be used to unregister
    observer_id add_object_observer(const std::string& table_name, int64_t row_id,
                                     std::function<void(const std::string&)> callback) {
        std::lock_guard<std::mutex> lock(object_observers_mutex_);
        auto id = next_observer_id_++;
        object_observers_[table_name][row_id].emplace_back(id, std::move(callback));
        return id;
    }

    /// Remove a specific object observer
    void remove_object_observer(const std::string& table_name, int64_t row_id, observer_id id) {
        std::lock_guard<std::mutex> lock(object_observers_mutex_);
        auto table_it = object_observers_.find(table_name);
        if (table_it == object_observers_.end()) return;

        auto row_it = table_it->second.find(row_id);
        if (row_it == table_it->second.end()) return;

        auto& observers = row_it->second;
        observers.erase(
            std::remove_if(observers.begin(), observers.end(),
                [id](const auto& pair) { return pair.first == id; }),
            observers.end());

        // Clean up empty entries
        if (observers.empty()) {
            table_it->second.erase(row_it);
            if (table_it->second.empty()) {
                object_observers_.erase(table_it);
            }
        }
    }

    /// Remove all observers for a specific object
    void remove_all_object_observers(const std::string& table_name, int64_t row_id) {
        std::lock_guard<std::mutex> lock(object_observers_mutex_);
        auto table_it = object_observers_.find(table_name);
        if (table_it != object_observers_.end()) {
            table_it->second.erase(row_id);
            if (table_it->second.empty()) {
                object_observers_.erase(table_it);
            }
        }
    }

    // Notify observers of a change (called internally by triggers/hooks)
    void notify_change(const std::string& table_name,
                       const std::string& operation,
                       int64_t row_id,
                       const std::string& global_row_id,
                       const std::string& changed_fields_names = "") {
        std::vector<std::function<void()>> callbacks;

        // Collect table-level observers
        {
            std::lock_guard<std::mutex> lock(observers_mutex_);
            auto it = table_observers_.find(table_name);
            if (it != table_observers_.end()) {
                for (const auto& [id, cb] : it->second) {
                    callbacks.push_back([cb, operation, row_id, global_row_id] {
                        cb(operation, row_id, global_row_id);
                    });
                }
            }
        }

        // Collect per-object observers
        {
            std::lock_guard<std::mutex> lock(object_observers_mutex_);
            auto table_it = object_observers_.find(table_name);
            if (table_it != object_observers_.end()) {
                auto row_it = table_it->second.find(row_id);
                if (row_it != table_it->second.end()) {
                    for (const auto& [id, callback] : row_it->second) {
                        callbacks.push_back([callback, changed_fields_names] {
                            callback(changed_fields_names);
                        });
                    }
                }
            }
        }

        // Dispatch all callbacks on the scheduler
        for (auto& cb : callbacks) {
            scheduler_->invoke(std::move(cb));
        }
    }

    // Find by primary key
    template<typename T>
    std::optional<managed<T>> find(primary_key_t id) {
        const auto& schema = managed<T>::schema();
        return find<T>(id, schema.table_name);
    }

    template<typename T>
    std::optional<managed<T>> find(primary_key_t id, const std::string& table_name) {
        std::string sql = "SELECT * FROM " + table_name + " WHERE id = ?";
        // Use read connection for queries
        auto rows = read_db().query(sql, {id});

        if (rows.empty()) {
            return std::nullopt;
        }
        return hydrate<T>(rows[0], table_name);
    }
    
    // Find by global ID (table name from schema)
    template<typename T>
    std::optional<managed<T>> find_by_global_id(const global_id_t& gid) {
        return find_by_global_id<T>(gid, managed<T>::schema().table_name);
    }

    // Find by global ID (explicit table name for dynamic objects)
    template<typename T>
    std::optional<managed<T>> find_by_global_id(const global_id_t& gid, const std::string& table_name) {
        std::string sql = "SELECT * FROM " + table_name + " WHERE globalId = ?";
        // Use read connection for queries
        auto rows = read_db().query(sql, {gid});

        if (rows.empty()) {
            return std::nullopt;
        }
        return hydrate<T>(rows[0], table_name);
    }

    // Remove an object - version with explicit table name (for dynamic objects)
    template<typename T>
    void remove(managed<T>& obj, const std::string& table_name) {
        if (!obj.is_valid()) return;

        db_->remove(table_name, obj.id_);
        obj.notify_deleted();
        obj.db_ = nullptr;
        obj.id_ = 0;
    }

    // Remove an object - gets table name from schema
    template<typename T>
    void remove(managed<T>& obj) {
        remove(obj, managed<T>::schema().table_name);
    }

    // Count rows in a table (with optional WHERE clause)
    size_t count(const std::string& table_name,
                 std::optional<std::string> where_clause = std::nullopt,
                 std::optional<std::string> group_by = std::nullopt) {
        std::string sql;
        if (group_by.has_value()) {
            sql = "SELECT COUNT(DISTINCT " + *group_by + ") as cnt FROM " + table_name;
        } else {
            sql = "SELECT COUNT(*) as cnt FROM " + table_name;
        }
        if (where_clause.has_value()) {
            sql += " WHERE " + *where_clause;
        }
        auto rows = read_db().query(sql);
        if (!rows.empty()) {
            auto it = rows[0].find("cnt");
            if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                return static_cast<size_t>(std::get<int64_t>(it->second));
            }
        }
        return 0;
    }

    // Delete rows from a table (with optional WHERE clause)
    bool delete_where(const std::string& table_name, std::optional<std::string> where_clause = std::nullopt) {
        std::string sql = "DELETE FROM " + table_name;
        if (where_clause.has_value()) {
            sql += " WHERE " + *where_clause;
        }
        try {
            db_->execute(sql);
            return true;
        } catch (...) {
            return false;
        }
    }

    /// Compact the audit log by replacing all entries with INSERT records
    /// representing the current state of all objects.
    /// This drops all history and creates a fresh snapshot.
    /// @return Number of INSERT entries created
    int64_t compact_audit_log() {
        // Clear all existing audit log entries (with sync disabled)
        db_->execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");
        try {
            db_->execute("DELETE FROM AuditLog");
            db_->execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");
        } catch (...) {
            db_->execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");
            throw;
        }

        // Generate fresh INSERT entries for all objects
        return generate_history();
    }

    /// Generate audit log INSERT entries for objects not already in the audit log.
    /// Unlike compact_audit_log, this preserves existing entries and only adds
    /// entries for objects that are missing from the audit log.
    /// Useful for initial migration when enabling sync on existing data.
    /// @return Number of INSERT entries created
    int64_t generate_history() {
        // Temporarily disable sync to prevent triggers from firing
        db_->execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");

        try {
            db_->begin_transaction();

            // Get all user tables (exclude system tables and virtual/auxiliary tables)
            // R*Tree creates shadow tables like _Table_col_rtree_node, _Table_col_rtree_rowid, etc.
            auto tables = db_->query(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' "
                "AND name NOT IN ('AuditLog', '_SyncControl', '_lattice_meta') "
                "AND name NOT LIKE '%_vec0' "
                "AND name NOT LIKE '%_rtree%' "
                "AND name NOT LIKE '\\_%' ESCAPE '\\'");

            int64_t total_entries = 0;

            for (const auto& table_row : tables) {
                auto it = table_row.find("name");
                if (it == table_row.end() || !std::holds_alternative<std::string>(it->second))
                    continue;

                std::string table_name = std::get<std::string>(it->second);

                // Get column info for this table
                auto cols = db_->query("PRAGMA table_info(" + table_name + ")");

                std::ostringstream json_cols;
                std::ostringstream json_names;
                bool first = true;

                for (const auto& col : cols) {
                    auto name_it = col.find("name");
                    auto type_it = col.find("type");
                    if (name_it == col.end() || !std::holds_alternative<std::string>(name_it->second))
                        continue;

                    std::string col_name = std::get<std::string>(name_it->second);
                    // Skip id and globalId - they're handled separately
                    if (col_name == "id" || col_name == "globalId")
                        continue;

                    std::string col_type;
                    if (type_it != col.end() && std::holds_alternative<std::string>(type_it->second)) {
                        col_type = std::get<std::string>(type_it->second);
                    }

                    if (!first) {
                        json_cols << ", ";
                        json_names << ", ";
                    }
                    first = false;

                    // Wrap BLOB columns with hex() for JSON compatibility
                    if (col_type == "BLOB") {
                        json_cols << "'" << col_name << "', hex(" << col_name << ")";
                    } else {
                        json_cols << "'" << col_name << "', " << col_name;
                    }
                    json_names << "'" << col_name << "'";
                }

                if (first) continue;  // No columns to track

                // Insert audit entries only for rows NOT already in AuditLog
                // A row is considered "in audit log" if there's any entry for that globalRowId
                std::ostringstream sql;
                sql << "INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, "
                    << "changedFields, changedFieldsNames, isSynchronized, timestamp) "
                    << "SELECT '" << table_name << "', 'INSERT', id, globalId, "
                    << "json_object(" << json_cols.str() << "), "
                    << "json_array(" << json_names.str() << "), "
                    << "0, unixepoch('subsec') "
                    << "FROM " << table_name << " t "
                    << "WHERE NOT EXISTS ("
                    << "  SELECT 1 FROM AuditLog a "
                    << "  WHERE a.tableName = '" << table_name << "' "
                    << "  AND a.globalRowId = t.globalId"
                    << ")";

                db_->execute(sql.str());

                // Count how many were inserted (use changes())
                auto changes = db_->query("SELECT changes() as cnt");
                if (!changes.empty()) {
                    auto cnt_it = changes[0].find("cnt");
                    if (cnt_it != changes[0].end() && std::holds_alternative<int64_t>(cnt_it->second)) {
                        total_entries += std::get<int64_t>(cnt_it->second);
                    }
                }
            }

            db_->commit();
            db_->execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

            return total_entries;
        } catch (...) {
            db_->rollback();
            db_->execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");
            throw;
        }
    }

    // Query objects from a table with optional filtering, sorting, and pagination
    // Returns raw row data - caller is responsible for hydrating into managed objects
    std::vector<std::unordered_map<std::string, column_value_t>> query_rows(
        const std::string& table_name,
        std::optional<std::string> where_clause = std::nullopt,
        std::optional<std::string> order_by = std::nullopt,
        std::optional<int64_t> limit = std::nullopt,
        std::optional<int64_t> offset = std::nullopt,
        std::optional<std::string> group_by = std::nullopt) {

        std::ostringstream sql;
        sql << "SELECT * FROM " << table_name;
        if (where_clause && !where_clause->empty()) {
            sql << " WHERE " << *where_clause;
        }
        if (group_by && !group_by->empty()) {
            sql << " GROUP BY " << *group_by;
        }
        if (order_by && !order_by->empty()) {
            sql << " ORDER BY " << *order_by;
        }
        if (limit) {
            sql << " LIMIT " << *limit;
        }
        if (offset) {
            sql << " OFFSET " << *offset;
        }
        return read_db().query(sql.str());
    }

    // Query objects from a table with optional filtering, sorting, and pagination
    // Returns raw row data - caller is responsible for hydrating into managed objects
    std::vector<std::unordered_map<std::string, column_value_t>> query_union_rows(
        const std::vector<std::string>& table_names,
        std::optional<std::string> where_clause = std::nullopt,
        std::optional<std::string> order_by = std::nullopt,
        std::optional<int64_t> limit = std::nullopt,
        std::optional<int64_t> offset = std::nullopt) {

        if (table_names.empty()) {
            return {};
        }

        // Get columns for each table using PRAGMA table_info (name -> type)
        std::vector<std::map<std::string, std::string>> table_columns;
        for (const auto& table_name : table_names) {
            auto pragma_result = read_db().query("PRAGMA table_info(" + table_name + ")");
            std::map<std::string, std::string> cols;
            for (const auto& row : pragma_result) {
                auto name_it = row.find("name");
                auto type_it = row.find("type");
                if (name_it != row.end() && type_it != row.end()) {
                    auto col_name = std::get<std::string>(name_it->second);
                    auto col_type = std::get<std::string>(type_it->second);
                    cols[col_name] = col_type;
                }
            }
            table_columns.push_back(std::move(cols));
        }

        // Find shared columns (intersection where name AND type match)
        std::vector<std::string> shared_columns;
        if (!table_columns.empty()) {
            for (const auto& [name, type] : table_columns[0]) {
                bool shared = true;
                for (size_t i = 1; i < table_columns.size(); i++) {
                    auto it = table_columns[i].find(name);
                    if (it == table_columns[i].end() || it->second != type) {
                        shared = false;
                        break;
                    }
                }
                if (shared) {
                    shared_columns.push_back(name);
                }
            }
        }

        // Inner limit must be offset + limit to ensure we fetch enough rows
        // from each table before the outer query applies the final offset
        std::optional<int64_t> inner_limit = std::nullopt;
        if (limit) {
            inner_limit = *limit + offset.value_or(0);
        }

        std::ostringstream sql;
        sql << "SELECT * FROM ( ";
        for (size_t i = 0; i < table_names.size(); i++) {
            const auto& table_name = table_names[i];

            sql << "SELECT * FROM (";
            sql << "SELECT '" << table_name << "' AS _type";

            // Select only shared columns
            for (const auto& col : shared_columns) {
                sql << ", \"" << col << "\"";
            }

            sql << " FROM " << table_name;
            if (where_clause && !where_clause->empty()) {
                sql << " WHERE " << *where_clause;
            }
            if (order_by && !order_by->empty()) {
                sql << " ORDER BY " << *order_by;
            }
            if (inner_limit) {
                sql << " LIMIT " << *inner_limit;
            }
            sql << " ) ";
            if (i != table_names.size() - 1) {
                sql << " UNION ALL ";
            }
        }
        sql << ")";
        if (order_by && !order_by->empty()) {
            sql << " ORDER BY " << *order_by;
        }
        if (limit) {
            sql << " LIMIT " << *limit;
        }
        if (offset) {
            sql << " OFFSET " << *offset;
        }
        return read_db().query(sql.str());
    }
    
    // Transaction support
    void begin_transaction() { db_->begin_transaction(); }
    void commit() { db_->commit(); }
    void rollback() { db_->rollback(); }

    template<typename F>
    void write(F&& block) {
        begin_transaction();
        try {
            block();
            commit();
        } catch (...) {
            rollback();
            throw;
        }
    }

    void attach(lattice_db& lattice);
    
    database& db() { return *db_; }

    /// Get the read-only database connection (falls back to write connection for in-memory DBs)
    database& read_db() { return read_db_ ? *read_db_ : *db_; }

    /// Close the read-only connection (for operations requiring exclusive access)
    void close_read_db() {
        read_db_.reset();
    }

    /// Reopen the read-only connection after exclusive operations
    void reopen_read_db() {
        if (config_.path != ":memory:" && !config_.read_only) {
            read_db_ = std::make_unique<database>(config_.path, database::open_mode::read_only);
        }
    }

    // Create a link table on demand (public for managed<T*> access)
    void ensure_link_table(const std::string& link_table_name) {
        if (db_->table_exists(link_table_name)) return;

        // Create link table with globalId for sync and PRIMARY KEY to prevent duplicates
        // Matches Lattice.swift's createLinkTable()
        std::string sql = "CREATE TABLE IF NOT EXISTS " + link_table_name + "("
            "lhs TEXT NOT NULL, "
            "rhs TEXT NOT NULL, "
            "globalId TEXT UNIQUE COLLATE NOCASE DEFAULT ("
                "lower(hex(randomblob(4))) || '-' || "
                "lower(hex(randomblob(2))) || '-' || "
                "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
                "substr('89AB', 1 + (abs(random()) % 4), 1) || "
                "substr(lower(hex(randomblob(2))),2) || '-' || "
                "lower(hex(randomblob(6)))"
            "), "
            "PRIMARY KEY(lhs, rhs)"
        ")";
        db_->execute(sql);

        // Create index for efficient lookups
        std::string idx_sql = "CREATE INDEX IF NOT EXISTS idx_" +
            link_table_name + "_lhs ON " + link_table_name + "(lhs)";
        db_->execute(idx_sql);

        // Create audit triggers for sync/observation
        create_link_table_triggers(link_table_name);
    }

    /// Get column types for a table (for sync schema lookup)
    /// Returns map of column_name -> column_type
    std::unordered_map<std::string, column_type> get_table_schema(const std::string& table_name) {
        auto info = db_->get_table_info(table_name);
        std::unordered_map<std::string, column_type> schema;
        for (const auto& [col, sql_type] : info) {
            if (sql_type == "INTEGER") {
                schema[col] = column_type::integer;
            } else if (sql_type == "REAL") {
                schema[col] = column_type::real;
            } else if (sql_type == "BLOB") {
                schema[col] = column_type::blob;
            } else {
                schema[col] = column_type::text;
            }
        }
        return schema;
    }

    // ========================================================================
    // Vector Search API (sqlite-vec integration)
    // ========================================================================

    /// Distance metric for vector search
    enum class distance_metric {
        l2,      // Euclidean distance (default)
        cosine,  // Cosine distance
        l1       // Manhattan distance
    };

    /// Result from a KNN query: globalId + distance
    struct knn_result {
        std::string global_id;
        double distance;
    };

    /// Ensure a vec0 virtual table exists for a vector column.
    /// Table name format: _{ModelTable}_{column}_vec
    /// Dimensions are inferred from first insert.
    /// Also creates triggers to keep vec0 in sync with main table.
    void ensure_vec0_table(const std::string& model_table,
                           const std::string& column_name,
                           int dimensions) {
        std::string vec_table = "_" + model_table + "_" + column_name + "_vec";

        // Check if table already exists
        std::string check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        auto results = db_->query(check_sql, {vec_table});
        if (!results.empty()) {
            // Table exists — verify sync triggers are intact (rebuild_table can drop them)
            auto trig = db_->query(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='"
                + vec_table + "_insert' LIMIT 1");
            if (!trig.empty()) return;
            // Fall through to recreate triggers only
        }

        // Create vec0 virtual table with globalId as primary key (if it doesn't exist)
        if (results.empty()) {
            std::ostringstream sql;
            sql << "CREATE VIRTUAL TABLE " << vec_table << " USING vec0("
                << "global_id TEXT PRIMARY KEY, "
                << "embedding float[" << dimensions << "]"
                << ")";
            db_->execute(sql.str());
        }

        // Create triggers to keep vec0 in sync with main table
        // INSERT trigger
        std::ostringstream insert_trigger;
        insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << vec_table << "_insert "
                       << "AFTER INSERT ON " << model_table << " "
                       << "WHEN NEW." << column_name << " IS NOT NULL "
                       << "BEGIN "
                       << "DELETE FROM " << vec_table << " WHERE global_id = NEW.globalId; "
                       << "INSERT INTO " << vec_table << "(global_id, embedding) "
                       << "VALUES (NEW.globalId, NEW." << column_name << "); "
                       << "END";
        db_->execute(insert_trigger.str());

        // UPDATE trigger
        std::ostringstream update_trigger;
        update_trigger << "CREATE TRIGGER IF NOT EXISTS " << vec_table << "_update "
                       << "AFTER UPDATE OF " << column_name << " ON " << model_table << " "
                       << "WHEN NEW." << column_name << " IS NOT NULL "
                       << "BEGIN "
                       << "DELETE FROM " << vec_table << " WHERE global_id = NEW.globalId; "
                       << "INSERT INTO " << vec_table << "(global_id, embedding) "
                       << "VALUES (NEW.globalId, NEW." << column_name << "); "
                       << "END";
        db_->execute(update_trigger.str());

        // DELETE trigger
        std::ostringstream delete_trigger;
        delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << vec_table << "_delete "
                       << "AFTER DELETE ON " << model_table << " "
                       << "BEGIN "
                       << "DELETE FROM " << vec_table << " WHERE global_id = OLD.globalId; "
                       << "END";
        db_->execute(delete_trigger.str());
    }

    /// Ensure an R*Tree virtual table exists for a geo_bounds column.
    /// Table name format: _{ModelTable}_{column}_rtree
    /// Also creates triggers to keep R*Tree in sync with main table.
    void ensure_rtree_table(const std::string& model_table,
                            const std::string& column_name) {
        std::string rtree_table = "_" + model_table + "_" + column_name + "_rtree";

        // Check if table already exists
        std::string check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
        auto results = db_->query(check_sql, {rtree_table});
        bool table_exists = !results.empty();
        if (table_exists) {
            // Table exists — verify sync triggers are intact (rebuild_table can drop them)
            auto trig = db_->query(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='"
                + rtree_table + "_insert' LIMIT 1");
            if (!trig.empty()) return;
            // Fall through to recreate triggers only
        }

        // Create R*Tree virtual table (if it doesn't exist)
        // Uses row id for joining back to main table
        if (!table_exists) {
            std::ostringstream sql;
            sql << "CREATE VIRTUAL TABLE " << rtree_table << " USING rtree("
                << "id, "           // Matches main table id
                << "minLat, maxLat, "
                << "minLon, maxLon"
                << ")";
            db_->execute(sql.str());
        }

        // Column names in main table
        std::string minLat = column_name + "_minLat";
        std::string maxLat = column_name + "_maxLat";
        std::string minLon = column_name + "_minLon";
        std::string maxLon = column_name + "_maxLon";

        // Create triggers to keep R*Tree in sync with main table
        // INSERT trigger - add to R*Tree when row is inserted with non-null geo data
        std::ostringstream insert_trigger;
        insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_insert "
                       << "AFTER INSERT ON " << model_table << " "
                       << "WHEN NEW." << minLat << " IS NOT NULL "
                       << "BEGIN "
                       << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                       << "VALUES (NEW.id, NEW." << minLat << ", NEW." << maxLat << ", "
                       << "NEW." << minLon << ", NEW." << maxLon << "); "
                       << "END";
        db_->execute(insert_trigger.str());

        // UPDATE trigger - update R*Tree when geo columns change
        std::ostringstream update_trigger;
        update_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_update "
                       << "AFTER UPDATE OF " << minLat << ", " << maxLat << ", "
                       << minLon << ", " << maxLon << " ON " << model_table << " "
                       << "BEGIN "
                       << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                       << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                       << "SELECT NEW.id, NEW." << minLat << ", NEW." << maxLat << ", "
                       << "NEW." << minLon << ", NEW." << maxLon << " "
                       << "WHERE NEW." << minLat << " IS NOT NULL; "
                       << "END";
        db_->execute(update_trigger.str());

        // DELETE trigger - remove from R*Tree when row is deleted
        std::ostringstream delete_trigger;
        delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_delete "
                       << "AFTER DELETE ON " << model_table << " "
                       << "BEGIN "
                       << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                       << "END";
        db_->execute(delete_trigger.str());

        // Populate rtree from existing data (only on first creation)
        if (!table_exists) {
            repopulate_geo_bounds_rtree(model_table, column_name);
        }
    }

    /// Repopulate an rtree table from the main table data.
    /// Clears and repopulates to ensure correct values after migration.
    void repopulate_geo_bounds_rtree(const std::string& model_table,
                                     const std::string& column_name) {
        std::string rtree_table = "_" + model_table + "_" + column_name + "_rtree";
        std::string minLat = column_name + "_minLat";
        std::string maxLat = column_name + "_maxLat";
        std::string minLon = column_name + "_minLon";
        std::string maxLon = column_name + "_maxLon";

        // Clear existing rtree data
        db_->execute("DELETE FROM " + rtree_table);

        // Repopulate from main table
        std::ostringstream populate_sql;
        populate_sql << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                     << "SELECT id, " << minLat << ", " << maxLat << ", " << minLon << ", " << maxLon << " "
                     << "FROM " << model_table << " "
                     << "WHERE " << minLat << " IS NOT NULL";
        db_->execute(populate_sql.str());
    }

    /// Ensure all geo_bounds rtree tables exist for a set of model schemas.
    /// Call this AFTER migration data has been applied to ensure rtrees contain correct data.
    void ensure_geo_bounds_rtrees(const std::vector<model_schema>& schemas) {
        for (const auto& schema : schemas) {
            for (const auto& prop : schema.properties) {
                if (prop.is_geo_bounds && prop.kind != property_kind::list) {
                    ensure_rtree_table(schema.table_name, prop.name);
                }
            }
        }
    }

    /// Ensure an FTS5 virtual table exists for a text column.
    /// Creates external content FTS5 table + INSERT/UPDATE/DELETE triggers.
    void ensure_fts5_table(const std::string& model_table,
                           const std::string& column_name) {
        std::string fts_table = "_" + model_table + "_" + column_name + "_fts";

        // Check if table already exists
        bool table_exists = db_->table_exists(fts_table);
        if (table_exists) {
            // Table exists — verify sync triggers are intact (rebuild_table can drop them)
            auto trig = db_->query(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='"
                + fts_table + "_insert' LIMIT 1");
            if (!trig.empty()) return;
            // Fall through to recreate triggers only
        }

        // Create external content FTS5 virtual table (if it doesn't exist)
        if (!table_exists) {
            std::ostringstream sql;
            sql << "CREATE VIRTUAL TABLE " << fts_table << " USING fts5("
                << column_name << ", "
                << "content='" << model_table << "', "
                << "content_rowid='id', "
                << "tokenize='porter'"
                << ")";
            db_->execute(sql.str());
        }

        // INSERT trigger - copy text to FTS on insert
        std::ostringstream insert_trigger;
        insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << fts_table << "_insert "
                       << "AFTER INSERT ON " << model_table << " "
                       << "BEGIN "
                       << "INSERT INTO " << fts_table << "(rowid, " << column_name << ") "
                       << "VALUES (NEW.id, NEW." << column_name << "); "
                       << "END";
        db_->execute(insert_trigger.str());

        // UPDATE trigger - FTS5 delete-then-insert
        std::ostringstream update_trigger;
        update_trigger << "CREATE TRIGGER IF NOT EXISTS " << fts_table << "_update "
                       << "AFTER UPDATE OF " << column_name << " ON " << model_table << " "
                       << "BEGIN "
                       << "INSERT INTO " << fts_table << "(" << fts_table << ", rowid, " << column_name << ") "
                       << "VALUES ('delete', OLD.id, OLD." << column_name << "); "
                       << "INSERT INTO " << fts_table << "(rowid, " << column_name << ") "
                       << "VALUES (NEW.id, NEW." << column_name << "); "
                       << "END";
        db_->execute(update_trigger.str());

        // DELETE trigger - BEFORE DELETE to use OLD values
        std::ostringstream delete_trigger;
        delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << fts_table << "_delete "
                       << "BEFORE DELETE ON " << model_table << " "
                       << "BEGIN "
                       << "INSERT INTO " << fts_table << "(" << fts_table << ", rowid, " << column_name << ") "
                       << "VALUES ('delete', OLD.id, OLD." << column_name << "); "
                       << "END";
        db_->execute(delete_trigger.str());

        // Populate FTS from existing data (only on first creation)
        if (!table_exists) {
            std::ostringstream populate_sql;
            populate_sql << "INSERT INTO " << fts_table << "(rowid, " << column_name << ") "
                         << "SELECT id, " << column_name << " FROM " << model_table
                         << " WHERE " << column_name << " IS NOT NULL";
            db_->execute(populate_sql.str());
        }
    }

    /// Ensure all FTS5 tables exist for a set of model schemas.
    void ensure_fts5_tables(const std::vector<model_schema>& schemas) {
        for (const auto& schema : schemas) {
            for (const auto& prop : schema.properties) {
                if (prop.is_full_text && prop.type == column_type::text) {
                    ensure_fts5_table(schema.table_name, prop.name);
                }
            }
        }
    }

    /// Ensure a geo_bounds list table exists with its R*Tree.
    /// Creates table: _<ModelTable>_<column> with parent_id and geo bounds columns
    /// Creates R*Tree: _<ModelTable>_<column>_rtree for spatial indexing
    void ensure_geo_bounds_list_table(const std::string& model_table,
                                      const std::string& column_name) {
        std::string list_table = "_" + model_table + "_" + column_name;
        std::string rtree_table = list_table + "_rtree";

        // Check if table already exists
        if (db_->table_exists(list_table)) {
            return;
        }

        // Create geo_bounds list table
        // Uses parent_id (globalId of parent) for relationship, like link tables
        std::ostringstream sql;
        sql << "CREATE TABLE IF NOT EXISTS " << list_table << "("
            << "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            << "parent_id TEXT NOT NULL, "  // globalId of parent row
            << "minLat REAL NOT NULL, "
            << "maxLat REAL NOT NULL, "
            << "minLon REAL NOT NULL, "
            << "maxLon REAL NOT NULL, "
            << "globalId TEXT UNIQUE COLLATE NOCASE DEFAULT ("
            << "lower(hex(randomblob(4))) || '-' || "
            << "lower(hex(randomblob(2))) || '-' || "
            << "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
            << "substr('89AB', 1 + (abs(random()) % 4), 1) || "
            << "substr(lower(hex(randomblob(2))),2) || '-' || "
            << "lower(hex(randomblob(6)))"
            << ")"
            << ")";
        db_->execute(sql.str());

        // Create index for efficient parent lookups
        std::string idx_sql = "CREATE INDEX IF NOT EXISTS idx_" +
            list_table + "_parent ON " + list_table + "(parent_id)";
        db_->execute(idx_sql);

        // Create R*Tree virtual table for spatial indexing (if it doesn't exist)
        if (!db_->table_exists(rtree_table)) {
            std::ostringstream rtree_sql;
            rtree_sql << "CREATE VIRTUAL TABLE " << rtree_table << " USING rtree("
                      << "id, "  // Matches list table id
                      << "minLat, maxLat, "
                      << "minLon, maxLon"
                      << ")";
            db_->execute(rtree_sql.str());
        }

        // Create triggers to keep R*Tree in sync with list table
        // INSERT trigger
        std::ostringstream insert_trigger;
        insert_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_insert "
                       << "AFTER INSERT ON " << list_table << " "
                       << "BEGIN "
                       << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                       << "VALUES (NEW.id, NEW.minLat, NEW.maxLat, NEW.minLon, NEW.maxLon); "
                       << "END";
        db_->execute(insert_trigger.str());

        // UPDATE trigger
        std::ostringstream update_trigger;
        update_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_update "
                       << "AFTER UPDATE OF minLat, maxLat, minLon, maxLon ON " << list_table << " "
                       << "BEGIN "
                       << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                       << "INSERT INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                       << "VALUES (NEW.id, NEW.minLat, NEW.maxLat, NEW.minLon, NEW.maxLon); "
                       << "END";
        db_->execute(update_trigger.str());

        // DELETE trigger
        std::ostringstream delete_trigger;
        delete_trigger << "CREATE TRIGGER IF NOT EXISTS " << rtree_table << "_delete "
                       << "AFTER DELETE ON " << list_table << " "
                       << "BEGIN "
                       << "DELETE FROM " << rtree_table << " WHERE id = OLD.id; "
                       << "END";
        db_->execute(delete_trigger.str());

        // Create audit triggers for sync
        create_geo_bounds_list_triggers(list_table);
    }

    /// Create audit triggers for a geo_bounds list table (for sync/observation)
    void create_geo_bounds_list_triggers(const std::string& list_table) {
        // INSERT trigger for AuditLog
        std::string insert_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + list_table + "Insert"
            " AFTER INSERT ON " + list_table +
            " WHEN NOT sync_disabled()"
            " BEGIN"
            "   INSERT INTO AuditLog(tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames)"
            "   VALUES("
            "       '" + list_table + "',"
            "       'INSERT',"
            "       NEW.id,"
            "       NEW.globalId,"
            "       json_object("
            "           'parent_id', json_object('kind', 2, 'value', NEW.parent_id),"
            "           'minLat', json_object('kind', 3, 'value', NEW.minLat),"
            "           'maxLat', json_object('kind', 3, 'value', NEW.maxLat),"
            "           'minLon', json_object('kind', 3, 'value', NEW.minLon),"
            "           'maxLon', json_object('kind', 3, 'value', NEW.maxLon)"
            "       ),"
            "       json_array('parent_id', 'minLat', 'maxLat', 'minLon', 'maxLon')"
            "   );"
            " END";
        db_->execute(insert_trigger);

        // DELETE trigger for AuditLog
        std::string delete_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + list_table + "Delete"
            " AFTER DELETE ON " + list_table +
            " WHEN NOT sync_disabled()"
            " BEGIN"
            "   INSERT INTO AuditLog(tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames)"
            "   VALUES("
            "       '" + list_table + "',"
            "       'DELETE',"
            "       OLD.id,"
            "       OLD.globalId,"
            "       '{}',"
            "       '[]'"
            "   );"
            " END";
        db_->execute(delete_trigger);
    }

    /// Update or insert a vector into the vec0 table.
    /// vector_data should be packed float32 bytes (4 bytes per dimension).
    void upsert_vec0(const std::string& model_table,
                     const std::string& column_name,
                     const std::string& global_id,
                     const std::vector<uint8_t>& vector_data) {
        std::string vec_table = "_" + model_table + "_" + column_name + "_vec";

        // Infer dimensions and ensure table exists
        int dimensions = static_cast<int>(vector_data.size() / sizeof(float));
        if (dimensions > 0) {
            ensure_vec0_table(model_table, column_name, dimensions);
        }

        // Delete existing entry if any (vec0 doesn't support ON CONFLICT)
        std::string delete_sql = "DELETE FROM " + vec_table + " WHERE global_id = ?";
        db_->execute(delete_sql, {global_id});

        // Insert new vector
        std::string insert_sql = "INSERT INTO " + vec_table + "(global_id, embedding) VALUES (?, ?)";

        // Create blob from vector data
        std::vector<uint8_t> blob_copy = vector_data;
        db_->execute(insert_sql, {global_id, blob_copy});
    }

    /// Delete a vector from the vec0 table.
    void delete_vec0(const std::string& model_table,
                     const std::string& column_name,
                     const std::string& global_id) {
        std::string vec_table = "_" + model_table + "_" + column_name + "_vec";
        std::string sql = "DELETE FROM " + vec_table + " WHERE global_id = ?";
        try {
            db_->execute(sql, {global_id});
        } catch (...) {
            // Table might not exist yet, ignore
        }
    }

    /// Perform a KNN (K-Nearest Neighbors) query.
    /// Returns up to k results sorted by distance.
    /// Optional where_clause filters on the main model table (e.g., "category = 'foo'").
    std::vector<knn_result> knn_query(const std::string& model_table,
                                       const std::string& column_name,
                                       const std::vector<uint8_t>& query_vector,
                                       int k,
                                       distance_metric metric = distance_metric::l2,
                                       const std::optional<std::string>& where_clause = std::nullopt) {
        std::string vec_table = "_" + model_table + "_" + column_name + "_vec";

        // vec0 table is created lazily on first vector insert (needs dimensions).
        // If no data has been inserted yet, return empty results.
        if (!db_->table_exists(vec_table)) {
            return {};
        }

        // Build the KNN query
        // If there's a where clause, we need to JOIN with the main table
        std::ostringstream sql;
        std::string dist_func;
        switch (metric) {
            case distance_metric::cosine: dist_func = "vec_distance_cosine"; break;
            case distance_metric::l1: dist_func = "vec_distance_L1"; break;
            default: dist_func = "vec_distance_L2"; break;
        }

        if (where_clause && !where_clause->empty()) {
            // With filter: JOIN main table and apply WHERE clause
            // Use explicit distance function for consistent behavior
            // Don't alias model table so column names in predicate resolve naturally
            sql << "SELECT v.global_id, " << dist_func << "(v.embedding, ?) as distance "
                << "FROM " << vec_table << " v "
                << "JOIN " << model_table << " ON " << model_table << ".globalId = v.global_id "
                << "WHERE " << *where_clause << " "
                << "ORDER BY distance LIMIT " << k;
        } else if (metric == distance_metric::l2) {
            // No filter, L2: use vec0's native MATCH for fastest performance
            sql << "SELECT global_id, distance FROM " << vec_table
                << " WHERE embedding MATCH ? AND k = " << k;
        } else {
            // No filter, non-L2: use explicit distance function
            sql << "SELECT global_id, " << dist_func << "(embedding, ?) as distance "
                << "FROM " << vec_table
                << " ORDER BY distance LIMIT " << k;
        }

        auto rows = db_->query(sql.str(), {query_vector});

        std::vector<knn_result> results;
        results.reserve(rows.size());
        for (const auto& row : rows) {
            knn_result r;
            auto gid_it = row.find("global_id");
            if (gid_it != row.end() && std::holds_alternative<std::string>(gid_it->second)) {
                r.global_id = std::get<std::string>(gid_it->second);
            }
            auto dist_it = row.find("distance");
            if (dist_it != row.end()) {
                if (std::holds_alternative<double>(dist_it->second)) {
                    r.distance = std::get<double>(dist_it->second);
                } else if (std::holds_alternative<int64_t>(dist_it->second)) {
                    r.distance = static_cast<double>(std::get<int64_t>(dist_it->second));
                }
            }
            results.push_back(r);
        }
        return results;
    }

private:
    template<typename U> friend class query;
    template<typename U> friend class results;

    configuration config_;
    std::unique_ptr<database> db_;       // Write connection
    std::unique_ptr<database> read_db_;  // Read-only connection for concurrent reads
    std::shared_ptr<scheduler> scheduler_;
    std::unique_ptr<synchronizer> synchronizer_;

    // Sync callbacks
    std::function<void(bool)> on_sync_state_change_;
    std::function<void(const std::string&)> on_sync_error_;

    // Synchronizer flag - true if this connection handles remote changes
    bool is_synchronizer_ = false;

    // Change buffering - accumulates changes until WAL hook fires
    std::mutex change_buffer_mutex_;
    std::vector<std::tuple<std::string, std::string, int64_t, std::string>> change_buffer_;  // (table, op, rowId, globalId)
    bool is_flushing_ = false;

    // Initialize synchronizer if configured
    void setup_sync_if_configured();

    // Table-level observer storage (for Results observation)
    std::mutex observers_mutex_;
    observer_id next_observer_id_ = 1;
    std::map<std::string, std::map<observer_id, std::function<void(const std::string&, int64_t, const std::string&)>>> table_observers_;

    // Per-object observer storage (for individual model observation)
    // Maps: tableName -> rowId -> [observer callbacks]
    std::mutex object_observers_mutex_;
    std::map<std::string, std::map<int64_t, std::vector<std::pair<observer_id, std::function<void(const std::string&)>>>>> object_observers_;

    // Setup hooks for change notifications
    // Update hook buffers changes, WAL hook flushes on commit (matches Swift's pattern)
    void setup_change_hook();

    // Cross-process observation
    std::unique_ptr<cross_process_notifier> xproc_notifier_;
    int64_t last_seen_audit_id_ = 0;
    void setup_cross_process_notifier();
public:
    void handle_cross_process_notification();
private:

    void ensure_tables() {
        // First create sync control table and register sync_disabled() function
        ensure_sync_control_table();

        // Create meta table for schema versioning
        ensure_lattice_meta_table();

        // Create AuditLog BEFORE model tables (triggers reference it)
        ensure_audit_log_table();

        // Phase 1: Detect all schema changes (but don't apply yet)
        migration_context migration_ctx(*db_);
        std::vector<const model_schema*> new_tables;
        std::vector<const model_schema*> existing_tables;

        for (const auto* schema : schema_registry::instance().all_schemas()) {
            if (!db_->table_exists(schema->table_name)) {
                new_tables.push_back(schema);
            } else {
                existing_tables.push_back(schema);
                // Detect changes for this table
                auto changes = detect_table_changes(*schema);
                if (changes.has_changes()) {
                    migration_ctx.add_table_changes(std::move(changes));
                }
            }
        }

        // Phase 2: If migration block is set and there are changes, call it
        if (config_.migration_block && migration_ctx.has_any_changes()) {
            config_.migration_block(migration_ctx);
        }

        // Phase 3: Create new tables
        for (const auto* schema : new_tables) {
            create_model_table(*schema);
        }

        // Phase 4: Apply auto-migration to existing tables
        for (const auto* schema : existing_tables) {
            migrate_model_table(*schema);
        }

        // Phase 5: Apply pending updates from migration block
        // (now that new columns exist)
        migration_ctx.apply_pending_updates();

        ensure_link_tables();
    }

    /// Detect schema changes for a table without applying them.
    /// Returns table_changes describing what would change.
    table_changes detect_table_changes(const model_schema& schema) {
        table_changes changes;
        changes.table_name = schema.table_name;

        // Get existing columns from database
        auto existing = db_->get_table_info(schema.table_name);

        // Build expected columns map
        std::unordered_map<std::string, std::string> model_cols;
        model_cols["id"] = "INTEGER";
        model_cols["globalId"] = "TEXT";
        for (const auto& prop : schema.properties) {
            if (prop.is_geo_bounds) {
                model_cols[prop.name + "_minLat"] = "REAL";
                model_cols[prop.name + "_maxLat"] = "REAL";
                model_cols[prop.name + "_minLon"] = "REAL";
                model_cols[prop.name + "_maxLon"] = "REAL";
            } else {
                model_cols[prop.name] = sql_type_string(prop.type);
            }
        }

        // Find added columns
        for (const auto& [col, type] : model_cols) {
            auto it = existing.find(col);
            if (it == existing.end()) {
                changes.added_columns.push_back(col);
            } else if (it->second != type) {
                changes.changed_columns.push_back(col);
            }
        }

        // Find removed columns
        for (const auto& [col, type] : existing) {
            if (model_cols.find(col) == model_cols.end()) {
                changes.removed_columns.push_back(col);
            }
        }

        return changes;
    }

    void create_model_table(const model_schema& schema) {
        // Create table with SQL-generated UUID default for globalId
        // This matches Lattice.swift's table creation
        std::ostringstream sql;
        sql << "CREATE TABLE IF NOT EXISTS " << schema.table_name << "(";
        sql << "id INTEGER PRIMARY KEY AUTOINCREMENT, ";
        sql << "globalId TEXT UNIQUE COLLATE NOCASE DEFAULT ("
               "lower(hex(randomblob(4))) || '-' || "
               "lower(hex(randomblob(2))) || '-' || "
               "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
               "substr('89AB', 1 + (abs(random()) % 4), 1) || "
               "substr(lower(hex(randomblob(2))),2) || '-' || "
               "lower(hex(randomblob(6)))"
               ")";

        // Collect column names and types for triggers
        std::vector<std::pair<std::string, column_type>> columns;

        for (const auto& prop : schema.properties) {
            // Skip list properties - they use separate tables (link_list, geo_bounds_list)
            if (prop.kind == property_kind::list) {
                continue;
            }
            if (prop.is_geo_bounds) {
                // geo_bounds expands to 4 REAL columns
                sql << ", " << prop.name << "_minLat REAL";
                sql << ", " << prop.name << "_maxLat REAL";
                sql << ", " << prop.name << "_minLon REAL";
                sql << ", " << prop.name << "_maxLon REAL";
                // Add all 4 columns to trigger list
                columns.push_back({prop.name + "_minLat", column_type::real});
                columns.push_back({prop.name + "_maxLat", column_type::real});
                columns.push_back({prop.name + "_minLon", column_type::real});
                columns.push_back({prop.name + "_maxLon", column_type::real});
            } else {
                sql << ", " << prop.name << " ";
                switch (prop.type) {
                    case column_type::integer: sql << "INTEGER"; break;
                    case column_type::real: sql << "REAL"; break;
                    case column_type::text: sql << "TEXT"; break;
                    case column_type::blob: sql << "BLOB"; break;
                }
                if (!prop.nullable) {
                    sql << " NOT NULL";
                }
                columns.push_back({prop.name, prop.type});
            }
        }
        sql << ")";
        db_->execute(sql.str());

        // Create audit triggers for sync/observation
        create_model_table_triggers(schema.table_name, columns);

        // Create R*Tree tables for geo_bounds properties
        for (const auto& prop : schema.properties) {
            if (prop.is_geo_bounds) {
                if (prop.kind == property_kind::list) {
                    // geo_bounds list - create separate table with R*Tree
                    ensure_geo_bounds_list_table(schema.table_name, prop.name);
                } else {
                    // Single geo_bounds - create R*Tree for main table columns
                    ensure_rtree_table(schema.table_name, prop.name);
                }
            }
        }
    }

    std::string sql_type_string(column_type type) {
        switch (type) {
            case column_type::integer: return "INTEGER";
            case column_type::real: return "REAL";
            case column_type::text: return "TEXT";
            case column_type::blob: return "BLOB";
        }
        return "TEXT";
    }

    void migrate_model_table(const model_schema& schema) {
        // Get existing columns from database
        auto existing = db_->get_table_info(schema.table_name);

        // Build model schema map: column_name -> SQL_TYPE
        // geo_bounds properties expand to 4 columns (but not geo_bounds lists)
        std::unordered_map<std::string, std::string> model_cols;
        model_cols["id"] = "INTEGER";
        model_cols["globalId"] = "TEXT";
        for (const auto& prop : schema.properties) {
            // Skip list properties - they use separate tables (link_list, geo_bounds_list)
            if (prop.kind == property_kind::list) {
                continue;
            }
            if (prop.is_geo_bounds) {
                model_cols[prop.name + "_minLat"] = "REAL";
                model_cols[prop.name + "_maxLat"] = "REAL";
                model_cols[prop.name + "_minLon"] = "REAL";
                model_cols[prop.name + "_maxLon"] = "REAL";
            } else {
                model_cols[prop.name] = sql_type_string(prop.type);
            }
        }

        // Find added, removed, and changed columns
        std::vector<std::string> added;
        std::vector<std::string> removed;
        std::vector<std::string> changed;

        for (const auto& [col, type] : model_cols) {
            auto it = existing.find(col);
            if (it == existing.end()) {
                added.push_back(col);
            } else if (it->second != type) {
                changed.push_back(col);
            }
        }
        for (const auto& [col, type] : existing) {
            if (model_cols.find(col) == model_cols.end()) {
                removed.push_back(col);
            }
        }

        // No changes needed
        if (added.empty() && removed.empty() && changed.empty()) {
            // Still ensure geo_bounds LIST tables exist (they're separate join tables)
            // Single geo_bounds rtrees are created in Phase 6 after all migrations
            for (const auto& prop : schema.properties) {
                if (prop.is_geo_bounds && prop.kind == property_kind::list) {
                    ensure_geo_bounds_list_table(schema.table_name, prop.name);
                }
            }
            // Retroactive safety: ensure audit triggers exist — a previous bug in
            // rebuild_table could silently drop them during schema migration.
            ensure_audit_triggers(schema);
            return;
        }

        // Add new columns with ALTER TABLE
        bool columns_added = false;
        bool geo_bounds_added = false;
        for (const auto& col : added) {
            if (col == "id" || col == "globalId") continue;  // Built-in columns

            // Check if this is a geo_bounds column (ends with _minLat, _maxLat, etc.)
            bool is_geo_col = false;
            std::string geo_prop_name;
            for (const auto& prop : schema.properties) {
                if (prop.is_geo_bounds) {
                    if (col == prop.name + "_minLat" || col == prop.name + "_maxLat" ||
                        col == prop.name + "_minLon" || col == prop.name + "_maxLon") {
                        is_geo_col = true;
                        geo_prop_name = prop.name;
                        break;
                    }
                }
            }

            if (is_geo_col) {
                // Add geo_bounds column (nullable REAL)
                std::string sql = "ALTER TABLE " + schema.table_name +
                    " ADD COLUMN " + col + " REAL";
                db_->execute(sql);
                columns_added = true;
                geo_bounds_added = true;
            } else {
                // Find the property to get its type and default
                for (const auto& prop : schema.properties) {
                    if (prop.name == col) {
                        std::string sql = "ALTER TABLE " + schema.table_name +
                            " ADD COLUMN " + col + " " + sql_type_string(prop.type);
                        if (!prop.nullable) {
                            // For NOT NULL columns, need a DEFAULT
                            switch (prop.type) {
                                case column_type::integer: sql += " DEFAULT 0"; break;
                                case column_type::real: sql += " DEFAULT 0.0"; break;
                                case column_type::text: sql += " DEFAULT ''"; break;
                                case column_type::blob: sql += " DEFAULT X''"; break;
                            }
                        }
                        db_->execute(sql);
                        columns_added = true;
                        break;
                    }
                }
            }
        }

        // Ensure geo_bounds LIST tables exist (these are separate join tables, not affected by migration timing)
        for (const auto& prop : schema.properties) {
            if (prop.is_geo_bounds && prop.kind == property_kind::list) {
                ensure_geo_bounds_list_table(schema.table_name, prop.name);
            }
        }

        // NOTE: rtree creation for single geo_bounds properties is deferred.
        // During migration, columns are added first, then data is updated via apply_pending_updates(),
        // and finally ensure_geo_bounds_rtrees() is called to create rtree tables with correct data.

        // If columns were removed or changed type, need to rebuild the table
        // (DROP COLUMN has limitations in SQLite, so always use rebuild for safety)
        if (!removed.empty() || !changed.empty()) {
            rebuild_table(schema, existing, model_cols);
        } else if (columns_added) {
            // Recreate triggers to include new columns in audit logging
            recreate_model_table_triggers(schema);
        }
    }

    void ensure_audit_triggers(const model_schema& schema) {
        // Quick check: if the insert trigger exists, all three do.
        auto rows = db_->query(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='Audit"
            + schema.table_name + "Insert' LIMIT 1");
        if (rows.empty()) {
            recreate_model_table_triggers(schema);
        }
    }

    void recreate_model_table_triggers(const model_schema& schema) {
        // Drop existing triggers
        drop_model_table_triggers(schema.table_name);

        // Build column list for triggers
        // geo_bounds properties expand to 4 columns
        std::vector<std::pair<std::string, column_type>> columns;
        for (const auto& prop : schema.properties) {
            if (prop.kind == property_kind::primitive) {
                if (prop.is_geo_bounds) {
                    columns.emplace_back(prop.name + "_minLat", column_type::real);
                    columns.emplace_back(prop.name + "_maxLat", column_type::real);
                    columns.emplace_back(prop.name + "_minLon", column_type::real);
                    columns.emplace_back(prop.name + "_maxLon", column_type::real);
                } else {
                    columns.emplace_back(prop.name, prop.type);
                }
            }
        }

        // Create new triggers with all columns
        create_model_table_triggers(schema.table_name, columns);
    }

    void rebuild_table(const model_schema& schema,
                       const std::unordered_map<std::string, std::string>& existing,
                       const std::unordered_map<std::string, std::string>& model_cols) {
        const std::string& table = schema.table_name;
        std::string tmp = table + "_old";

        // 1. Drop ALL triggers on this table before rename — SQLite preserves
        //    trigger names after ALTER TABLE RENAME, so CREATE TRIGGER IF NOT
        //    EXISTS would skip creation, leaving the new table without triggers
        //    once the old table (and its triggers) are dropped. This affects
        //    AuditLog, FTS5, vec0, and R*Tree sync triggers.
        {
            auto trigger_rows = db_->query(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='"
                + table + "'");
            for (const auto& row : trigger_rows) {
                db_->execute("DROP TRIGGER IF EXISTS "
                             + std::get<std::string>(row.at("name")));
            }
        }

        // 2. Rename existing table
        db_->execute("ALTER TABLE " + table + " RENAME TO " + tmp);

        // 3. Create new table with correct schema (including fresh triggers)
        create_model_table(schema);

        // 4. Build column lists for INSERT
        // - dest_cols: column names for the INSERT INTO clause
        // - src_exprs: expressions for the SELECT clause (column name or default value)
        std::vector<std::string> dest_cols;
        std::vector<std::string> src_exprs;

        dest_cols.push_back("id");
        dest_cols.push_back("globalId");
        src_exprs.push_back("id");
        src_exprs.push_back("globalId");

        for (const auto& prop : schema.properties) {
            // Skip list properties - they use separate tables (link_list, geo_bounds_list)
            if (prop.kind == property_kind::list) {
                continue;
            }
            if (prop.is_geo_bounds) {
                // geo_bounds expands to 4 columns
                std::array<std::string, 4> suffixes = {"_minLat", "_maxLat", "_minLon", "_maxLon"};
                for (const auto& suffix : suffixes) {
                    std::string col_name = prop.name + suffix;
                    dest_cols.push_back(col_name);
                    if (existing.find(col_name) != existing.end()) {
                        src_exprs.push_back(col_name);
                    } else {
                        src_exprs.push_back("0.0");  // Default for REAL geo columns
                    }
                }
            } else {
                dest_cols.push_back(prop.name);
                if (existing.find(prop.name) != existing.end()) {
                    // Column exists in old table - copy it
                    src_exprs.push_back(prop.name);
                } else {
                    // New column - use default value
                    std::string default_val;
                    switch (prop.type) {
                        case column_type::integer: default_val = "0"; break;
                        case column_type::real: default_val = "0.0"; break;
                        case column_type::text: default_val = "''"; break;
                        case column_type::blob: default_val = "X''"; break;
                    }
                    src_exprs.push_back(default_val);
                }
            }
        }

        // 5. Copy data with defaults for new columns
        std::string dest_str = dest_cols[0];
        std::string src_str = src_exprs[0];
        for (size_t i = 1; i < dest_cols.size(); ++i) {
            dest_str += ", " + dest_cols[i];
            src_str += ", " + src_exprs[i];
        }
        db_->execute("INSERT INTO " + table + " (" + dest_str + ") SELECT " + src_str + " FROM " + tmp);

        // 6. Drop old table
        db_->execute("DROP TABLE " + tmp);

        // 7. Populate rtree tables for geo_bounds properties (data now exists)
        for (const auto& prop : schema.properties) {
            if (prop.is_geo_bounds && prop.kind != property_kind::list) {
                std::string rtree_table = "_" + table + "_" + prop.name + "_rtree";
                std::string minLat = prop.name + "_minLat";
                std::string maxLat = prop.name + "_maxLat";
                std::string minLon = prop.name + "_minLon";
                std::string maxLon = prop.name + "_maxLon";

                std::ostringstream populate_sql;
                populate_sql << "INSERT OR IGNORE INTO " << rtree_table << "(id, minLat, maxLat, minLon, maxLon) "
                             << "SELECT id, " << minLat << ", " << maxLat << ", " << minLon << ", " << maxLon << " "
                             << "FROM " << table << " "
                             << "WHERE " << minLat << " IS NOT NULL";
                db_->execute(populate_sql.str());
            }
        }
    }

    void ensure_link_tables() {
        // Link tables are now created on-demand when first used
        // No need for manual registration
    }

    void ensure_audit_log_table() {
        // Create AuditLog table matching Lattice.swift's schema
        std::string sql = R"(
            CREATE TABLE IF NOT EXISTS AuditLog(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE COLLATE NOCASE DEFAULT (
                    lower(hex(randomblob(4)))   || '-' ||
                    lower(hex(randomblob(2)))   || '-' ||
                    '4' || substr(lower(hex(randomblob(2))),2) || '-' ||
                    substr('89AB', 1 + (abs(random()) % 4), 1) ||
                      substr(lower(hex(randomblob(2))),2)     || '-' ||
                    lower(hex(randomblob(6)))
                ),
                tableName TEXT,
                operation TEXT,
                rowId INTEGER,
                globalRowId TEXT,
                changedFields TEXT,
                changedFieldsNames TEXT,
                isFromRemote INTEGER DEFAULT 0,
                isSynchronized INTEGER DEFAULT 0,
                timestamp REAL DEFAULT (unixepoch('subsec'))
            )
        )";
        db_->execute(sql);
    }

    void ensure_sync_control_table() {
        // Create _SyncControl table for temporarily disabling sync
        db_->execute(R"(
            CREATE TABLE IF NOT EXISTS _SyncControl (
                id INTEGER PRIMARY KEY CHECK(id=1),
                disabled INTEGER NOT NULL DEFAULT 0
            )
        )");
        db_->execute("INSERT OR IGNORE INTO _SyncControl(id, disabled) VALUES(1, 0)");

        // Create sync_disabled() SQL function
        // This allows triggers to check if sync is disabled
        sqlite3_create_function(
            db_->handle(),
            "sync_disabled",
            0,  // No arguments
            SQLITE_UTF8,
            db_->handle(),  // Pass db handle as user data
            [](sqlite3_context* ctx, int, sqlite3_value**) {
                sqlite3* db = static_cast<sqlite3*>(sqlite3_user_data(ctx));
                sqlite3_stmt* stmt = nullptr;
                int disabled = 0;

                if (sqlite3_prepare_v2(db, "SELECT disabled FROM _SyncControl WHERE id=1", -1, &stmt, nullptr) == SQLITE_OK) {
                    if (sqlite3_step(stmt) == SQLITE_ROW) {
                        disabled = sqlite3_column_int(stmt, 0);
                    }
                    sqlite3_finalize(stmt);
                }
                sqlite3_result_int(ctx, disabled);
            },
            nullptr,
            nullptr
        );
    }

protected:
    // ========================================================================
    // Schema Version Management (protected for swift_lattice access)
    // ========================================================================

    void ensure_lattice_meta_table() {
        db_->execute(R"(
            CREATE TABLE IF NOT EXISTS _lattice_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        )");
        // Initialize schema version to 1 if not set
        db_->execute("INSERT OR IGNORE INTO _lattice_meta(key, value) VALUES('schema_version', '1')");
    }

    /// Store a schema snapshot as JSON for a specific version.
    /// This allows us to recreate old schema during incremental migrations.
    void store_schema_snapshot(int version, const std::vector<model_schema>& schemas) {
        std::ostringstream json;
        json << "{";
        bool first_table = true;
        for (const auto& schema : schemas) {
            if (!first_table) json << ",";
            first_table = false;
            json << "\"" << schema.table_name << "\":{";
            bool first_prop = true;
            for (const auto& prop : schema.properties) {
                if (!first_prop) json << ",";
                first_prop = false;
                json << "\"" << prop.name << "\":{";
                json << "\"type\":" << static_cast<int>(prop.type) << ",";
                json << "\"kind\":" << static_cast<int>(prop.kind) << ",";
                json << "\"nullable\":" << (prop.nullable ? "true" : "false") << ",";
                json << "\"is_vector\":" << (prop.is_vector ? "true" : "false") << ",";
                json << "\"is_geo_bounds\":" << (prop.is_geo_bounds ? "true" : "false");
                if (!prop.target_table.empty()) {
                    json << ",\"target_table\":\"" << prop.target_table << "\"";
                }
                if (!prop.link_table.empty()) {
                    json << ",\"link_table\":\"" << prop.link_table << "\"";
                }
                json << "}";
            }
            json << "}";
        }
        json << "}";

        // Escape single quotes for SQL
        std::string json_str = json.str();
        std::string escaped;
        for (char c : json_str) {
            if (c == '\'') escaped += "''";
            else escaped += c;
        }

        db_->execute("INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES('schema_v"
                     + std::to_string(version) + "', '" + escaped + "')");
    }

    /// Get the schema snapshot for a specific version.
    /// Returns empty string if not found.
    std::string get_schema_snapshot(int version) {
        auto results = db_->query("SELECT value FROM _lattice_meta WHERE key = 'schema_v"
                                  + std::to_string(version) + "'");
        if (!results.empty()) {
            return std::get<std::string>(results[0].at("value"));
        }
        return "";
    }

    void drop_model_table_triggers(const std::string& table_name) {
        db_->execute("DROP TRIGGER IF EXISTS AuditLog_Update_" + table_name);
        db_->execute("DROP TRIGGER IF EXISTS Audit" + table_name + "Insert");
        db_->execute("DROP TRIGGER IF EXISTS Audit" + table_name + "Delete");
    }

    void create_model_table_triggers(const std::string& table_name,
                                      const std::vector<std::pair<std::string, column_type>>& columns) {
        // Skip if no columns to track
        if (columns.empty()) return;

        // Helper to wrap BLOB columns with hex() for JSON compatibility
        auto value_expr = [](const std::string& prefix, const std::string& col, column_type type) {
            if (type == column_type::blob) {
                return "hex(" + prefix + "." + col + ")";
            }
            return prefix + "." + col;
        };

        // Build the changed fields comparison for UPDATE trigger
        std::string update_when_clause;
        std::string json_fields;
        std::string json_names;

        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& [col, type] = columns[i];
            if (i > 0) {
                update_when_clause += " OR ";
                json_fields += ",";
                json_names += ",";
            }
            update_when_clause += "OLD." + col + " IS NOT NEW." + col;
            // For UPDATE: only include changed fields (simple values like Swift's format)
            json_fields += "'" + col + "', "
                "CASE WHEN OLD." + col + " IS NOT NEW." + col + " THEN " + value_expr("NEW", col, type) + " ELSE NULL END";
            json_names += "CASE WHEN OLD." + col + " IS NOT NEW." + col + " THEN '" + col + "' ELSE NULL END";
        }

        // INSERT JSON (all fields)
        std::string insert_json_fields;
        std::string insert_json_names;
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& [col, type] = columns[i];
            if (i > 0) {
                insert_json_fields += ",";
                insert_json_names += ",";
            }
            insert_json_fields += "'" + col + "', " + value_expr("NEW", col, type);
            insert_json_names += "'" + col + "'";
        }

        // DELETE JSON (all old fields)
        std::string delete_json_fields;
        std::string delete_json_names;
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& [col, type] = columns[i];
            if (i > 0) {
                delete_json_fields += ",";
                delete_json_names += ",";
            }
            delete_json_fields += "'" + col + "', " + value_expr("OLD", col, type);
            delete_json_names += "'" + col + "'";
        }

        // UPDATE trigger
        std::string update_trigger = "CREATE TRIGGER IF NOT EXISTS AuditLog_Update_" + table_name +
            " AFTER UPDATE ON " + table_name +
            " WHEN ((sync_disabled() = 0) AND (" + update_when_clause + "))"
            " BEGIN"
            "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
            "   VALUES ("
            "       '" + table_name + "',"
            "       'UPDATE',"
            "       OLD.id,"
            "       OLD.globalId,"
            "       json_object(" + json_fields + "),"
            "       json_array(" + json_names + "),"
            "       unixepoch('subsec')"
            "   );"
            " END";
        db_->execute(update_trigger);

        // INSERT trigger
        std::string insert_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + table_name + "Insert"
            " AFTER INSERT ON " + table_name +
            " WHEN (sync_disabled() = 0)"
            " BEGIN"
            "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
            "   VALUES ("
            "       '" + table_name + "',"
            "       'INSERT',"
            "       NEW.id,"
            "       NEW.globalId,"
            "       json_object(" + insert_json_fields + "),"
            "       json_array(" + insert_json_names + "),"
            "       unixepoch('subsec')"
            "   );"
            " END";
        db_->execute(insert_trigger);

        // DELETE trigger
        std::string delete_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + table_name + "Delete"
            " AFTER DELETE ON " + table_name +
            " WHEN (sync_disabled() = 0)"
            " BEGIN"
            "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
            "   VALUES ("
            "       '" + table_name + "',"
            "       'DELETE',"
            "       OLD.id,"
            "       OLD.globalId,"
            "       json_object(" + delete_json_fields + "),"
            "       json_array(" + delete_json_names + "),"
            "       unixepoch('subsec')"
            "   );"
            " END";
        db_->execute(delete_trigger);
    }

    void create_link_table_triggers(const std::string& link_table_name) {
        // INSERT trigger for link table
        std::string insert_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + link_table_name + "Insert"
            " AFTER INSERT ON " + link_table_name +
            " WHEN (sync_disabled() = 0)"
            " BEGIN"
            "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
            "   VALUES ("
            "       '" + link_table_name + "',"
            "       'INSERT',"
            "       0,"
            "       NEW.globalId,"
            "       json_object('lhs', NEW.lhs, 'rhs', NEW.rhs),"
            "       json_array('lhs', 'rhs'),"
            "       unixepoch('subsec')"
            "   );"
            " END";
        db_->execute(insert_trigger);

        // DELETE trigger for link table
        std::string delete_trigger = "CREATE TRIGGER IF NOT EXISTS Audit" + link_table_name + "Delete"
            " AFTER DELETE ON " + link_table_name +
            " WHEN (sync_disabled() = 0)"
            " BEGIN"
            "   INSERT INTO AuditLog (tableName, operation, rowId, globalRowId, changedFields, changedFieldsNames, timestamp)"
            "   VALUES ("
            "       '" + link_table_name + "',"
            "       'DELETE',"
            "       0,"
            "       OLD.globalId,"
            "       json_object('lhs', OLD.lhs, 'rhs', OLD.rhs),"
            "       json_array('lhs', 'rhs'),"
            "       unixepoch('subsec')"
            "   );"
            " END";
        db_->execute(delete_trigger);
    }

    std::string generate_global_id() {
        static std::random_device rd;
        static std::mt19937_64 gen(rd());
        static std::uniform_int_distribution<uint64_t> dis;

        uint64_t a = dis(gen);
        uint64_t b = dis(gen);

        a = (a & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
        b = (b & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;

        std::ostringstream ss;
        ss << std::hex << std::setfill('0');
        ss << std::setw(8) << ((a >> 32) & 0xFFFFFFFF) << "-";
        ss << std::setw(4) << ((a >> 16) & 0xFFFF) << "-";
        ss << std::setw(4) << (a & 0xFFFF) << "-";
        ss << std::setw(4) << ((b >> 48) & 0xFFFF) << "-";
        ss << std::setw(12) << (b & 0xFFFFFFFFFFFFULL);

        return ss.str();
    }

protected:
    int get_schema_version() {
        auto results = db_->query("SELECT value FROM _lattice_meta WHERE key = 'schema_version'");
        if (!results.empty()) {
            return std::stoi(std::get<std::string>(results[0].at("value")));
        }
        return 1;  // Default version
    }

    void set_schema_version(int version) {
        db_->execute("UPDATE _lattice_meta SET value = '" + std::to_string(version) + "' WHERE key = 'schema_version'");
    }
    
    // Exposed for swift_lattice to create tables from Swift schemas
    void create_model_table_public(const model_schema& schema) {
        create_model_table(schema);
    }

    // Exposed for swift_lattice to migrate tables from Swift schemas
    void migrate_model_table_public(const model_schema& schema) {
        migrate_model_table(schema);
    }

    // Exposed for swift_lattice to detect changes before migration
    table_changes detect_table_changes_public(const model_schema& schema) {
        return detect_table_changes(schema);
    }

    template<typename T>
    managed<T> hydrate(const database::row_t& row) {
        const auto& schema = managed<T>::schema();
        return hydrate<T>(row, schema.table_name);
    }

    // Hydrate with explicit table name (for dynamic objects like swift_dynamic_object)
    template<typename T>
    managed<T> hydrate(const database::row_t& row, const std::string& table_name) {
        managed<T> obj;

        // Set base properties
        auto id_it = row.find("id");
        if (id_it != row.end()) {
            obj.id_ = std::get<int64_t>(id_it->second);
        }

        auto gid_it = row.find("globalId");
        if (gid_it != row.end()) {
            obj.global_id_ = std::get<std::string>(gid_it->second);
        }

        obj.db_ = db_.get();
        obj.lattice_ = this;

        // If _source column present (from same-model UNION attach view),
        // qualify table_name so lazy reads/writes target the correct schema.
        auto source_it = row.find("_source");
        if (source_it != row.end() && std::holds_alternative<std::string>(source_it->second)) {
            auto source_schema = std::get<std::string>(source_it->second);
            obj.table_name_ = source_schema + "." + table_name;
        } else {
            obj.table_name_ = table_name;
        }

        // Populate the source object's values from the row (only for types with 'source' member)
        if constexpr (has_source_member<T>::value) {
            for (const auto& [key, value] : row) {
                if (key != "id" && key != "globalId" && key != "_source") {
                    obj.source.values[key] = value;
                }
            }
            obj.source.table_name = table_name;
        }

        // Bind properties to DB
        obj.bind_to_db();

        return obj;
    }
};

// ============================================================================
// Query template implementations
// ============================================================================

template<typename T>
std::vector<managed<T>> query<T>::execute() {
    const auto& schema = managed<T>::schema();
    const std::string& table = schema.table_name;

    std::string sql;

    if (geo_bbox_) {
        // Check if this is a geo_bounds list (separate table) or single geo_bounds (inline columns)
        std::string list_table = "_" + table + "_" + geo_column_;
        std::string list_rtree_table = list_table + "_rtree";
        std::string single_rtree_table = "_" + table + "_" + geo_column_ + "_rtree";

        // Check if list table exists - if so, use list query pattern
        bool is_list = db_.db().table_exists(list_table);

        if (is_list) {
            // Geo bounds list: join through list table's R*Tree
            // Match parent's globalId to list table's parent_id
            sql = "SELECT DISTINCT " + table + ".* FROM " + table +
                  " JOIN " + list_table + " lt ON " + table + ".globalId = lt.parent_id" +
                  " JOIN " + list_rtree_table + " r ON lt.id = r.id" +
                  " WHERE r.minLat <= " + std::to_string(geo_bbox_->max_lat) +
                  " AND r.maxLat >= " + std::to_string(geo_bbox_->min_lat) +
                  " AND r.minLon <= " + std::to_string(geo_bbox_->max_lon) +
                  " AND r.maxLon >= " + std::to_string(geo_bbox_->min_lon);
        } else {
            // Single geo_bounds: join main table's R*Tree directly
            sql = "SELECT " + table + ".* FROM " + table +
                  " JOIN " + single_rtree_table + " r ON " + table + ".id = r.id" +
                  " WHERE r.minLat <= " + std::to_string(geo_bbox_->max_lat) +
                  " AND r.maxLat >= " + std::to_string(geo_bbox_->min_lat) +
                  " AND r.minLon <= " + std::to_string(geo_bbox_->max_lon) +
                  " AND r.maxLon >= " + std::to_string(geo_bbox_->min_lon);
        }

        // Add additional WHERE clause if present
        if (!where_clause_.empty()) {
            sql += " AND " + where_clause_;
        }
    } else {
        sql = "SELECT * FROM " + table;
        if (!where_clause_.empty()) {
            sql += " WHERE " + where_clause_;
        }
    }

    if (!order_clause_.empty()) {
        sql += " ORDER BY " + order_clause_;
    }
    if (limit_ > 0) {
        sql += " LIMIT " + std::to_string(limit_);
    }
    if (offset_ > 0) {
        sql += " OFFSET " + std::to_string(offset_);
    }

    // Use read connection for queries
    auto rows = db_.read_db().query(sql);
    std::vector<managed<T>> items;
    items.reserve(rows.size());

    for (const auto& row : rows) {
        items.push_back(db_.hydrate<T>(row));
    }

    return items;
}

template<typename T>
size_t query<T>::count() {
    const auto& schema = managed<T>::schema();
    const std::string& table = schema.table_name;

    std::string sql;

    if (geo_bbox_) {
        // Spatial query with R*Tree join
        std::string rtree_table = "_" + table + "_" + geo_column_ + "_rtree";
        sql = "SELECT COUNT(*) as cnt FROM " + table +
              " JOIN " + rtree_table + " r ON " + table + ".id = r.id" +
              " WHERE r.minLat <= " + std::to_string(geo_bbox_->max_lat) +
              " AND r.maxLat >= " + std::to_string(geo_bbox_->min_lat) +
              " AND r.minLon <= " + std::to_string(geo_bbox_->max_lon) +
              " AND r.maxLon >= " + std::to_string(geo_bbox_->min_lon);

        if (!where_clause_.empty()) {
            sql += " AND " + where_clause_;
        }
    } else {
        sql = "SELECT COUNT(*) as cnt FROM " + table;
        if (!where_clause_.empty()) {
            sql += " WHERE " + where_clause_;
        }
    }

    // Use read connection for queries
    auto rows = db_.read_db().query(sql);
    if (!rows.empty()) {
        auto it = rows[0].find("cnt");
        if (it != rows[0].end()) {
            return static_cast<size_t>(std::get<int64_t>(it->second));
        }
    }
    return 0;
}

template<typename T>
std::optional<managed<T>> query<T>::first() {
    limit_ = 1;
    auto items = execute();
    if (items.empty()) {
        return std::nullopt;
    }
    return std::move(items[0]);
}

// ============================================================================
// managed<T*> template implementations (to-one relationships)
// ============================================================================

template<typename T>
bool managed<T*, std::enable_if_t<is_model<T>::value>>::has_value() const {
    if (is_bound()) {
        return get_linked_id().has_value();
    }
    return cached_object_ != nullptr;
}

template<typename T>
managed<T>* managed<T*, std::enable_if_t<is_model<T>::value>>::get_value() const {
    if (!has_value()) return nullptr;
    load_if_needed();
    return cached_object_.get();
}

template<typename T>
std::optional<global_id_t> managed<T*, std::enable_if_t<is_model<T>::value>>::get_linked_id() const {
    if (!is_bound() || link_table_.empty()) return std::nullopt;

    // Link table may not exist yet if no link has been set
    if (!db->table_exists(link_table_)) {
        return std::nullopt;
    }

    std::string sql = "SELECT rhs FROM " + link_table_ + " WHERE lhs = ?";
    auto rows = db->query(sql, {parent_global_id_});

    if (rows.empty()) {
        return std::nullopt;
    }

    auto it = rows[0].find("rhs");
    if (it != rows[0].end() && std::holds_alternative<std::string>(it->second)) {
        return std::get<std::string>(it->second);
    }
    return std::nullopt;
}

template<typename T>
void managed<T*, std::enable_if_t<is_model<T>::value>>::set_link(const global_id_t& child_global_id) {
    if (!is_bound() || link_table_.empty()) return;

    // Ensure link table exists
    lattice->ensure_link_table(link_table_);

    // Delete existing link first
    clear_link();

    // Insert new link
    std::string sql = "INSERT INTO " + link_table_ + " (lhs, rhs) VALUES (?, ?)";
    db->execute(sql, {parent_global_id_, child_global_id});
}

template<typename T>
void managed<T*, std::enable_if_t<is_model<T>::value>>::clear_link() {
    if (!is_bound() || link_table_.empty()) return;

    // Link table may not exist yet
    if (!db->table_exists(link_table_)) return;

    std::string sql = "DELETE FROM " + link_table_ + " WHERE lhs = ?";
    db->execute(sql, {parent_global_id_});
}

template<typename T>
void managed<T*, std::enable_if_t<is_model<T>::value>>::load_if_needed() const {
    if (loaded_) return;
    loaded_ = true;

    auto child_gid = get_linked_id();
    if (!child_gid.has_value()) {
        cached_object_.reset();
        return;
    }

    // Find the child object by global ID, using target_table_ if set
    auto found = lattice->find_by_global_id<T>(*child_gid, get_target_table());
    if (found.has_value()) {
        cached_object_ = std::make_shared<managed<T>>(std::move(*found));
    }
}

// operator=(const T&) - the nice UX: owner.pet = Pet{"Fido", 30.0}
template<typename T>
managed<T*, std::enable_if_t<is_model<T>::value>>&
managed<T*, std::enable_if_t<is_model<T>::value>>::operator=(const T& obj) {
    // Create managed version from unmanaged struct and bind to DB
    managed<T> m(obj);
    if (lattice != nullptr) {
        if constexpr(has_instance_schema<T>::value) {
            lattice->bind_managed(m, obj.instance_schema());
        } else {
            lattice->bind_managed(m, managed<T>::schema());
        }
    }

    if (is_bound() && m.is_valid()) {
        set_link(m.global_id());
    }

    cached_object_ = std::make_shared<managed<T>>(std::move(m));
    loaded_ = true;
    return *this;
}

template<typename T>
managed<T*, std::enable_if_t<is_model<T>::value>>&
managed<T*, std::enable_if_t<is_model<T>::value>>::operator=(managed<T>* obj) {
    if (obj == nullptr) {
        return operator=(nullptr);
    }

    // If the object isn't in the database yet, bind it
    if (!obj->is_valid() && lattice != nullptr) {
        if constexpr(has_instance_schema<T>::value) {
            lattice->bind_managed(*obj, obj->instance_schema());
        } else {
            lattice->bind_managed(*obj, managed<T>::schema());
        }
    }

    if (is_bound() && obj->is_valid()) {
        set_link(obj->global_id());
    }

    cached_object_ = std::make_shared<managed<T>>(*obj);
    loaded_ = true;
    return *this;
}

template<typename T>
managed<T*, std::enable_if_t<is_model<T>::value>>&
managed<T*, std::enable_if_t<is_model<T>::value>>::operator=(managed<T*> obj) {
    if (!obj.has_value()) {
        return operator=(nullptr);
    }

    // If the object isn't in the database yet, bind it
    if (!obj->is_valid() && lattice != nullptr) {
        lattice->bind_managed(*obj, managed<T>::schema());
    }

    if (is_bound() && obj->is_valid()) {
        set_link(obj->global_id());
    }

    cached_object_ = std::make_shared<managed<T>>(*obj);
    loaded_ = true;
    return *this;
}

template<typename T>
managed<T*, std::enable_if_t<is_model<T>::value>>&
managed<T*, std::enable_if_t<is_model<T>::value>>::operator=(std::nullptr_t) {
    if (is_bound()) {
        clear_link();
    }
    cached_object_.reset();
    loaded_ = true;
    return *this;
}

// ============================================================================
// managed<std::vector<T*>> template implementations (to-many relationships)
// ============================================================================

template<typename T>
size_t managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::size() const {
    if (!is_bound() || link_table_.empty()) return cached_objects_.size();

    // Link table may not exist yet
    if (!db->table_exists(link_table_)) return 0;

    std::string sql = "SELECT COUNT(*) as cnt FROM " + link_table_ + " WHERE lhs = ?";
    auto rows = db->query(sql, {parent_global_id_});
    if (!rows.empty()) {
        auto it = rows[0].find("cnt");
        if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
            return static_cast<size_t>(std::get<int64_t>(it->second));
        }
    }
    return 0;
}

template<typename T>
std::vector<global_id_t> managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::get_linked_ids() const {
    if (!is_bound() || link_table_.empty()) return {};

    // Link table may not exist yet
    if (!db->table_exists(link_table_)) return {};

    // ORDER BY rowid to preserve insertion order
    std::string sql = "SELECT rhs FROM " + link_table_ + " WHERE lhs = ? ORDER BY rowid";
    auto rows = db->query(sql, {parent_global_id_});

    std::vector<global_id_t> ids;
    ids.reserve(rows.size());
    for (const auto& row : rows) {
        auto it = row.find("rhs");
        if (it != row.end() && std::holds_alternative<std::string>(it->second)) {
            ids.push_back(std::get<std::string>(it->second));
        }
    }
    return ids;
}

template<typename T>
void managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::add_link(const global_id_t& child_global_id) {
    if (!is_bound() || link_table_.empty()) return;

    // Ensure link table exists
    lattice->ensure_link_table(link_table_);

    std::string sql = "INSERT INTO " + link_table_ + " (lhs, rhs) VALUES (?, ?)";
    db->execute(sql, {parent_global_id_, child_global_id});
}

template<typename T>
void managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::load_if_needed() const {
    if (loaded_) return;
    loaded_ = true;

    auto child_gids = get_linked_ids();
    cached_objects_.clear();
    cached_objects_.reserve(child_gids.size());
    
    auto target_table = !target_table_.empty() ? target_table_ : managed<T>::schema().table_name;
    
    for (const auto& gid : child_gids) {
        
        auto found = lattice->find_by_global_id<T>(gid, target_table);
        if (found.has_value()) {
            cached_objects_.push_back(std::make_shared<managed<T>>(std::move(*found)));
        }
    }
}

template<typename T>
void managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::push_back(managed<T>* obj) {
    if (obj == nullptr) return;

    // If the object isn't in the database yet, add it
//    if (!obj->is_valid() && lattice != nullptr) {
//        lattice->add(*obj);
//    }

    if (is_bound() && obj->is_valid()) {
        add_link(obj->global_id());
    }

    cached_objects_.push_back(std::make_shared<managed<T>>(*obj));
}

// push_back(const T&) - the nice UX: trip.destinations.push_back(Destination{...})
template<typename T>
void managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::push_back(const T& obj) {
    // Create managed version and bind to DB
    managed<T> m(obj);
    if (lattice != nullptr) {
        lattice->bind_managed(m, managed<T>::schema());
    }

    if (is_bound() && m.is_valid()) {
        add_link(m.global_id());
    }

    cached_objects_.push_back(std::make_shared<managed<T>>(std::move(m)));
}

template<typename T>
void managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::erase(managed<T>* obj) {
    if (!obj || !obj->is_valid()) return;

    if (is_bound() && !link_table_.empty() && db->table_exists(link_table_)) {
        std::string sql = "DELETE FROM " + link_table_ + " WHERE lhs = ? AND rhs = ?";
        db->execute(sql, {parent_global_id_, obj->global_id()});
    }

    // Remove from cache
    cached_objects_.erase(
        std::remove_if(cached_objects_.begin(), cached_objects_.end(),
            [&](const std::shared_ptr<managed<T>>& p) {
                return p && p->global_id() == obj->global_id();
            }),
        cached_objects_.end());
    loaded_ = false;
}

template<typename T>
void managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::clear() {
    if (is_bound() && !link_table_.empty() && db->table_exists(link_table_)) {
        std::string sql = "DELETE FROM " + link_table_ + " WHERE lhs = ?";
        db->execute(sql, {parent_global_id_});
    }
    cached_objects_.clear();
    loaded_ = false;
}

template<typename T>
typename managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::iterator
managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::begin() {
    load_if_needed();
    return iterator(cached_objects_.begin());
}

template<typename T>
typename managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::iterator
managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::end() {
    load_if_needed();
    return iterator(cached_objects_.end());
}

template<typename T>
typename managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::element_proxy
managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::operator[](size_t index) {
    load_if_needed();
    return element_proxy(this, index);
}

template<typename T>
const managed<T>& managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::operator[](size_t index) const {
    load_if_needed();
    return *cached_objects_[index];
}

template<typename T>
void managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::replace_link_at(
    size_t index, const global_id_t& new_child_global_id) {
    if (!is_bound() || parent_global_id_.empty()) return;

    load_if_needed();
    if (index >= cached_objects_.size()) {
        throw std::out_of_range("Link list index out of range");
    }

    // Get the old global ID at this position
    auto linked_ids = get_linked_ids();
    if (index >= linked_ids.size()) {
        throw std::out_of_range("Link list index out of range");
    }
    const auto& old_child_global_id = linked_ids[index];

    // Update the link table: change child_global_id where position matches
    std::string sql = "UPDATE " + link_table_ +
                      " SET child_global_id = ? WHERE parent_global_id = ? AND child_global_id = ?";
    db->execute(sql, {new_child_global_id, parent_global_id_, old_child_global_id});

    // Invalidate cache so next access reloads
    loaded_ = false;
    cached_objects_.clear();
}

// element_proxy implementations
template<typename T>
managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::element_proxy::operator managed<T>&() const {
    list_->load_if_needed();
    return *list_->cached_objects_[index_];
}

template<typename T>
managed<T>* managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::element_proxy::operator->() const {
    list_->load_if_needed();
    return list_->cached_objects_[index_].get();
}

template<typename T>
typename managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::element_proxy&
managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::element_proxy::operator=(const T& obj) {
    if (!list_->is_bound() || !list_->lattice) return *this;

    // Add the new object to the database
    auto added = list_->lattice->template add<T>(obj);
    list_->replace_link_at(index_, added.global_id());

    return *this;
}

template<typename T>
typename managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::element_proxy&
managed<std::vector<T*>, std::enable_if_t<is_model<T>::value>>::element_proxy::operator=(managed<T>* obj) {
    if (!list_->is_bound() || !obj || !obj->is_managed()) return *this;

    list_->replace_link_at(index_, obj->global_id());

    return *this;
}

// ============================================================================
// results<T>::execute_query() implementation
// ============================================================================

template<typename T>
void results<T>::execute_query() {
    if (!db_) return;

    // Build SQL query
    std::string sql = "SELECT * FROM " + table_name_;
    if (!where_clause_.empty()) {
        sql += " WHERE " + where_clause_;
    }
    if (!order_clause_.empty()) {
        sql += " ORDER BY " + order_clause_;
    }
    // SQLite requires LIMIT before OFFSET, and OFFSET requires LIMIT
    if (limit_ > 0) {
        sql += " LIMIT " + std::to_string(limit_);
        if (offset_ > 0) {
            sql += " OFFSET " + std::to_string(offset_);
        }
    } else if (offset_ > 0) {
        // OFFSET without LIMIT: use -1 (unlimited) for LIMIT
        sql += " LIMIT -1 OFFSET " + std::to_string(offset_);
    }

    // Execute and hydrate using read connection
    auto rows = db_->read_db().query(sql);
    items_.clear();
    items_.reserve(rows.size());
    for (const auto& row : rows) {
        items_.push_back(db_->template hydrate<T>(row));
    }
}

// ============================================================================
// results<T>::observe() implementations
// ============================================================================

/// Legacy observe - receives full snapshot on each change
template<typename T>
notification_token results<T>::observe(observer_t callback) {
    if (!db_) {
        // Can't observe results without a database reference
        return notification_token();
    }

    const auto& schema = managed<T>::schema();
    std::string table_name = schema.table_name;

    // Register observer that re-queries and calls the callback
    auto observer_id = db_->add_table_observer(table_name,
        [db = db_, callback = std::move(callback), table_name](
            const std::string& operation, int64_t row_id, const std::string& global_row_id) {
            // Re-query to get fresh results using read connection
            std::string sql = "SELECT * FROM " + table_name;
            auto rows = db->read_db().query(sql);

            std::vector<managed<T>> items;
            items.reserve(rows.size());
            for (const auto& row : rows) {
                items.push_back(db->template hydrate<T>(row));
            }

            // Call the user's callback with fresh data
            callback(items);
        }
    );

    // Return token that removes observer when destroyed
    return notification_token([db = db_, table_name, observer_id] {
        db->remove_table_observer(table_name, observer_id);
    });
}

/// New observe - receives detailed change info (realm-cpp style)
template<typename T>
notification_token results<T>::observe(change_observer_t callback) {
    if (!db_) {
        return notification_token();
    }

    const auto& schema = managed<T>::schema();
    std::string table_name = schema.table_name;

    // Track the previous state to compute diffs
    auto prev_ids = std::make_shared<std::vector<int64_t>>();
    for (const auto& item : items_) {
        prev_ids->push_back(item.id());
    }

    // Register observer that computes change info
    auto observer_id = db_->add_table_observer(table_name,
        [this, db = db_, callback = std::move(callback), table_name, prev_ids](
            const std::string& operation, int64_t row_id, const std::string& global_row_id) {

            // Re-query to get current state using read connection
            std::string sql = "SELECT id FROM " + table_name;
            auto rows = db->read_db().query(sql);

            std::vector<int64_t> current_ids;
            current_ids.reserve(rows.size());
            for (const auto& row : rows) {
                auto it = row.find("id");
                if (it != row.end() && std::holds_alternative<int64_t>(it->second)) {
                    current_ids.push_back(std::get<int64_t>(it->second));
                }
            }

            // Compute changes
            typename results<T>::results_change change;
            change.collection = this;

            // Find insertions (in current but not in prev)
            for (size_t i = 0; i < current_ids.size(); ++i) {
                if (std::find(prev_ids->begin(), prev_ids->end(), current_ids[i]) == prev_ids->end()) {
                    change.insertions.push_back(i);
                }
            }

            // Find deletions (in prev but not in current)
            for (size_t i = 0; i < prev_ids->size(); ++i) {
                if (std::find(current_ids.begin(), current_ids.end(), (*prev_ids)[i]) == current_ids.end()) {
                    change.deletions.push_back(i);
                }
            }

            // Find modifications (same id but changed - we mark all existing as potentially modified
            // since SQLite update hook doesn't tell us the exact row for UPDATE on this table)
            if (operation == "UPDATE") {
                for (size_t i = 0; i < current_ids.size(); ++i) {
                    auto it = std::find(prev_ids->begin(), prev_ids->end(), current_ids[i]);
                    if (it != prev_ids->end()) {
                        // Check if this is the modified row
                        if (current_ids[i] == row_id) {
                            change.modifications.push_back(i);
                        }
                    }
                }
            }

            // Update prev_ids for next change
            *prev_ids = current_ids;

            // Update items_ with fresh data using read connection
            std::string full_sql = "SELECT * FROM " + table_name;
            auto full_rows = db->read_db().query(full_sql);
            items_.clear();
            items_.reserve(full_rows.size());
            for (const auto& row : full_rows) {
                items_.push_back(db->template hydrate<T>(row));
            }

            // Call callback
            callback(change);
        }
    );

    return notification_token([db = db_, table_name, observer_id] {
        db->remove_table_observer(table_name, observer_id);
    });
}

#if LATTICE_HAS_COROUTINES
/// Create a coroutine-based change stream
template<typename T>
change_stream<collection_change> results<T>::changes() {
    if (!db_) {
        return change_stream<collection_change>();
    }

    const auto& schema = managed<T>::schema();
    std::string table_name = schema.table_name;
    auto db = db_;

    return change_stream<collection_change>([db, table_name](auto push) {
        // Register observer that pushes changes to the stream
        auto observer_id = db->add_table_observer(table_name,
            [push, table_name](const std::string& operation, int64_t row_id, const std::string& global_row_id) {
                collection_change change;
                if (operation == "INSERT") {
                    change.insertions.push_back(static_cast<uint64_t>(row_id));
                } else if (operation == "UPDATE") {
                    change.modifications.push_back(static_cast<uint64_t>(row_id));
                } else if (operation == "DELETE") {
                    change.deletions.push_back(static_cast<uint64_t>(row_id));
                }
                push(change);
            }
        );

        return notification_token([db, table_name, observer_id] {
            db->remove_table_observer(table_name, observer_id);
        });
    });
}
#endif

} // namespace lattice

// ============================================================================
// Include sync.hpp here so synchronizer is fully defined
// This must be after lattice_db is fully defined since synchronizer uses it
// ============================================================================
#include "sync.hpp"

namespace lattice {

// ============================================================================
// lattice_db sync method implementations
// ============================================================================

inline lattice_db::~lattice_db() {
    // Unregister from instance registry FIRST so that any stale GCD callbacks
    // from the cross-process notifier can't find us via get_instances().
    instance_registry::instance().unregister_instance(config_.path, this);

    // Stop cross-process listener after unregistering
    if (xproc_notifier_) {
        xproc_notifier_->stop_listening();
        xproc_notifier_.reset();
    }

    // Disconnect synchronizer before destroying
    if (synchronizer_) {
        synchronizer_->disconnect();
    }
}

inline bool lattice_db::is_sync_connected() const {
    return synchronizer_ && synchronizer_->is_connected();
}

inline void lattice_db::sync_now() {
    if (synchronizer_) {
        synchronizer_->sync_now();
    }
}

inline void lattice_db::connect_sync() {
    if (synchronizer_) {
        synchronizer_->connect();
    } else if (config_.is_sync_enabled()) {
        // Lazily create synchronizer if config is set but sync wasn't started
        setup_sync_if_configured();
    }
}

inline void lattice_db::disconnect_sync() {
    if (synchronizer_) {
        synchronizer_->disconnect();
    }
}

inline void lattice_db::set_on_sync_state_change(std::function<void(bool connected)> handler) {
    on_sync_state_change_ = std::move(handler);
    if (synchronizer_) {
        synchronizer_->set_on_state_change(on_sync_state_change_);
    }
}

inline void lattice_db::set_on_sync_error(std::function<void(const std::string& error)> handler) {
    on_sync_error_ = std::move(handler);
    if (synchronizer_) {
        synchronizer_->set_on_error(on_sync_error_);
    }
}

// Implementation of managed<std::vector<uint8_t>> methods that need lattice_db
inline void managed<std::vector<uint8_t>>::ensure_vec0_for_blob(
    lattice_db* lattice, const std::string& table,
    const std::string& column, const std::vector<uint8_t>& val) {
    if (lattice && !val.empty()) {
        int dimensions = static_cast<int>(val.size() / sizeof(float));
        if (dimensions > 0) {
            lattice->ensure_vec0_table(table, column, dimensions);
        }
    }
}

inline void managed<std::vector<uint8_t>>::set_value(const std::vector<uint8_t>& val) {
    unmanaged_value = val;
    if (!is_bound()) return;

    // If this is a vector column, ensure vec0 table + triggers exist
    // This handles the migration case where vec0 wasn't created initially
    if (is_vector_column) {
        ensure_vec0_for_blob(lattice, table_name, column_name, val);
    }

    std::string sql = "UPDATE " + table_name + " SET " + column_name + " = ? WHERE id = ?";
    db->execute(sql, {val, row_id});
}

// ============================================================================
// managed<std::vector<geo_bounds>> method implementations
// ============================================================================

inline void managed<std::vector<geo_bounds>>::push_back(const geo_bounds& bounds) {
    if (!is_bound()) {
        unmanaged_value.push_back(bounds);
        return;
    }

    // Ensure the list table exists
    if (lattice) {
        lattice->ensure_geo_bounds_list_table(table_name, column_name);
    }

    // Insert into list table
    std::string sql = "INSERT INTO " + list_table_ +
        " (parent_id, minLat, maxLat, minLon, maxLon) VALUES (?, ?, ?, ?, ?)";
    db->execute(sql, {
        parent_global_id_,
        bounds.min_lat,
        bounds.max_lat,
        bounds.min_lon,
        bounds.max_lon
    });

    // Update cache
    cached_objects_.push_back(bounds);
}

inline void managed<std::vector<geo_bounds>>::erase(size_t index) {
    if (!is_bound()) {
        if (index < unmanaged_value.size()) {
            unmanaged_value.erase(unmanaged_value.begin() + static_cast<std::ptrdiff_t>(index));
        }
        return;
    }

    load_if_needed();
    if (index >= cached_objects_.size()) return;

    // Get the id of the row to delete
    std::string sql = "SELECT id FROM " + list_table_ +
        " WHERE parent_id = ? ORDER BY id LIMIT 1 OFFSET ?";
    auto rows = db->query(sql, {parent_global_id_, static_cast<int64_t>(index)});

    if (!rows.empty()) {
        auto it = rows[0].find("id");
        if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
            int64_t row_id_to_delete = std::get<int64_t>(it->second);
            db->execute("DELETE FROM " + list_table_ + " WHERE id = ?", {row_id_to_delete});
        }
    }

    // Update cache
    cached_objects_.erase(cached_objects_.begin() + static_cast<std::ptrdiff_t>(index));
}

inline void managed<std::vector<geo_bounds>>::clear() {
    if (!is_bound()) {
        unmanaged_value.clear();
        return;
    }

    // Delete all entries for this parent
    std::string sql = "DELETE FROM " + list_table_ + " WHERE parent_id = ?";
    db->execute(sql, {parent_global_id_});

    // Clear cache
    cached_objects_.clear();
    is_loaded_ = true;  // Mark as loaded (empty)
}

} // namespace lattice
