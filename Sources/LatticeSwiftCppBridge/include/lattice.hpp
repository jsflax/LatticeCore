#pragma once
#include <lattice/store_physical_api.hpp>

#ifdef __cplusplus

#include <LatticeCore.hpp>
#include <lattice/spatial_query.hpp>
#include <bridging.hpp>
#include <cassert>
#include <array>
#include <cctype>
#include <chrono>
#include <concepts>
#include <filesystem>
#include <future>
#include <set>
#include <thread>

#include <dynamic_object.hpp>
#include <swift_runtime_schema.hpp>
#include <swift_callback_input_v2.hpp>
#include <observer_delivery_input.hpp>
#include <bulk_mutation.hpp>
#include <projection.hpp>
#include <recovery_export.hpp>
#include <recovery_relay.hpp>
#include <recovery_continuity.hpp>
#include <ordinary_sync_export.hpp>
#include <managed_product_route.hpp>
#include "../../LatticeCore/src/ordinary_normal_startup.hpp"
#include "../../LatticeCore/src/ordinary_managed_history.hpp"
#include "../../LatticeCore/src/ordinary_export.hpp"
#include "../../LatticeCore/src/ordinary_cohort_attachment_qualification.hpp"
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#include "../../LatticeCore/src/recovery_refresh.hpp"
#endif
#include <lattice/platform_retirement.hpp>
#include <lattice/configured_platform.hpp>
#include <list.hpp>
#include <error.hpp>
#include <transaction_receipt.hpp>
#include <resource_startup.hpp>
#include <lattice/store_resource_context.hpp>

#if defined(__BLOCKS__) && defined(__APPLE__) && !defined(__swift__)
#include <Block.h>
#endif

// Forward declarations
namespace lattice {
class swift_lattice_ref;
class automatic_read_maintenance_fixture_access;
class swift_migration_context_ref;
namespace detail { class LatticeCache; }
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

// Post a cross-process change notification for the given database path.
// Used by tests and external writers that bypass Lattice's write hooks.
void _lattice_post_cross_process_notification(const std::string& db_path);

namespace lattice {

// Owned, sealed close outcome; no member borrows the retired implementation.
class swift_close_result {
    friend class swift_lattice_ref;
    friend class swift_managed_wss_owner;
    int32_t sync_=0;
    bool cleanup_complete_=false,failed_=false,cleanup_failed_=false,message_unavailable_=false;
    std::string message_;
    explicit swift_close_result(const lattice_close_result& result) noexcept
        :sync_(static_cast<int32_t>(result.sync)),cleanup_complete_(result.cleanup_complete),
         failed_(bool(result.error)),cleanup_failed_(bool(result.cleanup_error)) {
        if(!result.error)return;
        try {std::rethrow_exception(result.error);}
        catch(const std::exception& error){try{message_=error.what();}catch(...){message_unavailable_=true;}}
        catch(...){try{message_="Unknown C++ close failure";}catch(...){message_unavailable_=true;}}
    }
public:
    swift_close_result() noexcept=default;
    int32_t sync_state()const noexcept SWIFT_NAME(syncState()){return sync_;}
    bool cleanup_complete()const noexcept SWIFT_NAME(cleanupComplete()){return cleanup_complete_;}
    bool failed()const noexcept{return failed_;}
    bool cleanup_failed()const noexcept SWIFT_NAME(cleanupFailed()){return cleanup_failed_;}
    bool message_unavailable()const noexcept SWIFT_NAME(messageUnavailable()){return message_unavailable_;}
    std::string take_message()noexcept SWIFT_NAME(takeMessage()){return std::move(message_);}
};

inline swift_close_result swift_managed_wss_owner::close()const noexcept{
#ifndef __EMSCRIPTEN__
    if(value_)return swift_close_result(value_->close(std::chrono::steady_clock::now()+std::chrono::seconds(2)));
#endif
    return {};
}

// ============================================================================
// swift_dynamic_object - A type-erased model for Swift interop
// Swift models map to this, with schema/properties set dynamically
// ============================================================================

using SwiftSchema = std::unordered_map<std::string, property_descriptor>;
using OptionalString = std::optional<std::string>;
using OptionalInt64 = std::optional<int64_t>;
using ByteVector = std::vector<uint8_t>;

// ============================================================================
// union_value - Type-erased representation of a union field's current value.
// Typed getters/setters mirror dynamic_object's pattern for Swift-C++ interop.
// ============================================================================

struct union_value {
    std::string case_name;  // discriminator (empty = no value / nil)

    void set_string(const std::string& key, const std::string& value)
        SWIFT_NAME(setString(_:_:)) { string_fields_[key] = value; }
    std::string get_string(const std::string& key) const
        SWIFT_NAME(getString(_:)) {
        auto it = string_fields_.find(key);
        return it != string_fields_.end() ? it->second : "";
    }
    bool has_string(const std::string& key) const
        SWIFT_NAME(hasString(_:)) { return string_fields_.count(key) > 0; }

    void set_int(const std::string& key, int64_t value)
        SWIFT_NAME(setInt(_:_:)) { int_fields_[key] = value; }
    int64_t get_int(const std::string& key) const
        SWIFT_NAME(getInt(_:)) {
        auto it = int_fields_.find(key);
        return it != int_fields_.end() ? it->second : 0;
    }
    bool has_int(const std::string& key) const
        SWIFT_NAME(hasInt(_:)) { return int_fields_.count(key) > 0; }

    void set_double(const std::string& key, double value)
        SWIFT_NAME(setDouble(_:_:)) { double_fields_[key] = value; }
    double get_double(const std::string& key) const
        SWIFT_NAME(getDouble(_:)) {
        auto it = double_fields_.find(key);
        return it != double_fields_.end() ? it->second : 0.0;
    }
    bool has_double(const std::string& key) const
        SWIFT_NAME(hasDouble(_:)) { return double_fields_.count(key) > 0; }

    void set_blob(const std::string& key, const std::vector<uint8_t>& value)
        SWIFT_NAME(setBlob(_:_:)) { blob_fields_[key] = value; }
    std::vector<uint8_t> get_blob(const std::string& key) const
        SWIFT_NAME(getBlob(_:)) {
        auto it = blob_fields_.find(key);
        return it != blob_fields_.end() ? it->second : std::vector<uint8_t>{};
    }
    bool has_blob(const std::string& key) const
        SWIFT_NAME(hasBlob(_:)) { return blob_fields_.count(key) > 0; }

    bool has_field(const std::string& key) const
        SWIFT_NAME(hasField(_:)) {
        return has_string(key) || has_int(key) || has_double(key) || has_blob(key);
    }

    // Link object references — for unmanaged objects where globalId isn't set yet.
    // persist_union_values resolves these to globalIds at add() time.
    void set_link_ref(const std::string& key, const dynamic_object_ref& ref)
        SWIFT_NAME(setLinkRef(_:_:)) { link_refs_[key] = ref.impl_; }
    // Returns a wrapped ref for Swift interop. FRT path: heap pointer (nullptr
    // when absent). Value path: by value, empty ref (isValid()==false) when
    // absent — callers test isValid() instead of optional-unwrapping.
#if LATTICE_HAS_FRT
    dynamic_object_ref* get_link_ref(const std::string& key) const
        SWIFT_NAME(getLinkRef(_:)) SWIFT_RETURNS_UNRETAINED {
        auto it = link_refs_.find(key);
        if (it == link_refs_.end() || !it->second) return nullptr;
        return dynamic_object_ref::wrap(it->second);
    }
#else
    dynamic_object_ref get_link_ref(const std::string& key) const
        SWIFT_NAME(getLinkRef(_:)) {
        auto it = link_refs_.find(key);
        if (it == link_refs_.end() || !it->second)
            return dynamic_object_ref::wrap(std::shared_ptr<dynamic_object>(nullptr));
        return dynamic_object_ref::wrap(it->second);
    }
#endif
    bool has_link_ref(const std::string& key) const
        SWIFT_NAME(hasLinkRef(_:)) { return link_refs_.count(key) > 0; }

    // All field keys across all types
    std::vector<std::string> all_keys() const SWIFT_NAME(allKeys());

    // Convert a field to column_value_t for SQL binding
    column_value_t field_as_column_value(const std::string& key) const;

    // Link refs map (for persist_union_values)
    const std::unordered_map<std::string, std::shared_ptr<dynamic_object>>& link_refs() const { return link_refs_; }
    std::unordered_map<std::string, std::shared_ptr<dynamic_object>>& link_refs() { return link_refs_; }

private:
    friend class swift_lattice;
    void swap_graph_value(union_value& other) noexcept {
        case_name.swap(other.case_name);
        string_fields_.swap(other.string_fields_); int_fields_.swap(other.int_fields_);
        double_fields_.swap(other.double_fields_); blob_fields_.swap(other.blob_fields_);
        link_refs_.swap(other.link_refs_);
    }
    std::unordered_map<std::string, std::string> string_fields_;
    std::unordered_map<std::string, int64_t> int_fields_;
    std::unordered_map<std::string, double> double_fields_;
    std::unordered_map<std::string, std::vector<uint8_t>> blob_fields_;
    std::unordered_map<std::string, std::shared_ptr<dynamic_object>> link_refs_;
};

/// What a TRUNCATE checkpoint actually did (swift_lattice::checkpoint).
struct checkpoint_outcome {
    bool busy = true;            // a reader/writer held the WAL — nothing could run
    bool complete = false;       // every WAL frame folded back and the -wal truncated
    int64_t log_frames = -1;     // frames in the WAL before the call (-1 unavailable)
    int64_t checkpointed = -1;   // frames folded back (< log_frames = partial)
};

/// One checked maintenance observation, never an admission or cleanup grant.
/// The conservative default cannot turn a refused Swift call into success.
struct read_pool_maintenance_result {
    bool completed = false;
    bool pressure_pending = true;
    std::string error;
};

/// Checked ready-original automatic maintenance enrollment. An ineligible
/// memory/read-only owner returns completed=true, enabled=false; an actual
/// admission/startup failure keeps the conservative default and its error.
struct automatic_read_maintenance_enrollment_result {
    bool completed = false;
    bool enabled = false;
    std::string error;
};

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
/// Qualification-only control of the actual automatic actor. Copies share one
/// opaque Native hold; explicit release retires it once without a callback or
/// lock held by this wrapper. It grants no SQL/read/pressure-reset authority.
class automatic_read_maintenance_fixture_token {
    struct control {
        std::shared_ptr<void> hold;
        std::atomic<bool> released{false};
        explicit control(std::shared_ptr<void> value) : hold(std::move(value)) {}
    };
    std::shared_ptr<control> control_;
    bool busy_ = false;
    explicit automatic_read_maintenance_fixture_token(std::shared_ptr<void> value)
        : control_(value ? std::make_shared<control>(std::move(value)) : nullptr) {}
    friend class automatic_read_maintenance_fixture_access;
public:
    automatic_read_maintenance_fixture_token() = default;
    bool valid() const noexcept { return control_ && !control_->released.load(); }
    // Only a genuine running tick at locked native admission permits retry.
    // A default exception token is neither valid nor busy.
    bool busy() const noexcept { return busy_; }
    void release() const noexcept {
        auto retained = control_;
        if (retained && !retained->released.exchange(true)) retained->hold.reset();
    }
};
struct automatic_read_maintenance_fixture_snapshot {
    bool available = false;
    bool enabled = false;
    bool running = false;
    uint64_t pause_count = 0, attempts = 0, quiet_returns = 0, deferred = 0, completed = 0;
};
#endif

/// One audit row's HEADER — the columns that precede `changedFields` in the
/// row, so reading them never walks the payload's overflow chain. What
/// `Lattice.changeHeaders` yields per delivered change.
struct audit_header {
    bool found = false;
    std::string table_name;
    std::string operation;
    int64_t row_id = 0;
    std::string global_row_id;
};

/// What reclaim_space did — page counts are the ground truth for "the file
/// shrank"; `error` is non-empty when VACUUM threw (also in last_bridge_error).
struct reclaim_result {
    bool ok = false;
    int64_t pages_before = -1;
    int64_t pages_after = -1;
    int64_t wal_bytes_after = 0;
    int passes = 0;
    bool checkpoint_busy = false;
    std::string error;
};

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
using SyncFilterVector = std::vector<sync_filter_entry>;
using IPCTargetVector = std::vector<configuration::ipc_target>;

/// Ordered bindings for a parameterized predicate: `params[i]` binds the i-th
/// `?` in the WHERE fragment. Named so Swift can construct one directly.
using ColumnValueVector = std::vector<column_value_t>;

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

/// Create a NULL column_value_t (the nullptr_t alternative). Swift cannot
/// spell `column_value_t(nullptr)` through interop, so it needs a factory.
inline column_value_t column_value_null() {
    return nullptr;
}

/// Append to a ColumnValueVector. `push_back` on a std::vector of a variant is
/// not reliably importable into Swift; this free function is.
inline void push_column_value(ColumnValueVector* vec, const column_value_t& v) {
    vec->push_back(v);
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

// Immutable receiver-issued ownership. Default construction is empty and
// cannot grant an open; the only positive factory consumes the real inherited
// launch exchange before any native file/cache admission.
class swift_ordinary_open_context {
    friend struct swift_configuration;
    friend class detail::LatticeCache;
    friend class swift_lattice_ref;
    friend class swift_managed_history_startup;
    detail::ordinary_context context_;
    std::shared_ptr<const detail::ordinary_managed_child_admission> admission_;
public:
    swift_ordinary_open_context() = default;
    static swift_ordinary_open_context receive_engram_primary() noexcept {
        swift_ordinary_open_context result;
        try { result.context_=detail::ordinary_installation::normal_receiver::receive(
            std::chrono::steady_clock::now()+std::chrono::seconds(30)); } catch(...) {}
        return result;
    }
    bool valid() const noexcept { return static_cast<bool>(context_); }
    std::string primary_path() const noexcept { return sealed([&]{return context_ ? context_->primary_path() : std::string{};}); }
    bool validate_path(const std::string& path) const noexcept {
        return sealed([&]{
            if(!context_)throw std::runtime_error("receiver-issued managed open context required");
            detail::ordinary_require_creator(context_);
            detail::ordinary_before_open(path,context_);return true;
        });
    }
};

// Strong historical launch custody is retained separately from every ordinary
// open view. No path/Boolean/default instance creates a positive admission.
class swift_managed_history_startup final {
    friend class swift_lattice_ref;
    detail::ordinary_context opens_;
    std::shared_ptr<const detail::ordinary_managed_child_admission> admission_;
    detail::ordinary_launch::deadline receive_end_{};
public:
    swift_managed_history_startup()=default;
    static swift_managed_history_startup receive_engram_history() noexcept {
        swift_managed_history_startup result;
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
        try{auto issued=detail::ordinary_installation::managed_history_receiver::receive(end);
            result.opens_=std::move(issued.opens);result.admission_=std::move(issued.admission);
            result.receive_end_=end;
        }catch(...){}return result;
    }
    static swift_managed_history_startup receive_engram_paired_history() noexcept {
        swift_managed_history_startup result;
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
        try{auto issued=detail::ordinary_installation::managed_history_receiver::receive_paired(end);
            result.opens_=std::move(issued.opens);result.admission_=std::move(issued.admission);
            result.receive_end_=end;
        }catch(...){}return result;
    }
    static swift_managed_history_startup receive_engram_daemon_history() noexcept {
        swift_managed_history_startup result;
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
        try{auto issued=detail::ordinary_installation::managed_history_receiver::receive_daemon(end);
            result.opens_=std::move(issued.opens);result.admission_=std::move(issued.admission);
            result.receive_end_=end;
        }catch(...){}return result;
    }
    bool valid()const noexcept{return bool(opens_)&&bool(admission_);}
    std::string primary_path()const noexcept{return sealed([&]{return opens_?opens_->primary_path():std::string{};});}
    std::vector<std::string> registered_paths()const noexcept {
        try{if(valid())return opens_->managed_paths();}catch(...){}return {};
    }
    swift_ordinary_open_context context_for_path(const std::string& path)const noexcept {
        swift_ordinary_open_context result;
        try{if(valid()){result.context_=opens_->for_managed_path(path);result.admission_=admission_;}}catch(...){}return result;
    }
};

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

    void set_recovery_source_expectation(const std::string& value) { recovery_source_expectation=value; }
    void set_ordinary_sync_required(bool value) { ordinary_policy=value?ordinary_sync_policy::ordered_required:ordinary_sync_policy::legacy_provisional; }
    void set_ordinary_history_required() { ordinary_policy=ordinary_sync_policy::ordered_history_required; }
    void set_ordinary_model_profile(const std::string& value) { ordinary_model_profile=value; }

    void set_sync_filter(const SyncFilterVector& filter) {
        sync_filter = std::vector<sync_filter_entry>(filter.begin(), filter.end());
    }

    void set_ipc_targets(const IPCTargetVector& targets) {
        ipc_targets = std::vector<ipc_target>(targets.begin(), targets.end());
    }

    // Copy only the actual opaque owners; open/route validators remain the
    // issuers. Empty wrappers create no positive admission or credentials.
    void set_ordinary_open_context(const swift_ordinary_open_context& context) { ordinary_context=context.context_; }
    void set_managed_product_credentials(const swift_managed_product_credentials& credentials) { managed_product_credentials=credentials.value_; }

    // Sync tuning (1.0 item I1): forward-only overrides of sync_config
    // defaults. Snake_case value-type setters (same shape as set_sync_filter)
    // — unset knobs keep sync.hpp's defaults. Nonsensical values are IGNORED
    // at this boundary (the knob stays at its default) rather than being
    // forwarded: chunk_size <= 0 would permanently stall uploads (and a
    // negative value cast through size_t becomes a giant window); negative
    // delays/windows have no meaning. 0 remains valid where it means
    // "disabled" (reconnect attempts = unlimited, coalesce = legacy,
    // checkpoint = off).
    void set_sync_chunk_size(int64_t v) { if (v > 0) tuning.chunk_size = static_cast<size_t>(v); }
    void set_sync_max_reconnect_attempts(int32_t v) { if (v >= 0) tuning.max_reconnect_attempts = static_cast<int>(v); }
    void set_sync_base_delay_seconds(double v) { if (v > 0) tuning.base_delay_seconds = v; }
    void set_sync_max_delay_seconds(double v) { if (v > 0) tuning.max_delay_seconds = v; }
    void set_sync_stable_connection_ms(int64_t v) { if (v >= 0) tuning.stable_connection_ms = v; }
    void set_sync_upload_coalesce_ms(int32_t v) { if (v >= 0) tuning.upload_coalesce_ms = static_cast<int>(v); }
    void set_sync_checkpoint_passive_interval_ms(int32_t v) { if (v >= 0) tuning.checkpoint_passive_interval_ms = static_cast<int>(v); }
    void set_sync_checkpoint_truncate_interval_ms(int32_t v) { if (v >= 0) tuning.checkpoint_truncate_interval_ms = static_cast<int>(v); }
    void set_sync_use_upload_floor(bool v) { tuning.use_upload_floor = v; }


private:
    detail::swift_callback_input_v2<void(const std::string& table_name,
                       dynamic_object_ref* old_row,
                       dynamic_object_ref* new_row)> row_migration_fn_;
    detail::swift_callback_input_v2<std::optional<SchemaPair>(const std::string& table_name,
                                            size_t version_number)> get_schema_pair_fn_;
    std::unordered_map<size_t, std::unordered_map<std::string, SchemaPair>> migration_schemas_;

    friend class swift_lattice;
    friend class swift_lattice_ref;
    friend class detail::LatticeCache;
};

// Internal implementation - inherits from lattice_db
class swift_lattice : public lattice_db {
    struct owned_resources;
    detail::process_bound_resource_owner<owned_resources> swift_resources_;
public:
    // Construct with full configuration (including optional sync)
    explicit swift_lattice(swift_configuration&& config)
        : lattice_db(config,false,detail::recovery_owner_schema::capture_native(),{},{},true),
    swift_resources_(std::move(config))
    {
        bind_pending_model_registry_checker_();
        if(delivery_startup_) {
            finish_observer_delivery_startup_();
            publish_observer_delivery_facilities_();
        }
    }

    // Construct with swift_configuration (includes row migration callback)
    swift_lattice(const swift_configuration& config, const SchemaVector& schemas);
    swift_lattice(swift_configuration&& config, const SchemaVector& schemas);
    // Closed synchronous factory key. Public overload permits the actual
    // make_shared single allocation; only the real cache builder issues it.
    class normal_factory_key {
        friend class swift_lattice;
        friend class detail::LatticeCache;
        detail::store_resource_initialization_original* original_;
        explicit normal_factory_key(detail::store_resource_initialization_original* original)noexcept:original_(original){}
    };
    swift_lattice(const swift_configuration&,const SchemaVector&,normal_factory_key);

    /// Strict inner API; callers must own the active Core transaction.
    int64_t apply_selected_mutations(const selected_mutation_batch& batch);

    // Explicit ordinary owning-object boundary. It is a denial check, not an
    // account or SQL grant; pending work must keep using borrowed runtime DATA.
    void require_runtime_schema_ordinary_() const {
        require_creator_process();
        if(db_&&schema_initializing_(*db_))throw db_error("pending runtime schema cannot materialize ordinary objects");
        detail::require_swift_runtime_ordinary_copy_();
    }
    SwiftSchema copy_runtime_properties_(const RuntimeSwiftSchema& properties) const {
        require_runtime_schema_ordinary_();return properties.ordinary_copy();
    }

    // Get properties for a table (used when hydrating objects)
    const RuntimeSwiftSchema* get_properties_for_table(const std::string& table_name) const {
        auto it = schemas_.find(table_name);
        if (it != schemas_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    swift_property_info get_property_info(const std::string& table,const std::string& property) const {
        require_creator_process();const auto* schema=get_properties_for_table(table);
        if(!schema)return {};const auto found=schema->find(property);
        return found==schema->end()?swift_property_info{}:swift_property_info(found->second);
    }

    // Get constraints for a table (used for upsert logic)
    const RuntimeConstraintVector* get_constraints_for_table(const std::string& table_name) const {
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

    // Get columns for the upsert ON CONFLICT clause. Link-kind constraint
    // columns are materialized on the base table as `<col>__link_gid` shadow
    // columns (see ensure_swift_tables Phase 8a), so resolve them here too —
    // otherwise the prepared INSERT carries `ON CONFLICT (<linkName>)` against
    // a column that doesn't exist and SQLite fails to compile it.
    //
    // NOTE: the shadow column is populated by the post-insert link-table
    // maintenance trigger, so it is NULL at row-INSERT time and `ON CONFLICT`
    // can't actually detect a *link* conflict on a fresh insert — a real
    // duplicate surfaces as a UNIQUE violation on the trigger's UPDATE. Callers
    // that need overwrite-in-place against a link-bearing key should find the
    // existing row and mutate it rather than relying on this upsert path.
    std::vector<std::string> get_upsert_columns(const std::string& table_name) const {
        require_runtime_schema_ordinary_();
        auto* constraints = get_constraints_for_table(table_name);
        if (!constraints) return {};
        const RuntimeSwiftSchema* props = get_properties_for_table(table_name);
        for (const auto& c : *constraints) {
            if (!c.allows_upsert) continue;
            std::vector<std::string> resolved;
            resolved.reserve(c.columns.size());
            for (const auto& col : c.columns) {
                if (props) {
                    auto it = props->find(col);
                    if (it != props->end()
                        && it->second.kind == property_kind::link
                        && !it->second.target_table.empty()) {
                        resolved.push_back(detail::swift_runtime_ordinary_text_(col) + "__link_gid");
                        continue;
                    }
                }
                resolved.push_back(detail::swift_runtime_ordinary_text_(col));
            }
            return resolved;
        }
        return {};
    }

    std::string path_copy() const {
        return swift_config_.path;
    }
    
    /// Block until background IVF training completes. No-op if no training is running.
    void wait_for_vec0_training()
        SWIFT_NAME(waitForVec0Training()) {
        require_creator_process();
        if (vec0_training_future_.valid()) {
            vec0_training_future_.wait();
        }
    }

    // ---- Dynamic (schema-from-file) support --------------------------------
    // These let a process open a `.lattice` file with NO compile-time model
    // types and still query it via the ORM. The full SwiftSchema (property
    // descriptors + constraints) is persisted into _lattice_meta on the DDL
    // path; a dynamic open reads it back and rebuilds in-memory state with no
    // DDL. See store_swift_schema_snapshot / reconstruct_swift_schema_from_db.

    /// Serialize the Swift schema (full property descriptors + constraints) to
    /// _lattice_meta. Called from the slow/DDL path of ensure_swift_tables,
    /// inside its exclusive transaction, so the snapshot commits atomically
    /// with the DDL it describes.
    void store_swift_schema_snapshot(const SchemaVector& schemas);

    /// Rebuild the SchemaVector from the persisted snapshot. Falls back to a
    /// best-effort reconstruction from sqlite_master/_lattice_meta for
    /// pre-snapshot databases (lossy: embedded-vs-string and union case detail
    /// are not recoverable without the snapshot).
    SchemaVector reconstruct_swift_schema_from_db();

    /// Read-only open without compile-time types: reconstruct the schema from
    /// the file and populate in-memory state (no DDL). Call once right after
    /// constructing a read-only swift_lattice. Used by create_dynamic().
    void open_dynamic() {
        SchemaVector schemas = reconstruct_swift_schema_from_db();
        populate_swift_in_memory_state(schemas);
    }

    /// Names of all user model tables known to a dynamically-opened lattice.
    std::vector<std::string> dynamic_table_names() const
        SWIFT_NAME(dynamicTableNames()) {
        require_runtime_schema_ordinary_();
        std::vector<std::string> out;
        out.reserve(schemas_.size());
        for (const auto& kv : schemas_) out.push_back(detail::swift_runtime_ordinary_text_(kv.first));
        return out;
    }

    /// Property descriptors for a table, for building a dynamic schema in Swift.
    std::vector<property_descriptor> dynamic_properties_for(const std::string& table_name) const
        SWIFT_NAME(dynamicPropertiesFor(_:)) {
        require_runtime_schema_ordinary_();
        std::vector<property_descriptor> out;
        auto it = schemas_.find(table_name);
        if (it != schemas_.end()) {
            out.reserve(it->second.size());
            for (const auto& kv : it->second) out.push_back(kv.second.ordinary_copy());
        }
        return out;
    }

private:
    struct selected_mutation_connection_guard;
    friend class swift_lattice_ref;
    void bind_pending_model_registry_checker_();
    static void require_pending_model_registries_data_(const lattice_db&,const detail::recovery_owner_schema&);
    swift_lattice(const swift_configuration&,const SchemaVector&,detail::recovery_owner_schema,
        std::shared_ptr<database>,lattice_db::checked_managed_existing_key);
    swift_lattice(const swift_configuration&,const SchemaVector&,detail::recovery_owner_schema,
        std::shared_ptr<database>,lattice_db::checked_mounted_server_existing_key);
    static std::shared_ptr<swift_lattice> open_checked_mounted_server(const swift_configuration&,const SchemaVector&);
    void publish_managed_ordinary() { publish_checked_existing_facilities(); }
    static void claim_managed_source_open(const std::shared_ptr<const detail::ordinary_managed_child_admission>& admission,const std::string& path) {
        detail::ordinary_export_access::claim_managed_source_open(admission,path);
    }
    static void remember_managed_open_failure(const std::shared_ptr<const detail::ordinary_managed_child_admission>& admission,
        const std::string& path,std::exception_ptr error) noexcept {
        detail::ordinary_export_access::remember_managed_open_failure(admission,path,std::move(error));
    }
    static void open_managed_group_receiver(const swift_configuration&,const SchemaVector&,const detail::ordinary_context&,
        const std::shared_ptr<const detail::ordinary_managed_child_admission>&,detail::ordinary_launch::deadline,
        std::shared_ptr<swift_lattice>& retained,bool& opening_claimed);
    void finish_managed_group_preparation_control(detail::ordinary_launch::deadline);
    void validate_managed_ordinary() {
        detail::ordinary_export_access::validate_checked_managed_schema(*this,
            detail::ordinary_export_access::checked_reference_phase::enrolled);
    }
    swift_lattice(const swift_configuration&,swift_lattice& managed_parent);
    swift_lattice(const swift_configuration&,const SchemaVector&,
        const std::shared_ptr<detail::recovery_continuous_admission>&);
    /// Best-effort schema reconstruction for databases written before the
    /// snapshot existed. Enumerates user tables from sqlite_master, reads column
    /// types via PRAGMA table_info, and detects link / vec / fts / geo sidecars.
    SchemaVector reconstruct_swift_schema_fallback();
    // Stored schemas for hydration
    detail::swift_runtime_map<RuntimeSwiftSchema>& schemas_ = swift_resources_.get().schemas_;
    // Immutable source-only schemas, owned by one attachment lifetime.
    // Guarded by Core attach_mutex_; never merges into this store's schemas_.
    using attached_schema_map = detail::swift_runtime_map<RuntimeSwiftSchema>;
    // Stored constraints per table
    detail::swift_runtime_map<RuntimeConstraintVector>& constraints_ = swift_resources_.get().constraints_;
    // Union table descriptors (union_table_name -> descriptor)
    detail::swift_runtime_map<detail::swift_runtime_union_descriptor>& union_schemas_ = swift_resources_.get().union_schemas_;
    // Swift-specific configuration (stores row migration callback)
    swift_configuration& swift_config_ = swift_resources_.get().swift_config_;
    // Error from last receive_sync_data call (nullopt if none)
    OptionalString& last_receive_error_ = swift_resources_.get().last_receive_error_;
    OptionalString& last_attach_error_ = swift_resources_.get().last_attach_error_;
    std::mutex& attach_error_mutex_ = swift_resources_.get().attach_error_mutex_;  // guards last_attach_error_ (attach/detach can race a reader)
    std::future<void>& vec0_training_future_ = swift_resources_.get().vec0_training_future_;
    // Background vec0 gap healing dispatched after open (off the open path).
    std::future<void>& vec0_reconcile_future_ = swift_resources_.get().vec0_reconcile_future_;

    void ensure_swift_tables(const SchemaVector& schemas,bool publish_background=true);
    /// Fingerprint of the Swift-declared schemas covering every DDL-driving
    /// attribute (properties, constraints, unions). See kLatticeSchemaFormatEpoch.
    std::string compute_swift_fingerprint_key(const SchemaVector& schemas) const;
    static std::string compute_swift_fingerprint_key_for(int32_t target_version,const SchemaVector& schemas);
    static detail::recovery_owner_schema recovery_catalog(const swift_configuration&,const SchemaVector&);
    // Lossless bounded declaration identity, never enrollment or source authority.
    // Swift constraints are bound by the actual recovery_catalog digest. Every
    // native descriptor field is length/count encoded rather than a schema cookie.
    static std::string managed_catalog_identity(const detail::recovery_owner_schema& catalog) {
        if(!catalog.valid()||catalog.models.empty())throw std::runtime_error("managed open catalog unavailable");
        std::ostringstream out;
        auto text=[&](const std::string& value){out<<value.size()<<':'<<value;};
        auto number=[&](auto value){out<<value<<';';};
        text("managed-open-catalog-v2");text(catalog.swift_digest);text(catalog.swift_fingerprint);number(catalog.models.size());
        for(const auto& [name,model]:catalog.models){
            text(name);text(model.table_name);number(catalog.swift_models.count(name));number(model.properties.size());
            const auto constraints=catalog.swift_constraints.find(name);
            if(catalog.swift_models.count(name)&&constraints==catalog.swift_constraints.end())
                throw std::runtime_error("managed Swift constraints catalog unavailable");
            number(constraints!=catalog.swift_constraints.end());
            number(constraints==catalog.swift_constraints.end()?0:constraints->second.size());
            if(constraints!=catalog.swift_constraints.end())for(const auto& c:constraints->second){
                number(c.allows_upsert);number(c.columns.size());for(const auto& column:c.columns)text(column);
            }
            std::vector<const property_descriptor*> properties;
            for(const auto& property:model.properties)properties.push_back(&property);
            if(catalog.swift_models.count(name))std::sort(properties.begin(),properties.end(),
                [](const auto* a,const auto* b){return a->name<b->name;});
            for(const auto* property:properties){const auto& p=*property;
                text(p.name);number(static_cast<int>(p.type));number(static_cast<int>(p.kind));
                text(p.target_table);text(p.link_table);text(p.column_name);
                for(bool flag:{p.nullable,p.is_vector,p.is_geo_bounds,p.is_full_text,p.is_indexed,p.is_unique,p.is_union,p.no_history})number(flag);
                text(p.union_desc.union_table_name);number(p.union_desc.cases.size());
                for(const auto& c:p.union_desc.cases){text(c.case_name);number(c.values.size());
                    for(const auto& v:c.values){text(v.param_name);number(static_cast<int>(v.type));number(v.is_link);text(v.link_target);}}
            }
        }
        return out.str();
    }
    // REQUIRED PRIVATE OWNER UNIT: this declaration is a source prerequisite,
    // not an existing issuer/definition. Its implementation must validate the
    // retained native launch owner, physical file, exact current version and
    // complete catalog before any Core/Swift DDL, preserve nonmutating existing
    // open, and distinguish true seeded fresh origin without a caller flag.
    // There is deliberately no generic constructor fallback here.
    static std::shared_ptr<swift_lattice> open_managed_ordinary(const swift_configuration&,
        const SchemaVector&,const detail::ordinary_context&,
        const std::shared_ptr<const detail::ordinary_managed_child_admission>&,
        const detail::ordinary_launch::deadline* dormant_end = nullptr,bool* dormant_claimed = nullptr);
    friend class detail::LatticeCache;

    /// Populate schemas_/constraints_/union_schemas_ and the core link-table
    /// registries with zero SQL writes (write-free fast path).
    void populate_swift_in_memory_state(const SchemaVector& schemas);
    void populate_pending_swift_model_registries_();
    void replace_swift_model_registry_data_(const detail::recovery_owner_schema&,const detail::observer_delivery_open_startup* pending=nullptr);
    friend struct observer_delivery_custody_test_access;
    /// Heal vec0 rows missed by other connections' triggers, asynchronously.
    void dispatch_vec0_reconcile(const SchemaVector& schemas);
    friend struct dynamic_object;
    friend struct link_list;
    struct graph_mutation_plan;
    using graph_objects = std::vector<std::shared_ptr<dynamic_object>>;
    // Existing managed handles remain borrowed read targets only after exact
    // physical owner comparison. Unmanaged roots are privately cloned PRE SQL.
    void mutate_graph(const std::vector<dynamic_object*>& roots,
                      graph_objects retained,
                      const std::function<void(const graph_objects&)>& finish_body = {},
                      const std::function<void()>& publish = {},
                      const std::string& preserved_global_id = "");
    // Closed borrowed input is admitted before a roots/holds/clone or managed
    // result is allocated. The existing graph journal owns final adoption.
    void add_observer_delivery(const detail::observer_delivery_swift_input::input&);
    std::vector<managed<swift_dynamic_object>> add_observer_delivery_values(
        const detail::observer_delivery_swift_input::input&);
    void mutate_link(dynamic_object&, const std::string&, const std::shared_ptr<dynamic_object>&);
    void mutate_list(link_list&, const std::shared_ptr<dynamic_object>&, std::optional<size_t>);
    void mutate_union(dynamic_object&, const std::string&, const union_value&);
    void mutate_list_remove(link_list&, std::optional<size_t>);
    void persist_union_values(swift_dynamic_object& unmanaged_obj,
                              const std::string& table_name, int64_t parent_id);
    // For an `allowsUpsert` constraint that includes a to-one link column, write
    // the link's globalId into the `<link>__link_gid` shadow column as part of
    // the row INSERT so `ON CONFLICT (..., <link>__link_gid)` can fire.
    static void materialize_link_shadow_conflict_cols(swift_dynamic_object& obj,
                                                      const std::vector<std::string>& upsert_cols);

public:
    // Add with schema from the object instance
    void add(dynamic_object& obj);
    void add_preserving_global_id(dynamic_object& obj, const std::string& preserved_global_id);
    void add(const dynamic_object_ref& ref) {
        if (!ref.impl_) throw std::runtime_error("Cannot add an empty object ref");
        add(*ref.impl_);
    }
    void add(const dynamic_object_ref& ref,
             cxx_error& err) {
        (void)sealed_with_error(err, [&] {
            if (!ref.impl_) throw std::runtime_error("Cannot add an empty object ref");
            if (is_closed()) throw std::runtime_error("Cannot add to a closed lattice");
            add(*ref.impl_);
        });
    }

    void add_preserving_global_id(const dynamic_object_ref& ref, const std::string& preserved_global_id) {
        if (!ref.impl_) throw std::runtime_error("Cannot add an empty object ref");
        add_preserving_global_id(*ref.impl_, preserved_global_id);
    }

    // Preserve identity without allowing a SQLite/C++ failure to unwind through Swift.
    void add_preserving_global_id(const dynamic_object_ref& ref, const std::string& preserved_global_id,
                                  cxx_error& err) {
        (void)sealed_with_error(err, [&] {
            if (!ref.impl_) throw std::runtime_error("Cannot add an empty object ref");
            if (is_closed()) throw std::runtime_error("Cannot add to a closed lattice");
            add_preserving_global_id(*ref.impl_, preserved_global_id);
        });
    }

    void add_bulk(std::vector<dynamic_object>& objects);
    void add_bulk(std::vector<dynamic_object*>& objects);
#if LATTICE_HAS_FRT
    void add_bulk(std::vector<dynamic_object_ref*>& objects);
#else
    void add_bulk(std::vector<dynamic_object_ref>& objects);
#endif
    
    // Bulk add with schema from the first object instance
    std::vector<managed<swift_dynamic_object>> add_bulk(std::vector<swift_dynamic_object>&& objects) {
        if (objects.empty()) {
            return {};
        }
        if(detail::observer_delivery_enabled(*this))
            return add_observer_delivery_values(detail::observer_delivery_swift_input::unmanaged(objects));
        // Use schema from first object (all objects in the batch should have the same schema)
        auto schema = objects[0].instance_schema();
        return lattice_db::add_bulk_with_schema(std::move(objects), schema);
    }

    // Remove using the object's table name
    bool remove(dynamic_object&& obj);
    bool remove(const dynamic_object_ref& obj);
    
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
                result->properties_ = copy_runtime_properties_(*props);
                result->source.properties = copy_runtime_properties_(*props);
            }
        }
        return result;
    }

    /// Get all objects from a table, optionally filtered and sorted.
    /// `params` binds the `?` placeholders in `where_clause`; it is TRAILING
    /// and DEFAULTED so every positional caller (notably LatticeJS's 7-argument
    /// wasm binding) keeps compiling unchanged.
    std::vector<managed<swift_dynamic_object>> objects(
        const std::string& table_name,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt,
        OptionalString distinct_by = std::nullopt,
        const ColumnValueVector& params = {}) {
        // Never throws into Swift (interop cannot catch C++ exceptions) —
        // empty on failure, e.g. transient SQLITE_NOMEM under memory pressure.
        clear_bridge_outcome();
        try {
        auto rows = query_rows(table_name, where_clause, order_by, limit, offset, group_by, distinct_by, params);
        return hydrate_swift_rows(std::move(rows), table_name);
        } catch (const std::exception& e) {
            record_bridge_error(e.what());
            LOG_ERROR("query", "objects(%s) failed: %s", table_name.c_str(), e.what());
            return {};
        }
    }

    std::vector<managed<swift_dynamic_object>> union_objects(
        const std::vector<std::string>& table_names,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        const ColumnValueVector& params = {}) {
        // Never throws into Swift — empty on failure. A row missing '_type'
        // is a malformed union query; skip the row (it used to throw, which
        // terminated the process at the interop boundary).
        clear_bridge_outcome();
        try {
        auto rows = query_union_rows(table_names, where_clause, order_by, limit, offset, params);
        std::vector<managed<swift_dynamic_object>> results;
        results.reserve(rows.size());

        for (const auto& row : rows) {
            auto type_it = row.find("_type");
            if (type_it == row.end() ||
                !std::holds_alternative<std::string>(type_it->second)) {
                LOG_ERROR("swift_lattice", "Bad query: row missing '_type' column");
                continue;
            }
            auto table_name = std::get<std::string>(type_it->second);
            const RuntimeSwiftSchema* props = get_properties_for_table(table_name);
            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            // Populate properties_ from stored schema
            if (props) {
                obj.properties_ = copy_runtime_properties_(*props);
                obj.source.properties = copy_runtime_properties_(*props);
            }
            results.push_back(std::move(obj));
        }
        return results;
        } catch (const std::exception& e) {
            record_bridge_error(e.what());
            LOG_ERROR("query", "union_objects failed: %s", e.what());
            return {};
        }
    }
    
    // count overloads: never throw into Swift — 0 on failure.
    size_t count(const std::string& table_name,
                 OptionalString where_clause,
                 OptionalString group_by,
                 OptionalString distinct_by,
                 const ColumnValueVector& params = {}) {
        clear_bridge_outcome();
        try { return lattice_db::count(table_name, where_clause, group_by, distinct_by, params); }
        catch (const std::exception& e) { record_bridge_error(e.what()); LOG_ERROR("query", "count failed: %s", e.what()); return 0; }
    }

    size_t count(const std::string& table_name,
                 OptionalString where_clause,
                 OptionalString group_by) {
        clear_bridge_outcome();
        try { return lattice_db::count(table_name, where_clause, group_by); }
        catch (const std::exception& e) { record_bridge_error(e.what()); LOG_ERROR("query", "count failed: %s", e.what()); return 0; }
    }

    size_t count(const std::string& table_name,
                 OptionalString where_clause) {
        clear_bridge_outcome();
        try { return lattice_db::count(table_name, where_clause, std::nullopt); }
        catch (const std::exception& e) { record_bridge_error(e.what()); LOG_ERROR("query", "count failed: %s", e.what()); return 0; }
    }

    size_t count(const std::string& table_name) {
        return lattice_db::count(table_name, std::nullopt, std::nullopt);
    }

    bool delete_where(const std::string& table_name, std::optional<std::string> where_clause,
                      const ColumnValueVector& params = {}) {
        return lattice_db::delete_where(table_name, where_clause, params);
    }

    bool delete_where(const std::string& table_name) {
        return lattice_db::delete_where(table_name, std::nullopt);
    }

    int64_t compact_audit_log() {
        return lattice_db::compact_audit_log();
    }

    int64_t force_compact_audit_log() {
        return lattice_db::force_compact_audit_log();
    }

    int64_t safe_compact_audit_log(int64_t stale_threshold_seconds = 0) {
        return lattice_db::safe_compact_audit_log(stale_threshold_seconds);
    }

    void backdate_replication_slots(int64_t seconds) {
        lattice_db::backdate_replication_slots(seconds);
    }

    int64_t generate_history() {
        return lattice_db::generate_history();
    }

    /// Process-global count of SQL statements issued through the database
    /// layer's public funnels, across ALL connections and lattices. Exposed
    /// for statement-budget regression tests (e.g. "a depth-1 recall issues
    /// O(K+C+F) statements, not O(fields×K)").
    static uint64_t total_sql_statement_count() SWIFT_NAME(totalSQLStatementCount()) {
        return database::total_statement_count();
    }

    /// Thread-local twin — exact budgets on single-threaded read paths.
    static uint64_t thread_sql_statement_count() SWIFT_NAME(threadSQLStatementCount()) {
        return database::thread_statement_count();
    }

    /// Checkpoint the WAL file, flushing all changes to the main database file.
    /// Logs AND returns the outcome — TRUNCATE checkpoints silently fail under
    /// concurrent readers, and an ignored rc is how multi-GB WAL files
    /// accumulate. `busy` 1 = a reader/writer held the WAL; `checkpointed <
    /// log_frames` = partial.
    checkpoint_outcome checkpoint() {
        // PRAGMA instead of sqlite3_wal_checkpoint_v2: on Linux this header is
        // compiled in TUs that include sqlite3ext.h, where the C API names are
        // macros over the undeclared sqlite3_api extension pointer. The pragma
        // returns one row: (busy, log-frames, checkpointed-frames).
        long long busy = 1, nLog = -1, nCkpt = -1;
        try {
            auto rows = db().query("PRAGMA wal_checkpoint(TRUNCATE)", {});
            if (!rows.empty()) {
                auto get = [&](const char* k) -> long long {
                    auto it = rows[0].find(k);
                    return (it != rows[0].end() && std::holds_alternative<int64_t>(it->second))
                        ? std::get<int64_t>(it->second) : -1;
                };
                busy = get("busy"); nLog = get("log"); nCkpt = get("checkpointed");
            }
        } catch (...) { busy = 1; }
        if (busy != 0 || (nLog >= 0 && nCkpt < nLog)) {
            LOG_INFO("swift_lattice", "checkpoint(TRUNCATE): busy=%lld, frames=%lld, checkpointed=%lld%s",
                     busy, nLog, nCkpt,
                     (busy == 0 && nCkpt < nLog) ? " (partial — readers held the WAL)" : "");
        }
        checkpoint_outcome out;
        out.busy = busy != 0;
        out.log_frames = static_cast<int64_t>(nLog);
        out.checkpointed = static_cast<int64_t>(nCkpt);
        out.complete = busy == 0 && nLog >= 0 && nCkpt >= nLog;
        return out;
    }

    /// Bounded WAL checkpoint — the sync-pacer recipe (sync.cpp
    /// maybe_checkpoint) exposed as a callable: TRUNCATE with a small busy
    /// budget so a held reader snapshot makes it FAIL FAST instead of
    /// stalling every writer for the connection's full 30s busy timeout;
    /// on busy, fall back to PASSIVE (backfills as far as readers allow,
    /// blocks no one) and ask same-path keepers to advance their read
    /// generations so the NEXT truncate can land behind them. Returns the
    /// number of frames checkpointed (-1 when nothing could run).
    ///
    /// This is what maintenance paths (embedding-migration sweeps, repair)
    /// must call between write batches: the unbounded checkpoint() above is
    /// for teardown; calling it mid-workload trades read starvation for
    /// write starvation (Aug 2026 incident review).
    int64_t checkpoint_bounded(int64_t busy_budget_ms = 250) {
        auto res = db().wal_checkpoint(/*truncate=*/true,
                                       static_cast<int>(busy_budget_ms));
        if (res.busy != 0) {
            auto passive = db().wal_checkpoint(/*truncate=*/false,
                                               static_cast<int>(busy_budget_ms));
            lattice_db::request_generation_advance();
            LOG_INFO("swift_lattice",
                     "checkpoint_bounded: truncate busy — passive fallback "
                     "(frames=%lld checkpointed=%lld), generation advance requested",
                     (long long)passive.log_frames, (long long)passive.checkpointed);
            return passive.checkpointed;
        }
        return res.checkpointed;
    }

    /// Incremental statistics refresh ("PRAGMA optimize", bounded by
    /// analysis_limit). Cheap; safe to run from maintenance paths. Long-lived
    /// processes should call this periodically — the automatic close-time
    /// optimize never runs when the process is killed.
    void optimize() {
        try { optimize_statistics_(); } catch (...) {}
    }

    int64_t rebuild_vec0(const std::string& table, const std::string& column, int dims) {
        return lattice_db::rebuild_vec0(table, column, dims);
    }

    int64_t vacuum_vec0(const std::string& table, const std::string& column) {
        return lattice_db::vacuum_vec0(table, column);
    }

    /// Heal vec0 for one table+column: CREATES the index (dims sampled from
    /// data) when it is missing entirely — the sync-only-DB lifecycle — and
    /// backfills rows missed by other connections' triggers. Idempotent.
    /// Dispatched in the background on every open; public so maintenance
    /// paths and tests can invoke the heal deterministically.
    void reconcile_vec0_gaps_for(const std::string& table, const std::string& prop);

    /// Train all untrained IVF vec0 tables on the main write connection.
    void train_untrained_vec0_tables()
        SWIFT_NAME(trainUntrainedVec0Tables()) {
        LOG_INFO("train_untrained", "Starting training scan");
        // Core owns this writer's WAL commit hook. wal_autocheckpoint would
        // replace that hook and lose ordinary commit delivery after training.
        require_runtime_schema_ordinary_();
        for (const auto& [stored_table_name, schema] : schemas_) {
            const auto table_name=detail::swift_runtime_ordinary_text_(stored_table_name);
            for (const auto& [stored_prop_name, prop] : schema) {
                const auto prop_name=detail::swift_runtime_ordinary_text_(stored_prop_name);
                if (!prop.is_vector || prop.type != column_type::blob) continue;
                std::string vec_table = "_" + table_name + "_" + prop_name + "_vec";
                if (!db().table_exists(vec_table)) continue;
                train_vec0(table_name, prop_name);
            }
        }
        // Refresh read connections, then use the existing explicit checkpoint.
        close_read_db();
        try { db().query("PRAGMA wal_checkpoint(TRUNCATE)", {}); } catch (...) {}
        reopen_read_db();
        LOG_INFO("train_untrained", "Training scan complete");
    }

    /// Explicitly close all database connections and stop background services.
    /// Also evicts this instance from the LatticeCache so subsequent opens
    /// on the same path create a fresh instance (e.g., after nuclear compaction).
    /// Call before deleting database files to avoid "vnode unlinked while in use".
    void close(); // defined after LatticeCache
    lattice_close_result close_checked() noexcept;
    bool is_sync_connected() const { return lattice_db::is_sync_connected(); }
    bool is_sync_agent() const { return lattice_db::is_sync_agent(); }

    /// Count AuditLog entries pending sync whose rows are in the sync set.
    /// Deliberately UNSCOPED across sync_ids (per-sync_id set shape): this
    /// powers the whole-database "pending upload" progress counter, so an
    /// entry counts if ANY channel considers its row shared. EXISTS keeps
    /// the count per-entry — a row in N channels' sets never double-counts.
    int64_t pending_sync_entry_count() {
        auto rows = query_xproc(
            "SELECT COUNT(*) FROM AuditLog a"
            " WHERE a.isSynchronized = 0"
            " AND EXISTS (SELECT 1 FROM _lattice_sync_set ss"
            "             WHERE ss.table_name = a.tableName"
            "             AND ss.global_row_id = a.globalRowId)", {}
        );
        if (rows.empty()) return 0;
        auto& val = rows[0].begin()->second;
        return std::holds_alternative<int64_t>(val) ? std::get<int64_t>(val) : 0;
    }

    void update_sync_filter(const SyncFilterVector& filter);
    /// Per-channel variant: `channel` matches an ipc_target's channel name;
    /// the empty string targets the WSS synchronizer. Required on
    /// multi-channel hubs — the fan-out variant reconciles every channel
    /// against one filter.
    void update_sync_filter_for_channel(const std::string& channel, const SyncFilterVector& filter);
    void clear_sync_filter();
    void clear_sync_filter_for_channel(const std::string& channel);
    /// A6 — permanently retire a dead sync channel (state + set + slot).
    /// For channels that will reconnect, use reset via the daemon instead.
    void remove_sync_channel_state(const std::string& sync_id) {
        lattice_db::remove_sync_channel_state(sync_id);
    }

    /// Register a callback for cross-process idle hints (no new AuditLog entries).
    /// Fires directly on the xproc background thread, NOT through the scheduler.
    void set_on_xproc_idle(void* context,
                           void (*callback)(void*),
                           void (*destroy)(void*) = nullptr) {
        auto shared_ctx = context || callback
            ? std::shared_ptr<void>(context, destroy ? destroy : [](void*){})
            : std::shared_ptr<void>{};
        if (!callback) {
            lattice_db::set_on_xproc_idle(nullptr);
            return;
        }
        auto cb = callback;
        lattice_db::set_on_xproc_idle([shared_ctx, cb]() {
            cb(shared_ctx.get());
        });
    }

    // Sync progress — callback (fires on synchronizer's std_thread_scheduler thread).
    // sync_id labels WHICH channel the update belongs to: multiple
    // synchronizers on one database multiplex through this one callback, and
    // unlabeled interleaved counters are undiagnosable (Aug 2026 incident).
    void set_on_sync_progress(void* context,
                               void (*callback)(void* ctx,
                                                int64_t pending_upload,
                                                int64_t total_upload,
                                                int64_t acked,
                                                int64_t received,
                                                const char* sync_id),
                               void (*destroy)(void*) = nullptr) {
        auto shared_ctx = context || callback
            ? std::shared_ptr<void>(context, destroy ? destroy : [](void*){})
            : std::shared_ptr<void>{};
        LOG_INFO("swift_lattice", "set_on_sync_progress: callback=%s (db=%s)",
                 callback ? "SET" : "CLEAR", config().path.c_str());
        if (!callback) {
            lattice_db::set_on_sync_progress(nullptr);
            return;
        }
        auto cb = callback;
        lattice_db::set_on_sync_progress(
            [shared_ctx, cb](const synchronizer::sync_progress& p) {
                cb(shared_ctx.get(), p.pending_upload, p.total_upload, p.acked, p.received,
                   p.sync_id.c_str());
            });
    }
    
    // Sync error — callback (fires on synchronizer's scheduler thread)
    void set_on_sync_error(void* context,
                           void (*callback)(void* ctx, const char* error, int64_t len),
                           void (*destroy)(void*) = nullptr) {
        auto shared_ctx = context || callback
            ? std::shared_ptr<void>(context, destroy ? destroy : [](void*){})
            : std::shared_ptr<void>{};
        if (!callback) {
            lattice_db::set_on_sync_error(nullptr);
            return;
        }
        auto cb = callback;
        lattice_db::set_on_sync_error(
            [shared_ctx, cb](const std::string& error) {
                cb(shared_ctx.get(), error.c_str(), static_cast<int64_t>(error.size()));
            });
    }

    // Sync state change — callback (fires on synchronizer's scheduler thread)
    void set_on_sync_state_change(void* context,
                                  void (*callback)(void* ctx, bool connected),
                                  void (*destroy)(void*) = nullptr) {
        auto shared_ctx = context || callback
            ? std::shared_ptr<void>(context, destroy ? destroy : [](void*){})
            : std::shared_ptr<void>{};
        if (!callback) {
            lattice_db::set_on_sync_state_change(nullptr);
            return;
        }
        auto cb = callback;
        lattice_db::set_on_sync_state_change(
            [shared_ctx, cb](bool connected) {
                cb(shared_ctx.get(), connected);
            });
    }

    /// Rebuild the database file, reclaiming unused space.
    /// Retires published readers before vacuuming and reopens them after.
    /// In-flight owned readers may finish on the retired connections; their
    /// SQLite snapshots can still make maintenance busy or partial.
    /// Returns true on success; a failure is LOGGED (it used to vanish inside
    /// the sealed wrapper — a "vacuum" that did nothing left no trace) and
    /// rethrown, which the sealed tier turns into `false` + last_bridge_error.
    ///
    /// NOTE: in WAL mode VACUUM writes the rebuilt image into the WAL; the
    /// main file only shrinks at the NEXT checkpoint. Callers that want the
    /// file size back should use reclaim_space(), which orders the steps.
    bool vacuum() {
        close_read_db();
        try {
            db().execute("VACUUM");
        } catch (const std::exception& e) {
            LOG_ERROR("swift_lattice", "VACUUM failed: %s", e.what());
            reopen_read_db();
            throw;
        }
        reopen_read_db();
        return true;
    }

    /// The whole "give the disk space back" recipe, in the order SQLite needs
    /// under WAL: retire this process's published readers and pooled read
    /// generations (in-flight borrows can still make a TRUNCATE partial) →
    /// VACUUM (rebuilds the live pages into the WAL) → TRUNCATE checkpoint
    /// (folds them into the main file and zeroes the WAL) → reopen readers.
    /// A second pass runs only if the page count did not fall (a checkpoint
    /// that lost to a concurrent reader). Reports what happened instead of
    /// pretending: page counts before/after, WAL bytes left, busy flag, and
    /// the failure message when a step threw.
    reclaim_result reclaim_space(int max_passes = 2) {
        reclaim_result r;
        r.pages_before = page_count_();
        for (int pass = 1; pass <= std::max(1, max_passes); ++pass) {
            r.passes = pass;
            retire_all_read_generations();
            close_read_db();
            try {
                db().execute("VACUUM");
            } catch (const std::exception& e) {
                r.error = e.what();
                LOG_ERROR("swift_lattice", "reclaim_space: VACUUM failed: %s", e.what());
                reopen_read_db();
                r.pages_after = page_count_();
                return r;
            }
            auto ck = db().wal_checkpoint(/*truncate=*/true, /*busy_budget_ms=*/2000);
            r.checkpoint_busy = ck.busy != 0;
            reopen_read_db();
            r.pages_after = page_count_();
            if (r.pages_after < r.pages_before || !r.checkpoint_busy) break;
        }
        std::error_code ec;
        const auto wal = std::filesystem::file_size(path() + "-wal", ec);
        r.wal_bytes_after = ec ? 0 : static_cast<int64_t>(wal);
        r.ok = r.error.empty();
        LOG_INFO("swift_lattice", "reclaim_space: pages %lld -> %lld, wal=%lld bytes, passes=%d, busy=%d",
                 (long long)r.pages_before, (long long)r.pages_after, (long long)r.wal_bytes_after,
                 r.passes, r.checkpoint_busy ? 1 : 0);
        return r;
    }

    int64_t page_count_() {
        try {
            auto rows = db().query("PRAGMA page_count", {});
            if (!rows.empty()) {
                const auto& v = rows[0].begin()->second;
                if (const auto* i = std::get_if<int64_t>(&v)) return *i;
            }
        } catch (...) {}
        return -1;
    }

    /// The header columns of one audit row (see `audit_header`). Selects only
    /// columns declared BEFORE `changedFields`, so a 300 KB payload row costs
    /// the same as an empty one.
    audit_header audit_header_for(int64_t audit_id) {
        audit_header h;
        auto rows = query_read(
            "SELECT tableName, operation, rowId, globalRowId FROM AuditLog WHERE id = ?", {audit_id});
        if (rows.empty()) return h;
        const auto& r = rows[0];
        auto str = [&](const char* k) {
            auto it = r.find(k);
            return (it != r.end() && std::holds_alternative<std::string>(it->second))
                ? std::get<std::string>(it->second) : std::string();
        };
        h.found = true;
        h.table_name = str("tableName");
        h.operation = str("operation");
        h.global_row_id = str("globalRowId");
        auto it = r.find("rowId");
        if (it != r.end() && std::holds_alternative<int64_t>(it->second)) h.row_id = std::get<int64_t>(it->second);
        return h;
    }

    const std::string& path() const { return config().path; }

    void begin_transaction() {
        lattice_db::begin_transaction();
    }
    void begin_transaction(cxx_error& e) {
        try {
            lattice_db::begin_transaction();
        } catch (const std::exception& ex) {
            e = ex;
        }
    }
    
    void commit() { lattice_db::commit(); }
    void rollback() { lattice_db::rollback(); }
    void write(void (*fn)()) {
        begin_transaction();
        fn();
        commit();
    }
    
    /// Attach another lattice. Returns false on failure (repeat attach of
    /// the same db is SUCCESS — idempotent); the reason is available via
    /// last_attach_error(). C++ exceptions never cross the Swift boundary.
    bool attach(swift_lattice& lattice);

    /// Remove an attached lattice (idempotent). Returns false on failure;
    /// reason via last_attach_error().
    bool detach(swift_lattice& lattice);

    OptionalString last_attach_error() const {
        std::lock_guard<std::mutex> lock(attach_error_mutex_);
        return last_attach_error_;
    }

    // ========================================================================
    // MARK: Observation API
    // ========================================================================

    // C function pointer observers (Swift-visible).
    // Same pattern as generic_scheduler: void* context + C fn pointers.
    //
    // The callback receives parallel arrays of the same per-row fields
    // the singular API exposed historically — `count` rows worth of
    // (op, row_id, global_row_id). One call per WAL flush. Single-row
    // events are delivered as a count=1 batch; this matches the C++
    // observer contract one-to-one and lets Swift consumers iterate.
    uint64_t add_table_observer(const std::string& table_name,
                                 void* context,
                                 void (*callback)(void* ctx,
                                                  const char* const* operations,
                                                  const int64_t* row_ids,
                                                  const char* const* global_row_ids,
                                                  size_t count),
                                 void (*destroy)(void*) = nullptr) {
        auto shared_ctx = std::shared_ptr<void>(context, destroy ? destroy : [](void*){});
        auto cb = callback;
        return lattice_db::add_table_observer(table_name,
            [shared_ctx, cb](const std::vector<change_event>& batch) {
                if (batch.empty()) return;
                // Build parallel arrays into stable storage that lives
                // for the duration of this synchronous callback. The C
                // pointers we hand to Swift point into these vectors;
                // they're valid until cb() returns.
                std::vector<const char*> ops; ops.reserve(batch.size());
                std::vector<int64_t> row_ids; row_ids.reserve(batch.size());
                std::vector<const char*> gids; gids.reserve(batch.size());
                for (const auto& [_, op, row_id, gid, __] : batch) {
                    ops.push_back(op.c_str());
                    row_ids.push_back(row_id);
                    gids.push_back(gid.c_str());
                }
                cb(shared_ctx.get(), ops.data(), row_ids.data(), gids.data(), batch.size());
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
    // Block-based observers (C++ callers on Apple only — hidden from Swift).
    // The block receives the same batched view as the lattice_db API.
    using swift_table_observer_callback = void (^)(void* context,
                                                   const std::vector<change_event>& batch);

    uint64_t add_table_observer(const std::string& table_name,
                                 void* context,
                                 swift_table_observer_callback callback) {
        return lattice_db::add_table_observer(table_name,
            [context, callback](const std::vector<change_event>& batch) {
                callback(context, batch);
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
    // MARK: Read generations + synchronous invalidation hooks
    // Live Results item A, Commit 4 (lattice repo
    // docs/design-results-item-A-SPEC.md §2.2/§2.3/§3/§4.1).
    //
    // Bridge-wide rules for THIS section:
    //   - Every entry point is wrapped in a catch-all. A db_error (or any
    //     exception) crossing the Swift interop boundary is std::terminate
    //     (§1.2 rung 5), so failures surface as sentinels — empty result,
    //     -1, 0, false — plus the per-thread stale flag below. No C++
    //     exception can reach Swift through this surface.
    //   - Generation-scoped reads route the SAME SQL builders the live read
    //     paths use (lattice_db::build_query_rows_sql / build_count_sql /
    //     build_bbox_shape_query) through lattice_db::query_at_generation, so
    //     the statement text is identical either way.
    // ========================================================================

private:
    /// Per-thread stale marker for the tolerant ladder (§2.5): set when a
    /// generation-scoped read (or a §4.1 id capture) could not be served —
    /// retired/unknown generation, statement interrupted by a force-retire,
    /// or a throwing core read — and the sentinel result must NOT be
    /// interpreted as "genuinely empty". thread_local rather than a member:
    /// generation reads run concurrently on reader threads, and a shared
    /// member flag would cross-talk between them. Swift consumers check
    /// last_generation_read_stale() immediately after the read, on the same
    /// thread.
    static bool& generation_read_stale_tl() {
        static thread_local bool stale = false;
        return stale;
    }

    /// Shared hydration for row-shaped query results (objects / objects_at /
    /// bbox variants): hydrate + populate stored schema properties. Keep the
    /// actual query image separately for position metadata; moving each row
    /// avoids a second copy of its strings/blobs. This never seeds the mutable
    /// row cache or contributes fields to a later write.
    std::vector<managed<swift_dynamic_object>> hydrate_swift_rows(
        std::vector<database::row_t>&& rows,
        const std::string& table_name) {
        std::vector<managed<swift_dynamic_object>> results;
        results.reserve(rows.size());
        const RuntimeSwiftSchema* props = get_properties_for_table(table_name);
        for (auto& row : rows) {
            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            if (props) {
                obj.properties_ = copy_runtime_properties_(*props);
                obj.source.properties = copy_runtime_properties_(*props);
            }
            obj.query_row_image_ = std::make_shared<const database::row_t>(std::move(row));
            results.push_back(std::move(obj));
        }
        return results;
    }

    struct bbox_query_plan {
        std::string sql;
        ColumnValueVector parameters;
    };

    /// Diagnostics are best-effort at the Swift exception boundary. Recording
    /// an allocation failure must not itself allocate successfully to be safe.
    static void record_bbox_failure(const char* message) noexcept {
        try { record_bridge_error(message); }
        catch (...) {} // Preserve the sentinel return even if diagnostic text is lost.
    }

    /// Metadata is inspected through the SAME query connection as the rows.
    /// A keeper acquired before ATTACH has only its main schema; consulting
    /// the writer's attachment vectors here would change that old snapshot.
    template <typename Query>
    static bbox_query_plan build_bbox_shape_query(
        Query&& query,
        const std::string& table_name,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        const OptionalString& where_clause,
        const OptionalString& order_by,
        OptionalInt64 limit,
        OptionalInt64 offset,
        const OptionalString& group_by,
        const OptionalString& distinct_by) {
        if (!std::isfinite(min_lat) || !std::isfinite(max_lat) || !std::isfinite(min_lon) || !std::isfinite(max_lon) ||
            min_lat > max_lat || min_lon > max_lon || (limit && *limit < -1) || (offset && *offset < 0))
            throw std::invalid_argument("invalid spatial bounds or pagination");
        const auto model = spatial_quoted_identifier(table_name);
        const auto sidecar = "_" + table_name + "_" + geo_column;
        (void)spatial_quoted_identifier(geo_column);
        bool routed = false;
        bool found_table = false;
        std::set<std::string> columns;
        for (const auto& row : query("PRAGMA table_info(" + model + ")", {})) {
            auto name = row.find("name");
            if (name == row.end() || !std::holds_alternative<std::string>(name->second)) continue;
            found_table = true;
            const auto& column = std::get<std::string>(name->second);
            columns.insert(column);
            routed = routed || column == "_source";
        }
        if (!found_table) throw db_error("spatial model table is missing");
        auto group_column = [&](const OptionalString& column) -> OptionalString {
            if (!column || column->empty()) return std::nullopt;
            // Typed Swift fields get identifier quoting (including spaces).
            // Existing native bbox accepted SQL grouping fragments; retain
            // qualified identifiers/expressions rather than narrowing that API.
            return columns.count(*column) ? spatial_quoted_identifier(*column) : *column;
        };
        const auto group = group_column(group_by), distinct = group_column(distinct_by);
        auto table_exists = [&](const std::string& schema, const std::string& name) {
            return !query("SELECT 1 FROM " + spatial_quoted_identifier(schema) +
                ".sqlite_master WHERE type='table' AND name=? LIMIT 1", {name}).empty();
        };
        std::vector<spatial_query_arm> arms;
        for (const auto& row : query("PRAGMA database_list", {})) {
            auto name = row.find("name");
            if (name == row.end() || !std::holds_alternative<std::string>(name->second)) continue;
            const auto& schema = std::get<std::string>(name->second);
            if (schema == "temp" || (!routed && schema != "main") || !table_exists(schema, table_name)) continue;
            if (!table_exists(schema, sidecar + "_rtree"))
                throw db_error("spatial index is missing from a physical store");
            arms.push_back({schema, table_exists(schema, sidecar)});
        }
        if (arms.empty()) throw db_error("spatial model has no physical table/index");
        auto predicate = build_spatial_membership_predicate(table_name, geo_column, arms, routed,
            {"?1", "?2", "?3", "?4"});
        if (where_clause && !where_clause->empty()) predicate += " AND (" + *where_clause + ')';
        std::string from = model;
        if (group && distinct) {
            from = "(SELECT " + model + ".* FROM " + model + " WHERE " + predicate +
                " GROUP BY " + *distinct + ") AS " + model;
            predicate.clear();
        }
        std::string sql = "SELECT " + model + ".* FROM " + from;
        if (!predicate.empty()) sql += " WHERE " + predicate;
        if (group) sql += " GROUP BY " + *group;
        else if (distinct) sql += " GROUP BY " + *distinct;
        if (order_by && !order_by->empty()) sql += " ORDER BY " + *order_by;
        if (limit || offset) sql += " LIMIT " + std::to_string(limit.value_or(-1));
        if (offset) sql += " OFFSET " + std::to_string(*offset);
        return {std::move(sql), {min_lat, max_lat, min_lon, max_lon}};
    }

    /// SQLite's connection mutex is recursive. This holds the actual live
    /// topology stable across metadata and SELECT without acquiring parent or
    /// foreign topology locks beneath it. Hydration runs after release.
    struct bbox_connection_guard {
        detail::store_physical_native_scope physical_call;
        sqlite3_mutex* mutex;
        explicit bbox_connection_guard(database& connection) : physical_call(connection) {
            if(!physical_call.entered())throw db_error("resource Native caller original unavailable");
            if (connection.is_closed() || !connection.internal_handle()) throw db_error("spatial connection is closed");
            mutex = sqlite3_db_mutex(connection.internal_handle());
            sqlite3_mutex_enter(mutex);
        }
        ~bbox_connection_guard() { sqlite3_mutex_leave(mutex); }
        bbox_connection_guard(const bbox_connection_guard&) = delete;
        bbox_connection_guard& operator=(const bbox_connection_guard&) = delete;
    };

    /// The guard spans metadata + rows. database::query may drain a settled
    /// write's deferred callbacks, so it must not run under this extra lock.
    /// This narrow collector accepts read-only statements and never drains or
    /// invokes lifecycle/observation hooks. Normal write paths are untouched.
    static std::vector<database::row_t> query_bbox_locked(database& connection,
        const std::string& sql, const ColumnValueVector& parameters) {
        sqlite3_stmt* statement = nullptr;
        struct finalize { sqlite3_stmt*& statement; ~finalize() { if (statement) ::lattice::detail::store_physical_finalize(statement); } } cleanup{statement};
        const char* tail = nullptr;
        database::record_statement();
        if (::lattice::detail::store_physical_prepare_v2(connection.internal_handle(), sql.c_str(), -1, &statement, &tail) != SQLITE_OK)
            throw db_error(sqlite3_errmsg(connection.internal_handle()));
        while (tail && *tail && std::isspace(static_cast<unsigned char>(*tail))) ++tail;
        if (!statement || (tail && *tail) || !sqlite3_stmt_readonly(statement) ||
            sqlite3_bind_parameter_count(statement) != static_cast<int>(parameters.size()))
            throw db_error("spatial query requires one read-only statement with matching bindings");
        for (size_t i = 0; i != parameters.size(); ++i) {
            const int index = static_cast<int>(i + 1);
            const int result = std::visit([&](const auto& value) -> int {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, std::nullptr_t>) return sqlite3_bind_null(statement, index);
                else if constexpr (std::is_same_v<T, int64_t>) return sqlite3_bind_int64(statement, index, value);
                else if constexpr (std::is_same_v<T, double>) return sqlite3_bind_double(statement, index, value);
                else if constexpr (std::is_same_v<T, std::string>)
                    return sqlite3_bind_text64(statement, index, value.data(), value.size(), SQLITE_TRANSIENT, SQLITE_UTF8);
                else return value.empty() ? sqlite3_bind_zeroblob(statement, index, 0) :
                    sqlite3_bind_blob64(statement, index, value.data(), value.size(), SQLITE_TRANSIENT);
            }, parameters[i]);
            if (result != SQLITE_OK) throw db_error("spatial parameter binding failed");
        }
        std::vector<std::string> columns;
        for (int i = 0; i != sqlite3_column_count(statement); ++i) {
            const auto* name = sqlite3_column_name(statement, i);
            if (!name) throw db_error("spatial column name allocation failed");
            columns.emplace_back(name);
        }
        std::vector<database::row_t> rows;
        int result;
        while ((result = ::lattice::detail::store_physical_step(statement)) == SQLITE_ROW) {
            database::row_t row;
            for (int i = 0; i != static_cast<int>(columns.size()); ++i) {
                column_value_t value;
                switch (sqlite3_column_type(statement, i)) {
                    case SQLITE_INTEGER: value = static_cast<int64_t>(sqlite3_column_int64(statement, i)); break;
                    case SQLITE_FLOAT: value = sqlite3_column_double(statement, i); break;
                    case SQLITE_TEXT: {
                        const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(statement, i));
                        if (!text) throw db_error("spatial text allocation failed");
                        value = std::string(text, sqlite3_column_bytes(statement, i)); break;
                    }
                    case SQLITE_BLOB: {
                        const auto* blob = static_cast<const uint8_t*>(sqlite3_column_blob(statement, i));
                        const int length = sqlite3_column_bytes(statement, i);
                        if (length && !blob) throw db_error("spatial blob allocation failed");
                        value = length ? std::vector<uint8_t>(blob, blob + length) : std::vector<uint8_t>{}; break;
                    }
                    default: value = nullptr; break;
                }
                row.emplace(columns[static_cast<size_t>(i)], std::move(value));
            }
            rows.push_back(std::move(row));
        }
        if (result != SQLITE_DONE) throw db_error(sqlite3_errmsg(connection.internal_handle()));
        return rows;
    }

public:
    /// Whether the LAST generation-scoped read on THIS THREAD returned a
    /// sentinel because it could not be served (retired generation, thrown
    /// core read, exhausted §4.1 capture retry). Cleared at the start of
    /// every generation-scoped entry point; check it on the same thread,
    /// immediately after the read, before trusting an empty result.
    static bool last_generation_read_stale()
        SWIFT_NAME(lastGenerationReadStale()) {
        return generation_read_stale_tl();
    }

    // ---- Synchronous invalidation hooks (§2.3) ----
    //
    // C function pointer trampoline (Swift-visible), same pattern as
    // add_table_observer: void* context + C fn pointer + optional destroy.
    //
    // THE CALLBACK RUNS INLINE ON THE WRITER'S THREAD — for file DBs inside
    // SQLite's post-commit C hook frame — once per settled top-level
    // transaction, BEFORE the scheduler-dispatched observer fan-out, fanned
    // to every alive same-path instance. The §2.3 restrictions apply to the
    // Swift body verbatim: atomic epoch increments / dirty-flag stores ONLY.
    // No SQL, no allocation-heavy work, nothing that can throw, and no lock
    // that any thread ever holds across a SQL statement (leaf locks only).
    //
    // `reason` maps invalidation_reason: 0 = commit (payload = the batch's
    // changed table names, EMPTY for bookkeeping-only commits), 1 = rollback
    // (no change batch — re-capture at next access), 2 = advance (re-pin at
    // next access; §3.3/§3.4), 3 = recovery (payload-free all-content dirty).
    // The pointer arrays are valid only for the
    // duration of the callback.
    uint64_t add_invalidation_hook(void* context,
                                   void (*callback)(void* ctx,
                                                    const char* const* changed_tables,
                                                    size_t count,
                                                    int reason),
                                   void (*destroy)(void*) = nullptr) {
        auto shared_ctx = std::shared_ptr<void>(context, destroy ? destroy : [](void*){});
        auto cb = callback;
        return lattice_db::add_invalidation_hook(
            [shared_ctx, cb](const std::vector<std::string>& changed_tables,
                             invalidation_reason reason) {
                try {
                    std::vector<const char*> tables;
                    tables.reserve(changed_tables.size());
                    for (const auto& t : changed_tables) tables.push_back(t.c_str());
                    cb(shared_ctx.get(), tables.data(), tables.size(),
                       static_cast<int>(reason));
                } catch (...) {
                    // §2.3: nothing may throw out of the hook frame — for
                    // file DBs it is SQLite's C hook frame, and an unwind
                    // through it is process death.
                }
            });
    }

    /// Additive overload (spec Commit 4): identical contract to
    /// add_invalidation_hook, plus `changed_fields` — parallel to
    /// `changed_tables`, per-table. changed_fields[i] is NON-EMPTY only when
    /// every event for changed_tables[i] in the batch was an UPDATE with a
    /// known changed-field list, and holds the comma-joined deduped union of
    /// plain field names ("age,name"). EMPTY = must invalidate (INSERT or
    /// DELETE present, fields unknown, rollback/advance). Delivered from
    /// Commit 4; consumed by the v1.1 changedFields skip (Commit 8).
    uint64_t add_invalidation_hook_with_fields(
        void* context,
        void (*callback)(void* ctx,
                         const char* const* changed_tables,
                         const char* const* changed_fields,
                         size_t count,
                         int reason),
        void (*destroy)(void*) = nullptr) {
        auto shared_ctx = std::shared_ptr<void>(context, destroy ? destroy : [](void*){});
        auto cb = callback;
        return lattice_db::add_invalidation_hook_detailed(
            [shared_ctx, cb](const std::vector<invalidation_table_change>& changes,
                             invalidation_reason reason) {
                try {
                    std::vector<const char*> tables;
                    std::vector<const char*> fields;
                    tables.reserve(changes.size());
                    fields.reserve(changes.size());
                    for (const auto& c : changes) {
                        tables.push_back(c.table.c_str());
                        fields.push_back(c.changed_fields.c_str());
                    }
                    cb(shared_ctx.get(), tables.data(), fields.data(),
                       tables.size(), static_cast<int>(reason));
                } catch (...) {
                    // See the basic overload: never unwind the hook frame.
                }
            });
    }

    void remove_invalidation_hook(uint64_t token) {
        lattice_db::remove_invalidation_hook(token);
    }

    /// Payload-free recovery wake. File owners opt into background witness
    /// preparation and periodic retry; callback delivery uses their scheduler.
    /// The context is consumed even when registration refuses (returns zero).
    /// No AuditLog/CollectionChange history is manufactured by this signal.
    uint64_t add_recovery_refresh_observer(void* context, void (*callback)(void*),
                                           void (*destroy)(void*) = nullptr) noexcept {
        try {
            auto shared = std::shared_ptr<void>(context, destroy ? destroy : [](void*){});
            if (!callback) return 0;
            return lattice_db::add_recovery_refresh_observer([shared, callback] { callback(shared.get()); });
        } catch (...) { return 0; }
    }
    void remove_recovery_refresh_observer(uint64_t token) noexcept {
        try { lattice_db::remove_recovery_refresh_observer(token); } catch (...) {}
    }

    // ---- Read-generation pool (§2.2/§3) ----
    // Catch-all wrappers over the core pool. The core primitives already
    // avoid throwing on their designed refusal paths (memory-family stores
    // return 0, dead generations return nullopt); the wrappers guarantee it
    // for everything else too.

    /// Returns the new generation id, or 0 when this storage refuses keepers
    /// (memory family, Emscripten, closed instance) or the store cannot be
    /// pinned — treat 0 as "no keeper" and use the non-generation path.
    uint64_t acquire_read_generation() {
        try {
            return lattice_db::acquire_read_generation();
        } catch (...) {
            return 0;
        }
    }

    /// Add a logical hold on a live generation. false = already retired or
    /// retiring — re-resolve.
    bool retain_read_generation(uint64_t generation_id) {
        try {
            return lattice_db::retain_read_generation(generation_id);
        } catch (...) {
            return false;
        }
    }

    /// Drop a hold; at refcount 0 the keeper COMMITs and its connection
    /// returns to the idle pool. Releasing an already-retired id is a no-op.
    void release_read_generation(uint64_t generation_id) {
        try {
            lattice_db::release_read_generation(generation_id);
        } catch (...) {
        }
    }

    /// Force-retire every live generation on this instance (§3.4 protocol).
    /// Bridged for Lattice.retireAllGenerations() — the §3.6 iOS suspension
    /// contract (retire on backgrounding so checkpoints land while
    /// suspended).
    void retire_all_read_generations() {
        try {
            lattice_db::retire_all_read_generations();
        } catch (...) {
        }
    }

    /// Live generations across EVERY alive same-path instance (§3.3
    /// aggregation — the synchronizer owns its own instance, so any
    /// single-instance count reads 0 from the wrong handle).
    size_t read_generations_outstanding() {
        try {
            return lattice_db::read_generations_outstanding();
        } catch (...) {
            return 0;
        }
    }

    /// Live generations held by THIS instance only.
    size_t local_read_generations_outstanding() {
        try {
            return lattice_db::local_read_generations_outstanding();
        } catch (...) {
            return 0;
        }
    }

    /// §3.2 maintenance tick: TTL retire, absolute age cap, pending
    /// WAL-threshold evictions. The Swift coordinator's maintenance timer is
    /// the actor of record for EVERY storage/config (a sync pacer is only
    /// ever a second caller).
    void run_read_pool_maintenance() {
        try {
            lattice_db::run_read_pool_maintenance();
        } catch (...) {
        }
    }

private:
    /// Checked public maintenance path. Keep the legacy timer's void/catch
    /// behavior unchanged; this distinct path lets its real error reach the
    /// outer Swift boundary. No pressure flag or epoch is reset here.
    read_pool_maintenance_result run_read_pool_maintenance_checked() {
        require_creator_process();
        detail::ordinary_require_creator(config().ordinary_context);
        if (is_closed()) throw db_error("read-pool maintenance requires its live original owner");
        lattice_db::run_read_pool_maintenance();
        const bool pending = lattice_db::wal_eviction_pending();
        if (is_closed()) throw db_error("read-pool maintenance owner closed during its tick");
        read_pool_maintenance_result result;
        result.completed = true;
        result.pressure_pending = pending;
        return result;
    }

    automatic_read_maintenance_enrollment_result enable_automatic_read_pool_maintenance_checked() {
        require_creator_process();
        detail::ordinary_require_creator(config().ordinary_context);
        if (is_closed()) throw db_error("automatic read maintenance requires its live original owner");
        const bool enabled = lattice_db::enable_automatic_read_pool_maintenance();
        if (is_closed()) throw db_error("automatic read maintenance owner closed during enrollment");
        automatic_read_maintenance_enrollment_result result;
        result.completed = true;
        result.enabled = enabled;
        return result;
    }

public:
    // ---- Pool tunables (§1.7 ResultsTuning; forwarded per-Lattice from
    //      Swift in Commit 5). Plain atomic stores — nothrow by
    //      construction. ----
    void set_read_generation_ttl_ms(int64_t ms) {
        lattice_db::set_read_generation_ttl_ms(ms);
    }
    void set_read_generation_max_age_ms(int64_t ms) {
        lattice_db::set_read_generation_max_age_ms(ms);
    }
    void set_wal_keeper_eviction_threshold_bytes(int64_t bytes) {
        lattice_db::set_wal_keeper_eviction_threshold_bytes(bytes);
    }
    int64_t wal_keeper_eviction_threshold_bytes() const {
        return lattice_db::wal_keeper_eviction_threshold_bytes();
    }
    /// Diagnostics/tests: an armed-but-unserviced WAL threshold eviction.
    bool wal_eviction_pending() const {
        return lattice_db::wal_eviction_pending();
    }

    // ---- Generation-scoped reads (§2.2: one MVCC snapshot per generation,
    //      keeper-routed; §2.5 tolerant ladder on failure) ----

    /// objects() executed at a held read generation. Same SQL builder as
    /// objects(); rows come from the generation's keeper connection. Returns
    /// EMPTY + stale flag (last_generation_read_stale()) when the generation
    /// is no longer live or the read failed — an empty vector with the flag
    /// clear is a genuine empty result.
    std::vector<managed<swift_dynamic_object>> objects_at(
        uint64_t generation_id,
        const std::string& table_name,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt,
        OptionalString distinct_by = std::nullopt,
        const ColumnValueVector& params = {}) {
        generation_read_stale_tl() = false;
        try {
            auto rows = query_at_generation(
                generation_id,
                build_query_rows_sql(table_name, where_clause, order_by, limit,
                                     offset, group_by, distinct_by),
                params);
            if (!rows) {
                generation_read_stale_tl() = true;
                return {};
            }
            return hydrate_swift_rows(std::move(*rows), table_name);
        } catch (...) {
            generation_read_stale_tl() = true;
            return {};
        }
    }

    /// count() executed at a held read generation (same SQL builder,
    /// keeper-routed). Returns -1 + stale flag when the generation is no
    /// longer live or the read failed — callers re-resolve through the
    /// tolerant ladder; 0 is a genuine zero.
    int64_t count_at(uint64_t generation_id,
                     const std::string& table_name,
                     OptionalString where_clause = std::nullopt,
                     OptionalString group_by = std::nullopt,
                     OptionalString distinct_by = std::nullopt,
                     const ColumnValueVector& params = {}) {
        generation_read_stale_tl() = false;
        try {
            auto rows = query_at_generation(
                generation_id,
                build_count_sql(table_name, where_clause, group_by, distinct_by),
                params);
            if (!rows) {
                generation_read_stale_tl() = true;
                return -1;
            }
            if (!rows->empty()) {
                auto it = (*rows)[0].find("cnt");
                if (it != (*rows)[0].end() &&
                    std::holds_alternative<int64_t>(it->second)) {
                    return std::get<int64_t>(it->second);
                }
            }
            return 0;
        } catch (...) {
            generation_read_stale_tl() = true;
            return -1;
        }
    }

    /// objects_within_bbox() executed at a held read generation (same SQL
    /// builder, keeper-routed). EMPTY + stale flag on failure, as objects_at.
    std::vector<managed<swift_dynamic_object>> objects_within_bbox_at(
        uint64_t generation_id,
        const std::string& table_name,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt)
        SWIFT_NAME(objectsWithinBBoxAt(generation:table:geoColumn:minLat:maxLat:minLon:maxLon:where:orderBy:limit:offset:groupBy:)) {
        return objects_within_bbox_shape_at(generation_id, table_name, geo_column,
            min_lat, max_lat, min_lon, max_lon, where_clause, order_by, limit, offset, group_by, std::nullopt);
    }

    std::vector<managed<swift_dynamic_object>> objects_within_bbox_shape_at(
        uint64_t generation_id,
        const std::string& table_name, const std::string& geo_column,
        double min_lat, double max_lat, double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt, OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt, OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt, OptionalString distinct_by = std::nullopt)
        SWIFT_NAME(objectsWithinBBoxShapeAt(generation:table:geoColumn:minLat:maxLat:minLon:maxLon:where:orderBy:limit:offset:groupBy:distinctBy:)) {
        generation_read_stale_tl() = false;
        clear_bridge_outcome();
        try {
            auto query = [&](const std::string& sql, const ColumnValueVector& params) {
                auto rows = query_at_generation(generation_id, sql, params);
                if (!rows) throw db_error("spatial read generation is stale");
                return std::move(*rows);
            };
            auto plan = build_bbox_shape_query(query, table_name, geo_column, min_lat, max_lat,
                min_lon, max_lon, where_clause, order_by, limit, offset, group_by, distinct_by);
            return hydrate_swift_rows(query(plan.sql, plan.parameters), table_name);
        } catch (const std::exception& error) {
            generation_read_stale_tl() = true;
            record_bbox_failure(error.what());
            return {};
        } catch (...) {
            generation_read_stale_tl() = true;
            record_bbox_failure("spatial generation read failed");
            return {};
        }
    }

    /// §4.1 materialized-id capture for the memory family (and any storage
    /// without keepers): `SELECT id FROM T [WHERE …] [ORDER BY …]` — the
    /// same builder as objects()/objects_at with an `id` select list, so the
    /// id order is EXACTLY the row order the equivalent row query returns.
    /// Runs inside a capture transaction (BEGIN waits out a concurrent
    /// same-connection transaction) under the per-store write gate, with a
    /// bounded LOCKED retry: SQLITE_LOCKED is immediate-fail on shared-cache
    /// stores and sqlite3_unlock_notify is not compiled uniformly, so a
    /// sleep-backoff loop is mandatory (§4.1 mechanism 3), degrading after a
    /// ~250 ms budget to EMPTY + stale flag. Never throws into Swift.
    std::vector<int64_t> query_ids_at(const std::string& table_name,
                                      OptionalString where_clause = std::nullopt,
                                      OptionalString order_by = std::nullopt,
                                      const ColumnValueVector& params = {}) {
        generation_read_stale_tl() = false;
        const auto sql = build_query_rows_sql(table_name, where_clause, order_by,
                                              std::nullopt, std::nullopt,
                                              std::nullopt, std::nullopt, "id");
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::milliseconds(250);
        int backoff_ms = 1;
        for (;;) {
            bool in_txn = false;
            try {
                lattice_db::begin_transaction();  // gate + BEGIN (retries in-txn races)
                in_txn = true;
                auto rows = db().query(sql, params);
                lattice_db::commit();
                in_txn = false;
                std::vector<int64_t> ids;
                ids.reserve(rows.size());
                for (const auto& row : rows) {
                    auto it = row.find("id");
                    if (it != row.end() &&
                        std::holds_alternative<int64_t>(it->second)) {
                        ids.push_back(std::get<int64_t>(it->second));
                    }
                }
                return ids;
            } catch (const db_error& e) {
                if (in_txn) {
                    try { lattice_db::rollback(); } catch (...) {}
                }
                // Retry lock contention only ("database table is locked" /
                // "database is locked"); anything else is not transient.
                const bool locked =
                    std::string(e.what()).find("locked") != std::string::npos;
                if (!locked || clock::now() >= deadline) {
                    generation_read_stale_tl() = true;
                    return {};
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                backoff_ms = std::min(backoff_ms * 2, 20);
            } catch (...) {
                if (in_txn) {
                    try { lattice_db::rollback(); } catch (...) {}
                }
                generation_read_stale_tl() = true;
                return {};
            }
        }
    }

    /// `PRAGMA data_version` on the dedicated non-transaction xproc read
    /// connection (spec §4.4 belt; consumed by Commit 6). Changes exactly
    /// when a DIFFERENT connection (including another process) commits to
    /// the database — never for this connection's own reads. Returns -1 on
    /// failure; never throws into Swift.
    int64_t data_version() {
        try {
            auto rows = query_xproc("PRAGMA data_version", {});
            if (!rows.empty()) {
                auto it = rows[0].find("data_version");
                if (it != rows[0].end() &&
                    std::holds_alternative<int64_t>(it->second)) {
                    return std::get<int64_t>(it->second);
                }
                // Column-name defensive fallback (pragma naming varies).
                for (const auto& [key, value] : rows[0]) {
                    if (std::holds_alternative<int64_t>(value)) {
                        return std::get<int64_t>(value);
                    }
                }
            }
            return -1;
        } catch (...) {
            return -1;
        }
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
        return objects_within_bbox_shape(table_name, geo_column, min_lat, max_lat, min_lon, max_lon,
            where_clause, order_by, limit, offset, group_by, std::nullopt);
    }

    std::vector<managed<swift_dynamic_object>> objects_within_bbox_shape(
        const std::string& table_name, const std::string& geo_column,
        double min_lat, double max_lat, double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt, OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt, OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt, OptionalString distinct_by = std::nullopt)
        SWIFT_NAME(objectsWithinBBoxShape(table:geoColumn:minLat:maxLat:minLon:maxLon:where:orderBy:limit:offset:groupBy:distinctBy:)) {
        clear_bridge_outcome();
        try {
            std::vector<database::row_t> rows;
            {
                auto& connection = db();
                bbox_connection_guard guard(connection);
                auto query = [&](const std::string& sql, const ColumnValueVector& params) { return query_bbox_locked(connection, sql, params); };
                auto plan = build_bbox_shape_query(query, table_name, geo_column, min_lat, max_lat,
                    min_lon, max_lon, where_clause, order_by, limit, offset, group_by, distinct_by);
                rows = query(plan.sql, plan.parameters);
            }
            return hydrate_swift_rows(std::move(rows), table_name);
        } catch (const std::exception& error) {
            record_bbox_failure(error.what());
            return {};
        } catch (...) {
            record_bbox_failure("spatial read failed");
            return {};
        }
    }

    /// Count objects within a bounding box
    int64_t count_within_bbox(
        const std::string& table_name,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt)
        SWIFT_NAME(countWithinBBox(table:geoColumn:minLat:maxLat:minLon:maxLon:where:)) {
        return count_within_bbox_shape(table_name, geo_column, min_lat, max_lat, min_lon, max_lon,
            where_clause, std::nullopt, std::nullopt);
    }

    int64_t count_within_bbox_shape(
        const std::string& table_name, const std::string& geo_column,
        double min_lat, double max_lat, double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt,
        OptionalString group_by = std::nullopt, OptionalString distinct_by = std::nullopt)
        SWIFT_NAME(countWithinBBoxShape(table:geoColumn:minLat:maxLat:minLon:maxLon:where:groupBy:distinctBy:)) {
        clear_bridge_outcome();
        try {
            auto& connection = db();
            bbox_connection_guard guard(connection);
            auto query = [&](const std::string& sql, const ColumnValueVector& params) { return query_bbox_locked(connection, sql, params); };
            auto plan = build_bbox_shape_query(query, table_name, geo_column, min_lat, max_lat,
                min_lon, max_lon, where_clause, std::nullopt, std::nullopt, std::nullopt, group_by, distinct_by);
            const auto rows = query("SELECT COUNT(*) AS cnt FROM (" + plan.sql + ") AS " + spatial_quoted_identifier(table_name), plan.parameters);
            if (!rows.empty()) {
                auto count = rows.front().find("cnt");
                if (count != rows.front().end() && std::holds_alternative<int64_t>(count->second))
                    return std::get<int64_t>(count->second);
            }
            return 0;
        } catch (const std::exception& error) {
            record_bbox_failure(error.what());
            return 0;
        } catch (...) {
            record_bbox_failure("spatial count failed");
            return 0;
        }
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
        // Never throws into Swift — empty on failure.
        clear_bridge_outcome();
        try {
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

        const RuntimeSwiftSchema* props = get_properties_for_table(table_name);

        for (const auto& row : rows) {
            double distance = 0.0;
            auto dist_it = row.find("_distance");
            if (dist_it != row.end() && std::holds_alternative<double>(dist_it->second)) {
                distance = std::get<double>(dist_it->second);
            }

            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            if (props) {
                obj.properties_ = copy_runtime_properties_(*props);
                obj.source.properties = copy_runtime_properties_(*props);
            }
            results.push_back(std::make_pair(std::move(obj), distance));
        }
        return results;
        } catch (const std::exception& e) {
            record_bridge_error(e.what());
            LOG_ERROR("query", "geo_nearest failed: %s", e.what());
            return {};
        }
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
        // Never throws into Swift — empty on failure.
        clear_bridge_outcome();
        try {
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

        const RuntimeSwiftSchema* props = get_properties_for_table(table_name);

        for (const auto& row : rows) {
            double distance = 0.0;
            auto dist_it = row.find("_distance");
            if (dist_it != row.end() && std::holds_alternative<double>(dist_it->second)) {
                distance = std::get<double>(dist_it->second);
            }

            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            if (props) {
                obj.properties_ = copy_runtime_properties_(*props);
                obj.source.properties = copy_runtime_properties_(*props);
            }
            results.push_back(std::make_pair(std::move(obj), distance));
        }
        return results;
        } catch (const std::exception& e) {
            record_bridge_error(e.what());
            LOG_ERROR("query", "combined_query failed: %s", e.what());
            return {};
        }
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
    /// Result of CTE building — shared between full query and count query.
    struct CombinedQueryCtes {
        std::string sql;  // "WITH ... final_candidates AS (...) "
        std::vector<std::string> geo_ctes;
        std::vector<std::string> vec_ctes;
        std::vector<std::string> fts_ctes;
    };

    /// Builds the CTE prefix for a combined nearest query.
    /// Returns nullopt if no results are possible (e.g. empty query vector, missing vec0 table).
    /// Side effect: ensures vec0 tables exist for vector constraints.
    std::optional<CombinedQueryCtes> build_combined_nearest_ctes_(
        const std::string& table_name,
        const BoundsConstraintVector& bounds,
        const VectorConstraintVector& vectors,
        const GeoConstraintVector& geos,
        const TextConstraintVector& texts,
        const OptionalString& where_clause) {

        // Constants for geo calculations
        constexpr double METERS_PER_DEGREE = 111000.0;
        constexpr double DEG_TO_RAD = 3.14159265358979323846 / 180.0;

        // If no constraints, return empty CTEs
        if (bounds.empty() && vectors.empty() && geos.empty() && texts.empty()) {
            return CombinedQueryCtes{"", {}, {}, {}};
        }

        // Ensure vec0 tables exist — they may not have been created yet if
        // data arrived via sync after ensure_swift_tables ran with no vector data.
        for (const auto& vc : vectors) {
            std::string vec_table = "_" + table_name + "_" + vc.column + "_vec";
            int dims = static_cast<int>(vc.query_vector.size() / sizeof(float));
            if (dims > 0 && !db().table_exists(vec_table)) {
                ensure_vec0_table(table_name, vc.column, dims);
            }
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

            // Candidate currency is globalId (see the cross-arm note in
            // Step 4): join the main-arm model table to translate rtree
            // rowids. rtree sidecars are main-only today; main. keeps the
            // join off the TEMP union view.
            sql << cte_name << " AS ("
                << "SELECT m.globalId AS gid FROM " << rtree_table << " r"
                << " JOIN main." << table_name << " m ON m.id = r.id"
                << " WHERE r.minLat <= " << std::to_string(bc.max_lat)
                << " AND r.maxLat >= " << std::to_string(bc.min_lat)
                << " AND r.minLon <= " << std::to_string(bc.max_lon)
                << " AND r.maxLon >= " << std::to_string(bc.min_lon)
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
                << "SELECT m.globalId AS gid, " << haversine << " AS distance"
                << " FROM " << rtree_table << " r"
                << " JOIN main." << table_name << " m ON m.id = r.id"
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
                sql << "SELECT gid FROM " << cte_names[i];
            }
            sql << "), ";
        }

        // ====================================================================
        // Step 4: Vector constraints with pre-filtering
        // ====================================================================
        std::vector<std::string> vec_cte_names;
        for (const auto& vc : vectors) {
            // Empty query vector would crash sqlite-vec; return no results
            if (vc.query_vector.empty()) {
                return std::nullopt;
            }

            std::string vec_table = "_" + table_name + "_" + vc.column + "_vec";

            // Collect schemas that have this vec table (main + attached)
            std::vector<std::string> vec_schemas;
            if (db().table_exists(vec_table)) {
                vec_schemas.push_back("main");
            }
            for (const auto& alias : attached_aliases_) {
                std::string check_sql = "SELECT name FROM \"" + alias + "\".sqlite_master "
                                        "WHERE type='table' AND name=?";
                auto check = db().query(check_sql, {vec_table});
                if (!check.empty()) {
                    vec_schemas.push_back("\"" + alias + "\"");
                }
            }

            // vec0 table is created lazily on first vector insert (needs dimensions).
            // If no schema has the table, return no results.
            if (vec_schemas.empty()) {
                return std::nullopt;
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

            // Build WHERE conditions
            std::vector<std::string> where_conditions;
            if (!cte_names.empty()) {
                where_conditions.push_back("m.globalId IN (SELECT gid FROM " + candidates_cte + ")");
            }
            if (where_clause.has_value() && !where_clause.value().empty()) {
                where_conditions.push_back("(" + where_clause.value() + ")");
            }
            std::string where_suffix;
            if (!where_conditions.empty()) {
                where_suffix = " WHERE ";
                for (size_t i = 0; i < where_conditions.size(); ++i) {
                    if (i > 0) where_suffix += " AND ";
                    where_suffix += where_conditions[i];
                }
            }

            // Build CTE: UNION ALL across all schemas' vec tables.
            // MATCH retrieves IVF candidates with L2 distance. If the requested
            // metric is not L2, we re-score using the actual distance function.
            // WHERE filters are applied as post-filtering on candidates.
            // Oversample by 2x when filtering to compensate for post-filter loss.
            // When re-scoring with a non-L2 metric, additionally oversample 4x:
            // MATCH selects candidates by L2 distance, and the true top-k under
            // the requested metric need not lie within the L2 top-k. The CTE
            // then orders by the re-scored distance and truncates back to k.
            bool needs_rescore = (distance_func != "vec_distance_L2");
            int fetch_k = where_conditions.empty() ? vc.k : vc.k * 2;
            if (needs_rescore) fetch_k *= 4;
            // MATCH CTE: each schema's vec table is queried with MATCH, then
            // JOINed to THAT SCHEMA's model table (not the UNION ALL view) to
            // resolve gid → id and carry the distance through.
            // One candidate row per DISTINCT memory: the same globalId can
            // appear in several arms (hub + spoke replicas) — group to the
            // best distance so the outer joins can't multiply result rows.
            //
            // The KNN MATCH lives ALONE in an inner subquery whose LIMIT
            // blocks flattening. With MATCH+k in the same SELECT as the
            // model-table join and pre-filters, the planner is free to make
            // the model table the outer loop (it will, when an arm's table
            // is small) — and vec0's KNN driven as the inner side of a join
            // returns garbage or nothing. Observed live: attached spokes
            // vanished from filtered recalls while an unfiltered query
            // returned them (plus duplicate rows with borrowed distances).
            sql << cte_name << " AS (SELECT gid, MIN(distance) AS distance FROM (";
            for (size_t si = 0; si < vec_schemas.size(); ++si) {
                if (si > 0) sql << " UNION ALL ";
                sql << "SELECT m.globalId AS gid, "
                    << (needs_rescore
                        ? distance_func + "(k.embedding, " + query_blob + ")"
                        : "k.distance")
                    << " AS distance"
                    << " FROM (SELECT global_id, embedding, distance"
                    << " FROM " << vec_schemas[si] << "." << vec_table
                    << " WHERE embedding MATCH " << query_blob
                    << " ORDER BY distance LIMIT " << fetch_k << ") k"
                    << " JOIN " << vec_schemas[si] << "." << table_name
                    << " m ON m.globalId = k.global_id";
                if (!where_conditions.empty()) {
                    sql << " WHERE ";
                    for (size_t wi = 0; wi < where_conditions.size(); ++wi) {
                        if (wi > 0) sql << " AND ";
                        sql << where_conditions[wi];
                    }
                }
            }
            sql << ") GROUP BY gid ORDER BY distance ASC";
            // Truncate the oversampled, re-scored candidate set back to the
            // requested k so the constraint keeps its top-k semantics.
            if (needs_rescore) sql << " LIMIT " << vc.k;
            sql << "), ";
        }

        // ====================================================================
        // Step 4b: FTS5 full-text search constraints
        // ====================================================================
        std::vector<std::string> fts_cte_names;
        for (const auto& tc : texts) {
            std::string fts_table = "_" + table_name + "_" + tc.column + "_fts";

            // Collect schemas that have this FTS table
            std::vector<std::string> fts_schemas;
            if (db().table_exists(fts_table)) {
                fts_schemas.push_back("main");
            }
            for (const auto& alias : attached_aliases_) {
                std::string check_sql = "SELECT name FROM \"" + alias + "\".sqlite_master "
                                        "WHERE type='table' AND name=?";
                auto check = db().query(check_sql, {fts_table});
                if (!check.empty()) {
                    fts_schemas.push_back("\"" + alias + "\"");
                }
            }
            // If no schema has the FTS table, skip this constraint
            if (fts_schemas.empty()) continue;

            std::string cte_name = "fts_" + std::to_string(cte_index++);
            fts_cte_names.push_back(cte_name);

            // Escape single quotes in search text
            std::string escaped_text = tc.search_text;
            size_t pos = 0;
            while ((pos = escaped_text.find('\'', pos)) != std::string::npos) {
                escaped_text.replace(pos, 1, "''");
                pos += 2;
            }

            // Pre-filters expressed against the JOINED model row (m) so they
            // are arm-correct: fts.rowid is only meaningful within its own
            // arm, so each arm's FTS hits are translated to globalId by
            // joining THAT arm's model table before any cross-arm set logic.
            std::string fts_where_suffix;
            if (!cte_names.empty()) {
                fts_where_suffix += " AND m.globalId IN (SELECT gid FROM " + candidates_cte + ")";
            }
            if (where_clause.has_value() && !where_clause.value().empty()) {
                fts_where_suffix += " AND (" + where_clause.value() + ")";
            }

            // Build CTE: UNION ALL across all schemas' FTS tables, collapsed
            // to one row per memory (best rank), then ORDER + LIMIT.
            sql << cte_name << " AS (SELECT gid, MIN(distance) AS distance FROM (";
            for (size_t si = 0; si < fts_schemas.size(); ++si) {
                if (si > 0) sql << " UNION ALL ";
                std::string qualified_fts = fts_schemas[si] + "." + fts_table;
                sql << "SELECT m.globalId AS gid, fts.rank AS distance"
                    << " FROM " << qualified_fts << " fts"
                    << " JOIN " << fts_schemas[si] << "." << table_name
                    << " m ON m.id = fts.rowid"
                    << " WHERE " << fts_table << " MATCH '" << escaped_text << "'"
                    << fts_where_suffix;
            }
            sql << ") GROUP BY gid ORDER BY distance LIMIT " << tc.limit
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
                sql << "SELECT gid FROM " << all_candidate_ctes[i];
            }
            // Also intersect with spatial candidates if present
            if (!cte_names.empty()) {
                sql << " INTERSECT SELECT gid FROM " << candidates_cte;
            }
        } else if (!cte_names.empty()) {
            // No vector/FTS constraints, just use spatial candidates
            sql << "SELECT gid FROM " << candidates_cte;
        } else {
            // No constraints at all - select all rows (globalId currency)
            sql << "SELECT globalId AS gid FROM " << table_name;
        }
        sql << ") ";

        LOG_DEBUG("build_ctes", "CTE SQL length: %zu", sql.str().size());
        return CombinedQueryCtes{sql.str(), geo_cte_names, vec_cte_names, fts_cte_names};
    }

    CombinedQueryResultVector combined_nearest_query(
        const std::string& table_name,
        const BoundsConstraintVector& bounds,
        const VectorConstraintVector& vectors,
        const GeoConstraintVector& geos,
        const TextConstraintVector& texts,
        OptionalString where_clause,
        const sort_descriptor& sort,
        int64_t limit,
        OptionalString group_by = std::nullopt,
        OptionalString distinct_by = std::nullopt)
        SWIFT_NAME(combinedNearestQuery(table:bounds:vectors:geos:texts:where:sort:limit:groupBy:distinctBy:)) {
        // C++ exceptions must NOT propagate across the Swift/C++ boundary —
        // that is std::terminate() → SIGTRAP (observed live: db_error under
        // system memory pressure escaped here and killed the app). Empty on
        // failure; callers already treat empty as "no results".
        clear_bridge_outcome();
        try {
        auto ctes_opt = build_combined_nearest_ctes_(table_name, bounds, vectors, geos, texts, where_clause);
        if (!ctes_opt) return CombinedQueryResultVector{};
        auto& ctes = *ctes_opt;

        bool has_distinct = distinct_by.has_value() && !distinct_by.value().empty();
        bool has_group = group_by.has_value() && !group_by.value().empty();

        // No constraints — simple SELECT
        if (ctes.sql.empty()) {
            std::string q = "SELECT * FROM " + table_name +
                (where_clause.has_value() && !where_clause.value().empty()
                    ? " WHERE " + where_clause.value() : "");
            if (has_distinct) q += " GROUP BY " + distinct_by.value();
            q += " LIMIT " + std::to_string(limit);
            auto rows = db().query(q);

            CombinedQueryResultVector results;
            results.reserve(rows.size());
            const RuntimeSwiftSchema* props = get_properties_for_table(table_name);
            for (const auto& row : rows) {
                auto obj = hydrate<swift_dynamic_object>(row, table_name);
                if (props) { obj.properties_ = copy_runtime_properties_(*props); obj.source.properties = copy_runtime_properties_(*props); }
                results.emplace_back(std::move(obj), DistanceEntryVector{});
            }
            return results;
        }

        // Build final SELECT with distance columns and JOINs
        std::ostringstream sql;
        sql << ctes.sql;

        sql << "SELECT " << table_name << ".*";
        for (size_t i = 0; i < geos.size(); ++i)
            sql << ", g" << i << ".distance AS _dist_" << geos[i].column;
        for (size_t i = 0; i < vectors.size(); ++i)
            sql << ", v" << i << ".distance AS _dist_" << vectors[i].column;
        for (size_t i = 0; i < texts.size(); ++i)
            sql << ", f" << i << ".distance AS _dist_" << texts[i].column;

        sql << " FROM " << table_name
            << " JOIN final_candidates fc ON " << table_name << ".globalId = fc.gid";

        for (size_t i = 0; i < ctes.geo_ctes.size(); ++i)
            sql << " LEFT JOIN " << ctes.geo_ctes[i] << " g" << i
                << " ON " << table_name << ".globalId = g" << i << ".gid";
        for (size_t i = 0; i < ctes.vec_ctes.size(); ++i)
            sql << " LEFT JOIN " << ctes.vec_ctes[i] << " v" << i
                << " ON " << table_name << ".globalId = v" << i << ".gid";
        for (size_t i = 0; i < ctes.fts_ctes.size(); ++i)
            sql << " LEFT JOIN " << ctes.fts_ctes[i] << " f" << i
                << " ON " << table_name << ".globalId = f" << i << ".gid";

        if (where_clause.has_value() && !where_clause.value().empty())
            sql << " WHERE " << where_clause.value();

        if (has_distinct && has_group) {
            sql << " GROUP BY " << distinct_by.value();
        } else if (has_distinct) {
            sql << " GROUP BY " << distinct_by.value();
        } else if (has_group) {
            sql << " GROUP BY " << group_by.value();
        }

        bool emitted_order_by = false;
        if (sort.type != sort_descriptor::Type::none && !sort.column.empty()) {
            std::string order_col;
            switch (sort.type) {
                case sort_descriptor::Type::geo_distance:
                case sort_descriptor::Type::vector_distance:
                case sort_descriptor::Type::text_rank:
                    order_col = "_dist_" + sort.column; break;
                case sort_descriptor::Type::property:
                    order_col = table_name + "." + sort.column; break;
                default: break;
            }
            if (!order_col.empty()) {
                sql << " ORDER BY " << order_col << (sort.ascending ? " ASC" : " DESC");
                emitted_order_by = true;
            }
        }
        // SQL does not guarantee that a subquery's ORDER BY survives to the
        // outer SELECT, so the vec CTE's ordering by (possibly re-scored)
        // distance is lost here — without an explicit sort, rows would come
        // back in vec0's L2 MATCH candidate order, which is wrong for any
        // non-L2 metric. Default to ordering by vector distance ascending.
        if (!emitted_order_by && !vectors.empty()) {
            sql << " ORDER BY ";
            for (size_t i = 0; i < vectors.size(); ++i) {
                if (i > 0) sql << ", ";
                sql << "_dist_" << vectors[i].column << " ASC";
            }
        }

        sql << " LIMIT " << limit;

        std::string final_sql;
        if (has_distinct && has_group) {
            final_sql = "SELECT * FROM (" + sql.str() + ") GROUP BY " + group_by.value();
        } else {
            final_sql = sql.str();
        }

        LOG_DEBUG("combinedNearestQuery", "Final SQL length: %zu", final_sql.size());
        if (std::getenv("LATTICE_DUMP_SQL")) {
            fprintf(stderr, "[LATTICE_DUMP_SQL]\n%s\n[/LATTICE_DUMP_SQL]\n", final_sql.c_str());
        }
        auto rows = db().query(final_sql);
        LOG_DEBUG("combinedNearestQuery", "Query returned %zu rows", rows.size());
        CombinedQueryResultVector results;
        results.reserve(rows.size());

        const RuntimeSwiftSchema* props = get_properties_for_table(table_name);

        for (const auto& row : rows) {
            DistanceEntryVector distances;

            for (const auto& gc : geos) {
                std::string dist_col = "_dist_" + gc.column;
                auto it = row.find(dist_col);
                if (it != row.end() && std::holds_alternative<double>(it->second)) {
                    distances.emplace_back(gc.column, std::get<double>(it->second));
                }
            }

            for (const auto& vc : vectors) {
                std::string dist_col = "_dist_" + vc.column;
                auto it = row.find(dist_col);
                if (it != row.end() && std::holds_alternative<double>(it->second)) {
                    distances.emplace_back(vc.column, std::get<double>(it->second));
                }
            }

            for (const auto& tc : texts) {
                std::string dist_col = "_dist_" + tc.column;
                auto it = row.find(dist_col);
                if (it != row.end() && std::holds_alternative<double>(it->second)) {
                    distances.emplace_back(tc.column, std::get<double>(it->second));
                }
            }

            auto obj = hydrate<swift_dynamic_object>(row, table_name);
            if (props) {
                obj.properties_ = copy_runtime_properties_(*props);
                obj.source.properties = copy_runtime_properties_(*props);
            }
            results.emplace_back(std::move(obj), std::move(distances));
        }

        return results;
        } catch (const std::exception& e) {
            record_bridge_error(e.what());
            LOG_ERROR("query", "combined_nearest_query failed: %s", e.what());
            return CombinedQueryResultVector{};
        }
    }

    /// Count-only variant of combined_nearest_query.
    /// Lean final SELECT — no distance JOINs, no ORDER BY, no SELECT *.
    int64_t combined_nearest_query_count(
        const std::string& table_name,
        const BoundsConstraintVector& bounds,
        const VectorConstraintVector& vectors,
        const GeoConstraintVector& geos,
        const TextConstraintVector& texts,
        OptionalString where_clause,
        const sort_descriptor& sort,
        int64_t limit,
        OptionalString group_by = std::nullopt,
        OptionalString distinct_by = std::nullopt)
        SWIFT_NAME(combinedNearestQueryCount(table:bounds:vectors:geos:texts:where:sort:limit:groupBy:distinctBy:)) {
        // Never throws into Swift — 0 on failure.
        clear_bridge_outcome();
        try {
        auto ctes_opt = build_combined_nearest_ctes_(table_name, bounds, vectors, geos, texts, where_clause);
        if (!ctes_opt) return 0;
        auto& ctes = *ctes_opt;

        bool has_distinct = distinct_by.has_value() && !distinct_by.value().empty();
        bool has_group = group_by.has_value() && !group_by.value().empty();

        // No constraints — simple count
        if (ctes.sql.empty()) {
            std::string q = "SELECT COUNT(*) FROM " + table_name +
                (where_clause.has_value() && !where_clause.value().empty()
                    ? " WHERE " + where_clause.value() : "");
            if (has_distinct) q += " GROUP BY " + distinct_by.value();
            q += " LIMIT " + std::to_string(limit);
            // GROUP BY makes COUNT per-group; wrap to get total
            if (has_distinct) q = "SELECT COUNT(*) FROM (" + q + ")";
            auto rows = db().query(q);
            if (!rows.empty()) {
                for (const auto& [key, val] : rows[0]) {
                    if (std::holds_alternative<int64_t>(val))
                        return std::get<int64_t>(val);
                }
            }
            return 0;
        }

        // Lean query: only id, no distance JOINs, no ORDER BY
        std::ostringstream sql;
        sql << ctes.sql;
        sql << "SELECT " << table_name << ".id"
            << " FROM " << table_name
            << " JOIN final_candidates fc ON " << table_name << ".globalId = fc.gid";

        if (where_clause.has_value() && !where_clause.value().empty())
            sql << " WHERE " << where_clause.value();

        if (has_distinct && has_group) {
            sql << " GROUP BY " << distinct_by.value();
        } else if (has_distinct) {
            sql << " GROUP BY " << distinct_by.value();
        } else if (has_group) {
            sql << " GROUP BY " << group_by.value();
        }

        std::string inner = sql.str();
        if (has_distinct && has_group)
            inner = "SELECT * FROM (" + inner + ") GROUP BY " + group_by.value();

        std::string count_sql = "SELECT COUNT(*) FROM (" + inner + ")";
        auto rows = db().query(count_sql);
        if (!rows.empty()) {
            for (const auto& [key, val] : rows[0]) {
                if (std::holds_alternative<int64_t>(val))
                    return std::get<int64_t>(val);
            }
        }
        return 0;
        } catch (const std::exception& e) {
            record_bridge_error(e.what());
            LOG_ERROR("query", "combined_nearest_query_count failed: %s", e.what());
            return 0;
        }
    }

    // ========================================================================
    // Sync-server helpers
    // ========================================================================
    AuditLogEntryVector events_after(const OptionalString& checkpoint_global_id) {
        return ::lattice::events_after_provisional(this->db(), checkpoint_global_id);
    }
    
    /// Receive sync data from a client (server-side)
    /// data: JSON bytes containing ServerSentEvent
    /// Returns list of acknowledged global IDs (only successfully applied entries)
    /// Sets last_receive_error_ on failure (check via last_receive_error())
    std::vector<std::string> receive_sync_data(const ByteVector& data) {
        // C++ exceptions must NOT propagate across the Swift/C++ boundary — that
        // causes std::terminate() → SIGTRAP. Catch here and surface via error accessor.
        last_receive_error_.reset();
        try {
            std::string json_str(data.begin(), data.end());
            auto event = server_sent_event::from_json(json_str);
            if (!event) {
                return {};  // Parse failed
            }
            std::vector<std::string> result;

            if (event->event_type == server_sent_event::type::audit_log) {
                // Apply remote changes — returns only successfully applied IDs
                result = ::lattice::apply_remote_changes(*this, event->audit_logs);
            } else if (event->event_type == server_sent_event::type::ack) {
                // Mark entries as synchronized (includes observer notification).
                // Return NOTHING: the caller acks whatever we return, and
                // echoing acked_ids here made every relay re-ack each client
                // download-ack — an infinite ping-pong of empty bookkeeping
                // (B3.8). An ack frame applies no entries, so there is
                // nothing to acknowledge.
                ::lattice::mark_audit_entries_synced(*this, event->acked_ids);
            }

            return result;
        } catch (const std::exception& e) {
            last_receive_error_ = std::string(e.what());
            LOG_ERROR("receive_sync_data", "Exception: %s", e.what());
            return {};
        } catch (...) {
            last_receive_error_ = std::string("unknown C++ exception in receive_sync_data");
            LOG_ERROR("receive_sync_data", "Unknown exception");
            return {};
        }
    }

    /// Returns the error from the last receive_sync_data call, or nullopt if none.
    OptionalString last_receive_error() const { return last_receive_error_; }

private:
    struct owned_resources {
        detail::swift_runtime_map<RuntimeSwiftSchema> schemas_;
        detail::swift_runtime_map<RuntimeConstraintVector> constraints_;
        detail::swift_runtime_map<detail::swift_runtime_union_descriptor> union_schemas_;
        swift_configuration swift_config_;
        OptionalString last_receive_error_;
        OptionalString last_attach_error_;
        mutable std::mutex attach_error_mutex_;
        std::future<void> vec0_training_future_;
        std::future<void> vec0_reconcile_future_;

        explicit owned_resources(const swift_configuration& config):swift_config_(config) {}
        explicit owned_resources(swift_configuration&& config):swift_config_(std::move(config)) {}
    };
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
    /// Create a migration context ref wrapping an existing context.
    /// All mutable state (the wrapped context pointer and the queued row
    /// updates) lives behind one shared_ptr, so — like every other *_ref
    /// handle — value-path copies alias the same state instead of forking it,
    /// and the methods below can be const (importing non-mutating into Swift).
    explicit swift_migration_context_ref(migration_context* ctx)
        : state_(std::make_shared<state>(ctx)) {}

    /// Get all pending schema changes
    std::vector<swift_table_changes> pending_changes() const SWIFT_NAME(pendingChanges()) {
        std::vector<swift_table_changes> result;
        for (const auto& tc : state_->ctx->pending_changes()) {
            result.emplace_back(tc);
        }
        return result;
    }

    /// Check if a specific table has pending changes
    bool has_changes_for(const std::string& table_name) const SWIFT_NAME(hasChanges(for:)) {
        return state_->ctx->has_changes_for(table_name);
    }

    /// Get pending changes for a specific table (returns empty if none)
    swift_table_changes changes_for(const std::string& table_name) const SWIFT_NAME(changes(for:)) {
        auto* tc = state_->ctx->changes_for(table_name);
        return tc ? swift_table_changes(*tc) : swift_table_changes();
    }
#ifdef __BLOCKS__
    /// Enumerate all objects in a table for migration.
    /// Swift callback receives each row as a dictionary for transformation.
    /// Use get_row_value/set_row_value for type-safe access.
    void enumerate_objects(
        const std::string& table_name,
        void(^callback)(int64_t row_id, const std::unordered_map<std::string, column_value_t>& old_row)
    ) const SWIFT_NAME(enumerateObjects(table:callback:)) {
        state_->ctx->enumerate_objects(table_name, [&](const migration_row& old_row, migration_row& new_row) {
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
    ) const SWIFT_NAME(enumerateObjects(table:callback:)) {
        state_->ctx->enumerate_objects(table_name, [&](const migration_row& old_row, migration_row& new_row) {
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
                       const std::string& column_name, const column_value_t& value) const
        SWIFT_NAME(setRowValue(table:rowId:column:value:)) {
        // Queue the update for application after migration
        state_->pending_row_updates[table_name][row_id][column_name] = value;
    }

    /// Rename a property (copies old column value to new column name)
    void rename_property(const std::string& table_name,
                         const std::string& old_name,
                         const std::string& new_name) const SWIFT_NAME(renameProperty(table:from:to:)) {
        state_->ctx->rename_property(table_name, old_name, new_name);
    }

    /// Delete all objects in a table
    void delete_all(const std::string& table_name) const SWIFT_NAME(deleteAll(table:)) {
        state_->ctx->delete_all(table_name);
    }

    /// Execute raw SQL for complex migrations
    void execute_sql(const std::string& sql) const SWIFT_NAME(executeSQL(_:)) {
        state_->ctx->execute_sql(sql);
    }

    /// Query raw SQL for reading data
    std::vector<std::unordered_map<std::string, column_value_t>> query_sql(const std::string& sql) const
        SWIFT_NAME(querySQL(_:)) {
        return state_->ctx->query_sql(sql);
    }

#if LATTICE_HAS_FRT
    // For SWIFT_SHARED_REFERENCE
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }
#endif

    // Apply pending row updates to the context
    // Called internally before migration context goes out of scope
    void apply_row_updates() const {
        for (const auto& [table_name, rows] : state_->pending_row_updates) {
            state_->ctx->enumerate_objects(table_name, [&](const migration_row& old_row, migration_row& new_row) {
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
    // Shared state: the wrapped context + queued row updates
    // (table_name -> row_id -> column_name -> value).
    struct state {
        explicit state(migration_context* c) : ctx(c) {}
        migration_context* ctx;
        std::unordered_map<std::string, std::unordered_map<int64_t, std::unordered_map<std::string, column_value_t>>> pending_row_updates;
    };
    std::shared_ptr<state> state_;
#if LATTICE_HAS_FRT
    std::atomic<int> ref_count_{0};
#endif
#if LATTICE_HAS_FRT
} SWIFT_SHARED_REFERENCE(retainSwiftMigrationContextRef, releaseSwiftMigrationContextRef);
#else
};
#endif

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
    /// Deterministic fingerprint of a config's IPC targets (channel, socket
    /// path, filter tables + WHERE clauses). Without this in the cache key, an
    /// open WITHOUT ipc targets (e.g. a maintenance/compact connection) would
    /// alias an existing instance WITH a live synchronizer — and vice versa —
    /// silently dropping the requested config and accumulating sync progress
    /// across logically separate sessions.
    /// Stable fingerprint of the sync-tuning overlay for the instance-cache key —
/// two opens of the same path with DIFFERENT tuning must not share an
/// instance (the first opener's synchronizer would silently win).
inline std::string sync_tuning_fingerprint(const lattice::configuration& c) {
    const auto& t = c.tuning;
    std::string fp;
    fp += t.chunk_size ? std::to_string(*t.chunk_size) : "-"; fp += "|";
    fp += t.max_reconnect_attempts ? std::to_string(*t.max_reconnect_attempts) : "-"; fp += "|";
    fp += t.base_delay_seconds ? std::to_string(*t.base_delay_seconds) : "-"; fp += "|";
    fp += t.max_delay_seconds ? std::to_string(*t.max_delay_seconds) : "-"; fp += "|";
    fp += t.stable_connection_ms ? std::to_string(*t.stable_connection_ms) : "-"; fp += "|";
    fp += t.upload_coalesce_ms ? std::to_string(*t.upload_coalesce_ms) : "-"; fp += "|";
    fp += t.checkpoint_passive_interval_ms ? std::to_string(*t.checkpoint_passive_interval_ms) : "-"; fp += "|";
    fp += t.checkpoint_truncate_interval_ms ? std::to_string(*t.checkpoint_truncate_interval_ms) : "-"; fp += "|";
    fp += t.use_upload_floor ? (*t.use_upload_floor ? "1" : "0") : "-";
    return fp;
}

template <typename ConfigT>
    inline std::string ipc_targets_fingerprint(const ConfigT& config) {
        std::string fp;
        for (const auto& t : config.ipc_targets) {
            fp += t.channel;
            if (t.socket_path) { fp += '@'; fp += *t.socket_path; }
            if (t.sync_filter) {
                fp += '[';
                for (const auto& e : *t.sync_filter) {
                    fp += e.table_name;
                    if (e.where_clause) { fp += ':'; fp += *e.where_clause; }
                    fp += ',';
                }
                fp += ']';
            }
            fp += ';';
        }
        return fp;
    }

    struct LatticeRefCacheKey {
        std::string path;
        std::shared_ptr<scheduler> sched;
        std::string websocket_url;  // Include sync config in cache key
        std::string schema_hash;    // Hash of table names + property types
        int32_t schema_version;     // Target schema version (differentiates pre/post migration)
        std::string ipc_fingerprint; // Channels + socket paths + sync filter (see ipc_targets_fingerprint)
        std::string tuning_fingerprint; // sync_tuning overlay (different tuning must not share an instance)
        std::string recovery_expectation; // never reuse a cached owner for changed source policy
        detail::ordinary_context managed_context;
        std::shared_ptr<const detail::ordinary_managed_child_admission> managed_admission;
        // Append policy selectors after actual managed custody; neither selector issues a grant.
        ordinary_sync_policy ordinary_policy=ordinary_sync_policy::legacy_provisional;
        std::string ordinary_model_profile;
        bool read_only = false; // Actual native access mode is part of owner identity.
        std::optional<sqlite3_int64> wal_extent_limit_bytes;

        bool operator<(const LatticeRefCacheKey& other) const {
            if(managed_admission!=other.managed_admission)return managed_admission.owner_before(other.managed_admission);
            if(!managed_admission&&managed_context!=other.managed_context)return managed_context.owner_before(other.managed_context);
            if (path != other.path) return path < other.path;
            if (websocket_url != other.websocket_url) return websocket_url < other.websocket_url;
            if (schema_hash != other.schema_hash) return schema_hash < other.schema_hash;
            if (schema_version != other.schema_version) return schema_version < other.schema_version;
            if (ipc_fingerprint != other.ipc_fingerprint) return ipc_fingerprint < other.ipc_fingerprint;
            if (tuning_fingerprint != other.tuning_fingerprint) return tuning_fingerprint < other.tuning_fingerprint;
            if (recovery_expectation != other.recovery_expectation) return recovery_expectation < other.recovery_expectation;
            if (ordinary_policy != other.ordinary_policy) return ordinary_policy < other.ordinary_policy;
            if (ordinary_model_profile != other.ordinary_model_profile) return ordinary_model_profile < other.ordinary_model_profile;
            if (read_only != other.read_only) return read_only < other.read_only;
            if (wal_extent_limit_bytes != other.wal_extent_limit_bytes) return wal_extent_limit_bytes < other.wal_extent_limit_bytes;
            // Compare schedulers: both null, or use is_same_as
            if (!sched && !other.sched) return false;
            if (!sched) return true;  // null < non-null
            if (!other.sched) return false;  // non-null > null
            // Use pointer comparison for ordering (is_same_as is for equality)
            return sched.get() < other.sched.get();
        }

        bool operator==(const LatticeRefCacheKey& other) const {
            if(managed_admission!=other.managed_admission)return false;
            if(!managed_admission&&managed_context!=other.managed_context)return false;
            if (path != other.path) return false;
            if (websocket_url != other.websocket_url) return false;
            if (schema_hash != other.schema_hash) return false;
            if (schema_version != other.schema_version) return false;
            if (ipc_fingerprint != other.ipc_fingerprint) return false;
            if (tuning_fingerprint != other.tuning_fingerprint) return false;
            if (recovery_expectation != other.recovery_expectation) return false;
            if (ordinary_policy != other.ordinary_policy) return false;
            if (ordinary_model_profile != other.ordinary_model_profile) return false;
            if (read_only != other.read_only) return false;
            if (wal_extent_limit_bytes != other.wal_extent_limit_bytes) return false;
            if (!sched && !other.sched) return true;
            if (!sched || !other.sched) return false;
            return sched->is_same_as(other.sched.get());
        }
    };

    // Cache for swift_lattice instances - provides both key-based and pointer-based lookups
    class LatticeCache {
        struct owned_resources;
        process_bound_resource_owner<owned_resources> cache_resources_;
        void require_creator_process() const {
            if (!cache_resources_.in_creator_process()) throw std::runtime_error("inherited LatticeCache belongs to another process");
        }
    public:
        static LatticeCache& instance();

        // Get or create a swift_lattice by configuration key
        // Template to preserve swift_configuration type for constructor overload resolution
        template<typename ConfigT>
        std::shared_ptr<swift_lattice> get_or_create(const ConfigT& requested, const SchemaVector& schemas) {
            require_creator_process();
            detail::ordinary_before_open(requested.path,requested.ordinary_context);
            if(requested.ordinary_context)throw std::runtime_error("managed opens require their receiver-issued cache entry");
            if(requested.wal_extent_limit_bytes&&(*requested.wal_extent_limit_bytes<=0||requested.read_only||requested.is_in_memory()))
                throw std::runtime_error("bounded WAL cache open requires positive writable file policy");
            // Own the actual entry through cache hit/build publication. No root
            // IO or final admission destruction runs beneath the cache leaf.
            const auto resource_end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
            detail::store_resource_factory_admission resource_admission(requested.path,!requested.read_only,resource_end);
            ConfigT config=requested;
            config.path=resource_admission.actual_path();
            // A cache hit must not bypass the selected native opening policy.
            LOG_DEBUG("LatticeCache", "get_or_create() path=%s", config.path.c_str());

            // Build schema hash from table names and properties (sorted for
            // consistency). Pure computation over `schemas` — done before locking
            // so the (potentially long) hash never holds mutex_.
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

            // Skip cache for in-memory databases — each ":memory:" open creates a
            // distinct SQLite database, so reusing a cached instance would incorrectly
            // share state between callers that expect isolated storage.
            // Migration is NOT a reason to skip cache: it's idempotent (checks
            // current_version vs target_version and skips if already applied).
            bool skip_cache = config.path == ":memory:" || config.path.empty();

            LatticeRefCacheKey key{config.path, config.sched, config.websocket_url, schema_hash, config.target_schema_version, ipc_targets_fingerprint(config), sync_tuning_fingerprint(config), config.recovery_source_expectation, {}, {}, config.ordinary_policy, config.ordinary_model_profile, config.read_only,config.wal_extent_limit_bytes};

            // ---- :memory: path -------------------------------------------------
            // Constructs under the lock (unchanged). These opens are rare and fast,
            // each must yield a distinct instance (no de-dup), and the under-lock
            // construct relies on the recursive mutex for its commit-hook re-entry
            // into get_by_pointer (see member comment).
            if (skip_cache) {
                std::lock_guard<std::recursive_mutex> lock(mutex_);
                // Remove any stale key entry so a fresh instance is created; preserve
                // ptr_cache_ — an old instance may still be alive and its managed
                // objects need get_by_pointer() to resolve the lattice ref.
                for (auto it = key_cache_.begin(); it != key_cache_.end(); ++it) {
                    if (it->first == key) { key_cache_.erase(it); break; }
                }
                LOG_DEBUG("LatticeCache", "Creating new in-memory swift_lattice for path=%s", config.path.c_str());
                auto inst = std::make_shared<swift_lattice>(config, schemas);
                LOG_DEBUG("LatticeCache", "swift_lattice created, ptr=%p", inst.get());
                key_cache_.emplace_back(key, inst);
                ptr_cache_[inst.get()] = inst;
                return inst;
            }

            // ---- on-disk path --------------------------------------------------
            // Construct OUTSIDE the lock so an open/migration on one thread never
            // blocks get_by_pointer (per-object hot path) or unrelated opens on
            // other threads. Concurrent opens of the SAME key de-dup via in_flight_:
            // the first caller publishes a shared_future and builds; others wait on
            // it instead of opening a second connection to the same file.
            std::promise<std::shared_ptr<swift_lattice>> my_promise;
            std::shared_future<std::shared_ptr<swift_lattice>> wait_future;
            {
                std::lock_guard<std::recursive_mutex> lock(mutex_);

                // 1. Live cached instance?
                for (auto it = key_cache_.begin(); it != key_cache_.end(); ++it) {
                    if (it->first == key) {
                        if (auto existing = it->second.lock()) {
                            resource_admission.require_no_unclaimed_creation_();
                            return existing;
                        }
                        key_cache_.erase(it);  // expired
                        break;
                    }
                }
                // 2. Construction already in flight for this key? Wait on its result.
                for (auto& entry : in_flight_) {
                    if (entry.first == key) {
                        resource_admission.require_no_unclaimed_creation_();
                        wait_future = entry.second;
                        break;
                    }
                }
                // 3. Otherwise claim construction of this key for ourselves.
                if (!wait_future.valid()) {
                    in_flight_.emplace_back(key, my_promise.get_future().share());
                }
            }

            // Another thread is building this exact key — wait without holding the
            // lock. Note: if that builder failed, get() rethrows its exception, so
            // concurrent waiters share the failure (later arrivals retry fresh).
            if (wait_future.valid()) {
                return wait_future.get();
            }

            // We own construction. Build outside the lock.
            LOG_DEBUG("LatticeCache", "Creating new swift_lattice for path=%s", config.path.c_str());
            std::shared_ptr<swift_lattice> inst;
            try {
                auto original=resource_admission.take_for_builder_();
                inst=std::make_shared<swift_lattice>(config,schemas,swift_lattice::normal_factory_key(&original));
            } catch (...) {
                // Erase the in-flight marker FIRST so it can never linger as a
                // poison entry, then propagate the failure to any waiters.
                std::lock_guard<std::recursive_mutex> lock(mutex_);
                erase_in_flight(key);
                my_promise.set_exception(std::current_exception());
                throw;
            }
            LOG_DEBUG("LatticeCache", "swift_lattice created, ptr=%p", inst.get());

            // Publish + fulfill under the lock. Erase the in-flight marker first
            // (guarantees no poison/leak regardless of the inserts below); insert
            // into key_cache_ + ptr_cache_ together so a late waiter never observes
            // "no live instance AND no in-flight future".
            {
                std::lock_guard<std::recursive_mutex> lock(mutex_);
                erase_in_flight(key);
                key_cache_.emplace_back(key, inst);
                ptr_cache_[inst.get()] = inst;
                my_promise.set_value(inst);
            }
            return inst;
        }

        // Separate receiver-issued local opening path. Normal open custody
        // never becomes history admission; only a real history receiver supplies
        // the stronger owner consumed by post-COMMIT enrollment below.
        std::shared_ptr<swift_lattice> get_or_create_managed(swift_configuration config,
            const SchemaVector& schemas,const swift_ordinary_open_context& startup) {
            if(!startup.context_)throw std::runtime_error("receiver-issued managed open context required");
            detail::ordinary_require_creator(startup.context_);
            require_creator_process();
            if(config.ordinary_context&&config.ordinary_context!=startup.context_)
                throw std::runtime_error("managed configuration differs from its receiver-issued context");
            config.ordinary_context=startup.context_;
            detail::ordinary_before_open(config.path,config.ordinary_context);
            config.wal_extent_limit_bytes=detail::ordinary_select_wal_extent_limit(config.path,
                config.ordinary_context,config.wal_extent_limit_bytes);
            if(config.is_in_memory()||config.read_only||config.is_sync_enabled()||config.is_ipc_enabled()||
               config.audit_retention_seconds!=0||config.migration_block||config.row_migration_fn_||
               !config.migration_schemas_.empty()||config.managed_product_credentials||config.target_schema_version<=0)
                throw std::runtime_error("managed initial open requires a quiescent writable ordinary owner");
            const auto catalog=swift_lattice::recovery_catalog(config,schemas);
            const auto schema_identity=swift_lattice::managed_catalog_identity(catalog);
            LatticeRefCacheKey key{config.path,config.sched,config.websocket_url,schema_identity,
                config.target_schema_version,ipc_targets_fingerprint(config),sync_tuning_fingerprint(config),
                config.recovery_source_expectation,config.ordinary_context,startup.admission_,
                config.ordinary_policy,config.ordinary_model_profile,config.read_only,config.wal_extent_limit_bytes};
            auto validate=[&](const std::shared_ptr<swift_lattice>& owner){
                detail::ordinary_require_creator(config.ordinary_context);
                detail::ordinary_before_open(config.path,config.ordinary_context);
                if(!owner||!owner->owns_creator_process()||owner->is_closed())throw std::runtime_error("managed cached owner retired");
                if(owner->config().path!=config.path||owner->config().wal_extent_limit_bytes!=config.wal_extent_limit_bytes||
                   swift_lattice::managed_catalog_identity(owner->recovery_declarations())!=schema_identity)
                    throw std::runtime_error("managed cached catalog differs from actual declarations");
                owner->validate_managed_ordinary();
                detail::ordinary_before_open(config.path,config.ordinary_context);
            };
            std::shared_ptr<swift_lattice> existing;
            std::size_t source_failure_slot=256;
            std::promise<std::shared_ptr<swift_lattice>> promise;
            std::shared_future<std::shared_ptr<swift_lattice>> waiting;
            {
                std::lock_guard<std::recursive_mutex> lock(mutex_);
                for(auto it=key_cache_.begin();it!=key_cache_.end();++it)if(it->first==key){
                    existing=it->second.lock();if(!existing)key_cache_.erase(it);break;
                }
                if(!existing){
                    for(auto& entry:in_flight_)if(entry.first==key){waiting=entry.second;break;}
                    if(!waiting.valid()){
                        if(startup.admission_){
                            auto& resources=cache_resources_.get();
                            if(resources.failed_source_attempts>=resources.failed_sources.size())
                                throw std::runtime_error("managed source opening ledger exhausted");
                            source_failure_slot=resources.failed_source_attempts++;
                        }
                        in_flight_.emplace_back(key,promise.get_future().share());
                    }
                }
            }
            // No journal, physical, catalog or owned-writer validation runs in
            // the cache leaf, including after another builder's future settles.
            if(existing){validate(existing);return existing;}
            if(waiting.valid()){auto owner=waiting.get();validate(owner);return owner;}
            std::shared_ptr<swift_lattice> owner;bool claimed_opening=false;
            try{
                detail::ordinary_before_open(config.path,config.ordinary_context);
                if(startup.admission_){swift_lattice::claim_managed_source_open(startup.admission_,config.path);claimed_opening=true;}
                owner=swift_lattice::open_managed_ordinary(config,schemas,config.ordinary_context,startup.admission_);
                if(!owner||!owner->owns_creator_process()||owner->is_closed()||
                   owner->config().ordinary_context!=config.ordinary_context||owner->config().path!=config.path||
                   owner->config().target_schema_version!=config.target_schema_version||
                   owner->config().wal_extent_limit_bytes!=config.wal_extent_limit_bytes||
                   swift_lattice::managed_catalog_identity(owner->recovery_declarations())!=schema_identity)
                    throw std::runtime_error("checked managed opener did not return its exact retained owner");
                if(startup.admission_)detail::ordinary_export_access::enroll_managed_history(*owner,startup.admission_);
                validate(owner);
                owner->publish_managed_ordinary();
                validate(owner);
                {
                    std::lock_guard<std::recursive_mutex> lock(mutex_);
                    bool inserted_key=false,inserted_pointer=false;
                    try {
                        key_cache_.emplace_back(key,owner);inserted_key=true;
                        const auto inserted=ptr_cache_.emplace(owner.get(),owner);
                        if(!inserted.second)throw std::runtime_error("managed cache reverse owner already registered");
                        inserted_pointer=true;
                        // Both maps and this actual builder future become visible
                        // in one cache publication leaf. Later off-leaf validation
                        // remains inside this builder's retained failure boundary.
                        promise.set_value(owner);
                        erase_in_flight(key);
                    }catch(...){
                        if(inserted_pointer)ptr_cache_.erase(owner.get());
                        if(inserted_key)for(auto it=key_cache_.begin();it!=key_cache_.end();++it)
                            if(it->first==key&&it->second.lock().get()==owner.get()){key_cache_.erase(it);break;}
                        throw;
                    }
                }
                // Late current/schema validation is still part of THIS
                // builder's retained outcome, even after future publication.
                validate(owner);
            }catch(...){
                const auto error=std::current_exception();
                if(claimed_opening){
                    remember_failed_source(source_failure_slot,owner,error);
                    swift_lattice::remember_managed_open_failure(startup.admission_,config.path,error);
                }
                {std::lock_guard<std::recursive_mutex> lock(mutex_);erase_in_flight(key);}
                try{promise.set_exception(error);}catch(...){}
                std::rethrow_exception(error);
            }
            return owner;
        }

        // Only the unique winning builder writes its charged slot. Retain its
        // SAME owner and first error before propagation; no diagnostic, mutex,
        // native close or arbitrary target destruction occurs in this leaf.
        void remember_failed_source(std::size_t slot,const std::shared_ptr<swift_lattice>& owner,
            std::exception_ptr first) noexcept {
            if(!cache_resources_.in_creator_process())return;
            auto& resources=cache_resources_.get();
            if(slot>=resources.failed_sources.size())return;
            auto& retained=resources.failed_sources[slot];
            if(!retained.first_error){retained.owner=owner;retained.first_error=std::move(first);}
        }

        // Fixed registry for named dormant preparations, including failed or
        // unknown opens. Admission is charged before holder allocation. There
        // is no refund/reset or second physical-opening attempt in this unit.
        std::size_t reserve_group_receiver_preparation() {
            require_creator_process();std::lock_guard<std::recursive_mutex> lock(mutex_);
            auto& resources=cache_resources_.get();
            if(resources.group_receiver_attempts>=resources.group_receivers.size())
                throw std::runtime_error("dormant GROUP preparation registry exhausted");
            return resources.group_receiver_attempts++;
        }
        void retain_group_receiver_preparation(std::size_t slot,
            const std::shared_ptr<swift_managed_group_receiver::state>& actual) {
            require_creator_process();std::lock_guard<std::recursive_mutex> lock(mutex_);
            auto& resources=cache_resources_.get();
            if(!actual||slot>=resources.group_receiver_attempts||resources.group_receivers[slot])
                throw std::runtime_error("dormant GROUP original registry slot differs");
            resources.group_receivers[slot]=actual;
        }

        // Look up swift_lattice by raw pointer (reverse lookup from lattice_db*)
        std::shared_ptr<swift_lattice> get_by_pointer(swift_lattice* ptr) {
            require_creator_process();
            std::lock_guard<std::recursive_mutex> lock(mutex_);

            auto it = ptr_cache_.find(ptr);
            if (it != ptr_cache_.end()) {
                if (auto shared = it->second.lock()) {
                    return shared;
                }
                ptr_cache_.erase(it);
            }
            return nullptr;
        }

        /// Construct a NEW instance regardless of any live key-cache entry,
        /// registered in ptr_cache_ only (like dynamic opens). Used for
        /// query-only clone connections (Swift attaching()): key-cache dedup
        /// would return the clone's PARENT when their configs coincide, and
        /// the subsequent ATTACH would mutate the parent's own connection.
        std::shared_ptr<swift_lattice> create_uncached(const swift_configuration& config,
                                                       const SchemaVector& schemas) {
            require_creator_process();
            auto inst = std::make_shared<swift_lattice>(config, schemas);
            register_pointer(inst);
            return inst;
        }

        /// Register a bare-constructed instance (e.g. a dynamic/read-only open
        /// that bypasses get_or_create) into ptr_cache_ only, so its managed
        /// objects can resolve the owning lattice via get_by_pointer /
        /// shared_for_lattice. NOT added to key_cache_: dynamic opens are not
        /// shared or deduped by config key.
        void register_pointer(const std::shared_ptr<swift_lattice>& inst) {
            require_creator_process();
            if (!inst) return;
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            ptr_cache_[inst.get()] = inst;
        }

        /// Remove a swift_lattice from both caches so subsequent get_or_create()
        /// for the same path creates a fresh instance.
        void evict(swift_lattice* ptr) {
            require_creator_process();
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            // Remove from key_cache_ so a subsequent open on the same path/schema
            // creates a fresh instance (match by shared_ptr identity).
            for (auto it = key_cache_.begin(); it != key_cache_.end(); ++it) {
                if (auto sp = it->second.lock()) {
                    if (sp.get() == ptr) {
                        key_cache_.erase(it);
                        break;
                    }
                }
            }
            // Do NOT erase ptr_cache_[ptr]: the instance may still be alive (Swift
            // holds a ref) and a concurrent reader's managed objects resolve the
            // lattice via get_by_pointer(). The weak_ptr expires when the instance
            // is destroyed and is reaped lazily by get_by_pointer (same invariant
            // the migration path in get_or_create relies on).
        }

    private:
        LatticeCache() = default;

        // Remove the in-flight construction marker for `key`, if present.
        // Caller must hold mutex_.
        void erase_in_flight(const LatticeRefCacheKey& key) {
            for (auto it = in_flight_.begin(); it != in_flight_.end(); ++it) {
                if (it->first == key) { in_flight_.erase(it); break; }
            }
        }

        // Recursive: the :memory: path in `get_or_create` still constructs a
        // `swift_lattice` while holding mutex_, and SQLite's commit hook fires
        // synchronously inside that ctor (`ensure_swift_tables` -> COMMIT). The
        // hook walks `instance_registry::for_each_alive` and notifies sibling
        // changeStream observers, whose `dynamic_object` ctors call back into
        // `get_by_pointer`. With a non-recursive mutex that re-entry self-
        // deadlocks. Same-thread re-entry is safe: the sibling being looked up
        // is already published in `ptr_cache_` (that's how `for_each_alive`
        // found it). The on-disk path builds OUTSIDE the lock (see in_flight_),
        // so for that path mutex_ is only held for short map operations.
        std::recursive_mutex& mutex_ = cache_resources_.get().mutex_;
        std::vector<std::pair<LatticeRefCacheKey, std::weak_ptr<swift_lattice>>>& key_cache_ = cache_resources_.get().key_cache_;
        std::unordered_map<swift_lattice*, std::weak_ptr<swift_lattice>>& ptr_cache_ = cache_resources_.get().ptr_cache_;
        // On-disk construction-in-progress markers. A thread building a given key
        // publishes a shared_future here so concurrent callers for the same key
        // wait on the result instead of opening a duplicate connection.
        // Construction itself runs without holding mutex_. Not used for :memory:.
        std::vector<std::pair<LatticeRefCacheKey,
                              std::shared_future<std::shared_ptr<swift_lattice>>>>& in_flight_ = cache_resources_.get().in_flight_;

        struct owned_resources {
            std::recursive_mutex mutex_;
            struct failed_source {
                std::shared_ptr<swift_lattice> owner;
                std::exception_ptr first_error;
            };
            std::size_t failed_source_attempts=0;
            std::array<failed_source,256> failed_sources{};
            std::size_t group_receiver_attempts=0;
            std::array<std::shared_ptr<swift_managed_group_receiver::state>,256> group_receivers{};
            std::vector<std::pair<LatticeRefCacheKey, std::weak_ptr<swift_lattice>>> key_cache_;
            std::unordered_map<swift_lattice*, std::weak_ptr<swift_lattice>> ptr_cache_;
            std::vector<std::pair<LatticeRefCacheKey,
                              std::shared_future<std::shared_ptr<swift_lattice>>>> in_flight_;
        };
    };
} // namespace detail

// Out-of-line: defined after LatticeCache so evict() is visible.
inline void swift_lattice::close() {
    const auto result=close_checked();if(result.error)std::rethrow_exception(result.error);
}
inline lattice_close_result swift_lattice::close_checked() noexcept {
    lattice_close_result result;
    if (!owns_creator_process()) {
        try { throw std::runtime_error("inherited Swift lattice cleanup belongs to another process"); }
        catch (...) { result.remember(std::current_exception()); }
        return result;
    }
    // Refuse before cache eviction as well as before Core retirement. A
    // catalog consumer must not mutate the parent's lookup identity on a
    // close that cannot actually run.
    try { require_catalog_callback_absent(); }
    catch (...) { result.remember(std::current_exception()); return result; }
    try{detail::LatticeCache::instance().evict(this);}catch(...){result.remember(std::current_exception());}
    result.merge(lattice_db::close_checked());return result;
}

/// Owned result fields remain independent of text and every sealed copy.
class owned_catalog_result {
    catalog_stream_result result_;
    explicit owned_catalog_result(catalog_stream_result value) noexcept : result_(std::move(value)) {}
    friend class owned_catalog_operation;
public:
    owned_catalog_result() = default;
    bool success() const noexcept { return result_.success(); }
    bool cleanup_failed() const noexcept SWIFT_NAME(cleanupFailed()) { return result_.cleanup_failed(); }
    int32_t finalize_code() const noexcept SWIFT_NAME(finalizeCode()) { return result_.finalize_code(); }
    int32_t transaction_code() const noexcept SWIFT_NAME(transactionCode()) { return result_.transaction_code(); }
    int32_t rollback_code() const noexcept SWIFT_NAME(rollbackCode()) { return result_.rollback_code(); }
    int32_t stop_code() const noexcept SWIFT_NAME(stopCode()) { return result_.stop_code(); }
    std::string error_message() const SWIFT_NAME(errorMessage()) {
        return sealed([&] { return result_.error_message(); });
    }
};
class owned_catalog_operation {
    catalog_stream_operation operation_;
    explicit owned_catalog_operation(catalog_stream_operation value) : operation_(std::move(value)) {}
    friend class swift_lattice_ref;
public:
    owned_catalog_operation() = default;
    bool valid() const noexcept { return operation_.valid(); }
    void cancel() const noexcept { operation_.cancel(); }
    owned_catalog_result capture(void* context,
        bool (*callback)(void*, int32_t, const uint8_t*, int64_t)) const noexcept SWIFT_NAME(capture(context:callback:)) {
        return owned_catalog_result(operation_.capture(context, callback));
    }
};

class swift_lattice_ref {
    friend class automatic_read_maintenance_fixture_access;
public:
    // FRT path: the factories return a heap `swift_lattice_ref*` foreign
    // reference. Value path (iOS 15): they return a `swift_lattice_ref` by value
    // whose inner shared_ptr<swift_lattice> keeps the db alive. RefRet + _make +
    // the UNRETAINED macro let every factory body stay single-source.
#if LATTICE_HAS_FRT
#  define LATTICE_SLREF_RET swift_lattice_ref*
#  define LATTICE_SLREF_UNRETAINED SWIFT_RETURNS_UNRETAINED
    static swift_lattice_ref* _make(std::shared_ptr<swift_lattice> impl) {
        if (!impl) return nullptr;  // preserve nullptr-for-absent on the FRT path
        auto ref = new swift_lattice_ref();
        ref->impl_ = impl;
        return ref;
    }
#else
#  define LATTICE_SLREF_RET swift_lattice_ref
#  define LATTICE_SLREF_UNRETAINED
    static swift_lattice_ref _make(std::shared_ptr<swift_lattice> impl) {
        swift_lattice_ref ref;
        ref.impl_ = impl;
        return ref;
    }
#endif

    // Empty/default/copy-only configuration cannot issue this authority. The
    // retained receiver handle is checked before the cache singleton or schema.
    static LATTICE_SLREF_RET create_managed(const swift_configuration& config,const SchemaVector& schemas,
        const swift_ordinary_open_context& startup) SWIFT_NAME(createManaged(swiftConfig:schemas:startup:)) LATTICE_SLREF_UNRETAINED {
        return sealed([&]{
            if(!startup.context_)throw std::runtime_error("receiver-issued managed open context required");
            detail::ordinary_require_creator(startup.context_);
            detail::ordinary_before_open(config.path,startup.context_);
            return _make(detail::LatticeCache::instance().get_or_create_managed(config,schemas,startup));
        });
    }
    swift_managed_peer_owner start_managed_engram_peer() const noexcept SWIFT_NAME(startManagedEngramPeer()) {
        try{return swift_managed_peer_owner(detail::ordinary_managed_ipc_access::start_peer_endpoint(
            impl_,std::chrono::steady_clock::now()+std::chrono::seconds(10)));}
        catch(...){return {};}
    }
    swift_managed_product_request begin_managed_engram_route(const swift_lattice_ref& target,
        const swift_managed_product_credentials& credentials,uint8_t direction,const std::string& group) const noexcept
        SWIFT_NAME(beginManagedEngramRoute(target:credentials:direction:group:)) {
        try{return swift_managed_product_request(detail::ordinary_managed_ipc_access::begin(
            impl_,target.impl_,credentials.value_,direction,group));}
        catch(...){return {};}
    }
    // Compatibility for an actual already-open quiescent receiver owner.
    // Ordinary/source opening is NOT a receiver preparation substitute.
    // Preserve under the inherited receive end; never refresh after open.
    swift_owned_receiver_preservation preserve_managed_receiver(const swift_managed_history_startup& startup) const noexcept
        SWIFT_NAME(preserveManagedReceiver(startup:)) {
        return sealed([&]{
            auto owner=impl_;
            if(!owner||!owner->owns_creator_process()||!startup.valid()||
               std::chrono::steady_clock::now()>=startup.receive_end_)
                throw db_error("receiver preservation original startup unavailable");
            detail::ordinary_require_creator(startup.opens_);
            return swift_owned_receiver_preservation(detail::ordinary_export_access::preserve_managed_receiver(
                owner,startup.admission_,startup.receive_end_));
        });
    }
    bool enroll_managed_history(const swift_managed_history_startup& startup) const noexcept {
        return sealed([&]{
            auto owner=impl_;
            if(!owner||!owner->owns_creator_process()||!startup.valid())
                throw db_error("managed enrollment actual startup unavailable");
            detail::ordinary_require_creator(startup.opens_);
            return bool(detail::ordinary_export_access::enroll_managed_history(*owner,startup.admission_));
        });
    }

    // Named storage-only receiver opening. Retain `held` BEFORE interpreting
    // bool/error. It exposes no ordinary model ref and never activates a peer.
    static bool prepare_managed_group_receiver(const swift_configuration&,const SchemaVector&,
        const swift_managed_history_startup&,const swift_ordinary_open_context&,
        swift_managed_group_receiver& held,cxx_error&) noexcept
        SWIFT_NAME(prepareManagedGroupReceiver(swiftConfig:schemas:startup:context:retained:error:));

    bool validate_managed_open_if_present() const noexcept SWIFT_NAME(validateManagedOpenIfPresent()) {
        return sealed([&]{
            auto owner=impl_;
            if(!owner||!owner->owns_creator_process())throw std::runtime_error("native owner creator unavailable");
            const auto context=owner->config().ordinary_context;
            if(!context)return true;
            detail::ordinary_require_creator(context);
            detail::ordinary_before_open(owner->config().path,context);
            // Model/observer resolution may already be in a user WRITE. This
            // transports no authority and must not enter another owned turn.
            // Context validation binds current anchored generation and its
            // retained no-follow physical/catalog custody outside cache leaves.
            if(owner->is_closed())throw std::runtime_error("managed reference retired");
            return true;
        });
    }

    bool validate_managed_reference() const
        SWIFT_NAME(validateManagedReference()) {
        return sealed([&]{
            auto retained=impl_;
            if(!retained)throw std::runtime_error("managed reference owner released");
            detail::ordinary_export_access::validate_managed_reference(*retained);
            return true;
        });
    }
#if defined(LATTICE_MANAGED_ATTACHMENT_QUALIFICATION)
    detail::ordinary_cohort_attachment_qualification_result qualify_managed_attachment(
        const swift_lattice_ref& target) const SWIFT_NAME(qualifyManagedAttachment(target:)) {
        return sealed([&]{
            auto parent=impl_,other=target.impl_;
            if(!parent||!other)throw std::runtime_error("qualification retained owners missing");
            return detail::qualify_managed_cohort_attachment(*parent,*other);
        });
    }
#endif

    // Trusted SPI opens a real existing server owner. Actual relay authorization
    // is retained separately; this method issues no source, principal or cut.
    static LATTICE_SLREF_RET create_checked_mounted_server(const swift_configuration& config,const SchemaVector& schemas)
        SWIFT_NAME(createMountedServerExisting(swiftConfig:schemas:)) LATTICE_SLREF_UNRETAINED {
        return sealed([&]{
            // Allocate the FRT shell before any native opening. There is no
            // throwing wrapper allocation after the private failure fence ends.
#if LATTICE_HAS_FRT
            std::unique_ptr<swift_lattice_ref> result(new swift_lattice_ref());
            result->impl_=swift_lattice::open_checked_mounted_server(config,schemas);
            return result.release();
#else
            swift_lattice_ref result;
            result.impl_=swift_lattice::open_checked_mounted_server(config,schemas);
            return result;
#endif
        });
    }

    // Factory method - creates or reuses instance based on config path, with schemas
    static LATTICE_SLREF_RET create(const configuration& config, const SchemaVector& schemas) LATTICE_SLREF_UNRETAINED {
        return _make(get_or_create_shared(config, schemas));
    }

    // Factory for path-only config
    static LATTICE_SLREF_RET create_with_path(const std::string& path) LATTICE_SLREF_UNRETAINED {
        return create(configuration(path), {});
    }

    // Factory for in-memory
    static LATTICE_SLREF_RET create_in_memory() LATTICE_SLREF_UNRETAINED {
        return create(configuration(), {});
    }

    /// Factory for a generic, read-only "dynamic" open with NO compile-time
    /// model types. Opens read-only (no DDL), reconstructs the schema from the
    /// file, and populates in-memory state. Afterwards use the dynamic
    /// table/property accessors and the normal objects(table:)/count(table:)
    /// query surface.
    ///
    /// The read-only connection is WAL-aware: it joins a concurrent writer's
    /// WAL, so committed-but-not-yet-checkpointed rows ARE visible. Point it at a
    /// live database directly — no checkpoint required.
    static LATTICE_SLREF_RET create_dynamic(const swift_configuration& config)
        SWIFT_NAME(createDynamic(config:)) LATTICE_SLREF_UNRETAINED {
        swift_configuration cfg = config;
        cfg.read_only = true;
        auto impl = std::make_shared<swift_lattice>(std::move(cfg));
        // Register so managed objects hydrated from queries can resolve their
        // owning lattice (managed::lattice_shared -> shared_for_lattice ->
        // get_by_pointer). Bare make_shared bypasses get_or_create's cache.
        detail::LatticeCache::instance().register_pointer(impl);
        impl->open_dynamic();
        return _make(impl);
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
    static LATTICE_SLREF_RET create_with_migration(
        const configuration& config,
        const SchemaVector& schemas,
        swift_migration_block_t migration_block
    ) SWIFT_NAME(create(config:schemas:migration:)) LATTICE_SLREF_UNRETAINED {
        // Create a modified config with the migration block wrapped
        configuration new_config = config;
        new_config.migration_block = [migration_block](migration_context& ctx) {
#if LATTICE_HAS_FRT
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
#else
            // Value-type path: the ref is a plain stack value; no retain/release.
            swift_migration_context_ref swift_ctx(&ctx);
            migration_block(&swift_ctx);
            swift_ctx.apply_row_updates();
#endif
        };

        return _make(get_or_create_shared(new_config, schemas));
    }
    
    /// Uncached construction for query-only clone connections (Swift's
    /// attaching()). The keyed cache would dedup a clone whose config matches
    /// its parent (same path/scheduler/schema/no-sync) and return the PARENT
    /// instance — the subsequent ATTACH would then mutate the parent's own
    /// connection. A clone must always be a distinct instance.
    static LATTICE_SLREF_RET create_uncached(const swift_configuration& config,
                                             const SchemaVector& schemas)
    SWIFT_NAME(createUncached(swiftConfig:schemas:)) LATTICE_SLREF_UNRETAINED {
        auto impl = detail::LatticeCache::instance().create_uncached(config, schemas);
        return _make(impl);
    }

    static LATTICE_SLREF_RET create_managed_attachment_clone(const swift_lattice_ref& parent,
        const swift_configuration& config,cxx_error& error)
    SWIFT_NAME(createManagedAttachmentClone(parent:swiftConfig:error:)) LATTICE_SLREF_UNRETAINED {
        return sealed([&]() -> LATTICE_SLREF_RET {
            try {
                auto retained=parent.impl_;
                if(!retained)throw std::runtime_error("managed attachment parent unavailable");
                auto child=std::shared_ptr<swift_lattice>(new swift_lattice(config,*retained));
                detail::ordinary_export_access::admit_managed_clone(*retained,*child);
                detail::LatticeCache::instance().register_pointer(child);
                return _make(std::move(child));
            } catch(const std::exception& value) { error=value; }
            catch(...) { error=std::runtime_error("managed attachment clone failed"); }
            return _make({});
        });
    }

    static LATTICE_SLREF_RET create(const swift_configuration& config,
                                     const SchemaVector& schemas)
    SWIFT_NAME(create(swiftConfig:schemas:)) LATTICE_SLREF_UNRETAINED {
        LOG_DEBUG("swift_lattice_ref", "create() start path=%s schemas=%zu", config.path.c_str(), schemas.size());
        LOG_DEBUG("swift_lattice_ref", "create() calling get_or_create_shared");
        auto impl = get_or_create_shared(config, schemas);
        LOG_DEBUG("swift_lattice_ref", "create() done, impl=%p", impl.get());
        return _make(impl);
    }

    /// Factory method with swift_configuration (supports row migration callback).
    /// Called for each row in tables with schema changes.
    /// The callback receives (table_name, old_row, new_row) where:
    /// - old_row: populated with current row data
    /// - new_row: should be filled with transformed data
    static LATTICE_SLREF_RET create(const swift_configuration& config,
                                     const SchemaVector& schemas,
                                     cxx_error& err) SWIFT_NAME(create(swiftConfig:schemas:error:)) LATTICE_SLREF_UNRETAINED;


    // Explicit fresh-only opt-in; every facade retains the actual derived owner.
    static LATTICE_SLREF_RET create_continuous(const swift_configuration&,const SchemaVector&,
        const continuous_policy&,continuous_result&) SWIFT_NAME(createContinuous(swiftConfig:schemas:policy:result:)) LATTICE_SLREF_UNRETAINED;
    continuous_result begin_continuous(int64_t attempt) const noexcept SWIFT_NAME(beginContinuous(attempt:));
    continuous_result inspect_continuous() const noexcept SWIFT_NAME(inspectContinuous());

    // Access the underlying swift_lattice (returns pointer for Swift interop)
    swift_lattice* get() { return impl_.get(); }
    const swift_lattice* get() const { return impl_.get(); }

#if LATTICE_HAS_FRT
    // For SWIFT_SHARED_REFERENCE
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }
#endif

    // Whether this ref backs a real lattice (cross-path non-null signal; the
    // value path returns an empty ref instead of nullptr).
    bool valid() const SWIFT_NAME(isValid()) { return impl_ != nullptr; }

    // C++-only minting boundary: callers already possess the actual owner.
    // This exposes an opaque weak identity, never the private creator predicate.
    static bridge_owner_identity identity_for_owner(const std::shared_ptr<swift_lattice>& owner) noexcept {
        if (!owner || !owner->owns_creator_process()) return {};
        return bridge_owner_identity(owner);
    }
    bridge_owner_identity owner_identity() const noexcept SWIFT_NAME(ownerIdentity()) {
        return identity_for_owner(impl_);
    }

    // const so it's callable on a `let` value-type ref below the FRT floor.
    int64_t hash_value() const {
        return reinterpret_cast<intptr_t>(impl_.get());
    }

    std::string path() const { return sealed([&] { return impl().path(); }); }

    detail::store_resource_name_observation capture_store_name_observation() const noexcept
        SWIFT_NAME(captureStoreNameObservation()) {
        return sealed([&] { return impl().capture_store_name_observation(); });
    }



private:
    // Single deref point for the forwarders below. Its callback refusal throws
    // only INSIDE each exported forwarder's exception boundary. Asserts on an
    // empty ref: calling db ops on an invalid handle is a programmer error on
    // both paths — the FRT factories return nullptr instead, and the value-path
    // factories return an empty ref that callers must isValid()-check first.
    swift_lattice& impl() const {
        assert(impl_ && "swift_lattice_ref used while empty/invalid");
        impl_->require_catalog_callback_absent();
        return *impl_;
    }

    // A typed error overload must report admission failure in its caller's
    // cxx_error, in addition to the ordinary thread-local diagnostic. Call the
    // throwing underlying overload here so both std and foreign exceptions
    // are contained at this actual Swift boundary.
    template <typename F>
    static bool sealed_error(cxx_error& error, F&& body) noexcept {
        return sealed_with_error(error, std::forward<F>(body));
    }

    swift_lattice& checked_add_owner() const {
        if (!impl_) throw std::runtime_error("Cannot add through an empty lattice ref");
        auto& owner = impl(); // retain the existing real catalog callback guard
        if (owner.is_closed()) throw std::runtime_error("Cannot add to a closed lattice");
        return owner;
    }

    template <typename Objects>
    void checked_bulk_body(Objects& objects) const {
        for (const auto& object : objects) {
#if LATTICE_HAS_FRT
            if (!object || !object->valid()) throw std::runtime_error("Bulk add requires valid object refs");
#else
            if (!object.valid()) throw std::runtime_error("Bulk add requires valid object refs");
#endif
        }
        checked_add_owner().add_bulk(objects);
    }

    // Existing observer/setter APIs consume their context on every outcome.
    // Keep it here until impl() admits the call, then transfer exactly once to
    // the underlying trampoline (which takes custody before any other work).
    // Observer registrations already consume null context. The four callback
    // setters preserve their distinct nil-context + nil-callback clear contract:
    // that pair never invokes a supplied destroy(nullptr), even on refusal.
    template <typename F>
    decltype(auto) sealed_context(void* context, void (*destroy)(void*), F&& body,
                                  bool consumes_null_context = true) const {
        return sealed([&] {
            struct pending_context {
                void* value;
                void (*destroy)(void*);
                bool consumes_null;
                bool pending = true;
                ~pending_context() noexcept {
                    if (pending && destroy && (value || consumes_null)) { try { destroy(value); } catch (...) {} }
                }
            } pending{context, destroy, consumes_null_context};
            auto& owner = impl();
            pending.pending = false;
            return body(owner);
        });
    }

    // The generation surface has an independent stale signal; an outer
    // callback refusal must not become an apparently genuine empty image.
    template <typename F>
    auto sealed_generation(F&& body) const {
        using R = decltype(body());
        bool completed = false;
        auto result = sealed([&] { auto value = body(); completed = true; return value; });
        if (!completed) {
            swift_lattice::generation_read_stale_tl() = true;
            if constexpr (std::is_integral_v<R>) return R(-1);
        }
        return result;
    }

    template <typename F, typename R>
    static R sealed_or(F&& body, R failure) {
        bool completed = false;
        auto result = sealed([&] { auto value = body(); completed = true; return value; });
        return completed ? result : failure;
    }

public:
    // ---- Thin const forwarders to the underlying swift_lattice, so the db ops
    //      are callable on this handle on both paths: on iOS 16.4+ it is a
    //      foreign-reference class; below the floor it is a copyable value type
    //      whose inner shared_ptr aliases the same db. `const` is load-bearing:
    //      const member functions import into Swift as non-mutating, so the
    //      Swift side can hold the handle in a `let` (shallow const — calling
    //      non-const swift_lattice methods through the shared_ptr member is
    //      legal in a const method). ----

#if defined(LATTICE_SYNC_COMMIT_PROBE)
    // Opt-in harness adapter only; absent from ordinary modules and binaries.
    int32_t sync_commit_probe_arm(uint64_t operation, uint64_t attempt) const noexcept
        SWIFT_NAME(syncCommitProbeArm(operation:attempt:)) {
        if (!impl_) return 3;
        return impl_->sync_commit_probe_arm(operation, attempt);
    }
    sync_commit_probe_receipt sync_commit_probe_finish(uint64_t operation,
                                                       uint64_t attempt) const noexcept
        SWIFT_NAME(syncCommitProbeFinish(operation:attempt:)) {
        if (!impl_) return {};
        return impl_->sync_commit_probe_finish(operation, attempt);
    }
#endif

    // Mechanical qualification only. Retains THIS ref's actual impl_ on both
    // FRT/value paths. A valid nonthrowing destroy callback is the context
    // transfer precondition: null destroy refuses and leaves caller custody;
    // otherwise context is consumed on every outcome. Native handles
    // must be prepared/consumed/closed/released on the caller's IO lane.
    server_export_endpoint make_server_export_endpoint_for_qualification(
        void* context,int32_t(*enqueue)(void*,const uint8_t*,size_t,uint64_t),
        void(*destroy)(void*),const server_export_limits& limits) const noexcept
        SWIFT_NAME(makeServerExportEndpointForQualification(context:enqueue:destroy:limits:));

    // Real relay setup only: current/destroy retain the actual SDK connection
    // cell. No qualified admission is exposed; the exact live setup consumes
    // its application authorization once. Null destroy leaves caller custody;
    // a nonnull nonthrowing destroy transfers context on every outcome.
    relay_recovery_setup open_relay_recovery_setup(const std::string& policy,const std::string& connection,
        void* context,int32_t(*current)(void*),void(*destroy)(void*))const noexcept
        SWIFT_NAME(openRelayRecoverySetup(policy:connection:context:current:destroy:));
    // Package automatic setup: one current-identity mutex try, no retry. The
    // separate nonthrowing admissible callback only vetoes pre-effect entry;
    // it never changes the ordinary current callback or retained session.
    relay_recovery_setup open_relay_recovery_setup_automatic(const std::string& policy,const std::string& connection,
        void* context,int32_t(*current)(void*),int32_t(*admissible)(void*),void(*destroy)(void*))const noexcept
        SWIFT_NAME(openRelayRecoverySetupAutomatic(policy:connection:context:current:admissible:destroy:));
    // Package administration of the actual resolved relay owner. 1 is known
    // migrated, 2 is no-effect quiescence contention, 4 is an error with no
    // assertion of rollback/absence. This never creates a session admission.
    // One-shot administration: never returns or caches a privileged owner.
    static int32_t migrate_relay_receipt_coverage_file(const std::string& path,const SchemaVector& schemas,
        int64_t schema_version,int32_t busy_timeout_ms,const std::string& prior,const std::string& next)noexcept
        SWIFT_NAME(migrateRelayReceiptCoverageFile(path:schemas:schemaVersion:busyTimeoutMilliseconds:prior:next:));
    static relay_lifecycle_adoption_result adopt_relay_lifecycle_file(const std::string& path,const SchemaVector& schemas,
        int64_t schema_version,int32_t busy_timeout_ms,const std::string& prior,const std::string& next)noexcept
        SWIFT_NAME(adoptRelayLifecycleFile(path:schemas:schemaVersion:busyTimeoutMilliseconds:prior:next:));
    int32_t migrate_relay_receipt_coverage(const std::string& prior,const std::string& next)const noexcept
        SWIFT_NAME(migrateRelayReceiptCoverage(prior:next:));

    // CRUD: checked status is independent of message allocation/empty what().
    bool add_checked(const dynamic_object_ref& ref, cxx_error& err) const noexcept {
        return sealed_error(err, [&] { checked_add_owner().add(ref); });
    }
    void add(const dynamic_object_ref& ref, cxx_error& err) const { (void)add_checked(ref, err); }
    void add_preserving_global_id(const dynamic_object_ref& ref, const std::string& preserved_global_id) const { sealed([&] { checked_add_owner().add_preserving_global_id(ref, preserved_global_id); }); }
    bool add_preserving_global_id_checked(const dynamic_object_ref& ref, const std::string& preserved_global_id,
                                         cxx_error& err) const noexcept {
        return sealed_error(err, [&] { checked_add_owner().add_preserving_global_id(ref, preserved_global_id); });
    }
    void add_preserving_global_id(const dynamic_object_ref& ref, const std::string& preserved_global_id,
                                  cxx_error& err) const { (void)add_preserving_global_id_checked(ref, preserved_global_id, err); }
#if LATTICE_HAS_FRT
    bool add_bulk_checked(std::vector<dynamic_object_ref*>& objects, cxx_error& err) const noexcept {
        return sealed_error(err, [&] { checked_bulk_body(objects); });
    }
    void add_bulk(std::vector<dynamic_object_ref*>& objects) const { sealed([&] { checked_bulk_body(objects); }); }
#else
    bool add_bulk_checked(std::vector<dynamic_object_ref>& objects, cxx_error& err) const noexcept {
        return sealed_error(err, [&] { checked_bulk_body(objects); });
    }
    void add_bulk(std::vector<dynamic_object_ref>& objects) const { sealed([&] { checked_bulk_body(objects); }); }
#endif
    bool remove(const dynamic_object_ref& obj) const {
        return sealed([&] { return impl().remove(obj); });
    }

    // Single-object lookups query the row — sealed like the bulk reads
    // (Swift cannot catch C++ exceptions): nullopt + last_bridge_error()
    // on failure.
    std::optional<managed<swift_dynamic_object>> object(int64_t primary_key, const std::string& table_name) const {
        return sealed([&] { return impl().object(primary_key, table_name); });
    }
    std::optional<managed<swift_dynamic_object>> object_by_global_id(const std::string& global_id, const std::string& table_name) const {
        return sealed([&] { return impl().object_by_global_id(global_id, table_name); });
    }
    std::vector<managed<swift_dynamic_object>> objects(
        const std::string& table_name,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt,
        OptionalString distinct_by = std::nullopt,
        const ColumnValueVector& params = {}) const { return sealed([&] { return impl().objects(table_name, where_clause, order_by, limit, offset, group_by, distinct_by, params); }); }

    // Dynamic (schema-from-file) introspection — forwarders to the schema
    // reconstructed by a dynamic open. See swift_lattice::open_dynamic.
    swift_property_info property_info(const std::string& table,const std::string& property) const
        SWIFT_NAME(propertyInfo(table:property:)) {return sealed([&]{return impl().get_property_info(table,property);});}
    std::vector<std::string> dynamic_table_names() const SWIFT_NAME(dynamicTableNames()) { return sealed([&] { return impl().dynamic_table_names(); }); }
    std::vector<property_descriptor> dynamic_properties_for(const std::string& table_name) const
        SWIFT_NAME(dynamicPropertiesFor(_:)) { return sealed([&] { return impl().dynamic_properties_for(table_name); }); }

    std::vector<managed<swift_dynamic_object>> union_objects(
        const std::vector<std::string>& table_names,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        const ColumnValueVector& params = {}) const { return sealed([&] { return impl().union_objects(table_names, where_clause, order_by, limit, offset, params); }); }
    size_t count(const std::string& table_name,
                 OptionalString where_clause,
                 OptionalString group_by,
                 OptionalString distinct_by,
                 const ColumnValueVector& params = {}) const { return sealed([&] { return impl().count(table_name, where_clause, group_by, distinct_by, params); }); }
    bool delete_where(const std::string& table_name, std::optional<std::string> where_clause,
                      const ColumnValueVector& params = {}) const {
        return sealed([&] { return impl().delete_where(table_name, where_clause, params); });
    }

    // Maintenance — Swift-reachable via the MCP vacuum/compact tools; the
    // same pressure events that fail queries fail these. Sealed: 0/no-op +
    // last_bridge_error() on failure.
    void train_untrained_vec0_tables() const SWIFT_NAME(trainUntrainedVec0Tables()) {
        sealed([&] { impl().train_untrained_vec0_tables(); });
    }
    void wait_for_vec0_training() const SWIFT_NAME(waitForVec0Training()) {
        sealed([&] { impl().wait_for_vec0_training(); });
    }
    int64_t vacuum_vec0(const std::string& table, const std::string& column) const {
        return sealed([&] { return impl().vacuum_vec0(table, column); });
    }
    /// false when VACUUM threw — the message is in last_query_error().
    bool vacuum() const { return sealed([&] { return impl().vacuum(); }); }
    /// VACUUM + TRUNCATE checkpoint in the order WAL mode needs; see swift_lattice::reclaim_space.
    reclaim_result reclaim_space(int max_passes = 2) const
        SWIFT_NAME(reclaimSpace(maxPasses:)) {
        auto result = sealed([&] { return impl().reclaim_space(max_passes); });
        if (last_bridge_call_failed()) {
            result.ok = false;
            try { result.error = last_bridge_error(); } catch (...) {}
        }
        return result;
    }
    int64_t safe_compact_audit_log(int64_t stale_threshold_seconds = 0) const {
        return sealed([&] { return impl().safe_compact_audit_log(stale_threshold_seconds); });
    }
    /// Age-based, cursor-safe history prune (see lattice_db::prune_audit_log).
    int64_t prune_audit_log(int64_t retention_seconds) const
        SWIFT_NAME(pruneAuditLog(retentionSeconds:)) {
        return sealed([&] { return impl().prune_audit_log(retention_seconds); });
    }
    void record_audit_watermark() const SWIFT_NAME(recordAuditWatermark()) {
        sealed([&] { impl().record_audit_watermark(); });
    }
    /// Test-only: shift every recorded watermark `seconds` into the past.
    void backdate_audit_watermarks(int64_t seconds) const SWIFT_NAME(backdateAuditWatermarks(seconds:)) {
        sealed([&] { impl().backdate_audit_watermarks(seconds); });
    }
    void set_replication_slot_observer(const std::string& sync_id, bool is_observer) const
        SWIFT_NAME(setReplicationSlotObserver(syncId:isObserver:)) {
        sealed([&] { impl().set_replication_slot_observer(sync_id, is_observer); });
    }
    /// Header of one audit row without touching its payload (changeHeaders).
    audit_header audit_header_for(int64_t audit_id) const SWIFT_NAME(auditHeader(id:)) {
        return sealed([&] { return impl().audit_header_for(audit_id); });
    }
    /// no_history late-binding for audit rows serialized OUTSIDE core (the
    /// Swift relay's observer push): the live values of `columns` for one row
    /// as the wire JSON object ({"col":{"kind":..,"value":..}}); "{}" when the
    /// row is gone. See lattice::late_bind_no_history.
    std::string no_history_live_values_json(const std::string& table_name,
                                            const std::string& global_row_id,
                                            const std::vector<std::string>& columns) const
        SWIFT_NAME(noHistoryLiveValuesJSON(tableName:globalRowId:columns:)) {
        return sealed([&] {
            return lattice::no_history_live_values_json(impl().db(), table_name, global_row_id, columns);
        });
    }
    int64_t normalize_audit_timestamps() const
        SWIFT_NAME(normalizeAuditTimestamps()) {
        return sealed([&] { return impl().normalize_audit_timestamps(); });
    }
    int64_t force_compact_audit_log() const {
        return sealed([&] { return impl().force_compact_audit_log(); });
    }
    void backdate_replication_slots(int64_t seconds) const { sealed([&] { impl().backdate_replication_slots(seconds); }); }
    /// The TRUNCATE checkpoint's real outcome (busy / partial / complete).
    checkpoint_outcome checkpoint() const { return sealed([&] { return impl().checkpoint(); }); }
    /// Frames checkpointed; -1 when nothing could run; -2 when the call THREW
    /// (distinct from a legitimate 0 — the sealed default used to collide).
    int64_t checkpoint_bounded(int64_t busy_budget_ms = 250) const
        SWIFT_NAME(checkpointBounded(busyBudgetMs:)) {
        const int64_t frames = sealed([&] { return impl().checkpoint_bounded(busy_budget_ms); });
        return last_bridge_call_failed() ? -2 : frames;
    }
    void optimize() const { sealed([&] { impl().optimize(); }); }

    owned_catalog_operation make_owned_catalog_operation(const std::string& tables_json,
        int64_t timeout_nanoseconds) const SWIFT_NAME(makeOwnedCatalogOperation(tablesJSON:timeoutNanoseconds:)) {
        return sealed([&] {
            (void)impl(); // Actual callback/owner validation before control creation.
            return owned_catalog_operation(lattice::make_catalog_stream_operation(
                std::static_pointer_cast<lattice_db>(impl_), tables_json, timeout_nanoseconds));
        });
    }

    projection_operation start_projection(const projection_request& request) const
        SWIFT_NAME(startProjection(_:)) {
        return sealed([&] {
            if (request.failed_) {
                projection_query invalid;
                invalid.max_rows = -1;
                return projection_operation(impl().start_projection(invalid));
            }
            return projection_operation(impl().start_projection(request.query_));
        });
    }

    int64_t apply_selected_mutations(const selected_mutation_batch& batch) const
        SWIFT_NAME(applySelectedMutations(_:)) {
        return sealed([&] { return impl().apply_selected_mutations(batch); });
    }

    // Transactions — begin_transaction throws db_error when the busy
    // timeout lapses or an allocation fails under memory pressure; that
    // escaped into Swift and killed the Engram MCP server twice on
    // 2026-08-05 (recall's access-stat bump runs in a transaction).
    // Sealed: the failure lands in last_bridge_error(); a failed BEGIN
    // means subsequent writes autocommit individually (atomicity degrades,
    // process survives), and the paired commit/rollback no-op-fails into
    // the same slot instead of trapping.
private:
    swift_transaction_result begin_transaction_receipt_impl(bool strict) const noexcept {
        swift_transaction_result result;
        sealed([&] {
            auto& owner = impl();
            auto issued = owner.begin_transaction_receipt_(strict);
            result.receipt_ = swift_transaction_receipt(std::move(issued), owner_identity());
        });
        result.capture(borrow_bridge_outcome());
        return result;
    }
public:
    swift_transaction_result begin_transaction_receipt() const noexcept
        SWIFT_NAME(beginTransactionReceipt()) { return begin_transaction_receipt_impl(true); }
    // Legacy explicit APIs keep their existing raw-access/waiting contract.
    // Their receipt is only ending/bookkeeping DATA; it never relaxes the
    // strict expected-receipt SQL settlement used by withTransaction.
    swift_transaction_result begin_legacy_transaction_receipt() const noexcept
        SWIFT_NAME(beginLegacyTransactionReceipt()) { return begin_transaction_receipt_impl(false); }
private:
    swift_transaction_result settle_transaction_receipt(
        const swift_transaction_receipt& receipt, bool rollback, bool expected = true) const noexcept {
        swift_transaction_result result;
        result.receipt_ = receipt;
        sealed([&] {
            if (!receipt.valid() || !owner_identity().matches(receipt.owner_))
                throw std::runtime_error("Transaction receipt does not belong to this owner");
            auto& owner = impl();
            try {
                if (expected) {
                    if (rollback) owner.rollback_transaction_receipt_(receipt.core_);
                    else owner.commit_transaction_receipt_(receipt.core_);
                } else {
                    // This is the existing public "settle current" contract.
                    // The captured receipt controls only which Swift frame
                    // may retire after the actual old original ended.
                    if (rollback) owner.rollback();
                    else owner.commit();
                }
            } catch (...) {
                result.ended_ = owner.transaction_receipt_ended_(receipt.core_);
                throw;
            }
            result.ended_ = owner.transaction_receipt_ended_(receipt.core_);
        });
        result.capture(borrow_bridge_outcome());
        return result;
    }
public:
    swift_transaction_result commit_transaction_receipt(const swift_transaction_receipt& receipt) const noexcept
        SWIFT_NAME(commitTransactionReceipt(_:)) { return settle_transaction_receipt(receipt, false); }
    swift_transaction_result rollback_transaction_receipt(const swift_transaction_receipt& receipt) const noexcept
        SWIFT_NAME(rollbackTransactionReceipt(_:)) { return settle_transaction_receipt(receipt, true); }
    swift_transaction_result commit_legacy_transaction_receipt(const swift_transaction_receipt& receipt) const noexcept
        SWIFT_NAME(commitLegacyTransactionReceipt(_:)) { return settle_transaction_receipt(receipt, false, false); }
    swift_transaction_result rollback_legacy_transaction_receipt(const swift_transaction_receipt& receipt) const noexcept
        SWIFT_NAME(rollbackLegacyTransactionReceipt(_:)) { return settle_transaction_receipt(receipt, true, false); }
    void begin_transaction() const { sealed([&] { impl().begin_transaction(); }); }
    void commit() const { sealed([&] { impl().commit(); }); }
    void rollback() const { sealed([&] { impl().rollback(); }); }
    swift_close_result close_checked() const noexcept SWIFT_NAME(closeChecked()) {
        // Retain the real derived owner through cleanup and result conversion.
        // Error formatting is independently sealed, including allocation failure.
        try {
            const auto retained=impl_;
            if(!retained) {lattice_close_result result;result.cleanup_complete=false;return swift_close_result(result);}
            return swift_close_result(retained->close_checked());
        }catch(...) {
            lattice_close_result result;result.remember(std::current_exception());return swift_close_result(result);
        }
    }
    void close() const noexcept {
        auto result=close_checked();
        // Legacy compatibility remains nonthrowing and records an error without
        // conflating a pending/disconnected close with completed remote ACKs.
        try {
            clear_bridge_outcome();
            if(result.failed_)record_bridge_error(result.message_.empty()?"C++ close failed (message unavailable)":result.message_.c_str());
            else if(result.sync_!=static_cast<int32_t>(sync_drain_state::drained)&&result.sync_!=static_cast<int32_t>(sync_drain_state::not_attempted))
                record_bridge_error("Closed with synchronization incomplete; inspect closeChecked result");
        }catch(...) {record_bridge_error("C++ close diagnostic unavailable");}
    }
    bool is_closed() const SWIFT_NAME(isClosed()) { return sealed_or([&] { return impl().is_closed(); }, true); }
    bool has_attached_stores() const SWIFT_NAME(hasAttachedStores()) { return sealed([&] { return impl().has_attached_stores(); }); }

    // Sync status
    bool is_sync_agent() const { return sealed([&] { return impl().is_sync_agent(); }); }
    bool is_sync_connected() const { return sealed([&] { return impl().is_sync_connected(); }); }
    void clear_sync_filter() const { sealed([&] { impl().clear_sync_filter(); }); }

    // Spatial (R*Tree)
    std::vector<managed<swift_dynamic_object>> objects_within_bbox(
        const std::string& table_name,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt) const
        SWIFT_NAME(objectsWithinBBox(table:geoColumn:minLat:maxLat:minLon:maxLon:where:orderBy:limit:offset:groupBy:)) { return sealed([&] { return impl().objects_within_bbox(table_name, geo_column, min_lat, max_lat, min_lon, max_lon,
                                          where_clause, order_by, limit, offset, group_by); }); }
    int64_t count_within_bbox(
        const std::string& table_name,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt) const
        SWIFT_NAME(countWithinBBox(table:geoColumn:minLat:maxLat:minLon:maxLon:where:)) { return sealed([&] { return impl().count_within_bbox(table_name, geo_column, min_lat, max_lat, min_lon, max_lon, where_clause); }); }

    std::vector<managed<swift_dynamic_object>> objects_within_bbox_shape(
        const std::string& table_name, const std::string& geo_column,
        double min_lat, double max_lat, double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt, OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt, OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt, OptionalString distinct_by = std::nullopt) const
        SWIFT_NAME(objectsWithinBBoxShape(table:geoColumn:minLat:maxLat:minLon:maxLon:where:orderBy:limit:offset:groupBy:distinctBy:)) { return sealed([&] { return impl().objects_within_bbox_shape(table_name, geo_column, min_lat, max_lat, min_lon, max_lon,
            where_clause, order_by, limit, offset, group_by, distinct_by); }); }
    int64_t count_within_bbox_shape(
        const std::string& table_name, const std::string& geo_column,
        double min_lat, double max_lat, double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt,
        OptionalString group_by = std::nullopt, OptionalString distinct_by = std::nullopt) const
        SWIFT_NAME(countWithinBBoxShape(table:geoColumn:minLat:maxLat:minLon:maxLon:where:groupBy:distinctBy:)) { return sealed([&] { return impl().count_within_bbox_shape(table_name, geo_column, min_lat, max_lat, min_lon, max_lon,
            where_clause, group_by, distinct_by); }); }

    // Combined proximity
    CombinedQueryResultVector combined_nearest_query(
        const std::string& table_name,
        const BoundsConstraintVector& bounds,
        const VectorConstraintVector& vectors,
        const GeoConstraintVector& geos,
        const TextConstraintVector& texts,
        OptionalString where_clause,
        const sort_descriptor& sort,
        int64_t limit,
        OptionalString group_by = std::nullopt,
        OptionalString distinct_by = std::nullopt) const
        SWIFT_NAME(combinedNearestQuery(table:bounds:vectors:geos:texts:where:sort:limit:groupBy:distinctBy:)) { return sealed([&] { return impl().combined_nearest_query(table_name, bounds, vectors, geos, texts,
                                             where_clause, sort, limit, group_by, distinct_by); }); }
    int64_t combined_nearest_query_count(
        const std::string& table_name,
        const BoundsConstraintVector& bounds,
        const VectorConstraintVector& vectors,
        const GeoConstraintVector& geos,
        const TextConstraintVector& texts,
        OptionalString where_clause,
        const sort_descriptor& sort,
        int64_t limit,
        OptionalString group_by = std::nullopt,
        OptionalString distinct_by = std::nullopt) const
        SWIFT_NAME(combinedNearestQueryCount(table:bounds:vectors:geos:texts:where:sort:limit:groupBy:distinctBy:)) { return sealed([&] { return impl().combined_nearest_query_count(table_name, bounds, vectors, geos, texts,
                                                   where_clause, sort, limit, group_by, distinct_by); }); }

    // Attach another lattice's underlying handle. Takes a swift_lattice_ref so the
    // argument is iOS-15-safe (vs the raw swift_lattice). On the FRT path the ref
    // is a foreign-reference class (heap-only) so it must be passed by pointer; on
    // the value path it's a plain value passed by copy (shares impl_).
#if LATTICE_HAS_FRT
    bool attach(swift_lattice_ref* other) const { return sealed([&] { if (!other || !other->valid()) throw std::invalid_argument("attachment owner is empty"); return impl().attach(other->impl()); }); }
    bool detach(swift_lattice_ref* other) const { return sealed([&] { if (!other || !other->valid()) throw std::invalid_argument("attachment owner is empty"); return impl().detach(other->impl()); }); }
#else
    bool attach(swift_lattice_ref other) const { return sealed([&] { if (!other.valid()) throw std::invalid_argument("attachment owner is empty"); return impl().attach(other.impl()); }); }
    bool detach(swift_lattice_ref other) const { return sealed([&] { if (!other.valid()) throw std::invalid_argument("attachment owner is empty"); return impl().detach(other.impl()); }); }
#endif
    OptionalString last_attach_error() const { return sealed([&] { return impl().last_attach_error(); }); }

    // Sync data ingestion
    // Exact held owner on both FRT and value paths. Empty/retired owners return
    // an explicitly invalid result; neither path reads shared mutable error text.
    ordinary_upload_ids_result receive_ordinary_upload(const ByteVector& data,const std::vector<std::string>& models)const noexcept
        SWIFT_NAME(receiveOrdinaryUpload(data:models:)) {
        try{auto owner=impl_;if(!owner||!owner->owns_creator_process()||owner->is_closed())return {};
            return ordinary_upload_ids_result::receive(*owner,data,models);}
        catch(...){return {};}
    }
    ordinary_upload_acknowledgment_result receive_ordinary_upload_with_acknowledgment(const ByteVector& data,const std::vector<std::string>& models)const noexcept
        SWIFT_NAME(receiveOrdinaryUploadWithAcknowledgment(data:models:)) {
        try{auto owner=impl_;if(!owner||!owner->owns_creator_process()||owner->is_closed())return {};
            return ::lattice::receive_ordinary_source_upload_with_acknowledgment(*owner,data.data(),data.size(),models);}
        catch(...){return {};}
    }
    ordinary_sync_observation_ref ordinary_sync_observation_tables(const std::vector<std::string>& models)const noexcept
        SWIFT_NAME(ordinarySyncObservationTables(models:)) {
        try{auto owner=impl_;if(!owner||!owner->owns_creator_process()||owner->is_closed())return {};
            return ordinary_sync_observation_ref::select(*owner,models);}
        catch(...){return {};}
    }
    // A nonnull, nonthrowing destroy callback transfers context on every outcome.
    // Null destroy refuses before transfer and leaves context with its caller.
    // Keep a local share through registration/removal: final context destruction
    // cannot occur under Core's observer leaf even if registration throws.
    uint64_t add_payload_free_table_observer(const std::string& table,void* context,
        void (*callback)(void*),void (*destroy)(void*))const noexcept {
        if(!destroy)return 0;
        std::shared_ptr<void> owned;
        std::shared_ptr<swift_lattice> owner;
        uint64_t token=0;
        try{
            // shared_ptr invokes destroy(context) if its control-block allocation fails.
            owned=std::shared_ptr<void>(context,destroy);
            owner=impl_;
            if(!callback||!owner||!owner->owns_creator_process()||owner->is_closed())return 0;
            token=owner->lattice_db::add_table_observer(table,
                [owned,callback](const std::vector<lattice_db::change_event>& batch){
                    if(!batch.empty())callback(owned.get());
                });
            if(owner->is_closed()){owner->remove_table_observer(table,token);return 0;}
            return token;
        }catch(...){
            if(token&&owner){try{owner->remove_table_observer(table,token);}catch(...){}}
            return 0;
        }
    }
    std::vector<std::string> receive_sync_data(const ByteVector& data) const { return sealed([&] { return impl().receive_sync_data(data); }); }
    ordinary_sync_export_ref capture_ordinary_export(const std::vector<std::string>& models)const noexcept
        SWIFT_NAME(captureOrdinaryExport(models:)) {
        try{if(!valid()||impl().is_closed())return {};return ordinary_sync_export_ref::capture(impl(),models);}catch(...){return {};}
    }
    ordinary_mounted_history_source_ref register_ordinary_mounted_history_source(const std::vector<std::string>& mounted)const noexcept
        SWIFT_NAME(registerOrdinaryMountedHistorySource(models:)) {
        try{if(!valid()||impl().is_closed())return {};return ordinary_mounted_history_source_ref::issue(impl_,mounted);}catch(...){return {};}
    }
    ordinary_history_export_ref capture_ordinary_history_export(const std::vector<std::string>& models)const noexcept
        SWIFT_NAME(captureOrdinaryHistoryExport(models:)) {
        try{if(!valid()||impl().is_closed())return {};return ordinary_history_export_ref::capture(impl(),models);}catch(...){return {};}
    }
    OptionalString last_receive_error() const { return sealed([&] { return impl().last_receive_error(); }); }

    /// The sealed query surface's error slot (see error.hpp): nullopt if
    /// the last sealed call on THIS THREAD succeeded, else its failure
    /// message. Same-thread, immediately-after-the-call semantics — the
    /// query analog of last_receive_error(), thread-local because queries
    /// run concurrently across galaxy/recall threads.
    OptionalString last_query_error() const {
        if (last_bridge_error().empty()) return std::nullopt;
        return last_bridge_error();
    }

    // Sync filter
    void update_sync_filter(const SyncFilterVector& filter) const { sealed([&] { impl().update_sync_filter(filter); }); }
    void update_sync_filter_for_channel(const std::string& channel, const SyncFilterVector& filter) const { sealed([&] { impl().update_sync_filter_for_channel(channel, filter); }); }
    void clear_sync_filter_for_channel(const std::string& channel) const { sealed([&] { impl().clear_sync_filter_for_channel(channel); }); }
    void remove_sync_channel_state(const std::string& sync_id) const { sealed([&] { impl().remove_sync_channel_state(sync_id); }); }
    /// Hard-delete rows with the no-relay marker (admin tombstone purge) —
    /// see lattice_db::delete_rows_no_relay for why a normal delete is
    /// catastrophic on a shared group DB.
    int64_t delete_rows_no_relay(const std::string& table,
                                 const std::vector<std::string>& global_row_ids) const { return sealed([&] { return impl().delete_rows_no_relay(table, global_row_ids); }); }

    // Observers
    void remove_table_observer(const std::string& table_name, uint64_t observer_id) const { sealed([&] { impl().remove_table_observer(table_name, observer_id); }); }
    uint64_t add_table_observer(const std::string& table_name,
                                void* context,
                                void (*callback)(void* ctx,
                                                 const char* const* operations,
                                                 const int64_t* row_ids,
                                                 const char* const* global_row_ids,
                                                 size_t count),
                                void (*destroy)(void*) = nullptr) const { return sealed_context(context, destroy, [&](auto& owner) { return owner.add_table_observer(table_name, context, callback, destroy); }); }
    uint64_t add_object_observer(const std::string& table_name,
                                 int64_t row_id,
                                 void* context,
                                 void (*callback)(const char* changed_field_names, void* ctx),
                                 void (*destroy)(void*) = nullptr) const { return sealed_context(context, destroy, [&](auto& owner) { return owner.add_object_observer(table_name, row_id, context, callback, destroy); }); }
    void remove_object_observer(const std::string& table_name, int64_t row_id, uint64_t observer_id) const { sealed([&] { impl().remove_object_observer(table_name, row_id, observer_id); }); }

    uint64_t add_recovery_refresh_observer(void* context, void (*callback)(void*),
                                           void (*destroy)(void*) = nullptr) const { return sealed_context(context, destroy, [&](auto& owner) { return owner.add_recovery_refresh_observer(context, callback, destroy); }); }
    void remove_recovery_refresh_observer(uint64_t token) const { sealed([&] { impl().remove_recovery_refresh_observer(token); }); }

    int64_t pending_sync_entry_count() const { return sealed([&] { return impl().pending_sync_entry_count(); }); }

    // Sync callbacks (C trampolines)
    void set_on_sync_progress(void* context,
                              void (*callback)(void* ctx,
                                               int64_t pending_upload,
                                               int64_t total_upload,
                                               int64_t acked,
                                               int64_t received,
                                               const char* sync_id),
                              void (*destroy)(void*) = nullptr) const { sealed_context(context, destroy, [&](auto& owner) { owner.set_on_sync_progress(context, callback, destroy); }, callback != nullptr); }
    void set_on_sync_error(void* context,
                           void (*callback)(void* ctx, const char* error, int64_t len),
                           void (*destroy)(void*) = nullptr) const { sealed_context(context, destroy, [&](auto& owner) { owner.set_on_sync_error(context, callback, destroy); }, callback != nullptr); }
    void set_on_sync_state_change(void* context,
                                  void (*callback)(void* ctx, bool connected),
                                  void (*destroy)(void*) = nullptr) const { sealed_context(context, destroy, [&](auto& owner) { owner.set_on_sync_state_change(context, callback, destroy); }, callback != nullptr); }
    void set_on_xproc_idle(void* context,
                           void (*callback)(void*),
                           void (*destroy)(void*) = nullptr) const { sealed_context(context, destroy, [&](auto& owner) { owner.set_on_xproc_idle(context, callback, destroy); }, callback != nullptr); }

    // ---- Read generations + synchronous invalidation hooks (item A
    //      Commit 4). Const forwarders to the catch-all wrappers on
    //      swift_lattice — every signature here is scalar/string-only, so a
    //      SINGLE definition serves both the FRT and the value path (no
    //      #if LATTICE_HAS_FRT split to fall out of sync — the ATT-3
    //      forwarding lesson). See the swift_lattice section for contracts
    //      (§2.3 callback restrictions, sentinel + stale-flag semantics). ----
    /// Payload-free registration for bounded latest-state observers. The
    /// context is consumed on EVERY path, including failed registration.
    /// No table names, changed-field strings or row batches are copied by this
    /// trampoline. The callback inherits the synchronous leaf-lock-only hook
    /// contract; it must only mark bounded state dirty, never query or deliver
    /// user code. Removal does not revoke a callback already copied by Core.
    uint64_t add_coarse_invalidation_hook(
        void* context, void (*callback)(void*, int),
        void (*destroy)(void*) = nullptr) const noexcept {
        try {
            return sealed([&]() -> uint64_t {
                // shared_ptr invokes the deleter even if its control-block
                // allocation fails, so Swift must never release this context
                // again after calling the bridge.
                auto owned = std::shared_ptr<void>(context, destroy ? destroy : [](void*) {});
                if (!callback) throw std::invalid_argument("missing coarse invalidation callback");
                if (!valid()) throw std::runtime_error("Lattice reference is empty");
                if (impl().is_closed()) throw std::runtime_error("Lattice is closed");
                const auto token = impl().add_invalidation_hook_detailed(
                    [owned = std::move(owned), callback](
                        const std::vector<lattice_db::invalidation_table_change>&,
                        lattice_db::invalidation_reason reason) noexcept {
                        try { callback(owned.get(), static_cast<int>(reason)); }
                        catch (...) { /* Never unwind SQLite's C hook frame. */ }
                    });
                // Cover a close that completed between the admission check
                // and Core's hook insertion. Close can still race immediately
                // after this check, as with any returned registration: the
                // owning subscription must reconcile database lifecycle.
                if (impl().is_closed()) {
                    impl().remove_invalidation_hook(token);
                    throw std::runtime_error("Lattice closed during invalidation registration");
                }
                return token;
            });
        } catch (...) {
            // sealed records ordinary std::exception failures. Its diagnostic
            // allocation (or a foreign exception) must not escape into Swift;
            // zero is independently an unconditional registration failure.
            return 0;
        }
    }

    /// Idempotent leaf bookkeeping. Callers close their admission gate before
    /// removal, since an already copied hook can still run after this returns.
    bool remove_coarse_invalidation_hook(uint64_t token) const noexcept {
        try {
            return sealed([&] {
                if (!valid()) return false;
                impl().remove_invalidation_hook(token);
                return true;
            });
        } catch (...) { return false; }
    }

    uint64_t add_invalidation_hook(void* context,
                                   void (*callback)(void* ctx,
                                                    const char* const* changed_tables,
                                                    size_t count,
                                                    int reason),
                                   void (*destroy)(void*) = nullptr) const { return sealed_context(context, destroy, [&](auto& owner) { return owner.add_invalidation_hook(context, callback, destroy); }); }
    uint64_t add_invalidation_hook_with_fields(
        void* context,
        void (*callback)(void* ctx,
                         const char* const* changed_tables,
                         const char* const* changed_fields,
                         size_t count,
                         int reason),
        void (*destroy)(void*) = nullptr) const { return sealed_context(context, destroy, [&](auto& owner) { return owner.add_invalidation_hook_with_fields(context, callback, destroy); }); }
    void remove_invalidation_hook(uint64_t token) const { sealed([&] { impl().remove_invalidation_hook(token); }); }

    uint64_t acquire_read_generation() const { return sealed([&] { return impl().acquire_read_generation(); }); }
    bool retain_read_generation(uint64_t generation_id) const { return sealed([&] { return impl().retain_read_generation(generation_id); }); }
    void release_read_generation(uint64_t generation_id) const { sealed([&] { impl().release_read_generation(generation_id); }); }
    void retire_all_read_generations() const { sealed([&] { impl().retire_all_read_generations(); }); }
    size_t read_generations_outstanding() const { return sealed([&] { return impl().read_generations_outstanding(); }); }
    size_t local_read_generations_outstanding() const { return sealed([&] { return impl().local_read_generations_outstanding(); }); }
    void run_read_pool_maintenance() const { sealed([&] { impl().run_read_pool_maintenance(); }); }

    read_pool_maintenance_result run_read_pool_maintenance_checked() const
        SWIFT_NAME(runReadPoolMaintenanceChecked()) {
        auto result = sealed([&] {
            // Keep exactly this creator owner alive through the complete tick
            // and its same-owner pending observation, even if a public alias
            // is dropped. The ordinary callback guard latches any reentry.
            auto retained = impl_;
            if (!retained) throw db_error("read-pool maintenance original owner unavailable");
            retained->require_catalog_callback_absent();
            return retained->run_read_pool_maintenance_checked();
        });
        if (last_bridge_call_failed()) {
            result.completed = false;
            result.pressure_pending = true;
            try { result.error = last_bridge_error(); } catch (...) {}
        }
        return result;
    }

    automatic_read_maintenance_enrollment_result enable_automatic_read_pool_maintenance_checked() const
        SWIFT_NAME(enableAutomaticReadPoolMaintenanceChecked()) {
        auto result = sealed([&] {
            auto retained = impl_;
            if (!retained) throw db_error("automatic read maintenance original owner unavailable");
            retained->require_catalog_callback_absent();
            return retained->enable_automatic_read_pool_maintenance_checked();
        });
        if (last_bridge_call_failed()) {
            result.completed = false;
            result.enabled = false;
            try { result.error = last_bridge_error(); } catch (...) {}
        }
        return result;
    }


    void set_read_generation_ttl_ms(int64_t ms) const { sealed([&] { impl().set_read_generation_ttl_ms(ms); }); }
    void set_read_generation_max_age_ms(int64_t ms) const { sealed([&] { impl().set_read_generation_max_age_ms(ms); }); }
    void set_wal_keeper_eviction_threshold_bytes(int64_t bytes) const { sealed([&] { impl().set_wal_keeper_eviction_threshold_bytes(bytes); }); }
    int64_t wal_keeper_eviction_threshold_bytes() const { return sealed([&] { return impl().wal_keeper_eviction_threshold_bytes(); }); }
    bool wal_eviction_pending() const { return sealed([&] { return impl().wal_eviction_pending(); }); }

    std::vector<managed<swift_dynamic_object>> objects_at(
        uint64_t generation_id,
        const std::string& table_name,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt,
        OptionalString distinct_by = std::nullopt,
        const ColumnValueVector& params = {}) const { return sealed_generation([&] { return impl().objects_at(generation_id, table_name, where_clause, order_by,
                                 limit, offset, group_by, distinct_by, params); }); }
    int64_t count_at(uint64_t generation_id,
                     const std::string& table_name,
                     OptionalString where_clause = std::nullopt,
                     OptionalString group_by = std::nullopt,
                     OptionalString distinct_by = std::nullopt,
                     const ColumnValueVector& params = {}) const { return sealed_generation([&] { return impl().count_at(generation_id, table_name, where_clause, group_by, distinct_by, params); }); }
    std::vector<managed<swift_dynamic_object>> objects_within_bbox_at(
        uint64_t generation_id,
        const std::string& table_name,
        const std::string& geo_column,
        double min_lat, double max_lat,
        double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt,
        OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt,
        OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt) const
        SWIFT_NAME(objectsWithinBBoxAt(generation:table:geoColumn:minLat:maxLat:minLon:maxLon:where:orderBy:limit:offset:groupBy:)) { return sealed_generation([&] { return impl().objects_within_bbox_at(generation_id, table_name, geo_column,
                                             min_lat, max_lat, min_lon, max_lon,
                                             where_clause, order_by, limit, offset, group_by); }); }
    std::vector<managed<swift_dynamic_object>> objects_within_bbox_shape_at(
        uint64_t generation_id,
        const std::string& table_name, const std::string& geo_column,
        double min_lat, double max_lat, double min_lon, double max_lon,
        OptionalString where_clause = std::nullopt, OptionalString order_by = std::nullopt,
        OptionalInt64 limit = std::nullopt, OptionalInt64 offset = std::nullopt,
        OptionalString group_by = std::nullopt, OptionalString distinct_by = std::nullopt) const
        SWIFT_NAME(objectsWithinBBoxShapeAt(generation:table:geoColumn:minLat:maxLat:minLon:maxLon:where:orderBy:limit:offset:groupBy:distinctBy:)) { return sealed_generation([&] { return impl().objects_within_bbox_shape_at(generation_id, table_name, geo_column,
            min_lat, max_lat, min_lon, max_lon, where_clause, order_by, limit, offset, group_by, distinct_by); }); }
    std::vector<int64_t> query_ids_at(const std::string& table_name,
                                      OptionalString where_clause = std::nullopt,
                                      OptionalString order_by = std::nullopt,
                                      const ColumnValueVector& params = {}) const { return sealed_generation([&] { return impl().query_ids_at(table_name, where_clause, order_by, params); }); }
    int64_t data_version() const { return sealed_or([&] { return impl().data_version(); }, int64_t(-1)); }
    /// Per-thread stale flag for the generation reads above — see
    /// swift_lattice::last_generation_read_stale(). Instance-shaped for
    /// call-site convenience; the flag itself is thread-local.
    bool last_generation_read_stale() const
        SWIFT_NAME(lastGenerationReadStale()) {
        return swift_lattice::last_generation_read_stale();
    }

    // INTERNAL C++ lookup: the shared db wrapper for a raw lattice_db*
    // (nullptr when absent/not cached). dynamic_object holds this shared_ptr
    // directly; the *_ref::getLattice accessors combine it with `_make` to mint
    // a Swift-facing handle on demand (FRT: fresh unretained heap ref that
    // Swift owns and frees; value path: a by-value copy).
    static std::shared_ptr<swift_lattice> shared_for_lattice(lattice_db* lattice) {
        if (!lattice)
            return nullptr;
        return detail::LatticeCache::instance().get_by_pointer(static_cast<swift_lattice*>(lattice));
    }

private:
    // Kept private and unconditional: the separate fixture SPI uses the
    // existing swift_lattice_ref friendship without exposing native custody.
    static bool automatic_fixture_owns_creator(
        const std::shared_ptr<swift_lattice>& retained) noexcept {
        return retained && retained->owns_creator_process();
    }
    swift_lattice_ref() = default;

    std::shared_ptr<swift_lattice> impl_;
#if LATTICE_HAS_FRT
    std::atomic<int> ref_count_{0};
#endif

    // Delegate to LatticeCache - template to preserve config type
    static std::shared_ptr<swift_lattice> get_or_create_shared(const swift_configuration& config, const SchemaVector& schemas) {
        return detail::LatticeCache::instance().get_or_create(config, schemas);
    }
#if LATTICE_HAS_FRT
} SWIFT_SHARED_REFERENCE(retainSwiftLatticeRef, releaseSwiftLatticeRef);
#else
};
#endif
#undef LATTICE_SLREF_RET
#undef LATTICE_SLREF_UNRETAINED

#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
// Test-only type, separate from swift_lattice_ref: enabling this SPI for the
// test importer changes neither the production class definition nor layout.
// All access still uses the real retained creator and sealed callback fence.
class automatic_read_maintenance_fixture_access {
public:
    static automatic_read_maintenance_fixture_token pause_automatic_maintenance(
        const swift_lattice_ref& ref) SWIFT_NAME(pauseAutomaticMaintenance(ref:)) {
        return sealed([&] {
            auto retained = ref.impl_;
            if (!retained || !swift_lattice_ref::automatic_fixture_owns_creator(retained) || retained->is_closed())
                throw db_error("automatic maintenance pause requires its live creator");
            retained->require_catalog_callback_absent();
            auto hold = detail::recovery_refresh_test_access::pause_automatic_maintenance(*retained);
            automatic_read_maintenance_fixture_token result(std::move(hold));
            result.busy_ = !result.control_; // Native null means actual running only.
            return result;
        });
    }
    static automatic_read_maintenance_fixture_snapshot automatic_maintenance_snapshot(
        const swift_lattice_ref& ref) SWIFT_NAME(automaticMaintenanceSnapshot(ref:)) {
        return sealed([&] {
            auto retained = ref.impl_;
            if (!retained || !swift_lattice_ref::automatic_fixture_owns_creator(retained))
                throw db_error("automatic maintenance observation requires its creator");
            retained->require_catalog_callback_absent();
            const auto actual = detail::recovery_refresh_test_access::automatic_snapshot(*retained);
            automatic_read_maintenance_fixture_snapshot result;
            result.available = true;
            result.enabled = actual.enabled;
            result.running = actual.running;
            result.pause_count = actual.pause_count;
            result.attempts = actual.attempts;
            result.quiet_returns = actual.quiet_returns;
            result.deferred = actual.deferred;
            result.completed = actual.completed;
            return result;
        });
    }
    static automatic_read_maintenance_fixture_token fail_next_automatic_enrollment()
        SWIFT_NAME(failNextAutomaticEnrollment()) {
        return sealed([&] {
            return automatic_read_maintenance_fixture_token(
                detail::recovery_refresh_test_access::fail_next_automatic_enrollment());
        });
    }
};
#endif

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

/// Find all rows in `child_table` whose single-link `link_property` points at
/// the parent with `parent_global_id` (FK-to-List backfill helper). Fills a
/// thread-local result buffer and returns the match count. Only valid during
/// a migration callback. Retrieve results with migration_take_backlink_result.
int64_t migration_lookup_backlinks(const std::string& child_table,
                                   const std::string& parent_table,
                                   const std::string& link_property,
                                   const std::string& parent_global_id)
    SWIFT_NAME(migrationLookupBacklinks(childTable:parentTable:linkProperty:parentGlobalId:));

/// Take the result of the last successful migration_lookup call.
/// When no lookup result is available the FRT path returns nullptr (imports as
/// nil) and the value path returns an empty by-value ref (isValid()==false) —
/// Swift normalizes both through `_optRef`.
#if LATTICE_HAS_FRT
dynamic_object_ref* migration_take_lookup_result()
    SWIFT_NAME(migrationTakeLookupResult()) SWIFT_RETURNS_UNRETAINED;

/// Take result `index` from the last migration_lookup_backlinks call.
/// Returns a wrapped empty ref when index is out of range — Swift
/// normalizes through `_optRef` (same contract as the value path).
dynamic_object_ref* migration_take_backlink_result(int64_t index)
    SWIFT_NAME(migrationTakeBacklinkResult(at:)) SWIFT_RETURNS_UNRETAINED;

/// Get the old row ref during a row migration callback.
/// Only valid inside a setRowMigrationCallback callback.
dynamic_object_ref* migration_get_old_row()
    SWIFT_NAME(migrationGetOldRow()) SWIFT_RETURNS_UNRETAINED;

/// Get the new row ref during a row migration callback.
/// Only valid inside a setRowMigrationCallback callback.
dynamic_object_ref* migration_get_new_row()
    SWIFT_NAME(migrationGetNewRow()) SWIFT_RETURNS_UNRETAINED;
#else
dynamic_object_ref migration_take_lookup_result()
    SWIFT_NAME(migrationTakeLookupResult());

dynamic_object_ref migration_take_backlink_result(int64_t index)
    SWIFT_NAME(migrationTakeBacklinkResult(at:));

dynamic_object_ref migration_get_old_row()
    SWIFT_NAME(migrationGetOldRow());

dynamic_object_ref migration_get_new_row()
    SWIFT_NAME(migrationGetNewRow());
#endif

/// The schema version step currently being migrated (the incremental loop
/// walks one version at a time). Only valid inside a row migration callback;
/// returns 0 outside one. Swift uses this to dispatch the row to the correct
/// version's Migration instead of the final target's.
int migration_get_current_version()
    SWIFT_NAME(migrationGetCurrentVersion());

} // namespace lattice

namespace swift_lattice = lattice;

#endif // __cplusplus
