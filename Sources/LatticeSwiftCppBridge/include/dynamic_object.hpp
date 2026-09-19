#ifndef dynamic_object_hpp
#define dynamic_object_hpp

#ifdef __cplusplus

#include <bridging.hpp>
#include <string>
#include <atomic>
#include <memory>
#include <set>
#include <unmanaged_object.hpp>
#include <managed_object.hpp>
#include <LatticeCore.hpp>
#include <error.hpp>


namespace lattice {
    class dynamic_object_ref;
    class geo_bounds_list_ref;
    struct union_value;
}
// Forward declarations for Swift shared reference
void retainDynamicObjectRef(lattice::dynamic_object_ref* p);
void releaseDynamicObjectRef(lattice::dynamic_object_ref* p);

namespace lattice {

struct link_list;
class link_list_ref;
class swift_lattice_ref;
class swift_lattice;
class dynamic_object_ref;

// MARK: Dynamic Object
struct SWIFT_CONFORMS_TO_PROTOCOL(Lattice.CxxObject) dynamic_object {
    // Shared handle to the owning db wrapper; doubles as the managed/unmanaged
    // discriminant (null == unmanaged). Holding the shared_ptr directly (rather
    // than a heap swift_lattice_ref*) keeps the db alive with no per-object
    // allocation to free; Swift-facing refs are minted on demand in getLattice.
    std::shared_ptr<swift_lattice> lattice;
    bool deleted_ = false;

    dynamic_object() : lattice(nullptr) {
        new (&unmanaged_) swift_dynamic_object();
    }
    
    ~dynamic_object() {
        if (lattice) {
            managed_.~managed();
        } else {
            unmanaged_.~swift_dynamic_object();
        }
    }
    
    dynamic_object(const dynamic_object& o);
    
    dynamic_object(dynamic_object&& o) : lattice(o.lattice) {
        if (lattice) {
            new (&managed_) managed<swift_dynamic_object>(std::move(o.managed_));
        } else {
            new (&unmanaged_) swift_dynamic_object(std::move(o.unmanaged_));
        }
    }
    
    dynamic_object& operator=(const dynamic_object& o);
    
    dynamic_object& operator=(dynamic_object&& o) {
        if (this != &o) {
            // Destroy current
            if (lattice) {
                managed_.~managed();
            } else {
                unmanaged_.~swift_dynamic_object();
            }
            // Move new
            lattice = o.lattice;
            if (lattice) {
                new (&managed_) managed<swift_dynamic_object>(std::move(o.managed_));
            } else {
                new (&unmanaged_) swift_dynamic_object(std::move(o.unmanaged_));
            }
        }
        return *this;
    }
    
    dynamic_object(const swift_dynamic_object& o) : lattice(nullptr) {
        new (&unmanaged_) swift_dynamic_object(o);
    }

    // Construct default unmanaged storage in its final owning object. The
    // existing constructor still seeds every schema-defined default/list.
    dynamic_object(const std::string& table,
                   const std::unordered_map<std::string, property_descriptor>& props)
        : lattice(nullptr) {
        new (&unmanaged_) swift_dynamic_object(table, props);
    }
    
    dynamic_object(const managed<swift_dynamic_object>& o) : lattice(nullptr) {
        new (&managed_) managed<swift_dynamic_object>(o);
        lattice = managed_.lattice_shared();
    }

    dynamic_object(const managed<swift_dynamic_object*>& o) : lattice(nullptr) {
        new (&managed_) managed<swift_dynamic_object>(*o.get_value());
        lattice = managed_.lattice_shared();
    }

    /// Identity captured when this handle was bound, without reading the row
    /// or changing materialized-read mode. Zero means unmanaged. This is not
    /// an existence check: deleting the row does not erase a held handle's id.
    int64_t managed_primary_key() const noexcept SWIFT_NAME(managedPrimaryKey()) {
        return lattice ? managed_.id_ : 0;
    }

    /// Collection-query metadata, independent of live and materialized reads.
    /// An image survives writes/deletes unchanged until explicitly released.
    bool has_query_row_image() const noexcept SWIFT_NAME(hasQueryRowImage()) {
        return lattice && managed_.query_row_image_ != nullptr;
    }

    /// Exact stored type: -1 = absent, 0 = SQL NULL, 1 = Int64, 2 = Double,
    /// 3 = String, 4 = blob. Missing metadata never falls back to a SQL read.
    int32_t query_row_value_type(const std::string& name) const
        SWIFT_NAME(queryRowValueType(named:)) {
        if (!has_query_row_image()) return -1;
        const auto& image = *managed_.query_row_image_;
        auto it = image.find(name);
        return it == image.end() ? -1 : static_cast<int32_t>(it->second.index());
    }

    /// Copy one value out of the immutable query image. Check the type first
    /// to distinguish absence from SQL NULL, and the bridge error immediately
    /// afterward for allocation failure. No live-read fallback or cache change.
    column_value_t query_row_value(const std::string& name) const
        SWIFT_NAME(queryRowValue(named:)) {
        return sealed([&]() -> column_value_t {
            if (!has_query_row_image()) return nullptr;
            const auto& image = *managed_.query_row_image_;
            auto it = image.find(name);
            return it == image.end() ? column_value_t{nullptr} : it->second;
        });
    }

    /// Drop only this handle's query metadata; keep its field/cache semantics.
    void release_query_row_image() noexcept SWIFT_NAME(releaseQueryRowImage()) {
        if (lattice) managed_.query_row_image_.reset();
    }
    
    // ------------------------------------------------------------------
    // Row cache (materialized reads)
    //
    // Every query path already hydrates the full row into
    // managed_.source.values and then throws it away: each property get
    // minted a fresh managed<T> and ran its own `SELECT col WHERE id=?`
    // (~10-14 statements per object; Engram's recall paid ~300 statements
    // per call, each scanning a WAL bloated by the sync daemon — 228s
    // observed). Materialized mode serves gets from the hydrated snapshot.
    //
    // Contract:
    // - Opt-in; default reads preserve live per-column semantics.
    // - A materialized object is a read SNAPSHOT as of hydration/refresh;
    //   concurrent writers are invisible until refreshRowCache().
    // - Fail-safe: any miss or variant/type mismatch falls through to the
    //   live per-column read — slower, never wrong.
    // - nullptr_t in the snapshot means KNOWN NULL (get falls through to the
    //   live path's NULL convention; hasValue returns false); an ABSENT key
    //   falls through entirely.
    // - Writes are write-through and update the snapshot with exactly the
    //   stored form (bool→int64 0/1, float→double — mirroring
    //   managed<T>::operator=), so read-your-writes holds.
    // - Links/lists/unions always use the live path (v1).
    // ------------------------------------------------------------------

    void enable_row_cache() SWIFT_NAME(enableRowCache()) {
        if (!lattice) return;  // unmanaged objects are already value snapshots
        if (managed_.source.values.empty()) refresh_row_cache();
        row_cache_enabled_ = true;
    }
    void disable_row_cache() SWIFT_NAME(disableRowCache()) { row_cache_enabled_ = false; }
    bool is_row_cache_enabled() const SWIFT_NAME(isRowCacheEnabled()) { return row_cache_enabled_; }
    /// Re-fetch the full row in ONE statement (the staleness escape hatch).
    void refresh_row_cache() SWIFT_NAME(refreshRowCache());

    bool has_value(const std::string& name) const SWIFT_NAME(hasValue(named:)) {
        if (lattice) {
            if (row_cache_enabled_) {
                // "id" is deliberately absent from the hydrated values (it
                // must never ride a write); serve it from the handle itself.
                if (name == "id") return managed_.id_ != 0;
                auto it = managed_.source.values.find(name);
                if (it != managed_.source.values.end()) {
                    return !std::holds_alternative<std::nullptr_t>(it->second);
                }
                // absent → fall through to the live check
            }
            return managed_.has_value(name);
        } else {
            return unmanaged_.has_value(name);
        }
        return true;
    }

    template <typename T>
    T get_field(const std::string& name) const {
        if (lattice) {
            if (row_cache_enabled_) {
                // Serve the primary key from the handle's own id_ member —
                // it is excluded from the hydrated values by design (so it
                // can never leak into a write), which otherwise made every
                // `primaryKey` read (e.g. inside detached()) fall through to
                // a live SELECT.
                if constexpr (std::is_same_v<T, int64_t>) {
                    if (name == "id") return managed_.id_;
                }
                auto it = managed_.source.values.find(name);
                if (it != managed_.source.values.end()) {
                    if (auto* v = std::get_if<T>(&it->second)) return *v;
                    // KNOWN NULL or stored-type mismatch → live path decides
                    // (NULL convention, affinity coercion) — never guess here.
                }
            }
            if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, double> ||
                          std::is_same_v<T, std::string>) {
                return managed_.read_live_scalar<T>(name);
            } else {
                return managed_.get_managed_field<T>(name);
            }
        } else {
            auto value = unmanaged_.get(name);
            return *std::get_if<T>(&value);
        }
    }
    
    int64_t get_int(const std::string& name) const SWIFT_NAME(getInt(named:)) {
        return get_field<int64_t>(name);
    }
    std::string get_string(const std::string& name) const SWIFT_NAME(getString(named:)) {
        return get_field<std::string>(name);
    }
    
    bool get_bool(const std::string& name) const SWIFT_NAME(getBool(named:)) {
        return get_field<int64_t>(name);
    }
    
    std::vector<uint8_t> get_data(const std::string& name) const SWIFT_NAME(getData(named:)) {
        return get_field<std::vector<uint8_t>>(name);
    }
    
    double get_double(const std::string& name) const SWIFT_NAME(getDouble(named:)) {
        return get_field<double>(name);
    }
    
    float get_float(const std::string& name) const SWIFT_NAME(getFloat(named:)) {
        return get_field<double>(name);
    }
    
    dynamic_object get_object(const std::string& name) const SWIFT_NAME(getObject(named:)) SWIFT_RETURNS_INDEPENDENT_VALUE;
    
#if LATTICE_HAS_FRT
    link_list_ref* get_link_list(const std::string& name) const SWIFT_NAME(getLinkList(named:)) SWIFT_RETURNS_UNRETAINED;
    geo_bounds_list_ref* get_geo_bounds_list(const std::string& name) const SWIFT_NAME(getGeoBoundsList(named:)) SWIFT_RETURNS_UNRETAINED;
#else
    link_list_ref get_link_list(const std::string& name) const SWIFT_NAME(getLinkList(named:));
    geo_bounds_list_ref get_geo_bounds_list(const std::string& name) const SWIFT_NAME(getGeoBoundsList(named:));
#endif

    template <typename T>
    void set_field(const std::string& name, const T& value) {
        if (lattice) {
            managed<T> field = managed_.get_managed_field<T>(name);
            field = value;  // DB write first — a throw must not poison the cache
            update_row_cache_value(name, value);
        } else {
            unmanaged_.set(name, value);
        }
    }

private:
    /// Write-through mirror of managed<T>::operator='s stored forms.
    /// Updated UNCONDITIONALLY (cache on or off): the hydrated snapshot must
    /// never go stale from this instance's own writes. Types the variant
    /// cannot represent drop the cached key so reads fall through live.
    template <typename T>
    void update_row_cache_value(const std::string& name, const T& value) {
        if constexpr (std::is_same_v<T, bool>) {
            managed_.source.values[name] = value ? int64_t{1} : int64_t{0};
        } else if constexpr (std::is_same_v<T, float>) {
            managed_.source.values[name] = static_cast<double>(value);
        } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, double> ||
                             std::is_same_v<T, std::string> ||
                             std::is_same_v<T, std::vector<uint8_t>>) {
            managed_.source.values[name] = value;
        } else {
            managed_.source.values.erase(name);
        }
    }

public:
    
    void set_int(const std::string& name, int64_t value) SWIFT_NAME(setInt(named:_:));
    void set_string(const std::string& name, const std::string& value) SWIFT_NAME(setString(named:_:)) {
        set_field(name, value);
    }
    void set_bool(const std::string& name, bool value) SWIFT_NAME(setBool(named:_:)) {
        set_field(name, value);
    }
    void set_data(const std::string& name, const std::vector<uint8_t>& value) SWIFT_NAME(setData(named:_:)) {
        set_field(name, value);
    }
    void set_double(const std::string& name, double value) SWIFT_NAME(setDouble(named:_:)) {
        set_field(name, value);
    }
    void set_float(const std::string& name, float value) SWIFT_NAME(setFloat(named:_:)) {
        set_field(name, value);
    }
    void set_nil(const std::string& name) SWIFT_NAME(setNil(named:)) {
        if (lattice) {
            managed_.set_nil(name);
            // Write-through: without this, a cached read after setNil would
            // return the stale pre-nil value — silent wrong-value corruption
            // for optionals (nullptr_t = KNOWN NULL in the cache contract).
            managed_.source.values[name] = nullptr;
        } else {
            unmanaged_.set_nil(name);
        }
    }

    /// SQL-side atomic increment: `SET col = col + delta` under the write
    /// lock — no read-modify-write race, and half the statements of get+set.
    /// Built for access-count bumps (Engram recall): two concurrent recalls
    /// both land their +1 instead of last-writer-wins on a stale read.
    /// `name` is a macro-generated column name, not user input (same
    /// interpolation convention as the query layer's filter clauses).
    void increment_int_field(const std::string& name, int64_t delta) SWIFT_NAME(incrementIntField(named:by:)) {
        if (lattice) {
            auto* db = managed_.db_;
            if (!db) return;
            {
            detail::managed_route_scope route_guard(db, managed_.lattice_, managed_.table_name_,
                managed_.attachment_token_, managed_.attachment_writer_);
            db->execute(
                "UPDATE " + managed_table_sql(managed_.table_name_) + " SET " + name + " = " + name +
                    " + ? WHERE id = ?",
                {delta, managed_.id_});
            }
            // New value unknown here — drop the cached key so the next
            // materialized read falls through live instead of going stale.
            managed_.source.values.erase(name);
        } else {
            auto v = unmanaged_.get(name);
            if (auto* i = std::get_if<int64_t>(&v)) {
                unmanaged_.set(name, *i + delta);
            }
        }
    }
    void set_object(const std::string& name, const dynamic_object_ref& value) SWIFT_NAME(setObject(named:_:));

    // geo_bounds accessors
    geo_bounds get_geo_bounds(const std::string& name) const SWIFT_NAME(getGeoBounds(named:)) {
        if (lattice) {
            return managed_.get_geo_bounds(name);
        } else {
            return unmanaged_.get_geo_bounds(name);
        }
    }

    void set_geo_bounds(const std::string& name, const geo_bounds& value) SWIFT_NAME(setGeoBounds(named:_:)) {
        if (lattice) {
            managed_.set_geo_bounds(name, value);
        } else {
            unmanaged_.set_geo_bounds(name, value);
        }
    }

    void set_geo_bounds(const std::string& name, double minLat, double maxLat, double minLon, double maxLon) SWIFT_NAME(setGeoBounds(named:minLat:maxLat:minLon:maxLon:)) {
        if (lattice) {
            managed_.set_geo_bounds(name, minLat, maxLat, minLon, maxLon);
        } else {
            unmanaged_.set_geo_bounds(name, minLat, maxLat, minLon, maxLon);
        }
    }

    bool has_geo_bounds(const std::string& name) const SWIFT_NAME(hasGeoBounds(named:)) {
        if (lattice) {
            return managed_.has_geo_bounds(name);
        } else {
            return unmanaged_.has_geo_bounds(name);
        }
    }


    geo_bounds get_geo_bounds_at(const std::string& name, size_t index) const SWIFT_NAME(getGeoBounds(named:at:)) {
        if (lattice) {
            return managed_.get_geo_bounds_at(name, index);
        } else {
            return unmanaged_.get_geo_bounds_at(name, index);
        }
    }

    void add_geo_bounds(const std::string& name, const geo_bounds& value) SWIFT_NAME(addGeoBounds(named:_:)) {
        if (lattice) {
            managed_.add_geo_bounds(name, value);
        } else {
            unmanaged_.add_geo_bounds_to_list(name, value);
        }
    }

    void clear_geo_bounds_list(const std::string& name) SWIFT_NAME(clearGeoBoundsList(named:)) {
        if (lattice) {
            managed_.clear_geo_bounds_list(name);
        } else {
            unmanaged_.clear_geo_bounds_list(name);
        }
    }

    void remove_geo_bounds_at(const std::string& name, size_t index) SWIFT_NAME(removeGeoBounds(named:at:)) {
        if (lattice) {
            managed_.remove_geo_bounds_at(name, index);
        } else {
            unmanaged_.remove_geo_bounds_at(name, index);
        }
    }

    // union accessors
    union_value get_union(const std::string& name) const SWIFT_NAME(getUnion(named:));
    void set_union(const std::string& name, const union_value& value) SWIFT_NAME(setUnion(named:_:));

    dynamic_object copy() const {
        return *this;
    }

    void manage(managed<swift_dynamic_object> o);

    /// Object-graph → JSON string. The output contract is PINNED to Swift's
    /// `DynamicObject.jsonObject(maxDepth:)` (lattice repo,
    /// Sources/Lattice/Dynamic/DynamicObject+JSON.swift) so a sibling SDK's
    /// Detached is a one-liner over the same shape — see the full contract
    /// comment above json_walk in dynamic_object.cpp and the C ABI docs for
    /// lattice_object_to_json.
    /// `max_depth` = link hops to follow; <= 0 collapses to-one links to
    /// {"globalId"} stubs and to-many lists to {"count"} summaries.
    std::string to_json(int64_t max_depth) const SWIFT_NAME(toJson(maxDepth:));

    std::string debug_description() const;
    
    std::shared_ptr<dynamic_object> make_shared() const { return std::make_shared<dynamic_object>(*this); }

    std::string get_table_name() const SWIFT_NAME(getTableName()) {
        if (lattice) {
            return managed_.table_name_;
        } else {
            return unmanaged_.table_name;
        }
    }

    /// Logical schema/model name captured at hydration or insertion. Keep it
    /// separate from getTableName(), whose qualified route preserves physical
    /// identity and directs live reads/writes to an attached store.
    std::string get_model_table_name() const SWIFT_NAME(getModelTableName()) {
        return lattice ? managed_.source.table_name : unmanaged_.table_name;
    }

private:
    // to_json internals (dynamic_object.cpp). out_json is a nlohmann::json*
    // passed as void* to keep the JSON dependency out of this header;
    // list_backing reproduces get_link_list without the ref wrapper (the
    // walker needs raw element access on both the FRT and value paths).
    static void json_walk(const dynamic_object& obj, int64_t depth,
                          std::set<std::string>& visited, void* out_json);
    link_list list_backing(const std::string& name) const;

    // Exact hydration already owns the parent through its ref. Avoid a cache
    // lookup (and cache/SQLite lock edge) while the exact writer lease is held.
    dynamic_object(const managed<swift_dynamic_object>& object,
                   std::shared_ptr<swift_lattice> owner) : lattice(std::move(owner)) {
        if (!lattice) throw db_error("exact managed object has no owning lattice");
        new (&managed_) managed<swift_dynamic_object>(object);
    }

    union {
        swift_dynamic_object unmanaged_;
        managed<swift_dynamic_object> managed_;
    };

    // Materialized-read mode (see the row-cache contract above the accessors).
    bool row_cache_enabled_ = false;

    friend class swift_lattice;
    friend struct link_list;
    friend class dynamic_object_ref;
    friend struct managed_attachment_test_access;
};


// MARK: - Dynamic Object Ref
// Reference-counted wrapper for Swift interop
class dynamic_object_ref {
public:
    // Single-source factories (same pattern as swift_lattice_ref): on the FRT
    // path they heap-allocate and Swift imports them as returning the class
    // (optional pointer, unretained); on the iOS-15 value path they return by
    // value — the inner shared_ptr carries ownership either way.
#if LATTICE_HAS_FRT
#  define LATTICE_DOREF_RET dynamic_object_ref*
#  define LATTICE_DOREF_UNRETAINED SWIFT_RETURNS_UNRETAINED
    static dynamic_object_ref* _make(std::shared_ptr<dynamic_object> impl) {
        auto ref = new dynamic_object_ref();
        ref->impl_ = impl;
        return ref;
    }
#else
#  define LATTICE_DOREF_RET dynamic_object_ref
#  define LATTICE_DOREF_UNRETAINED
    static dynamic_object_ref _make(std::shared_ptr<dynamic_object> impl) {
        dynamic_object_ref ref;
        ref.impl_ = impl;
        return ref;
    }
#endif

    static LATTICE_DOREF_RET create() LATTICE_DOREF_UNRETAINED {
        return _make(std::make_shared<dynamic_object>());
    }

    static LATTICE_DOREF_RET create(const std::string& table_name) LATTICE_DOREF_UNRETAINED {
        auto impl = std::make_shared<dynamic_object>();
        impl->unmanaged_.table_name = table_name;
        return _make(impl);
    }

    static LATTICE_DOREF_RET create_unmanaged(
        const std::string& table,
        const std::unordered_map<std::string, property_descriptor>& props)
        SWIFT_NAME(createUnmanaged(table:properties:)) LATTICE_DOREF_UNRETAINED {
        return _make(std::make_shared<dynamic_object>(table, props));
    }

    // Copy a managed row directly into its final owning object. The row is
    // borrowed only during this call; the resulting ref owns its managed copy.
    static LATTICE_DOREF_RET wrap_managed(
        const managed<swift_dynamic_object>& row)
        SWIFT_NAME(wrapManaged(_:)) LATTICE_DOREF_UNRETAINED {
        return _make(std::make_shared<dynamic_object>(row));
    }

    static LATTICE_DOREF_RET wrap(std::shared_ptr<dynamic_object> obj) LATTICE_DOREF_UNRETAINED {
        return _make(obj);
    }

    // Factory that copies the dynamic_object (avoids passing shared_ptr through Swift)
    static LATTICE_DOREF_RET wrap(const dynamic_object& obj) LATTICE_DOREF_UNRETAINED {
        return _make(std::make_shared<dynamic_object>(obj));
    }
#undef LATTICE_DOREF_RET
#undef LATTICE_DOREF_UNRETAINED

    dynamic_object_ref(managed<swift_dynamic_object>& o) {
        impl_ = std::make_shared<dynamic_object>(o);
    }
    
    dynamic_object_ref(const swift_dynamic_object& o) {
        impl_ = std::make_shared<dynamic_object>(o);
    }
    
#if LATTICE_HAS_FRT
    // Mints a fresh Swift-owned ref per access (unretained: Swift retains it
    // and deletes it on release; the object itself keeps the db alive via its
    // shared_ptr member). Defined out-of-line in dynamic_object.cpp where
    // swift_lattice_ref is a complete type. nullptr when unmanaged.
    swift_lattice_ref* getLattice() const SWIFT_COMPUTED_PROPERTY SWIFT_RETURNS_UNRETAINED;
#else
    // Value-type path: return by value (Swift imports the FRT-path pointer as
    // UnsafeMutablePointer otherwise). Defined out-of-line in dynamic_object.cpp
    // where swift_lattice_ref is a complete type. Empty ref (isValid()==false)
    // when there is no backing lattice.
    swift_lattice_ref getLattice() const SWIFT_COMPUTED_PROPERTY;
#endif

    // Access the underlying dynamic_object
    dynamic_object* get() { return impl_.get(); }
    const dynamic_object* get() const { return impl_.get(); }

    // Get the shared_ptr (for storing in link_values)
    std::shared_ptr<dynamic_object> shared() const { return impl_; }

#if LATTICE_HAS_FRT
    // For SWIFT_SHARED_REFERENCE
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }
#endif

    // Delegate common operations to impl_.
    //
    // Every method here is Swift-facing, and field access is NOT memory-only:
    // an uncached read lazily queries the row (managed::detach) and a write
    // executes an UPDATE — either can throw db_error (e.g. transient
    // SQLITE_NOMEM while macOS purges the purgeable page cache), and a C++
    // exception crossing into Swift is std::terminate → SIGTRAP (observed
    // live in Engram.app, Aug 2026). sealed() catches, stashes the message
    // in last_bridge_error() for Swift to bubble, and returns a default.
    // NOT sealed (default-value semantics don't fit their return types;
    // revisit if they ever appear in a crash): get_object, get_union/
    // set_union, get_link_list, get_geo_bounds_list.
    bool has_value(const std::string& name) const SWIFT_NAME(hasValue(named:)) {
        return sealed([&] { return impl_->has_value(name); });
    }

    int64_t get_int(const std::string& name) const SWIFT_NAME(getInt(named:)) {
        return sealed([&] { return impl_->get_int(name); });
    }

    std::string get_string(const std::string& name) const SWIFT_NAME(getString(named:)) {
        return sealed([&] { return impl_->get_string(name); });
    }

    bool get_bool(const std::string& name) const SWIFT_NAME(getBool(named:)) {
        return sealed([&] { return impl_->get_bool(name); });
    }

    std::vector<uint8_t> get_data(const std::string& name) const SWIFT_NAME(getData(named:)) {
        return sealed([&] { return impl_->get_data(name); });
    }

    double get_double(const std::string& name) const SWIFT_NAME(getDouble(named:)) {
        return sealed([&] { return impl_->get_double(name); });
    }

    float get_float(const std::string& name) const SWIFT_NAME(getFloat(named:)) {
        return sealed([&] { return impl_->get_float(name); });
    }

    void set_int(const std::string& name, int64_t value) const SWIFT_NAME(setInt(named:_:)) {
        sealed([&] { impl_->set_int(name, value); });
    }

    void set_string(const std::string& name, const std::string& value) const SWIFT_NAME(setString(named:_:)) {
        sealed([&] { impl_->set_string(name, value); });
    }

    void set_bool(const std::string& name, bool value) const SWIFT_NAME(setBool(named:_:)) {
        sealed([&] { impl_->set_bool(name, value); });
    }

    void set_data(const std::string& name, const std::vector<uint8_t>& value) const SWIFT_NAME(setData(named:_:)) {
        sealed([&] { impl_->set_data(name, value); });
    }

    void set_double(const std::string& name, double value) const SWIFT_NAME(setDouble(named:_:)) {
        sealed([&] { impl_->set_double(name, value); });
    }

    void set_float(const std::string& name, float value) const SWIFT_NAME(setFloat(named:_:)) {
        sealed([&] { impl_->set_float(name, value); });
    }

    void set_nil(const std::string& name) const SWIFT_NAME(setNil(named:)) {
        sealed([&] { impl_->set_nil(name); });
    }

    /// The sealed contract's error slot (see error.hpp): "" if the last
    /// sealed bridge call on THIS THREAD succeeded, else its failure
    /// message. Check immediately after a suspicious defaulted result.
    std::string last_query_error_message() const SWIFT_NAME(lastQueryErrorMessage()) {
        return last_bridge_error();
    }

    // Materialized reads (row cache) — see dynamic_object's contract.
    void enable_row_cache() const SWIFT_NAME(enableRowCache()) { impl_->enable_row_cache(); }
    void disable_row_cache() const SWIFT_NAME(disableRowCache()) { impl_->disable_row_cache(); }
    void refresh_row_cache() const SWIFT_NAME(refreshRowCache()) { impl_->refresh_row_cache(); }
    bool is_row_cache_enabled() const SWIFT_NAME(isRowCacheEnabled()) { return impl_->is_row_cache_enabled(); }
    void increment_int_field(const std::string& name, int64_t delta) const SWIFT_NAME(incrementIntField(named:by:)) {
        sealed([&] { impl_->increment_int_field(name, delta); });
    }

    void set_object(const std::string& name, const dynamic_object_ref& value) const SWIFT_NAME(setObject(named:_:)) {
        impl_->set_object(name, value);
    }

#if LATTICE_HAS_FRT
    dynamic_object_ref* get_object(const std::string& name) const SWIFT_NAME(getObject(named:)) SWIFT_RETURNS_UNRETAINED {
        return dynamic_object_ref::wrap(impl_->get_object(name).make_shared());
    }
#else
    dynamic_object_ref get_object(const std::string& name) const SWIFT_NAME(getObject(named:)) {
        return dynamic_object_ref::wrap(impl_->get_object(name).make_shared());
    }
#endif

    // union accessors (implemented in dynamic_object.cpp — union_value is incomplete here)
    union_value get_union(const std::string& name) const SWIFT_NAME(getUnion(named:));
    void set_union(const std::string& name, const union_value& value) const SWIFT_NAME(setUnion(named:_:));

#if LATTICE_HAS_FRT
    link_list_ref* get_link_list(const std::string& name) const SWIFT_NAME(getLinkList(named:)) SWIFT_RETURNS_UNRETAINED {
        return impl_->get_link_list(name);
    }

    geo_bounds_list_ref* get_geo_bounds_list(const std::string& name) const SWIFT_NAME(getGeoBoundsList(named:)) SWIFT_RETURNS_UNRETAINED {
        return impl_->get_geo_bounds_list(name);
    }
#else
    // Value path: returning by value needs the complete type, which is not
    // available here (forward-declared), so these are defined out-of-line in
    // dynamic_object.cpp.
    link_list_ref get_link_list(const std::string& name) const SWIFT_NAME(getLinkList(named:));
    geo_bounds_list_ref get_geo_bounds_list(const std::string& name) const SWIFT_NAME(getGeoBoundsList(named:));
#endif

    // geo_bounds accessors
    geo_bounds get_geo_bounds(const std::string& name) const SWIFT_NAME(getGeoBounds(named:)) {
        return sealed([&] { return impl_->get_geo_bounds(name); });
    }

    void set_geo_bounds(const std::string& name, const geo_bounds& value) const SWIFT_NAME(setGeoBounds(named:_:)) {
        sealed([&] { impl_->set_geo_bounds(name, value); });
    }

    void set_geo_bounds(const std::string& name, double minLat, double maxLat, double minLon, double maxLon) const SWIFT_NAME(setGeoBounds(named:minLat:maxLat:minLon:maxLon:)) {
        sealed([&] { impl_->set_geo_bounds(name, minLat, maxLat, minLon, maxLon); });
    }

    bool has_geo_bounds(const std::string& name) const SWIFT_NAME(hasGeoBounds(named:)) {
        return sealed([&] { return impl_->has_geo_bounds(name); });
    }


    void remove_geo_bounds_at(const std::string& name, size_t index) const SWIFT_NAME(removeGeoBounds(named:at:)) {
        sealed([&] { impl_->remove_geo_bounds_at(name, index); });
    }

    std::string get_table_name() const SWIFT_NAME(getTableName()) {
        return impl_->get_table_name();
    }

    std::string get_model_table_name() const SWIFT_NAME(getModelTableName()) {
        return impl_->get_model_table_name();
    }

    /// Object-graph → JSON — see dynamic_object::to_json for the pinned contract.
    std::string to_json(int64_t max_depth) const SWIFT_NAME(toJson(maxDepth:)) {
        return impl_->to_json(max_depth);
    }

    std::string debug_description() const {
        return impl_->debug_description();
    }

    // Whether this ref backs an actual object. Used as the cross-path "non-null"
    // signal: below the FRT floor the nullable getters return an *empty* ref
    // (impl_ == nullptr) rather than a null pointer, so callers test isValid()
    // uniformly instead of optional-unwrapping a foreign-reference pointer.
    bool valid() const SWIFT_NAME(isValid()) {
        return impl_ != nullptr;
    }

    // Check if this is a managed (persisted) object
    bool is_managed() const {
        return impl_ != nullptr && impl_->lattice != nullptr;
    }

    /// Statement-free bound identity; zero for an empty or unmanaged ref.
    int64_t managed_primary_key() const noexcept SWIFT_NAME(managedPrimaryKey()) {
        return impl_ ? impl_->managed_primary_key() : 0;
    }

    bool has_query_row_image() const noexcept SWIFT_NAME(hasQueryRowImage()) {
        return impl_ && impl_->has_query_row_image();
    }

    int32_t query_row_value_type(const std::string& name) const
        SWIFT_NAME(queryRowValueType(named:)) {
        return impl_ ? impl_->query_row_value_type(name) : -1;
    }

    column_value_t query_row_value(const std::string& name) const
        SWIFT_NAME(queryRowValue(named:)) {
        if (impl_) return impl_->query_row_value(name);
        last_bridge_error().clear();
        return nullptr;
    }

    void release_query_row_image() const noexcept SWIFT_NAME(releaseQueryRowImage()) {
        if (impl_) impl_->release_query_row_image();
    }

private:
    dynamic_object_ref() = default;

    friend class exact_vector_live_result; // nonallocating legacy failure value
    friend struct dynamic_object;
    friend struct swift_lattice;
    friend struct link_list;
    friend struct union_value;
    std::shared_ptr<dynamic_object> impl_;
#if LATTICE_HAS_FRT
    std::atomic<int> ref_count_{0};

    friend void ::retainDynamicObjectRef(lattice::dynamic_object_ref* p);
    friend void ::releaseDynamicObjectRef(lattice::dynamic_object_ref* p);
} SWIFT_SHARED_REFERENCE(retainDynamicObjectRef, releaseDynamicObjectRef);
#else
};
#endif

}

#endif // __cplusplus

#endif /* dynamic_object.hpp */
