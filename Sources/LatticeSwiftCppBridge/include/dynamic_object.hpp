#ifndef dynamic_object_hpp
#define dynamic_object_hpp

#ifdef __cplusplus

#include <bridging.hpp>
#include <string>
#include <atomic>
#include <memory>
#include <unmanaged_object.hpp>
#include <managed_object.hpp>
#include <LatticeCore.hpp>


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
    
    dynamic_object(const managed<swift_dynamic_object>& o) : lattice(nullptr) {
        new (&managed_) managed<swift_dynamic_object>(o);
        lattice = managed_.lattice_shared();
    }

    dynamic_object(const managed<swift_dynamic_object*>& o) : lattice(nullptr) {
        new (&managed_) managed<swift_dynamic_object>(*o.get_value());
        lattice = managed_.lattice_shared();
    }
    
    bool has_value(const std::string& name) const SWIFT_NAME(hasValue(named:)) {
        if (lattice) {
            return managed_.has_value(name);
        } else {
            return unmanaged_.has_value(name);
        }
        return true;
    }
    
    template <typename T>
    T get_field(const std::string& name) const {
        if (lattice) {
            return managed_.get_managed_field<T>(name);
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
            field = value;
        } else {
            unmanaged_.set(name, value);
        }
    }
    
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
        } else {
            unmanaged_.set_nil(name);
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
    
    std::string debug_description() const;
    
    std::shared_ptr<dynamic_object> make_shared() const { return std::make_shared<dynamic_object>(*this); }

    std::string get_table_name() const SWIFT_NAME(getTableName()) {
        if (lattice) {
            return managed_.table_name_;
        } else {
            return unmanaged_.table_name;
        }
    }

private:
    union {
        swift_dynamic_object unmanaged_;
        managed<swift_dynamic_object> managed_;
    };

    friend class swift_lattice;
    friend struct link_list;
    friend class dynamic_object_ref;
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

    // Delegate common operations to impl_
    bool has_value(const std::string& name) const SWIFT_NAME(hasValue(named:)) {
        return impl_->has_value(name);
    }

    int64_t get_int(const std::string& name) const SWIFT_NAME(getInt(named:)) {
        return impl_->get_int(name);
    }

    std::string get_string(const std::string& name) const SWIFT_NAME(getString(named:)) {
        return impl_->get_string(name);
    }

    bool get_bool(const std::string& name) const SWIFT_NAME(getBool(named:)) {
        return impl_->get_bool(name);
    }

    std::vector<uint8_t> get_data(const std::string& name) const SWIFT_NAME(getData(named:)) {
        return impl_->get_data(name);
    }

    double get_double(const std::string& name) const SWIFT_NAME(getDouble(named:)) {
        return impl_->get_double(name);
    }

    float get_float(const std::string& name) const SWIFT_NAME(getFloat(named:)) {
        return impl_->get_float(name);
    }

    void set_int(const std::string& name, int64_t value) const SWIFT_NAME(setInt(named:_:)) {
        impl_->set_int(name, value);
    }

    void set_string(const std::string& name, const std::string& value) const SWIFT_NAME(setString(named:_:)) {
        impl_->set_string(name, value);
    }

    void set_bool(const std::string& name, bool value) const SWIFT_NAME(setBool(named:_:)) {
        impl_->set_bool(name, value);
    }

    void set_data(const std::string& name, const std::vector<uint8_t>& value) const SWIFT_NAME(setData(named:_:)) {
        impl_->set_data(name, value);
    }

    void set_double(const std::string& name, double value) const SWIFT_NAME(setDouble(named:_:)) {
        impl_->set_double(name, value);
    }

    void set_float(const std::string& name, float value) const SWIFT_NAME(setFloat(named:_:)) {
        impl_->set_float(name, value);
    }

    void set_nil(const std::string& name) const SWIFT_NAME(setNil(named:)) {
        impl_->set_nil(name);
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
        return impl_->get_geo_bounds(name);
    }

    void set_geo_bounds(const std::string& name, const geo_bounds& value) const SWIFT_NAME(setGeoBounds(named:_:)) {
        impl_->set_geo_bounds(name, value);
    }

    void set_geo_bounds(const std::string& name, double minLat, double maxLat, double minLon, double maxLon) const SWIFT_NAME(setGeoBounds(named:minLat:maxLat:minLon:maxLon:)) {
        impl_->set_geo_bounds(name, minLat, maxLat, minLon, maxLon);
    }

    bool has_geo_bounds(const std::string& name) const SWIFT_NAME(hasGeoBounds(named:)) {
        return impl_->has_geo_bounds(name);
    }


    void remove_geo_bounds_at(const std::string& name, size_t index) const SWIFT_NAME(removeGeoBounds(named:at:)) {
        impl_->remove_geo_bounds_at(name, index);
    }

    std::string get_table_name() const SWIFT_NAME(getTableName()) {
        return impl_->get_table_name();
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

private:
    dynamic_object_ref() = default;

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
