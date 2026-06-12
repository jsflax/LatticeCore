#ifndef geo_bounds_hpp
#define geo_bounds_hpp

#ifdef __cplusplus

#include <LatticeCore.hpp>
#include <bridging.hpp>
#include <vector>
#include <optional>
#include <atomic>

namespace lattice {
class geo_bounds_ref;
class geo_bounds_list_ref;
}

// Forward declarations for Swift shared reference
void retainGeoBoundsRef(lattice::geo_bounds_ref* p);
void releaseGeoBoundsRef(lattice::geo_bounds_ref* p);
void retainGeoBoundsListRef(lattice::geo_bounds_list_ref* p);
void releaseGeoBoundsListRef(lattice::geo_bounds_list_ref* p);

namespace lattice {

class swift_lattice_ref;

// MARK: - GeoBounds Ref
// Reference-counted wrapper for Swift interop
class geo_bounds_ref {
public:
    using ManagedType = geo_bounds;
    
    // (No static factories: every producer — Swift's asRefType and the C++
    // element proxies — uses the converting constructors below.)

    geo_bounds_ref(managed<geo_bounds>& o) {
        impl_ = std::make_shared<geo_bounds>(o);
    }
    
    geo_bounds_ref(const geo_bounds& o) {
        impl_ = std::make_shared<geo_bounds>(o);
    }
    // Access the underlying dynamic_object
    geo_bounds* get() { return impl_.get(); }
    const geo_bounds* get() const { return impl_.get(); }
    
    // Get the shared_ptr (for storing in link_values)
    std::shared_ptr<geo_bounds> shared() const { return impl_; }

    // Whether this ref backs a real geo_bounds (cross-path non-null signal,
    // same contract as the rest of the *_ref family).
    bool valid() const SWIFT_NAME(isValid()) { return impl_ != nullptr; }

#if LATTICE_HAS_FRT
    // For SWIFT_SHARED_REFERENCE
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }
#endif

private:
    geo_bounds_ref() = default;

    friend struct dynamic_object;
    friend struct swift_lattice;
    friend struct geo_bounds_list;
    std::shared_ptr<geo_bounds> impl_;
#if LATTICE_HAS_FRT
    std::atomic<int> ref_count_{0};

    friend void ::retainGeoBoundsRef(lattice::geo_bounds_ref* p);
    friend void ::releaseGeoBoundsRef(lattice::geo_bounds_ref* p);
} SWIFT_SHARED_REFERENCE(retainGeoBoundsRef, releaseGeoBoundsRef);
#else
};
#endif

// MARK: - GeoBounds List
// List of geo_bounds with managed/unmanaged union (like link_list for dynamic objects)
struct geo_bounds_list {
    swift_lattice* lattice = nullptr;

    geo_bounds_list() : lattice(nullptr) {
        new (&unmanaged_) std::vector<geo_bounds>();
    }

    ~geo_bounds_list() {
        if (lattice) {
            managed_.~managed();
        } else {
            unmanaged_.~vector();
        }
    }

    geo_bounds_list(const geo_bounds_list& o) : lattice(o.lattice) {
        if (lattice) {
            new (&managed_) managed<std::vector<geo_bounds*>>(o.managed_);
        } else {
            new (&unmanaged_) std::vector<geo_bounds>(o.unmanaged_);
        }
    }

    geo_bounds_list(geo_bounds_list&& o) : lattice(o.lattice) {
        if (lattice) {
            new (&managed_) managed<std::vector<geo_bounds*>>(std::move(o.managed_));
        } else {
            new (&unmanaged_) std::vector<geo_bounds>(std::move(o.unmanaged_));
        }
    }

    geo_bounds_list& operator=(const geo_bounds_list& o) {
        if (this != &o) {
            if (lattice) {
                managed_.~managed();
            } else {
                unmanaged_.~vector();
            }
            lattice = o.lattice;
            if (lattice) {
                new (&managed_) managed<std::vector<geo_bounds*>>(o.managed_);
            } else {
                new (&unmanaged_) std::vector<geo_bounds>(o.unmanaged_);
            }
        }
        return *this;
    }

    geo_bounds_list& operator=(geo_bounds_list&& o) {
        if (this != &o) {
            if (lattice) {
                managed_.~managed();
            } else {
                unmanaged_.~vector();
            }
            lattice = o.lattice;
            if (lattice) {
                new (&managed_) managed<std::vector<geo_bounds*>>(std::move(o.managed_));
            } else {
                new (&unmanaged_) std::vector<geo_bounds>(std::move(o.unmanaged_));
            }
        }
        return *this;
    }

    geo_bounds_list(const std::vector<geo_bounds>& o) : lattice(nullptr) {
        new (&unmanaged_) std::vector<geo_bounds>(o);
    }

    geo_bounds_list(const managed<std::vector<geo_bounds*>>& o);

    // Proxy for element access with assignment support
    struct element_proxy {
#if LATTICE_HAS_FRT
        using RefType = geo_bounds_ref*;
#else
        using RefType = geo_bounds_ref;
#endif

        geo_bounds value;
        size_t idx;
        geo_bounds_list* list;

        // Assignment updates the list
        element_proxy& operator=(const geo_bounds& bounds);

        // For Swift protocol conformance
#if LATTICE_HAS_FRT
        void assign(geo_bounds_ref* ref) SWIFT_NAME(assign(_:));
#else
        void assign(const geo_bounds_ref& ref) SWIFT_NAME(assign(_:));
#endif
        RefType getObjectRef() const SWIFT_COMPUTED_PROPERTY;

        // Access the underlying geo_bounds
        geo_bounds* operator->() { return &value; }
        const geo_bounds* operator->() const { return &value; }
        operator geo_bounds&() { return value; }
        operator const geo_bounds&() const { return value; }

        // Convenience accessors
        double min_lat() const { return value.min_lat; }
        double max_lat() const { return value.max_lat; }
        double min_lon() const { return value.min_lon; }
        double max_lon() const { return value.max_lon; }
        double center_lat() const { return value.center_lat(); }
        double center_lon() const { return value.center_lon(); }
    };

    // Element access with proxy for assignment
    element_proxy operator[](size_t idx) const;

    // Capacity
    size_t size() const;
    bool empty() const;

    // Modifiers
    void push_back(const geo_bounds& bounds);
    void push_back(geo_bounds_ref* ref);
    void erase(size_t idx);
    void clear();

    // Set element at index
    void set(size_t idx, const geo_bounds& bounds);

    // Find index of a geo_bounds by coordinate comparison
    std::optional<size_t> find_index(const geo_bounds& bounds) const;

    // Find all elements matching SQL predicate (managed lists only)
    // Example: find_where("minLat > 37.0 AND maxLat < 38.0")
    std::vector<size_t> find_where(const std::string& sql_predicate) const;

    // Get the link table name — empty for unmanaged lists
    std::string get_link_table_name() const SWIFT_NAME(getLinkTableName()) {
        if (lattice) {
            return managed_.list_table_;
        }
        return "";
    }

private:
    union {
        std::vector<geo_bounds> unmanaged_;
        managed<std::vector<geo_bounds*>> managed_;
    };

    friend class swift_lattice;
    friend class geo_bounds_list_ref;
};

// MARK: - GeoBounds List Ref
// Reference-counted wrapper for Swift interop
class geo_bounds_list_ref final {
public:
    using ElementProxy = geo_bounds_list::element_proxy;
    // Single-source factories (same pattern as swift_lattice_ref): FRT path
    // heap-allocates; the iOS-15 value path returns by value — the inner
    // shared_ptr carries ownership either way.
#if LATTICE_HAS_FRT
    using RefType = geo_bounds_ref*;
#  define LATTICE_GBLREF_RET geo_bounds_list_ref*
    static geo_bounds_list_ref* _make(std::shared_ptr<geo_bounds_list> impl) {
        auto ref = new geo_bounds_list_ref();
        ref->impl_ = impl;
        return ref;
    }
#else
    using RefType = geo_bounds_ref;
#  define LATTICE_GBLREF_RET geo_bounds_list_ref
    static geo_bounds_list_ref _make(std::shared_ptr<geo_bounds_list> impl) {
        geo_bounds_list_ref ref;
        ref.impl_ = impl;
        return ref;
    }
#endif

    static LATTICE_GBLREF_RET create() {
        return _make(std::make_shared<geo_bounds_list>());
    }

    static LATTICE_GBLREF_RET wrap(std::shared_ptr<geo_bounds_list> list) {
        return _make(list);
    }

    static LATTICE_GBLREF_RET create(const managed<std::vector<geo_bounds*>>& m) {
        return _make(std::make_shared<geo_bounds_list>(m));
    }
#undef LATTICE_GBLREF_RET

    geo_bounds_list* get() { return impl_.get(); }
    const geo_bounds_list* get() const { return impl_.get(); }

    std::shared_ptr<geo_bounds_list> shared() const { return impl_; }

#if LATTICE_HAS_FRT
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }
#endif

#if LATTICE_HAS_FRT
    // Fresh unretained heap ref per access; Swift retains and frees it.
    swift_lattice_ref* getLattice() const SWIFT_COMPUTED_PROPERTY SWIFT_RETURNS_UNRETAINED;
#else
    // Value-type path: by value (see dynamic_object_ref::getLattice) — like
    // every handle in the family, geo_bounds_list_ref compiles as a copyable
    // value type below the FRT floor, and a pointer return would import as
    // UnsafeMutablePointer.
    swift_lattice_ref getLattice() const SWIFT_COMPUTED_PROPERTY;
#endif

    std::string getLinkTableName() const SWIFT_COMPUTED_PROPERTY {
        return impl_->get_link_table_name();
    }

    size_t size() const { return impl_->size(); }
    bool empty() const { return impl_->empty(); }

    geo_bounds_list::element_proxy operator[](size_t idx) const {
        return (*impl_)[idx];
    }

    // const (shallow): these mutate the pointee through the shared_ptr, so on
    // the value path they import non-mutating and are callable on a `let`.
    void push_back(const geo_bounds& bounds) const {
        impl_->push_back(bounds);
    }

#if LATTICE_HAS_FRT
    void push_back(geo_bounds_ref* ref) const SWIFT_NAME(pushBack(_:)) {
        impl_->push_back(ref);
    }
#else
    void push_back(const geo_bounds_ref& ref) const SWIFT_NAME(pushBack(_:)) {
        impl_->push_back(*ref.get());
    }
#endif

    void erase(size_t idx) const {
        impl_->erase(idx);
    }

    void clear() const {
        impl_->clear();
    }

    void set(size_t idx, const geo_bounds& bounds) const {
        impl_->set(idx, bounds);
    }

    // Find index by coordinate comparison
    std::optional<size_t> find_index(const geo_bounds_ref& ref) const SWIFT_NAME(findIndex(_:)) {
        if (ref.get()) {
            return impl_->find_index(*ref.get());
        }
        return std::nullopt;
    }

    // Find all elements matching SQL predicate (managed lists only)
    std::vector<size_t> find_where(const std::string& sql_predicate) const SWIFT_NAME(findWhere(_:)) {
        return impl_->find_where(sql_predicate);
    }

private:
    geo_bounds_list_ref() = default;

    std::shared_ptr<geo_bounds_list> impl_;
#if LATTICE_HAS_FRT
    std::atomic<int> ref_count_{0};

    friend void ::retainGeoBoundsListRef(lattice::geo_bounds_list_ref* p);
    friend void ::releaseGeoBoundsListRef(lattice::geo_bounds_list_ref* p);
} SWIFT_SHARED_REFERENCE(retainGeoBoundsListRef, releaseGeoBoundsListRef);
#else
};
#endif

}

#endif // __cplusplus

#endif
