#ifndef geo_bounds_hpp
#define geo_bounds_hpp

#ifdef __cplusplus

#include <LatticeCpp.hpp>
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

struct swift_lattice_ref;

// MARK: - GeoBounds Ref
// Reference-counted wrapper for Swift interop
class geo_bounds_ref {
public:
    using ManagedType = geo_bounds;
    
    // Factory methods for heap allocation
    static geo_bounds_ref* create() {
        auto ref = new geo_bounds_ref();
        ref->impl_ = std::make_shared<geo_bounds>();
        return ref;
    }
    
    static geo_bounds_ref* create(const std::string& table_name) {
        auto ref = new geo_bounds_ref();
        ref->impl_ = std::make_shared<geo_bounds>();
//        ref->impl_->unmanaged_.table_name = table_name;
        return ref;
    }
    
    static geo_bounds_ref* wrap(std::shared_ptr<geo_bounds> obj) {
        auto ref = new geo_bounds_ref();
        ref->impl_ = obj;
        return ref;
    }
    
    geo_bounds_ref(managed<geo_bounds>& o) {
        impl_ = std::make_shared<geo_bounds>(o);
    }
    
    geo_bounds_ref(const geo_bounds& o) {
        impl_ = std::make_shared<geo_bounds>(o);
    }
//    
//    swift_lattice_ref* getLattice() const SWIFT_COMPUTED_PROPERTY {
//        return impl_->lattice;
//    }
    
    // Access the underlying dynamic_object
    geo_bounds* get() { return impl_.get(); }
    const geo_bounds* get() const { return impl_.get(); }
    
    // Get the shared_ptr (for storing in link_values)
    std::shared_ptr<geo_bounds> shared() const { return impl_; }
    
    // For SWIFT_SHARED_REFERENCE
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }

private:
    geo_bounds_ref() = default;

    friend struct dynamic_object;
    friend struct swift_lattice;
    friend struct geo_bounds_list;
    std::shared_ptr<geo_bounds> impl_;
    std::atomic<int> ref_count_{0};

    friend void ::retainGeoBoundsRef(lattice::geo_bounds_ref* p);
    friend void ::releaseGeoBoundsRef(lattice::geo_bounds_ref* p);
} SWIFT_SHARED_REFERENCE(retainGeoBoundsRef, releaseGeoBoundsRef);

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
        using RefType = geo_bounds_ref*;

        geo_bounds value;
        size_t idx;
        geo_bounds_list* list;

        // Assignment updates the list
        element_proxy& operator=(const geo_bounds& bounds);

        // For Swift protocol conformance
        void assign(geo_bounds_ref* ref) SWIFT_NAME(assign(_:));
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
    using RefType = geo_bounds_ref*;
    using ElementProxy = geo_bounds_list::element_proxy;

    static geo_bounds_list_ref* create() {
        auto ref = new geo_bounds_list_ref();
        ref->impl_ = std::make_shared<geo_bounds_list>();
        return ref;
    }

    static geo_bounds_list_ref* wrap(std::shared_ptr<geo_bounds_list> list) {
        auto ref = new geo_bounds_list_ref();
        ref->impl_ = list;
        return ref;
    }

    static geo_bounds_list_ref* create(const managed<std::vector<geo_bounds*>>& m) {
        auto ref = new geo_bounds_list_ref();
        ref->impl_ = std::make_shared<geo_bounds_list>(m);
        return ref;
    }

    geo_bounds_list* get() { return impl_.get(); }
    const geo_bounds_list* get() const { return impl_.get(); }

    std::shared_ptr<geo_bounds_list> shared() const { return impl_; }

    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }

    swift_lattice_ref* getLattice() const SWIFT_COMPUTED_PROPERTY;

    size_t size() const { return impl_->size(); }
    bool empty() const { return impl_->empty(); }

    geo_bounds_list::element_proxy operator[](size_t idx) const {
        return (*impl_)[idx];
    }

    void push_back(const geo_bounds& bounds) {
        impl_->push_back(bounds);
    }

    void push_back(geo_bounds_ref* ref) SWIFT_NAME(pushBack(_:)) {
        impl_->push_back(ref);
    }

    void erase(size_t idx) {
        impl_->erase(idx);
    }

    void clear() {
        impl_->clear();
    }

    void set(size_t idx, const geo_bounds& bounds) {
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
    std::atomic<int> ref_count_{0};

    friend void ::retainGeoBoundsListRef(lattice::geo_bounds_list_ref* p);
    friend void ::releaseGeoBoundsListRef(lattice::geo_bounds_list_ref* p);
} SWIFT_SHARED_REFERENCE(retainGeoBoundsListRef, releaseGeoBoundsListRef);

}

#endif // __cplusplus

#endif
