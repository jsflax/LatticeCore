#ifndef list_hpp
#define list_hpp

#ifdef __cplusplus

#include <bridging.hpp>
#include <vector>
#include <optional>
#include <atomic>
#include <dynamic_object.hpp>

namespace lattice {
    class link_list_ref;
}

// Forward declarations for Swift shared reference
void retainLinkListRef(lattice::link_list_ref* p);
void releaseLinkListRef(lattice::link_list_ref* p);

namespace lattice {

struct swift_dynamic_object;
struct swift_lattice_ref;
struct dynamic_object_ref;
struct swift_lattice;

struct link_list {
    using UnmanagedType = swift_dynamic_object;
    using ManagedType = dynamic_object;
    using RefType = dynamic_object_ref;
    
    swift_lattice* lattice = nullptr;
    
    link_list() : lattice(nullptr) {
        new (&unmanaged_) std::vector<swift_dynamic_object *>();
    }
    
    ~link_list() {
        if (lattice) {
            managed_.~managed();
        } else {
            unmanaged_.~vector();
        }
    }
    
    link_list(const link_list& o) : lattice(o.lattice) {
        if (lattice) {
            new (&managed_) managed(o.managed_);
        } else {
            new (&unmanaged_) std::vector(o.unmanaged_);
        }
    }
    
    link_list(link_list&& o) : lattice(o.lattice) {
        if (lattice) {
            new (&managed_) managed(std::move(o.managed_));
        } else {
            new (&unmanaged_) std::vector(std::move(o.unmanaged_));
        }
    }
    
    link_list& operator=(const link_list& o) {
        if (this != &o) {
            // Destroy current
            if (lattice) {
                managed_.~managed();
            } else {
                unmanaged_.~vector();
            }
            // Copy new
            lattice = o.lattice;
            if (lattice) {
                new (&managed_) managed(o.managed_);
            } else {
                new (&unmanaged_) std::vector(o.unmanaged_);
            }
        }
        return *this;
    }
    
    link_list& operator=(link_list&& o) {
        if (this != &o) {
            // Destroy current
            if (lattice) {
                managed_.~managed();
            } else {
                unmanaged_.~vector();
            }
            // Move new
            lattice = o.lattice;
            if (lattice) {
                new (&managed_) managed(std::move(o.managed_));
            } else {
                new (&unmanaged_) std::vector(std::move(o.unmanaged_));
            }
        }
        return *this;
    }
    
    link_list(const std::vector<swift_dynamic_object *>& o) : lattice(nullptr) {
        new (&unmanaged_) std::vector<swift_dynamic_object *>(o);
    }
    
    link_list(const managed<std::vector<swift_dynamic_object*>>& o);
    
    struct element_proxy {
        using RefType = dynamic_object_ref*;
        using ActualType = std::shared_ptr<dynamic_object>;
        
        std::shared_ptr<dynamic_object> object;
        size_t idx;
        link_list* list;

        element_proxy& operator=(dynamic_object_ref* o);
        void assign(dynamic_object_ref* o) SWIFT_NAME(assign(_:)) { this->operator=(o); }
        
        // Access the underlying object
        std::shared_ptr<dynamic_object> operator->() { return object; }
        const std::shared_ptr<dynamic_object> operator->() const { return object; }
        operator dynamic_object&() { return *object; }
        operator const dynamic_object&() const { return *object; }
        RefType getObjectRef() const SWIFT_COMPUTED_PROPERTY { return dynamic_object_ref::wrap(object); }
    };

    // Element access
    element_proxy operator[](size_t idx) const;
    
    // Capacity
    size_t size() const;
    bool empty() const;

    // Modifiers
    void push_back(dynamic_object_ref* obj);
    void push_back(const swift_dynamic_object& obj);
    void erase(size_t idx);
    void clear();

    // Find index of an object (compares by global_id for managed, pointer for unmanaged)
    std::optional<size_t> find_index(const dynamic_object_ref& obj) const;

    // Find all elements matching SQL predicate (managed lists only)
    // Example: find_where("name = 'John' AND age > 25")
    std::vector<size_t> find_where(const std::string& sql_predicate) const
        SWIFT_NAME(findWhere(predicate:));

    // Iterator support (C++ only - Swift uses subscript access)
    struct iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::shared_ptr<dynamic_object>;
        using difference_type = std::ptrdiff_t;
        using pointer = std::shared_ptr<dynamic_object>;
        using reference = dynamic_object&;

        iterator(link_list* list, size_t idx) : list_(list), idx_(idx) {}
        iterator(const iterator&) = default;
        iterator& operator=(const iterator&) = default;

        dynamic_object& operator*() const;
        iterator& operator++() { ++idx_; return *this; }
        iterator operator++(int) { iterator tmp = *this; ++idx_; return tmp; }

        friend bool operator==(const iterator& a, const iterator& b) {
            return a.list_ == b.list_ && a.idx_ == b.idx_;
        }
        friend bool operator!=(const iterator& a, const iterator& b) {
            return !(a == b);
        }

    private:
        link_list* list_;
        size_t idx_;
    };

    iterator begin();
    iterator end();

private:
    union {
        std::vector<dynamic_object_ref *> unmanaged_;
        managed<std::vector<swift_dynamic_object *>> managed_;
    };
    
    friend class swift_lattice;
    friend class link_list_ref;
};

// MARK: - Link List Ref
// Reference-counted wrapper for Swift interop
class link_list_ref final {
public:
    using UnmanagedType = swift_dynamic_object;
    using ManagedType = dynamic_object;
    using RefType = dynamic_object_ref*;
    using ElementProxy = link_list::element_proxy;
    
    // Factory methods for heap allocation
    static link_list_ref* create() SWIFT_NAME(create())  {
        auto ref = new link_list_ref();
        ref->impl_ = std::make_shared<link_list>();
        return ref;
    }

    static link_list_ref* wrap(std::shared_ptr<link_list> list) {
        auto ref = new link_list_ref();
        ref->impl_ = list;
        return ref;
    }

    // Create an owning link_list_ref from a managed vector
    static link_list_ref* create(const managed<std::vector<swift_dynamic_object*>>& m) {
        auto ref = new link_list_ref();
        ref->impl_ = std::make_shared<link_list>(m);
        return ref;
    }

    // Access the underlying link_list
    link_list* get() { return impl_.get(); }
    const link_list* get() const { return impl_.get(); }

    // Get the shared_ptr
    std::shared_ptr<link_list> shared() const { return impl_; }

    // For SWIFT_SHARED_REFERENCE
    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }

    // Delegate common operations to impl_
    swift_lattice_ref* getLattice() const SWIFT_COMPUTED_PROPERTY;

    size_t size() const { return impl_->size(); }
    bool empty() const { return impl_->empty(); }

    link_list::element_proxy operator[](size_t idx) const {
        return (*impl_)[idx];
    }

    void push_back(dynamic_object_ref* obj) SWIFT_NAME(pushBack(_:)) {
        impl_->push_back(obj);
    }

    void push_back(const swift_dynamic_object& obj) {
        impl_->push_back(obj);
    }

    void erase(size_t idx) {
        impl_->erase(idx);
    }

    void clear() {
        impl_->clear();
    }

    std::optional<size_t> find_index(const dynamic_object_ref& obj) const SWIFT_NAME(findIndex(_:)) {
        return impl_->find_index(obj);
    }

    std::vector<size_t> find_where(const std::string& sql_predicate) const
        SWIFT_NAME(findWhere(_:)) {
        return impl_->find_where(sql_predicate);
    }

    link_list::iterator begin() {
        return impl_->begin();
    }

    link_list::iterator end() {
        return impl_->end();
    }

private:
    link_list_ref() = default;

    std::shared_ptr<link_list> impl_;
    std::atomic<int> ref_count_{0};

    friend void ::retainLinkListRef(lattice::link_list_ref* p);
    friend void ::releaseLinkListRef(lattice::link_list_ref* p);
} SWIFT_SHARED_REFERENCE(retainLinkListRef, releaseLinkListRef)  SWIFT_CONFORMS_TO_PROTOCOL(Lattice.CxxLinkListRef);

using optional_size_t = std::optional<size_t>;
using vec_size_t = std::vector<size_t>;

}

#endif // __cplusplus

#endif /* list_hpp */
