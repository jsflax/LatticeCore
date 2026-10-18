#pragma once
#include <cstdint>
#include <memory>
#include <string_view>

namespace lattice {
class database;class lattice_db;struct dynamic_object;
struct instance_guard;struct managed_base;class model_base;
}
namespace lattice::detail {
class observer_delivery_connection;
class observer_delivery_update;

// One bind-issued immutable DATA original, shared by the model and all its
// fields. Weak Native/owner/profile references neither keep a store open nor
// prove any ending when they expire. The intrusive handle checks its actual
// creator before touching inherited reference counts, including final release.
class observer_delivery_identity final {
    struct payload;
    payload* value_=nullptr;
    explicit observer_delivery_identity(payload*)noexcept;
    static void retain(payload*);
    static void release(payload*)noexcept;
    static const payload& checked(payload*,bool published=true);
    static observer_delivery_identity prepare(lattice_db&,
        const std::shared_ptr<database>&,const std::shared_ptr<instance_guard>&,
        const std::shared_ptr<const observer_delivery_connection>&,
        std::string_view table,std::string_view global_id,int64_t attachment_generation);
    // Allocated before INSERT effects with rowid0; one real successful binding
    // publication supplies the actual SQL rowid without allocating or copying.
    void publish(int64_t actual_rowid)const noexcept;
    const lattice_db* owner_address()const;
    const std::weak_ptr<database>& writer_identity()const;
    const std::weak_ptr<instance_guard>& owner_lifetime()const;
    const std::weak_ptr<const observer_delivery_connection>& profile_identity()const;
    friend class ::lattice::lattice_db;
    friend class observer_delivery_update;
    friend class observer_delivery_target;
public:
    static uint64_t allocation_bytes()noexcept;
    observer_delivery_identity()noexcept=default;
    observer_delivery_identity(const observer_delivery_identity&);
    observer_delivery_identity(observer_delivery_identity&&)noexcept;
    observer_delivery_identity& operator=(const observer_delivery_identity&);
    observer_delivery_identity& operator=(observer_delivery_identity&&)noexcept;
    ~observer_delivery_identity()noexcept;
    bool valid()const noexcept;
    std::string_view table()const;
    std::string_view global_id()const;
    int64_t rowid()const;
    int64_t attachment_generation()const;
};

// A synchronous borrowed column selected by an actual bound model/field.
// Only the library binding adapters can mint one. The UPDATE preparer must
// copy/validate its column after its same-original conversion allowance, and
// retains the actual identity rather than a borrowed wrapper address.
class observer_delivery_target final {
    observer_delivery_identity identity_;
    std::string_view column_;
    observer_delivery_target(const observer_delivery_identity&,std::string_view);
    friend struct ::lattice::managed_base;
    friend class ::lattice::model_base;
    friend struct ::lattice::dynamic_object;
    friend class observer_delivery_update;
public:
    observer_delivery_target()noexcept=default;
    bool valid()const noexcept;
    // DATA equality for this real bind incarnation and borrowed column. It
    // neither promotes Native ownership nor grants/refunds any operation.
    bool same_binding(const observer_delivery_target&)const noexcept;
    std::string_view table()const;
    std::string_view global_id()const;
    std::string_view column()const;
    int64_t rowid()const;
    int64_t attachment_generation()const;
};
}
