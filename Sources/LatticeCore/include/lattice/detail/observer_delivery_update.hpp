#pragma once
#include "observer_delivery_target.hpp"
#include "observer_delivery_values.hpp"
#include <cstdint>
#include <cstddef>

namespace lattice::detail {
// Closed builtin setter allowance. Preparing reserves before any Swift/typed
// conversion or cache copy; a supplied extent is a ceiling, never SQL authority.
// One token can supply and execute once, even when a Bridge value wrapper shares
// its owning token. No caller callback/string becomes an executable producer.
class observer_delivery_update final {
    struct payload;payload* value_=nullptr;
    explicit observer_delivery_update(payload*)noexcept;
public:
    enum class value_kind:std::size_t {null_value,integer,real,text,blob};
    struct adoption_view {
        void* context=nullptr;
        // Existing closed library journal: publish prepared cache even for the
        // supported missing-row no-op. No allocation/displaced destruction.
        void(*publish)(void*,bool committed,bool row_present)noexcept=nullptr;
    };
    observer_delivery_update()noexcept=default;
    observer_delivery_update(observer_delivery_update&&)noexcept;
    observer_delivery_update(const observer_delivery_update&)=delete;
    observer_delivery_update& operator=(const observer_delivery_update&)=delete;
    observer_delivery_update& operator=(observer_delivery_update&&)=delete;
    ~observer_delivery_update()noexcept;
    static observer_delivery_update prepare(lattice_db&,const observer_delivery_target&,
        value_kind,uint64_t maximum_value_bytes);
    // Copies the actual checked UTF8/blob/builtin only under the earlier
    // allowance. The synchronous borrowed bytes remain valid through this call.
    void supply(const observer_delivery_values::value_view&);
    observer_delivery_values::value_view value()const;
    void reserve_output_workspace(uint64_t);
    uint64_t declared_output_workspace()const;
    void execute(lattice_db&,adoption_view);
    void execute(lattice_db& owner){execute(owner,{});}
    bool row_present()const;
};
}
