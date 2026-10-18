#pragma once
#include "observer_delivery_values.hpp"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace lattice { class lattice_db; struct model_schema; }
namespace lattice::detail {
// Internal public-BEGIN adapter. Empty and nonallocating in the legacy
// profile. An enabled owner reserves its ONE declared original before the
// existing Native/store entry; only the real Core BEGIN can claim the offer.
class observer_delivery_begin final {
    struct payload;
    payload* value_=nullptr;
public:
    explicit observer_delivery_begin(lattice_db&);
    ~observer_delivery_begin()noexcept;
    observer_delivery_begin(const observer_delivery_begin&)=delete;
    observer_delivery_begin& operator=(const observer_delivery_begin&)=delete;
};
// Internal closed producer interface. No SQL, collector, trust flag, budget
// grant or Native pointer is supplied by an adapter. The physical connection
// profile validates/adopts the input and fixes its one absolute wait deadline.
class observer_delivery_write final {
    struct payload;
    payload* value_=nullptr;
    explicit observer_delivery_write(payload*)noexcept;
    template<typename Input>static observer_delivery_write prepare_input(lattice_db&,const Input&);
public:
    struct inserted_row {int64_t rowid=0;std::string_view global_id;};
    struct adoption_view {
        void* context=nullptr;
        // Borrowed exact library journal; must not allocate/destroy displaced
        // targets. Invoked at existing post-leaf/pre-delivery adoption, once.
        void(*publish)(void*,bool,std::span<const inserted_row>)noexcept=nullptr;
    };
    observer_delivery_write()=default;
    observer_delivery_write(observer_delivery_write&&)noexcept;
    observer_delivery_write(const observer_delivery_write&)=delete;
    observer_delivery_write& operator=(const observer_delivery_write&)=delete;
    observer_delivery_write& operator=(observer_delivery_write&&)=delete;
    ~observer_delivery_write()noexcept;
    static bool enabled(const lattice_db&)noexcept;
    static observer_delivery_write prepare(lattice_db&,const observer_delivery_values::cursor&);
    static observer_delivery_write prepare(lattice_db&,std::span<const observer_delivery_values::row_view>);
    size_t row_count()const;
    observer_delivery_values::row_view row(size_t,observer_delivery_values::field_buffer&)const;
    // Borrowed immutable connection-catalog DATA; retained by this write.
    // It grants no owner liveness, SQL admission, ending or callback authority.
    const model_schema& schema(size_t)const;
    std::string_view global_id(size_t)const;
    // Closed adapters measure their actual structured output before copying.
    // Consumes workspace monotonically from this same admitted original; it
    // neither acquires/renews a lease nor estimates opaque callback captures.
    void reserve_output_workspace(uint64_t);
    uint64_t declared_output_workspace()const;
    std::span<const inserted_row> inserted_rows()const;
    void execute(lattice_db&,adoption_view);
    void execute(lattice_db& owner){execute(owner,{});}
};
inline bool observer_delivery_enabled(const lattice_db& owner)noexcept {
    return observer_delivery_write::enabled(owner);
}
}
