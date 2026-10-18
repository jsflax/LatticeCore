#include <lattice/detail/observer_delivery_assignment.hpp>
#include <lattice/detail/observer_delivery_update.hpp>
#include <lattice/lattice.hpp>

namespace lattice::detail {
namespace {
uint64_t closed_fresh_capacity(uint64_t extent) {
    uint64_t bytes=0;observer_delivery_value_detail::add_structured_bytes(bytes,extent);
    observer_delivery_value_detail::add_structured_bytes(bytes,extent);
    observer_delivery_value_detail::add_structured_bytes(bytes,32);return bytes;
}
}
bool observer_delivery_assignment::enabled(const managed_base& field) {
    if(!field.lattice)return false;
    field.lattice->require_creator_process();
    return field.is_bound()&&observer_delivery_enabled(*field.lattice);
}
template<typename T>void observer_delivery_assignment::builtin(managed<T,void>& field,
    std::size_t kind,uint64_t extent,
    observer_delivery_values::value_view input) {
    auto& owner=*field.lattice;owner.require_creator_process();
    const auto creator=database_native_process();
    const auto target=field.observer_delivery_target_for_field();
    auto update=observer_delivery_update::prepare(owner,target,static_cast<observer_delivery_update::value_kind>(kind),extent);
    update.supply(input);
    owner.require_creator_process();
    const auto actual=update.value();
    struct output {uint64_t creator=0;managed<T,void>* field=nullptr;T staged{};};
    using storage=native_retirement_storage<output>;
    uint64_t bytes=storage::allocation_bytes();
    std::visit([&](const auto& contents){using V=std::decay_t<decltype(contents)>;
        if constexpr(std::is_same_v<V,std::string_view>||std::is_same_v<V,std::span<const uint8_t>>)
            observer_delivery_value_detail::add_structured_bytes(bytes,closed_fresh_capacity(contents.size()));
    },actual);
    if constexpr(std::is_same_v<T,std::string>||std::is_same_v<T,std::vector<uint8_t>>) {
        observer_delivery_value_detail::add_structured_bytes(bytes,field.unmanaged_value.capacity());
        if constexpr(std::is_same_v<T,std::string>)observer_delivery_value_detail::add_structured_bytes(bytes,1);
    }
    update.reserve_output_workspace(bytes);
    storage retained;owner.require_creator_process();
    if(!retained.in_creator_process())throw db_error("delivery scalar output changed process");
    auto& result=retained.template get<0>();result.creator=creator;result.field=&field;
    result.staged=observer_delivery_value_detail::materialized_scalar<T>(actual);
    owner.require_creator_process();
    if constexpr(std::is_same_v<T,std::string>||std::is_same_v<T,std::vector<uint8_t>>) {
        const auto extent=std::visit([](const auto& value)->uint64_t {
            using V=std::decay_t<decltype(value)>;
            if constexpr(std::is_same_v<V,std::string_view>||std::is_same_v<V,std::span<const uint8_t>>)return value.size();
            else return 0;
        },actual);
        if(result.staged.capacity()>closed_fresh_capacity(extent))throw db_error("delivery fresh scalar cache capacity exceeded");
    }
    update.execute(owner,{&result,[](void* raw,bool committed,bool)noexcept {
        auto& actual=*static_cast<output*>(raw);
        if(actual.creator!=database_native_process()||!committed)return;
        using std::swap;swap(actual.field->unmanaged_value,actual.staged);
    }});
    owner.require_creator_process();
}
void observer_delivery_assignment::scalar(managed<int64_t,void>& field,int64_t value) {
    builtin(field,static_cast<std::size_t>(observer_delivery_update::value_kind::integer),0,int64_t(value));
}
void observer_delivery_assignment::scalar(managed<int,void>& field,int value) {
    builtin(field,static_cast<std::size_t>(observer_delivery_update::value_kind::integer),0,int64_t(value));
}
void observer_delivery_assignment::scalar(managed<double,void>& field,double value) {
    builtin(field,static_cast<std::size_t>(observer_delivery_update::value_kind::real),0,double(value));
}
void observer_delivery_assignment::scalar(managed<float,void>& field,float value) {
    builtin(field,static_cast<std::size_t>(observer_delivery_update::value_kind::real),0,double(value));
}
void observer_delivery_assignment::scalar(managed<bool,void>& field,bool value) {
    builtin(field,static_cast<std::size_t>(observer_delivery_update::value_kind::integer),0,int64_t(value?1:0));
}
void observer_delivery_assignment::blob(managed<std::vector<uint8_t>,void>& field,const std::vector<uint8_t>& value) {
    builtin(field,static_cast<std::size_t>(observer_delivery_update::value_kind::blob),value.size(),std::span<const uint8_t>(value));
}
void observer_delivery_assignment::string(managed<std::string,void>& field,
    const std::string& source,std::string* consumed) {
    auto& owner=*field.lattice;owner.require_creator_process();
    const auto creator=database_native_process();
    const auto target=field.observer_delivery_target_for_field();
    auto update=observer_delivery_update::prepare(owner,target,
        observer_delivery_update::value_kind::text,source.size());
    update.supply(std::string_view(source.data(),source.size()));
    owner.require_creator_process();
    const auto value=std::get<std::string_view>(update.value());
    struct output {
        uint64_t creator=0;
        managed<std::string,void>* field=nullptr;
        std::string staged;
    };
    using storage=native_retirement_storage<output>;
    uint64_t bytes=storage::allocation_bytes();
    observer_delivery_value_detail::add_structured_bytes(bytes,closed_fresh_capacity(value.size()));
    observer_delivery_value_detail::add_structured_bytes(bytes,field.unmanaged_value.capacity());
    observer_delivery_value_detail::add_structured_bytes(bytes,1); // old capacity transferred at adoption
    update.reserve_output_workspace(bytes);
    storage held;
    owner.require_creator_process();
    if(!held.in_creator_process())throw db_error("delivery string output changed process");
    auto& result=held.get<0>();result.creator=creator;result.field=&field;
    result.staged.assign(value);
    owner.require_creator_process();
    if(result.staged.capacity()+1>closed_fresh_capacity(value.size()))throw db_error("delivery fresh string cache capacity exceeded");
    // Consume only after every fallible admission/input/output preparation.
    // Clearing leaves a valid moved-from value while avoiding transfer of an
    // arbitrarily large caller capacity into library-owned callback custody.
    // Self-alias therefore becomes empty before SQL, as with the old owning
    // parameter, and a later SQL failure leaves it consumed.
    if(consumed)consumed->clear();
    update.execute(owner,{&result,[](void* raw,bool committed,bool)noexcept {
        auto& output=*static_cast<struct output*>(raw);
        if(output.creator!=database_native_process()||!committed)return;
        // Missing-row success adopts the cache too. Stale-gid/SQL failure
        // never reaches committed=true. Old cache retires off all leaves.
        output.field->unmanaged_value.swap(output.staged);
    }});
    owner.require_creator_process();
}

}
