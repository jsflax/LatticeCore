#include <lattice/detail/observer_delivery_write.hpp>
#include <lattice/lattice.hpp>
#include "observer_delivery_connection.hpp"
#include "observer_delivery_original.hpp"

namespace lattice::detail {
struct observer_delivery_begin::payload {
    const uint64_t creator;
    std::shared_ptr<database> writer;
    std::shared_ptr<const observer_delivery_connection> connection;
    std::unique_ptr<observer_delivery_original::begin_offer> offer;
    explicit payload(uint64_t issuer):creator(issuer){}
    static void destroy(payload* value)noexcept {
        if(!value||value->creator!=database_native_process())return;
        value->offer.reset();if(value->creator!=database_native_process())return;
        value->connection.reset();if(value->creator!=database_native_process())return;
        value->writer.reset();if(value->creator!=database_native_process())return;
        delete value;
    }
};
observer_delivery_begin::observer_delivery_begin(lattice_db& owner) {
    static_assert(sizeof(payload)<=observer_delivery_plan::begin_storage_bytes);
    owner.require_creator_process();
    if(!observer_delivery_write::enabled(owner))return;
    const auto issuer=database_native_process();
    auto* value=new payload(issuer);
    try {
        if(value->creator!=database_native_process()||!owner.owns_creator_process())
            throw db_error("inherited delivery BEGIN allocation");
        value->connection=std::atomic_load(&owner.observer_delivery_connection_);
        {
            std::lock_guard lock(owner.connection_ownership_mutex_);
            if(owner.closed_.load(std::memory_order_acquire)||!owner.db_||!value->connection)
                throw db_error("delivery BEGIN requires its live profile owner");
            value->writer=owner.db_;
        }
        if(!value->connection->current(owner,*value->writer)||
           database::sql_entry_scope::active_for(value->writer.get())||value->writer->is_in_transaction())
            throw db_error("delivery BEGIN cannot acquire while an original/Native entry is held");
        // This is the ONE absolute blocking admission for the actual caller
        // transaction, outside its Native/store entry. Subplans never call it.
        const auto& profile=*value->connection;
        auto admitted=profile.client_.acquire(observer_delivery_meter::admission_charge(profile.policy_.original),
            observer_delivery_store::clock::now()+profile.policy_.maximum_admission_wait);
        if(admitted.code!=observer_delivery_store::status::admitted)
            throw db_error("delivery BEGIN envelope refused before effects");
        if(value->creator!=database_native_process())throw db_error("inherited delivery BEGIN admission");
        // Construct once: the TLS offer's address cannot move. It retains
        // pure DATA only; actual writer ownership remains in this fenced heap.
        value->offer.reset(new observer_delivery_original::begin_offer(*value->writer,
            std::move(admitted.held),profile.policy_.original,std::weak_ptr<const void>{},true));
        if(value->creator!=database_native_process())throw db_error("inherited delivery BEGIN offer");
        value_=value;
    }catch(...) {payload::destroy(value);throw;}
}
observer_delivery_begin::~observer_delivery_begin()noexcept {payload::destroy(value_);}
}
