#pragma once
#include "database_retirement.hpp"
#include <array>
#include <mutex>
#include <limits>
namespace lattice::detail {
// Per-child lifetime accounting only: no schema/profile/source/receipt authority.
// A slot is reserved before allocation/open and survives publication/local tails.
class database_open_ledger {
public:
    static constexpr size_t maximum_opens=64;
    struct admission {size_t slot;uint64_t serial;};
private:
    struct entry {
        uint64_t serial=0;
        bool occupied=false,attempted=false,object_done=false,builder_done=false,draining=false,drain_returned=false;
        int first_checked_close=database_retirement_state::not_attempted;
        std::shared_ptr<const database_retirement_state> native;
    };
    const uint64_t creator_=database_native_process();
    std::mutex leaf_;
    std::array<entry,maximum_opens> entries_{};
    uint64_t serial_=0;
    size_t occupied_=0;
    bool sealed_=false;
    void* tail_=nullptr;
    void (*finish_)(void*) noexcept=nullptr;
    entry& exact(size_t slot,uint64_t serial) {
        if(slot>=maximum_opens||!entries_[slot].occupied||entries_[slot].serial!=serial)
            throw db_error("history native open custody mismatch");
        return entries_[slot];
    }
    void drain(size_t slot,uint64_t serial) noexcept {
        if(!in_creator_process()||slot>=maximum_opens)return;
        std::shared_ptr<const database_retirement_state> native;
        {
            std::lock_guard lock(leaf_);
            auto& value=entries_[slot];
            if(!value.occupied||value.serial!=serial||value.draining||!value.object_done||!value.builder_done)return;
            if(value.attempted){
                if(!value.native)return;
                // Terminal acquire precedes the move/refusal observation.
                if(value.native->state()!=database_retirement_state::phase::closed||value.native->owning_wrapper_moved())return;
            }
            value.first_checked_close=value.native?value.native->checked_close_result():database_retirement_state::not_attempted;
            value.draining=true;native=std::move(value.native);
        }
        native.reset(); // Witness targets release OFF the ledger leaf.
        if(!in_creator_process())return;
        void* tail=nullptr;void (*finish)(void*) noexcept=nullptr;
        {
            std::lock_guard lock(leaf_);
            auto& value=entries_[slot];
            if(!value.occupied||value.serial!=serial||!value.draining)return;
            value.drain_returned=true; // Actual original native witness reset returned before this publication.
            value.occupied=false;value.draining=false;--occupied_;
            if(sealed_&&occupied_==0&&tail_){tail=tail_;finish=finish_;tail_=nullptr;finish_=nullptr;}
        }
        if(finish)finish(tail); // No later inherited leaf work after arbitrary finalization.
    }
protected:
    database_open_ledger()=default;
    ~database_open_ledger()=default;
    bool disposable() const noexcept {return in_creator_process()&&occupied_==0&&!tail_;}
public:
    bool in_creator_process() const noexcept {return creator_==database_native_process();}
    admission reserve_open() {
        if(!in_creator_process())throw db_error("inherited history native open ledger");
        std::lock_guard lock(leaf_);
        if(sealed_||serial_==std::numeric_limits<uint64_t>::max())throw db_error("history child native admission closed");
        for(size_t n=0;n<maximum_opens;++n)if(!entries_[n].occupied){
            auto& value=entries_[n];value.serial=++serial_;value.occupied=true;value.attempted=false;
            value.object_done=false;value.builder_done=false;value.draining=false;value.drain_returned=false;
            value.first_checked_close=database_retirement_state::not_attempted;++occupied_;
            return {n,value.serial};
        }
        throw db_error("history child native open capacity exhausted");
    }
    void bind_retirement(size_t slot,uint64_t serial,const std::shared_ptr<const database_retirement_state>& native) {
        if(!in_creator_process())throw db_error("inherited history native retirement binding");
        std::lock_guard lock(leaf_);auto& value=exact(slot,serial);
        if(value.native||value.attempted||!native)throw db_error("history native retirement already bound");
        value.native=native;
    }
    void native_open_entered(size_t slot,uint64_t serial) {
        if(!in_creator_process())throw db_error("inherited history native open entry");
        std::lock_guard lock(leaf_);auto& value=exact(slot,serial);
        if(!value.native||value.attempted)throw db_error("history native open lacks retirement custody");
        value.attempted=true; // Existing reservation permits admitted settlement after seal.
    }
    void object_returned(size_t slot,uint64_t serial) noexcept {
        if(!in_creator_process())return;
        {std::lock_guard lock(leaf_);if(slot>=maximum_opens)return;auto& value=entries_[slot];
            if(!value.occupied||value.serial!=serial)return;value.object_done=true;}
        drain(slot,serial);
    }
    void builder_returned(size_t slot,uint64_t serial) noexcept {
        if(!in_creator_process())return;
        {std::lock_guard lock(leaf_);if(slot>=maximum_opens)return;auto& value=entries_[slot];
            if(!value.occupied||value.serial!=serial)return;value.builder_done=true;}
        drain(slot,serial);
    }
    bool fixed_scope_original_ended(size_t slot,uint64_t serial) noexcept {
        if(!in_creator_process()||slot>=maximum_opens||!serial)return false;
        std::lock_guard lock(leaf_);const auto& value=entries_[slot];
        return sealed_&&value.serial==serial&&value.attempted&&value.object_done&&value.builder_done&&
            value.drain_returned&&value.first_checked_close==SQLITE_OK&&!value.occupied&&!value.draining&&!value.native;
        // Positive original object/builder/drain returns, never an empty-slot inference.
    }
    bool seal_fixed_scope() noexcept {
        if(!in_creator_process())return false;
        std::lock_guard lock(leaf_);sealed_=true;return true;
    }
    void handoff(void* tail,void (*finish)(void*) noexcept) noexcept {
        if(!in_creator_process())return;
        void* ready=nullptr;
        {std::lock_guard lock(leaf_);sealed_=true;
            if(tail_||!tail||!finish)return;tail_=tail;finish_=finish;
            if(occupied_==0){ready=tail_;tail_=nullptr;finish_=nullptr;}}
        if(ready)finish(ready);
    }
};
} // namespace lattice::detail
