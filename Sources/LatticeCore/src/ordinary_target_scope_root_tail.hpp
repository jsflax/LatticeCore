#pragma once
#include "ordinary_owned_launch.hpp"
#include <memory>
#include <thread>
#include <array>
#include <cstdint>
#include <cstddef>
#include <initializer_list>
#include <exception>
namespace lattice { class database; }
namespace lattice::detail {
class ordinary_target_staging_connection;
class ordinary_target_staging_io;
template<bool Receiving> class ordinary_resource_reservation;
struct ordinary_target_scope_root_record;
struct ordinary_target_scope_environment_record;
}
#include "../include/lattice/native_retirement_storage.hpp"
#include "../include/lattice/ordinary_history.hpp"
namespace lattice::detail {
struct ordinary_target_scope_native_fact final {
    const void* state=nullptr;const void* retirement=nullptr;
    std::size_t slot=0,retained_bytes=0;std::uint64_t serial=0,flags=0;
    int open_rc=0,close_rc=0,fallback_rc=0,udf_rc=0;
    std::array<const void*,5> histories{};
    std::exception_ptr first_failure;
};
struct ordinary_target_scope_predecessor_fact final {
    const void* receiving=nullptr;const void* roots=nullptr;const void* schema=nullptr;const void* state=nullptr;
    const void* scope=nullptr;std::uint64_t generation=0;std::size_t retained_bytes=0;
    std::exception_ptr first_failure;
};
struct ordinary_target_scope_resource_fact final {
    const void* original=nullptr;const void* borrower=nullptr;
    std::uint64_t cookie=0,device=0,inode=0,flags=0;
    std::int64_t size=0;
    int fd=-1,close_rc=-1,close_error=0,sync_rc=0,unlink_sync_rc=0,lock_level=0;
};
struct ordinary_target_scope_call_fact final {
    const void* original=nullptr; // Real first Native call object address, never dereferenced after ending.
    unsigned kind=0;std::size_t resource_index=0,callback_slot=64;
    int fd=-1,error=0;std::int64_t requested=0,result=-1;
    std::uint64_t callback_generation=0,flags=0;
    ordinary_target_scope_resource_fact resource;
};
inline std::uint64_t terminal_scope_fact_flags(std::initializer_list<bool> values)noexcept {
    std::uint64_t flags=0,bit=1;for(bool value:values){if(value)flags|=bit;bit<<=1;}return flags;
}
template<typename Resource> ordinary_target_scope_resource_fact terminal_scope_resource_fact(const Resource& value)noexcept {
    return {&value,value.borrower,value.cookie,value.device,value.inode,
        terminal_scope_fact_flags({value.journal,value.birth,value.named,value.borrowed,value.borrow_attempted,
            value.birth_attempted,value.birth_entered,value.birth_returned,value.write_pending,value.unlink_attempted,value.unlinked,
            value.close_attempted,value.close_returned,value.sync_attempted,value.sync_returned,value.unlink_sync_required,
            value.unlink_sync_returned,value.pending_byte,value.pending_write,value.reserved_byte,value.shared_range,value.exclusive_range}),
        value.size,value.fd,value.close_rc,value.close_errno,value.sync_rc,value.unlink_sync_rc,value.lock_level};
}
template<typename Call> ordinary_target_scope_call_fact terminal_scope_call_fact(const Call& value)noexcept {
    ordinary_target_scope_call_fact out;
    out.original=&value;out.kind=static_cast<unsigned>(value.kind);out.resource_index=value.resource_index;out.callback_slot=value.callback_slot;
    out.fd=value.fd;out.error=value.error;out.requested=value.requested;out.result=value.result;
    out.callback_generation=value.callback_generation;out.flags=terminal_scope_fact_flags({value.attempted,value.entered,value.returned});
    if(value.original)out.resource=terminal_scope_resource_fact(*value.original);
    return out;
}
// Only scalar first facts and the SAME real receiving reservation. The charge
// custody is first, so it releases after all first DATA members have ended.
struct ordinary_target_scope_root_record final {
    native_retirement_storage<std::shared_ptr<ordinary_resource_reservation<true>>> charge;
    const void* const original;
    const unsigned kind; // 0wrapper,1scope_state,2call_bank,3resource,4environment; 5Conn,6Conn-state,7Native-state,8shell,9target-role.
    const bool constructor_returned;
    ordinary_target_scope_resource_fact resource;
    bool binding_attempted=false,binding_returned=false,members_destroyed=false,partial_constructor=false;
    ordinary_target_scope_root_record(const void* object,unsigned selected,bool constructed,
        const std::shared_ptr<ordinary_resource_reservation<true>>& actual)
        :original(object),kind(selected),constructor_returned(constructed){charge.template get<0>()=actual;}
};
class ordinary_target_scope_root_tail final {
    friend class ordinary_target_staging_connection;
    friend class ordinary_target_staging_io;
    friend class ordinary_unpublished_group_target;
    friend class ::lattice::database;
    const ordinary_launch::process_identity process_=ordinary_launch::current_process();
    const std::thread::id thread_=std::this_thread::get_id();
    const void* const original_;
    const unsigned kind_;
    bool construction_returned_=false;
    std::shared_ptr<ordinary_target_scope_root_record> first_;
    ordinary_target_scope_root_tail(const void* original,unsigned kind)noexcept:original_(original),kind_(kind){}
    bool bind(const std::shared_ptr<ordinary_target_scope_root_record>& first)noexcept {
        if(process_!=ordinary_launch::current_process()||thread_!=std::this_thread::get_id()||!construction_returned_||first_||
           !first||first->original!=original_||first->kind!=kind_||!first->constructor_returned||first->binding_attempted)return false;
        first->binding_attempted=true;first_=first;first->binding_returned=true;return true;
    }
public:
    ~ordinary_target_scope_root_tail()noexcept {
        if(process_!=ordinary_launch::current_process()||thread_!=std::this_thread::get_id()||!first_||
           first_->original!=original_||first_->kind!=kind_)return;
        first_->partial_constructor=!construction_returned_;first_->members_destroyed=true;
    }
    ordinary_target_scope_root_tail(const ordinary_target_scope_root_tail&)=delete;
    ordinary_target_scope_root_tail& operator=(const ordinary_target_scope_root_tail&)=delete;
};
struct ordinary_target_scope_environment_record final {
    struct call_fact {int kind=0,argument=0,result=0;std::uint64_t callback_generation=0,flags=0;};
    const void* original=nullptr;const void* connection=nullptr;
    std::uint64_t generation=0,settlement_generation=0,cleanup_generation=0,flags=0;
    std::size_t count=0;
    std::array<call_fact,128> calls{};
    std::shared_ptr<ordinary_target_scope_root_record> ending;
};
static_assert(sizeof(ordinary_target_scope_root_record)<=256);
static_assert(sizeof(ordinary_target_scope_call_fact)<=192);
static_assert(sizeof(ordinary_target_scope_environment_record)<=8192);
}
