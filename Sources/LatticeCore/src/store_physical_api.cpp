#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "store_physical_api.hpp"
#include "ordinary_resource_normal_startup.hpp"
#include "database_initialization_budget.hpp"
#include "store_physical_vfs.hpp"
#include <cstring>
#include <lattice/db.hpp>
#include <atomic>
#include <mutex>
#include <optional>

namespace lattice::detail {
struct store_physical_api::original {
    const std::uint64_t creator=database_native_process();
    store_physical_lifetime::pin held;
    // Fixed typed DATA only. This is setup/compiled-cache custody, never an
    // ordinary transaction/observer-payload refund. Actual API/VFS/Native
    // ending precedes off-leaf completion/deletion of this SAME original.
    std::shared_ptr<const database_initialization_budget> initialization_budget;
    original* next=nullptr;
    enum class returned_action : std::uint8_t {none,complete,destroy};
    original* returned_next=nullptr;
    returned_action returned=returned_action::none;
    sqlite3* actual=nullptr; // lookup DATA only; never dereferenced or invoked
    std::uint64_t references=1;
    bool primary=true,listed=false,current=false,completion_claimed=false,completion_returned=false;
    void* completion_cookie=nullptr;
    void(*completion)(void*)noexcept=nullptr;
    explicit original(const store_physical_lifetime::pin& pin):held(pin){}
};
namespace {
const std::uint64_t physical_api_creator=database_native_process();
std::mutex physical_api_leaf;
std::atomic<bool> physical_api_used{false};

}
store_physical_api::original* store_physical_api::head_=nullptr;
thread_local store_physical_api::original* store_physical_api::returned_tails_=nullptr;
thread_local database_initialization_budget* store_physical_api::returned_budgets_=nullptr;
thread_local bool store_physical_api::draining_tails_=false;
thread_local std::uint64_t store_physical_api::native_return_work_=0;
thread_local store_physical_api::api_scope* store_physical_api::api_scope::current_=nullptr;
store_physical_api::original* store_physical_api::prepare_(const store_physical_lifetime::pin& held,void* cookie,void(*completed)(void*)noexcept) {
    if(!held.current()||physical_api_creator!=database_native_process())
        throw db_error("physical API original requires its actual creator");
    if(!cookie||!completed)throw db_error("physical API original requires its actual cleanup producer");
    auto* value=new original(held);
    value->completion_cookie=cookie;value->completion=completed;
    if(value->creator!=database_native_process())
        throw db_error("physical API original changed process during admission"); // child abandons capsule
    if(!held.current()){delete value;throw db_error("physical API original lost its lease during admission");}
    return value;
}
bool store_physical_api::publish_(original* issued,sqlite3* actual,void* cookie,void(*completed)(void*)noexcept)noexcept {
    if(!issued||issued->creator!=database_native_process()||physical_api_creator!=database_native_process())return false;
    try {
        std::lock_guard lock(physical_api_leaf);
        if(issued->listed||!issued->held.current()||!completed)return false;
        // Only the actual new SQLite open return can supersede an address.
        // Old cleanup frames already carry their immutable original pin.
        for(auto* at=head_;at;at=at->next)
            if(at->actual==actual)at->current=false;
        issued->actual=actual;issued->completion_cookie=cookie;issued->completion=completed;
        issued->listed=true;issued->current=actual!=nullptr;
        issued->next=head_;head_=issued;
        physical_api_used.store(true,std::memory_order_release);return true;
    }catch(...){return false;} // Issuer retains unproved registration/lease custody.
}
bool store_physical_api::publish_member_(original* issued,sqlite3* actual,void* cookie,void(*completed)(void*)noexcept)noexcept {
    if(!issued||!actual||issued->creator!=database_native_process()||physical_api_creator!=database_native_process())return false;
    try{std::lock_guard lock(physical_api_leaf);
        if(issued->listed||!issued->held.current()||!completed||!cookie)return false;
        bool primary=false;std::size_t count=0;
        for(auto* at=head_;at;at=at->next)if(at->current&&at->actual==actual){
            primary=primary||at->primary;if(at->completion_cookie==cookie)return false;++count;
        }
        if(!primary||count>=maximum_originals)return false;
        issued->primary=false;issued->actual=actual;issued->completion_cookie=cookie;issued->completion=completed;
        issued->listed=true;issued->current=true;issued->next=head_;head_=issued;return true;
    }catch(...){return false;}
}
void store_physical_api::abandon_unpublished_(original* issued)noexcept {
    if(!issued||issued->creator!=database_native_process())return;
    if(!issued->listed)delete issued; // called outside Native/VFS/registry leaves
}
bool store_physical_api::foreign_protected_()noexcept {
    return physical_api_creator!=database_native_process()&&physical_api_used.load(std::memory_order_acquire);
}
store_physical_api::ticket store_physical_api::capture_issued_(original* actual)noexcept {
    ticket result;result.creator=database_native_process();
    if(!actual||actual->creator!=result.creator||physical_api_creator!=result.creator){result.inherited=true;return result;}
    try{std::lock_guard lock(physical_api_leaf);
        if(actual->references==UINT64_MAX){result.inherited=true;return result;}
        ++actual->references;result.value=actual;result.values[0]=actual;result.count=1;
    }catch(...){result.inherited=true;}
    return result;
}
store_physical_api::ticket store_physical_api::capture_(sqlite3* actual)noexcept {
    ticket result;result.creator=database_native_process();
    if(!physical_api_used.load(std::memory_order_acquire))return result;
    if(physical_api_creator!=result.creator){result.inherited=true;return result;}
    try{
        std::lock_guard lock(physical_api_leaf);
        std::size_t count=0;
        for(auto* at=head_;at;at=at->next)if(at->current&&at->actual==actual){
            if(at->references==UINT64_MAX||count==maximum_originals){result.inherited=true;return result;}++count;
        }
        // Count/overflow validation precedes all reference changes. No Native
        // pin allocation or destructor is performed beneath this DATA leaf.
        for(auto* at=head_;at;at=at->next)if(at->current&&at->actual==actual){
            ++at->references;result.values[result.count++]=at;
            if(at->primary)result.value=at;
        }
        if(!result.value&&result.count)result.value=result.values[0];
    }catch(...){result.inherited=true;}
    return result;
}
store_physical_vfs* store_physical_api::actual_owner_(const ticket& actual)noexcept {
    if(actual.inherited||actual.creator!=database_native_process()||!actual.value||!actual.value->primary||
       actual.value->completion!=&store_physical_vfs::complete_||!actual.value->completion_cookie)return nullptr;
    auto* root=static_cast<store_physical_vfs*>(actual.value->completion_cookie);
    return root->current_()&&root->root_==root?root:nullptr;
}
thread_local store_physical_native_scope* store_physical_native_scope::current_=nullptr;
bool store_physical_api::retain_initialization_budget_(store_physical_open_original& owner,
    const std::shared_ptr<const database_initialization_budget>& budget)noexcept {
    if(!budget||!budget->current_()||owner.creator_!=database_native_process()||
       owner.exclusive_target_||owner.legacy_||foreign_protected_())return false;
    auto actual=capture_open_(owner,owner.actual_);
    if(actual.inherited||actual.count!=1||!actual.value||!actual.value->primary||
       !actual_owner_(actual)||native_custody_here_(actual.value))return false;
    const auto observed=store_physical_lifetime::observe_(actual.value->held);
    if(!observed.current||observed.unknown||observed.connection_retired||
       observed.active_api||observed.active_native||observed.active_vfs||
       store_physical_lifetime::observer_policy_unsettled_(actual.value->held))return false;
    try {
        std::lock_guard lock(physical_api_leaf);
        auto* original=actual.value;
        if(owner.creator_!=database_native_process()||!budget->current_()||
           !original->listed||!original->current||original->actual!=owner.actual_||
           original->completion_claimed||original->completion_returned)return false;
        if(original->initialization_budget)return original->initialization_budget==budget;
        // Empty destination + closed DATA shared_ptr: no user copy/allocation,
        // displaced destruction or Native call occurs under this leaf.
        original->initialization_budget=budget;
        return true;
    }catch(...){return false;}
}
bool store_physical_api::can_issue_open_(const store_physical_open_original& owner)noexcept {
    return !owner.bound_&&!owner.original_&&owner.creator_==database_native_process()&&!foreign_protected_();
}
bool store_physical_api::issue_open_(store_physical_open_original& owner,original* issued)noexcept {
    if(!can_issue_open_(owner)||!issued||issued->creator!=database_native_process()||!issued->primary)return false;
    // The producer preadmits this exact reference BEFORE SQLite can return a
    // handle or run any VFS callback. No current-address lookup occurs here.
    auto retained=capture_issued_(issued);if(retained.inherited||!retained.value)return false;
    owner.original_=retained.value;retained.count=0;retained.value=nullptr;
    owner.bound_=true;owner.legacy_=false;return true;
}
bool store_physical_api::issue_legacy_open_(store_physical_open_original& owner)noexcept {
    if(!can_issue_open_(owner))return false;
    // Only the actual open producer calls this AFTER resource admission has
    // positively selected its uninstalled or exact memory/TEMP route.
    owner.bound_=true;owner.legacy_=true;return true;
}
bool store_physical_api::open_returned_(store_physical_open_original& owner,original* issued,sqlite3* native)noexcept {
    if(!owner.bound_||owner.creator_!=database_native_process()||foreign_protected_()||owner.actual_||
       owner.native_retired_||owner.original_!=issued||owner.legacy_!=(issued==nullptr))return false;
    owner.actual_=native;return true;
}
store_physical_api::ticket store_physical_api::capture_cleanup_(const store_physical_open_original& owner,sqlite3* native)noexcept {
    ticket result;result.creator=database_native_process();
    if(owner.creator_!=result.creator||foreign_protected_()||!owner.bound_||owner.native_retired_||
       owner.actual_!=native||!native){result.inherited=true;return result;}
    // Exact target provenance grants only its existing closed caller route;
    // no normal lifetime pin, policy token or generic completion is invented.
    if(owner.exclusive_target_&&owner.exclusive_open_attempted_&&!owner.original_&&!owner.legacy_)return result;
    if(owner.legacy_&&!owner.original_)return result;
    auto* primary=static_cast<original*>(owner.original_);
    if(!primary||primary->creator!=result.creator||!primary->primary){result.inherited=true;return result;}
    try{std::lock_guard lock(physical_api_leaf);
        if(primary->listed){
            if(!primary->current||primary->actual!=native){result.inherited=true;return result;}
            std::size_t count=0;
            for(auto* at=head_;at;at=at->next)if(at->current&&at->actual==native){
                if(at->references==UINT64_MAX||count==maximum_originals){result.inherited=true;return result;}++count;}
            for(auto* at=head_;at;at=at->next)if(at->current&&at->actual==native){
                ++at->references;result.values[result.count++]=at;}
        }else{
            // A failed publication never selects another current row. It may
            // only clean this actual preadmitted partial-open producer. Its
            // unknown registration debt still retains the lease/capsule.
            if(primary->completion_claimed||primary->completion_returned||primary->references==UINT64_MAX){result.inherited=true;return result;}
            ++primary->references;result.values[0]=primary;result.count=1;
        }
        result.value=primary;
    }catch(...){result.inherited=true;}
    return result;
}
void store_physical_api::capture_policy_(const store_physical_open_original& owner,store_physical_policy_original& token)noexcept {
    if(token.original_||token.creator_!=database_native_process()||owner.creator_!=database_native_process())return;
    // Actual bound primary provenance, not a new current-address lookup. A
    // later closed/superseded/legacy original cannot activate a new policy.
    auto current=capture_open_(owner,owner.actual_);
    if(current.inherited||!current.value||!current.value->primary)return;
    auto retained=capture_issued_(current.value);if(retained.inherited||!retained.value)return;
    token.original_=retained.value;retained.count=0;retained.value=nullptr;
}
void store_physical_api::release_policy_(store_physical_policy_original& token)noexcept {
    auto* actual=static_cast<original*>(std::exchange(token.original_,nullptr));
    if(!actual||token.creator_!=database_native_process()||physical_api_creator!=database_native_process())return;
    bool retire=false;
    try{
        {std::lock_guard lock(physical_api_leaf);
         if(!actual->references)return; // Retain malformed-reference debt.
         --actual->references;
         if(!actual->references&&actual->completion_returned){
             // Replace the final reference with this ONE original-owned tail.
             actual->references=1;actual->returned=original::returned_action::destroy;retire=true;
         }}
        if(retire)handoff_returned_tail_(actual);
    }catch(...){/* Preserve DATA reference debt; never manufacture ending. */}
}
bool store_physical_api::observer_policy_unsettled_(const store_physical_policy_original& token)noexcept {
    if(token.creator_!=database_native_process()||foreign_protected_())return true;
    const auto* actual=static_cast<original*>(token.original_);
    return !actual||actual->creator!=database_native_process()||store_physical_lifetime::observer_policy_unsettled_(actual->held);
}
void store_physical_api::note_observer_policy_unsettled_(const store_physical_policy_original& token)noexcept {
    if(token.creator_!=database_native_process()||foreign_protected_())return;
    const auto* actual=static_cast<original*>(token.original_);
    if(actual&&actual->creator==database_native_process())store_physical_lifetime::note_observer_policy_unsettled_(actual->held);
}
void store_physical_api::release_open_(store_physical_open_original& owner)noexcept {
    auto* original=static_cast<store_physical_api::original*>(std::exchange(owner.original_,nullptr));
    if(owner.creator_!=database_native_process())return;
    release_original_(original,owner.creator_);
}
store_physical_api::ticket store_physical_api::capture_open_(const store_physical_open_original& owner,sqlite3* native)noexcept {
    ticket result;result.creator=database_native_process();
    if(owner.creator_!=result.creator||foreign_protected_()){result.inherited=true;return result;}
    // A deferred target shell has no actual SQLite handle. This grants no
    // Native access; every nonnull connection requires its real bound receipt.
    if(!owner.bound_&&!native)return result;
    if(!owner.bound_||owner.native_retired_||owner.actual_!=native){result.inherited=true;return result;}
    if(owner.exclusive_target_&&owner.exclusive_open_attempted_&&native&&!owner.original_&&!owner.legacy_)return result;
    if(owner.legacy_&&!owner.original_)return result;
    auto* primary=static_cast<original*>(owner.original_);
    if(!primary||primary->creator!=result.creator){result.inherited=true;return result;}
    try{std::lock_guard lock(physical_api_leaf);
        // Missing/ended/superseded OWN original never falls back to legacy.
        if(!primary->primary||!primary->listed||!primary->current||primary->actual!=native){result.inherited=true;return result;}
        std::size_t count=0;
        for(auto* at=head_;at;at=at->next)if(at->current&&at->actual==native){
            if(at->references==UINT64_MAX||count==maximum_originals){result.inherited=true;return result;}++count;
        }
        for(auto* at=head_;at;at=at->next)if(at->current&&at->actual==native){
            ++at->references;result.values[result.count++]=at;
        }
        result.value=primary;
    }catch(...){result.inherited=true;}
    return result;
}
bool store_physical_api::native_custody_here_(original* actual)noexcept {
    if(!actual||actual->creator!=database_native_process())return false;
    for(auto* frame=store_physical_native_scope::current_;frame;frame=frame->previous_){
        if(frame->creator_!=database_native_process())return false;
        if(!frame->entered_)continue;
        for(std::size_t i=0;i<frame->count_;++i)
            if((frame->entered_mask_&(std::uint64_t(1)<<i))&&frame->originals_[i]==actual)return true;
    }
    return false;
}
bool store_physical_api::native_continuation_(original* actual)noexcept {
    if(!actual||actual->creator!=database_native_process())return false;
    for(auto* frame=store_physical_native_scope::current_;frame;frame=frame->previous_){
        if(frame->creator_!=database_native_process())return false;
        if(!frame->entered_||frame->cleanup_only_)continue; // cleanup/refused frames lend no SQL admission
        for(std::size_t i=0;i<frame->count_;++i)
            if((frame->entered_mask_&(std::uint64_t(1)<<i))&&frame->originals_[i]==actual)return true;
    }
    return false;
}
bool store_physical_api::seal_open_(const store_physical_open_original& owner,std::chrono::steady_clock::time_point end)noexcept {
    if(!owner.bound_||owner.legacy_||owner.creator_!=database_native_process()||foreign_protected_())return false;
    auto* actual=static_cast<original*>(owner.original_);
    if(!actual||actual->creator!=database_native_process()||native_custody_here_(actual))return false;
    // This is admission quiescence DATA ONLY, not physical close or unlink.
    return store_physical_lifetime::seal_native_and_wait_(actual->held,end);
}
bool store_physical_api::observer_policy_unsettled_(const store_physical_open_original& owner)noexcept {
    if(!owner.bound_||owner.legacy_||owner.creator_!=database_native_process()||foreign_protected_())return true;
    const auto* original=static_cast<store_physical_api::original*>(owner.original_);
    return !original||original->creator!=database_native_process()||
        store_physical_lifetime::observer_policy_unsettled_(original->held);
}
void store_physical_api::note_observer_policy_unsettled_(const store_physical_open_original& owner)noexcept {
    if(!owner.bound_||owner.legacy_||owner.creator_!=database_native_process()||foreign_protected_())return;
    const auto* original=static_cast<store_physical_api::original*>(owner.original_);
    if(original&&original->creator==database_native_process())store_physical_lifetime::note_observer_policy_unsettled_(original->held);
}
void store_physical_api::enter_captured_native_(ticket&& actual,store_physical_native_scope& scope)noexcept {
    static_assert(maximum_originals==64);scope.creator_=actual.creator;
    if(actual.inherited){scope.entered_=false;return;}
    for(std::size_t i=0;i<actual.count;++i){
        auto* original=actual.values[i];scope.originals_[scope.count_++]=original;
        // Continuation is per immutable original, NOT merely per primary
        // connection. A newly attached target cannot borrow an older frame.
        if(store_physical_lifetime::native_scope_entered_(original->held,scope.cleanup_only_||native_continuation_(original)))scope.entered_mask_|=std::uint64_t(1)<<i;
        else scope.entered_=false;
    }
    actual.count=0;actual.value=nullptr;
    // Even a genuine legacy/memory route observes its managed leaf nesting.
    // An empty frame has no originals and grants NO continuation/resource SQL.
    scope.previous_=store_physical_native_scope::current_;
    store_physical_native_scope::current_=&scope;scope.registered_=true;
}
void store_physical_api::enter_database_native_(const ::lattice::database& owner,store_physical_native_scope& scope)noexcept {
    if(!owner.creator_current_()){scope.entered_=false;return;}
    enter_native_scope_(owner.physical_open_original_,owner.db_,scope);
}
void store_physical_api::enter_database_cleanup_(const ::lattice::database& owner,store_physical_native_scope& scope)noexcept {
    if(!owner.creator_current_()){scope.entered_=false;return;}
    if(!owner.db_){scope.creator_=database_native_process();return;} // no Native entry
    enter_cleanup_scope_(owner.physical_open_original_,owner.db_,scope);
}
void store_physical_api::enter_cleanup_scope_(const store_physical_open_original& owner,sqlite3* native,store_physical_native_scope& scope)noexcept {
    scope.cleanup_only_=true;
    enter_captured_native_(capture_cleanup_(owner,native),scope);
}
void store_physical_api::enter_native_scope_(sqlite3* native,store_physical_native_scope& scope)noexcept {
    enter_captured_native_(capture_(native),scope);
}
void store_physical_api::enter_native_scope_(const store_physical_open_original& owner,sqlite3* native,store_physical_native_scope& scope)noexcept {
    enter_captured_native_(capture_open_(owner,native),scope);
}
void store_physical_api::leave_native_scope_(store_physical_native_scope& scope)noexcept {
    if(scope.creator_!=database_native_process()||
       (physical_api_creator!=database_native_process()&&(scope.count_||foreign_protected_())))return;
    // A newly-issued current-creator EMPTY legacy observation touches no
    // inherited physical original/counter/registry. Preserve that old route.
    // Keep every exact release below queued until this whole return's counters
    // and metadata leaves have ended, even after unlinking its TLS node.
    const bool return_overflow=native_return_work_==UINT64_MAX;
    if(!return_overflow)++native_return_work_;
    if(scope.registered_){
        // Explicit outer-scope release is allowed while a real nested engine
        // frame remains. Unlink this exact live TLS node, never a current owner.
        auto** link=&store_physical_native_scope::current_;
        while(*link&&*link!=&scope)link=&(*link)->previous_;
        if(*link==&scope)*link=scope.previous_;
        scope.registered_=false;scope.previous_=nullptr;
    }
    if(return_overflow)return; // TLS unlinked; retain actual counter/reference debt.
    while(scope.count_){const auto i=--scope.count_;auto* original=static_cast<store_physical_api::original*>(scope.originals_[i]);
        scope.originals_[i]=nullptr;
        if(scope.entered_mask_&(std::uint64_t(1)<<i))store_physical_lifetime::native_scope_returned_(original->held);
        release_original_(original,scope.creator_);
    }
    scope.entered_mask_=0;
    --native_return_work_;
    returned_tail();
}
store_physical_policy_original::store_physical_policy_original()noexcept:creator_(database_native_process()){}
store_physical_policy_original::store_physical_policy_original(store_physical_policy_original&& other)noexcept
    :original_(std::exchange(other.original_,nullptr)),creator_(other.creator_){}
store_physical_policy_original& store_physical_policy_original::operator=(store_physical_policy_original&& other)noexcept {
    if(this!=&other){store_physical_api::release_policy_(*this);original_=std::exchange(other.original_,nullptr);creator_=other.creator_;}return *this;
}
store_physical_policy_original::~store_physical_policy_original()noexcept{store_physical_api::release_policy_(*this);}
store_physical_policy_original store_physical_policy_original::capture_(const store_physical_open_original& owner)noexcept {
    store_physical_policy_original token;store_physical_api::capture_policy_(owner,token);return token;
}
bool store_physical_policy_original::observer_policy_unsettled()const noexcept{return store_physical_api::observer_policy_unsettled_(*this);}
void store_physical_policy_original::note_observer_policy_unsettled()noexcept{store_physical_api::note_observer_policy_unsettled_(*this);}
store_physical_open_original::store_physical_open_original()noexcept:creator_(database_native_process()){}
store_physical_open_original::store_physical_open_original(store_physical_open_original&& other)noexcept
    :original_(std::exchange(other.original_,nullptr)),actual_(std::exchange(other.actual_,nullptr)),creator_(other.creator_),
     bound_(std::exchange(other.bound_,false)),legacy_(std::exchange(other.legacy_,false)),native_retired_(std::exchange(other.native_retired_,false)),
     exclusive_target_(std::exchange(other.exclusive_target_,false)),exclusive_open_attempted_(std::exchange(other.exclusive_open_attempted_,false)){}
store_physical_open_original& store_physical_open_original::operator=(store_physical_open_original&& other)noexcept {
    if(this!=&other){store_physical_api::release_open_(*this);original_=std::exchange(other.original_,nullptr);
        actual_=std::exchange(other.actual_,nullptr);creator_=other.creator_;bound_=std::exchange(other.bound_,false);legacy_=std::exchange(other.legacy_,false);native_retired_=std::exchange(other.native_retired_,false);
        exclusive_target_=std::exchange(other.exclusive_target_,false);exclusive_open_attempted_=std::exchange(other.exclusive_open_attempted_,false);}
    return *this;
}
store_physical_open_original::~store_physical_open_original()noexcept{store_physical_api::release_open_(*this);}
bool store_physical_open_original::retain_initialization_budget_(
    const std::shared_ptr<const database_initialization_budget>& budget)noexcept {
    return store_physical_api::retain_initialization_budget_(*this,budget);
}
bool store_physical_open_original::issue_exclusive_target_(std::uint64_t issuer)noexcept {
    // database alone calls this after its SAME retained constructor scope and
    // both genuine target preopen checks. No allocation or pathname adoption.
    if(issuer!=database_native_process()||creator_!=issuer||!store_physical_api::can_issue_open_(*this))return false;
    bound_=true;exclusive_target_=true;return true;
}
int store_physical_open_original::open_exclusive_target_(const char* file,sqlite3** actual,int flags,
    const char* vfs,std::chrono::steady_clock::time_point end)noexcept {
    const auto issuer=creator_;
    if(issuer!=database_native_process()||store_physical_api::foreign_protected_())return SQLITE_MISUSE;
    if(!bound_||!exclusive_target_||exclusive_open_attempted_||original_||legacy_||native_retired_||actual_)return SQLITE_MISUSE;
    exclusive_open_attempted_=true; // Never retry this original, including a null/error return.
    if(!actual)return SQLITE_MISUSE;
    *actual=nullptr;
#ifdef SQLITE_OPEN_NOFOLLOW
    constexpr int required=SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX|SQLITE_OPEN_NOFOLLOW;
    if(!file||!*file||!vfs||!*vfs||flags!=required||std::chrono::steady_clock::now()>=end)return SQLITE_CANTOPEN;
    // Deliberately bypass normal-factory acquisition: the true target already
    // owns its exclusive descriptor/custom VFS and admitted constructor scope.
    store_physical_api::api_scope returned;if(!returned.entered())return SQLITE_MISUSE;
    const int rc=sqlite3_open_v2(file,actual,flags,vfs);
    if(issuer!=database_native_process()){*actual=nullptr;return SQLITE_MISUSE;}
    actual_=*actual; // Bind ONLY this real return, including a partial error handle.
    store_physical_api::unresourced_open_returned(actual_); // stale-map denial, never adoption
    return rc;
#else
    (void)file;(void)flags;(void)vfs;(void)end;return SQLITE_CANTOPEN;
#endif
}
bool store_physical_open_original::matches_open_(sqlite3* actual)const noexcept {
    return creator_==database_native_process()&&bound_&&!native_retired_&&actual_==actual&&
        (!exclusive_target_||(exclusive_open_attempted_&&actual));
}
bool store_physical_open_original::seal_and_wait_(std::chrono::steady_clock::time_point end)const noexcept{return store_physical_api::seal_open_(*this,end);}
bool store_physical_open_original::observer_policy_unsettled()const noexcept{return store_physical_api::observer_policy_unsettled_(*this);}
void store_physical_open_original::note_observer_policy_unsettled()noexcept{store_physical_api::note_observer_policy_unsettled_(*this);}
store_physical_cleanup_scope::store_physical_cleanup_scope(const ::lattice::database& owner)noexcept{store_physical_api::enter_database_cleanup_(owner,held_);}
store_physical_cleanup_scope::store_physical_cleanup_scope(const store_physical_open_original& owner,sqlite3* actual)noexcept{store_physical_api::enter_cleanup_scope_(owner,actual,held_);}
store_physical_native_scope::store_physical_native_scope(sqlite3* actual)noexcept{store_physical_api::enter_native_scope_(actual,*this);}
store_physical_native_scope::store_physical_native_scope(const ::lattice::database& owner)noexcept{store_physical_api::enter_database_native_(owner,*this);}
store_physical_native_scope::store_physical_native_scope(const store_physical_open_original& owner,sqlite3* actual)noexcept{store_physical_api::enter_native_scope_(owner,actual,*this);}
store_physical_native_scope::store_physical_native_scope(const store_physical_open_original* owner,sqlite3* actual)noexcept{
    if(owner)store_physical_api::enter_native_scope_(*owner,actual,*this);
    else if(actual)entered_=false; // null managed-route has no Native access
}
store_physical_native_scope::~store_physical_native_scope()noexcept{reset();}
void store_physical_native_scope::reset()noexcept{store_physical_api::leave_native_scope_(*this);}
void store_physical_api::release_(ticket& held)noexcept {
    const auto creator=held.creator;held.value=nullptr;
    while(held.count){auto* actual=held.values[--held.count];held.values[held.count]=nullptr;release_original_(actual,creator);}
}
bool store_physical_api::managed_return_here_()noexcept {
    // The immutable process fence precedes even reading inherited TLS links.
    if(physical_api_creator!=database_native_process())return true;
    return native_return_work_||store_physical_native_scope::current_||
        api_scope::current_||store_physical_lifetime::api_return::current_;
}
void store_physical_api::retire_initialization_budget_(database_initialization_budget* actual)noexcept {
    // The last shared-owner deleter transfers DELETE, not a new reference.
    // Creator checks precede inherited TLS, node or lease access.
    if(physical_api_creator!=database_native_process()||!actual||
       actual->creator_!=database_native_process())return;
    if(actual->returned_delete_)return; // malformed duplicate: retain debt
    actual->returned_delete_=true;
    actual->returned_next_=returned_budgets_;returned_budgets_=actual;
    returned_tail();
}
void store_physical_api::handoff_returned_tail_(original* actual)noexcept {
    if(!actual||physical_api_creator!=database_native_process()||actual->creator!=database_native_process())return;
    // Caller owns this original's unique synthetic reference/action. The
    // registry leaf was released before publishing this preallocated link.
    actual->returned_next=returned_tails_;returned_tails_=actual;
    returned_tail();
}
void store_physical_api::finish_returned_tail_(original* actual)noexcept {
    if(!actual||physical_api_creator!=database_native_process()||actual->creator!=database_native_process())return;
    const auto creator=actual->creator;
    const bool complete=actual->returned==original::returned_action::complete;
    if(complete&&actual->completion){
        if(returned_tail_observation_)++returned_tail_observation_->completed;
        actual->completion(actual->completion_cookie);
    }
    // Fixed completion/capture destruction is still a possible foreign return.
    // Abandon inherited capsule before its mutex, DATA holders or further work.
    if(creator!=database_native_process())return;
    bool destroy=false;
    try{
        {std::lock_guard lock(physical_api_leaf);
         if(complete)actual->completion_returned=true;
         actual->returned=original::returned_action::none;
         if(!actual->references)return; // Preserve malformed-reference debt.
         --actual->references;
         destroy=!actual->references&&actual->completion_returned;
         if(destroy&&managed_return_here_()){
             actual->references=1;actual->returned=original::returned_action::destroy;
         }}
        if(!destroy)return;
        if(managed_return_here_()){handoff_returned_tail_(actual);return;}
        if(returned_tail_observation_)++returned_tail_observation_->destroyed;
        delete actual; // typed DATA budget/held pin end only off BOTH real stacks
    }catch(...){/* Keep actual completion/deletion debt; never infer refund. */}
}
void store_physical_api::returned_tail()noexcept {
    if(physical_api_creator!=database_native_process()||draining_tails_||managed_return_here_())return;
    draining_tails_=true;
    while((returned_tails_||returned_budgets_)&&!managed_return_here_()){
        if(returned_tails_){
            auto* actual=returned_tails_;returned_tails_=actual->returned_next;actual->returned_next=nullptr;
            finish_returned_tail_(actual);
        }else{
            auto* actual=returned_budgets_;returned_budgets_=actual->returned_next_;actual->returned_next_=nullptr;
            // No Native/API/VFS frame or own-return counter work remains.
            // The same embedded admission record returns its actual DATA
            // credit only here, including pre-grant/control-allocation failure.
            actual->delete_after_return_();
        }
        if(physical_api_creator!=database_native_process())return; // inherited lists abandoned
    }
    draining_tails_=false;
}
void store_physical_api::release_original_(original* actual,std::uint64_t creator)noexcept {
    if(!actual||creator!=database_native_process()||physical_api_creator!=database_native_process())return;
    const auto ended=store_physical_lifetime::observe_(actual->held).physical_ended;
    bool retire=false;
    try {
        {std::lock_guard lock(physical_api_leaf);
         if(ended&&actual->listed&&!actual->completion_claimed){
             auto** cursor=&head_;
             while(*cursor&&*cursor!=actual)cursor=&(*cursor)->next;
             if(*cursor==actual){
                 *cursor=actual->next;actual->listed=false;actual->current=false;
                 actual->completion_claimed=true;
                 // Exchange the registry reference for ONE returned-tail
                 // reference (no overflow or extra allocation).
                 actual->returned=original::returned_action::complete;retire=true;
             }
         }
         if(!actual->references)return;
         --actual->references;
         if(!retire&&!actual->references&&actual->completion_returned){
             actual->references=1;actual->returned=original::returned_action::destroy;retire=true;
         }}
        if(retire)handoff_returned_tail_(actual);
    }catch(...){/* Preserve actual registry/lease ending debt. */}
}
store_physical_api::ticket::~ticket()noexcept {store_physical_api::release_(*this);}
store_physical_api::api_scope::api_scope()noexcept:creator_(database_native_process()) {
    if(store_physical_api::foreign_protected_()){entered_=false;return;}
    previous_=current_;current_=this;registered_=true;
}
store_physical_api::api_scope::api_scope(const ticket& actual)noexcept:creator_(actual.creator) {
    if(actual.inherited||creator_!=database_native_process()||
       (actual.count&&creator_!=physical_api_creator)||store_physical_api::foreign_protected_()){entered_=false;return;}
    // A valid empty ticket is still an observation of this genuine wrapper C
    // call. It has no physical originals and grants NO SQL/continuation right.
    previous_=current_;current_=this;registered_=true;
    for(std::size_t i=0;i<actual.count;++i){
        auto* frame=::new(static_cast<void*>(storage_+count_*sizeof(store_physical_lifetime::api_return)))
            store_physical_lifetime::api_return(actual.values[i]->held);
        ++count_;
        if(!frame->entered()){entered_=false;return;}
    }
}
store_physical_api::api_scope::~api_scope()noexcept {
    if(creator_!=database_native_process()||
       (creator_!=physical_api_creator&&(count_||store_physical_api::foreign_protected_())))return;
    // Only a NEW current-process empty observation may unlink here without
    // parent-registry access. Inherited observations still creator-refuse.
    // Keep this observation linked while every actual per-original counter,
    // held-pin and metadata return tail runs, including partial admission.
    while(count_)at_(--count_)->~api_return();
    if(registered_){
        auto** link=&current_;while(*link&&*link!=this)link=&(*link)->previous_;
        if(*link==this)*link=previous_;
        registered_=false;previous_=nullptr;
    }
    store_physical_api::returned_tail();
}

struct store_physical_api::attachment_operation::implementation {
    const std::uint64_t creator=database_native_process();
    store_resource_open resource;
    ticket retained;
    std::optional<ticket> call;
    std::optional<api_scope> frame;
    store_physical_vfs* member=nullptr;
    bool detach=false,entered=false,returned=false;
    implementation(store_resource_open value,ticket prior):resource(std::move(value)),retained(std::move(prior)){}
};
store_physical_api::attachment_operation::attachment_operation(implementation* value)noexcept:impl_(value){}
store_physical_api::attachment_operation::attachment_operation(attachment_operation&& other)noexcept
    :impl_(std::exchange(other.impl_,nullptr)){}
store_physical_api::attachment_operation::~attachment_operation()noexcept {
    auto* value=impl_;if(!value||value->creator!=database_native_process())return;
    if(value->member&&!value->returned)value->member->member_returned_(value->detach,value->entered,false);
    // Actual synchronous SQLite call, permit and caller Native leaf have
    // returned before this owned frame retires. Capture destruction is offleaf.
    value->frame.reset();value->member=nullptr;
    delete value; // call tickets follow frame, then entry SH and exact retained originals
}
void store_physical_api::attachment_operation::enter(){
    auto* value=impl_;
    if(!value||value->creator!=database_native_process()||!value->member||value->entered||value->returned||!value->call)
        throw db_error("resource attachment first call unavailable");
    value->frame.emplace(*value->call);
    if(!value->frame->entered()||!value->member->member_entered_(value->detach))
        throw db_error("resource attachment first call original changed");
    value->entered=true;
}
void store_physical_api::attachment_operation::complete(){
    auto* value=impl_;
    if(!value||value->creator!=database_native_process()||!value->member||!value->entered||value->returned)
        throw db_error("resource attachment actual return unavailable");
    if(!value->member->member_returned_(value->detach,true,true))
        throw db_error("resource attachment return DATA unproved");
    value->returned=true;
}
store_physical_api::attachment_operation store_physical_api::enroll_attachment(sqlite3* native,
    const std::string& path,const std::string& alias,const ::lattice::physical_store_identity& expected,
    std::int64_t token,int attached_limit,std::chrono::steady_clock::time_point end){
    if(foreign_protected_()||std::chrono::steady_clock::now()>=end)throw db_error("resource attachment admission expired or inherited");
    auto prior=capture_(native);
    if(prior.inherited||!prior.value||!prior.value->primary||prior.value->completion!=&store_physical_vfs::complete_||
       !prior.value->completion_cookie)throw db_error("resource attachment actual MAIN original missing");
    auto* root=static_cast<store_physical_vfs*>(prior.value->completion_cookie);
    auto resource=store_resource_open::acquire_(path,false,end);
    if(!resource.impl_)throw db_error("resource attachment target is not enrolled");
    const auto creator=database_native_process();
    auto* state=new attachment_operation::implementation(std::move(resource),std::move(prior));
    attachment_operation result(state);
    if(creator!=database_native_process())throw db_error("resource attachment allocation changed creator");
    state->member=state->resource.enroll_(*root,native,alias,expected,token,attached_limit);
    if(creator!=database_native_process())throw db_error("resource attachment publication changed creator");
    state->call.emplace(capture_(native));
    if(state->call->inherited||!state->call->value)throw db_error("resource attachment complete original set unavailable");
    return result;
}
store_physical_api::attachment_operation store_physical_api::retire_attachment(sqlite3* native,
    const std::string& path,const std::string& alias,std::int64_t token){
    if(foreign_protected_())throw db_error("resource detach inherited");
    auto prior=capture_(native);
    if(prior.inherited||!prior.value||!prior.value->primary||prior.value->completion!=&store_physical_vfs::complete_||
       !prior.value->completion_cookie)throw db_error("resource detach actual MAIN original missing");
    auto* root=static_cast<store_physical_vfs*>(prior.value->completion_cookie);
    const auto creator=database_native_process();
    auto* state=new attachment_operation::implementation({},std::move(prior));
    attachment_operation result(state);
    if(creator!=database_native_process())throw db_error("resource detach allocation changed creator");
    state->detach=true;state->member=root->find_member_(native,path,alias,token);
    state->call.emplace(capture_(native));
    if(state->call->inherited||!state->call->value)throw db_error("resource detach complete original set unavailable");
    return result;
}

int store_physical_api::open(const char* file,sqlite3** actual,int flags,const char* vfs,
    std::chrono::steady_clock::time_point original_end,store_physical_open_original* issued)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    if(!actual)return SQLITE_MISUSE;
    *actual=nullptr;
    if(issued&&!can_issue_open_(*issued))return SQLITE_MISUSE;
    const auto creator=database_native_process();
    try {
        if(!file)return SQLITE_MISUSE;
        // This producer either admits the genuine fixed-account resource
        // context, or positively establishes the unchanged uninstalled route.
        // Installed/failed/foreign contexts never fall back after refusal.
        auto resource=store_resource_open::acquire_(file,(flags&SQLITE_OPEN_CREATE)!=0,original_end,(flags&SQLITE_OPEN_URI)!=0);
        if(resource.impl_){
            auto* lower=sqlite3_vfs_find(vfs);
            if(!lower)return SQLITE_CANTOPEN;
#ifdef SQLITE_OPEN_NOFOLLOW
            flags=(flags&~SQLITE_OPEN_CREATE)|SQLITE_OPEN_NOFOLLOW;
#else
            return SQLITE_CANTOPEN;
#endif
            return resource.open_(lower,actual,flags,issued);
        }
        if(issued&&!issue_legacy_open_(*issued))return SQLITE_MISUSE;
        api_scope returned;if(!returned.entered())return SQLITE_MISUSE;
        const int rc=sqlite3_open_v2(file,actual,flags,vfs);
        if(creator!=database_native_process()){*actual=nullptr;return SQLITE_MISUSE;}
        unresourced_open_returned(*actual);
        if(issued&&!open_returned_(*issued,nullptr,*actual))return SQLITE_CANTOPEN;
        return rc;
    }catch(...){return SQLITE_CANTOPEN;}
}
int store_physical_api::open_initializing_(const char* file,sqlite3** actual,int flags,const char* vfs,
    std::chrono::steady_clock::time_point original_end,store_physical_open_original& issued,
    store_resource_initialization_original& recipient,store_resource_initialization_original* supplied) {
    if(foreign_protected_()||!actual||!file||!can_issue_open_(issued)||recipient.creator_||recipient.impl_)
        throw db_error("initializing Native open lost its exact empty producer originals");
    *actual=nullptr;const auto creator=database_native_process();
    if(supplied&&(!supplied->transport_current_()||original_end>supplied->original_deadline_()))
        throw db_error("normal factory original changed creator/builder/end before Native");
    // A supplied factory already created/selected this exact incarnation (or
    // genuinely selected the legacy domain). Never create a second original or
    // recreate a changed namespace while transporting that SAME factory owner.
    auto resource=store_resource_open::acquire_(file,(flags&SQLITE_OPEN_CREATE)!=0&&!supplied,
        original_end,(flags&SQLITE_OPEN_URI)!=0,!supplied);
    if(resource.impl_) {
        auto* lower=sqlite3_vfs_find(vfs);if(!lower)throw db_error("actual initializing lower VFS unavailable");
#ifdef SQLITE_OPEN_NOFOLLOW
        flags=(flags&~SQLITE_OPEN_CREATE)|SQLITE_OPEN_NOFOLLOW;
#else
        throw db_error("actual initializing Native nofollow support unavailable");
#endif
        return resource.open_initializing_(lower,actual,flags,issued,recipient,supplied);
    }
    if(supplied) {
        if(supplied->impl_)throw db_error("actual normal factory resource domain disappeared before Native");
        supplied->transfer_to_(recipient); // genuine legacy factory DATA/end only
    }
    if(!issue_legacy_open_(issued))throw db_error("actual initializing legacy original unavailable");
    api_scope returned;if(!returned.entered())throw db_error("actual initializing legacy API frame unavailable");
    const int rc=sqlite3_open_v2(file,actual,flags,vfs);
    if(creator!=database_native_process()){*actual=nullptr;throw db_error("inherited initializing Native return");}
    unresourced_open_returned(*actual);
    if(!open_returned_(issued,nullptr,*actual))throw db_error("actual initializing legacy return binding unavailable");
    return rc;
}
const store_physical_open_original* store_physical_api::mutex_owner_(const ::lattice::database& owner)noexcept {
    // Creator denial precedes even reading a borrowed inherited owner field.
    return owner.creator_current_()?&owner.physical_open_original_:nullptr;
}
void store_physical_api::return_mutex_(const store_physical_open_original& original,sqlite3_mutex* mutex)noexcept {
    if(original.creator_!=database_native_process()||foreign_protected_()||
       !original.bound_||original.native_retired_)return;
    // Fixed nil-default DATA observation at the real Native attempt boundary.
    // It never authorizes/changes the return and invokes no arbitrary callback.
    api_scope returned;if(!returned.entered())return;
    if(mutex_return_observation_)++*mutex_return_observation_;
    sqlite3_mutex_leave(mutex);
}
store_physical_mutex_return::store_physical_mutex_return(const ::lattice::database& owner,sqlite3_mutex* mutex)noexcept
    :original_(store_physical_api::mutex_owner_(owner)),mutex_(mutex){}
store_physical_mutex_return::store_physical_mutex_return(const store_physical_open_original& owner,sqlite3_mutex* mutex)noexcept
    :original_(&owner),mutex_(mutex){}
store_physical_mutex_return::~store_physical_mutex_return()noexcept{reset();}
void store_physical_mutex_return::reset()noexcept {
    const auto* original=std::exchange(original_,nullptr);auto* mutex=std::exchange(mutex_,nullptr);
    if(original&&mutex)store_physical_api::return_mutex_(*original,mutex);
}
bool store_physical_api::process_current()noexcept{return !foreign_protected_();}
bool store_physical_api::enrolled(sqlite3* native)noexcept{
    if(foreign_protected_())return true; // denial, never an inherited lookup
    auto actual=capture_(native);return actual.inherited||actual.value;
}
bool store_physical_api::main_for_lower(sqlite3* native,sqlite3_vfs* expected,sqlite3_file*& inner)noexcept {
    inner=nullptr;
    if(foreign_protected_())return false;
    auto actual=capture_(native);if(actual.inherited)return false;
    if(!actual.value)return true; // Exact legacy wrapper checks remain at caller.
    if(actual.value->completion!=&store_physical_vfs::complete_||!actual.value->completion_cookie)return false;
    auto* owner=static_cast<store_physical_vfs*>(actual.value->completion_cookie);
    sqlite3_vfs* observed=nullptr;sqlite3_file* outer=nullptr;sqlite3_vfs* lower=nullptr;
    api_scope returned(actual);if(!returned.entered())return false;
    const int vfs_result=sqlite3_file_control(native,"main",SQLITE_FCNTL_VFS_POINTER,&observed);
    if(actual.creator!=database_native_process()){inner=nullptr;return false;}
    if(vfs_result!=SQLITE_OK){inner=nullptr;return false;}
    const int file_result=sqlite3_file_control(native,"main",SQLITE_FCNTL_FILE_POINTER,&outer);
    if(actual.creator!=database_native_process()){inner=nullptr;return false;}
    if(file_result!=SQLITE_OK||!owner->identity_layer_(outer,observed,inner,lower)||lower!=expected){inner=nullptr;return false;}
    return true;
}
sqlite3_vfs* store_physical_api::identity_vfs(sqlite3* native,const char* schema,sqlite3_vfs* observed,sqlite3_file*& inner)noexcept {
    inner=nullptr;
    if(foreign_protected_())return nullptr;
    auto actual=capture_(native);if(actual.inherited)return nullptr;
    if(!actual.value)return observed;
    if(!schema||!*schema||actual.value->completion!=&store_physical_vfs::complete_||!actual.value->completion_cookie)return nullptr;
    auto* owner=static_cast<store_physical_vfs*>(actual.value->completion_cookie);
    sqlite3_file* outer=nullptr;sqlite3_vfs* lower=nullptr;
    api_scope returned(actual);if(!returned.entered())return nullptr;
    const int file_result=sqlite3_file_control(native,schema,SQLITE_FCNTL_FILE_POINTER,&outer);
    if(actual.creator!=database_native_process()){inner=nullptr;return nullptr;}
    if(file_result!=SQLITE_OK||!owner->identity_layer_(outer,observed,inner,lower)){inner=nullptr;return nullptr;}
    return lower;
}
int store_physical_api::prepare(sqlite3* owner,const char* sql,int size,unsigned int flags,sqlite3_stmt** statement,const char** tail,bool v3)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);if(!returned.entered())return SQLITE_MISUSE;
    return v3?sqlite3_prepare_v3(owner,sql,size,flags,statement,tail):sqlite3_prepare_v2(owner,sql,size,statement,tail);
}
int store_physical_api::clear_bindings(sqlite3_stmt* statement)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    if(!statement)return sqlite3_clear_bindings(statement);
    auto actual=capture_(sqlite3_db_handle(statement));if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_clear_bindings(statement);
}
int store_physical_api::create_function(sqlite3* owner,const char* name,int n,int encoding,void* data,
    void(*function)(sqlite3_context*,int,sqlite3_value**),void(*step)(sqlite3_context*,int,sqlite3_value**),
    void(*final)(sqlite3_context*),void(*destroy)(void*),bool v2,bool* actual_native_entered)noexcept {
    if(actual_native_entered)*actual_native_entered=false;
    const auto creator=database_native_process();
    // Never release inherited Native-bearing callback payload on refusal.
    if(foreign_protected_())return SQLITE_MISUSE;
    int result=SQLITE_MISUSE;bool native_entered=false;
    {
        auto actual=capture_(owner);
        if(!actual.inherited){
            api_scope returned(actual);
            if(returned.entered()){
                // Actual SQLite owns v2's failure/success xDestroy contract.
                // Replacing a registration may destroy its old callback.
                native_entered=true;
                if(actual_native_entered)*actual_native_entered=true;
                result=v2?sqlite3_create_function_v2(owner,name,n,encoding,data,function,step,final,destroy):
                    sqlite3_create_function(owner,name,n,encoding,data,function,step,final);
            }
        }
    } // Every actual API/registry/physical scope ended BEFORE user destruction.
    // SQLite v2 consumes its userdata even on failure. Preserve that contract
    // for genuine same-creator pre-Native refusal too, exactly once. The local
    // original PID and registry PID must both still match; a callback/fork can
    // never turn this into inherited cleanup. No Native-bearing local follows.
    if(!native_entered&&v2&&destroy&&creator==database_native_process()&&creator==physical_api_creator)
        destroy(data);
    return result;
}
int store_physical_api::finalize(sqlite3_stmt* statement)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    if(!statement)return sqlite3_finalize(nullptr);
    auto actual=capture_(sqlite3_db_handle(statement));
    if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);
    if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_finalize(statement); // actual tail destructor runs AFTER this C API returns
}
int store_physical_api::step(sqlite3_stmt* statement,bool reset)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    if(!statement)return reset?sqlite3_reset(nullptr):sqlite3_step(nullptr);
    auto actual=capture_(sqlite3_db_handle(statement));if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);
    if(!returned.entered())return SQLITE_MISUSE;
    return reset?sqlite3_reset(statement):sqlite3_step(statement);
}
int store_physical_api::exec(sqlite3* owner,const char* sql,int(*callback)(void*,int,char**,char**),void* data,char** error)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);
    if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_exec(owner,sql,callback,data,error);
}
int store_physical_api::checkpoint(sqlite3* owner,const char* schema,int mode,int* log,int* done)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);
    if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_wal_checkpoint_v2(owner,schema,mode,log,done);
}
int store_physical_api::blob_close(sqlite3_blob* blob,sqlite3* owner)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    if(!blob)return sqlite3_blob_close(nullptr);
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);
    if(!returned.entered())return SQLITE_MISUSE;
    return sqlite3_blob_close(blob);
}
int store_physical_api::close(sqlite3* owner,bool deferred)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    auto actual=capture_(owner);if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);
    if(!returned.entered())return SQLITE_MISUSE;
    if(actual.value&&actual.value->completion==&store_physical_vfs::complete_&&actual.value->completion_cookie)
        static_cast<store_physical_vfs*>(actual.value->completion_cookie)->root_->seal_();
    const int result=deferred?sqlite3_close_v2(owner):sqlite3_close(owner);
    for(std::size_t i=0;i<actual.count;++i)
        store_physical_lifetime::native_close_returned_(actual.values[i]->held,deferred,result);
    return result;
}
int store_physical_api::close_owned(store_physical_open_original& owner,sqlite3* native,bool deferred)noexcept {
    if(foreign_protected_())return SQLITE_MISUSE;
    auto actual=capture_cleanup_(owner,native);if(actual.inherited)return SQLITE_MISUSE;
    api_scope returned(actual);if(!returned.entered())return SQLITE_MISUSE;
    if(auto* root=actual_owner_(actual))root->root_->seal_();
    const int rc=deferred?sqlite3_close_v2(native):sqlite3_close(native);
    if(owner.creator_!=database_native_process())return rc;
    // BUSY is never rewritten; it permits only the actual fallback. A genuine
    // close/close_v2 success makes this pointer unusable for repeat cleanup.
    if(rc==SQLITE_OK)owner.native_retired_=true;
    for(std::size_t i=0;i<actual.count;++i)
        store_physical_lifetime::native_close_returned_(actual.values[i]->held,deferred,rc);
    return rc;
}
void store_physical_api::unresourced_open_returned(sqlite3* actual)noexcept {
    if(!actual||physical_api_creator!=database_native_process()||!physical_api_used.load(std::memory_order_acquire))return;
    try{std::lock_guard lock(physical_api_leaf);
        for(auto* at=head_;at;at=at->next)
            if(at->actual==actual)at->current=false;
    }catch(...){/* No resource grant is created by this denial-only observation. */}
}
int store_physical_open_v2_at(const char* file,sqlite3** actual,int flags,const char* vfs,std::chrono::steady_clock::time_point end)noexcept{return store_physical_api::open(file,actual,flags,vfs,end);}
int store_physical_open_v2(const char* file,sqlite3** actual,int flags,const char* vfs)noexcept{return store_physical_api::open(file,actual,flags,vfs,std::chrono::steady_clock::now()+std::chrono::seconds(30));}
int store_physical_open_v2_owned_at(const char* file,sqlite3** actual,int flags,const char* vfs,std::chrono::steady_clock::time_point end,store_physical_open_original& issued)noexcept{return store_physical_api::open(file,actual,flags,vfs,end,&issued);}
int store_physical_close_owned(store_physical_open_original& issued,sqlite3* actual)noexcept{return store_physical_api::close_owned(issued,actual,false);}
int store_physical_close_v2_owned(store_physical_open_original& issued,sqlite3* actual)noexcept{return store_physical_api::close_owned(issued,actual,true);}
bool store_physical_process_current()noexcept{return store_physical_api::process_current();}
bool store_physical_is_enrolled(sqlite3* native)noexcept{return store_physical_api::enrolled(native);}
bool store_physical_main_for_lower(sqlite3* native,sqlite3_vfs* expected,sqlite3_file*& inner)noexcept{return store_physical_api::main_for_lower(native,expected,inner);}
sqlite3_vfs* store_physical_identity_vfs(sqlite3* native,const char* schema,sqlite3_vfs* observed,sqlite3_file*& inner)noexcept{return store_physical_api::identity_vfs(native,schema,observed,inner);}
int store_physical_prepare_v2(sqlite3* owner,const char* sql,int size,sqlite3_stmt** statement,const char** tail)noexcept{return store_physical_api::prepare(owner,sql,size,0,statement,tail,false);}
int store_physical_prepare_v3(sqlite3* owner,const char* sql,int size,unsigned int flags,sqlite3_stmt** statement,const char** tail)noexcept{return store_physical_api::prepare(owner,sql,size,flags,statement,tail,true);}
int store_physical_clear_bindings(sqlite3_stmt* statement)noexcept{return store_physical_api::clear_bindings(statement);}
int store_physical_create_function(sqlite3* owner,const char* name,int n,int encoding,void* data,void(*function)(sqlite3_context*,int,sqlite3_value**),void(*step)(sqlite3_context*,int,sqlite3_value**),void(*final)(sqlite3_context*))noexcept{return store_physical_api::create_function(owner,name,n,encoding,data,function,step,final,nullptr,false);}
int store_physical_create_function_v2(sqlite3* owner,const char* name,int n,int encoding,void* data,void(*function)(sqlite3_context*,int,sqlite3_value**),void(*step)(sqlite3_context*,int,sqlite3_value**),void(*final)(sqlite3_context*),void(*destroy)(void*))noexcept{return store_physical_api::create_function(owner,name,n,encoding,data,function,step,final,destroy,true);}
int store_physical_create_function_v2_checked(sqlite3* owner,const char* name,int n,int encoding,void* data,void(*function)(sqlite3_context*,int,sqlite3_value**),void(*step)(sqlite3_context*,int,sqlite3_value**),void(*final)(sqlite3_context*),void(*destroy)(void*),bool* entered)noexcept{return store_physical_api::create_function(owner,name,n,encoding,data,function,step,final,destroy,true,entered);}
int store_physical_finalize(sqlite3_stmt* actual)noexcept{return store_physical_api::finalize(actual);}
int store_physical_step(sqlite3_stmt* actual)noexcept{return store_physical_api::step(actual,false);}
int store_physical_reset(sqlite3_stmt* actual)noexcept{return store_physical_api::step(actual,true);}
int store_physical_exec(sqlite3* actual,const char* sql,int(*callback)(void*,int,char**,char**),void* data,char** error)noexcept{return store_physical_api::exec(actual,sql,callback,data,error);}
int store_physical_wal_checkpoint(sqlite3* actual,const char* schema,int mode,int* log,int* done)noexcept{return store_physical_api::checkpoint(actual,schema,mode,log,done);}
int store_physical_blob_close(sqlite3_blob* actual,sqlite3* owner)noexcept{return store_physical_api::blob_close(actual,owner);}
int store_physical_close(sqlite3* actual)noexcept{return store_physical_api::close(actual,false);}
int store_physical_close_v2(sqlite3* actual)noexcept{return store_physical_api::close(actual,true);}
void store_physical_unresourced_open_returned(sqlite3* actual)noexcept{store_physical_api::unresourced_open_returned(actual);}
}

#else
#include <lattice/store_physical_api.hpp>
namespace lattice::detail {
int store_physical_open_v2(const char* file,sqlite3** actual,int flags,const char* vfs)noexcept{return sqlite3_open_v2(file,actual,flags,vfs);}
thread_local store_physical_native_scope* store_physical_native_scope::current_=nullptr;
store_physical_policy_original::store_physical_policy_original()noexcept=default;
store_physical_policy_original::store_physical_policy_original(store_physical_policy_original&&)noexcept=default;
store_physical_policy_original& store_physical_policy_original::operator=(store_physical_policy_original&&)noexcept=default;
store_physical_policy_original::~store_physical_policy_original()noexcept=default;
store_physical_policy_original store_physical_policy_original::capture_(const store_physical_open_original&)noexcept{return {};}
bool store_physical_policy_original::observer_policy_unsettled()const noexcept{return true;}
void store_physical_policy_original::note_observer_policy_unsettled()noexcept{}
int store_physical_api::open_initializing_(const char* file,sqlite3** actual,int flags,const char* vfs,
    std::chrono::steady_clock::time_point original_end,store_physical_open_original&,
    store_resource_initialization_original& recipient,store_resource_initialization_original* supplied) {
    if(supplied) {
        if(!supplied->transport_current_()||supplied->impl_||original_end>supplied->original_deadline_())
            throw db_error("unsupported resource initialization original");
        supplied->transfer_to_(recipient);
    }
    return sqlite3_open_v2(file,actual,flags,vfs);
}
store_physical_open_original::store_physical_open_original()noexcept=default;
store_physical_open_original::store_physical_open_original(store_physical_open_original&&)noexcept=default;
store_physical_open_original& store_physical_open_original::operator=(store_physical_open_original&&)noexcept=default;
store_physical_open_original::~store_physical_open_original()noexcept=default;
bool store_physical_open_original::retain_initialization_budget_(const std::shared_ptr<const database_initialization_budget>&)noexcept{return false;}
bool store_physical_open_original::issue_exclusive_target_(std::uint64_t)noexcept{return false;}
int store_physical_open_original::open_exclusive_target_(const char*,sqlite3**,int,const char*,std::chrono::steady_clock::time_point)noexcept{return SQLITE_MISUSE;}
bool store_physical_open_original::matches_open_(sqlite3*)const noexcept{return true;}
bool store_physical_open_original::seal_and_wait_(std::chrono::steady_clock::time_point)const noexcept{return false;}
bool store_physical_open_original::observer_policy_unsettled()const noexcept{return true;}
void store_physical_open_original::note_observer_policy_unsettled()noexcept{}
store_physical_cleanup_scope::store_physical_cleanup_scope(const ::lattice::database&)noexcept{}
store_physical_cleanup_scope::store_physical_cleanup_scope(const store_physical_open_original&,sqlite3*)noexcept{}
store_physical_mutex_return::store_physical_mutex_return(const ::lattice::database&,sqlite3_mutex* mutex)noexcept:mutex_(mutex){}
store_physical_mutex_return::store_physical_mutex_return(const store_physical_open_original&,sqlite3_mutex* mutex)noexcept:mutex_(mutex){}
store_physical_mutex_return::~store_physical_mutex_return()noexcept{reset();}
void store_physical_mutex_return::reset()noexcept{if(auto* mutex=std::exchange(mutex_,nullptr))sqlite3_mutex_leave(mutex);}
store_physical_native_scope::store_physical_native_scope(sqlite3*)noexcept{}
store_physical_native_scope::store_physical_native_scope(const ::lattice::database&)noexcept{}
store_physical_native_scope::store_physical_native_scope(const store_physical_open_original&,sqlite3*)noexcept{}
store_physical_native_scope::store_physical_native_scope(const store_physical_open_original*,sqlite3*)noexcept{}
store_physical_native_scope::~store_physical_native_scope()noexcept=default;
void store_physical_native_scope::reset()noexcept{}
int store_physical_open_v2_at(const char* file,sqlite3** actual,int flags,const char* vfs,std::chrono::steady_clock::time_point)noexcept{return sqlite3_open_v2(file,actual,flags,vfs);}
int store_physical_open_v2_owned_at(const char* file,sqlite3** actual,int flags,const char* vfs,std::chrono::steady_clock::time_point,store_physical_open_original&)noexcept{return sqlite3_open_v2(file,actual,flags,vfs);}
int store_physical_close_owned(store_physical_open_original&,sqlite3* actual)noexcept{return sqlite3_close(actual);}
int store_physical_close_v2_owned(store_physical_open_original&,sqlite3* actual)noexcept{return sqlite3_close_v2(actual);}
bool store_physical_process_current()noexcept{return true;}
bool store_physical_is_enrolled(sqlite3*)noexcept{return false;}
bool store_physical_main_for_lower(sqlite3*,sqlite3_vfs*,sqlite3_file*& inner)noexcept{inner=nullptr;return true;}
sqlite3_vfs* store_physical_identity_vfs(sqlite3*,const char*,sqlite3_vfs* observed,sqlite3_file*& inner)noexcept{inner=nullptr;return observed;}
int store_physical_prepare_v2(sqlite3* owner,const char* sql,int size,sqlite3_stmt** statement,const char** tail)noexcept{return sqlite3_prepare_v2(owner,sql,size,statement,tail);}
int store_physical_prepare_v3(sqlite3* owner,const char* sql,int size,unsigned int flags,sqlite3_stmt** statement,const char** tail)noexcept{return sqlite3_prepare_v3(owner,sql,size,flags,statement,tail);}
int store_physical_clear_bindings(sqlite3_stmt* statement)noexcept{return sqlite3_clear_bindings(statement);}
int store_physical_create_function(sqlite3* owner,const char* name,int n,int encoding,void* data,void(*function)(sqlite3_context*,int,sqlite3_value**),void(*step)(sqlite3_context*,int,sqlite3_value**),void(*final)(sqlite3_context*))noexcept{return sqlite3_create_function(owner,name,n,encoding,data,function,step,final);}
int store_physical_create_function_v2(sqlite3* owner,const char* name,int n,int encoding,void* data,void(*function)(sqlite3_context*,int,sqlite3_value**),void(*step)(sqlite3_context*,int,sqlite3_value**),void(*final)(sqlite3_context*),void(*destroy)(void*))noexcept{return sqlite3_create_function_v2(owner,name,n,encoding,data,function,step,final,destroy);}
int store_physical_create_function_v2_checked(sqlite3* owner,const char* name,int n,int encoding,void* data,void(*function)(sqlite3_context*,int,sqlite3_value**),void(*step)(sqlite3_context*,int,sqlite3_value**),void(*final)(sqlite3_context*),void(*destroy)(void*),bool* entered)noexcept{if(entered)*entered=true;return sqlite3_create_function_v2(owner,name,n,encoding,data,function,step,final,destroy);}
int store_physical_finalize(sqlite3_stmt* p)noexcept{return sqlite3_finalize(p);}
int store_physical_step(sqlite3_stmt* p)noexcept{return sqlite3_step(p);}
int store_physical_reset(sqlite3_stmt* p)noexcept{return sqlite3_reset(p);}
int store_physical_exec(sqlite3* p,const char* sql,int(*callback)(void*,int,char**,char**),void* data,char** error)noexcept{return sqlite3_exec(p,sql,callback,data,error);}
int store_physical_wal_checkpoint(sqlite3* p,const char* schema,int mode,int* log,int* done)noexcept{return sqlite3_wal_checkpoint_v2(p,schema,mode,log,done);}
int store_physical_blob_close(sqlite3_blob* p,sqlite3*)noexcept{return sqlite3_blob_close(p);}
int store_physical_close(sqlite3* p)noexcept{return sqlite3_close(p);}
int store_physical_close_v2(sqlite3* p)noexcept{return sqlite3_close_v2(p);}
void store_physical_unresourced_open_returned(sqlite3*)noexcept{}
}
#endif
