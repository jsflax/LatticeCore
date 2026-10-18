#include "database_initialization_budget.hpp"
#include "store_physical_api.hpp"
#include "database_sql_dispatcher.hpp"
#include "database_sql_request.hpp"
#include "database_initialization_test_probe.hpp"
#include <new>
#include <limits>
#include <lattice/db.hpp>
#include <lattice/detail/observer_delivery_startup.hpp>
#include <lattice/native_retirement_storage.hpp>
#include <array>
#include <atomic>
#include <thread>
#include <utility>

namespace lattice::detail {
namespace {
[[noreturn]] void initialization_refuse(const char* reason){throw db_error(reason);}
}
struct database_initialization_budget::state final {
    allocation_account account;
    // Closed DATA only: no pre-open Store/current_for/Open/SQL permission.
    const std::shared_ptr<observer_delivery_data_domain> domain;
    struct open {
        const database* owner=nullptr; // comparison DATA only
        std::weak_ptr<const observer_delivery_open_startup> original;
    };
    std::array<open,3> opens{};
    state(std::uint64_t issuer,std::thread::id thread,limits bounds,
          std::chrono::steady_clock::time_point deadline,std::uint64_t initial_workspace,
          const std::shared_ptr<observer_delivery_data_domain>& actual)
        :account(issuer,thread,bounds,deadline,initial_workspace),domain(actual){}
};
struct database_initialization_budget::admission_record final:observer_delivery_domain::lease_state {
    state fields;
    database_initialization_budget budget;
    admission_record(std::uint64_t issuer,std::thread::id thread,limits bounds,
        std::chrono::steady_clock::time_point end,
        const std::shared_ptr<observer_delivery_data_domain>& domain)
        :fields(issuer,thread,bounds,end,sizeof(admission_record),domain),budget(issuer,&fields,this){}
};
template<class T> class database_initialization_budget::bootstrap_allocator {
    template<class> friend class bootstrap_allocator;
    allocation_account* account_;const std::uint64_t creator_;
public:
    using value_type=T;
    bootstrap_allocator(allocation_account& account)noexcept:account_(&account),creator_(database_native_process()){}
    template<class U> bootstrap_allocator(const bootstrap_allocator<U>& other)noexcept
        :account_(other.account_),creator_(other.creator_){}
    T* allocate(std::size_t count) {
        if(creator_!=database_native_process()||!account_||
           count>std::numeric_limits<std::size_t>::max()/sizeof(T))
            initialization_refuse("initialization bootstrap allocator original differs");
        const auto bytes=count*sizeof(T);
        if(bytes>32u*1024u*1024u){account_->fail_();initialization_refuse("initialization bootstrap request exceeds fixed object ceiling");}
        account_->claim_workspace_(bytes); // actual rebound request BEFORE allocation
        account_->bootstrap_bytes_.fetch_add(bytes,std::memory_order_relaxed);
        account_->bootstrap_requests_.fetch_add(1,std::memory_order_relaxed);
        if(database_initialization_test_probe::consume(database_initialization_test_probe::allocation_stage::bootstrap_control))
            throw std::bad_alloc();
        auto* result=std::allocator<T>{}.allocate(count);
        if(creator_!=database_native_process())
            initialization_refuse("initialization bootstrap allocation crossed creator");
        return result;
    }
    void deallocate(T* value,std::size_t count)noexcept {
        // No account access, debit or refund after its record has been deleted.
        if(creator_==database_native_process())std::allocator<T>{}.deallocate(value,count);
    }
    template<class U> bool operator==(const bootstrap_allocator<U>& other)const noexcept {
        return creator_==other.creator_; // deallocation compatibility, never provenance
    }
};
database_initialization_budget::database_initialization_budget(std::uint64_t issuer,state* value,admission_record* record)noexcept
    :creator_(issuer),state_(value),record_(record){}
database_initialization_budget::~database_initialization_budget()noexcept=default;
void database_initialization_budget::delete_after_return_()noexcept {
    if(creator_!=database_native_process()||!record_||state_!=&record_->fields||
       &record_->budget!=this||record_->creator!=creator_)return;
    // Exactly the real admitted record returns credit; ungranted failures own
    // no charge. Invariant/locking failure retains the record and its debt.
    auto* record=record_;
    if(!observer_delivery_domain::retire_initialization_record_(*record))return;
    if(creator_==database_native_process())delete record;
}
std::shared_ptr<observer_delivery_domain::lease_state> database_initialization_budget::make_pending_(
    observer_delivery_domain::initialization_limits issued,
    std::chrono::steady_clock::time_point end,
    const std::shared_ptr<observer_delivery_data_domain>& domain,
    const std::shared_ptr<observer_delivery_domain::client_state>& client,bool& transferred) {
    const auto creator=database_native_process();const auto builder=std::this_thread::get_id();
    const limits maximum{issued.statements,issued.workspace_bytes,issued.columns,issued.rows};
    // This exact self request is bounded BEFORE new while the existing actual
    // client building-cell owns prospective custody. The same account starts
    // with that sole compiled request; no preexisting counters are copied/reset.
    constexpr auto self_bytes=sizeof(admission_record);
    if(!domain||!maximum.statements||!maximum.workspace_bytes||!maximum.columns||!maximum.rows||
       self_bytes>32u*1024u*1024u||maximum.workspace_bytes<self_bytes||
       end==std::chrono::steady_clock::time_point::max()||std::chrono::steady_clock::now()>=end)
        initialization_refuse("initialization declaration is invalid or expired");
    if(database_initialization_test_probe::consume(database_initialization_test_probe::allocation_stage::bootstrap_record))
        throw std::bad_alloc();
    auto* raw=new admission_record(creator,builder,maximum,end,domain);
    if(creator!=database_native_process())initialization_refuse("inherited initialization admission allocation");
    observer_delivery_domain::transfer_initialization_cell_(*raw,client,transferred);
    try {
        if(database_initialization_test_probe::consume(database_initialization_test_probe::allocation_stage::bootstrap_handoff))
            throw std::bad_alloc();
        observer_delivery_domain::require_initialization_cell_(*raw,client);
    }catch(...){if(creator==database_native_process())destroy(&raw->budget);throw;}
    // Constructor failure/control allocation failure invokes this SAME embedded
    // intrusive return node. Even before grant no wrapper allocation is needed.
    return std::shared_ptr<observer_delivery_domain::lease_state>(raw,
        [creator](admission_record* record)noexcept {
            if(creator==database_native_process())destroy(&record->budget);
        },bootstrap_allocator<admission_record>(raw->fields.account));
}
std::shared_ptr<database_initialization_budget> database_initialization_budget::from_pending_(
    std::shared_ptr<observer_delivery_domain::lease_state> held) {
    if(!held||held->creator!=database_native_process()||!held->granted)
        initialization_refuse("initialization admission record is not actually granted");
    auto* record=static_cast<admission_record*>(held.get());
    auto* actual=&record->budget;
    if(actual->record_!=record||actual->state_!=&record->fields||!actual->current_())
        initialization_refuse("initialization admission record original differs");
    return std::shared_ptr<database_initialization_budget>(std::move(held),actual);
}
void database_initialization_budget::destroy(database_initialization_budget* value)noexcept {
    if(!value||value->creator_!=database_native_process())return;
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    store_physical_api::retire_initialization_budget_(value);
#else
    value->delete_after_return_(); // same typed DATA ending, no resource permission
#endif
}
std::shared_ptr<database_initialization_budget> database_initialization_budget::create(
    database& actual,const observer_delivery_store& store,
    const observer_delivery_store::client& client,limits maximum,
    std::chrono::steady_clock::time_point end) {
    actual.require_creator_();
    const auto issuer=database_native_process();
    if(database_sql_dispatcher::callback_active_for_(actual)||
       database::sql_entry_scope::active_for(&actual)||database::engine_query_scope::active_for(actual)||
       database::mutation_sql_scope::active_for(&actual))
        initialization_refuse("initialization credit must precede Native/SQL entry");
    if(!maximum.statements||!maximum.workspace_bytes||!maximum.columns||!maximum.rows||
       maximum.workspace_bytes<sizeof(admission_record)||
       end==std::chrono::steady_clock::time_point::max()||std::chrono::steady_clock::now()>=end)
        initialization_refuse("initialization declaration is invalid or expired");
    if(!store.state_||client.store_!=store.state_)
        initialization_refuse("initialization client is not its actual Store domain");
    if(actual.raw_handle_escaped_.load(std::memory_order_acquire)||
       !actual.physical_open_original_.matches_open_(actual.db_))
        initialization_refuse("initialization credit requires its actual unescaped open");
    // All existing current callers still validate their genuine Native physical
    // original before private DATA selection/admission. No early issuer here.
    if(!store.current_for(actual))initialization_refuse("initialization store original differs");
    if(issuer!=database_native_process()||!actual.creator_current_())
        initialization_refuse("inherited initialization admission custody");
    const bool cannot_wait=store_physical_api::managed_return_here_();
    auto admitted=observer_delivery_domain::acquire_initialization_(client.client_,store.state_,
        {maximum.statements,maximum.workspace_bytes,maximum.columns,maximum.rows},end,!cannot_wait);
    if(issuer!=database_native_process()||!actual.creator_current_())
        initialization_refuse("inherited initialization credit return");
    if(admitted.code!=observer_delivery_store::status::admitted)
        initialization_refuse("initialization credit refused before first SQL");
    auto budget=std::move(admitted.budget);
    if(!budget||budget->state_->domain!=store.state_)
        initialization_refuse("initialization credit returned a different Store domain");
    budget->require_current_();
    return budget;
}
bool database_initialization_budget::current_()const noexcept {
    return creator_==database_native_process()&&state_&&state_->account.creator_==creator_&&
        state_->account.current_();
}
bool database_initialization_budget::usable_()const noexcept {
    return current_()&&!state_->account.failed_.load(std::memory_order_acquire);
}
void database_initialization_budget::require_current_()const {
    if(!current_())initialization_refuse("initialization original/deadline is no longer current");
    state_->account.require_current_();
}
void database_initialization_budget::bind_open_(database& owner,
    const observer_delivery_open_startup& open,
    const std::shared_ptr<database_initialization_budget>& self) {
    owner.require_creator_();require_current_();
    if(self.get()!=this||database_sql_dispatcher::callback_active_for_(owner))
        initialization_refuse("initialization budget binding lacks its original");
    open.require_initializing_(owner);
    native_retirement_storage<std::shared_ptr<const observer_delivery_open_startup>> retained;
    if(!current_())initialization_refuse("inherited initialization open custody");
    retained.get<0>()=open.initialization_original_(owner);
    if(!current_()||retained.get<0>().get()!=&open)
        initialization_refuse("initialization open token differs");
    state::open* available=nullptr;
    for(auto& slot:state_->opens) {
        if(slot.owner==&owner) {
            if(!slot.original.owner_before(retained.get<0>())&&!retained.get<0>().owner_before(slot.original))return;
            initialization_refuse("initialization owner address is a different open");
        }
        if(!slot.owner&&!available)available=&slot;
    }
    if(!available)initialization_refuse("initialization fixed open slots exhausted");
    if(!owner.physical_open_original_.retain_initialization_budget_(self))
        initialization_refuse("initialization physical-original tail refused");
    if(!current_())initialization_refuse("inherited initialization physical-original binding");
    available->original=retained.get<0>();available->owner=&owner;
}
bool database_initialization_budget::bound_for_(const database& owner,
    const observer_delivery_open_startup& open)const noexcept {
    if(!current_()||!owner.creator_current_())return false;
    for(const auto& slot:state_->opens)if(slot.owner==&owner) {
        // The live DB token pins this weak control block; expiry is denial,
        // never an SQL ending, physical retirement or credit refund.
        auto actual=slot.original.lock();return actual&&actual.get()==&open;
    }
    return false;
}
database_initialization_budget::allocation_account::allocation_account(
    std::uint64_t issuer,std::thread::id builder,limits maximum,
    std::chrono::steady_clock::time_point end,std::uint64_t initial_workspace)noexcept
    :creator_(issuer),builder_(builder),maximum_(maximum),end_(end),workspace_(initial_workspace),
     bootstrap_bytes_(initial_workspace),bootstrap_requests_(1){}
bool database_initialization_budget::allocation_account::current_()const noexcept {
    return creator_==database_native_process()&&builder_==std::this_thread::get_id();
}
void database_initialization_budget::allocation_account::require_current_()const {
    if(!current_()||failed_.load(std::memory_order_acquire)||
       std::chrono::steady_clock::now()>=end_)
        initialization_refuse("initialization original/deadline is no longer current");
}
void database_initialization_budget::allocation_account::claim_workspace_(std::uint64_t bytes) {
    require_current_();auto used=workspace_.load(std::memory_order_relaxed);
    for(;;) {
        if(used>maximum_.workspace_bytes||bytes>maximum_.workspace_bytes-used) {
            fail_();initialization_refuse("initialization aggregate workspace exhausted before copy");
        }
        if(workspace_.compare_exchange_weak(used,used+bytes,std::memory_order_acq_rel))return;
    }
}
void database_initialization_budget::allocation_account::claim_statements_(std::uint64_t count) {
    require_current_();auto used=statements_.load(std::memory_order_relaxed);
    for(;;) {
        if(used>maximum_.statements||count>maximum_.statements-used) {
            fail_();initialization_refuse("initialization statement tally exhausted before prepare");
        }
        if(statements_.compare_exchange_weak(used,used+count,std::memory_order_acq_rel))return;
    }
}
void database_initialization_budget::allocation_account::fail_()noexcept {
    if(current_())failed_.store(true,std::memory_order_release);
}
void database_initialization_budget::allocation_account::note_api_error_(int returned,bool cleanup)noexcept {
    if(!current_()||returned==SQLITE_OK||returned==SQLITE_ROW||returned==SQLITE_DONE)return;
    auto& slot=cleanup?cleanup_api_rc_:first_api_rc_;
    int empty=SQLITE_OK;slot.compare_exchange_strong(empty,returned,std::memory_order_acq_rel);
    if(!cleanup)failed_.store(true,std::memory_order_release);
}
void database_initialization_budget::claim_workspace_(std::uint64_t bytes) {
    if(!current_())initialization_refuse("initialization original/deadline is no longer current");
    state_->account.claim_workspace_(bytes);
}
void database_initialization_budget::claim_statements_(std::uint64_t count) {
    if(!current_())initialization_refuse("initialization original/deadline is no longer current");
    state_->account.claim_statements_(count);
}
void database_initialization_budget::fail_()noexcept {
    if(current_())state_->account.fail_();
}
void database_initialization_budget::note_api_error_(int returned,bool cleanup)noexcept {
    if(current_())state_->account.note_api_error_(returned,cleanup);
}
database_initialization_budget::inspection database_initialization_budget::inspect_for_test_()const noexcept {
    if(!current_())return {};
    const auto& account=state_->account;
    return {this,account.maximum_,account.statements_.load(std::memory_order_acquire),
        account.workspace_.load(std::memory_order_acquire),account.end_,account.failed_.load(std::memory_order_acquire),
        account.first_api_rc_.load(std::memory_order_acquire),account.cleanup_api_rc_.load(std::memory_order_acquire),&account,
        record_,sizeof(admission_record),account.bootstrap_bytes_.load(std::memory_order_acquire),
        account.bootstrap_requests_.load(std::memory_order_acquire),
        observer_delivery_domain::inspect_initialization_record_(*record_)};
}
database_initialization_budget::limits database_initialization_budget::limits_()const {
    require_current_();return state_->account.maximum_;
}
std::chrono::steady_clock::time_point database_initialization_budget::deadline_()const {
    if(!current_())initialization_refuse("inherited initialization deadline");return state_->account.end_;
}
database_initialization_episode::database_initialization_episode(std::uint64_t issuer,database* owner)noexcept
    :creator_(issuer),owner_(owner){}
void database_initialization_episode::destroy(database_initialization_episode* value)noexcept {
    if(value&&value->creator_==database_native_process())delete value;
}
std::shared_ptr<database_initialization_episode> database_initialization_episode::prepare_(database& owner,
    const observer_delivery_initialization_sql& begin,const observer_delivery_initialization_sql& commit,
    const observer_delivery_initialization_sql& rollback) {
    owner.require_creator_();const auto issuer=database_native_process();
    begin.check_for_(owner);commit.check_for_(owner);rollback.check_for_(owner);
    using recipe=observer_delivery_initialization_sql::recipe;
    switch(begin.recipe_id_()) {
      case recipe::schema_transaction_begin:case recipe::schema_transaction_deferred_begin:
      case recipe::schema_transaction_exclusive_begin:break;
      default:initialization_refuse("initialization BEGIN descriptor differs");
    }
    if(commit.recipe_id_()!=recipe::schema_transaction_commit||rollback.recipe_id_()!=recipe::schema_transaction_rollback||
       !begin.parameters_().empty()||!commit.parameters_().empty()||!rollback.parameters_().empty())
        initialization_refuse("initialization ending descriptors differ");
    auto* open=owner.delivery_startup_.get();
    if(!open)initialization_refuse("initialization transaction has no exact open");
    auto& budget=open->initialization_budget_(owner);
    if(!budget||!budget->bound_for_(owner,*open))initialization_refuse("initialization transaction budget differs");
    // Fixed one-BEGIN + one-COMMIT + one-fallback-ROLLBACK allowance. Count
    // the actual three request shells/SQL views before any BEGIN effect.
    // This is structured library custody, not SQLite allocator/RSS accounting.
    const auto ending_workspace=3*database_sql_request::initialization_shell_()+
        begin.sql_().size()+commit.sql_().size()+rollback.sql_().size()+3;
    database_initialization_episode* raw=nullptr;
    try {
        budget->claim_statements_(3);
        budget->claim_workspace_(ending_workspace+sizeof(database_initialization_episode));
        if(database_initialization_test_probe::consume(database_initialization_test_probe::allocation_stage::episode))
            throw std::bad_alloc();
        raw=new database_initialization_episode(issuer,&owner);
        if(issuer!=database_native_process())initialization_refuse("inherited initialization episode allocation");
        raw->open_=open->initialization_original_(owner);raw->budget_=budget;raw->maximum_=budget->limits_();
        raw->begin_=begin.entry_identity_();raw->commit_=commit.entry_identity_();raw->rollback_=rollback.entry_identity_();
        raw->cleanup_maximum_=ending_workspace;
        if(issuer!=database_native_process())initialization_refuse("inherited initialization episode binding");
        auto* issued=std::exchange(raw,nullptr);
        return std::shared_ptr<database_initialization_episode>(issued,destroy);
    }catch(...) {
        if(issuer==database_native_process()){budget->fail_();delete raw;}
        throw;
    }
}
void database_initialization_episode::claim_request_(database& owner,
    const observer_delivery_initialization_sql& entry,std::uint64_t shell,bool cleanup) {
    if(creator_!=database_native_process()||owner_!=&owner||!owner.creator_current_()||!budget_||!open_)
        initialization_refuse("initialization episode original differs");
    const auto* identity=entry.entry_identity_();std::atomic<bool>* claim=nullptr;
    if(identity==begin_&&!cleanup&&!live_.load(std::memory_order_acquire)&&!ended_.load(std::memory_order_acquire))claim=&begin_claimed_;
    else if(identity==commit_&&!cleanup&&live_.load(std::memory_order_acquire)&&!ended_.load(std::memory_order_acquire))claim=&commit_claimed_;
    else if(identity==rollback_&&cleanup_for_(owner,entry))claim=&rollback_claimed_;
    if(!claim||claim->exchange(true,std::memory_order_acq_rel))
        initialization_refuse("initialization ending was not the exact unused issued entry");
    // All three requests spend the prepaid ending allowance, without renewal.
    claim_cleanup_workspace_(shell);
}
void database_initialization_episode::claim_cleanup_workspace_(std::uint64_t bytes) {
    if(creator_!=database_native_process())initialization_refuse("inherited initialization ending workspace");
    auto used=cleanup_workspace_.load(std::memory_order_relaxed);
    for(;;) {
        if(used>cleanup_maximum_||bytes>cleanup_maximum_-used)
            initialization_refuse("prepaid initialization ending workspace exceeded");
        if(cleanup_workspace_.compare_exchange_weak(used,used+bytes,std::memory_order_acq_rel))return;
    }
}
bool database_initialization_episode::original_for_(const database& owner,
    const observer_delivery_initialization_sql& entry)const noexcept {
    return creator_==database_native_process()&&owner_==&owner&&owner.creator_current_()&&open_&&budget_&&
        entry.entry_identity_()==rollback_&&open_->cleanup_for_(owner)&&budget_->bound_for_(owner,*open_);
}
bool database_initialization_episode::cleanup_for_(const database& owner,
    const observer_delivery_initialization_sql& entry)const noexcept {
    return original_for_(owner,entry)&&live_.load(std::memory_order_acquire)&&!ended_.load(std::memory_order_acquire);
}
}
