#include "database_sql_request.hpp"
#include "database_sql_dispatcher.hpp"
#include "database_initialization_budget.hpp"
#include "database_initialization_test_probe.hpp"
#include <new>
#include <lattice/detail/observer_delivery_startup.hpp>
#include "database_sql_read_admission.hpp"
#include "observer_delivery_original.hpp"
#include <algorithm>
#include <atomic>
#include <optional>
#include <lattice/db.hpp>
#include <cmath>
#include <cstring>
#include <climits>
#include <type_traits>
#include <exception>
namespace lattice::detail {
namespace {
uint64_t checked_add(uint64_t a,uint64_t b) {
    if(b>UINT64_MAX-a)throw db_error("read declaration arithmetic overflow");return a+b;
}
uint64_t bytes(const column_value_t& value) {
    return std::visit([](const auto& item)->uint64_t {
        using T=std::decay_t<decltype(item)>;
        if constexpr(std::is_same_v<T,std::string>||std::is_same_v<T,std::vector<uint8_t>>)return item.size();
        else return sizeof(T);
    },value);
}
bool same(const char* actual,const char* expected)noexcept {
    return actual&&sqlite3_stricmp(actual,expected)==0;
}
}
struct database_sql_request::read_budget {
    const uint64_t creator;
    std::shared_ptr<const database_sql_read_binding> binding;
    observer_delivery_store::lease credit;
    std::shared_ptr<const observer_delivery_original> original;
    std::weak_ptr<const database::core_transaction_original> original_identity;
    std::unique_ptr<observer_delivery_store::delivery_scope> reentry;
    std::chrono::steady_clock::time_point deadline;
    uint64_t maximum=0;
    std::atomic<uint64_t> used{0};
    explicit read_budget(uint64_t issuer):creator(issuer){}
    static void destroy(read_budget* value)noexcept {
        if(!value||value->creator!=database_native_process())return;
        value->reentry.reset();if(value->creator!=database_native_process())return;
        value->original_identity.reset();value->original.reset();
        if(value->creator!=database_native_process())return;
        value->credit.reset();if(value->creator!=database_native_process())return;
        value->binding.reset();delete value;
    }
};
struct database_sql_request::custody {
    const uint64_t creator;
    std::shared_ptr<const database_sql_read_binding> binding;
    std::shared_ptr<read_budget> budget;
    explicit custody(uint64_t issuer):creator(issuer){}
    static void destroy(custody* value)noexcept {
        if(!value||value->creator!=database_native_process())return;
        value->budget.reset();if(value->creator!=database_native_process())return;
        value->binding.reset();delete value;
    }
};
void database_sql_request::admit_(std::string_view sql) {
    if(auto* observed=database_initialization_test_probe::read_admission_attempts)++*observed;
    if(owner_->closed_.load(std::memory_order_acquire))return;
    if(!std::atomic_load(&owner_->sql_read_binding_))return; // no new legacy allocation
    const auto issuer=creator_;
    auto* held=new custody(issuer);
    try {
        if(issuer!=database_native_process()||!owner_->creator_current_())throw db_error("inherited read admission allocation");
        held->binding=std::atomic_load(&owner_->sql_read_binding_);
        if(!held->binding){custody::destroy(held);return;} // exact legacy path
        if(!held->binding->current(*owner_))throw db_error("read binding is not its current published original");
        if(owner_->raw_handle_escaped_.load(std::memory_order_acquire))
            throw db_error("read envelope refuses escaped Native custody before waiting");
        const auto& profile=*held->binding->profile_;
        // A nested PUBLIC request remains a new purpose/exact statement. It
        // may share only its outer operation's monotonically consumed DATA
        // budget, never SQL permission or a renewed domain lease/deadline.
        for(auto* node=database::engine_query_scope::current;node;node=node->previous) {
            if(&node->owner!=owner_||!node->request||!node->request->admission_)continue;
            const auto* previous=node->request->admission_;
            if(previous->creator==issuer&&previous->binding==held->binding&&previous->budget) {
                held->budget=previous->budget;break;
            }
        }
        if(!held->budget) {
            held->budget=std::shared_ptr<read_budget>(new read_budget(issuer),read_budget::destroy);
            if(issuer!=database_native_process())throw db_error("inherited read budget allocation");
            auto& budget=*held->budget;budget.binding=held->binding;
            const auto start=std::chrono::steady_clock::now();
            budget.deadline=start+profile.policy_.maximum_duration;
            budget.maximum=profile.policy_.workspace_bytes;
            // No Native probe precedes public sql_entry_scope: that would
            // bypass the existing fresh-reader fairness gate. This atomic
            // mirror can only identify a budget to debit, never admit SQL.
            const auto actual=std::atomic_load(&owner_->read_admission_original_);
            if(actual&&actual->current_thread()) {
                budget.original=actual->delivery;budget.original_identity=actual->actual;
            }
            if(!budget.original&&(database::sql_entry_scope::active_for(owner_)||
               database::engine_query_scope::active_for(*owner_)||database::update_hook_scope::active_for(owner_->db_)||
               database::mutation_sql_scope::active_for(owner_)))
                throw db_error("reentrant read requires an already admitted exact operation");
            const bool transaction=owner_->sql_transaction_live_.load(std::memory_order_acquire);
            if(!budget.original) {
                const observer_delivery_store::units wanted{1,budget.maximum};
                // Existing foreign/read transactions cannot wait for credit
                // while keeping their Native resource. A fresh standalone
                // request waits only here, outside every Native/store leaf.
                auto admitted=transaction?profile.client_.try_acquire(wanted):profile.client_.acquire(wanted,
                    std::min(budget.deadline,start+profile.maximum_admission_wait_));
                if(issuer!=database_native_process())throw db_error("inherited read credit admission");
                if(admitted.code!=observer_delivery_store::status::admitted)
                    throw db_error("read envelope refused before Native prepare/bind");
                budget.credit=std::move(admitted.held);
                budget.reentry=std::make_unique<observer_delivery_store::delivery_scope>(budget.credit);
                if(issuer!=database_native_process())throw db_error("inherited read reentry custody");
            }
        }
        if(issuer!=database_native_process())throw db_error("inherited read publication");
        const auto& policy=held->binding->profile_->policy_;
        limits_.sql_bytes=std::min(limits_.sql_bytes,policy.workspace_bytes);
        limits_.parameter_bytes=std::min(limits_.parameter_bytes,policy.workspace_bytes);
        limits_.result_bytes=std::min(limits_.result_bytes,policy.workspace_bytes);
        limits_.columns=std::min(limits_.columns,policy.columns);
        limits_.rows=std::min(limits_.rows,policy.rows);
        limits_.deadline=std::min(limits_.deadline,held->budget->deadline);
        bounded_=true;admission_=held;
        // Fixed admission/request shells are accounted as structured DATA,
        // plus actual SQL bytes; this is not sizeof(opaque-capture) accounting.
        workspace_(checked_add(checked_add(sql.size(),1),sizeof(custody)+sizeof(read_budget)+sizeof(database_sql_request)));
    }catch(...) {if(admission_==held)admission_=nullptr;custody::destroy(held);throw;}
}
struct database_sql_request::initialization_custody final {
    const uint64_t creator;
    observer_delivery_initialization_sql entry;
    std::shared_ptr<const observer_delivery_open_startup> open;
    std::shared_ptr<database_initialization_budget> budget;
    std::shared_ptr<database_initialization_episode> episode;
    // Captured only by the actual private execute channel. No automatic
    // original-bearing owner spans Native, and no successor is looked up at
    // retirement. This DATA hold is part of the already-charged request shell.
    std::shared_ptr<database_initialization_episode> schema_episode;
    bool schema_attempt_pending=false;
    const bool cleanup;
    const int uncaught=std::uncaught_exceptions();
    initialization_custody(uint64_t issuer,observer_delivery_initialization_sql&& actual,bool ending)
        :creator(issuer),entry(std::move(actual)),cleanup(ending){}
    static void destroy(initialization_custody* value)noexcept {
        if(value&&value->creator==database_native_process())delete value;
    }
};
database_sql_request::database_sql_request(database& owner,
    const observer_delivery_initialization_sql& descriptor,
    const std::shared_ptr<database_initialization_episode>& episode,bool cleanup)
    :creator_(database_native_process()),owner_(&owner),
     kind_(descriptor.recipe_id_()==observer_delivery_initialization_sql::recipe::busy_timeout_getter?
         database_sql_read_kind::busy_timeout_getter:database_sql_read_kind::diagnostic),bounded_(true) {
    owner.require_creator_();
    if(database_sql_dispatcher::callback_active_for_(owner))
        throw db_error("initialization request refuses actual callback reentry before Native");
    if(cleanup) {
        if(!episode||!episode->cleanup_for_(owner,descriptor))
            throw db_error("initialization rollback lacks its preissued live episode");
    }else descriptor.check_for_(owner);
    auto* open=owner.delivery_startup_.get();
    if(!open)throw db_error("initialization request has no actual open token");
    auto* budget=cleanup?episode->budget_.get():open->initialization_budget_(owner).get();
    if(!budget||!budget->bound_for_(owner,*open))throw db_error("initialization budget/open differ");
    const auto bounds=cleanup?episode->maximum_:budget->limits_();
    limits_={bounds.workspace_bytes,bounds.workspace_bytes,bounds.columns,bounds.rows,
        bounds.workspace_bytes,budget->deadline_()};
    descriptor.constrain_result_(limits_.columns,limits_.rows,limits_.result_bytes);
    // Claim BEFORE request/descriptor custody allocates. The private transaction
    // prepays BEGIN, COMMIT and ROLLBACK; no ending can reacquire/renew credit.
    const auto shell=checked_add(sizeof(initialization_custody)+sizeof(database_sql_request),
        checked_add(descriptor.sql_().size(),1));
    initialization_custody* held=nullptr;
    try {
        if(episode)episode->claim_request_(owner,descriptor,shell,cleanup);
        else {
            if(transaction_recipe_(descriptor))throw db_error("initialization transaction requires its exact episode");
            budget->claim_statements_(1);budget->claim_workspace_(shell);
        }
        if(database_initialization_test_probe::consume(database_initialization_test_probe::allocation_stage::request))
            throw std::bad_alloc();
        held=new initialization_custody(creator_,descriptor.retain_(),cleanup);
        if(creator_!=database_native_process())throw db_error("inherited initialization request allocation");
        held->open=cleanup?episode->open_:open->initialization_original_(owner);
        held->budget=cleanup?episode->budget_:open->initialization_budget_(owner);
        held->episode=episode;
        if(creator_!=database_native_process())throw db_error("inherited initialization request custody");
        initialization_=held;
        if(!initialization_current_())throw db_error("initialization request entry/open is no longer current");
        uint64_t total=0;
        if(descriptor.bootstrap_parameters_()) {
            for(std::size_t index=0;index<descriptor.bootstrap_parameter_count_();++index) {
                const auto kind=descriptor.bootstrap_parameter_kind_(index);
                if(kind==2)total=checked_add(total,descriptor.bootstrap_parameter_blob_(index).size());
                else if(kind==0||kind==1)total=checked_add(total,8);
                else throw db_error("unissued receiver parameter type");
            }
        }else if(descriptor.preimage_parameters_()) {
            for(std::size_t index=0;index<descriptor.preimage_parameter_count_();++index)
                total=checked_add(total,descriptor.preimage_parameter_text_(index).size());
        }else for(const auto& param:descriptor.parameters_())total=checked_add(total,bytes(param));
        if(total>limits_.parameter_bytes)throw db_error("initialization parameter declaration exceeds original");
    }catch(...) {
        if(creator_==database_native_process()&&!cleanup)budget->fail_();
        initialization_=nullptr;initialization_custody::destroy(held);throw;
    }
}
database_sql_request::database_sql_request(database& owner,const database_sql_request& parent)
    :database_sql_request(owner,observer_delivery_initialization_sql::fixed_(
        *owner.delivery_startup_,owner,observer_delivery_initialization_sql::recipe::busy_timeout_getter)) {
    if(!parent.initialization_||parent.owner_!=&owner||
       parent.initialization_->budget!=initialization_->budget||parent.initialization_->open!=initialization_->open)
        throw db_error("initialization getter lost the same actual setup original");
}
bool database_sql_request::transaction_recipe_(const observer_delivery_initialization_sql& entry)noexcept {
    try {using recipe=observer_delivery_initialization_sql::recipe;
        switch(entry.recipe_id_()) {
          case recipe::schema_transaction_begin:case recipe::schema_transaction_deferred_begin:
          case recipe::schema_transaction_exclusive_begin:case recipe::schema_transaction_commit:
          case recipe::schema_transaction_rollback:return true;
          default:return false;
        }
    }catch(...){return true;}
}
void database_sql_request::note_initialization_api_error_(int result,bool cleanup)noexcept {
    if(creator_==database_native_process()&&initialization_&&initialization_->creator==creator_&&initialization_->budget)
        initialization_->budget->note_api_error_(result,cleanup||initialization_->cleanup);
}
void database_sql_request::arm_initialization_schema_attempt_() {
    if(creator_!=database_native_process()||!owner_)throw db_error("inherited initialization schema attempt");
    owner_->require_creator_();check();
    if(!initialization_||initialization_->creator!=creator_||!initialization_->budget||
       !initialization_->budget->current_())throw db_error("initialization schema attempt lacks its actual request");
    auto& held=*initialization_;
    if(!held.entry.schema_may_change_())return;
    if(held.cleanup||held.schema_attempt_pending||held.schema_episode)
        throw db_error("initialization schema attempt cannot be reused");
    auto actual=std::atomic_load(&owner_->initialization_episode_);
    if(!actual||actual->creator_!=creator_||actual->owner_!=owner_||
       actual->open_!=held.open||actual->budget_!=held.budget||
       !actual->live_.load(std::memory_order_acquire)||actual->ended_.load(std::memory_order_acquire)) {
        held.budget->fail_();
        throw db_error("initialization schema attempt requires its exact live private BEGIN");
    }
    if(actual->schema_revision_.load(std::memory_order_acquire)==UINT64_MAX) {
        held.budget->fail_();owner_->note_observer_policy_unsettled_();
        throw db_error("initialization schema revision exhausted before prepare");
    }
    held.schema_episode=std::move(actual);
    held.schema_attempt_pending=true;
}
bool database_sql_request::retire_initialization_schema_attempt_()noexcept {
    // This executes after exact VM/request use (or exceptional unwind), not
    // inside authorizer/step. It invalidates reusable proof; it grants none.
    if(creator_!=database_native_process()||!owner_||!owner_->creator_current_()||
       !initialization_||initialization_->creator!=creator_||!initialization_->budget||
       !initialization_->budget->current_())return false;
    auto& held=*initialization_;
    if(!held.schema_attempt_pending)return true;
    held.schema_attempt_pending=false;
    auto* actual=held.schema_episode.get();
    if(!actual||actual->creator_!=creator_||actual->owner_!=owner_||
       actual->open_!=held.open||actual->budget_!=held.budget) {
        held.budget->fail_();owner_->note_observer_policy_unsettled_();return false;
    }
    // Retire THIS captured original even if its real ending was observed on
    // failure. Never advance a successor found at the same owner address.
    auto revision=actual->schema_revision_.load(std::memory_order_acquire);
    for(;;) {
        if(revision==UINT64_MAX) {
            held.budget->fail_();owner_->note_observer_policy_unsettled_();return false;
        }
        if(actual->schema_revision_.compare_exchange_weak(revision,revision+1,
                std::memory_order_acq_rel,std::memory_order_acquire))return true;
    }
}
std::uint64_t database_sql_request::initialization_shell_()noexcept {
    return sizeof(initialization_custody)+sizeof(database_sql_request);
}
bool database_sql_request::initialization_cleanup_()const noexcept {
    return creator_==database_native_process()&&initialization_&&initialization_->creator==creator_&&initialization_->cleanup;
}
bool database_sql_request::initialization_current_()const noexcept {
    if(creator_!=database_native_process()||!owner_||!owner_->creator_current_()||
       !initialization_||initialization_->creator!=creator_||!initialization_->open||!initialization_->budget||
       !initialization_->entry.entry_identity_()||
       !initialization_->budget->bound_for_(*owner_,*initialization_->open))return false;
    if(initialization_->cleanup)return initialization_->episode&&
        initialization_->open->cleanup_for_(*owner_)&&
        initialization_->episode->original_for_(*owner_,initialization_->entry);
    return initialization_->open->cleanup_for_(*owner_);
}
bool database_sql_request::initialization_entry_matches_(const observer_delivery_initialization_sql& descriptor)const noexcept {
    // Cleanup identity remains independent of work validity. This only pins
    // the shared prepare helper to the SAME immutable entry already retained.
    return initialization_current_()&&initialization_->entry.entry_identity_()==descriptor.entry_identity_();
}
bool database_sql_request::initialization_admits_(sqlite3_stmt* actual)const noexcept {
    return initialization_current_()&&!stopped()&&(!actual||matches(actual));
}
database_sql_request::~database_sql_request()noexcept {
    if(creator_==database_native_process()&&initialization_&&initialization_->creator==creator_)
        (void)retire_initialization_schema_attempt_();
    if(creator_==database_native_process()&&initialization_&&initialization_->creator==creator_&&
       !initialization_->cleanup&&initialization_->budget&&
       std::uncaught_exceptions()>initialization_->uncaught)initialization_->budget->fail_();
    initialization_custody::destroy(initialization_);custody::destroy(admission_);
}

database_sql_request::admission_data database_sql_request::inspect_admission_for_test_()const noexcept {
    if(!current()||!admission_||!admission_->budget)return {};
    const auto& budget=*admission_->budget;
    return {&budget,this,budget.used.load(std::memory_order_acquire),bool(budget.original),budget.deadline};
}
void database_sql_request::workspace_(uint64_t bytes) {
    if(initialization_) {
        check();
        if(initialization_cleanup_())initialization_->episode->claim_cleanup_workspace_(bytes);
        else initialization_->budget->claim_workspace_(bytes);
        return;
    }
    if(!admission_)return;
    check();auto& budget=*admission_->budget;
    auto used=budget.used.load(std::memory_order_relaxed);
    for(;;) {
        if(used>budget.maximum||bytes>budget.maximum-used)throw db_error("read aggregate structured workspace exceeded before copy");
        if(budget.used.compare_exchange_weak(used,used+bytes,std::memory_order_acq_rel))break;
    }
    // Monotonic SAME-BEGIN charge. A caught refusal/rollback-to does not refund
    // prior copied storage or renew the original's delivery credit.
    if(budget.original)budget.original->claim_read_workspace(bytes);
}
void database_sql_request::validate_admission_locked_()const {
    if(initialization_) {
        if(!initialization_current_())throw db_error("initialization request lost its exact open/entry");
        if(!initialization_cleanup_()&&!initialization_->entry.work_current_for_(*owner_))
            throw db_error("initialization request work proof is no longer current");
        return;
    }
    auto actual=std::atomic_load(&owner_->sql_read_binding_);
    if(!actual)return;
    if(!admission_||!admission_->budget||actual!=admission_->binding||!actual->current(*owner_))
        throw db_error("prepared request lacks its actual reader-profile admission");
    const auto& budget=*admission_->budget;
    if(budget.original) {
        const auto& original=owner_->core_transaction_original_;
        if(!original||!original->current_for(*owner_)||original->observer_delivery!=budget.original||
           budget.original_identity.owner_before(original)||original.owner_before(budget.original_identity))
            throw db_error("read request lost its exact borrowed BEGIN original");
    }
}
database_sql_request::database_sql_request(database& owner,database_sql_read_kind kind,
    std::string_view sql,const std::vector<column_value_t>& params,const database_sql_read_limits* declaration)
    :creator_(database_native_process()),owner_(&owner),kind_(kind),
     limits_(declaration?*declaration:database_sql_read_limits{}),bounded_(declaration!=nullptr) {
    // No owning allocation, Native/store leaf, SQL or permission publication.
    owner.require_creator_();
    if(owner.delivery_startup_&&owner.delivery_startup_->public_sql_pending_for_(owner))
        throw db_error("unpublished initialization requires its exact private SQL request");
    if(database_sql_dispatcher::callback_active_for_(owner))
        throw db_error("new SQL request refuses reentry from its actual SQLite callback");
    admit_(sql);
    try {
    if(bounded_&&(limits_.sql_bytes==UINT64_MAX||limits_.parameter_bytes==UINT64_MAX||
       limits_.columns==UINT64_MAX||limits_.rows==UINT64_MAX||limits_.result_bytes==UINT64_MAX||
       limits_.deadline==std::chrono::steady_clock::time_point::max()))
        throw db_error("declared read requires all finite count/byte limits and its absolute original deadline");
    if(bounded_&&((kind_==database_sql_read_kind::busy_timeout_getter&&sql!="PRAGMA busy_timeout"&&sql!="PRAGMA busy_timeout;")||
       (kind_==database_sql_read_kind::function_inventory&&sql!="PRAGMA function_list")))
        throw db_error("closed metadata purpose requires its actual literal recipe");
    if(sql.size()>limits_.sql_bytes)throw db_error("read SQL declaration exceeded before prepare");
    if(!bounded_)return;
    uint64_t retained=0;
    for(const auto& param:params)retained=checked_add(retained,bytes(param));
    if(retained>limits_.parameter_bytes)throw db_error("read parameter declaration exceeded before bind");
    check();
    }catch(...) {custody::destroy(admission_);admission_=nullptr;throw;}
}
bool database_sql_request::current()const noexcept {
    return creator_==database_native_process()&&owner_&&owner_->creator_current_()&&
        (!initialization_||initialization_current_())&&
        (!admission_||(admission_->creator==creator_&&admission_->binding->current(*owner_)));
}
bool database_sql_request::stopped()const noexcept {
    return !current()||(initialization_&&!initialization_cleanup_()&&(!initialization_->budget->usable_()||!initialization_->open->pending_for_(*owner_)||!initialization_->entry.work_current_for_(*owner_)))||(!initialization_cleanup_()&&limits_.deadline!=std::chrono::steady_clock::time_point::max()&&
        std::chrono::steady_clock::now()>=limits_.deadline);
}
void database_sql_request::check()const {
    if(stopped())throw db_error("read request original/deadline is no longer current");
}
int database_sql_request::authorize(int action,const char* first,const char* second,
    const char* schema,const char* origin)const noexcept {
    if(stopped())return SQLITE_DENY;
    // Exact outer SQL is insufficient: existing triggers/views/subprograms
    // execute in the same VM. The retained plan must authorize actual operands,
    // schema and origin against its SAME validated preimage; no name-only grant.
    // Public/direct reentry replaces this binding and cannot inherit the entry.
    if(initialization_)return initialization_current_()?
        initialization_->entry.authorize_(action,first,second,schema,origin):SQLITE_DENY;
    if(action!=SQLITE_PRAGMA) {
        if(!bounded_)return SQLITE_OK;
        // Direct writes/DDL/transaction/savepoint/attachment/control refuse
        // during compile, before either execution or prepare-time effects.
        switch(action) {
          case SQLITE_SELECT:case SQLITE_READ:case SQLITE_FUNCTION:case SQLITE_RECURSIVE:return SQLITE_OK;
          default:return SQLITE_DENY;
        }
    }
    // Literal metadata remains literal: no TVF, schema qualification or
    // projection/filter rewrite. Function inventory still uses NO_VTAB.
    switch(kind_) {
      case database_sql_read_kind::table_info:
        if(same(first,"table_info")){metadata_observed_=true;return SQLITE_OK;}return SQLITE_DENY;
      case database_sql_read_kind::function_inventory:
        if(same(first,"function_list")&&!second){metadata_observed_=true;return SQLITE_OK;}return SQLITE_DENY;
      case database_sql_read_kind::busy_timeout_getter:
        if(same(first,"busy_timeout")&&!second){metadata_observed_=true;return SQLITE_OK;}return SQLITE_DENY;
      default:break;
    }
    // Legacy public diagnostics keep their existing semantics. A declared
    // bounded request cannot invoke a prepare-time PRAGMA control or claim
    // that stmt_readonly was a pre-effect authorization.
    if(!bounded_)return SQLITE_OK;
    if(second)return SQLITE_DENY;
    return (same(first,"database_list")||same(first,"schema_version")||
        same(first,"user_version"))?SQLITE_OK:SQLITE_DENY;
}
void database_sql_request::adopt(sqlite3_stmt* statement) {
    check();
    if(!statement||sqlite3_db_handle(statement)!=owner_->db_||(statement_&&(statement_!=statement||!initialization_)))
        throw db_error("read request exact statement differs");
    if(!initialization_&&bounded_&&(!sqlite3_stmt_readonly(statement)||sqlite3_column_count(statement)<=0))
        throw db_error("declared read requires an actual readonly result statement");
    if(!initialization_&&bounded_&&(kind_==database_sql_read_kind::table_info||kind_==database_sql_read_kind::function_inventory||
       kind_==database_sql_read_kind::busy_timeout_getter)&&!metadata_observed_)
        throw db_error("closed metadata lacks its actual SQLite compile action");
    statement_=statement;
}
bool database_sql_request::matches(sqlite3_stmt* statement)const noexcept {
    return current()&&statement_&&statement_==statement;
}
void database_sql_request::parameter_before_copy(const column_value_t& param) {
    check();if(!bounded_)return;
    parameter_bytes_before_copy_(bytes(param));
}
void database_sql_request::parameter_text_before_copy(std::string_view text) {
    check();if(bounded_)parameter_bytes_before_copy_(text.size());
}
void database_sql_request::parameter_blob_before_copy(std::string_view blob) {
    check();if(bounded_)parameter_bytes_before_copy_(blob.size());
}
void database_sql_request::parameter_bytes_before_copy_(uint64_t count) {
    const auto total=checked_add(parameter_bytes_,count);
    if(total>limits_.parameter_bytes)throw db_error("read changed parameter declaration exceeded before actual bind copy");
    workspace_(checked_add(count,sizeof(column_value_t)));parameter_bytes_=total;
}
void database_sql_request::sql_before_copy(uint64_t count) {
    check();if(!bounded_)return;
    if(count>limits_.sql_bytes)throw db_error("read SQL declaration exceeded before producer copy");
    workspace_(checked_add(count,1));
}
void database_sql_request::add_result_(uint64_t count) {
    const auto next=checked_add(result_bytes_,count);
    if(next>limits_.result_bytes)throw db_error("read result declaration exceeded before owning copy");
    result_bytes_=next;
}
void database_sql_request::columns(sqlite3_stmt* statement) {
    check();if(!matches(statement))throw db_error("read columns lack exact statement request");
    if(!bounded_)return;
    const int count=sqlite3_column_count(statement);
    if(count<0||uint64_t(count)>limits_.columns)throw db_error("read column declaration exceeded before name copies");
    uint64_t names=0;
    for(int i=0;i<count;++i) {
        const auto* name=sqlite3_column_name(statement,i);
        if(!name)throw db_error("read column name unavailable");
        names=checked_add(names,std::strlen(name));
    }
    if(uint64_t(count)>UINT64_MAX/sizeof(std::string))throw db_error("read name shell overflow");
    workspace_(checked_add(names,uint64_t(count)*sizeof(std::string)));
    materialized_columns_=uint64_t(count);name_bytes_=names;add_result_(names);
}
void database_sql_request::remember_busy_(sqlite3_stmt* statement) {
    if(kind_!=database_sql_read_kind::busy_timeout_getter)return;
    auto* cookie=owner_->sql_dispatcher_.load(std::memory_order_acquire);
    if(!cookie||cookie->creator_!=creator_||cookie->request_!=this||!matches(statement)||
       sqlite3_column_count(statement)!=1||sqlite3_column_type(statement,0)!=SQLITE_INTEGER)
        throw db_error("actual busy timeout getter identity/type differs");
    const auto timeout=sqlite3_column_int64(statement,0);
    if(timeout<0||timeout>INT_MAX)throw db_error("actual busy timeout value unsupported");
    cookie->numeric_timeout_=int(timeout);cookie->numeric_known_=true;
}
void database_sql_request::row(sqlite3_stmt* statement,bool materialize,int selected) {
    check();if(!matches(statement))throw db_error("read row lacks exact statement request");
    if(!bounded_)return;
    if(rows_==limits_.rows)throw db_error("read row declaration exceeded before owning copy");
    ++rows_;remember_busy_(statement);if(!materialize)return;
    if(admission_&&uint64_t(sqlite3_column_count(statement))!=materialized_columns_)
        throw db_error("read autoreprepare changed its admitted result shape");
    uint64_t count=selected<0?name_bytes_:0;
    const int columns=sqlite3_column_count(statement);
    for(int i=0;i<columns;++i)if(selected<0||i==selected) {
        switch(sqlite3_column_type(statement,i)) {
          case SQLITE_NULL:count=checked_add(count,sizeof(std::nullptr_t));break;
          case SQLITE_INTEGER:count=checked_add(count,sizeof(int64_t));break;
          case SQLITE_FLOAT:
            if(bounded_&&!std::isfinite(sqlite3_column_double(statement,i)))throw db_error("bounded read finite REAL required");
            count=checked_add(count,sizeof(double));break;
          case SQLITE_TEXT:case SQLITE_BLOB: {
            const int length=sqlite3_column_bytes(statement,i);
            if(length<0)throw db_error("read value byte length unavailable");
            count=checked_add(count,uint64_t(length));break;
          }
          default:throw db_error("read value type unavailable");
        }
    }
    uint64_t shells=sizeof(column_value_t);
    if(selected<0) {
        if(uint64_t(columns)>UINT64_MAX/sizeof(database::row_t::value_type))throw db_error("read row shell overflow");
        shells=checked_add(sizeof(database::row_t),uint64_t(columns)*sizeof(database::row_t::value_type));
    }
    workspace_(checked_add(count,shells));
    // table_info owns a normalized temporary type plus its map value. Its
    // result-column census already charges all row values; add the second
    // type copy/key/map shell before these existing copies occur.
    if(kind_==database_sql_read_kind::table_info)workspace_(checked_add(count,sizeof(std::pair<const std::string,std::string>)));
    add_result_(count);
}
void database_sql_request::binding::pin_native_(database& owner,bool cleanup) {
    if(cleanup){cleanup_.emplace(owner);if(!cleanup_->entered())throw db_error("read cleanup original unavailable");}
    else {native_.emplace(owner);if(!native_->entered())throw db_error("read Native original unavailable");}
}
database_sql_request::binding::binding(database_sql_request* request,sqlite3_stmt* exact,bool cleanup) {
    if(!request)return;
    if(request->creator_!=database_native_process())throw db_error("inherited SQL request binding");
    cleanup=cleanup||request->initialization_cleanup_();
    if(database_sql_dispatcher::callback_active_for_(*request->owner_))
        throw db_error("SQL binding refuses its actual SQLite callback before Native publication");
    pin_native_(*request->owner_,cleanup);
    // This overload always has a real request, hence an exact actual owner.
    if(cleanup){if(!request->current())throw db_error("read cleanup original changed");}
    else request->check();
    if(exact&&!request->matches(exact))throw db_error("read step request mismatch");
    if(!cleanup)request->validate_admission_locked_();
    if(request->owner_->raw_handle_escaped_.load(std::memory_order_acquire)) {
        if(request->bounded_)throw db_error("declared read refuses escaped Native policy");
        return;
    }
    auto* cookie=request->owner_->sql_dispatcher_.load(std::memory_order_acquire);
    if(!cookie) {
        if(request->bounded_)throw db_error("bounded read requires actual owned dispatcher");
        return;
    }
    if(cookie->creator_!=database_native_process()||!cookie->active_.load(std::memory_order_acquire)||cookie->in_callback_||
       cookie->native_!=request->owner_->db_)throw db_error("read dispatcher original changed");
    creator_=cookie->creator_;cookie_=cookie;previous_=cookie->request_;previous_cleanup_=cookie->request_cleanup_;
    cookie->references_.fetch_add(1,std::memory_order_relaxed);
    cookie->request_=request;cookie->request_cleanup_=cleanup;cookie->refresh_progress_();
    if(creator_!=database_native_process()){cookie_=nullptr;throw db_error("inherited read policy publication");}
    const int busy_result=cookie->refresh_busy_();
    if(creator_!=database_native_process()){cookie_=nullptr;throw db_error("inherited read busy policy");}
    if(busy_result!=SQLITE_OK) {
        cookie->request_=previous_;cookie->request_cleanup_=previous_cleanup_;cookie->refresh_progress_();
        cookie->release_();cookie_=nullptr;throw db_error("read request actual busy policy unavailable");
    }
}
database_sql_request::binding::binding(database& owner,database_sql_request* request,
    sqlite3_stmt* exact,bool cleanup) {
    owner.require_creator_();
    cleanup=cleanup||(request&&request->initialization_cleanup_());
    if(database_sql_dispatcher::callback_active_for_(owner))
        throw db_error("SQL binding refuses its actual SQLite callback before Native publication");
    pin_native_(owner,cleanup);
    if(request) {
        if(request->owner_!=&owner)throw db_error("read request belongs to another exact owner");
        if(cleanup){if(!request->current())throw db_error("read cleanup original changed");}
        else request->check();
        if(exact&&!request->matches(exact))throw db_error("read step request mismatch");
        if(!cleanup)request->validate_admission_locked_();
    }
    if(owner.raw_handle_escaped_.load(std::memory_order_acquire)) {
        if(request&&request->bounded_)throw db_error("declared read refuses escaped Native policy");
        return;
    }
    auto* cookie=owner.sql_dispatcher_.load(std::memory_order_acquire);
    if(!cookie) {
        if(request&&request->bounded_)throw db_error("bounded read requires actual owned dispatcher");
        return;
    }
    if(cookie->creator_!=database_native_process()||!cookie->active_.load(std::memory_order_acquire)||cookie->in_callback_||
       cookie->native_!=owner.db_)throw db_error("read dispatcher original changed");
    creator_=cookie->creator_;cookie_=cookie;previous_=cookie->request_;previous_cleanup_=cookie->request_cleanup_;
    cookie->references_.fetch_add(1,std::memory_order_relaxed);
    // A public/direct producer with no request explicitly clears an outer
    // request. Reentry does not acquire the caller's metadata purpose.
    cookie->request_=request;cookie->request_cleanup_=cleanup;cookie->refresh_progress_();
    if(creator_!=database_native_process()){cookie_=nullptr;throw db_error("inherited read policy publication");}
    const int busy_result=cookie->refresh_busy_();
    if(creator_!=database_native_process()){cookie_=nullptr;throw db_error("inherited read busy policy");}
    if(busy_result!=SQLITE_OK) {
        cookie->request_=previous_;cookie->request_cleanup_=previous_cleanup_;cookie->refresh_progress_();
        cookie->release_();cookie_=nullptr;throw db_error("read request actual busy policy unavailable");
    }
}
database_sql_request::binding::~binding()noexcept {
    if(cookie_&&creator_==database_native_process()){
        cookie_->request_=previous_;cookie_->request_cleanup_=previous_cleanup_;
        // Real close/finalize may already have destroyed the Native handle.
        // The binding retains only DATA and never touches that handle after
        // actual wrapper retirement deactivated this exact cookie.
        if(cookie_->active_.load(std::memory_order_acquire)) {
            cookie_->refresh_progress_();
            if(cookie_->refresh_busy_(true)!=SQLITE_OK)cookie_->original_.note_observer_policy_unsettled();
        }
        cookie_->release_();
    }
}
}
