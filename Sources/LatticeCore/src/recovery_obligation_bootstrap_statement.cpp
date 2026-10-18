#include "recovery_obligation_bootstrap_statement.hpp"
#include "recovery_obligation_producer.hpp"
#include "recovery_producer_continuity.hpp"
#include "observer_delivery_startup_test_probe.hpp"
#include "observer_delivery_initialization_plan.hpp"
#include "ordinary_initialization_storage.hpp"
#include "receive_install_initialization_recipe.hpp"
#include "database_initialization_budget.hpp"
#include "database_sql_dispatcher.hpp"
#include <lattice/detail/observer_delivery_startup.hpp>
#include <lattice/native_retirement_storage.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <optional>
#include <thread>
#include <utility>

namespace lattice::detail {
#include "recovery_obligation_bootstrap_queries.inc"

struct recovery_obligation_bootstrap_statement::snapshot final {
    const std::uint64_t creator;
    const std::thread::id builder=std::this_thread::get_id();
    database* owner=nullptr; // comparison/dispatch while actual constructor retains it
    // Comparison DATA only. The synchronous actual enrollment retains both
    // its real parent and exact strongly held physical writer for this lifetime.
    lattice_db* enrollment_owner=nullptr;
    const recovery_continuous_admission* enrollment_admission=nullptr;
    const recovery_obligation_bootstrap_reader* enrollment_reader=nullptr;
    std::atomic<std::uint64_t> enrollment_receiver_program{0};
    std::atomic<std::uint32_t> enrollment_receiver_active{0};
    std::atomic<std::uint32_t> enrollment_schema_active{0}; // denial-only exact one operator lifetime
    std::shared_ptr<const observer_delivery_open_startup> open;
    std::shared_ptr<database_initialization_budget> budget;
    std::shared_ptr<database_initialization_episode> episode;
    std::shared_ptr<const observer_delivery_initialization_preimage> preimage;
    std::atomic<bool> active{false};
    explicit snapshot(std::uint64_t issuer):creator(issuer){}
};
struct recovery_obligation_bootstrap_statement::storage final {
    const std::uint64_t creator;
    std::shared_ptr<snapshot> view;
    kind id=kind::family;
    bootstrap_recipe recipe;
    ordinary_initialization_text receiver_sql;
    struct receiver_parameter {int type=0;std::int64_t integer=0;ordinary_initialization_text bytes;};
    std::array<receiver_parameter,13> receiver_parameters;
    std::size_t receiver_parameter_count=0;
    std::uint64_t receiver_program=0;
    std::vector<column_value_t> params;
    const char* table=nullptr;
    const char* schema="main";
    bool system=false;
    std::uint64_t schema_revision=0;
    std::shared_ptr<const observer_delivery_initialization_preimage> enrollment_proof;
    const char* enrollment_table=nullptr;
    const char* enrollment_index=nullptr;
    ordinary_initialization_vector<ordinary_initialization_text> enrollment_implicit_indexes;
    explicit storage(std::uint64_t issuer):creator(issuer){}
};
struct recovery_obligation_bootstrap_reader::state final {
    const std::uint64_t creator;
    database* owner=nullptr;
    recovery_obligation_producer_discovery_limits independent{},effective{};
    bool shared_domains=false;
    std::shared_ptr<recovery_obligation_bootstrap_statement::snapshot> view;
    std::optional<database::initialization_transaction> transaction;
    std::shared_ptr<const observer_delivery_initialization_preimage> enrollment_proof;
    bool enrollment_schema_started=false,enrollment_schema_complete=false;
    std::size_t receiver_contribution=std::size_t(-1);
    explicit state(std::uint64_t issuer):creator(issuer){}
};
recovery_obligation_bootstrap_statement::recovery_obligation_bootstrap_statement(
    std::uint64_t issuer,storage* held)noexcept:creator_(issuer),storage_(held){}
recovery_obligation_bootstrap_statement::~recovery_obligation_bootstrap_statement()noexcept {
    if(creator_==database_native_process())delete storage_;
}
bool recovery_obligation_bootstrap_statement::creator_current()const noexcept {
    return creator_==database_native_process()&&storage_&&storage_->creator==creator_&&
        storage_->view&&storage_->view->creator==creator_&&
        storage_->view->builder==std::this_thread::get_id();
}
bool recovery_obligation_bootstrap_statement::enrollment_schema_kind_(kind id)noexcept {
    switch(id) {
    case kind::enrollment_install_store_create:case kind::enrollment_install_channel_create:case kind::enrollment_install_domain_create:
    case kind::enrollment_obligation_store_create:case kind::enrollment_obligation_scope_create:case kind::enrollment_obligation_entry_create:
    case kind::enrollment_obligation_audit_create:case kind::enrollment_obligation_original_create:case kind::enrollment_obligation_domain_create:
    case kind::enrollment_producer_store_create:case kind::enrollment_producer_profile_create:case kind::enrollment_producer_stamp_create:
    case kind::enrollment_install_seed:case kind::enrollment_obligation_seed:case kind::enrollment_producer_seed:return true;
    default:return false;
    }
}
bool recovery_obligation_bootstrap_statement::enrollment_receiver_kind_(kind id)noexcept {
    switch(id) {
    case kind::enrollment_receiver_configuration:case kind::enrollment_receiver_shared_schema:
    case kind::enrollment_receiver_row_sizes:case kind::enrollment_receiver_row_values:
    case kind::enrollment_receiver_aliases:case kind::enrollment_receiver_unique_domain:
    case kind::enrollment_receiver_insert:case kind::enrollment_receiver_usage:case kind::enrollment_receiver_changes:return true;
    default:return false;
    }
}
bool recovery_obligation_bootstrap_statement::enrollment_receiver_write_()const noexcept {
    return creator_current()&&(storage_->id==kind::enrollment_receiver_insert||storage_->id==kind::enrollment_receiver_usage);
}
bool recovery_obligation_bootstrap_statement::receiver_parameters_()const {
    if(!creator_current())throw db_error("inherited receiver parameter source");
    return enrollment_receiver_kind_(storage_->id);
}
std::size_t recovery_obligation_bootstrap_statement::receiver_parameter_count_()const {
    if(!receiver_parameters_())throw db_error("bootstrap has no closed receiver parameter source");
    return storage_->receiver_parameter_count;
}
int recovery_obligation_bootstrap_statement::receiver_parameter_kind_(std::size_t index)const {
    if(index>=receiver_parameter_count_())throw db_error("receiver parameter index differs");
    return storage_->receiver_parameters[index].type;
}
std::int64_t recovery_obligation_bootstrap_statement::receiver_parameter_integer_(std::size_t index)const {
    if(receiver_parameter_kind_(index)!=1)throw db_error("receiver parameter is not an integer");
    return storage_->receiver_parameters[index].integer;
}
std::string_view recovery_obligation_bootstrap_statement::receiver_parameter_blob_(std::size_t index)const {
    if(receiver_parameter_kind_(index)!=2)throw db_error("receiver parameter is not an owned BLOB");
    return storage_->receiver_parameters[index].bytes;
}
bool recovery_obligation_bootstrap_statement::enrollment_schema_()const noexcept {
    return creator_current()&&enrollment_schema_kind_(storage_->id);
}
bool recovery_obligation_bootstrap_statement::schema_may_change_()const noexcept {
    if(!creator_current())return true;
    return enrollment_schema_()&&storage_->id!=kind::enrollment_install_seed&&
        storage_->id!=kind::enrollment_obligation_seed&&storage_->id!=kind::enrollment_producer_seed;
}
bool recovery_obligation_bootstrap_statement::work_current_for_(const database& actual)const noexcept {
    if(!creator_current()||!actual.creator_current_())return false;
    const auto& view=*storage_->view;
    if(view.owner!=&actual||!view.active.load(std::memory_order_acquire)||!view.open||!view.budget||
       !view.episode||view.episode->budget_!=view.budget||actual.delivery_startup_.get()!=view.open.get()||
       !view.open->pending_for_(actual))return false;
    if(view.enrollment_owner&&(!view.enrollment_admission||
       recovery_continuous_producer::enrollment_reader_binding_(*view.enrollment_owner,actual)!=view.enrollment_admission))return false;
    std::uint64_t revision=0;
    if(!actual.initialization_schema_revision_(*view.episode,*view.open,revision)||
       revision!=storage_->schema_revision)return false;
    if(enrollment_schema_()) {
        if(!view.enrollment_owner||!view.enrollment_reader||!storage_->enrollment_table||!storage_->enrollment_proof||
           view.enrollment_schema_active.load(std::memory_order_acquire)!=std::uint32_t(storage_->id)+1)return false;
        using op=observer_delivery_open_startup::enrollment_ledger_operator;
        const auto action=storage_->enrollment_index?op::create_index:schema_may_change_()?op::create_table:op::seed_store;
        return view.open->preimage_enrollment_ledger_for_(actual,storage_->enrollment_proof,view.enrollment_reader,action,
            storage_->enrollment_table,storage_->enrollment_index?storage_->enrollment_index:"");
    }
    if(enrollment_receiver_kind_(storage_->id)) {
        if(!view.enrollment_owner||!view.enrollment_reader||!storage_->enrollment_proof||!storage_->receiver_program||
           view.enrollment_receiver_program.load(std::memory_order_acquire)!=storage_->receiver_program||
           view.enrollment_receiver_active.load(std::memory_order_acquire)!=std::uint32_t(storage_->id)+1)return false;
        using op=observer_delivery_open_startup::enrollment_ledger_operator;
        return view.open->preimage_enrollment_ledger_for_(actual,storage_->enrollment_proof,view.enrollment_reader,
            op::bind_receiver,"_lattice_install_store","")&&
            view.open->preimage_enrollment_ledger_for_(actual,storage_->enrollment_proof,view.enrollment_reader,
            op::bind_receiver,"_lattice_install_channel","");
    }
    // These fields are privately selected by the closed generator below. No
    // SQL text/name or boolean supplied by a caller can mint table authority.
    if(storage_->system) {
        if(!storage_->table||!view.open->preimage_system_metadata_for_(actual,view.preimage,storage_->table))return false;
        if(storage_->id==kind::databases)return true;
        if(storage_->id!=kind::model_columns||storage_->params.size()!=1)return false;
        const auto* table=std::get_if<std::string>(&storage_->params[0]);
        return table&&view.open->preimage_plain_table_for_(actual,view.preimage,*table);
    }
    if(storage_->table) {
        const std::string_view table=storage_->table;
        if(table!="sqlite_master"&&table!="sqlite_schema")
            return view.open->preimage_plain_table_for_(actual,view.preimage,table);
    }
    // Closed catalog/scalar getter entries precede full table proof; they still
    // require the SAME active actual episode and its captured revision above.
    return true;
}
void recovery_obligation_bootstrap_statement::check_for_(const database& actual)const {
    if(!creator_current())throw db_error("inherited bootstrap statement");
    actual.require_creator_();const auto& view=*storage_->view;
    if(view.owner!=&actual||!view.active.load(std::memory_order_acquire)||!view.open||
       actual.delivery_startup_.get()!=view.open.get()||
       !view.episode||!view.episode->live_.load(std::memory_order_acquire)||
       view.episode->ended_.load(std::memory_order_acquire))
        throw db_error("bootstrap statement lost its actual open/read snapshot");
    const auto current=std::atomic_load(&actual.initialization_episode_);
    if(current!=view.episode||current.owner_before(view.episode)||view.episode.owner_before(current))
        throw db_error("bootstrap statement belongs to a different actual BEGIN");
    view.budget->require_current_();
    if(!work_current_for_(actual))throw db_error("bootstrap statement work proof is no longer current");
}
recovery_obligation_bootstrap_statement::kind recovery_obligation_bootstrap_statement::kind_id()const {
    if(!creator_current())throw db_error("inherited bootstrap kind");return storage_->id;
}
std::string_view recovery_obligation_bootstrap_statement::sql()const {
    if(!creator_current())throw db_error("inherited bootstrap SQL");
    return enrollment_receiver_kind_(storage_->id)?std::string_view(storage_->receiver_sql):std::string_view(storage_->recipe.sql);
}
const std::vector<column_value_t>& recovery_obligation_bootstrap_statement::parameters()const {
    if(!creator_current())throw db_error("inherited bootstrap parameters");
    if(enrollment_receiver_kind_(storage_->id))throw db_error("receiver requires its exact owned parameter views");
    return storage_->params;
}
std::uint64_t recovery_obligation_bootstrap_statement::column_limit()const {
    if(!creator_current())throw db_error("inherited bootstrap column limit");return storage_->recipe.columns;
}
std::uint64_t recovery_obligation_bootstrap_statement::row_limit()const {
    if(!creator_current())throw db_error("inherited bootstrap row limit");return storage_->recipe.rows;
}
std::uint64_t recovery_obligation_bootstrap_statement::result_byte_limit()const {
    if(!creator_current())throw db_error("inherited bootstrap byte limit");return storage_->recipe.result_bytes;
}
int recovery_obligation_bootstrap_statement::authorize_(int action,const char* first,
    const char* second,const char* schema,const char* origin)const noexcept {
    if(!creator_current()||!storage_->view->owner||
       !work_current_for_(*storage_->view->owner)||(origin&&*origin))return SQLITE_DENY;
    const auto equal=[](const char* a,const char* b)noexcept{return a&&b&&sqlite3_stricmp(a,b)==0;};
    if(enrollment_receiver_kind_(storage_->id)) {
        const auto id=storage_->id;const bool main=equal(schema,"main");
        if(action==SQLITE_SELECT)return !enrollment_receiver_write_()?SQLITE_OK:SQLITE_DENY;
        if(action==SQLITE_FUNCTION) {
            if(id==kind::enrollment_receiver_changes)return equal(second,"changes")?SQLITE_OK:SQLITE_DENY;
            return (equal(second,"typeof")||equal(second,"length"))?SQLITE_OK:SQLITE_DENY;
        }
        if(action==SQLITE_INSERT)return main&&id==kind::enrollment_receiver_insert&&equal(first,"_lattice_install_channel")?SQLITE_OK:SQLITE_DENY;
        if(action==SQLITE_UPDATE)return main&&id==kind::enrollment_receiver_usage&&equal(first,"_lattice_install_store")&&
            (equal(second,"channels")||equal(second,"bytes"))?SQLITE_OK:SQLITE_DENY;
        if(action!=SQLITE_READ||!main)return SQLITE_DENY;
        if(id==kind::enrollment_receiver_shared_schema)
            return (equal(first,"sqlite_master")||equal(first,"sqlite_schema"))&&
                (equal(second,"type")||equal(second,"name")||equal(second,"sql"))?SQLITE_OK:SQLITE_DENY;
        if(id==kind::enrollment_receiver_configuration&&equal(first,"_lattice_install_store")) {
            for(const char* column:{"id","version","max_channels","max_field_bytes","max_bytes","channels","bytes"})
                if(equal(second,column))return SQLITE_OK;
            return SQLITE_DENY;
        }
        if(id==kind::enrollment_receiver_usage&&equal(first,"_lattice_install_store"))
            return equal(second,"id")||equal(second,"channels")||equal(second,"bytes")?SQLITE_OK:SQLITE_DENY;
        if((id==kind::enrollment_receiver_row_sizes||id==kind::enrollment_receiver_row_values||
            id==kind::enrollment_receiver_aliases||id==kind::enrollment_receiver_unique_domain)&&equal(first,"_lattice_install_channel")) {
            if(id==kind::enrollment_receiver_aliases||id==kind::enrollment_receiver_unique_domain)
                return equal(second,"channel")||equal(second,"authority")||equal(second,"scope")?SQLITE_OK:SQLITE_DENY;
            for(const char* column:{"channel","authority","source","epoch","scope","schema_digest","frontier_kind","frontier",
                "revision","last_sequence","active","last_install","bytes"})if(equal(second,column))return SQLITE_OK;
        }
        return SQLITE_DENY;
    }
    if(enrollment_schema_()) {
        const auto* target=storage_->enrollment_table;
        const bool main=equal(schema,"main");
        const bool catalog=equal(first,"sqlite_master")||equal(first,"sqlite_schema");
        const bool ddl=schema_may_change_();
        if(action==SQLITE_CREATE_TABLE)return ddl&&!storage_->enrollment_index&&main&&equal(first,target)?SQLITE_OK:SQLITE_DENY;
        if(action==SQLITE_CREATE_INDEX) {
            if(!ddl||!main||!equal(second,target))return SQLITE_DENY;
            if(storage_->enrollment_index)return equal(first,storage_->enrollment_index)?SQLITE_OK:SQLITE_DENY;
            for(const auto& name:storage_->enrollment_implicit_indexes)if(first&&name==first)return SQLITE_OK;
            return SQLITE_DENY;
        }
        if(action==SQLITE_REINDEX)return ddl&&main&&storage_->enrollment_index&&equal(first,storage_->enrollment_index)?SQLITE_OK:SQLITE_DENY;
        if(action==SQLITE_INSERT)return main&&((ddl&&catalog)||(!ddl&&equal(first,target)))?SQLITE_OK:SQLITE_DENY;
        if(action==SQLITE_UPDATE&&ddl&&main&&catalog) {
            for(const char* column:{"type","name","tbl_name","rootpage","sql"})if(equal(second,column))return SQLITE_OK;
            return SQLITE_DENY;
        }
        if(action==SQLITE_READ&&main) {
            if(ddl&&catalog&&equal(second,"ROWID"))return SQLITE_OK;
            // All target columns/index programs come from this exact fixed
            // library CREATE, with strict actual no-trigger prefix proof.
            if(equal(first,target))return SQLITE_OK;
        }
        return SQLITE_DENY;
    }
    if(storage_->id==kind::enrollment_body_savepoint||storage_->id==kind::enrollment_body_release) {
        if(!storage_->view->enrollment_owner||!storage_->view->enrollment_admission)return SQLITE_DENY;
        return action==SQLITE_SAVEPOINT&&equal(second,"_lattice_recovery_body_settled")&&
            equal(first,storage_->id==kind::enrollment_body_savepoint?"BEGIN":"RELEASE")?SQLITE_OK:SQLITE_DENY;
    }
    if(action==SQLITE_SELECT)return SQLITE_OK;
    if(action==SQLITE_FUNCTION) {
        for(const char* name:{"typeof","length","count","coalesce","sum","substr","glob"})
            if(equal(second,name))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(action==SQLITE_READ) {
        if(!equal(schema,storage_->schema))return SQLITE_DENY;
        if(storage_->table&&(equal(first,storage_->table)||
           (equal(storage_->table,"sqlite_master")&&equal(first,"sqlite_schema"))))return SQLITE_OK;
        // Only the pre-proved built-in targeted metadata module may inspect
        // catalog bytes as part of its closed implementation. Public reentry
        // creates no such descriptor and gets no inherited permission.
        if(storage_->system&&(equal(first,"sqlite_master")||equal(first,"sqlite_schema")))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(action==SQLITE_PRAGMA) {
        switch(storage_->id) {
        case kind::journal_mode:return equal(first,"journal_mode")&&!second?SQLITE_OK:SQLITE_DENY;
        case kind::synchronous:return equal(first,"synchronous")&&!second?SQLITE_OK:SQLITE_DENY;
        case kind::databases:return equal(first,"database_list")&&!second?SQLITE_OK:SQLITE_DENY;
        case kind::model_columns:
            if(equal(first,"table_xinfo")&&storage_->params.size()==1) {
                const auto* table=std::get_if<std::string>(&storage_->params[0]);
                return table&&second&&*table==second?SQLITE_OK:SQLITE_DENY;
            }
            return SQLITE_DENY;
        default:return SQLITE_DENY;
        }
    }
    return SQLITE_DENY;
}
recovery_obligation_bootstrap_reader::recovery_obligation_bootstrap_reader(
    std::uint64_t issuer,state* held)noexcept:creator_(issuer),state_(held){}
recovery_obligation_bootstrap_reader::~recovery_obligation_bootstrap_reader()noexcept {
    if(creator_!=database_native_process()||!state_)return;
    if(state_->view)state_->view->active.store(false,std::memory_order_release);
    delete state_; // exact private transaction RAII owns only its preissued cleanup
}
std::unique_ptr<recovery_obligation_bootstrap_reader> recovery_obligation_bootstrap_reader::create_(
    database& actual,const recovery_obligation_producer_discovery_limits& caps,bool shared) {
    actual.require_creator_();if(!actual.delivery_startup_)return {};
    const auto issuer=database_native_process();
    if(actual.initialization_callback_active_())throw db_error("bootstrap refuses actual SQLite callback reentry");
    auto* open=actual.delivery_startup_.get();open->require_work_current_(actual);
    auto* budget=open->initialization_budget_(actual).get();
    if(caps.obligations.scopes<0||caps.obligations.records<0||caps.obligations.field_bytes<=0||
       caps.obligations.encoded_bytes<0||caps.installations.channels<0||caps.installations.field_bytes<=0||
       caps.installations.encoded_bytes<0||caps.producers.profiles<0||caps.producers.stamps<0||
       caps.producers.field_bytes<=0||caps.producers.manifest_bytes<0||caps.producers.encoded_bytes<0)
        throw db_error("bootstrap requires finite independent discovery declarations");
    // Scratch is one synchronous closed generator/parameter staging liability.
    // Large audit/profile collection liability is charged only after actual
    // fixed policy validation, before any corresponding collection is built.
    const auto scratch=bootstrap_add(131072,bootstrap_mul(8,std::max<std::uint64_t>(
        caps.obligations.field_bytes,caps.producers.field_bytes)));
    state* held=nullptr;recovery_obligation_bootstrap_reader* result=nullptr;
    try {
        budget->claim_workspace_(bootstrap_add(scratch,sizeof(state)+sizeof(recovery_obligation_bootstrap_statement::snapshot)));
        held=new state(issuer);
        if(issuer!=database_native_process())throw db_error("inherited bootstrap reader allocation");
        actual.require_creator_();held->owner=&actual;held->independent=caps;held->effective=caps;held->shared_domains=shared;
        auto* raw=new recovery_obligation_bootstrap_statement::snapshot(issuer);
        if(issuer!=database_native_process())throw db_error("inherited bootstrap snapshot allocation");
        held->view=std::shared_ptr<recovery_obligation_bootstrap_statement::snapshot>(raw,
            [issuer](auto* value)noexcept{if(issuer==database_native_process())delete value;});
        if(issuer!=database_native_process())throw db_error("inherited bootstrap snapshot custody");
        raw->owner=&actual;raw->open=open->initialization_original_(actual);raw->budget=open->initialization_budget_(actual);
        if(issuer!=database_native_process())throw db_error("inherited bootstrap issuer capture");
        result=new recovery_obligation_bootstrap_reader(issuer,held);
        if(issuer!=database_native_process())throw db_error("inherited bootstrap reader wrapper");
        return std::unique_ptr<recovery_obligation_bootstrap_reader>(result);
    }catch(...) {
        if(issuer==database_native_process()) {budget->fail_();if(result)delete result;else delete held;}
        throw;
    }
}
std::unique_ptr<recovery_obligation_bootstrap_reader>
recovery_obligation_bootstrap_reader::create_continuous_enrollment_(lattice_db& owner,database& actual) {
    recovery_obligation_producer_discovery_limits caps{};bool shared=false;
    const auto* admission=recovery_continuous_producer::enrollment_reader_binding_(owner,actual,&caps,&shared);
    if(!admission)throw db_error("continuous bootstrap reader lost the actual retained pending enrollment");
    auto reader=create_(actual,caps,shared);
    if(!reader||reader->creator_!=database_native_process())
        throw db_error("continuous enrollment requires its current actual pending startup reader");
    reader->state_->view->enrollment_owner=&owner;reader->state_->view->enrollment_admission=admission;
    reader->state_->view->enrollment_reader=reader.get();
    reader->require_continuous_enrollment_(owner,actual);return reader;
}
void recovery_obligation_bootstrap_reader::require_continuous_enrollment_(const lattice_db& parent,const database& actual)const {
    require_owner_(actual);
    const auto& view=*state_->view;
    if(view.enrollment_owner!=&parent||!view.enrollment_admission||
       recovery_continuous_producer::enrollment_reader_binding_(parent,actual)!=view.enrollment_admission)
        throw db_error("continuous bootstrap reader/actual enrollment owner differ");
}
void recovery_obligation_bootstrap_reader::require_owner_(const database& actual)const {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        throw db_error("inherited bootstrap reader");
    actual.require_creator_();
    if(state_->owner!=&actual||!state_->view||state_->view->builder!=std::this_thread::get_id()||
       actual.delivery_startup_.get()!=state_->view->open.get())throw db_error("bootstrap reader/open differ");
}
void recovery_obligation_bootstrap_reader::claim_workspace_(std::uint64_t amount)const {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap workspace");
    require_owner_(*state_->owner);state_->view->budget->claim_workspace_(amount);
}
bool recovery_obligation_bootstrap_reader::shared_domains_()const {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap declaration");
    require_owner_(*state_->owner);return state_->shared_domains;
}
void recovery_obligation_bootstrap_reader::fail_()const noexcept {
    if(creator_==database_native_process()&&state_&&state_->creator==creator_&&state_->view)
        state_->view->budget->fail_();
}
void recovery_obligation_bootstrap_reader::begin_() {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap BEGIN");
    auto& actual=*state_->owner;require_owner_(actual);
    if(state_->view->enrollment_owner)throw db_error("enrollment reader requires its closed owned WRITE entry");
    if(state_->transaction)throw db_error("bootstrap BEGIN already issued");
    try {
        using recipe=observer_delivery_initialization_sql::recipe;
        auto start=observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,recipe::schema_transaction_deferred_begin);
        auto commit=observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,recipe::schema_transaction_commit);
        auto rollback=observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,recipe::schema_transaction_rollback);
        state_->transaction.emplace(actual.begin_initialization_(start,commit,rollback));
        if(creator_!=database_native_process())throw db_error("inherited actual bootstrap BEGIN return");
        state_->view->episode=std::atomic_load(&actual.initialization_episode_);
        if(!state_->view->episode||!state_->view->episode->live_.load(std::memory_order_acquire))
            throw db_error("bootstrap BEGIN issued no live actual receipt");
        state_->view->active.store(true,std::memory_order_release);
    }catch(...) {fail_();throw;}
}
void recovery_obligation_bootstrap_reader::begin_continuous_enrollment_(std::exception_ptr& cleanup_error) {
    if(creator_!=database_native_process()||!state_||!state_->view||!state_->view->enrollment_owner)
        throw db_error("inherited or absent enrollment BEGIN original");
    auto& actual=*state_->owner;require_continuous_enrollment_(*state_->view->enrollment_owner,actual);
    if(state_->transaction||state_->view->episode)throw db_error("continuous enrollment BEGIN already issued");
    try {
        using recipe=observer_delivery_initialization_sql::recipe;
        auto start=observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,recipe::schema_transaction_begin);
        auto commit=observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,recipe::schema_transaction_commit);
        auto rollback=observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,recipe::schema_transaction_rollback);
        state_->transaction.emplace(actual.begin_initialization_(start,commit,rollback,cleanup_error));
        if(creator_!=database_native_process())throw db_error("inherited actual enrollment BEGIN return");
        state_->view->episode=std::atomic_load(&actual.initialization_episode_);
        if(!state_->view->episode||state_->view->episode->owner_!=&actual||
           state_->view->episode->budget_!=state_->view->budget||
           !state_->view->episode->live_.load(std::memory_order_acquire)||
           state_->view->episode->ended_.load(std::memory_order_acquire))
            throw db_error("continuous enrollment BEGIN issued no actual live original");
        state_->view->active.store(true,std::memory_order_release);
    }catch(...) {
        // The constructor may have thrown AFTER Native BEGIN and its one
        // actual cleanup attempt. Preserve actual receipt/debt DATA without
        // any throwing health observation or guessed Native ownership.
        if(creator_==database_native_process()) {
            if(!state_->view->episode)state_->view->episode=std::atomic_load(&actual.initialization_episode_);
            fail_();
        }
        throw;
    }
}
bool recovery_obligation_bootstrap_reader::enrollment_transaction_issued_()const noexcept {
    return creator_==database_native_process()&&state_&&state_->creator==creator_&&
        state_->view&&state_->view->enrollment_owner&&state_->transaction.has_value();
}
bool recovery_obligation_bootstrap_reader::enrollment_transaction_live_()const noexcept {
    if(!enrollment_transaction_issued_())return false;
    const auto& view=*state_->view;
    return view.episode&&view.episode->creator_==creator_&&view.episode->owner_==state_->owner&&
        view.episode->budget_==view.budget&&view.episode->live_.load(std::memory_order_acquire)&&
        !view.episode->ended_.load(std::memory_order_acquire);
}
bool recovery_obligation_bootstrap_reader::enrollment_begin_debt_()const noexcept {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||!state_->view)return false;
    const auto& view=*state_->view;
    return (view.episode&&view.episode->creator_==creator_&&view.episode->owner_==state_->owner&&
        view.episode->budget_==view.budget&&view.episode->live_.load(std::memory_order_acquire)&&
        !view.episode->ended_.load(std::memory_order_acquire))||state_->owner->observer_policy_unsettled_();
}
void recovery_obligation_bootstrap_reader::commit_continuous_enrollment_() {
    if(!enrollment_transaction_issued_())throw db_error("continuous enrollment COMMIT lacks its actual ending owner");
    require_continuous_enrollment_(*state_->view->enrollment_owner,*state_->owner);
    state_->transaction->commit();
    if(creator_!=database_native_process())throw db_error("inherited actual enrollment COMMIT return");
    state_->view->active.store(false,std::memory_order_release);state_->transaction.reset();
}
void recovery_obligation_bootstrap_reader::rollback_continuous_enrollment_() {
    if(!enrollment_transaction_issued_())throw db_error("continuous enrollment rollback lacks its actual ending owner");
    // No work-current health check before cleanup: the original transaction's
    // own exact prepaid rollback performs all real creator/Native admission.
    state_->transaction->rollback();
    if(creator_!=database_native_process())throw db_error("inherited actual enrollment rollback return");
    state_->view->active.store(false,std::memory_order_release);state_->transaction.reset();
}
void recovery_obligation_bootstrap_reader::retire_failed_enrollment_rollback_()noexcept {
    if(!enrollment_transaction_issued_())return;
    if(state_->transaction->retire_failed_rollback_()) {
        state_->view->active.store(false,std::memory_order_release);state_->transaction.reset();
    }
}
void recovery_obligation_bootstrap_reader::settle_continuous_enrollment_body_() {
    if(!enrollment_transaction_issued_())throw db_error("continuous enrollment settlement lacks its actual owned WRITE");
    require_continuous_enrollment_(*state_->view->enrollment_owner,*state_->owner);
    (void)run_(kind::enrollment_body_savepoint,{});(void)run_(kind::enrollment_body_release,{});
}
void recovery_obligation_bootstrap_reader::require_enrollment_current_()const {
    if(!enrollment_transaction_live_()||!state_->view->enrollment_owner)
        throw db_error("ledger initialization lacks its actual live enrollment WRITE");
    require_continuous_enrollment_(*state_->view->enrollment_owner,*state_->owner);
    state_->view->budget->require_current_();
}
database* recovery_obligation_bootstrap_reader::state_owner_()const {
    require_enrollment_current_();return state_->owner;
}
const recovery_obligation_producer_discovery_limits& recovery_obligation_bootstrap_reader::enrollment_declaration_()const {
    require_enrollment_current_();return state_->effective;
}
void recovery_obligation_bootstrap_reader::capture_enrollment_ledger_prefix_(enrollment_ledger_proof selected) {
    require_enrollment_current_();auto& actual=*state_->owner;
    if(selected!=enrollment_ledger_proof::construction&&selected!=enrollment_ledger_proof::complete)
        throw db_error("unissued actual ledger proof stage");
    const bool complete=selected==enrollment_ledger_proof::complete;
    using phase=observer_delivery_open_startup::enrollment_ledger_phase;
    using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>>;
    claim_workspace_(custody::allocation_bytes());
    custody proof;
    if(creator_!=database_native_process())throw db_error("inherited ledger prefix proof custody");
    proof.get<0>()=actual.delivery_startup_->capture_enrollment_ledger_preimage_(actual,*this,
        complete?phase::complete:phase::construction);
    if(creator_!=database_native_process())throw db_error("inherited ledger prefix proof return");
    state_->enrollment_proof=std::move(proof.get<0>());
    if(complete)state_->view->preimage=state_->enrollment_proof;
}
void recovery_obligation_bootstrap_reader::execute_enrollment_schema_(kind id) {
    require_enrollment_current_();
    if(!state_->enrollment_schema_started||state_->enrollment_schema_complete||
       !recovery_obligation_bootstrap_statement::enrollment_schema_kind_(id)||!state_->enrollment_proof||
       state_->view->enrollment_schema_active.load(std::memory_order_acquire))
        throw db_error("ledger operator is not its unique actual initialization sequence");
    const auto& caps=state_->effective;std::vector<column_value_t> parameters;
    // Fixed <=5 scalar cells are within the original bootstrap scratch charge.
    switch(id) {
    case kind::enrollment_install_seed:parameters={caps.installations.channels,caps.installations.field_bytes,caps.installations.encoded_bytes};break;
    case kind::enrollment_obligation_seed:parameters={caps.obligations.scopes,caps.obligations.records,caps.obligations.field_bytes,caps.obligations.encoded_bytes};break;
    case kind::enrollment_producer_seed:parameters={caps.producers.profiles,caps.producers.stamps,caps.producers.field_bytes,caps.producers.manifest_bytes,caps.producers.encoded_bytes};break;
    default:break;
    }
    if(creator_!=database_native_process())throw db_error("inherited ledger scalar parameter staging");
    state_->view->enrollment_schema_active.store(std::uint32_t(id)+1,std::memory_order_release);
    try {
        (void)run_(id,parameters);
        if(creator_!=database_native_process())throw db_error("inherited actual ledger operator return");
        state_->view->enrollment_schema_active.store(0,std::memory_order_release);
        if(id==kind::enrollment_install_seed||id==kind::enrollment_obligation_seed||id==kind::enrollment_producer_seed) {
            require_enrollment_current_();
            if(state_->owner->changes()!=1)throw db_error("ledger policy seed did not change exactly its one new row");
        }
    }catch(...) {
        if(creator_==database_native_process()){state_->view->enrollment_schema_active.store(0,std::memory_order_release);fail_();}
        throw;
    }
}
void recovery_obligation_bootstrap_reader::initialize_enrollment_ledgers_() {
    require_enrollment_current_();auto& view=*state_->view;
    if(state_->enrollment_schema_started||!recovery_continuous_producer::enrollment_reader_fresh_(
        *view.enrollment_owner,*state_->owner,view.enrollment_admission))
        throw db_error("ledger creation requires the actual unconsumed fresh creation lineage");
    state_->enrollment_schema_started=true;
    try {
        if(!query_(kind::enrollment_ledger_presence).empty()||
           !query_(kind::continuous_presence,{std::string("_lattice_producer_continuity")}).empty())
            throw db_error("fresh ledger creation found prior actual lineage/history");
        constexpr kind sequence[]={kind::enrollment_install_store_create,kind::enrollment_install_channel_create,
            kind::enrollment_install_domain_create,kind::enrollment_obligation_store_create,kind::enrollment_obligation_scope_create,
            kind::enrollment_obligation_entry_create,kind::enrollment_obligation_audit_create,kind::enrollment_obligation_original_create,
            kind::enrollment_obligation_domain_create,kind::enrollment_producer_store_create,kind::enrollment_producer_profile_create,
            kind::enrollment_producer_stamp_create};
        for(const auto id:sequence) {
            if(!state_->shared_domains&&(id==kind::enrollment_install_domain_create||id==kind::enrollment_obligation_domain_create))continue;
            capture_enrollment_ledger_prefix_(enrollment_ledger_proof::construction);execute_enrollment_schema_(id);
        }
        capture_enrollment_ledger_prefix_(enrollment_ledger_proof::complete); // every actual table/index required BEFORE any policy seed
        execute_enrollment_schema_(kind::enrollment_install_seed);
        execute_enrollment_schema_(kind::enrollment_obligation_seed);
        execute_enrollment_schema_(kind::enrollment_producer_seed);
        verify_seeded_enrollment_ledgers_();
        require_enrollment_current_();state_->enrollment_schema_complete=true;
        if(const auto hook=observer_delivery_startup_test_probe::after_continuous_enrollment_ledgers)
            hook(*view.enrollment_owner,{state_->owner,view.enrollment_admission,view.open.get(),view.budget.get(),view.episode.get(),false});
        if(creator_!=database_native_process())throw db_error("inherited actual ledger completion observation return");
    }catch(...) {fail_();throw;}
}
const receive_install_binding& recovery_obligation_bootstrap_reader::enrollment_receiver_binding_()const {
    require_enrollment_current_();const auto& view=*state_->view;
    if(!state_->enrollment_schema_complete||state_->receiver_contribution==std::size_t(-1)||
       view.enrollment_receiver_program.load(std::memory_order_acquire)!=state_->receiver_contribution+1)
        throw db_error("receiver has no actual selected enrollment contribution");
    return recovery_continuous_producer::enrollment_receiver_binding_(*view.enrollment_owner,*state_->owner,
        view.enrollment_admission,state_->receiver_contribution);
}
const receive_install_limits& recovery_obligation_bootstrap_reader::enrollment_receiver_limits_()const {
    (void)enrollment_receiver_binding_();return state_->effective.installations;
}
void recovery_obligation_bootstrap_reader::bind_enrollment_receiver_(std::size_t contribution) {
    require_enrollment_current_();auto& view=*state_->view;
    if(!state_->enrollment_schema_complete||!state_->enrollment_proof||state_->receiver_contribution!=std::size_t(-1)||
       view.enrollment_schema_active.load(std::memory_order_acquire)||view.enrollment_receiver_active.load(std::memory_order_acquire)||
       view.enrollment_receiver_program.load(std::memory_order_acquire)||contribution==std::size_t(-1))
        throw db_error("receiver bind is not its unique completed-ledger enrollment program");
    (void)recovery_continuous_producer::enrollment_receiver_binding_(*view.enrollment_owner,*state_->owner,view.enrollment_admission,contribution);
    state_->receiver_contribution=contribution;view.enrollment_receiver_program.store(contribution+1,std::memory_order_release);
    try {
        const auto usage=receive_install_store::bind_enrollment_(*this);
        if(creator_!=database_native_process())throw db_error("inherited actual receiver bind return");
        require_enrollment_current_();
        view.enrollment_receiver_program.store(0,std::memory_order_release);state_->receiver_contribution=std::size_t(-1);
        if(const auto hook=observer_delivery_startup_test_probe::after_continuous_enrollment_receiver)
            hook(*view.enrollment_owner,{state_->owner,view.enrollment_admission,view.open.get(),view.budget.get(),view.episode.get(),false},
                contribution,usage.channels,usage.encoded_bytes);
        if(creator_!=database_native_process())throw db_error("inherited actual receiver completion observation return");
    }catch(...) {
        if(creator_==database_native_process()) {
            view.enrollment_receiver_active.store(0,std::memory_order_release);
            view.enrollment_receiver_program.store(0,std::memory_order_release);state_->receiver_contribution=std::size_t(-1);fail_();
        }
        throw;
    }
}
std::vector<database::row_t> recovery_obligation_bootstrap_reader::enrollment_receiver_run_(kind selected,
    std::string_view channel,std::int64_t count,std::int64_t bytes,std::int64_t prior_count,std::int64_t prior_bytes,std::int64_t row_bytes) {
    require_enrollment_current_();const auto& binding=enrollment_receiver_binding_();auto& actual=*state_->owner;auto& view=*state_->view;
    if(!recovery_obligation_bootstrap_statement::enrollment_receiver_kind_(selected)||
       view.enrollment_receiver_active.load(std::memory_order_acquire)||!state_->enrollment_proof)
        throw db_error("unissued or reentered receiver enrollment operator");
    const auto& limits=state_->effective.installations;
    if(limits.channels<0||limits.field_bytes<=0||limits.field_bytes>(std::numeric_limits<int64_t>::max()-176)/16||limits.encoded_bytes<0)
        throw db_error("receiver enrollment declaration is not representable");
    const bool addressed=selected==kind::enrollment_receiver_row_sizes||selected==kind::enrollment_receiver_row_values;
    if(!addressed&&!channel.empty())throw db_error("receiver operator has no caller channel parameter");
    if(addressed&&channel.empty())channel=binding.channel;
    if(addressed&&(channel.empty()||channel.size()>std::uint64_t(limits.field_bytes)))throw db_error("receiver addressed channel exceeds actual declaration");
    if((selected==kind::enrollment_receiver_aliases&&!state_->shared_domains)||
       (selected==kind::enrollment_receiver_shared_schema&&!state_->shared_domains)||
       (selected==kind::enrollment_receiver_unique_domain&&state_->shared_domains))
        throw db_error("receiver operator differs from original shared-domain policy");
    if(selected!=kind::enrollment_receiver_usage&&(count||bytes||prior_count||prior_bytes))throw db_error("receiver operator has no counter parameters");
    std::uint64_t binding_bytes=0;
    for(const auto* field:{&binding.channel,&binding.authority,&binding.source,&binding.epoch,&binding.scope,&binding.schema}) {
        if(field->empty()||field->size()>std::uint64_t(limits.field_bytes))throw db_error("receiver original binding exceeds its actual declaration");
        binding_bytes=bootstrap_add(binding_bytes,field->size());
    }
    if(binding_bytes>std::uint64_t(limits.encoded_bytes))throw db_error("receiver original binding exceeds actual total declaration");
    if(selected==kind::enrollment_receiver_insert) {
        if(row_bytes<0||std::uint64_t(row_bytes)!=binding_bytes)throw db_error("receiver insert bytes differ from its actual immutable binding");
    }else if(row_bytes)throw db_error("receiver operator has no row-byte parameter");
    if(selected==kind::enrollment_receiver_usage) {
        if(prior_count<0||prior_count==std::numeric_limits<int64_t>::max()||count!=prior_count+1||count>limits.channels||
           prior_bytes<0||bytes<0||std::uint64_t(bytes)!=bootstrap_add(std::uint64_t(prior_bytes),binding_bytes)||bytes>limits.encoded_bytes)
            throw db_error("receiver usage CAS is not the exact bounded new binding increment");
    }
    recovery_obligation_bootstrap_statement::storage* held=nullptr;
    view.enrollment_receiver_active.store(std::uint32_t(selected)+1,std::memory_order_release);
    try {
        view.budget->require_current_();
        ordinary_initialization_storage_scope storage_scope(const_cast<observer_delivery_open_startup*>(view.open.get()),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        using custody=native_retirement_storage<std::shared_ptr<const recovery_obligation_bootstrap_statement>,std::vector<database::row_t>>;
        claim_workspace_(custody::allocation_bytes());custody retained;
        if(creator_!=database_native_process())throw db_error("inherited receiver descriptor custody");
        // Keep the existing bootstrap scratch reservation. Actual new SQL/BLOB
        // characters/control blocks additionally use this SAME debit account.
        claim_workspace_(32768+sizeof(recovery_obligation_bootstrap_statement::storage)+sizeof(recovery_obligation_bootstrap_statement));
        held=new recovery_obligation_bootstrap_statement::storage(creator_);
        if(creator_!=database_native_process())throw db_error("inherited receiver descriptor allocation");
        held->view=state_->view;held->id=selected;held->receiver_program=state_->receiver_contribution+1;
        held->enrollment_proof=state_->enrollment_proof;
        if(!actual.initialization_schema_revision_(*view.episode,*view.open,held->schema_revision))
            throw db_error("receiver descriptor lost its original actual schema revision");
        using recipe=receive_install_initialization_recipe;using text=ordinary_initialization_text;
        switch(selected) {
        case kind::enrollment_receiver_configuration:held->receiver_sql=recipe::configuration<text>();held->recipe.columns=7;held->recipe.rows=2;held->recipe.result_bytes=256;break;
        case kind::enrollment_receiver_shared_schema:
            held->receiver_sql="SELECT type,name,CASE WHEN length(CAST(sql AS BLOB))<=2048 THEN sql END AS sql FROM main.sqlite_schema WHERE name IN ('_lattice_install_channel','_lattice_install_domain') ORDER BY name LIMIT 3";
            held->recipe.columns=3;held->recipe.rows=3;held->recipe.result_bytes=8192;break;
        case kind::enrollment_receiver_row_sizes:held->receiver_sql=recipe::row_sizes<text>();held->recipe.columns=8;held->recipe.rows=2;held->recipe.result_bytes=512;break;
        case kind::enrollment_receiver_row_values:held->receiver_sql=recipe::row_values<text>(limits);held->recipe.columns=16;held->recipe.rows=2;
            held->recipe.result_bytes=bootstrap_add(1024,bootstrap_mul(2,std::uint64_t(limits.encoded_bytes)));break;
        case kind::enrollment_receiver_aliases:held->receiver_sql=recipe::aliases<text>(limits);held->recipe.columns=1;held->recipe.rows=17;
            held->recipe.result_bytes=bootstrap_add(1024,bootstrap_mul(17,std::uint64_t(limits.field_bytes)));break;
        case kind::enrollment_receiver_unique_domain:held->receiver_sql="SELECT 1 FROM main._lattice_install_channel WHERE authority=? AND scope=? LIMIT 1";
            held->recipe.columns=1;held->recipe.rows=1;held->recipe.result_bytes=32;break;
        case kind::enrollment_receiver_changes:held->receiver_sql=recipe::affected_rows();held->recipe.columns=1;held->recipe.rows=1;held->recipe.result_bytes=32;break;
        case kind::enrollment_receiver_insert:held->receiver_sql=recipe::insert_channel();held->recipe.columns=0;held->recipe.rows=0;held->recipe.result_bytes=0;break;
        case kind::enrollment_receiver_usage:held->receiver_sql=recipe::update_usage();held->recipe.columns=0;held->recipe.rows=0;held->recipe.result_bytes=0;break;
        default:throw db_error("unissued receiver recipe");
        }
        const auto integer=[&](int64_t value){auto& parameter=held->receiver_parameters.at(held->receiver_parameter_count++);parameter.type=1;parameter.integer=value;};
        const auto null=[&](){auto& parameter=held->receiver_parameters.at(held->receiver_parameter_count++);parameter.type=0;};
        const auto blob=[&](std::string_view value){auto& parameter=held->receiver_parameters.at(held->receiver_parameter_count++);
            parameter.type=2;parameter.bytes.assign(value.data(),value.size());
            if(creator_!=database_native_process())throw db_error("inherited receiver actual BLOB copy");};
        if(addressed)blob(channel);
        else if(selected==kind::enrollment_receiver_aliases||selected==kind::enrollment_receiver_unique_domain){blob(binding.authority);blob(binding.scope);}
        else if(selected==kind::enrollment_receiver_insert) {
            blob(binding.channel);blob(binding.authority);blob(binding.source);blob(binding.epoch);blob(binding.scope);blob(binding.schema);
            integer(0);null();integer(0);integer(0);null();null();integer(row_bytes);
        }else if(selected==kind::enrollment_receiver_usage){integer(count);integer(bytes);integer(prior_count);integer(prior_bytes);}
        if(creator_!=database_native_process())throw db_error("inherited receiver actual recipe return");
        if(held->receiver_sql.size()>16384||held->receiver_parameter_count!=std::size_t(std::count(held->receiver_sql.begin(),held->receiver_sql.end(),'?')))
            throw db_error("receiver actual recipe count/extent differs");
        auto* raw=new recovery_obligation_bootstrap_statement(creator_,held);held=nullptr;
        if(creator_!=database_native_process())throw db_error("inherited receiver descriptor wrapper");
        retained.get<0>()=std::shared_ptr<const recovery_obligation_bootstrap_statement>(raw,
            [issuer=creator_](auto* value)noexcept{if(issuer==database_native_process())delete value;},
            ordinary_initialization_allocator<recovery_obligation_bootstrap_statement>{});
        if(creator_!=database_native_process())throw db_error("inherited receiver descriptor publication");
        auto descriptor=observer_delivery_initialization_sql::from_bootstrap_(retained.get<0>(),*view.open,actual);
        if(selected==kind::enrollment_receiver_insert||selected==kind::enrollment_receiver_usage)actual.execute_initialization_(descriptor);
        else retained.get<1>()=actual.query_initialization_(descriptor);
        if(creator_!=database_native_process())throw db_error("inherited receiver actual SQL return");
        require_enrollment_current_();view.enrollment_receiver_active.store(0,std::memory_order_release);
        return std::move(retained.get<1>());
    }catch(...) {
        if(creator_==database_native_process()){delete held;view.enrollment_receiver_active.store(0,std::memory_order_release);fail_();}
        throw;
    }
}

void recovery_obligation_bootstrap_reader::observe_continuous_enrollment_reads_(bool existing)const {
    if(!enrollment_transaction_issued_())throw db_error("continuous enrollment observation lacks its actual owned WRITE");
    const auto& view=*state_->view;
    require_continuous_enrollment_(*view.enrollment_owner,*state_->owner);
    if(!view.active.load(std::memory_order_acquire)||!view.preimage||!view.episode||
       !view.episode->live_.load(std::memory_order_acquire)||view.episode->ended_.load(std::memory_order_acquire))
        throw db_error("continuous enrollment observation lost its actual read prefix/episode");
    if(const auto hook=observer_delivery_startup_test_probe::after_continuous_enrollment_reads)
        hook(*view.enrollment_owner,{state_->owner,view.enrollment_admission,view.open.get(),view.budget.get(),view.episode.get(),existing});
    if(creator_!=database_native_process())throw db_error("inherited enrollment read observation return");
}
void recovery_obligation_bootstrap_reader::validated_family_() {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap schema capture");
    auto& actual=*state_->owner;require_owner_(actual);
    if(!state_->view->active.load(std::memory_order_acquire)||state_->view->preimage)
        throw db_error("bootstrap preimage requires its one actual read snapshot");
    try {
        auto proof=actual.delivery_startup_->capture_preimage_(actual,this);
        if(creator_!=database_native_process())throw db_error("inherited bootstrap preimage return");
        if(!proof)throw db_error("bootstrap actual preimage is absent");
        state_->view->preimage=std::move(proof);
    }catch(...) {fail_();throw;}
}
void recovery_obligation_bootstrap_reader::stored_limits_(const recovery_obligation_producer_discovery_limits& selected) {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap stored declaration");
    require_owner_(*state_->owner);const auto& cap=state_->independent;
    if(!state_->view->preimage||selected.obligations.scopes<0||selected.obligations.scopes>cap.obligations.scopes||
       selected.obligations.records<0||selected.obligations.records>cap.obligations.records||
       selected.obligations.field_bytes<=0||selected.obligations.field_bytes>cap.obligations.field_bytes||
       selected.obligations.encoded_bytes<0||selected.obligations.encoded_bytes>cap.obligations.encoded_bytes||
       selected.installations.channels<0||selected.installations.channels>cap.installations.channels||
       selected.installations.field_bytes<=0||selected.installations.field_bytes>cap.installations.field_bytes||
       selected.installations.encoded_bytes<0||selected.installations.encoded_bytes>cap.installations.encoded_bytes||
       selected.producers.profiles<0||selected.producers.profiles>cap.producers.profiles||
       selected.producers.stamps<0||selected.producers.stamps>cap.producers.stamps||
       selected.producers.field_bytes<=0||selected.producers.field_bytes>cap.producers.field_bytes||
       selected.producers.manifest_bytes<0||selected.producers.manifest_bytes>cap.producers.manifest_bytes||
       selected.producers.encoded_bytes<0||selected.producers.encoded_bytes>cap.producers.encoded_bytes)
        throw db_error("bootstrap stored declarations exceed independent policy");
    try {
        const auto values=bootstrap_add(std::uint64_t(selected.obligations.encoded_bytes),std::uint64_t(selected.producers.encoded_bytes));
        const auto cells=bootstrap_add(bootstrap_add(selected.obligations.scopes,selected.obligations.records),
            bootstrap_add(selected.producers.profiles,selected.producers.stamps));
        // All retained decoded audit/profile/scope containers and one generated
        // profile manifest are charged before the actual full-audit walk.
        const auto retained=bootstrap_add(bootstrap_mul(4,values),bootstrap_mul(1024,cells));
        // Actual adapter ceilings: a descriptor manifest <=1MiB; <=32
        // relations with <=32 columns; ordinary metadata already participates
        // in that manifest; aggregate enrolled SQL <=1MiB; each tail/complete
        // SQL <=256KiB. One relation's emitted/guard/normalization work can
        // coexist with both retained maps and both manifest copies before its
        // aggregate rejection. Reserve16MiB per declared profile in addition
        // to the decoded-payload/cell charge above, before any adapter staging.
        // This counts structured bytes/shells, not allocator/RSS consumption.
        const auto programs=bootstrap_mul(16777216,std::uint64_t(selected.producers.profiles));
        claim_workspace_(bootstrap_add(retained,programs));state_->effective=selected;
    }catch(...) {fail_();throw;}
}
void recovery_obligation_bootstrap_reader::rollback_() {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap rollback");
    require_owner_(*state_->owner);
    if(!state_->transaction)throw db_error("bootstrap rollback has no issued receipt");
    state_->transaction->rollback();
    if(creator_!=database_native_process())throw db_error("inherited bootstrap rollback return");
    state_->view->active.store(false,std::memory_order_release);state_->transaction.reset();
}
std::vector<database::row_t> recovery_obligation_bootstrap_reader::query_(
    kind selected,const std::vector<column_value_t>& parameters) {
    if(selected==kind::enrollment_body_savepoint||selected==kind::enrollment_body_release||
       recovery_obligation_bootstrap_statement::enrollment_schema_kind_(selected)||
       recovery_obligation_bootstrap_statement::enrollment_receiver_kind_(selected))
        throw db_error("bootstrap query cannot issue enrollment transaction controls");
    if((selected==kind::enrollment_audit_presence||selected==kind::enrollment_ledger_presence||
        selected==kind::enrollment_install_channel_presence)&&
       !enrollment_transaction_live_())throw db_error("enrollment presence read lacks its actual owned WRITE");
    return run_(selected,parameters);
}
std::vector<database::row_t> recovery_obligation_bootstrap_reader::run_(
    kind selected,const std::vector<column_value_t>& parameters) {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap query");
    auto& actual=*state_->owner;require_owner_(actual);
    auto& view=*state_->view;
    if(!view.active.load(std::memory_order_acquire)||!view.episode||
       !view.episode->live_.load(std::memory_order_acquire)||view.episode->ended_.load(std::memory_order_acquire))
        throw db_error("bootstrap query requires its actual live read snapshot");
    recovery_obligation_bootstrap_statement::storage* held=nullptr;
    try {
        view.budget->require_current_();
        // Pending Core storage shares the exact retained Open account even
        // when a preceding capture scope has ended. The generic reader keeps
        // its original unprofiled path; it does not gain pending authority.
        std::optional<ordinary_initialization_storage_scope> initialization_storage;
        if(view.open)initialization_storage.emplace(
            const_cast<observer_delivery_open_startup*>(view.open.get()),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        // Claim the fixed generator and parameter shells before any owning
        // recipe/parameter copy. The caller's synchronous borrowed parameters
        // must stay stable through this method, including allocator reentry.
        if(parameters.size()>5)throw db_error("bootstrap recipe parameter count");
        const auto field=std::max<std::uint64_t>(state_->effective.obligations.field_bytes,
            state_->effective.producers.field_bytes);
        const auto measure=[&]() {
            std::uint64_t bytes=bootstrap_mul(parameters.size(),sizeof(column_value_t));
            for(const auto& value:parameters) {
                if(const auto* text=std::get_if<std::string>(&value)) {
                    if(text->size()>field)throw db_error("bootstrap TEXT parameter exceeds declared field");
                    bytes=bootstrap_add(bytes,text->size());
                }else if(const auto* blob=std::get_if<std::vector<std::uint8_t>>(&value)) {
                    if(blob->size()>field)throw db_error("bootstrap BLOB parameter exceeds declared field");
                    bytes=bootstrap_add(bytes,blob->size());
                }else if(!std::holds_alternative<std::int64_t>(value))
                    throw db_error("bootstrap recipe requires closed integer/string/blob parameters");
            }
            return bytes;
        };
        const auto parameter_bytes=measure();
        using statement_custody=native_retirement_storage<
            std::shared_ptr<const recovery_obligation_bootstrap_statement>>;
        claim_workspace_(statement_custody::allocation_bytes());
        statement_custody statement;
        if(creator_!=database_native_process())throw db_error("inherited bootstrap statement custody");
        claim_workspace_(bootstrap_add(32768,bootstrap_add(parameter_bytes,
            sizeof(recovery_obligation_bootstrap_statement::storage)+sizeof(recovery_obligation_bootstrap_statement)+128)));
        held=new recovery_obligation_bootstrap_statement::storage(creator_);
        if(creator_!=database_native_process())throw db_error("inherited bootstrap recipe allocation");
        require_owner_(actual);held->view=state_->view;held->id=selected;
        if(!actual.initialization_schema_revision_(*view.episode,*view.open,held->schema_revision))
            throw db_error("bootstrap statement lacks its actual current schema revision");
        held->recipe=bootstrap_generate(selected,state_->effective);
        if(recovery_obligation_bootstrap_statement::enrollment_schema_kind_(selected)) {
            require_enrollment_current_();held->recipe.sql=enrollment_schema_sql_(selected);
            held->enrollment_table=enrollment_schema_table_(selected);held->enrollment_index=enrollment_schema_index_(selected);
            held->enrollment_proof=state_->enrollment_proof;
            if(!held->enrollment_table)throw db_error("unissued actual ledger schema target");
            if(selected!=kind::enrollment_install_seed&&selected!=kind::enrollment_obligation_seed&&selected!=kind::enrollment_producer_seed&&
               !held->enrollment_index)held->enrollment_implicit_indexes=observer_delivery_initialization_plan::enrollment_implicit_indexes_(
                    held->recipe.sql,held->enrollment_table);
        }
        if(creator_!=database_native_process())throw db_error("inherited bootstrap generator return");
        if(held->recipe.sql.size()>16384)throw db_error("bootstrap generator exceeded its fixed shell");
        // The exact regenerated SQL fixes count independently of parameter data.
        const auto expected=std::count(held->recipe.sql.begin(),held->recipe.sql.end(),'?');
        if(parameters.size()!=static_cast<std::size_t>(expected)||measure()!=parameter_bytes)
            throw db_error("bootstrap recipe parameter mismatch/growth");
        held->params.reserve(parameters.size());
        for(std::size_t i=0;i<parameters.size();++i) {
            if(creator_!=database_native_process())throw db_error("inherited bootstrap parameter copy");
            if(measure()!=parameter_bytes)throw db_error("bootstrap borrowed parameter growth");
            held->params.push_back(parameters[i]);
        }
        if(creator_!=database_native_process())throw db_error("inherited bootstrap parameter postimage");
        if(held->params.size()!=parameters.size())throw db_error("bootstrap parameter postimage count");
        const auto text_parameter=[&](std::size_t index)->const std::string& {
            if(index>=held->params.size())throw db_error("bootstrap missing identifier parameter");
            const auto* name=std::get_if<std::string>(&held->params[index]);
            if(!name||name->empty()||name->size()>128)throw db_error("bootstrap invalid identifier parameter");
            return *name;
        };
        const char* plain=nullptr;
        switch(selected) {
        case kind::enrollment_install_channel_presence:plain="_lattice_install_channel";break;
        case kind::obligation_policy:case kind::obligation_config:plain="_lattice_obligation_store";break;
        case kind::installation_policy:plain="_lattice_install_store";break;
        case kind::producer_policy:case kind::producer_config:plain="_lattice_obligation_producer_store";break;
        case kind::scope:case kind::scope_page_first:case kind::scope_page_after:
        case kind::scope_count:case kind::distinct_scopes:case kind::alias_channels:
            plain="_lattice_obligation_scope";break;
        case kind::entry:case kind::entry_page:case kind::record_count:case kind::distinct_entries:
            plain="_lattice_obligation_entry";break;
        case kind::actual_audit:case kind::enrollment_audit_presence:plain="AuditLog";break;
        case kind::producer_profile:case kind::profile_page_first:case kind::profile_page_after:
        case kind::producer_present:plain="_lattice_obligation_producer_profile";break;
        case kind::producer_stamp:case kind::stamp_page_first:case kind::stamp_page_after:
            plain="_lattice_obligation_producer_stamp";break;
        case kind::continuous_row:plain="_lattice_producer_continuity";break;
        case kind::model_columns: {
            const auto& table=text_parameter(0);
            if(!view.open->preimage_plain_table_for_(actual,view.preimage,table)||
               !view.open->preimage_system_metadata_for_(actual,view.preimage,"pragma_table_xinfo"))
                throw db_error("bootstrap targeted metadata lacks actual plain-table/module preimage");
            held->table="pragma_table_xinfo";held->system=true;break;
        }
        case kind::databases:
            if(!view.open->preimage_system_metadata_for_(actual,view.preimage,"pragma_database_list"))
                throw db_error("bootstrap database metadata lacks actual namespace/registration preimage");
            held->table="pragma_database_list";held->system=true;break;
        case kind::temporary_presence:held->table="sqlite_master";held->schema="temp";break;
        case kind::journal_mode:case kind::synchronous:break;
        case kind::enrollment_body_savepoint:case kind::enrollment_body_release:
            if(!enrollment_transaction_issued_())throw db_error("bootstrap control lacks actual enrollment WRITE custody");
            require_continuous_enrollment_(*view.enrollment_owner,actual);break;
        case kind::obligation_shape: {
            // SQLite table_list initializes unrelated views/modules before its
            // name predicate. Consume actual targeted engine shape from the
            // SAME validated snapshot instead; never prepare table_list here.
            const auto& table=text_parameter(0);std::uint64_t columns=0;bool without=false;
            if(!view.open->preimage_shape_for_(actual,view.preimage,table,columns,without)||
               columns>std::uint64_t(std::numeric_limits<std::int64_t>::max()))throw db_error("bootstrap actual table shape is absent");
            claim_workspace_(1024);
            std::vector<database::row_t> result;
            result.push_back({{"ncol",std::int64_t(columns)},{"wr",std::int64_t(without)}});
            delete held;held=nullptr;return result;
        }
        default:
            if(!recovery_obligation_bootstrap_statement::enrollment_schema_kind_(selected))held->table="sqlite_master";
            break;
        }
        if(plain) {
            if(!view.open->preimage_plain_table_for_(actual,view.preimage,plain))
                throw db_error("bootstrap app/ledger read lacks actual same-snapshot plain-table preimage");
            held->table=plain;
        }
        auto* raw=new recovery_obligation_bootstrap_statement(creator_,held);held=nullptr;
        if(creator_!=database_native_process())throw db_error("inherited bootstrap descriptor wrapper");
        statement.get<0>()=std::shared_ptr<const recovery_obligation_bootstrap_statement>(raw,
            [issuer=creator_](auto* value)noexcept{if(issuer==database_native_process())delete value;});
        if(creator_!=database_native_process())throw db_error("inherited bootstrap descriptor custody");
        auto descriptor=observer_delivery_initialization_sql::from_bootstrap_(statement.get<0>(),*actual.delivery_startup_,actual);
        if(selected==kind::enrollment_body_savepoint||selected==kind::enrollment_body_release||
           recovery_obligation_bootstrap_statement::enrollment_schema_kind_(selected)) {
            actual.execute_initialization_(descriptor);return {};
        }
        return actual.query_initialization_(descriptor);
    }catch(...) {
        if(creator_==database_native_process()){delete held;fail_();}
        throw;
    }
}
void recovery_obligation_bootstrap_reader::registration_enable_() {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap recursive-trigger setup");
    auto& actual=*state_->owner;require_owner_(actual);
    try {
        auto statement=observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,
            observer_delivery_initialization_sql::recipe::connection_recursive_triggers_on);
        actual.execute_initialization_(statement);
    }catch(...){fail_();throw;}
}
std::vector<database::row_t> recovery_obligation_bootstrap_reader::registration_getter_() {
    if(creator_!=database_native_process()||!state_)throw db_error("inherited bootstrap recursive-trigger getter");
    auto& actual=*state_->owner;require_owner_(actual);
    try {
        auto statement=observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,
            observer_delivery_initialization_sql::recipe::connection_recursive_triggers_getter);
        return actual.query_initialization_(statement);
    }catch(...){fail_();throw;}
}
} // namespace lattice::detail
