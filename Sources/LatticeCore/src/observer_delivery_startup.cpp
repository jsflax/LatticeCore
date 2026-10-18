#include <lattice/detail/observer_delivery_startup.hpp>
#include <lattice/lattice.hpp>
#include <lattice/native_retirement_storage.hpp>
#include "observer_delivery_startup_policy.hpp"
#include "recovery_producer_continuity.hpp"
#include "observer_delivery_startup_test_probe.hpp"
#include "observer_delivery_connection.hpp"
#include "observer_delivery_sql_runtime.hpp"
#include "observer_delivery_schema.hpp"
#include "database_sql_dispatcher.hpp"
#include "database_sql_read_admission.hpp"
#include "database_initialization_budget.hpp"
#include "observer_delivery_initialization_plan.hpp"
#include "ordinary_schema_validation.hpp"
#include "ordinary_initialization_storage.hpp"
#include "recovery_obligation_bootstrap_statement.hpp"
#include "receive_guard_initialization_statement.hpp"
#include "database_initialization_test_probe.hpp"
#include <atomic>
#include <initializer_list>
#include <algorithm>
#include <thread>
#include <utility>
#include <optional>
#include <array>

namespace lattice::detail {
namespace {
[[noreturn]] void startup_refuse(const char* reason){throw db_error(reason);}
}
struct observer_delivery_startup::state final {
    const std::uint64_t creator;
    const observer_delivery_startup_policy policy;
    // Borrowed only while the actual owned_resources constructor retains this
    // immutable pre-open catalog. finish_owner_ verifies the exact address and
    // relinquishes the borrow before publication. No destructor dereferences it.
    const recovery_owner_schema* catalog;
    const std::thread::id builder=std::this_thread::get_id();
    const bool read_only;
    const std::int64_t schema_version;
    std::atomic<bool> finished{false},failed{false};
    observer_delivery_store store;
    observer_delivery_store::client client;
    std::shared_ptr<database_initialization_budget> initial_budget;
    std::shared_ptr<const database_sql_read_profile> read_profile;
    std::shared_ptr<const observer_delivery_initialization_plan> plan;
    // Exact unpublished owner/catalog family. No Native/owner reference and
    // no completion authority; actual_owner is still set ONLY by real final
    // publication. Initial role issuance is finite and stops at this binding.
    lattice_db* pending_owner=nullptr;
    unsigned initial_roles=0;
    // Compare-only admission identity; never retains its Native directory or
    // writer. The actual admission owns this SAME startup/open DATA family.
    const recovery_continuous_admission* continuous_admission=nullptr;
    lattice_db* actual_owner=nullptr; // comparison DATA only; no Native retention
    state(observer_delivery_startup_policy selected,const recovery_owner_schema& actual,
        const configuration& config)
        :creator(selected.creator_),policy(std::move(selected)),catalog(&actual),
         read_only(config.read_only),schema_version(config.target_schema_version){}
};
struct observer_delivery_startup::replacement_episode final {
    const std::uint64_t creator;
    const std::thread::id builder=std::this_thread::get_id();
    const std::shared_ptr<observer_delivery_startup> startup;
    lattice_db* const owner; // comparison DATA only, never retains Native
    database* const anchor; // exact captured writer; comparison only, not Native ownership
    const bool read_pair;
    const std::chrono::steady_clock::time_point end;
    const std::shared_ptr<database_initialization_budget> budget;
    unsigned issued=0; // same original builder, before every actual open
    std::atomic<bool> failed{false},published{false};
    // Both fixed ownership slots are included in the real sizeof episode
    // reservation before open. They never hold an owner/database original.
    std::shared_ptr<const observer_delivery_connection> prepared_connection,displaced_connection;
    replacement_episode(std::uint64_t issuer,std::shared_ptr<observer_delivery_startup> context,
        lattice_db& actual,database& actual_anchor,bool pair,std::chrono::steady_clock::time_point deadline,
        std::shared_ptr<database_initialization_budget> credit)
        :creator(issuer),startup(std::move(context)),owner(&actual),anchor(&actual_anchor),read_pair(pair),end(deadline),budget(std::move(credit)){}
};
struct observer_delivery_open_startup::state final {
    const std::uint64_t creator;
    const observer_delivery_startup_policy policy;
    const std::shared_ptr<observer_delivery_startup> startup;
    const observer_delivery_startup::open_kind kind;
    const std::shared_ptr<observer_delivery_startup::replacement_episode> replacement;
    const std::thread::id builder=std::this_thread::get_id();
    std::atomic<bool> attempted{false},opened{false},finished{false},failed{false};
    database* actual=nullptr; // compare-only; no database/Native ownership
    std::shared_ptr<database_initialization_budget> budget;
    std::shared_ptr<const observer_delivery_initialization_stage> model_stage; // DATA+weak episode only, never Open/proof cycle
    const recovery_continuous_admission* fresh_catalog_original=nullptr; // comparison only, no authority reconstruction
    const store_resource_initialization_original* fresh_normal_original=nullptr; // actual transported original, never a READY selector
    const database_initialization_episode* fresh_catalog_snapshot=nullptr; // exact local transaction retains this original
    bool fresh_catalog_completed=false; // set only after actual successful fresh COMMIT, never durable freshness
    state(observer_delivery_startup_policy selected,std::shared_ptr<observer_delivery_startup> owner,
        observer_delivery_startup::open_kind operation,
        std::shared_ptr<observer_delivery_startup::replacement_episode> episode)
        :creator(selected.creator_),policy(std::move(selected)),startup(std::move(owner)),kind(operation),replacement(std::move(episode)){}
};
struct observer_delivery_initialization_sql::entry final {
    const std::uint64_t creator;
    std::atomic<std::uint64_t> references{1};
    // Builder mutates these ONLY in creator-fenced heap before issuance.
    // Thereafter retain_/views expose a single immutable entry; no setter.
    std::shared_ptr<const observer_delivery_open_startup> original;
    const void* const catalog_generation;
    const recipe id;
    ordinary_initialization_text sql;
    ordinary_initialization_vector<ordinary_initialization_text> text_parameters;
    // SAME privately issued immutable metadata recipe; no generic SQL copy or
    // observed-name digest can acquire this field. It owns DATA only.
    std::shared_ptr<observer_delivery_initialization_meter> meter;
    std::shared_ptr<const observer_delivery_initialization_plan> meter_plan;
    std::size_t meter_operator=0;
    database* meter_owner=nullptr;
    std::string_view meter_sql;
    std::shared_ptr<const ordinary_preimage_statement> metadata;
    std::shared_ptr<const recovery_obligation_bootstrap_statement> bootstrap;
    std::shared_ptr<const observer_delivery_initialization_preimage> schema_read_preimage;
    std::shared_ptr<const receive_guard_initialization_statement> receive;
    std::shared_ptr<const database_initialization_episode> metadata_snapshot;
    std::uint64_t metadata_revision=0;
    bool plan_schema_effect=false;
    std::shared_ptr<const observer_delivery_initialization_plan> model_plan;
    std::size_t model_operator=0;
    std::string_view model_sql;
    database* model_owner=nullptr;
    std::shared_ptr<const observer_delivery_initialization_preimage> model_preimage;
    std::shared_ptr<const observer_delivery_initialization_plan> metadata_plan;
    std::size_t metadata_operator=0;
    std::string_view metadata_sql;
    database* metadata_owner=nullptr; // comparison/dispatch only during actual request
    std::shared_ptr<const observer_delivery_initialization_preimage> metadata_write_preimage;
    std::shared_ptr<const observer_delivery_initialization_plan> sync_control_plan;
    std::size_t sync_control_operator=0;
    std::string_view sync_control_sql;
    database* sync_control_owner=nullptr; // comparison/dispatch during actual request
    std::shared_ptr<const observer_delivery_initialization_preimage> sync_control_write_preimage;
    std::shared_ptr<const observer_delivery_initialization_plan> static_sync_plan;
    std::size_t static_sync_operator=0;
    std::string_view static_sync_sql;
    database* static_sync_owner=nullptr; // comparison/dispatch only
    std::shared_ptr<const observer_delivery_initialization_preimage> static_sync_preimage;
    std::optional<observer_delivery_open_startup::static_sync_preimage_phase> static_sync_phase;
    std::shared_ptr<const observer_delivery_initialization_plan> audit_plan;
    std::size_t audit_operator=0;
    std::string_view audit_sql;
    database* audit_owner=nullptr; // comparison/dispatch only
    std::shared_ptr<const observer_delivery_initialization_preimage> audit_preimage;
    std::optional<observer_delivery_open_startup::audit_preimage_phase> audit_phase;
    entry(std::uint64_t issuer,const void* generation,recipe kind)noexcept
        :creator(issuer),catalog_generation(generation),id(kind){}
};
observer_delivery_initialization_sql::observer_delivery_initialization_sql(std::uint64_t creator,entry* value)noexcept
    :creator_(creator),entry_(value){}
observer_delivery_initialization_sql::observer_delivery_initialization_sql(observer_delivery_initialization_sql&& other)noexcept
    :creator_(other.creator_),entry_(other.creator_==database_native_process()?std::exchange(other.entry_,nullptr):nullptr){}
observer_delivery_initialization_sql::~observer_delivery_initialization_sql()noexcept {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)return;
    if(entry_->references.fetch_sub(1,std::memory_order_acq_rel)==1)delete entry_;
}
observer_delivery_initialization_sql observer_delivery_initialization_sql::retain_()const {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)
        startup_refuse("inherited initialization descriptor");
    auto previous=entry_->references.load(std::memory_order_relaxed);
    do {
        if(!previous||previous==UINT64_MAX)startup_refuse("initialization descriptor reference bound");
    }while(!entry_->references.compare_exchange_weak(previous,previous+1,std::memory_order_acq_rel));
    return observer_delivery_initialization_sql(creator_,entry_);
}
observer_delivery_initialization_sql observer_delivery_initialization_sql::from_meter_(
    const std::shared_ptr<observer_delivery_initialization_meter>& meter,std::size_t index,
    const observer_delivery_open_startup& open,database& actual) {
    try {
        open.require_work_current_(actual);
        const auto issuer=database_native_process();
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        const auto& plan=open.initialization_plan_(actual);
        if(!meter||!plan)startup_refuse("meter entry lacks actual closed continuation/Plan");
        recipe id=recipe::delivery_ledger;std::string_view sql;
        plan->meter_entry_(*meter,index,open,actual,id,sql);
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_open_startup>>;
        ordinary_initialization_storage_scope::charge(sizeof(entry)+custody::allocation_bytes());custody held;
        if(issuer!=database_native_process()||issuer!=open.creator_)startup_refuse("inherited meter entry custody");
        held.get<0>()=open.initialization_original_(actual);
        auto* value=new entry(issuer,open.catalog_generation_(),id);
        if(issuer!=database_native_process())startup_refuse("inherited meter entry allocation");
        try {
            value->original=std::move(held.get<0>());value->meter=meter;value->meter_plan=plan;
            value->meter_operator=index;value->meter_owner=&actual;value->meter_sql=sql;
            plan->meter_parameters_(*meter,index,open,actual,value->text_parameters);
            if(issuer!=database_native_process())startup_refuse("inherited meter parameter return");
            value->plan_schema_effect=true; // real execute retirement advances exact original once, including DML
            return observer_delivery_initialization_sql(issuer,value);
        }catch(...) {if(issuer==database_native_process())delete value;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_sql::from_preimage_(
    const std::shared_ptr<const ordinary_preimage_statement>& metadata,
    const observer_delivery_open_startup& open,database& actual) {
    try {
        open.require_initializing_(actual);
        if(!metadata||!metadata->creator_current()||
           metadata->kind_id()==ordinary_preimage_statement::kind::unissued||
           !metadata->row_limit()||!metadata->result_byte_limit()||
           metadata->parameter_count()>ordinary_preimage_statement::parameter_limit()||
           metadata->parameter_bytes()>ordinary_preimage_statement::parameter_byte_limit())
            startup_refuse("initialization metadata lacks its actual library recipe");
        // Bind the compatibility-denial seam before any real request retains
        // this immutable metadata. A legacy copy cannot silently bypass debit.
        metadata->enter_native_();
        const auto creator=database_native_process();
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_open_startup>,
            std::shared_ptr<const database_initialization_episode>>;
        open.claim_workspace_(actual,sizeof(entry)+custody::allocation_bytes());
        custody held;
        if(creator!=database_native_process())startup_refuse("inherited metadata entry custody");
        held.get<0>()=open.initialization_original_(actual);
        held.get<1>()=actual.capture_initialization_snapshot_(open);
        if(creator!=database_native_process())startup_refuse("inherited metadata entry snapshot");
        std::uint64_t revision=0;
        if(!actual.initialization_schema_revision_(*held.get<1>(),open,revision))
            startup_refuse("metadata recipe lacks its exact initialization revision");
        auto* value=new entry(creator,open.catalog_generation_(),recipe::preimage_metadata);
        if(creator!=database_native_process())startup_refuse("inherited metadata entry allocation");
        try {
            value->original=std::move(held.get<0>());value->metadata=metadata;
            value->metadata_snapshot=std::move(held.get<1>());value->metadata_revision=revision;
            if(creator!=database_native_process())startup_refuse("inherited metadata entry publication");
            return observer_delivery_initialization_sql(creator,value);
        }catch(...) {if(creator==database_native_process())delete value;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_sql::from_bootstrap_(
    const std::shared_ptr<const recovery_obligation_bootstrap_statement>& statement,
    const observer_delivery_open_startup& open,database& actual) {
    try {
        open.require_initializing_(actual);
        if(!statement||!statement->creator_current())
            startup_refuse("initialization bootstrap lacks its actual library recipe");
        statement->check_for_(actual); // SAME live private raw snapshot/open.
        using kind=recovery_obligation_bootstrap_statement::kind;
        const auto id=statement->kind_id();
        const bool savepoint=id==kind::enrollment_body_savepoint;
        const bool release=id==kind::enrollment_body_release;
        const bool ledger=statement->enrollment_schema_();
        const bool receiver_write=statement->enrollment_receiver_write_();
        if(savepoint||release||ledger||receiver_write) {
            // Only the two parameterless controls and the closed typed ledger
            // or receiver DML operators have zero result shape. Values/counts come
            // from the actual immutable declaration/generator; check_for_ and
            // the authorizer still require SAME owner/BEGIN/revision/prefix.
            if(statement->column_limit()||statement->row_limit()||statement->result_byte_limit()||
               (!ledger&&!receiver_write&&(!statement->parameters().empty()||statement->sql()!=(savepoint?
                    "SAVEPOINT _lattice_recovery_body_settled":"RELEASE _lattice_recovery_body_settled"))))
                startup_refuse("initialization enrollment control declaration differs");
        }else if(!statement->column_limit()||!statement->row_limit()||!statement->result_byte_limit())
            startup_refuse("initialization bootstrap declaration is empty");
        open.claim_workspace_(actual,sizeof(entry));
        const auto creator=database_native_process();
        native_retirement_storage<std::shared_ptr<const observer_delivery_open_startup>> held;
        if(creator!=database_native_process())startup_refuse("inherited bootstrap entry custody");
        held.get<0>()=open.initialization_original_(actual);
        auto* value=new entry(creator,open.catalog_generation_(),recipe::bootstrap_read);
        if(creator!=database_native_process())startup_refuse("inherited bootstrap entry allocation");
        try {
            value->original=std::move(held.get<0>());value->bootstrap=statement;
            if(creator!=database_native_process())startup_refuse("inherited bootstrap entry publication");
            return observer_delivery_initialization_sql(creator,value);
        }catch(...) {if(creator==database_native_process())delete value;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_sql::fixed_(
    const observer_delivery_open_startup& open,database& actual,recipe id) {
    try {
    open.require_initializing_(actual);
    ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
        &observer_delivery_open_startup::debit_initialization_storage_);
    const char* statement=nullptr;
    switch(id) {
    case recipe::busy_timeout_getter:statement="PRAGMA busy_timeout";break;
    case recipe::function_inventory:statement="PRAGMA function_list";break;
    case recipe::schema_cookie:statement="PRAGMA schema_version";break;
    case recipe::static_sync_inventory:statement="SELECT type,name,tbl_name AS owner,sql FROM main.sqlite_schema WHERE name COLLATE NOCASE IN ('_lattice_sync_set','_lattice_sync_state','idx_sync_state_pending','_lattice_replication_slots','_lattice_sync_set_v1') ORDER BY name LIMIT 6";break;
    case recipe::audit_inventory:statement="SELECT type,name,tbl_name AS owner,sql FROM main.sqlite_schema WHERE name COLLATE NOCASE IN ('AuditLog','idx_audit_log_pending_sync','idx_audit_log_change_lookup') ORDER BY name LIMIT 4";break;
    case recipe::receive_prior_table_presence:statement="SELECT 1 FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1";break;
    case recipe::metadata_table_discovery:statement="SELECT name FROM sqlite_master WHERE type='table' AND name='_lattice_meta'";break;
    case recipe::connection_foreign_keys:statement="PRAGMA foreign_keys = ON";break;
    case recipe::connection_recursive_triggers_on:statement="PRAGMA recursive_triggers=ON";break;
    case recipe::connection_recursive_triggers_getter:statement="PRAGMA recursive_triggers";break;
    case recipe::connection_journal:statement="PRAGMA journal_mode = WAL";break;
    case recipe::connection_cache_small:statement="PRAGMA cache_size = 2000";break;
    case recipe::connection_cache_large:statement="PRAGMA cache_size = 50000";break;
    case recipe::connection_mmap_off:statement="PRAGMA mmap_size = 0";break;
    case recipe::connection_mmap_on:statement="PRAGMA mmap_size = 300000000";break;
    case recipe::connection_temp_file:statement="PRAGMA temp_store = FILE";break;
    case recipe::connection_temp_memory:statement="PRAGMA temp_store = MEMORY";break;
    case recipe::connection_analysis:statement="PRAGMA analysis_limit=400";break;
    case recipe::connection_journal_size:statement="PRAGMA journal_size_limit=268435456";break;
    case recipe::connection_schema_count:statement="SELECT count(*) FROM sqlite_master";break;
    case recipe::continuous_schema_discovery:statement="SELECT 1 FROM main.sqlite_schema WHERE name='_lattice_producer_continuity' LIMIT 1";break;
    case recipe::continuous_synchronous_full:statement="PRAGMA synchronous=FULL";break;
    case recipe::continuous_synchronous_getter:statement="PRAGMA synchronous";break;
    case recipe::fresh_catalog_schema_presence:statement=ordinary_fresh_catalog_presence_program_;break;
    case recipe::continuous_fresh_schema_presence:statement="SELECT 1 FROM main.sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1";break;
    case recipe::schema_transaction_begin:statement="BEGIN IMMEDIATE";break;
    case recipe::schema_transaction_deferred_begin:statement="BEGIN";break;
    case recipe::schema_transaction_exclusive_begin:statement="BEGIN EXCLUSIVE";break;
    case recipe::schema_transaction_commit:statement="COMMIT";break;
    case recipe::schema_transaction_rollback:statement="ROLLBACK";break;
    default:startup_refuse("initialization recipe is not a fixed control");
    }
    const auto sql=std::string_view(statement);
    // Before the actual entry/string/control-block allocation. This fixed
    // control owns no user data or opaque callback; no SQL/type conversion.
    open.claim_workspace_(actual,sizeof(entry)+sql.size()+1);
    const auto creator=database_native_process();
    native_retirement_storage<std::shared_ptr<const observer_delivery_open_startup>> held;
    if(creator!=database_native_process())startup_refuse("inherited fixed initialization entry storage");
    held.get<0>()=open.initialization_original_(actual);
    auto* value=new entry(creator,open.state_->startup->state_,id);
    if(creator!=database_native_process())startup_refuse("inherited fixed initialization recipe allocation");
    try {
        value->original=std::move(held.get<0>());
        // Owning input is now only in creator-fenced heap. Allocation failure
        // or an inherited return cannot unwind a new automatic Native hold.
        value->sql.assign(sql);
        if(creator!=database_native_process())startup_refuse("inherited fixed initialization SQL copy");
        return observer_delivery_initialization_sql(creator,value);
    }catch(...) {if(creator==database_native_process())delete value;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_sql::from_plan_(
    const observer_delivery_initialization_plan& plan,std::size_t actual_entry,
    const observer_delivery_open_startup& open,database& actual,
    std::string_view marker_key,std::string_view marker_cookie,std::string_view version) {
    try {
    open.require_initializing_(actual);
    ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
        &observer_delivery_open_startup::debit_initialization_storage_);
    recipe id=recipe::model_table;std::string_view sql;
    plan.entry_(actual_entry,open,actual,id,sql);
    const bool model=plan.model_operator_for_(actual_entry,open,actual,id);
    const bool model_version=id==recipe::model_version_write;
    if(model_version) {
        if(version.empty()||version.size()>10||(version.size()>1&&version.front()=='0'))
            startup_refuse("model version parameter is not canonical");
        std::uint64_t value=0;for(const auto ch:version){if(ch<'0'||ch>'9')startup_refuse("model version parameter is not canonical");value=value*10+std::uint64_t(ch-'0');}
        if(value==0||value>std::uint64_t(open.initialization_schema_version_(actual)))
            startup_refuse("model version exceeds its actual immutable target");
    }else if(!version.empty())startup_refuse("non-version operator has no version parameter");
    const bool marker=id==recipe::schema_marker||id==recipe::schema_marker_prune;
    const bool metadata=id==recipe::metadata_table||id==recipe::metadata_initial_version||marker;
    if(marker) {
        constexpr std::string_view prefix="schema_fingerprint:";
        if(marker_key.size()!=prefix.size()+16||!marker_key.starts_with(prefix))
            startup_refuse("marker parameter key is not canonical");
        for(const auto ch:marker_key.substr(prefix.size()))
            if(!((ch>='0'&&ch<='9')||(ch>='a'&&ch<='f')))startup_refuse("marker parameter key is not canonical");
        if(marker_cookie.empty()||marker_cookie.size()>10||
           (marker_cookie.size()>1&&marker_cookie.front()=='0'))startup_refuse("marker cookie is not canonical");
        for(const auto ch:marker_cookie)if(ch<'0'||ch>'9')startup_refuse("marker cookie is not canonical");
    }else if(!marker_key.empty()||!marker_cookie.empty())startup_refuse("non-marker operator has no marker parameters");
    const bool sync_control=id==recipe::sync_control_table||id==recipe::sync_control_initial;
    const bool static_sync=id==recipe::sync_set||id==recipe::sync_state||id==recipe::sync_state_pending||
        id==recipe::replication_slots||id==recipe::replication_upload_floor;
    const bool audit=id==recipe::audit_table||id==recipe::audit_pending_index||id==recipe::audit_lookup_index;
    using audit_phase=observer_delivery_open_startup::audit_preimage_phase;
    audit_phase audit_step=audit_phase::table_create;
    using static_phase=observer_delivery_open_startup::static_sync_preimage_phase;
    static_phase sync_phase=static_phase::set_create;
    const auto creator=database_native_process();
    using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_open_startup>,
        std::shared_ptr<const observer_delivery_initialization_preimage>>;
    open.claim_workspace_(actual,sizeof(entry)+((metadata||sync_control||static_sync||audit||model)?0:sql.size()+1)+
        (marker?2*sizeof(column_value_t)+marker_key.size()+marker_cookie.size()+2:0)+
        (model_version?sizeof(column_value_t)+version.size()+1:0)+custody::allocation_bytes());
    custody held;
    if(creator!=database_native_process())startup_refuse("inherited catalog recipe custody");
    held.get<0>()=open.initialization_original_(actual);
    if(creator!=database_native_process())startup_refuse("inherited catalog recipe original");
    if(model) {
        if(open.state_->startup->state_->plan.get()!=&plan)startup_refuse("model entry is not its actual immutable Plan");
        held.get<1>()=open.capture_model_preimage_(actual);
        if(creator!=database_native_process())startup_refuse("inherited model prefix return");
        open.require_model_write_(actual,held.get<1>(),observer_delivery_open_startup::model_preimage_phase::construction);
        if(!held.get<1>()||!plan.model_preimage_for_(actual_entry,open,actual,*held.get<1>()))
            startup_refuse("model operator lacks actual required-present parent/table dependency");
    }
    if(metadata) {
        const auto& retained=open.state_->startup->state_->plan;
        if(retained.get()!=&plan||!plan.metadata_operator_for_(actual_entry,open,actual,id))
            startup_refuse("metadata entry differs from its actual immutable Plan operator");
        using selection=observer_delivery_initialization_plan::preimage_selection;
        using phase=observer_delivery_open_startup::metadata_preimage_phase;
        const bool seed=id==recipe::metadata_initial_version;
        held.get<1>()=plan.capture_preimage_(open,actual,nullptr,
            marker?selection::schema_metadata_marker_present:seed?selection::schema_metadata_seed:selection::schema_metadata_create);
        if(creator!=database_native_process())startup_refuse("inherited metadata operator preimage return");
        open.require_metadata_write_(actual,held.get<1>(),marker?phase::marker_write:seed?phase::seed_version:phase::create_table);
    }
    if(sync_control) {
        const auto& retained=open.state_->startup->state_->plan;
        if(retained.get()!=&plan||!plan.sync_control_operator_for_(actual_entry,open,actual,id))
            startup_refuse("sync-control entry differs from its actual immutable Plan operator");
        using selection=observer_delivery_initialization_plan::preimage_selection;
        using phase=observer_delivery_open_startup::sync_control_preimage_phase;
        const bool seed=id==recipe::sync_control_initial;
        held.get<1>()=plan.capture_preimage_(open,actual,nullptr,
            seed?selection::sync_control_seed:selection::sync_control_create);
        if(creator!=database_native_process())startup_refuse("inherited sync-control operator preimage return");
        open.require_sync_control_write_(actual,held.get<1>(),seed?phase::seed_row:phase::create_table);
    }
    if(static_sync) {
        const auto& retained=open.state_->startup->state_->plan;
        if(retained.get()!=&plan||!plan.static_sync_operator_for_(actual_entry,open,actual,id))
            startup_refuse("static sync entry differs from its actual immutable Plan operator");
        using selection=observer_delivery_initialization_plan::preimage_selection;
        selection selected=selection::static_sync_set_create;
        switch(id) {
          case recipe::sync_set:break;
          case recipe::sync_state:selected=selection::static_sync_state_create;sync_phase=static_phase::state_create;break;
          case recipe::sync_state_pending:selected=selection::static_sync_state_index_create;sync_phase=static_phase::state_index_create;break;
          case recipe::replication_slots:selected=selection::static_sync_slots_create;sync_phase=static_phase::slots_create;break;
          case recipe::replication_upload_floor:selected=selection::static_sync_upload_floor;sync_phase=static_phase::upload_floor;break;
          default:startup_refuse("unknown static sync Plan operator");
        }
        held.get<1>()=plan.capture_preimage_(open,actual,nullptr,selected);
        if(creator!=database_native_process())startup_refuse("inherited static sync operator preimage return");
        open.require_static_sync_write_(actual,held.get<1>(),sync_phase);
        if(id==recipe::replication_upload_floor&&held.get<1>()->static_sync_upload_current_for_(actual))
            startup_refuse("upload floor already current; no ALTER request may be issued");
    }
    if(audit) {
        const auto& retained=open.state_->startup->state_->plan;
        if(retained.get()!=&plan||!plan.audit_operator_for_(actual_entry,open,actual,id))
            startup_refuse("AuditLog entry differs from its actual immutable Plan operator");
        using selection=observer_delivery_initialization_plan::preimage_selection;
        auto selected=selection::audit_create;
        if(id==recipe::audit_pending_index){selected=selection::audit_pending_create;audit_step=audit_phase::pending_create;}
        else if(id==recipe::audit_lookup_index){selected=selection::audit_lookup_create;audit_step=audit_phase::lookup_create;}
        held.get<1>()=plan.capture_preimage_(open,actual,nullptr,selected);
        if(creator!=database_native_process())startup_refuse("inherited AuditLog operator preimage return");
        open.require_audit_write_(actual,held.get<1>(),audit_step);
    }
    auto* value=new entry(creator,open.catalog_generation_(),id);
    if(creator!=database_native_process())startup_refuse("inherited catalog recipe allocation");
    try {
        value->original=std::move(held.get<0>());
        if(model) {
            value->model_plan=open.state_->startup->state_->plan;value->model_operator=actual_entry;
            value->model_sql=sql;value->model_owner=&actual;value->model_preimage=std::move(held.get<1>());
            if(model_version) {
                value->text_parameters.reserve(1);
                if(creator!=database_native_process())startup_refuse("inherited version parameter allocation");
                if(value->text_parameters.capacity()!=1)startup_refuse("version parameter capacity differs");
                value->text_parameters.emplace_back(version.data(),version.size());
                if(creator!=database_native_process())startup_refuse("inherited version parameter copy");
            }
        }else if(metadata) {
            value->metadata_plan=open.state_->startup->state_->plan;
            value->metadata_operator=actual_entry;value->metadata_sql=sql;value->metadata_owner=&actual;
            value->metadata_write_preimage=std::move(held.get<1>());
            if(marker) {
                value->text_parameters.reserve(2);
                if(creator!=database_native_process())startup_refuse("inherited marker parameter allocation");
                if(value->text_parameters.capacity()!=2)startup_refuse("marker parameter capacity differs");
                value->text_parameters.emplace_back(marker_key.data(),marker_key.size());
                if(creator!=database_native_process())startup_refuse("inherited marker key copy");
                value->text_parameters.emplace_back(marker_cookie.data(),marker_cookie.size());
                if(creator!=database_native_process())startup_refuse("inherited marker cookie copy");
            }
        }else if(sync_control) {
            value->sync_control_plan=open.state_->startup->state_->plan;
            value->sync_control_operator=actual_entry;value->sync_control_sql=sql;value->sync_control_owner=&actual;
            value->sync_control_write_preimage=std::move(held.get<1>());
        }else if(static_sync) {
            value->static_sync_plan=open.state_->startup->state_->plan;
            value->static_sync_operator=actual_entry;value->static_sync_sql=sql;value->static_sync_owner=&actual;
            value->static_sync_preimage=std::move(held.get<1>());value->static_sync_phase=sync_phase;
        }else if(audit) {
            value->audit_plan=open.state_->startup->state_->plan;
            value->audit_operator=actual_entry;value->audit_sql=sql;value->audit_owner=&actual;
            value->audit_preimage=std::move(held.get<1>());value->audit_phase=audit_step;
        }else value->sql.assign(sql);
        value->plan_schema_effect=id!=recipe::model_metadata_read; // conservative write-attempt invalidation, not authority
        if(creator!=database_native_process())startup_refuse("inherited catalog recipe copy");
        return observer_delivery_initialization_sql(creator,value);
    }catch(...) {if(creator==database_native_process())delete value;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_sql::from_receive_guard_(
    const std::shared_ptr<const receive_guard_initialization_statement>& statement,
    const observer_delivery_open_startup& open,database& actual) {
    try {
        open.require_work_current_(actual);
        if(!statement||!statement->creator_current())startup_refuse("receive initializer lacks its actual closed statement");
        statement->check_for_(actual);
        const auto kind=statement->kind_id();
        using receive_kind=receive_guard_initialization_statement::kind;
        const bool writes=kind==receive_kind::create_store||kind==receive_kind::create_channel||
            kind==receive_kind::create_state_index||kind==receive_kind::seed_store;
        const bool no_result=statement->column_limit()==0&&statement->row_limit()==0&&
            statement->result_byte_limit()==0;
        if((writes?!no_result:(!statement->column_limit()||statement->column_limit()>7||
             !statement->row_limit()||statement->row_limit()>257||
             !statement->result_byte_limit()||statement->result_byte_limit()>2*1024*1024))||
           statement->parameters().size()>1)
            startup_refuse("receive initializer declaration is outside its closed bounds");
        const auto creator=database_native_process();
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_open_startup>>;
        open.claim_workspace_(actual,sizeof(entry)+custody::allocation_bytes());
        custody retained;
        if(creator!=database_native_process())startup_refuse("inherited receive initializer custody");
        retained.get<0>()=open.initialization_original_(actual);
        if(creator!=database_native_process())startup_refuse("inherited receive initializer original");
        auto* value=new entry(creator,open.catalog_generation_(),recipe::receive_initializer);
        if(creator!=database_native_process())startup_refuse("inherited receive initializer entry");
        try {
            value->original=std::move(retained.get<0>());value->receive=statement;
            if(creator!=database_native_process())startup_refuse("inherited receive initializer publication");
            return observer_delivery_initialization_sql(creator,value);
        }catch(...) {if(creator==database_native_process())delete value;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
bool observer_delivery_initialization_sql::model_work_current_for_(const database& actual)const noexcept {
    return creator_==database_native_process()&&entry_&&entry_->creator==creator_&&actual.creator_current_()&&
        !actual.catalog_quarantined_()&&entry_->original&&entry_->model_plan&&entry_->model_owner==&actual&&
        entry_->original->pending_for_(actual)&&entry_->catalog_generation==entry_->original->catalog_generation_()&&
        entry_->model_plan.get()==entry_->original->state_->startup->state_->plan.get()&&
        entry_->model_plan->model_operator_for_(entry_->model_operator,*entry_->original,actual,entry_->id)&&
        entry_->model_preimage&&entry_->model_plan->model_preimage_for_(entry_->model_operator,
            *entry_->original,actual,*entry_->model_preimage);
}
bool observer_delivery_initialization_sql::metadata_work_current_for_(const database& actual)const noexcept {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_||
       !actual.creator_current_()||actual.catalog_quarantined_()||
       !entry_->original||!entry_->metadata_plan||entry_->metadata_owner!=&actual||
       !entry_->original->pending_for_(actual)||entry_->catalog_generation!=entry_->original->catalog_generation_()||
       entry_->metadata_plan.get()!=entry_->original->state_->startup->state_->plan.get()||
       !entry_->metadata_plan->metadata_operator_for_(entry_->metadata_operator,*entry_->original,actual,entry_->id)||
       !entry_->metadata_write_preimage)return false;
    using phase=observer_delivery_open_startup::metadata_preimage_phase;
    const bool marker=entry_->id==recipe::schema_marker||entry_->id==recipe::schema_marker_prune;
    if(entry_->id!=recipe::metadata_table&&entry_->id!=recipe::metadata_initial_version&&!marker)return false;
    return entry_->metadata_write_preimage->metadata_phase_for_(actual,
        marker?phase::marker_write:entry_->id==recipe::metadata_initial_version?phase::seed_version:phase::create_table);
}
bool observer_delivery_initialization_sql::sync_control_work_current_for_(const database& actual)const noexcept {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_||
       !actual.creator_current_()||actual.catalog_quarantined_()||
       !entry_->original||!entry_->sync_control_plan||entry_->sync_control_owner!=&actual||
       !entry_->original->pending_for_(actual)||entry_->catalog_generation!=entry_->original->catalog_generation_()||
       entry_->sync_control_plan.get()!=entry_->original->state_->startup->state_->plan.get()||
       !entry_->sync_control_plan->sync_control_operator_for_(entry_->sync_control_operator,*entry_->original,actual,entry_->id)||
       !entry_->sync_control_write_preimage)return false;
    using phase=observer_delivery_open_startup::sync_control_preimage_phase;
    if(entry_->id!=recipe::sync_control_table&&entry_->id!=recipe::sync_control_initial)return false;
    return entry_->sync_control_write_preimage->sync_control_phase_for_(actual,
        entry_->id==recipe::sync_control_initial?phase::seed_row:phase::create_table);
}
bool observer_delivery_initialization_sql::static_sync_work_current_for_(const database& actual)const noexcept {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_||
       !actual.creator_current_()||actual.catalog_quarantined_()||!entry_->original||
       !entry_->static_sync_plan||entry_->static_sync_owner!=&actual||!entry_->static_sync_phase||
       !entry_->original->pending_for_(actual)||entry_->catalog_generation!=entry_->original->catalog_generation_()||
       entry_->static_sync_plan.get()!=entry_->original->state_->startup->state_->plan.get()||
       !entry_->static_sync_plan->static_sync_operator_for_(entry_->static_sync_operator,*entry_->original,actual,entry_->id)||
       !entry_->static_sync_preimage)return false;
    return entry_->static_sync_preimage->static_sync_phase_for_(actual,*entry_->static_sync_phase);
}
bool observer_delivery_initialization_sql::audit_work_current_for_(const database& actual)const noexcept {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_||
       !actual.creator_current_()||actual.catalog_quarantined_()||!entry_->original||
       !entry_->audit_plan||entry_->audit_owner!=&actual||!entry_->audit_phase||
       !entry_->original->pending_for_(actual)||entry_->catalog_generation!=entry_->original->catalog_generation_()||
       entry_->audit_plan.get()!=entry_->original->state_->startup->state_->plan.get()||
       !entry_->audit_plan->audit_operator_for_(entry_->audit_operator,*entry_->original,actual,entry_->id)||
       !entry_->audit_preimage)return false;
    return entry_->audit_preimage->audit_phase_for_(actual,*entry_->audit_phase);
}
bool observer_delivery_initialization_sql::schema_may_change_()const noexcept {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)return true;
    if(entry_->receive)return entry_->receive->schema_may_change_();
    if(entry_->bootstrap)return entry_->bootstrap->schema_may_change_();
    // All Plan entries originate in the actual immutable library schema plan.
    // Conservatively invalidating its static DML is safe; this does not grant
    // that entry any action, prove mutation or retire a Native original.
    return entry_->plan_schema_effect;
}
bool observer_delivery_initialization_sql::work_current_for_(const database& actual)const noexcept {
    // This predicate performs no Native call, allocation, throwing validation
    // or authority publication. It is deliberately NOT cleanup identity:
    // exact owned VMs still finalize after work health/snapshot proof fails.
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_||
       !actual.creator_current_()||!entry_->original)return false;
    const auto& open=*entry_->original;
    if(!open.creator_current_()||!open.pending_for_(actual)||
       entry_->catalog_generation!=open.catalog_generation_())return false;
    if(entry_->metadata) {
        if(!entry_->metadata->creator_current()||!entry_->metadata_snapshot)return false;
        std::uint64_t revision=0;
        if(!actual.initialization_schema_revision_(*entry_->metadata_snapshot,open,revision)||
           revision!=entry_->metadata_revision)return false;
    }
    if(entry_->meter&&(!entry_->meter_plan||entry_->meter_owner!=&actual||
       !entry_->meter_plan->meter_current_for_(*entry_->meter,open,actual,entry_->meter_operator)))return false;
    if(entry_->model_plan&&!model_work_current_for_(actual))return false;
    if(entry_->bootstrap&&!entry_->bootstrap->work_current_for_(actual))return false;
    if(entry_->receive&&!entry_->receive->work_current_for_(actual))return false;
    if(entry_->schema_read_preimage&&!entry_->schema_read_preimage->plain_table_for_(actual,"_lattice_meta"))return false;
    if((entry_->id==recipe::metadata_table||entry_->id==recipe::metadata_initial_version||
        entry_->id==recipe::schema_marker||entry_->id==recipe::schema_marker_prune)&&
       !metadata_work_current_for_(actual))return false;
    if((entry_->id==recipe::sync_control_table||entry_->id==recipe::sync_control_initial)&&
       !sync_control_work_current_for_(actual))return false;
    if(entry_->static_sync_plan&&!static_sync_work_current_for_(actual))return false;
    if((entry_->id==recipe::audit_table||entry_->id==recipe::audit_pending_index||entry_->id==recipe::audit_lookup_index)&&
       !audit_work_current_for_(actual))return false;
    // Remaining unimplemented Plan operators retain their existing DENY
    // action closure. A pending Open/generation is not write/schema authority.
    return true;
}
observer_delivery_initialization_sql observer_delivery_initialization_sql::metadata_read_(
    const observer_delivery_open_startup& open,database& actual,recipe id,std::string_view key) {
    try {
        open.require_work_current_(actual);
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        const char* text=nullptr;
        if(id==recipe::metadata_version_read) {
            if(!key.empty())startup_refuse("version recipe has no caller key");
            text="SELECT value FROM _lattice_meta WHERE key = 'schema_version'";
        }else if(id==recipe::metadata_marker_read) {
            // These are actual private Core/Swift fingerprint-key producers;
            // no arbitrary SQL/table/column or unbounded parameter is accepted.
            constexpr std::string_view prefix="schema_fingerprint:";
            if(key.size()!=prefix.size()+16||!key.starts_with(prefix))
                startup_refuse("fingerprint metadata key is not canonical");
            for(const auto ch:key.substr(prefix.size()))
                if(!((ch>='0'&&ch<='9')||(ch>='a'&&ch<='f')))
                    startup_refuse("fingerprint metadata key is not canonical");
            text="SELECT value FROM _lattice_meta WHERE key = ?";
        }else startup_refuse("metadata read recipe is not a closed value getter");
        const auto creator=database_native_process();
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>,
            std::shared_ptr<const observer_delivery_open_startup>>;
        open.claim_workspace_(actual,sizeof(entry)+std::char_traits<char>::length(text)+1+
            sizeof(column_value_t)+key.size()+1+custody::allocation_bytes());
        custody retained;
        if(creator!=database_native_process())startup_refuse("inherited metadata read custody");
        const auto& plan=open.state_->startup->state_->plan;
        if(!plan)startup_refuse("metadata read lacks its actual immutable plan");
        retained.get<0>()=plan->capture_preimage_(open,actual,nullptr,
            observer_delivery_initialization_plan::preimage_selection::schema_metadata_read);
        if(creator!=database_native_process())startup_refuse("inherited metadata preimage return");
        if(!retained.get<0>()||!retained.get<0>()->plain_table_for_(actual,"_lattice_meta"))
            startup_refuse("metadata getter requires its actual same-snapshot plain table");
        retained.get<1>()=open.initialization_original_(actual);
        if(creator!=database_native_process())startup_refuse("inherited metadata read original");
        auto* value=new entry(creator,open.catalog_generation_(),id);
        if(creator!=database_native_process())startup_refuse("inherited metadata read allocation");
        try {
            value->original=std::move(retained.get<1>());
            value->schema_read_preimage=std::move(retained.get<0>());
            value->sql.assign(text);
            if(creator!=database_native_process())startup_refuse("inherited metadata read SQL copy");
            if(id==recipe::metadata_marker_read) {
                value->text_parameters.reserve(1);
                if(creator!=database_native_process())startup_refuse("inherited metadata parameter vector");
                if(value->text_parameters.capacity()!=1)startup_refuse("metadata parameter capacity differs");
                value->text_parameters.emplace_back(key.data(),key.size());
                if(creator!=database_native_process())startup_refuse("inherited metadata parameter copy");
            }
            return observer_delivery_initialization_sql(creator,value);
        }catch(...) {if(creator==database_native_process())delete value;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
void observer_delivery_initialization_sql::check_for_(const database& actual)const {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_||!entry_->original)
        startup_refuse("initialization descriptor original unavailable");
    entry_->original->require_initializing_(actual);
    if(entry_->model_plan) {
        if(!model_work_current_for_(actual))startup_refuse("model operator lost its actual Plan/BEGIN/revision original");
        entry_->original->require_model_write_(const_cast<database&>(actual),entry_->model_preimage,
            observer_delivery_open_startup::model_preimage_phase::construction);
    }
    if(entry_->bootstrap)entry_->bootstrap->check_for_(actual);
    if(entry_->receive)entry_->receive->check_for_(actual);
    if(entry_->id==recipe::metadata_table||entry_->id==recipe::metadata_initial_version||
       entry_->id==recipe::schema_marker||entry_->id==recipe::schema_marker_prune) {
        using phase=observer_delivery_open_startup::metadata_preimage_phase;
        if(!metadata_work_current_for_(actual))
            startup_refuse("metadata operator lost its actual Plan/BEGIN/revision identity");
        entry_->original->require_metadata_write_(const_cast<database&>(actual),entry_->metadata_write_preimage,
            entry_->id==recipe::schema_marker||entry_->id==recipe::schema_marker_prune?phase::marker_write:
                entry_->id==recipe::metadata_initial_version?phase::seed_version:phase::create_table);
    }
    if(entry_->id==recipe::sync_control_table||entry_->id==recipe::sync_control_initial) {
        using phase=observer_delivery_open_startup::sync_control_preimage_phase;
        if(!sync_control_work_current_for_(actual))
            startup_refuse("sync-control operator lost its actual Plan/BEGIN/revision identity");
        entry_->original->require_sync_control_write_(const_cast<database&>(actual),entry_->sync_control_write_preimage,
            entry_->id==recipe::sync_control_initial?phase::seed_row:phase::create_table);
    }
    if(entry_->static_sync_plan) {
        if(!static_sync_work_current_for_(actual))startup_refuse("static sync operator lost its actual Plan/BEGIN/revision identity");
        entry_->original->require_static_sync_write_(const_cast<database&>(actual),entry_->static_sync_preimage,*entry_->static_sync_phase);
    }
    if(entry_->audit_plan) {
        if(!audit_work_current_for_(actual))startup_refuse("AuditLog operator lost its actual Plan/BEGIN/revision identity");
        entry_->original->require_audit_write_(const_cast<database&>(actual),entry_->audit_preimage,*entry_->audit_phase);
    }
    if(entry_->schema_read_preimage&&!entry_->schema_read_preimage->plain_table_for_(actual,"_lattice_meta"))
        startup_refuse("metadata read lost its actual retained snapshot");
    const auto& open=*entry_->original;
    if(entry_->catalog_generation!=open.state_->startup->state_)
        startup_refuse("initialization descriptor catalog generation differs");
    if(!work_current_for_(actual))startup_refuse("initialization descriptor lost its actual work snapshot");
}
std::string_view observer_delivery_initialization_sql::sql_()const {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)
        startup_refuse("inherited initialization SQL view");
    if(entry_->meter)return entry_->meter_sql;
    if(entry_->metadata)return entry_->metadata->sql_view();
    if(entry_->bootstrap)return entry_->bootstrap->sql();
    if(entry_->receive)return entry_->receive->sql();
    if(entry_->model_plan)return entry_->model_sql;
    if(entry_->metadata_plan)return entry_->metadata_sql;
    if(entry_->sync_control_plan)return entry_->sync_control_sql;
    if(entry_->static_sync_plan)return entry_->static_sync_sql;
    if(entry_->audit_plan)return entry_->audit_sql;
    return entry_->sql;
}
const std::vector<column_value_t>& observer_delivery_initialization_sql::parameters_()const {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)
        startup_refuse("inherited initialization parameters");
    if(entry_->metadata)startup_refuse("Native preimage requires its retained parameter views");
    if(entry_->bootstrap)return entry_->bootstrap->parameters();
    if(entry_->receive)return entry_->receive->parameters();
    startup_refuse("owned initialization parameters require retained TEXT views");
}
bool observer_delivery_initialization_sql::bootstrap_parameters_()const {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)
        startup_refuse("inherited bootstrap parameter view");
    return entry_->bootstrap&&entry_->bootstrap->receiver_parameters_();
}
std::size_t observer_delivery_initialization_sql::bootstrap_parameter_count_()const {
    if(!bootstrap_parameters_())startup_refuse("entry has no closed receiver parameter source");
    return entry_->bootstrap->receiver_parameter_count_();
}
int observer_delivery_initialization_sql::bootstrap_parameter_kind_(std::size_t index)const {
    if(!bootstrap_parameters_())startup_refuse("entry has no closed receiver parameter source");
    return entry_->bootstrap->receiver_parameter_kind_(index);
}
std::int64_t observer_delivery_initialization_sql::bootstrap_parameter_integer_(std::size_t index)const {
    if(!bootstrap_parameters_())startup_refuse("entry has no closed receiver parameter source");
    return entry_->bootstrap->receiver_parameter_integer_(index);
}
std::string_view observer_delivery_initialization_sql::bootstrap_parameter_blob_(std::size_t index)const {
    if(!bootstrap_parameters_())startup_refuse("entry has no closed receiver parameter source");
    return entry_->bootstrap->receiver_parameter_blob_(index);
}
bool observer_delivery_initialization_sql::preimage_parameters_()const {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)
        startup_refuse("inherited initialization parameter view");
    return bool(entry_->metadata)||(!entry_->bootstrap&&!entry_->receive);
}
std::size_t observer_delivery_initialization_sql::preimage_parameter_count_()const {
    if(!preimage_parameters_())startup_refuse("initialization entry is not a preimage parameter source");
    return entry_->metadata?entry_->metadata->parameter_count():entry_->text_parameters.size();
}
std::string_view observer_delivery_initialization_sql::preimage_parameter_text_(std::size_t index)const {
    if(!preimage_parameters_())startup_refuse("initialization entry is not a preimage parameter source");
    if(entry_->metadata)return entry_->metadata->parameter_text(index);
    if(index>=entry_->text_parameters.size())startup_refuse("owned initialization TEXT parameter index");
    return entry_->text_parameters[index];
}
observer_delivery_initialization_sql::recipe observer_delivery_initialization_sql::recipe_id_()const {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)
        startup_refuse("inherited initialization recipe");
    return entry_->id;
}
const void* observer_delivery_initialization_sql::entry_identity_()const noexcept {
    return creator_==database_native_process()?entry_:nullptr;
}
void observer_delivery_initialization_sql::constrain_result_(std::uint64_t& columns,
    std::uint64_t& rows,std::uint64_t& bytes)const {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)
        startup_refuse("inherited initialization result declaration");
    if(entry_->metadata) {
        if(!entry_->metadata->creator_current())startup_refuse("inherited immutable metadata result recipe");
        columns=std::min<std::uint64_t>(columns,ordinary_preimage_statement::column_limit());
        rows=std::min<std::uint64_t>(rows,entry_->metadata->row_limit());
        bytes=std::min<std::uint64_t>(bytes,entry_->metadata->result_byte_limit());
        return;
    }
    if(entry_->bootstrap) {
        if(!entry_->bootstrap->creator_current())startup_refuse("inherited bootstrap result recipe");
        columns=std::min<std::uint64_t>(columns,entry_->bootstrap->column_limit());
        rows=std::min<std::uint64_t>(rows,entry_->bootstrap->row_limit());
        bytes=std::min<std::uint64_t>(bytes,entry_->bootstrap->result_byte_limit());
        return;
    }
    if(entry_->model_plan&&entry_->id==recipe::model_metadata_read) {
        columns=std::min<std::uint64_t>(columns,1);rows=std::min<std::uint64_t>(rows,1);
        bytes=std::min<std::uint64_t>(bytes,recovery_owner_schema::max_bytes+256);return;
    }
    if(entry_->meter||entry_->model_plan||entry_->metadata_plan||entry_->sync_control_plan||entry_->static_sync_plan||entry_->audit_plan) {columns=0;rows=0;bytes=0;return;}
    if(entry_->receive) {
        if(!entry_->receive->creator_current())startup_refuse("inherited receive initializer result recipe");
        columns=std::min<std::uint64_t>(columns,entry_->receive->column_limit());
        rows=std::min<std::uint64_t>(rows,entry_->receive->row_limit());
        bytes=std::min<std::uint64_t>(bytes,entry_->receive->result_byte_limit());return;
    }
    switch(entry_->id) {
      case recipe::continuous_synchronous_full:
        columns=0;rows=0;bytes=0;break;
      case recipe::continuous_synchronous_getter:case recipe::continuous_fresh_schema_presence:case recipe::fresh_catalog_schema_presence:
        columns=std::min<std::uint64_t>(columns,1);rows=std::min<std::uint64_t>(rows,1);
        bytes=std::min<std::uint64_t>(bytes,256);break;
      case recipe::audit_inventory:
        columns=std::min<std::uint64_t>(columns,4);rows=std::min<std::uint64_t>(rows,4);
        bytes=std::min<std::uint64_t>(bytes,2*1024*1024);break;
      case recipe::static_sync_inventory:
        columns=std::min<std::uint64_t>(columns,4);rows=std::min<std::uint64_t>(rows,6);
        bytes=std::min<std::uint64_t>(bytes,2*1024*1024);break;
      case recipe::receive_prior_table_presence:
      case recipe::connection_recursive_triggers_getter:
      case recipe::busy_timeout_getter:case recipe::connection_schema_count:
      case recipe::schema_cookie:case recipe::metadata_table_discovery:
      case recipe::metadata_marker_read:case recipe::metadata_version_read:
      case recipe::continuous_schema_discovery:
        columns=std::min<std::uint64_t>(columns,1);rows=std::min<std::uint64_t>(rows,1);break;
      default:break;
    }
}
int observer_delivery_initialization_sql::authorize_(int action,const char* first,const char* second,
    const char* schema,const char* origin)const noexcept {
    if(creator_!=database_native_process()||!entry_||entry_->creator!=creator_)return SQLITE_DENY;
    const auto* open=entry_->original.get();
    const auto* actual=open&&open->creator_current_()&&open->state_?open->state_->actual:nullptr;
    if(!actual)return SQLITE_DENY;
    // Only the closed preissued fallback ROLLBACK may use cleanup identity
    // after work proof/health expires. The Request still conjunctively proves
    // its exact reserved live episode and entry; this recipe ID grants none.
    if(entry_->id==recipe::schema_transaction_rollback) {
        if(!open->cleanup_for_(*actual))return SQLITE_DENY;
    }else if(!work_current_for_(*actual))return SQLITE_DENY;
    // An existing trigger/view body is NOT implied by an approved outer
    // statement. Legitimate subprogram support requires the same actual
    // validated preimage, authored separately; name equality is insufficient.
    if(origin&&*origin)return SQLITE_DENY;
    const auto same=[](const char* actual,std::string_view expected)noexcept {
        if(!actual)return expected.empty();
        std::size_t i=0;
        for(;actual[i]&&i<expected.size();++i) {
            const auto fold=[](unsigned char c){return c>='A'&&c<='Z'?static_cast<unsigned char>(c+('a'-'A')):c;};
            if(fold(static_cast<unsigned char>(actual[i]))!=fold(static_cast<unsigned char>(expected[i])))return false;
        }
        return i==expected.size()&&!actual[i];
    };
    if(entry_->meter) {
        if(!entry_->meter_owner||!entry_->meter_plan)return SQLITE_DENY;
        return entry_->meter_plan->meter_authorize_(*entry_->meter,entry_->meter_operator,*open,
            *entry_->meter_owner,action,first,second,schema);
    }
    if(entry_->model_plan) {
        if(!entry_->model_owner||!model_work_current_for_(*entry_->model_owner))return SQLITE_DENY;
        return entry_->model_plan->model_authorize_(entry_->model_operator,*entry_->original,
            *entry_->model_owner,entry_->id,action,first,second,schema);
    }
    if(entry_->id==recipe::schema_marker||entry_->id==recipe::schema_marker_prune) {
        if(!entry_->metadata_owner||!metadata_work_current_for_(*entry_->metadata_owner))return SQLITE_DENY;
        const bool prune=entry_->id==recipe::schema_marker_prune;
        if(prune&&action==SQLITE_FUNCTION&&same(second,"like"))return SQLITE_OK;
        if(!same(schema,"main")||!same(first,"_lattice_meta"))return SQLITE_DENY;
        if(action==(prune?SQLITE_DELETE:SQLITE_INSERT))return SQLITE_OK;
        if(prune&&action==SQLITE_READ&&(same(second,"key")||same(second,"value")))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(entry_->id==recipe::metadata_table||entry_->id==recipe::metadata_initial_version) {
        // Only an actual immutable Plan operator plus this exact current
        // phase proof can authorize these two producer statements.
        if(!entry_->metadata_owner||!metadata_work_current_for_(*entry_->metadata_owner)||!same(schema,"main"))return SQLITE_DENY;
        const bool catalog=same(first,"sqlite_master")||same(first,"sqlite_schema");
        if(entry_->id==recipe::metadata_initial_version)
            return action==SQLITE_INSERT&&same(first,"_lattice_meta")?SQLITE_OK:SQLITE_DENY;
        if(action==SQLITE_CREATE_TABLE&&same(first,"_lattice_meta"))return SQLITE_OK;
        if(action==SQLITE_CREATE_INDEX&&same(first,"sqlite_autoindex__lattice_meta_1")&&same(second,"_lattice_meta"))return SQLITE_OK;
        if(action==SQLITE_INSERT&&catalog)return SQLITE_OK;
        if(action==SQLITE_UPDATE&&catalog) {
            for(const char* column:{"type","name","tbl_name","rootpage","sql"})if(same(second,column))return SQLITE_OK;
        }
        if(action==SQLITE_READ&&catalog&&same(second,"ROWID"))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(entry_->id==recipe::sync_control_table||entry_->id==recipe::sync_control_initial) {
        if(!entry_->sync_control_owner||!sync_control_work_current_for_(*entry_->sync_control_owner)||!same(schema,"main"))return SQLITE_DENY;
        const bool catalog=same(first,"sqlite_master")||same(first,"sqlite_schema");
        if(entry_->id==recipe::sync_control_initial)
            return (action==SQLITE_INSERT&&same(first,"_SyncControl"))||
                (action==SQLITE_READ&&same(first,"_SyncControl")&&same(second,"id"))?SQLITE_OK:SQLITE_DENY;
        if(action==SQLITE_CREATE_TABLE&&same(first,"_SyncControl"))return SQLITE_OK;
        if(action==SQLITE_INSERT&&catalog)return SQLITE_OK;
        if(action==SQLITE_UPDATE&&catalog) {
            for(const char* column:{"type","name","tbl_name","rootpage","sql"})if(same(second,column))return SQLITE_OK;
        }
        if(action==SQLITE_READ&&((catalog&&same(second,"ROWID"))||
           (same(first,"_SyncControl")&&same(second,"id"))))return SQLITE_OK;
        return SQLITE_DENY; // INTEGER PRIMARY KEY is actual ROWID: no index grant.
    }
    if(entry_->static_sync_plan) {
        if(!entry_->static_sync_owner||!static_sync_work_current_for_(*entry_->static_sync_owner))return SQLITE_DENY;
        const bool main=same(schema,"main");
        const bool alteration=entry_->id==recipe::replication_upload_floor;
        const bool index=entry_->id==recipe::sync_state_pending;
        const std::string_view table=entry_->id==recipe::sync_set?"_lattice_sync_set":
            (entry_->id==recipe::sync_state||index)?"_lattice_sync_state":"_lattice_replication_slots";
        if(alteration&&action==SQLITE_ALTER_TABLE)
            return same(first,"main")&&same(second,table)&&(!schema||!*schema||main)?SQLITE_OK:SQLITE_DENY;
        if((!schema||!*schema||main)&&action==SQLITE_FUNCTION&&(!first||!*first)) {
            if(alteration&&(same(second,"printf")||same(second,"substr")||same(second,"length")))return SQLITE_OK;
            if(entry_->id==recipe::replication_slots&&same(second,"datetime"))return SQLITE_OK;
        }
        if(alteration&&action==SQLITE_SELECT&&(!schema||!*schema||main))return SQLITE_OK;
        if(!main)return SQLITE_DENY;
        const bool catalog=same(first,"sqlite_master")||same(first,"sqlite_schema");
        if(action==SQLITE_READ&&catalog) {
            for(const char* column:{"type","name","tbl_name","rootpage","sql","ROWID"})if(same(second,column))return SQLITE_OK;
        }
        if(action==SQLITE_UPDATE&&catalog) {
            if(alteration)return same(second,"sql")?SQLITE_OK:SQLITE_DENY;
            for(const char* column:{"type","name","tbl_name","rootpage","sql"})if(same(second,column))return SQLITE_OK;
        }
        if(!alteration&&action==SQLITE_INSERT&&catalog)return SQLITE_OK;
        if(index) {
            if((action==SQLITE_CREATE_INDEX||action==SQLITE_REINDEX)&&same(first,"idx_sync_state_pending")&&
               (action==SQLITE_REINDEX||same(second,table)))return SQLITE_OK;
            if(action==SQLITE_READ&&same(first,table)&&(same(second,"sync_id")||same(second,"is_synchronized")||same(second,"ROWID")))return SQLITE_OK;
            return SQLITE_DENY;
        }
        if(alteration) {
            // Exact engine ADD COLUMN path only. No function/caller SQL grant
            // survives this retained immutable Plan/WRITE/revision entry.
            return SQLITE_DENY;
        }
        if(action==SQLITE_CREATE_TABLE&&same(first,table))return SQLITE_OK;
        const std::string_view automatic=entry_->id==recipe::sync_set?"sqlite_autoindex__lattice_sync_set_1":
            entry_->id==recipe::sync_state?"sqlite_autoindex__lattice_sync_state_1":"sqlite_autoindex__lattice_replication_slots_1";
        if(action==SQLITE_CREATE_INDEX&&same(first,automatic)&&same(second,table))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(entry_->audit_plan) {
        if(!entry_->audit_owner||!audit_work_current_for_(*entry_->audit_owner))return SQLITE_DENY;
        const bool create=entry_->id==recipe::audit_table;
        const bool main=same(schema,"main");
        if(create&&action==SQLITE_FUNCTION&&(!schema||!*schema||main)&&(!first||!*first)) {
            for(const char* function:{"lower","hex","randomblob","substr","abs","random","unixepoch"})
                if(same(second,function))return SQLITE_OK;
            return SQLITE_DENY;
        }
        if(!main)return SQLITE_DENY;
        const bool catalog=same(first,"sqlite_master")||same(first,"sqlite_schema");
        if(action==SQLITE_INSERT&&catalog)return SQLITE_OK;
        if(action==SQLITE_UPDATE&&catalog) {
            for(const char* column:{"type","name","tbl_name","rootpage","sql"})if(same(second,column))return SQLITE_OK;
        }
        if(action==SQLITE_READ&&catalog) {
            for(const char* column:{"type","name","tbl_name","rootpage","sql","ROWID"})if(same(second,column))return SQLITE_OK;
        }
        if(create) {
            // sqlite_sequence is only SQLite's automatic CREATE side effect
            // of this exact immutable AUTOINCREMENT declaration. No sequence
            // row read/write or caller statement is admitted here.
            if(action==SQLITE_CREATE_TABLE&&(same(first,"AuditLog")||same(first,"sqlite_sequence")))return SQLITE_OK;
            if((action==SQLITE_CREATE_INDEX||action==SQLITE_REINDEX)&&same(first,"sqlite_autoindex_AuditLog_1")&&
               (action==SQLITE_REINDEX||same(second,"AuditLog")))return SQLITE_OK;
            if(action==SQLITE_READ&&same(first,"AuditLog")&&(same(second,"globalId")||same(second,"ROWID")))return SQLITE_OK;
            return SQLITE_DENY;
        }
        const bool pending=entry_->id==recipe::audit_pending_index;
        const std::string_view index=pending?"idx_audit_log_pending_sync":"idx_audit_log_change_lookup";
        if((action==SQLITE_CREATE_INDEX||action==SQLITE_REINDEX)&&same(first,index)&&
           (action==SQLITE_REINDEX||same(second,"AuditLog")))return SQLITE_OK;
        if(action==SQLITE_READ&&same(first,"AuditLog")) {
            if(same(second,"ROWID"))return SQLITE_OK;
            if(pending)return same(second,"isSynchronized")?SQLITE_OK:SQLITE_DENY;
            return same(second,"tableName")||same(second,"rowId")||same(second,"operation")?SQLITE_OK:SQLITE_DENY;
        }
        return SQLITE_DENY;
    }
    if(entry_->id==recipe::receive_initializer)
        return entry_->receive?entry_->receive->authorize_(action,first,second,schema,origin):SQLITE_DENY;
    if(entry_->id==recipe::bootstrap_read) {
        // The actual closed bootstrap issuer owns its exact recipe/action
        // closure and SAME retained raw snapshot/preimage. SQL shape or a
        // validated family name cannot independently confer this authority.
        return entry_->bootstrap?entry_->bootstrap->authorize_(action,first,second,schema,origin):SQLITE_DENY;
    }
    if(entry_->id==recipe::preimage_metadata) {
        // This branch consumes the actual privately issued validator recipe,
        // never a caller purpose/string or a familiar observed table name.
        try {
            const auto& metadata=entry_->metadata;
            if(!metadata||!metadata->creator_current())return SQLITE_DENY;
            using kind=ordinary_preimage_statement::kind;
            const auto issued=metadata->kind_id();
            const bool temporary=issued==kind::temp_cookie||issued==kind::temp_inventory||
                                 issued==kind::temp_table_foreign_keys;
            const bool cookie=issued==kind::main_cookie||issued==kind::temp_cookie;
            const bool inventory=issued==kind::main_inventory||issued==kind::temp_inventory;
            // Separate the eponymous TVF's MAIN namespace from its fixed
            // target schema. TEMP is not a blanket metadata read allowance.
            const auto schema_is=[&](std::string_view expected)noexcept {
                return !schema||!*schema||same(schema,expected);
            };
            if((cookie||inventory||action==SQLITE_PRAGMA)&&
               !schema_is(temporary?"temp":"main"))return SQLITE_DENY;
            const auto read=[&](std::string_view table,std::initializer_list<std::string_view> columns)noexcept {
                if(action!=SQLITE_READ||!same(first,table))return false;
                for(const auto column:columns)if(same(second,column))return true;
                return false;
            };
            if(issued==kind::main_cookie||issued==kind::temp_cookie)
                return action==SQLITE_PRAGMA&&same(first,"schema_version")&&same(second,{})?SQLITE_OK:SQLITE_DENY;
            if(issued==kind::main_inventory||issued==kind::temp_inventory) {
                if(action==SQLITE_SELECT)return SQLITE_OK;
                return (read("sqlite_master",{"type","name","tbl_name","sql"})||
                        read("sqlite_schema",{"type","name","tbl_name","sql"}))?SQLITE_OK:SQLITE_DENY;
            }
            if(issued==kind::final_meter_row||issued==kind::final_metadata_rows||issued==kind::final_sync_control_row||
               issued==kind::final_encoding||issued==kind::final_recursive_triggers||
               issued==kind::final_foreign_keys||issued==kind::final_databases) {
                if(metadata->parameter_count()!=0||!schema_is("main"))return SQLITE_DENY;
                if(issued==kind::final_encoding)return action==SQLITE_PRAGMA&&same(first,"encoding")&&same(second,{})?SQLITE_OK:SQLITE_DENY;
                if(issued==kind::final_recursive_triggers)return action==SQLITE_PRAGMA&&same(first,"recursive_triggers")&&same(second,{})?SQLITE_OK:SQLITE_DENY;
                if(issued==kind::final_foreign_keys)return action==SQLITE_PRAGMA&&same(first,"foreign_keys")&&same(second,{})?SQLITE_OK:SQLITE_DENY;
                if(issued==kind::final_databases)return action==SQLITE_PRAGMA&&same(first,"database_list")&&same(second,{})?SQLITE_OK:SQLITE_DENY;
                if(action==SQLITE_SELECT)return SQLITE_OK;
                if(issued==kind::final_sync_control_row)return read("_SyncControl",{"id","disabled"})?SQLITE_OK:SQLITE_DENY;
                if(issued==kind::final_metadata_rows) {
                    if(action==SQLITE_FUNCTION&&same(second,"substr"))return SQLITE_OK;
                    return read("_lattice_meta",{"key","value"})?SQLITE_OK:SQLITE_DENY;
                }
                if(action==SQLITE_FUNCTION&&(same(second,"typeof")||same(second,"length")))return SQLITE_OK;
                return read("_lattice_observer_delivery_meter_v1",{"id","token","events","bytes"})?SQLITE_OK:SQLITE_DENY;
            }
            if(issued==kind::module_fts_config||issued==kind::module_vector_info) {
                if(metadata->parameter_count()!=0||!schema_is("main"))return SQLITE_DENY;
                const auto sql=metadata->sql_view();
                const std::string_view prefix=issued==kind::module_fts_config?"SELECT k,v FROM main.\"":"SELECT key,value FROM main.\"";
                if(!sql.starts_with(prefix))return SQLITE_DENY;
                const auto end=sql.find('\"',prefix.size());if(end==std::string_view::npos)return SQLITE_DENY;
                const auto target=sql.substr(prefix.size(),end-prefix.size());
                if(target.empty()||target.size()>256)return SQLITE_DENY;
                for(const auto c:target)if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_'))return SQLITE_DENY;
                if(issued==kind::module_fts_config) {
                    if(!target.ends_with("_config")||sql.substr(end)!="\" LIMIT 2"||metadata->row_limit()>1)return SQLITE_DENY;
                }else if(!target.ends_with("_info")||(sql.substr(end)!="\" LIMIT 5"&&sql.substr(end)!="\" LIMIT 6")||metadata->row_limit()>5)return SQLITE_DENY;
                if(action==SQLITE_SELECT)return SQLITE_OK;
                return issued==kind::module_fts_config?read(target,{"k","v"})?SQLITE_OK:SQLITE_DENY:
                    read(target,{"key","value"})?SQLITE_OK:SQLITE_DENY;
            }
            if(metadata->parameter_count()!=1)return SQLITE_DENY;
            // The complete validator alone issues this kind after actual
            // pre-connect schema closure. Vec xConnect reads ONLY its exact
            // selected MAIN _info key/value while the outer xinfo request
            // retains the SAME Open/episode/revision. No other shadow or
            // outer metadata purpose gains a read permission.
            if(issued==kind::module_vector_columns&&action==SQLITE_READ&&
               metadata->vector_connect_read_(first,second,schema))return SQLITE_OK;
            const auto target=metadata->parameter_text(0);
            std::string_view pragma,table;
            switch(issued) {
              case kind::table_columns:case kind::module_vector_columns:pragma="table_xinfo";table="pragma_table_xinfo";break;
              case kind::table_foreign_keys:case kind::temp_table_foreign_keys:
                pragma="foreign_key_list";table="pragma_foreign_key_list";break;
              case kind::index_list:pragma="index_list";table="pragma_index_list";break;
              case kind::index_columns:pragma="index_xinfo";table="pragma_index_xinfo";break;
              default:return SQLITE_DENY;
            }
            if(action==SQLITE_SELECT)return (!schema||!*schema)?SQLITE_OK:SQLITE_DENY;
            if(action==SQLITE_PRAGMA)return same(first,pragma)&&same(second,target)?SQLITE_OK:SQLITE_DENY;
            if(action==SQLITE_FUNCTION)return (!schema||!*schema)&&same(second,table)?SQLITE_OK:SQLITE_DENY;
            // SQLite's fixed pragma TVFs may compile their own schema read;
            // the exact issued recipe/parameters remain pinned throughout it.
            if((read("sqlite_master",{"type","name","tbl_name","sql"})||
                read("sqlite_schema",{"type","name","tbl_name","sql"}))&&
               schema_is(temporary?"temp":"main"))return SQLITE_OK;
            if(!schema_is("main"))return SQLITE_DENY;
            switch(issued) {
              case kind::table_columns:case kind::module_vector_columns:return read(table,{"cid","name","type","notnull","dflt_value","pk","hidden"})?SQLITE_OK:SQLITE_DENY;
              case kind::table_foreign_keys:case kind::temp_table_foreign_keys:
                return read(table,{"id","table"})?SQLITE_OK:SQLITE_DENY;
              case kind::index_list:return read(table,{"name","unique","origin","partial"})?SQLITE_OK:SQLITE_DENY;
              case kind::index_columns:return read(table,{"seqno","cid","name","desc","coll","key"})?SQLITE_OK:SQLITE_DENY;
              default:return SQLITE_DENY;
            }
        }catch(...) {return SQLITE_DENY;}
    }
    const bool main=!schema||!*schema||same(schema,"main");
    if(!main)return SQLITE_DENY;
    const auto pragma=[&](std::string_view name,std::string_view setting)noexcept {
        return action==SQLITE_PRAGMA&&same(first,name)&&same(second,setting)?SQLITE_OK:SQLITE_DENY;
    };
    switch(entry_->id) {
    case recipe::busy_timeout_getter:return pragma("busy_timeout",{});
    case recipe::schema_cookie:return pragma("schema_version",{});
    case recipe::metadata_marker_read:case recipe::metadata_version_read: {
        const auto* open=entry_->original.get();
        const auto* actual=open&&open->creator_current_()&&open->state_?open->state_->actual:nullptr;
        if(!actual||!entry_->schema_read_preimage||
           !entry_->schema_read_preimage->plain_table_for_(*actual,"_lattice_meta"))return SQLITE_DENY;
        if(action==SQLITE_SELECT)return SQLITE_OK;
        if(action==SQLITE_READ&&same(first,"_lattice_meta")&&
           (same(second,"key")||same(second,"value")))return SQLITE_OK;
        return SQLITE_DENY;
    }
    case recipe::receive_prior_table_presence:
        if(action==SQLITE_SELECT)return SQLITE_OK;
        if(action==SQLITE_FUNCTION&&same(second,"like"))return SQLITE_OK;
        if(action==SQLITE_READ&&(same(first,"sqlite_master")||same(first,"sqlite_schema"))&&
           (same(second,"name")||same(second,"type")))return SQLITE_OK;
        return SQLITE_DENY;
    case recipe::audit_inventory:
    case recipe::static_sync_inventory:
        if(action==SQLITE_SELECT)return SQLITE_OK;
        if(action==SQLITE_READ&&(same(first,"sqlite_master")||same(first,"sqlite_schema"))&&
           (same(second,"type")||same(second,"name")||same(second,"tbl_name")||same(second,"sql")))return SQLITE_OK;
        return SQLITE_DENY;
    case recipe::metadata_table_discovery:
        if(action==SQLITE_SELECT)return SQLITE_OK;
        if(action==SQLITE_READ&&(same(first,"sqlite_master")||same(first,"sqlite_schema"))&&
           (same(second,"name")||same(second,"type")))return SQLITE_OK;
        return SQLITE_DENY;
    case recipe::connection_foreign_keys:return pragma("foreign_keys","ON");
    case recipe::connection_recursive_triggers_on:return pragma("recursive_triggers","ON");
    case recipe::connection_recursive_triggers_getter:return pragma("recursive_triggers",{});
    case recipe::connection_journal:return pragma("journal_mode","WAL");
    case recipe::connection_cache_small:return pragma("cache_size","2000");
    case recipe::connection_cache_large:return pragma("cache_size","50000");
    case recipe::connection_mmap_off:return pragma("mmap_size","0");
    case recipe::connection_mmap_on:return pragma("mmap_size","300000000");
    case recipe::connection_temp_file:return pragma("temp_store","FILE");
    case recipe::connection_temp_memory:return pragma("temp_store","MEMORY");
    case recipe::connection_analysis:return pragma("analysis_limit","400");
    case recipe::connection_journal_size:return pragma("journal_size_limit","268435456");
    case recipe::schema_transaction_begin:
    case recipe::schema_transaction_deferred_begin:
    case recipe::schema_transaction_exclusive_begin:
        return action==SQLITE_TRANSACTION&&same(first,"BEGIN")?SQLITE_OK:SQLITE_DENY;
    case recipe::schema_transaction_commit:
        return action==SQLITE_TRANSACTION&&same(first,"COMMIT")?SQLITE_OK:SQLITE_DENY;
    case recipe::schema_transaction_rollback:
        return action==SQLITE_TRANSACTION&&same(first,"ROLLBACK")?SQLITE_OK:SQLITE_DENY;
    case recipe::function_inventory:return pragma("function_list",{});
    case recipe::connection_schema_count:
        if(action==SQLITE_SELECT)return SQLITE_OK;
        if(action==SQLITE_FUNCTION&&same(second,"count"))return SQLITE_OK;
        if(action==SQLITE_READ&&(same(first,"sqlite_master")||same(first,"sqlite_schema"))&&same(second,{}))return SQLITE_OK;
        return SQLITE_DENY;
    case recipe::continuous_synchronous_full:return pragma("synchronous","FULL");
    case recipe::continuous_synchronous_getter:return pragma("synchronous",{});
    case recipe::fresh_catalog_schema_presence:
        if(action==SQLITE_SELECT)return SQLITE_OK;
        if(action==SQLITE_READ&&(same(first,"sqlite_master")||same(first,"sqlite_schema"))&&same(second,"name"))return SQLITE_OK;
        return SQLITE_DENY;
    case recipe::continuous_fresh_schema_presence:
        if(action==SQLITE_SELECT)return SQLITE_OK;
        if(action==SQLITE_FUNCTION&&same(second,"like"))return SQLITE_OK;
        if(action==SQLITE_READ&&(same(first,"sqlite_master")||same(first,"sqlite_schema"))&&same(second,"name"))return SQLITE_OK;
        return SQLITE_DENY;
    case recipe::continuous_schema_discovery:
        if(action==SQLITE_SELECT)return SQLITE_OK;
        if(action==SQLITE_READ&&(same(first,"sqlite_master")||same(first,"sqlite_schema"))&&same(second,"name"))return SQLITE_OK;
        return SQLITE_DENY;
    default:
        // Catalog/preimage action closure is still an explicit unfinished
        // producer obligation. This source family is UNSELECTED until it is
        // implemented; no generic private-entry=>all-actions success exists.
        return SQLITE_DENY;
    }
}
observer_delivery_startup::observer_delivery_startup(std::uint64_t issuer,state* value)noexcept:creator_(issuer),state_(value){}
observer_delivery_startup::~observer_delivery_startup()noexcept {
    if(creator_==database_native_process()&&state_)delete state_;
}
void observer_delivery_startup::destroy(observer_delivery_startup* value)noexcept {
    if(value&&value->creator_==database_native_process())delete value;
}
observer_delivery_open_startup::observer_delivery_open_startup(std::uint64_t issuer,state* value)noexcept:creator_(issuer),state_(value){}
observer_delivery_open_startup::~observer_delivery_open_startup()noexcept {
    if(creator_==database_native_process()&&state_)delete state_;
}
void observer_delivery_open_startup::destroy(observer_delivery_open_startup* value)noexcept {
    if(value&&value->creator_==database_native_process())delete value;
}
std::shared_ptr<observer_delivery_startup> observer_delivery_startup::select(
    const configuration& config,const recovery_owner_schema& catalog) {
    return select_with_deadline_(config,catalog,std::chrono::steady_clock::now()+std::chrono::seconds(30));
}
std::shared_ptr<observer_delivery_startup> observer_delivery_startup::select_with_deadline_(
    const configuration& config,const recovery_owner_schema& catalog,std::chrono::steady_clock::time_point end) {
    // SAME actual factory-entry end for continuous resources and all later
    // startup/role/SQL work. No resource staging may renew construction time.
    if(end==std::chrono::steady_clock::time_point::max()||std::chrono::steady_clock::now()>=end)
        startup_refuse("delivery startup original factory deadline unavailable");
    // No service, thread, root creation or Native SQLite call on this path.
    // The actual normal issuer alone decides whether this is the legacy route.
    auto selected=store_resource_delivery_for_factory(config.path,end);
    if(!selected)return {};
    const auto creator=selected->creator_;
    if(creator!=database_native_process())startup_refuse("inherited delivery startup selection");
    selected->verify_for_factory_(config.path);
    if(!catalog.valid()||config.target_schema_version<=0||config.ordinary_context||config.migration_block)
        startup_refuse("delivery startup requires its immutable normal catalog and closed current schema producer");
    const auto& limits=selected->limits_;
    observer_delivery_meter_budget original{limits.events,limits.event_bytes,limits.workspace_bytes};
    const auto normal=observer_delivery_meter::admission_charge(original);
    const auto count=std::max<std::uint64_t>(normal.count,1);
    const auto bytes=std::max<std::uint64_t>(normal.bytes,limits.read_workspace_bytes);
    // The old actual-open tails can retain their original setup credit while
    // one separately admitted replacement is staged. Reserve BOTH, plus the
    // ordinary maximum, rather than relying on prompt destructor scheduling.
    if(limits.initialization_statements>(UINT64_MAX-count)/2||
       limits.initialization_workspace_bytes>(UINT64_MAX-bytes)/2||
       limits.guaranteed_count<2*limits.initialization_statements+count||
       limits.guaranteed_bytes<2*limits.initialization_workspace_bytes+bytes)
        startup_refuse("startup guarantee must retain old and replacement setup plus one maximum normal original");
    // The actual already-captured catalog stays in owned_resources. Do not
    // clone metadata or arbitrary callbacks here, before physical admission.
    native_retirement_storage<std::shared_ptr<observer_delivery_startup>> built;
    if(creator!=database_native_process())startup_refuse("inherited delivery startup result storage");
    auto* held=new state(*selected,catalog,config);
    if(creator!=database_native_process())startup_refuse("inherited delivery startup allocation");
    try {
        auto* raw=new observer_delivery_startup(creator,held);
        if(creator!=database_native_process())startup_refuse("inherited delivery startup owner allocation");
        held=nullptr; // raw owns it; shared_ptr failure invokes the exact deleter
        built.get<0>()=std::shared_ptr<observer_delivery_startup>(raw,destroy);
        if(creator!=database_native_process())startup_refuse("inherited delivery startup publication");
        return std::move(built.get<0>());
    }catch(...) {
        if(creator==database_native_process())delete held;
        throw;
    }
}
void observer_delivery_startup::destroy_replacement_(replacement_episode* value)noexcept {
    if(value&&value->creator==database_native_process())delete value;
}
observer_delivery_startup::inspection observer_delivery_startup::inspect_for_test_(
    const lattice_db& owner)noexcept {
    if(!owner.owns_creator_process()||owner.closed_.load(std::memory_order_acquire)||
       !owner.delivery_startup_)return {};
    const auto& startup=*owner.delivery_startup_;
    if(startup.creator_!=database_native_process()||!startup.state_||
       startup.state_->creator!=startup.creator_||startup.state_->actual_owner!=&owner||
       !startup.state_->finished.load(std::memory_order_acquire))return {};
    const auto actual=startup.state_->store.inspect();
    if(actual.code!=observer_delivery_store::status::admitted)return {};
    const auto& issued=startup.state_->policy.limits_;
    return {true,actual.used.count,actual.used.bytes,actual.leases,
        issued.initialization_statements,issued.initialization_workspace_bytes,
        issued.guaranteed_count+issued.burst_count,issued.guaranteed_bytes+issued.burst_bytes};
}
std::shared_ptr<observer_delivery_startup::replacement_episode>
observer_delivery_startup::begin_replacement_(lattice_db& owner,database& anchor,
    bool read_pair,std::chrono::steady_clock::time_point end) {
    owner.require_creator_process();anchor.require_creator_();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       !state_->finished.load(std::memory_order_acquire)||state_->failed.load(std::memory_order_acquire)||
       state_->actual_owner!=&owner||!state_->read_profile||!state_->plan||
       end==std::chrono::steady_clock::time_point::max()||std::chrono::steady_clock::now()>=end)
        startup_refuse("replacement requires its actually published original owner and finite episode");
    const auto creator=creator_;
    const auto& issued=state_->policy.limits_;
    native_retirement_storage<std::shared_ptr<database_initialization_budget>,
        std::shared_ptr<observer_delivery_startup>,std::shared_ptr<replacement_episode>> retained;
    if(creator!=database_native_process())startup_refuse("inherited replacement admission custody");
    // This existing, captured writer is only the actual physical/domain
    // validation anchor. It issues no new-open or SQL permission. Under any
    // genuine managed Native/API frame, budget admission is try-only.
    retained.get<0>()=database_initialization_budget::create(anchor,state_->store,state_->client,
        {issued.initialization_statements,issued.initialization_workspace_bytes,issued.read_columns,issued.read_rows},end);
    if(creator!=database_native_process())startup_refuse("inherited replacement admission return");
    // DATA-only failure observation at the genuine unbound-credit seam. A
    // throw here owns no new Native open; the admitted budget's actual last
    // owner must retire only on a genuine outermost managed return.
    const auto observed=observer_delivery_startup_test_probe::after_replacement_admitted;
    if(observed)observed(retained.get<0>().get());
    if(creator!=database_native_process())startup_refuse("inherited replacement admission observation");
    retained.get<0>()->claim_workspace_(sizeof(replacement_episode)+sizeof(observer_delivery_open_startup::state)*
        (read_pair?2:1)+sizeof(observer_delivery_open_startup)*(read_pair?2:1));
    retained.get<1>()=shared_from_this();
    if(creator!=database_native_process())startup_refuse("inherited replacement context capture");
    auto* value=new replacement_episode(creator,std::move(retained.get<1>()),owner,anchor,read_pair,end,
        std::move(retained.get<0>()));
    if(creator!=database_native_process())startup_refuse("inherited replacement episode allocation");
    retained.get<2>()=std::shared_ptr<replacement_episode>(value,destroy_replacement_);
    if(creator!=database_native_process())startup_refuse("inherited replacement episode publication");
    return std::move(retained.get<2>());
}
std::shared_ptr<observer_delivery_open_startup> observer_delivery_startup::make_open_(
    open_kind kind,const std::string& path,std::chrono::steady_clock::time_point end,
    const std::shared_ptr<replacement_episode>& replacement) {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||state_->failed.load(std::memory_order_acquire))
        startup_refuse("delivery startup original unavailable");
    const auto creator=state_->creator;
    const bool initial=kind==open_kind::writer||kind==open_kind::reader||kind==open_kind::cross_process_reader;
    if(initial&&(replacement||state_->finished.load(std::memory_order_acquire)||
       state_->builder!=std::this_thread::get_id()||end!=state_->policy.original_end_))
        startup_refuse("initial delivery open changed its original builder or deadline");
    unsigned initial_bit=0;
    if(initial) {
        initial_bit=kind==open_kind::writer?1u:kind==open_kind::reader?2u:4u;
        if(state_->pending_owner || (state_->initial_roles&initial_bit) ||
           (!(state_->initial_roles&1u) && initial_bit!=1u))
            startup_refuse("initial role requires its unique pre-owner writer-first token");
    }
    unsigned replacement_bit=0;
    if(!initial) {
        if(!state_->finished.load(std::memory_order_acquire)||!replacement||
           replacement->creator!=creator||replacement->builder!=std::this_thread::get_id()||
           replacement->startup.get()!=this||replacement->owner!=state_->actual_owner||
           replacement->failed.load(std::memory_order_acquire)||replacement->end!=end||
           !replacement->budget)
            startup_refuse("replacement delivery open lost its exact preadmitted episode");
        replacement->budget->require_current_();
        if(!replacement->read_pair&&kind==open_kind::replacement_writer)replacement_bit=1;
        else if(replacement->read_pair&&kind==open_kind::replacement_reader)replacement_bit=1;
        else if(replacement->read_pair&&kind==open_kind::replacement_cross_process_reader)replacement_bit=2;
        else startup_refuse("replacement open kind is outside its actual fixed episode");
        if(replacement->issued&replacement_bit)startup_refuse("replacement episode open token already issued");
    }
    auto selected=store_resource_delivery_for_factory(path,end);
    if(!selected||creator!=database_native_process())startup_refuse("delivery open lost actual installed issuer");
    selected->verify_for_factory_(path);
    const auto& prior=state_->policy;
    if(selected->creator_!=creator||selected->installation_!=prior.installation_||
       selected->root_!=prior.root_||selected->role_!=prior.role_||
       selected->member_size_!=prior.member_size_||selected->member_!=prior.member_||
       !(selected->limits_==prior.limits_))
        startup_refuse("delivery open changed its immutable installation/member/profile");
    native_retirement_storage<std::shared_ptr<observer_delivery_startup>,
        std::shared_ptr<observer_delivery_open_startup>> retained;
    if(creator!=database_native_process())startup_refuse("inherited delivery open storage");
    retained.get<0>()=shared_from_this();
    auto* held=new observer_delivery_open_startup::state(*selected,std::move(retained.get<0>()),kind,replacement);
    if(creator!=database_native_process())startup_refuse("inherited delivery open allocation");
    try {
        auto* raw=new observer_delivery_open_startup(creator,held);
        if(creator!=database_native_process())startup_refuse("inherited delivery open holder allocation");
        held=nullptr; // raw owns it; shared_ptr failure invokes the exact deleter
        retained.get<1>()=std::shared_ptr<observer_delivery_open_startup>(raw,observer_delivery_open_startup::destroy);
        if(creator!=database_native_process())startup_refuse("inherited delivery open publication");
        if(initial)state_->initial_roles|=initial_bit;
        if(replacement)replacement->issued|=replacement_bit;
        return std::move(retained.get<1>());
    }catch(...) {
        if(creator==database_native_process()) {
            delete held;
            if(replacement){replacement->failed.store(true,std::memory_order_release);replacement->budget->fail_();}
        }
        throw;
    }
}
std::shared_ptr<observer_delivery_open_startup> observer_delivery_startup::make_continuous_writer_(
    const recovery_continuous_admission* admission,const recovery_owner_schema& catalog,const std::string& path) {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited continuous startup custody");
    auto& common=*state_;
    try {
        // A real pre-open producer must use the exact owned_resources catalog,
        // original builder/deadline and untouched initial role family. No late
        // selection, second startup or adoption of an existing writer occurs.
        if(!admission || common.catalog!=&catalog || !catalog.valid() || common.read_only ||
           common.builder!=std::this_thread::get_id() || common.pending_owner || common.actual_owner ||
           common.continuous_admission || common.initial_roles || common.initial_budget ||
           common.plan || common.read_profile || common.finished.load(std::memory_order_acquire) ||
           common.failed.load(std::memory_order_acquire))
            startup_refuse("continuous writer changed its untouched pre-open startup family");
        common.continuous_admission=admission;
        return make_open_(open_kind::writer,path,common.policy.original_end_);
    }catch(...) {
        if(creator_==database_native_process()) {
            common.failed.store(true,std::memory_order_release);
            if(common.initial_budget)common.initial_budget->fail_();
        }
        throw;
    }
}
void observer_delivery_startup::bind_pending_owner_(lattice_db& owner) {
    // Real constructor seam. owned_resources already retains the exact const
    // catalog and ALL initial actual opens; no facility can escape yet.
    owner.require_creator_process();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited pending owner binding");
    auto& common=*state_;
    try {
        if(common.builder!=std::this_thread::get_id() || common.pending_owner || common.actual_owner ||
           common.finished.load(std::memory_order_acquire) || common.failed.load(std::memory_order_acquire) ||
           owner.delivery_startup_.get()!=this || common.catalog!=&owner.recovery_schemas_ ||
           !common.catalog || !common.catalog->valid() ||
           common.continuous_admission!=owner.recovery_continuous_.get() ||
           owner.closed_.load(std::memory_order_acquire) || common.read_only!=owner.config_.read_only ||
           common.schema_version!=owner.config_.target_schema_version ||
           !common.initial_budget || !common.plan || !common.read_profile)
            startup_refuse("pending owner differs from its actual constructor/catalog family");
        common.initial_budget->require_current_();
        if(owner.recovery_continuous_ &&
           !recovery_continuous_producer::pending_startup_matches_(owner,*this))
            startup_refuse("continuous owner lost its actual pre-open startup handoff");
        // Bounded new binding bookkeeping/working DATA consumes the SAME
        // already-admitted original workspace, never another setup account.
        static_assert(sizeof(observer_delivery_startup_test_probe::owner_binding_observation)<=192);
        common.initial_budget->claim_workspace_(sizeof(common.pending_owner)+sizeof(common.initial_roles)+
            sizeof(observer_delivery_startup_test_probe::owner_binding_observation)+
            sizeof(std::array<database*,3>)+sizeof(std::array<open_kind,3>));
        const auto& profile=*common.read_profile;
        const auto& limits=common.policy.limits_;
        if(profile.creator_!=creator_ || profile.publisher_!=&common ||
           profile.published_.load(std::memory_order_acquire) || !profile.store_.same_domain(common.store) ||
           profile.policy_.workspace_bytes!=limits.read_workspace_bytes ||
           profile.policy_.columns!=limits.read_columns || profile.policy_.rows!=limits.read_rows ||
           profile.policy_.maximum_duration!=std::chrono::milliseconds(limits.read_duration_ms) ||
           profile.maximum_admission_wait_!=std::chrono::milliseconds(limits.admission_wait_ms))
            startup_refuse("pending owner lost its same unpublished common read profile");
        // Fixed role census, not another open/lease/profile or a catalog copy.
        std::array<database*,3> actual{owner.db_.get(),owner.read_db_.get(),owner.xproc_read_db_.get()};
        constexpr std::array<open_kind,3> kinds{open_kind::writer,open_kind::reader,open_kind::cross_process_reader};
        observer_delivery_startup_test_probe::owner_binding_observation observed{};
        observed.catalog=common.catalog;observed.budget=common.initial_budget.get();
        observed.profile=common.read_profile.get();
        for(size_t index=0;index<actual.size();++index) {
            auto* current=actual[index];if(!current)continue;
            for(size_t prior=0;prior<index;++prior)if(actual[prior]==current)
                startup_refuse("pending owner has aliased actual initial role originals");
            const auto bit=1u<<index;observed.roles|=bit;
            current->require_creator_();
            auto* open=current->delivery_startup_.get();
            if(!open || open->creator_!=creator_ || !open->state_ ||
               open->state_->creator!=creator_ || open->state_->startup.get()!=this ||
               open->state_->builder!=common.builder || open->state_->kind!=kinds[index] ||
               open->state_->replacement || open->state_->actual!=current ||
               open->state_->policy.original_end_!=common.policy.original_end_ ||
               open->state_->budget.get()!=common.initial_budget.get() ||
               open->state_->budget.owner_before(common.initial_budget) ||
               common.initial_budget.owner_before(open->state_->budget) ||
               current->mode_!=(index==0 && !common.read_only?database::open_mode::read_write:database::open_mode::read_only))
                startup_refuse("pending owner role lacks its same initial open/budget original");
            open->require_work_current_(*current);
            // This producer may validate the retained physical store, never
            // bind/register another one. Finish checks remain separate.
            if(!common.store.current_for(*current))
                startup_refuse("pending role no longer matches its admitted physical store");
            {
                database::catalog_native_admission_ leaf(*current);
                open->require_work_current_(*current);
                const auto binding=std::atomic_load(&current->sql_read_binding_);
                if(!binding || binding->creator_!=creator_ || binding->owner_!=current ||
                   binding->profile_.get()!=common.read_profile.get() ||
                   binding->profile_.owner_before(common.read_profile) ||
                   common.read_profile.owner_before(binding->profile_) ||
                   !current->observer_delivery_sql_ ||
                   !current->observer_delivery_sql_->pending_original_for_(*current,*open) ||
                   current->raw_handle_escaped_.load(std::memory_order_acquire) ||
                   current->engine_reads_ || sqlite3_get_autocommit(current->db_)==0)
                    startup_refuse("pending role lost its installed runtime/read binding custody");
                size_t statements=0;
                for(auto* statement=sqlite3_next_stmt(current->db_,nullptr);statement;
                    statement=sqlite3_next_stmt(current->db_,statement)) {
                    if(++statements>4096 || sqlite3_stmt_busy(statement))
                        startup_refuse("pending role is not an idle bounded actual Native corpus");
                }
                observed.actual[index]=current;observed.open[index]=open;
                observed.role_budget[index]=open->state_->budget.get();
                observed.role_profile[index]=binding->profile_.get();
                observed.runtime[index]=current->observer_delivery_sql_.get();
            }
            open->require_work_current_(*current); // after actual Native return
        }
        if(!(observed.roles&1u) || observed.roles!=common.initial_roles ||
           common.initial_roles>7u || !owner.db_)
            startup_refuse("pending owner does not contain its entire issued initial role family");
        common.initial_budget->require_current_();
        // Identity binding only. In particular published profile, actual_owner,
        // finished flags, SQL grants and private four finishing bodies remain
        // untouched. Duplicate/late initial issuance now refuses.
        common.pending_owner=&owner;
        if(const auto hook=observer_delivery_startup_test_probe::after_owner_bound)hook(owner,observed);
        if(creator_!=database_native_process())startup_refuse("inherited pending owner observation return");
    }catch(...) {
        if(creator_==database_native_process()) {
            common.failed.store(true,std::memory_order_release);
            if(common.initial_budget)common.initial_budget->fail_();
        }
        throw; // preserve real first validation/Host/observer error
    }
}
std::shared_ptr<const observer_delivery_connection>
observer_delivery_startup::stage_connection_(lattice_db& owner,database& writer,
    observer_delivery_open_startup& open) {
    owner.require_creator_process();writer.require_creator_();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited startup connection staging");
    try {
        open.require_work_current_(writer);auto& common=*state_;
        if(common.read_only||owner.config_.read_only||owner.delivery_startup_.get()!=this||
           owner.closed_.load(std::memory_order_acquire)||common.failed.load(std::memory_order_acquire)||
           !common.read_profile||!common.plan||!owner.recovery_schemas_owner_||
           owner.recovery_schemas_owner_.get()!=&owner.recovery_schemas_||!owner.recovery_schemas_.valid()||
           !open.state_||open.state_->startup.get()!=this||open.state_->actual!=&writer)
            startup_refuse("startup connection staging lost its same actual owner/catalog");
        const bool replacement=open.state_->kind==open_kind::replacement_writer;
        if(replacement) {
            if(!common.finished.load(std::memory_order_acquire)||common.actual_owner!=&owner||
               !open.state_->replacement||open.state_->replacement->owner!=&owner||
               open.state_->replacement->read_pair)
                startup_refuse("successor connection staging lacks its actual writer episode");
        }else if(open.state_->kind!=open_kind::writer||common.pending_owner!=&owner||common.actual_owner||
            common.catalog!=&owner.recovery_schemas_||common.finished.load(std::memory_order_acquire))
                startup_refuse("initial connection staging lacks its unpublished original owner");
        if(!common.store.current_for(writer))startup_refuse("staged connection physical store differs");
        {
            database::catalog_native_admission_ leaf(writer);open.require_work_current_(writer);
            const auto binding=std::atomic_load(&writer.sql_read_binding_);
            if(!binding||binding->creator_!=creator_||binding->owner_!=&writer||
               binding->profile_.get()!=common.read_profile.get()||
               binding->profile_.owner_before(common.read_profile)||common.read_profile.owner_before(binding->profile_)||
               !writer.observer_delivery_sql_||!writer.observer_delivery_sql_->pending_original_for_(writer,open))
                startup_refuse("staged connection lost its installed pending runtime/profile");
        }
        open.require_work_current_(writer);
        const auto& limits=common.policy.limits_;
        observer_delivery_connection::policy config{
            {size_t(limits.clients),size_t(limits.waiters),
                {limits.guaranteed_count,limits.guaranteed_bytes},{limits.burst_count,limits.burst_bytes}},
            {limits.events,limits.event_bytes,limits.workspace_bytes},std::chrono::milliseconds(limits.admission_wait_ms),
            {limits.read_workspace_bytes,limits.read_columns,limits.read_rows,std::chrono::milliseconds(limits.read_duration_ms)}};
        using custody=native_retirement_storage<std::shared_ptr<observer_delivery_connection>>;
        open.claim_workspace_(writer,sizeof(observer_delivery_connection)+custody::allocation_bytes());custody held;
        // Actual rebound shared control-block request is charged before its
        // allocation. The immutable schema is SHARED: no deep catalog copy.
        ordinary_initialization_storage_scope storage(&open,&writer,
            &observer_delivery_open_startup::debit_initialization_storage_);
        auto* value=new observer_delivery_connection(creator_,owner,writer,owner.recovery_schemas_owner_,
            config,common.store,common.client);
        if(creator_!=database_native_process())startup_refuse("inherited startup connection allocation");
        held.get<0>()=std::shared_ptr<observer_delivery_connection>(value,
            observer_delivery_connection::destroy,ordinary_initialization_allocator<observer_delivery_connection>{});
        if(creator_!=database_native_process())startup_refuse("inherited startup connection custody return");
        held.get<0>()->runtime_=writer.observer_delivery_sql_;
        held.get<0>()->read_profile_=common.read_profile;
        open.require_work_current_(writer);
        return std::move(held.get<0>());
    }catch(...) {open.fail_initialization_();throw;}
}

void observer_delivery_startup::require_completed_role_(const lattice_db& owner,
    const database& actual,open_kind kind,const replacement_episode* episode)const {
    // Pure prepublication DATA checks. No Native leaf, runtime mutable state,
    // domain admission, new clock or binding.current() is used here. The same
    // budget's original clock remains authoritative until actual publication.
    owner.require_creator_process();actual.require_creator_();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited completed role publication");
    const auto& common=*state_;
    const auto* open=actual.delivery_startup_.get();
    if(!open||open->creator_!=creator_||!open->state_||open->state_->creator!=creator_||
       open->state_->startup.get()!=this||open->state_->actual!=&actual||
       open->state_->builder!=std::this_thread::get_id()||open->state_->kind!=kind||
       !open->state_->attempted.load(std::memory_order_acquire)||
       !open->state_->opened.load(std::memory_order_acquire)||
       !open->state_->finished.load(std::memory_order_acquire)||
       open->state_->failed.load(std::memory_order_acquire)||!open->state_->budget||
       actual.closed_.load(std::memory_order_acquire)||
       actual.raw_handle_escaped_.load(std::memory_order_acquire)||!actual.observer_delivery_sql_)
        startup_refuse("publication requires its actually completed role original");
    const auto& selected=*open->state_;
    const bool writer=kind==open_kind::writer||kind==open_kind::replacement_writer;
    if(actual.mode_!=(writer&&!common.read_only?database::open_mode::read_write:database::open_mode::read_only))
        startup_refuse("completed role mode differs from actual owner field");
    const auto& budget=episode?episode->budget:common.initial_budget;
    if(!budget||selected.budget.get()!=budget.get()||selected.budget.owner_before(budget)||
       budget.owner_before(selected.budget)||selected.replacement.get()!=episode||
       selected.policy.original_end_!=budget->deadline_())
        startup_refuse("completed role changed its original budget or deadline");
    budget->require_current_(); // atomic/builder/original-clock DATA only
    if(episode&&(episode->creator!=creator_||episode->startup.get()!=this||
       episode->owner!=&owner||episode->builder!=std::this_thread::get_id()||
       episode->end!=selected.policy.original_end_||episode->failed.load(std::memory_order_acquire)))
        startup_refuse("completed successor changed its actual admitted episode");
    const auto binding=std::atomic_load(&actual.sql_read_binding_);
    if(!binding||binding->creator_!=creator_||binding->owner_!=&actual||
       binding->profile_.get()!=common.read_profile.get()||
       binding->profile_.owner_before(common.read_profile)||common.read_profile.owner_before(binding->profile_))
        startup_refuse("completed role lost its same retained read binding");
}

void observer_delivery_startup::finish_owner_(lattice_db& owner) {
    owner.require_creator_process();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited owner finishing");
    auto& common=*state_;
    try {
        if(common.builder!=std::this_thread::get_id()||common.pending_owner!=&owner||common.actual_owner||
           common.catalog!=&owner.recovery_schemas_||common.finished.load(std::memory_order_acquire)||
           common.failed.load(std::memory_order_acquire)||owner.delivery_startup_.get()!=this||
           owner.closed_.load(std::memory_order_acquire)||owner.delivery_facilities_published_||
           !common.initial_budget||!common.plan||!common.read_profile||
           !owner.recovery_schemas_owner_||owner.recovery_schemas_owner_.get()!=common.catalog||
           !common.catalog->valid()||common.read_only!=owner.config_.read_only||
           common.schema_version!=owner.config_.target_schema_version)
            startup_refuse("owner finishing lost its actual unpublished constructor family");
        common.initial_budget->require_current_();
        // Actual Core/Swift containers, checked by their real closed builder.
        // Schema presence alone cannot stand in for cascade/hydration state.
        owner.require_pending_model_registries_(*common.catalog);
        using custody=native_retirement_storage<std::shared_ptr<database>,std::shared_ptr<database>,
            std::shared_ptr<database>,std::shared_ptr<const observer_delivery_connection>>;
        common.initial_budget->claim_workspace_(custody::allocation_bytes());
        custody held;
        if(creator_!=database_native_process())startup_refuse("inherited owner finishing custody");
        {
            std::unique_lock<std::mutex> lock(owner.connection_ownership_mutex_,std::try_to_lock);
            if(!lock.owns_lock()||owner.closed_.load(std::memory_order_acquire)||
               !owner.db_||std::atomic_load(&owner.observer_delivery_connection_))
                startup_refuse("initial owner already has a connection publication");
            held.get<0>()=owner.db_;held.get<1>()=owner.read_db_;held.get<2>()=owner.xproc_read_db_;
        }
        std::array<database*,3> actual{held.get<0>().get(),held.get<1>().get(),held.get<2>().get()};
        constexpr std::array<open_kind,3> kinds{open_kind::writer,open_kind::reader,open_kind::cross_process_reader};
        unsigned roles=0;
        for(std::size_t index=0;index<actual.size();++index) {
            auto* current=actual[index];if(!current)continue;
            for(std::size_t prior=0;prior<index;++prior)if(actual[prior]==current)
                startup_refuse("initial owner finishing aliases actual role originals");
            roles|=1u<<index;
            auto* open=current->delivery_startup_.get();
            if(!open||!open->state_||open->state_->kind!=kinds[index]||open->state_->replacement||
               open->state_->startup.get()!=this||open->state_->actual!=current||
               open->state_->budget.get()!=common.initial_budget.get())
                startup_refuse("initial owner finishing changed its retained role family");
            open->require_work_current_(*current);
        }
        if(!(roles&1u)||roles!=common.initial_roles||roles>7u||
           (common.read_only&&(roles!=1u||held.get<1>()||held.get<2>())))
            startup_refuse("initial owner finishing lacks its entire issued role family");
        const auto& profile=*common.read_profile;
        if(profile.creator_!=creator_||profile.publisher_!=&common||
           profile.published_.load(std::memory_order_acquire)||!profile.store_.same_domain(common.store))
            startup_refuse("initial owner changed its unpublished common read profile");
        // Stage DATA before sealing the writer; staging requires that SAME
        // installed pending runtime and cannot finish or publish anything.
        if(!common.read_only)held.get<3>()=stage_connection_(owner,*actual[0],*actual[0]->delivery_startup_);
        for(std::size_t index=0;index<actual.size();++index)if(actual[index]) {
            actual[index]->delivery_startup_->finish_open_(*actual[index]);
            if(creator_!=database_native_process())startup_refuse("inherited actual role finishing return");
            require_completed_role_(owner,*actual[index],kinds[index],nullptr);
        }
        common.initial_budget->require_current_();
        {
            std::unique_lock<std::mutex> lock(owner.connection_ownership_mutex_,std::try_to_lock);
            if(!lock.owns_lock()||owner.closed_.load(std::memory_order_acquire)||
               owner.db_.get()!=actual[0]||owner.read_db_.get()!=actual[1]||owner.xproc_read_db_.get()!=actual[2]||
               common.failed.load(std::memory_order_acquire)||common.pending_owner!=&owner||
               common.catalog!=&owner.recovery_schemas_||std::atomic_load(&owner.observer_delivery_connection_))
                startup_refuse("initial owner publication changed after actual role proofs");
            for(std::size_t index=0;index<actual.size();++index)if(actual[index])
                require_completed_role_(owner,*actual[index],kinds[index],nullptr);
            // From here only nonthrowing DATA publication remains. No Native,
            // user callback, extra admission, allocation or last-owner drop.
            if(held.get<3>())std::atomic_store(&owner.observer_delivery_connection_,std::move(held.get<3>()));
            common.actual_owner=&owner;common.pending_owner=nullptr;common.catalog=nullptr;
            common.read_profile->publish();
            common.finished.store(true,std::memory_order_release);
        }
    }catch(...) {
        if(creator_==database_native_process()) {
            common.failed.store(true,std::memory_order_release);
            if(common.initial_budget)common.initial_budget->fail_();
        }
        throw;
    }
}

void observer_delivery_startup::finish_replacement_(lattice_db& owner,database& actual) {
    owner.require_creator_process();actual.require_creator_();
    auto* open=actual.delivery_startup_.get();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       !open||open->creator_!=creator_||!open->state_||open->state_->startup.get()!=this)
        startup_refuse("replacement finishing lacks its actual startup/open");
    try {
        auto& common=*state_;auto& selected=*open->state_;
        const auto& episode=selected.replacement;
        if(!common.finished.load(std::memory_order_acquire)||common.failed.load(std::memory_order_acquire)||
           common.actual_owner!=&owner||common.pending_owner||common.catalog||common.read_only||
           owner.delivery_startup_.get()!=this||owner.closed_.load(std::memory_order_acquire)||
           !common.read_profile||!common.read_profile->published_.load(std::memory_order_acquire)||
           !episode||episode->creator!=creator_||episode->startup.get()!=this||episode->owner!=&owner||
           episode->builder!=std::this_thread::get_id()||!episode->anchor||
           episode->failed.load(std::memory_order_acquire)||episode->published.load(std::memory_order_acquire)||
           episode->prepared_connection||episode->displaced_connection||
           selected.actual!=&actual||selected.budget.get()!=episode->budget.get()||
           selected.policy.original_end_!=episode->end)
            startup_refuse("replacement finishing changed its original unpublished episode");
        open->require_work_current_(actual);
        const bool writer=selected.kind==open_kind::replacement_writer;
        if(writer) {
            if(episode->read_pair||episode->issued!=1u)
                startup_refuse("replacement writer lacks its exact one-slot episode");
            episode->prepared_connection=stage_connection_(owner,actual,*open);
        }else if(!episode->read_pair||episode->issued!=3u||
            (selected.kind!=open_kind::replacement_reader&&selected.kind!=open_kind::replacement_cross_process_reader))
                startup_refuse("replacement reader lacks its actual complete two-slot episode");
        open->finish_open_(actual);
        if(creator_!=database_native_process())startup_refuse("inherited replacement proof return");
        require_completed_role_(owner,actual,selected.kind,episode.get());
    }catch(...) {open->fail_initialization_();throw;}
}

void observer_delivery_startup::require_replacement_writer_publication_(lattice_db& owner,
    database& next,database& previous)const {
    // Caller holds its existing owner publication mutex and previous-writer
    // publication hold. Every failure precedes ANY owner field swap.
    owner.require_creator_process();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited successor writer publication");
    auto& common=*state_;
    const auto* open=next.delivery_startup_.get();
    if(!open||!open->state_||!open->state_->replacement)
        startup_refuse("successor writer publication lost its actual episode");
    const auto& episode=*open->state_->replacement;
    require_completed_role_(owner,next,open_kind::replacement_writer,&episode);
    const auto old=std::atomic_load(&owner.observer_delivery_connection_);
    const auto& staged=episode.prepared_connection;
    if(common.actual_owner!=&owner||!common.finished.load(std::memory_order_acquire)||
       common.failed.load(std::memory_order_acquire)||common.pending_owner||common.catalog||
       owner.delivery_startup_.get()!=this||owner.closed_.load(std::memory_order_acquire)||
       owner.db_.get()!=&previous||&next==&previous||episode.anchor!=&previous||
       episode.read_pair||episode.issued!=1u||episode.published.load(std::memory_order_acquire)||
       episode.displaced_connection||!staged||!old||old->creator_!=creator_||old->owner_!=&owner||
       old->writer_!=&previous||old->runtime_.get()!=previous.observer_delivery_sql_.get()||
       !common.read_profile->published_.load(std::memory_order_acquire)||
       staged->creator_!=creator_||staged->owner_!=&owner||staged->writer_!=&next||
       staged->runtime_.get()!=next.observer_delivery_sql_.get()||
       staged->catalog_.get()!=owner.recovery_schemas_owner_.get()||
       staged->catalog_.owner_before(owner.recovery_schemas_owner_)||owner.recovery_schemas_owner_.owner_before(staged->catalog_)||
       staged->read_profile_.get()!=common.read_profile.get()||
       staged->read_profile_.owner_before(common.read_profile)||common.read_profile.owner_before(staged->read_profile_)||
       !staged->store_.same_domain(common.store))
        startup_refuse("successor writer and observer publication differ before swap");
}

void observer_delivery_startup::publish_replacement_writer_(lattice_db& owner,database& actual)noexcept {
    // Closed caller immediately follows the checks above under the SAME
    // locks. Empty displaced custody is preallocated in the real episode.
    if(creator_!=database_native_process()||!owner.owns_creator_process())return;
    auto& episode=*actual.delivery_startup_->state_->replacement;
    episode.displaced_connection=std::atomic_exchange(&owner.observer_delivery_connection_,
        std::move(episode.prepared_connection));
    episode.published.store(true,std::memory_order_release);
}

void observer_delivery_startup::require_replacement_reader_publication_(lattice_db& owner,
    database& reader,database& xproc)const {
    owner.require_creator_process();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited successor reader publication");
    const auto* first=reader.delivery_startup_.get();const auto* second=xproc.delivery_startup_.get();
    if(!first||!first->state_||!second||!second->state_||!first->state_->replacement||
       !second->state_->replacement||first->state_->replacement.get()!=second->state_->replacement.get()||
       first->state_->replacement.owner_before(second->state_->replacement)||
       second->state_->replacement.owner_before(first->state_->replacement))
        startup_refuse("successor readers changed their exact shared episode");
    const auto& episode=*first->state_->replacement;const auto& common=*state_;
    require_completed_role_(owner,reader,open_kind::replacement_reader,&episode);
    require_completed_role_(owner,xproc,open_kind::replacement_cross_process_reader,&episode);
    if(common.actual_owner!=&owner||!common.finished.load(std::memory_order_acquire)||
       common.failed.load(std::memory_order_acquire)||common.pending_owner||common.catalog||
       owner.delivery_startup_.get()!=this||owner.closed_.load(std::memory_order_acquire)||
       !owner.db_||owner.db_.get()!=episode.anchor||&reader==&xproc||&reader==episode.anchor||&xproc==episode.anchor||
       !episode.read_pair||episode.issued!=3u||episode.published.load(std::memory_order_acquire)||
       episode.prepared_connection||episode.displaced_connection||
       !common.read_profile->published_.load(std::memory_order_acquire))
        startup_refuse("successor reader pair differs before joint publication");
}

void observer_delivery_startup::publish_replacement_reader_pair_(database& reader)noexcept {
    if(creator_!=database_native_process())return;
    reader.delivery_startup_->state_->replacement->published.store(true,std::memory_order_release);
}

void observer_delivery_startup::retire_replacement_publication_(
    const std::shared_ptr<replacement_episode>& episode)noexcept {
    // Actual caller is OUTSIDE attachment, Native-publication and owner locks,
    // while its original previous_writer still pins the displaced Native.
    if(creator_!=database_native_process()||!episode||episode->creator!=creator_||
       episode->startup.get()!=this)return;
    episode->displaced_connection.reset();
}

void observer_delivery_startup::require_finished_(const lattice_db& owner)const {
    // Observation only: it cannot complete/reopen an owner, issue a lease or
    // reuse constructor time as an expiry rule for a legitimately live owner.
    owner.require_creator_process();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited finished owner observation");
    const auto& common=*state_;
    if(!common.finished.load(std::memory_order_acquire)||common.failed.load(std::memory_order_acquire)||
       common.actual_owner!=&owner||common.catalog||owner.delivery_startup_.get()!=this||
       owner.closed_.load(std::memory_order_acquire)||!common.plan||!common.read_profile||
       !owner.recovery_schemas_owner_||owner.recovery_schemas_owner_.get()!=&owner.recovery_schemas_||
       !owner.recovery_schemas_.valid()||common.read_only!=owner.config_.read_only||
       common.schema_version!=owner.config_.target_schema_version)
        startup_refuse("owner lacks its actual completed publication");
    const auto& profile=*common.read_profile;const auto& limits=common.policy.limits_;
    if(profile.creator_!=creator_||profile.publisher_!=&common||
       !profile.published_.load(std::memory_order_acquire)||!profile.store_.same_domain(common.store)||
       profile.policy_.workspace_bytes!=limits.read_workspace_bytes||
       profile.policy_.columns!=limits.read_columns||profile.policy_.rows!=limits.read_rows||
       profile.policy_.maximum_duration!=std::chrono::milliseconds(limits.read_duration_ms)||
       profile.maximum_admission_wait_!=std::chrono::milliseconds(limits.admission_wait_ms))
        startup_refuse("finished owner lost its same published common read profile");
    // Never wait while a caller might hold a Native leaf. No Native call,
    // domain/current-for-store query or user callback occurs under this lock.
    std::unique_lock<std::mutex> lock(owner.connection_ownership_mutex_,std::try_to_lock);
    if(!lock.owns_lock()||owner.closed_.load(std::memory_order_acquire)||!owner.db_)
        startup_refuse("finished owner publication unavailable");
    std::array<database*,3> actual{owner.db_.get(),owner.read_db_.get(),owner.xproc_read_db_.get()};
    constexpr std::array<open_kind,3> initial{open_kind::writer,open_kind::reader,open_kind::cross_process_reader};
    constexpr std::array<open_kind,3> successor{open_kind::replacement_writer,open_kind::replacement_reader,open_kind::replacement_cross_process_reader};
    for(std::size_t index=0;index<actual.size();++index) {
        auto* current=actual[index];if(!current)continue;
        for(std::size_t prior=0;prior<index;++prior)if(actual[prior]==current)
            startup_refuse("finished owner aliases its actual role originals");
        current->require_creator_();const auto* open=current->delivery_startup_.get();
        if(!open||open->creator_!=creator_||!open->state_||open->state_->creator!=creator_||
           open->state_->startup.get()!=this||open->state_->actual!=current||
           !open->state_->attempted.load(std::memory_order_acquire)||
           !open->state_->opened.load(std::memory_order_acquire)||
           !open->state_->finished.load(std::memory_order_acquire)||
           open->state_->failed.load(std::memory_order_acquire)||!open->state_->budget||
           current->closed_.load(std::memory_order_acquire)||current->raw_handle_escaped_.load(std::memory_order_acquire)||
           current->mode_!=(index==0&&!common.read_only?database::open_mode::read_write:database::open_mode::read_only))
            startup_refuse("finished owner lost its actually completed role original");
        if(open->state_->kind==initial[index]) {
            if(open->state_->replacement||open->state_->budget.get()!=common.initial_budget.get()||
               open->state_->budget.owner_before(common.initial_budget)||common.initial_budget.owner_before(open->state_->budget))
                startup_refuse("finished initial role changed its retained original budget");
        }else if(open->state_->kind==successor[index]) {
            const auto& episode=open->state_->replacement;
            if(!episode||episode->creator!=creator_||episode->startup.get()!=this||episode->owner!=&owner||
               episode->read_pair!=(index!=0)||episode->failed.load(std::memory_order_acquire)||
               !episode->published.load(std::memory_order_acquire)||
               episode->budget.get()!=open->state_->budget.get()||
               episode->budget.owner_before(open->state_->budget)||open->state_->budget.owner_before(episode->budget))
                startup_refuse("finished successor role changed its actual admitted episode");
        }else startup_refuse("finished role kind differs from actual owner field");
        const auto binding=std::atomic_load(&current->sql_read_binding_);
        if(!binding||binding->creator_!=creator_||binding->owner_!=current||
           binding->profile_.get()!=common.read_profile.get()||
           binding->profile_.owner_before(common.read_profile)||common.read_profile.owner_before(binding->profile_))
            startup_refuse("finished role lost its same actual published read binding");
    }
    if((actual[1]&&actual[1]->delivery_startup_->state_->replacement)||
       (actual[2]&&actual[2]->delivery_startup_->state_->replacement)) {
        if(!actual[1]||!actual[2])startup_refuse("finished successor reader pair is incomplete");
        const auto& first=actual[1]->delivery_startup_->state_->replacement;
        const auto& second=actual[2]->delivery_startup_->state_->replacement;
        if(!first||!second||first.get()!=second.get()||first.owner_before(second)||second.owner_before(first)||
           !first->read_pair||first->issued!=3u)
            startup_refuse("finished reader pair does not share its complete actual two-slot episode");
    }
    if(common.read_only) {
        if(owner.read_db_||owner.xproc_read_db_||std::atomic_load(&owner.observer_delivery_connection_))
            startup_refuse("read-only owner has an unexpected writable publication");
    }else {
        const auto connection=std::atomic_load(&owner.observer_delivery_connection_);
        if(!connection||connection->creator_!=creator_||connection->owner_!=&owner||
           connection->writer_!=actual[0]||connection->catalog_.get()!=owner.recovery_schemas_owner_.get()||
           connection->catalog_.owner_before(owner.recovery_schemas_owner_)||owner.recovery_schemas_owner_.owner_before(connection->catalog_)||
           connection->read_profile_.get()!=common.read_profile.get()||
           connection->read_profile_.owner_before(common.read_profile)||common.read_profile.owner_before(connection->read_profile_)||
           !connection->runtime_||connection->runtime_.get()!=actual[0]->observer_delivery_sql_.get()||
           !connection->store_.same_domain(common.store))
                startup_refuse("finished owner writer and connection publication differ");
    }
}
bool observer_delivery_startup::requires_initialization_(const lattice_db& owner)const noexcept {
    // Phase selection is denial-only. Unknown/unpublished contexts stay on
    // the exact private route; a failed/expired setup cannot fall back to SQL.
    // Ordinary work after genuine publication keeps its existing route.
    if(!owner.owns_creator_process()||creator_!=database_native_process()||
       !state_||state_->creator!=creator_)return true;
    if(!state_->finished.load(std::memory_order_acquire))return true;
    return state_->actual_owner!=&owner||state_->failed.load(std::memory_order_acquire);
}
std::chrono::steady_clock::time_point observer_delivery_startup::initial_deadline_()const {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||state_->failed.load(std::memory_order_acquire))
        startup_refuse("delivery construction deadline original unavailable");
    return state_->policy.original_end_;
}
std::chrono::steady_clock::time_point observer_delivery_open_startup::original_deadline_()const {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       state_->failed.load(std::memory_order_acquire))startup_refuse("delivery open deadline original unavailable");
    return state_->policy.original_end_;
}
bool observer_delivery_open_startup::creator_current_()const noexcept {
    return creator_==database_native_process();
}
void observer_delivery_open_startup::claim_open_(const std::string& path) {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||state_->builder!=std::this_thread::get_id()||
       state_->failed.load(std::memory_order_acquire))startup_refuse("delivery open token original unavailable");
    state_->policy.verify_for_factory_(path);
    bool fresh=false;
    if(!state_->attempted.compare_exchange_strong(fresh,true,std::memory_order_acq_rel))
        startup_refuse("delivery open token already consumed before Native open");
}
void observer_delivery_open_startup::opened_(database& owner) {
    // This runs immediately after actual owned open returned and BEFORE the
    // constructor enters its first Native scope or prepares any statement.
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       state_->builder!=std::this_thread::get_id()||!state_->attempted.load(std::memory_order_acquire)||
       state_->opened.load(std::memory_order_acquire)||state_->failed.load(std::memory_order_acquire)||
       !owner.delivery_initialization_open_current_(this)||!state_->startup||
       state_->startup->creator_!=creator_)
        startup_refuse("initialization open is not its genuine first Native return");
    auto& common=*state_->startup->state_;
    if(common.failed.load(std::memory_order_acquire))startup_refuse("initialization context already failed");
    state_->actual=&owner;
    state_->opened.store(true,std::memory_order_release);
    const auto& issued=state_->policy.limits_;
    const bool initial=state_->kind==observer_delivery_startup::open_kind::writer||
        state_->kind==observer_delivery_startup::open_kind::reader||
        state_->kind==observer_delivery_startup::open_kind::cross_process_reader;
    try {
        if(initial&&!common.initial_budget) {
            if(common.builder!=std::this_thread::get_id()||common.finished.load(std::memory_order_acquire))
                startup_refuse("initial initialization builder publication changed");
            const observer_delivery_domain::limits limits{size_t(issued.clients),size_t(issued.waiters),
                {issued.guaranteed_count,issued.guaranteed_bytes},{issued.burst_count,issued.burst_bytes}};
            common.store=observer_delivery_store::bind(owner,limits);
            native_retirement_storage<observer_delivery_store::registration> registered;
            if(creator_!=database_native_process())startup_refuse("inherited startup registration storage");
            registered.get<0>()=common.store.register_client();
            if(creator_!=database_native_process())startup_refuse("inherited startup client registration");
            if(registered.get<0>().code!=observer_delivery_store::status::admitted)
                startup_refuse("initialization actual client registration unavailable");
            common.client=std::move(registered.get<0>().value);
            common.initial_budget=database_initialization_budget::create(owner,common.store,common.client,
                {issued.initialization_statements,issued.initialization_workspace_bytes,issued.read_columns,issued.read_rows},
                state_->policy.original_end_);
            if(creator_!=database_native_process())startup_refuse("inherited initialization budget creation");
            // New fixed handoff DATA is charged once to this SAME original
            // budget at its real creation, before profile/plan or any SQL.
            if(common.continuous_admission)
                common.initial_budget->claim_workspace_(observer_delivery_startup::continuous_custody_charge_);
            common.read_profile=database_sql_read_profile::create(creator_,&common,common.store,common.client,
                {issued.read_workspace_bytes,issued.read_columns,issued.read_rows,std::chrono::milliseconds(issued.read_duration_ms)},
                std::chrono::milliseconds(issued.admission_wait_ms));
            if(creator_!=database_native_process())startup_refuse("inherited startup read profile creation");
        }
        if(!common.read_profile||!common.store.current_for(owner))
            startup_refuse("initialization physical profile differs before first SQL");
        if(initial)state_->budget=common.initial_budget;
        else {
            const auto& replacement=state_->replacement;
            if(!replacement||replacement->creator!=creator_||
               replacement->builder!=std::this_thread::get_id()||replacement->startup.get()!=state_->startup.get()||
               replacement->owner!=common.actual_owner||replacement->end!=state_->policy.original_end_||
               replacement->failed.load(std::memory_order_acquire)||!replacement->budget)
                startup_refuse("actual replacement open lost its pre-open shared budget");
            replacement->budget->require_current_();state_->budget=replacement->budget;
        }
        if(creator_!=database_native_process())startup_refuse("inherited per-open initialization budget");
        state_->budget->bind_open_(owner,*this,state_->budget);
        if(initial&&!common.plan)common.plan=observer_delivery_initialization_plan::build_(*this,owner);
        if(creator_!=database_native_process())startup_refuse("inherited initialization recipe plan return");
        if(!common.plan)startup_refuse("initialization immutable plan was not constructed");
        // No legacy registration is pruned or adopted. Both installers retain
        // their strict actual zero-statement gate at the genuine empty open.
        (void)observer_delivery_sql_runtime::install_pending_(owner,*this);
        (void)database_sql_read_binding::install(owner,common.read_profile);
        // Passive nil-default DATA observation only, after both installers
        // returned their leaves and before the actual first SQL statement.
        const auto observed=database_initialization_test_probe::after_pending;
        if(observed)observed(owner);
        if(creator_!=database_native_process())startup_refuse("inherited pre-SQL observation return");
        require_initializing_(owner);
    }catch(...) {
        if(creator_==database_native_process()) {
            state_->failed.store(true,std::memory_order_release);
            if(initial)common.failed.store(true,std::memory_order_release);
            if(state_->replacement)state_->replacement->failed.store(true,std::memory_order_release);
            if(state_->budget)state_->budget->fail_();
        }
        throw;
    }
}
void observer_delivery_open_startup::require_initializing_(const database& owner)const {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       state_->builder!=std::this_thread::get_id()||state_->actual!=&owner||
       !state_->opened.load(std::memory_order_acquire)||state_->finished.load(std::memory_order_acquire)||
       state_->failed.load(std::memory_order_acquire)||!state_->startup||
       state_->startup->creator_!=creator_||state_->startup->state_->failed.load(std::memory_order_acquire)||
       !owner.creator_current_()||
       owner.closed_.load(std::memory_order_acquire)||owner.delivery_startup_.get()!=this||
       !owner.delivery_initialization_open_current_(this)||
       std::chrono::steady_clock::now()>=state_->policy.original_end_)
        startup_refuse("initialization lost its exact pending open/builder/deadline");
}
void observer_delivery_startup::require_continuous_enrollment_owner_(lattice_db& owner,
    const recovery_continuous_admission* admission,std::chrono::steady_clock::time_point actual_entry_end) {
    owner.require_creator_process();
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        startup_refuse("inherited continuous enrollment owner validation");
    auto& common=*state_;
    try {
        if(!admission||common.continuous_admission!=admission||owner.recovery_continuous_.get()!=admission||
           actual_entry_end==std::chrono::steady_clock::time_point::max()||common.policy.original_end_!=actual_entry_end||
           std::chrono::steady_clock::now()>=actual_entry_end||
           common.builder!=std::this_thread::get_id()||common.pending_owner!=&owner||common.actual_owner||
           common.finished.load(std::memory_order_acquire)||common.failed.load(std::memory_order_acquire)||
           common.read_only||owner.config_.read_only||owner.closed_.load(std::memory_order_acquire)||
           owner.delivery_startup_.get()!=this||common.catalog!=&owner.recovery_schemas_||
           !common.catalog||!common.catalog->valid()||common.schema_version!=owner.config_.target_schema_version||
           !common.initial_budget||!common.plan||!common.read_profile)
            startup_refuse("continuous enrollment requires the same unpublished writable owner/catalog");
        common.initial_budget->require_current_();
        common.initial_budget->claim_workspace_(sizeof(observer_delivery_startup_test_probe::owner_binding_observation)+
            sizeof(std::array<database*,3>)+sizeof(std::array<open_kind,3>));
        const auto& profile=*common.read_profile;const auto& limits=common.policy.limits_;
        if(profile.creator_!=creator_||profile.publisher_!=&common||profile.published_.load(std::memory_order_acquire)||
           !profile.store_.same_domain(common.store)||profile.policy_.workspace_bytes!=limits.read_workspace_bytes||
           profile.policy_.columns!=limits.read_columns||profile.policy_.rows!=limits.read_rows||
           profile.policy_.maximum_duration!=std::chrono::milliseconds(limits.read_duration_ms)||
           profile.maximum_admission_wait_!=std::chrono::milliseconds(limits.admission_wait_ms))
            startup_refuse("continuous enrollment lost its original unpublished common profile");
        std::array<database*,3> actual{owner.db_.get(),owner.read_db_.get(),owner.xproc_read_db_.get()};
        constexpr std::array<open_kind,3> kinds{open_kind::writer,open_kind::reader,open_kind::cross_process_reader};
        observer_delivery_startup_test_probe::owner_binding_observation observed{};
        observed.catalog=common.catalog;observed.budget=common.initial_budget.get();observed.profile=common.read_profile.get();
        for(std::size_t index=0;index<actual.size();++index) {
            auto* current=actual[index];if(!current)continue;
            for(std::size_t prior=0;prior<index;++prior)if(actual[prior]==current)
                startup_refuse("continuous enrollment actual roles alias");
            observed.roles|=1u<<index;current->require_creator_();auto* open=current->delivery_startup_.get();
            if(!open||open->creator_!=creator_||!open->state_||open->state_->creator!=creator_||
               open->state_->startup.get()!=this||open->state_->builder!=common.builder||open->state_->kind!=kinds[index]||
               open->state_->replacement||open->state_->actual!=current||
               open->state_->policy.original_end_!=common.policy.original_end_||
               open->state_->budget.get()!=common.initial_budget.get()||
               open->state_->budget.owner_before(common.initial_budget)||common.initial_budget.owner_before(open->state_->budget)||
               current->mode_!=(index==0?database::open_mode::read_write:database::open_mode::read_only))
                startup_refuse("continuous enrollment role original differs");
            open->require_work_current_(*current);
            if(!common.store.current_for(*current))startup_refuse("continuous enrollment role physical store differs");
            {
                database::catalog_native_admission_ leaf(*current);open->require_work_current_(*current);
                const auto binding=std::atomic_load(&current->sql_read_binding_);
                if(!binding||binding->creator_!=creator_||binding->owner_!=current||
                   binding->profile_.get()!=common.read_profile.get()||binding->profile_.owner_before(common.read_profile)||
                   common.read_profile.owner_before(binding->profile_)||!current->observer_delivery_sql_||
                   !current->observer_delivery_sql_->pending_original_for_(*current,*open)||
                   current->raw_handle_escaped_.load(std::memory_order_acquire)||current->engine_reads_||
                   sqlite3_get_autocommit(current->db_)==0)
                    startup_refuse("continuous enrollment role runtime/profile is not the original idle pending family");
                std::size_t statements=0;
                for(auto* statement=sqlite3_next_stmt(current->db_,nullptr);statement;statement=sqlite3_next_stmt(current->db_,statement))
                    if(++statements>4096||sqlite3_stmt_busy(statement))startup_refuse("continuous enrollment role Native corpus is active or unbounded");
                observed.actual[index]=current;observed.open[index]=open;observed.role_budget[index]=open->state_->budget.get();
                observed.role_profile[index]=binding->profile_.get();observed.runtime[index]=current->observer_delivery_sql_.get();
            }
            open->require_work_current_(*current);
        }
        if(!(observed.roles&1u)||observed.roles!=common.initial_roles||common.initial_roles>7u)
            startup_refuse("continuous enrollment lost its whole original initial role family");
        common.initial_budget->require_current_();
        if(const auto hook=observer_delivery_startup_test_probe::after_continuous_enrollment_bound)hook(owner,admission,observed);
        if(creator_!=database_native_process())startup_refuse("inherited continuous enrollment observation return");
    }catch(...) {
        if(creator_==database_native_process()) {common.failed.store(true,std::memory_order_release);if(common.initial_budget)common.initial_budget->fail_();}
        throw;
    }
}
std::shared_ptr<const observer_delivery_open_startup>
observer_delivery_open_startup::initialization_original_(const database& owner)const {
    require_initializing_(owner);
    return owner.delivery_startup_.shared_();
}
const std::shared_ptr<database_initialization_budget>&
observer_delivery_open_startup::initialization_budget_(const database& owner)const {
    require_initializing_(owner);
    if(!state_->budget)startup_refuse("initialization budget was not admitted before SQL");
    return state_->budget;
}
bool observer_delivery_open_startup::cleanup_for_(const database& owner)const noexcept {
    // Identity/ending custody only. The exact preissued transaction episode
    // separately decides whether its RESERVED rollback may run after failure
    // or deadline. This neither mints SQL nor repairs health/publication debt.
    return creator_==database_native_process()&&state_&&state_->creator==creator_&&
        state_->builder==std::this_thread::get_id()&&state_->actual==&owner&&
        state_->opened.load(std::memory_order_acquire)&&!state_->finished.load(std::memory_order_acquire)&&
        owner.delivery_initialization_open_current_(this);
}
void observer_delivery_open_startup::fail_initialization_()const noexcept {
    // Failure is monotonic on this actual unpublished episode. Inherited
    // returns quarantine custody instead of touching the parent's budget.
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)return;
    state_->failed.store(true,std::memory_order_release);
    if(state_->budget)state_->budget->fail_();
    if(state_->replacement)state_->replacement->failed.store(true,std::memory_order_release);
    const bool initial=state_->kind==observer_delivery_startup::open_kind::writer||
        state_->kind==observer_delivery_startup::open_kind::reader||
        state_->kind==observer_delivery_startup::open_kind::cross_process_reader;
    // A failed replacement episode preserves the previously published owner
    // and its healthy original; it cannot reset/reuse this failed episode.
    if(initial&&state_->startup&&state_->startup->creator_==creator_&&state_->startup->state_)
        state_->startup->state_->failed.store(true,std::memory_order_release);
}
void observer_delivery_open_startup::require_work_current_(const database& owner)const {
    require_initializing_(owner);
    initialization_budget_(owner)->require_current_();
}
void observer_delivery_open_startup::claim_workspace_(const database& owner,std::uint64_t bytes)const {
    initialization_budget_(owner)->claim_workspace_(bytes);
}
void observer_delivery_open_startup::debit_initialization_storage_(
    void* actual_open,void* actual_database,std::uint64_t bytes) {
    if(!actual_open||!actual_database)startup_refuse("initialization storage lacks its actual retained Open/database");
    const auto* open=static_cast<const observer_delivery_open_startup*>(actual_open);
    if(!open->creator_current_())startup_refuse("inherited initialization storage Open");
    auto& actual=*static_cast<database*>(actual_database);
    open->require_work_current_(actual);
    open->claim_workspace_(actual,bytes);
}

const std::shared_ptr<const observer_delivery_initialization_plan>&
observer_delivery_open_startup::initialization_plan_(const database& actual)const {
    require_work_current_(actual);
    const auto& plan=state_->startup->state_->plan;
    if(!plan)startup_refuse("actual pending immutable initialization Plan missing");return plan;
}
std::array<char,35> observer_delivery_open_startup::model_meter_marker_key_(database& actual)const {
    require_work_current_(actual);
    const auto& common=*state_->startup->state_;
    if(!common.pending_owner||common.continuous_admission)
        startup_refuse("existing normal model meter requires its actual pending owner/family");
    auto& owner=*common.pending_owner;require_model_owner_(owner,actual);
    if(owner.recovery_continuous_)startup_refuse("continuous model meter requires its genuine enrolled-program supplier");
    return fingerprint_marker_bytes_(owner,actual);
}
void observer_delivery_open_startup::require_final_meter_writer_(database& actual)const {
    require_work_current_(actual);
    const auto& common=*state_->startup->state_;
    if(state_->kind!=observer_delivery_startup::open_kind::writer||state_->replacement||
       !common.pending_owner||common.actual_owner||common.finished.load(std::memory_order_acquire)||
       common.read_only||common.continuous_admission||common.catalog!=&common.pending_owner->recovery_schemas_)
        startup_refuse("absent final meter requires its genuine unpublished normal initial writer");
    auto& owner=*common.pending_owner;owner.require_creator_process();actual.require_creator_();
    if(owner.db_.get()!=&actual||owner.delivery_startup_.get()!=state_->startup.get()||owner.recovery_continuous_||
       owner.closed_.load(std::memory_order_acquire)||actual.mode_!=database::open_mode::read_write)
        startup_refuse("final meter installer changed its actual unpublished writer/owner");
}
const recovery_owner_schema& observer_delivery_open_startup::final_catalog_(const database& actual)const {
    require_work_current_(actual); // creator/process/thread before common/owner payload
    const auto& common=*state_->startup->state_;
    const bool initial=state_->kind==observer_delivery_startup::open_kind::writer||
        state_->kind==observer_delivery_startup::open_kind::reader||
        state_->kind==observer_delivery_startup::open_kind::cross_process_reader;
    const bool replacement=state_->kind==observer_delivery_startup::open_kind::replacement_writer||
        state_->kind==observer_delivery_startup::open_kind::replacement_reader||
        state_->kind==observer_delivery_startup::open_kind::replacement_cross_process_reader;
    if(!initial&&!replacement)startup_refuse("final catalog requires one actual owner role");
    const auto* owner=initial?common.pending_owner:common.actual_owner;
    if(!owner)startup_refuse("final catalog actual bound owner is absent");
    owner->require_creator_process();
    if(owner->delivery_startup_.get()!=state_->startup.get()||owner->closed_.load(std::memory_order_acquire)||
       !owner->recovery_schemas_owner_||owner->recovery_schemas_owner_.get()!=&owner->recovery_schemas_||
       !owner->recovery_schemas_.valid()||common.schema_version!=owner->config_.target_schema_version||
       common.read_only!=owner->config_.read_only||common.continuous_admission!=owner->recovery_continuous_.get())
        startup_refuse("final catalog differs from the same actual owner declaration");
    if(initial) {
        if(common.catalog!=&owner->recovery_schemas_||common.actual_owner||
           common.finished.load(std::memory_order_acquire)||state_->replacement||
           state_->budget.get()!=common.initial_budget.get()||state_->budget.owner_before(common.initial_budget)||
           common.initial_budget.owner_before(state_->budget))
            startup_refuse("initial final catalog lost its exact constructor borrow/budget");
        const auto* retained=state_->kind==observer_delivery_startup::open_kind::writer?owner->db_.get():
            state_->kind==observer_delivery_startup::open_kind::reader?owner->read_db_.get():owner->xproc_read_db_.get();
        if(retained!=&actual)startup_refuse("initial final catalog role wrapper differs");
    }else {
        const auto& episode=state_->replacement;
        if(common.catalog||common.pending_owner||!common.finished.load(std::memory_order_acquire)||
           !episode||episode->creator!=creator_||episode->owner!=owner||
           episode->startup.get()!=state_->startup.get()||episode->builder!=state_->builder||
           episode->failed.load(std::memory_order_acquire)||episode->end!=state_->policy.original_end_||
           episode->budget.get()!=state_->budget.get()||episode->budget.owner_before(state_->budget)||
           state_->budget.owner_before(episode->budget))
            startup_refuse("replacement final catalog lost its same admitted owner episode");
    }
    return owner->recovery_schemas_; // immutable borrowed DATA; no Native/profile/finish grant
}
const recovery_owner_schema& observer_delivery_open_startup::initialization_catalog_(const database& owner)const {
    require_initializing_(owner);
    const auto& common=*state_->startup->state_;
    if(common.builder!=std::this_thread::get_id()||common.finished.load(std::memory_order_acquire)||!common.catalog)
        startup_refuse("initialization catalog is not its retained original builder input");
    return *common.catalog;
}
std::shared_ptr<const observer_delivery_initialization_preimage>
observer_delivery_open_startup::capture_enrollment_ledger_preimage_(database& owner,
    const recovery_obligation_bootstrap_reader& bootstrap,enrollment_ledger_phase phase) {
    try {
        require_work_current_(owner);bootstrap.require_owner_(owner);bootstrap.require_enrollment_current_();
        if(phase!=enrollment_ledger_phase::construction&&phase!=enrollment_ledger_phase::complete)
            startup_refuse("unissued ledger preimage phase");
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("ledger initialization lacks its immutable original catalog plan");
        using selection=observer_delivery_initialization_plan::preimage_selection;
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>>;
        claim_workspace_(owner,custody::allocation_bytes());custody held;
        if(creator_!=database_native_process())startup_refuse("inherited ledger proof custody");
        held.get<0>()=plan->capture_preimage_(*this,owner,&bootstrap,
            phase==enrollment_ledger_phase::complete?selection::enrollment_ledger_complete:selection::enrollment_ledger_prefix);
        if(creator_!=database_native_process())startup_refuse("inherited actual ledger proof return");
        return std::move(held.get<0>());
    }catch(...) {fail_initialization_();throw;}
}
bool observer_delivery_open_startup::preimage_enrollment_ledger_for_(const database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,
    const recovery_obligation_bootstrap_reader* reader,enrollment_ledger_operator op,
    std::string_view table,std::string_view index)const noexcept {
    return pending_for_(owner)&&proof&&proof->enrollment_ledger_for_(owner,reader,op,table,index);
}
std::shared_ptr<const observer_delivery_initialization_preimage>
observer_delivery_open_startup::capture_preimage_(database& owner,const recovery_obligation_bootstrap_reader* bootstrap) {
    try {
        require_work_current_(owner);
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("initialization preimage has no immutable catalog plan");
        // Construction is synchronous on this actual builder. All owning
        // capture/result storage remains creator-fenced before publication.
        native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>> held;
        if(creator_!=database_native_process())startup_refuse("inherited preimage capture custody");
        held.get<0>()=plan->capture_preimage_(*this,owner,bootstrap);
        if(creator_!=database_native_process())startup_refuse("inherited preimage capture return");
        // The explicit reader retains this receipt. Open owns no reverse
        // strong link, avoiding an Open->proof->episode->Open custody cycle.
        return std::move(held.get<0>());
    }catch(...) {fail_initialization_();throw;}
}
std::shared_ptr<const observer_delivery_initialization_preimage>
observer_delivery_open_startup::capture_receive_guard_preimage_(database& owner,receive_preimage_phase phase) {
    try {
        require_work_current_(owner);
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("receive initializer lacks actual library plan");
        using selection=observer_delivery_initialization_plan::preimage_selection;
        selection selected=selection::receive_read_existing;
        switch(phase) {
          case receive_preimage_phase::read_existing:break;
          case receive_preimage_phase::before_create_store:selected=selection::receive_before_store;break;
          case receive_preimage_phase::before_create_channel:selected=selection::receive_before_channel;break;
          case receive_preimage_phase::before_create_index:selected=selection::receive_before_index;break;
          case receive_preimage_phase::strict_seed:selected=selection::receive_strict_seed;break;
          default:startup_refuse("unknown receive initialization prefix");
        }
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>>;
        claim_workspace_(owner,custody::allocation_bytes());custody retained;
        if(!creator_current_())startup_refuse("inherited receive preimage custody");
        retained.get<0>()=plan->capture_preimage_(*this,owner,nullptr,selected);
        if(!creator_current_())startup_refuse("inherited receive preimage return");
        if(!retained.get<0>()||!retained.get<0>()->receive_phase_for_(owner,phase))
            startup_refuse("receive initialization prefix was not actually proved");
        return std::move(retained.get<0>());
    }catch(...) {fail_initialization_();throw;}
}
bool observer_delivery_open_startup::preimage_receive_phase_for_(const database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,receive_preimage_phase phase)const noexcept {
    return creator_current_()&&proof&&pending_for_(owner)&&proof->receive_phase_for_(owner,phase);
}
bool observer_delivery_open_startup::preimage_receive_seed_for_(const database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof)const noexcept {
    return preimage_receive_phase_for_(owner,proof,receive_preimage_phase::strict_seed);
}
observer_delivery_initialization_sql observer_delivery_open_startup::metadata_table_recipe_(database& owner)const {
    try {
        require_work_current_(owner);
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("metadata creation lacks its actual library plan");
        return plan->metadata_table_recipe_(*this,owner);
    }catch(...) {fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_open_startup::metadata_initial_version_recipe_(database& owner)const {
    try {
        require_work_current_(owner);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("metadata seed lacks its actual library Plan");
        return plan->metadata_initial_version_recipe_(*this,owner);
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_model_write_(database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,model_preimage_phase phase)const {
    require_work_current_(owner);
    const auto current=[&] {
        if(!creator_current_())startup_refuse("inherited model WRITE return");
        owner.require_creator_();
        if(!proof||!proof->model_phase_for_(owner,phase))
            startup_refuse("model operator lost its actual same-BEGIN/revision phase proof");
    };
    current();store_physical_native_scope native(owner);
    if(!native.entered())startup_refuse("model WRITE Native original unavailable");
    database::catalog_native_admission_ leaf(owner);current();
    auto* actual=owner.internal_handle();if(!actual)startup_refuse("model WRITE database unavailable");
    const int autocommit=sqlite3_get_autocommit(actual);current();
    const int transaction=sqlite3_txn_state(actual,"main");current();
    if(autocommit||transaction!=SQLITE_TXN_WRITE)startup_refuse("model operator requires actual private bootstrap WRITE");
}
std::int64_t observer_delivery_open_startup::initialization_schema_version_(database& actual)const {
    require_work_current_(actual);
    if(state_->kind!=observer_delivery_startup::open_kind::writer||!state_->startup||!state_->startup->state_||
       state_->startup->state_->schema_version<=0)startup_refuse("actual pending target version unavailable");
    return state_->startup->state_->schema_version;
}
void observer_delivery_open_startup::initialize_model_metadata_(lattice_db& owner,database& actual,
    std::string_view key,std::string_view value)const {
    try {
        require_model_owner_(owner,actual);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("model metadata lacks actual immutable Plan");
        const auto creator=database_native_process();
        auto read=plan->model_metadata_recipe_(*this,actual,true,key,value);
        auto before=actual.query_initialization_(read);
        if(creator!=database_native_process())startup_refuse("inherited model metadata pre-read");
        const auto exact=[&](const std::vector<database::row_t>& rows) {
            if(rows.size()!=1||rows.front().size()!=1)return false;
            const auto at=rows.front().find("value");return at!=rows.front().end()&&
                std::holds_alternative<std::string>(at->second)&&std::get<std::string>(at->second)==value;
        };
        // An unknown/stale library value is DATA to refuse, never replacement
        // authority. Supported absent/current rows use the actual normal store.
        if(!before.empty()&&!exact(before))startup_refuse("model metadata preimage differs from retained current producer");
        auto write=plan->model_metadata_recipe_(*this,actual,false,key,value);
        actual.execute_initialization_(write);
        require_model_owner_(owner,actual);
        auto post=capture_model_preimage_(actual);
        require_model_write_(actual,post,model_preimage_phase::construction);
        if(!post||!post->plain_table_for_(actual,"_lattice_meta"))startup_refuse("actual model metadata table postimage absent");
        auto readback=plan->model_metadata_recipe_(*this,actual,true,key,value);
        auto after=actual.query_initialization_(readback);
        if(creator!=database_native_process())startup_refuse("inherited model metadata post-read");
        if(!exact(after))startup_refuse("actual model metadata value postimage differs");
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::initialize_model_version_(lattice_db& owner,database& actual,std::int64_t next)const {
    try {
        require_model_owner_(owner,actual);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("model version lacks actual immutable Plan");
        const auto read_version=[&] {
            auto issued=observer_delivery_initialization_sql::metadata_read_(*this,actual,
                observer_delivery_initialization_sql::recipe::metadata_version_read);
            const auto rows=actual.query_initialization_(issued);
            if(rows.size()!=1||rows.front().size()!=1||!std::holds_alternative<std::string>(rows.front().at("value")))
                startup_refuse("model version requires one actual TEXT metadata value");
            const auto& text=std::get<std::string>(rows.front().at("value"));
            if(text.empty()||text.size()>10||(text.size()>1&&text.front()=='0'))startup_refuse("actual model version is not canonical");
            std::int64_t value=0;for(const auto ch:text){if(ch<'0'||ch>'9')startup_refuse("actual model version is not canonical");value=value*10+ch-'0';}
            if(value<=0||value>initialization_schema_version_(actual))startup_refuse("actual model version is outside supported positive current/target range");
            return value;
        };
        const auto current=read_version();
        if(next!=current+1||next>initialization_schema_version_(actual))startup_refuse("model version does not advance exactly one supported step");
        auto issued=plan->model_version_recipe_(*this,actual,next);actual.execute_initialization_(issued);
        require_model_owner_(owner,actual);
        auto post=capture_model_preimage_(actual);
        require_model_write_(actual,post,model_preimage_phase::construction);
        if(read_version()!=next)startup_refuse("actual model version postimage differs");
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_model_owner_(lattice_db& owner,database& actual)const {
    require_work_current_(actual);owner.require_creator_process();
    const auto& common=*state_->startup->state_;
    if(state_->kind!=observer_delivery_startup::open_kind::writer||common.read_only||
    common.builder!=std::this_thread::get_id()||common.pending_owner!=&owner||common.actual_owner||
    owner.delivery_startup_.get()!=state_->startup.get()||owner.db_.get()!=&actual||
    common.catalog!=&owner.recovery_schemas_||!common.catalog||!common.catalog->valid()||
    common.schema_version!=owner.config_.target_schema_version||owner.closed_.load(std::memory_order_acquire))
    startup_refuse("model producer requires its exact unpublished writable owner/catalog");
}
void observer_delivery_open_startup::initialize_model_recipe_(lattice_db& owner,database& actual,std::string_view text)const {
    try {
        require_model_owner_(owner,actual);
        const auto& common=*state_->startup->state_;
        const auto creator=database_native_process();const auto& plan=common.plan;
        if(!plan)startup_refuse("model producer lacks its actual immutable Plan");
        auto issued=plan->select_(*this,actual,text,{});
        if(creator!=database_native_process())startup_refuse("inherited model descriptor return");
        if(!issued.entry_||issued.entry_->model_plan.get()!=plan.get()||!issued.model_work_current_for_(actual))
            startup_refuse("schema program is not an actual typed model operator");
        if(issued.entry_->id!=observer_delivery_initialization_sql::recipe::model_table&&
           issued.entry_->id!=observer_delivery_initialization_sql::recipe::model_program&&
           issued.entry_->id!=observer_delivery_initialization_sql::recipe::model_index&&
           issued.entry_->id!=observer_delivery_initialization_sql::recipe::model_unique_index&&
           issued.entry_->id!=observer_delivery_initialization_sql::recipe::model_shadow_column)
            startup_refuse("model DDL cannot substitute a metadata/version operator");
        const auto index=issued.entry_->model_operator;
        actual.execute_initialization_(issued);
        require_work_current_(actual);
        // No previously issued proof survives a real DDL attempt. Capture the
        // actual return's complete metadata prefix and require this target.
        auto post=capture_model_preimage_(actual,issued.entry_->model_preimage,index);
        if(creator!=database_native_process())startup_refuse("inherited model postimage return");
        require_model_write_(actual,post,model_preimage_phase::construction);
        if(!post||!plan->model_postimage_for_(index,*this,actual,*post))
            startup_refuse("actual generated model object postimage missing");
    }catch(...) {fail_initialization_();throw;}
}
std::shared_ptr<const observer_delivery_initialization_preimage>
observer_delivery_open_startup::capture_model_preimage_(database& actual,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& predecessor,std::size_t after_operator)const {
    try {
        require_work_current_(actual);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("model construction lacks actual immutable Plan");
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_stage>,
            std::shared_ptr<const observer_delivery_initialization_preimage>>;
        claim_workspace_(actual,custody::allocation_bytes());custody held;
        if(!creator_current_())startup_refuse("inherited physical model stage custody");
        held.get<0>()=state_->model_stage;
        if(!creator_current_())startup_refuse("inherited physical model stage retain");
        held.get<1>()=plan->capture_preimage_(*this,actual,nullptr,
            observer_delivery_initialization_plan::preimage_selection::model_construction_prefix,
            held.get<0>().get(),predecessor.get(),after_operator);
        if(!creator_current_())startup_refuse("inherited physical model stage proof return");
        require_model_write_(actual,held.get<1>(),model_preimage_phase::construction);
        if(after_operator!=std::size_t(-1)&&!plan->model_postimage_for_(after_operator,*this,actual,*held.get<1>()))
            startup_refuse("model stage cannot publish before actual target postimage");
        if(after_operator!=std::size_t(-1)||!held.get<0>()||!plan->model_stage_for_(*held.get<0>(),*held.get<1>())) {
            auto next=plan->model_stage_from_(*this,actual,*held.get<1>());
            if(!creator_current_())startup_refuse("inherited physical model stage issue return");
            require_model_write_(actual,held.get<1>(),model_preimage_phase::construction);
            state_->model_stage=std::move(next); // pure DATA retirement, no Native/Open ownership cycle
        }
        return std::move(held.get<1>());
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_model_catalog_present_(lattice_db& owner,database& actual)const {
    try {
        require_model_owner_(owner,actual);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("required model catalog lacks actual Plan");
        auto proof=plan->capture_preimage_(*this,actual,nullptr,
            observer_delivery_initialization_plan::preimage_selection::model_required_present);
        require_model_write_(actual,proof,model_preimage_phase::complete);
    }catch(...) {fail_initialization_();throw;}
}
bool observer_delivery_open_startup::model_table_present_(lattice_db& owner,database& actual,std::string_view table)const {
    try {
        require_model_owner_(owner,actual);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("model inventory lacks immutable Plan");
        auto proof=capture_model_preimage_(actual);
        require_model_write_(actual,proof,model_preimage_phase::construction);
        return proof->model_table_present_for_(actual,table);
    }catch(...) {fail_initialization_();throw;}
}
observer_delivery_initialization_columns
observer_delivery_open_startup::model_table_info_(lattice_db& owner,database& actual,std::string_view table)const {
    try {
        require_model_owner_(owner,actual);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("model columns lack immutable Plan");
        auto proof=capture_model_preimage_(actual);
        require_model_write_(actual,proof,model_preimage_phase::construction);
        auto result=proof->model_table_info_for_(actual,table,proof);
        require_model_write_(actual,proof,model_preimage_phase::construction);
        return result;
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_current_model_(lattice_db& owner,database& actual,const model_schema& model)const {
    try {
        require_model_owner_(owner,actual);const auto& catalog=initialization_catalog_(actual);
        if(model.table_name.empty()||model.table_name.size()>256)
            startup_refuse("current model no-op requires its actual captured declaration");
        const auto found=catalog.models.find(model.table_name);
        if(found==catalog.models.end()||&found->second!=&model)
            startup_refuse("current model no-op cannot adopt a foreign/copied declaration");
        require_model_catalog_present_(owner,actual);
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("current model no-op lacks its actual immutable Plan");
        plan->require_current_model_metadata_(*this,actual);
        require_model_owner_(owner,actual);
    }catch(...) {fail_initialization_();throw;}
}
bool observer_delivery_open_startup::fresh_catalog_active_for_(const database& actual)const noexcept {
    if(!creator_current_()||!actual.creator_current_()||!state_||state_->actual!=&actual||
       state_->builder!=std::this_thread::get_id()||
       bool(state_->fresh_catalog_original)==bool(state_->fresh_normal_original)||!state_->fresh_catalog_snapshot||
       !pending_for_(actual))return false;
    const auto episode=std::atomic_load(&actual.initialization_episode_);
    return episode.get()==state_->fresh_catalog_snapshot;
}
void observer_delivery_open_startup::initialize_captured_model_catalog_(lattice_db& owner,database& actual,bool swift_builder)const {
    try {
        require_model_owner_(owner,actual);
        const auto* first=recovery_continuous_producer::pending_fresh_catalog_original_(owner,actual,*state_->startup);
        const auto* normal=actual.normal_initialization_original_.fresh_for_(actual,*this);
        if(first&&normal)startup_refuse("fresh catalog cannot join independent first-start originals");
        if((first||normal)&&!state_->fresh_catalog_completed){initialize_fresh_model_catalog_(owner,actual,swift_builder,first,normal);return;}
        // An ordinary existing/READY path has no recreated fresh claim. Its
        // complete current catalog proof or genuine typed migration remains required.
        initialize_current_model_catalog_(owner,actual,swift_builder);
    }catch(...){fail_initialization_();throw;}
}
void observer_delivery_open_startup::initialize_fresh_model_catalog_(lattice_db& owner,database& actual,bool swift_builder,
    const recovery_continuous_admission* original,const store_resource_initialization_original* normal_original)const {
    try {
        require_model_owner_(owner,actual);
        const auto same_original=[&] {
            if(bool(original)==bool(normal_original))startup_refuse("fresh catalog requires ONE actual first-start source");
            const auto* continuous=recovery_continuous_producer::pending_fresh_catalog_original_(owner,actual,*state_->startup);
            const auto* normal=actual.normal_initialization_original_.fresh_for_(actual,*this);
            if(continuous!=original||normal!=normal_original)startup_refuse("fresh catalog original changed before effects");
        };
        if(state_->fresh_catalog_completed||state_->fresh_catalog_original||state_->fresh_normal_original||state_->fresh_catalog_snapshot)
            startup_refuse("fresh catalog lacks its exact one-use first-start original");
        same_original();
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(this),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        actual.require_initialization_registrations_(*this); // actual selected providers before any schema effect
        const auto creator=database_native_process();const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("fresh catalog lacks its immutable full captured Plan");
        auto transaction=owner.begin_schema_initialization_(actual,lattice_db::schema_startup_transaction::immediate);
        if(!creator_current_()||creator!=database_native_process())startup_refuse("inherited fresh catalog WRITE return");
        plan->require_fresh_catalog_empty_(*this,actual);
        same_original(); // actual empty WRITE, before the first durable schema effect
        auto recursion=observer_delivery_initialization_sql::fixed_(*this,actual,
            observer_delivery_initialization_sql::recipe::connection_recursive_triggers_on);
        actual.execute_initialization_(recursion);
        using snapshot_custody=native_retirement_storage<std::shared_ptr<const database_initialization_episode>>;
        ordinary_initialization_storage_scope::charge(snapshot_custody::allocation_bytes());snapshot_custody snapshot;
        if(!creator_current_())startup_refuse("inherited fresh snapshot custody");
        snapshot.get<0>()=actual.capture_initialization_snapshot_(*this);
        if(!creator_current_()||!snapshot.get<0>())startup_refuse("fresh catalog lacks its actual WRITE snapshot");
        struct active_scope {
            state* held;const std::uint64_t creator;
            active_scope(state* value,const recovery_continuous_admission* original,
                const store_resource_initialization_original* normal,const database_initialization_episode* snapshot)
                :held(value),creator(database_native_process()){held->fresh_catalog_original=original;held->fresh_normal_original=normal;held->fresh_catalog_snapshot=snapshot;}
            ~active_scope(){if(creator==database_native_process()){held->fresh_catalog_original=nullptr;held->fresh_normal_original=nullptr;held->fresh_catalog_snapshot=nullptr;}}
        } active(state_,original,normal_original,snapshot.get<0>().get());
        auto meta=metadata_table_recipe_(actual);actual.execute_initialization_(meta);
        auto version=metadata_initial_version_recipe_(actual);actual.execute_initialization_(version);
        auto control=sync_control_table_recipe_(actual);actual.execute_initialization_(control);
        auto seed=sync_control_initial_recipe_(actual);actual.execute_initialization_(seed);
        for(const auto operation:{audit_operation::table,audit_operation::pending_index,audit_operation::lookup_index})initialize_audit_(actual,operation);
        initialize_receive_guard_schema(actual,false); // false derives from proved empty genuine first-start WRITE
        for(const auto operation:{static_sync_operation::set_table,static_sync_operation::state_table,
            static_sync_operation::state_pending_index,static_sync_operation::slots_table,static_sync_operation::upload_floor})
            initialize_static_sync_(actual,operation);
        plan->initialize_fresh_catalog_families_(*this,owner,actual);
        const auto target=initialization_schema_version_(actual);
        if(target<=0)startup_refuse("fresh catalog requires its actual positive target version");
        for(std::int64_t next=1;next<target;){++next;initialize_model_version_(owner,actual,next);}
        require_and_mark_current_catalog_(owner,actual,swift_builder);
        const auto marker=fingerprint_marker_bytes_(owner,actual);
        using evidence_custody=native_retirement_storage<observer_delivery_initialization_plan::meter_entry_evidence>;
        ordinary_initialization_storage_scope::charge(evidence_custody::allocation_bytes());evidence_custody evidence;
        plan->require_normal_state_(*this,actual,initialization_catalog_(actual),target,std::string_view(marker.data(),marker.size()),&evidence.get<0>());
        same_original();
        if(!fresh_catalog_active_for_(actual))startup_refuse("fresh full catalog postimage lost its original WRITE/origin");
        transaction.commit();
        if(!creator_current_())startup_refuse("inherited fresh catalog COMMIT return");
        if(normal_original)actual.normal_initialization_original_.finish_initialization_(actual,*this);
        state_->fresh_catalog_completed=true; // genuine known COMMIT only; no finish/seal/publication permission
        require_work_current_(actual);
    }catch(...){fail_initialization_();throw;}
}
void observer_delivery_open_startup::initialize_current_model_catalog_(lattice_db& owner,database& actual,bool swift_builder)const {
    try {
        require_model_owner_(owner,actual);
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(this),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        const auto creator=database_native_process();
        // SAME actual catalog across Core/Swift/link/derived families. No
        // scalar/shape subset grants an easier proof. Existing incompatible,
        // absent or incomplete programs require their real typed producer.
        auto transaction=owner.begin_schema_initialization_(actual,lattice_db::schema_startup_transaction::immediate);
        if(!creator_current_()||creator!=database_native_process())startup_refuse("inherited current-model WRITE return");
        require_and_mark_current_catalog_(owner,actual,swift_builder);
        transaction.commit();
        if(!creator_current_())startup_refuse("inherited current-model commit return");
        require_work_current_(actual);
        // Schema-only constructor work. No finished/open-owner state, store
        // grant or publication is changed. Actual side-registry/meter/guard
        // registration remains a required separate finalization producer.
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_and_mark_current_catalog_(lattice_db& owner,database& actual,bool swift_builder)const {
    const auto creator=database_native_process();
        require_model_catalog_present_(owner,actual);
        const auto target=initialization_schema_version_(actual);
        if(target<=0)startup_refuse("current-model proof requires a positive actual target version");
        auto version=observer_delivery_initialization_sql::metadata_read_(*this,actual,
            observer_delivery_initialization_sql::recipe::metadata_version_read);
        using rows_custody=native_retirement_storage<std::vector<database::row_t>>;
        ordinary_initialization_storage_scope::charge(rows_custody::allocation_bytes());rows_custody rows;
        if(!creator_current_())startup_refuse("inherited current-model version custody");
        rows.get<0>()=actual.query_initialization_(version);
        if(!creator_current_())startup_refuse("inherited current-model version read");
        const auto expected=ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(target));
        if(rows.get<0>().size()!=1||rows.get<0>().front().size()!=1)
            startup_refuse("current-model version row requires its real typed migration");
        const column_value_t* version_cell=nullptr;
        for(const auto& item:rows.get<0>().front())if(std::string_view(item.first)=="value")version_cell=&item.second;
        const auto* observed=version_cell?std::get_if<std::string>(version_cell):nullptr;
        if(!observed||std::string_view(*observed)!=std::string_view(expected))
            startup_refuse("current-model version differs; real typed migration is required");
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("current-model metadata lacks its actual immutable Plan");
        plan->require_current_model_metadata_(*this,actual);
        require_model_owner_(owner,actual);
        const auto key=fingerprint_marker_bytes_(owner,actual);
        auto cookie_recipe=observer_delivery_initialization_sql::fixed_(*this,actual,
            observer_delivery_initialization_sql::recipe::schema_cookie);
        rows.get<0>()=actual.query_initialization_(cookie_recipe);
        if(!creator_current_())startup_refuse("inherited current-model cookie read");
        if(rows.get<0>().size()!=1||rows.get<0>().front().size()!=1)
            startup_refuse("current-model cookie shape differs");
        const auto* cookie=std::get_if<std::int64_t>(&rows.get<0>().front().begin()->second);
        if(!cookie||*cookie<0)startup_refuse("current-model cookie is not its actual nonnegative value");
        const auto observed_cookie=*cookie;
        const auto& catalog=initialization_catalog_(actual);
        if(swift_builder&&catalog.swift_fingerprint.empty())
            startup_refuse("current Swift builder lacks its actual captured fingerprint");
        // Preserve the actual builder's marker identity: the unchanged prune
        // recipe retains only this key, exactly as each ordinary builder does.
        const auto marker=swift_builder?std::string_view(catalog.swift_fingerprint):std::string_view(key.data(),key.size());
        initialize_fingerprint_marker_(owner,actual,marker,observed_cookie);
        if(!creator_current_())startup_refuse("inherited current-model marker return");
        require_model_catalog_present_(owner,actual);
}
std::array<char,35> observer_delivery_open_startup::fingerprint_marker_bytes_(lattice_db& owner,database& actual)const {
    try {
        require_work_current_(actual);owner.require_creator_process();
        const auto& common=*state_->startup->state_;
        if(state_->kind!=observer_delivery_startup::open_kind::writer||common.read_only||
           common.builder!=std::this_thread::get_id()||common.pending_owner!=&owner||common.actual_owner||
           owner.delivery_startup_.get()!=state_->startup.get()||owner.db_.get()!=&actual||
           common.catalog!=&owner.recovery_schemas_||!common.catalog||!common.catalog->valid()||
           common.schema_version!=owner.config_.target_schema_version||owner.closed_.load(std::memory_order_acquire))
            startup_refuse("marker producer requires its exact unpublished writable owner/catalog");
        // The same original serializer owns only allocator-aware character
        // buffers and a borrowed declaration-pointer vector in this actual
        // scope. Returned marker DATA is a fixed35-byte value, not an owning
        // standard-string conversion or a renewed initialization account.
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(this),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        ordinary_initialization_storage_scope::charge(native_retirement_storage<ordinary_initialization_text>::allocation_bytes());
        const auto creator=database_native_process();
        native_retirement_storage<ordinary_initialization_text> held;
        if(creator!=database_native_process())startup_refuse("inherited marker key custody");
        held.get<0>()=owner.compute_core_fingerprint_key_storage_<ordinary_initialization_text>();
        if(creator!=database_native_process())startup_refuse("inherited marker key producer return");
        require_work_current_(actual);
        if(held.get<0>().size()!=35)startup_refuse("actual fingerprint producer returned a noncanonical extent");
        std::array<char,35> result{};std::copy(held.get<0>().begin(),held.get<0>().end(),result.begin());return result;
    }catch(...) {fail_initialization_();throw;}
}
std::string observer_delivery_open_startup::fingerprint_marker_key_(lattice_db& owner,database& actual)const {
    // Kept for the existing private ordinary/component API. Actual Native
    // marker writes consume fixed borrowed bytes below, never this adapter.
    const auto key=fingerprint_marker_bytes_(owner,actual);
    return ordinary_initialization_legacy_text(std::string_view(key.data(),key.size()));
}

void observer_delivery_open_startup::initialize_fingerprint_marker_(lattice_db& owner,database& actual,
    std::string_view key,std::int64_t cookie)const {
    try {
        require_work_current_(actual);
        const auto creator=database_native_process();
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(this),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        using marker_custody=native_retirement_storage<std::array<char,35>,ordinary_initialization_text>;
        ordinary_initialization_storage_scope::charge(marker_custody::allocation_bytes());
        marker_custody held;
        if(creator!=database_native_process())startup_refuse("inherited marker producer custody");
        held.get<0>()=fingerprint_marker_bytes_(owner,actual);
        if(creator!=database_native_process())startup_refuse("inherited marker owner proof return");
        const auto& catalog=initialization_catalog_(actual);
        if(key!=std::string_view(held.get<0>().data(),held.get<0>().size())&&(catalog.swift_fingerprint.empty()||key!=catalog.swift_fingerprint))
            startup_refuse("marker key differs from both actual Core/Swift producers");
        if(cookie<0)startup_refuse("marker requires the actual nonnegative schema cookie");
        {
            auto issued=observer_delivery_initialization_sql::fixed_(*this,actual,
                observer_delivery_initialization_sql::recipe::schema_cookie);
            const auto rows=actual.query_initialization_(issued);
            if(creator!=database_native_process())startup_refuse("inherited marker cookie return");
            if(rows.size()!=1||rows.front().size()!=1)startup_refuse("marker schema cookie shape differs");
            const auto* observed=std::get_if<std::int64_t>(&rows.front().begin()->second);
            if(!observed||*observed!=cookie)startup_refuse("marker cookie changed before actual write");
        }
        held.get<1>()=ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(cookie));
        if(creator!=database_native_process())startup_refuse("inherited marker cookie copy");
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("marker producer lacks its actual immutable Plan");
        {
            auto store=plan->metadata_marker_recipe_(*this,actual,false,key,held.get<1>());
            actual.execute_initialization_(store);
        }
        require_work_current_(actual);
        // Every real return invalidates the previous attempt proof. Prune
        // reissues the SAME required-present family at the new revision.
        {
            auto prune=plan->metadata_marker_recipe_(*this,actual,true,key,held.get<1>());
            actual.execute_initialization_(prune);
        }
        require_work_current_(actual);
        // Genuine required-present POST-image, not a missing-tolerant catalog
        // inventory. It supplies NO full-model/ledger/seal/finish authority.
        auto post=plan->capture_preimage_(*this,actual,nullptr,
            observer_delivery_initialization_plan::preimage_selection::schema_metadata_marker_present);
        if(creator!=database_native_process())startup_refuse("inherited marker post-image return");
        require_metadata_write_(actual,post,metadata_preimage_phase::marker_write);
        auto read=observer_delivery_initialization_sql::metadata_read_(*this,actual,
            observer_delivery_initialization_sql::recipe::metadata_marker_read,key);
        const auto rows=actual.query_initialization_(read);
        if(creator!=database_native_process())startup_refuse("inherited marker value proof return");
        if(rows.size()!=1||rows.front().size()!=1)startup_refuse("marker value post-image shape differs");
        const auto* value=std::get_if<std::string>(&rows.front().at("value"));
        if(!value||*value!=held.get<1>())startup_refuse("marker value post-image differs from actual cookie");
        require_work_current_(actual);
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_metadata_write_(database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,metadata_preimage_phase phase)const {
    require_work_current_(owner);
    const auto current=[&] {
        if(!creator_current_())startup_refuse("inherited metadata WRITE return");
        owner.require_creator_();
        if(!proof||!proof->metadata_phase_for_(owner,phase))
            startup_refuse("metadata operator lost its actual same-BEGIN/revision phase proof");
        if(phase!=metadata_preimage_phase::create_table&&!proof->plain_table_for_(owner,"_lattice_meta"))
            startup_refuse("metadata write requires an actual strict present table");
    };
    current();store_physical_native_scope native(owner);
    if(!native.entered())startup_refuse("metadata WRITE Native original unavailable");
    database::catalog_native_admission_ leaf(owner);current();
    auto* actual=owner.internal_handle();if(!actual)startup_refuse("metadata WRITE database unavailable");
    const int autocommit=sqlite3_get_autocommit(actual);current();
    const int transaction=sqlite3_txn_state(actual,"main");current();
    if(autocommit||transaction!=SQLITE_TXN_WRITE)
        startup_refuse("metadata operator requires actual private bootstrap WRITE");
}
observer_delivery_initialization_sql observer_delivery_open_startup::sync_control_table_recipe_(database& owner)const {
    try {
        require_work_current_(owner);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("sync-control creation lacks its actual library Plan");
        return plan->sync_control_table_recipe_(*this,owner);
    }catch(...) {fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_open_startup::sync_control_initial_recipe_(database& owner)const {
    try {
        require_work_current_(owner);const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("sync-control seed lacks its actual library Plan");
        return plan->sync_control_initial_recipe_(*this,owner);
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_sync_control_write_(database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,sync_control_preimage_phase phase)const {
    require_work_current_(owner);
    const auto current=[&] {
        if(!creator_current_())startup_refuse("inherited sync-control WRITE return");
        owner.require_creator_();
        if(!proof||!proof->sync_control_phase_for_(owner,phase))
            startup_refuse("sync-control operator lost actual same-BEGIN/revision phase proof");
        if(phase==sync_control_preimage_phase::seed_row&&!proof->plain_table_for_(owner,"_SyncControl"))
            startup_refuse("sync-control seed requires actual strict present table");
    };
    current();store_physical_native_scope native(owner);
    if(!native.entered())startup_refuse("sync-control WRITE Native original unavailable");
    database::catalog_native_admission_ leaf(owner);current();
    auto* actual=owner.internal_handle();if(!actual)startup_refuse("sync-control WRITE database unavailable");
    const int autocommit=sqlite3_get_autocommit(actual);current();
    const int transaction=sqlite3_txn_state(actual,"main");current();
    if(autocommit||transaction!=SQLITE_TXN_WRITE)
        startup_refuse("sync-control operator requires actual private bootstrap WRITE");
}
void observer_delivery_open_startup::require_static_sync_write_(database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,static_sync_preimage_phase phase)const {
    require_work_current_(owner);
    const auto current=[&] {
        if(!creator_current_())startup_refuse("inherited static sync WRITE return");
        owner.require_creator_();
        if(!proof||!proof->static_sync_phase_for_(owner,phase))
            startup_refuse("static sync operator lost actual same-BEGIN/revision phase proof");
    };
    current();store_physical_native_scope native(owner);
    if(!native.entered())startup_refuse("static sync WRITE Native original unavailable");
    database::catalog_native_admission_ leaf(owner);current();
    auto* actual=owner.internal_handle();if(!actual)startup_refuse("static sync WRITE database unavailable");
    const int autocommit=sqlite3_get_autocommit(actual);current();
    const int transaction=sqlite3_txn_state(actual,"main");current();
    if(autocommit||transaction!=SQLITE_TXN_WRITE)startup_refuse("static sync operator requires actual private bootstrap WRITE");
}
void observer_delivery_open_startup::initialize_static_sync_(database& owner,static_sync_operation operation)const {
    try {
        require_work_current_(owner);
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("static sync initialization lacks actual immutable Plan");
        using recipe=observer_delivery_initialization_sql::recipe;
        using selection=observer_delivery_initialization_plan::preimage_selection;
        recipe id=recipe::sync_set;selection post=selection::static_sync_set_present;
        switch(operation) {
          case static_sync_operation::set_table:break;
          case static_sync_operation::state_table:id=recipe::sync_state;post=selection::static_sync_state_present;break;
          case static_sync_operation::state_pending_index:id=recipe::sync_state_pending;post=selection::static_sync_state_index_present;break;
          case static_sync_operation::slots_table:id=recipe::replication_slots;post=selection::static_sync_slots_present;break;
          case static_sync_operation::upload_floor:id=recipe::replication_upload_floor;post=selection::static_sync_upload_floor_present;break;
          default:startup_refuse("unknown closed static sync operation");
        }
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>>;
        claim_workspace_(owner,custody::allocation_bytes());custody held;
        if(!creator_current_())startup_refuse("inherited static sync procedure custody");
        if(operation==static_sync_operation::upload_floor) {
            held.get<0>()=plan->capture_preimage_(*this,owner,nullptr,selection::static_sync_upload_floor);
            if(!creator_current_())startup_refuse("inherited upload-floor preimage return");
            require_static_sync_write_(owner,held.get<0>(),static_sync_preimage_phase::upload_floor);
            if(held.get<0>()->static_sync_upload_current_for_(owner)) {
                // Strict current state is the original intended no-op. Never
                // issue a duplicate ALTER then erase its failed setup budget.
                held.get<0>()=plan->capture_preimage_(*this,owner,nullptr,post);
                if(!creator_current_())startup_refuse("inherited upload-floor no-op postimage return");
                return;
            }
        }
        auto actual=plan->static_sync_recipe_(*this,owner,id);
        owner.execute_initialization_(actual);
        if(!creator_current_())startup_refuse("inherited static sync effect return");
        held.get<0>()=plan->capture_preimage_(*this,owner,nullptr,post);
        if(!creator_current_())startup_refuse("inherited static sync postimage return");
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_current_static_sync_(database& owner,static_sync_observation observation)const {
    try {
        require_work_current_(owner);
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("current sync observation lacks actual immutable Plan");
        using selection=observer_delivery_initialization_plan::preimage_selection;
        selection selected=selection::static_sync_set_present;
        auto phase=static_sync_preimage_phase::set_present;
        switch(observation) {
          case static_sync_observation::per_sync_set:break;
          case static_sync_observation::cursor:selected=selection::static_sync_slots_present;phase=static_sync_preimage_phase::slots_present;break;
          default:startup_refuse("unknown closed current sync observation");
        }
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>>;
        claim_workspace_(owner,custody::allocation_bytes());custody held;
        if(!creator_current_())startup_refuse("inherited current sync observation custody");
        // Capture afresh after the preceding static producers. The immutable
        // current slots declaration includes the cursor in its original
        // physical position. A present NULL cursor is not a request to seed it.
        held.get<0>()=plan->capture_preimage_(*this,owner,nullptr,selected);
        if(!creator_current_())startup_refuse("inherited current sync observation return");
        require_work_current_(owner);
        if(!held.get<0>()||!held.get<0>()->static_sync_phase_for_(owner,phase))
            startup_refuse("current sync observation lost actual same-BEGIN/revision proof");
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::require_audit_write_(database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,audit_preimage_phase phase)const {
    require_work_current_(owner);
    const auto current=[&] {
        if(!creator_current_())startup_refuse("inherited AuditLog WRITE return");
        owner.require_creator_();
        if(!proof||!proof->audit_phase_for_(owner,phase))
            startup_refuse("AuditLog operator lost actual same-BEGIN/revision phase proof");
    };
    current();store_physical_native_scope native(owner);
    if(!native.entered())startup_refuse("AuditLog WRITE Native original unavailable");
    database::catalog_native_admission_ leaf(owner);current();
    auto* actual=owner.internal_handle();if(!actual)startup_refuse("AuditLog WRITE database unavailable");
    const int autocommit=sqlite3_get_autocommit(actual);current();
    const int transaction=sqlite3_txn_state(actual,"main");current();
    if(autocommit||transaction!=SQLITE_TXN_WRITE)startup_refuse("AuditLog operator requires actual private bootstrap WRITE");
}
void observer_delivery_open_startup::initialize_audit_(database& owner,audit_operation operation)const {
    try {
        require_work_current_(owner);
        const auto& plan=state_->startup->state_->plan;
        if(!plan)startup_refuse("AuditLog initialization lacks actual immutable Plan");
        using recipe=observer_delivery_initialization_sql::recipe;
        using selection=observer_delivery_initialization_plan::preimage_selection;
        recipe id=recipe::audit_table;selection post=selection::audit_present;
        switch(operation) {
          case audit_operation::table:break;
          case audit_operation::pending_index:id=recipe::audit_pending_index;post=selection::audit_pending_present;break;
          case audit_operation::lookup_index:id=recipe::audit_lookup_index;post=selection::audit_lookup_present;break;
          default:startup_refuse("unknown closed AuditLog operation");
        }
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>>;
        claim_workspace_(owner,custody::allocation_bytes());custody held;
        if(!creator_current_())startup_refuse("inherited AuditLog procedure custody");
        auto actual=plan->audit_recipe_(*this,owner,id);
        owner.execute_initialization_(actual);
        if(!creator_current_())startup_refuse("inherited AuditLog effect return");
        held.get<0>()=plan->capture_preimage_(*this,owner,nullptr,post);
        if(!creator_current_())startup_refuse("inherited AuditLog postimage return");
    }catch(...) {fail_initialization_();throw;}
}
void observer_delivery_open_startup::finish_open_(database& actual) {
    const auto issuer=database_native_process();
    // No inherited process_resources_/shared payload is touched before this
    // process-only fence. The real caller owns the stable wrapper/owner lifetime.
    if(creator_!=issuer||!state_||state_->creator!=issuer)
        startup_refuse("inherited per-open finish");
    struct error_custody final {
        using payload=std::array<std::exception_ptr,2>;const std::uint64_t creator;
        alignas(payload) std::byte bytes[sizeof(payload)];
        explicit error_custody(std::uint64_t value)noexcept:creator(value){::new(static_cast<void*>(bytes))payload{};}
        payload& get()noexcept{return *std::launder(reinterpret_cast<payload*>(bytes));}
        ~error_custody(){if(creator==database_native_process())get().~payload();}
    } errors(issuer);
    std::optional<database::initialization_transaction> transaction;
    try {
        require_work_current_(actual);
        auto& common=*state_->startup->state_;
        const bool initial=state_->kind==observer_delivery_startup::open_kind::writer||
            state_->kind==observer_delivery_startup::open_kind::reader||
            state_->kind==observer_delivery_startup::open_kind::cross_process_reader;
        const bool replacement=state_->kind==observer_delivery_startup::open_kind::replacement_writer||
            state_->kind==observer_delivery_startup::open_kind::replacement_reader||
            state_->kind==observer_delivery_startup::open_kind::replacement_cross_process_reader;
        if(!initial&&!replacement)startup_refuse("keeper is not a published owner role finish");
        auto* owner=initial?common.pending_owner:common.actual_owner;
        if(!owner)startup_refuse("per-open finish has no same actual bound owner");
        owner->require_creator_process();
        if(owner->delivery_startup_.get()!=state_->startup.get()||owner->closed_.load(std::memory_order_acquire)||
           !owner->recovery_schemas_owner_||owner->recovery_schemas_owner_.get()!=&owner->recovery_schemas_||
           !owner->recovery_schemas_.valid()||common.read_only!=owner->config_.read_only||
           common.schema_version!=owner->config_.target_schema_version||!common.plan||!common.read_profile||
           (initial&&(common.actual_owner||common.finished.load(std::memory_order_acquire)||
              common.catalog!=&owner->recovery_schemas_||common.builder!=state_->builder||
              state_->replacement||state_->budget.get()!=common.initial_budget.get()||
              state_->budget.owner_before(common.initial_budget)||common.initial_budget.owner_before(state_->budget)))||
           (replacement&&(!common.finished.load(std::memory_order_acquire)||common.catalog||common.pending_owner||
              !state_->replacement||state_->replacement->creator!=issuer||state_->replacement->owner!=owner||
              state_->replacement->startup.get()!=state_->startup.get()||state_->replacement->builder!=state_->builder||
              state_->replacement->end!=state_->policy.original_end_||state_->replacement->failed.load(std::memory_order_acquire)||
              state_->replacement->budget.get()!=state_->budget.get()||
              state_->replacement->budget.owner_before(state_->budget)||state_->budget.owner_before(state_->replacement->budget))))
            startup_refuse("per-open finish lost its actual owner/catalog/role/budget episode");
        const auto role=state_->kind;
        const bool writer=role==observer_delivery_startup::open_kind::writer||role==observer_delivery_startup::open_kind::replacement_writer;
        if(initial) {
            auto* retained=writer?owner->db_.get():role==observer_delivery_startup::open_kind::reader?
                owner->read_db_.get():owner->xproc_read_db_.get();
            if(retained!=&actual)startup_refuse("initial finish is not its actual retained role wrapper");
        }
        if(actual.mode_!=(writer&&!common.read_only?database::open_mode::read_write:database::open_mode::read_only))
            startup_refuse("per-open finish actual access mode differs from its role");
        // Enrolled triggers/retention/lineage are a DIFFERENT exact producer.
        // Normal schema proof cannot substitute for that required supplier.
        // This typed refusal stays until the actual retained continuous context
        // supplies its full programs/rows/counters on this same pending view.
        if(common.continuous_admission||owner->recovery_continuous_)
            startup_refuse("continuous final proof requires the actual enrolled-program/retention/lineage supplier");
        ordinary_initialization_storage_scope storage(this,&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        using custody=native_retirement_storage<std::shared_ptr<observer_delivery_sql_runtime>,
            std::shared_ptr<const database_sql_read_binding>,std::shared_ptr<const observer_delivery_initialization_plan>,
            ordinary_initialization_text>;
        ordinary_initialization_storage_scope::charge(custody::allocation_bytes());custody retained;
        if(issuer!=database_native_process())startup_refuse("inherited per-open final custody");
        retained.get<0>()=actual.observer_delivery_sql_;
        retained.get<1>()=std::atomic_load(&actual.sql_read_binding_);
        retained.get<2>()=common.plan;
        retained.get<3>()=owner->compute_core_fingerprint_key_storage_<ordinary_initialization_text>();
        if(issuer!=database_native_process())startup_refuse("inherited actual final catalog fingerprint return");
        if(retained.get<3>().size()!=35)startup_refuse("final actual Core marker extent differs");
        const auto pending=[&](bool sealed) {
            require_work_current_(actual);owner->require_creator_process();
            const auto binding=std::atomic_load(&actual.sql_read_binding_);
            const auto& profile=*common.read_profile;const auto& limits=common.policy.limits_;
            if(owner->closed_.load(std::memory_order_acquire)||owner->delivery_startup_.get()!=state_->startup.get()||
               (initial?common.pending_owner!=owner:common.actual_owner!=owner)||common.plan.get()!=retained.get<2>().get()||
               common.plan.owner_before(retained.get<2>())||retained.get<2>().owner_before(common.plan)||
               !binding||binding.get()!=retained.get<1>().get()||binding.owner_before(retained.get<1>())||
               retained.get<1>().owner_before(binding)||binding->creator_!=issuer||binding->owner_!=&actual||
               binding->profile_.get()!=common.read_profile.get()||binding->profile_.owner_before(common.read_profile)||
               common.read_profile.owner_before(binding->profile_)||profile.creator_!=issuer||profile.publisher_!=&common||
               profile.published_.load(std::memory_order_acquire)!=replacement||!profile.store_.same_domain(common.store)||
               profile.policy_.workspace_bytes!=limits.read_workspace_bytes||profile.policy_.columns!=limits.read_columns||
               profile.policy_.rows!=limits.read_rows||profile.policy_.maximum_duration!=std::chrono::milliseconds(limits.read_duration_ms)||
               profile.maximum_admission_wait_!=std::chrono::milliseconds(limits.admission_wait_ms)||
               !retained.get<0>()||actual.observer_delivery_sql_.get()!=retained.get<0>().get()||
               actual.observer_delivery_sql_.owner_before(retained.get<0>())||retained.get<0>().owner_before(actual.observer_delivery_sql_)||
               actual.raw_handle_escaped_.load(std::memory_order_acquire)||actual.engine_reads_||
               actual.initialization_callback_active_()||database::mutation_callback_active_()||
               actual.mutation_operation_unsettled_.load(std::memory_order_acquire)||actual.observer_policy_unsettled_())
                startup_refuse("per-open final custody/profile/policy differs from original pending binding");
            // Real receipt producer, never a function/module inventory-name
            // inference. Its checked registration dependency is separate and
            // must be selected; this family supplies no replacement stub.
            actual.require_initialization_registrations_(*this);
            auto* native=retained.get<0>()->native(actual);
            if(!native||native!=actual.db_||(!sealed&&!retained.get<0>()->pending_original_for_(actual,*this))||
               (sealed&&!retained.get<0>()->sealed_original_for_(actual,*this))||sqlite3_get_autocommit(native)==0||
               std::atomic_load(&actual.initialization_episode_))
                startup_refuse("per-open final Native runtime/ending differs from original");
            size_t count=0;
            for(auto* statement=sqlite3_next_stmt(native,nullptr);statement;statement=sqlite3_next_stmt(native,statement))
                if(++count>4096||sqlite3_stmt_busy(statement))
                    startup_refuse("per-open final Native corpus has busy or excess statements");
        };
        if(database::sql_entry_scope::active_for(&actual)||database::engine_query_scope::active_for(actual)||
           database::mutation_sql_scope::active_for(&actual)||actual.initialization_callback_active_())
            startup_refuse("per-open finish refuses inherited SQL/callback work");
        if(!common.store.current_for(actual))startup_refuse("per-open final physical store differs");
        {
            store_physical_native_scope physical(actual);if(!physical.entered())startup_refuse("final Native original unavailable");
            database::catalog_native_admission_ leaf(actual);pending(false);
        }
        using sql=observer_delivery_initialization_sql;using recipe=sql::recipe;
        // Real connection-local closed producer, before the final snapshot.
        // Metered REPLACE must not bypass its DELETE guard. No caller SQL or
        // inherited setting is interpreted as proof of this policy.
        auto recursive=sql::fixed_(*this,actual,recipe::connection_recursive_triggers_on);
        actual.execute_initialization_(recursive);
        require_work_current_(actual);
        auto begin=sql::fixed_(*this,actual,writer&&!common.read_only?
            recipe::schema_transaction_begin:recipe::schema_transaction_deferred_begin);
        auto commit=sql::fixed_(*this,actual,recipe::schema_transaction_commit);
        auto rollback=sql::fixed_(*this,actual,recipe::schema_transaction_rollback);
        transaction.emplace(actual.begin_initialization_(begin,commit,rollback,errors.get()[1]));
        if(issuer!=database_native_process())startup_refuse("inherited actual final snapshot BEGIN return");
        retained.get<2>()->install_final_meter_(*this,actual,owner->recovery_schemas_,common.schema_version,retained.get<3>());
        if(issuer!=database_native_process())startup_refuse("inherited actual finite meter installation return");
        retained.get<2>()->require_final_(*this,actual,owner->recovery_schemas_,common.schema_version,retained.get<3>());
        if(issuer!=database_native_process())startup_refuse("inherited actual complete final proof return");
        require_work_current_(actual);
        if(!receive_guard_initialization_reader::observe_final_pending_(actual))
            startup_refuse("final receive policy requires its actual pending reader");
        require_work_current_(actual);
        transaction->commit(); // the SAME actual privately owned ending, never a guessed autocommit
        if(issuer!=database_native_process())startup_refuse("inherited actual final snapshot COMMIT return");
        if(!common.store.current_for(actual))startup_refuse("final physical store changed after actual ending");
        {
            store_physical_native_scope physical(actual);if(!physical.entered())startup_refuse("final seal Native original unavailable");
            database::catalog_native_admission_ leaf(actual);pending(false);
            retained.get<0>()->seal_startup_(actual,*this); // actual dispatcher/runtime seal MUST precede finished
            if(issuer!=database_native_process())startup_refuse("inherited actual runtime seal return");
            pending(true);
        }
        // Return the actual catalog/physical Native leaves before any public
        // phase changes. Their real ending debt must remain a refusal; no
        // destructor after this point can publish or issue another SQL grant.
        require_work_current_(actual);owner->require_creator_process();
        if(actual.observer_policy_unsettled_()||actual.observer_delivery_sql_.get()!=retained.get<0>().get()||
           owner->closed_.load(std::memory_order_acquire)||owner->delivery_startup_.get()!=state_->startup.get())
            startup_refuse("per-open final seal return retained real policy/lifetime debt");
        state_->finished.store(true,std::memory_order_release); // no owner/profile/connection publication
    }catch(...) {
        if(issuer!=database_native_process())
            throw recovery_obligation_error(recovery_obligation_error_code::cleanup_failed,
                "inherited per-open final cleanup has unknown ending; parent error is inaccessible");
        errors.get()[0]=std::current_exception(); // BEFORE any same-process throwing health/cleanup
        fail_initialization_();
        if(transaction) {
            try {transaction->rollback();}
            catch(...) {
                if(issuer!=database_native_process())
                    throw recovery_obligation_error(recovery_obligation_error_code::cleanup_failed,
                        "inherited final rollback has unknown ending; parent errors are inaccessible");
                if(!errors.get()[1])errors.get()[1]=std::current_exception();
                (void)transaction->retire_failed_rollback_(); // actual failure debt, no second Native attempt
            }
        }
        if(issuer!=database_native_process())
            throw recovery_obligation_error(recovery_obligation_error_code::cleanup_failed,
                "inherited final cleanup return has unknown ending; parent errors are inaccessible");
        if(errors.get()[1])throw recovery_obligation_error(recovery_obligation_error_code::cleanup_failed,
            "actual per-open final proof/ending failed with retained cleanup debt",errors.get()[0],errors.get()[1]);
        std::rethrow_exception(errors.get()[0]);
    }
}
bool observer_delivery_open_startup::preimage_plain_table_for_(
    const database& owner,const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,
    std::string_view table)const noexcept {
    if(!creator_current_()||!proof||!pending_for_(owner))return false;
    return proof->plain_table_for_(owner,table);
}
bool observer_delivery_open_startup::preimage_shape_for_(const database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,
    std::string_view table,std::uint64_t& columns,bool& without_rowid)const noexcept {
    columns=0;without_rowid=false;
    if(!creator_current_()||!proof||!pending_for_(owner))return false;
    return proof->shape_for_(owner,table,columns,without_rowid);
}
bool observer_delivery_open_startup::preimage_system_metadata_for_(const database& owner,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& proof,
    std::string_view name)const noexcept {
    if(!creator_current_()||!proof||!pending_for_(owner))return false;
    return proof->system_metadata_for_(owner,name);
}
const void* observer_delivery_open_startup::catalog_generation_()const noexcept {
    return creator_==database_native_process()&&state_&&state_->creator==creator_&&state_->startup&&
        state_->startup->creator_==creator_?state_->startup->state_:nullptr;
}
bool observer_delivery_open_startup::public_sql_pending_for_(const database& owner)const noexcept {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||!owner.creator_current_())return true;
    // The slot is privately bound once before Native construction and never
    // replaced while this actual database is published. Only the atomic
    // terminal phase is consulted; failure/deadline cannot erase denial.
    return owner.delivery_startup_.get()!=this||!state_->finished.load(std::memory_order_acquire);
}
bool observer_delivery_open_startup::pending_for_(const database& owner)const noexcept {
    return creator_==database_native_process()&&state_&&state_->creator==creator_&&
        state_->builder==std::this_thread::get_id()&&state_->actual==&owner&&
        state_->opened.load(std::memory_order_acquire)&&!state_->finished.load(std::memory_order_acquire)&&
        !state_->failed.load(std::memory_order_acquire)&&state_->startup&&
        state_->startup->creator_==creator_&&state_->startup->state_&&
        !state_->startup->state_->failed.load(std::memory_order_acquire)&&
        state_->budget&&state_->budget->usable_()&&owner.creator_current_()&&
        !owner.closed_.load(std::memory_order_acquire)&&owner.delivery_startup_.get()==this&&
        owner.delivery_initialization_open_current_(this)&&
        std::chrono::steady_clock::now()<state_->policy.original_end_;
}
// The remaining methods are deliberately not stubbed: this mutable source
// family cannot be enrolled or built as a complete activation until actual
// early-runtime, closed statement, finalization, owner and replacement wiring
// below are implemented and independently reviewed.
}

namespace lattice {
bool database::initialization_callback_active_()const noexcept {
    // Denial only. Foreign entry must not inspect the actual callback cookie
    // or the thread's inherited callback frame chain.
    if(!creator_current_())return true;
    return detail::database_sql_dispatcher::callback_active_for_(*this);
}
std::shared_ptr<const detail::database_initialization_episode>
database::capture_initialization_snapshot_(const detail::observer_delivery_open_startup& open)const {
    require_creator_();
    if(!open.pending_for_(*this))throw db_error("initialization snapshot requires its pending actual open");
    auto actual=std::atomic_load(&initialization_episode_);
    if(!actual||!initialization_snapshot_current_(*actual,open))
        throw db_error("initialization snapshot requires its actual live private BEGIN");
    return actual; // pure DATA; caller pins it in creator-fenced heap before SQL
}
bool database::initialization_snapshot_current_(const detail::database_initialization_episode& episode,
    const detail::observer_delivery_open_startup& open)const noexcept {
    if(!creator_current_()||episode.creator_!=detail::database_native_process()||
       episode.owner_!=this||episode.open_.get()!=&open||!open.pending_for_(*this)||
       !episode.live_.load(std::memory_order_acquire)||episode.ended_.load(std::memory_order_acquire))return false;
    const auto actual=std::atomic_load(&initialization_episode_);
    return actual&&actual.get()==&episode;
}
bool database::initialization_schema_revision_(const detail::database_initialization_episode& episode,
    const detail::observer_delivery_open_startup& open,std::uint64_t& result)const noexcept {
    result=0;
    if(!initialization_snapshot_current_(episode,open))return false;
    const auto observed=episode.schema_revision_.load(std::memory_order_acquire);
    if(observed==UINT64_MAX)return false;
    result=observed;return true;
}
}
