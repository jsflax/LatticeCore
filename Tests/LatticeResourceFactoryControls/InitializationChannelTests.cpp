#include <gtest/gtest.h>
#include <lattice/lattice.hpp>
#include "ActualModelProducerTestAccess.hpp"
#include <lattice/detail/observer_delivery_startup.hpp>
#include "../../Sources/LatticeCore/src/database_initialization_budget.hpp"
#include "../../Sources/LatticeCore/src/ordinary_schema_validation.hpp"
#include "../../Sources/LatticeCore/src/database_initialization_test_probe.hpp"
#include "../../Sources/LatticeCore/src/receive_delivery_guard.hpp"
#include "../../Sources/LatticeCore/src/database_sql_dispatcher.hpp"
#include "../../Sources/LatticeCore/src/database_sql_request.hpp"
#include <array>
#include <chrono>
#include <stdexcept>
#include <new>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#if defined(__APPLE__) || defined(__linux__)
#include <pwd.h>
#include <unistd.h>
#endif

namespace lattice::detail {
struct database_initialization_budget_test_access {
    using recipe=observer_delivery_initialization_sql::recipe;
    static auto& budget(database& actual) {
        actual.require_creator_();
        if(!actual.delivery_startup_)throw db_error("genuine installed initialization is required");
        return *actual.delivery_startup_->initialization_budget_(actual);
    }
    static auto inspect(database& actual){return budget(actual).inspect_for_test_();}
    static auto fixed(database& actual,recipe id) {
        actual.require_creator_();
        if(!actual.delivery_startup_)throw db_error("actual startup open is absent");
        return observer_delivery_initialization_sql::fixed_(*actual.delivery_startup_,actual,id);
    }
    static auto count(database& actual) {
        auto descriptor=fixed(actual,recipe::connection_schema_count);
        return actual.query_initialization_(descriptor);
    }
    static auto begin(database& actual) {
        auto start=fixed(actual,recipe::schema_transaction_begin);
        auto commit=fixed(actual,recipe::schema_transaction_commit);
        auto rollback=fixed(actual,recipe::schema_transaction_rollback);
        return actual.begin_initialization_(start,commit,rollback);
    }
    static bool unreserved_begin_refuses(database& actual) {
        auto descriptor=fixed(actual,recipe::schema_transaction_begin);
        try{actual.execute_initialization_(descriptor);}catch(const db_error&){return true;}
        return false;
    }
    static bool autocommit(database& actual) {
        actual.require_creator_();database::catalog_native_admission_ admitted(actual);
        return sqlite3_get_autocommit(actual.db_)!=0;
    }
    static bool has_core_original(database& actual) {
        actual.require_creator_();database::catalog_native_admission_ admitted(actual);
        return bool(actual.core_transaction_original_);
    }
    struct unrequested_prepare_result {
        int prepare_rc=SQLITE_MISUSE;
        int cleanup_rc=SQLITE_OK;
        bool statement_absent=false;
    };
    static unrequested_prepare_result prepare_without_request(database& actual) {
        actual.require_creator_();
        unrequested_prepare_result result;
        {
            database::catalog_native_admission_ admitted(actual);
            sqlite3_stmt* statement=nullptr;
            // Actual same-open Native frame, deliberately no Request/binding.
            result.prepare_rc=store_physical_prepare_v2(actual.db_,"SELECT 1",-1,&statement,nullptr);
            result.statement_absent=statement==nullptr;
            if(statement)result.cleanup_rc=store_physical_finalize(statement);
        }
        // Unexpected success must still clean its actual statement. A nonOK
        // wrapper return cannot prove that cleanup consumed it; retain debt.
        if(result.cleanup_rc!=SQLITE_OK) {
            std::fprintf(stderr,"initialization unrequested-prepare cleanup debt rc=%d; namespace retained\n",result.cleanup_rc);
            std::_Exit(96);
        }
        return result;
    }
    static std::pair<const void*,std::uint64_t> schema_revision(database& actual) {
        actual.require_creator_();
        auto episode=std::atomic_load(&actual.initialization_episode_);
        if(!episode||episode->creator_!=database_native_process()||episode->owner_!=&actual||
           !episode->open_||!episode->budget_||!episode->budget_->current_()||
           !episode->live_.load(std::memory_order_acquire)||episode->ended_.load(std::memory_order_acquire))
            throw db_error("revision observation lacks its actual live private episode");
        return {episode.get(),episode->schema_revision_.load(std::memory_order_acquire)};
    }
    static auto metadata_table_recipe(database& actual) {
        actual.require_creator_();
        if(!actual.delivery_startup_)throw db_error("actual startup open is absent");
        return actual.delivery_startup_->metadata_table_recipe_(actual);
    }
    static void execute_metadata_table(database& actual,const observer_delivery_initialization_sql& entry) {
        actual.execute_initialization_(entry);
    }
    static void exhaust_schema_revision_data_fault(database& actual) {
        (void)schema_revision(actual);
        auto episode=std::atomic_load(&actual.initialization_episode_);
        episode->schema_revision_.store(UINT64_MAX,std::memory_order_release);
    }
    static bool actual_policy_debt(database& actual) {
        actual.require_creator_();return actual.observer_policy_unsettled_();
    }
    static std::int64_t actual_schema_count(database& actual) {
        const auto rows=count(actual);
        if(rows.size()!=1||rows.front().size()!=1||
           !std::holds_alternative<std::int64_t>(rows.front().begin()->second))
            throw db_error("actual fixed schema count has unsupported result");
        return std::get<std::int64_t>(rows.front().begin()->second);
    }
    static std::int64_t actual_change_count(database& actual) {
        actual.require_creator_();database::catalog_native_admission_ admitted(actual);
        const auto count=sqlite3_total_changes64(actual.db_);actual.require_creator_();return count;
    }
    static int set_receive_test_role(database& actual,
        int(*callback)(void*,int,const char*,const char*,const char*,const char*)noexcept,void* data)noexcept {
        return actual.set_sql_authorizer_(database_authorizer_slot::owned_test_restriction,callback,data);
    }
    static int clear_receive_test_role(database& actual,void* data)noexcept {
        return actual.clear_sql_authorizer_(database_authorizer_slot::owned_test_restriction,data);
    }
    struct old_vm_result {
        bool prepared=false,identity_retained=false,work_refused=false,adoption_refused=false,
             step_refused=false,never_ran=false,finalized=false,successor_unchanged=false,
             old_revision_only=false,successor_rollback_ended=false;
    };
    static old_vm_result old_vm_cannot_enter_successor(database& actual) {
        old_vm_result result;
        auto first=begin(actual);
        const auto old=schema_revision(actual);
        // Comparison/observation only. The genuine first transaction and
        // armed Request both retain this exact episode through the block.
        auto* old_episode=actual.initialization_episode_.get();
        auto descriptor=metadata_table_recipe(actual); // actual closed Plan/preimage
        database_sql_request request(actual,descriptor);
        {
            database::sql_entry_scope entry(actual);
            database::engine_query_scope issued(actual,false,&request);
            request.arm_initialization_schema_attempt_();
            // SAME production helper: real statement count, Request/node,
            // actual private prepare/adopt, tail and parameter-copy ordering.
            auto* statement=actual.prepare_initialization_statement_(descriptor,request,issued);
            result.prepared=statement&&request.matches(statement);
            first.commit();
            auto successor=begin(actual);
            const auto next=schema_revision(actual);
            result.identity_retained=request.current(); // cleanup identity, not work
            try{request.check();}catch(const db_error&){result.work_refused=true;}
            try{request.adopt(statement);}catch(const db_error&){result.adoption_refused=true;}
            try{(void)actual.step_initialization_(statement,request,{},false,false);}
            catch(const db_error&){result.step_refused=true;}
            {
                actual.require_creator_();database::catalog_native_admission_ admitted(actual);
                // RUN increments on the first sqlite3_step of a cycle. This
                // reads the real retained VM; it does not reset its counters.
                result.never_ran=sqlite3_stmt_status(statement,SQLITE_STMTSTATUS_RUN,0)==0;
                actual.require_creator_();
            }
            issued.reset();
            result.finalized=!issued.statement&&!issued.active&&issued.finalization_result==SQLITE_OK;
            const bool retired=request.retire_initialization_schema_attempt_();
            const auto after=schema_revision(actual);
            result.successor_unchanged=old.first!=next.first&&next==after&&after.second==0&&!autocommit(actual);
            // The old attempt retires its captured original, never the new
            // owner slot. Observe both genuine episodes, not just a bool.
            actual.require_creator_();
            result.old_revision_only=retired&&old_episode&&
                old_episode->schema_revision_.load(std::memory_order_acquire)==old.second+1;
            successor.rollback();result.successor_rollback_ended=autocommit(actual);
            entry.release();
        }
        return result;
    }
    // Actual existing budget DATA only; no fabricated account/profile/Open.
    static auto retain_account_budget(database& actual) {
        actual.require_creator_();
        if(!actual.delivery_startup_)throw db_error("actual startup open is absent");
        return actual.delivery_startup_->initialization_budget_(actual);
    }
    static void account_workspace(database& actual,std::uint64_t bytes) {budget(actual).claim_workspace_(bytes);}
    static void account_statements(database& actual,std::uint64_t count) {budget(actual).claim_statements_(count);}
    static auto normal_factory_builder(store_resource_factory_admission& actual){return actual.take_for_builder_();}
    static bool normal_transport_current(const store_resource_initialization_original& actual){return actual.transport_current_();}
    static const void* normal_payload_identity(const store_resource_initialization_original& actual){
        if(!actual.transport_current_())throw db_error("actual normal original is not current");return actual.impl_;
    }
    static const void* normal_owned_payload_identity(database& actual){actual.require_creator_();return actual.normal_initialization_original_.impl_;}
    static bool actual_normal_fresh(database& actual){
        actual.require_creator_();if(!actual.delivery_startup_)throw db_error("actual pending normal profile required");
        return actual.normal_initialization_original_.fresh_for_(actual,*actual.delivery_startup_)==&actual.normal_initialization_original_;
    }
    static auto retained_marker_budget_data(const database_initialization_budget& retained) {return retained.inspect_for_test_();}
    static bool actual_metadata_absent(database& actual) {
        auto issued=fixed(actual,recipe::metadata_table_discovery);return actual.query_initialization_(issued).empty();
    }
    static void ensure_marker_metadata(database& actual) {
        auto create=actual.delivery_startup_->metadata_table_recipe_(actual);actual.execute_initialization_(create);
        auto seed=actual.delivery_startup_->metadata_initial_version_recipe_(actual);actual.execute_initialization_(seed);
    }
    static std::string actual_marker_key(lattice_db& owner) {
        auto& actual=owner.db();return actual.delivery_startup_->fingerprint_marker_key_(owner,actual);
    }
    static std::int64_t actual_cookie(database& actual) {
        auto issued=fixed(actual,recipe::schema_cookie);const auto rows=actual.query_initialization_(issued);
        if(rows.size()!=1||rows.front().size()!=1||!std::holds_alternative<std::int64_t>(rows.front().begin()->second))
            throw db_error("actual marker control cookie shape differs");
        return std::get<std::int64_t>(rows.front().begin()->second);
    }
    static void store_actual_marker(lattice_db& owner,std::string_view key,std::int64_t cookie) {
        auto& actual=owner.db();actual.delivery_startup_->initialize_fingerprint_marker_(owner,actual,key,cookie);
    }
    static std::string actual_metadata_value(database& actual,recipe id,std::string_view key={}) {
        auto issued=observer_delivery_initialization_sql::metadata_read_(*actual.delivery_startup_,actual,id,key);
        const auto rows=actual.query_initialization_(issued);
        if(rows.size()!=1||rows.front().size()!=1||!std::holds_alternative<std::string>(rows.front().at("value")))
            throw db_error("actual marker control value shape differs");
        return std::get<std::string>(rows.front().at("value"));
    }
    static void model_machinery(database& actual) {
        ensure_marker_metadata(actual);
        auto create=actual.delivery_startup_->sync_control_table_recipe_(actual);actual.execute_initialization_(create);
        auto seed=actual.delivery_startup_->sync_control_initial_recipe_(actual);actual.execute_initialization_(seed);
        using operation=observer_delivery_open_startup::audit_operation;
        for(const auto step:{operation::table,operation::pending_index,operation::lookup_index})
            actual.delivery_startup_->initialize_audit_(actual,step);
    }
    static void setup_actual_native_models(lattice_db& owner) {
        auto& actual=owner.db();
        const auto& catalog=actual.delivery_startup_->initialization_catalog_(actual);
        if(!catalog.valid())throw db_error("actual model control lacks retained catalog");
        for(const auto& [name,model]:catalog.models)if(!catalog.swift_models.count(name)) {
            lattice::observer_delivery_custody_test_access::create_model(owner,model);owner.ensure_unique_property_indexes(model);
        }
    }
    static bool model_present(lattice_db& owner,std::string_view name) {
        auto& actual=owner.db();return actual.delivery_startup_->model_table_present_(owner,actual,name);
    }
    static auto model_columns(lattice_db& owner,std::string_view name) {
        auto& actual=owner.db();return actual.delivery_startup_->model_table_info_(owner,actual,name);
    }
    static void model_complete(lattice_db& owner) {
        auto& actual=owner.db();actual.delivery_startup_->require_model_catalog_present_(owner,actual);
    }
    static void unknown_model_program(lattice_db& owner) {
        auto& actual=owner.db();actual.delivery_startup_->initialize_model_recipe_(owner,actual,
            "CREATE TABLE startup_not_in_catalog(value TEXT)");
    }
    static void exhaust_statement_budget_data_fault(database& actual) {
        // Explicit DATA fault: the following real request must refuse before
        // prepare, while its genuine preissued fallback ROLLBACK remains usable.
        auto& held=budget(actual);const auto data=held.inspect_for_test_();
        held.claim_statements_(data.maximum.statements-data.statements);
    }
};
}
namespace {
using access=lattice::detail::database_initialization_budget_test_access;
using probe=lattice::detail::database_initialization_test_probe;
struct stopped_before_publication {};
enum class mode { fixed_and_public,exhaustion_rollback,successor,unreserved,three_opens,allocation_request,allocation_transaction,allocation_episode };
struct observation {
    mode operation;
    std::uint64_t creator;
    unsigned pending=0,ready=0;
    std::array<const void*,3> identities{};
    std::array<std::chrono::steady_clock::time_point,3> ends{};
    bool count=false,public_refused=false,monotonic=false,raw_has_no_core=false,
        work_refused=false,rollback_ended=false,old_cannot_end_new=false,unreserved_refused=false,
        allocation_refused=false,allocation_sticky=false,no_fabricated_native_error=false;
};
thread_local observation* selected=nullptr;
void pending(lattice::database& actual) {
    if(!selected||selected->creator!=lattice::detail::database_native_process())return;
    const auto data=access::inspect(actual);const auto index=selected->pending++;
    if(index>=selected->identities.size())throw std::runtime_error("initial three-open fixture overflow");
    selected->identities[index]=data.identity;selected->ends[index]=data.deadline;
    // DATA only: no SQL may precede vec0 registration/schema-load ordering.
}
void ready(lattice::database& actual) {
    if(!selected||selected->creator!=lattice::detail::database_native_process())return;
    ++selected->ready;
    if(selected->operation==mode::three_opens)return;
    if(selected->ready!=1)throw std::runtime_error("single-open control unexpectedly continued");
    const auto before=access::inspect(actual);
    switch(selected->operation) {
      case mode::fixed_and_public: {
        const auto rows=access::count(actual);
        selected->count=rows.size()==1&&rows.front().size()==1&&
            std::holds_alternative<std::int64_t>(rows.front().begin()->second);
        const auto after=access::inspect(actual);
        selected->monotonic=before.identity==after.identity&&before.deadline==after.deadline&&
            after.statements>before.statements&&after.workspace>before.workspace;
        try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){selected->public_refused=true;}
        break;
      }
      case mode::exhaustion_rollback: {
        auto txn=access::begin(actual);
        selected->raw_has_no_core=!access::autocommit(actual)&&!access::has_core_original(actual);
        access::exhaust_statement_budget_data_fault(actual);
        try{(void)access::count(actual);}catch(const lattice::db_error&){selected->work_refused=true;}
        txn.rollback();
        selected->rollback_ended=access::autocommit(actual);
        // DATA snapshot is intentionally not read through healthy accessors
        // after failure. No counter reset, deadline renewal or fresh lease.
        break;
      }
      case mode::successor: {
        auto first=access::begin(actual);first.commit();
        auto second=access::begin(actual);
        first.rollback(); // Known-ended rollback is a no-op on its own receipt.
        selected->old_cannot_end_new=!access::autocommit(actual);
        second.rollback();selected->rollback_ended=access::autocommit(actual);
        break;
      }
      case mode::unreserved:
        selected->unreserved_refused=access::unreserved_begin_refuses(actual);
        selected->rollback_ended=access::autocommit(actual);break;
      case mode::allocation_request: {
        auto txn=access::begin(actual);
        probe::fail_once=probe::allocation_stage::request;
        try{(void)access::count(actual);}catch(const std::bad_alloc&){selected->allocation_refused=true;}
        const auto data=access::inspect(actual);selected->allocation_sticky=data.failed;
        selected->no_fabricated_native_error=data.first_api_rc==0&&data.cleanup_api_rc==0;
        txn.rollback();selected->rollback_ended=access::autocommit(actual);break;
      }
      case mode::allocation_transaction:case mode::allocation_episode: {
        probe::fail_once=selected->operation==mode::allocation_transaction?
            probe::allocation_stage::transaction:probe::allocation_stage::episode;
        try{auto txn=access::begin(actual);}catch(const std::bad_alloc&){selected->allocation_refused=true;}
        const auto data=access::inspect(actual);selected->allocation_sticky=data.failed;
        selected->no_fabricated_native_error=data.first_api_rc==0&&data.cleanup_api_rc==0;
        selected->rollback_ended=access::autocommit(actual);break;
      }
      case mode::three_opens:break;
    }
    // This deliberate stop prevents a successful channel control from being
    // mislabeled full owner/schema/profile activation. Real constructor close
    // and actual Host/physical-original teardown still run on this exception.
    throw stopped_before_publication{};
}
struct installed_probe {
    observation& data;
    explicit installed_probe(observation& value):data(value) {
        if(selected||probe::after_pending||probe::after_functions)
            throw std::runtime_error("initialization probe is already selected");
        selected=&data;probe::after_pending=pending;probe::after_functions=ready;
    }
    ~installed_probe() {
        probe::fail_once=probe::allocation_stage::none;
        probe::after_functions=nullptr;probe::after_pending=nullptr;selected=nullptr;
    }
};
std::string actual_logical_root() {
#if defined(__APPLE__) || defined(__linux__)
    struct passwd data{},*found=nullptr;char bytes[16384];
    if(::getpwuid_r(::geteuid(),&data,bytes,sizeof(bytes),&found)!=0||!found||!found->pw_dir||found->pw_dir[0]!='/')
        throw std::runtime_error("actual account home unavailable");
    return std::string(found->pw_dir)+"/.claude/memory.sqlite";
#else
    throw std::runtime_error("actual hosted factory is unsupported");
#endif
}
void construct_actual_owner() {
    // SAME normal issuer/image/root path as the genuine factory component.
    // Missing installation/setup policy is failure, never legacy/fake enable.
    lattice::configuration config(actual_logical_root());lattice::lattice_db actual(config,true);
}
void run_stopped(observation& data) {
    installed_probe hold(data);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(data.pending,1u);ASSERT_EQ(data.ready,1u);
    ASSERT_NE(data.identities[0],nullptr);
}
}
TEST(InitializationChannel, GenuinePendingFixedEntryUsesSameBudgetAndPublicSQLGetsNoPurpose) {
    observation data{mode::fixed_and_public,lattice::detail::database_native_process()};
    run_stopped(data);EXPECT_TRUE(data.count);EXPECT_TRUE(data.monotonic);EXPECT_TRUE(data.public_refused);
}
TEST(InitializationChannel, ActualReservedRawRollbackStillEndsAfterStatementBudgetDataFault) {
    observation data{mode::exhaustion_rollback,lattice::detail::database_native_process()};
    run_stopped(data);EXPECT_TRUE(data.raw_has_no_core);EXPECT_TRUE(data.work_refused);EXPECT_TRUE(data.rollback_ended);
}
TEST(InitializationChannel, KnownEndedReceiptCannotRollbackActualSuccessor) {
    observation data{mode::successor,lattice::detail::database_native_process()};
    run_stopped(data);EXPECT_TRUE(data.old_cannot_end_new);EXPECT_TRUE(data.rollback_ended);
}
TEST(InitializationChannel, FixedBEGINCannotBypassPreissuedEndingReservation) {
    observation data{mode::unreserved,lattice::detail::database_native_process()};
    run_stopped(data);EXPECT_TRUE(data.unreserved_refused);EXPECT_TRUE(data.rollback_ended);
}
TEST(InitializationChannel, GenuineInitialThreeOpensKeepOneBudgetAndOneOriginalEnd) {
    observation data{mode::three_opens,lattice::detail::database_native_process()};
    installed_probe hold(data);construct_actual_owner();
    ASSERT_EQ(data.pending,3u);ASSERT_EQ(data.ready,3u);ASSERT_NE(data.identities[0],nullptr);
    EXPECT_EQ(data.identities[1],data.identities[0]);EXPECT_EQ(data.identities[2],data.identities[0]);
    EXPECT_EQ(data.ends[1],data.ends[0]);EXPECT_EQ(data.ends[2],data.ends[0]);
}

TEST(InitializationChannel, CaughtPostBudgetAllocationFaultsStayFailedWithoutInventingNativeErrors) {
    for(const auto stage:{mode::allocation_request,mode::allocation_transaction,mode::allocation_episode}) {
        observation data{stage,lattice::detail::database_native_process()};
        run_stopped(data);EXPECT_TRUE(data.allocation_refused);EXPECT_TRUE(data.allocation_sticky);
        EXPECT_TRUE(data.no_fabricated_native_error);EXPECT_TRUE(data.rollback_ended);
    }
}

namespace {
struct unrequested_prepare_observation {
    const std::uint64_t creator=lattice::detail::database_native_process();
    unsigned calls=0;
    access::unrequested_prepare_result result;
};
thread_local unrequested_prepare_observation* unrequested_selected=nullptr;
void prepare_unrequested_after_functions(lattice::database& actual) {
    if(!unrequested_selected||unrequested_selected->creator!=lattice::detail::database_native_process())return;
    ++unrequested_selected->calls;
    unrequested_selected->result=access::prepare_without_request(actual);
    throw stopped_before_publication{};
}
struct unrequested_prepare_probe {
    explicit unrequested_prepare_probe(unrequested_prepare_observation& value) {
        if(selected||unrequested_selected||probe::after_pending||probe::after_functions)
            throw std::runtime_error("initialization probe is already selected");
        unrequested_selected=&value;probe::after_functions=prepare_unrequested_after_functions;
    }
    ~unrequested_prepare_probe() {
        probe::after_functions=nullptr;unrequested_selected=nullptr;
    }
};
}
TEST(InitializationChannel, ActualPendingAuthorizerDeniesUnrequestedPrepareBeforeVMExists) {
    unrequested_prepare_observation data;
    unrequested_prepare_probe hold(data);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(data.calls,1u);
    EXPECT_EQ(data.result.prepare_rc,SQLITE_AUTH);
    EXPECT_TRUE(data.result.statement_absent);
    EXPECT_EQ(data.result.cleanup_rc,SQLITE_OK);
}

namespace {
struct public_entry_observation {
    const std::uint64_t creator=lattice::detail::database_native_process();
    unsigned admissions=0,calls=0;
    bool refused=false;
};
thread_local public_entry_observation* public_entry_selected=nullptr;
void public_entry_after_functions(lattice::database& actual) {
    if(!public_entry_selected||public_entry_selected->creator!=lattice::detail::database_native_process())return;
    ++public_entry_selected->calls;
    try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){public_entry_selected->refused=true;}
    throw stopped_before_publication{};
}
struct public_entry_probe {
    explicit public_entry_probe(public_entry_observation& value) {
        if(selected||unrequested_selected||public_entry_selected||probe::after_pending||
           probe::after_functions||probe::read_admission_attempts)
            throw std::runtime_error("initialization probe is already selected");
        public_entry_selected=&value;probe::read_admission_attempts=&value.admissions;
        probe::after_functions=public_entry_after_functions;
    }
    ~public_entry_probe() {
        probe::after_functions=nullptr;probe::read_admission_attempts=nullptr;public_entry_selected=nullptr;
    }
};
}
TEST(InitializationChannel, PublicPendingQueryRefusesBeforeAnyFreshReadAdmissionAttempt) {
    public_entry_observation data;
    public_entry_probe hold(data);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(data.calls,1u);
    EXPECT_TRUE(data.refused);EXPECT_EQ(data.admissions,0u);
}


#include "../../Sources/LatticeCore/src/recovery_obligation_bootstrap_statement.hpp"
#include "../../Sources/LatticeCore/src/recovery_obligation_producer.hpp"
namespace lattice::detail {
struct recovery_obligation_bootstrap_test_access {
    struct observed {bool callback=false,snapshot=false,public_refused=false,ended=false,sticky=false;unsigned public_admissions=99;};
    struct validator_failure {};
    static observed run(database& actual,bool fail_validator) {
        constexpr recovery_obligation_producer_discovery_limits caps{
            {16,100000,4096,67108864},{16,4096,1048576},{16,100000,4096,1048576,67108864}};
        auto reader=recovery_obligation_bootstrap_reader::create_(actual,caps,false);
        if(!reader)throw db_error("actual Host-selected bootstrap reader missing");
        observed data;
        // The actual constructor/after_functions caller retains this database
        // for the entire synchronous call. This no-op alias only supplies the
        // old store API's value parameter; it issues no owner/open authority.
        auto borrowed=std::shared_ptr<database>(&actual,[](database*)noexcept{});
        try {
            recovery_obligation_producer_store::bootstrap_profiles(borrowed,caps,
                [&](database& view,const recovery_obligation_producer_inventory&) {
                    data.callback=&view==&actual;
                    data.snapshot=!database_initialization_budget_test_access::autocommit(view)&&
                        !database_initialization_budget_test_access::has_core_original(view);
                    unsigned admissions=0;
                    struct counter_guard {
                        unsigned* previous;
                        explicit counter_guard(unsigned& value):previous(database_initialization_test_probe::read_admission_attempts) {
                            database_initialization_test_probe::read_admission_attempts=&value;
                        }
                        ~counter_guard(){database_initialization_test_probe::read_admission_attempts=previous;}
                    } observe(admissions);
                    try{(void)view.query("SELECT 1");}catch(const db_error&){data.public_refused=true;}
                    data.public_admissions=admissions;
                    if(fail_validator)throw validator_failure{};
                },false,reader.get());
        }catch(const validator_failure&) {
            if(!fail_validator)throw;
            data.sticky=database_initialization_budget_test_access::inspect(actual).failed;
        }
        data.ended=database_initialization_budget_test_access::autocommit(actual);
        return data;
    }
};
}
namespace {
struct bootstrap_observation {
    std::uint64_t creator=lattice::detail::database_native_process();
    bool fail=false,visited=false;
    lattice::detail::recovery_obligation_bootstrap_test_access::observed facts;
};
thread_local bootstrap_observation* bootstrap_selected=nullptr;
void bootstrap_ready(lattice::database& actual) {
    if(!bootstrap_selected||bootstrap_selected->creator!=lattice::detail::database_native_process())return;
    bootstrap_selected->visited=true;
    bootstrap_selected->facts=lattice::detail::recovery_obligation_bootstrap_test_access::run(actual,bootstrap_selected->fail);
    throw stopped_before_publication{};
}
void run_bootstrap_control(bootstrap_observation& data) {
    ASSERT_EQ(probe::after_functions,nullptr);ASSERT_EQ(bootstrap_selected,nullptr);
    struct selected_control {
        explicit selected_control(bootstrap_observation& data){bootstrap_selected=&data;probe::after_functions=bootstrap_ready;}
        ~selected_control(){probe::after_functions=nullptr;bootstrap_selected=nullptr;}
    } selected(data);
    bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_TRUE(data.visited);
}
}
TEST(InitializationBootstrap, ActualStoreSnapshotSettlesUsingExactPrivateBootstrapRecipes) {
    bootstrap_observation data;run_bootstrap_control(data);
    EXPECT_TRUE(data.facts.callback);EXPECT_TRUE(data.facts.snapshot);EXPECT_TRUE(data.facts.ended);
}
TEST(InitializationBootstrap, GenericValidatorPublicQueryCannotBorrowBootstrapPurposeOrAdmission) {
    bootstrap_observation data;run_bootstrap_control(data);
    EXPECT_TRUE(data.facts.callback);EXPECT_TRUE(data.facts.public_refused);
    EXPECT_EQ(data.facts.public_admissions,0u);EXPECT_TRUE(data.facts.ended);
}
TEST(InitializationBootstrap, ActualValidatorExceptionKeepsFailureStickyAndUsesPrepaidRollback) {
    bootstrap_observation data;data.fail=true;run_bootstrap_control(data);
    EXPECT_TRUE(data.facts.callback);EXPECT_TRUE(data.facts.snapshot);EXPECT_TRUE(data.facts.sticky);
    EXPECT_TRUE(data.facts.ended);
}

namespace {
enum class schema_operation {fixed_read,plan_attempt,overflow};
struct schema_observation {
    const std::uint64_t creator=lattice::detail::database_native_process();
    schema_operation operation;
    unsigned calls=0;
    bool read_unchanged=false,attempt_invalidated=false,same_original=false,
         refusal=false,sticky=false,policy_debt=false,no_fabricated_error=false,
         no_wrap=false,rollback_ended=false,successor_fresh=false;
    explicit schema_observation(schema_operation value):operation(value){}
};
thread_local schema_observation* schema_selected=nullptr;
void schema_after_functions(lattice::database& actual) {
    if(!schema_selected||schema_selected->creator!=lattice::detail::database_native_process())return;
    auto& result=*schema_selected;++result.calls;
    if(result.calls!=1)throw std::runtime_error("schema-revision control unexpectedly continued");
    auto txn=access::begin(actual);
    const auto before=access::schema_revision(actual);
    const auto budget_before=access::inspect(actual);
    if(result.operation==schema_operation::fixed_read) {
        const auto rows=access::count(actual);
        const auto after=access::schema_revision(actual);
        result.read_unchanged=rows.size()==1&&before==after&&after.second==0;
        txn.commit();
        // The old receipt remains retained by txn while its genuine successor
        // is created. Revision zero never aliases the old original's identity.
        auto successor=access::begin(actual);
        const auto next=access::schema_revision(actual);
        result.successor_fresh=next.first!=before.first&&next.second==0;
        successor.rollback();result.rollback_ended=access::autocommit(actual);
    }else {
        // This descriptor comes from the SAME already-built actual immutable
        // Plan. No supplied SQL, may-change bool or fabricated receipt is used.
        auto entry=access::metadata_table_recipe(actual);
        if(result.operation==schema_operation::overflow)
            access::exhaust_schema_revision_data_fault(actual);
        try{access::execute_metadata_table(actual,entry);}
        catch(const lattice::db_error&){result.refusal=true;}
        const auto after=access::schema_revision(actual);
        const auto budget_after=access::inspect(actual);
        result.same_original=before.first==after.first&&budget_before.identity==budget_after.identity&&
            budget_before.deadline==budget_after.deadline;
        result.sticky=budget_after.failed;
        result.policy_debt=access::actual_policy_debt(actual);
        if(result.operation==schema_operation::overflow) {
            result.no_wrap=after.second==UINT64_MAX;
            result.no_fabricated_error=budget_after.first_api_rc==SQLITE_OK&&budget_after.cleanup_api_rc==SQLITE_OK;
        }else result.attempt_invalidated=after.second==before.second+1;
        // Current unfinished schema authority deliberately rejects this real
        // CREATE at prepare. The same control also remains valid once its
        // closed DDL authority is completed: either real attempt retires once.
        txn.rollback();result.rollback_ended=access::autocommit(actual);
    }
    throw stopped_before_publication{};
}
struct schema_probe {
    explicit schema_probe(schema_observation& value) {
        if(schema_selected||selected||unrequested_selected||public_entry_selected||
           probe::after_pending||probe::after_functions)
            throw std::runtime_error("initialization schema probe is already selected");
        schema_selected=&value;probe::after_functions=schema_after_functions;
    }
    ~schema_probe(){probe::after_functions=nullptr;schema_selected=nullptr;}
};
void run_schema_stopped(schema_observation& data) {
    schema_probe hold(data);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(data.calls,1u);
}
}
TEST(InitializationChannel, ActualFixedReadKeepsRevisionAndSuccessorHasItsOwnZero) {
    schema_observation data(schema_operation::fixed_read);run_schema_stopped(data);
    EXPECT_TRUE(data.read_unchanged);EXPECT_TRUE(data.successor_fresh);EXPECT_TRUE(data.rollback_ended);
}
TEST(InitializationChannel, ActualClosedPlanAttemptInvalidatesExactlyOnceBeforeNextEntry) {
    schema_observation data(schema_operation::plan_attempt);run_schema_stopped(data);
    EXPECT_TRUE(data.same_original);EXPECT_TRUE(data.attempt_invalidated);EXPECT_TRUE(data.rollback_ended);
    if(data.refusal)EXPECT_TRUE(data.sticky);
}
TEST(InitializationChannel, ActualRevisionOverflowKeepsDebtAndReservedRollbackWithoutWrap) {
    schema_observation data(schema_operation::overflow);run_schema_stopped(data);
    EXPECT_TRUE(data.same_original);EXPECT_TRUE(data.refusal);EXPECT_TRUE(data.sticky);
    EXPECT_TRUE(data.policy_debt);EXPECT_TRUE(data.no_wrap);EXPECT_TRUE(data.no_fabricated_error);
    EXPECT_TRUE(data.rollback_ended);
}

namespace {
enum class receive_operation {fresh,budget,callback};
struct receive_observation {
    const std::uint64_t creator=lattice::detail::database_native_process();
    const receive_operation operation;
    const bool legacy;
    unsigned calls=0,callback_calls=0,read_admissions=0;
    bool created=false,read_only_reinvoke=false,same_original=false,refused=false,
         sticky=false,no_schema_attempt=false,no_fabricated_error=false,rollback_ended=false,
         rollback_restored_schema=false,public_refused=false,unexpected_public_error=false;
    lattice::database* callback_owner=nullptr; // exact constructor dispatch, never owns Native
    receive_observation(receive_operation value,bool old=false):operation(value),legacy(old){}
};
thread_local receive_observation* receive_selected=nullptr;
int receive_public_reentry(void* context,int action,const char*,const char*,const char*,const char*)noexcept {
    auto* data=static_cast<receive_observation*>(context);
    if(!data||data->creator!=lattice::detail::database_native_process())return SQLITE_DENY;
    if(action!=SQLITE_SELECT||data->callback_calls)return SQLITE_OK;
    ++data->callback_calls;
    // This is SQLite's ACTUAL authorizer callback while the private receive
    // schema-inventory request is bound. The public entry gets no descriptor.
    try{(void)data->callback_owner->query("SELECT 1");}
    catch(const lattice::db_error&){
        if(data->creator!=lattice::detail::database_native_process())return SQLITE_DENY;
        data->public_refused=true;return SQLITE_OK;
    }catch(...){
        if(data->creator!=lattice::detail::database_native_process())return SQLITE_DENY;
        data->unexpected_public_error=true;return SQLITE_DENY;
    }
    return SQLITE_DENY; // Unexpected public success cannot bless outer work.
}
struct receive_role {
    const std::uint64_t creator=lattice::detail::database_native_process();
    lattice::database* owner=nullptr;
    receive_observation* context=nullptr;
    receive_role(lattice::database& actual,receive_observation& data):owner(&actual),context(&data) {
        data.callback_owner=&actual;
        const auto rc=access::set_receive_test_role(actual,receive_public_reentry,&data);
        if(creator!=lattice::detail::database_native_process())
            throw lattice::db_error("inherited receive test role publication");
        if(rc!=SQLITE_OK) {
            // Publication failure does not prove that the raw context was
            // never retained. Clear this exact context before it can retire.
            clear();throw lattice::db_error("receive test restriction could not be published");
        }
    }
    void clear()noexcept {
        if(creator!=lattice::detail::database_native_process()||!owner)return;
        const auto rc=access::clear_receive_test_role(*owner,context);
        if(creator!=lattice::detail::database_native_process())return;
        if(rc!=SQLITE_OK) {
            std::fprintf(stderr,"receive test restriction cleanup debt rc=%d; namespace retained\n",rc);
            std::_Exit(96);
        }
        owner=nullptr;
    }
    ~receive_role(){clear();}
};
void receive_after_functions(lattice::database& actual) {
    if(!receive_selected||receive_selected->creator!=lattice::detail::database_native_process())return;
    auto& result=*receive_selected;++result.calls;
    if(result.calls!=1)throw std::runtime_error("receive initializer control unexpectedly continued");
    auto txn=access::begin(actual);
    const auto before=access::schema_revision(actual);
    const auto budget_before=access::inspect(actual);
    const auto changes_before=access::actual_change_count(actual);
    const auto schema_before=access::actual_schema_count(actual);
    if(result.operation==receive_operation::budget) {
        access::exhaust_statement_budget_data_fault(actual);
        try{lattice::detail::receive_delivery_guard_access::initialize_schema(actual,result.legacy);}
        catch(const lattice::db_error&){result.refused=true;}
        const auto after=access::schema_revision(actual);const auto budget_after=access::inspect(actual);
        result.same_original=before.first==after.first&&budget_before.identity==budget_after.identity&&
            budget_before.deadline==budget_after.deadline;
        result.sticky=budget_after.failed;
        result.no_schema_attempt=after.second==before.second;
        result.no_fabricated_error=budget_after.first_api_rc==SQLITE_OK&&budget_after.cleanup_api_rc==SQLITE_OK;
    }else {
        std::optional<receive_role> restriction;
        if(result.operation==receive_operation::callback)restriction.emplace(actual,result);
        lattice::detail::receive_delivery_guard_access::initialize_schema(actual,result.legacy);
        const auto after=access::schema_revision(actual);
        const auto seeded_changes=access::actual_change_count(actual);
        result.created=after.first==before.first&&after.second==before.second+3&&
            seeded_changes==changes_before+1;
        // The SAME actual initializer's existing-family branch runs its full
        // schema/store/count/key/point audit. A different legacy argument must
        // not rewrite the existing origin or seed a second store row.
        lattice::detail::receive_delivery_guard_access::initialize_schema(actual,!result.legacy);
        const auto audited=access::schema_revision(actual);
        result.read_only_reinvoke=audited==after&&access::actual_change_count(actual)==seeded_changes;
        const auto budget_after=access::inspect(actual);
        result.same_original=budget_before.identity==budget_after.identity&&budget_before.deadline==budget_after.deadline;
        if(restriction)restriction->clear();
    }
    txn.rollback();result.rollback_ended=access::autocommit(actual);
    // total_changes is cumulative across rollback; it is never a restoration
    // oracle. The unchanged fixed query instead compares actual schema counts
    // before and after this exact closed three-CREATE transaction. No second
    // initialization episode, preimage census, or lease is manufactured here.
    if(result.operation!=receive_operation::budget)
        result.rollback_restored_schema=access::actual_schema_count(actual)==schema_before;
    throw stopped_before_publication{};
}
struct receive_probe {
    explicit receive_probe(receive_observation& value) {
        if(receive_selected||schema_selected||selected||unrequested_selected||public_entry_selected||
           probe::after_pending||probe::after_functions||probe::read_admission_attempts)
            throw std::runtime_error("receive initializer probe is already selected");
        receive_selected=&value;probe::after_functions=receive_after_functions;
        probe::read_admission_attempts=&value.read_admissions;
    }
    ~receive_probe(){probe::after_functions=nullptr;probe::read_admission_attempts=nullptr;receive_selected=nullptr;}
};
void run_receive_stopped(receive_observation& data) {
    receive_probe hold(data);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(data.calls,1u);
}
}
TEST(InitializationChannel, ActualFreshReceiveFamilyAndExistingAuditUseSameBudgetAndRollback) {
    receive_observation data(receive_operation::fresh);run_receive_stopped(data);
    EXPECT_TRUE(data.created);EXPECT_TRUE(data.read_only_reinvoke);EXPECT_TRUE(data.same_original);
    EXPECT_TRUE(data.rollback_ended);EXPECT_TRUE(data.rollback_restored_schema);
}
TEST(InitializationChannel, ActualLegacyOriginReceiveSeedIsNotRewrittenByExistingAudit) {
    receive_observation data(receive_operation::fresh,true);run_receive_stopped(data);
    EXPECT_TRUE(data.created);EXPECT_TRUE(data.read_only_reinvoke);EXPECT_TRUE(data.same_original);
    EXPECT_TRUE(data.rollback_ended);EXPECT_TRUE(data.rollback_restored_schema);
}
TEST(InitializationChannel, ActualReceiveBudgetRefusalKeepsSameOriginalAndPrepaidRollback) {
    receive_observation data(receive_operation::budget);run_receive_stopped(data);
    EXPECT_TRUE(data.refused);EXPECT_TRUE(data.same_original);EXPECT_TRUE(data.sticky);
    EXPECT_TRUE(data.no_schema_attempt);EXPECT_TRUE(data.no_fabricated_error);EXPECT_TRUE(data.rollback_ended);
}
TEST(InitializationChannel, ActualReceiveAuthorizerCallbackCannotInheritPrivateRequestForPublicSQL) {
    receive_observation data(receive_operation::callback);run_receive_stopped(data);
    EXPECT_EQ(data.callback_calls,1u);EXPECT_TRUE(data.public_refused);EXPECT_FALSE(data.unexpected_public_error);
    EXPECT_EQ(data.read_admissions,0u);EXPECT_TRUE(data.created);EXPECT_TRUE(data.read_only_reinvoke);
    EXPECT_TRUE(data.same_original);EXPECT_TRUE(data.rollback_ended);EXPECT_TRUE(data.rollback_restored_schema);
}

namespace {
struct stale_vm_observation {
    const std::uint64_t creator=lattice::detail::database_native_process();
    unsigned calls=0;
    access::old_vm_result result{};
};
thread_local stale_vm_observation* stale_vm_selected=nullptr;
void stale_vm_after_functions(lattice::database& actual) {
    if(!stale_vm_selected||stale_vm_selected->creator!=lattice::detail::database_native_process())return;
    if(++stale_vm_selected->calls!=1)throw std::runtime_error("old-VM control unexpectedly continued");
    stale_vm_selected->result=access::old_vm_cannot_enter_successor(actual);
    throw stopped_before_publication{};
}
struct stale_vm_probe {
    explicit stale_vm_probe(stale_vm_observation& value) {
        if(stale_vm_selected||receive_selected||schema_selected||selected||unrequested_selected||
           public_entry_selected||probe::after_pending||probe::after_functions||probe::read_admission_attempts)
            throw std::runtime_error("old-VM initialization probe is already selected");
        stale_vm_selected=&value;probe::after_functions=stale_vm_after_functions;
    }
    ~stale_vm_probe(){probe::after_functions=nullptr;stale_vm_selected=nullptr;}
};
}
TEST(InitializationChannel, ActualPreparedOldEpisodeVMRefusesWorkButFinalizesWithoutEndingSuccessor) {
    stale_vm_observation data;stale_vm_probe hold(data);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(data.calls,1u);
    const auto& result=data.result;
    EXPECT_TRUE(result.prepared);EXPECT_TRUE(result.identity_retained);
    EXPECT_TRUE(result.work_refused);EXPECT_TRUE(result.adoption_refused);EXPECT_TRUE(result.step_refused);
    EXPECT_TRUE(result.never_ran);EXPECT_TRUE(result.finalized);EXPECT_TRUE(result.successor_unchanged);
    EXPECT_TRUE(result.old_revision_only);EXPECT_TRUE(result.successor_rollback_ended);
}

#include "../../Sources/LatticeCore/src/observer_delivery_startup_test_probe.hpp"
namespace {
using owner_binding_probe=lattice::detail::observer_delivery_startup_test_probe;
struct owner_binding_result {
    const std::uint64_t creator=lattice::detail::database_native_process();
    unsigned pending=0,bound=0,read_admissions=0;
    std::array<const void*,3> actual{},budgets{};
    std::array<std::chrono::steady_clock::time_point,3> ends{};
    owner_binding_probe::owner_binding_observation observation{};
    const void* owner=nullptr;
    const void* bound_budget=nullptr;
    std::chrono::steady_clock::time_point bound_end{};
    bool public_refused=false,unexpected_error=false;
};
thread_local owner_binding_result* owner_binding_selected=nullptr;
void owner_binding_after_pending(lattice::database& actual) {
    if(!owner_binding_selected || owner_binding_selected->creator!=lattice::detail::database_native_process())return;
    auto& data=*owner_binding_selected;
    if(data.pending>=data.actual.size())throw std::runtime_error("owner binding exceeded actual three-open family");
    const auto before=access::inspect(actual); // actual already-admitted Open budget
    data.actual[data.pending]=&actual;data.budgets[data.pending]=before.identity;
    data.ends[data.pending]=before.deadline;++data.pending;
}
void owner_binding_after_bound(lattice::lattice_db& actual,
    const owner_binding_probe::owner_binding_observation& observation) {
    if(!owner_binding_selected || owner_binding_selected->creator!=lattice::detail::database_native_process())return;
    auto& data=*owner_binding_selected;
    if(++data.bound!=1)throw std::runtime_error("actual pending owner was rebound");
    data.owner=&actual;data.observation=observation; // comparison DATA only
    const auto after=access::inspect(actual.db());
    data.bound_budget=after.identity;data.bound_end=after.deadline;
    // Binding this actual owner is neither finish nor public read authority.
    try{(void)actual.db().query("SELECT 1");}
    catch(const lattice::db_error&){data.public_refused=true;}
    catch(...){data.unexpected_error=true;}
    throw stopped_before_publication{}; // BEFORE Core schema work or any four-finish call
}
struct owner_binding_hook {
    explicit owner_binding_hook(owner_binding_result& data) {
        if(owner_binding_selected||selected||unrequested_selected||public_entry_selected||
           schema_selected||receive_selected||stale_vm_selected||probe::after_pending||
           probe::after_functions||probe::read_admission_attempts||owner_binding_probe::after_owner_bound)
            throw std::runtime_error("pending owner binding control already selected");
        owner_binding_selected=&data;probe::after_pending=owner_binding_after_pending;
        probe::read_admission_attempts=&data.read_admissions;
        owner_binding_probe::after_owner_bound=owner_binding_after_bound;
    }
    ~owner_binding_hook() {
        owner_binding_probe::after_owner_bound=nullptr;probe::after_pending=nullptr;
        probe::read_admission_attempts=nullptr;owner_binding_selected=nullptr;
    }
};
void check_actual_pending_owner_binding(bool simple_path) {
    owner_binding_result data;owner_binding_hook hold(data);bool stopped=false;
    try {
        if(simple_path){lattice::lattice_db actual(actual_logical_root());}
        else construct_actual_owner();
    }catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(data.bound,1u);
    const unsigned roles=simple_path?3u:7u,count=simple_path?2u:3u;
    ASSERT_EQ(data.pending,count);EXPECT_EQ(data.observation.roles,roles);
    ASSERT_NE(data.owner,nullptr);ASSERT_NE(data.observation.catalog,nullptr);
    ASSERT_NE(data.observation.budget,nullptr);ASSERT_NE(data.observation.profile,nullptr);
    EXPECT_EQ(data.bound_budget,data.observation.budget);EXPECT_EQ(data.bound_end,data.ends[0]);
    EXPECT_TRUE(data.public_refused);EXPECT_FALSE(data.unexpected_error);EXPECT_EQ(data.read_admissions,0u);
    for(unsigned i=0;i<count;++i) {
        EXPECT_EQ(data.observation.actual[i],data.actual[i]);
        EXPECT_EQ(data.observation.role_budget[i],data.budgets[i]);
        EXPECT_EQ(data.budgets[i],data.observation.budget);EXPECT_EQ(data.ends[i],data.ends[0]);
        EXPECT_EQ(data.observation.role_profile[i],data.observation.profile);
        ASSERT_NE(data.observation.open[i],nullptr);ASSERT_NE(data.observation.runtime[i],nullptr);
        for(unsigned j=0;j<i;++j) {
            EXPECT_NE(data.observation.actual[i],data.observation.actual[j]);
            EXPECT_NE(data.observation.open[i],data.observation.open[j]);
            EXPECT_NE(data.observation.runtime[i],data.observation.runtime[j]);
        }
    }
    for(unsigned i=count;i<3;++i) {
        EXPECT_EQ(data.observation.actual[i],nullptr);EXPECT_EQ(data.observation.open[i],nullptr);
        EXPECT_EQ(data.observation.runtime[i],nullptr);EXPECT_EQ(data.observation.role_budget[i],nullptr);
        EXPECT_EQ(data.observation.role_profile[i],nullptr);
    }
}
}
TEST(InitializationChannel, ActualConfiguredOwnerBindsEntireThreeOpenPendingFamilyWithoutPublishing) {
    check_actual_pending_owner_binding(false);
}
TEST(InitializationChannel, ActualPathOwnerBindsOnlyItsEntireTwoOpenPendingFamilyWithoutPublishing) {
    check_actual_pending_owner_binding(true);
}

namespace {
enum class marker_control {success,missing_table,wrong_key,stale_cookie,denied_insert};
struct marker_control_result {
    const std::uint64_t creator=lattice::detail::database_native_process();
    marker_control operation;
    unsigned bound=0,denials=0;
    bool complete=false,raw_no_core=false,required_present_refused=false,wrong_key_refused=false,
        stale_cookie_refused=false,actual_insert_refused=false,first_auth=false,clean_cleanup=false,
        sticky=false,no_fabricated_error=false,no_effect=false,
        marker_exact=false,version_prefix_exact=false,repeated_exact=false,revision_advanced=false,
        same_budget=false,rollback_ended=false,rollback_restored=false,public_refused=false;
    explicit marker_control_result(marker_control selected):operation(selected){}
};
thread_local marker_control_result* marker_selected=nullptr;
int marker_deny_actual_insert(void* context,int action,const char* table,const char*,const char* schema,const char*)noexcept {
    auto& data=*static_cast<marker_control_result*>(context);
    if(action==SQLITE_INSERT&&table&&schema&&std::strcmp(table,"_lattice_meta")==0&&std::strcmp(schema,"main")==0) {
        ++data.denials;return SQLITE_DENY;
    }
    return SQLITE_OK;
}
void marker_after_bound(lattice::lattice_db& owner,const owner_binding_probe::owner_binding_observation&) {
    if(!marker_selected||marker_selected->creator!=lattice::detail::database_native_process())return;
    auto& data=*marker_selected;if(++data.bound!=1)throw std::runtime_error("marker control unexpected owner binding");
    auto& actual=owner.db();const auto& retained_budget=access::budget(actual);
    const auto budget_before=access::retained_marker_budget_data(retained_budget);
    const auto schema_before=access::actual_schema_count(actual);
    auto transaction=access::begin(actual);
    data.raw_no_core=!access::autocommit(actual)&&!access::has_core_original(actual);
    if(data.operation==marker_control::missing_table) {
        if(!access::actual_metadata_absent(actual))throw std::runtime_error("missing-table control requires actual absent metadata");
    }else access::ensure_marker_metadata(actual);
    const auto key=access::actual_marker_key(owner);const auto cookie=access::actual_cookie(actual);
    const auto revision_before=access::schema_revision(actual);const auto changes_before=access::actual_change_count(actual);
    if(data.operation==marker_control::success) {
        const auto version_prefix=access::actual_metadata_value(actual,access::recipe::metadata_version_read);
        access::store_actual_marker(owner,key,cookie);
        data.marker_exact=access::actual_metadata_value(actual,access::recipe::metadata_marker_read,key)==std::to_string(cookie);
        data.version_prefix_exact=access::actual_metadata_value(actual,access::recipe::metadata_version_read)==version_prefix;
        access::store_actual_marker(owner,key,cookie);
        data.repeated_exact=access::actual_metadata_value(actual,access::recipe::metadata_marker_read,key)==std::to_string(cookie);
        const auto revision_after=access::schema_revision(actual);
        data.revision_advanced=revision_after.first==revision_before.first&&revision_after.second==revision_before.second+4;
        const auto budget_after=access::inspect(actual);
        data.same_budget=budget_after.identity==budget_before.identity&&budget_after.deadline==budget_before.deadline&&
            budget_after.statements>budget_before.statements&&budget_after.workspace>budget_before.workspace;
        data.complete=true;
    } else {
        struct restriction {
            lattice::database& actual;marker_control_result& data;bool installed=false;
            ~restriction(){if(installed&&access::clear_receive_test_role(actual,&data)!=SQLITE_OK)std::_Exit(97);}
        } ending{actual,data};
        if(data.operation==marker_control::denied_insert) {
            if(access::set_receive_test_role(actual,marker_deny_actual_insert,&data)!=SQLITE_OK)
                throw std::runtime_error("marker INSERT restriction installation failed");
            ending.installed=true;
        }
        auto offered=key;if(data.operation==marker_control::wrong_key)offered.back()=offered.back()=='a'?'b':'a';
        try{access::store_actual_marker(owner,offered,cookie+(data.operation==marker_control::stale_cookie?1:0));}
        catch(const lattice::db_error&) {
            data.required_present_refused=data.operation==marker_control::missing_table;
            data.wrong_key_refused=data.operation==marker_control::wrong_key;
            data.stale_cookie_refused=data.operation==marker_control::stale_cookie;
            data.actual_insert_refused=data.operation==marker_control::denied_insert;
        }
        const auto budget_after=access::retained_marker_budget_data(retained_budget);data.sticky=budget_after.failed;
        data.first_auth=budget_after.first_api_rc==SQLITE_AUTH;
        data.clean_cleanup=budget_after.cleanup_api_rc==0;
        data.no_fabricated_error=budget_after.first_api_rc==0&&budget_after.cleanup_api_rc==0;
        data.no_effect=access::actual_change_count(actual)==changes_before;
        // The retained budget inspection above is DATA only; no failed-open
        // healthy getter, counter reset, successor or admission is created.
    }
    transaction.rollback();data.rollback_ended=access::autocommit(actual);
    if(data.operation==marker_control::denied_insert) {
        const auto after_ending=access::retained_marker_budget_data(retained_budget);
        data.first_auth=after_ending.first_api_rc==SQLITE_AUTH;data.clean_cleanup=after_ending.cleanup_api_rc==0;
    }
    if(data.operation==marker_control::success)data.rollback_restored=access::actual_schema_count(actual)==schema_before;
    try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){data.public_refused=true;}
    throw stopped_before_publication{};
}
struct marker_hook {
    explicit marker_hook(marker_control_result& data) {
        if(marker_selected||owner_binding_selected||selected||unrequested_selected||public_entry_selected||
           schema_selected||receive_selected||stale_vm_selected||probe::after_pending||probe::after_functions||
           owner_binding_probe::after_owner_bound)throw std::runtime_error("marker control already selected");
        marker_selected=&data;owner_binding_probe::after_owner_bound=marker_after_bound;
    }
    ~marker_hook(){owner_binding_probe::after_owner_bound=nullptr;marker_selected=nullptr;}
};
void run_actual_marker_control(marker_control_result& data) {
    marker_hook hold(data);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(data.bound,1u);EXPECT_TRUE(data.raw_no_core);EXPECT_TRUE(data.rollback_ended);EXPECT_TRUE(data.public_refused);
}
TEST(InitializationChannel, ActualCoreMarkerTypedStorePruneAndReadProofKeepSameOriginalAndRollback) {
    marker_control_result data(marker_control::success);run_actual_marker_control(data);
    EXPECT_TRUE(data.complete);EXPECT_TRUE(data.marker_exact);EXPECT_TRUE(data.version_prefix_exact);
    EXPECT_TRUE(data.repeated_exact);EXPECT_TRUE(data.revision_advanced);EXPECT_TRUE(data.same_budget);EXPECT_TRUE(data.rollback_restored);
}
TEST(InitializationChannel, MarkerRequiredPresentProofRefusesMissingActualMetadataBeforeWrite) {
    marker_control_result data(marker_control::missing_table);run_actual_marker_control(data);
    EXPECT_TRUE(data.required_present_refused);EXPECT_TRUE(data.sticky);EXPECT_TRUE(data.no_fabricated_error);EXPECT_TRUE(data.no_effect);
}
TEST(InitializationChannel, MarkerRefusesCanonicalKeyFromAnotherDeclarationBeforeWrite) {
    marker_control_result data(marker_control::wrong_key);run_actual_marker_control(data);
    EXPECT_TRUE(data.wrong_key_refused);EXPECT_TRUE(data.sticky);EXPECT_TRUE(data.no_fabricated_error);EXPECT_TRUE(data.no_effect);
}
TEST(InitializationChannel, MarkerRefusesChangedActualCookieBeforeWrite) {
    marker_control_result data(marker_control::stale_cookie);run_actual_marker_control(data);
    EXPECT_TRUE(data.stale_cookie_refused);EXPECT_TRUE(data.sticky);EXPECT_TRUE(data.no_fabricated_error);EXPECT_TRUE(data.no_effect);
}
TEST(InitializationChannel, ActualMarkerInsertDenialPreservesFirstSQLiteErrorAndReservedRollback) {
    marker_control_result data(marker_control::denied_insert);run_actual_marker_control(data);
    EXPECT_TRUE(data.actual_insert_refused);EXPECT_GT(data.denials,0u);EXPECT_TRUE(data.first_auth);
    EXPECT_TRUE(data.clean_cleanup);EXPECT_TRUE(data.sticky);EXPECT_TRUE(data.no_effect);
}
} // namespace


#include "../../Sources/LatticeCore/src/recovery_producer_continuity.hpp"
namespace lattice::detail {
// Calls the SAME two private production producers. It cannot construct a
// continuous admission, startup, Open, catalog, SQL string or new budget.
struct recovery_continuous_startup_test_access {
    static void durability(database& actual){recovery_continuous_producer::initialize_connection_durability_(actual);}
    static bool presence(database& actual){return recovery_continuous_producer::fresh_schema_present_(actual);}
};
}
namespace {
using continuous_controls=lattice::detail::recovery_continuous_startup_test_access;
enum class continuous_fixed_mode {real_controls,budget,callback};
struct continuous_fixed_result {
    continuous_fixed_mode mode;
    unsigned calls=0,callbacks=0,read_admissions=0;
    bool durability=false,before_empty=false,created_present=false,rollback_empty=false,
        same_budget=false,monotonic=false,rollback_ended=false,public_refused=false,
        work_refused=false,sticky=false,no_fabricated_error=false,callback_public_refused=false,
        callback_unexpected=false;
    lattice::database* actual=nullptr;
};
thread_local continuous_fixed_result* continuous_fixed_selected=nullptr;
int continuous_fixed_authorizer(void* data,int action,const char* first,const char* second,const char*,const char*)noexcept {
    auto* result=static_cast<continuous_fixed_result*>(data);
    if(!result||!result->actual||action!=SQLITE_PRAGMA||!first||!second||sqlite3_stricmp(first,"synchronous")||sqlite3_stricmp(second,"FULL"))return SQLITE_OK;
    ++result->callbacks;
    try{(void)result->actual->query("SELECT 1");}
    catch(const lattice::db_error&){result->callback_public_refused=true;}
    catch(...){result->callback_unexpected=true;}
    return SQLITE_OK;
}
struct continuous_fixed_role {
    lattice::database& actual;continuous_fixed_result& result;bool held=false;
    continuous_fixed_role(lattice::database& db,continuous_fixed_result& observed):actual(db),result(observed) {
        if(access::set_receive_test_role(actual,continuous_fixed_authorizer,&result)!=SQLITE_OK)
            throw std::runtime_error("continuous fixed control restriction install failed");
        held=true;
    }
    void clear() {
        if(!held)return;
        const auto rc=access::clear_receive_test_role(actual,&result);
        if(rc!=SQLITE_OK) {
            std::fprintf(stderr,"continuous fixed control cleanup debt rc=%d; namespace retained\n",rc);
            std::_Exit(96);
        }
        held=false;
    }
    ~continuous_fixed_role(){clear();}
};
void continuous_fixed_after_functions(lattice::database& actual) {
    if(!continuous_fixed_selected)return;
    auto& result=*continuous_fixed_selected;
    if(++result.calls!=1)throw std::runtime_error("continuous fixed producer control unexpectedly continued");
    result.actual=&actual;
    const auto before=access::inspect(actual);
    if(result.mode==continuous_fixed_mode::budget) {
        auto transaction=access::begin(actual); // real prepaid ending before DATA fault
        access::exhaust_statement_budget_data_fault(actual);
        try{continuous_controls::durability(actual);}catch(const lattice::db_error&){result.work_refused=true;}
        const auto after=access::inspect(actual);
        result.sticky=after.failed;result.no_fabricated_error=after.first_api_rc==0&&after.cleanup_api_rc==0;
        result.same_budget=after.identity==before.identity&&after.deadline==before.deadline;
        transaction.rollback();result.rollback_ended=access::autocommit(actual);
    }else {
        std::optional<continuous_fixed_role> role;
        if(result.mode==continuous_fixed_mode::callback)role.emplace(actual,result);
        continuous_controls::durability(actual);result.durability=true; // setter AND actual >=FULL getter
        if(role)role->clear();
        result.before_empty=!continuous_controls::presence(actual);
        auto transaction=access::begin(actual);
        const auto table=access::metadata_table_recipe(actual);
        access::execute_metadata_table(actual,table); // genuine immutable CREATE, not caller SQL
        result.created_present=continuous_controls::presence(actual);
        transaction.rollback();result.rollback_ended=access::autocommit(actual);
        result.rollback_empty=!continuous_controls::presence(actual);
        const auto after=access::inspect(actual);
        result.same_budget=after.identity==before.identity&&after.deadline==before.deadline;
        result.monotonic=after.statements>before.statements&&after.workspace>before.workspace;
        try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){result.public_refused=true;}
    }
    result.actual=nullptr;throw stopped_before_publication{};
}
struct continuous_fixed_hook {
    explicit continuous_fixed_hook(continuous_fixed_result& result) {
        if(continuous_fixed_selected||probe::after_functions||probe::after_pending||probe::read_admission_attempts)
            throw std::runtime_error("continuous fixed producer control already selected");
        continuous_fixed_selected=&result;probe::after_functions=continuous_fixed_after_functions;
        probe::read_admission_attempts=&result.read_admissions;
    }
    ~continuous_fixed_hook(){probe::after_functions=nullptr;probe::read_admission_attempts=nullptr;continuous_fixed_selected=nullptr;}
};
void run_continuous_fixed_control(continuous_fixed_result& result) {
    continuous_fixed_hook hook(result);bool stopped=false;
    // Genuine dynamic .sqlite member in the installed sync root. This tests
    // real closed connection SQL producers; it does NOT pretend that this
    // ordinary leaf is an admitted continuous container or enrollment.
    const auto memory=actual_logical_root();const auto path=memory.substr(0,memory.rfind('/'))+"/sync/resource-continuous-prefix.sqlite";
    lattice::configuration config(path);config.audit_retention_seconds=0;
    try{lattice::lattice_db actual(config,true);}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(result.calls,1u);EXPECT_EQ(result.read_admissions,0u);
}
}
TEST(InitializationChannel, ActualContinuousFixedDurabilityAndSchemaProbeKeepSamePendingOriginal) {
    continuous_fixed_result result{continuous_fixed_mode::real_controls};run_continuous_fixed_control(result);
    EXPECT_TRUE(result.durability);EXPECT_TRUE(result.before_empty);EXPECT_TRUE(result.created_present);
    EXPECT_TRUE(result.rollback_empty);EXPECT_TRUE(result.rollback_ended);EXPECT_TRUE(result.same_budget);
    EXPECT_TRUE(result.monotonic);EXPECT_TRUE(result.public_refused);
}
TEST(InitializationChannel, ActualContinuousFixedBudgetRefusalKeepsRealPrepaidRollback) {
    continuous_fixed_result result{continuous_fixed_mode::budget};run_continuous_fixed_control(result);
    EXPECT_TRUE(result.work_refused);EXPECT_TRUE(result.same_budget);EXPECT_TRUE(result.sticky);
    EXPECT_TRUE(result.no_fabricated_error);EXPECT_TRUE(result.rollback_ended);
}
TEST(InitializationChannel, ActualContinuousFixedAuthorizerReentryCannotBorrowPrivateSQL) {
    continuous_fixed_result result{continuous_fixed_mode::callback};run_continuous_fixed_control(result);
    EXPECT_EQ(result.callbacks,1u);EXPECT_TRUE(result.callback_public_refused);EXPECT_FALSE(result.callback_unexpected);
    EXPECT_TRUE(result.durability);EXPECT_TRUE(result.before_empty);EXPECT_TRUE(result.created_present);
    EXPECT_TRUE(result.rollback_empty);EXPECT_TRUE(result.same_budget);EXPECT_TRUE(result.rollback_ended);
    EXPECT_TRUE(result.public_refused);EXPECT_TRUE(result.monotonic);
}


#include "../../Sources/LatticeCore/src/recovery_obligation_bootstrap_statement.hpp"
namespace lattice::detail {
struct recovery_continuous_row_test_access {
    struct observation {bool exact_refusal=false,rollback_ended=false;};
    static observation missing_table(database& actual) {
        // Same finite independent caps as the actual continuous constructor;
        // no supplied schema or expected table is used to fabricate evidence.
        const recovery_obligation_producer_discovery_limits caps{
            {16,100000,4096,67108864},{16,4096,1048576},{16,100000,4096,1048576,67108864}};
        auto reader=recovery_obligation_bootstrap_reader::create_(actual,caps,false);
        reader->begin_();reader->validated_family_();
        observation result;
        try{(void)reader->query_(recovery_obligation_bootstrap_statement::kind::continuous_row);}
        catch(const db_error& error){result.exact_refusal=std::string_view(error.what())==
            "bootstrap app/ledger read lacks actual same-snapshot plain-table preimage";}
        reader->rollback_();result.rollback_ended=true;
        return result;
    }
};
}
namespace {
struct continuous_row_result {
    unsigned calls=0,read_admissions=0;bool exact_refusal=false,rollback_ended=false,
        actual_autocommit=false,same_budget=false,sticky=false,no_fabricated_error=false,no_writes=false;
};
thread_local continuous_row_result* continuous_row_selected=nullptr;
void continuous_row_after_functions(lattice::database& actual) {
    if(!continuous_row_selected)return;
    auto& result=*continuous_row_selected;if(++result.calls!=1)throw std::runtime_error("continuous row control unexpectedly continued");
    const auto before=access::inspect(actual);const auto changes=access::actual_change_count(actual);
    const auto observed=lattice::detail::recovery_continuous_row_test_access::missing_table(actual);
    result.exact_refusal=observed.exact_refusal;result.rollback_ended=observed.rollback_ended;
    result.actual_autocommit=access::autocommit(actual);result.no_writes=changes==access::actual_change_count(actual);
    const auto after=access::inspect(actual);result.same_budget=before.identity==after.identity&&before.deadline==after.deadline;
    result.sticky=after.failed;result.no_fabricated_error=after.first_api_rc==0&&after.cleanup_api_rc==0;
    throw stopped_before_publication{};
}
struct continuous_row_hook {
    explicit continuous_row_hook(continuous_row_result& result) {
        if(continuous_row_selected||probe::after_functions||probe::after_pending||probe::read_admission_attempts)
            throw std::runtime_error("continuous row control already selected");
        continuous_row_selected=&result;probe::after_functions=continuous_row_after_functions;probe::read_admission_attempts=&result.read_admissions;
    }
    ~continuous_row_hook(){probe::after_functions=nullptr;probe::read_admission_attempts=nullptr;continuous_row_selected=nullptr;}
};
}
TEST(InitializationChannel, ActualContinuousRowReadRequiresPresentPlainTableAndRetainsItsOwnRollback) {
    continuous_row_result result;continuous_row_hook hook(result);bool stopped=false;
    const auto memory=actual_logical_root();
    lattice::configuration config(memory.substr(0,memory.rfind('/'))+"/sync/resource-continuous-row.sqlite");config.audit_retention_seconds=0;
    try{lattice::lattice_db actual(config,true);}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(result.calls,1u);EXPECT_EQ(result.read_admissions,0u);
    EXPECT_TRUE(result.exact_refusal);EXPECT_TRUE(result.rollback_ended);EXPECT_TRUE(result.actual_autocommit);
    EXPECT_TRUE(result.same_budget);EXPECT_TRUE(result.sticky);EXPECT_TRUE(result.no_fabricated_error);EXPECT_TRUE(result.no_writes);
}

namespace {
struct startup_typed_model_tag {};
const lattice::model_schema& startup_typed_model_declaration() {
    static const auto schema=[] {
        lattice::model_schema out;out.table_name="StartupTypedModel";
        lattice::property_descriptor label;label.name="label";label.type=lattice::column_type::text;
        label.kind=lattice::property_kind::primitive;label.nullable=true;label.is_unique=true;label.no_history=true;
        lattice::property_descriptor count;count.name="count";count.type=lattice::column_type::integer;
        count.kind=lattice::property_kind::primitive;count.nullable=true;
        out.properties.push_back(label);out.properties.push_back(count);return out;
    }();
    static const bool registered=[] {
        // Genuine public declaration path, captured BEFORE normal owner open.
        lattice::schema_registry::instance().register_model(typeid(startup_typed_model_tag),schema);return true;
    }();
    (void)registered;return schema;
}
enum class model_control_mode {success,missing,unknown,deny_create,deny_metadata,skip_version,deny_version};
struct model_control_result {
    const std::uint64_t creator=lattice::detail::database_native_process();
    model_control_mode mode;unsigned bound=0,denials=0;
    bool raw_no_core=false,before_absent=false,present=false,columns_exact=false,complete=false,repeated=false,
        version2=false,version3=false,same_original=false,monotonic=false,refused=false,sticky=false,
        missing_family_exact=false,skipped_step_exact=false,unknown_recipe_exact=false,
        first_auth=false,clean_cleanup=false,no_fabricated_error=false,no_effect=false,
        rollback_ended=false,rollback_restored=false,public_refused=false;
    explicit model_control_result(model_control_mode chosen):mode(chosen){}
};
thread_local model_control_result* model_selected=nullptr;
int model_actual_restriction(void* data,int action,const char* first,const char* second,const char* schema,const char*)noexcept {
    auto& result=*static_cast<model_control_result*>(data);
    if(!schema||std::strcmp(schema,"main"))return SQLITE_OK;
    const bool denied=(result.mode==model_control_mode::deny_create&&action==SQLITE_CREATE_TABLE&&first&&std::strcmp(first,"StartupTypedModel")==0)||
        (result.mode==model_control_mode::deny_metadata&&action==SQLITE_INSERT&&first&&std::strcmp(first,"_lattice_meta")==0)||
        (result.mode==model_control_mode::deny_version&&action==SQLITE_UPDATE&&first&&second&&
            std::strcmp(first,"_lattice_meta")==0&&std::strcmp(second,"value")==0);
    if(denied){++result.denials;return SQLITE_DENY;}return SQLITE_OK;
}
void model_after_bound(lattice::lattice_db& owner,const owner_binding_probe::owner_binding_observation&) {
    if(!model_selected||model_selected->creator!=lattice::detail::database_native_process())return;
    auto& result=*model_selected;if(++result.bound!=1)throw std::runtime_error("model control unexpectedly continued");
    auto& actual=owner.db();const auto& retained=access::budget(actual);
    const auto original=access::retained_marker_budget_data(retained);const auto schema_before=access::actual_schema_count(actual);
    auto transaction=access::begin(actual);result.raw_no_core=!access::autocommit(actual)&&!access::has_core_original(actual);
    access::model_machinery(actual);
    const auto& declaration=startup_typed_model_declaration();result.before_absent=!access::model_present(owner,declaration.table_name);
    const auto changes_before=access::actual_change_count(actual);
    struct restriction {
        lattice::database& actual;model_control_result& result;bool held=false;
        void clear(){if(held){if(access::clear_receive_test_role(actual,&result)!=SQLITE_OK)std::_Exit(97);held=false;}}
        ~restriction(){clear();}
    } role{actual,result};
    if(result.mode==model_control_mode::deny_create||result.mode==model_control_mode::deny_metadata||result.mode==model_control_mode::deny_version) {
        if(access::set_receive_test_role(actual,model_actual_restriction,&result)!=SQLITE_OK)
            throw std::runtime_error("actual model restriction install failed");
        role.held=true;
    }
    try {
        if(result.mode==model_control_mode::missing)access::model_complete(owner);
        else if(result.mode==model_control_mode::unknown)access::unknown_model_program(owner);
        else if(result.mode==model_control_mode::skip_version)lattice::observer_delivery_custody_test_access::set_schema_version(owner,3);
        else if(result.mode==model_control_mode::deny_version)lattice::observer_delivery_custody_test_access::set_schema_version(owner,2);
        else {
            // Actual original shared public producer. Its private branch uses
            // only the normal captured catalog and genuine current raw BEGIN.
            access::setup_actual_native_models(owner);
            result.present=access::model_present(owner,declaration.table_name);
            const auto columns=access::model_columns(owner,declaration.table_name);
            result.columns_exact=columns.size()==4&&columns.at("id")=="INTEGER"&&columns.at("globalId")=="TEXT"&&
                columns.at("label")=="TEXT"&&columns.at("count")=="INTEGER";
            access::model_complete(owner);result.complete=true;
            access::setup_actual_native_models(owner);
            access::model_complete(owner);result.repeated=true;
            lattice::observer_delivery_custody_test_access::set_schema_version(owner,2);result.version2=lattice::observer_delivery_custody_test_access::get_schema_version(owner)==2;
            lattice::observer_delivery_custody_test_access::set_schema_version(owner,3);result.version3=lattice::observer_delivery_custody_test_access::get_schema_version(owner)==3;
        }
    }catch(const lattice::db_error& error) {
        result.refused=true;
        result.skipped_step_exact=result.mode==model_control_mode::skip_version&&
            std::string_view(error.what())=="model version does not advance exactly one supported step";
        result.unknown_recipe_exact=result.mode==model_control_mode::unknown&&
            std::string_view(error.what())=="initialization statement has no actual immutable library recipe";
    }catch(const lattice::detail::ordinary_schema_validation_error& error) {
        if(result.mode!=model_control_mode::missing)throw;
        result.refused=true;result.missing_family_exact=
            std::string_view(error.what())=="ordinary completed family still has absent required tables/modules";
    }
    role.clear();
    const auto after=access::retained_marker_budget_data(retained);
    result.same_original=after.identity==original.identity&&after.deadline==original.deadline;
    result.monotonic=after.statements>original.statements&&after.workspace>original.workspace;
    result.sticky=after.failed;result.first_auth=after.first_api_rc==SQLITE_AUTH;result.clean_cleanup=after.cleanup_api_rc==0;
    result.no_fabricated_error=after.first_api_rc==0&&after.cleanup_api_rc==0;
    if(result.mode!=model_control_mode::success)result.no_effect=access::actual_change_count(actual)==changes_before;
    transaction.rollback();result.rollback_ended=access::autocommit(actual);
    const auto ended=access::retained_marker_budget_data(retained);
    result.first_auth=ended.first_api_rc==SQLITE_AUTH;result.clean_cleanup=ended.cleanup_api_rc==0;
    if(result.mode==model_control_mode::success)result.rollback_restored=access::actual_schema_count(actual)==schema_before;
    try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){result.public_refused=true;}
    throw stopped_before_publication{};
}
struct model_control_hook {
    explicit model_control_hook(model_control_result& result) {
        if(model_selected||owner_binding_probe::after_owner_bound||probe::after_pending||probe::after_functions)
            throw std::runtime_error("actual model control already selected");
        model_selected=&result;owner_binding_probe::after_owner_bound=model_after_bound;
    }
    ~model_control_hook(){owner_binding_probe::after_owner_bound=nullptr;model_selected=nullptr;}
};
void run_actual_model_control(model_control_result& result) {
    (void)startup_typed_model_declaration();model_control_hook hook(result);bool stopped=false;
    lattice::configuration config(actual_logical_root());config.target_schema_version=3;
    try{lattice::lattice_db owner(config,true);}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(result.bound,1u);EXPECT_TRUE(result.raw_no_core);EXPECT_TRUE(result.before_absent);
    EXPECT_TRUE(result.same_original);EXPECT_TRUE(result.monotonic);EXPECT_TRUE(result.rollback_ended);EXPECT_TRUE(result.public_refused);
}
TEST(InitializationChannel, ActualModelCreateAuditIndexFlagsVersionAndRequiredPresentKeepOriginalAndRollback) {
    model_control_result result(model_control_mode::success);run_actual_model_control(result);
    EXPECT_FALSE(result.refused);EXPECT_FALSE(result.sticky);EXPECT_TRUE(result.present);EXPECT_TRUE(result.columns_exact);
    EXPECT_TRUE(result.complete);EXPECT_TRUE(result.repeated);EXPECT_TRUE(result.version2);EXPECT_TRUE(result.version3);
    EXPECT_TRUE(result.rollback_restored);EXPECT_TRUE(result.no_fabricated_error);EXPECT_TRUE(result.clean_cleanup);
}
TEST(InitializationChannel, ActualModelRequiredPresentRefusesMissingFamilyAndRetainsRollback) {
    model_control_result result(model_control_mode::missing);run_actual_model_control(result);
    EXPECT_TRUE(result.missing_family_exact);EXPECT_TRUE(result.refused);EXPECT_TRUE(result.sticky);EXPECT_TRUE(result.no_effect);EXPECT_TRUE(result.no_fabricated_error);
}
TEST(InitializationChannel, ActualModelPlanRefusesUnknownProgramBeforeNativeEffects) {
    model_control_result result(model_control_mode::unknown);run_actual_model_control(result);
    EXPECT_TRUE(result.unknown_recipe_exact);EXPECT_TRUE(result.refused);EXPECT_TRUE(result.sticky);EXPECT_TRUE(result.no_effect);EXPECT_TRUE(result.no_fabricated_error);
}
TEST(InitializationChannel, ActualModelCreateDenialPreservesFirstAuthAndCleanup) {
    model_control_result result(model_control_mode::deny_create);run_actual_model_control(result);
    EXPECT_TRUE(result.refused);EXPECT_TRUE(result.sticky);EXPECT_GT(result.denials,0u);EXPECT_TRUE(result.first_auth);EXPECT_TRUE(result.clean_cleanup);
}
TEST(InitializationChannel, ActualModelFlagInsertDenialPreservesFirstAuthAndWholeRollback) {
    model_control_result result(model_control_mode::deny_metadata);run_actual_model_control(result);
    EXPECT_TRUE(result.refused);EXPECT_TRUE(result.sticky);EXPECT_GT(result.denials,0u);EXPECT_TRUE(result.first_auth);EXPECT_TRUE(result.clean_cleanup);
}
TEST(InitializationChannel, ActualModelVersionRefusesSkippedStepBeforeMutation) {
    model_control_result result(model_control_mode::skip_version);run_actual_model_control(result);
    EXPECT_TRUE(result.skipped_step_exact);EXPECT_TRUE(result.refused);EXPECT_TRUE(result.sticky);EXPECT_TRUE(result.no_effect);EXPECT_TRUE(result.no_fabricated_error);
}
TEST(InitializationChannel, ActualModelVersionUpdateDenialPreservesFirstAuthAndReservedRollback) {
    model_control_result result(model_control_mode::deny_version);run_actual_model_control(result);
    EXPECT_TRUE(result.refused);EXPECT_TRUE(result.sticky);EXPECT_GT(result.denials,0u);EXPECT_TRUE(result.first_auth);EXPECT_TRUE(result.clean_cleanup);
}
} // namespace


namespace lattice::detail {
struct database_initialization_registrations_test_access {
    static bool current(database& actual) {
        actual.require_creator_();database::catalog_native_admission_ native(actual);
        return actual.delivery_startup_&&actual.initialization_registrations_current_(*actual.delivery_startup_);
    }
    static int replace(database& actual,int chosen,bool& actual_native_entered) {
        actual.require_creator_();database::catalog_native_admission_ native(actual);
        if(chosen==3) {
            // Actual pre-use vec_each removal. This does not claim immediate
            // xDestroy for replacement while an old connected vtab survives.
            actual_native_entered=true;
            return sqlite3_create_module_v2(actual.db_,"vec_each",nullptr,nullptr,nullptr);
        }
        return store_physical_create_function_v2_checked(actual.db_,chosen==1?"sync_disabled":"vec_version",0,SQLITE_UTF8,
            nullptr,[](sqlite3_context* context,int,sqlite3_value**){sqlite3_result_int(context,17);},nullptr,nullptr,nullptr,&actual_native_entered);
    }
    static bool require_retired(database& actual) {
        actual.require_creator_();
        try{actual.require_initialization_registrations_(*actual.delivery_startup_);}
        catch(const db_error& error){return std::string_view(error.what())=="pending Native registration was retired or failed";}
        return false;
    }
};
}
namespace {
struct native_registration_result {
    const std::uint64_t creator=lattice::detail::database_native_process();
    int mutation=0,rc=SQLITE_OK;unsigned calls=0;
    bool original=false,after=false,same_budget=false,no_query=false,no_workspace=false,actual_native_entered=false,
         exact_refusal=false,sticky=false,no_fabricated_error=false,public_refused=false;
};
thread_local native_registration_result* native_registration_selected=nullptr;
void native_registration_ready(lattice::database& actual) {
    if(!native_registration_selected||native_registration_selected->creator!=lattice::detail::database_native_process())return;
    auto& result=*native_registration_selected;if(++result.calls!=1)throw std::runtime_error("registration control unexpectedly continued");
    using receipts=lattice::detail::database_initialization_registrations_test_access;
    const auto before=access::inspect(actual);result.original=receipts::current(actual);
    if(result.mutation) {
        result.rc=receipts::replace(actual,result.mutation,result.actual_native_entered);
        result.after=receipts::current(actual);
        result.exact_refusal=receipts::require_retired(actual);
    }else result.after=receipts::current(actual);
    const auto after=access::inspect(actual);
    result.same_budget=before.identity==after.identity&&before.deadline==after.deadline;
    result.no_query=before.statements==after.statements;result.no_workspace=before.workspace==after.workspace;
    result.sticky=after.failed;result.no_fabricated_error=after.first_api_rc==0&&after.cleanup_api_rc==0;
    try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){result.public_refused=true;}
    throw stopped_before_publication{};
}
struct native_registration_hook {
    explicit native_registration_hook(native_registration_result& value) {
        if(native_registration_selected||probe::after_pending||probe::after_functions||owner_binding_probe::after_owner_bound)
            throw std::runtime_error("actual Native registration control already selected");
        native_registration_selected=&value;probe::after_functions=native_registration_ready;
    }
    ~native_registration_hook(){probe::after_functions=nullptr;native_registration_selected=nullptr;}
};
void run_native_registration_control(native_registration_result& result) {
    native_registration_hook hook(result);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(result.calls,1u);EXPECT_TRUE(result.original);EXPECT_TRUE(result.same_budget);
    EXPECT_TRUE(result.no_query);EXPECT_TRUE(result.no_workspace);EXPECT_TRUE(result.no_fabricated_error);EXPECT_TRUE(result.public_refused);
}
TEST(InitializationChannel, ActualCoreVecRegistrationReceiptsKeepOriginalWithoutSQLOrPublication) {
    native_registration_result result;run_native_registration_control(result);
    EXPECT_TRUE(result.after);EXPECT_FALSE(result.sticky);EXPECT_FALSE(result.exact_refusal);
}
TEST(InitializationChannel, ActualCoreAndVecFunctionReplacementRetiresTheirRealSQLiteCookies) {
    for(const int mutation:{1,2}) {
        native_registration_result result;result.mutation=mutation;run_native_registration_control(result);
        EXPECT_EQ(result.rc,SQLITE_OK);EXPECT_TRUE(result.actual_native_entered);EXPECT_FALSE(result.after);EXPECT_TRUE(result.exact_refusal);EXPECT_TRUE(result.sticky);
    }
}
TEST(InitializationChannel, ActualUnusedVecEachRemovalRetiresItsRealModuleCookie) {
    native_registration_result result;result.mutation=3;run_native_registration_control(result);
    EXPECT_EQ(result.rc,SQLITE_OK);EXPECT_TRUE(result.actual_native_entered);EXPECT_FALSE(result.after);EXPECT_TRUE(result.exact_refusal);EXPECT_TRUE(result.sticky);
}
} // namespace


namespace {
struct current_builder_result {
    const std::uint64_t creator=lattice::detail::database_native_process();
    bool foreign=false,entered=false,used=false,exact_columns=false,missing_key=false,
         same_budget=false,monotonic=false,ended=false,public_refused=false,
         retained_after_ending=false,exact_foreign_refusal=false,sticky=false,
         no_fabricated_error=false,marker_exact=false;
    unsigned calls=0;
};
thread_local current_builder_result* current_builder_selected=nullptr;
void current_builder_after_bound(lattice::lattice_db& owner,const owner_binding_probe::owner_binding_observation&) {
    if(!current_builder_selected||current_builder_selected->creator!=lattice::detail::database_native_process())return;
    auto& result=*current_builder_selected;if(++result.calls!=1)throw std::runtime_error("current builder control unexpectedly continued");
    auto& actual=owner.db();const auto original=access::inspect(actual);
    auto seed=access::begin(actual);access::model_machinery(actual);access::setup_actual_native_models(owner);
    lattice::observer_delivery_custody_test_access::set_schema_version(owner,2);
    lattice::observer_delivery_custody_test_access::set_schema_version(owner,3);
    access::model_complete(owner);
    auto columns=access::model_columns(owner,startup_typed_model_declaration().table_name);
    result.exact_columns=columns.size()==4&&columns.count("id")==1&&columns.at("id")=="INTEGER"&&
        columns.at("globalId")=="TEXT"&&columns.at("label")=="TEXT"&&columns.at("count")=="INTEGER";
    try{(void)columns.at("__not_a_declared_column");}catch(const std::out_of_range&){result.missing_key=true;}
    seed.commit();
    result.entered=access::autocommit(actual)&&!access::has_core_original(actual);
    auto retained=std::move(columns);
    if(result.foreign) {
        // The ordinary migration method is genuine; a copied declaration must
        // not be mistaken for the exact immutable catalog member.
        auto copied=startup_typed_model_declaration();auto transaction=access::begin(actual);
        try{lattice::observer_delivery_custody_test_access::migrate_current_model(owner,copied);}
        catch(const lattice::db_error& error){result.exact_foreign_refusal=std::string_view(error.what())==
            "current model no-op cannot adopt a foreign/copied declaration";}
        transaction.rollback();
    }else {
        result.used=lattice::observer_delivery_custody_test_access::prove_current_model_catalog(owner);
        result.ended=access::autocommit(actual)&&!access::has_core_original(actual);
        // Verify the actual marker through its original closed read recipe in
        // a genuine successor. No public SQL purpose is borrowed.
        const auto key=access::actual_marker_key(owner);auto snapshot=access::begin(actual);
        const auto cookie=access::actual_cookie(actual);
        result.marker_exact=access::actual_metadata_value(actual,access::recipe::metadata_marker_read,key)==std::to_string(cookie);
        snapshot.rollback();
    }
    result.retained_after_ending=retained.size()==4&&retained.at("label")=="TEXT"&&retained.count("__not_a_declared_column")==0;
    const auto after=access::inspect(actual);result.same_budget=after.identity==original.identity&&after.deadline==original.deadline;
    result.monotonic=after.workspace>original.workspace&&after.statements>original.statements;
    result.sticky=after.failed;result.no_fabricated_error=after.first_api_rc==0&&after.cleanup_api_rc==0;
    result.ended=access::autocommit(actual)&&!access::has_core_original(actual);
    try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){result.public_refused=true;}
    throw stopped_before_publication{};
}
struct current_builder_hook {
    explicit current_builder_hook(current_builder_result& value) {
        if(current_builder_selected||owner_binding_probe::after_owner_bound||probe::after_pending||probe::after_functions)
            throw std::runtime_error("current builder control already selected");
        current_builder_selected=&value;owner_binding_probe::after_owner_bound=current_builder_after_bound;
    }
    ~current_builder_hook(){owner_binding_probe::after_owner_bound=nullptr;current_builder_selected=nullptr;}
};
void run_current_builder_control(current_builder_result& result) {
    (void)startup_typed_model_declaration();current_builder_hook hook(result);bool stopped=false;
    lattice::configuration config(actual_logical_root());config.target_schema_version=3;
    try{lattice::lattice_db owner(config,true);}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(result.calls,1u);EXPECT_TRUE(result.entered);EXPECT_TRUE(result.exact_columns);
    EXPECT_TRUE(result.missing_key);EXPECT_TRUE(result.same_budget);EXPECT_TRUE(result.monotonic);EXPECT_TRUE(result.ended);
    EXPECT_TRUE(result.retained_after_ending);EXPECT_TRUE(result.public_refused);EXPECT_TRUE(result.no_fabricated_error);
}
TEST(InitializationChannel, ActualCurrentCatalogProofRetainsBorrowedColumnsAcrossKnownEndingWithoutPublishing) {
    current_builder_result result;run_current_builder_control(result);
    EXPECT_TRUE(result.used);EXPECT_TRUE(result.marker_exact);EXPECT_FALSE(result.sticky);
}
TEST(InitializationChannel, ActualCurrentModelNoOpRefusesCopiedDeclarationAndKeepsPrepaidRollback) {
    current_builder_result result;result.foreign=true;run_current_builder_control(result);
    EXPECT_TRUE(result.exact_foreign_refusal);EXPECT_TRUE(result.sticky);EXPECT_FALSE(result.used);
}
} // namespace

namespace {
enum class data_account_mode {monotone,workspace_denial,actual_auth};
struct data_account_result {
    const std::uint64_t creator=lattice::detail::database_native_process();
    data_account_mode mode=data_account_mode::monotone;
    unsigned calls=0,auth_hits=0;
    bool same=false,exact_delta=false,exact_denial=false,sticky=false,raw_rollback=false,
         auth_observed=false,first_auth=false,no_cleanup_error=false,public_refused=false;
    std::shared_ptr<lattice::detail::database_initialization_budget> retained;
    const void* budget_identity=nullptr,*account_identity=nullptr;
    std::chrono::steady_clock::time_point end{};
    std::uint64_t statements=0,workspace=0;
    lattice::detail::database_initialization_budget::limits maximum{};
    int first_rc=SQLITE_OK,cleanup_rc=SQLITE_OK;
};
thread_local data_account_result* data_account_selected=nullptr;
int data_account_auth(void* user,int action,const char*,const char*,const char*,const char*)noexcept {
    auto* result=static_cast<data_account_result*>(user);
    if(!result||result->creator!=lattice::detail::database_native_process())return SQLITE_DENY;
    if(action==SQLITE_SELECT){++result->auth_hits;return SQLITE_DENY;}
    return SQLITE_OK;
}
struct data_account_auth_role {
    lattice::database& actual;data_account_result& result;bool bound=false;
    data_account_auth_role(lattice::database& db,data_account_result& data):actual(db),result(data) {
        const auto rc=access::set_receive_test_role(actual,data_account_auth,&result);
        if(rc!=SQLITE_OK)throw std::runtime_error("actual account authorizer role failed");
        bound=true;
    }
    void clear() {
        if(!bound)return;
        const auto rc=access::clear_receive_test_role(actual,&result);
        if(rc!=SQLITE_OK)throw std::runtime_error("actual account authorizer clear failed");
        bound=false;
    }
    ~data_account_auth_role()noexcept {
        if(!bound||result.creator!=lattice::detail::database_native_process())return;
        // Do not continue after unknown authorizer custody; preserve the store.
        if(access::clear_receive_test_role(actual,&result)!=SQLITE_OK)std::_Exit(97);
    }
};
void data_account_after_functions(lattice::database& actual) {
    if(!data_account_selected||data_account_selected->creator!=lattice::detail::database_native_process())return;
    auto& result=*data_account_selected;
    if(++result.calls!=1)throw std::runtime_error("actual DATA account control unexpectedly continued");
    const auto before=access::inspect(actual);result.retained=access::retain_account_budget(actual);
    result.budget_identity=before.identity;result.account_identity=before.account_identity;result.end=before.deadline;result.maximum=before.maximum;
    if(result.mode==data_account_mode::monotone) {
        access::account_workspace(actual,7);access::account_statements(actual,3);
        const auto after=access::inspect(actual);
        result.exact_delta=after.workspace==before.workspace+7&&after.statements==before.statements+3;
    }else if(result.mode==data_account_mode::workspace_denial) {
        // Deliberate DATA exhaustion of the same genuinely admitted account;
        // no inferred setup receipt, copied counter, deadline or new credit.
        access::account_workspace(actual,before.maximum.workspace_bytes-before.workspace);
        try{access::account_workspace(actual,1);}
        catch(const lattice::db_error& error){result.exact_denial=std::string_view(error.what())==
            "initialization aggregate workspace exhausted before copy";}
        const auto after=access::inspect(actual);
        result.exact_delta=after.workspace==before.maximum.workspace_bytes&&after.statements==before.statements;
    }else {
        auto transaction=access::begin(actual); // Real ending reserve before AUTH.
        {
            data_account_auth_role restriction(actual,result);
            try{(void)access::count(actual);}
            catch(const lattice::db_error&){result.auth_observed=true;}
            restriction.clear();
        }
        const auto failed=access::inspect(actual);
        result.first_auth=failed.first_api_rc==SQLITE_AUTH&&result.auth_hits>0;
        transaction.rollback();result.raw_rollback=access::autocommit(actual);
    }
    const auto after=access::inspect(actual);
    result.same=after.identity==before.identity&&after.account_identity==before.account_identity&&
        before.account_identity&&after.deadline==before.deadline;
    result.sticky=after.failed;result.no_cleanup_error=after.cleanup_api_rc==SQLITE_OK;
    if(result.mode!=data_account_mode::actual_auth)result.first_auth=after.first_api_rc==SQLITE_OK;
    result.statements=after.statements;result.workspace=after.workspace;
    result.first_rc=after.first_api_rc;result.cleanup_rc=after.cleanup_api_rc;
    try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){result.public_refused=true;}
    throw stopped_before_publication{};
}
struct data_account_hook {
    explicit data_account_hook(data_account_result& result) {
        if(data_account_selected||probe::after_pending||probe::after_functions)
            throw std::runtime_error("actual DATA account control is already selected");
        data_account_selected=&result;probe::after_functions=data_account_after_functions;
    }
    ~data_account_hook(){probe::after_functions=nullptr;data_account_selected=nullptr;}
};
void run_data_account_control(data_account_result& result) {
    {
        data_account_hook hook(result);bool stopped=false;
        try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
        ASSERT_TRUE(stopped);ASSERT_EQ(result.calls,1u);
    } // The actual constructor has unwound and closed its real Native owners.
    ASSERT_TRUE(result.retained);const auto after=access::retained_marker_budget_data(*result.retained);
    EXPECT_EQ(after.identity,result.budget_identity);EXPECT_EQ(after.account_identity,result.account_identity);
    EXPECT_NE(after.account_identity,nullptr);EXPECT_EQ(after.deadline,result.end);
    EXPECT_EQ(after.statements,result.statements);EXPECT_EQ(after.workspace,result.workspace);
    EXPECT_EQ(after.maximum.statements,result.maximum.statements);EXPECT_EQ(after.maximum.workspace_bytes,result.maximum.workspace_bytes);
    EXPECT_EQ(after.maximum.columns,result.maximum.columns);EXPECT_EQ(after.maximum.rows,result.maximum.rows);
    EXPECT_EQ(after.first_api_rc,result.first_rc);EXPECT_EQ(after.cleanup_api_rc,result.cleanup_rc);EXPECT_EQ(after.failed,result.sticky);
    EXPECT_TRUE(result.same);EXPECT_TRUE(result.no_cleanup_error);EXPECT_TRUE(result.public_refused);
    // Actual credit remains pinned by this original retained budget until this
    // final DATA hold ends through the unchanged managed-return deleter.
    result.retained.reset();
}
TEST(InitializationChannel, ActualDataAccountKeepsIdentityAndMonotoneDebitsAfterOwnerUnwinds) {
    data_account_result result;run_data_account_control(result);
    EXPECT_TRUE(result.exact_delta);EXPECT_FALSE(result.sticky);EXPECT_TRUE(result.first_auth);
}
TEST(InitializationChannel, ActualDataAccountWorkspaceExhaustionStaysFailedWithoutResetOrNativeError) {
    data_account_result result;result.mode=data_account_mode::workspace_denial;run_data_account_control(result);
    EXPECT_TRUE(result.exact_denial);EXPECT_TRUE(result.exact_delta);EXPECT_TRUE(result.sticky);EXPECT_TRUE(result.first_auth);
}
TEST(InitializationChannel, ActualDataAccountPreservesRealFirstAuthAcrossReservedRollbackAndOwnerUnwind) {
    data_account_result result;result.mode=data_account_mode::actual_auth;run_data_account_control(result);
    EXPECT_GT(result.auth_hits,0u);EXPECT_TRUE(result.auth_observed);EXPECT_TRUE(result.first_auth);
    EXPECT_TRUE(result.raw_rollback);EXPECT_TRUE(result.sticky);
}
} // namespace

#include <utility>

namespace {
using typed_bootstrap_inspection=decltype(access::inspect(std::declval<lattice::database&>()));
struct typed_bootstrap_result {
    const std::uint64_t creator=lattice::detail::database_native_process();
    unsigned calls=0;
    typed_bootstrap_inspection observed{};
    std::shared_ptr<lattice::detail::database_initialization_budget> retained;
};
thread_local typed_bootstrap_result* typed_bootstrap_selected=nullptr;
void typed_bootstrap_after_functions(lattice::database& actual) {
    if(!typed_bootstrap_selected||typed_bootstrap_selected->creator!=lattice::detail::database_native_process())return;
    auto& result=*typed_bootstrap_selected;
    if(++result.calls!=1)throw std::runtime_error("typed bootstrap control unexpectedly continued");
    result.observed=access::inspect(actual);result.retained=access::retain_account_budget(actual);
    throw stopped_before_publication{};
}
struct typed_bootstrap_hook {
    explicit typed_bootstrap_hook(typed_bootstrap_result& result) {
        if(typed_bootstrap_selected||probe::after_pending||probe::after_functions||
           probe::fail_once!=probe::allocation_stage::none)
            throw std::runtime_error("typed bootstrap control already selected");
        typed_bootstrap_selected=&result;probe::after_functions=typed_bootstrap_after_functions;
    }
    ~typed_bootstrap_hook(){probe::after_functions=nullptr;typed_bootstrap_selected=nullptr;probe::fail_once=probe::allocation_stage::none;}
};
void capture_actual_bootstrap(typed_bootstrap_result& result) {
    typed_bootstrap_hook hook(result);bool stopped=false;
    try{construct_actual_owner();}catch(const stopped_before_publication&){stopped=true;}
    ASSERT_TRUE(stopped);ASSERT_EQ(result.calls,1u);ASSERT_TRUE(result.retained);
}
TEST(InitializationChannel, ActualTypedAdmissionEmbedsOneAccountAndChargesRealBootstrapRequestsBeforeCopies) {
    typed_bootstrap_result result;capture_actual_bootstrap(result);ASSERT_TRUE(result.retained);
    const auto& before=result.observed;
    const auto after=access::retained_marker_budget_data(*result.retained);
    ASSERT_NE(before.record_identity,nullptr);ASSERT_GT(before.record_bytes,0u);
    const auto record=reinterpret_cast<std::uintptr_t>(before.record_identity);
    const auto account=reinterpret_cast<std::uintptr_t>(before.account_identity);
    const auto budget=reinterpret_cast<std::uintptr_t>(before.identity);
    ASSERT_GE(account,record);ASSERT_GE(budget,record);
    EXPECT_LT(account-record,before.record_bytes);EXPECT_LT(budget-record,before.record_bytes);
    EXPECT_GE(before.bootstrap_requests,2u);EXPECT_GT(before.bootstrap_bytes,before.record_bytes);
    EXPECT_GE(before.workspace,before.bootstrap_bytes);EXPECT_LE(before.workspace,before.maximum.workspace_bytes);
    EXPECT_EQ(before.domain_snapshot.code,lattice::detail::observer_delivery_domain::status::admitted);
    EXPECT_GE(before.domain_snapshot.used.count,before.maximum.statements);
    EXPECT_GE(before.domain_snapshot.used.bytes,before.maximum.workspace_bytes);
    EXPECT_EQ(after.record_identity,before.record_identity);EXPECT_EQ(after.identity,before.identity);
    EXPECT_EQ(after.account_identity,before.account_identity);EXPECT_EQ(after.deadline,before.deadline);
    EXPECT_EQ(after.bootstrap_bytes,before.bootstrap_bytes);EXPECT_EQ(after.bootstrap_requests,before.bootstrap_requests);
    EXPECT_EQ(after.workspace,before.workspace);EXPECT_EQ(after.statements,before.statements);
    EXPECT_EQ(after.first_api_rc,SQLITE_OK);EXPECT_EQ(after.cleanup_api_rc,SQLITE_OK);EXPECT_FALSE(after.failed);
}
void check_actual_bootstrap_allocation_failure(probe::allocation_stage stage) {
    typed_bootstrap_result keeper;capture_actual_bootstrap(keeper);ASSERT_TRUE(keeper.retained);
    const auto baseline=access::retained_marker_budget_data(*keeper.retained);
    ASSERT_EQ(probe::fail_once,probe::allocation_stage::none);
    struct clear_cut {~clear_cut(){probe::fail_once=probe::allocation_stage::none;}} reset;
    probe::fail_once=stage;bool allocation_refused=false;
    try{construct_actual_owner();}catch(const std::bad_alloc&){allocation_refused=true;}
    ASSERT_TRUE(allocation_refused);ASSERT_EQ(probe::fail_once,probe::allocation_stage::none);
    // The fixture installs at least two real clients. The keeper is actual
    // lease-only custody; the second real constructor must reach THIS fault,
    // really unwind, and return all of its client/prospective-credit metadata.
    const auto after=access::retained_marker_budget_data(*keeper.retained);
    EXPECT_EQ(after.domain_snapshot.used,baseline.domain_snapshot.used);
    EXPECT_EQ(after.domain_snapshot.burst_used,baseline.domain_snapshot.burst_used);
    EXPECT_EQ(after.domain_snapshot.clients,baseline.domain_snapshot.clients);
    EXPECT_EQ(after.domain_snapshot.waiters,baseline.domain_snapshot.waiters);
    EXPECT_EQ(after.domain_snapshot.leases,baseline.domain_snapshot.leases);
    EXPECT_EQ(after.account_identity,baseline.account_identity);EXPECT_EQ(after.record_identity,baseline.record_identity);
    EXPECT_EQ(after.workspace,baseline.workspace);EXPECT_EQ(after.statements,baseline.statements);
    EXPECT_EQ(after.deadline,baseline.deadline);EXPECT_FALSE(after.failed);
    EXPECT_EQ(after.first_api_rc,SQLITE_OK);EXPECT_EQ(after.cleanup_api_rc,SQLITE_OK);
    keeper.retained.reset();
    typed_bootstrap_result retry;capture_actual_bootstrap(retry);ASSERT_TRUE(retry.retained);
    const auto current=access::retained_marker_budget_data(*retry.retained);
    EXPECT_FALSE(current.failed);EXPECT_GE(current.bootstrap_requests,2u);
    EXPECT_EQ(current.first_api_rc,SQLITE_OK);EXPECT_EQ(current.cleanup_api_rc,SQLITE_OK);
}
TEST(InitializationChannel, ActualTypedAdmissionRecordAllocationFailureReturnsProspectiveCustodyAndAllowsRetry) {
    check_actual_bootstrap_allocation_failure(probe::allocation_stage::bootstrap_record);
}
TEST(InitializationChannel, ActualTypedAdmissionControlAllocationFailureUsesEmbeddedManagedReturnAndAllowsRetry) {
    check_actual_bootstrap_allocation_failure(probe::allocation_stage::bootstrap_control);
}
} // namespace

TEST(InitializationChannel, ActualTypedAdmissionPostHandoffFailureKeepsCellThroughRealReturnAndAllowsRetry) {
    check_actual_bootstrap_allocation_failure(probe::allocation_stage::bootstrap_handoff);
}


namespace {
// Dedicated installed Host/account fixture selectors. Each path MUST name a
// newly permitted, absent member in a disposable real approved root. Existing
// data is never removed/reset, and missing/mismatched setup is a hard failure.
// These controls prove original transport/ending only, not complete activation.
std::string normal_creation_fixture_path(const char* variable) {
    const auto* value=std::getenv(variable);if(!value||value[0]!='/')throw std::runtime_error("actual normal creation fixture path is required");
    const auto count=std::strlen(value);if(!count||count>4096)throw std::runtime_error("actual normal creation fixture path bound");
    return std::string(value,count);
}
struct normal_creation_stop {};
struct normal_creation_observation {
    const std::uint64_t creator=lattice::detail::database_native_process();
    unsigned calls=0;bool fresh=false,public_refused=false;const void* payload=nullptr;
};
thread_local normal_creation_observation* normal_creation_selected=nullptr;
void normal_creation_after_functions(lattice::database& actual) {
    if(!normal_creation_selected||normal_creation_selected->creator!=lattice::detail::database_native_process())
        throw std::runtime_error("actual normal creation observation lost its original");
    auto& result=*normal_creation_selected;if(++result.calls!=1)throw std::runtime_error("actual normal creation fixture continued after stop");
    result.payload=access::normal_owned_payload_identity(actual);result.fresh=access::actual_normal_fresh(actual);
    try{(void)actual.query("SELECT 1");}catch(const lattice::db_error&){result.public_refused=true;}
    throw normal_creation_stop{};
}
struct normal_creation_hook {
    normal_creation_observation& result;
    explicit normal_creation_hook(normal_creation_observation& actual):result(actual) {
        if(normal_creation_selected||probe::after_pending||probe::after_functions)
            throw std::runtime_error("actual normal creation fixture hook is already occupied");
        normal_creation_selected=&result;probe::after_functions=normal_creation_after_functions;
    }
    ~normal_creation_hook(){if(result.creator==lattice::detail::database_native_process()){
        probe::after_functions=nullptr;normal_creation_selected=nullptr;}}
};
struct actual_normal_factory_owner final:lattice::lattice_db {
    actual_normal_factory_owner(const lattice::configuration& config,lattice::detail::store_resource_initialization_original& original)
        :lattice_db(config,true,lattice::detail::recovery_owner_schema::capture_native(),{},{},true,&original){}
};
void normal_construct_factory_stopped(const lattice::configuration& config,
    lattice::detail::store_resource_initialization_original& original,normal_creation_observation& result) {
    normal_creation_hook hook(result);bool stopped=false;
    try{actual_normal_factory_owner actual(config,original);}catch(const normal_creation_stop&){stopped=true;}
    ASSERT_TRUE(stopped);EXPECT_EQ(result.calls,1u);EXPECT_NE(result.payload,nullptr);EXPECT_TRUE(result.public_refused);
}
void normal_construct_direct_stopped(const lattice::configuration& config,normal_creation_observation& result) {
    normal_creation_hook hook(result);bool stopped=false;
    try{lattice::lattice_db actual(config,true);}catch(const normal_creation_stop&){stopped=true;}
    ASSERT_TRUE(stopped);EXPECT_EQ(result.calls,1u);EXPECT_NE(result.payload,nullptr);EXPECT_TRUE(result.public_refused);
}
}
TEST(InitializationChannel, ActualNormalFactoryMovesOneCreationOriginalThroughNativeAndCannotRecreateItFromReady) {
    const auto path=normal_creation_fixture_path("LATTICE_RESOURCE_NORMAL_CREATION_PATH_FACTORY");
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
    {
        lattice::detail::store_resource_factory_admission factory(path,true,end);
        auto original=access::normal_factory_builder(factory);ASSERT_TRUE(access::normal_transport_current(original));
        const auto* identity=access::normal_payload_identity(original);ASSERT_NE(identity,nullptr);
        lattice::configuration config(factory.actual_path());normal_creation_observation first;
        normal_construct_factory_stopped(config,original,first);
        EXPECT_TRUE(first.fresh);EXPECT_EQ(first.payload,identity);EXPECT_FALSE(access::normal_transport_current(original));
        // The SAME consumed original cannot reach another Native builder. It is
        // not reset/refreshed or recreated from the still-present member record.
        normal_creation_observation reused;normal_creation_hook hook(reused);
        EXPECT_THROW((void)actual_normal_factory_owner(config,original),std::exception);EXPECT_EQ(reused.calls,0u);
    } // Actual Native wrappers, creation descriptions and factory guards ended.
    {
        lattice::detail::store_resource_factory_admission factory(path,true,end);
        auto original=access::normal_factory_builder(factory);lattice::configuration config(factory.actual_path());
        normal_creation_observation ready;normal_construct_factory_stopped(config,original,ready);
        EXPECT_FALSE(ready.fresh);EXPECT_FALSE(access::normal_transport_current(original));
    }
}
TEST(InitializationChannel, ActualNormalDirectPendingWriterCapturesItsOwnCreatorButReadyReopenHasNoFreshOriginal) {
    const auto path=normal_creation_fixture_path("LATTICE_RESOURCE_NORMAL_CREATION_PATH_DIRECT");
    lattice::configuration config(path);normal_creation_observation first;
    normal_construct_direct_stopped(config,first);EXPECT_TRUE(first.fresh);
    normal_creation_observation ready;normal_construct_direct_stopped(config,ready);EXPECT_FALSE(ready.fresh);
}

#include "../../Sources/LatticeCore/src/observer_delivery_owner_data_test_probe.hpp"
namespace {
using prepared_probe=lattice::detail::observer_delivery_owner_data_test_probe;
struct prepared_owner_data_stop {};
struct prepared_owner_control {
    const std::uint64_t creator=lattice::detail::database_native_process();
    unsigned observations=0,copies=0,calls=0,live_targets=0;
    bool stop=false;
    prepared_probe::observation seen{};
};
struct prepared_owner_callback {
    prepared_owner_control* control;int marker;
    prepared_owner_callback(prepared_owner_control& value,int key):control(&value),marker(key){++control->live_targets;}
    prepared_owner_callback(const prepared_owner_callback& other):control(other.control),marker(other.marker) {
        ++control->copies;++control->live_targets;
    }
    ~prepared_owner_callback(){--control->live_targets;}
    void operator()(lattice::migration_context&)const {++control->calls;}
};
thread_local prepared_owner_control* prepared_owner_selected=nullptr;
void prepared_owner_after_data(const prepared_probe::observation& value) {
    if(!prepared_owner_selected||prepared_owner_selected->creator!=lattice::detail::database_native_process())return;
    auto& control=*prepared_owner_selected;++control.observations;control.seen=value;
    if(control.stop)throw prepared_owner_data_stop{};
}
struct prepared_owner_hook {
    explicit prepared_owner_hook(prepared_owner_control& value) {
        if(prepared_owner_selected||prepared_probe::after_data)throw std::runtime_error("prepared owner control already selected");
        prepared_owner_selected=&value;prepared_probe::after_data=prepared_owner_after_data;
    }
    ~prepared_owner_hook(){prepared_probe::after_data=nullptr;prepared_owner_selected=nullptr;}
};
void expect_actual_prepared_data(const prepared_owner_control& control) {
    ASSERT_EQ(control.observations,1u);EXPECT_EQ(control.seen.creator,control.creator);
    EXPECT_TRUE(control.seen.writer_absent);EXPECT_TRUE(control.seen.reader_absent);
    EXPECT_TRUE(control.seen.cross_process_reader_absent);EXPECT_TRUE(control.seen.catalog_present);
    EXPECT_TRUE(control.seen.scheduler_present);EXPECT_TRUE(control.seen.guard_present);
    EXPECT_TRUE(control.seen.callback_mutex_present);EXPECT_TRUE(control.seen.core_registries_empty);
    EXPECT_TRUE(control.seen.migration_callback_present);
}
TEST(InitializationChannel, ActualPreparedOwnerDataPrecedesNativeAndRetainsAllowedLegacyMigrationCallback) {
    prepared_owner_control control;
    lattice::configuration config(":memory:");config.migration_block=prepared_owner_callback(control,17);
    prepared_owner_hook hook(control);
    {
        lattice::lattice_db owner(config,true);expect_actual_prepared_data(control);
        const auto* callback=owner.config().migration_block.target<prepared_owner_callback>();
        ASSERT_NE(callback,nullptr);EXPECT_EQ(callback->marker,17);EXPECT_EQ(callback->control,&control);
        const auto calls=control.calls;lattice::migration_context context(owner.db());
        owner.config().migration_block(context);EXPECT_EQ(control.calls,calls+1);
        const auto rows=owner.db().query("SELECT 42 AS prepared_value");
        ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::int64_t>(rows.front().at("prepared_value")),42);
    }
    EXPECT_GT(control.copies,0u);EXPECT_EQ(control.live_targets,1u); // original input target only
}
TEST(InitializationChannel, ActualPreparedDataFailureUnwindsCallbackTargetsBeforeNativeAndAllowsRetry) {
    prepared_owner_control control;
    lattice::configuration config(":memory:");config.migration_block=prepared_owner_callback(control,29);
    const auto input_targets=control.live_targets;control.stop=true;bool stopped=false;
    {
        prepared_owner_hook hook(control);
        try{lattice::lattice_db owner(config,true);}catch(const prepared_owner_data_stop&){stopped=true;}
    }
    ASSERT_TRUE(stopped);expect_actual_prepared_data(control);EXPECT_EQ(control.live_targets,input_targets);
    control.stop=false;control.observations=0;
    {
        prepared_owner_hook hook(control);lattice::lattice_db owner(config,true);expect_actual_prepared_data(control);
        const auto rows=owner.db().query("SELECT 42 AS prepared_value");
        ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::int64_t>(rows.front().at("prepared_value")),42);
    }
    EXPECT_EQ(control.live_targets,input_targets);
}
} // namespace
