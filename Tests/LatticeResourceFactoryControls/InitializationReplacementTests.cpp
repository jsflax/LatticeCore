#include <gtest/gtest.h>
#include <lattice/lattice.hpp>
#include <lattice/store_physical_api.hpp>
#include <lattice/detail/observer_delivery_startup.hpp>
#include "../../Sources/LatticeCore/src/observer_delivery_startup_test_probe.hpp"
#include "../../Sources/LatticeCore/src/store_physical_api.hpp"
#include <algorithm>
#include <cstring>
#include "../LatticeCoreTests/FixtureCheckedOwnerEnding.hpp"
#include <deque>
#include <mutex>
#include <utility>
#if defined(__APPLE__) || defined(__linux__)
#include <pwd.h>
#include <unistd.h>
#endif
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
// A real component model registration gives the ordinary PUBLIC factory its
// complete nonempty Native catalog. No protected constructor, filtered global
// registry, fake enabled flag or test-supplied recovery declaration is used.
struct InitializationReplacementModel {std::string text;std::int64_t value=0;};
LATTICE_SCHEMA(InitializationReplacementModel,text,value);
namespace lattice::detail {
struct observer_delivery_startup_test_access final {
    static auto inspect(const lattice_db& owner)noexcept{return observer_delivery_startup::inspect_for_test_(owner);}
    using returned=store_physical_api::returned_tail_observation;
    static bool observe_returns(returned* value)noexcept {
        if(value&&store_physical_api::returned_tail_observation_)return false;
        store_physical_api::returned_tail_observation_=value;return true;
    }
};
}
namespace {
using namespace lattice;
using namespace lattice::detail;
struct Queue final:scheduler {
    const std::thread::id thread=std::this_thread::get_id();
    std::mutex leaf;std::deque<std::function<void()>> pending;
    void invoke(std::function<void()>&& work)override{std::lock_guard lock(leaf);pending.push_back(std::move(work));}
    bool is_on_thread()const noexcept override{return std::this_thread::get_id()==thread;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void drain(){for(;;){std::function<void()> work;{std::lock_guard lock(leaf);if(pending.empty())return;work=std::move(pending.front());pending.pop_front();}work();}}
    void clear(){std::deque<std::function<void()>> retired;{std::lock_guard lock(leaf);retired.swap(pending);}}
};
std::string logical_root(){
#if defined(__APPLE__) || defined(__linux__)
    struct passwd data{},*found=nullptr;char bytes[16384];
    if(::getpwuid_r(::geteuid(),&data,bytes,sizeof(bytes),&found)!=0||!found||!found->pw_dir||found->pw_dir[0]!='/')
        throw std::runtime_error("actual account home unavailable");
    return std::string(found->pw_dir)+"/.claude/memory.sqlite";
#else
    throw std::runtime_error("actual hosted factory is unsupported");
#endif
}
struct Owner {
    std::shared_ptr<Queue> queue=std::make_shared<Queue>();
    std::unique_ptr<lattice_db> value;
    Owner(){
        try{
            configuration config(logical_root());config.sched=queue;config.audit_retention_seconds=0;
            value=std::make_unique<lattice_db>(config,true);
            // Stop/join the real notifier before measuring the SAME actual Store;
            // remaining main-queue callbacks execute before the baseline sample.
            instance_registry::instance().get_or_create_notifier(value->config().path)->stop_listening();queue->drain();
        }catch(...){
            if(value)end_fixture_owner_checked(value.get(),"initialization-replacement-constructor");
            queue->clear();value.reset();throw;
        }
    }
    ~Owner(){if(value)end_fixture_owner_checked(value.get(),"initialization-replacement");queue->clear();value.reset();}
};
struct admitted_stop {};
struct Observation {
    lattice_db* owner=nullptr;
    bool called=false,caught=false,unexpected=false,admitted=false;
    std::uint64_t admission_calls=0;
    const void* identity=nullptr;
    std::uint64_t before_count=0,before_bytes=0,before_leases=0,
        admitted_count=0,admitted_bytes=0,admitted_leases=0,
        caught_count=0,caught_bytes=0,caught_leases=0;
};
thread_local Observation* selected=nullptr;
void stop_after_actual_admission(const void* identity){
    if(!selected||!selected->owner)throw std::runtime_error("replacement DATA observation absent");
    auto actual=observer_delivery_startup_test_access::inspect(*selected->owner);
    if(!actual.current)throw std::runtime_error("actual published Store observation absent");
    selected->admitted=true;selected->identity=identity;++selected->admission_calls;
    selected->admitted_count=actual.count;selected->admitted_bytes=actual.bytes;selected->admitted_leases=actual.leases;
    throw admitted_stop{}; // BEFORE any new physical-open/budget binding.
}
struct Probe {
    explicit Probe(Observation& data){
        if(selected||observer_delivery_startup_test_probe::after_replacement_admitted)throw std::runtime_error("replacement probe already selected");
        selected=&data;observer_delivery_startup_test_probe::after_replacement_admitted=stop_after_actual_admission;
    }
    ~Probe(){observer_delivery_startup_test_probe::after_replacement_admitted=nullptr;selected=nullptr;}
};
struct Memory {
    sqlite3* value=nullptr;
    ~Memory(){if(value){const auto rc=store_physical_close(std::exchange(value,nullptr));
        if(rc!=SQLITE_OK){std::fputs("physical-fixture-ending-debt:replacement-memory-close\n",stderr);::_exit(96);}}}
};
struct Statement {
    sqlite3_stmt* value=nullptr;
    ~Statement(){if(value){const auto rc=store_physical_finalize(std::exchange(value,nullptr));
        if(rc!=SQLITE_OK){std::fputs("physical-fixture-ending-debt:replacement-statement-finalize\n",stderr);::_exit(96);}}}
};
}
TEST(InitializationReplacement, GenuineUnboundSetupCreditRetiresOnlyAfterActualMemoryWrapperReturn){
    Owner owner;
    for(const bool use_exec:{true,false}){
        Observation data;data.owner=owner.value.get(); // UDF DATA outlives its actual registration.
        const auto before=observer_delivery_startup_test_access::inspect(*data.owner);
        ASSERT_TRUE(before.current);data.before_count=before.count;data.before_bytes=before.bytes;data.before_leases=before.leases;
        Memory memory;
        ASSERT_EQ(store_physical_open_v2(":memory:",&memory.value,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_FULLMUTEX,nullptr),SQLITE_OK);
        ASSERT_EQ(store_physical_create_function_v2(memory.value,"actual_replacement_failure",0,SQLITE_UTF8,&data,
            [](sqlite3_context* result,int,sqlite3_value**){
                auto& observed=*static_cast<Observation*>(sqlite3_user_data(result));observed.called=true;
                try{observed.owner->reopen_read_db();observed.unexpected=true;}
                catch(const admitted_stop&){observed.caught=true;}
                catch(...){observed.unexpected=true;}
                const auto actual=observer_delivery_startup_test_access::inspect(*observed.owner);
                if(!actual.current){sqlite3_result_error(result,"actual replacement Store disappeared",-1);return;}
                observed.caught_count=actual.count;observed.caught_bytes=actual.bytes;observed.caught_leases=actual.leases;
                sqlite3_result_int(result,observed.caught&&!observed.unexpected?1:0);
            },nullptr,nullptr,nullptr),SQLITE_OK);
        {Probe observation(data);
            if(use_exec){ASSERT_EQ(store_physical_exec(memory.value,"SELECT actual_replacement_failure()",nullptr,nullptr,nullptr),SQLITE_OK);}
            else{Statement statement;ASSERT_EQ(store_physical_prepare_v2(memory.value,"SELECT actual_replacement_failure()",-1,&statement.value,nullptr),SQLITE_OK);
                ASSERT_EQ(store_physical_step(statement.value),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int(statement.value,0),1);
                const auto returned=observer_delivery_startup_test_access::inspect(*data.owner);
                EXPECT_EQ(returned.leases,before.leases); // Real step returned; VM may still be busy.
            }
        }
        const auto after=observer_delivery_startup_test_access::inspect(*data.owner);
        EXPECT_TRUE(data.called);EXPECT_TRUE(data.admitted);EXPECT_TRUE(data.caught);EXPECT_FALSE(data.unexpected);EXPECT_NE(data.identity,nullptr);
        EXPECT_EQ(data.admitted_leases,before.leases+1);EXPECT_GT(data.admitted_count,before.count);EXPECT_GT(data.admitted_bytes,before.bytes);
        EXPECT_EQ(data.caught_leases,data.admitted_leases);EXPECT_EQ(data.caught_count,data.admitted_count);EXPECT_EQ(data.caught_bytes,data.admitted_bytes);
        ASSERT_TRUE(after.current);EXPECT_EQ(after.leases,before.leases);EXPECT_EQ(after.count,before.count);EXPECT_EQ(after.bytes,before.bytes);
    }
}

namespace {
struct ReturnObservation {
    const std::uint64_t creator=database_native_process();
    observer_delivery_startup_test_access::returned data;
    bool selected=observer_delivery_startup_test_access::observe_returns(&data);
    ~ReturnObservation(){if(selected&&creator==database_native_process())observer_delivery_startup_test_access::observe_returns(nullptr);}
};
struct RefusalObservation {
    Observation admission;
    observer_delivery_startup_test_access::returned* returns=nullptr;
    std::uint64_t maximum_admitted=0,refused=0;
    bool called=false,unexpected=false,first_refusal=false;
    std::uint64_t held_count=0,held_bytes=0,held_leases=0,queued_inside=0,delete_calls_inside=0;
};
}
TEST(InitializationReplacement, RepeatedActualRefusedSetupShellsDoNotJoinManagedReturnCensus){
    Owner owner;
    for(const bool use_exec:{true,false}){
        ReturnObservation returns;
        ASSERT_TRUE(returns.selected);
        RefusalObservation data;data.admission.owner=owner.value.get();data.returns=&returns.data;
        const auto before=observer_delivery_startup_test_access::inspect(*data.admission.owner);
        ASSERT_TRUE(before.current);ASSERT_GT(before.setup_count,0u);ASSERT_GT(before.setup_bytes,0u);
        data.maximum_admitted=std::min(before.client_count_ceiling/before.setup_count,
            before.client_byte_ceiling/before.setup_bytes);
        // This genuine Host control needs a finite small test-role profile.
        // Larger production policies remain valid; this test never loops an
        // unbounded configured declaration or substitutes a fake budget.
        ASSERT_GT(data.maximum_admitted,0u);ASSERT_LE(data.maximum_admitted,64u);
        Memory memory;
        ASSERT_EQ(store_physical_open_v2(":memory:",&memory.value,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_FULLMUTEX,nullptr),SQLITE_OK);
        ASSERT_EQ(store_physical_create_function_v2(memory.value,"actual_repeated_replacement_refusal",0,SQLITE_UTF8,&data,
            [](sqlite3_context* result,int,sqlite3_value**){
                auto& observed=*static_cast<RefusalObservation*>(sqlite3_user_data(result));observed.called=true;
                const auto attempt=[&] {
                    try{observed.admission.owner->reopen_read_db();observed.unexpected=true;}
                    catch(const admitted_stop&){}
                    catch(const db_error& error){
                        if(std::strcmp(error.what(),"initialization credit refused before first SQL")!=0)observed.unexpected=true;
                        else{++observed.refused;observed.first_refusal=true;}
                    }
                    catch(...){observed.unexpected=true;}
                };
                // Every successful offer owns one genuine admitted budget on
                // this B stack. Exhaustion is derived from actual declarations.
                for(std::uint64_t n=0;n<=observed.maximum_admitted&&!observed.first_refusal&&!observed.unexpected;++n)attempt();
                const auto held=observer_delivery_startup_test_access::inspect(*observed.admission.owner);
                observed.held_count=held.count;observed.held_bytes=held.bytes;observed.held_leases=held.leases;
                if(!held.current||!observed.first_refusal||!observed.admission.admission_calls)observed.unexpected=true;
                const auto admitted=observed.admission.admission_calls;
                for(unsigned n=0;n<32&&!observed.unexpected;++n){
                    const auto refusals=observed.refused;attempt();
                    const auto now=observer_delivery_startup_test_access::inspect(*observed.admission.owner);
                    if(observed.refused!=refusals+1||observed.admission.admission_calls!=admitted||
                       !now.current||now.count!=held.count||now.bytes!=held.bytes||now.leases!=held.leases)
                        observed.unexpected=true;
                }
                observed.queued_inside=observed.returns->budget_enqueued;
                observed.delete_calls_inside=observed.returns->budget_delete_calls;
                sqlite3_result_int(result,observed.first_refusal&&!observed.unexpected?1:0);
            },nullptr,nullptr,nullptr),SQLITE_OK);
        {Probe observation(data.admission);
            if(use_exec){ASSERT_EQ(store_physical_exec(memory.value,"SELECT actual_repeated_replacement_refusal()",nullptr,nullptr,nullptr),SQLITE_OK);}
            else{Statement statement;ASSERT_EQ(store_physical_prepare_v2(memory.value,"SELECT actual_repeated_replacement_refusal()",-1,&statement.value,nullptr),SQLITE_OK);
                ASSERT_EQ(store_physical_step(statement.value),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int(statement.value,0),1);
                const auto returned=observer_delivery_startup_test_access::inspect(*data.admission.owner);
                EXPECT_EQ(returned.leases,before.leases); // Actual step return, before VM finalize.
            }
        }
        const auto after=observer_delivery_startup_test_access::inspect(*data.admission.owner);
        EXPECT_TRUE(data.called);EXPECT_TRUE(data.first_refusal);EXPECT_FALSE(data.unexpected);EXPECT_EQ(data.refused,33u);
        ASSERT_GT(data.admission.admission_calls,0u);
        EXPECT_EQ(data.held_leases,before.leases+data.admission.admission_calls);
        EXPECT_EQ(data.queued_inside,data.admission.admission_calls);EXPECT_EQ(data.delete_calls_inside,0u);
        EXPECT_EQ(returns.data.budget_enqueued,data.admission.admission_calls);
        EXPECT_EQ(returns.data.budget_delete_calls,data.admission.admission_calls);
        ASSERT_TRUE(after.current);EXPECT_EQ(after.leases,before.leases);EXPECT_EQ(after.count,before.count);EXPECT_EQ(after.bytes,before.bytes);
    }
}

#endif
