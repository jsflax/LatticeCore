#include "TestHelpers.hpp"
#include "ExceptionObservation.hpp"
#include <lattice/ordinary_history.hpp>
#include "../../Sources/LatticeCore/src/ordinary_source_upload.hpp"
#include "../../Sources/LatticeCore/src/ordinary_wire_test_hooks.hpp"
#include "../../Sources/LatticeCore/src/ordinary_resource_pool.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <optional>
#include <cstring>
#include <array>
#include <new>
#include "../../Sources/LatticeCore/src/database_retirement.hpp"
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

#ifndef __EMSCRIPTEN__
namespace {
using namespace lattice;using namespace lattice::detail;using json=nlohmann::json;
configuration history_config(const TempDB& p){configuration c(p.str());c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;}
json decoded(const ordinary_history_export& issued,uint64_t n){auto lease=issued.page(n);const auto bytes=lease.bytes();return json::parse(bytes.begin(),bytes.end())["latticeOrdinarySync"];}
std::string complete_hash(const ordinary_history_export& issued){picosha2::hash256_one_by_one content;for(uint64_t n=1;n<issued.frame_count();++n){auto lease=issued.page(n);const auto bytes=lease.bytes();content.process(bytes.begin(),bytes.end());}content.finish();return picosha2::get_hash_hex_string(content);}
struct history_child_hook {std::function<void(lattice_db&)> prior;explicit history_child_hook(std::function<void(lattice_db&)> next):prior(std::move(ordinary_wire_test_hooks::after_child_open)){ordinary_wire_test_hooks::after_child_open=std::move(next);}~history_child_hook(){ordinary_wire_test_hooks::after_child_open=std::move(prior);}};
int64_t history_usage(lattice_db& owner){const auto rows=owner.db().query("SELECT (SELECT COALESCE(SUM(length(CAST(stage AS BLOB))+length(CAST(source AS BLOB))+length(CAST(scope AS BLOB))+length(CAST(schema AS BLOB))+length(CAST(device AS BLOB))+length(CAST(inode AS BLOB))+length(header)+length(CAST(content AS BLOB))+length(CAST(seal AS BLOB))+80),0) FROM _lattice_ordered_history_stage_v1) + (SELECT COALESCE(SUM(length(CAST(stage AS BLOB))+length(CAST(previous AS BLOB))+length(CAST(digest AS BLOB))+length(payload)+length(CAST(seal AS BLOB))+32),0) FROM _lattice_ordered_history_page_v1) AS n");return std::get<int64_t>(rows.at(0).at("n"));}
}

TEST(OrdinaryPagedHistory, NineThousandTwoKiBRowsAndOriginalsUseBoundedDurablePages) {
    const char* phase="construct-source";int seed_row=-1;uint64_t observed_page=0;bool page_observed=false;
    try {
    TempDB path{"ordinary_history_9000"};lattice_db source{history_config(path)};
    phase="seed-begin";source.begin_transaction();
    phase="seed-add";for(int n=0;n<9000;++n){seed_row=n;source.add(TestPerson{std::string(2048,'x'),n,std::nullopt});}
    phase="seed-commit";source.commit();
    phase="seed-flags";const auto flags=source.db().query("SELECT COUNT(*) AS n,SUM(isSynchronized) AS synced FROM AuditLog");
    phase="strict-refusal";EXPECT_THROW((void)capture_ordinary_sync_export(source,{"TestPerson"}),db_error); // strict-v2 limit unchanged
    phase="history-capture";const auto issued=capture_ordinary_history_export(source,{"TestPerson"});phase="history-header";const auto header=decoded(issued,0);
    EXPECT_EQ(header["profile"],"paged-history-v1");EXPECT_EQ(header["rows"],"9000");EXPECT_EQ(header["knownOriginals"],"9000");EXPECT_EQ(header["unknownOriginals"],"0");
    const auto columns=header["tables"][0]["columns"];const auto name_column=std::find(columns.begin(),columns.end(),"name")-columns.begin();ASSERT_LT(size_t(name_column),columns.size());
    size_t rows=0,originals=0;std::string previous;
    for(uint64_t n=1;n<issued.frame_count();++n){page_observed=true;observed_page=n;phase="history-page";auto lease=issued.page(n);const auto bytes=lease.bytes();EXPECT_LE(bytes.size(),ordinary_history_limits{}.page_bytes);
        phase="history-page-decode";const auto page=json::parse(bytes.begin(),bytes.end())["latticeOrdinarySync"];EXPECT_EQ(page["previousDigest"],previous);EXPECT_EQ(page["index"],std::to_string(n-1));EXPECT_LE(page["records"].size(),64u);
        for(const auto& record:page["records"]){if(record.contains("row")){++rows;EXPECT_EQ(record["row"]["values"][size_t(name_column)][1].get<std::string>().size(),2048u);}else if(record.contains("original"))++originals;else ADD_FAILURE()<<"unexpected unknown record";}
        previous=picosha2::hash256_hex_string(bytes.begin(),bytes.end());}
    EXPECT_EQ(rows,9000u);EXPECT_EQ(originals,9000u);phase="content-digest";EXPECT_EQ(header["contentDigest"],complete_hash(issued));phase="final-audit";EXPECT_EQ(source.db().query("SELECT COUNT(*) AS n,SUM(isSynchronized) AS synced FROM AuditLog"),flags);
    phase="durable-extent";EXPECT_LT(issued.durable_bytes(),ordinary_history_limits{}.durable_stage_bytes);
    } catch (...) {
        const auto original=std::current_exception();
        const auto text=lattice_test::actual_exception_text(original);
        try {
            ::testing::Test::RecordProperty("historyFailurePhase",phase);
            ::testing::Test::RecordProperty("historyFailureSeedRow",std::to_string(seed_row));
            ::testing::Test::RecordProperty("historyFailurePageObserved",page_observed?"true":"false");
            ::testing::Test::RecordProperty("historyFailurePage",std::to_string(observed_page));
            ::testing::Test::RecordProperty("historyFirstError",text);
        } catch (...) {
            std::fprintf(stderr,"LATTICE_HISTORY_ERROR_PROPERTY_UNAVAILABLE phase=%s seed_row=%d page_observed=%d page=%llu first_error=%s\n",
                phase,seed_row,page_observed?1:0,static_cast<unsigned long long>(observed_page),text.c_str());
        }
        throw; // The exact original type/settlement/first errors still reach GoogleTest.
    }
}
TEST(OrdinaryPagedHistory, MultiPageCaptureKeepsInstalledGuardAcrossLiveSourceRowCursor) {
    TempDB path{"ordinary_history_live_cursor_guard"};lattice_db source{history_config(path)};
    source.begin_transaction();
    for(int n=0;n<65;++n)source.add(TestPerson{std::string(2048,'p'),n,std::nullopt});
    source.commit();
    const auto before=source.db().query("SELECT COUNT(*) AS n,SUM(isSynchronized) AS synced FROM AuditLog");
    const auto issued=capture_ordinary_history_export(source,{"TestPerson"});
    const auto header=decoded(issued,0);
    EXPECT_EQ(header["rows"],"65");EXPECT_EQ(header["knownOriginals"],"65");
    ASSERT_GT(issued.frame_count(),2u); // A page must flush before its live source cursor finishes.
    size_t rows=0,originals=0;
    for(uint64_t n=1;n<issued.frame_count();++n){
        const auto page=decoded(issued,n);EXPECT_LE(page["records"].size(),64u);
        for(const auto& record:page["records"]){if(record.contains("row"))++rows;else if(record.contains("original"))++originals;else ADD_FAILURE()<<"unexpected unknown record";}
    }
    EXPECT_EQ(rows,65u);EXPECT_EQ(originals,65u);
    EXPECT_EQ(header["contentDigest"],complete_hash(issued));
    EXPECT_EQ(source.db().query("SELECT COUNT(*) AS n,SUM(isSynchronized) AS synced FROM AuditLog"),before);
    EXPECT_EQ(source.db().query("SELECT id FROM TestPerson").size(),65u);
}
TEST(OrdinaryPagedHistory, UnknownLegacyRemoteAndMalformedAgeNeverBecomeKnownOriginals) {
    TempDB up{"ordinary_history_unknown_up"},path{"ordinary_history_unknown_source"};lattice_db uploader{history_config(up)},source{history_config(path)};
    uploader.add(TestPerson{"legacy",1,std::nullopt});const auto input=query_ordinary_upload_entries(uploader.db(),"history-legacy",std::nullopt,0,64);ASSERT_EQ(input.size(),1u);
    ASSERT_EQ(apply_remote_changes(source,input).size(),1u);source.add(TestPerson{"malformed-age",2,std::nullopt});source.db().execute("UPDATE AuditLog SET timestamp='unparseable-original-age' WHERE isFromRemote=0");
    const auto actual=source.db().query("SELECT * FROM AuditLog");const auto issued=capture_ordinary_history_export(source,{"TestPerson"});const auto header=decoded(issued,0);
    EXPECT_EQ(header["rows"],"2");EXPECT_EQ(header["knownOriginals"],"0");EXPECT_EQ(header["unknownOriginals"],"2");size_t unknown=0;bool exact_age=false;
    for(uint64_t n=1;n<issued.frame_count();++n){const auto saved=decoded(issued,n);for(const auto& record:saved["records"]){EXPECT_FALSE(record.contains("originalDescriptor"));if(record.contains("unknownOriginal")){++unknown;const auto& observation=record["unknownOriginal"];EXPECT_EQ(observation["provenance"],"legacy-observed");if(observation["audit"][8]==json::array({"textBytes","756e706172736561626c652d6f726967696e616c2d616765"}))exact_age=true;}}}
    EXPECT_EQ(unknown,2u);EXPECT_TRUE(exact_age);EXPECT_EQ(source.db().query("SELECT * FROM AuditLog"),actual);
}
TEST(OrdinaryPagedHistory, RealIngressEnvelopePreservesExactKnownTimestampIdentityAndNames) {
    TempDB up{"ordinary_history_known_up"},path{"ordinary_history_known_source"};lattice_db uploader{history_config(up)},source{history_config(path)};uploader.add(TestDog{"known",1.234567890123,true});
    auto input=query_ordinary_upload_entries(uploader.db(),"history-known",std::nullopt,0,64);ASSERT_EQ(input.size(),1u);input[0].timestamp="2026-09-30T02:30:00.123456789+02:30";input[0].original_identity=audit_original_identity{1,{"weight"},std::string(64,'a')};input[0].changed_fields.at("weight").kind=any_property_kind::float_kind;
    const auto raw=encode_ordinary_source_upload(input);ASSERT_EQ(receive_ordinary_source_upload(source,reinterpret_cast<const uint8_t*>(raw.data()),raw.size(),{"TestDog"}).size(),1u);
    const auto issued=capture_ordinary_history_export(source,{"TestDog"});EXPECT_EQ(decoded(issued,0)["knownOriginals"],"1");bool found=false;
    for(uint64_t n=1;n<issued.frame_count();++n){const auto saved=decoded(issued,n);for(const auto& record:saved["records"])if(record.contains("original")){found=true;const auto& e=record["original"];EXPECT_EQ(e["timestamp"],input[0].timestamp);EXPECT_EQ(e["changedFieldsNames"],input[0].changed_fields_names);EXPECT_EQ(e["originalIdentity"]["digest"],std::string(64,'a'));EXPECT_EQ(record["originalDescriptor"]["fields"],input[0].upload_original->fields);}}
    EXPECT_TRUE(found);
}
TEST(OrdinaryPagedHistory, PageLeaseSurvivesSourceCloseAndRestartResumesTheSameSealedCut) {
    TempDB path{"ordinary_history_restart"};auto source=std::make_unique<lattice_db>(history_config(path));source->add(TestPerson{"retained",3,std::nullopt});
    std::string stage,source_id,hash;int64_t cut=0;
    {auto issued=capture_ordinary_history_export(*source,{"TestPerson"});stage=issued.stage_id();source_id=issued.source_id();cut=issued.cut();hash=complete_hash(issued);
        {const auto header=issued.page(0);auto alias=header;EXPECT_EQ(header.bytes().data(),alias.bytes().data());source.reset();EXPECT_FALSE(header.bytes().empty());EXPECT_THROW((void)issued.page(1),db_error);}
        EXPECT_EQ(complete_hash(issued),hash);}
    source=std::make_unique<lattice_db>(history_config(path));auto resumed=resume_ordinary_history_export(*source,stage);EXPECT_EQ(resumed.source_id(),source_id);EXPECT_EQ(resumed.cut(),cut);EXPECT_EQ(complete_hash(resumed),hash);
    source->add(TestPerson{"later",4,std::nullopt});{auto later=capture_ordinary_history_export(*source,{"TestPerson"});EXPECT_GT(later.cut(),cut);}
    auto older=resume_ordinary_history_export(*source,stage);EXPECT_EQ(complete_hash(older),hash);EXPECT_EQ(decoded(older,0)["rows"],"1");
}
TEST(OrdinaryPagedHistory, SharedDurableQuotaRefusesBeforeWritesAndNeverEvictsExistingStage) {
    TempDB path{"ordinary_history_quota"};lattice_db source{history_config(path)};source.add(TestPerson{std::string(2048,'q'),5,std::nullopt});
    ordinary_history_limits tiny;tiny.durable_stage_bytes=128;
    EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"},tiny),db_error);EXPECT_FALSE(source.db().table_exists("_lattice_ordered_source_v1"));EXPECT_FALSE(source.db().table_exists("_lattice_ordered_history_stage_v1"));
    std::string stage;{auto initial=capture_ordinary_history_export(source,{"TestPerson"});stage=initial.stage_id();}
    const auto stored=source.db().query("SELECT * FROM _lattice_ordered_history_stage_v1"),frontier=source.db().query("SELECT * FROM _lattice_ordered_source_v1");
    ordinary_history_limits bounded;bounded.durable_stage_bytes=uint64_t(history_usage(source))+64;
    EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"},bounded),db_error);EXPECT_EQ(source.db().query("SELECT * FROM _lattice_ordered_history_stage_v1"),stored);EXPECT_EQ(source.db().query("SELECT * FROM _lattice_ordered_source_v1"),frontier);
    bounded.durable_stage_bytes=uint64_t(history_usage(source))-1;EXPECT_THROW((void)resume_ordinary_history_export(source,stage,bounded),db_error);
    EXPECT_EQ(source.db().query("SELECT * FROM _lattice_ordered_history_stage_v1"),stored);
}
TEST(OrdinaryPagedHistory, GuardRestoredPageCorruptionStillRefusesActualRestart) {
    TempDB path{"ordinary_history_corrupt"};lattice_db source{history_config(path)};source.add(TestPerson{"original",6,std::nullopt});std::string stage;{auto issued=capture_ordinary_history_export(source,{"TestPerson"});stage=issued.stage_id();}
    const auto sql=std::get<std::string>(source.db().query("SELECT sql FROM sqlite_master WHERE name='_lattice_ordered_history_page_v1_UPDATE'").at(0).at("sql"));source.db().execute("DROP TRIGGER _lattice_ordered_history_page_v1_UPDATE");source.db().execute("UPDATE _lattice_ordered_history_page_v1 SET payload=x'7b7d' WHERE stage=?",{stage});source.db().execute(sql);
    EXPECT_THROW((void)resume_ordinary_history_export(source,stage),db_error);EXPECT_EQ(source.db().query("SELECT id FROM TestPerson").size(),1u);
}
TEST(OrdinaryPagedHistory, ActualPageInsertCommitDenialRollsBackStageAndSourceCutTogether) {
    TempDB path{"ordinary_history_commit"};lattice_db source{history_config(path)};source.add(TestPerson{"commit",7,std::nullopt});
    struct state {bool page=false;int denied=0;};auto retained=std::make_shared<state>();
    {history_child_hook hook([retained](lattice_db& child){if(sqlite3_set_authorizer(child.db().handle(),[](void* p,int action,const char* name,const char*,const char*,const char*)noexcept{auto& s=*static_cast<state*>(p);if(action==SQLITE_INSERT&&name&&std::strcmp(name,"_lattice_ordered_history_page_v1")==0)s.page=true;if(action==SQLITE_TRANSACTION&&name&&std::strcmp(name,"COMMIT")==0&&s.page){++s.denied;return SQLITE_DENY;}return SQLITE_OK;},retained.get())!=SQLITE_OK)throw db_error("history actual authorizer failed");});
        EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"}),db_error);}
    EXPECT_GT(retained->denied,0);EXPECT_FALSE(source.db().table_exists("_lattice_ordered_history_stage_v1"));EXPECT_FALSE(source.db().table_exists("_lattice_ordered_source_v1"));EXPECT_EQ(source.db().query("SELECT id FROM TestPerson").size(),1u);
}
TEST(OrdinaryPagedHistory, StrictAndPagedExportsShareOneAdmissionPoolBeforeChildConstruction) {
    TempDB path{"ordinary_history_pool"};lattice_db source{history_config(path)};source.add(TestPerson{"pool",8,std::nullopt});const auto baseline=ordinary_export_slots.load();ASSERT_EQ(baseline,0u);
    std::vector<ordinary_sync_export> held;for(size_t n=0;n<ordinary_sync_limits::simultaneous_exports;++n)held.push_back(capture_ordinary_sync_export(source,{"TestPerson"}));
    int opened=0;{history_child_hook hook([&](lattice_db&){++opened;});EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"}),db_error);}EXPECT_EQ(opened,0);
    held.pop_back();{auto issued=capture_ordinary_history_export(source,{"TestPerson"});EXPECT_EQ(ordinary_export_slots.load(),ordinary_sync_limits::simultaneous_exports);EXPECT_THROW((void)capture_ordinary_sync_export(source,{"TestPerson"}),db_error);}
    EXPECT_EQ(ordinary_export_slots.load(),ordinary_sync_limits::simultaneous_exports-1);
}
TEST(OrdinaryPagedHistory, ReopenedUnenrolledHistoricalFileCannotMintFirstSourceFromStagedBytes) {
    TempDB path{"ordinary_history_unadmitted"};{lattice_db original{history_config(path)};original.add(TestPerson{"old-populated",9,std::nullopt});}
    lattice_db historical{history_config(path)};const auto before=historical.db().query("SELECT * FROM TestPerson");
    EXPECT_THROW((void)capture_ordinary_history_export(historical,{"TestPerson"}),db_error);
    EXPECT_FALSE(historical.db().table_exists("_lattice_ordered_source_v1"));EXPECT_FALSE(historical.db().table_exists("_lattice_ordered_history_stage_v1"));EXPECT_EQ(historical.db().query("SELECT * FROM TestPerson"),before);
    // Actual managed old-writer/cohort admission is a separate required route,
    // never inferred from this file being writable or its table count.
}

#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__))
// These real schedules compare original admission with actual physical completion.
// A threadsafe death-test subprocess starts fresh native registries; it never
// resets real capacity counters or manufactures a physical-close result.
namespace {
struct history_fresh_process {
    std::string prior=::testing::FLAGS_gtest_death_test_style;
    history_fresh_process(){::testing::FLAGS_gtest_death_test_style="threadsafe";}
    ~history_fresh_process(){::testing::FLAGS_gtest_death_test_style=prior;}
};
[[noreturn]] void history_physical_custody_scenario(int which) {
    bool ok=true;
    try {
        TempDB path{"history_physical_custody"};lattice_db source{history_config(path)};
        source.add(TestPerson{"physical",7,std::nullopt});
        const auto baseline=ordinary_export_slots.load();
        if(which==3){
            int actual_opened=0;bool refused=false;
            const auto before=source.db().query("SELECT * FROM TestPerson");
            database_retirement_test_hooks::probe fault;
            fault.after_open=[&](database&,sqlite3*){if(++actual_opened==2)throw db_error("real second child connection constructor fault");};
            const auto* previous=database_retirement_test_hooks::current;
            database_retirement_test_hooks::current=&fault;
            try{(void)capture_ordinary_history_export(source,{"TestPerson"});}catch(const db_error&){refused=true;}
            database_retirement_test_hooks::current=previous;
            ok=refused&&actual_opened==2&&ordinary_export_slots.load()==baseline&&source.db().query("SELECT * FROM TestPerson")==before;
            // Reach the common exit only after source and TempDB leave scope.
        } else {
        std::shared_ptr<database> reader,xproc;
        std::shared_ptr<const database_retirement_state> writer_fact,reader_fact,xproc_fact;
        {
            history_child_hook hook([&](lattice_db& child){
                writer_fact=database_retirement_access::retain(child.db());
                reader=child.borrow_read_connection();xproc=child.borrow_xproc_read_connection();
                reader_fact=database_retirement_access::retain(*reader);xproc_fact=database_retirement_access::retain(*xproc);
                if(which==2){const auto keeper=child.acquire_read_generation();ok=ok&&keeper!=0;child.release_read_generation(keeper);}
            });
            auto cut=std::optional<ordinary_history_export>(capture_ordinary_history_export(source,{"TestPerson"}));
            ok=ok&&reader&&xproc&&reader!=xproc&&reader_fact!=writer_fact&&xproc_fact!=writer_fact;
            if(which==0){reader.reset();xproc.reset();}
            cut.reset(); // Real child close + object destruction is synchronous here.
        }
        ok=ok&&writer_fact&&writer_fact->state()==database_retirement_state::phase::closed;
        if(which==0){
            ok=ok&&reader_fact->state()==database_retirement_state::phase::closed&&xproc_fact->state()==database_retirement_state::phase::closed;
            ok=ok&&ordinary_export_slots.load()==baseline;
        } else {
            ok=ok&&reader_fact->state()==database_retirement_state::phase::live&&xproc_fact->state()==database_retirement_state::phase::live;
            ok=ok&&ordinary_export_slots.load()==baseline+1;
            reader.reset();xproc.reset();
            ok=ok&&reader_fact->state()==database_retirement_state::phase::closed&&xproc_fact->state()==database_retirement_state::phase::closed;
            // The real last owning deleter now completes both tracked readers;
            // only then does the original charged tail reclaim its same slot.
            ok=ok&&ordinary_export_slots.load()==baseline;
        }
        }
    }catch(...){ok=false;}
    ::_exit(ok?0:19);
}
}
TEST(OrdinaryPagedHistory, ActualWriterReaderAndXprocPhysicalCloseReleaseSameCutCharge) {
    history_fresh_process fresh;
    ASSERT_EXIT(history_physical_custody_scenario(0),::testing::ExitedWithCode(0),"");
}
TEST(OrdinaryPagedHistory, ActualBorrowedReadersKeepCutDebtAfterChildDestructorReturns) {
    history_fresh_process fresh;
    ASSERT_EXIT(history_physical_custody_scenario(1),::testing::ExitedWithCode(0),"");
}
TEST(OrdinaryPagedHistory, ActualKeeperAndFixedReadersReclaimOnlyAfterRealLastRetirement) {
    history_fresh_process fresh;
    ASSERT_EXIT(history_physical_custody_scenario(2),::testing::ExitedWithCode(0),"");
}
TEST(OrdinaryPagedHistory, ActualPartialChildConstructorReclaimsOnlyItsRecordedCleanNativePrefix) {
    history_fresh_process fresh;
    ASSERT_EXIT(history_physical_custody_scenario(3),::testing::ExitedWithCode(0),"");
}
TEST(OrdinaryPagedHistory, ActualChildCallbackFailureSettlesOnlyProvedThreeConnectionPrefix) {
    TempDB path{"history_physical_callback_failure"};lattice_db source{history_config(path)};
    source.add(TestPerson{"unchanged",9,std::nullopt});const auto before=source.db().query("SELECT * FROM TestPerson");
    const auto baseline=ordinary_export_slots.load();int opened=0;
    std::array<std::shared_ptr<const database_retirement_state>,3> physical;
    {history_child_hook hook([&](lattice_db& child){++opened;
        physical[0]=database_retirement_access::retain(child.db());
        const auto reader=child.borrow_read_connection(),xproc=child.borrow_xproc_read_connection();
        physical[1]=database_retirement_access::retain(*reader);physical[2]=database_retirement_access::retain(*xproc);
        throw db_error("real child rendezvous failure");});
        EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"}),db_error);}
    EXPECT_EQ(opened,1);for(const auto& connection:physical){ASSERT_TRUE(connection);EXPECT_EQ(connection->state(),database_retirement_state::phase::closed);}
    EXPECT_EQ(ordinary_export_slots.load(),baseline);EXPECT_EQ(source.db().query("SELECT * FROM TestPerson"),before);
}
#endif
#endif

#include <future>

#ifndef __EMSCRIPTEN__
namespace {
struct history_native_probe_scope {
    const database_retirement_test_hooks::probe* prior;
    explicit history_native_probe_scope(const database_retirement_test_hooks::probe& probe):prior(database_retirement_test_hooks::current){database_retirement_test_hooks::current=&probe;}
    ~history_native_probe_scope(){database_retirement_test_hooks::current=prior;}
};
}
TEST(OrdinaryHistoryOpenLedger, ForeignLastReadersReclaimOriginalCutOnlyAfterBothDestructorsReturn) {
    TempDB path{"history_ledger_foreign_readers"};lattice_db source{history_config(path)};source.add(TestPerson{"kept",1,std::nullopt});
    const auto baseline=ordinary_export_slots.load();std::shared_ptr<database> reader,xproc;
    {history_child_hook hook([&](lattice_db& child){reader=child.borrow_read_connection();xproc=child.borrow_xproc_read_connection();});
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});}
    EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    std::thread first([owned=std::move(reader)]()mutable{owned.reset();});first.join();
    EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    std::thread last([owned=std::move(xproc)]()mutable{owned.reset();});last.join();
    EXPECT_EQ(ordinary_export_slots.load(),baseline);
    auto successor=capture_ordinary_history_export(source,{"TestPerson"});EXPECT_FALSE(successor.page(0).bytes().empty());
}
TEST(OrdinaryHistoryOpenLedger, SeventyKeeperAndReplacementCyclesReuseCleanBoundedNativeSlots) {
    TempDB path{"history_ledger_reuse"};lattice_db source{history_config(path)};source.add(TestPerson{"reuse",2,std::nullopt});
    const auto baseline=ordinary_export_slots.load();int actual=0;
    {history_child_hook hook([&](lattice_db& child){for(int n=0;n<70;++n){const auto generation=child.acquire_read_generation();if(!generation)throw db_error("real keeper was unavailable");child.release_read_generation(generation);child.reopen_read_db();++actual;}});
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});}
    EXPECT_EQ(actual,70);EXPECT_EQ(ordinary_export_slots.load(),baseline);
}
TEST(OrdinaryHistoryOpenLedger, DisplacedReaderKeepsSameQuotaAcrossReopenUntilForeignFinalRelease) {
    TempDB path{"history_ledger_displaced"};lattice_db source{history_config(path)};source.add(TestPerson{"replace",3,std::nullopt});
    const auto baseline=ordinary_export_slots.load();std::shared_ptr<database> old;
    {history_child_hook hook([&](lattice_db& child){old=child.borrow_read_connection();child.reopen_read_db();});
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});}
    EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    std::thread release([owned=std::move(old)]()mutable{owned.reset();});release.join();
    EXPECT_EQ(ordinary_export_slots.load(),baseline);
}
TEST(OrdinaryHistoryOpenLedger, CallbackUnwindKeepsActualEscapedReaderThenReclaimsCleanPrefix) {
    TempDB path{"history_ledger_prefix"};lattice_db source{history_config(path)};source.add(TestPerson{"same",4,std::nullopt});
    const auto before=source.db().query("SELECT * FROM TestPerson");const auto baseline=ordinary_export_slots.load();
    std::shared_ptr<database> escaped;
    {history_child_hook hook([&](lattice_db& child){escaped=child.borrow_xproc_read_connection();throw db_error("actual child callback failure");});
        EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"}),db_error);}
    EXPECT_EQ(ordinary_export_slots.load(),baseline+1);escaped.reset();
    EXPECT_EQ(ordinary_export_slots.load(),baseline);EXPECT_EQ(source.db().query("SELECT * FROM TestPerson"),before);
}
TEST(OrdinaryHistoryOpenLedger, NativeOpenCapacityRefusesBeforeAnAdditionalUntrackedConnection) {
    TempDB path{"history_ledger_capacity"};lattice_db source{history_config(path)};source.add(TestPerson{"bounded",5,std::nullopt});
    const auto baseline=ordinary_export_slots.load();std::vector<std::shared_ptr<database>> held;
    int opened=0;database_retirement_test_hooks::probe probe;probe.after_open=[&](database&,sqlite3*){++opened;};
    {history_native_probe_scope native(probe);history_child_hook hook([&](lattice_db& child){for(size_t n=0;n<70;++n){held.push_back(child.borrow_read_connection());held.push_back(child.borrow_xproc_read_connection());child.reopen_read_db();}});
        EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"}),db_error);}
    EXPECT_LE(opened,64);EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    held.clear();EXPECT_EQ(ordinary_export_slots.load(),baseline);
}
TEST(OrdinaryHistoryOpenLedger, TrackedProjectionRetiresNativeOutsideExecutionLeafBeforeSameQuotaRelease) {
    TempDB path{"history_ledger_projection"};lattice_db source{history_config(path)};source.add(TestPerson{"project",6,std::nullopt});
    const auto baseline=ordinary_export_slots.load();projection_read_operation projected;
    {history_child_hook hook([&](lattice_db& child){projection_query query;query.table="TestPerson";query.columns={"id","name"};projected=child.start_projection(query);const auto batch=projected.next_batch(1);if(batch.row_count()!=1)throw db_error("real projection did not read native row");});
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});}
    EXPECT_FALSE(projected.has_resources());EXPECT_EQ(ordinary_export_slots.load(),baseline);
}
TEST(OrdinaryHistoryOpenLedger, ClosingParentRefusesLateKeeperPoolPublication) {
    TempDB path{"history_ledger_postclose_keeper"};lattice_db owner{history_config(path)};owner.add(TestPerson{"parent",7,std::nullopt});
    std::promise<void> entered,release;auto ready=entered.get_future();auto resumed=release.get_future().share();
    uint64_t generation=99;std::exception_ptr failed;std::shared_ptr<const database_retirement_state> native;
    std::thread acquire([&]{try{database_retirement_test_hooks::probe probe;probe.after_open=[&](database& actual,sqlite3*){native=database_retirement_access::retain(actual);entered.set_value();if(resumed.wait_for(std::chrono::seconds(2))!=std::future_status::ready)throw db_error("real keeper rendezvous deadline");};history_native_probe_scope hook(probe);generation=owner.acquire_read_generation();}catch(...){failed=std::current_exception();}});
    const bool opened=ready.wait_for(std::chrono::seconds(2))==std::future_status::ready;
    if(opened)owner.close();release.set_value();acquire.join();
    ASSERT_TRUE(opened);EXPECT_FALSE(failed);EXPECT_EQ(generation,0u);EXPECT_EQ(owner.idle_read_pool_size(),0u);
    ASSERT_TRUE(native);EXPECT_EQ(native->state(),database_retirement_state::phase::closed);
}
#endif

#ifndef __EMSCRIPTEN__
namespace lattice::detail {
// Genuine fixture-only borrow of an existing keeper; no issuance/completion mutation.
struct ordinary_history_open_ledger_test_access {
    static std::shared_ptr<database> keeper(lattice_db& owner,uint64_t id) {
        owner.require_creator_process();
        std::lock_guard lock(owner.read_pool_mutex_);
        for(const auto& actual:owner.live_generations_)if(actual->id==id)return actual->conn;
        throw db_error("actual test keeper missing");
    }
};
}
TEST(OrdinaryHistoryOpenLedger, BorrowedActualKeeperRetainsSameCutThenReclaimsOnForeignDestruction) {
    TempDB path{"history_ledger_borrowed_keeper"};lattice_db source{history_config(path)};source.add(TestPerson{"keeper",8,std::nullopt});
    const auto baseline=ordinary_export_slots.load();std::shared_ptr<database> keeper;
    {history_child_hook hook([&](lattice_db& child){const auto generation=child.acquire_read_generation();keeper=ordinary_history_open_ledger_test_access::keeper(child,generation);child.release_read_generation(generation);});
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});}
    EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    std::thread release([owned=std::move(keeper)]()mutable{owned.reset();});release.join();
    EXPECT_EQ(ordinary_export_slots.load(),baseline);
}
#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__))
TEST(OrdinaryHistoryOpenLedger, UnprovedNativeCloseKeepsOriginalQuotaAfterRawStatementFinalization) {
    history_fresh_process fresh;
    ASSERT_EXIT({
        bool ok=true;try{
            TempDB path{"history_ledger_unproved"};lattice_db source{history_config(path)};source.add(TestPerson{"busy",9,std::nullopt});
            const auto baseline=ordinary_export_slots.load();sqlite3_stmt* retained=nullptr;
            std::shared_ptr<const database_retirement_state> physical;
            {history_child_hook hook([&](lattice_db& child){physical=database_retirement_access::retain(child.db());if(sqlite3_prepare_v2(child.db().handle(),"SELECT name FROM TestPerson",-1,&retained,nullptr)!=SQLITE_OK)throw db_error("real native statement prepare failed");});
                auto cut=capture_ordinary_history_export(source,{"TestPerson"});}
            ok=retained&&physical&&physical->state()==database_retirement_state::phase::close_unproved&&ordinary_export_slots.load()==baseline+1;
            if(retained)sqlite3_finalize(retained);
            ok=ok&&physical->state()==database_retirement_state::phase::close_unproved&&ordinary_export_slots.load()==baseline+1;
        }catch(...){ok=false;}
        ::_exit(ok?0:19);
    },::testing::ExitedWithCode(0),"");
}
#endif

namespace {
struct history_control_allocation_fault {
    std::function<void()> prior;
    explicit history_control_allocation_fault(std::function<void()> fault):prior(std::move(ordinary_wire_test_hooks::before_history_cut_control_block_allocate)){
        ordinary_wire_test_hooks::before_history_cut_control_block_allocate=std::move(fault);
    }
    ~history_control_allocation_fault(){ordinary_wire_test_hooks::before_history_cut_control_block_allocate=std::move(prior);}
};
}
TEST(OrdinaryHistoryOpenLedger, ActualCutControlBlockFailureKeepsBorrowedReaderUntilLastDelete) {
    TempDB path{"history_ledger_cut_control_failure"};lattice_db source{history_config(path)};
    source.add(TestPerson{"actual",10,std::nullopt});const auto baseline=ordinary_export_slots.load();
    std::shared_ptr<database> reader;size_t allocations=0;
    {history_child_hook child([&](lattice_db& owner){reader=owner.borrow_read_connection();});
        history_control_allocation_fault fault([&]{++allocations;throw std::bad_alloc();});
        EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"}),std::bad_alloc);}
    ASSERT_EQ(allocations,1u);ASSERT_TRUE(reader);EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    // The source WRITE really committed before the actual failed publication.
    EXPECT_EQ(std::get<int64_t>(source.db().query("SELECT COUNT(*) AS n FROM _lattice_ordered_history_stage_v1").at(0).at("n")),1);
    std::thread release([held=std::move(reader)]()mutable{held.reset();});release.join();
    EXPECT_EQ(ordinary_export_slots.load(),baseline);
    const auto successor=capture_ordinary_history_export(source,{"TestPerson"});
    EXPECT_FALSE(successor.page(0).bytes().empty());EXPECT_EQ(successor.cut(),2);
}
TEST(OrdinaryHistoryOpenLedger, ActualCutControlBlockFailureWithoutEscapeReclaimsOnePrefixTail) {
    TempDB path{"history_ledger_cut_control_clean"};lattice_db source{history_config(path)};
    source.add(TestPerson{"clean",11,std::nullopt});const auto baseline=ordinary_export_slots.load();size_t allocations=0;
    {history_control_allocation_fault fault([&]{++allocations;throw std::bad_alloc();});
        EXPECT_THROW((void)capture_ordinary_history_export(source,{"TestPerson"}),std::bad_alloc);}
    EXPECT_EQ(allocations,1u);EXPECT_EQ(ordinary_export_slots.load(),baseline);
    const auto successor=capture_ordinary_history_export(source,{"TestPerson"});
    EXPECT_EQ(successor.cut(),2);EXPECT_FALSE(successor.page(0).bytes().empty());
}
#endif

#ifndef __EMSCRIPTEN__
#include "../../Sources/LatticeCore/src/ordinary_history_refund_test_access.hpp"
#include "CanonicalWriterTestAccess.hpp"
namespace {
using refund_access=lattice::detail::ordinary_history_refund_test_access;
using refund_status=lattice::detail::ordinary_history_refund_status;
struct refund_blocked_destructor {
    std::promise<void> entered,release;
    std::shared_future<void> resumed=release.get_future().share();
    std::atomic<bool> armed{false},timed_out{false},returned{false};
};
struct refund_join_guard {
    std::shared_ptr<refund_blocked_destructor> state;
    std::thread& worker;
    ~refund_join_guard(){
        try{state->release.set_value();}catch(...){}
        if(worker.joinable())worker.join();
    }
};
}
TEST(OrdinaryHistoryRefundObservation, ObservingIsDataOnlyAndPreservesActualFrames) {
    TempDB path{"history_refund_data"};lattice_db source{history_config(path)};
    source.add(TestPerson{"preserved",12,std::nullopt});
    const auto baseline=ordinary_export_slots.load();
    ordinary_history_refund_observation receipt,alias;
    std::shared_ptr<const database_retirement_state> native;
    {
        history_child_hook child([&](lattice_db& original){native=database_retirement_access::retain(original.db());});
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});
        EXPECT_FALSE(refund_access::has_observation(cut));
        const auto header=decoded(cut,0);
        const auto digest=complete_hash(cut);
        receipt=refund_access::observe(cut);alias=refund_access::observe(cut);
        EXPECT_TRUE(receipt.same_reservation(alias));
        EXPECT_EQ(receipt.status(),refund_status::held);
        EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
        EXPECT_EQ(decoded(cut,0),header);EXPECT_EQ(complete_hash(cut),digest);
        alias={};EXPECT_EQ(receipt.status(),refund_status::held);
    }
    ASSERT_TRUE(native);EXPECT_EQ(native->state(),database_retirement_state::phase::closed);
    EXPECT_EQ(ordinary_export_slots.load(),baseline);
    EXPECT_EQ(receipt.status(),refund_status::refunded);
    EXPECT_EQ(ordinary_history_refund_observation{}.status(),refund_status::unavailable);
}
TEST(OrdinaryHistoryRefundObservation, RealOffListReadersDelayTheSameReservationReceipt) {
    TempDB path{"history_refund_readers"};lattice_db source{history_config(path)};
    source.add(TestPerson{"readers",13,std::nullopt});const auto baseline=ordinary_export_slots.load();
    std::shared_ptr<database> reader,xproc;
    std::shared_ptr<const database_retirement_state> reader_fact,xproc_fact;
    ordinary_history_refund_observation receipt;
    {
        history_child_hook child([&](lattice_db& original){
            reader=original.borrow_read_connection();xproc=original.borrow_xproc_read_connection();
            reader_fact=database_retirement_access::retain(*reader);xproc_fact=database_retirement_access::retain(*xproc);
        });
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});receipt=refund_access::observe(cut);
    }
    ASSERT_TRUE(reader);ASSERT_TRUE(xproc);EXPECT_EQ(receipt.status(),refund_status::held);
    EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    std::thread first([original=std::move(reader)]()mutable{original.reset();});first.join();
    EXPECT_EQ(reader_fact->state(),database_retirement_state::phase::closed);
    EXPECT_EQ(xproc_fact->state(),database_retirement_state::phase::live);
    EXPECT_EQ(receipt.status(),refund_status::held);EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    std::thread last([original=std::move(xproc)]()mutable{original.reset();});last.join();
    EXPECT_EQ(xproc_fact->state(),database_retirement_state::phase::closed);
    EXPECT_EQ(ordinary_export_slots.load(),baseline);EXPECT_EQ(receipt.status(),refund_status::refunded);
}
TEST(OrdinaryHistoryRefundObservation, BlockedActualSQLiteDestructorCannotPublishRefundEarly) {
    TempDB path{"history_refund_blocked"};lattice_db source{history_config(path)};
    source.add(TestPerson{"blocked",14,std::nullopt});const auto baseline=ordinary_export_slots.load();
    auto state=std::make_shared<refund_blocked_destructor>();auto entered=state->entered.get_future();
    std::shared_ptr<database> reader;std::shared_ptr<const database_retirement_state> native;
    ordinary_history_refund_observation receipt;
    {
        history_child_hook child([&](lattice_db& original){
            reader=original.borrow_read_connection();native=database_retirement_access::retain(*reader);
            auto* context=new std::shared_ptr<refund_blocked_destructor>(state);
            const int rc=sqlite3_create_function_v2(reader->handle(),"history_refund_block",0,SQLITE_UTF8,context,
                [](sqlite3_context* context,int,sqlite3_value**){sqlite3_result_null(context);},nullptr,nullptr,
                [](void* raw)noexcept{
                    auto held=std::unique_ptr<std::shared_ptr<refund_blocked_destructor>>(static_cast<std::shared_ptr<refund_blocked_destructor>*>(raw));
                    const auto state=*held;
                    if(!state->armed.load(std::memory_order_acquire))return;
                    try{state->entered.set_value();}catch(...){state->timed_out.store(true);}
                    if(state->resumed.wait_for(std::chrono::seconds(2))!=std::future_status::ready)state->timed_out.store(true);
                    state->returned.store(true,std::memory_order_release);
                });
            if(rc!=SQLITE_OK)throw db_error("actual SQLite destruction control registration failed");
            state->armed.store(true,std::memory_order_release);
        });
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});receipt=refund_access::observe(cut);
    }
    ASSERT_TRUE(reader);
    std::thread worker([original=std::move(reader)]()mutable{original.reset();});
    refund_join_guard ending{state,worker};
    const bool blocked=entered.wait_for(std::chrono::seconds(2))==std::future_status::ready;
    EXPECT_TRUE(blocked);
    EXPECT_FALSE(state->returned.load(std::memory_order_acquire));
    EXPECT_EQ(receipt.status(),refund_status::held);EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    state->release.set_value();worker.join();
    EXPECT_FALSE(state->timed_out.load());EXPECT_TRUE(state->returned.load());
    ASSERT_TRUE(native);EXPECT_EQ(native->state(),database_retirement_state::phase::closed);
    EXPECT_EQ(ordinary_export_slots.load(),baseline);EXPECT_EQ(receipt.status(),refund_status::refunded);
}
TEST(OrdinaryHistoryRefundObservation, NoObserverNormalEndingKeepsOriginalNativeAccounting) {
    TempDB path{"history_refund_absent"};lattice_db source{history_config(path)};
    source.add(TestPerson{"unobserved",15,std::nullopt});const auto baseline=ordinary_export_slots.load();
    std::shared_ptr<const database_retirement_state> native;
    {
        history_child_hook child([&](lattice_db& original){native=database_retirement_access::retain(original.db());});
        auto cut=capture_ordinary_history_export(source,{"TestPerson"});
        EXPECT_FALSE(refund_access::has_observation(cut));
        EXPECT_FALSE(cut.page(0).bytes().empty());
    }
    ASSERT_TRUE(native);EXPECT_EQ(native->state(),database_retirement_state::phase::closed);
    EXPECT_EQ(ordinary_export_slots.load(),baseline);
}
#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__))
TEST(OrdinaryHistoryRefundObservation, ActualNativeCloseUnprovedNeverBecomesARefundAfterFinalize) {
    history_fresh_process fresh;
    ASSERT_EXIT({
        bool ok=true;try{
            TempDB path{"history_refund_busy"};lattice_db source{history_config(path)};
            source.add(TestPerson{"busy",16,std::nullopt});const auto baseline=ordinary_export_slots.load();
            sqlite3_stmt* statement=nullptr;std::shared_ptr<const database_retirement_state> native;
            ordinary_history_refund_observation receipt;
            {
                history_child_hook child([&](lattice_db& original){
                    native=database_retirement_access::retain(original.db());
                    if(sqlite3_prepare_v2(original.db().handle(),"SELECT name FROM TestPerson",-1,&statement,nullptr)!=SQLITE_OK)
                        throw db_error("actual history close-failure statement refused");
                });
                auto cut=capture_ordinary_history_export(source,{"TestPerson"});receipt=refund_access::observe(cut);
            }
            ok=ok&&native&&native->state()==database_retirement_state::phase::close_unproved;
            ok=ok&&native->checked_close_result()==SQLITE_BUSY;
            ok=ok&&statement&&ordinary_export_slots.load()==baseline+1&&receipt.status()!=refund_status::refunded;
            if(statement)ok=ok&&sqlite3_finalize(statement)==SQLITE_OK;
            ok=ok&&native->state()==database_retirement_state::phase::close_unproved;
            ok=ok&&ordinary_export_slots.load()==baseline+1&&receipt.status()!=refund_status::refunded;
        }catch(...){ok=false;}
        ::_exit(ok?0:29);
    },::testing::ExitedWithCode(0),"");
}
TEST(OrdinaryHistoryRefundObservation, ActualCheckedCloseCleanupFailureQuarantinesObservedReservation) {
    history_fresh_process fresh;
    ASSERT_EXIT({
        bool ok=true;try{
            TempDB path{"history_refund_checked_failure"};lattice_db source{history_config(path)};
            source.add(TestPerson{"cleanup",17,std::nullopt});const auto baseline=ordinary_export_slots.load();
            ordinary_history_refund_observation receipt;
            {
                auto cut=capture_ordinary_history_export(source,{"TestPerson"});receipt=refund_access::observe(cut);
                auto reader=refund_access::borrow_actual_xproc(cut);
                auto* native=lattice::detail::canonical_writer_custody_test_access::fault_handle(*reader);
                if(!native)throw db_error("actual observed child reader unavailable");
                if(sqlite3_set_authorizer(native,
                    [](void*,int action,const char* operation,const char*,const char*,const char*){
                        return action==SQLITE_TRANSACTION&&operation&&std::strcmp(operation,"ROLLBACK")==0?SQLITE_DENY:SQLITE_OK;
                    },nullptr)!=SQLITE_OK)throw db_error("actual rollback fault registration failed");
                auto operation=refund_access::catalog_actual_child(cut,R"({"version":1,"tables":["TestPerson"]})",5'000'000'000LL);
                int actual_tables=0;
                const auto failed=operation.capture(&actual_tables,
                    [](void* context,int32_t kind,const uint8_t*,int64_t){
                        if(kind==0){++*static_cast<int*>(context);return false;}return true;
                    });
                ok=ok&&actual_tables==1&&!failed.success()&&failed.cleanup_failed()&&failed.rollback_code()==SQLITE_AUTH;
                ok=ok&&sqlite3_get_autocommit(native)==0;
                const auto closed=refund_access::close_actual_child(cut);
                ok=ok&&bool(closed.error)&&bool(closed.cleanup_error)&&!closed.cleanup_complete;
                // Restore no Native authority and perform no post-quarantine
                // rollback/query. The fresh process retains actual debt.
            }
            ok=ok&&receipt.status()==refund_status::quarantined&&ordinary_export_slots.load()==baseline+1;
        }catch(...){ok=false;}
        ::_exit(ok?0:29);
    },::testing::ExitedWithCode(0),"");
}
#endif
#ifndef __linux__
#include <lattice.hpp>
namespace {
std::shared_ptr<lattice::swift_lattice> refund_actual_swift_owner(const TempDB& path) {
    lattice::property_descriptor value;value.name="value";value.type=column_type::text;
    value.kind=lattice::property_kind::primitive;value.nullable=false;
    lattice::swift_schema_entry entry;entry.table_name="HistoryRefundItem";entry.properties["value"]=value;
    lattice::swift_configuration config(path.str());config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    auto owner=lattice::detail::LatticeCache::instance().get_or_create(config,lattice::SchemaVector{entry});
    lattice::swift_dynamic_object row;row.table_name=entry.table_name;row.properties=entry.properties;
    row.values["value"]=std::string("actual");lattice::dynamic_object object(row);owner->add(object);
    return owner;
}
}
TEST(OrdinaryHistoryRefundObservation, CompletedMountedCutDoesNotRefundALiveRegistration) {
    TempDB path{"history_refund_mount"};auto owner=refund_actual_swift_owner(path);
    const auto baseline=ordinary_export_slots.load();ordinary_history_refund_observation receipt;
    {
        auto mounted=refund_access::register_actual_swift_mount(owner,{"HistoryRefundItem"});
        receipt=refund_access::observe(mounted);
        {
            auto cut=mounted.capture({"HistoryRefundItem"});auto alias=refund_access::observe(cut);
            EXPECT_TRUE(receipt.same_reservation(alias));EXPECT_EQ(receipt.status(),refund_status::held);
        }
        EXPECT_EQ(ordinary_export_slots.load(),baseline+1);EXPECT_EQ(receipt.status(),refund_status::held);
        {auto successor=mounted.capture({"HistoryRefundItem"});EXPECT_FALSE(successor.page(0).bytes().empty());}
        EXPECT_EQ(receipt.status(),refund_status::held);
    }
    EXPECT_EQ(ordinary_export_slots.load(),baseline);EXPECT_EQ(receipt.status(),refund_status::refunded);
    EXPECT_TRUE(owner->close_checked().cleanup_complete);
}
TEST(OrdinaryHistoryRefundObservation, ActualPageAliasKeepsObservedMountSlotAfterParentClose) {
    TempDB path{"history_refund_page"};auto owner=refund_actual_swift_owner(path);
    const auto baseline=ordinary_export_slots.load();ordinary_history_refund_observation receipt;
    std::optional<ordinary_history_page> page;std::vector<uint8_t> bytes;
    {
        auto mounted=refund_access::register_actual_swift_mount(owner,{"HistoryRefundItem"});
        auto cut=mounted.capture({"HistoryRefundItem"});receipt=refund_access::observe(cut);
        page.emplace(cut.page(0));const auto actual=page->bytes();bytes.assign(actual.begin(),actual.end());
        EXPECT_TRUE(owner->close_checked().cleanup_complete);
    }
    EXPECT_EQ(ordinary_export_slots.load(),baseline+1);EXPECT_EQ(receipt.status(),refund_status::held);
    const auto retained=page->bytes();EXPECT_EQ(std::vector<uint8_t>(retained.begin(),retained.end()),bytes);
    std::thread last([original=std::move(page)]()mutable{original.reset();});last.join();
    EXPECT_EQ(ordinary_export_slots.load(),baseline);EXPECT_EQ(receipt.status(),refund_status::refunded);
}
TEST(OrdinaryHistoryRefundObservation, FailedMountedFactoryPrefixKeepsSameReceiptUntilReaderReturn) {
    TempDB path{"history_refund_prefix"};auto owner=refund_actual_swift_owner(path);
    const auto baseline=ordinary_export_slots.load();ordinary_history_refund_observation receipt;
    std::shared_ptr<database> reader;std::shared_ptr<const database_retirement_state> native;
    {
        auto mounted=refund_access::register_actual_swift_mount(owner,{"HistoryRefundItem"});
        receipt=refund_access::observe(mounted);
        history_child_hook child([&](lattice_db& original){
            reader=original.borrow_xproc_read_connection();native=database_retirement_access::retain(*reader);
            throw db_error("actual observed history child factory failure");
        });
        EXPECT_THROW((void)mounted.capture({"HistoryRefundItem"}),db_error);
    }
    ASSERT_TRUE(reader);EXPECT_EQ(ordinary_export_slots.load(),baseline+1);
    EXPECT_EQ(receipt.status(),refund_status::held);
    std::thread last([original=std::move(reader)]()mutable{original.reset();});last.join();
    ASSERT_TRUE(native);EXPECT_EQ(native->state(),database_retirement_state::phase::closed);
    EXPECT_EQ(ordinary_export_slots.load(),baseline);EXPECT_EQ(receipt.status(),refund_status::refunded);
    EXPECT_TRUE(owner->close_checked().cleanup_complete);
}
#endif
#endif
