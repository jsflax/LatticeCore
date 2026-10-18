// Qualification runner only: all original current test TUs remain whole.
#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#if defined(LATTICE_COUNT_SHORTCUT_PREDECESSOR_HELPER)
#error The current count receiver must never compile the predecessor helper mode.
#endif
namespace {
constexpr std::array<const char*, 53> expected_models = {"AuditMaintenanceRow","CanonicalCaptureRecord","CanonicalStageKeptModel","ConfiguredConnectionRow","ContinuousLocalRow","ContinuousSharedRow","CursorRegressionItem","CustodyRecord","DurableReadyRow","ExportCallbackRow","ExportCoverageOther","ExportCoverageRow","ExportDiscoveryRow","ExportFailureRow","ExportHistoryOther","ExportHistoryRow","ExportPayloadDoc","ExportPayloadLeaf","ExportPayloadRoot","ExportPublicCallbackRow","ExportTimestampRow","HistoryOwnedEmpty","HistoryOwnedRow","LegacyAdmissionRow","LocalOriginalLeaf","LocalOriginalRoot","LocalOriginalStream","MaintenanceProducerRow","NamespaceRow","PackageSourceRow","RetainedDeleteRow","RetentionCustodyRow","RetentionRow","ServerExportRow","SourceSessionLeaf","SourceSessionLocal","SourceSessionRecord","SourceSessionRoot","TestAllTypes","TestDog","TestLandmark","TestOwner","TestPerson","TestPet","TestPlace","TestRegion","TestTrip","UpstreamLeaf","UpstreamNode","UpstreamRoot","WriterMarkerLeaf","WriterMarkerNode","WriterMarkerRoot"};
constexpr std::array<const char*, 6> expected_parents = {"CountShortcutUpgrade.OriginalEnabledStoreUpgradesAllProgramsAndPreservesEveryDurableCell","CountShortcutUpgrade.CompletedUpgradeReopenIsProgramAndCookieIdempotent","CountShortcutUpgrade.ActualUpgradeCommitDenialRollsBackProgramsMarkersAndLineage","CountShortcutUpgrade.MixedHistoricalAndCurrentProgramsRefuseWithoutAnyRepair","CountShortcutUpgrade.TamperedHistoricalProgramRefusesWithoutAnyRepair","CountShortcutUpgrade.NoncanonicalCurrentMarkerRefusesWithoutCookieOrProgramHealing"};
constexpr const char* catalog_marker = "count verification native catalog=AuditMaintenanceRow,CanonicalCaptureRecord,CanonicalStageKeptModel,ConfiguredConnectionRow,ContinuousLocalRow,ContinuousSharedRow,CursorRegressionItem,CustodyRecord,DurableReadyRow,ExportCallbackRow,ExportCoverageOther,ExportCoverageRow,ExportDiscoveryRow,ExportFailureRow,ExportHistoryOther,ExportHistoryRow,ExportPayloadDoc,ExportPayloadLeaf,ExportPayloadRoot,ExportPublicCallbackRow,ExportTimestampRow,HistoryOwnedEmpty,HistoryOwnedRow,LegacyAdmissionRow,LocalOriginalLeaf,LocalOriginalRoot,LocalOriginalStream,MaintenanceProducerRow,NamespaceRow,PackageSourceRow,RetainedDeleteRow,RetentionCustodyRow,RetentionRow,ServerExportRow,SourceSessionLeaf,SourceSessionLocal,SourceSessionRecord,SourceSessionRoot,TestAllTypes,TestDog,TestLandmark,TestOwner,TestPerson,TestPet,TestPlace,TestRegion,TestTrip,UpstreamLeaf,UpstreamNode,UpstreamRoot,WriterMarkerLeaf,WriterMarkerNode,WriterMarkerRoot; DurableReadyRow.body.no_history=1";
constexpr const char* parent_filter = "CountShortcutUpgrade.OriginalEnabledStoreUpgradesAllProgramsAndPreservesEveryDurableCell:CountShortcutUpgrade.CompletedUpgradeReopenIsProgramAndCookieIdempotent:CountShortcutUpgrade.ActualUpgradeCommitDenialRollsBackProgramsMarkersAndLineage:CountShortcutUpgrade.MixedHistoricalAndCurrentProgramsRefuseWithoutAnyRepair:CountShortcutUpgrade.TamperedHistoricalProgramRefusesWithoutAnyRepair:CountShortcutUpgrade.NoncanonicalCurrentMarkerRefusesWithoutCookieOrProgramHealing";
std::filesystem::path admitted_executable;
void require(bool value,const char* message) {
    if(!value)throw std::runtime_error(message);
}
void require_catalog() {
    const auto registered=lattice::schema_registry::instance().bounded_schemas(expected_models.size());
    require(registered.has_value(),"count helper native model catalog exceeds exact application");
    std::vector<std::string> actual;actual.reserve(registered->size());
    for(const auto* model:*registered)actual.push_back(model->table_name);
    std::sort(actual.begin(),actual.end());
    require(actual.size()==expected_models.size(),"count helper native model catalog size differs");
    for(size_t i=0;i<actual.size();++i)
        require(actual[i]==expected_models[i],"count helper native model declaration differs");
    const auto* ready=lattice::schema_registry::instance().get_schema("DurableReadyRow");
    require(ready&&ready->properties.size()==1&&ready->properties[0].name=="body"&&
        ready->properties[0].type==lattice::column_type::text&&ready->properties[0].no_history,
        "count helper whole Ready declaration override differs");
}
void require_route() {
    const auto* path=std::getenv("LATTICE_COUNT_SHORTCUT_CURRENT_HELPER_EXECUTABLE");
    require(path&&*path,"count helper requires explicit qualified executable");
    const std::filesystem::path declared(path);
    require(declared.is_absolute()&&!std::filesystem::is_symlink(declared)&&
        declared==std::filesystem::canonical(declared)&&declared==admitted_executable,
        "count helper executable route differs");
    const auto* seeds=std::getenv("LATTICE_COUNT_SHORTCUT_PREDECESSOR_ROOT");
    require(seeds&&std::filesystem::path(seeds).is_absolute(),"count helper requires original seeded root");
    require(std::getenv("LATTICE_COUNT_SHORTCUT_SEED_CONTAINER")==nullptr,
        "current count helper cannot run predecessor seed mode");
    require(std::getenv("GTEST_TOTAL_SHARDS")==nullptr&&std::getenv("GTEST_SHARD_INDEX")==nullptr,
        "current count helper cannot use a partial shard");
}
class catalog_listener final:public ::testing::EmptyTestEventListener {
    void OnTestStart(const ::testing::TestInfo& info)override {
        require_route();require_catalog();
        const std::string name=std::string(info.test_suite_name())+"."+info.name();
        require(std::find(expected_parents.begin(),expected_parents.end(),name)!=expected_parents.end(),
            "count helper may execute only the six original current parents");
        const auto message=std::string(catalog_marker)+"; test="+name+"\n";
        std::fputs(message.c_str(),stderr);std::fflush(stderr);
    }
    void OnTestEnd(const ::testing::TestInfo&)override {require_catalog();}
};
}
int main(int argc,char** argv) {
    try {
        require(argc>0&&argv&&argv[0],"count helper executable absent");
        const std::filesystem::path executable(argv[0]);
        require(executable.is_absolute()&&!std::filesystem::is_symlink(executable)&&
            executable==std::filesystem::canonical(executable),"count helper must use its actual absolute image");
        admitted_executable=executable;
        ::testing::InitGoogleTest(&argc,argv);
        require_catalog();
        if(GTEST_FLAG_GET(list_tests)) {
            const auto message=std::string("count helper inventory ")+catalog_marker+"\n";
            std::fputs(message.c_str(),stderr);std::fflush(stderr);
            return RUN_ALL_TESTS();
        }
        require_route();
        require(GTEST_FLAG_GET(filter)==parent_filter&&GTEST_FLAG_GET(repeat)==1&&!GTEST_FLAG_GET(shuffle),
            "count helper requires the exact original six-parent invocation");
        ::testing::UnitTest::GetInstance()->listeners().Append(new catalog_listener);
        const int result=RUN_ALL_TESTS();
        require_catalog();
        std::fputs("count helper selected six catalog unchanged after run\n",stderr);std::fflush(stderr);
        return result;
    }catch(const std::exception& error) {
        std::fprintf(stderr,"current count helper qualification refused: %s\n",error.what());
        return 1;
    }
}
