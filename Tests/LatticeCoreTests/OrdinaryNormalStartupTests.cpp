#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_normal_startup.hpp"
#include <algorithm>
#include <limits>
#include <type_traits>

namespace {
namespace install=lattice::detail::ordinary_installation;
namespace admission=lattice::detail::ordinary_admission;
install::identifier id(std::uint8_t byte){install::identifier v{};v[0]=byte;return v;}
install::digest digest(std::uint8_t byte){install::digest v{};v[0]=byte;return v;}
install::normal_launch_offer sample(){
    install::manifest origin;origin.application=install::product::engram;origin.installation=id(1);origin.revision=1;
    origin.catalog_directory={1,10};origin.launch_gate={1,11};
    origin.supervisor={"/installed/memory-installation-launcher",{1,12},digest(1)};
    install::store_fact store;store.binding={id(1),id(2),id(3),{1,20},{1,21}};
    store.controls={{1,22},{1,23},{1,24}};store.control_leaf="control";
    store.aliases={{"/installation/data","memory.sqlite",{1,21}}};origin.stores={store};
    install::role_fact role;role.name="engram-initializer-v1";
    role.executable={"/installed/memory",{1,30},digest(2)};role.working_directory="/installation/data";
    role.arguments={"--lattice-seed-store-v1"};role.environment={"PATH=/usr/bin:/bin"};role.stores={id(2)};
    origin.roles={role};auto normal=origin;normal.catalog_directory={1,40};normal.launch_gate={1,41};
    auto& mcp=normal.roles.front();mcp.name="engram-primary-mcp-v1";mcp.arguments={"--lattice-managed-mcp-v1"};
    mcp.environment={"PATH=/usr/bin:/bin","CLAUDE_MEMORY_DB=/installation/data/memory.sqlite"};
    install::normal_launch_offer offer;offer.nonce=id(4);offer.supervisor={100,90,123,0};offer.child={101,100,124,0};
    offer.external_anchor.assign(320,0);offer.external_identity={1,50};offer.normal_catalog_identity={1,51};offer.external_leaf="installation.origin";
    offer.origin_catalog=install::encode_manifest(origin);offer.normal_catalog=install::encode_manifest(normal);
    offer.admission.binding=store.binding;offer.admission.control=store.controls.control;
    offer.admission.entry=store.controls.entry;offer.admission.generation=store.controls.generation;
    return offer;
}
template<class T> concept publicly_admits_normal = requires(T& journal,const admission::record& record,const admission::identifier& launch) {
    journal.admit_ordinary(record,launch);
};
static_assert(!publicly_admits_normal<admission::journal>);
static_assert(!std::is_default_constructible_v<lattice::detail::ordinary_open_context>);
static_assert(!std::is_copy_constructible_v<lattice::detail::ordinary_open_context>);
}

TEST(OrdinaryNormalStartup, ExactFourPhasesCarryTheSameStoreAndLaunchWithoutConstructingAuthority) {
    auto offer=sample();
    for(std::uint64_t phase=1;phase<=4;++phase){
        offer.phase=phase;
        if(phase==3){offer.admission.state=admission::stage::ordinary_admitted;offer.admission.revision=2;offer.admission.normal_launch=offer.nonce;}
        const auto bytes=install::encode_normal_offer(offer);const auto result=install::decode_normal_offer(bytes);
        EXPECT_EQ(install::encode_normal_offer(result),bytes);EXPECT_EQ(result.admission,offer.admission);
        EXPECT_EQ(result.child,offer.child);EXPECT_EQ(result.supervisor,offer.supervisor);EXPECT_EQ(result.nonce,offer.nonce);
        EXPECT_EQ(result.origin_catalog,offer.origin_catalog);EXPECT_EQ(result.normal_catalog,offer.normal_catalog);
    }
}
TEST(OrdinaryNormalStartup, TruncationTrailingDataOversizedLengthsAndUnknownPhaseRefuse) {
    const auto bytes=install::encode_normal_offer(sample());
    for(std::size_t n=0;n<bytes.size();++n){
        SCOPED_TRACE(n);
        EXPECT_THROW(install::decode_normal_offer({bytes.begin(),bytes.begin()+n}),std::runtime_error);
    }
    auto changed=bytes;changed.push_back(0);EXPECT_THROW(install::decode_normal_offer(changed),std::runtime_error);
    changed=bytes;changed[8]=5;EXPECT_THROW(install::decode_normal_offer(changed),std::runtime_error);
    // External leaf length follows magic/phase/nonce/two process identities/two file identities.
    changed=bytes;std::fill_n(changed.begin()+128,8,255);EXPECT_THROW(install::decode_normal_offer(changed),std::runtime_error);
    changed.assign(lattice::detail::ordinary_launch::maximum_frame_bytes+1,0);
    EXPECT_THROW(install::decode_normal_offer(changed),std::runtime_error);
    auto invalid=sample();invalid.child.parent++;
    EXPECT_THROW(install::encode_normal_offer(invalid),std::runtime_error);
    invalid=sample();invalid.supervisor.birth_major=0;
    EXPECT_THROW(install::encode_normal_offer(invalid),std::runtime_error);
}
TEST(OrdinaryNormalStartup, PrimaryRoleCannotExpandStoresArgumentsEnvironmentOrExecutable) {
    const auto offer=sample();const auto origin=install::decode_manifest(offer.origin_catalog);
    auto role=install::decode_manifest(offer.normal_catalog);
    role.roles[0].arguments.push_back("--anything-else");EXPECT_THROW(install::validate_primary_mcp_contract(origin,role),std::runtime_error);
    role=install::decode_manifest(offer.normal_catalog);role.roles[0].environment[1]="CLAUDE_MEMORY_DB=/different/memory.sqlite";
    EXPECT_THROW(install::validate_primary_mcp_contract(origin,role),std::runtime_error);
    role=install::decode_manifest(offer.normal_catalog);role.roles[0].environment.push_back("ENGRAM_READY=1");
    EXPECT_THROW(install::validate_primary_mcp_contract(origin,role),std::runtime_error);
    role=install::decode_manifest(offer.normal_catalog);role.roles[0].executable.content[0]^=1;
    EXPECT_THROW(install::validate_primary_mcp_contract(origin,role),std::runtime_error);
    role=install::decode_manifest(offer.normal_catalog);role.stores[0].binding.main.inode++;
    EXPECT_THROW(install::validate_primary_mcp_contract(origin,role),std::runtime_error);
    role=install::decode_manifest(offer.normal_catalog);role.stores[0].aliases.push_back({"/other","memory.sqlite",{1,70}});
    EXPECT_THROW(install::validate_primary_mcp_contract(origin,role),std::runtime_error);
}
TEST(OrdinaryNormalStartup, OnlyOriginalBoundedMcpSettingsCanAccompanyTheFixedStoreRole) {
    const auto offer=sample();const auto origin=install::decode_manifest(offer.origin_catalog);
    auto normal=install::decode_manifest(offer.normal_catalog);
    normal.roles[0].environment.push_back("CLAUDE_MEMORY_MODEL=/models/actual-model");
    normal.roles[0].environment.push_back("CLAUDE_SESSION_ID=session-123");
    normal.roles[0].environment.push_back("ENGRAM_LATTICE_LOG_LEVEL=error");
    EXPECT_NO_THROW(install::validate_primary_mcp_contract(origin,normal));
    normal.roles[0].environment.push_back("OTHER=value");
    EXPECT_THROW(install::validate_primary_mcp_contract(origin,normal),std::runtime_error);
    normal=install::decode_manifest(offer.normal_catalog);
    normal.roles[0].environment.push_back("CLAUDE_SESSION_ID="+std::string(8192,'x'));
    EXPECT_THROW(install::validate_primary_mcp_contract(origin,normal),std::runtime_error);
}
TEST(OrdinaryNormalStartup, AdmittedJournalUsesNewVersionAndRetainsIdentityAndRetirementHistory) {
    auto value=sample().admission;const auto old=admission::encode(value);EXPECT_EQ(old[8],1);
    EXPECT_EQ(std::vector<std::uint8_t>(old.begin()+176,old.begin()+224),std::vector<std::uint8_t>(48,0));
    value.state=admission::stage::ordinary_admitted;value.revision=2;value.normal_launch=id(4);
    const auto admitted=admission::encode(value);EXPECT_EQ(admitted[8],2);EXPECT_EQ(admission::decode(admitted),value);
    EXPECT_EQ(std::vector<std::uint8_t>(admitted.begin()+32,admitted.begin()+80),std::vector<std::uint8_t>(old.begin()+32,old.begin()+80));
    EXPECT_EQ(std::vector<std::uint8_t>(admitted.begin()+96,admitted.begin()+176),std::vector<std::uint8_t>(old.begin()+96,old.begin()+176));
    value.state=admission::stage::retirement_requested;value.revision=3;value.cutover=id(5);
    EXPECT_EQ(admission::decode(admission::encode(value)).normal_launch,id(4));
    value.state=admission::stage::unadopted;value.cutover={};EXPECT_THROW(admission::encode(value),admission::error);
}
TEST(OrdinaryNormalStartup, EarlyAdmittedLateUnadoptedAndWrongLaunchOffersRefuse) {
    auto offer=sample();offer.admission.state=admission::stage::ordinary_admitted;offer.admission.revision=2;offer.admission.normal_launch=offer.nonce;
    EXPECT_THROW(install::encode_normal_offer(offer),std::runtime_error);
    offer=sample();offer.phase=3;EXPECT_THROW(install::encode_normal_offer(offer),std::runtime_error);
    offer.admission.state=admission::stage::ordinary_admitted;offer.admission.revision=2;offer.admission.normal_launch=id(99);
    EXPECT_THROW(install::encode_normal_offer(offer),std::runtime_error);
    offer=sample();offer.admission.revision=2;EXPECT_THROW(install::encode_normal_offer(offer),std::runtime_error);
}
