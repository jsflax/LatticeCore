#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_installation_manifest.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <algorithm>
#include <limits>

namespace {
namespace installation = lattice::detail::ordinary_installation;
installation::identifier installation_id(std::uint8_t byte) {
    installation::identifier value{}; value[0] = byte; return value;
}
installation::digest installation_digest(std::uint8_t byte) {
    installation::digest value{}; value[0] = byte; return value;
}
installation::manifest manifest_sample() {
    installation::manifest value;
    value.installation = installation_id(1); value.revision = 7;
    value.catalog_directory = {4, 10}; value.launch_gate = {4, 11};
    value.supervisor = {"/installed/lattice-installation-launch", {4, 12}, installation_digest(20)};
    installation::store_fact memory;
    memory.binding = {installation_id(1), installation_id(2), installation_id(3), {4, 30}, {4, 31}};
    memory.controls = {{4, 40}, {4, 41}, {4, 42}}; memory.control_leaf = "memory";
    memory.aliases = {{"/ordinary", "memory.sqlite", {4, 31}}}; value.stores.push_back(memory);
    installation::role_fact mcp;
    mcp.name = "memory-mcp"; mcp.executable = {"/installed/memory", {4, 50}, installation_digest(21)};
    mcp.working_directory = "/ordinary"; mcp.arguments = {"--stdio"}; mcp.environment = {"PATH=/usr/bin:/bin"};
    mcp.stores = {installation_id(2)}; value.roles.push_back(mcp);
    return value;
}
void rehash_manifest(std::vector<std::uint8_t>& bytes) {
    picosha2::hash256(bytes.begin(), bytes.end() - 32, bytes.end() - 32, bytes.end());
}
}

TEST(OrdinaryInstallationManifest, CanonicalCatalogMatchesIndependentByteLayout) {
    const auto bytes = installation::encode_manifest(manifest_sample());
    installation::digest hash{};
    picosha2::hash256(bytes.begin(), bytes.end(), hash.begin(), hash.end());
    EXPECT_EQ(bytes.size(), 607u);
    // Independent Python struct/hashlib encoding of the documented field order.
    EXPECT_EQ(picosha2::bytes_to_hex_string(hash), "a6b041d68a2a18b0b4472d9ce6c02f070e881d20308aff244c3cce9482b04668");
    EXPECT_EQ(installation::decode_manifest(bytes), manifest_sample());
}

TEST(OrdinaryInstallationManifest, ProductRoleArgumentsAliasesAndCustodyRoundTripExactly) {
    auto value = manifest_sample();
    value.application = installation::product::orbital;
    value.roles[0].custody = installation::lifetime::installation_bound;
    value.roles[0].arguments.push_back("");
    value.roles[0].arguments.push_back("a value with spaces");
    value.stores[0].aliases.push_back({"/legacy", "memory.sqlite", {4, 60}});
    EXPECT_EQ(installation::decode_manifest(installation::encode_manifest(value)), value);
}

TEST(OrdinaryInstallationManifest, EveryMutatedByteAndEveryTruncatedLengthRefuses) {
    const auto original = installation::encode_manifest(manifest_sample());
    for (std::size_t i = 0; i != original.size(); ++i) {
        auto bytes = original; bytes[i] ^= 1;
        EXPECT_THROW(installation::decode_manifest(bytes), installation::manifest_error);
        bytes.assign(original.begin(), original.begin() + i);
        EXPECT_THROW(installation::decode_manifest(bytes), installation::manifest_error);
    }
    auto extra = original; extra.push_back(0);
    EXPECT_THROW(installation::decode_manifest(extra), installation::manifest_error);
}

TEST(OrdinaryInstallationManifest, RehashedUnboundedCountAndUnknownVersionStillRefuse) {
    const auto original = installation::encode_manifest(manifest_sample());
    auto bytes = original;
    // Independent canonical store-count offset, before allocation of any store.
    std::fill_n(bytes.begin() + 174, 8, 255); rehash_manifest(bytes);
    EXPECT_THROW(installation::decode_manifest(bytes), installation::manifest_error);
    bytes = original; bytes[8] = 2; rehash_manifest(bytes);
    EXPECT_THROW(installation::decode_manifest(bytes), installation::manifest_error);
    bytes = original; bytes[16] = 3; rehash_manifest(bytes);
    EXPECT_THROW(installation::decode_manifest(bytes), installation::manifest_error);
}

TEST(OrdinaryInstallationManifest, DuplicatePhysicalControlAliasAndRoleBindingsRefuse) {
    auto value = manifest_sample();
    auto second = value.stores[0]; second.binding.store = installation_id(9); second.control_leaf = "other";
    value.stores.push_back(second);
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.stores[0].controls.generation = value.stores[0].controls.entry;
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.stores[0].aliases.push_back(value.stores[0].aliases[0]);
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.roles.push_back(value.roles[0]);
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.roles[0].stores.push_back(installation_id(99));
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.roles[0].environment.push_back("PATH=/other");
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
}

TEST(OrdinaryInstallationManifest, ParentTraversalMissingAnchorAndUnsupportedBoundsRefuse) {
    auto value = manifest_sample(); value.stores[0].aliases[0].parent_path = "/ordinary/../other";
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.stores[0].control_leaf = "../other";
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.supervisor.content = {};
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.catalog_directory = {};
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.stores[0].aliases[0].parent = {4, 99};
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.roles[0].arguments.assign(129, "x");
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
    value = manifest_sample(); value.roles[0].arguments.assign(128, std::string(8192, 'x'));
    EXPECT_THROW(installation::encode_manifest(value), installation::manifest_error);
}
