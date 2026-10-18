#include <gtest/gtest.h>
#include <lattice/lattice.hpp>
#include <lattice/store_resource_context.hpp>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>
#if defined(__APPLE__) || defined(__linux__)
#include <pwd.h>
#include <unistd.h>
#endif

namespace lattice::detail {
struct resource_factory_test_access {
    static bool original_policy_is_healthy(const database& owner) {
        owner.require_creator_();
        return !owner.physical_open_original_.observer_policy_unsettled();
    }
};
}
namespace {
std::string actual_home() {
#if defined(__APPLE__) || defined(__linux__)
    struct passwd value{},*found=nullptr;char buffer[16384];
    if(::getpwuid_r(::geteuid(),&value,buffer,sizeof(buffer),&found)!=0||!found||!found->pw_dir||found->pw_dir[0]!='/')
        throw std::runtime_error("actual account home unavailable");
    return found->pw_dir;
#else
    throw std::runtime_error("actual resource profile unsupported on this platform");
#endif
}
std::int64_t scalar(lattice::database& db,const std::string& sql) {
    const auto rows=db.query(sql);if(rows.size()!=1||rows.front().size()!=1)
        throw std::runtime_error("one actual scalar required");
    return std::get<std::int64_t>(rows.front().begin()->second);
}
}
TEST(StoreResourceFactory, GenuineColdEpochOrdinaryFactoryKeepsPriorCommitAndIssuedOriginal) {
    using namespace lattice::detail;
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(20);
    const auto home=actual_home();const auto logical=home+"/.claude/memory.sqlite";
    // This enters the REAL image-checked normal issuer before any Native open.
    // An absent/failed epoch fails here without opening a legacy test database.
    const auto physical=store_resource_factory_path(logical,end);
    ASSERT_NE(physical,logical);ASSERT_TRUE(store_resource_profile_active());
    ASSERT_TRUE(std::filesystem::path(physical).is_absolute());
    lattice::configuration config(logical);lattice::lattice_db owner(config,true);
    const auto identity=owner.db().physical_identity("main",{},true);
    ASSERT_TRUE(identity);EXPECT_EQ(identity->filename,physical);
    EXPECT_TRUE(resource_factory_test_access::original_policy_is_healthy(owner.db()));
    // Seeded by a real prior raw-SQLite writer, then moved only by actual Host
    // retirement/namespace exchange. No direct physical lease adoption occurs.
    EXPECT_EQ(scalar(owner.db(),"SELECT value FROM resource_bootstrap WHERE globalId='old'"),41);
    EXPECT_THROW(owner.db().handle(),lattice::db_error);
    owner.db().execute("INSERT INTO resource_bootstrap(globalId,value) VALUES('new',42)");
    EXPECT_EQ(scalar(owner.db(),"SELECT value FROM resource_bootstrap WHERE globalId='new'"),42);
    // Also exercise actual normal CREATE/initial-intent admission in a permitted
    // dynamic group slot. Retain files as evidence; this is not delete testing.
    const auto created_logical=home+"/.claude/sync/resource-bootstrap.sqlite";
    lattice::configuration created_config(created_logical);lattice::lattice_db created(created_config,true);
    EXPECT_TRUE(resource_factory_test_access::original_policy_is_healthy(created.db()));
    const auto created_identity=created.db().physical_identity("main",{},true);ASSERT_TRUE(created_identity);
    EXPECT_NE(created_identity->filename,created_logical);
    created.db().execute("CREATE TABLE bootstrap_created(globalId TEXT NOT NULL UNIQUE,value INTEGER NOT NULL)");
    created.db().execute("INSERT INTO bootstrap_created(globalId,value) VALUES('created',43)");
    EXPECT_EQ(scalar(created.db(),"SELECT value FROM bootstrap_created WHERE globalId='created'"),43);
    EXPECT_THROW(created.db().handle(),lattice::db_error);
}

TEST(StoreResourceFactory, NestedRootDenialDoesNotSelectItsParentOrComponentSibling) {
    using namespace lattice::detail;
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(20);
    const auto home=actual_home();
    const auto approved=home+"/.claude/sync/resource-policy-control.sqlite";
    const auto physical=store_resource_factory_path(approved,end);
    ASSERT_NE(physical,approved);ASSERT_TRUE(store_resource_profile_active());
    // The same real installed issuer must accept its already translated route.
    EXPECT_EQ(store_resource_factory_path(physical,end),physical);
    for(const auto& rejected:{home+"/.claude/sync/resource-policy-denied.bin",
                             home+"/.claude/sync-sibling.sqlite"}) {
        // Refuse before reaching Native even if a later factory accidentally
        // tries to treat a parent or similarly prefixed sibling as this root.
        ASSERT_THROW((void)store_resource_factory_path(rejected,end),std::runtime_error);
        lattice::configuration configuration(rejected);
        EXPECT_THROW({lattice::lattice_db actual(configuration,true);},std::runtime_error);
    }
}
