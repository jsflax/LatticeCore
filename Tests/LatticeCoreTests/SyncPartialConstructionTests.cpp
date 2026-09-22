#include "TestHelpers.hpp"

#ifndef __EMSCRIPTEN__
namespace {
using namespace lattice;

struct construction_probe {
    int factories=0, transports_destroyed=0, disconnects=0;
};
class construction_transport final : public mock_sync_transport {
    std::shared_ptr<construction_probe> probe_;
public:
    explicit construction_transport(std::shared_ptr<construction_probe> probe):probe_(std::move(probe)){}
    ~construction_transport()override{++probe_->transports_destroyed;}
    void disconnect()override{++probe_->disconnects;mock_sync_transport::disconnect();}
};
class construction_factory final : public network_factory {
public:
    enum class behavior { fail, empty, succeed } mode=behavior::fail;
    const std::shared_ptr<construction_probe> probe=std::make_shared<construction_probe>();
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override{
        ++probe->factories;
        if(mode==behavior::fail)throw db_error("fixture transport factory failed");
        if(mode==behavior::empty)return nullptr;
        return std::make_unique<construction_transport>(probe);
    }
};
struct construction_factory_scope {
    std::shared_ptr<network_factory> prior=get_network_factory();
    const std::shared_ptr<construction_factory> factory=std::make_shared<construction_factory>();
    construction_factory_scope(){set_network_factory(factory);}
    ~construction_factory_scope(){set_network_factory(prior);}
};
}

TEST(SyncPartialConstruction, NullOwnersRefuseBeforeFactoryAndReleaseInjectedTransport) {
    construction_factory_scope scope;
    const lattice::sync_config config;
    EXPECT_THROW((void)lattice::synchronizer(std::shared_ptr<lattice::lattice_db>{},config),lattice::db_error);
    EXPECT_THROW((void)lattice::synchronizer(std::unique_ptr<lattice::lattice_db>{},config),lattice::db_error);
    EXPECT_THROW((void)lattice::synchronizer(std::unique_ptr<lattice::lattice_db>{},config,
        std::make_unique<construction_transport>(scope.factory->probe)),lattice::db_error);
    EXPECT_EQ(scope.factory->probe->factories,0);
    EXPECT_EQ(scope.factory->probe->transports_destroyed,1);
    EXPECT_EQ(scope.factory->probe->disconnects,0);
}

TEST(SyncPartialConstruction, ThrowingAndEmptyFactoryLeaveOwnerUsableForLaterConstruction) {
    construction_factory_scope scope;
    TempDB file("sync_partial_factory");
    auto owner=std::make_shared<lattice::lattice_db>(lattice::configuration(file.str()));
    lattice::sync_config config;config.checkpoint_passive_interval_ms=0;
    EXPECT_THROW((void)lattice::synchronizer(owner,config),lattice::db_error);
    scope.factory->mode=construction_factory::behavior::empty;
    EXPECT_THROW((void)lattice::synchronizer(owner,config),lattice::db_error);
    EXPECT_EQ(scope.factory->probe->factories,2);
    EXPECT_EQ(scope.factory->probe->transports_destroyed,0);
    EXPECT_FALSE(owner->is_closed());
    owner->add(TestPerson{"after-refusal",42,std::nullopt});
    scope.factory->mode=construction_factory::behavior::succeed;
    {lattice::synchronizer live(owner,config);EXPECT_FALSE(live.is_connected());}
    EXPECT_EQ(scope.factory->probe->factories,3);
    EXPECT_EQ(scope.factory->probe->disconnects,1);
    EXPECT_EQ(scope.factory->probe->transports_destroyed,1);
    const auto rows=owner->db().query("SELECT name FROM TestPerson");
    ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows.front().at("name")),"after-refusal");
}

TEST(SyncPartialConstruction, EmptyInjectedTransportUnwindsWithoutPoisoningStore) {
    construction_factory_scope scope;
    TempDB file("sync_partial_injected");
    auto owner=std::make_unique<lattice::lattice_db>(lattice::configuration(file.str()));
    owner->add(TestPerson{"survives",7,std::nullopt});
    const lattice::sync_config config;
    EXPECT_THROW((void)lattice::synchronizer(std::move(owner),config,
        std::unique_ptr<lattice::sync_transport>{}),lattice::db_error);
    EXPECT_FALSE(owner);
    EXPECT_EQ(scope.factory->probe->factories,0);
    lattice::lattice_db reopened{lattice::configuration(file.str())};
    const auto rows=reopened.db().query("SELECT name FROM TestPerson");
    ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows.front().at("name")),"survives");
}
#endif
