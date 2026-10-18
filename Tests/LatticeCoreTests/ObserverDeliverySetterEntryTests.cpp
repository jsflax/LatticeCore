#include "TestHelpers.hpp"
#include <LatticeCore.hpp>

struct DeliverySetterRow {std::string value;};
LATTICE_SCHEMA(DeliverySetterRow,value);

namespace {
class ObserverDeliverySetterEntry:public ::testing::Test {
protected:
    lattice::lattice_db owner{lattice::configuration(":memory:")};
    std::string stored() {
        const auto rows=owner.db().query("SELECT value FROM DeliverySetterRow");
        if(rows.size()!=1)throw std::runtime_error("one actual setter row required");
        return std::get<std::string>(rows[0].at("value"));
    }
    void refuse_update() {
        owner.db().execute("CREATE TRIGGER delivery_setter_refuse BEFORE UPDATE ON DeliverySetterRow "
                           "BEGIN SELECT RAISE(ABORT,'actual setter failure'); END");
    }
};

TEST_F(ObserverDeliverySetterEntry, LegacyLvalueSelfAliasKeepsOwningSnapshotAndFirstSqlError) {
    const auto initial=std::string(4096,'a');
    auto model=owner.add(DeliverySetterRow{initial});
    const auto& alias=model.value.unmanaged_value;
    model.value=alias;
    EXPECT_EQ(stored(),initial);EXPECT_EQ(model.value.unmanaged_value,initial);
    refuse_update();
    EXPECT_THROW(model.value=alias,lattice::db_error);
    EXPECT_EQ(stored(),initial);EXPECT_EQ(model.value.unmanaged_value,initial);
}

TEST_F(ObserverDeliverySetterEntry, LegacyRvalueAliasConsumesBeforeActualSqlFailure) {
    const auto initial=std::string(4096,'b');
    auto model=owner.add(DeliverySetterRow{initial});
    // Compare to the actual standard-library move used by the old by-value
    // entry, rather than specify a portable moved-from string's contents.
    std::string reference=initial;
    {std::string old_parameter(std::move(reference));}
    refuse_update();
    EXPECT_THROW(model.value=std::move(model.value.unmanaged_value),lattice::db_error);
    EXPECT_EQ(stored(),initial);
    EXPECT_EQ(model.value.unmanaged_value,reference);
}

TEST_F(ObserverDeliverySetterEntry, LegacyRvalueSuccessAndMissingRowCacheRemainSupported) {
    auto model=owner.add(DeliverySetterRow{"before"});
    std::string source(4096,'c');
    std::string reference=source;
    {std::string old_parameter(std::move(reference));}
    model.value=std::move(source);
    EXPECT_EQ(source,reference);EXPECT_EQ(stored(),std::string(4096,'c'));
    owner.db().execute("DELETE FROM DeliverySetterRow");
    EXPECT_NO_THROW(model.value=std::string("missing-row-cache"));
    EXPECT_EQ(model.value.unmanaged_value,"missing-row-cache");
    EXPECT_TRUE(owner.db().query("SELECT id FROM DeliverySetterRow").empty());
}
}
