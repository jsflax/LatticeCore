#include "ObserverDeliveryConnectionTestSupport.hpp"
#include <lattice/detail/observer_delivery_target.hpp>

namespace {
using observer_delivery_testing::Fixture;
struct CallerRollback {
    lattice::lattice_db& owner;
    ~CallerRollback() noexcept {
        try {if(owner.db().is_in_transaction())owner.rollback();}
        catch(...) {
            // A cleanup refusal must retain the file and original ownership;
            // do not terminate without facts or unwind into TempDB unlink.
            std::fprintf(stderr,"delivery caller-rollback ending-debt process=%ld group=%ld namespaceRetained=1\n",
                long(::getpid()),long(::getpgrp()));
            std::fflush(stderr);::_exit(96);
        }
    }
};

TEST(ObserverDeliveryTarget, ActualQueryBindingAndFieldsRetainSameRowIdentity) {
    Fixture actual;
    auto queried=actual.owner->find<TestPerson>(1);
    ASSERT_TRUE(queried.has_value());
    const auto model=queried->observer_delivery_target_for("name");
    const auto field=queried->name.observer_delivery_target_for_field();
    EXPECT_TRUE(model.valid());EXPECT_TRUE(field.valid());
    EXPECT_EQ(model.rowid(),queried->id());
    EXPECT_EQ(field.rowid(),model.rowid());
    EXPECT_EQ(field.global_id(),queried->global_id());
    EXPECT_EQ(field.table(),"TestPerson");EXPECT_EQ(field.column(),"name");
    queried->name=std::string("query-bound-update");
    EXPECT_EQ(queried->name.unmanaged_value,"query-bound-update");
    const auto rows=actual.owner->db().query("SELECT name,globalId FROM TestPerson WHERE id=1");
    ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")),"query-bound-update");
    EXPECT_EQ(std::get<std::string>(rows[0].at("globalId")),model.global_id());
}

TEST(ObserverDeliveryTarget, ActualCallerBeginInsertAndStringUpdateSpendOneOriginal) {
    Fixture actual;CallerRollback rollback{*actual.owner};
    actual.owner->begin_transaction();
    auto inserted=actual.owner->add(TestPerson{"original",9,std::nullopt});
    const auto id=inserted.id();const auto gid=inserted.global_id();
    inserted.name=std::string("same-transaction");
    EXPECT_TRUE(actual.owner->db().is_in_transaction());
    actual.owner->commit();
    const auto rows=actual.owner->db().query("SELECT name,globalId FROM TestPerson WHERE id=?",{id});
    ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")),"same-transaction");
    EXPECT_EQ(std::get<std::string>(rows[0].at("globalId")),gid);
    EXPECT_EQ(inserted.name.unmanaged_value,"same-transaction");
}

TEST(ObserverDeliveryTarget, RolledBackInsertedBindingPreservesMissingRowNoopAndCacheAdoption) {
    Fixture actual;CallerRollback rollback{*actual.owner};
    actual.owner->begin_transaction();
    auto inserted=actual.owner->add(TestPerson{"rolled-back",9,std::nullopt});
    const auto id=inserted.id();
    ASSERT_TRUE(inserted.name.observer_delivery_target_for_field().valid());
    actual.owner->rollback();
    ASSERT_TRUE(actual.owner->db().query("SELECT id FROM TestPerson WHERE id=?",{id}).empty());
    EXPECT_NO_THROW(inserted.name=std::string("missing-row-cache"));
    EXPECT_EQ(inserted.name.unmanaged_value,"missing-row-cache");
    EXPECT_TRUE(actual.owner->db().query("SELECT id FROM TestPerson WHERE id=?",{id}).empty());
}

TEST(ObserverDeliveryTarget, RolledBackRowidReusedByDifferentGlobalIdRefusesStaleUpdate) {
    Fixture actual;CallerRollback rollback{*actual.owner};
    actual.owner->begin_transaction();
    auto stale=actual.owner->add(TestPerson{"old-binding",9,std::nullopt});
    const auto id=stale.id();const auto old_gid=stale.global_id();
    actual.owner->rollback();
    auto replacement=actual.owner->add(TestPerson{"replacement",10,std::nullopt});
    ASSERT_EQ(replacement.id(),id) << "real rolled-back SQLite sequence must reuse this ID";
    ASSERT_NE(replacement.global_id(),old_gid);
    const auto old_cache=stale.name.unmanaged_value;
    const std::string requested="must-not-replace-other-row";
    EXPECT_THROW(stale.name=requested,lattice::db_error);
    EXPECT_EQ(stale.name.unmanaged_value,old_cache);
    const auto rows=actual.owner->db().query("SELECT name,globalId FROM TestPerson WHERE id=?",{id});
    ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")),"replacement");
    EXPECT_EQ(std::get<std::string>(rows[0].at("globalId")),replacement.global_id());
}
TEST(ObserverDeliveryTarget, ActualNumericFieldUpdateUsesBoundIdentityAndCache) {
    Fixture actual;auto row=actual.owner->find<TestPerson>(1);
    ASSERT_TRUE(row.has_value());ASSERT_TRUE(row->age.observer_delivery_target_for_field().valid());
    row->age=41;
    EXPECT_EQ(row->age.unmanaged_value,41);
    const auto rows=actual.owner->db().query("SELECT age FROM TestPerson WHERE id=1");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<int64_t>(rows[0].at("age")),41);
}



TEST(ObserverDeliveryTarget, ActualTransferredStringCapacityIsChargedBeforeSqlOrCacheAdoption) {
    Fixture actual;auto row=actual.owner->find<TestPerson>(1);
    ASSERT_TRUE(row.has_value());row->name.unmanaged_value.reserve(2u*1024*1024);
    ASSERT_GE(row->name.unmanaged_value.capacity(),2u*1024*1024);
    const auto before=row->name.unmanaged_value;
    try {
        row->name=std::string("new");
        FAIL()<<"actual retained capacity must exceed this original's workspace";
    }catch(const std::length_error& error) {
        EXPECT_STREQ(error.what(),"delivery output exceeds the original admitted workspace");
    }
    EXPECT_EQ(row->name.unmanaged_value,before);
    const auto rows=actual.owner->db().query("SELECT name FROM TestPerson WHERE id=1");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("name")),before);
}

}
