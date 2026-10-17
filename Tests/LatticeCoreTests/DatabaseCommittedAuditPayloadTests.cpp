#include "TestHelpers.hpp"
#include <utility>

namespace lattice {
struct database_query_test_access {
    static void set(std::function<void(sqlite3_stmt*)> hook) {
        database::test_hook_query_row_ = std::move(hook);
    }
};
} // namespace lattice

namespace {
struct AuditFixture {
    TempDB file{"committed_audit_payload"};
    std::unique_ptr<lattice::lattice_db> owner;
    std::vector<int64_t> observed_ids;
    std::vector<std::vector<lattice::lattice_db::change_event>> model_batches;
    lattice::lattice_db::observer_id audit_observer, model_observer;
    std::string table;
    explicit AuditFixture(bool disk, std::string model="TestPerson") : table(std::move(model)) {
        lattice::configuration config;
        if (disk) config.path=file.str();
        owner=std::make_unique<lattice::lattice_db>(config);
        audit_observer=owner->add_table_observer("AuditLog",[&](const auto& events){
            for(const auto& event:events) if(std::get<1>(event)=="INSERT") observed_ids.push_back(std::get<2>(event));
        });
        model_observer=owner->add_table_observer(table,[&](const auto& events){model_batches.push_back(events);});
    }
    ~AuditFixture() { owner->remove_table_observer("AuditLog",audit_observer); owner->remove_table_observer(table,model_observer); }
    lattice::database& db() { return owner->db(); }
    void reset() { observed_ids.clear(); model_batches.clear(); }
    std::vector<int64_t> ids(const std::string& table_name,const std::string& operation="UPDATE") {
        auto rows=db().query("SELECT id FROM AuditLog WHERE tableName=? AND operation=? ORDER BY id",{table_name,operation});
        std::vector<int64_t> result;
        for(const auto& row:rows) result.push_back(std::get<int64_t>(row.at("id")));
        return result;
    }
};
struct OuterRowHook {
    explicit OuterRowHook(std::function<void()> body) {
        lattice::database_query_test_access::set([body=std::move(body),used=false](sqlite3_stmt* statement) mutable {
            const char* sql=sqlite3_sql(statement);
            if(!sql || std::string(sql).find("/* audit outer */")==std::string::npos || std::exchange(used,true)) return;
            body();
        });
    }
    ~OuterRowHook() { lattice::database_query_test_access::set(nullptr); }
};
void same_row(bool disk) {
    AuditFixture f(disk);
    auto person=f.owner->add(TestPerson{"one",1,std::nullopt}); f.reset();
    const std::string where=" WHERE id="+std::to_string(person.id());
    f.db().execute("UPDATE TestPerson SET age=2"+where+";UPDATE TestPerson SET age=3"+where);
    const auto actual=f.ids("TestPerson");
    ASSERT_EQ(actual.size(),2u); EXPECT_EQ(f.observed_ids,actual);
    ASSERT_EQ(f.model_batches.size(),2u);
    for(const auto& batch:f.model_batches) { ASSERT_EQ(batch.size(),1u); EXPECT_EQ(std::get<3>(batch[0]),person.global_id()); }
}
void same_row_fields(bool disk) {
    AuditFixture f(disk);
    f.db().execute("CREATE UNIQUE INDEX unique_person_name ON TestPerson(name)");
    auto person=f.owner->add(TestPerson{"one",1,std::nullopt}); f.reset();
    {
        OuterRowHook hook([&]{
            f.owner->add(TestPerson{"one",2,std::nullopt},lattice::managed<TestPerson>::schema(),{"name"});
            f.owner->add(TestPerson{"one",2,"changed@example.test"},lattice::managed<TestPerson>::schema(),{"name"});
            EXPECT_TRUE(f.model_batches.empty());
        });
        f.db().query("SELECT 1 /* audit outer */");
    }
    const auto actual=f.ids("TestPerson"); ASSERT_EQ(actual.size(),2u); EXPECT_EQ(f.observed_ids,actual);
    ASSERT_EQ(f.model_batches.size(),2u);
    ASSERT_EQ(f.model_batches[0].size(),1u); ASSERT_EQ(f.model_batches[1].size(),1u);
    const auto history=f.db().query("SELECT changedFieldsNames FROM AuditLog WHERE tableName='TestPerson' AND operation='UPDATE' ORDER BY id");
    ASSERT_EQ(history.size(),2u);
    for(size_t i=0;i<2;++i) EXPECT_EQ(std::get<4>(f.model_batches[i][0]),std::get<std::string>(history[i].at("changedFieldsNames")));
    EXPECT_NE(std::get<4>(f.model_batches[0][0]).find("\"age\""),std::string::npos);
    EXPECT_EQ(std::get<4>(f.model_batches[0][0]).find("\"email\""),std::string::npos);
    EXPECT_NE(std::get<4>(f.model_batches[1][0]).find("\"email\""),std::string::npos);
    EXPECT_EQ(std::get<4>(f.model_batches[1][0]).find("\"age\""),std::string::npos);
}
void internal_links(bool disk) {
    AuditFixture f(disk,"TestOwner");
    auto owner=f.owner->add(TestOwner{"owner",nullptr});
    auto p1=f.owner->add(TestPet{"pet1",1}); auto p2=f.owner->add(TestPet{"pet2",2});
    f.owner->ensure_link_table("_TestOwner_TestPet_pet","TestOwner:pet","TestPet");
    ASSERT_EQ(std::get<std::string>(f.db().query("SELECT value FROM _lattice_meta WHERE key='internal_table:_TestOwner_TestPet_pet'")[0].at("value")),"TestOwner:pet");
    f.reset();
    {
        OuterRowHook hook([&]{
            f.db().execute("INSERT INTO _TestOwner_TestPet_pet(lhs,rhs) VALUES(?,?)",{owner.global_id(),p1.global_id()});
            f.db().execute("INSERT INTO _TestOwner_TestPet_pet(lhs,rhs) VALUES(?,?)",{owner.global_id(),p2.global_id()});
            EXPECT_TRUE(f.observed_ids.empty());
        });
        f.db().query("SELECT 1 /* audit outer */");
    }
    const auto actual=f.ids("_TestOwner_TestPet_pet","INSERT"); ASSERT_EQ(actual.size(),2u); EXPECT_EQ(f.observed_ids,actual);
    ASSERT_EQ(f.model_batches.size(),2u);
    for(const auto& batch:f.model_batches) { ASSERT_EQ(batch.size(),1u); EXPECT_EQ(std::get<1>(batch[0]),"UPDATE"); EXPECT_EQ(std::get<4>(batch[0]),"[\"pet\"]"); }
}
void later_rollback(bool disk) {
    AuditFixture f(disk);
    auto person=f.owner->add(TestPerson{"one",1,std::nullopt});f.reset();
    const std::string where=" WHERE id="+std::to_string(person.id());
    f.db().execute("UPDATE TestPerson SET age=2"+where+";BEGIN;UPDATE TestPerson SET age=3"+where+";ROLLBACK");
    const auto actual=f.ids("TestPerson"); ASSERT_EQ(actual.size(),1u); EXPECT_EQ(f.observed_ids,actual);
    ASSERT_EQ(f.model_batches.size(),1u);
    EXPECT_EQ(std::get<int64_t>(f.db().query("SELECT age FROM TestPerson")[0].at("age")),2);
}
}
TEST(AuditPayloadQualification, MemorySameRowCommitsRetainDistinctAuditIds) { same_row(false); }
TEST(AuditPayloadQualification, FileSameRowCommitsRetainDistinctAuditIds) { same_row(true); }
TEST(AuditPayloadQualification, MemoryUpsertFieldsBelongToTheirOwnCommit) { same_row_fields(false); }
TEST(AuditPayloadQualification, FileUpsertFieldsBelongToTheirOwnCommit) { same_row_fields(true); }
TEST(AuditPayloadQualification, MemoryLinkCommitsRetainIdsAndParentFields) { internal_links(false); }
TEST(AuditPayloadQualification, FileLinkCommitsRetainIdsAndParentFields) { internal_links(true); }
TEST(AuditPayloadQualification, MemoryLaterRollbackPreservesOnlyEarlierCommittedAudit) { later_rollback(false); }
TEST(AuditPayloadQualification, FileLaterRollbackPreservesOnlyEarlierCommittedAudit) { later_rollback(true); }
