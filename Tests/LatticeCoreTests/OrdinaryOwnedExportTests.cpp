#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/ordinary_export.hpp"

namespace {
lattice::configuration config(const TempDB& path) {
    lattice::configuration result(path.str());result.audit_retention_seconds=0;result.busy_timeout_ms=100;return result;
}
int64_t scalar(lattice::database& db,const std::string& sql){return std::get<int64_t>(db.query(sql).at(0).at("n"));}
std::string captured_name(const lattice::owned_server_export& value) {
    TempDB path{"owned_export_inspect_through_receiver"};lattice::lattice_db receiver{config(path)};
    const auto ids=lattice::apply_owned_server_export(receiver,value);
    if(ids.size()!=value.entries().size())throw std::runtime_error("issued capture did not install");
    return std::get<std::string>(receiver.db().query("SELECT name FROM TestPerson").at(0).at("name"));
}

struct authorizer_reset {
    sqlite3* handle;
    ~authorizer_reset(){sqlite3_set_authorizer(handle,nullptr,nullptr);}
};
}

TEST(OrdinaryOwnedExport, ActualOwnerIssuesImmutableCompleteCutWithOriginalAuditIdentity) {
    TempDB path{"owned_export_capture"};lattice::lattice_db owner{config(path)};
    owner.add(TestPerson{"first",4,std::nullopt});
    const auto original=lattice::query_audit_log(owner.db(),false,std::nullopt);
    ASSERT_EQ(original.size(),1u);
    const auto first=lattice::capture_owned_server_export(owner,{"TestPerson"});
    EXPECT_EQ(first.epoch(),1);EXPECT_EQ(first.cut(),1);EXPECT_EQ(first.row_count(),1u);
    ASSERT_EQ(first.entries().size(),1u);EXPECT_EQ(first.entries()[0].global_id,original[0].global_id);
    EXPECT_EQ(captured_name(first),"first");
    owner.db().execute("UPDATE TestPerson SET name='second'");
    const auto second=lattice::capture_owned_server_export(owner,{"TestPerson"});
    EXPECT_EQ(first.source_id(),second.source_id());EXPECT_EQ(second.cut(),2);EXPECT_EQ(captured_name(second),"second");
    EXPECT_EQ(captured_name(first),"first");EXPECT_EQ(first.entries().size(),1u);EXPECT_EQ(first.cut(),1);
    EXPECT_FALSE(owner.db().is_in_transaction());
}

TEST(OrdinaryOwnedExport, DurableSourceAndCutSurviveClosingAndReopeningTheOwner) {
    TempDB path{"owned_export_reopen"};std::string source;
    {
        lattice::lattice_db owner{config(path)};owner.add(TestPerson{"durable",8,std::nullopt});
        source=lattice::capture_owned_server_export(owner,{"TestPerson"}).source_id();
        EXPECT_EQ(lattice::capture_owned_server_export(owner,{"TestPerson"}).cut(),2);
    }
    lattice::lattice_db reopened{config(path)};
    const auto next=lattice::capture_owned_server_export(reopened,{"TestPerson"});
    EXPECT_EQ(next.source_id(),source);EXPECT_EQ(next.cut(),3);EXPECT_EQ(captured_name(next),"durable");
}

TEST(OrdinaryOwnedExport, UnenrolledHistoricalReopenCannotAcquireSourceFromEmptyMarkerAbsence) {
    TempDB path{"owned_export_historical"};
    {lattice::lattice_db owner{config(path)};owner.add(TestPerson{"preserved",6,std::nullopt});}
    lattice::lattice_db reopened{config(path)};
    EXPECT_THROW(lattice::capture_owned_server_export(reopened,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
    EXPECT_FALSE(reopened.db().table_exists("_lattice_ordered_source_v1"));
    EXPECT_EQ(scalar(reopened.db(),"SELECT COUNT(*) AS n FROM TestPerson WHERE name='preserved'"),1);
    EXPECT_FALSE(reopened.db().is_in_transaction());
}

TEST(OrdinaryOwnedExport, CallerTransactionCannotPublishAnUncommittedCut) {
    TempDB path{"owned_export_caller_turn"};lattice::lattice_db owner{config(path)};
    owner.add(TestPerson{"committed",1,std::nullopt});owner.begin_transaction();
    owner.db().execute("UPDATE TestPerson SET name='caller-only'");
    EXPECT_THROW(lattice::capture_owned_server_export(owner,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
    EXPECT_TRUE(owner.db().is_in_transaction());EXPECT_FALSE(owner.db().table_exists("_lattice_ordered_source_v1"));
    owner.rollback();const auto first=lattice::capture_owned_server_export(owner,{"TestPerson"});
    EXPECT_EQ(first.cut(),1);EXPECT_EQ(captured_name(first),"committed");
}

TEST(OrdinaryOwnedExport, ActualCommitDenialLeavesPriorDurableCutAndIssuesNoCandidate) {
    TempDB path{"owned_export_commit_denied"};lattice::lattice_db owner{config(path)};
    owner.add(TestPerson{"before",1,std::nullopt});const auto first=lattice::capture_owned_server_export(owner,{"TestPerson"});
    owner.db().execute("UPDATE TestPerson SET name='after'");auto* handle=owner.db().handle();authorizer_reset reset{handle};int denials=0;
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* raw,int action,const char* first,const char*,const char*,const char*)noexcept{
        if(action==SQLITE_TRANSACTION&&first&&std::strcmp(first,"COMMIT")==0){++*static_cast<int*>(raw);return SQLITE_DENY;}return SQLITE_OK;
    },&denials),SQLITE_OK);
    EXPECT_THROW(lattice::capture_owned_server_export(owner,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);
    EXPECT_EQ(denials,1);EXPECT_EQ(scalar(owner.db(),"SELECT cut AS n FROM _lattice_ordered_source_v1"),1);
    const auto next=lattice::capture_owned_server_export(owner,{"TestPerson"});
    EXPECT_EQ(next.source_id(),first.source_id());EXPECT_EQ(next.cut(),2);EXPECT_EQ(captured_name(next),"after");
}

TEST(OrdinaryOwnedExport, DroppedGuardRefusesWithoutRecreatingOrAdvancingSource) {
    TempDB path{"owned_export_guard_loss"};lattice::lattice_db owner{config(path)};
    owner.add(TestPerson{"safe",1,std::nullopt});const auto first=lattice::capture_owned_server_export(owner,{"TestPerson"});
    const auto before=owner.db().query("SELECT * FROM _lattice_ordered_source_v1");
    owner.db().execute("DROP TRIGGER _lattice_ordered_source_UPDATE_v1");
    EXPECT_THROW(lattice::capture_owned_server_export(owner,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(owner.db().query("SELECT * FROM _lattice_ordered_source_v1"),before);
    EXPECT_EQ(scalar(owner.db(),"SELECT COUNT(*) AS n FROM sqlite_master WHERE name='_lattice_ordered_source_UPDATE_v1'"),0);
    EXPECT_EQ(first.cut(),1);
}

TEST(OrdinaryOwnedExport, ActualIndependentSQLiteHandleCannotRewriteDurableOrdering) {
    TempDB path{"owned_export_old_sqlite"};lattice::lattice_db owner{config(path)};
    owner.add(TestPerson{"safe",1,std::nullopt});const auto first=lattice::capture_owned_server_export(owner,{"TestPerson"});
    sqlite3* raw=nullptr;ASSERT_EQ(sqlite3_open_v2(path.str().c_str(),&raw,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX,nullptr),SQLITE_OK);
    const int rc=sqlite3_exec(raw,"UPDATE _lattice_ordered_source_v1 SET cut=cut+1",nullptr,nullptr,nullptr);
    EXPECT_EQ(rc,SQLITE_ERROR);EXPECT_EQ(sqlite3_get_autocommit(raw),1);ASSERT_EQ(sqlite3_close(raw),SQLITE_OK);
    EXPECT_EQ(scalar(owner.db(),"SELECT cut AS n FROM _lattice_ordered_source_v1"),first.cut());
    EXPECT_EQ(lattice::capture_owned_server_export(owner,{"TestPerson"}).cut(),2);
}

TEST(OrdinaryOwnedExport, ChangedActualModelProgramRequiresExplicitSchemaTransition) {
    TempDB path{"owned_export_schema_change"};lattice::lattice_db owner{config(path)};
    owner.add(TestPerson{"safe",1,std::nullopt});const auto first=lattice::capture_owned_server_export(owner,{"TestPerson"});
    owner.db().execute("CREATE INDEX owned_export_added_index ON TestPerson(name)");
    EXPECT_THROW(lattice::capture_owned_server_export(owner,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(scalar(owner.db(),"SELECT cut AS n FROM _lattice_ordered_source_v1"),first.cut());
    owner.db().execute("DROP INDEX owned_export_added_index");
    EXPECT_EQ(lattice::capture_owned_server_export(owner,{"TestPerson"}).cut(),2);
}

TEST(OrdinaryOwnedExport, OversizedActualPostimageCannotAdvanceThePreviousCut) {
    TempDB path{"owned_export_bound"};lattice::lattice_db owner{config(path)};
    owner.add(TestPerson{"bounded",1,std::nullopt});const auto first=lattice::capture_owned_server_export(owner,{"TestPerson"});
    owner.db().execute("UPDATE TestPerson SET name=?",{std::string(16*1024*1024+1,'x')});
    EXPECT_THROW(lattice::capture_owned_server_export(owner,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(scalar(owner.db(),"SELECT cut AS n FROM _lattice_ordered_source_v1"),first.cut());
    EXPECT_EQ(captured_name(first),"bounded");EXPECT_FALSE(owner.db().is_in_transaction());
}

namespace {
std::string row_name(lattice::lattice_db& owner){return std::get<std::string>(owner.db().query("SELECT name FROM TestPerson").at(0).at("name"));}
std::vector<std::string> issued_ids(const lattice::owned_server_export& frame){std::vector<std::string> result;for(const auto& entry:frame.entries())result.push_back(entry.global_id);return result;}
bool archive_contains(lattice::lattice_db& owner,const std::string& kind,const std::string& text) {
    for(const auto& row:owner.db().query("SELECT payload FROM _lattice_ordered_receiver_conflict_v1 WHERE kind=?",{kind})) {
        const auto& bytes=std::get<std::vector<uint8_t>>(row.at("payload"));
        if(std::search(bytes.begin(),bytes.end(),text.begin(),text.end())!=bytes.end())return true;
    }
    return false;
}
}

TEST(OrdinaryOwnedReceive, TwoOptimisticWritersMaterializeTheActualRelayCut) {
    TempDB relay_path{"ordered_relay"},a_path{"ordered_a"},b_path{"ordered_b"};
    lattice::lattice_db relay{config(relay_path)},a{config(a_path)},b{config(b_path)};
    relay.add(TestPerson{"seed",10,std::nullopt});const auto seed=lattice::events_after(relay.db(),std::nullopt);
    ASSERT_EQ(seed.size(),1u);ASSERT_EQ(lattice::apply_remote_changes(a,seed).size(),1u);ASSERT_EQ(lattice::apply_remote_changes(b,seed).size(),1u);
    a.db().execute("UPDATE TestPerson SET name='writer-A'");b.db().execute("UPDATE TestPerson SET name='writer-B'");
    const auto a_local=lattice::query_audit_log(a.db(),true,std::nullopt),b_local=lattice::query_audit_log(b.db(),true,std::nullopt);
    ASSERT_EQ(a_local.size(),1u);ASSERT_EQ(b_local.size(),1u);
    ASSERT_EQ(lattice::apply_remote_changes(relay,a_local).size(),1u);ASSERT_EQ(lattice::apply_remote_changes(b,a_local).size(),1u);
    ASSERT_EQ(lattice::apply_remote_changes(relay,b_local).size(),1u);ASSERT_EQ(lattice::apply_remote_changes(a,b_local).size(),1u);
    ASSERT_NE(row_name(a),row_name(b)); // The original provisional path still diverges.
    const auto cut=lattice::capture_owned_server_export(relay,{"TestPerson"});ASSERT_EQ(cut.entries().size(),3u);
    for(int replay=0;replay<2;++replay){EXPECT_EQ(lattice::apply_owned_server_export(a,cut),issued_ids(cut));EXPECT_EQ(lattice::apply_owned_server_export(b,cut),issued_ids(cut));}
    EXPECT_EQ(row_name(a),row_name(relay));EXPECT_EQ(row_name(b),row_name(relay));
    for(auto* peer:{&a,&b}) {
        EXPECT_EQ(scalar(peer->db(),"SELECT COUNT(*) AS n FROM AuditLog"),3);
        EXPECT_EQ(scalar(peer->db(),"SELECT cut AS n FROM _lattice_ordered_receiver_v1"),cut.cut());
        EXPECT_FALSE(peer->db().is_in_transaction());EXPECT_EQ(scalar(peer->db(),"SELECT disabled AS n FROM _SyncControl"),0);
    }
}

TEST(OrdinaryOwnedReceive, DisjointRealLocalFieldsBothSurviveServerMaterialization) {
    TempDB relay_path{"ordered_disjoint_relay"},a_path{"ordered_disjoint_a"},b_path{"ordered_disjoint_b"};
    lattice::lattice_db relay{config(relay_path)},a{config(a_path)},b{config(b_path)};
    relay.add(TestPerson{"seed",10,std::nullopt});const auto seed=lattice::events_after(relay.db(),std::nullopt);
    ASSERT_EQ(lattice::apply_remote_changes(a,seed).size(),1u);ASSERT_EQ(lattice::apply_remote_changes(b,seed).size(),1u);
    a.db().execute("UPDATE TestPerson SET name='A-name'");b.db().execute("UPDATE TestPerson SET age=29");
    const auto first=lattice::query_audit_log(a.db(),true,std::nullopt),second=lattice::query_audit_log(b.db(),true,std::nullopt);
    ASSERT_EQ(lattice::apply_remote_changes(relay,first).size(),1u);ASSERT_EQ(lattice::apply_remote_changes(relay,second).size(),1u);
    const auto cut=lattice::capture_owned_server_export(relay,{"TestPerson"});
    for(auto* peer:{&a,&b}){ASSERT_EQ(lattice::apply_owned_server_export(*peer,cut),issued_ids(cut));EXPECT_EQ(row_name(*peer),"A-name");EXPECT_EQ(scalar(peer->db(),"SELECT age AS n FROM TestPerson"),29);}
}

TEST(OrdinaryOwnedReceive, UnknownLocalEditHasServerActiveAndDurableConflictWithoutFalseSync) {
    TempDB server_path{"ordered_conflict_server"},client_path{"ordered_conflict_client"};
    lattice::lattice_db server{config(server_path)},client{config(client_path)};server.add(TestPerson{"server-first",1,std::nullopt});
    const auto first=lattice::capture_owned_server_export(server,{"TestPerson"});ASSERT_EQ(lattice::apply_owned_server_export(client,first),issued_ids(first));
    client.db().execute("UPDATE TestPerson SET name='local-unconfirmed'");
    const auto before=client.db().query("SELECT * FROM AuditLog ORDER BY id");
    server.db().execute("UPDATE TestPerson SET name='server-active',age=9");const auto second=lattice::capture_owned_server_export(server,{"TestPerson"});
    ASSERT_EQ(lattice::apply_owned_server_export(client,second),issued_ids(second));
    EXPECT_EQ(row_name(client),"server-active");EXPECT_EQ(scalar(client.db(),"SELECT age AS n FROM TestPerson"),9);
    EXPECT_EQ(client.db().query("SELECT * FROM AuditLog ORDER BY id"),before);
    EXPECT_TRUE(archive_contains(client,"audit","local-unconfirmed"));EXPECT_TRUE(archive_contains(client,"row","local-unconfirmed"));
    EXPECT_TRUE(archive_contains(client,"row","server-first"));EXPECT_TRUE(archive_contains(client,"row","server-active"));
    EXPECT_THROW(lattice::query_audit_log(client.db(),true,std::nullopt),lattice::db_error);
    EXPECT_THROW(lattice::capture_owned_server_export(client,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
}

TEST(OrdinaryOwnedReceive, DelayedRetainedReplayCannotOverwriteNewLocalStateAcrossReopen) {
    TempDB server_path{"ordered_delay_server"},client_path{"ordered_delay_client"};
    lattice::lattice_db server{config(server_path)};server.add(TestPerson{"cut-one",1,std::nullopt});const auto first=lattice::capture_owned_server_export(server,{"TestPerson"});
    server.db().execute("UPDATE TestPerson SET name='cut-two'");const auto second=lattice::capture_owned_server_export(server,{"TestPerson"});
    {
        lattice::lattice_db client{config(client_path)};ASSERT_EQ(lattice::apply_owned_server_export(client,first),issued_ids(first));ASSERT_EQ(lattice::apply_owned_server_export(client,second),issued_ids(second));
        client.db().execute("UPDATE TestPerson SET name='new-local'");
    }
    lattice::lattice_db reopened{config(client_path)};
    const auto changes=sqlite3_total_changes64(reopened.db().handle());
    EXPECT_EQ(lattice::apply_owned_server_export(reopened,first),issued_ids(first));
    EXPECT_EQ(lattice::apply_owned_server_export(reopened,second),issued_ids(second));
    EXPECT_EQ(sqlite3_total_changes64(reopened.db().handle()),changes);EXPECT_EQ(row_name(reopened),"new-local");
    EXPECT_EQ(scalar(reopened.db(),"SELECT cut AS n FROM _lattice_ordered_receiver_v1"),second.cut());
}

TEST(OrdinaryOwnedReceive, ARealLocalDeletionSurvivesAuditCompactionAsAnUnresolvedAbsence) {
    TempDB server_path{"ordered_delete_server"},client_path{"ordered_delete_client"};
    lattice::lattice_db server{config(server_path)},client{config(client_path)};server.add(TestPerson{"server-kept",1,std::nullopt});
    const auto first=lattice::capture_owned_server_export(server,{"TestPerson"});ASSERT_EQ(lattice::apply_owned_server_export(client,first),issued_ids(first));
    client.db().execute("DELETE FROM TestPerson");client.db().execute("DELETE FROM AuditLog");
    const auto second=lattice::capture_owned_server_export(server,{"TestPerson"});ASSERT_EQ(lattice::apply_owned_server_export(client,second),issued_ids(second));
    EXPECT_EQ(row_name(client),"server-kept");EXPECT_EQ(scalar(client.db(),"SELECT COUNT(*) AS n FROM AuditLog"),0);
    EXPECT_EQ(scalar(client.db(),"SELECT COUNT(*) AS n FROM _lattice_ordered_receiver_conflict_v1 WHERE kind='absence'"),1);
    EXPECT_TRUE(archive_contains(client,"absence","server-kept"));
}

TEST(OrdinaryOwnedReceive, ActualCommitDenialRollsBackRowsFrontierAndNewConflictsTogether) {
    TempDB server_path{"ordered_deny_server"},client_path{"ordered_deny_client"};
    lattice::lattice_db server{config(server_path)},client{config(client_path)};server.add(TestPerson{"first",1,std::nullopt});
    const auto first=lattice::capture_owned_server_export(server,{"TestPerson"});ASSERT_EQ(lattice::apply_owned_server_export(client,first),issued_ids(first));
    client.db().execute("UPDATE TestPerson SET name='pending-local'");server.db().execute("UPDATE TestPerson SET name='next-server'");const auto second=lattice::capture_owned_server_export(server,{"TestPerson"});
    const auto before=client.db().query("SELECT * FROM _lattice_ordered_receiver_v1");auto* handle=client.db().handle();authorizer_reset reset{handle};int denials=0;
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* raw,int action,const char* operation,const char*,const char*,const char*)noexcept{
        if(action==SQLITE_TRANSACTION&&operation&&std::strcmp(operation,"COMMIT")==0){++*static_cast<int*>(raw);return SQLITE_DENY;}return SQLITE_OK;
    },&denials),SQLITE_OK);
    EXPECT_THROW(lattice::apply_owned_server_export(client,second),lattice::detail::ordinary_order_write_error);
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);
    EXPECT_EQ(denials,1);EXPECT_EQ(row_name(client),"pending-local");EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_receiver_v1"),before);
    EXPECT_EQ(scalar(client.db(),"SELECT COUNT(*) AS n FROM _lattice_ordered_receiver_conflict_v1"),0);
    EXPECT_EQ(scalar(client.db(),"SELECT COUNT(*) AS n FROM _lattice_ordered_receiver_cut_v1"),1);
    EXPECT_EQ(scalar(client.db(),"SELECT disabled AS n FROM _SyncControl"),0);EXPECT_FALSE(client.db().is_in_transaction());
    EXPECT_EQ(lattice::apply_owned_server_export(client,second),issued_ids(second));EXPECT_EQ(row_name(client),"next-server");EXPECT_TRUE(archive_contains(client,"audit","pending-local"));
}

TEST(OrdinaryOwnedReceive, DifferentIssuedSourceRequiresExplicitRebind) {
    TempDB one_path{"ordered_source_one"},two_path{"ordered_source_two"},client_path{"ordered_source_client"};
    lattice::lattice_db one{config(one_path)},two{config(two_path)},client{config(client_path)};
    one.add(TestPerson{"one",1,std::nullopt});two.add(TestPerson{"two",2,std::nullopt});
    const auto first=lattice::capture_owned_server_export(one,{"TestPerson"}),other=lattice::capture_owned_server_export(two,{"TestPerson"});
    ASSERT_NE(first.source_id(),other.source_id());ASSERT_EQ(lattice::apply_owned_server_export(client,first),issued_ids(first));
    const auto before=client.db().query("SELECT * FROM _lattice_ordered_receiver_v1");
    EXPECT_THROW(lattice::apply_owned_server_export(client,other),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(row_name(client),"one");EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_receiver_v1"),before);
}

TEST(OrdinaryOwnedReceive, UnobservedEarlierCutAndLegacyVectorsCannotMintAcknowledgements) {
    TempDB server_path{"ordered_unseen_server"},client_path{"ordered_unseen_client"};
    lattice::lattice_db server{config(server_path)},client{config(client_path)};server.add(TestPerson{"first",1,std::nullopt});
    const auto first=lattice::capture_owned_server_export(server,{"TestPerson"});server.db().execute("UPDATE TestPerson SET name='second'");const auto second=lattice::capture_owned_server_export(server,{"TestPerson"});
    ASSERT_EQ(lattice::apply_owned_server_export(client,second),issued_ids(second));
    EXPECT_THROW(lattice::apply_owned_server_export(client,first),lattice::detail::ordinary_order_write_error);
    server.db().execute("UPDATE TestPerson SET name='unnegotiated'");const auto later=lattice::events_after(server.db(),std::nullopt);
    EXPECT_TRUE(lattice::apply_remote_changes(client,later).empty());EXPECT_EQ(row_name(client),"second");
    EXPECT_EQ(scalar(client.db(),"SELECT cut AS n FROM _lattice_ordered_receiver_v1"),second.cut());
}

TEST(OrdinaryOwnedReceive, HistoricUnenrolledReceiverIsNotPromotedFromAValidFrame) {
    TempDB server_path{"ordered_historical_server"},client_path{"ordered_historical_client"};
    lattice::lattice_db server{config(server_path)};server.add(TestPerson{"server",1,std::nullopt});const auto frame=lattice::capture_owned_server_export(server,{"TestPerson"});
    {lattice::lattice_db original{config(client_path)};original.add(TestPerson{"historical",1,std::nullopt});}
    lattice::lattice_db reopened{config(client_path)};
    EXPECT_THROW(lattice::apply_owned_server_export(reopened,frame),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(row_name(reopened),"historical");EXPECT_FALSE(reopened.db().table_exists("_lattice_ordered_receiver_v1"));
}

TEST(OrdinaryOwnedReceive, DamagedBaselineCannotBeSilentlyRepairedByTheNextCut) {
    TempDB server_path{"ordered_baseline_server"},client_path{"ordered_baseline_client"};
    lattice::lattice_db server{config(server_path)},client{config(client_path)};server.add(TestPerson{"first",1,std::nullopt});
    const auto first=lattice::capture_owned_server_export(server,{"TestPerson"});ASSERT_EQ(lattice::apply_owned_server_export(client,first),issued_ids(first));
    // Controlled durable corruption, preserving exact restored guard text. The
    // candidate must inspect the actual baseline, not trust its prior frontier.
    const auto guard=std::get<std::string>(client.db().query("SELECT sql FROM sqlite_master WHERE name='_lattice_ordered_receiver_baseline_v1_UPDATE'").at(0).at("sql"));
    client.db().execute("DROP TRIGGER _lattice_ordered_receiver_baseline_v1_UPDATE");client.db().execute("UPDATE _lattice_ordered_receiver_baseline_v1 SET value=x'00'");client.db().execute(guard);
    server.db().execute("UPDATE TestPerson SET name='second'");const auto second=lattice::capture_owned_server_export(server,{"TestPerson"});
    EXPECT_THROW(lattice::apply_owned_server_export(client,second),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(row_name(client),"first");EXPECT_EQ(scalar(client.db(),"SELECT cut AS n FROM _lattice_ordered_receiver_v1"),first.cut());
    EXPECT_EQ(scalar(client.db(),"SELECT length(value) AS n FROM _lattice_ordered_receiver_baseline_v1"),1);
}

TEST(OrdinaryOwnedExport, TempShadowAndUnownedModelTriggerCannotIssueAStoredCut) {
    TempDB path{"owned_export_shadow"};lattice::lattice_db owner{config(path)};owner.add(TestPerson{"main-value",1,std::nullopt});
    const auto first=lattice::capture_owned_server_export(owner,{"TestPerson"});
    owner.db().execute("CREATE TEMP TABLE TestPerson AS SELECT * FROM main.TestPerson");owner.db().execute("UPDATE temp.TestPerson SET name='shadow-value'");
    EXPECT_THROW(lattice::capture_owned_server_export(owner,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(scalar(owner.db(),"SELECT cut AS n FROM main._lattice_ordered_source_v1"),first.cut());owner.db().execute("DROP TABLE temp.TestPerson");
    owner.db().execute("CREATE TRIGGER unowned_model_program AFTER UPDATE ON TestPerson BEGIN SELECT 1; END");
    EXPECT_THROW(lattice::capture_owned_server_export(owner,{"TestPerson"}),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(scalar(owner.db(),"SELECT cut AS n FROM main._lattice_ordered_source_v1"),first.cut());owner.db().execute("DROP TRIGGER unowned_model_program");
    const auto next=lattice::capture_owned_server_export(owner,{"TestPerson"});EXPECT_EQ(next.cut(),2);EXPECT_EQ(captured_name(next),"main-value");
}

TEST(OrdinaryOwnedReceive, ReceiptExpiryRefusesOldCutWhileAnotherScopesCurrentReceiptSurvives) {
    TempDB source_path{"owned_receipt_window_source"},receiver_path{"owned_receipt_window_receiver"};
    lattice::lattice_db source{config(source_path)},receiver{config(receiver_path)};
    source.add(TestPerson{"person",1,std::nullopt});source.add(TestDog{"dog",4.5,true});
    const auto dog=lattice::capture_owned_server_export(source,{"TestDog"}),first=lattice::capture_owned_server_export(source,{"TestPerson"});
    ASSERT_EQ(lattice::apply_owned_server_export(receiver,dog),issued_ids(dog));ASSERT_EQ(lattice::apply_owned_server_export(receiver,first),issued_ids(first));
    for(int n=0;n<50;++n){const auto next=lattice::capture_owned_server_export(source,{"TestPerson"});ASSERT_EQ(lattice::apply_owned_server_export(receiver,next),issued_ids(next));}
    EXPECT_LE(scalar(receiver.db(),"SELECT COUNT(*) AS n FROM _lattice_ordered_receiver_cut_v1"),64);
    EXPECT_THROW(lattice::apply_owned_server_export(receiver,first),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(lattice::apply_owned_server_export(receiver,dog),issued_ids(dog));EXPECT_EQ(row_name(receiver),"person");
    EXPECT_EQ(scalar(receiver.db(),"SELECT COUNT(*) AS n FROM TestDog WHERE name='dog'"),1);
    EXPECT_EQ(scalar(receiver.db(),"SELECT COUNT(*) AS n FROM _lattice_ordered_receiver_conflict_v1"),0);
}
