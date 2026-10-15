#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/ordinary_export.hpp"

namespace {
lattice::configuration helper_config(const TempDB& path) {
    lattice::configuration value(path.str());value.audit_retention_seconds=0;value.busy_timeout_ms=100;return value;
}
int64_t helper_scalar(lattice::lattice_db& owner,const std::string& sql){return std::get<int64_t>(owner.db().query(sql).at(0).at("n"));}
std::string helper_name(lattice::lattice_db& owner){return std::get<std::string>(owner.db().query("SELECT name FROM TestPerson").at(0).at("name"));}
std::vector<std::string> helper_ids(const lattice::server_export_batch& batch){std::vector<std::string> value;for(const auto& entry:batch)value.push_back(entry.global_id);return value;}
bool helper_conflict_contains(lattice::lattice_db& owner,const std::string& value) {
    for(const auto& row:owner.db().query("SELECT payload FROM _lattice_ordered_receiver_conflict_v1 WHERE kind='provisional'")) {
        const auto& bytes=std::get<std::vector<uint8_t>>(row.at("payload"));if(std::search(bytes.begin(),bytes.end(),value.begin(),value.end())!=bytes.end())return true;
    }
    return false;
}
struct helper_authorizer_reset {sqlite3* db;~helper_authorizer_reset(){sqlite3_set_authorizer(db,nullptr,nullptr);}};
}

TEST(OrdinaryOwnedHelper, LiteralAutoBatchOwnsTheCompleteCheckpointCutAfterSourceClose) {
    TempDB source_path{"helper_complete_source"},client_path{"helper_complete_client"};
    lattice::lattice_db client{helper_config(client_path)};
    const auto batch=[&] {
        lattice::lattice_db source{helper_config(source_path)};source.add(TestPerson{"before",2,std::nullopt});
        const auto initial=lattice::events_after(source.db(),std::nullopt);EXPECT_TRUE(initial.ordered());
        source.db().execute("UPDATE TestPerson SET name='complete-after',age=7");
        return lattice::events_after(source.db(),initial.front().global_id);
    }();
    ASSERT_TRUE(batch.ordered());ASSERT_EQ(batch.size(),1u);EXPECT_EQ(batch.front().operation,"UPDATE");
    EXPECT_EQ(lattice::apply_remote_changes(client,batch),helper_ids(batch));
    EXPECT_EQ(helper_name(client),"complete-after");EXPECT_EQ(helper_scalar(client,"SELECT age AS n FROM TestPerson"),7);
    EXPECT_EQ(helper_scalar(client,"SELECT COUNT(*) AS n FROM AuditLog"),2);
    EXPECT_EQ(helper_scalar(client,"SELECT cut AS n FROM _lattice_ordered_receiver_v1"),2);
}

TEST(OrdinaryOwnedHelper, ExplicitVectorConversionRetainsAppliedLegacySemanticsWithoutSourceAuthority) {
    TempDB source_path{"helper_slice_source"},client_path{"helper_slice_client"};
    lattice::lattice_db source{helper_config(source_path)},client{helper_config(client_path)};
    source.add(TestPerson{"legacy-copy",4,std::nullopt});const auto batch=lattice::events_after(source.db(),std::nullopt);ASSERT_TRUE(batch.ordered());
    const std::vector<lattice::audit_log_entry> copy=batch;
    EXPECT_EQ(lattice::apply_remote_changes(client,copy),helper_ids(batch));EXPECT_EQ(helper_name(client),"legacy-copy");
    EXPECT_FALSE(client.db().table_exists("_lattice_ordered_receiver_v1"));EXPECT_FALSE(client.db().table_exists("_lattice_ordered_helper_v1"));
}

TEST(OrdinaryOwnedHelper, SiblingACKAppliesTheValueButDoesNotCertifyAServerCut) {
    TempDB source_path{"helper_ack_source"},a_path{"helper_ack_a"},b_path{"helper_ack_b"};
    lattice::lattice_db source{helper_config(source_path)},a{helper_config(a_path)},b{helper_config(b_path)};
    source.add(TestPerson{"seed",1,std::nullopt});const auto initial=lattice::events_after(source.db(),std::nullopt);
    ASSERT_EQ(lattice::apply_remote_changes(a,initial),helper_ids(initial));ASSERT_EQ(lattice::apply_remote_changes(b,initial),helper_ids(initial));
    a.db().execute("UPDATE TestPerson SET name='actual-A'");b.db().execute("UPDATE TestPerson SET name='actual-B'");
    const auto first=lattice::query_audit_log(a.db(),true),second=lattice::query_audit_log(b.db(),true);ASSERT_EQ(first.size(),1u);ASSERT_EQ(second.size(),1u);
    const auto frontier=a.db().query("SELECT * FROM _lattice_ordered_receiver_v1");
    EXPECT_EQ(lattice::apply_remote_changes(a,second),(std::vector<std::string>{second.front().global_id}));
    EXPECT_EQ(helper_name(a),"actual-B");EXPECT_EQ(a.db().query("SELECT * FROM _lattice_ordered_receiver_v1"),frontier);
    EXPECT_EQ(helper_scalar(a,"SELECT COUNT(*) AS n FROM _lattice_ordered_helper_entry_v1 WHERE canonical=0"),2);
    lattice::mark_audit_entries_synced(a,{first.front().global_id});
    EXPECT_EQ(helper_scalar(a,"SELECT COUNT(*) AS n FROM AuditLog WHERE isFromRemote=0 AND isSynchronized=1"),1);
    EXPECT_EQ(helper_scalar(a,"SELECT COUNT(*) AS n FROM _lattice_ordered_helper_entry_v1 WHERE canonical=0"),2);
}

TEST(OrdinaryOwnedHelper, ACKAndRealHistoryPruneCannotEraseAnOverwrittenUnconfirmedLocalEdit) {
    TempDB source_path{"helper_prune_source"},a_path{"helper_prune_a"},b_path{"helper_prune_b"};
    lattice::lattice_db source{helper_config(source_path)},a{helper_config(a_path)},b{helper_config(b_path)};
    source.add(TestPerson{"server-seed",1,std::nullopt});const auto initial=lattice::events_after(source.db(),std::nullopt);
    ASSERT_EQ(lattice::apply_remote_changes(a,initial),helper_ids(initial));ASSERT_EQ(lattice::apply_remote_changes(b,initial),helper_ids(initial));
    a.db().execute("UPDATE TestPerson SET name='preserve-before-overwrite'");b.db().execute("UPDATE TestPerson SET name='sibling-overwrite'");
    const auto local=lattice::query_audit_log(a.db(),true),sibling=lattice::query_audit_log(b.db(),true);ASSERT_EQ(local.size(),1u);ASSERT_EQ(sibling.size(),1u);
    ASSERT_EQ(lattice::apply_remote_changes(a,sibling).size(),1u);lattice::mark_audit_entries_synced(a,{local.front().global_id});
    const auto last=helper_scalar(a,"SELECT MAX(id) AS n FROM AuditLog");EXPECT_GT(a.delete_audit_below_(last,false),0);
    EXPECT_TRUE(a.db().query("SELECT id FROM AuditLog WHERE globalId=?",{local.front().global_id}).empty());
    source.db().execute("UPDATE TestPerson SET name='actual-server-choice'");const auto final=lattice::events_after(source.db(),std::nullopt);
    EXPECT_EQ(lattice::apply_remote_changes(a,final),helper_ids(final));EXPECT_EQ(helper_name(a),"actual-server-choice");
    EXPECT_TRUE(helper_conflict_contains(a,"preserve-before-overwrite"));EXPECT_TRUE(helper_conflict_contains(a,"sibling-overwrite"));
    EXPECT_EQ(helper_scalar(a,"SELECT COUNT(*) AS n FROM _lattice_ordered_helper_entry_v1 WHERE canonical=0"),2);
}

TEST(OrdinaryOwnedHelper, AppliedSiblingReplayAfterNewCutPruneAndReopenNeverRegressesTheRow) {
    TempDB source_path{"helper_delay_source"},client_path{"helper_delay_client"};
    lattice::lattice_db source{helper_config(source_path)};source.add(TestPerson{"seed",1,std::nullopt});
    const auto initial=lattice::events_after(source.db(),std::nullopt);source.db().execute("UPDATE TestPerson SET name='old-applied'");
    const auto history=lattice::query_audit_log(source.db(),false);ASSERT_EQ(history.size(),2u);const auto old=history.back();
    {
        lattice::lattice_db client{helper_config(client_path)};ASSERT_EQ(lattice::apply_remote_changes(client,initial),helper_ids(initial));
        ASSERT_EQ(lattice::apply_remote_changes(client,std::vector<lattice::audit_log_entry>{old}),(std::vector<std::string>{old.global_id}));EXPECT_EQ(helper_name(client),"old-applied");
        source.db().execute("UPDATE TestPerson SET name='new-authoritative'");const auto final=lattice::events_after(source.db(),std::nullopt);
        ASSERT_EQ(lattice::apply_remote_changes(client,final),helper_ids(final));
        EXPECT_GT(client.delete_audit_below_(helper_scalar(client,"SELECT MAX(id) AS n FROM AuditLog"),false),0);
        client.db().execute("DELETE FROM _lattice_applied_receipts");
    }
    lattice::lattice_db reopened{helper_config(client_path)};
    const auto frontier=reopened.db().query("SELECT * FROM _lattice_ordered_receiver_v1");
    EXPECT_EQ(lattice::apply_remote_changes(reopened,std::vector<lattice::audit_log_entry>{old}),(std::vector<std::string>{old.global_id}));
    EXPECT_EQ(helper_name(reopened),"new-authoritative");EXPECT_EQ(reopened.db().query("SELECT * FROM _lattice_ordered_receiver_v1"),frontier);
    auto changed=old;changed.changed_fields["name"]=lattice::any_property(std::string("same-id-different-effect"));
    EXPECT_TRUE(lattice::apply_remote_changes(reopened,std::vector<lattice::audit_log_entry>{changed}).empty());
    EXPECT_EQ(helper_name(reopened),"new-authoritative");
}

TEST(OrdinaryOwnedHelper, SourceAndChannelChangesRefuseWithoutChangingOriginalBinding) {
    TempDB one_path{"helper_source_one"},two_path{"helper_source_two"},client_path{"helper_source_bound"};
    lattice::lattice_db one{helper_config(one_path)},two{helper_config(two_path)},client{helper_config(client_path)};
    one.add(TestPerson{"one",1,std::nullopt});two.add(TestPerson{"two",2,std::nullopt});
    const auto first=lattice::events_after(one.db(),std::nullopt),other=lattice::events_after(two.db(),std::nullopt);
    lattice::ensure_cursor_column(client.db());lattice::register_replication_slot(client.db(),"helper-channel");
    ASSERT_EQ(lattice::apply_remote_changes_for(client,first,"helper-channel"),helper_ids(first));
    const auto binding=client.db().query("SELECT * FROM _lattice_ordered_helper_v1");
    EXPECT_THROW(lattice::apply_remote_changes_for(client,first,"other-channel"),lattice::detail::ordinary_order_write_error);
    EXPECT_THROW(lattice::apply_remote_changes_for(client,other,"helper-channel"),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_helper_v1"),binding);EXPECT_EQ(helper_name(client),"one");
}

TEST(OrdinaryOwnedHelper, RealCommitDenialRollsBackFirstHelperBindingAuditAndPostimageTogether) {
    TempDB source_path{"helper_deny_source"},client_path{"helper_deny_client"};
    lattice::lattice_db source{helper_config(source_path)},client{helper_config(client_path)};source.add(TestPerson{"committed-source",1,std::nullopt});
    const auto batch=lattice::events_after(source.db(),std::nullopt);auto* handle=client.db().handle();helper_authorizer_reset reset{handle};int denied=0;
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* context,int action,const char* operation,const char*,const char*,const char*)noexcept {
        if(action==SQLITE_TRANSACTION&&operation&&std::strcmp(operation,"COMMIT")==0){++*static_cast<int*>(context);return SQLITE_DENY;}return SQLITE_OK;
    },&denied),SQLITE_OK);
    EXPECT_THROW(lattice::apply_remote_changes(client,batch),lattice::detail::ordinary_order_write_error);
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);EXPECT_EQ(denied,1);
    EXPECT_EQ(helper_scalar(client,"SELECT COUNT(*) AS n FROM TestPerson"),0);EXPECT_EQ(helper_scalar(client,"SELECT COUNT(*) AS n FROM AuditLog"),0);
    EXPECT_FALSE(client.db().table_exists("_lattice_ordered_helper_v1"));EXPECT_FALSE(client.db().is_in_transaction());
    EXPECT_EQ(lattice::apply_remote_changes(client,batch),helper_ids(batch));EXPECT_EQ(helper_name(client),"committed-source");
}

TEST(OrdinaryOwnedHelper, MutableCallerVectorCannotChangeTheAdmittedProvisionalPayload) {
    TempDB source_path{"helper_copy_source"},client_path{"helper_copy_client"};
    lattice::lattice_db source{helper_config(source_path)},client{helper_config(client_path)};source.add(TestPerson{"seed",1,std::nullopt});
    const auto initial=lattice::events_after(source.db(),std::nullopt);ASSERT_EQ(lattice::apply_remote_changes(client,initial),helper_ids(initial));
    source.db().execute("UPDATE TestPerson SET name='copied-before-hook'");auto input=lattice::query_audit_log(source.db(),false);input.erase(input.begin());ASSERT_EQ(input.size(),1u);
    const auto prior=lattice::test_hooks::apply_chunk_begin;
    struct restore {std::function<void(size_t)> previous;~restore(){lattice::test_hooks::apply_chunk_begin=std::move(previous);}} guard{prior};
    lattice::test_hooks::apply_chunk_begin=[&](size_t index){if(prior)prior(index);input.front().changed_fields["name"]=lattice::any_property(std::string("mutated-caller"));};
    EXPECT_EQ(lattice::apply_remote_changes(client,input),(std::vector<std::string>{input.front().global_id}));
    EXPECT_EQ(helper_name(client),"copied-before-hook");EXPECT_EQ(std::get<std::string>(input.front().changed_fields.at("name").value),"mutated-caller");
}

TEST(OrdinaryOwnedHelper, OversizedProvisionalCopyRefusesBeforeAnyModelOrLedgerChange) {
    TempDB source_path{"helper_bound_source"},client_path{"helper_bound_client"};
    lattice::lattice_db source{helper_config(source_path)},client{helper_config(client_path)};source.add(TestPerson{"seed",1,std::nullopt});
    const auto initial=lattice::events_after(source.db(),std::nullopt);ASSERT_EQ(lattice::apply_remote_changes(client,initial),helper_ids(initial));
    source.db().execute("UPDATE TestPerson SET name='small'");const auto history=lattice::query_audit_log(source.db(),false);auto entry=history.back();entry.changed_fields["name"]=lattice::any_property(std::string(16*1024*1024+1,'x'));
    const auto before=client.db().query("SELECT * FROM _lattice_ordered_helper_entry_v1");
    EXPECT_THROW(lattice::apply_remote_changes(client,std::vector<lattice::audit_log_entry>{entry}),lattice::db_error);
    EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_helper_entry_v1"),before);EXPECT_EQ(helper_name(client),"seed");
}

TEST(OrdinaryOwnedHelper, ActualModelSQLDenialCannotBecomeAnAppliedWitnessOrACK) {
    TempDB source_path{"helper_model_deny_source"},client_path{"helper_model_deny_client"};
    lattice::lattice_db source{helper_config(source_path)},client{helper_config(client_path)};source.add(TestPerson{"seed",1,std::nullopt});
    const auto initial=lattice::events_after(source.db(),std::nullopt);ASSERT_EQ(lattice::apply_remote_changes(client,initial),helper_ids(initial));
    source.db().execute("UPDATE TestPerson SET name='must-not-ack'");const auto history=lattice::query_audit_log(source.db(),false);const auto update=history.back();
    const auto before=client.db().query("SELECT * FROM _lattice_ordered_helper_entry_v1");auto* handle=client.db().handle();helper_authorizer_reset reset{handle};int denied=0;
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* context,int action,const char* table,const char*,const char*,const char*)noexcept {
        if(action==SQLITE_UPDATE&&table&&std::strcmp(table,"TestPerson")==0){++*static_cast<int*>(context);return SQLITE_DENY;}return SQLITE_OK;
    },&denied),SQLITE_OK);
    EXPECT_TRUE(lattice::apply_remote_changes(client,std::vector<lattice::audit_log_entry>{update}).empty());
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);EXPECT_GT(denied,0);
    EXPECT_EQ(helper_name(client),"seed");EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_helper_entry_v1"),before);
    EXPECT_TRUE(client.db().query("SELECT id FROM AuditLog WHERE globalId=?",{update.global_id}).empty());
    EXPECT_EQ(lattice::apply_remote_changes(client,std::vector<lattice::audit_log_entry>{update}),(std::vector<std::string>{update.global_id}));
    EXPECT_EQ(helper_name(client),"must-not-ack");
}

TEST(OrdinaryOwnedHelper, CoherentlyRestoredGuardCannotBlessDamagedPendingPayloadCustody) {
    TempDB source_path{"helper_damage_source"},client_path{"helper_damage_client"};
    lattice::lattice_db source{helper_config(source_path)},client{helper_config(client_path)};source.add(TestPerson{"seed",1,std::nullopt});
    const auto initial=lattice::events_after(source.db(),std::nullopt);ASSERT_EQ(lattice::apply_remote_changes(client,initial),helper_ids(initial));
    client.db().execute("UPDATE TestPerson SET name='unconfirmed-local'");const auto local=lattice::query_audit_log(client.db(),true);ASSERT_EQ(local.size(),1u);
    lattice::mark_audit_entries_synced(client,{local.front().global_id});
    const auto trigger=std::get<std::string>(client.db().query("SELECT sql FROM sqlite_master WHERE name='_lattice_ordered_helper_entry_v1_UPDATE'").at(0).at("sql"));
    client.db().execute("DROP TRIGGER _lattice_ordered_helper_entry_v1_UPDATE");
    client.db().execute("UPDATE _lattice_ordered_helper_entry_v1 SET payload=X'00' WHERE canonical=0");client.db().execute(trigger);
    source.db().execute("UPDATE TestPerson SET name='new-source'");const auto next=lattice::events_after(source.db(),std::nullopt);
    const auto before=client.db().query("SELECT * FROM _lattice_ordered_receiver_v1");
    EXPECT_THROW(lattice::apply_remote_changes(client,next),lattice::detail::ordinary_order_write_error);
    EXPECT_EQ(helper_name(client),"unconfirmed-local");EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_receiver_v1"),before);
    EXPECT_FALSE(client.db().is_in_transaction());
}


TEST(OrdinaryOwnedHelper, PublicQueryBoundaryRetainsValidatedPendingReadsAndRefusesCanonicalExport) {
    TempDB source_path{"public_query_owned_source"},client_path{"public_query_owned_client"};
    lattice::lattice_db source{helper_config(source_path)},client{helper_config(client_path)};
    source.add(TestPerson{"actual-source",1,std::nullopt});
    const auto initial=lattice::events_after(source.db(),std::nullopt);ASSERT_TRUE(initial.ordered());
    ASSERT_EQ(lattice::apply_remote_changes(client,initial),helper_ids(initial));
    client.db().execute("UPDATE TestPerson SET name='actual-pending'");
    const auto frontier=client.db().query("SELECT * FROM _lattice_ordered_receiver_v1");
    const auto custody=client.db().query("SELECT * FROM _lattice_ordered_helper_v1");
    const auto audit=client.db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto pending=lattice::query_audit_log(client.db(),true);ASSERT_EQ(pending.size(),1u);
    EXPECT_EQ(std::get<std::string>(pending.front().changed_fields.at("name").value),"actual-pending");
    EXPECT_FALSE(pending.front().upload_original);EXPECT_FALSE(pending.front().ordinary_delivery_version);
    const auto channel=lattice::query_audit_log_for_sync(client.db(),"public-query-boundary",std::nullopt,0,64);
    EXPECT_NE(std::find_if(channel.begin(),channel.end(),[&](const auto& e){return e.global_id==pending.front().global_id;}),channel.end());
    EXPECT_THROW((void)lattice::query_audit_log(client.db(),false),lattice::db_error);
    EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_receiver_v1"),frontier);
    EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_helper_v1"),custody);
    EXPECT_EQ(client.db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_FALSE(client.db().is_in_transaction());
}

TEST(OrdinaryOwnedHelper, PublicQueryNativeReadFailurePreservesTheActualCallerReadTransaction) {
    TempDB path{"public_query_actual_read_failure"};lattice::lattice_db owner{helper_config(path)};
    owner.add(TestPerson{"committed-query-row",1,std::nullopt});
    auto* handle=owner.db().handle();helper_authorizer_reset reset{handle};int denied=0;
    owner.db().execute("BEGIN");const auto before=owner.db().query("SELECT * FROM AuditLog ORDER BY id");
    ASSERT_EQ(sqlite3_txn_state(handle,"main"),SQLITE_TXN_READ);
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* state,int action,const char* table,const char*,const char*,const char*)noexcept {
        if(action==SQLITE_READ&&table&&std::strcmp(table,"AuditLog")==0){++*static_cast<int*>(state);return SQLITE_DENY;}return SQLITE_OK;
    },&denied),SQLITE_OK);
    EXPECT_THROW((void)lattice::query_audit_log(owner.db(),true),lattice::db_error);
    EXPECT_THROW((void)lattice::query_audit_log_for_sync(owner.db(),"public-query-read-failure",std::nullopt,0,64),lattice::db_error);
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);EXPECT_GE(denied,2);
    EXPECT_EQ(sqlite3_get_autocommit(handle),0);EXPECT_EQ(sqlite3_txn_state(handle,"main"),SQLITE_TXN_READ);
    EXPECT_EQ(owner.db().query("SELECT * FROM AuditLog ORDER BY id"),before);
    owner.db().execute("ROLLBACK");EXPECT_FALSE(owner.db().is_in_transaction());
}
