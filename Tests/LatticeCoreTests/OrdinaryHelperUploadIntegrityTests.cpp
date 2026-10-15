#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_admission_test_probe.hpp"
#include <string_view>
#include <future>

namespace {
lattice::configuration upload_config(const TempDB& path) {
    lattice::configuration value(path.str());value.audit_retention_seconds=0;value.busy_timeout_ms=100;return value;
}
void enroll_upload(lattice::lattice_db& source,lattice::lattice_db& client) {
    source.add(TestPerson{"seed",1,std::nullopt});
    const auto batch=lattice::events_after(source.db(),std::nullopt);
    ASSERT_TRUE(batch.ordered());ASSERT_EQ(lattice::apply_remote_changes(client,batch).size(),batch.size());
    lattice::ensure_cursor_column(client.db());lattice::register_replication_slot(client.db(),"upload-integrity");
    client.db().execute("UPDATE TestPerson SET name='pending-upload'");
}
std::vector<lattice::audit_log_entry> read_upload(lattice::database& db,bool per_sync) {
    return per_sync?lattice::query_audit_log_for_sync(db,"upload-integrity",std::nullopt,0,256):lattice::query_audit_log(db,true);
}
void both_refuse_unchanged(lattice::lattice_db& client) {
    const auto audit=client.db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto model=client.db().query("SELECT * FROM TestPerson ORDER BY id");
    const auto helper=client.db().query("SELECT * FROM _lattice_ordered_helper_v1 ORDER BY scope");
    for(bool per_sync:{false,true})EXPECT_THROW((void)read_upload(client.db(),per_sync),lattice::db_error);
    EXPECT_EQ(client.db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    EXPECT_EQ(client.db().query("SELECT * FROM TestPerson ORDER BY id"),model);
    EXPECT_EQ(client.db().query("SELECT * FROM _lattice_ordered_helper_v1 ORDER BY scope"),helper);
    EXPECT_FALSE(client.db().is_in_transaction());
}
std::string exact_trigger(lattice::database& db,const std::string& name) {
    return std::get<std::string>(db.query("SELECT sql FROM sqlite_master WHERE name=?",{name}).at(0).at("sql"));
}
void controlled_receiver_change(lattice::database& db,const std::string& assignment) {
    const auto trigger=exact_trigger(db,"_lattice_ordered_receiver_v1_UPDATE");
    db.execute("DROP TRIGGER _lattice_ordered_receiver_v1_UPDATE");
    db.execute("UPDATE _lattice_ordered_receiver_v1 SET "+assignment);
    db.execute(trigger);
    ASSERT_EQ(exact_trigger(db,"_lattice_ordered_receiver_v1_UPDATE"),trigger);
}
struct upload_read_hook {
    using callback=void (*)(lattice::database&,sqlite3_stmt*);
    callback prior=lattice::detail::recovery_admission_test_hooks::after_read_row;
    upload_read_hook* previous=current;
    lattice::database& writer;
    std::function<void()> action;
    bool fired=false;
    static inline thread_local upload_read_hook* current=nullptr;
    upload_read_hook(lattice::database& db,std::function<void()> value):writer(db),action(std::move(value)) {
        current=this;
        lattice::detail::recovery_admission_test_hooks::after_read_row=[](lattice::database& db,sqlite3_stmt* statement) {
            auto* self=current;if(self->prior)self->prior(db,statement);
            const char* sql=sqlite3_sql(statement);
            if(!self->fired && &db==&self->writer && sql &&
               (std::string_view(sql).find("SELECT * FROM AuditLog")==0||std::string_view(sql).find("SELECT a.* FROM AuditLog")==0)) {
                self->fired=true;self->action();
            }
        };
    }
    ~upload_read_hook(){lattice::detail::recovery_admission_test_hooks::after_read_row=prior;current=previous;}
};
struct raw_upload_connection {
    sqlite3* handle=nullptr;
    explicit raw_upload_connection(const std::string& path) {
        if(sqlite3_open_v2(path.c_str(),&handle,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX,nullptr)!=SQLITE_OK) {
            if(handle)sqlite3_close_v2(handle);handle=nullptr;
            throw std::runtime_error("upload fixture could not open second connection");
        }
        sqlite3_busy_timeout(handle,100);
    }
    ~raw_upload_connection(){if(handle)sqlite3_close_v2(handle);}
    void execute(const std::string& sql) {
        if(sqlite3_exec(handle,sql.c_str(),nullptr,nullptr,nullptr)!=SQLITE_OK)
            throw std::runtime_error("upload fixture second connection statement failed");
    }
};
}

TEST(OrdinaryHelperUploadIntegrity, MissingReceiverWithSurvivingHelperNeverFallsBackThroughEitherQuery) {
    TempDB source_path{"upload_drop_source"},client_path{"upload_drop_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    ASSERT_EQ(lattice::query_audit_log(client.db(),true).size(),1u);
    for(const char* name:{"_lattice_ordered_receiver_baseline_v1","_lattice_ordered_receiver_cut_v1","_lattice_ordered_receiver_conflict_v1","_lattice_ordered_receiver_v1"})
        client.db().execute(std::string("DROP TABLE ")+name);
    ASSERT_TRUE(client.db().table_exists("_lattice_ordered_helper_v1"));
    ASSERT_FALSE(client.db().table_exists("_lattice_ordered_receiver_v1"));
    both_refuse_unchanged(client);
}

TEST(OrdinaryHelperUploadIntegrity, RestoredExactGuardsCannotBlessAnUnreceiptedFrontier) {
    TempDB source_path{"upload_frontier_source"},client_path{"upload_frontier_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    controlled_receiver_change(client.db(),"cut=cut+1");both_refuse_unchanged(client);
}

TEST(OrdinaryHelperUploadIntegrity, ActualSourceEpochAndPhysicalSchemaBindingMismatchesRefuseBothQueries) {
    for(const std::string assignment:{"source='00000000-0000-0000-0000-000000000000'","epoch=epoch+1","device=device||'x'","inode=inode||'x'","schema=schema||'x'"}) {
        SCOPED_TRACE(assignment);TempDB source_path{"upload_binding_source"},client_path{"upload_binding_client"};
        lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
        controlled_receiver_change(client.db(),assignment);both_refuse_unchanged(client);
    }
}

TEST(OrdinaryHelperUploadIntegrity, CommittedBaselineDamageWithRestoredGuardRefusesBothQueries) {
    TempDB source_path{"upload_baseline_source"},client_path{"upload_baseline_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    const auto trigger=exact_trigger(client.db(),"_lattice_ordered_receiver_baseline_v1_UPDATE");
    client.db().execute("DROP TRIGGER _lattice_ordered_receiver_baseline_v1_UPDATE");
    client.db().execute("UPDATE _lattice_ordered_receiver_baseline_v1 SET value=X'00'");client.db().execute(trigger);
    both_refuse_unchanged(client);
}

TEST(OrdinaryHelperUploadIntegrity, ActualModelSchemaChangeRefusesBothQueries) {
    TempDB source_path{"upload_schema_source"},client_path{"upload_schema_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    client.db().execute("ALTER TABLE TestPerson ADD COLUMN unownedUploadColumn TEXT");both_refuse_unchanged(client);
}

TEST(OrdinaryHelperUploadIntegrity, CopiedEnrolledFileDoesNotAcquireTheOriginalPhysicalBinding) {
    TempDB source_path{"upload_copy_source"},client_path{"upload_copy_client"},copy_path{"upload_copy_other_inode"};
    {
        lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
        ASSERT_EQ(lattice::query_audit_log(client.db(),true).size(),1u);
    }
    ASSERT_TRUE(std::filesystem::copy_file(client_path.path,copy_path.path));
    lattice::lattice_db copied{upload_config(copy_path)};
    ASSERT_TRUE(copied.db().table_exists("_lattice_ordered_helper_v1"));both_refuse_unchanged(copied);
}

TEST(OrdinaryHelperUploadIntegrity, ValidUploadsPreserveCallerReadAndWriteTransactionCustody) {
    TempDB source_path{"upload_turn_source"},client_path{"upload_turn_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    const auto initial=lattice::query_audit_log(client.db(),true);ASSERT_EQ(initial.size(),1u);
    for(bool write:{false,true}) {
        client.db().execute("BEGIN DEFERRED");
        if(write)client.db().execute("UPDATE TestPerson SET name='caller-uncommitted'");
        for(bool per_sync:{false,true}) {
            const auto output=read_upload(client.db(),per_sync);ASSERT_FALSE(output.empty());
            EXPECT_TRUE(client.db().is_in_transaction());
            EXPECT_EQ(sqlite3_txn_state(client.db().handle(),"main"),write?SQLITE_TXN_WRITE:SQLITE_TXN_READ);
        }
        client.db().execute("ROLLBACK");EXPECT_FALSE(client.db().is_in_transaction());
        EXPECT_EQ(std::get<std::string>(client.db().query("SELECT name FROM TestPerson").at(0).at("name")),"pending-upload");
    }
    const auto ordinary=read_upload(client.db(),false),per_sync=read_upload(client.db(),true);
    ASSERT_EQ(ordinary.size(),1u);ASSERT_EQ(per_sync.size(),1u);
    EXPECT_EQ(ordinary.front().global_id,initial.front().global_id);EXPECT_EQ(per_sync.front().global_id,initial.front().global_id);
}

TEST(OrdinaryHelperUploadIntegrity, ARealConcurrentMetadataCommitCannotSplitThePinnedUploadView) {
    for(bool per_sync:{false,true}) {
    TempDB source_path{"upload_concurrent_source"},client_path{"upload_concurrent_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    raw_upload_connection second(client_path.str());
    const auto trigger=exact_trigger(client.db(),"_lattice_ordered_receiver_v1_UPDATE");
    upload_read_hook hook(client.db(),[&] {
        // A distinct physical connection commits while the upload's actual
        // main read cursor is live. SQLite WAL readers keep their old view.
        second.execute("BEGIN IMMEDIATE");second.execute("DROP TRIGGER _lattice_ordered_receiver_v1_UPDATE");
        second.execute("UPDATE _lattice_ordered_receiver_v1 SET cut=cut+1");second.execute(trigger);second.execute("COMMIT");
    });
    const auto output=read_upload(client.db(),per_sync);
    EXPECT_TRUE(hook.fired);ASSERT_EQ(output.size(),1u);EXPECT_EQ(output.front().operation,"UPDATE");
    // The next view sees the real damaged frontier and must refuse.
    both_refuse_unchanged(client);
    }
}

TEST(OrdinaryHelperUploadIntegrity, SameConnectionCallbackMutationCannotReleaseMixedUploadRows) {
    for(bool per_sync:{false,true}) {
    TempDB source_path{"upload_mutation_source"},client_path{"upload_mutation_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    upload_read_hook hook(client.db(),[&]{client.db().execute("UPDATE TestPerson SET name='callback-mutation'");});
    EXPECT_THROW((void)read_upload(client.db(),per_sync),lattice::db_error);EXPECT_TRUE(hook.fired);
    EXPECT_FALSE(client.db().is_in_transaction());
    EXPECT_EQ(std::get<std::string>(client.db().query("SELECT name FROM TestPerson").at(0).at("name")),"callback-mutation");
    }
}

TEST(OrdinaryHelperUploadIntegrity, SameConnectionCallbackCloseCannotReleasePartialUploadRows) {
    for(bool per_sync:{false,true}) {
    TempDB source_path{"upload_close_source"},client_path{"upload_close_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    auto& writer=client.db();upload_read_hook hook(writer,[&]{writer.close();});
    EXPECT_THROW((void)read_upload(writer,per_sync),lattice::db_error);EXPECT_TRUE(hook.fired);EXPECT_TRUE(writer.is_closed());
    }
}

TEST(OrdinaryHelperUploadIntegrity, DeniedEnumerationReleasesItsCursorWithoutSettlingTheCaller) {
    TempDB source_path{"upload_deny_source"},client_path{"upload_deny_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    auto* handle=client.db().handle();client.db().execute("BEGIN DEFERRED");int denied=0;
    struct reset {sqlite3* handle;~reset(){sqlite3_set_authorizer(handle,nullptr,nullptr);}} restore{handle};
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* context,int action,const char* table,const char*,const char*,const char*)noexcept {
        if(action==SQLITE_READ&&table&&std::strcmp(table,"AuditLog")==0){++*static_cast<int*>(context);return SQLITE_DENY;}return SQLITE_OK;
    },&denied),SQLITE_OK);
    for(bool per_sync:{false,true})EXPECT_THROW((void)read_upload(client.db(),per_sync),lattice::db_error);
    EXPECT_GT(denied,0);EXPECT_TRUE(client.db().is_in_transaction());
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);client.db().execute("ROLLBACK");
    EXPECT_EQ(lattice::query_audit_log(client.db(),true).size(),1u);
}

TEST(OrdinaryHelperUploadIntegrity, BusyConnectionRefusesBeforeTheOwningThreadReleasesItsMutex) {
    TempDB source_path{"upload_mutex_source"},client_path{"upload_mutex_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    auto& writer=client.db();auto* mutex=sqlite3_db_mutex(writer.handle());ASSERT_NE(mutex,nullptr);
    for(bool per_sync:{false,true}) {
        sqlite3_mutex_enter(mutex);
        std::future<bool> result;
        try {result=std::async(std::launch::async,[&]{try{(void)read_upload(writer,per_sync);return false;}catch(const lattice::db_error&){return true;}});}
        catch(...){sqlite3_mutex_leave(mutex);throw;}
        const auto ready=result.wait_for(std::chrono::seconds(10));
        // Release even on failure, then join the actual call. The oracle is
        // completion BEFORE release, not eventual success after the deadline.
        sqlite3_mutex_leave(mutex);
        EXPECT_EQ(ready,std::future_status::ready);EXPECT_TRUE(result.get());
    }
}

TEST(OrdinaryHelperUploadIntegrity, ForeignCoreTransactionCustodyCannotBeBorrowedByEitherQuery) {
    TempDB source_path{"upload_foreign_source"},client_path{"upload_foreign_client"};
    lattice::lattice_db source{upload_config(source_path)},client{upload_config(client_path)};enroll_upload(source,client);
    auto& writer=client.db();client.begin_transaction();
    for(bool per_sync:{false,true}) {
        auto result=std::async(std::launch::async,[&]{try{(void)read_upload(writer,per_sync);return false;}catch(const lattice::db_error&){return true;}});
        const auto ready=result.wait_for(std::chrono::seconds(10));
        // A failure cannot strand a foreign waiter behind this caller's turn.
        if(ready!=std::future_status::ready)client.rollback();
        EXPECT_EQ(ready,std::future_status::ready);EXPECT_TRUE(result.get());
        ASSERT_TRUE(writer.is_in_transaction());
    }
    client.rollback();EXPECT_EQ(lattice::query_audit_log(writer,true).size(),1u);
}
