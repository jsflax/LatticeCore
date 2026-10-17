#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/receive_ledger.hpp"
#include <limits>

namespace {
using namespace lattice::detail;
using error_code = receive_ledger_error_code;
using disposition = receive_intake_disposition;
using acceptance = receive_acceptance;
using checkpoint_kind = receive_checkpoint_kind;
using blob = std::vector<uint8_t>;
blob encoded(const std::string& value) { return blob(value.begin(),value.end()); }

template<class F> void expect_error(error_code expected, F&& function) {
    try { function(); FAIL() << "expected receive ledger refusal"; }
    catch (const receive_ledger_error& error) { EXPECT_EQ(error.code,expected) << error.what(); }
}
struct OwnedTransaction {
    lattice::lattice_db& owner;
    bool done = false;
    explicit OwnedTransaction(lattice::lattice_db& db) : owner(db) { owner.begin_transaction(); }
    ~OwnedTransaction() { if (!done) { try { owner.rollback(); } catch (...) {} } }
    void commit() { owner.commit(); done = true; }
    void rollback() { owner.rollback(); done = true; }
};
lattice::configuration test_config(const std::string& path) {
    lattice::configuration config(path);
    config.audit_retention_seconds = 0;
    config.busy_timeout_ms = 100;
    return config;
}
void stop_notifier(lattice::lattice_db& owner) {
    auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(owner.config().path);
    ASSERT_NE(notifier,nullptr);
    notifier->stop_listening();
    ASSERT_FALSE(notifier->is_listening());
}
class SyncReceiveLedger : public ::testing::Test {
protected:
    // Deliberately tiny test inputs, not recommended production defaults.
    receive_ledger_limits limits{4,64,8,128,8,128,32};
    lattice::lattice_db owner{test_config(":memory:")};
    receive_ledger ledger() { return receive_ledger(owner,limits); }
    receive_ledger_token create(const std::string& name = "channel") {
        OwnedTransaction tx(owner);
        auto l = ledger(); l.initialize();
        auto token = l.create(name);
        if (!token) throw std::runtime_error("test channel creation unexpectedly refused");
        tx.commit();
        return *token; // publish only after checked COMMIT
    }
    receive_ledger_token reserve(receive_ledger_token token,
                                  const std::vector<receive_identity_request>& ids) {
        OwnedTransaction tx(owner);
        auto result = ledger().reserve(token,ids);
        if (!result.admitted) throw std::runtime_error("test intake unexpectedly refused");
        tx.commit();
        return result.token;
    }
};
}

TEST_F(SyncReceiveLedger, RequiresActualOwnedActiveWriteTransaction) {
    auto l = ledger();
    expect_error(error_code::transaction_required,[&] { l.initialize(); });
    owner.db().begin_transaction(); // active SQLite state alone grants no Core ownership
    expect_error(error_code::transaction_required,[&] { l.initialize(); });
    owner.db().rollback();
    OwnedTransaction tx(owner);
    l.initialize();
    std::optional<error_code> other_thread;
    std::thread other([&] {
        try { l.create("wrong-thread"); }
        catch (const receive_ledger_error& e) { other_thread=e.code; }
    });
    other.join();
    ASSERT_TRUE(other_thread);
    EXPECT_EQ(*other_thread,error_code::transaction_required);
    EXPECT_EQ(l.usage().channels,0);
    tx.commit();
    expect_error(error_code::transaction_required,[&] { l.read("channel"); });
}

TEST_F(SyncReceiveLedger, DistinguishesAbsentInitializedNullValueAndCorruptState) {
    OwnedTransaction tx(owner);
    auto l=ledger();
    EXPECT_EQ(l.read("channel").kind,checkpoint_kind::absent);
    l.initialize();
    EXPECT_EQ(l.read("channel").kind,checkpoint_kind::absent);
    ASSERT_TRUE(l.create("channel"));
    EXPECT_EQ(l.read("channel").kind,checkpoint_kind::initialized_null);
    EXPECT_FALSE(l.read("channel").checkpoint);
    // Storage read coverage for a future reviewed checkpoint writer. The
    // primitive itself deliberately exposes no import/advance API.
    const std::string checkpoint("opaque\0checkpoint",17);
    owner.db().execute("UPDATE _lattice_receive_channel SET checkpoint=?",{encoded(checkpoint)});
    auto state=l.read("channel");
    EXPECT_EQ(state.kind,checkpoint_kind::value);
    EXPECT_EQ(state.checkpoint,std::optional<std::string>(checkpoint));
    owner.db().execute("UPDATE _lattice_receive_channel SET checkpoint='wrong SQL type'");
    expect_error(error_code::corrupt_state,[&] { l.read("channel"); });
    tx.rollback();
}

TEST_F(SyncReceiveLedger, InitializationRollsBackAndPartialSchemaIsNotMigrated) {
    { OwnedTransaction tx(owner); ledger().initialize(); tx.rollback(); }
    OwnedTransaction tx(owner);
    EXPECT_EQ(ledger().read("channel").kind,checkpoint_kind::absent);
    owner.db().execute("CREATE TABLE _lattice_receive_store(inherited TEXT)");
    expect_error(error_code::corrupt_state,[&] { ledger().initialize(); });
    expect_error(error_code::corrupt_state,[&] { ledger().read("channel"); });
    tx.rollback();
}

TEST_F(SyncReceiveLedger, DuplicateEncodedIdsAndCompletedTailKeepTheirCharges) {
    const auto initial=create();
    const std::string binary("id\0tail",7), unicode="\xc3\xa9";
    auto token=reserve(initial,{{binary,disposition::known_schema},{binary,disposition::known_schema},
                               {unicode,disposition::unknown_schema_policy}});
    OwnedTransaction tx(owner);
    auto l=ledger();
    EXPECT_EQ(l.read("channel").identities,2);
    EXPECT_EQ(l.read("channel").identity_bytes,9);
    l.complete(token,binary,acceptance::applied);
    l.complete(token,unicode,acceptance::policy);
    EXPECT_EQ(l.read("channel").pending,0);
    EXPECT_EQ(l.usage().identities,2);
    EXPECT_EQ(l.usage().identity_bytes,9);
    auto duplicate=l.reserve(token,{{binary,disposition::known_schema},{unicode,disposition::unknown_schema_policy}});
    ASSERT_TRUE(duplicate.admitted);
    EXPECT_EQ(duplicate.new_identities,0);
    EXPECT_EQ(duplicate.new_identity_bytes,0);
    EXPECT_EQ(l.identity(duplicate.token,binary)->acceptance,acceptance::applied);
    EXPECT_EQ(l.read("channel").kind,checkpoint_kind::initialized_null);
    tx.commit();
}

TEST_F(SyncReceiveLedger, WholeDeliveryOverflowReservesNoPrefixAndNeverSelfClears) {
    limits.identities=limits.identities_per_channel=2;
    auto token=reserve(create(),{{"known",disposition::known_schema}});
    {
        OwnedTransaction tx(owner);
        auto l=ledger();
        auto rejected=l.reserve(token,{{"second",disposition::known_schema},{"third",disposition::known_schema}});
        EXPECT_FALSE(rejected.admitted);
        EXPECT_EQ(rejected.new_identities,0);
        EXPECT_FALSE(l.identity(rejected.token,"second"));
        EXPECT_FALSE(l.identity(rejected.token,"third"));
        EXPECT_EQ(l.usage().identities,1);
        EXPECT_TRUE(l.read("channel").overflow);
        tx.commit(); token=rejected.token;
    }
    {
        OwnedTransaction tx(owner);
        auto l=ledger();
        l.complete(token,"known",acceptance::no_op);
        EXPECT_EQ(l.read("channel").pending,0);
        EXPECT_TRUE(l.read("channel").overflow);
        auto refused=l.reserve(token,{{"fits-now",disposition::known_schema}});
        EXPECT_FALSE(refused.admitted) << "overflow is not cleared by pending==0 or unused space";
        auto replay=l.reserve(refused.token,{{"known",disposition::known_schema}});
        ASSERT_TRUE(replay.admitted);
        l.complete(replay.token,"known",acceptance::no_op);
        EXPECT_EQ(l.usage().identities,1);
        EXPECT_TRUE(l.read("channel").overflow);
        tx.commit();
    }
}

TEST_F(SyncReceiveLedger, AcceptedTailExhaustsBudgetWhileEarlierIdentityRemainsPending) {
    limits.identities=limits.identities_per_channel=3;
    auto token=reserve(create(),{{"gap",disposition::known_schema},{"tail1",disposition::known_schema}});
    {
        OwnedTransaction tx(owner);
        ledger().complete(token,"tail1",acceptance::applied);
        tx.commit();
    }
    token=reserve(token,{{"tail2",disposition::known_schema}});
    OwnedTransaction tx(owner);
    auto l=ledger();
    l.complete(token,"tail2",acceptance::applied);
    EXPECT_EQ(l.read("channel").pending,1);
    EXPECT_EQ(l.usage().identities,3);
    auto refused=l.reserve(token,{{"tail3",disposition::known_schema}});
    EXPECT_FALSE(refused.admitted);
    EXPECT_FALSE(l.identity(refused.token,"tail3"));
    EXPECT_EQ(l.usage().identities,3);
    tx.commit();
}

TEST_F(SyncReceiveLedger, SharedCountAndEncodedByteBudgetsApplyAcrossChannels) {
    limits.identities=3; limits.identity_bytes=5;
    auto a=reserve(create("a"),{{"abc",disposition::known_schema}});
    auto b=create("b");
    OwnedTransaction tx(owner);
    auto l=ledger();
    const std::string binary("x\0y",3);
    auto refused=l.reserve(b,{{binary,disposition::known_schema}});
    EXPECT_FALSE(refused.admitted);
    EXPECT_EQ(l.usage().identity_bytes,3);
    EXPECT_EQ(l.read("b").identities,0);
    l.complete(a,"abc",acceptance::applied);
    EXPECT_EQ(l.usage().identity_bytes,3);
    tx.commit();
}

TEST_F(SyncReceiveLedger, StoreCountRefusalPreservesOtherChannelPendingAndAcceptance) {
    limits.identities=2;
    auto a=reserve(create("a"),{{"a1",disposition::known_schema},{"a2",disposition::known_schema}});
    auto b=create("b");
    OwnedTransaction tx(owner);
    auto l=ledger();
    l.complete(a,"a2",acceptance::applied);
    auto refused=l.reserve(b,{{"b1",disposition::known_schema}});
    EXPECT_FALSE(refused.admitted);
    EXPECT_EQ(l.read("a").pending,1);
    EXPECT_EQ(l.identity(a,"a2")->acceptance,acceptance::applied);
    EXPECT_EQ(l.read("b").identities,0);
    EXPECT_EQ(l.usage().identities,2);
    tx.commit();
}

TEST_F(SyncReceiveLedger, PerChannelCountAndByteCapsRefuseBeforeStoreBudget) {
    limits.identities_per_channel=1; limits.identity_bytes_per_channel=3;
    auto a=reserve(create("a"),{{"a1",disposition::known_schema}});
    auto b=create("b");
    OwnedTransaction tx(owner);
    auto l=ledger();
    auto count_refused=l.reserve(a,{{"x",disposition::known_schema}});
    EXPECT_FALSE(count_refused.admitted);
    auto bytes_refused=l.reserve(b,{{std::string("a\0bc",4),disposition::known_schema}});
    EXPECT_FALSE(bytes_refused.admitted);
    EXPECT_EQ(l.usage().identities,1);
    EXPECT_EQ(l.usage().identity_bytes,2);
    EXPECT_TRUE(l.read("a").overflow);
    EXPECT_TRUE(l.read("b").overflow);
    tx.commit();
}

TEST_F(SyncReceiveLedger, FixedChannelCountAndIdBytesAreBoundedWithDurableRefusal) {
    limits.channels=1; limits.channel_id_bytes=3;
    auto a=create("abc");
    OwnedTransaction tx(owner);
    auto l=ledger();
    EXPECT_FALSE(l.create("b"));
    EXPECT_TRUE(l.usage().channel_overflow);
    EXPECT_EQ(l.usage().channels,1);
    EXPECT_EQ(l.usage().channel_id_bytes,3);
    l.retire(a);
    EXPECT_EQ(l.usage().channels,0);
    EXPECT_TRUE(l.usage().channel_overflow);
    EXPECT_FALSE(l.create("c")) << "retirement does not authenticate overflow repair";
    tx.commit();
}

TEST_F(SyncReceiveLedger, OversizedChannelIdRefusesWithoutAllocatingChannelState) {
    limits.channel_id_bytes=3;
    OwnedTransaction tx(owner);
    auto l=ledger(); l.initialize();
    EXPECT_FALSE(l.create(std::string("a\0bc",4)));
    EXPECT_EQ(l.usage().channels,0);
    EXPECT_EQ(l.usage().channel_id_bytes,0);
    EXPECT_TRUE(l.usage().channel_overflow);
    tx.commit();
}

TEST_F(SyncReceiveLedger, NewGenerationFencesOldEntryAdmissionAndEveryMutation) {
    auto older=reserve(create(),{{"entry",disposition::known_schema}});
    auto current=reserve(older,{{"entry",disposition::known_schema}});
    OwnedTransaction tx(owner);
    auto l=ledger();
    expect_error(error_code::stale_token,[&] { l.assert_current(older); });
    expect_error(error_code::stale_token,[&] { l.complete(older,"entry",acceptance::applied); });
    expect_error(error_code::stale_token,[&] { l.reserve(older,{{"new",disposition::known_schema}}); });
    expect_error(error_code::stale_token,[&] { l.retire(older); });
    EXPECT_EQ(l.identity(current,"entry")->acceptance,acceptance::pending);
    EXPECT_EQ(l.read("channel").identities,1);
    // Future apply integration calls this before model effects in each unit.
    EXPECT_NO_THROW(l.assert_current(current));
    tx.commit();
}

TEST_F(SyncReceiveLedger, RetirementAndRecreationNeverReuseCommittedIncarnation) {
    auto old=reserve(create(),{{"entry",disposition::known_schema}});
    { OwnedTransaction tx(owner); ledger().retire(old); tx.commit(); }
    auto fresh=create();
    EXPECT_GT(fresh.incarnation,old.incarnation);
    EXPECT_EQ(fresh.generation,0);
    OwnedTransaction tx(owner);
    auto l=ledger();
    expect_error(error_code::stale_token,[&] { l.complete(old,"entry",acceptance::applied); });
    expect_error(error_code::stale_token,[&] { l.reserve(old,{{"entry",disposition::known_schema}}); });
    expect_error(error_code::stale_token,[&] { l.retire(old); });
    EXPECT_EQ(l.read("channel").token,fresh);
    EXPECT_EQ(l.usage().identities,0);
    tx.commit();
}

TEST_F(SyncReceiveLedger, RolledBackUnpublishedTokensAreDiscardedBeforeReinitialization) {
    { OwnedTransaction tx(owner); ledger().initialize(); tx.commit(); }
    {
        OwnedTransaction tx(owner);
        auto unpublished=ledger().create("channel");
        ASSERT_TRUE(unpublished);
        auto draft=ledger().reserve(*unpublished,{{"draft",disposition::known_schema}});
        ASSERT_TRUE(draft.admitted);
        tx.rollback();
        // Neither provisional token escapes this failed intake. No claim of
        // nonreuse is made for invalid/unpublished transaction-local values.
    }
    auto fresh=create();
    OwnedTransaction tx(owner);
    EXPECT_EQ(ledger().read("channel").token,fresh);
    EXPECT_EQ(ledger().usage().identities,0);
    EXPECT_FALSE(ledger().identity(fresh,"draft"));
    tx.commit();
}

TEST_F(SyncReceiveLedger, KnownFailureCannotHealThroughLaterPolicyOnlyAcceptance) {
    auto token=reserve(create(),{{"known",disposition::known_schema},{"unknown",disposition::unknown_schema_policy},
                                 {"filtered",disposition::filter_policy}});
    OwnedTransaction tx(owner);
    auto l=ledger();
    expect_error(error_code::identity_missing,[&] { l.complete(token,"missing",acceptance::applied); });
    expect_error(error_code::disposition_conflict,[&] { l.reserve(token,{{"known",disposition::unknown_schema_policy}}); });
    expect_error(error_code::invalid_acceptance,[&] { l.complete(token,"known",acceptance::policy); });
    expect_error(error_code::invalid_acceptance,[&] { l.complete(token,"unknown",acceptance::applied); });
    EXPECT_EQ(l.identity(token,"known")->acceptance,acceptance::pending);
    l.complete(token,"unknown",acceptance::policy);
    l.complete(token,"filtered",acceptance::policy);
    l.complete(token,"known",acceptance::no_op);
    expect_error(error_code::invalid_acceptance,[&] { l.complete(token,"known",acceptance::applied); });
    EXPECT_EQ(l.read("channel").pending,0);
    EXPECT_EQ(l.usage().identities,3);
    tx.commit();
}

TEST_F(SyncReceiveLedger, IntakeSqlFailureRestoresAllReservationsAndGeneration) {
    auto token=create();
    OwnedTransaction tx(owner);
    auto l=ledger();
    owner.db().execute("CREATE TRIGGER reject_second_receive BEFORE INSERT ON _lattice_receive_identity "
        "WHEN NEW.event_id=X'62' BEGIN SELECT RAISE(ABORT,'second reserve refused'); END");
    EXPECT_THROW(l.reserve(token,{{"a",disposition::known_schema},{"b",disposition::known_schema}}),lattice::db_error);
    EXPECT_EQ(l.read("channel").token,token);
    EXPECT_EQ(l.usage().identities,0);
    EXPECT_FALSE(l.read("channel").overflow) << "SQL failure is not a fabricated durable overflow receipt";
    owner.db().execute("DROP TRIGGER reject_second_receive");
    tx.commit();
    OwnedTransaction verify(owner);
    EXPECT_EQ(l.read("channel").token,token);
    EXPECT_EQ(l.usage().identities,0);
    verify.commit();
}

TEST_F(SyncReceiveLedger, IgnoredSqlWriteIsAnErrorRatherThanFalseAdmission) {
    auto token=create();
    OwnedTransaction tx(owner);
    auto l=ledger();
    owner.db().execute("CREATE TRIGGER ignore_receive BEFORE INSERT ON _lattice_receive_identity "
        "BEGIN SELECT RAISE(IGNORE); END");
    EXPECT_THROW(l.reserve(token,{{"entry",disposition::known_schema}}),lattice::db_error);
    EXPECT_EQ(l.read("channel").token,token);
    EXPECT_EQ(l.usage().identities,0);
    owner.db().execute("DROP TRIGGER ignore_receive");
    tx.commit();
}

TEST_F(SyncReceiveLedger, FailedIntakeCommitPublishesNoTokenOrDurableReservation) {
    auto token=create();
    {
        OwnedTransaction tx(owner);
        auto draft=ledger().reserve(token,{{"entry",disposition::known_schema}});
        ASSERT_TRUE(draft.admitted);
        auto* handle=owner.db().handle();
        int commit_attempts=0;
        ASSERT_EQ(sqlite3_set_authorizer(handle,
            [](void* context,int action,const char* first,const char*,const char*,const char*) noexcept {
                if (action==SQLITE_TRANSACTION && first && std::strcmp(first,"COMMIT")==0) {
                    ++*static_cast<int*>(context);
                    return SQLITE_DENY;
                }
                return SQLITE_OK;
            },&commit_attempts),SQLITE_OK);
        EXPECT_THROW(tx.commit(),lattice::db_error);
        EXPECT_EQ(commit_attempts,1);
        ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);
        tx.rollback();
        // Discard draft; no model effects were attempted after failed intake.
    }
    OwnedTransaction verify(owner);
    EXPECT_EQ(ledger().read("channel").token,token);
    EXPECT_EQ(ledger().usage().identities,0);
    verify.commit();
}

TEST_F(SyncReceiveLedger, CleanupFailurePreservesBothErrorsAndRequiresOuterRollback) {
    auto token=create();
    {
        OwnedTransaction tx(owner);
        owner.db().execute("CREATE TRIGGER fail_receive BEFORE INSERT ON _lattice_receive_identity "
            "WHEN NEW.event_id=X'62' BEGIN SELECT RAISE(ABORT,'original intake failure'); END");
        auto* handle=owner.db().handle();
        int denied_cleanup=0;
        ASSERT_EQ(sqlite3_set_authorizer(handle,
            [](void* context,int action,const char* first,const char* second,const char*,const char*) noexcept {
                if (action==SQLITE_SAVEPOINT && first && second && std::strcmp(first,"ROLLBACK")==0 &&
                    std::strcmp(second,"lattice_receive_primitive")==0) {
                    ++*static_cast<int*>(context); return SQLITE_DENY;
                }
                return SQLITE_OK;
            },&denied_cleanup),SQLITE_OK);
        bool caught=false;
        try { ledger().reserve(token,{{"a",disposition::known_schema},{"b",disposition::known_schema}}); }
        catch (const receive_ledger_error& error) {
            caught=true;
            EXPECT_EQ(error.code,error_code::cleanup_failed);
            EXPECT_TRUE(error.primary_error);
            EXPECT_TRUE(error.cleanup_error);
        }
        catch (...) { ADD_FAILURE() << "expected a structured helper cleanup failure"; }
        sqlite3_set_authorizer(handle,nullptr,nullptr);
        EXPECT_TRUE(caught);
        EXPECT_EQ(denied_cleanup,1);
        tx.rollback(); // mandatory outer abort, not partial intake COMMIT
    }
    OwnedTransaction verify(owner);
    EXPECT_EQ(ledger().read("channel").token,token);
    EXPECT_EQ(ledger().usage().identities,0);
    verify.commit();
}

TEST_F(SyncReceiveLedger, EntryAcceptanceSharesCallerRollbackWithItsModelEffect) {
    auto token=reserve(create(),{{"entry",disposition::known_schema}});
    OwnedTransaction tx(owner);
    auto l=ledger();
    owner.db().execute("CREATE TABLE _ledger_test_model(value INTEGER)");
    owner.db().execute("INSERT INTO _ledger_test_model VALUES(0)");
    owner.db().execute("CREATE TRIGGER reject_receive_acceptance BEFORE UPDATE ON _lattice_receive_identity "
        "BEGIN SELECT RAISE(ABORT,'acceptance refused'); END");
    owner.db().execute("SAVEPOINT caller_entry");
    l.assert_current(token);
    owner.db().execute("UPDATE _ledger_test_model SET value=1");
    EXPECT_THROW(l.complete(token,"entry",acceptance::applied),lattice::db_error);
    owner.db().execute("ROLLBACK TO caller_entry");
    owner.db().execute("RELEASE caller_entry");
    EXPECT_EQ(l.identity(token,"entry")->acceptance,acceptance::pending);
    EXPECT_EQ(std::get<int64_t>(owner.db().query("SELECT value FROM _ledger_test_model").at(0).at("value")),0);
    owner.db().execute("DROP TRIGGER reject_receive_acceptance");
    owner.db().execute("SAVEPOINT caller_entry");
    l.assert_current(token);
    owner.db().execute("UPDATE _ledger_test_model SET value=2");
    l.complete(token,"entry",acceptance::applied);
    owner.db().execute("RELEASE caller_entry");
    tx.rollback(); // committed intake survives; both tentative effects roll back
    OwnedTransaction verify(owner);
    EXPECT_EQ(l.identity(token,"entry")->acceptance,acceptance::pending);
    EXPECT_EQ(l.usage().identities,1);
    verify.commit();
}

TEST_F(SyncReceiveLedger, LimitsAndExhaustedSequencesRefuseWithoutMigrationOrWraparound) {
    auto token=create();
    OwnedTransaction tx(owner);
    auto l=ledger();
    auto changed=limits; changed.identities++;
    receive_ledger incompatible(owner,changed);
    expect_error(error_code::limits_mismatch,[&] { incompatible.initialize(); });
    owner.db().execute("UPDATE _lattice_receive_store SET last_incarnation=?",{std::numeric_limits<int64_t>::max()});
    expect_error(error_code::sequence_exhausted,[&] { l.create("next"); });
    owner.db().execute("UPDATE _lattice_receive_channel SET generation=?",{std::numeric_limits<int64_t>::max()});
    token=l.read("channel").token;
    expect_error(error_code::sequence_exhausted,[&] { l.reserve(token,{}); });
    EXPECT_EQ(l.usage().channels,1);
    EXPECT_EQ(l.usage().identities,0);
    tx.rollback();
}

TEST_F(SyncReceiveLedger, SetupAuditAndAddressedReadRefuseInvalidDurableAcceptance) {
    auto token=reserve(create(),{{"known",disposition::known_schema}});
    OwnedTransaction tx(owner);
    auto l=ledger();
    owner.db().execute("UPDATE _lattice_receive_identity SET acceptance=3");
    expect_error(error_code::corrupt_state,[&] { l.identity(token,"known"); });
    expect_error(error_code::corrupt_state,[&] { l.audit(); });
    expect_error(error_code::corrupt_state,[&] { l.initialize(); });
    tx.rollback();
}

TEST_F(SyncReceiveLedger, PointSettlementDoesNotScanLargeRetainedAcceptedTail) {
    limits.identities=limits.identities_per_channel=4096;
    limits.identity_bytes=limits.identity_bytes_per_channel=65536;
    std::vector<receive_identity_request> ids;
    for (int i=0;i<3072;++i) ids.push_back({"retained-"+std::to_string(i),disposition::known_schema});
    auto token=reserve(create(),ids);
    OwnedTransaction tx(owner);
    auto l=ledger();
    // Bulk fixture arrangement, outside the bounded operation. Other tests
    // cover every supported pending->complete transition through the helper.
    owner.db().execute("UPDATE _lattice_receive_identity SET acceptance=1 WHERE event_id!=?",{encoded("retained-3071")});
    EXPECT_EQ(l.read("channel").pending,1);
    EXPECT_EQ(l.usage().identities,3072);
    auto* handle=owner.db().handle();
    int callbacks=0;
    sqlite3_progress_handler(handle,10,[](void* context) noexcept {
        return ++*static_cast<int*>(context)>1000 ? 1 : 0;
    },&callbacks);
    // This is a deterministic SQLite VM-work ceiling, not a wall-clock
    // performance claim. A full retained-ledger scan exceeds the allowance;
    // addressed indexed metadata/identity reads and one UPDATE fit beneath it.
    EXPECT_NO_THROW(l.complete(token,"retained-3071",acceptance::applied));
    sqlite3_progress_handler(handle,0,nullptr,nullptr);
    EXPECT_LE(callbacks,1000);
    EXPECT_EQ(l.identity(token,"retained-3071")->acceptance,acceptance::applied);
    EXPECT_EQ(l.read("channel").pending,0);
    EXPECT_EQ(l.usage().identities,3072);
    tx.commit();
}

TEST_F(SyncReceiveLedger, MetadataWritesDoNotPublishModelObserverEvents) {
    struct Observed {
        lattice::lattice_db& owner;
        std::vector<std::pair<std::string,lattice::lattice_db::observer_id>> tokens;
        int metadata=0, model=0;
        explicit Observed(lattice::lattice_db& db) : owner(db) {
            for (auto name : {"_lattice_receive_store","_lattice_receive_channel","_lattice_receive_identity"})
                tokens.emplace_back(name,owner.add_table_observer(name,[this](const auto&) { ++metadata; }));
            tokens.emplace_back("TestPerson",owner.add_table_observer("TestPerson",[this](const auto&) { ++model; }));
        }
        ~Observed() { for (const auto& [name,id] : tokens) owner.remove_table_observer(name,id); }
    } observed(owner);
    auto token=reserve(create(),{{"entry",disposition::known_schema}});
    { OwnedTransaction tx(owner); ledger().complete(token,"entry",acceptance::applied); tx.commit(); }
    { OwnedTransaction tx(owner); ledger().retire(token); tx.commit(); }
    owner.add(TestPerson{"positive observer fence",1,std::nullopt});
    EXPECT_GT(observed.model,0);
    EXPECT_EQ(observed.metadata,0) << "WITHOUT ROWID metadata does not enter the generic update-hook path";
}

TEST(SyncReceiveLedgerDurable, PendingAcceptedAndOverflowSurviveAnotherOwnerAndReopen) {
    TempDB file{"sync_receive_ledger"};
    const receive_ledger_limits limits{2,64,2,64,2,64,32};
    receive_ledger_token token;
    {
        lattice::lattice_db owner{test_config(file.str())};
        ASSERT_NO_FATAL_FAILURE(stop_notifier(owner));
        receive_ledger l(owner,limits);
        {
            OwnedTransaction tx(owner); l.initialize();
            auto created=l.create("channel"); ASSERT_TRUE(created);
            auto intake=l.reserve(*created,{{"gap",disposition::known_schema},{"tail",disposition::known_schema}});
            ASSERT_TRUE(intake.admitted);
            tx.commit(); token=intake.token;
        }
        {
            OwnedTransaction tx(owner); l.complete(token,"tail",acceptance::applied);
            auto overflow=l.reserve(token,{{"refused",disposition::known_schema}});
            EXPECT_FALSE(overflow.admitted);
            tx.commit(); token=overflow.token;
        }
        lattice::lattice_db sibling{test_config(file.str())};
        ASSERT_NO_FATAL_FAILURE(stop_notifier(sibling));
        OwnedTransaction tx(sibling);
        receive_ledger peer(sibling,limits);
        EXPECT_TRUE(peer.read("channel").overflow);
        EXPECT_EQ(peer.read("channel").pending,1);
        EXPECT_EQ(peer.identity(token,"tail")->acceptance,acceptance::applied);
        tx.commit();
    }
    {
        lattice::lattice_db reopened{test_config(file.str())};
        ASSERT_NO_FATAL_FAILURE(stop_notifier(reopened));
        receive_ledger l(reopened,limits);
        OwnedTransaction tx(reopened);
        EXPECT_EQ(l.read("channel").token,token);
        EXPECT_EQ(l.read("channel").kind,checkpoint_kind::initialized_null);
        l.complete(token,"gap",acceptance::no_op);
        EXPECT_EQ(l.read("channel").pending,0);
        EXPECT_EQ(l.usage().identities,2);
        EXPECT_TRUE(l.read("channel").overflow);
        EXPECT_FALSE(l.identity(token,"refused"));
        tx.commit();
    }
}
