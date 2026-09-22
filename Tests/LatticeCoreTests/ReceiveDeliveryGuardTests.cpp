#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/receive_delivery_guard.hpp"
#include <lattice/sync.hpp>
#include <cstring>
#include <functional>
#include <deque>
#include <memory>

namespace {
using namespace lattice;
using access = detail::receive_delivery_guard_access;
using state = detail::receive_guard_state;
using reason = detail::receive_guard_reason;
constexpr const char* channel = "receive-guard-a";
configuration config(const std::string& path = ":memory:") {
    configuration c(path); c.audit_retention_seconds = 0; c.busy_timeout_ms = 100; return c;
}
std::unique_ptr<lattice_db> open(const std::string& path = ":memory:") {
    auto owner = std::make_unique<lattice_db>(config(path));
    if (path != ":memory:") {
        auto* notifier = instance_registry::instance().get_or_create_notifier(path);
        if (notifier) notifier->stop_listening();
    }
    return owner;
}
int64_t scalar(lattice_db& owner, const std::string& sql) { return std::get<int64_t>(owner.db().query(sql).at(0).at("n")); }
audit_log_entry change(lattice_db& owner, const std::string& id, const std::string& initial) {
    owner.add(TestPerson{initial, 1, std::nullopt});
    audit_log_entry e; e.global_id = id; e.table_name = "TestPerson"; e.operation = "UPDATE";
    e.global_row_id = std::get<std::string>(owner.db().query("SELECT globalId FROM TestPerson WHERE name=?", {initial}).at(0).at("globalId"));
    e.changed_fields_names = {"name"}; e.changed_fields = {{"name", any_property(initial + "-changed")}};
    e.timestamp = "1789819200.0"; return e;
}
std::string name(lattice_db& owner, const audit_log_entry& e) { return std::get<std::string>(owner.db().query("SELECT name FROM TestPerson WHERE globalId=?", {e.global_row_id}).at(0).at("name")); }
audit_log_entry invalid(audit_log_entry e) { e.changed_fields["name"] = any_property(nullptr); return e; }
std::vector<std::string> apply(lattice_db& owner, const std::vector<audit_log_entry>& entries, const std::string& target = channel) { return apply_remote_changes_for(owner, entries, target); }
struct intake_hook {
    std::function<void()> previous;
    explicit intake_hook(std::function<void()> f) : previous(std::move(detail::receive_guard_test_hooks::after_intake_commit)) { detail::receive_guard_test_hooks::after_intake_commit = std::move(f); }
    ~intake_hook() { detail::receive_guard_test_hooks::after_intake_commit = std::move(previous); }
};
struct commit_fault {
    sqlite3* h; int seen = 0; int first_denied;
    explicit commit_fault(lattice_db& owner, int first) : h(owner.db().handle()), first_denied(first) {
        sqlite3_set_authorizer(h, [](void* p, int op, const char* arg, const char*, const char*, const char*) -> int {
            auto& self = *static_cast<commit_fault*>(p);
            if (op == SQLITE_TRANSACTION && arg && std::strcmp(arg, "COMMIT") == 0 && ++self.seen >= self.first_denied) return SQLITE_DENY;
            return SQLITE_OK;
        }, this);
    }
    ~commit_fault() { sqlite3_set_authorizer(h, nullptr, nullptr); }
};
}

TEST(ReceiveDeliveryGuard, KnownFailureKeepsPriorCheckpointAndIndependentAcknowledgmentsAcrossReopen) {
    TempDB file{"receive_guard_gap"}; std::string failed_row;
    {
        auto owner = open(file.str()); const auto first = change(*owner,"guard201","first");
        const auto failed = change(*owner,"guard202","failed"); const auto later = change(*owner,"guard203","later"); failed_row = failed.global_row_id;
        EXPECT_EQ(apply(*owner,{first,invalid(failed)}),(std::vector<std::string>{first.global_id}));
        EXPECT_EQ(apply(*owner,{later}),(std::vector<std::string>{later.global_id}));
        const auto signal = access::read(*owner,channel);
        EXPECT_EQ(signal.state,state::recovery_required); EXPECT_EQ(signal.reason,reason::entry_failed);
        EXPECT_EQ(signal.checkpoint,std::optional<std::string>(first.global_id)); EXPECT_EQ(name(*owner,failed),"failed");
        EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT last_received_event_id AS c FROM _lattice_replication_slots WHERE sync_id=?",{std::string(channel)}).at(0).at("c")),first.global_id);
    }
    auto owner = open(file.str()); const auto signal = access::read(*owner,channel);
    EXPECT_EQ(signal.state,state::recovery_required); EXPECT_EQ(signal.checkpoint,std::optional<std::string>("guard201"));
    EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT name FROM TestPerson WHERE globalId=?",{failed_row}).at(0).at("name")),"failed");
    EXPECT_FALSE(owner->db().is_in_transaction());
}

TEST(ReceiveDeliveryGuard, InitializedNullCannotBorrowAnotherChannelsLatestRemoteRow) {
    auto owner=open(); const auto bad=change(*owner,"null-failure","bad"); const auto other=change(*owner,"other-success","other");
    EXPECT_TRUE(apply(*owner,{invalid(bad)}).empty()); EXPECT_EQ(apply(*owner,{other},"other"),(std::vector<std::string>{other.global_id}));
    const auto a=access::read(*owner,channel), b=access::read(*owner,"other");
    EXPECT_TRUE(a.present); EXPECT_FALSE(a.checkpoint); EXPECT_EQ(a.state,state::recovery_required);
    EXPECT_EQ(b.state,state::idle); EXPECT_EQ(b.checkpoint,std::optional<std::string>(other.global_id));
    ensure_cursor_column(owner->db()); EXPECT_FALSE(access::read(*owner,channel).checkpoint);
}

TEST(ReceiveDeliveryGuard, SuccessfulRepairAndUploadResetCannotClearStickyGap) {
    auto owner=open(); const auto e=change(*owner,"repair-identity","before");
    EXPECT_TRUE(apply(*owner,{invalid(e)}).empty()); const auto first=access::read(*owner,channel);
    EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id}));
    const auto repaired=access::read(*owner,channel); EXPECT_EQ(repaired.state,state::recovery_required); EXPECT_EQ(repaired.checkpoint,first.checkpoint);
    owner->reset_sync_state(channel); EXPECT_EQ(access::read(*owner,channel),repaired);
}

TEST(ReceiveDeliveryGuard, CleanNoopDeliveriesKeepOneBoundedRowAndAdvanceOpaquePrefixes) {
    auto owner=open(); auto e=change(*owner,"noop0","value"); e.changed_fields["name"]=any_property("value");
    for(int i=0;i<32;++i) { e.global_id="opaque-"+std::to_string(32-i); EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id})); }
    const auto signal=access::read(*owner,channel); EXPECT_EQ(signal.state,state::idle); EXPECT_EQ(signal.generation,32); EXPECT_EQ(signal.checkpoint,std::optional<std::string>(e.global_id));
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_receive_guard"),1);
    EXPECT_EQ(scalar(*owner,"SELECT channels AS n FROM _lattice_receive_guard_store"),1);
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM AuditLog WHERE globalId LIKE 'opaque-%'"),0);
}

TEST(ReceiveDeliveryGuard, IntakeCommitFailureHasNoEffectsAndReportsUncommittedAdmission) {
    auto owner=open(); const auto e=change(*owner,"intake-refused","before");
    { commit_fault fault(*owner,1);
        try { (void)apply(*owner,{e}); FAIL()<<"intake COMMIT must refuse"; }
        catch(const detail::receive_admission_error& error) { EXPECT_FALSE(error.durable_intake); EXPECT_NE(error.cause,nullptr); }
    }
    EXPECT_FALSE(owner->db().is_in_transaction()); EXPECT_FALSE(access::read(*owner,channel).present); EXPECT_EQ(name(*owner,e),"before");
    EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id}));
}

TEST(ReceiveDeliveryGuard, AdmittedIntakeRefreshesSlotActivityWithoutClaimingEntryAcceptance) {
    auto owner=open(); const auto e=change(*owner,"activity-entry","before");
    register_replication_slot(owner->db(),channel);
    const std::string stale="1900-01-01 00:00:00";
    const auto expire=[&]{owner->db().execute("UPDATE _lattice_replication_slots SET last_active_at=? WHERE sync_id=?",{stale,std::string(channel)});};
    const auto activity=[&]{return std::get<std::string>(owner->db().query("SELECT last_active_at AS value FROM _lattice_replication_slots WHERE sync_id=?",{std::string(channel)}).at(0).at("value"));};
    expire();
    {
        commit_fault fault(*owner,1);
        EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error);
    }
    EXPECT_EQ(activity(),stale); // Uncommitted intake publishes no activity.
    {
        intake_hook hook([]{throw std::runtime_error("stop after committed activity");});
        EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error);
    }
    EXPECT_NE(activity(),stale); EXPECT_EQ(activity().size(),19u);
    EXPECT_EQ(name(*owner,e),"before");
    EXPECT_EQ(access::read(*owner,channel).state,state::in_progress);
    EXPECT_FALSE(access::read(*owner,channel).checkpoint);
    expire();
    EXPECT_TRUE(apply(*owner,{invalid(e)}).empty());
    EXPECT_NE(activity(),stale); EXPECT_EQ(name(*owner,e),"before");
    EXPECT_FALSE(access::read(*owner,channel).checkpoint);
    expire();
    EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id}));
    EXPECT_NE(activity(),stale);
    EXPECT_EQ(access::read(*owner,channel).state,state::recovery_required);
    EXPECT_FALSE(access::read(*owner,channel).checkpoint);
}

TEST(ReceiveDeliveryGuard, ChunkCommitFailureRetainsCommittedIntakeAndBlocksLaterCursorMovement) {
    auto owner=open(); const auto e=change(*owner,"chunk-refused","before"); const auto later=change(*owner,"later-committed","later");
    { commit_fault fault(*owner,2); EXPECT_TRUE(apply(*owner,{e}).empty()); EXPECT_EQ(fault.seen,3); }
    const auto unfinished=access::read(*owner,channel); EXPECT_EQ(unfinished.state,state::in_progress); EXPECT_FALSE(unfinished.checkpoint); EXPECT_EQ(name(*owner,e),"before");
    EXPECT_EQ(apply(*owner,{later}),(std::vector<std::string>{later.global_id}));
    EXPECT_EQ(access::read(*owner,channel).state,state::recovery_required); EXPECT_FALSE(access::read(*owner,channel).checkpoint);
}

TEST(ReceiveDeliveryGuard, IgnoredIntakeCounterWriteRollsBackEveryNewGuardAndSlot) {
    auto owner=open(); const auto e=change(*owner,"counter-refused","before");
    owner->db().execute("CREATE TRIGGER ignore_guard_counter BEFORE UPDATE OF last_incarnation ON _lattice_receive_guard_store BEGIN SELECT RAISE(IGNORE); END");

    EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error);
    EXPECT_FALSE(access::read(*owner,channel).present); EXPECT_EQ(scalar(*owner,"SELECT channels AS n FROM _lattice_receive_guard_store"),0); EXPECT_EQ(name(*owner,e),"before");
    owner->db().execute("DROP TRIGGER ignore_guard_counter"); EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id}));
}

TEST(ReceiveDeliveryGuard, IgnoredFinalGuardOrSlotWritesCannotAcknowledgeRolledBackEffects) {
    for(bool slot:{false,true}) {
        auto owner=open(); const auto e=change(*owner,"final-refused","before");
        owner->db().execute(slot?
            "CREATE TRIGGER ignore_final BEFORE UPDATE OF last_received_event_id ON _lattice_replication_slots WHEN NEW.last_received_event_id IS NOT NULL BEGIN SELECT RAISE(IGNORE); END":
            "CREATE TRIGGER ignore_final BEFORE UPDATE OF state ON _lattice_receive_guard WHEN NEW.state=0 BEGIN SELECT RAISE(IGNORE); END");
        EXPECT_TRUE(apply(*owner,{e}).empty()); EXPECT_EQ(name(*owner,e),"before");
        EXPECT_EQ(access::read(*owner,channel).state,state::in_progress); EXPECT_FALSE(access::read(*owner,channel).checkpoint);
        owner->db().execute("DROP TRIGGER ignore_final"); EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id}));
        EXPECT_EQ(access::read(*owner,channel).state,state::recovery_required);
    }
}

TEST(ReceiveDeliveryGuard, IgnoredAuditSuppressionRefusesBeforeModelOrReceiptEffects) {
    auto owner=open(); const auto e=change(*owner,"suppression-refused","before");
    const auto original_count=scalar(*owner,"SELECT COUNT(*) AS n FROM AuditLog");
    owner->db().execute("CREATE TRIGGER ignore_receive_suppression BEFORE UPDATE OF disabled ON _SyncControl WHEN NEW.disabled=1 BEGIN SELECT RAISE(IGNORE); END");
    EXPECT_TRUE(apply(*owner,{e}).empty());
    EXPECT_EQ(name(*owner,e),"before");
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM AuditLog"),original_count);
    EXPECT_EQ(scalar(*owner,"SELECT disabled AS n FROM _SyncControl"),0);
    EXPECT_EQ(access::read(*owner,channel).state,state::in_progress);
    owner->db().execute("DROP TRIGGER ignore_receive_suppression");
    EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id}));
    EXPECT_EQ(access::read(*owner,channel).state,state::recovery_required);
}

TEST(ReceiveDeliveryGuard, ModelEffectsCannotChangeTheChunkStartGuardOrBudgetPostimage) {
    for(bool counters:{false,true}) {
        auto owner=open(); const auto e=change(*owner,"entry-guard-corruption","before");
        owner->db().execute(counters?
            "CREATE TRIGGER corrupt_receive_effect AFTER UPDATE OF name ON TestPerson BEGIN UPDATE _lattice_receive_guard_store SET channel_bytes=0; END":
            "CREATE TRIGGER corrupt_receive_effect AFTER UPDATE OF name ON TestPerson BEGIN UPDATE _lattice_receive_guard SET checkpoint=x'626164'; END");
        EXPECT_TRUE(apply(*owner,{e}).empty());
        EXPECT_EQ(name(*owner,e),"before");
        const auto signal=access::read(*owner,channel);
        EXPECT_EQ(signal.state,state::in_progress); EXPECT_FALSE(signal.checkpoint);
        EXPECT_EQ(signal.store_channel_bytes,static_cast<int64_t>(std::strlen(channel)));
        owner->db().execute("DROP TRIGGER corrupt_receive_effect");
        EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id}));
        EXPECT_EQ(access::read(*owner,channel).state,state::recovery_required);
    }
}

TEST(ReceiveDeliveryGuard, LastSyncControlMutationCannotCorruptAnEarlierGuardPostimage) {
    auto owner=open(); const auto e=change(*owner,"restore-corruption","before");
    owner->db().execute("CREATE TRIGGER corrupt_receive_restore AFTER UPDATE OF disabled ON _SyncControl WHEN NEW.disabled=0 BEGIN UPDATE _lattice_receive_guard SET checkpoint=x'626164'; END");
    EXPECT_TRUE(apply(*owner,{e}).empty()); EXPECT_EQ(name(*owner,e),"before"); EXPECT_EQ(access::read(*owner,channel).state,state::in_progress); EXPECT_FALSE(access::read(*owner,channel).checkpoint);
    owner->db().execute("DROP TRIGGER corrupt_receive_restore"); EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id}));
}

TEST(ReceiveDeliveryGuard, InterruptedCommittedIntakeSurvivesReopenWithoutAnyEntryEffect) {
    TempDB file{"receive_guard_intake_reopen"}; audit_log_entry first;
    {
        auto owner=open(file.str()); first=change(*owner,"interrupted-entry","before");
        intake_hook hook([]{throw std::runtime_error("controlled exit after intake");});
        try { (void)apply(*owner,{first}); FAIL()<<"controlled exit must stop effects"; }
        catch(const detail::receive_admission_error& error) { EXPECT_TRUE(error.durable_intake); }
        EXPECT_EQ(name(*owner,first),"before"); EXPECT_EQ(access::read(*owner,channel).state,state::in_progress);
    }
    auto owner=open(file.str()); const auto later=change(*owner,"after-reopen","later");
    EXPECT_EQ(apply(*owner,{later}),(std::vector<std::string>{later.global_id})); EXPECT_FALSE(access::read(*owner,channel).checkpoint); EXPECT_EQ(access::read(*owner,channel).reason,reason::interrupted);
}

TEST(ReceiveDeliveryGuard, CompetingOwnerAdmissionCannotLetOlderGenerationAdvanceOrApply) {
    TempDB file{"receive_guard_competing"}; auto a=open(file.str()); const auto first=change(*a,"old-generation","first"); const auto later=change(*a,"new-generation","later"); auto b=open(file.str());
    bool entered=false;
    { intake_hook hook([&]{ intake_hook nested({}); entered=true; EXPECT_EQ(apply(*b,{later}),(std::vector<std::string>{later.global_id})); });
        EXPECT_TRUE(apply(*a,{first}).empty());
    }
    EXPECT_TRUE(entered); EXPECT_EQ(name(*a,first),"first"); EXPECT_EQ(name(*a,later),"later-changed");
    const auto signal=access::read(*a,channel); EXPECT_EQ(signal.generation,2); EXPECT_EQ(signal.state,state::recovery_required); EXPECT_FALSE(signal.checkpoint);
}

TEST(ReceiveDeliveryGuard, RetirementFencesPendingGenerationAndKeepsAmbiguityOnSlotRecreation) {
    auto owner=open(); const auto e=change(*owner,"retired-entry","before");
    { intake_hook hook([&]{owner->remove_sync_channel_state(channel);}); EXPECT_TRUE(apply(*owner,{e}).empty()); }
    const auto retired=access::read(*owner,channel); EXPECT_EQ(retired.state,state::retired); EXPECT_EQ(retired.reason,reason::interrupted); EXPECT_EQ(name(*owner,e),"before");
    register_replication_slot(owner->db(),channel);

    EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error); EXPECT_EQ(access::read(*owner,channel),retired);
}

TEST(ReceiveDeliveryGuard, OutboundOnlyRetirementDoesNotInventReceiveUncertainty) {
    auto owner=open(); register_replication_slot(owner->db(),"outbound-only"); owner->remove_sync_channel_state("outbound-only");
    EXPECT_FALSE(access::read(*owner,"outbound-only").present); EXPECT_EQ(scalar(*owner,"SELECT channels AS n FROM _lattice_receive_guard_store"),0);
}

TEST(ReceiveDeliveryGuard, CallerOwnedAndRawTransactionsRemainOwnedByCallerAfterAdmissionRefusal) {
    for(bool raw:{false,true}) {
        auto owner=open(); const auto e=change(*owner,"caller-entry","before");
        if(raw)owner->db().execute("BEGIN IMMEDIATE"); else owner->begin_transaction();
        owner->db().execute("UPDATE TestPerson SET age=9");

    EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error); EXPECT_TRUE(owner->db().is_in_transaction());
        EXPECT_EQ(scalar(*owner,"SELECT age AS n FROM TestPerson"),9); EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_receive_guard"),0);
        if(raw)owner->db().rollback(); else owner->rollback(); EXPECT_EQ(scalar(*owner,"SELECT age AS n FROM TestPerson"),1);
    }
}

TEST(ReceiveDeliveryGuard, PostcommitObserverSuccessorIsNotRolledBackAndCommittedAckSurvives) {
    auto owner=open(); const auto e=change(*owner,"observer-committed","before"); int calls=0;
    const auto token=owner->add_table_observer("TestPerson",[&](const auto&){++calls; owner->db().begin_transaction(); owner->db().execute("INSERT INTO _lattice_meta(key,value) VALUES('receive-successor','pending')"); throw std::runtime_error("observer throw");});
    EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id})); owner->remove_table_observer("TestPerson",token);
    EXPECT_EQ(calls,1); EXPECT_TRUE(owner->db().is_in_transaction()); EXPECT_EQ(name(*owner,e),"before-changed");
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_meta WHERE key='receive-successor'"),1); owner->db().rollback();
    EXPECT_EQ(access::read(*owner,channel).state,state::idle); EXPECT_EQ(access::read(*owner,channel).checkpoint,std::optional<std::string>(e.global_id));
}

TEST(ReceiveDeliveryGuard, OversizedOpaqueInputRefusesBeforeDurableAdmission) {
    auto owner=open(); auto e=change(*owner,"bounded","before");

    EXPECT_THROW(apply(*owner,{e},std::string(4097,'c')),detail::receive_admission_error);
    e.global_id.assign(4097,'e');

    EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error);
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_receive_guard"),0); EXPECT_EQ(name(*owner,e),"before");
}

TEST(ReceiveDeliveryGuard, OpaqueChannelAndCheckpointKeepEmbeddedNulBytes) {
    auto owner=open(); const std::string target("chan\0tail",9), id("event\0tail",10); const auto e=change(*owner,id,"before");
    EXPECT_EQ(apply(*owner,{e},target),(std::vector<std::string>{id})); const auto signal=access::read(*owner,target);
    EXPECT_EQ(signal.channel,target); EXPECT_EQ(signal.checkpoint,std::optional<std::string>(id)); EXPECT_FALSE(access::read(*owner,"chan").present);
}

TEST(ReceiveDeliveryGuard, UnsupportedIndexOrCheckpointTypeIsAnErrorNotAnIdleFallback) {
    for(bool index:{false,true}) {
        auto owner=open(); const auto e=change(*owner,"corrupt-read","before"); ASSERT_EQ(apply(*owner,{e}).size(),1u);
        if(index)owner->db().execute("DROP INDEX _lattice_receive_guard_state"); else owner->db().execute("UPDATE _lattice_receive_guard SET checkpoint='bad-text'");

    EXPECT_THROW(access::read(*owner,channel),db_error);

    EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error); EXPECT_FALSE(owner->db().is_in_transaction());
    }
}

TEST(ReceiveDeliveryGuard, LegacyShapedUpgradeCannotImportAnUnverifiedSlotCheckpoint) {
    TempDB file{"receive_guard_legacy"}; audit_log_entry e;
    { auto owner=open(file.str()); e=change(*owner,"legacy-next","before"); register_replication_slot(owner->db(),channel);
        owner->db().execute("UPDATE _lattice_replication_slots SET last_received_event_id='unverified-old'");
        owner->db().execute("DROP TABLE _lattice_receive_guard"); owner->db().execute("DROP TABLE _lattice_receive_guard_store");
    }
    auto owner=open(file.str()); EXPECT_TRUE(access::read(*owner,channel).legacy_origin); EXPECT_FALSE(access::read(*owner,channel).checkpoint);
    EXPECT_EQ(apply(*owner,{e}),(std::vector<std::string>{e.global_id})); const auto signal=access::read(*owner,channel);
    EXPECT_EQ(signal.state,state::recovery_required); EXPECT_EQ(signal.reason,reason::legacy_unverified); EXPECT_FALSE(signal.checkpoint);
}

TEST(ReceiveDeliveryGuard, UnresolvedReceiveEvidenceBlocksPruningButNotUnrelatedWrites) {
    auto owner=open(); const auto e=change(*owner,"retention-gap","before"); EXPECT_TRUE(apply(*owner,{invalid(e)}).empty());

    EXPECT_THROW(owner->safe_compact_audit_log(),db_error);

    EXPECT_THROW(owner->force_compact_audit_log(),db_error);

    EXPECT_THROW(owner->generate_history(1),db_error);

    EXPECT_NO_THROW(owner->add(TestPerson{"unrelated",4,std::nullopt})); const auto other=change(*owner,"other-live","other"); EXPECT_EQ(apply(*owner,{other},"other"),(std::vector<std::string>{other.global_id}));
    owner->remove_sync_channel_state(channel);

    EXPECT_THROW(owner->force_compact_audit_log(),db_error);
}

TEST(ReceiveDeliveryGuard, FixedChannelCapacityPersistsRefusalWithoutEvictingKnownState) {
    auto owner=open(); audit_log_entry policy; policy.global_id="policy-accepted"; policy.table_name="UnregisteredGuardPolicyTable"; policy.operation="INSERT"; policy.global_row_id="absent";
    for(int i=0;i<256;++i) ASSERT_EQ(apply(*owner,{policy},"bounded-channel-"+std::to_string(i)),(std::vector<std::string>{policy.global_id}));
    try { (void)apply(*owner,{policy},"one-too-many"); FAIL()<<"capacity should refuse"; }
    catch(const detail::receive_admission_error& error) { EXPECT_TRUE(error.durable_intake); }
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_receive_guard"),256); EXPECT_EQ(scalar(*owner,"SELECT capacity_refused AS n FROM _lattice_receive_guard_store"),1);
    const auto signal=access::read(*owner,"one-too-many"); EXPECT_FALSE(signal.present); EXPECT_TRUE(signal.capacity_refused);
    struct progress {
        sqlite3* h; int steps=0;
        explicit progress(sqlite3* raw):h(raw) { sqlite3_progress_handler(h,1,[](void* p) -> int { return ++static_cast<progress*>(p)->steps>20000; },this); }
        ~progress(){sqlite3_progress_handler(h,0,nullptr,nullptr);}
    };
    std::vector<std::string> acknowledged;
    { progress bounded(owner->db().handle());
        EXPECT_NO_THROW(acknowledged=apply(*owner,{policy},"bounded-channel-0"));
        EXPECT_LE(bounded.steps,20000);
    }
    EXPECT_EQ(acknowledged,(std::vector<std::string>{policy.global_id}));
}

TEST(ReceiveDeliveryGuard, GenerationExhaustionCannotWrapOrReuseAnEarlierDelivery) {
    auto owner=open(); const auto e=change(*owner,"generation-last","before");
    ASSERT_EQ(apply(*owner,{e}).size(),1u);
    owner->db().execute("UPDATE _lattice_receive_guard SET generation=9223372036854775807");
    const auto before=access::read(*owner,channel);
    EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error);
    EXPECT_EQ(access::read(*owner,channel),before);
    EXPECT_FALSE(owner->db().is_in_transaction());
}

TEST(ReceiveDeliveryGuard, IgnoredRetirementTombstoneRollsBackOutboundChannelRemoval) {
    auto owner=open(); const auto e=change(*owner,"retirement-refused","before");
    EXPECT_TRUE(apply(*owner,{invalid(e)}).empty()); const auto before=access::read(*owner,channel);
    owner->db().execute("INSERT INTO _lattice_sync_set VALUES(?, 'TestPerson', ?)",{std::string(channel),e.global_row_id});
    owner->db().execute("CREATE TRIGGER ignore_retirement BEFORE UPDATE OF state ON _lattice_receive_guard WHEN NEW.state=3 BEGIN SELECT RAISE(IGNORE); END");
    EXPECT_THROW(owner->remove_sync_channel_state(channel),db_error);
    EXPECT_EQ(access::read(*owner,channel),before);
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),1);
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_replication_slots"),1);
    owner->db().execute("DROP TRIGGER ignore_retirement"); owner->remove_sync_channel_state(channel);
    EXPECT_EQ(access::read(*owner,channel).state,state::retired);
}

TEST(ReceiveDeliveryGuard, SlotRemovalCannotDowngradeCapturedReceiveAmbiguityOrCharges) {
    const std::vector<std::string> corruptions={
        "UPDATE _lattice_receive_guard SET state=0,reason=0;",
        "UPDATE _lattice_receive_guard SET checkpoint=NULL;",
        "UPDATE _lattice_receive_guard_store SET channel_bytes=0;"};
    for(const auto& corruption:corruptions) {
        auto owner=open(); const auto first=change(*owner,"retire-safe-prefix","first"), failed=change(*owner,"retire-known-failure","failed");
        ASSERT_EQ(apply(*owner,{first,invalid(failed)}),(std::vector<std::string>{first.global_id}));
        const auto before=access::read(*owner,channel);
        ASSERT_EQ(before.state,state::recovery_required);
        ASSERT_EQ(before.checkpoint,std::optional<std::string>(first.global_id));
        owner->db().execute("INSERT INTO _lattice_sync_set VALUES(?, 'TestPerson', ?)",{std::string(channel),first.global_row_id});
        owner->db().execute("CREATE TRIGGER corrupt_receive_retirement AFTER DELETE ON _lattice_replication_slots BEGIN "+corruption+" END");
        EXPECT_THROW(owner->remove_sync_channel_state(channel),db_error);
        EXPECT_EQ(access::read(*owner,channel),before);
        EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_replication_slots"),1);
        EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),1);
        EXPECT_FALSE(owner->db().is_in_transaction());
        owner->db().execute("DROP TRIGGER corrupt_receive_retirement");
        owner->remove_sync_channel_state(channel);
        const auto after=access::read(*owner,channel);
        EXPECT_EQ(after.state,state::retired); EXPECT_EQ(after.reason,reason::entry_failed);
        EXPECT_EQ(after.checkpoint,before.checkpoint); EXPECT_EQ(after.store_channel_bytes,before.store_channel_bytes);
        EXPECT_EQ(after.generation,before.generation+1);
        EXPECT_THROW(owner->force_compact_audit_log(),db_error);
    }
}

TEST(ReceiveDeliveryGuard, TriggerCannotUnderchargeNewChannelDuringIntake) {
    auto owner=open(); const auto e=change(*owner,"undercharged","before");
    owner->db().execute("CREATE TRIGGER undercharge AFTER INSERT ON _lattice_receive_guard BEGIN UPDATE _lattice_receive_guard_store SET channels=0,channel_bytes=0; END");

    EXPECT_THROW(apply(*owner,{e}),detail::receive_admission_error);
    EXPECT_EQ(scalar(*owner,"SELECT COUNT(*) AS n FROM _lattice_receive_guard"),0);
    EXPECT_EQ(scalar(*owner,"SELECT last_incarnation AS n FROM _lattice_receive_guard_store"),0);
    EXPECT_EQ(name(*owner,e),"before");
}

#ifndef __EMSCRIPTEN__
namespace {
class receive_manual_scheduler final : public scheduler {
    std::deque<std::function<void()>> pending_;
    bool executing_=false, stopped_=false;
public:
    void invoke(std::function<void()>&& fn) override { if(!stopped_)pending_.push_back(std::move(fn)); }
    bool is_on_thread() const noexcept override { return executing_; }
    bool is_same_as(const scheduler* other) const noexcept override { return this==other; }
    bool can_invoke() const noexcept override { return !stopped_; }
    void shutdown() override { stopped_=true; std::deque<std::function<void()>> old; old.swap(pending_); }
    bool run_one() {
        if(pending_.empty())return false;
        auto fn=std::move(pending_.front()); pending_.pop_front(); executing_=true;
        try{fn();}catch(...){executing_=false;throw;} executing_=false; return true;
    }
    void run_all() { for(int i=0;i<64;++i)if(!run_one())return; throw std::runtime_error("controlled receive queue exceeded finite turn budget"); }
};
class receive_controlled_transport final : public sync_transport {
    on_message_handler message_;
public:
    int connects=0;
    std::vector<std::vector<std::string>> acknowledgments;
    void connect(const std::string&,const std::map<std::string,std::string>&) override { ++connects; }
    void disconnect() override {}
    transport_state state() const override { return transport_state::connecting; }
    bool supports_reconnect() const override { return true; }
    void send(const transport_message& message) override {
        const auto event=server_sent_event::from_json(message.as_string());
        if(event && event->event_type==server_sent_event::type::ack)acknowledgments.push_back(event->acked_ids);
    }
    void set_on_open(on_open_handler) override {}
    void set_on_message(on_message_handler fn) override { message_=std::move(fn); }
    void set_on_error(on_error_handler) override {}
    void set_on_close(on_close_handler) override {}
    void deliver(const std::vector<audit_log_entry>& entries) {
        const auto json=server_sent_event::make_audit_log(entries).to_json();
        if(message_)message_(transport_message::from_binary({json.begin(),json.end()}));
    }
};
}

TEST(ReceiveDeliveryGuard, MissingGuardFamilyCannotDialUsingLatestRemoteAuditFallback) {
    auto queue=std::make_shared<receive_manual_scheduler>(); auto c=config(); c.sched=queue;
    auto owned=std::make_unique<lattice_db>(c); auto* owner=owned.get();
    const auto e=change(*owner,"other-route-checkpoint","before");
    ASSERT_EQ(apply(*owner,{e},"other-route").size(),1u);
    auto transport=std::make_unique<receive_controlled_transport>(); auto* wire=transport.get();
    sync_config settings; settings.websocket_url="ws://test.invalid/missing-receive-guard"; settings.sync_id="wss:missing-guard";
    settings.all_active_sync_ids={settings.sync_id}; settings.upload_coalesce_ms=0; settings.checkpoint_passive_interval_ms=0;
    synchronizer sync(std::move(owned),settings,std::move(transport));
    owner->db().execute("DROP TABLE _lattice_receive_guard");
    owner->db().execute("DROP TABLE _lattice_receive_guard_store");
    EXPECT_THROW(sync.connect(),db_error);
    EXPECT_EQ(wire->connects,0);
    EXPECT_FALSE(owner->db().is_in_transaction());
}

TEST(ReceiveDeliveryGuard, OldIntakeRefusalCannotStopANewerExplicitConnection) {
    auto queue=std::make_shared<receive_manual_scheduler>(); auto c=config(); c.sched=queue;
    auto owned=std::make_unique<lattice_db>(c); auto* owner=owned.get();
    const auto first=change(*owner,"old-intake","first"), later=change(*owner,"replacement-route","later");
    auto transport=std::make_unique<receive_controlled_transport>(); auto* wire=transport.get();
    sync_config settings; settings.websocket_url="ws://test.invalid/replacement-receive-guard"; settings.sync_id="wss:replacement-guard";
    settings.all_active_sync_ids={settings.sync_id}; settings.upload_coalesce_ms=0; settings.checkpoint_passive_interval_ms=0;
    synchronizer sync(std::move(owned),settings,std::move(transport)); sync.connect();
    {
        intake_hook hook([&]{sync.connect(); throw std::runtime_error("old intake tail refused after replacement");});
        wire->deliver({first});
        ASSERT_NO_THROW(queue->run_all());
    }
    EXPECT_TRUE(wire->acknowledgments.empty()); EXPECT_EQ(wire->connects,2);
    wire->deliver({later});
    ASSERT_NO_THROW(queue->run_all());
    ASSERT_EQ(wire->acknowledgments.size(),1u);
    EXPECT_EQ(wire->acknowledgments[0],(std::vector<std::string>{later.global_id}));
    EXPECT_EQ(name(*owner,first),"first"); EXPECT_EQ(name(*owner,later),"later-changed");
    EXPECT_EQ(access::read(*owner,settings.sync_id).state,state::recovery_required);
}

TEST(ReceiveDeliveryGuard, UnrecordableIntakeStopsQueuedRouteAndDoesNotAppendFilterRemovalAcks) {
    auto queue=std::make_shared<receive_manual_scheduler>(); auto c=config(); c.sched=queue;
    auto owned=std::make_unique<lattice_db>(c); auto* owner=owned.get();
    const auto first=change(*owner,"route-first","first"), later=change(*owner,"route-later","later");
    auto transport=std::make_unique<receive_controlled_transport>(); auto* wire=transport.get();
    sync_config settings; settings.websocket_url="ws://test.invalid/receive-guard"; settings.sync_id="wss:guard-route";
    settings.all_active_sync_ids={settings.sync_id}; settings.upload_coalesce_ms=0; settings.checkpoint_passive_interval_ms=0;
    synchronizer sync(std::move(owned),settings,std::move(transport)); sync.connect();
    audit_log_entry policy; policy.global_id="policy-removal"; policy.table_name="TestPerson"; policy.operation="DELETE"; policy.global_row_id=first.global_row_id; policy.changed_fields_names={"__lattice_filter_removal"};
    owner->db().execute("CREATE TRIGGER deny_route_intake BEFORE UPDATE OF last_incarnation ON _lattice_receive_guard_store BEGIN SELECT RAISE(IGNORE); END");
    wire->deliver({first,policy}); wire->deliver({later});

    ASSERT_NO_THROW(queue->run_all());
    EXPECT_TRUE(wire->acknowledgments.empty()); EXPECT_EQ(name(*owner,first),"first"); EXPECT_EQ(name(*owner,later),"later");
    owner->db().execute("DROP TRIGGER deny_route_intake"); wire->deliver({later});

    ASSERT_NO_THROW(queue->run_all());
    EXPECT_TRUE(wire->acknowledgments.empty()); EXPECT_EQ(name(*owner,later),"later");
    sync.connect(); wire->deliver({first,policy});

    ASSERT_NO_THROW(queue->run_all());
    ASSERT_EQ(wire->acknowledgments.size(),1u);
    EXPECT_EQ(wire->acknowledgments[0],(std::vector<std::string>{first.global_id,policy.global_id}));
    EXPECT_EQ(name(*owner,first),"first-changed"); EXPECT_EQ(wire->connects,2);
}
#endif
