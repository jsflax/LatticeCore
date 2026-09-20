#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/receive_install_state.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <limits>

namespace {
using namespace lattice::detail;
using err = receive_install_error_code;
using outcome = recovery_install_state;
using disposition = receive_install_disposition;
using blob = std::vector<uint8_t>;
blob encoded(const std::string& s) { return {s.begin(),s.end()}; }
template<class F> void expect_error(err wanted,F&& f) {
    try { f(); FAIL()<<"expected receiver installation refusal"; }
    catch (const receive_install_error& e) { EXPECT_EQ(e.code,wanted)<<e.what(); }
}
std::shared_ptr<lattice::lattice_db> owner_at(const std::string& path=":memory:") {
    lattice::configuration config(path);
    config.audit_retention_seconds=0; config.busy_timeout_ms=100;
    auto owner=std::make_shared<lattice::lattice_db>(config);
    owner->add(TestPerson{"seed",1,std::nullopt});
    if (!config.is_in_memory()) {
        auto* notifier=lattice::instance_registry::instance().get_or_create_notifier(path);
        if (notifier) notifier->stop_listening();
    }
    return owner;
}
void insert(lattice::database& db,const std::string& id,int64_t age=2) {
    db.execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?,?,?)",{id,id,age});
}
int64_t count(lattice::database& db,const std::string& id) {
    return std::get<int64_t>(db.query("SELECT COUNT(*) AS n FROM TestPerson WHERE globalId=?",{id}).at(0).at("n"));
}
receive_install_identity first(int64_t head=0) {
    return {1,0,{},head,receive_install_mode::full,"request-1","receipt-1","content-1","manifest-1"};
}
receive_install_identity successor(const receive_install_identity& prior,int64_t head) {
    return {prior.sequence+1,prior.expected_revision+1,{receive_frontier_kind::position,prior.head},
        head,receive_install_mode::delta,"request-next","receipt-next","content-next","manifest-next"};
}
class ReceiveInstallState : public ::testing::Test {
protected:
    // Test-only limits, not proposed production defaults.
    receive_install_limits limits{4,128,4096};
    std::shared_ptr<lattice::lattice_db> owner=owner_at();
    receive_install_binding binding{"channel","authority","source","epoch","scope","schema"};
    receive_install_store storage() { return {owner,limits}; }
    template<class F> void committed(F&& f) {
        const auto result=recovery_writer_access::install(owner,std::forward<F>(f));
        if (result.primary_error) {
            try { std::rethrow_exception(result.primary_error); }
            catch (const std::exception& e) { ADD_FAILURE()<<e.what(); }
        }
        EXPECT_EQ(result.state,outcome::committed);
        EXPECT_EQ(result.cleanup_error,nullptr); EXPECT_EQ(result.postcommit_error,nullptr);
    }
    void setup() { committed([&](auto&) { auto s=storage(); s.initialize(); s.bind(binding); }); }
    receive_install_snapshot snapshot() {
        std::optional<receive_install_snapshot> result;
        committed([&](auto&) { result=storage().read(binding.channel); });
        if (!result) throw std::runtime_error("missing test binding");
        return *result;
    }
    void install(const receive_install_identity& i,
                 std::optional<receive_install_identity> prior=std::nullopt) {
        committed([&](auto&) {
            EXPECT_EQ(storage().apply_if_new(binding,i,prior,[](auto&) {}).disposition,disposition::installed);
        });
    }
};
}

TEST_F(ReceiveInstallState, ActualOwnedWriterRequiredIncludingReadsAndOtherThread) {
    auto s=storage();
    expect_error(err::transaction_required,[&] { s.initialize(); });
    owner->db().execute("BEGIN IMMEDIATE");
    expect_error(err::transaction_required,[&] { s.initialize(); });
    owner->db().rollback();
    owner->begin_transaction();
    s.initialize(); s.bind(binding);
    std::optional<err> rejected;
    std::thread other([&] {
        try { s.read(binding.channel); } catch (const receive_install_error& e) { rejected=e.code; }
    });
    other.join();
    ASSERT_TRUE(rejected); EXPECT_EQ(*rejected,err::transaction_required);
    owner->commit();
    expect_error(err::transaction_required,[&] { s.read(binding.channel); });
    committed([&](auto&) { s.audit(); });
}

TEST_F(ReceiveInstallState, BindingIsUninitializedAndExplicitNullNeverMeansNumericZero) {
    committed([&](auto& db) {
        auto s=storage(); s.initialize();
        EXPECT_FALSE(s.read(binding.channel)); s.bind(binding);
        const auto uninitialized=s.read(binding.channel)->frontier;
        EXPECT_EQ(uninitialized,(receive_install_frontier{}));
        const receive_install_frontier beginning{receive_frontier_kind::beginning_null,std::nullopt};
        const receive_install_frontier zero{receive_frontier_kind::position,0};
        EXPECT_NE(uninitialized,beginning); EXPECT_NE(beginning,zero); EXPECT_NE(uninitialized,zero);
        auto wrong=first(); wrong.base=beginning;
        expect_error(err::stale,[&] { s.begin(binding,wrong); });
        // Read representation only: no public/private mutator in this slice
        // creates this explicit beginning state, and it is not a completed H.
        db.execute("UPDATE _lattice_install_channel SET frontier_kind=1 WHERE channel=?",{encoded(binding.channel)});
        EXPECT_EQ(s.read(binding.channel)->frontier,beginning);
        EXPECT_EQ(s.read(binding.channel)->revision,0);
        db.execute("UPDATE _lattice_install_channel SET frontier_kind=0 WHERE channel=?",{encoded(binding.channel)});
        EXPECT_EQ(s.apply_if_new(binding,first(),std::nullopt,[](auto&) {}).head,0);
        EXPECT_EQ(s.read(binding.channel)->frontier,zero);
        EXPECT_EQ(s.read(binding.channel)->revision,1);
    });
}

TEST_F(ReceiveInstallState, InitializationAndUnpublishedAttemptRollBackWithoutInventedNonreuse) {
    const auto refused=recovery_writer_access::install(owner,[&](auto&) {
        auto s=storage(); s.initialize(); s.bind(binding); s.begin(binding,first());
        throw std::runtime_error("discard provisional sequence");
    });
    EXPECT_EQ(refused.state,outcome::rolled_back);
    setup(); // failed transaction's token was never published and is discarded
    EXPECT_EQ(snapshot().last_sequence,0);
    install(first());
    EXPECT_EQ(snapshot().last_sequence,1);
}

TEST_F(ReceiveInstallState, ModelAndInstallationCommitAndRollbackTogetherInTrustedFrame) {
    setup(); auto s=storage(); const auto i=first(9);
    auto failed=recovery_writer_access::install(owner,[&](auto&) {
        s.apply_if_new(binding,i,std::nullopt,[](auto& db) { insert(db,"discarded"); });
        throw std::runtime_error("outer refusal");
    });
    EXPECT_EQ(failed.state,outcome::rolled_back);
    EXPECT_EQ(count(owner->db(),"discarded"),0);
    EXPECT_EQ(snapshot().revision,0); EXPECT_FALSE(snapshot().active);
    committed([&](auto&) {
        const auto result=s.apply_if_new(binding,i,std::nullopt,[](auto& db) { insert(db,"kept"); });
        EXPECT_EQ(result.disposition,disposition::installed);
        EXPECT_EQ(result.revision,1); EXPECT_EQ(result.head,9);
    });
    EXPECT_EQ(count(owner->db(),"kept"),1); EXPECT_EQ(snapshot().last_installed,i);
}

TEST_F(ReceiveInstallState, EffectFailureRestoresSavepointAndAllowsOuterSuccessor) {
    setup();
    committed([&](auto& db) {
        auto s=storage(); const auto before=s.read(binding.channel); const auto usage=s.usage();
        EXPECT_THROW(s.apply_if_new(binding,first(),std::nullopt,[](auto& writer) {
            insert(writer,"partial"); throw std::runtime_error("effect refused");
        }),std::runtime_error);
        EXPECT_EQ(count(db,"partial"),0); EXPECT_EQ(s.read(binding.channel),before); EXPECT_EQ(s.usage(),usage);
        insert(db,"ordinary-after-refusal");
    });
    EXPECT_EQ(count(owner->db(),"ordinary-after-refusal"),1);
}

TEST_F(ReceiveInstallState, CommittedActiveAttemptSurvivesFailedEffectsAndCanResume) {
    setup(); const auto i=first(5);
    committed([&](auto&) { EXPECT_EQ(storage().begin(binding,i).disposition,disposition::active); });
    auto before=snapshot();
    auto failed=recovery_writer_access::install(owner,[&](auto&) {
        storage().apply_if_new(binding,i,std::nullopt,[](auto& db) { insert(db,"failed-active"); throw std::runtime_error("failure"); });
    });
    EXPECT_EQ(failed.state,outcome::rolled_back); EXPECT_EQ(snapshot(),before);
    install(i); EXPECT_EQ(snapshot().revision,1); EXPECT_FALSE(snapshot().active);
}

TEST_F(ReceiveInstallState, SameHeadRefreshIncrementsRevisionAndExactRetryPreservesNewLocalEdit) {
    setup(); const auto one=first(7),two=successor(one,7);
    committed([&](auto&) {
        storage().apply_if_new(binding,one,std::nullopt,[](auto& db) { insert(db,"value",10); });
    });
    committed([&](auto&) {
        auto result=storage().apply_if_new(binding,two,one,[](auto& db) {
            db.execute("UPDATE TestPerson SET age=20 WHERE globalId='value'");
        });
        EXPECT_EQ(result.revision,2); EXPECT_EQ(result.head,7);
    });
    owner->db().execute("UPDATE TestPerson SET age=99 WHERE globalId='value'");
    int effects=0;
    committed([&](auto& db) {
        auto s=storage(); const auto before=s.usage();
        const auto result=s.apply_if_new(binding,two,std::nullopt,[&](auto&) { ++effects; });
        EXPECT_EQ(result.disposition,disposition::already_installed); EXPECT_EQ(result.revision,2);
        EXPECT_EQ(s.usage(),before);
        EXPECT_EQ(std::get<int64_t>(db.query("SELECT age FROM TestPerson WHERE globalId='value'").at(0).at("age")),99);
        expect_error(err::stale,[&] { s.begin(binding,one); }); // no false ACK after explicit supersession
    });
    EXPECT_EQ(effects,0); EXPECT_EQ(snapshot().revision,2);
}

TEST_F(ReceiveInstallState, ExplicitExactPriorResultRequiredBeforeEffectsOrReplacement) {
    setup(); const auto one=first(2),two=successor(one,3); install(one);
    int called=0;
    committed([&](auto&) {
        auto s=storage(); const auto before=s.read(binding.channel); auto wrong=one; wrong.content_digest="different";
        expect_error(err::supersession_required,[&] { s.apply_if_new(binding,two,std::nullopt,[&](auto&) { ++called; }); });
        expect_error(err::supersession_required,[&] { s.apply_if_new(binding,two,wrong,[&](auto&) { ++called; }); });
        EXPECT_EQ(s.read(binding.channel),before);
        s.begin(binding,two);
        expect_error(err::supersession_required,[&] { s.complete(binding,two); });
        EXPECT_EQ(s.read(binding.channel)->last_installed,one);
        EXPECT_EQ(s.complete(binding,two,one).revision,2);
    });
    EXPECT_EQ(called,0);
}

TEST_F(ReceiveInstallState, StaleRevisionEpochBaseSequenceAndChangedDigestRefuse) {
    setup(); const auto one=first(4); install(one);
    committed([&](auto&) {
        auto s=storage(); const auto before=s.read(binding.channel);
        auto changed=one; changed.receipt_digest="different";
        expect_error(err::stale,[&] { s.begin(binding,changed); });
        changed=one; changed.manifest_digest="same-QEC-different-M";
        expect_error(err::stale,[&] { s.apply_if_new(binding,changed,std::nullopt,[](auto&) { FAIL()<<"changed manifest ran effects"; }); });
        auto next=successor(one,5); next.expected_revision=0; next.base={};
        // A delta with no numeric base is structurally invalid before the CAS.
        expect_error(err::invalid_argument,[&] { s.begin(binding,next); });
        next.mode=receive_install_mode::full;
        // A well-formed full request can carry an old uninitialized revision.
        expect_error(err::stale,[&] { s.begin(binding,next); });
        next=successor(one,5); next.base.position=3;
        expect_error(err::stale,[&] { s.begin(binding,next); });
        next=successor(one,5); next.sequence=3;
        expect_error(err::stale,[&] { s.begin(binding,next); });
        auto other=binding; other.epoch="new-epoch";
        expect_error(err::binding_mismatch,[&] { s.begin(other,one); });
        expect_error(err::binding_mismatch,[&] { s.bind(other); });
        EXPECT_EQ(s.read(binding.channel),before);
    });
}

TEST_F(ReceiveInstallState, ActiveAttemptCannotBeSilentlyReplacedAndLastRetryRemainsExact) {
    setup(); const auto one=first(1),two=successor(one,2); install(one);
    committed([&](auto&) {
        auto s=storage(); s.begin(binding,two); const auto before=s.read(binding.channel);
        EXPECT_EQ(s.begin(binding,two).disposition,disposition::active);
        EXPECT_EQ(s.begin(binding,one).disposition,disposition::already_installed);
        auto alternate=two; alternate.sequence=3;
        expect_error(err::active_conflict,[&] { s.begin(binding,alternate); });
        alternate=two; alternate.content_digest="different";
        expect_error(err::stale,[&] { s.complete(binding,alternate,one); });
        EXPECT_EQ(s.read(binding.channel),before);
    });
}

TEST_F(ReceiveInstallState, AuthorityScopeAliasRefusedEvenAcrossSourceEpochAndSchema) {
    setup();
    committed([&](auto&) {
        auto s=storage(); auto alias=binding; alias.channel="alias"; alias.source="other";
        alias.epoch="other-epoch"; alias.schema="other-schema";
        const auto before=s.usage();
        expect_error(err::alias,[&] { s.bind(alias); });
        EXPECT_EQ(s.usage(),before); EXPECT_FALSE(s.read(alias.channel));
        alias.scope="different-scope"; s.bind(alias); EXPECT_EQ(s.usage().channels,2);
    });
}

TEST_F(ReceiveInstallState, IdentifiersAndDigestsRemainLengthExactIncludingNul) {
    binding.channel=std::string("c\0tail",6); binding.authority=std::string("a\0tail",6);
    binding.scope=std::string("s\0tail",6); setup();
    auto i=first(); i.request_digest=std::string("r\0tail",6); i.receipt_digest=std::string("p\0tail",6);
    i.content_digest=std::string("d\0tail",6); i.manifest_digest=std::string("m\0tail",6); install(i);
    EXPECT_EQ(snapshot().binding,binding); EXPECT_EQ(snapshot().last_installed,i);
    committed([&](auto&) {
        auto s=storage(); EXPECT_FALSE(s.read("c"));
        auto different=i; different.request_digest="r";
        expect_error(err::stale,[&] { s.begin(binding,different); });
        s.audit();
    });
}

TEST_F(ReceiveInstallState, ChannelAndEncodedBudgetsRetainAcceptedEvidenceAndRefuseWholeAdmission) {
    limits.channels=1; limits.encoded_bytes=180; setup(); const auto one=first(); install(one);
    committed([&](auto&) {
        auto s=storage(); const auto before=s.read(binding.channel); const auto usage=s.usage();
        auto next=successor(one,1);
        expect_error(err::capacity,[&] { s.begin(binding,next); });
        auto other=binding; other.channel="other"; other.scope="other";
        expect_error(err::capacity,[&] { s.bind(other); });
        EXPECT_EQ(s.read(binding.channel),before); EXPECT_EQ(s.usage(),usage);
        EXPECT_EQ(s.begin(binding,one).disposition,disposition::already_installed);
        s.audit();
    });
}

TEST_F(ReceiveInstallState, FailedAndIgnoredMetadataWritesRollBackEffectsAndCounterChanges) {
    setup(); const auto one=first(); install(one); const auto two=successor(one,1);
    for (const auto& table : {std::string("_lattice_install_channel"),std::string("_lattice_install_store")}) {
        for (const auto& action : {std::string("ABORT,'injected'"),std::string("IGNORE")}) {
            const auto result=recovery_writer_access::install(owner,[&](auto& db) {
                auto s=storage(); const auto before=s.read(binding.channel); const auto usage=s.usage();
                // Fail completion, after begin and model DML have succeeded.
                s.begin(binding,two);
                const auto active=s.read(binding.channel); const auto active_usage=s.usage();
                const std::string when=table=="_lattice_install_channel" ? "NEW.revision=2" : "NEW.bytes<OLD.bytes";
                db.execute("CREATE TEMP TRIGGER install_fault BEFORE UPDATE ON "+table+" WHEN "+when+
                    " BEGIN SELECT RAISE("+action+"); END");
                int effect_calls=0;
                EXPECT_THROW(s.apply_if_new(binding,two,one,[&](auto& writer) { ++effect_calls; insert(writer,"must-rollback"); }),std::exception);
                EXPECT_EQ(effect_calls,1);
                EXPECT_EQ(count(db,"must-rollback"),0); EXPECT_EQ(s.read(binding.channel),active); EXPECT_EQ(s.usage(),active_usage);
                db.execute("DROP TRIGGER install_fault");
                EXPECT_EQ(s.complete(binding,two,one).revision,2);
                // Roll this whole owned unit back, preserving the first result
                // for the next independently injected branch.
                EXPECT_NE(s.read(binding.channel),before); EXPECT_LE(s.usage().encoded_bytes,usage.encoded_bytes+128);
                throw std::runtime_error("test rollback injection unit");
            });
            EXPECT_EQ(result.state,outcome::rolled_back);
            EXPECT_NE(result.primary_error,nullptr);
            EXPECT_EQ(result.cleanup_error,nullptr);
            EXPECT_EQ(snapshot().last_installed,one);
        }
    }
}

TEST_F(ReceiveInstallState, IgnoredNewBindingAndCounterWritesLeaveNoOrphanOrUndercharge) {
    committed([&](auto& db) {
        auto s=storage(); s.initialize();
        for (const auto& table : {std::string("_lattice_install_channel"),std::string("_lattice_install_store")}) {
            const auto event=table=="_lattice_install_channel" ? "INSERT" : "UPDATE";
            db.execute("CREATE TEMP TRIGGER bind_fault BEFORE "+std::string(event)+" ON "+table+" BEGIN SELECT RAISE(IGNORE); END");
            EXPECT_THROW(s.bind(binding),std::exception);
            EXPECT_FALSE(s.read(binding.channel)); EXPECT_EQ(s.usage(),receive_install_usage{});
            db.execute("DROP TRIGGER bind_fault");
        }
        s.bind(binding); s.audit();
    });
}

TEST_F(ReceiveInstallState, AddressedCorruptionAndFullCounterDriftRefuseWithoutCopyingLargeBlobs) {
    setup(); install(first());
    committed([&](auto& db) {
        auto s=storage();
        db.execute("SAVEPOINT test_corruption");
        db.execute("UPDATE _lattice_install_channel SET last_install=zeroblob(1048576)");
        expect_error(err::corrupt_state,[&] { s.read(binding.channel); });
        db.execute("ROLLBACK TO test_corruption");
        db.execute("UPDATE _lattice_install_channel SET frontier=zeroblob(1048576)");
        expect_error(err::corrupt_state,[&] { s.read(binding.channel); });
        db.execute("ROLLBACK TO test_corruption");
        db.execute("UPDATE _lattice_install_store SET bytes=bytes+1");
        expect_error(err::corrupt_state,[&] { s.audit(); });
        db.execute("ROLLBACK TO test_corruption");
        db.execute("UPDATE _lattice_install_store SET channels=zeroblob(1048576)");
        expect_error(err::corrupt_state,[&] { s.usage(); });
        db.execute("ROLLBACK TO test_corruption"); db.execute("RELEASE test_corruption"); s.audit();
    });
}

TEST_F(ReceiveInstallState, PartialSchemaVersionLimitsAndRegressingHeadAreNotMigrated) {
    setup();
    committed([&](auto& db) {
        auto other=limits; ++other.channels;
        expect_error(err::limits_mismatch,[&] { receive_install_store(owner,other).initialize(); });
        db.execute("UPDATE _lattice_install_store SET version=2");
        expect_error(err::corrupt_state,[&] { storage().initialize(); });
        db.execute("UPDATE _lattice_install_store SET version=1");
        auto invalid=first(); invalid.head=-1;
        expect_error(err::invalid_argument,[&] { storage().begin(binding,invalid); });
        invalid=first(); invalid.mode=receive_install_mode::delta;
        expect_error(err::invalid_argument,[&] { storage().begin(binding,invalid); });
    });
    const auto one=first(10); install(one);
    committed([&](auto&) {
        expect_error(err::invalid_argument,[&] { storage().begin(binding,successor(one,9)); });
    });
    auto fresh=owner_at();
    EXPECT_EQ(recovery_writer_access::install(fresh,[&](auto& db) {
        db.execute("CREATE TABLE _lattice_install_store(inherited TEXT)");
        expect_error(err::corrupt_state,[&] { receive_install_store(fresh,limits).initialize(); });
    }).state,outcome::committed);
}

TEST_F(ReceiveInstallState, InvalidAndExhaustedInputsRefuseWithoutWrappingOrAllocatingRecords) {
    setup();
    committed([&](auto&) {
        auto s=storage(); const auto before=s.read(binding.channel); const auto usage=s.usage();
        auto bad=binding; bad.channel=std::string(static_cast<size_t>(limits.field_bytes)+1,'x');
        expect_error(err::invalid_argument,[&] { s.bind(bad); });
        auto i=first(); i.expected_revision=std::numeric_limits<int64_t>::max();
        expect_error(err::invalid_argument,[&] { s.begin(binding,i); });
        i=first(); i.sequence=std::numeric_limits<int64_t>::max();
        expect_error(err::stale,[&] { s.begin(binding,i); });
        i=first(); i.request_digest.clear();
        expect_error(err::invalid_argument,[&] { s.begin(binding,i); });
        i=first(); i.content_digest=std::string(static_cast<size_t>(limits.field_bytes)+1,'d');
        expect_error(err::invalid_argument,[&] { s.begin(binding,i); });
        EXPECT_EQ(s.read(binding.channel),before); EXPECT_EQ(s.usage(),usage);
    });
}

TEST_F(ReceiveInstallState, PointBeginCompleteAndReadHaveBoundedVmWorkWithManyOtherBindings) {
    limits.channels=2048; limits.encoded_bytes=1024*1024; setup();
    committed([&](auto& db) {
        auto s=storage();
        for (int n=0;n<1024;++n) {
            auto other=binding; other.channel="other-channel-"+std::to_string(n);
            other.scope="other-scope-"+std::to_string(n); s.bind(other);
        }
        s.audit();
        // A deterministic SQLite VM ceiling, not elapsed-time/performance proof.
        // Each separate positive operation must finish below its own allowance.
        struct budget {
            sqlite3* db; int steps=0;
            explicit budget(sqlite3* connection):db(connection) {
                sqlite3_progress_handler(db,1,[](void* context) {
                    return ++static_cast<budget*>(context)->steps>12000 ? 1 : 0;
                },this);
            }
            ~budget() { sqlite3_progress_handler(db,0,nullptr,nullptr); }
        };
        const auto i=first(8);
        { budget limit(db.handle()); EXPECT_EQ(s.begin(binding,i).disposition,disposition::active); EXPECT_LE(limit.steps,12000); }
        { budget limit(db.handle()); EXPECT_EQ(s.complete(binding,i).disposition,disposition::installed); EXPECT_LE(limit.steps,12000); }
        { budget limit(db.handle()); EXPECT_EQ(s.read(binding.channel)->revision,1); EXPECT_LE(limit.steps,12000); }
    });
}

TEST_F(ReceiveInstallState, RetainedOwnerOutlivesExternalReferenceAndObserverFailureDoesNotReplay) {
    setup(); auto retained=storage(); const auto i=first(3);
    auto token=owner->add_table_observer("TestPerson",[](const auto&) { throw std::runtime_error("observer tail"); });
    auto result=recovery_writer_access::install(owner,[&](auto&) {
        retained.apply_if_new(binding,i,std::nullopt,[](auto& db) { insert(db,"durable-before-observer"); });
    });
    EXPECT_EQ(result.state,outcome::committed); EXPECT_NE(result.postcommit_error,nullptr);
    owner->remove_table_observer("TestPerson",token);
    std::weak_ptr<lattice::lattice_db> weak=owner;
    auto actual=owner; owner.reset(); EXPECT_FALSE(weak.expired());
    int effects=0;
    auto retry_result=recovery_writer_access::install(actual,[&](auto&) {
        actual.reset(); // install frame and storage independently retain actual owner
        EXPECT_FALSE(weak.expired());
        EXPECT_EQ(retained.apply_if_new(binding,i,std::nullopt,[&](auto&) { ++effects; }).disposition,disposition::already_installed);
    });
    EXPECT_EQ(retry_result.state,outcome::committed); EXPECT_EQ(effects,0); EXPECT_FALSE(weak.expired());
}

TEST(ReceiveInstallStateFile, ReopenRetainsExactLastIdentityAndCommittedActiveRefresh) {
    TempDB file{"receive_install_reopen"};
    const receive_install_limits limits{2,128,4096};
    const receive_install_binding binding{"channel","authority","source","epoch","scope","schema"};
    const auto one=first(7),two=successor(one,7);
    {
        auto owner=owner_at(file.str()); receive_install_store s(owner,limits);
        EXPECT_EQ(recovery_writer_access::install(owner,[&](auto& db) {
            s.initialize(); s.bind(binding);
            s.apply_if_new(binding,one,std::nullopt,[](auto& writer) { insert(writer,"persisted"); });
            s.begin(binding,two); EXPECT_EQ(count(db,"persisted"),1);
        }).state,outcome::committed);
    }
    {
        auto owner=owner_at(file.str()); receive_install_store s(owner,limits);
        EXPECT_EQ(recovery_writer_access::install(owner,[&](auto& db) {
            s.initialize(); const auto before=s.read(binding.channel);
            ASSERT_TRUE(before); EXPECT_EQ(before->last_installed,one); EXPECT_EQ(before->active,two);
            EXPECT_EQ(s.begin(binding,one).disposition,disposition::already_installed);
            int effects=0;
            EXPECT_EQ(s.apply_if_new(binding,two,one,[&](auto&) { ++effects; }).revision,2);
            EXPECT_EQ(effects,1); EXPECT_EQ(count(db,"persisted"),1); s.audit();
        }).state,outcome::committed);
    }
}
