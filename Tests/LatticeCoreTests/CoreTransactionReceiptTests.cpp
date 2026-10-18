#include "TestHelpers.hpp"
#include <LatticeCore.hpp>
#include <lattice/core_transaction_receipt.hpp>
#include "../../Sources/LatticeCore/src/database_retirement.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include <cstdio>
#include <future>
#include <cerrno>
#include <new>
#include <thread>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

namespace lattice {
struct core_transaction_receipt_test_access {
    static detail::core_transaction_receipt begin(lattice_db& owner) { return owner.begin_transaction_receipt_(); }
    static detail::core_transaction_receipt begin_legacy(lattice_db& owner) { return owner.begin_transaction_receipt_(false); }
    static void commit(lattice_db& owner, const detail::core_transaction_receipt& value) { owner.commit_transaction_receipt_(value); }
    static void rollback(lattice_db& owner, const detail::core_transaction_receipt& value) { owner.rollback_transaction_receipt_(value); }
    static bool ended(const lattice_db& owner, const detail::core_transaction_receipt& value) noexcept { return owner.transaction_receipt_ended_(value); }
    static std::weak_ptr<database> writer(const lattice_db& owner) noexcept { return owner.db_; }
    static std::weak_ptr<const void> original(const lattice_db& owner, const detail::core_transaction_receipt& value) noexcept {
        const auto* fields = owner.receipt_fields_(value);
        return fields ? std::weak_ptr<const void>(fields->original) : std::weak_ptr<const void>{};
    }
    static auto ending(const lattice_db& owner, const detail::core_transaction_receipt& value) noexcept {
        const auto* fields = owner.receipt_fields_(value);
        return fields ? fields->ending : nullptr;
    }
};
}
struct ReceiptRow { int64_t value; };
LATTICE_SCHEMA(ReceiptRow, value);
namespace {
using Access = lattice::core_transaction_receipt_test_access;
using Receipt = lattice::detail::core_transaction_receipt;
class CoreTransactionReceipt : public ::testing::Test {
protected:
    lattice::lattice_db owner{lattice::configuration(":memory:")};
    void TearDown() override {
        if (owner.db().is_in_transaction()) owner.rollback();
        owner.close();
    }
    std::vector<int64_t> values() {
        std::vector<int64_t> out;
        for (const auto& row : owner.db().query("SELECT value FROM ReceiptRow ORDER BY id")) out.push_back(std::get<int64_t>(row.at("value")));
        return out;
    }
};
TEST_F(CoreTransactionReceipt, EmptyCannotSettleAndBeginMintsExactNonallocatingIdentity) {
    Receipt empty; EXPECT_FALSE(empty.valid()); EXPECT_FALSE(empty.same_original(empty));
    EXPECT_THROW(Access::commit(owner, empty), std::exception);
    auto first = Access::begin(owner); auto copy = first;
    static_assert(noexcept(first.valid())); static_assert(noexcept(first.same_original(copy)));
    EXPECT_TRUE(first.valid()); EXPECT_TRUE(first.same_original(copy)); EXPECT_FALSE(Access::ended(owner, first));
    Access::commit(owner, first); EXPECT_TRUE(first.valid()); EXPECT_TRUE(Access::ended(owner, copy));
    auto next = Access::begin(owner); EXPECT_FALSE(first.same_original(next)); Access::rollback(owner, next);
}
TEST_F(CoreTransactionReceipt, ForeignOwnerCannotBorrowOriginalReceipt) {
    auto first = Access::begin(owner); lattice::lattice_db other(lattice::configuration(":memory:"));
    EXPECT_THROW(Access::rollback(other, first), std::exception); EXPECT_FALSE(Access::ended(other, first));
    EXPECT_TRUE(owner.db().is_in_transaction()); Access::rollback(owner, first); other.close();
}
TEST_F(CoreTransactionReceipt, ReceiptCommitCannotRollbackCallbackCreatedSuccessor) {
    Receipt next; bool started = false;
    auto observer = owner.add_table_observer("ReceiptRow", [&](const auto&) {
        if (started) return; started = true;
        next = Access::begin(owner); owner.add(ReceiptRow{99});
        throw std::runtime_error("original receipt callback failure");
    });
    auto first = Access::begin(owner); owner.add(ReceiptRow{7});
    std::string error;
    try { Access::commit(owner, first); } catch (const std::exception& value) { error = value.what(); }
    EXPECT_EQ(error, "original receipt callback failure"); ASSERT_TRUE(started);
    ASSERT_TRUE(Access::ended(owner, first)); ASSERT_FALSE(Access::ended(owner, next));
    // Models the old Swift outer catch, now carrying its ORIGINAL receipt.
    Access::rollback(owner, first); EXPECT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7,99}));
    owner.remove_table_observer("ReceiptRow", observer); Access::commit(owner, next);
    EXPECT_EQ(values(), (std::vector<int64_t>{7,99}));
}
TEST_F(CoreTransactionReceipt, CommitOfAlreadyEndedOriginalRefusesWithoutTouchingSuccessor) {
    auto old = Access::begin(owner); owner.add(ReceiptRow{7}); Access::rollback(owner, old);
    auto next = Access::begin(owner); owner.add(ReceiptRow{99});
    std::string error;
    try { Access::commit(owner, old); } catch (const lattice::db_error& value) { error = value.what(); }
    EXPECT_EQ(error, "Core COMMIT receipt original already ended");
    EXPECT_TRUE(old.valid()); EXPECT_TRUE(Access::ended(owner, old));
    EXPECT_FALSE(Access::ended(owner, next)); EXPECT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{99}));
    EXPECT_NO_THROW(Access::rollback(owner, old));
    EXPECT_NO_THROW(Access::rollback(owner, old));
    EXPECT_TRUE(owner.db().is_in_transaction()); EXPECT_FALSE(Access::ended(owner, next));
    Access::commit(owner, next); EXPECT_EQ(values(), (std::vector<int64_t>{99}));
}
TEST_F(CoreTransactionReceipt, DeferredCommitFailureHandlerSuccessorSurvivesOuterOldRollback) {
    owner.db().execute("PRAGMA foreign_keys=ON");
    owner.db().execute("CREATE TABLE _receipt_fk_parent(id INTEGER PRIMARY KEY)");
    owner.db().execute("CREATE TABLE _receipt_fk_child(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES _receipt_fk_parent(id) DEFERRABLE INITIALLY DEFERRED)");
    auto old = Access::begin(owner); owner.add(ReceiptRow{7});
    owner.db().execute("INSERT INTO _receipt_fk_child(id,parent) VALUES(1,999)");
    std::string primary;
    try { Access::commit(owner, old); } catch (const std::exception& error) { primary = error.what(); }
    ASSERT_FALSE(primary.empty()); ASSERT_FALSE(Access::ended(owner, old));
    // The Bridge/Swift handler runs AFTER the failed native result was copied.
    Access::rollback(owner, old); auto next = Access::begin(owner); owner.add(ReceiptRow{99});
    Access::rollback(owner, old); // original outer catch carries old authority
    EXPECT_TRUE(owner.db().is_in_transaction()); EXPECT_FALSE(Access::ended(owner, next));
    EXPECT_EQ(values(), (std::vector<int64_t>{99})); Access::commit(owner, next);
    EXPECT_EQ(values(), (std::vector<int64_t>{99})); EXPECT_NE(primary.find("FOREIGN KEY"), std::string::npos);
}
TEST_F(CoreTransactionReceipt, OldReceiptDoesNotReleaseSuccessorNamedMemoryGate) {
    lattice::lattice_db shared(lattice::configuration("file:receipt_gate_control?mode=memory&cache=shared"));
    auto old = Access::begin(shared); Access::commit(shared, old);
    auto next = Access::begin(shared); Access::rollback(shared, old);
    auto gate = shared.store_write_gate(); ASSERT_TRUE(gate);
    std::promise<bool> probe; auto result = probe.get_future();
    std::thread reader([&] { const bool entered = gate->try_lock(); if (entered) gate->unlock(); probe.set_value(entered); });
    reader.join(); EXPECT_FALSE(result.get()); EXPECT_TRUE(shared.db().is_in_transaction());
    Access::rollback(shared, next); shared.close();
}
TEST_F(CoreTransactionReceipt, ForeignThreadCannotSettleOrClaimEnded) {
    auto first = Access::begin(owner);
    auto result = std::async(std::launch::async, [&] {
        const bool ended = Access::ended(owner, first); bool refused = false;
        try { Access::rollback(owner, first); } catch (...) { refused = true; }
        return refused && !ended;
    });
    EXPECT_EQ(result.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    Access::rollback(owner, first); EXPECT_TRUE(result.get());
}
TEST_F(CoreTransactionReceipt, RawEscapeCannotMintPrivateReceipt) {
    auto* actual = owner.db().handle(); ASSERT_NE(actual, nullptr);
    EXPECT_THROW(Access::begin(owner), std::exception); EXPECT_FALSE(owner.db().is_in_transaction());
    // Existing parameterless public contract remains available after raw access.
    owner.begin_transaction(); owner.add(ReceiptRow{7}); owner.commit(); EXPECT_EQ(values(), (std::vector<int64_t>{7}));
}
TEST_F(CoreTransactionReceipt, LegacyRawBeginTracksOriginalWithoutWeakeningCheckedSettlement) {
    ASSERT_NE(owner.db().handle(), nullptr);
    EXPECT_THROW(Access::begin(owner), lattice::db_error); EXPECT_FALSE(owner.db().is_in_transaction());
    auto legacy = Access::begin_legacy(owner); ASSERT_TRUE(legacy.valid());
    owner.add(ReceiptRow{7}); EXPECT_FALSE(Access::ended(owner, legacy));
    EXPECT_THROW(Access::commit(owner, legacy), lattice::db_error); // expected SQL still strictly refuses raw escape
    EXPECT_TRUE(owner.db().is_in_transaction()); EXPECT_FALSE(Access::ended(owner, legacy));
    owner.commit(); // original public parameterless contract owns this SQL settlement
    EXPECT_TRUE(Access::ended(owner, legacy)); EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_THROW(Access::begin(owner), lattice::db_error); EXPECT_FALSE(owner.db().is_in_transaction());
}
TEST_F(CoreTransactionReceipt, LegacyForeignCallerWaitsThenReceivesItsActualBeginOriginal) {
    lattice::lattice_db shared(lattice::configuration("file:receipt_legacy_wait?mode=memory&cache=shared"));
    shared.begin_transaction();
    std::promise<void> started; auto starting = started.get_future();
    std::promise<bool> completed; auto completion = completed.get_future();
    std::thread foreign([&] {
        started.set_value();
        try {
            auto legacy = Access::begin_legacy(shared);
            const bool issued = legacy.valid() && !Access::ended(shared, legacy) && shared.db().is_in_transaction();
            shared.rollback(); completed.set_value(issued && Access::ended(shared, legacy));
        } catch (...) { completed.set_value(false); }
    });
    EXPECT_EQ(starting.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    const auto while_parent_held = completion.wait_for(std::chrono::milliseconds(100));
    // Parent gate always ends before joining, including a failing baseline.
    shared.commit(); foreign.join();
    EXPECT_EQ(while_parent_held, std::future_status::timeout); EXPECT_TRUE(completion.get());
    EXPECT_FALSE(shared.db().is_in_transaction()); shared.close();
}
TEST_F(CoreTransactionReceipt, CheckedForeignCallerWaitsThenReceivesItsActualBeginOriginal) {
    lattice::lattice_db shared(lattice::configuration("file:receipt_checked_wait?mode=memory&cache=shared"));
    shared.begin_transaction();
    std::promise<void> started; auto starting = started.get_future();
    std::promise<bool> completed; auto completion = completed.get_future();
    std::thread foreign([&] {
        started.set_value();
        try {
            auto legacy = Access::begin(shared);
            const bool issued = legacy.valid() && !Access::ended(shared, legacy) && shared.db().is_in_transaction();
            shared.rollback(); completed.set_value(issued && Access::ended(shared, legacy));
        } catch (...) { completed.set_value(false); }
    });
    EXPECT_EQ(starting.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    const auto while_parent_held = completion.wait_for(std::chrono::milliseconds(100));
    // Parent gate always ends before joining, including a failing baseline.
    shared.commit(); foreign.join();
    EXPECT_EQ(while_parent_held, std::future_status::timeout); EXPECT_TRUE(completion.get());
    EXPECT_FALSE(shared.db().is_in_transaction()); shared.close();
}
TEST_F(CoreTransactionReceipt, PostCommitObserverRawEscapeCannotEraseProvedOldEnding) {
    bool observed = false;
    auto token = owner.add_table_observer("ReceiptRow", [&](const auto&) {
        observed = true; EXPECT_NE(owner.db().handle(), nullptr); // AFTER actual COMMIT
    });
    auto old = Access::begin(owner); owner.add(ReceiptRow{7}); Access::commit(owner, old);
    EXPECT_TRUE(observed); EXPECT_TRUE(old.valid()); EXPECT_TRUE(Access::ended(owner, old));
    owner.remove_table_observer("ReceiptRow", token);
    owner.db().execute("BEGIN IMMEDIATE"); // actual independent RAW successor, not old authority
    Access::rollback(owner, old); EXPECT_TRUE(owner.db().is_in_transaction());
    owner.db().execute("ROLLBACK"); EXPECT_EQ(values(), (std::vector<int64_t>{7}));
}
TEST_F(CoreTransactionReceipt, LiveRawEscapeStillRefusesExpectedSettlementBeforeSQL) {
    auto old = Access::begin(owner); owner.add(ReceiptRow{7}); ASSERT_NE(owner.db().handle(), nullptr);
    EXPECT_FALSE(Access::ended(owner, old)); EXPECT_THROW(Access::rollback(owner, old), std::exception);
    EXPECT_TRUE(owner.db().is_in_transaction()); owner.rollback(); EXPECT_TRUE(values().empty());
    EXPECT_TRUE(Access::ended(owner, old)); // genuine legacy rollback established it, no current relabeling
}
TEST_F(CoreTransactionReceipt, EndedTicketDoesNotAuthorizeAnotherOwnerAtReusedAddress) {
    alignas(lattice::lattice_db) unsigned char storage[sizeof(lattice::lattice_db)];
    auto* old = new (storage) lattice::lattice_db(lattice::configuration(":memory:"));
    auto receipt = Access::begin(*old); Access::commit(*old, receipt); ASSERT_TRUE(Access::ended(*old, receipt));
    old->~lattice_db(); auto* replacement = new (storage) lattice::lattice_db(lattice::configuration(":memory:"));
    EXPECT_FALSE(Access::ended(*replacement, receipt)); EXPECT_THROW(Access::rollback(*replacement, receipt), std::exception);
    replacement->~lattice_db();
}
TEST_F(CoreTransactionReceipt, LogicalNativeCloseAfterBeginStillSettlesThatActualOriginal) {
    auto receipt = Access::begin(owner); owner.add(ReceiptRow{7}); owner.db().close();
    EXPECT_NO_THROW(Access::commit(owner, receipt)); EXPECT_TRUE(Access::ended(owner, receipt));
}
#if defined(__APPLE__) || defined(__linux__)
TEST_F(CoreTransactionReceipt, InheritedReceiptCopyAndSettlementAbandonNativePayload) {
    auto receipt = Access::begin(owner);
    const pid_t child = fork();
    if (!child) {
        struct sigaction action{}; action.sa_handler=SIG_DFL; sigemptyset(&action.sa_mask); sigaction(SIGALRM,&action,nullptr);
        sigset_t set; sigemptyset(&set); sigaddset(&set,SIGALRM); sigprocmask(SIG_UNBLOCK,&set,nullptr); alarm(5);
        Receipt inherited(receipt); bool refused=false;
        try { Access::rollback(owner, receipt); } catch (...) { refused=true; }
        const bool safe = !receipt.valid() && !inherited.valid() && !Access::ended(owner, receipt) && refused;
        receipt = {}; inherited = {}; // real foreign shells end without control-block destruction
        _exit(safe ? 0 : 8);
    }
    ASSERT_GT(child, 0); // parent only; no test framework call in child
    int status=0; pid_t observed=0; const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(7);
    while (std::chrono::steady_clock::now()<end) {
        observed=waitpid(child,&status,WNOHANG);
        if(observed==child || (observed<0&&errno!=EINTR))break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if(observed!=child) {
        (void)kill(child,SIGKILL); const auto cleanup=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(std::chrono::steady_clock::now()<cleanup) {
            observed=waitpid(child,&status,WNOHANG); if(observed==child || (observed<0&&errno!=EINTR))break;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    if (observed != child) {
        static constexpr char debt[] = "core-receipt child ending unproved; Native namespace retained\n";
        (void)::write(STDERR_FILENO, debt, sizeof(debt)-1); _exit(94);
    }
    ASSERT_TRUE(WIFEXITED(status)); EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_TRUE(receipt.valid()); EXPECT_FALSE(Access::ended(owner,receipt)); Access::rollback(owner,receipt);
}
#endif
}


namespace {
// Only SQLite owns this function registration after SQLITE_OK. The name is
// never invoked/replaced/removed by this fixture. Its xDestroy observes actual
// native teardown, not Swift/Native wrapper expiry or an assumed close status.
// The observer contains only atomic DATA and cannot keep any Native owner alive.
int observe_receipt_physical_close(lattice::lattice_db& owner,
    const std::shared_ptr<std::atomic<size_t>>& observed) {
    auto* native=lattice::detail::canonical_writer_custody_test_access::fault_handle(owner.db());
    if(!native)return SQLITE_MISUSE;
    auto* data=new std::shared_ptr<std::atomic<size_t>>(observed);
    return sqlite3_create_function_v2(native,"_receipt_retirement_observer",0,SQLITE_UTF8,data,
        +[](sqlite3_context* context,int,sqlite3_value**) noexcept { sqlite3_result_null(context); },
        nullptr,nullptr,+[](void* context) noexcept {
            auto* data=static_cast<std::shared_ptr<std::atomic<size_t>>*>(context);
            (*data)->fetch_add(1,std::memory_order_release); delete data;
        });
}
TEST_F(CoreTransactionReceipt, HeldReceiptDoesNotRetainActualPhysicalWriterAfterOwnerDrop) {
    auto actual = std::make_unique<lattice::lattice_db>(lattice::configuration(":memory:"));
    auto closed=std::make_shared<std::atomic<size_t>>(0);
    ASSERT_EQ(observe_receipt_physical_close(*actual,closed),SQLITE_OK);
    auto held = Access::begin(*actual);
    ASSERT_TRUE(actual->db().is_in_transaction());
    auto writer = Access::writer(*actual);
    auto original = Access::original(*actual, held);
    auto ending = Access::ending(*actual, held); // pure atomic DATA, no Native holder
    std::weak_ptr<const lattice::detail::database_retirement_state> retirement=
        lattice::detail::database_retirement_access::retain(actual->db());
    ASSERT_TRUE(ending); ASSERT_FALSE(ending->ended.load(std::memory_order_acquire));
    ASSERT_EQ(closed->load(std::memory_order_acquire),0u);
    actual.reset(); // no explicit COMMIT/ROLLBACK or close, and held stays alive
    EXPECT_TRUE(writer.expired()); EXPECT_TRUE(original.expired()); EXPECT_TRUE(retirement.expired());
    EXPECT_EQ(closed->load(std::memory_order_acquire),1u); // real SQLite xDestroy
    // Physical wrapper retirement is not a forged Core transaction ending.
    EXPECT_FALSE(ending->ended.load(std::memory_order_acquire)); EXPECT_TRUE(held.valid());
}
TEST_F(CoreTransactionReceipt, LogicalOwnerCloseThenDropStillPhysicallyClosesWithHeldReceipt) {
    auto actual = std::make_unique<lattice::lattice_db>(lattice::configuration(":memory:"));
    auto closed=std::make_shared<std::atomic<size_t>>(0);
    ASSERT_EQ(observe_receipt_physical_close(*actual,closed),SQLITE_OK);
    auto held = Access::begin(*actual); ASSERT_TRUE(actual->db().is_in_transaction());
    auto writer = Access::writer(*actual);
    std::weak_ptr<const lattice::detail::database_retirement_state> retirement=
        lattice::detail::database_retirement_access::retain(actual->db());
    actual->close_checked();
    EXPECT_EQ(closed->load(std::memory_order_acquire),0u);
    actual.reset();
    EXPECT_TRUE(writer.expired()); EXPECT_TRUE(held.valid()); EXPECT_TRUE(retirement.expired());
    EXPECT_EQ(closed->load(std::memory_order_acquire),1u);
}
TEST_F(CoreTransactionReceipt, RetainedReceiptsDoNotAccumulatePhysicalWritersAcrossDistinctOwners) {
    std::vector<Receipt> held;
    std::vector<std::weak_ptr<lattice::database>> writers;
    std::vector<std::weak_ptr<const lattice::detail::database_retirement_state>> retirements;
    std::vector<std::shared_ptr<std::atomic<size_t>>> closed;
    for (int index=0; index<8; ++index) {
        auto actual = std::make_unique<lattice::lattice_db>(lattice::configuration(":memory:"));
        auto observed=std::make_shared<std::atomic<size_t>>(0);
        ASSERT_EQ(observe_receipt_physical_close(*actual,observed),SQLITE_OK);
        held.push_back(Access::begin(*actual)); ASSERT_TRUE(actual->db().is_in_transaction());
        writers.push_back(Access::writer(*actual)); closed.push_back(observed);
        retirements.push_back(lattice::detail::database_retirement_access::retain(actual->db()));
        actual.reset();
    }
    ASSERT_EQ(held.size(),8u);
    for (size_t index=0;index<held.size();++index) {
        EXPECT_TRUE(held[index].valid()); EXPECT_TRUE(writers[index].expired()); EXPECT_TRUE(retirements[index].expired());
        EXPECT_EQ(closed[index]->load(std::memory_order_acquire),1u);
    }
}
TEST_F(CoreTransactionReceipt, RemainingActualOwnerAliasRetainsLiveSettlementWithoutReceiptOwnership) {
    auto first = std::make_shared<lattice::lattice_db>(lattice::configuration(":memory:"));
    auto alias = first;
    auto closed=std::make_shared<std::atomic<size_t>>(0);
    ASSERT_EQ(observe_receipt_physical_close(*first,closed),SQLITE_OK);
    auto held = Access::begin(*first); first->add(ReceiptRow{77});
    auto writer = Access::writer(*first);
    first.reset(); // a facade disappears; the genuine original owner remains
    EXPECT_FALSE(writer.expired()); EXPECT_FALSE(Access::ended(*alias, held));
    EXPECT_NO_THROW(Access::commit(*alias, held)); EXPECT_TRUE(Access::ended(*alias, held));
    EXPECT_EQ(std::get<int64_t>(alias->db().query("SELECT value FROM ReceiptRow").at(0).at("value")),77);
    std::weak_ptr<const lattice::detail::database_retirement_state> retirement=
        lattice::detail::database_retirement_access::retain(alias->db());
    EXPECT_EQ(closed->load(std::memory_order_acquire),0u);
    alias.reset();
    EXPECT_TRUE(writer.expired()); EXPECT_TRUE(retirement.expired());
    EXPECT_EQ(closed->load(std::memory_order_acquire),1u);
}
TEST_F(CoreTransactionReceipt, ExactEndingDataSurvivesOriginalReclamationAndCannotSettleSuccessor) {
    auto old = Access::begin(owner); owner.add(ReceiptRow{7});
    auto original = Access::original(owner, old); auto ending = Access::ending(owner, old);
    Access::commit(owner, old);
    EXPECT_TRUE(original.expired()); ASSERT_TRUE(ending);
    EXPECT_TRUE(ending->ended.load(std::memory_order_acquire)); EXPECT_TRUE(Access::ended(owner,old));
    auto next=Access::begin(owner); owner.add(ReceiptRow{99});
    EXPECT_NO_THROW(Access::rollback(owner,old)); EXPECT_THROW(Access::commit(owner,old),lattice::db_error);
    EXPECT_TRUE(owner.db().is_in_transaction()); EXPECT_FALSE(Access::ended(owner,next));
    EXPECT_EQ(values(),(std::vector<int64_t>{7,99})); Access::rollback(owner,next);
    EXPECT_EQ(values(),(std::vector<int64_t>{7}));
}
#if defined(__APPLE__) || defined(__linux__)
TEST_F(CoreTransactionReceipt, ForkReturningDetachedCallbackAbandonsTemporaryNativeSettlementPins) {
    bool inherited=false; pid_t child=-1;
    std::chrono::steady_clock::time_point child_work_end{};
    auto observer=owner.add_table_observer("ReceiptRow",[&](const auto&) {
        child_work_end=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        const pid_t actual=fork();
        if(actual==0) { inherited=true; return; }
        child=actual;
    });
    auto receipt=Access::begin(owner); owner.add(ReceiptRow{7});
    try { Access::commit(owner,receipt); }
    catch(...) { if(inherited) _exit(receipt.valid()?8:0); throw; }
    if(inherited) _exit(9); // the creator check must refuse after callback returns
    owner.remove_table_observer("ReceiptRow",observer);
    ASSERT_GT(child,0);
    int status=0;pid_t observed=0;
    int last_wait_error=0,kill_result=-999,kill_error=0;
    size_t wait_attempts=0,wait_errors=0,recorded_errors=0;
    int errors[8]{};
    bool wait_refused=false,kill_attempted=false;
    const pid_t actual_parent=::getpid(),actual_group=::getpgrp();
    const auto work_end=child_work_end;
    const auto ending_end=work_end+std::chrono::seconds(1); // fixed once, never renewed
    const auto poll=[&] {
        errno=0; observed=waitpid(child,&status,WNOHANG); ++wait_attempts;
        last_wait_error=observed<0?errno:0;
        if(observed<0) {
            ++wait_errors;
            if(recorded_errors<8)errors[recorded_errors++]=last_wait_error;
            if(last_wait_error!=EINTR)wait_refused=true;
        }
    };
    while(std::chrono::steady_clock::now()<work_end) {
        poll();
        if(observed==child||wait_refused)break;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    const bool work_timed_out=std::chrono::steady_clock::now()>=work_end;
    if(observed!=child&&!wait_refused) {
        kill_attempted=true;errno=0;kill_result=kill(child,SIGKILL);kill_error=kill_result<0?errno:0;
        while(std::chrono::steady_clock::now()<ending_end) {
            poll();if(observed==child||wait_refused)break;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }
    char facts[1024];
    const int length=std::snprintf(facts,sizeof(facts),
        "RECEIPT_CALLBACK_FORK_ENDING child=%ld parent=%ld pgid=%ld store=:memory: "
        "workTimedOut=%d waitRefused=%d waitAttempts=%zu lastWait=%ld lastErrno=%d "
        "killAttempted=%d killResult=%d killErrno=%d status=%d waitErrors=%zu recordedErrors=%zu "
        "errors=%d,%d,%d,%d,%d,%d,%d,%d\n",
        static_cast<long>(child),static_cast<long>(actual_parent),static_cast<long>(actual_group),
        work_timed_out,wait_refused,wait_attempts,static_cast<long>(observed),last_wait_error,
        kill_attempted,kill_result,kill_error,status,wait_errors,recorded_errors,
        errors[0],errors[1],errors[2],errors[3],errors[4],errors[5],errors[6],errors[7]);
    if(length>0)(void)::write(STDERR_FILENO,facts,std::min(static_cast<size_t>(length),sizeof(facts)-1));
    if(observed!=child) {
        static constexpr char debt[]="receipt callback child ending unproved; exact child/group facts above; test-process artifacts retained\n";
        (void)::write(STDERR_FILENO,debt,sizeof(debt)-1);_exit(94);
    }
    // Cleanup/reap cannot turn an expired work deadline into success, including
    // a natural exit0 racing the signal after the original work deadline.
    EXPECT_FALSE(work_timed_out); EXPECT_FALSE(wait_refused); EXPECT_FALSE(kill_attempted);
    ASSERT_TRUE(WIFEXITED(status)); EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_TRUE(Access::ended(owner,receipt)); EXPECT_EQ(values(),(std::vector<int64_t>{7}));
}
#endif
}
