#include "TestHelpers.hpp"
#include <sys/socket.h>
#include <unistd.h>

// ============================================================================
// Instance Lifecycle Tests — registry, close/reopen, memory modes
// ============================================================================

TEST(Lifecycle, InMemoryIsolation) {
    lattice::lattice_db db1;
    lattice::lattice_db db2;

    db1.add(TestPerson{"OnlyInDb1", 25, std::nullopt});
    db2.add(TestDog{"OnlyInDb2", 10.0, true});

    // Each sees only its own data
    EXPECT_EQ(db1.objects<TestPerson>().size(), 1u);
    EXPECT_EQ(db1.objects<TestDog>().size(), 0u);
    EXPECT_EQ(db2.objects<TestPerson>().size(), 0u);
    EXPECT_EQ(db2.objects<TestDog>().size(), 1u);
}

TEST(Lifecycle, CloseReopenReadDb) {
    TempDB tmp{"closereopen"};

    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.add(TestPerson{"Alice", 30, std::nullopt});
        db.add(TestDog{"Max", 20.0, true});
    }

    // Reopen and verify
    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        EXPECT_EQ(db.objects<TestPerson>().size(), 1u);
        EXPECT_EQ(db.objects<TestDog>().size(), 1u);

        auto person = db.objects<TestPerson>();
        EXPECT_EQ(std::string(person[0].name), "Alice");
    }
}

TEST(Lifecycle, MultipleReopens) {
    TempDB tmp{"multireopen"};

    for (int i = 0; i < 5; i++) {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.add(TestPerson{"Person" + std::to_string(i), i * 10, std::nullopt});
    }

    lattice::lattice_db db(lattice::configuration(tmp.str()));
    EXPECT_EQ(db.objects<TestPerson>().size(), 5u);
}

TEST(Lifecycle, ConcurrentReads) {
    TempDB tmp{"concurrent"};

    // Seed data
    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.write([&]() {
            for (int i = 0; i < 10; i++) {
                db.add(TestPerson{"P" + std::to_string(i), i, std::nullopt});
            }
        });
    }

    // Multiple instances reading concurrently
    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&] {
            try {
                lattice::lattice_db db(lattice::configuration(tmp.str()));
                auto count = db.objects<TestPerson>().size();
                if (count == 10) success_count++;
            } catch (...) {
                // Database busy is expected under contention
            }
        });
    }

    for (auto& t : threads) t.join();
    // At least one reader should succeed
    EXPECT_GE(success_count.load(), 1);
}

TEST(Lifecycle, DestructorCleansUp) {
    TempDB tmp{"cleanup"};

    {
        lattice::lattice_db db(lattice::configuration(tmp.str()));
        db.add(TestPerson{"Temp", 25, std::nullopt});
        // db goes out of scope — destructor runs
    }

    // Should still be able to reopen
    lattice::lattice_db db(lattice::configuration(tmp.str()));
    EXPECT_EQ(db.objects<TestPerson>().size(), 1u);
}

TEST(Lifecycle, EmptyDatabase) {
    lattice::lattice_db db;

    // All queries on empty DB should return 0
    EXPECT_EQ(db.objects<TestPerson>().size(), 0u);
    EXPECT_EQ(db.objects<TestDog>().size(), 0u);
    EXPECT_EQ(db.objects<TestTrip>().size(), 0u);

    EXPECT_FALSE(db.find<TestPerson>(1).has_value());
    EXPECT_FALSE(db.find_by_global_id<TestPerson>("nonexistent").has_value());
}

// IPC socket path resolution
TEST(Lifecycle, IPCSocketPathResolve) {
    auto path = lattice::resolve_ipc_socket_path("test-channel");
    EXPECT_FALSE(path.empty());
    EXPECT_NE(path.find("test-channel"), std::string::npos);
#ifdef __APPLE__
    EXPECT_NE(path.find("Lattice/ipc"), std::string::npos);
#endif
}

// IPC length-prefix framing
TEST(Lifecycle, LengthPrefixFraming) {
    // Create a socketpair for testing
    int fds[2];
    int ret = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    ASSERT_EQ(ret, 0);

    // Test single byte
    std::vector<uint8_t> data = {0x42};
    ASSERT_TRUE(lattice::write_length_prefixed(fds[0], data.data(), data.size()));
    auto received = lattice::read_length_prefixed(fds[1]);
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0], 0x42);

    // Test multi-byte message
    std::string msg = "Hello, length-prefix!";
    ASSERT_TRUE(lattice::write_length_prefixed(
        fds[0], msg.data(), static_cast<uint32_t>(msg.size())));
    received = lattice::read_length_prefixed(fds[1]);
    ASSERT_EQ(received.size(), msg.size());
    EXPECT_EQ(std::string(received.begin(), received.end()), msg);

    // Test large message (1MB) — needs threads to avoid socketpair deadlock
    std::vector<uint8_t> large(1024 * 1024, 0xAB);
    std::vector<uint8_t> large_received;
    std::thread writer([&] {
        lattice::write_length_prefixed(
            fds[0], large.data(), static_cast<uint32_t>(large.size()));
    });
    large_received = lattice::read_length_prefixed(fds[1]);
    writer.join();
    EXPECT_EQ(large_received.size(), large.size());

    close(fds[0]);
    close(fds[1]);
}

#ifndef __EMSCRIPTEN__
#include "../../Sources/LatticeCore/src/configured_recovery_connection.hpp"
#include <sys/wait.h>
#include <signal.h>

namespace {
class default_listener_owner : public lattice::lattice_db {
public:
    using lattice::lattice_db::lattice_db;
    auto listener() { return configured_listener_cell(); }
    bool has_controller() const noexcept { return bool(configured_connection()); }
    bool listener_leaf_available() {
        require_creator_process();
        if (!managed_wss_mutex_.try_lock()) return false;
        managed_wss_mutex_.unlock();
        return true;
    }
};
struct listener_release_probe {
    default_listener_owner* owner;
    int* destroyed;
    bool* released_off_leaf;
    ~listener_release_probe() {
        ++*destroyed;
        *released_off_leaf = owner->listener_leaf_available();
    }
};
}

// Genuine default native owner/listener storage only: no managed admission,
// transport, source cut, profile or positive cleanup receipt is constructed.
TEST(Lifecycle, ConfiguredListenerUsesDefaultOwnerWithoutController) {
    default_listener_owner owner;
    int state_calls = 0, error_calls = 0;
    owner.set_on_sync_state_change([&](bool connected) { if (connected) ++state_calls; });
    owner.set_on_sync_error([&](const std::string& error) { if (error == "actual-listener") ++error_calls; });
    auto cell = owner.listener();
    ASSERT_TRUE(cell);
    EXPECT_EQ(cell, owner.listener());
    EXPECT_FALSE(owner.has_controller());
    auto state = cell->state.load(std::memory_order_acquire);
    auto error = cell->error.load(std::memory_order_acquire);
    ASSERT_TRUE(state && *state);
    ASSERT_TRUE(error && *error);
    (*state)(true);
    (*error)("actual-listener");
    EXPECT_EQ(state_calls, 1);
    EXPECT_EQ(error_calls, 1);
    owner.set_on_sync_state_change({});
    owner.set_on_sync_error({});
    EXPECT_EQ(cell, owner.listener());
    EXPECT_FALSE(*cell->state.load(std::memory_order_acquire));
    EXPECT_FALSE(*cell->error.load(std::memory_order_acquire));
    EXPECT_FALSE(owner.has_controller());
}

TEST(Lifecycle, ConfiguredProgressListenerUsesActualSiblingRegistry) {
    TempDB tmp{"configured-listener-siblings"};
    default_listener_owner first{lattice::configuration(tmp.str())};
    default_listener_owner second{lattice::configuration(tmp.str())};
    int calls = 0;
    first.set_on_sync_progress([&](const lattice::synchronizer::sync_progress&) { ++calls; });
    auto first_cell = first.listener(), second_cell = second.listener();
    ASSERT_TRUE(first_cell && second_cell);
    EXPECT_NE(first_cell, second_cell);
    auto first_handler = first_cell->progress.load(std::memory_order_acquire);
    auto second_handler = second_cell->progress.load(std::memory_order_acquire);
    ASSERT_TRUE(first_handler && *first_handler);
    ASSERT_TRUE(second_handler && *second_handler);
    (*first_handler)({});
    (*second_handler)({});
    EXPECT_EQ(calls, 2);
    first.set_on_sync_progress({});
    EXPECT_FALSE(*first_cell->progress.load(std::memory_order_acquire));
    EXPECT_FALSE(*second_cell->progress.load(std::memory_order_acquire));
    EXPECT_FALSE(first.has_controller());
    EXPECT_FALSE(second.has_controller());
}

TEST(Lifecycle, ConfiguredListenerForkRefusalAndOffLeafRelease) {
    auto* owner = new default_listener_owner;
    int destroyed = 0;
    bool released_off_leaf = false;
    auto capture = std::shared_ptr<listener_release_probe>(
        new listener_release_probe{owner, &destroyed, &released_off_leaf});
    owner->set_on_sync_state_change([capture](bool) {});
    capture.reset();
    ASSERT_EQ(destroyed, 0);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        unsigned refused = 0;
        try { owner->set_on_sync_state_change({}); } catch (const std::runtime_error&) { ++refused; }
        try { owner->set_on_sync_error({}); } catch (const std::runtime_error&) { ++refused; }
        try { owner->set_on_sync_progress({}); } catch (const std::runtime_error&) { ++refused; }
        try { (void)owner->listener(); } catch (const std::runtime_error&) { ++refused; }
        const bool no_controller = !owner->has_controller();
        delete owner; // Actual foreign-PID holder abandons its untouched payload.
        ::_exit(refused == 4 && no_controller && destroyed == 0 ? 0 : 91);
    }
    int status = 0;
    pid_t terminal = 0;
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((terminal = ::waitpid(child, &status, WNOHANG)) == 0 &&
           std::chrono::steady_clock::now() < end) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (terminal == 0) { ::kill(child, SIGKILL); terminal = ::waitpid(child, &status, 0); }
    EXPECT_EQ(terminal, child);
    EXPECT_TRUE(WIFEXITED(status));
    if (WIFEXITED(status)) EXPECT_EQ(WEXITSTATUS(status), 0);
    EXPECT_EQ(destroyed, 0);
    owner->set_on_sync_state_change({});
    EXPECT_EQ(destroyed, 1);
    EXPECT_TRUE(released_off_leaf);
    EXPECT_FALSE(owner->has_controller());
    delete owner;
}
#endif

#ifndef __EMSCRIPTEN__
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace lattice::detail {
struct configured_listener_retirement_test_access {
    static bool publication_leaf_available(configured_managed_handlers& cell) {
        if(!cell.publication_mutex_.try_lock())return false;
        cell.publication_mutex_.unlock();return true;
    }
};
}
namespace {
struct application_retirement_probe {
    default_listener_owner* owner;
    lattice::detail::configured_managed_handlers* cell;
    std::atomic<int>* destroyed;
    std::atomic<bool>* payload_and_leaves_available;
    std::atomic<bool>* nested_pending=nullptr;
    ~application_retirement_probe() {
        bool intact=false;
        try {
            intact=!owner->config().path.empty()&&owner->listener_leaf_available()&&
                lattice::detail::configured_listener_retirement_test_access::publication_leaf_available(*cell);
            if(nested_pending){
                const auto report=owner->close_checked();
                nested_pending->store(!report.cleanup_complete,std::memory_order_release);
            }
        }catch(...){intact=false;}
        payload_and_leaves_available->store(intact,std::memory_order_release);
        destroyed->fetch_add(1,std::memory_order_acq_rel);
    }
};
struct actual_publication_gate {
    std::mutex mutex;
    std::condition_variable changed;
    bool entered=false,released=false;
    void pause() {
        std::unique_lock<std::mutex> lock(mutex);entered=true;changed.notify_all();
        if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return released;}))
            throw lattice::db_error("actual publication fixture gate expired");
    }
    bool await_entry() {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock,std::chrono::seconds(5),[&]{return entered;});
    }
    void release() {std::lock_guard<std::mutex> lock(mutex);released=true;changed.notify_all();}
};
struct actual_publication_scope {
    std::function<void()> old;
    explicit actual_publication_scope(actual_publication_gate& gate)
        :old(std::move(lattice::detail::configured_recovery_test_hooks::before_listener_publication)) {
        lattice::detail::configured_recovery_test_hooks::before_listener_publication=[&gate]{gate.pause();};
    }
    ~actual_publication_scope(){lattice::detail::configured_recovery_test_hooks::before_listener_publication=std::move(old);}
};
}

// These cases exercise actual native default app/registry/callback storage.
// They create no managed origin, admission, transport, cut or cleanup receipt.
TEST(Lifecycle, ApplicationCloseDisposesAttachedListenerTargetsWithPayloadAlive) {
    default_listener_owner owner;
    auto cell=owner.listener();
    std::atomic<int> destroyed{0};std::atomic<bool> intact{false};
    auto target=std::shared_ptr<application_retirement_probe>(
        new application_retirement_probe{&owner,cell.get(),&destroyed,&intact});
    owner.set_on_sync_state_change([target](bool){});
    owner.set_on_sync_error([target](const std::string&){});
    owner.set_on_sync_progress([target](const lattice::synchronizer::sync_progress&){});
    target.reset();
    EXPECT_EQ(destroyed.load(),0);
    const auto report=owner.close_checked();
    EXPECT_TRUE(report.cleanup_complete);
    EXPECT_EQ(destroyed.load(),1);
    EXPECT_TRUE(intact.load());
    EXPECT_TRUE(cell->application_retiring());
    EXPECT_TRUE(cell->retirement_disposed());
    EXPECT_FALSE(cell->state.load());EXPECT_FALSE(cell->error.load());EXPECT_FALSE(cell->progress.load());
    EXPECT_THROW(owner.listener(),lattice::db_error);
    EXPECT_THROW(owner.set_on_sync_state_change({}),lattice::db_error);
    EXPECT_THROW(owner.set_on_sync_error({}),lattice::db_error);
    EXPECT_THROW(owner.set_on_sync_progress({}),lattice::db_error);
}

TEST(Lifecycle, ApplicationDestructorDisposesAttachedCopyBeforePayloadMembersDie) {
    std::atomic<int> destroyed{0};std::atomic<bool> intact{false};
    std::shared_ptr<lattice::detail::configured_managed_handlers> retained_cell;
    {
        default_listener_owner owner;
        retained_cell=owner.listener();
        auto target=std::shared_ptr<application_retirement_probe>(
            new application_retirement_probe{&owner,retained_cell.get(),&destroyed,&intact});
        owner.set_on_sync_state_change([target](bool){});target.reset();
        EXPECT_EQ(destroyed.load(),0);
    }
    EXPECT_EQ(destroyed.load(),1);EXPECT_TRUE(intact.load());
    EXPECT_TRUE(retained_cell->retirement_disposed());EXPECT_FALSE(retained_cell->state.load());
}

TEST(Lifecycle, OrdinaryDisconnectKeepsAppListenerIdentityAndRegistration) {
    default_listener_owner owner;
    int calls=0;
    owner.set_on_sync_state_change([&](bool value){if(value)++calls;});
    const auto cell=owner.listener();const auto installed=cell->state.load();
    owner.disconnect_sync(); // Actual default facade stop, no managed issuer.
    EXPECT_EQ(cell,owner.listener());EXPECT_EQ(installed,cell->state.load());
    EXPECT_FALSE(cell->application_retiring());
    ASSERT_TRUE(installed&&*installed);(*installed)(true);EXPECT_EQ(calls,1);
    owner.set_on_sync_state_change([&](bool value){if(value)calls+=2;});
    EXPECT_EQ(cell,owner.listener());EXPECT_NE(installed,cell->state.load());
}

TEST(Lifecycle, AppCloseRejectsActualPausedCellCreationBeforePublication) {
    default_listener_owner owner;
    actual_publication_gate gate;std::atomic<bool> retired_refusal{false};
    std::thread worker([&]{
        actual_publication_scope scoped(gate);
        try{owner.set_on_sync_state_change([](bool){});}
        catch(const lattice::db_error& error){retired_refusal.store(std::string(error.what())=="application listener registration is retired");}
        catch(...){}
    });
    const bool entered=gate.await_entry();
    EXPECT_TRUE(entered);
    const auto report=owner.close_checked();
    gate.release();worker.join(); // Mandatory actual cleanup even on fixture failure.
    EXPECT_TRUE(report.cleanup_complete);EXPECT_TRUE(retired_refusal.load());
    EXPECT_THROW(owner.listener(),lattice::db_error);
}

TEST(Lifecycle, AppCloseRejectsActualPausedExistingCellPublisher) {
    default_listener_owner owner;const auto cell=owner.listener();
    actual_publication_gate gate;std::atomic<bool> completed{false};
    std::thread worker([&]{
        actual_publication_scope scoped(gate);
        try{owner.set_on_sync_error([](const std::string&){});completed.store(true);}
        catch(...){}
    });
    const bool entered=gate.await_entry();EXPECT_TRUE(entered);
    const auto report=owner.close_checked();
    gate.release();worker.join();
    EXPECT_TRUE(report.cleanup_complete);EXPECT_TRUE(completed.load());
    EXPECT_TRUE(cell->retirement_disposed());EXPECT_FALSE(cell->error.load());
}

TEST(Lifecycle, ReentrantAttachedTargetDisposalCannotReportItselfComplete) {
    default_listener_owner owner;const auto cell=owner.listener();
    std::atomic<int> destroyed{0};std::atomic<bool> intact{false},nested_pending{false};
    auto target=std::shared_ptr<application_retirement_probe>(
        new application_retirement_probe{&owner,cell.get(),&destroyed,&intact,&nested_pending});
    owner.set_on_sync_state_change([target](bool){});target.reset();
    const auto report=owner.close_checked();
    EXPECT_TRUE(report.cleanup_complete);EXPECT_EQ(destroyed.load(),1);
    EXPECT_TRUE(intact.load());EXPECT_TRUE(nested_pending.load());
    EXPECT_TRUE(cell->retirement_disposed());
}

TEST(Lifecycle, RetainedExternalListenerCopyRemainsDistinctAfterAppRetirement) {
    default_listener_owner owner;const auto cell=owner.listener();
    std::atomic<int> destroyed{0};std::atomic<bool> intact{false};
    auto target=std::shared_ptr<application_retirement_probe>(
        new application_retirement_probe{&owner,cell.get(),&destroyed,&intact});
    owner.set_on_sync_state_change([target](bool){});target.reset();
    auto external=cell->state.load();
    const auto report=owner.close_checked();
    EXPECT_TRUE(report.cleanup_complete);EXPECT_TRUE(cell->retirement_disposed());
    EXPECT_EQ(destroyed.load(),0); // Actual external callable owner remains.
    external.reset();EXPECT_EQ(destroyed.load(),1);EXPECT_TRUE(intact.load());
}
#endif


#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <sys/wait.h>
#include <cerrno>
#include <signal.h>
#include <fcntl.h>
namespace lattice::detail {
struct configured_listener_retirement_fork_test_access {
    static std::mutex& leaf(configured_managed_handlers& cell){return cell.publication_mutex_;}
};
}
namespace {
struct native_fork_legacy_owner:default_listener_owner {
    std::mutex& actual_app_leaf(){return managed_wss_mutex_;}
};
struct actual_retirement_fork_state {
    const pid_t creator=::getpid();
    std::mutex* leaf;
    std::atomic<bool> entered{false},release{false},armed{false},setup_failed{false};
    pid_t child=-1;
    int marker_fd;
};
struct actual_retirement_fork_target {
    std::shared_ptr<actual_retirement_fork_state> state;
    ~actual_retirement_fork_target() {
        if(!state->armed.load()||state->creator!=::getpid())return;
        // Genuine other-thread leaf ownership survives fork in the child.
        // Thread storage is prepared and parent-cleaned; child has no automatic
        // inherited thread destructor or synthetic positive cleanup receipt.
        std::thread* holder=nullptr;
        try{holder=new std::thread([s=state]{
            std::lock_guard<std::mutex> lock(*s->leaf);s->entered.store(true,std::memory_order_release);
            const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
            while(!s->release.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
                std::this_thread::yield();
        });}catch(...){state->setup_failed.store(true);return;}
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(!state->entered.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
            std::this_thread::yield();
        if(!state->entered.load()){state->setup_failed.store(true);state->release.store(true);holder->join();delete holder;return;}
        const pid_t child=::fork();
        if(child==0)return; // Return through the REAL arbitrary target destructor.
        state->child=child;if(child<0)state->setup_failed.store(true);
        state->release.store(true,std::memory_order_release);holder->join();delete holder;
    }
};
struct actual_remaining_target {
    const pid_t creator=::getpid();int fd;
    ~actual_remaining_target(){if(creator!=::getpid()){const char marker='M';(void)::write(fd,&marker,1);}}
};
bool actual_child_completed(pid_t child) {
    if(child<=0)return false;
    int status=0;const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(2);
    do{
        const auto waited=::waitpid(child,&status,WNOHANG);
        if(waited==child)return WIFEXITED(status)&&WEXITSTATUS(status)==0;
        if(waited<0)return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }while(std::chrono::steady_clock::now()<end);
    (void)::kill(child,SIGKILL);while(::waitpid(child,&status,0)<0&&errno==EINTR){}
    return false; // Failure-only cleanup cannot make the source schedule pass.
}
struct actual_marker_pipe {
    int read_fd=-1,write_fd=-1;
    actual_marker_pipe(){int fds[2];if(::pipe(fds)!=0)throw lattice::db_error("actual fork fixture pipe failed");read_fd=fds[0];write_fd=fds[1];
        if(::fcntl(read_fd,F_SETFL,O_NONBLOCK)<0){::close(read_fd);::close(write_fd);throw lattice::db_error("actual fork fixture nonblocking pipe failed");}}
    ~actual_marker_pipe(){::close(read_fd);::close(write_fd);}
    bool no_foreign_release(){char byte;return ::read(read_fd,&byte,1)<0&&(errno==EAGAIN||errno==EWOULDBLOCK);}
};
}

TEST(Lifecycle, AttachedTargetForkStopsBeforeInheritedPublicationLeafAndRemainingTargets) {
    actual_marker_pipe markers;
    auto cell=std::make_shared<lattice::detail::configured_managed_handlers>();
    auto state=std::make_shared<actual_retirement_fork_state>();state->leaf=&lattice::detail::configured_listener_retirement_fork_test_access::leaf(*cell);state->marker_fd=markers.write_fd;
    auto target=std::shared_ptr<actual_retirement_fork_target>(new actual_retirement_fork_target{state});
    cell->publish_state(std::make_shared<const lattice::synchronizer::on_state_change_handler>([target](bool){}));target.reset();
    auto other=std::shared_ptr<actual_remaining_target>(new actual_remaining_target{::getpid(),markers.write_fd});
    cell->publish_error(std::make_shared<const lattice::synchronizer::on_error_handler>([other](const std::string&){}));
    cell->publish_progress(std::make_shared<const lattice::synchronizer::on_progress_handler>([other](const auto&){}));other.reset();
    cell->retire_publication();state->armed.store(true);
    const bool disposed=cell->dispose_retired();
    if(::getpid()!=state->creator)::_exit(!disposed&&!cell->retirement_disposed()&&!cell->application_retiring()?0:17);
    EXPECT_TRUE(disposed);EXPECT_FALSE(state->setup_failed.load());EXPECT_TRUE(actual_child_completed(state->child));
    EXPECT_TRUE(markers.no_foreign_release());EXPECT_TRUE(cell->retirement_disposed());
}

TEST(Lifecycle, DisplacedTargetForkCannotReturnSuccessfulPublicationInForeignProcess) {
    actual_marker_pipe markers;
    auto cell=std::make_shared<lattice::detail::configured_managed_handlers>();
    auto state=std::make_shared<actual_retirement_fork_state>();state->leaf=&lattice::detail::configured_listener_retirement_fork_test_access::leaf(*cell);state->marker_fd=markers.write_fd;
    auto target=std::shared_ptr<actual_retirement_fork_target>(new actual_retirement_fork_target{state});
    cell->publish_state(std::make_shared<const lattice::synchronizer::on_state_change_handler>([target](bool){}));target.reset();state->armed.store(true);
    const bool published=cell->publish_state(std::make_shared<const lattice::synchronizer::on_state_change_handler>());
    if(::getpid()!=state->creator)::_exit(!published?0:18);
    EXPECT_TRUE(published);EXPECT_FALSE(state->setup_failed.load());EXPECT_TRUE(actual_child_completed(state->child));
    cell->retire_publication();EXPECT_TRUE(cell->dispose_retired());
}

TEST(Lifecycle, LegacyAttachedTargetForkStopsActualCloseBeforeAppLeafAndRemainingCopies) {
    actual_marker_pipe markers;native_fork_legacy_owner owner;auto cell=owner.listener();
    auto state=std::make_shared<actual_retirement_fork_state>();state->leaf=&owner.actual_app_leaf();state->marker_fd=markers.write_fd;
    auto target=std::shared_ptr<actual_retirement_fork_target>(new actual_retirement_fork_target{state});
    owner.set_on_sync_state_change([target](bool){});cell->state.exchange({}).reset();target.reset();
    auto other=std::shared_ptr<actual_remaining_target>(new actual_remaining_target{::getpid(),markers.write_fd});
    owner.set_on_sync_error([other](const std::string&){});owner.set_on_sync_progress([other](const auto&){});
    cell->error.exchange({}).reset();cell->progress.exchange({}).reset();other.reset();state->armed.store(true);
    const auto report=owner.close_checked();
    if(::getpid()!=state->creator)::_exit(!report.cleanup_complete?0:19);
    EXPECT_TRUE(report.cleanup_complete);EXPECT_FALSE(state->setup_failed.load());EXPECT_TRUE(actual_child_completed(state->child));
    EXPECT_TRUE(markers.no_foreign_release());
}

TEST(Lifecycle, FinalCellTargetForkQuarantinesRemainingAutomaticAtomicPayload) {
    actual_marker_pipe markers;auto cell=std::make_shared<lattice::detail::configured_managed_handlers>();
    auto state=std::make_shared<actual_retirement_fork_state>();state->leaf=&lattice::detail::configured_listener_retirement_fork_test_access::leaf(*cell);state->marker_fd=markers.write_fd;
    auto target=std::shared_ptr<actual_retirement_fork_target>(new actual_retirement_fork_target{state});
    cell->publish_state(std::make_shared<const lattice::synchronizer::on_state_change_handler>([target](bool){}));target.reset();
    auto other=std::shared_ptr<actual_remaining_target>(new actual_remaining_target{::getpid(),markers.write_fd});
    cell->publish_error(std::make_shared<const lattice::synchronizer::on_error_handler>([other](const std::string&){}));other.reset();state->armed.store(true);
    cell.reset();
    if(::getpid()!=state->creator)::_exit(0);
    EXPECT_FALSE(state->setup_failed.load());EXPECT_TRUE(actual_child_completed(state->child));EXPECT_TRUE(markers.no_foreign_release());
}
#endif


#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <new>
namespace {
class actual_preparation_failure_scope final {
    std::function<void()> previous_;
public:
    explicit actual_preparation_failure_scope(int& calls) {
        previous_.swap(lattice::detail::configured_recovery_test_hooks::before_listener_retirement_preparation);
        lattice::detail::configured_recovery_test_hooks::before_listener_retirement_preparation=[&calls] {
            ++calls;
            throw std::bad_alloc();
        };
    }
    ~actual_preparation_failure_scope() {
        previous_.swap(lattice::detail::configured_recovery_test_hooks::before_listener_retirement_preparation);
    }
};
struct independently_copied_fallback_state {
    const pid_t creator=::getpid();
    std::atomic<bool> armed{false},fork_started{false};
    pid_t child=-1;
};
// Each std::function COPY owns a distinct callable object and independently
// runs this destructor. The shared object is coordination, not the target.
// This test therefore does not depend on the last shared capture being released.
struct independently_copied_fork_functor {
    std::shared_ptr<independently_copied_fallback_state> state;
    void operator()(const lattice::synchronizer::sync_progress&) const {}
    ~independently_copied_fork_functor() {
        if(!state->armed.load()||::getpid()!=state->creator||state->fork_started.exchange(true))return;
        const auto child=::fork();
        if(child==0)return; // Continue through the REAL std::function destruction.
        state->child=child;
    }
};
struct independently_copied_remaining_functor {
    const pid_t creator=::getpid();int fd;
    void operator()(bool) const {}
    void operator()(const std::string&) const {}
    ~independently_copied_remaining_functor() {
        if(::getpid()!=creator){const char marker='R';(void)::write(fd,&marker,1);}
    }
};
struct independently_copied_counting_functor {
    bool* armed;int* count;
    void operator()(bool) const {}
    void operator()(const std::string&) const {}
    void operator()(const lattice::synchronizer::sync_progress&) const {}
    ~independently_copied_counting_functor(){if(*armed)++*count;}
};
}

TEST(Lifecycle, PreparationFailureLastOwnerFallbackQuarantinesRemainingActualPayloadAfterFunctorFork) {
    actual_marker_pipe markers;
    auto state=std::make_shared<independently_copied_fallback_state>();
    auto owner=std::make_unique<default_listener_owner>();
    auto cell=owner->listener();
    owner->set_on_sync_state_change(independently_copied_remaining_functor{::getpid(),markers.write_fd});
    owner->set_on_sync_error(independently_copied_remaining_functor{::getpid(),markers.write_fd});
    owner->set_on_sync_progress(independently_copied_fork_functor{state});
    // Remove only attached independent copies during unarmed setup. The real
    // three legacy callable objects remain in the actual application payload.
    cell->state.exchange({}).reset();cell->error.exchange({}).reset();cell->progress.exchange({}).reset();
    int failures=0;actual_preparation_failure_scope failed_preparation(failures);
    state->armed.store(true);
    owner.reset(); // Actual last-owner destructor catches bad_alloc, then fallback.
    if(::getpid()!=state->creator)::_exit(markers.no_foreign_release()?0:31);
    EXPECT_EQ(failures,1);
    EXPECT_GT(state->child,0);
    EXPECT_TRUE(actual_child_completed(state->child));
    EXPECT_TRUE(markers.no_foreign_release());
}

TEST(Lifecycle, FailedListenerPreparationKeepsCloseFailureAndFinalFallbackReleasesEachActualFunctor) {
    auto owner=std::make_unique<default_listener_owner>();auto cell=owner->listener();
    bool armed=false;int state_count=0,error_count=0,progress_count=0;
    owner->set_on_sync_state_change(independently_copied_counting_functor{&armed,&state_count});
    owner->set_on_sync_error(independently_copied_counting_functor{&armed,&error_count});
    owner->set_on_sync_progress(independently_copied_counting_functor{&armed,&progress_count});
    cell->state.exchange({}).reset();cell->error.exchange({}).reset();cell->progress.exchange({}).reset();
    int failures=0;actual_preparation_failure_scope failed_preparation(failures);armed=true;
    const auto report=owner->close_checked();
    EXPECT_FALSE(report.cleanup_complete);ASSERT_TRUE(report.error);
    EXPECT_THROW(std::rethrow_exception(report.error),std::bad_alloc);
    EXPECT_EQ(state_count,0);EXPECT_EQ(error_count,0);EXPECT_EQ(progress_count,0);
    owner.reset();
    EXPECT_EQ(state_count,1);EXPECT_EQ(error_count,1);EXPECT_EQ(progress_count,1);
    EXPECT_GE(failures,2); // Explicit close and real destructor both kept failure.
}
#endif
