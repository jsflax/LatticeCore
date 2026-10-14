#include "TestHelpers.hpp"
#include <cerrno>
#include <future>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#ifndef __linux__
#include <lattice.hpp>
#endif

#if defined(__unix__) || defined(__APPLE__)
namespace {
struct quarantine_pipe {
    int reader=-1,writer=-1;
    quarantine_pipe() {
        int fds[2];
        if (::pipe(fds)!=0) throw std::runtime_error("quarantine test pipe");
        reader=fds[0];writer=fds[1];
        if (::fcntl(reader,F_SETFL,O_NONBLOCK)!=0) {
            ::close(reader);::close(writer);
            throw std::runtime_error("quarantine test nonblocking pipe");
        }
    }
    ~quarantine_pipe(){::close(reader);::close(writer);}
    std::string available() const {
        std::string result;char bytes[32];
        for (;;) {
            const auto count=::read(reader,bytes,sizeof(bytes));
            if(count>0){result.append(bytes,size_t(count));continue;}
            if(count<0&&errno==EINTR)continue;
            return result;
        }
    }
};

int bounded_child_status(pid_t child) {
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(4);
    int status=0;
    while(std::chrono::steady_clock::now()<deadline) {
        const auto actual=::waitpid(child,&status,WNOHANG);
        if(actual==child)return status;
        if(actual<0&&errno!=EINTR)return -1;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ::kill(child,SIGKILL);
    while(::waitpid(child,&status,0)<0&&errno==EINTR){}
    return -1;
}

// If an inherited capture is destroyed it takes a mutex that was locked at
// fork, then reports C. The real creator's eventual release reports P.
struct capture_release_probe {
    pid_t creator=::getpid();
    std::mutex* gate;
    int output;
    capture_release_probe(std::mutex* value,int fd):gate(value),output(fd){}
    ~capture_release_probe() {
        std::lock_guard<std::mutex> lock(*gate);
        const char actual=::getpid()==creator?'P':'C';
        (void)::write(output,&actual,1);
    }
};

void child_finished(int fd,bool success) {
    const char actual=success?'D':'E';
    (void)::write(fd,&actual,1);
    ::_exit(success?0:90);
}
}

TEST(InheritedOwnerQuarantine, ActualCoreAuditThreadAndObserverCapturesAreAbandonedInChild) {
    quarantine_pipe output;
    std::mutex gate;
    lattice::configuration config(":memory:");
    config.audit_retention_seconds=3600; // real constructor arms its real thread
    auto owner=std::make_unique<lattice::lattice_db>(config);
    auto capture=std::make_shared<capture_release_probe>(&gate,output.writer);
    owner->add_table_observer("TestPerson",[capture](const auto&){});
    capture.reset(); // observer map is the only capture owner at fork
    std::unique_lock<std::mutex> hold(gate);
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        owner.reset(); // actual destructor plus all automatic member destructors
        child_finished(output.writer,true);
    }
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    hold.unlock();
    ASSERT_NE(status,-1);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
    owner->add(TestPerson{"parent-still-current",42,std::nullopt});
    const auto rows=owner->db().query("SELECT COUNT(*) AS n FROM TestPerson");
    ASSERT_EQ(std::get<int64_t>(rows.at(0).at("n")),1);
    owner.reset(); // original same-process audit join and capture release
    EXPECT_EQ(output.available(),"P");
}

TEST(InheritedOwnerQuarantine, ActualCoreExplicitCleanupRefusesBeforeInheritedMutexOrThread) {
    quarantine_pipe output;
    lattice::configuration config(":memory:");
    config.audit_retention_seconds=3600;
    auto owner=std::make_unique<lattice::lattice_db>(config);
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        const auto result=owner->close_checked();
        bool audit_refused=false;
        try {owner->stop_audit_maintenance();}catch(...){audit_refused=true;}
        const bool refused=result.error&&!result.cleanup_complete&&
            result.sync==lattice::sync_drain_state::not_attempted&&audit_refused&&owner->is_closed();
        owner.reset();
        child_finished(output.writer,refused);
    }
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    ASSERT_NE(status,-1);ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
    const auto parent=owner->close_checked();
    EXPECT_FALSE(parent.error);EXPECT_TRUE(parent.cleanup_complete);
    owner.reset();
}

TEST(InheritedOwnerQuarantine, NativeHolderAbandonsActualAsyncFutureWithoutWaitingInChild) {
    // Component oracle for the same native holder used by both real Swift
    // future fields. It does not pretend to issue a managed store/context.
    struct future_payload {
        std::future<void> task;
        explicit future_payload(std::shared_future<void> finish)
            :task(std::async(std::launch::async,[finish]{finish.wait();})){}
    };
    quarantine_pipe output;
    std::promise<void> finish;
    auto owner=std::make_unique<lattice::detail::process_bound_resource_owner<future_payload>>(finish.get_future().share());
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        owner.reset(); // ordinary future destruction would wait for absent thread
        child_finished(output.writer,true);
    }
    if(child<0)finish.set_value();
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    finish.set_value(); // only the actual parent may settle its task
    owner.reset();
    ASSERT_NE(status,-1);ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
}

TEST(InheritedOwnerQuarantine, NativeHolderConstructionFailureReleasesRealMembersInReverseOrder) {
    struct token {
        std::vector<int>* order;int value;
        ~token(){order->push_back(value);}
    };
    struct failed_payload {
        token first,second;
        explicit failed_payload(std::vector<int>& order):first{&order,1},second{&order,2} {
            throw std::runtime_error("real payload constructor failure");
        }
    };
    std::vector<int> order;
    EXPECT_THROW({lattice::detail::process_bound_resource_owner<failed_payload> owner(order);},std::runtime_error);
    EXPECT_EQ(order,(std::vector<int>{2,1}));
}

#ifndef __linux__
TEST(InheritedOwnerQuarantine, ActualDerivedSwiftMigrationCaptureIsAbandonedBeforeBaseTeardown) {
    quarantine_pipe output;
    std::mutex gate;
    lattice::swift_configuration config;
    config.path=":memory:";
    config.audit_retention_seconds=3600;
    config.setRowMigrationCallback(new capture_release_probe(&gate,output.writer),
        [](const char*,void*){},[](void* value){delete static_cast<capture_release_probe*>(value);});
    auto owner=std::make_unique<lattice::swift_lattice>(std::move(config));
    std::unique_lock<std::mutex> hold(gate);
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        const auto refused=owner->close_checked();
        bool wait_refused=false;
        try {owner->wait_for_vec0_training();}catch(...){wait_refused=true;}
        const bool success=refused.error&&!refused.cleanup_complete&&wait_refused;
        owner.reset(); // derived owned fields destruct before Core base
        child_finished(output.writer,success);
    }
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    hold.unlock();
    ASSERT_NE(status,-1);ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
    owner.reset();
    EXPECT_EQ(output.available(),"P");
}

TEST(InheritedOwnerQuarantine, ActualRetainedSwiftCacheRefusesInheritedCallsBeforeItsMutex) {
    auto& cache=lattice::detail::LatticeCache::instance();
    quarantine_pipe output;
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        bool accessor_refused=false,retained_refused=false;
        try {(void)lattice::detail::LatticeCache::instance();}catch(...){accessor_refused=true;}
        try {cache.evict(nullptr);}catch(...){retained_refused=true;}
        child_finished(output.writer,accessor_refused&&retained_refused);
    }
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    ASSERT_NE(status,-1);ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
    EXPECT_NO_THROW(cache.evict(nullptr));
}
#endif

TEST(InheritedOwnerQuarantine, ActualRetainedCoreRegistryRefusesAfterChildWrapperAbandonment) {
    auto owner=std::make_unique<lattice::lattice_db>();
    auto& registry=lattice::instance_registry::instance();
    quarantine_pipe output;
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        owner.reset();
        bool accessor_refused=false,retained_refused=false;
        try {(void)lattice::instance_registry::instance();}catch(...){accessor_refused=true;}
        try {registry.for_each_alive(":memory:",[](lattice::lattice_db*){});}catch(...){retained_refused=true;}
        child_finished(output.writer,accessor_refused&&retained_refused);
    }
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    ASSERT_NE(status,-1);ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
    size_t parents=0;
    EXPECT_NO_THROW(registry.for_each_alive(":memory:",[&](lattice::lattice_db* actual){if(actual==owner.get())++parents;}));
    EXPECT_EQ(parents,1);
}

namespace {
// Expose genuine existing protected startup entries to a storage-only fixture.
// This creates no production friend, managed context, admission or endpoint.
struct deferred_setup_probe final : lattice::lattice_db {
    explicit deferred_setup_probe(const lattice::configuration& config)
        :lattice::lattice_db(config,true){}
    void invoke_sync_setup(){setup_sync_if_configured();}
    void invoke_ipc_setup(){setup_ipc_if_configured();}
};
template<class Operation> bool actual_inherited_owner_refusal(Operation&& operation) {
    try {operation();}
    catch(const std::runtime_error& failure) {
        return std::string(failure.what())=="inherited lattice owner belongs to another process";
    }
    catch(...) {}
    return false;
}
}

TEST(InheritedOwnerQuarantine, ActualDeferredWssEntriesRefuseBeforeRetainedSyncStateInChild) {
    quarantine_pipe output;
    lattice::configuration config(":memory:","wss://127.0.0.1:1/quarantine-never-start","storage-only-no-credential");
    auto owner=std::make_unique<deferred_setup_probe>(config); // genuine defer_sync constructor
    EXPECT_TRUE(owner->is_sync_enabled());
    EXPECT_FALSE(owner->is_sync_connected());
    EXPECT_FALSE(owner->is_sync_agent());
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        bool refused=true;
        // Check every actual public sync owner entry and both protected setup
        // entries. Empty callback/filter values issue no managed authority.
        refused&=actual_inherited_owner_refusal([&]{owner->invoke_sync_setup();});
        refused&=actual_inherited_owner_refusal([&]{owner->invoke_ipc_setup();});
        refused&=actual_inherited_owner_refusal([&]{owner->connect_sync();});
        refused&=actual_inherited_owner_refusal([&]{owner->disconnect_sync();});
        refused&=actual_inherited_owner_refusal([&]{owner->sync_now();});
        refused&=actual_inherited_owner_refusal([&]{(void)owner->is_sync_enabled();});
        refused&=actual_inherited_owner_refusal([&]{(void)owner->is_sync_connected();});
        refused&=actual_inherited_owner_refusal([&]{(void)owner->is_sync_agent();});
        refused&=actual_inherited_owner_refusal([&]{owner->update_sync_filter(std::vector<lattice::sync_filter_entry>{});});
        refused&=actual_inherited_owner_refusal([&]{owner->update_sync_filter("",std::vector<lattice::sync_filter_entry>{});});
        refused&=actual_inherited_owner_refusal([&]{owner->clear_sync_filter();});
        refused&=actual_inherited_owner_refusal([&]{owner->clear_sync_filter("");});
        refused&=actual_inherited_owner_refusal([&]{owner->set_on_sync_state_change({});});
        refused&=actual_inherited_owner_refusal([&]{owner->set_on_sync_error({});});
        refused&=actual_inherited_owner_refusal([&]{owner->set_on_sync_progress({});});
        refused&=actual_inherited_owner_refusal([&]{(void)owner->get_sync_progress();});
        refused&=actual_inherited_owner_refusal([&]{owner->set_on_xproc_idle({});});
        refused&=actual_inherited_owner_refusal([&]{owner->handle_cross_process_notification();});
        refused&=actual_inherited_owner_refusal([&]{(void)owner->is_synchronizer();});
        refused&=actual_inherited_owner_refusal([&]{owner->set_is_synchronizer(false);});
        refused&=actual_inherited_owner_refusal([&]{owner->reset_sync_state("quarantine-inherited");});
        refused&=actual_inherited_owner_refusal([&]{owner->remove_sync_channel_state("quarantine-inherited");});
        owner.reset(); // still abandons the actual inherited automatic payload
        child_finished(output.writer,refused);
    }
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    ASSERT_NE(status,-1);ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
    // No positive network start is requested. Creator neutral sync use and
    // actual store/observer behavior remain live after its child is rejected.
    EXPECT_NO_THROW(owner->disconnect_sync());
    EXPECT_NO_THROW(owner->sync_now());
    int observed=0;owner->add_table_observer("TestPerson",[&](const auto&){++observed;});
    owner->add(TestPerson{"deferred-wss-parent",11,std::nullopt});
    EXPECT_EQ(observed,1);
    const auto rows=owner->db().query("SELECT COUNT(*) AS n FROM TestPerson");
    ASSERT_EQ(std::get<int64_t>(rows.at(0).at("n")),1);
}

TEST(InheritedOwnerQuarantine, ActualDeferredIpcSetupRefusesBeforeEndpointAndParentRemainsLive) {
    quarantine_pipe output;
    lattice::configuration config(":memory:");
    // A real configured target deliberately cannot be started; the native
    // owner refusal must precede endpoint construction/path use in the child.
    config.ipc_targets.push_back({"quarantine-inherited-do-not-start",std::nullopt,std::string("/dev/null")});
    auto owner=std::make_unique<deferred_setup_probe>(config);
    EXPECT_FALSE(owner->is_sync_enabled());EXPECT_FALSE(owner->is_sync_agent());
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        const bool refused=actual_inherited_owner_refusal([&]{owner->invoke_ipc_setup();})&&
            actual_inherited_owner_refusal([&]{owner->connect_sync();})&&
            actual_inherited_owner_refusal([&]{owner->disconnect_sync();})&&
            actual_inherited_owner_refusal([&]{owner->sync_now();});
        owner.reset();child_finished(output.writer,refused);
    }
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    ASSERT_NE(status,-1);ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
    EXPECT_NO_THROW(owner->disconnect_sync());EXPECT_NO_THROW(owner->sync_now());
    owner->add(TestPerson{"deferred-ipc-parent",12,std::nullopt});
    const auto rows=owner->db().query("SELECT COUNT(*) AS n FROM TestPerson");
    ASSERT_EQ(std::get<int64_t>(rows.at(0).at("n")),1);
}

TEST(InheritedOwnerQuarantine, ActualNativeVectorReceiveRefusesBeforeInheritedDeliveryResources) {
    quarantine_pipe output;
    auto owner=std::make_unique<deferred_setup_probe>(lattice::configuration(":memory:"));
    const std::vector<lattice::audit_log_entry> empty;
    std::vector<lattice::audit_log_entry> input(1);
    input.front().global_id="inherited-vector-never-admitted";
    input.front().table_name="TestPerson";
    input.front().operation="INSERT";
    input.front().global_row_id="inherited-vector-never-applied";
    const pid_t child=::fork();
    if(child==0) {
        ::alarm(3);
        const bool refused=
            actual_inherited_owner_refusal([&]{(void)lattice::apply_remote_changes(*owner,empty);})&&
            actual_inherited_owner_refusal([&]{(void)lattice::apply_remote_changes(*owner,input);})&&
            actual_inherited_owner_refusal([&]{(void)lattice::apply_remote_changes_for(*owner,empty,"inherited-vector");})&&
            actual_inherited_owner_refusal([&]{(void)lattice::apply_remote_changes_for(*owner,input,"inherited-vector");});
        owner.reset();child_finished(output.writer,refused);
    }
    ASSERT_GT(child,0);
    const auto status=bounded_child_status(child);
    ASSERT_NE(status,-1);ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(output.available(),"D");
    // Empty vectors create neither receipt nor model effect in their creator.
    EXPECT_TRUE(lattice::apply_remote_changes(*owner,empty).empty());
    EXPECT_TRUE(lattice::apply_remote_changes_for(*owner,empty,"inherited-vector").empty());
    owner->add(TestPerson{"receive-parent-remains-live",13,std::nullopt});
    const auto rows=owner->db().query("SELECT COUNT(*) AS n FROM TestPerson");
    ASSERT_EQ(std::get<int64_t>(rows.at(0).at("n")),1);
}
#endif
