#include "configured_platform_fixture.hpp"
#include "../../LatticeCore/src/configured_retirement.hpp"
#include "../../LatticeCore/src/configured_attempt_custody.hpp"
#include <lattice/scheduler.hpp>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <utility>
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;
namespace lattice {
struct configured_platform_test_driver::state : std::enable_shared_from_this<state> {
    using registry=detail::configured_retirement_registry;
    std::shared_ptr<registry> registry_owner=registry::instance();
    std::optional<registry::reservation> owner;
    platform_retirement_receipt receipt;
    std::shared_ptr<detail::configured_attempt_custody> custody;
    std::shared_ptr<sync_transport> transport;
    mutable std::mutex mutex;
    std::condition_variable changed;
    configured_platform_fixture_facts observed;
    bool collecting=false,hold_armed=false,construction_hold_armed=false;
    state(){owner.emplace(registry_owner->reserve());receipt=owner->begin_attempt();custody=registry_owner->attempt_custody(receipt);}
    void fail(int32_t error){std::lock_guard lock(mutex);if(!observed.fixture_error)observed.fixture_error=error;}
    struct call_use {
        std::shared_ptr<state> owner;
        std::shared_ptr<sync_transport> transport;
        ~call_use(){transport.reset();if(owner){std::lock_guard lock(owner->mutex);--owner->observed.calls;}}
    };
    struct callback_use {
        std::shared_ptr<state> owner;
        explicit callback_use(std::shared_ptr<state> value):owner(std::move(value)){
            std::lock_guard lock(owner->mutex);++owner->observed.callbacks;
        }
        ~callback_use(){std::lock_guard lock(owner->mutex);--owner->observed.callbacks;}
    };
    void pre_pointer(detail::configured_bridge_operation operation){
        if(operation!=detail::configured_bridge_operation::connect)return;
        std::unique_lock lock(mutex);
        if(!hold_armed)return;
        hold_armed=false;observed.pre_pointer_entered=true;changed.notify_all();
        const auto until=std::chrono::steady_clock::now()+std::chrono::seconds(10);
        if(!changed.wait_until(lock,until,[&]{return observed.pre_pointer_released;})){
            observed.safety_release=true;if(!observed.fixture_error)observed.fixture_error=202;
        }
    }
    void construction_boundary(){
        std::unique_lock lock(mutex);observed.construction_admitted=true;
        if(!construction_hold_armed)return;
        construction_hold_armed=false;observed.construction_hold_entered=true;changed.notify_all();
        const auto until=std::chrono::steady_clock::now()+std::chrono::seconds(10);
        if(!changed.wait_until(lock,until,[&]{return observed.construction_hold_released;})){
            observed.safety_release=true;if(!observed.fixture_error)observed.fixture_error=206;
        }
    }
    void attach_callbacks(const std::shared_ptr<sync_transport>& value){
        const std::weak_ptr<state> weak=shared_from_this();
        value->set_on_open([weak]{if(auto s=weak.lock()){callback_use use(s);std::lock_guard lock(s->mutex);++s->observed.opens;}});
        value->set_on_error([weak](const std::string&){if(auto s=weak.lock()){callback_use use(s);std::lock_guard lock(s->mutex);++s->observed.errors;}});
        value->set_on_message([weak](const transport_message&){if(auto s=weak.lock()){callback_use use(s);}});
        value->set_on_close([weak](int,const std::string&){if(auto s=weak.lock()){callback_use use(s);}});
    }
};
configured_platform_test_driver::configured_platform_test_driver() noexcept {
    try {
        auto s=std::make_shared<state>();const std::weak_ptr<state> weak=s;
        s->custody->bind_foreign_probe([weak](detail::configured_bridge_operation operation){if(auto s=weak.lock())s->pre_pointer(operation);});
        state_=std::move(s);
    }catch(...){}
}
platform_retirement_receipt configured_platform_test_driver::issued_receipt() const noexcept {
    return state_?state_->receipt:platform_retirement_receipt{};
}
bool configured_platform_test_driver::construct(void* context,create_configured_transport_fn_ptr create) const noexcept {
    if(!state_||!create)return false;
    const auto s=state_;
    {std::lock_guard lock(s->mutex);if(s->observed.construction_entered||s->collecting)return false;s->observed.construction_entered=true;++s->observed.calls;}
    state::call_use call;call.owner=s;
    bool accepted=false;
    try {
        // Actual SDK registration may synchronously receive an earlier request
        // and close admission, but this already-admitted construction survives.
        auto command=s->custody->admit(detail::configured_attempt_custody::kind::command);
        if(!command){std::lock_guard lock(s->mutex);s->observed.construction_returned=true;return false;}
        s->construction_boundary();
        std::shared_ptr<scheduler> scheduler_owner=std::make_shared<immediate_scheduler>();
        std::shared_ptr<sync_transport> returned(create(context,&scheduler_owner,&s->receipt));
        if(returned){
            s->attach_callbacks(returned);
            // The registry intentionally retains its own final alias. Driver
            // cleanup drops only driver aliases before native settlement.
            accepted=s->registry_owner->retain_transport(s->receipt,returned);
            bool closing=false;
            {std::lock_guard lock(s->mutex);s->observed.transport_returned=true;s->transport=returned;closing=s->observed.closing;}
            if(closing)returned->disconnect();
            if(!accepted)s->fail(203);
        }
    }catch(...){s->fail(201);}
    // The command and temporary returned owner above have actually left scope.
    {std::lock_guard lock(s->mutex);s->observed.construction_returned=true;}
    return accepted;
}
bool configured_platform_test_driver::arm_construction_hold() const noexcept {
    if(!state_)return false;std::lock_guard lock(state_->mutex);
    if(state_->observed.construction_entered||state_->observed.closing||state_->construction_hold_armed)return false;
    state_->construction_hold_armed=true;return true;
}
void configured_platform_test_driver::release_construction_hold() const noexcept {
    if(!state_)return;{std::lock_guard lock(state_->mutex);state_->observed.construction_hold_released=true;}state_->changed.notify_all();
}
bool configured_platform_test_driver::connect(const std::string& url) const noexcept {
    if(!state_)return false;
    const auto s=state_;state::call_use call;
    {std::lock_guard lock(s->mutex);if(s->observed.closing||s->collecting||!s->transport)return false;++s->observed.calls;call.owner=s;call.transport=s->transport;}
    try {call.transport->connect(url);return true;}
    catch(...){
        // A real already-admitted bridge call may refuse after retirement.
        // That refusal is expected; it never fabricates a callback or dial.
        if(!s->registry_owner->snapshot(s->receipt).requested)s->fail(204);
        return false;
    }
}
bool configured_platform_test_driver::request_retirement() const noexcept {
    if(!state_)return false;
    const auto s=state_;state::call_use call;
    {std::lock_guard lock(s->mutex);if(s->collecting||s->observed.collected)return false;s->observed.closing=true;++s->observed.calls;call.owner=s;call.transport=s->transport;}
    try {if(call.transport)call.transport->disconnect();}catch(...){s->fail(205);}
    return s->registry_owner->request_retirement(s->receipt);
}
bool configured_platform_test_driver::collect_if_settled() const noexcept {
    if(!state_)return false;
    const auto s=state_;const auto before=s->registry_owner->snapshot(s->receipt);
    if(!before.valid||!before.requested||!before.adapter_complete||before.first_error)return false;
    std::shared_ptr<sync_transport> released;
    {
        std::lock_guard lock(s->mutex);
        if(!s->observed.closing||!s->observed.construction_returned||s->observed.calls||s->observed.callbacks||
           s->observed.fixture_error||s->collecting||s->observed.collected)return false;
        const auto uses=s->custody->snapshot();if(uses.commands||uses.payloads||uses.workers||uses.first_error)return false;
        s->collecting=true;released=std::move(s->transport);
    }
    // All admitted fixture invocations returned and its callback endpoint was
    // disconnected. Drop the actual last driver transport alias first. The
    // registry's remaining transport/context is destroyed by collect_completed.
    released.reset();
    const bool settled=before.native_complete||s->registry_owner->complete_native_cleanup(s->receipt,0);
    const bool collected=settled&&s->registry_owner->collect_completed(s->receipt);
    if(collected)s->owner.reset();
    {std::lock_guard lock(s->mutex);s->collecting=false;s->observed.collected=collected;}
    return collected;
}
configured_platform_fixture_facts configured_platform_test_driver::facts() const noexcept {
    if(!state_)return {};
    const auto s=state_;configured_platform_fixture_facts result;
    {std::lock_guard lock(s->mutex);result=s->observed;}
    const auto actual=s->registry_owner->snapshot(s->receipt);const auto uses=s->custody->snapshot();
    result.issued=actual.valid;result.requested=actual.requested;result.adapter_complete=actual.adapter_complete;
    result.native_complete=actual.native_complete;result.quarantined=actual.quarantined;result.first_error=actual.first_error;
    result.native_commands=uses.commands;result.native_payloads=uses.payloads;result.native_workers=uses.workers;
    result.charged_owners=s->registry_owner->charged_owners();return result;
}
bool configured_platform_test_driver::arm_pre_pointer_hold() const noexcept {
    if(!state_)return false;std::lock_guard lock(state_->mutex);
    if(state_->observed.closing||state_->hold_armed||state_->observed.pre_pointer_entered)return false;
    state_->hold_armed=true;return true;
}
void configured_platform_test_driver::release_pre_pointer_hold() const noexcept {
    if(!state_)return;{std::lock_guard lock(state_->mutex);state_->observed.pre_pointer_released=true;}state_->changed.notify_all();
}
bool install_configured_factory_reentry_probe(void* context,void(*release)(void*)) noexcept {
    if(!context||!release){if(release)release(context);return false;}
    std::shared_ptr<generic_network_factory> factory;
    try {factory=std::make_shared<generic_network_factory>(context,nullptr,nullptr,release);}
    catch(...){release(context);return false;}
    set_network_factory(std::move(factory));return true;
}

configured_factory_child_result run_configured_factory_child(
    const std::string& executable,const std::string& nonce) noexcept {
    configured_factory_child_result result;
    // One fixed process-lived child slot. Failed/unproved cleanup retains its
    // exact unreaped PID and refuses another launch; no growing orphan list.
    struct child_slot {std::mutex mutex;bool reserved=false;pid_t unresolved=-1;};
    static child_slot slot;
    {std::lock_guard lock(slot.mutex);if(slot.reserved){result.child_slot_unavailable=true;return result;}slot.reserved=true;}
    int descriptors[2]={-1,-1};pid_t child=-1;bool owned=false;
    const auto clock=[] {return std::chrono::steady_clock::now();};
    auto close_fds=[&]{for(auto& fd:descriptors){if(fd>=0){if(::close(fd)!=0)result.io_error=true;fd=-1;}}};
    auto reap=[&]() {
        int status=0;const auto value=::waitpid(child,&status,WNOHANG);
        if(value==child){owned=false;result.reaped=true;result.wait_status=status;result.normal_exit=WIFEXITED(status)&&WEXITSTATUS(status)==0;return true;}
        if(value<0&&errno==ECHILD){owned=false;result.custody_lost=true;return true;}
        if(value<0&&errno!=EINTR)result.io_error=true;
        return false;
    };
    auto read_output=[&] {
        char bytes[1024];
        // Fixed work per turn as well as a fixed transcript capacity.
        for(unsigned i=0;i<8;++i){
            const auto count=::read(descriptors[0],bytes,sizeof(bytes));
            if(count>0){
                if(static_cast<size_t>(count)>4096-result.output.size())result.output_overflow=true;
                else result.output.append(bytes,static_cast<size_t>(count));
                continue;
            }
            if(count<0&&errno!=EAGAIN&&errno!=EWOULDBLOCK&&errno!=EINTR)result.io_error=true;
            break;
        }
    };
    try {
        if(executable.empty()||executable.front()!='/'||executable.find('\0')!=std::string::npos||
           nonce.size()!=36||nonce.find_first_not_of("0123456789abcdefABCDEF-")!=std::string::npos)
            throw std::invalid_argument("invalid standalone factory child invocation");
        #if defined(__linux__)
        if(::pipe2(descriptors,O_CLOEXEC)!=0)throw std::runtime_error("child pipe failed");
        #else
        if(::pipe(descriptors)!=0)throw std::runtime_error("child pipe failed");
        for(auto fd:descriptors)if(::fcntl(fd,F_SETFD,FD_CLOEXEC)<0)throw std::runtime_error("child pipe close-on-exec failed");
        #endif
        if(descriptors[0]<3||descriptors[1]<3)throw std::runtime_error("standard descriptors must be open");
        const auto flags=::fcntl(descriptors[0],F_GETFL);
        if(flags<0||::fcntl(descriptors[0],F_SETFL,flags|O_NONBLOCK)<0)throw std::runtime_error("child pipe nonblocking failed");
        posix_spawn_file_actions_t actions;
        if(posix_spawn_file_actions_init(&actions)!=0)throw std::runtime_error("child file actions failed");
        struct actions_owner {posix_spawn_file_actions_t* value;~actions_owner(){posix_spawn_file_actions_destroy(value);}} actions_lifetime{&actions};
        if(posix_spawn_file_actions_adddup2(&actions,descriptors[1],STDOUT_FILENO)!=0||
           posix_spawn_file_actions_adddup2(&actions,descriptors[1],STDERR_FILENO)!=0||
           posix_spawn_file_actions_addclose(&actions,descriptors[0])!=0||
           posix_spawn_file_actions_addclose(&actions,descriptors[1])!=0)
            throw std::runtime_error("child descriptor mapping failed");
        // Standalone helper has no test-runner entry or filter and never forks.
        char mode[]="--configured-factory-publication";
        char* argv[]={const_cast<char*>(executable.c_str()),mode,const_cast<char*>(nonce.c_str()),nullptr};
        const auto until=clock()+std::chrono::seconds(10);
        if(::posix_spawn(&child,executable.c_str(),&actions,nullptr,argv,environ)!=0)
            throw std::runtime_error("standalone child spawn failed");
        owned=true;result.launched=true;result.child_pid=child;
        if(::close(descriptors[1])!=0)result.io_error=true;descriptors[1]=-1;
        while(clock()<until){
            read_output();
            if(clock()>=until)break;
            if(reap()){
                // A late scheduling return is retained as a real reap but is
                // not retroactively a timely positive observation.
                if(clock()>=until)result.timed_out=true;
                break;
            }
            if(result.io_error)break;
            pollfd ready{descriptors[0],POLLIN,0};
            const auto polled=::poll(&ready,1,10);
            if(polled<0&&errno!=EINTR){result.io_error=true;break;}
            if(ready.revents&POLLHUP)::poll(nullptr,0,10);
        }
        if(owned&&clock()>=until)result.timed_out=true;
        if(result.reaped)read_output();
    }catch(...){result.io_error=true;}
    if(owned){
        // This helper's creator is its only permitted reaper. Verify actual
        // unreaped custody immediately before any signal; after ECHILD or an
        // observed reap, never signal that numeric PID again.
        const auto cleanup_until=clock()+std::chrono::seconds(10);
        bool signaled=false;
        while(owned&&clock()<cleanup_until){
            if(!signaled){
                siginfo_t observed{};
                const auto observation=::waitid(P_PID,static_cast<id_t>(child),&observed,WEXITED|WNOHANG|WNOWAIT);
                if(observation<0){
                    if(errno==EINTR)continue;
                    if(errno==ECHILD){result.custody_lost=true;owned=false;}
                    else result.io_error=true;
                    break;
                }
                if(observed.si_pid==0){
                    if(::kill(child,SIGKILL)!=0&&errno!=ESRCH)result.io_error=true;
                    signaled=true;
                }else if(observed.si_pid!=child){result.custody_lost=true;owned=false;break;}
            }
            if(reap())break;
            if(descriptors[0]>=0)read_output();
            // Bounded wait only; every subsequent syscall rechecks the clock.
            ::poll(nullptr,0,10);
        }
        if(owned)result.cleanup_unproved=true;
    }
    close_fds();
    {std::lock_guard lock(slot.mutex);if(owned)slot.unresolved=child;else slot.reserved=false;}
    return result;
}
}
