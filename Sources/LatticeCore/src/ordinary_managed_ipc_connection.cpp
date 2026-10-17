#include "ordinary_managed_ipc_role.hpp"
#include "ordinary_managed_ipc_route.hpp"
#include "ordinary_managed_peer_owner.hpp"
#include "ordinary_managed_ipc_endpoint.hpp"
#include "ordinary_export.hpp"
#include "ordinary_managed_ipc_workers.hpp"
#include "ordinary_managed_ipc_connection_test.hpp"
#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace lattice::detail {
namespace managed_ipc_connection_private {
using clock=std::chrono::steady_clock;
constexpr std::size_t connection_limit=ordinary_managed_ipc_workers::maximum_owners;
const pid_t connection_process=::getpid();
void same_process() {
    // Precedes even a function-local static guard, not merely its mutex.
    if(::getpid()!=connection_process)throw std::runtime_error("inherited managed IPC connection refused");
}
void require(bool value,const char* message) {if(!value)throw std::runtime_error(message);}
struct connection_record;
struct connection_registry {
    std::mutex leaf;
    std::array<bool,connection_limit> reserved{};
    std::array<std::shared_ptr<connection_record>,connection_limit> records;
};
connection_registry& registry() {
    same_process();
    // Live or uncertain retirement survives dropped wrappers without running
    // descriptor/role/thread destruction during process static teardown.
    static auto* owners=new connection_registry;
    return *owners;
}
struct connection_payload {
    std::shared_ptr<const ordinary_managed_ipc_role> role;
    std::shared_ptr<ordinary_managed_ipc_workers> workers;
    std::shared_ptr<ordinary_managed_peer_owner> peer;
};
struct connection_record {
    const pid_t process=::getpid();
    const std::size_t slot;
    std::mutex leaf;
    std::atomic<bool> stopping{false};
    // Allocated before any role/callback is retained; destructor may release
    // this allocation into the child's quarantine without allocating there.
    std::unique_ptr<connection_payload> payload;
    int fd=-1;
    bool registered=false,published=false,collecting=false,close_attempted=false,closed=false,released=false;
    int close_error=0;
    explicit connection_record(std::size_t value):slot(value),payload(std::make_unique<connection_payload>()){}
    ~connection_record();
    bool current_process()const noexcept{return ::getpid()==process;}
    void stop()noexcept {
        if(!current_process())return;
        stopping.store(true,std::memory_order_release);
        // workers is installed before any public wrapper is constructed and
        // retained until wrapper/record destruction. It is never reset by stop.
        if(payload&&payload->workers)payload->workers->request_stop();
        // Only shutdown; exact link ownership remains until the collector.
        if(payload&&payload->peer)payload->peer->request_stop();
    }
};
struct reservation_pair {
    std::array<std::size_t,2> slots{connection_limit,connection_limit};
    std::array<bool,2> handed_off{};
    bool committed=false;
    reservation_pair() {
        auto& owners=registry();std::lock_guard lock(owners.leaf);
        std::size_t n=0;
        for(std::size_t i=0;i<connection_limit&&n<2;++i)
            if(!owners.reserved[i])slots[n++]=i;
        require(n==2,"managed IPC connection capacity exhausted");
        for(auto i:slots)owners.reserved[i]=true;
    }
    ~reservation_pair() {
        if(committed||::getpid()!=connection_process)return;
        auto& owners=registry();std::lock_guard lock(owners.leaf);
        // Failure before records are installed creates no endpoint admission.
        for(std::size_t n=0;n<2;++n)if(!handed_off[n]&&slots[n]!=connection_limit)owners.reserved[slots[n]]=false;
    }
};
void register_record(const std::shared_ptr<connection_record>& record) {
    auto& owners=registry();std::lock_guard lock(owners.leaf);
    require(owners.reserved[record->slot]&&!owners.records[record->slot],"managed IPC reservation changed");
    owners.records[record->slot]=record;
    record->registered=true;
}
void unlist_record(const std::shared_ptr<connection_record>& record) {
    auto& owners=registry();std::shared_ptr<connection_record> retired;
    {
        std::lock_guard lock(owners.leaf);
        require(owners.records[record->slot]==record,"managed IPC exact registry changed");
        retired=std::move(owners.records[record->slot]);
    }
    // All current and transient aliases have already lost operation admission.
    // Destruction of the retained role and concrete worker owner is off-leaf.
    retired.reset();
    // Slot remains charged until the very last record/connection alias and
    // its real role/capture payload are destroyed below.
}
connection_record::~connection_record() {
    auto* held=payload.release(); // no automatic captured-owner destruction
    if(!current_process())return; // quarantine inherited allocation
    if(!registered){delete held;return;}
    if(!released)return; // an unproved record must never destroy its custody
    held->role.reset(); // each actual context release can execute foreign code
    if(!current_process())return;
    held->workers.reset();
    if(!current_process())return;
    held->peer.reset();
    if(!current_process())return;
    delete held; // both captured owners are now empty
    auto& owners=registry();std::lock_guard lock(owners.leaf);
    owners.reserved[slot]=false;
}
bool joined(const ordinary_managed_ipc_workers::observation& facts) {
    if(facts.inherited||!facts.stopping||facts.join_error)return false;
    for(std::size_t i=0;i<2;++i)
        if(facts.starting[i]||(facts.started[i]&&!facts.joined[i]))return false;
    return true;
}
bool release_record(const std::shared_ptr<connection_record>& record,clock::time_point deadline=clock::time_point::max()) {
    require(record&&record->current_process(),"inherited managed IPC release refused");
    if(record->payload->peer&&deadline==clock::time_point::max())return false;
    const auto workers=record->payload->workers;
    // Read the actual immutable publication flag under its leaf. Before
    // publication no callback/worker may have obtained this private record.
    if(workers&&!joined(workers->observe()))return false;
    {
        std::lock_guard lock(record->leaf);
        if(record->released)return true;
        if(!record->stopping.load(std::memory_order_acquire)||record->collecting)return false;
        if(record->close_error)throw std::system_error(record->close_error,std::generic_category(),"managed IPC close uncertain");
        record->collecting=true;
    }
    try {
        // Only one collector owns the descriptor. No attempt operation or
        // caller worker remains; no leaf spans the actual close or release.
        int descriptor=-1;bool close_now=false;
        {
            std::lock_guard lock(record->leaf);
            if(!record->close_attempted){record->close_attempted=true;descriptor=record->fd;close_now=true;}
        }
        if(close_now && record->payload->peer) {
            const auto settled=record->payload->peer->settle(deadline);
            using phase=ordinary_managed_cohort_link_settlement_state;
            if(settled.state==phase::pending||settled.state==phase::in_progress){
                std::lock_guard lock(record->leaf);record->close_attempted=false;record->collecting=false;return false;
            }
            if(settled.state!=phase::clean){
                const auto error=settled.native_error?settled.native_error:EIO;
                {std::lock_guard lock(record->leaf);record->close_error=error;}
                throw std::system_error(error,std::generic_category(),"managed peer close uncertain");
            }
            {std::lock_guard lock(record->leaf);record->fd=-1;record->closed=true;}
            // Actual close is recorded even if late. Return false this turn;
            // a later collector may settle capacity without erasing timeout.
            if(!settled.deadline_met){std::lock_guard lock(record->leaf);record->collecting=false;return false;}
        } else if(close_now) {
            if(descriptor>=0&&::close(descriptor)!=0) {
                const auto error=errno;
                {std::lock_guard lock(record->leaf);record->close_error=error;}
                // EINTR can be ambiguous. Never retry an integer that may
                // already be recyclable; keep the exact uncertain record.
                throw std::system_error(error,std::generic_category(),"managed IPC close uncertain");
            }
            std::lock_guard lock(record->leaf);record->fd=-1;record->closed=true;
        }
        {std::lock_guard lock(record->leaf);require(record->closed,"managed IPC descriptor retirement unproved");}
        if(workers) {
            const bool settled=workers->release_if_joined();
            // Endpoint destruction is outside the worker leaf and can change
            // execution context. A child must not resume connection collection.
            if(!record->current_process())return false;
            if(!settled){std::lock_guard lock(record->leaf);record->collecting=false;return false;}
        }
        // release_if_joined may destroy the concrete endpoint outside its
        // leaf. That reentrant destructor can request_stop, never collect.
        unlist_record(record);
        {std::lock_guard lock(record->leaf);record->released=true;record->collecting=false;}
        return true;
    } catch(...) {
        if(!record->current_process())return false;
        std::lock_guard lock(record->leaf);record->collecting=false;throw;
    }
}
void discard_unpublished(const std::shared_ptr<connection_record>& record,clock::time_point deadline=clock::time_point::max())noexcept {
    if(!record||!record->current_process()||!record->registered)return;
    record->stop();
    // This is solely constructor unwind before either connection escaped.
    // Worker002 proves neither worker started, rather than a caller joined bit.
    try{(void)release_record(record,deadline);}catch(...){/* keep exact registered debt */}
}
}

using namespace managed_ipc_connection_private;
struct ordinary_managed_ipc_role::implementation {
    const pid_t process=::getpid();
    const std::shared_ptr<const ordinary_managed_ipc_local_route> route;
    // Socket direction is observation only. The one product route already
    // names source/recipient and its authenticated partition independently.
    const bool source_side;
    const std::shared_ptr<ordinary_managed_peer_owner> peer;
    implementation(std::shared_ptr<const ordinary_managed_ipc_local_route> value,bool side)
        :route(std::move(value)),source_side(side){}
    explicit implementation(std::shared_ptr<ordinary_managed_peer_owner> actual)
        :source_side(false),peer(std::move(actual)){}
};
ordinary_managed_ipc_role::ordinary_managed_ipc_role(std::shared_ptr<const implementation> value):impl_(std::move(value)){}
ordinary_managed_ipc_role::~ordinary_managed_ipc_role()=default;
void ordinary_managed_ipc_role::validate(ordinary_managed_ipc_operation operation)const {
    require(impl_&&impl_->process==::getpid(),"managed IPC role inherited or absent");
    switch(operation) {
        case ordinary_managed_ipc_operation::receive:
        case ordinary_managed_ipc_operation::send:
        case ordinary_managed_ipc_operation::publication:break;
        default:throw std::runtime_error("managed IPC unknown operation");
    }
    if(impl_->route)impl_->route->validate_lifetime(); // no SQL, callback or endpoint leaf
    else if(impl_->peer)impl_->peer->validate_lifetime();
    else throw std::runtime_error("managed IPC role has no retained actual owner");
}
const std::shared_ptr<ordinary_managed_peer_owner>& ordinary_managed_ipc_role::peer_owner()const{
    require(impl_&&impl_->process==::getpid()&&bool(impl_->peer),"managed IPC actual peer role absent");return impl_->peer;
}
struct ordinary_managed_ipc_connection::implementation {
    const std::shared_ptr<connection_record> record;
    explicit implementation(std::shared_ptr<connection_record> value):record(std::move(value)){}
};
ordinary_managed_ipc_connection::ordinary_managed_ipc_connection(std::unique_ptr<implementation> value):impl_(std::move(value)){}
ordinary_managed_ipc_connection::~ordinary_managed_ipc_connection(){request_stop();}
ordinary_managed_ipc_connection::ordinary_managed_ipc_connection(ordinary_managed_ipc_connection&&)noexcept=default;
ordinary_managed_ipc_connection& ordinary_managed_ipc_connection::operator=(ordinary_managed_ipc_connection&& other)noexcept {
    if(this!=&other){request_stop();impl_=std::move(other.impl_);}return *this;
}
void ordinary_managed_ipc_connection::request_stop()noexcept {if(impl_)impl_->record->stop();}
int ordinary_managed_ipc_connection::descriptor()const {
    require(impl_&&impl_->record->current_process(),"managed IPC descriptor inherited or moved");
    auto& record=*impl_->record;std::lock_guard lock(record.leaf);
    require(!record.close_attempted&&!record.released&&record.fd>=0,"managed IPC descriptor retired");
    // Borrowing during request_stop is needed for the attempt's real shutdown.
    // Only the actual attempt's operation/terminal count excludes collection.
    return record.fd;
}
const ordinary_managed_ipc_role& ordinary_managed_ipc_connection::role()const {
    require(impl_&&impl_->record->current_process(),"managed IPC role inherited or moved");
    require(bool(impl_->record->payload->role),"managed IPC role unavailable");return *impl_->record->payload->role;
}
void ordinary_managed_ipc_connection::validate(ordinary_managed_ipc_operation operation)const {
    require(impl_&&impl_->record->current_process(),"managed IPC connection inherited or moved");
    require(!impl_->record->stopping.load(std::memory_order_acquire),"managed IPC connection stopped");
    role().validate(operation);
}
bool ordinary_managed_ipc_connection::release_after_settlement(clock::time_point deadline){return impl_&&release_record(impl_->record,deadline);}
ordinary_managed_ipc_issued::ordinary_managed_ipc_issued(ordinary_managed_ipc_connection&& connection_value,
    ordinary_managed_ipc_worker_owner&& worker_value):connection(std::move(connection_value)),workers(std::move(worker_value)){}
ordinary_managed_ipc_issued::ordinary_managed_ipc_issued(ordinary_managed_ipc_issued&&)noexcept=default;
ordinary_managed_ipc_issued& ordinary_managed_ipc_issued::operator=(ordinary_managed_ipc_issued&&)noexcept=default;

std::pair<ordinary_managed_ipc_issued,ordinary_managed_ipc_issued> ordinary_managed_ipc_access::make_local_connections(
    std::shared_ptr<const ordinary_managed_ipc_local_route> route,clock::time_point deadline) {
    same_process();require(bool(route),"managed IPC actual local route missing");
    reservation_pair capacity; // both connection slots before retaining owners
    std::array<std::shared_ptr<connection_record>,2> records;
    try {
        for(std::size_t i=0;i<2;++i) {
            records[i]=std::make_shared<connection_record>(capacity.slots[i]);
            register_record(records[i]);capacity.handed_off[i]=true;
            records[i]->payload->workers=ordinary_managed_ipc_workers::reserve();
            auto actual_role=std::make_shared<const ordinary_managed_ipc_role::implementation>(route,i==0);
            records[i]->payload->role=std::shared_ptr<const ordinary_managed_ipc_role>(new ordinary_managed_ipc_role(std::move(actual_role)));
        }
        validate_local_route(*route,deadline); // actual sequential owned views
        // Allocate every throwing wrapper before actual descriptor creation.
        auto first_workers=make_workers(records[0]->payload->workers),second_workers=make_workers(records[1]->payload->workers);
        ordinary_managed_ipc_connection first(std::make_unique<ordinary_managed_ipc_connection::implementation>(records[0]));
        ordinary_managed_ipc_connection second(std::make_unique<ordinary_managed_ipc_connection::implementation>(records[1]));
        int sockets[2]{-1,-1};
        #if defined(__linux__)
        constexpr int socket_type=SOCK_STREAM|SOCK_CLOEXEC|SOCK_NONBLOCK;
        #else
        constexpr int socket_type=SOCK_STREAM;
        #endif
        if(::socketpair(AF_UNIX,socket_type,0,sockets)!=0)
            throw std::system_error(errno,std::generic_category(),"managed IPC local socketpair");
        records[0]->fd=sockets[0];records[1]->fd=sockets[1];
        #if defined(__APPLE__)
        // Darwin lacks atomic socketpair CLOEXEC. The enrolled managed runtime
        // independently forbids child fork/exec; inherited API use also refuses.
        // This is not a claim of an OS-atomic descriptor flag transition.
        for(int fd:sockets) {
            const int flags=::fcntl(fd,F_GETFL,0);
            if(flags<0||::fcntl(fd,F_SETFL,flags|O_NONBLOCK)<0||::fcntl(fd,F_SETFD,FD_CLOEXEC)<0)
                throw std::system_error(errno,std::generic_category(),"managed IPC descriptor flags");
        }
        #endif
        validate_local_route(*route,deadline);
        for(const auto& record:records)record->payload->role->validate(ordinary_managed_ipc_operation::publication);
        require(clock::now()<deadline,"managed IPC local publication expired");
        for(const auto& record:records)record->published=true;
        capacity.committed=true;
        return {ordinary_managed_ipc_issued(std::move(first),std::move(first_workers)),
                ordinary_managed_ipc_issued(std::move(second),std::move(second_workers))};
    } catch(...) {
        // No user callback or worker has been admitted; do not erase a record
        // if even this no-users actual close/worker settlement is uncertain.
        for(const auto& record:records)discard_unpublished(record);
        throw;
    }
}


// Narrow wrappers keep the link's closed friend boundary in the actual IPC
// issuer. None accepts an fd, decoded facts, callback or settlement Boolean.
std::shared_ptr<ordinary_managed_cohort_link> ordinary_managed_ipc_access::claim_cohort_link(
    const ordinary_managed_profile& p,lattice_db& owner,database& writer,ordinary_launch::deadline end){return p.claim_cohort_link(owner,writer,end);}
ordinary_managed_cohort_link_facts ordinary_managed_ipc_access::cohort_link_facts(
    const std::shared_ptr<ordinary_managed_cohort_link>& p,ordinary_launch::deadline end){require(bool(p),"managed peer link absent");return p->facts(end);}
int ordinary_managed_ipc_access::cohort_link_descriptor(const std::shared_ptr<ordinary_managed_cohort_link>& p,ordinary_launch::deadline end){require(bool(p),"managed peer link absent");return p->descriptor(end);}
void ordinary_managed_ipc_access::validate_cohort_link(const std::shared_ptr<ordinary_managed_cohort_link>& p,ordinary_launch::deadline end){require(bool(p),"managed peer link absent");p->validate(end);}
void ordinary_managed_ipc_access::stop_cohort_link(const std::shared_ptr<ordinary_managed_cohort_link>& p)noexcept{if(p)p->request_stop();}
ordinary_managed_cohort_link_settlement ordinary_managed_ipc_access::settle_cohort_link(
    const std::shared_ptr<ordinary_managed_cohort_link>& p,ordinary_launch::deadline end)noexcept{
    if(!p)return {ordinary_managed_cohort_link_settlement_state::clean,0,std::chrono::steady_clock::now()<end};
    return p->settle_descriptor(end);
}
std::shared_ptr<ordinary_managed_ipc_endpoint> ordinary_managed_ipc_access::start_peer_endpoint(
    std::shared_ptr<lattice_db> owner,clock::time_point end){
    auto issued=make_cohort_connection(std::move(owner),end);
    auto delivery=ordinary_managed_ipc_delivery::for_peer(issued.connection.role().peer_owner());
    return ordinary_managed_ipc_endpoint_access::start(std::move(issued),std::move(delivery),end);
}
void ordinary_managed_ipc_access::submit_peer_observation(const std::shared_ptr<ordinary_managed_ipc_endpoint>& endpoint,
    std::shared_ptr<const ordinary_managed_ipc_local_route> route,const std::string& scope,const std::string& gid,clock::time_point end){
    ordinary_managed_ipc_endpoint_access::submit(endpoint,std::move(route),scope,gid,end);
}
ordinary_managed_ipc_issued ordinary_managed_ipc_access::make_cohort_connection(
    std::shared_ptr<lattice_db> owner,clock::time_point end) {
    same_process();require(end!=clock::time_point::max()&&clock::now()<end,"managed peer requires original finite deadline");
    // One endpoint per actual child. Reserve a real native slot before retaining
    // its application owner/profile or claiming the one-shot inherited link.
    std::size_t slot=connection_limit;
    {auto& owners=registry();std::lock_guard lock(owners.leaf);for(std::size_t i=0;i<connection_limit;++i)
        if(!owners.reserved[i]){owners.reserved[i]=true;slot=i;break;}}
    require(slot!=connection_limit,"managed IPC connection capacity exhausted");
    std::shared_ptr<connection_record> record;
    try{
        record=std::make_shared<connection_record>(slot);register_record(record);
        record->payload->workers=ordinary_managed_ipc_workers::reserve();
        auto workers=make_workers(record->payload->workers);
        ordinary_managed_ipc_connection connection(std::make_unique<ordinary_managed_ipc_connection::implementation>(record));
        ordinary_export_access::retain_managed_peer_owner(std::move(owner),end,record->payload->peer);
        record->fd=record->payload->peer->descriptor(); // stable borrow, never separately close
        auto role=std::make_shared<const ordinary_managed_ipc_role::implementation>(record->payload->peer);
        record->payload->role=std::shared_ptr<const ordinary_managed_ipc_role>(new ordinary_managed_ipc_role(std::move(role)));
        record->payload->peer->validate(end);record->payload->role->validate(ordinary_managed_ipc_operation::publication);
        require(clock::now()<end,"managed peer publication expired");record->published=true;
        return ordinary_managed_ipc_issued(std::move(connection),std::move(workers));
    }catch(...){
        if(record&&record->registered)discard_unpublished(record,end);
        else{auto& owners=registry();std::lock_guard lock(owners.leaf);owners.reserved[slot]=false;}
        throw;
    }
}

ordinary_managed_ipc_connection_probe exercise_managed_ipc_unpublished_cleanup(bool failing_worker) {
    same_process();
    const auto deadline=clock::now()+std::chrono::seconds(5);
    reservation_pair capacity;
    const auto slots=capacity.slots;
    std::array<std::shared_ptr<connection_record>,2> records;
    struct gate_state {
        std::mutex mutex;std::condition_variable changed;
        bool entered=false,released=false;
        std::atomic<bool> capture_destroyed{false};
    };
    const auto gate=std::make_shared<gate_state>();
    auto release_gate=[&]{
        {std::lock_guard lock(gate->mutex);gate->released=true;}
        gate->changed.notify_all();
    };
    ordinary_managed_ipc_connection_probe result;
    try {
        for(std::size_t i=0;i<2;++i) {
            records[i]=std::make_shared<connection_record>(slots[i]);
            register_record(records[i]);capacity.handed_off[i]=true;
            records[i]->payload->workers=ordinary_managed_ipc_workers::reserve();
        }
        int sockets[2]{-1,-1};
        if(::socketpair(AF_UNIX,SOCK_STREAM,0,sockets)!=0)
            throw std::system_error(errno,std::generic_category(),"managed IPC cleanup probe socketpair");
        records[0]->fd=sockets[0];records[1]->fd=sockets[1];
        // This fixed probe never publishes the descriptor or a role and never
        // starts a protocol worker. The real ownership record runs one actual
        // gated caller thread solely to test the cleanup prerequisite.
        auto payload=std::shared_ptr<int>(new int(1),[gate](int* value){
            delete value;gate->capture_destroyed.store(true,std::memory_order_release);
        });
        records[0]->payload->workers->start(ordinary_managed_ipc_workers::slot::reader,
            [gate,payload=std::move(payload),failing_worker]{
                {
                    std::unique_lock lock(gate->mutex);gate->entered=true;gate->changed.notify_all();
                    gate->changed.wait(lock,[&]{return gate->released;});
                }
                if(failing_worker)throw std::runtime_error("actual managed IPC probe worker failed");
            });
        {
            std::unique_lock lock(gate->mutex);
            require(gate->changed.wait_until(lock,deadline,[&]{return gate->entered;}),"managed IPC probe worker never entered");
        }
        records[0]->stop();records[1]->stop();
        result.descriptor_open_while_worker_held=::fcntl(sockets[0],F_GETFD,0)>=0;
        result.release_refused_while_worker_held=!release_record(records[0]);
        result.expired_join_refused=!records[0]->payload->workers->join(clock::now());
        release_gate();
        result.real_worker_joined=records[0]->payload->workers->join(deadline);
        result.capture_destroyed_before_join_return=gate->capture_destroyed.load(std::memory_order_acquire);
        const auto actual=records[0]->payload->workers->observe();
        result.primary_failure_preserved=bool(actual.first_error)==failing_worker&&!actual.join_error;
        result.both_records_released=release_record(records[0])&&release_record(records[1]);
        {
            std::lock_guard lock(records[0]->leaf);
            result.descriptor_closed_after_join=records[0]->close_attempted&&records[0]->closed&&
                records[0]->fd==-1&&!records[0]->close_error;
        }
        {
            auto& owners=registry();std::lock_guard lock(owners.leaf);
            result.capacity_held_by_collected_shell=owners.reserved[slots[0]]&&owners.reserved[slots[1]]&&
                !owners.records[slots[0]]&&!owners.records[slots[1]];
        }
        records={}; // actual final record/role/capture destruction, off-leaf
        {
            auto& owners=registry();std::lock_guard lock(owners.leaf);
            result.capacity_released_after_last_shell=!owners.reserved[slots[0]]&&!owners.reserved[slots[1]];
        }
        require(clock::now()<deadline,"managed IPC probe exceeded original observation deadline");
        return result;
    } catch(...) {
        // Test failure stays failure even if this separately bounded emergency
        // cleanup succeeds. No detached worker or fabricated join completion.
        const auto failure=std::current_exception();release_gate();
        const auto cleanup_deadline=clock::now()+std::chrono::seconds(5);
        for(const auto& record:records)if(record){
            record->stop();
            try{if(record->payload->workers)(void)record->payload->workers->join(cleanup_deadline);}catch(...){}
            discard_unpublished(record);
        }
        std::rethrow_exception(failure);
    }
}
}
#else
namespace lattice::detail {
struct ordinary_managed_ipc_role::implementation {};
struct ordinary_managed_ipc_connection::implementation {};
ordinary_managed_ipc_role::ordinary_managed_ipc_role(std::shared_ptr<const implementation> value):impl_(std::move(value)){}
ordinary_managed_ipc_role::~ordinary_managed_ipc_role()=default;
void ordinary_managed_ipc_role::validate(ordinary_managed_ipc_operation)const {throw std::runtime_error("managed IPC platform unavailable");}
const std::shared_ptr<ordinary_managed_peer_owner>& ordinary_managed_ipc_role::peer_owner()const{throw std::runtime_error("managed IPC platform unavailable");}
ordinary_managed_ipc_connection::ordinary_managed_ipc_connection(std::unique_ptr<implementation> value):impl_(std::move(value)){}
ordinary_managed_ipc_connection::~ordinary_managed_ipc_connection()=default;
ordinary_managed_ipc_connection::ordinary_managed_ipc_connection(ordinary_managed_ipc_connection&&)noexcept=default;
ordinary_managed_ipc_connection& ordinary_managed_ipc_connection::operator=(ordinary_managed_ipc_connection&&)noexcept=default;
int ordinary_managed_ipc_connection::descriptor()const{throw std::runtime_error("managed IPC platform unavailable");}
const ordinary_managed_ipc_role& ordinary_managed_ipc_connection::role()const{throw std::runtime_error("managed IPC platform unavailable");}
void ordinary_managed_ipc_connection::request_stop()noexcept{}
void ordinary_managed_ipc_connection::validate(ordinary_managed_ipc_operation)const{throw std::runtime_error("managed IPC platform unavailable");}
bool ordinary_managed_ipc_connection::release_after_settlement(std::chrono::steady_clock::time_point){return false;}
ordinary_managed_ipc_issued::ordinary_managed_ipc_issued(ordinary_managed_ipc_connection&& c,ordinary_managed_ipc_worker_owner&& w):connection(std::move(c)),workers(std::move(w)){}
ordinary_managed_ipc_issued::ordinary_managed_ipc_issued(ordinary_managed_ipc_issued&&)noexcept=default;
ordinary_managed_ipc_issued& ordinary_managed_ipc_issued::operator=(ordinary_managed_ipc_issued&&)noexcept=default;
std::pair<ordinary_managed_ipc_issued,ordinary_managed_ipc_issued> ordinary_managed_ipc_access::make_local_connections(
    std::shared_ptr<const ordinary_managed_ipc_local_route>,std::chrono::steady_clock::time_point){throw std::runtime_error("managed IPC platform unavailable");}
std::shared_ptr<ordinary_managed_cohort_link> ordinary_managed_ipc_access::claim_cohort_link(const ordinary_managed_profile&,lattice_db&,database&,ordinary_launch::deadline){throw std::runtime_error("managed IPC platform unavailable");}
ordinary_managed_cohort_link_facts ordinary_managed_ipc_access::cohort_link_facts(const std::shared_ptr<ordinary_managed_cohort_link>&,ordinary_launch::deadline){throw std::runtime_error("managed IPC platform unavailable");}
int ordinary_managed_ipc_access::cohort_link_descriptor(const std::shared_ptr<ordinary_managed_cohort_link>&,ordinary_launch::deadline){throw std::runtime_error("managed IPC platform unavailable");}
void ordinary_managed_ipc_access::validate_cohort_link(const std::shared_ptr<ordinary_managed_cohort_link>&,ordinary_launch::deadline){throw std::runtime_error("managed IPC platform unavailable");}
void ordinary_managed_ipc_access::stop_cohort_link(const std::shared_ptr<ordinary_managed_cohort_link>&)noexcept{}
ordinary_managed_cohort_link_settlement ordinary_managed_ipc_access::settle_cohort_link(const std::shared_ptr<ordinary_managed_cohort_link>&,ordinary_launch::deadline)noexcept{return {ordinary_managed_cohort_link_settlement_state::uncertain,ENOTSUP,false};}
ordinary_managed_ipc_issued ordinary_managed_ipc_access::make_cohort_connection(std::shared_ptr<lattice_db>,std::chrono::steady_clock::time_point){throw std::runtime_error("managed IPC platform unavailable");}
std::shared_ptr<ordinary_managed_ipc_endpoint> ordinary_managed_ipc_access::start_peer_endpoint(std::shared_ptr<lattice_db>,std::chrono::steady_clock::time_point){throw std::runtime_error("managed IPC platform unavailable");}
void ordinary_managed_ipc_access::submit_peer_observation(const std::shared_ptr<ordinary_managed_ipc_endpoint>&,std::shared_ptr<const ordinary_managed_ipc_local_route>,const std::string&,const std::string&,std::chrono::steady_clock::time_point){throw std::runtime_error("managed IPC platform unavailable");}
ordinary_managed_ipc_connection_probe exercise_managed_ipc_unpublished_cleanup(bool){throw std::runtime_error("managed IPC platform unavailable");}
}
#endif
