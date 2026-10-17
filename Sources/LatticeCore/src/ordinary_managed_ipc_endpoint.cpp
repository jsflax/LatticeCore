#include "ordinary_managed_ipc_endpoint.hpp"
#include <cerrno>
#include <algorithm>
#include <stdexcept>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <unistd.h>

namespace lattice::detail {
struct ordinary_managed_ipc_endpoint::state {
    const pid_t process=::getpid();
    // Issuer promises this address is stable until the attempt collects.
    const ordinary_managed_ipc_role* role;
    ordinary_managed_ipc_attempt attempt;
    ordinary_managed_ipc_worker_owner workers;
    std::unique_ptr<ordinary_managed_ipc_delivery> delivery;
    // The startup deadline bounds startup only. Each newly admitted frame has
    // a finite window no greater than that original window; partial frames,
    // queued replies and a multi-frame request never restart their deadlines.
    const std::chrono::steady_clock::duration operation_window;
    ordinary_ipc_deadline operation_end()const noexcept{return std::chrono::steady_clock::now()+operation_window;}
    ordinary_ipc_deadline idle_end()const noexcept{return std::chrono::steady_clock::now()+
        std::min(operation_window,std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::seconds(2)));}
    mutable std::mutex mutex;
    std::condition_variable changed;
    observation facts;
    size_t stop_calls=0,submissions=0;
    bool dirty=true,retiring=false;
    std::optional<ordinary_managed_ipc_output> response;
    state(ordinary_managed_ipc_issued&& issued,
          std::unique_ptr<ordinary_managed_ipc_delivery>&& consumer,
          ordinary_ipc_deadline deadline)
        :role(&issued.connection.role()),attempt(std::move(issued.connection)),
         workers(std::move(issued.workers)),delivery(std::move(consumer)),operation_window(std::min(deadline-std::chrono::steady_clock::now(),std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::seconds(10)))) {}
    bool same_process() const noexcept { return process==::getpid(); }
};

ordinary_managed_ipc_endpoint::ordinary_managed_ipc_endpoint(
    ordinary_managed_ipc_issued&& issued,
    std::unique_ptr<ordinary_managed_ipc_delivery>&& delivery, ordinary_ipc_deadline deadline)
    :state_(std::make_unique<state>(std::move(issued),std::move(delivery),deadline)) {}
ordinary_managed_ipc_endpoint::~ordinary_managed_ipc_endpoint() {
    if(state_&&!state_->same_process()){(void)state_.release();return;}
    request_stop();
    // Registry retention is the normal cleanup owner. A dropped wrapper never
    // destroys unfinished worker/descriptor/foreign capture custody.
    if(state_&&!state_->facts.collected)(void)state_.release();
}

void ordinary_managed_ipc_endpoint::record_failure(const ordinary_ipc_result& failure) noexcept {
    if(!state_->same_process())return;
    std::lock_guard lock(state_->mutex);
    if(!failure.succeeded()&&state_->facts.first_failure.succeeded())state_->facts.first_failure=failure;
}
void ordinary_managed_ipc_endpoint::notify_changed() noexcept {
    if(!state_->same_process())return;
    {std::lock_guard lock(state_->mutex);if(state_->facts.stopping)return;state_->dirty=true;}
    state_->changed.notify_all();
}
void ordinary_managed_ipc_endpoint::request_stop() noexcept {
    if(!state_->same_process())return;
    {std::lock_guard lock(state_->mutex);if(state_->facts.stopping)return;state_->facts.stopping=true;++state_->stop_calls;}
    // Never retain the endpoint leaf across native closure, socket shutdown or
    // delivery cancellation. Issuer retains the descriptor; shutdown is not close.
    state_->attempt.request_stop();
    state_->workers.request_stop();
    if(state_->delivery)state_->delivery->request_stop();
    if(!state_->same_process())return;
    {std::lock_guard lock(state_->mutex);--state_->stop_calls;}
    state_->changed.notify_all();
}

void ordinary_managed_ipc_endpoint::reader() noexcept {
    if(!state_->same_process())return;
    try {
        for(;;){
            {
                std::unique_lock lock(state_->mutex);
                const auto waiting=state_->response?state_->response->deadline(state_->operation_end()):state_->idle_end();
                if(!state_->changed.wait_until(lock,waiting,[&]{return state_->facts.stopping||!state_->response;})){
                    lock.unlock();ordinary_ipc_result failure;failure.code=ordinary_ipc_code::timeout;record_failure(failure);break;
                }
                if(state_->facts.stopping)break;
            }
            // One actual reader owns the response reservation until this call
            // returns; no second input can race it into an unbounded mailbox.
            const auto ready=state_->attempt.wait_input(state_->delivery->wait_deadline(state_->idle_end()));
            if(!state_->same_process())return;
            if(ready.code==ordinary_ipc_code::idle)continue;
            if(!ready.succeeded()){record_failure(ready);break;}
            const auto end=state_->delivery->wait_deadline(state_->operation_end());
            std::optional<ordinary_managed_ipc_output> response;
            const auto result=state_->attempt.receive(end,
                [&](std::span<const uint8_t> bytes,const ordinary_managed_ipc_role& role){
                    if(&role!=state_->role)throw std::logic_error("managed IPC changed its exact role");
                    response=state_->delivery->consume_input(role,bytes,end);
                });
            if(!state_->same_process())return;
            if(!result.succeeded()){record_failure(result);break;}
            {
                std::lock_guard lock(state_->mutex);
                if(!state_->facts.stopping){
                    if(response)state_->response.emplace(std::move(*response));
                    state_->dirty=true;
                }
            }
            // A response discarded during retirement does not clear its real
            // committed request/repair obligation. A fresh attempt must retry.
            state_->changed.notify_all();
        }
    }catch(...){ordinary_ipc_result failure;failure.code=ordinary_ipc_code::callback_error;failure.exception=std::current_exception();record_failure(failure);}
    if(!state_->same_process())return;
    request_stop();
    {std::lock_guard lock(state_->mutex);state_->facts.reader_returned=true;}
    state_->changed.notify_all();
}
void ordinary_managed_ipc_endpoint::writer() noexcept {
    if(!state_->same_process())return;
    try {
        bool prefer_response=true;
        for(;;){
            std::optional<ordinary_managed_ipc_output> output;
            bool prepare=false;
            {
                std::lock_guard lock(state_->mutex);
                if(state_->facts.stopping)break;
                if(state_->response&&prefer_response){
                    output.emplace(std::move(*state_->response));state_->response.reset();
                }else {state_->dirty=false;prepare=true;}
            }
            state_->changed.notify_all();
            if(prepare)output=state_->delivery->prepare_output(*state_->role,state_->delivery->wait_deadline(state_->operation_end()));
            if(!state_->same_process())return;
            if(!output){
                const auto waiting=state_->delivery->wait_deadline(state_->idle_end());
                std::unique_lock lock(state_->mutex);
                prefer_response=true;
                (void)state_->changed.wait_until(lock,waiting,[&]{return state_->facts.stopping||state_->response||state_->dirty;});
                // Next iteration checks actual pending expiry. An idle slice
                // does not turn a request timeout into success or renew a role.
                continue;
            }
            const auto end=output->deadline(state_->operation_end());
            output->validate_before_send(*state_->role,end);
            if(!state_->same_process())return;
            const auto result=state_->attempt.send(output->frame(),end);
            if(!state_->same_process())return;
            if(!result.succeeded()){record_failure(result);break;}
            state_->delivery->output_written(*state_->role,*output);
            if(!state_->same_process())return;
            // Alternate a queued response with a durable-output opportunity;
            // either lane can be absent without spinning or dropping a frame.
            prefer_response=prepare;
            // Output/issued-frame captures are destroyed off the endpoint leaf.
        }
    }catch(...){ordinary_ipc_result failure;failure.code=ordinary_ipc_code::callback_error;failure.exception=std::current_exception();record_failure(failure);}
    if(!state_->same_process())return;
    request_stop();
    {std::lock_guard lock(state_->mutex);state_->facts.writer_returned=true;}
    state_->changed.notify_all();
}

bool ordinary_managed_ipc_endpoint::retire(ordinary_ipc_deadline deadline) noexcept {
    if(!state_->same_process()||deadline==ordinary_ipc_deadline::max())return false;
    request_stop();
    {
        std::unique_lock lock(state_->mutex);
        if(!state_->changed.wait_until(lock,deadline,[&]{return !state_->retiring;}))return false;
        if(state_->facts.collected)return true;
        state_->retiring=true;
    }
    // Serialize actual retirement using a charged operation, not a mutex held
    // across joins, SQL settlement, or final owner/callback destruction.
    struct retirement_call {state& owner;~retirement_call(){
        if(!owner.same_process())return;
        {std::lock_guard lock(owner.mutex);owner.retiring=false;}owner.changed.notify_all();
    }} current{*state_};
    try {
        // This method, not a sampled returned flag, performs the actual joins.
        if(!state_->workers.join(deadline))return false;
        if(!state_->same_process())return false;
        {
            std::unique_lock lock(state_->mutex);
            if(!state_->changed.wait_until(lock,deadline,[&]{return state_->stop_calls==0&&state_->submissions==0;}))return false;
        }
        if(std::chrono::steady_clock::now()>=deadline)return false;
        std::optional<ordinary_managed_ipc_output> response;
        {std::lock_guard lock(state_->mutex);if(state_->response){response.emplace(std::move(*state_->response));state_->response.reset();}}
        response.reset();
        if(!state_->same_process())return false;
        if(state_->delivery){
            if(!state_->delivery->retire(deadline))return false;
            if(!state_->same_process())return false;
            state_->delivery.reset(); // Actual captures destroyed outside leaves.
            if(!state_->same_process())return false;
            std::lock_guard lock(state_->mutex);state_->facts.delivery_retired=true;
        }
        if(!state_->attempt.collect(deadline))return false;
        if(!state_->same_process())return false;
        {std::lock_guard lock(state_->mutex);state_->facts.collected=true;}
        return true;
    }catch(...){if(!state_->same_process())return false;
        std::lock_guard lock(state_->mutex);if(!state_->facts.cleanup_error)state_->facts.cleanup_error=std::current_exception();return false;}
}
ordinary_managed_ipc_endpoint::observation ordinary_managed_ipc_endpoint::observe() const noexcept {
    if(!state_->same_process()){observation value;value.stopping=true;value.first_failure.code=ordinary_ipc_code::admission_refused;value.first_failure.system_error=ECHILD;return value;}
    std::lock_guard lock(state_->mutex);auto result=state_->facts;result.response_pending=state_->response.has_value();return result;
}
void ordinary_managed_ipc_endpoint_access::bind_route(const std::shared_ptr<ordinary_managed_ipc_endpoint>& endpoint,
    std::shared_ptr<const ordinary_managed_ipc_local_route> route,ordinary_ipc_deadline end){
    if(!endpoint||!endpoint->state_->same_process())throw std::runtime_error("managed peer endpoint absent or inherited");
    {std::lock_guard lock(endpoint->state_->mutex);if(endpoint->state_->facts.stopping)throw std::runtime_error("managed peer route admission closed");++endpoint->state_->submissions;}
    struct complete {ordinary_managed_ipc_endpoint& owner;~complete(){
        if(!owner.state_->same_process())return;
        {std::lock_guard lock(owner.state_->mutex);--owner.state_->submissions;}owner.state_->changed.notify_all();
    }} finish{*endpoint};
    endpoint->state_->delivery->bind_route(std::move(route),std::min(end,endpoint->state_->operation_end()));
    if(!endpoint->state_->same_process())throw std::runtime_error("managed peer route publication inherited");
    endpoint->notify_changed();
}
void ordinary_managed_ipc_access::bind_peer_route(const std::shared_ptr<ordinary_managed_ipc_endpoint>& endpoint,
    std::shared_ptr<const ordinary_managed_ipc_local_route> route,ordinary_ipc_deadline end){
    ordinary_managed_ipc_endpoint_access::bind_route(endpoint,std::move(route),end);
}
void ordinary_managed_ipc_endpoint_access::submit(const std::shared_ptr<ordinary_managed_ipc_endpoint>& endpoint,
    std::shared_ptr<const ordinary_managed_ipc_local_route> route,const std::string& scope,const std::string& gid,ordinary_ipc_deadline end){
    if(!endpoint||!endpoint->state_->same_process())throw std::runtime_error("managed peer endpoint absent or inherited");
    // Submission is counted across capture/owned SQL and off-lock delivery;
    // retirement may not destroy its delivery owner concurrently.
    {
        std::lock_guard lock(endpoint->state_->mutex);
        if(endpoint->state_->facts.stopping)throw std::runtime_error("managed peer submission closed");
        ++endpoint->state_->submissions;
    }
    struct complete {ordinary_managed_ipc_endpoint& owner;~complete(){
        if(!owner.state_->same_process())return;
        {std::lock_guard lock(owner.state_->mutex);--owner.state_->submissions;}owner.state_->changed.notify_all();
    }} finish{*endpoint};
    try{
        endpoint->state_->delivery->submit(std::move(route),scope,gid,std::min(end,endpoint->state_->operation_end()));
        if(!endpoint->state_->same_process())throw std::runtime_error("managed peer submission inherited");
        endpoint->notify_changed();
    }catch(...){
        if(endpoint->state_->same_process()){
            ordinary_ipc_result failure;failure.code=ordinary_ipc_code::callback_error;failure.exception=std::current_exception();
            endpoint->record_failure(failure);endpoint->request_stop();
        }
        throw;
    }
}
std::shared_ptr<ordinary_managed_ipc_endpoint> ordinary_managed_ipc_endpoint_access::start(
    ordinary_managed_ipc_issued&& issued,
    std::unique_ptr<ordinary_managed_ipc_delivery>&& delivery, ordinary_ipc_deadline deadline) {
    if(!delivery||deadline==ordinary_ipc_deadline::max()||std::chrono::steady_clock::now()>=deadline)
        throw std::invalid_argument("managed IPC requires its actual delivery owner and finite live deadline");
    auto result=std::shared_ptr<ordinary_managed_ipc_endpoint>(new ordinary_managed_ipc_endpoint(std::move(issued),std::move(delivery),deadline));
    // Exact fixed-type native retention precedes callback construction/launch.
    try {
        if(std::chrono::steady_clock::now()>=deadline||result->state_->operation_window<=std::chrono::steady_clock::duration::zero())
            throw std::runtime_error("managed IPC startup expired before workers");
        result->state_->workers.bind_endpoint(result);
        result->state_->workers.start_reader([result]{result->reader();});
        result->state_->workers.start_writer([result]{result->writer();});
    }catch(...){
        ordinary_ipc_result failure;failure.code=ordinary_ipc_code::admission_refused;failure.exception=std::current_exception();
        result->record_failure(failure);result->request_stop();
    }
    // Even partial thread-creation failure returns the retained cleanup owner.
    return result;
}
}
#else
namespace lattice::detail {
struct ordinary_managed_ipc_endpoint::state {};
ordinary_managed_ipc_endpoint::ordinary_managed_ipc_endpoint(ordinary_managed_ipc_issued&&,
    std::unique_ptr<ordinary_managed_ipc_delivery>&&,ordinary_ipc_deadline) {
    throw std::runtime_error("managed IPC endpoint requires a supported native platform");
}
ordinary_managed_ipc_endpoint::~ordinary_managed_ipc_endpoint()=default;
void ordinary_managed_ipc_endpoint::reader() noexcept {}
void ordinary_managed_ipc_endpoint::writer() noexcept {}
void ordinary_managed_ipc_endpoint::record_failure(const ordinary_ipc_result&) noexcept {}
void ordinary_managed_ipc_endpoint::notify_changed() noexcept {}
void ordinary_managed_ipc_endpoint::request_stop() noexcept {}
bool ordinary_managed_ipc_endpoint::retire(ordinary_ipc_deadline) noexcept {return false;}
ordinary_managed_ipc_endpoint::observation ordinary_managed_ipc_endpoint::observe() const noexcept {
    observation value;value.stopping=true;value.first_failure.code=ordinary_ipc_code::admission_refused;value.first_failure.system_error=ENOTSUP;return value;
}
void ordinary_managed_ipc_endpoint_access::bind_route(const std::shared_ptr<ordinary_managed_ipc_endpoint>&,
    std::shared_ptr<const ordinary_managed_ipc_local_route>,ordinary_ipc_deadline){throw std::runtime_error("managed IPC unsupported");}
void ordinary_managed_ipc_access::bind_peer_route(const std::shared_ptr<ordinary_managed_ipc_endpoint>&,
    std::shared_ptr<const ordinary_managed_ipc_local_route>,ordinary_ipc_deadline){throw std::runtime_error("managed IPC unsupported");}
void ordinary_managed_ipc_endpoint_access::submit(const std::shared_ptr<ordinary_managed_ipc_endpoint>&,
    std::shared_ptr<const ordinary_managed_ipc_local_route>,const std::string&,const std::string&,ordinary_ipc_deadline){
    throw std::runtime_error("managed IPC endpoint requires a supported native platform");
}
std::shared_ptr<ordinary_managed_ipc_endpoint> ordinary_managed_ipc_endpoint_access::start(
    ordinary_managed_ipc_issued&&,std::unique_ptr<ordinary_managed_ipc_delivery>&&,ordinary_ipc_deadline) {
    throw std::runtime_error("managed IPC endpoint requires a supported native platform");
}
}
#endif
