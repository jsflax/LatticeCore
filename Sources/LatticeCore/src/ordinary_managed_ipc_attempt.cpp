#include "ordinary_managed_ipc_attempt.hpp"
#include <lattice/ordinary_sync.hpp>
#include <cerrno>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <system_error>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <sys/socket.h>
#include <unistd.h>

namespace lattice::detail {
static_assert(ordinary_ipc_maximum_frame_bytes == ordinary_sync_limits::frame_bytes);
struct ordinary_managed_ipc_attempt::state {
    const pid_t process = ::getpid();
    std::mutex mutex;
    std::optional<ordinary_managed_ipc_connection> connection;
    std::atomic<bool> stopped{false};
    observation facts;
    explicit state(ordinary_managed_ipc_connection&& value) : connection(std::move(value)) {}
    bool same_process() const noexcept { return ::getpid() == process; }
    struct check_context { state& owner; ordinary_managed_ipc_operation operation; };
    static void check(void* raw) {
        const auto& context = *static_cast<check_context*>(raw);
        // Active operation/terminal counts forbid collect from releasing this
        // exact connection. No attempt mutex is held while checking the role.
        context.owner.connection->validate(context.operation);
    }
    ordinary_ipc_result begin(bool reading) noexcept {
        ordinary_ipc_result result;
        if (!same_process()) { result.code = ordinary_ipc_code::admission_refused; result.system_error = ECHILD; return result; }
        std::lock_guard<std::mutex> lock(mutex);
        if (facts.stopping || facts.collecting || facts.collected || !connection) {
            result.code = ordinary_ipc_code::stopped; return result;
        }
        auto& active = reading ? facts.reader : facts.writer;
        if (active) { result.code = ordinary_ipc_code::busy; return result; }
        active = true; return result;
    }
    void finish(bool reading, const ordinary_ipc_result& result) noexcept {
        if(!same_process())return; // callback return/throw may be in a fork child
        std::lock_guard<std::mutex> lock(mutex);
        if (!result.succeeded() && result.code!=ordinary_ipc_code::idle && facts.first_failure.succeeded()) facts.first_failure = result;
        if (reading) { facts.callback = false; facts.reader = false; }
        else facts.writer = false;
    }
};

ordinary_managed_ipc_attempt::ordinary_managed_ipc_attempt(ordinary_managed_ipc_connection&& connection)
    : state_(new state(std::move(connection)),[creator=::getpid()](state* value){
        if(creator==::getpid())delete value; // inherited final control block quarantines actual state
    }) {
    // The issuer registered actual custody before returning the connection.
    // Failure here cannot turn its destructor into positive cleanup proof.
    try {
        state_->connection->validate(ordinary_managed_ipc_operation::publication);
        const auto prepared = ordinary_ipc_prepare_socket(state_->connection->descriptor());
        if (!prepared.succeeded()) throw std::system_error(prepared.system_error, std::generic_category(), "managed IPC socket preparation");
    } catch (...) { stop(state_); throw; }
}
ordinary_managed_ipc_attempt::~ordinary_managed_ipc_attempt() { stop(state_); }

void ordinary_managed_ipc_attempt::stop(const std::shared_ptr<state>& keep) noexcept {
    if (!keep->same_process()) return; // Do not enter inherited leaves or signal a parent's endpoint.
    {
        std::lock_guard<std::mutex> lock(keep->mutex);
        if (keep->facts.stopping) return;
        keep->facts.stopping = true;
        keep->stopped.store(true, std::memory_order_release);
        ++keep->facts.terminal_calls;
    }
    int error = 0; std::exception_ptr exception;
    try {
        const int descriptor = keep->connection->descriptor();
        keep->connection->request_stop();
        if (::shutdown(descriptor, SHUT_RDWR) != 0 && errno != ENOTCONN) error = errno;
    } catch (...) {
        exception = std::current_exception();
        // Closure still occurs if borrowing a now-invalid descriptor refused.
        keep->connection->request_stop();
    }
    if(!keep->same_process())return;
    {
        std::lock_guard<std::mutex> lock(keep->mutex);
        keep->facts.shutdown_error = error;
        if (exception && !keep->facts.cleanup_error) keep->facts.cleanup_error = exception;
        --keep->facts.terminal_calls;
    }
}
void ordinary_managed_ipc_attempt::request_stop() noexcept { stop(state_); }

ordinary_ipc_result ordinary_managed_ipc_attempt::send(std::span<const uint8_t> bytes, ordinary_ipc_deadline deadline) noexcept {
    auto keep = state_;
    auto result = keep->begin(false);
    if (!result.succeeded()) return result;
    try {
        state::check_context context{*keep, ordinary_managed_ipc_operation::send};
        keep->connection->validate(context.operation);
        result = ordinary_ipc_write_frame(keep->connection->descriptor(), bytes, keep->stopped, deadline, {&context, state::check});
        if (result.succeeded()) {
            result.phase = ordinary_ipc_phase::publication;
            keep->connection->validate(ordinary_managed_ipc_operation::publication);
            if (keep->stopped.load(std::memory_order_acquire)) result.code = ordinary_ipc_code::stopped;
            else if (std::chrono::steady_clock::now() >= deadline) result.code = ordinary_ipc_code::timeout;
            else result.phase = ordinary_ipc_phase::complete;
        }
    } catch (...) { result.code = ordinary_ipc_code::admission_refused; result.exception = std::current_exception(); }
    if(!keep->same_process()){result.code=ordinary_ipc_code::admission_refused;result.system_error=ECHILD;return result;}
    // Any admitted framing failure retires this stream, including partial
    // header/payload timeouts. A sibling attempt cannot resume these bytes.
    if (!result.succeeded()) stop(keep);
    keep->finish(false, result);
    return result;
}
ordinary_ipc_result ordinary_managed_ipc_attempt::wait_input(ordinary_ipc_deadline deadline) noexcept {
    auto keep=state_;auto result=keep->begin(true);if(!result.succeeded())return result;
    try{
        state::check_context context{*keep,ordinary_managed_ipc_operation::receive};
        keep->connection->validate(context.operation);
        result=ordinary_ipc_wait_input(keep->connection->descriptor(),keep->stopped,deadline,{&context,state::check});
    }catch(...){result.code=ordinary_ipc_code::admission_refused;result.exception=std::current_exception();}
    if(!keep->same_process()){result.code=ordinary_ipc_code::admission_refused;result.system_error=ECHILD;return result;}
    if(!result.succeeded()&&result.code!=ordinary_ipc_code::idle)stop(keep);
    keep->finish(true,result);return result;
}
ordinary_ipc_result ordinary_managed_ipc_attempt::receive(ordinary_ipc_deadline deadline, const receive_callback& callback) noexcept {
    auto keep = state_;
    auto result = keep->begin(true);
    if (!result.succeeded()) return result;
    try {
        state::check_context context{*keep, ordinary_managed_ipc_operation::receive};
        keep->connection->validate(context.operation);
        auto frame = ordinary_ipc_read_frame(keep->connection->descriptor(), keep->stopped, deadline, {&context, state::check});
        result = frame.result;
        if (result.succeeded()) {
            result.phase = ordinary_ipc_phase::publication;
            keep->connection->validate(ordinary_managed_ipc_operation::publication);
            {
                std::lock_guard<std::mutex> lock(keep->mutex);
                if (keep->facts.stopping) result.code = ordinary_ipc_code::stopped;
                else if (std::chrono::steady_clock::now() >= deadline) result.code = ordinary_ipc_code::timeout;
                else keep->facts.callback = true;
            }
            if (result.succeeded()) {
                try {
                    if (!callback) throw std::invalid_argument("managed IPC receive requires its actual consumer");
                    callback(frame.bytes, keep->connection->role());
                    // A callback may request retirement after actually consuming
                    // this frame. Preserve that consumption rather than label it
                    // an unconsumed transport timeout after the callback returns.
                    result.phase = ordinary_ipc_phase::complete;
                } catch (...) { result.code = ordinary_ipc_code::callback_error; result.exception = std::current_exception(); }
            }
        }
        // frame bytes are destroyed here while the actual read remains held.
    } catch (...) { result.code = ordinary_ipc_code::admission_refused; result.exception = std::current_exception(); }
    if(!keep->same_process()){result.code=ordinary_ipc_code::admission_refused;result.system_error=ECHILD;return result;}
    if (!result.succeeded()) stop(keep);
    keep->finish(true, result);
    return result;
}
bool ordinary_managed_ipc_attempt::collect() noexcept {return collect(ordinary_ipc_deadline::max());}
bool ordinary_managed_ipc_attempt::collect(ordinary_ipc_deadline deadline) noexcept {
    auto keep = state_;
    if (!keep->same_process()) return false;
    {
        std::lock_guard<std::mutex> lock(keep->mutex);
        if (keep->facts.collected) return true;
        if (!keep->facts.stopping || keep->facts.collecting || keep->facts.reader || keep->facts.writer ||
            keep->facts.callback || keep->facts.terminal_calls) return false;
        keep->facts.collecting = true;
    }
    bool released = false; std::exception_ptr error;
    try { released = keep->connection->release_after_settlement(deadline); }
    catch (...) { error = std::current_exception(); }
    if (released) {
        // Admission is closed and collecting prevents concurrent release. Final
        // descriptor/role/capture destruction is deliberately outside the leaf.
        keep->connection.reset();
    }
    if(!keep->same_process())return false; // final context destruction may fork
    {
        std::lock_guard<std::mutex> lock(keep->mutex);
        if (error && !keep->facts.cleanup_error) keep->facts.cleanup_error = error;
        keep->facts.collected = released;
        keep->facts.collecting = false;
    }
    return released;
}
ordinary_managed_ipc_attempt::observation ordinary_managed_ipc_attempt::observe() const noexcept {
    auto keep = state_;
    if (!keep->same_process()) {
        observation result; result.stopping = true; result.first_failure.code = ordinary_ipc_code::admission_refused;
        result.first_failure.system_error = ECHILD; return result;
    }
    std::lock_guard<std::mutex> lock(keep->mutex); return keep->facts;
}
}
#else
namespace lattice::detail {
struct ordinary_managed_ipc_attempt::state {};
ordinary_managed_ipc_attempt::ordinary_managed_ipc_attempt(ordinary_managed_ipc_connection&&) {
    throw std::runtime_error("managed IPC requires a supported native socket platform");
}
ordinary_managed_ipc_attempt::~ordinary_managed_ipc_attempt() = default;
void ordinary_managed_ipc_attempt::stop(const std::shared_ptr<state>&) noexcept {}
void ordinary_managed_ipc_attempt::request_stop() noexcept {}
ordinary_ipc_result ordinary_managed_ipc_attempt::send(std::span<const uint8_t>, ordinary_ipc_deadline) noexcept {
    ordinary_ipc_result result; result.code = ordinary_ipc_code::admission_refused; result.system_error = ENOTSUP; return result;
}
ordinary_ipc_result ordinary_managed_ipc_attempt::wait_input(ordinary_ipc_deadline) noexcept { return send({}, {}); }
ordinary_ipc_result ordinary_managed_ipc_attempt::receive(ordinary_ipc_deadline, const receive_callback&) noexcept { return send({}, {}); }
bool ordinary_managed_ipc_attempt::collect() noexcept { return false; }
bool ordinary_managed_ipc_attempt::collect(ordinary_ipc_deadline) noexcept { return false; }
ordinary_managed_ipc_attempt::observation ordinary_managed_ipc_attempt::observe() const noexcept { return {}; }
}
#endif
