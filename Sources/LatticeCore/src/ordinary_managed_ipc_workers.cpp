#include "ordinary_managed_ipc_workers.hpp"
#ifdef LATTICE_MANAGED_IPC_WORKER_QUALIFICATION
#include "ordinary_managed_ipc_worker_leaf_fixture.hpp"
#endif
#include <array>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <unistd.h>

namespace lattice::detail {
namespace {
// Check this POD fact before even entering a function-local static's guard.
// A fork during registry initialization must not enter an inherited guard.
const pid_t registry_process = ::getpid();
// Intentionally process-lived. Unknown cleanup and abandoned joins retain a
// bounded charged record; process teardown never destroys a joinable thread.
struct worker_registry {
    const pid_t process = ::getpid();
    std::mutex mutex;
    std::array<bool, ordinary_managed_ipc_workers::maximum_owners> reserved{};
    std::array<std::shared_ptr<void>, ordinary_managed_ipc_workers::maximum_owners> owners;
};
worker_registry& registry() {
    if (::getpid() != registry_process) throw std::runtime_error("inherited managed IPC registry refused");
    static auto* value = new worker_registry;
    if (::getpid() != value->process) throw std::runtime_error("inherited managed IPC registry refused");
    return *value;
}
}
struct ordinary_managed_ipc_workers::state {
    const pid_t process = ::getpid();
    const std::size_t registry_slot;
    std::mutex mutex;
    std::condition_variable condition;
    observation facts;
    bool joining = false;
    bool releasing = false;
    bool worker_owner_claimed = false;
    std::shared_ptr<ordinary_managed_ipc_endpoint> endpoint;
    std::array<std::thread, 2> threads;
    std::array<std::thread::id, 2> executing;
    explicit state(std::size_t value) : registry_slot(value) {}
    bool same_process() const noexcept { return ::getpid() == process; }
    void require_process() const {
        // Kernel PID check precedes the first inherited mutex or thread access.
        // A live process cannot recycle its own PID; a fork has a distinct PID.
        if (!same_process()) throw std::runtime_error("inherited managed IPC worker refused");
    }
    // These capsules are allocated in the creator before any callback or
    // concrete endpoint can run. A forked continuation leaks its inherited
    // capsule rather than traversing owners or entering an inherited leaf.
    struct call {
        const pid_t process = ::getpid();
        std::shared_ptr<state> keep;
        std::function<void()> task;
        std::exception_ptr error;
        call(std::shared_ptr<state> owner, std::function<void()>&& value)
            : keep(std::move(owner)), task(std::move(value)) {}
        static void destroy(call* value) noexcept {
            if (!value || ::getpid() != value->process) return;
            value->task = {};
            if (::getpid() != value->process) return;
            value->error = {};
            if (::getpid() != value->process) return;
            value->keep.reset();
            if (::getpid() != value->process) return;
            delete value; // All callback-capable members are now empty.
        }
    };
    struct retirement {
        const pid_t process = ::getpid();
        std::shared_ptr<state> keep;
        std::shared_ptr<void> retired;
        std::shared_ptr<ordinary_managed_ipc_endpoint> endpoint;
        explicit retirement(std::shared_ptr<state> owner) : keep(std::move(owner)) {}
        static void destroy(retirement* value) noexcept {
            if (!value || ::getpid() != value->process) return;
            value->endpoint.reset();
            if (::getpid() != value->process) return;
            value->retired.reset();
            if (::getpid() != value->process) return;
            value->keep.reset();
            if (::getpid() != value->process) return;
            delete value;
        }
    };
};
#ifdef LATTICE_MANAGED_IPC_WORKER_QUALIFICATION
bool ordinary_managed_ipc_worker_leaf_fixture::hold(
    const std::shared_ptr<ordinary_managed_ipc_workers>& owner,
    std::atomic<bool>& entered,std::atomic<bool>& held,const std::atomic<bool>& release,
    std::chrono::steady_clock::time_point end) {
    static_assert(std::atomic<bool>::is_always_lock_free);
    if(!owner)throw std::invalid_argument("worker qualification owner missing");
    auto keep=owner->state_;keep->require_process();
    // The fixture may acquire only this actual leaf, under its existing
    // absolute budget. No callback, descriptor, profile or copied mutex enters.
    std::unique_lock lock(keep->mutex,std::defer_lock);
    while(!lock.try_lock()) {
        if(std::chrono::steady_clock::now()>=end)return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if(std::chrono::steady_clock::now()>=end||entered.load(std::memory_order_acquire))return false;
    held.store(true,std::memory_order_release);entered.store(true,std::memory_order_release);
    while(!release.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    const bool released=release.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end;
    held.store(false,std::memory_order_release);
    return released; // Unlock precedes destruction of the retained actual state.
}
#endif
ordinary_managed_ipc_workers::ordinary_managed_ipc_workers(std::shared_ptr<state> value)
    : state_(std::move(value)) {}
std::shared_ptr<ordinary_managed_ipc_workers> ordinary_managed_ipc_workers::reserve() {
    auto& owners = registry();
    std::size_t index = maximum_owners;
    {
        std::lock_guard lock(owners.mutex);
        for (std::size_t i = 0; i < maximum_owners; ++i) if (!owners.reserved[i]) {
            owners.reserved[i] = true; index = i; break;
        }
    }
    if (index == maximum_owners) throw std::runtime_error("managed IPC owner capacity exhausted");
    try {
        // Capacity is charged before record/control-block allocation. No
        // arbitrary capture, thread, FD or authority is created by reserve.
        auto keep = std::make_shared<state>(index);
        auto result = std::shared_ptr<ordinary_managed_ipc_workers>(new ordinary_managed_ipc_workers(keep));
        { std::lock_guard lock(owners.mutex); owners.owners[index] = std::move(keep); }
        return result;
    } catch (...) {
        std::lock_guard lock(owners.mutex); owners.reserved[index] = false;
        throw;
    }
}
ordinary_managed_ipc_workers::~ordinary_managed_ipc_workers() { request_stop(); }
void ordinary_managed_ipc_workers::claim_worker_owner() {
    auto keep = state_;
    keep->require_process();
    std::lock_guard lock(keep->mutex);
    if (keep->worker_owner_claimed || keep->facts.stopping || keep->facts.released ||
        keep->facts.starting[0] || keep->facts.starting[1] || keep->facts.started[0] || keep->facts.started[1])
        throw std::runtime_error("managed IPC paired worker owner already claimed or closed");
    keep->worker_owner_claimed = true;
}
void ordinary_managed_ipc_workers::bind_endpoint(std::shared_ptr<ordinary_managed_ipc_endpoint> endpoint) {
    auto keep = state_;
    keep->require_process();
    if (!endpoint) throw std::invalid_argument("managed IPC endpoint missing");
    std::lock_guard lock(keep->mutex);
    if (keep->endpoint || keep->facts.stopping || keep->facts.released ||
        keep->facts.starting[0] || keep->facts.starting[1] ||
        keep->facts.started[0] || keep->facts.started[1])
        throw std::runtime_error("managed IPC endpoint binding closed");
    keep->endpoint = std::move(endpoint);
}
void ordinary_managed_ipc_workers::request_stop() noexcept {
    auto keep = state_;
    if (!keep || !keep->same_process()) return;
    { std::lock_guard lock(keep->mutex); keep->facts.stopping = true; }
    keep->condition.notify_all();
}
void ordinary_managed_ipc_workers::start(slot which, std::function<void()>&& callback) {
    auto keep = state_;
    keep->require_process();
    const auto index = static_cast<std::size_t>(which);
    if (index >= 2) throw std::invalid_argument("managed IPC worker slot invalid");
    // Allocate the fixed worker-call capsule before admission/thread creation.
    // Its custom disposal vetoes every callback-capable destruction boundary.
    using call_owner = std::unique_ptr<state::call, decltype(&state::call::destroy)>;
    call_owner pending(new state::call(keep, std::move(callback)), &state::call::destroy);
    if (!pending->task) throw std::invalid_argument("managed IPC worker callback missing");
    {
        std::lock_guard lock(keep->mutex);
        if (keep->facts.stopping || keep->facts.released || keep->facts.starting[index] ||
            keep->facts.started[index] || keep->facts.returned[index])
            throw std::runtime_error("managed IPC worker admission closed");
        keep->facts.starting[index] = true;
    }
    try {
        // The thread functor itself owns only a pointer. A failed thread
        // construction leaves pending owning the exact capture; a successful
        // construction transfers it to the actual worker, including early exit.
        auto* supplied = pending.get();
        std::thread actual([supplied, index]() {
            call_owner active(supplied, &state::call::destroy);
            auto& keep = active->keep;
            if (!keep->same_process()) { (void)active.release(); return; }
            {
                std::lock_guard lock(keep->mutex);
                keep->executing[index] = std::this_thread::get_id();
            }
            try { active->task(); } catch (...) { active->error = std::current_exception(); }
            // Both normal and exceptional callback returns may follow fork.
            // Do not even destroy the remaining inherited captures in that case.
            if (!keep->same_process()) { (void)active.release(); return; }
            active->task = {}; // Actual arbitrary capture destruction, off leaf.
            if (!keep->same_process()) { (void)active.release(); return; }
            {
                std::lock_guard lock(keep->mutex);
                if (active->error && !keep->facts.first_error) keep->facts.first_error = active->error;
                if (active->error) keep->facts.stopping = true;
                keep->facts.returned[index] = true;
            }
            keep->condition.notify_all();
        });
        (void)pending.release(); // Actual worker now owns this capsule.
        {
            std::lock_guard lock(keep->mutex);
            keep->threads[index] = std::move(actual);
            keep->facts.started[index] = true;
            keep->facts.starting[index] = false;
        }
        keep->condition.notify_all();
    } catch (...) {
        // No actual worker exists when thread construction throws. Keep the
        // exception with the capsule while capture destruction can fork.
        if (!pending) throw; // Publication contains no intended throwing call.
        pending->error = std::current_exception();
        pending->task = {};
        keep->require_process(); // Capsule deleter quarantines on inherited unwind.
        {
            std::lock_guard lock(keep->mutex);
            if (!keep->facts.first_error) keep->facts.first_error = pending->error;
            keep->facts.stopping = true;
            keep->facts.starting[index] = false;
        }
        keep->condition.notify_all();
        std::rethrow_exception(pending->error);
    }
}

bool ordinary_managed_ipc_workers::join(std::chrono::steady_clock::time_point deadline) {
    auto keep = state_;
    keep->require_process();
    {
        std::unique_lock lock(keep->mutex);
        const auto caller = std::this_thread::get_id();
        for (std::size_t i = 0; i < 2; ++i)
            if (keep->executing[i] == caller && !keep->facts.joined[i])
                throw std::runtime_error("managed IPC worker self join refused");
        if (!keep->facts.stopping || keep->joining || keep->releasing) return false;
        const auto returned = [&] {
            for (std::size_t i = 0; i < 2; ++i)
                if (keep->facts.starting[i] || (keep->facts.started[i] && !keep->facts.returned[i])) return false;
            return true;
        };
        if (!keep->condition.wait_until(lock, deadline, returned)) return false;
        if (std::chrono::steady_clock::now() >= deadline) return false;
        keep->joining = true;
    }
    std::exception_ptr error;
    for (std::size_t i = 0; i < 2; ++i) {
        // Joining owns both handles exclusively; start is closed and the real
        // callbacks/captures have already ended. No leaf spans the native join.
        if (!keep->threads[i].joinable()) continue;
        try {
            keep->threads[i].join();
            std::lock_guard lock(keep->mutex);
            keep->facts.joined[i] = true;
        } catch (...) { error = std::current_exception(); break; }
    }
    {
        std::lock_guard lock(keep->mutex);
        if (error && !keep->facts.first_error) keep->facts.first_error = error;
        if (error && !keep->facts.join_error) keep->facts.join_error = error;
        keep->joining = false;
    }
    keep->condition.notify_all();
    if (error) std::rethrow_exception(error);
    return std::chrono::steady_clock::now() < deadline;
}
bool ordinary_managed_ipc_workers::release_if_joined() {
    auto keep = state_;
    keep->require_process();
    using retirement_owner = std::unique_ptr<state::retirement, decltype(&state::retirement::destroy)>;
    retirement_owner release(new state::retirement(keep), &state::retirement::destroy);
    {
        std::lock_guard lock(keep->mutex);
        if (keep->facts.released) return true;
        if (!keep->facts.stopping || keep->joining || keep->releasing) return false;
        if (keep->facts.join_error) std::rethrow_exception(keep->facts.join_error);
        for (std::size_t i = 0; i < 2; ++i)
            if (keep->facts.starting[i] || (keep->facts.started[i] && !keep->facts.joined[i])) return false;
        keep->releasing = true;
    }
    auto& owners = registry();
    {
        std::lock_guard lock(owners.mutex);
        // The only removal path reserves `releasing` on this exact state.
        // A mismatch is an implementation error; never publish clean release.
        if (owners.owners[keep->registry_slot].get() != keep.get()) {
            std::lock_guard state_lock(keep->mutex);
            keep->releasing = false;
            throw std::runtime_error("managed IPC worker registry association changed");
        }
        release->retired = std::move(owners.owners[keep->registry_slot]);
        // Keep the slot charged throughout concrete endpoint destruction.
    }
    {
        std::lock_guard lock(keep->mutex);
        release->endpoint = std::move(keep->endpoint);
    }
    release->endpoint.reset(); // Concrete endpoint/capture destruction outside leaves.
    keep->require_process(); // Inherited release capsule stays quarantined/charged.
    release->retired.reset(); // No owner/capture destruction under the registry leaf.
    keep->require_process(); // Never enter a registry leaf after a forking finalizer.
    { std::lock_guard lock(owners.mutex); owners.reserved[keep->registry_slot] = false; }
    {
        std::lock_guard lock(keep->mutex);
        keep->facts.released = true;
        keep->releasing = false;
    }
    return true;
}
ordinary_managed_ipc_workers::observation ordinary_managed_ipc_workers::observe() const {
    auto keep = state_;
    if (!keep->same_process()) { observation result; result.inherited = true; result.stopping = true; return result; }
    std::lock_guard lock(keep->mutex); return keep->facts;
}
}
#else
namespace lattice::detail {
struct ordinary_managed_ipc_workers::state {};
ordinary_managed_ipc_workers::ordinary_managed_ipc_workers(std::shared_ptr<state> value) : state_(std::move(value)) {}
ordinary_managed_ipc_workers::~ordinary_managed_ipc_workers() = default;
void ordinary_managed_ipc_workers::claim_worker_owner() { throw std::runtime_error("managed IPC workers unavailable"); }
std::shared_ptr<ordinary_managed_ipc_workers> ordinary_managed_ipc_workers::reserve() { throw std::runtime_error("managed IPC workers unavailable"); }
void ordinary_managed_ipc_workers::start(slot, std::function<void()>&&) { throw std::runtime_error("managed IPC workers unavailable"); }
void ordinary_managed_ipc_workers::bind_endpoint(std::shared_ptr<ordinary_managed_ipc_endpoint>) { throw std::runtime_error("managed IPC workers unavailable"); }
void ordinary_managed_ipc_workers::request_stop() noexcept {}
bool ordinary_managed_ipc_workers::join(std::chrono::steady_clock::time_point) { return false; }
bool ordinary_managed_ipc_workers::release_if_joined() { return false; }
ordinary_managed_ipc_workers::observation ordinary_managed_ipc_workers::observe() const { observation result; result.stopping = true; return result; }
}
#endif
