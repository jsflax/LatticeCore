#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "ordinary_owned_launch.hpp"
#include <array>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstring>
#include <exception>
#include <mutex>
#include <sstream>
#include <thread>
#include <utility>

#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <libproc.h>
#endif
#endif

#if defined(__APPLE__)
#define LATTICE_ORDINARY_OWNED_SPAWN 1
#elif defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 34)
#define LATTICE_ORDINARY_OWNED_SPAWN 1
#endif
#endif

namespace lattice::detail::ordinary_launch {
namespace {
[[noreturn]] void fail(error_code code) { throw error(code, "ordinary owned launch unavailable"); }
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
const pid_t initial_process = ::getpid();
struct descriptor {
    int value = -1;
    descriptor() = default;
    explicit descriptor(int fd) : value(fd) {}
    ~descriptor() { if (value >= 0) ::close(value); }
    descriptor(descriptor&& other) noexcept : value(std::exchange(other.value, -1)) {}
    descriptor& operator=(descriptor&& other) noexcept {
        if (this != &other) { if (value >= 0) ::close(value); value = std::exchange(other.value, -1); }
        return *this;
    }
    descriptor(const descriptor&) = delete;
    descriptor& operator=(const descriptor&) = delete;
};
void owner(pid_t expected) { if (::getpid() != expected) fail(error_code::inherited_use); }
void bound(deadline until, const std::atomic<bool>* canceled = nullptr) {
    if (canceled && canceled->load(std::memory_order_acquire)) fail(error_code::canceled);
    if (std::chrono::steady_clock::now() >= until) fail(error_code::timed_out);
}
void configure(int fd) {
    int type = 0; socklen_t size = sizeof(type);
    if (fd < 0 || ::getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &size) != 0 ||
        size != sizeof(type) || type != SOCK_STREAM) fail(error_code::invalid_specification);
    sockaddr_storage local{}, peer{};
    socklen_t local_size = sizeof(local), peer_size = sizeof(peer);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &local_size) != 0 ||
        ::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &peer_size) != 0 ||
        local.ss_family != AF_UNIX || peer.ss_family != AF_UNIX) fail(error_code::invalid_specification);
    const auto flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFD, FD_CLOEXEC) != 0 ||
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) fail(error_code::io);
#if defined(__APPLE__)
    int yes = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes)) != 0) fail(error_code::io);
#endif
}
void ready(int fd, short events, deadline until, const std::atomic<bool>* canceled) {
    for (;;) {
        bound(until, canceled);
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(until - std::chrono::steady_clock::now()).count();
        pollfd item{fd, events, 0};
        const auto result = ::poll(&item, 1, static_cast<int>(std::clamp<std::int64_t>(left, 1, 20)));
        if (result < 0) { if (errno == EINTR) continue; fail(error_code::io); }
        if (!result) continue;
        if (item.revents & POLLNVAL) fail(error_code::io);
        // HUP may accompany a final complete frame. Let recv consume the exact
        // remaining bytes; zero/partial EOF below is still a failed exchange.
        if (item.revents & (events | POLLERR | POLLHUP)) return;
    }
}
void transfer(int fd, std::uint8_t* bytes, std::size_t count, bool sending,
              deadline until, const std::atomic<bool>* canceled = nullptr) {
    std::size_t done = 0;
    while (done < count) {
        bound(until, canceled);
        ssize_t amount;
        if (sending) {
#if defined(MSG_NOSIGNAL)
            amount = ::send(fd, bytes + done, count - done, MSG_NOSIGNAL);
#else
            amount = ::send(fd, bytes + done, count - done, 0);
#endif
        } else amount = ::recv(fd, bytes + done, count - done, 0);
        if (amount > 0) { done += static_cast<std::size_t>(amount); continue; }
        if (amount == 0) fail(error_code::io);
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) fail(error_code::io);
        ready(fd, sending ? POLLOUT : POLLIN, until, canceled);
    }
    bound(until, canceled);
}
void write_frame(int fd, const frame& body, deadline until, const std::atomic<bool>* canceled = nullptr) {
    if (body.empty() || body.size() > maximum_frame_bytes) fail(error_code::malformed_frame);
    const auto size = static_cast<std::uint32_t>(body.size());
    std::array<std::uint8_t, 4> prefix{{static_cast<std::uint8_t>(size >> 24), static_cast<std::uint8_t>(size >> 16),
                                    static_cast<std::uint8_t>(size >> 8), static_cast<std::uint8_t>(size)}};
    transfer(fd, prefix.data(), prefix.size(), true, until, canceled);
    // send never modifies these bytes; transfer shares the bounded read/write loop.
    transfer(fd, const_cast<std::uint8_t*>(body.data()), body.size(), true, until, canceled);
}
frame read_frame(int fd, deadline until, const std::atomic<bool>* canceled = nullptr) {
    std::array<std::uint8_t, 4> prefix{};
    transfer(fd, prefix.data(), prefix.size(), false, until, canceled);
    std::uint32_t size = 0;
    for (const auto byte : prefix) size = (size << 8) | byte;
    if (!size || size > maximum_frame_bytes) fail(error_code::malformed_frame);
    frame result(size);
    transfer(fd, result.data(), result.size(), false, until, canceled);
    return result;
}
process_identity read_identity(pid_t pid) {
    if (pid <= 0) fail(error_code::identity_unproved);
#if defined(__APPLE__)
    proc_bsdinfo value{};
    if (::proc_pidinfo(pid, PROC_PIDTBSDINFO, 1, &value, sizeof(value)) != sizeof(value) ||
        value.pbi_pid != static_cast<std::uint32_t>(pid) || value.pbi_ppid == 0 ||
        !value.pbi_start_tvsec || value.pbi_start_tvusec >= 1000000) fail(error_code::identity_unproved);
    return {pid, value.pbi_ppid, value.pbi_start_tvsec, value.pbi_start_tvusec};
#else
    const auto name = "/proc/" + std::to_string(pid) + "/stat";
    descriptor fd(::open(name.c_str(), O_RDONLY | O_CLOEXEC));
    if (fd.value < 0) fail(error_code::identity_unproved);
    std::array<char, 8193> bytes{};
    std::size_t total = 0;
    for (;;) {
        auto got = ::read(fd.value, bytes.data() + total, bytes.size() - total);
        if (got < 0) { if (errno == EINTR) continue; fail(error_code::identity_unproved); }
        if (!got) break;
        total += static_cast<std::size_t>(got);
        if (total == bytes.size()) fail(error_code::identity_unproved);
    }
    std::string text(bytes.data(), total);
    const auto end = text.rfind(')');
    if (end == std::string::npos) fail(error_code::identity_unproved);
    std::istringstream beginning(text.substr(0, text.find(' ')));
    std::int64_t actual = 0; beginning >> actual;
    std::istringstream fields(text.substr(end + 1));
    char state = 0; std::int64_t parent = 0; std::string discard;
    fields >> state >> parent; // fields 3 and 4
    for (int field = 5; field < 22; ++field) fields >> discard;
    std::uint64_t birth = 0; fields >> birth;
    if (!beginning || !fields || actual != pid || parent <= 0 || !birth) fail(error_code::identity_unproved);
    return {pid, parent, birth, 0}; // start ticks distinguish this birth; never a persisted cross-boot grant.
#endif
}
void exclusive_reaper() {
    struct sigaction action{};
    if (::sigaction(SIGCHLD, nullptr, &action) != 0 || action.sa_handler != SIG_DFL ||
        (action.sa_flags & SA_NOCLDWAIT)) fail(error_code::terminal_unproved);
}
#endif
} // namespace

#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
process_identity current_process() { return read_identity(::getpid()); }
process_identity current_parent_process() {
    const auto parent = ::getppid();
    const auto observed = read_identity(parent);
    if (::getppid() != parent) fail(error_code::identity_unproved);
    return observed;
}
struct inherited_channel::implementation {
    descriptor fd;
    const pid_t creator = ::getpid();
    const std::thread::id thread = std::this_thread::get_id();
    // A failed partial transfer is terminal. A new call cannot reinterpret its
    // remaining payload bytes as another frame header.
    bool failed = false;
    implementation() = default;
};
inherited_channel::inherited_channel(int fd) {
    descriptor custody(fd);
    auto state = std::make_unique<implementation>();
    state->fd = std::move(custody);
    configure(state->fd.value);
    impl_ = std::move(state);
}
#else
process_identity current_process() { fail(error_code::unavailable); }
process_identity current_parent_process() { fail(error_code::unavailable); }
struct inherited_channel::implementation {};
inherited_channel::inherited_channel(int) { fail(error_code::unavailable); }
#endif

inherited_channel::~inherited_channel() = default;
inherited_channel::inherited_channel(inherited_channel&&) noexcept = default;
inherited_channel& inherited_channel::operator=(inherited_channel&&) noexcept = default;

void inherited_channel::send(const frame& body, deadline until) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (!impl_) fail(error_code::unavailable);
    owner(impl_->creator);
    if (impl_->thread != std::this_thread::get_id()) fail(error_code::wrong_thread);
    if (impl_->failed) fail(error_code::io);
    try { write_frame(impl_->fd.value, body, until); }
    catch (...) { impl_->failed = true; throw; }
#else
    (void)body; (void)until; fail(error_code::unavailable);
#endif
}
frame inherited_channel::receive(deadline until) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (!impl_) fail(error_code::unavailable);
    owner(impl_->creator);
    if (impl_->thread != std::this_thread::get_id()) fail(error_code::wrong_thread);
    if (impl_->failed) fail(error_code::io);
    try { return read_frame(impl_->fd.value, until); }
    catch (...) { impl_->failed = true; throw; }
#else
    (void)until; fail(error_code::unavailable);
#endif
}

#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
struct owned_child::implementation {
    const pid_t creator = ::getpid();
    descriptor fd;
    process_identity child;
    std::atomic<bool> stopped{false};
    std::timed_mutex io, retirement;
    bool exchange_failed = false, custody_lost = false;
    bool reaped = false;
    int status = 0;
    // The dedicated supervisor retains failures until actual process exit.
    // Intentionally no static destructor releases unresolved child custody.
    struct retained_registry {
        std::mutex mutex;
        std::array<std::shared_ptr<implementation>, 128> entries;
    };
    static retained_registry& registry() {
        owner(initial_process);
        static auto* value = new retained_registry; return *value;
    }
    static void retain(const std::shared_ptr<implementation>& state) {
        auto& values = registry(); std::lock_guard guard(values.mutex);
        for (auto& slot : values.entries) if (!slot) { slot = state; return; }
        fail(error_code::unavailable);
    }
    void release_retained() {
        auto& values = registry(); std::lock_guard guard(values.mutex);
        for (auto& slot : values.entries) if (slot.get() == this) { slot.reset(); return; }
    }
    bool exited(deadline until) {
        if (custody_lost || child.pid <= 0) fail(error_code::terminal_unproved);
        bound(until);
        try { exclusive_reaper(); }
        catch (...) { custody_lost = true; throw; }
        siginfo_t info{}; int rc;
        do {
            bound(until); // Includes every interrupted retry; custody is retained on timeout.
            if (test_hooks::interrupt && test_hooks::interrupt(test_hooks::wait_boundary::observe)) {
                errno = EINTR; rc = -1;
            } else rc = ::waitid(P_PID, static_cast<id_t>(child.pid), &info, WEXITED | WNOHANG | WNOWAIT);
        } while (rc != 0 && errno == EINTR);
        if (rc != 0) { custody_lost = true; fail(error_code::terminal_unproved); }
        if (info.si_pid != 0 && info.si_pid != child.pid) { custody_lost = true; fail(error_code::terminal_unproved); }
        return info.si_pid == child.pid;
    }
    terminal_observation stop(deadline until) {
        owner(creator);
        stopped.store(true, std::memory_order_release);
        std::unique_lock gate(retirement, std::defer_lock);
        if (!gate.try_lock_until(until)) fail(error_code::cleanup_unproved);
        if (reaped) return {child, status};
        if (custody_lost) fail(error_code::terminal_unproved);
        auto done = exited(until);
        if (!done) {
            bound(until);
            // Only this exact unreaped direct child. No group/remembered
            // descendant PID is signalled by this primitive.
            if (::kill(static_cast<pid_t>(child.pid), SIGTERM) != 0 && errno != ESRCH)
                fail(error_code::cleanup_unproved);
            const auto grace = std::min(until, std::chrono::steady_clock::now() + std::chrono::milliseconds(100));
            while (!(done = exited(until)) && std::chrono::steady_clock::now() < grace)
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        if (!done) {
            bound(until);
            if (::kill(static_cast<pid_t>(child.pid), SIGKILL) != 0 && errno != ESRCH)
                fail(error_code::cleanup_unproved);
            while (!(done = exited(until))) { bound(until); std::this_thread::sleep_for(std::chrono::milliseconds(2)); }
        }
        return reap_terminal(until);
    }
    // Caller holds retirement custody and has a positive WNOWAIT observation.
    terminal_observation reap_terminal(deadline until) {
        // No descriptor close can race a still-running read/write worker. Stop
        // makes those bounded polls unwind; failure to join retains everything.
        std::unique_lock channel(io, std::defer_lock);
        if (!channel.try_lock_until(until)) fail(error_code::cleanup_unproved);
        int result = 0; pid_t observed;
        do {
            bound(until); // Never start another reap attempt after the original deadline.
            if (test_hooks::interrupt && test_hooks::interrupt(test_hooks::wait_boundary::reap)) {
                errno = EINTR; observed = -1;
            } else observed = ::waitpid(static_cast<pid_t>(child.pid), &result, WNOHANG);
        } while (observed < 0 && errno == EINTR);
        // A successful real reap is authoritative even if the clock advances
        // immediately afterward: record it before any further fallible work.
        if (observed != child.pid) { custody_lost = true; fail(error_code::terminal_unproved); }
        status = result; reaped = true;
        fd = descriptor(); // close-only; no inherited OFD unlock or arbitrary callback.
        release_retained();
        return {child, status};
    }
    std::optional<terminal_observation> observe(deadline until) {
        owner(creator);
        bound(until);
        std::unique_lock gate(retirement, std::defer_lock);
        if (!gate.try_lock_until(until)) fail(error_code::cleanup_unproved);
        if (reaped) return terminal_observation{child, status};
        if (custody_lost) fail(error_code::terminal_unproved);
        if (!exited(until)) return std::nullopt;
        // Only actual terminal observation closes the channel to fresh I/O.
        // A live-child poll or failed wait cannot poison an active exchange.
        stopped.store(true, std::memory_order_release);
        return reap_terminal(until);
    }
    void abandon_wrapper() noexcept {
        // A forked copy never signals/reaps the parent's child or locks its
        // inherited C++ mutexes. Its descriptor copy closes with process exit.
        if (::getpid() != creator) return;
        try { (void)stop(std::chrono::steady_clock::now() + std::chrono::seconds(5)); }
        catch (...) { /* Registry retains the exact unresolved lifetime. */ }
    }
};

owned_child owned_child::spawn(const launch_specification& spec) {
#if !defined(LATTICE_ORDINARY_OWNED_SPAWN)
    (void)spec; fail(error_code::unavailable);
#else
    const auto valid_text = [](const std::string& value, std::size_t limit) {
        return value.size() <= limit && value.find('\0') == std::string::npos;
    };
    if (spec.executable.empty() || spec.executable.front() != '/' || !valid_text(spec.executable, 4096) ||
        spec.working_directory.empty() || spec.working_directory.front() != '/' || !valid_text(spec.working_directory, 4096) ||
        spec.arguments.size() > 128 || spec.environment.size() > 128 ||
        spec.inherited_directories.size() > maximum_inherited_directories) fail(error_code::invalid_specification);
    std::size_t total = spec.executable.size() + spec.working_directory.size();
    for (const auto& item : spec.arguments) {
        if (!valid_text(item, 8192)) fail(error_code::invalid_specification); total += item.size();
    }
    for (const auto& item : spec.environment) {
        const auto equals = item.find('=');
        if (!valid_text(item, 8192) || equals == 0 || equals == std::string::npos) fail(error_code::invalid_specification);
        total += item.size();
    }
    if (total > 256 * 1024) fail(error_code::invalid_specification);
    exclusive_reaper();
    auto state = std::make_shared<implementation>();
    int pair[2] = {-1, -1};
#if defined(__linux__)
    // Atomic descriptor inheritance exclusion; no fcntl-only race window.
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pair) != 0) fail(error_code::io);
#else
    // The dedicated Darwin supervisor has no external spawn/fork path. Every
    // launch through this primitive uses POSIX_SPAWN_CLOEXEC_DEFAULT below.
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) fail(error_code::io);
#endif
    state->fd = descriptor(pair[0]); descriptor child_channel(pair[1]);
    configure(state->fd.value);
    if (::fcntl(child_channel.value, F_SETFD, FD_CLOEXEC) != 0) fail(error_code::io);
    std::vector<int> originals{spec.input, spec.output, spec.diagnostic, child_channel.value};
    for (const auto fd : spec.inherited_directories) {
        struct stat value{};
        if (fd < 0 || ::fstat(fd, &value) != 0 || !S_ISDIR(value.st_mode)) fail(error_code::invalid_specification);
        originals.push_back(fd);
    }
    // Duplicate sources above the complete target range before adding any dup2
    // action, so one target mapping cannot overwrite another mapping's source.
    std::vector<descriptor> duplicates; duplicates.reserve(originals.size());
    for (const auto fd : originals) {
        if (fd < 0) fail(error_code::invalid_specification);
        descriptor copy(::fcntl(fd, F_DUPFD_CLOEXEC, static_cast<int>(originals.size()) + 8));
        if (copy.value < 0) fail(error_code::io);
        duplicates.push_back(std::move(copy));
    }
    posix_spawn_file_actions_t actions;
    if (::posix_spawn_file_actions_init(&actions) != 0) fail(error_code::launch_failed);
    struct destroy_actions { posix_spawn_file_actions_t* value; ~destroy_actions() { ::posix_spawn_file_actions_destroy(value); } } action_guard{&actions};
    posix_spawnattr_t attributes;
    if (::posix_spawnattr_init(&attributes) != 0) fail(error_code::launch_failed);
    struct destroy_attributes { posix_spawnattr_t* value; ~destroy_attributes() { ::posix_spawnattr_destroy(value); } } attribute_guard{&attributes};
    sigset_t mask, defaults;
    if (sigemptyset(&mask) != 0 || sigemptyset(&defaults) != 0) fail(error_code::launch_failed);
    for (const int number : {SIGTERM, SIGINT, SIGHUP, SIGPIPE, SIGCHLD})
        if (sigaddset(&defaults, number) != 0) fail(error_code::launch_failed);
    short flags = POSIX_SPAWN_SETSIGDEF | POSIX_SPAWN_SETSIGMASK;
#if defined(__APPLE__)
    flags |= POSIX_SPAWN_CLOEXEC_DEFAULT;
#endif
    if (::posix_spawnattr_setsigdefault(&attributes, &defaults) != 0 ||
        ::posix_spawnattr_setsigmask(&attributes, &mask) != 0 ||
        ::posix_spawnattr_setflags(&attributes, flags) != 0 ||
        ::posix_spawn_file_actions_addchdir_np(&actions, spec.working_directory.c_str()) != 0) fail(error_code::launch_failed);
    for (std::size_t i = 0; i < duplicates.size(); ++i)
        if (::posix_spawn_file_actions_adddup2(&actions, duplicates[i].value, static_cast<int>(i)) != 0) fail(error_code::launch_failed);
#if !defined(__APPLE__)
    if (::posix_spawn_file_actions_addclosefrom_np(&actions, static_cast<int>(duplicates.size())) != 0)
        fail(error_code::launch_failed);
#endif
    std::vector<char*> argv, envp; argv.reserve(spec.arguments.size() + 2); envp.reserve(spec.environment.size() + 1);
    argv.push_back(const_cast<char*>(spec.executable.c_str()));
    for (const auto& item : spec.arguments) argv.push_back(const_cast<char*>(item.c_str()));
    argv.push_back(nullptr);
    for (const auto& item : spec.environment) envp.push_back(const_cast<char*>(item.c_str()));
    envp.push_back(nullptr);
    implementation::retain(state); // before spawn: allocation failure cannot lose a child.
    pid_t child = 0;
    const auto result = ::posix_spawn(&child, spec.executable.c_str(), &actions, &attributes, argv.data(), envp.data());
    if (result != 0) { state->release_retained(); fail(error_code::launch_failed); }
    state->child.pid = child; // exact spawn ownership exists even if birth inspection fails.
    child_channel = descriptor();
    try {
        const auto identity = read_identity(child);
        if (identity.parent != state->creator) fail(error_code::identity_unproved);
        state->child = identity;
        return owned_child(std::move(state));
    } catch (...) {
        const auto primary = std::current_exception();
        state->abandon_wrapper(); // cleanup error cannot replace the original launch failure.
        std::rethrow_exception(primary);
    }
#endif
}
#else
struct owned_child::implementation {};
owned_child owned_child::spawn(const launch_specification&) { fail(error_code::unavailable); }
#endif

owned_child::owned_child(std::shared_ptr<implementation> value) : impl_(std::move(value)) {}
owned_child::~owned_child() {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (impl_) impl_->abandon_wrapper();
#endif
}
owned_child::owned_child(owned_child&&) noexcept = default;
owned_child& owned_child::operator=(owned_child&& other) noexcept {
    if (this != &other) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
        if (impl_) impl_->abandon_wrapper();
#endif
        impl_ = std::move(other.impl_);
    }
    return *this;
}
process_identity owned_child::identity() const {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (!impl_) fail(error_code::unavailable);
    owner(impl_->creator); return impl_->child;
#else
    fail(error_code::unavailable);
#endif
}
void owned_child::send(const frame& body, deadline until) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (!impl_) fail(error_code::unavailable); owner(impl_->creator);
    std::unique_lock channel(impl_->io, std::defer_lock);
    if (!channel.try_lock_until(until)) fail(error_code::timed_out);
    if (impl_->exchange_failed) fail(error_code::io);
    try { write_frame(impl_->fd.value, body, until, &impl_->stopped); }
    catch (...) { impl_->exchange_failed = true; throw; }
#else
    (void)body; (void)until; fail(error_code::unavailable);
#endif
}
frame owned_child::receive(deadline until) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (!impl_) fail(error_code::unavailable); owner(impl_->creator);
    std::unique_lock channel(impl_->io, std::defer_lock);
    if (!channel.try_lock_until(until)) fail(error_code::timed_out);
    if (impl_->exchange_failed) fail(error_code::io);
    try { return read_frame(impl_->fd.value, until, &impl_->stopped); }
    catch (...) { impl_->exchange_failed = true; throw; }
#else
    (void)until; fail(error_code::unavailable);
#endif
}
terminal_observation owned_child::stop_and_join(deadline until) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (!impl_) fail(error_code::unavailable); return impl_->stop(until);
#else
    (void)until; fail(error_code::unavailable);
#endif
}
std::optional<terminal_observation> owned_child::observe_terminal(deadline until) {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    if (!impl_) fail(error_code::unavailable); return impl_->observe(until);
#else
    (void)until; fail(error_code::unavailable);
#endif
}
std::size_t owned_child::unresolved_lifetimes() {
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
    auto& values = implementation::registry(); std::lock_guard guard(values.mutex);
    return static_cast<std::size_t>(std::count_if(values.entries.begin(), values.entries.end(), [](const auto& value) { return static_cast<bool>(value); }));
#else
    return 0;
#endif
}
} // namespace lattice::detail::ordinary_launch
