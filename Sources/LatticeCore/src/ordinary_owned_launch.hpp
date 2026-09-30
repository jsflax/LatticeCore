#pragma once

#include <chrono>
#include <cstdint>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// Private launch transport/lifetime infrastructure. An owned child, a received
// frame or a direct-child exit is NOT installation bootstrap, descendant
// coverage, store-open permission, or recovery/adoption authority.
namespace lattice::detail::ordinary_launch {
using deadline = std::chrono::steady_clock::time_point;
using frame = std::vector<std::uint8_t>;
inline constexpr std::size_t maximum_frame_bytes = 128 * 1024;
inline constexpr std::size_t maximum_inherited_directories = 128;

enum class error_code { unavailable, invalid_specification, inherited_use, wrong_thread,
    io, timed_out, canceled, malformed_frame, launch_failed, identity_unproved,
    terminal_unproved, cleanup_unproved };
class error : public std::runtime_error {
public:
    const error_code code;
    error(error_code value, const char* message) : std::runtime_error(message), code(value) {}
};

struct process_identity {
    std::int64_t pid = 0, parent = 0;
    std::uint64_t birth_major = 0, birth_minor = 0;
    bool operator==(const process_identity&) const = default;
};
process_identity current_process();

// The launching authority must own approved executable/invocation validation.
// This lower-level primitive performs no approval by inspecting these strings.
// Descriptors are deliberately mapped: stdio 0..2, private channel 3, then
// retained directories 4... . Every other descriptor is closed across exec.
struct launch_specification {
    std::string executable;
    std::string working_directory;
    std::vector<std::string> arguments;
    std::vector<std::string> environment;
    int input = 0, output = 1, diagnostic = 2;
    std::vector<int> inherited_directories;
};

class inherited_channel {
    struct implementation;
    std::unique_ptr<implementation> impl_;
public:
    // On supported hosts, takes this descriptor including on failure. The receiver still has to
    // verify its actual owned launch exchange; adopting a socket grants nothing.
    explicit inherited_channel(int descriptor);
    ~inherited_channel();
    inherited_channel(inherited_channel&&) noexcept;
    inherited_channel& operator=(inherited_channel&&) noexcept;
    inherited_channel(const inherited_channel&) = delete;
    inherited_channel& operator=(const inherited_channel&) = delete;
    // Both operations stay on the creating thread; use from another thread
    // refuses before consuming/changing frame state. Destruction/move still
    // requires the caller to have joined every operation on the wrapper.
    void send(const frame&, deadline);
    frame receive(deadline);
};

struct terminal_observation {
    process_identity child;
    int wait_status = 0;
    // Deliberately no descendantsGone/installationClosed success field.
};

class owned_child {
    struct implementation;
    std::shared_ptr<implementation> impl_;
    explicit owned_child(std::shared_ptr<implementation>);
public:
    // Use only in the dedicated installation supervisor, which exclusively
    // owns wait/reap and every spawn/fork path. Ambient SIGCHLD reapers and
    // concurrent external descriptor-inheriting launch paths are unsupported.
    // Darwin requires the actual dedicated-supervisor call-graph exclusion;
    // its POSIX_SPAWN_CLOEXEC_DEFAULT protects all launches through this API.
    static owned_child spawn(const launch_specification&);
    ~owned_child();
    owned_child(owned_child&&) noexcept;
    owned_child& operator=(owned_child&&) noexcept;
    owned_child(const owned_child&) = delete;
    owned_child& operator=(const owned_child&) = delete;
    process_identity identity() const;
    void send(const frame&, deadline);
    frame receive(deadline);
    // Stops admission/I/O, then terminates and joins the exact unreaped direct
    // child. The caller must separately retain every actual store-owning role
    // and descendant; this primitive never promotes a group census to proof.
    terminal_observation stop_and_join(deadline);
    // A failed teardown remains process-retained. Wrapper destruction cannot
    // turn unknown cleanup into a successful observation or release its PID.
    static std::size_t unresolved_lifetimes();
};
namespace test_hooks {
// Failure-only injection at actual retirement wait boundaries. Returning true
// emulates EINTR; it cannot fabricate a child identity or successful OS wait.
// The production default performs the original syscall without substitution.
enum class wait_boundary { observe, reap };
using interrupt_wait = bool (*)(wait_boundary);
inline thread_local interrupt_wait interrupt = nullptr;
}
} // namespace lattice::detail::ordinary_launch
