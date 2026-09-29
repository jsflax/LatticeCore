#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <stdexcept>

// Private, inactive substrate. Neither a journal nor an OS lease authorizes a
// database open, application work, legacy enrollment or recovery adoption.
// Installation-owned launcher and source-intake custody is still required.
namespace lattice::detail::ordinary_admission {

using identifier = std::array<std::uint8_t, 16>;
struct file_identity {
    std::uint64_t device = 0, inode = 0;
    bool operator==(const file_identity&) const = default;
};
struct store_binding {
    identifier installation{}, store{}, epoch{};
    file_identity main{}, parent{};
    bool operator==(const store_binding&) const = default;
};
enum class stage : std::uint8_t { unadopted = 1, retirement_requested = 2 };
struct record {
    std::uint64_t revision = 1;
    stage state = stage::unadopted;
    store_binding binding;
    identifier cutover{};
    file_identity control{}, entry{}, generation{};
    bool operator==(const record&) const = default;
};
using encoded_record = std::array<std::uint8_t, 256>;
encoded_record encode(const record&);
record decode(const encoded_record&);

enum class error_code { invalid_record, unavailable, busy, identity_changed,
                        conflicting_cutover, revision_exhausted, inherited_use,
                        durability_unproved };
class error : public std::runtime_error {
public:
    const error_code code;
    error(error_code code, const char* message) : std::runtime_error(message), code(code) {}
};

// Close-only custody, never explicit LOCK_UN: a forked child's destruction
// must not unlock the parent's shared open-file description.
class generation_hold {
    friend class journal;
    int fd_ = -1;
    explicit generation_hold(int fd) noexcept : fd_(fd) {}
public:
    generation_hold() = default;
    ~generation_hold();
    generation_hold(generation_hold&&) noexcept;
    generation_hold& operator=(generation_hold&&) noexcept;
    generation_hold(const generation_hold&) = delete;
    generation_hold& operator=(const generation_hold&) = delete;
    explicit operator bool() const noexcept { return fd_ >= 0; }
};

class journal {
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit journal(std::unique_ptr<implementation>);
public:
    // Caller owns selection of an existing empty 0700 directory. This API
    // duplicates that descriptor; it creates no parent/default/global catalog.
    // Bootstrap is exclusive. Partial bootstrap is an orphan, never reused.
    static journal create_unadopted(int directory_fd, const store_binding&);
    // First observation is not authentication of a coherent catalog reset.
    // The future installation authority must supply/validate expected catalog
    // identity and qualify its filesystem. This substrate grants no adoption.
    static journal open_existing(int directory_fd, const store_binding& expected);
    ~journal();
    journal(journal&&) noexcept;
    journal& operator=(journal&&) noexcept;
    journal(const journal&) = delete;
    journal& operator=(const journal&) = delete;
    record read() const;
    // Tests/next integration may retain this OS lease while observing the
    // unadopted epoch. It is deliberately not an open/work admission token.
    // New holds are refused once durable retirement intent exists.
    generation_hold try_hold_generation() const;
    record begin_retirement(const identifier& exact_cutover);
    // Only contention observation. False does NOT prove physical retirement,
    // complete owner coverage, launcher exit or source-intake drain.
    bool generation_busy() const;
};

namespace test_hooks {
// Fault injection only; it cannot manufacture successful I/O or authority.
enum class boundary { before_write, after_write, after_file_sync,
                      after_rename, after_directory_sync };
using fault = void (*)(boundary);
inline thread_local fault current = nullptr;
}
} // namespace lattice::detail::ordinary_admission
