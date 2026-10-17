#pragma once
#include "ordinary_installation_cohort.hpp"
#include "ordinary_installation_files.hpp"

namespace lattice::detail::ordinary_installation {
// A narrowly controlled, new-namespace initializer. This is not enrollment of
// an existing store, normal managed startup, or an active store-open context.
enum class seed_error_code { unavailable, invalid_offer, wrong_product, existing_file,
    initializer_failed, terminal_unproved, expired, inherited_use };
class seed_error : public std::runtime_error {
public:
    const seed_error_code code;
    explicit seed_error(seed_error_code value)
        : std::runtime_error("ordinary installation initializer unavailable"), code(value) {}
};
using seed_callback = int (*)(const char* exact_path, void* context);
// Actual product entry calls this before its ordinary startup side effects.
// Descriptors 3/4 are taken even on failure. Callback success is only a reply;
// the controller must still observe this actual process's zero exit itself.
// Only the primary Engram initializer is supported in this first source slice.
int receive_engram_seed(seed_callback, void* context, ordinary_launch::deadline);

// Defined only in the fixed product installation executable. The generic
// seed primitive and its protocol fixtures cannot commit a registration.
class engram_product_installer;
class seeded_installation_store {
    friend class engram_product_installer;
    static seeded_installation_store create_engram_with_output(int parent, const std::string& installation_leaf,
        const executable_fact& initializer, ordinary_launch::deadline, int initializer_output);
    void register_unadopted(const executable_fact& actual_launcher,
        const digest& package_provenance, const std::string& source_revision,
        const std::string& core_revision, ordinary_launch::deadline);
    int run_registered_mcp(ordinary_launch::deadline handshake_deadline);
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit seeded_installation_store(std::unique_ptr<implementation>);
public:
    // Called by the native installer entry, with installer-approved binary
    // identity and bytes. It creates its own data namespace before this exact
    // fixed initializer invocation. No existing namespace is adopted.
    static seeded_installation_store create_engram(int parent, const std::string& installation_leaf,
        const executable_fact& initializer, ordinary_launch::deadline);
    ~seeded_installation_store();
    seeded_installation_store(seeded_installation_store&&) noexcept;
    seeded_installation_store& operator=(seeded_installation_store&&) noexcept;
    seeded_installation_store(const seeded_installation_store&) = delete;
    seeded_installation_store& operator=(const seeded_installation_store&) = delete;
    // Facts derived from retained origin, exchange and actual process exit.
    // This object has no serialization constructor and no DB context conversion.
    file_identity main_file() const;
    file_identity parent_directory() const;
    ordinary_launch::terminal_observation initializer_terminal() const;
};
} // namespace lattice::detail::ordinary_installation
