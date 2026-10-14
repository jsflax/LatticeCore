#pragma once
#include "ordinary_installation_manifest.hpp"
#include "ordinary_owned_launch.hpp"
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace lattice::detail::ordinary_installation {
class engram_namespace_retirement;
class retained_engram_upgrade_session;
class retained_managed_history_launch;

// Private parent-side ownership, issued only after the actual historical
// controller's two retirement phases and namespace exchange. It is not a
// database-open, source, schema, IPC-role or recovery-profile capability.
// In particular, no completion record or ordinary_open_context converts here.
class retained_historical_generation final {
    friend class engram_namespace_retirement;
    friend class retained_engram_upgrade_session;
    friend class retained_managed_history_launch;
    struct member {
        std::string relative_name;
        file_identity identity, refusal;
        std::uint32_t mode = 0;
        bool directory = false;
    };
    struct observation {
        file_identity historical_parent, control, generation;
        digest barrier{};
        std::vector<member> members;
        // Separate permanent old-path facts. Runtime sidecars may disappear
        // or gain new inodes; their old refusal entries remain permanently 0.
        std::vector<member> old_refusals;
    };
    struct implementation {
        // Both closures own the real namespace/controller state. There is no
        // callback setter or deserialization constructor. Their callers stay
        // on the original dedicated controller thread.
        std::function<observation(ordinary_launch::deadline)> verify;
        std::function<int(ordinary_launch::deadline)> duplicate_generation;
    };
    std::shared_ptr<implementation> impl_;
    explicit retained_historical_generation(std::shared_ptr<implementation> value)
        : impl_(std::move(value)) {}
    observation before_launch(ordinary_launch::deadline) const;
    int duplicate_before_launch(ordinary_launch::deadline) const;
public:
    ~retained_historical_generation() = default;
    retained_historical_generation(retained_historical_generation&&) noexcept = default;
    retained_historical_generation& operator=(retained_historical_generation&&) noexcept = default;
    retained_historical_generation(const retained_historical_generation&) = delete;
    retained_historical_generation& operator=(const retained_historical_generation&) = delete;
};
}
