#pragma once
#include "ordinary_installation_manifest.hpp"
#include "ordinary_owned_launch.hpp"
#include <functional>
#include <memory>
#include <utility>

namespace lattice::detail::ordinary_installation {
class retained_engram_upgrade_session;
class engram_namespace_retirement;
// Private live controller custody after an exact managed predecessor's
// owned-child and fork/exec-rejecting kernel interval has closed. The fixed
// installed controller also re-observes the actual prior incarnation, held
// records and image. No public row/JSON/bool constructor or context conversion.
class retained_managed_runtime_retirement final {
    friend class retained_engram_upgrade_session;
    friend class engram_namespace_retirement;
    struct state {
        file_identity historical,control,generation;
        digest barrier{};
        std::function<void(ordinary_launch::deadline)> verify;
    };
    std::shared_ptr<const state> state_;
    explicit retained_managed_runtime_retirement(std::shared_ptr<const state> value):state_(std::move(value)){}
public:
    retained_managed_runtime_retirement(const retained_managed_runtime_retirement&)=delete;
    retained_managed_runtime_retirement& operator=(const retained_managed_runtime_retirement&)=delete;
    retained_managed_runtime_retirement(retained_managed_runtime_retirement&&) noexcept=default;
    retained_managed_runtime_retirement& operator=(retained_managed_runtime_retirement&&) noexcept=default;
    ~retained_managed_runtime_retirement()=default;
};
}
