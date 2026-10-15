#pragma once
#include "../../Sources/LatticeCore/src/ordinary_installation_manifest.hpp"
#include "../../Sources/LatticeCore/src/ordinary_owned_launch.hpp"
#include <memory>
#include <string>

namespace lattice::detail::ordinary_installation {
// Retained observations of existing historical files. No method returns a
// journal, ordinary context, generation lease or old-owner completion receipt.
// Only the independently verified product controller creates this object.
class engram_product_installer;
class retained_engram_restart_preparation final {
    friend class engram_product_installer;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit retained_engram_restart_preparation(int data_directory, const digest& installed_package,
                                                 ordinary_launch::deadline);
    // Consumes the actual retained inventory at the next controller boundary.
    // Missing real lifecycle adapters remain debt; decoded output cannot clear it.
    void publish_retirement_plan(int output_parent, const std::string& leaf,
                                 ordinary_launch::deadline);
public:
    ~retained_engram_restart_preparation();
    retained_engram_restart_preparation(const retained_engram_restart_preparation&) = delete;
    retained_engram_restart_preparation& operator=(const retained_engram_restart_preparation&) = delete;
};
}
