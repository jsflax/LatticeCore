#pragma once
#ifndef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
#error "Private constructor fixture is absent from release builds"
#endif
#include "ordinary_owned_launch.hpp"
#include "../include/nlohmann/json.hpp"
#include <memory>
namespace lattice::detail {
class ordinary_unpublished_group_target;
class ordinary_target_staging_io;
// Qualification-only closed Native entry. There is no caller path, admission,
// member ID, successful-return Boolean, quota or opaque target factory.
struct ordinary_target_reclamation_fixture final {
    static int run_memory(ordinary_launch::deadline);
    static int run_daemon(ordinary_launch::deadline);
private:
    static nlohmann::json construct_and_abandon(
        const std::shared_ptr<const ordinary_unpublished_group_target>&,ordinary_launch::deadline);
    static nlohmann::json registration_ending(const ordinary_target_staging_io&);
};
}
