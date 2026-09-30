#pragma once
#include "ordinary_installation_cohort.hpp"
#include "ordinary_installation_files.hpp"
#include "../include/lattice/ordinary_context.hpp"

namespace lattice::detail::ordinary_installation {
// Bounded untrusted exchange data. Parsing this struct never installs policy or
// creates an ordinary_open_context. The issuer remains the actual product
// controller, and the receiver checks its actual parent/child incarnations.
struct normal_launch_offer {
    std::uint64_t phase = 1;
    identifier nonce{};
    ordinary_launch::process_identity supervisor, child;
    std::vector<std::uint8_t> external_anchor, origin_catalog, normal_catalog;
    file_identity external_identity, normal_catalog_identity;
    std::string external_leaf;
    ordinary_admission::record admission;
};
std::vector<std::uint8_t> encode_normal_offer(const normal_launch_offer&);
normal_launch_offer decode_normal_offer(const std::vector<std::uint8_t>&);
void validate_primary_mcp_contract(const manifest& origin, const manifest& normal);
// Reads the fixed OS-account installation authority; never creates or repairs
// it. The authorized installer is its sole publisher, outside this executable.
void verify_normal_installed_root(const executable_fact& memory, const executable_fact& launcher,
    file_identity package_record, const digest& package_content, ordinary_launch::deadline);

class seeded_installation_store;
class normal_origin_authority final {
    friend class seeded_installation_store;
    static ordinary_admission::record admit(ordinary_admission::journal& journal,
        const ordinary_admission::record& expected, const identifier& exact_launch) {
        return journal.admit_ordinary(expected, exact_launch);
    }
};
struct normal_receiver {
    // Only actual inherited descriptors 3...8 and an owned launch exchange can
    // enter this path. Initial installation failure leaves sticky closed
    // policy; duplicate invocation grants nothing and takes no owned descriptor.
    static ordinary_context receive(ordinary_launch::deadline);
};
} // namespace lattice::detail::ordinary_installation
