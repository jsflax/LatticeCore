#pragma once
#include "../../Sources/LatticeCore/src/ordinary_installation_seed.hpp"
namespace lattice::detail::ordinary_installation {
// Reachable only from the dedicated native executable. The caller supplies a
// new namespace destination, never a product identity, revision or ready bit.
class engram_product_installer final {
    static seeded_installation_store prepare_registered(int parent, const std::string& leaf, ordinary_launch::deadline,
        int initializer_output);
public:
    static void create_registered(int parent, const std::string& leaf, ordinary_launch::deadline);
    static int create_and_run_primary_mcp(int parent, const std::string& leaf, ordinary_launch::deadline);
};
}
