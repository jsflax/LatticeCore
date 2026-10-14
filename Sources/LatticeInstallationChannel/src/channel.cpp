#include "LatticeInstallationChannel.h"
#include "../../LatticeCore/src/ordinary_installation_seed.hpp"
#include <chrono>
extern "C" int lattice_receive_engram_seed_v1(lattice_engram_seed_callback_v1 callback, void* context) {
    try {
        return lattice::detail::ordinary_installation::receive_engram_seed(callback, context,
            std::chrono::steady_clock::now() + std::chrono::seconds(30));
    } catch (...) {
        // Fixed failure, no exception crosses the C/Swift boundary. The actual
        // installer retains/joins this child and preserves its own first error.
        return 74;
    }
}
