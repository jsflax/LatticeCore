#include "lattice/network.hpp"

namespace lattice {

// Global network factory
static std::shared_ptr<network_factory> g_network_factory;

void set_network_factory(std::shared_ptr<network_factory> factory) {
    g_network_factory = std::move(factory);
}

std::shared_ptr<network_factory> get_network_factory() {
    if (!g_network_factory) {
        // Return mock factory by default (for testing)
        g_network_factory = std::make_shared<mock_network_factory>();
    }
    return g_network_factory;
}

} // namespace lattice
