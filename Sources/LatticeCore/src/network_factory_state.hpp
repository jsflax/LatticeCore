#pragma once
#include <lattice/network.hpp>
namespace lattice::detail {
// Publish under the factory leaf; caller owns and releases displaced outside it.
std::shared_ptr<network_factory> exchange_network_factory(std::shared_ptr<network_factory>);
}
