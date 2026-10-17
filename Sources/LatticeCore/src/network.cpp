#include "lattice/network.hpp"
#include "network_factory_state.hpp"

namespace lattice {

// One process-lived slot. Destructors may reenter registration even during
// process teardown; neither this mutex nor its current owner is destroyed by
// C++ static teardown. Ordinary replacements are still released promptly.
namespace {
struct factory_state {std::mutex mutex;std::shared_ptr<network_factory> current;};
factory_state& factories(){static auto* state=new factory_state;return *state;}
}

std::shared_ptr<network_factory> detail::exchange_network_factory(std::shared_ptr<network_factory> factory) {
    auto& state=factories();
    {std::lock_guard<std::mutex> lock(state.mutex);state.current.swap(factory);}
    return factory;
}

void set_network_factory(std::shared_ptr<network_factory> factory) {
    auto displaced=detail::exchange_network_factory(std::move(factory));
    displaced.reset(); // An arbitrary destructor may reenter registration.
}

std::shared_ptr<network_factory> get_network_factory() {
    std::shared_ptr<network_factory> result;
    auto& state=factories();
    {std::lock_guard<std::mutex> lock(state.mutex);result=state.current;}
    if(result)return result;
    // Construction and discarded candidate destruction occur off the leaf.
    auto candidate=std::make_shared<mock_network_factory>();
    {
        std::lock_guard<std::mutex> lock(state.mutex);
        if(!state.current)state.current=candidate;
        result=state.current;
    }
    return result;
}

} // namespace lattice
