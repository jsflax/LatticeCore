#if !defined(__APPLE__) && !defined(__linux__) && !defined(__ANDROID__)

#include "lattice/cross_process_notifier.hpp"

namespace lattice {

std::unique_ptr<cross_process_notifier> make_cross_process_notifier(const std::string& /*db_path*/) {
    return nullptr;
}

} // namespace lattice

#endif
