#pragma once
#include <bridging.hpp>
#include <cstdint>
#include <string>
#include <lattice/store_resource_context.hpp>

namespace lattice {
// Actual Bridge TU code address is supplied by this out-of-line producer.
// A Swift address is only observed as mapped-image DATA by the Native issuer;
// it cannot authorize a root, image or source not in the retained cold epoch.
// Capture the sealed outcome immediately, before any other Bridge operation.
bool bind_store_resource_code(std::uintptr_t actual_sdk) noexcept;
detail::store_resource_file_metadata resource_store_file_metadata(const std::string& logical_path) noexcept;
}
