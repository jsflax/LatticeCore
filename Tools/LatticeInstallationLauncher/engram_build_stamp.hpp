#pragma once
#include <array>
#include <cstdint>
#include <string_view>

// Build inputs constrain the actual product binary; they are not an enrollment
// or store-open grant. Only the retained installer controller may derive a
// registration after its actual child exchange, zero exit, reap and closure.
// Product packaging substitutes a generated header only AFTER final memory
// signing. Ordinary Core builds deliberately cannot register an installation.
#ifdef LATTICE_ENGRAM_BUILD_STAMP_HEADER
#include LATTICE_ENGRAM_BUILD_STAMP_HEADER
#else
namespace lattice::detail::ordinary_installation::engram_build_stamp {
inline constexpr bool available = false;
inline constexpr std::string_view product = "engram";
inline constexpr std::string_view source_revision = "";
inline constexpr std::string_view core_revision = "";
inline constexpr std::string_view initializer_leaf = "memory";
inline constexpr std::array<std::uint8_t, 32> initializer_sha256{};
}
#endif
