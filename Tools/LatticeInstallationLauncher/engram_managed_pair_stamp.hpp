#pragma once
#include <array>
#include <cstdint>
// Closed product image binding, compiled only after memory-sync's final signing.
// No signature, installation, profile or source authority follows from this hash.
#ifdef LATTICE_ENGRAM_MANAGED_PAIR_STAMP_HEADER
#include LATTICE_ENGRAM_MANAGED_PAIR_STAMP_HEADER
#else
namespace lattice::detail::ordinary_installation::engram_managed_pair_stamp {
inline constexpr bool available=false;
inline constexpr std::array<std::uint8_t,32> daemon_sha256{};
}
#endif
