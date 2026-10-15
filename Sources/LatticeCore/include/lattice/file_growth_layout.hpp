#pragma once
#ifdef __cplusplus
#include <cstdint>
namespace lattice {
/// Immutable layout computed only by the retained original accounting owner.
/// Copied limits are descriptive: not SQL-write headroom, outcome durability,
/// an authority grant or physical capacity. No caller-selected partition.
struct managed_fixed_file_ceiling {
    static constexpr int64_t page_size = 4096;
    static constexpr int64_t maximum_sector_size = 65536;
    int64_t main_bytes = 0, journal_bytes = 0, maximum_page_count = 0;
};
} // namespace lattice
#endif
