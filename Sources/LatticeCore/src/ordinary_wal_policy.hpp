#pragma once
#include <cstdint>
namespace lattice::detail {
// Shared engineering default, not a capability, grant or physical disk bound.
// Ordinary context issuance remains the sole app-writer policy authority.
inline constexpr std::int64_t ordinary_managed_wal_limit_bytes=64LL*1024*1024;
}
