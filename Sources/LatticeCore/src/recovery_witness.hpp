#pragma once
#include <array>
#include <cstdint>
#include <optional>

namespace lattice {
class database;
class lattice_db;
namespace detail {
// A local content-refresh witness, NOT source authority, a sync cursor, an
// operation receipt, or evidence that a consumer rendered anything.
struct recovery_witness {
    std::array<uint8_t, 16> incarnation{};
    int64_t generation = 0;
    bool operator==(const recovery_witness&) const = default;
};
// Internal friendship preserves physical-handle custody without invoking the
// public raw-handle escape hatch or changing a projection's admission state.
struct recovery_witness_access {
    static recovery_witness bump(lattice_db&);
    static std::optional<recovery_witness> read(database&);
};
// Only the actual newly-applied scoped installation calls this. Requires the
// caller's exact owned write transaction; creation and bump share its rollback.
// Never call merely because recovery_writer_access::install was entered.
recovery_witness bump_recovery_witness(lattice_db&);
// Bounded, exact scalar read. Absence is legacy/no witness; malformed existing
// state refuses. The caller must supply its privately owned committed read
// view (this helper does not turn a writer's uncommitted view into evidence).
std::optional<recovery_witness> read_recovery_witness(database&);
} // namespace detail
} // namespace lattice
