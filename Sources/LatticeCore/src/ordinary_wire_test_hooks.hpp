#pragma once
#include <functional>
#include <cstddef>
namespace lattice {class lattice_db;}
namespace lattice::detail::ordinary_wire_test_hooks {
// Source-test rendezvous only, null in production. The real child exists but
// is not published; the caller retains it synchronously. Hooks may observe or
// inject faults, never fabricate origin/source/transport authority or escape
// this borrowed pointer. Any SQLite callback context must outlive actual child
// destruction. No parent/SQLite ownership leaf is held at this rendezvous.
extern thread_local std::function<void(lattice_db&)> after_child_open;
// Actual shared_ptr supplied allocator calls this immediately before its real
// control-block allocation. Throwing exercises the constructor's own deleter
// exception guard. Null in production; no release, issuer or completion DTO.
inline thread_local std::function<void()> before_history_cut_control_block_allocate;
// Passive actual pool observation; does not reserve, release or issue custody.
size_t receive_slots()noexcept;
}
