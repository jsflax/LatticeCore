#pragma once
namespace lattice::detail::mutation_prefix_test_hooks {
// Private POD fault control, consumed only after a real savepoint rollback and
// before recovery vector reserve. No injected success or public authority.
extern thread_local bool fail_next_restore_reserve;
}
