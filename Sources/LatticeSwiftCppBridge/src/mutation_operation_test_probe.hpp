#pragma once
namespace lattice { class swift_lattice; }
namespace lattice::detail::mutation_operation_test_hooks {
// Private observation after actual journal staging and BEFORE SQL admission.
// No authority, receipt, success state or retry is supplied; production is null.
inline thread_local void (*after_graph_prepared)(swift_lattice&) = nullptr;
}
