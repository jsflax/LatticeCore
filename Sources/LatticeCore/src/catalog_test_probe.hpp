#pragma once
namespace lattice { class database; }
namespace lattice::detail::catalog_test_probe {
// Observation only: no mutable origin, completion, quarantine or slot setter.
// Issued after genuine creator/precheck and before actual prepare admission.
struct probe { void (*before_prepare_admission)(void*, const database*) = nullptr; void* context = nullptr; };
inline thread_local const probe* current = nullptr;
}
