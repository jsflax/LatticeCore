#pragma once
#include <array>
#include <chrono>
#include <cstdint>
namespace lattice {class lattice_db;}
namespace lattice::detail {
// Passive test DATA only. Nil in ordinary production. The address is solely
// identity for the actual admitted budget; it is not a budget/token/SQL API.
struct observer_delivery_startup_test_probe final {
    static inline thread_local void(*after_replacement_admitted)(const void*)=nullptr;
    struct continuous_handoff_observation {
        const void* admission=nullptr;const void* catalog=nullptr;
        const void* startup=nullptr;const void* writer_open=nullptr;
    };
    // Actual producer AFTER retaining its exact selected startup/token, BEFORE
    // Native open. Passive identity DATA only; never supplies or mutates a
    // policy, token, catalog, Native handle, owner or permission.
    static inline thread_local void(*after_continuous_handoff)(const continuous_handoff_observation&)=nullptr;
    struct continuous_native_open_observation {
        const void* admission=nullptr;const void* startup=nullptr;const void* writer_open=nullptr;
        std::uint64_t device=0,inode=0;
        std::chrono::steady_clock::time_point original_end{};
        bool managed=false,fresh=false;
    };
    // Passive DATA at actual Native return, AFTER known ending of this attempt's
    // pre-open resource original and BEFORE any continuous PRAGMA/lineage SQL.
    // Throwing fails the real factory; presence supplies no grant or completion.
    static inline thread_local void(*after_continuous_native_open)(const continuous_native_open_observation&)=nullptr;
    struct owner_binding_observation {
        const void* catalog=nullptr;const void* budget=nullptr;const void* profile=nullptr;
        std::array<const void*,3> actual{},open{},role_budget{},role_profile{},runtime{};
        unsigned roles=0;
    };
    // Actual constructor-only, AFTER all role leaves returned. Passive DATA;
    // a throw fails the same original setup. No finish/publication authority.
    static inline thread_local void(*after_owner_bound)(lattice_db&,const owner_binding_observation&)=nullptr;
    // Same pending family AFTER actual constructor/owner retention and before
    // enrollment BEGIN. Passive DATA; a throw fails this actual enrollment.
    static inline thread_local void(*after_continuous_enrollment_bound)(lattice_db&,const void*,const owner_binding_observation&)=nullptr;
    struct enrollment_transaction_observation {
        const void* actual=nullptr;const void* admission=nullptr;const void* open=nullptr;
        const void* budget=nullptr;const void* episode=nullptr;bool existing=false;
    };
    // Actual owned WRITE AFTER same-episode prefix/initial lineage reads and
    // BEFORE the first enrollment DML. Core gates ARE held. Passive DATA only;
    // throwing tests the existing real rollback path, never provides a grant.
    static inline thread_local void(*after_continuous_enrollment_reads)(lattice_db&,const enrollment_transaction_observation&)=nullptr;
    // SAME actual owned WRITE after eight exact ledger tables/indexes, three
    // actual policy seed INSERTs and strict present/row/audit checks. Identity
    // DATA only, default null; a throw uses the real prepaid rollback path.
    static inline thread_local void(*after_continuous_enrollment_ledgers)(lattice_db&,const enrollment_transaction_observation&)=nullptr;
    // Passive cut after actual receiver row+counter readback; no Reader or SQL
    // capability is given to the observer. A throw fails the same owned WRITE.
    static inline thread_local void(*after_continuous_enrollment_receiver)(lattice_db&,const enrollment_transaction_observation&,
        std::size_t contribution,std::int64_t channels,std::int64_t bytes)=nullptr;
};
}
