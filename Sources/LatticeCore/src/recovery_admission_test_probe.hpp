#pragma once
#include <lattice/db.hpp>
namespace lattice::detail::recovery_admission_test_hooks {
// A private observation/rendezvous after a real engine query yields a row.
// No provenance is synthesized. Production leaves this thread-local hook null.
inline thread_local void (*after_initial_probe)() = nullptr;
// Actual Core ending has returned and published its empty stamp under the
// physical SQLite mutex. This rendezvous runs off that mutex before delivery;
// it cannot set ownership, completion, authority, result or retry state.
inline thread_local void (*after_core_transaction_end)() = nullptr;
inline thread_local void (*after_read_row)(database&, sqlite3_stmt*) = nullptr;
inline void read_row(database& db, sqlite3_stmt* statement) {
    if (after_read_row) after_read_row(db, statement);
}
}

namespace lattice::detail::recovery_admission_test_hooks {
// Private, passive constructor-failure observation. No function pointer,
// mutex, owner, exception, SQL, authority, retry or admission is stored here.
// Only the constructing thread may install a stack record. Production leaves
// its pointer null; concurrent worker turns cannot write the record.
enum class route_admission_point : unsigned char {
    unobserved, entered, policy_checked, install_returned,
    primary_rethrow, committed, registered
};
enum class route_maintenance_point : unsigned char {
    unobserved, entered, mutex_busy, passed
};
struct route_admission_observation {
    unsigned char route_calls=0, maintenance_calls=0, install_state=255;
    route_admission_point point=route_admission_point::unobserved;
    route_maintenance_point maintenance=route_maintenance_point::unobserved;
    bool primary=false, cleanup=false, postcommit=false, notification=false;
    bool unexpected_commit=false, inside_install=false;
};
inline thread_local route_admission_observation* route_observation=nullptr;
struct route_observation_scope {
    route_admission_observation* prior;
    explicit route_observation_scope(route_admission_observation& record)noexcept
        :prior(route_observation){route_observation=&record;}
    ~route_observation_scope(){route_observation=prior;}
    route_observation_scope(const route_observation_scope&)=delete;
};
inline void route_point(route_admission_point point)noexcept {
    if(auto* out=route_observation)out->point=point;
}
inline void route_entered()noexcept {
    if(auto* out=route_observation){
        if(out->route_calls<2)++out->route_calls;
        out->point=route_admission_point::entered;
    }
}
struct route_install_observation_scope {
    route_admission_observation* record;
    bool prior;
    route_install_observation_scope()noexcept
        :record(route_observation),prior(record&&record->inside_install){
        if(record)record->inside_install=true;
    }
    void finish()noexcept{if(record)record->inside_install=prior;record=nullptr;}
    ~route_install_observation_scope(){finish();}
    route_install_observation_scope(const route_install_observation_scope&)=delete;
};
inline void route_maintenance_entered()noexcept {
    if(auto* out=route_observation;out&&out->inside_install){
        if(out->maintenance_calls<2)++out->maintenance_calls;
        out->maintenance=route_maintenance_point::entered;
    }
}
inline void route_maintenance(route_maintenance_point point)noexcept {
    if(auto* out=route_observation;out&&out->inside_install)out->maintenance=point;
}
}

namespace lattice::detail::recovery_admission_test_hooks {
// Private passive observation only. Production's default is null.
// Emit ONLY after the real admission classifier has proved that every actual
// busy statement is an owned readonly materialization on another thread, and
// after releasing every Core-owned/tracked Native and writer/store/ownership
// gate. Arbitrary caller raw locks are not enumerable and are not released.
// Eligibility is fresh autocommit, with no inherited tracked caller/store/Native
// gate levels. Raw/caller/nested/same-thread cases never gain waiting authority.
// One deadline starts at managed entry: min(positive busy_timeout_ms, 2000ms);
// explicit zero refuses a busy admission immediately and emits no wait. No reset.
// Emission precedes BEGIN/SAVEPOINT, mutation body/checkpoint or any retry grant.
// An observer can rendezvous/throw in a trusted control; it returns no status,
// ownership, cursor, replacement original, permission or successful ending.
// After it returns the producer must still use the ORIGINAL finite budget and
// revalidate actual writer/gate/transaction/read corpus/creator/closed/debt.
// Passive observers must not move/destroy or replace the stable wrapper. The
// producer emits only at its real classified off-gate wait; never self-emit in
// a test before the producer has actually entered this boundary.
inline thread_local void (*after_mutation_owned_read_wait_unlocked)(database&) = nullptr;
inline void mutation_owned_read_wait_unlocked(database& owner) {
    if (after_mutation_owned_read_wait_unlocked) after_mutation_owned_read_wait_unlocked(owner);
}
}

namespace lattice::detail::recovery_admission_test_hooks {
// Passive observation of a FAILED real publication try-lock, emitted only
// after every Core-owned/tracked Native/store/publication leaf is released.
// Initial capture uses the actual immutable owner timeout; retained-writer
// capture must then prove that same policy. All later waits keep the original
// admission deadline. This hook issues no status, owner, permission or retry,
// and must not mutate/destroy/move any observed owner or private lifetime.
// Production leaves the thread-local hook null. Zero/nested/caller/known Core
// callback or Native-frame entries refuse contention without emitting it.
inline thread_local void (*after_mutation_publication_wait_unlocked)(lattice_db&) = nullptr;
inline void mutation_publication_wait_unlocked(lattice_db& owner) {
    if(after_mutation_publication_wait_unlocked)after_mutation_publication_wait_unlocked(owner);
}
}
