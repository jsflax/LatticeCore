#pragma once

// Opt-in benchmark support only. No declarations, state, symbols or hook work
// exist in an ordinary build. Define this consistently for Core, its bridge,
// the Clang importer, and the benchmark consumer; never mix compiled graphs.
#if defined(LATTICE_SYNC_COMMIT_PROBE)
#if defined(__EMSCRIPTEN__)
#error "The sync commit probe qualifies native file-backed WAL stores only"
#endif

#include <cstdint>

struct sqlite3;

namespace lattice {

// A copy of one native-thread-local slot. Only finish transfers it out of the
// scope; publish that copy with the harness's own release/acquire protocol.
// Pointer identities are diagnostic addresses, never dereferenceable handles.
// thread_identity is the TLS slot address, not an OS thread ID. A run must not
// reuse operation/attempt identities, including after worker-thread teardown.
struct sync_commit_probe_receipt {
    // 0 = recorded, 1 = no matching WAL before finish, 2 = no active scope,
    // 3 = wrong owner or operation/attempt on this thread (scope preserved).
    int32_t status = 2;
    uint64_t operation_id = 0;
    uint64_t attempt_id = 0;
    uint64_t armed_ns = 0;
    uint64_t postcommit_ns = 0;
    uint64_t owner_identity = 0;
    uint64_t connection_identity = 0;
    uint64_t thread_identity = 0;
    uint64_t ignored_owner_commits = 0;
    uint64_t ignored_schema_commits = 0;
};

// Use this SAME clock for writer start/return and the public receiver read.
// This is a postcommit observation clock, not a physical-commit/fsync timestamp.
uint64_t sync_commit_probe_clock_ns() noexcept;

namespace sync_commit_probe_detail {
// Called only by lattice_db after validating its already-owned write txn.
// Arm: 0 success, 1 zero identity, 2 active scope. Additional owner admission
// errors (3 unsupported/closed, 4 no owned write transaction) come from it.
int32_t arm(const void* owner, sqlite3* connection,
            uint64_t operation, uint64_t attempt) noexcept;
sync_commit_probe_receipt finish(const void* owner, uint64_t operation,
                                  uint64_t attempt) noexcept;
// Same-thread qualification inspection. Does not finish/rearm the slot.
sync_commit_probe_receipt snapshot() noexcept;
// Existing WAL hook calls this BEFORE pressure handling and notification work.
// No allocation, SQL, lock, callback, exception or owner/connection dereference.
void record(const void* owner, sqlite3* connection, const char* schema) noexcept;
// A rolled-back attempt cannot accidentally acquire a later commit even if
// its caller has not yet reached finish. Keeps the missing-record receipt.
void rolled_back(const void* owner, sqlite3* connection) noexcept;
} // namespace sync_commit_probe_detail
} // namespace lattice
#endif
