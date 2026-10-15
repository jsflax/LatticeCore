#pragma once

#include <sqlite3.h>

namespace lattice::detail {

// Internal deterministic test seam, thread-local and inert unless explicitly
// scoped by a native test. Not a public callback or an ownership/lifetime API.
// The callback must not throw, allocate, run SQL, close/move the owner, or call
// user code. A test may use a bounded barrier to let another thread inspect
// mutex ownership. The owner and context must outlive this scoped probe.
enum class checkpoint_probe_stage { result_captured, before_generation_retirement };

struct checkpoint_test_probe {
    using callback = void (*)(checkpoint_probe_stage, void*) noexcept;
    sqlite3* connection;
    callback function;
    void* context;
    checkpoint_test_probe* previous;
    static inline thread_local checkpoint_test_probe* current = nullptr;

    checkpoint_test_probe(sqlite3* db, callback fn, void* arg) noexcept
        : connection(db), function(fn), context(arg), previous(current) { current = this; }
    ~checkpoint_test_probe() noexcept { current = previous; }
    checkpoint_test_probe(const checkpoint_test_probe&) = delete;
    checkpoint_test_probe& operator=(const checkpoint_test_probe&) = delete;

    static void fire(sqlite3* db, checkpoint_probe_stage stage) noexcept {
        if (auto* probe = current; probe && probe->connection == db && probe->function)
            probe->function(stage, probe->context);
    }
};

} // namespace lattice::detail
