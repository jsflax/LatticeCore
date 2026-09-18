// Candidate regression source. Define the adapter only for the exact-source red;
// define LATTICE_READER_BASELINE_ADAPTER only for the exact-source red experiment.
// The baseline adapter is intentionally non-owning and never queries a reader
// after the destruction marker fires. It is not a proposed production fallback.
// This executable calls SQLite directly; it is not a loadable extension.
// sqlite-vec.h is transitively imported by the bridge header on Linux.
#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <lattice.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <set>
#include <thread>
#include <vector>
#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <csignal>
#include <unistd.h>

namespace {
using Clock = std::chrono::steady_clock;
struct Gate {
    std::mutex mutex;
    std::condition_variable condition;
    bool ready = false, release = false;
    void wait_ready() {
        std::unique_lock hold(mutex);
        if (!condition.wait_for(hold, std::chrono::seconds(2), [&] { return ready; }))
            throw std::runtime_error("borrow did not become ready");
    }
    void enter() {
        std::unique_lock hold(mutex);
        ready = true; condition.notify_all();
        condition.wait(hold, [&] { return release; });
    }
    void finish() noexcept {
        std::lock_guard hold(mutex); release = true; condition.notify_all();
    }
};
struct Marker { std::atomic<unsigned> destroyed{0}; };
void mark_destroyed(void* p) noexcept {
    static_cast<Marker*>(p)->destroyed.fetch_add(1, std::memory_order_release);
}
void install_marker(lattice::database& db, Marker& marker) {
    // No prepared statement survives this call. xDestroy runs when the exact
    // connection is closed, not when an unrelated wrapper/path is retired.
    const int rc = sqlite3_create_function_v2(db.handle(), "reader_lifetime_marker", 0,
        SQLITE_UTF8, &marker,
        [](sqlite3_context* c, int, sqlite3_value**) { sqlite3_result_null(c); },
        nullptr, nullptr, mark_destroyed);
    if (rc != SQLITE_OK) throw std::runtime_error("marker registration failed");
}
std::shared_ptr<lattice::database> acquire(lattice::lattice_db& owner, bool xproc) {
#ifdef LATTICE_READER_BASELINE_ADAPTER
    auto* pointer = xproc ? &owner.xproc_read_db() : &owner.read_db();
    return std::shared_ptr<lattice::database>(pointer, [](lattice::database*) {});
#else
    return xproc ? owner.borrow_xproc_read_connection() : owner.borrow_read_connection();
#endif
}
int64_t read_value(lattice::database& db) {
    auto rows = db.query("SELECT value FROM ReaderLifetimeProbe WHERE id=1");
    return std::get<int64_t>(rows.at(0).at("value"));
}
void seed(lattice::lattice_db& owner) {
    owner.db().execute("CREATE TABLE ReaderLifetimeProbe(id INTEGER PRIMARY KEY, globalId TEXT NOT NULL DEFAULT 'reader-row', value INTEGER NOT NULL)");
    owner.db().execute("INSERT INTO ReaderLifetimeProbe(id, value) VALUES(1, 47)");
}
void stop_notifier(const std::string& path) {
    auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path);
    if (!notifier) throw std::runtime_error("native notifier missing");
    notifier->stop_listening();
    if (notifier->is_listening()) throw std::runtime_error("notifier still listening");
}
struct Fixture {
    // Declared before owner: marker storage remains alive through final close.
    Marker marker;
    std::filesystem::path path;
    std::unique_ptr<lattice::swift_lattice> owner;
    explicit Fixture(bool memory = false) {
        path = std::filesystem::temp_directory_path() /
            ("reader-lifetime-" + std::to_string(getpid()) + "-" +
             std::to_string(Clock::now().time_since_epoch().count()) + ".sqlite");
        lattice::swift_configuration config(memory ? ":memory:" : path.string());
        owner = std::make_unique<lattice::swift_lattice>(config, lattice::SchemaVector{});
        if (!memory) stop_notifier(path.string());
        seed(*owner);
    }
    ~Fixture() {
        owner.reset();
        std::error_code ignored;
        for (auto suffix : {"", "-wal", "-shm", "-signal"})
            std::filesystem::remove(path.string() + suffix, ignored);
    }
};
// A primary can hold SQLite ATTACH handles for the secondary. Close both
// owners before either Fixture unlinks its files, including failure unwinds.
struct PairTeardown {
    Fixture& primary;
    Fixture& secondary;
    ~PairTeardown() { primary.owner.reset(); secondary.owner.reset(); }
};
int held_retirement(bool xproc, bool vacuum, bool memory) {
    Fixture fixture(memory);
    auto& owner = *fixture.owner;
    auto& selected = xproc ? owner.xproc_read_db() : owner.read_db();
    if (memory && &selected != &owner.db()) return 10;
    if (!memory && &selected == &owner.db()) return 11;
    install_marker(selected, fixture.marker);
    Gate gate;
    std::exception_ptr worker_error, maintenance_error;
    bool queried = false;
    int64_t observed = -1;
    std::thread reader([&] {
        try {
            auto lease = acquire(owner, xproc);
            gate.enter();
            // The old arm must not touch a dangling pointer merely to make a red.
            if (fixture.marker.destroyed.load(std::memory_order_acquire) == 0) {
                observed = read_value(*lease); queried = true;
            }
        } catch (...) { worker_error = std::current_exception(); }
    });
    unsigned destroyed_while_held = 99;
    try {
        gate.wait_ready();
        if (vacuum) {
            if (!owner.vacuum()) throw std::runtime_error("vacuum returned false");
        } else {
            owner.close_read_db();
        }
        destroyed_while_held = fixture.marker.destroyed.load(std::memory_order_acquire);
    } catch (...) { maintenance_error = std::current_exception(); }
    gate.finish();
    reader.join();
    if (worker_error) std::rethrow_exception(worker_error);
    if (maintenance_error) std::rethrow_exception(maintenance_error);
    const auto destroyed_after_release = fixture.marker.destroyed.load(std::memory_order_acquire);
    std::fprintf(stderr, "reader_lifetime role=%s op=%s memory=%d destroyed_while_held=%u queried=%d value=%lld destroyed_after_release=%u\n",
        xproc ? "xproc" : "ordinary", vacuum ? "vacuum" : "retire", memory,
        destroyed_while_held, queried, static_cast<long long>(observed), destroyed_after_release);
    // A true memory fallback is the still-published writer, so reader retirement
    // must not destroy it even after the temporary borrow is released.
    return destroyed_while_held == 0 && queried && observed == 47 &&
        destroyed_after_release == (memory ? 0u : 1u) ? 0 : 20;
}
int transaction_route() {
    Fixture fixture;
    auto& owner = *fixture.owner;
    owner.begin_transaction();
    owner.db().execute("UPDATE ReaderLifetimeProbe SET value=59 WHERE id=1");
    auto local = acquire(owner, false);
    if (local.get() != &owner.db() || read_value(*local) != 59) return 30;
    int64_t other_value = -1;
    std::exception_ptr failure;
    std::thread other([&] {
        try { auto committed = acquire(owner, false); other_value = read_value(*committed); }
        catch (...) { failure = std::current_exception(); }
    });
    other.join();
    owner.rollback();
    if (failure) std::rethrow_exception(failure);
    if (other_value != 47 || read_value(*acquire(owner, false)) != 47) return 31;
    return 0;
}
int read_only_fallback() {
    Fixture fixture;
    lattice::configuration config(fixture.path.string());
    config.read_only = true;
    Marker marker;
    auto owner = std::make_unique<lattice::lattice_db>(config);
    install_marker(owner->db(), marker);
    {
        auto ordinary = acquire(*owner, false);
        auto xproc = acquire(*owner, true);
        if (ordinary.get() != &owner->db() || xproc.get() != &owner->db()) return 40;
        owner->close_read_db();
        if (marker.destroyed.load() != 0 || read_value(*ordinary) != 47 ||
            read_value(*xproc) != 47) return 41;
    }
    // Marker must outlive physical close, including failure exits above.
    owner.reset();
    return marker.destroyed.load() == 1 ? 0 : 42;
}
#ifndef LATTICE_READER_BASELINE_ADAPTER
// All additional integration cases exercise actual candidate APIs, not the
// non-owning baseline adapter. They have the same fresh-child five-second cap.
int writer_fallback_retirement() {
    Fixture fixture;
    auto& owner = *fixture.owner;
    owner.close_read_db();
    install_marker(owner.db(), fixture.marker);
    auto lease = owner.borrow_xproc_read_connection();
    owner.close_write_db();
    bool refused = false;
    try { (void)owner.query_read("SELECT 1"); }
    catch (const lattice::db_error&) { refused = true; }
    if (!refused || fixture.marker.destroyed.load() != 0 || read_value(*lease) != 47) return 50;
    owner.reopen_write_db();
    if (owner.borrow_read_connection().get() == lease.get() ||
        read_value(owner.db()) != 47 || fixture.marker.destroyed.load() != 0) return 51;
    auto disabled = owner.db().query("SELECT sync_disabled() AS disabled");
    if (std::get<int64_t>(disabled.at(0).at("disabled")) != 0) return 52;
    lease.reset();
    return fixture.marker.destroyed.load() == 1 ? 0 : 53;
}

struct TraceGate {
    Gate gate;
    const char* prefix;
    std::atomic<bool> visited{false}, callback_failed{false};
    explicit TraceGate(const char* value) : prefix(value) {}
    static int callback(unsigned event, void* raw, void* statement, void*) noexcept {
        auto& self = *static_cast<TraceGate*>(raw);
        try {
            if (event != SQLITE_TRACE_STMT) return 0;
            const char* sql = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
            if (!sql || std::strncmp(sql, self.prefix, std::strlen(self.prefix)) != 0) return 0;
            if (!self.visited.exchange(true)) self.gate.enter();
        } catch (...) { self.callback_failed.store(true); }
        return 0;
    }
};

int actual_query_path(int kind) {
    if (std::getenv("LATTICE_DISABLE_XPROC")) return 54;
    Fixture fixture;
    auto& owner = *fixture.owner;
    auto& reader = owner.xproc_read_db(); // installation is serialized before worker starts
    install_marker(reader, fixture.marker);
    TraceGate trace(kind == 0 ? "SELECT id, tableName, operation" :
                    kind == 1 ? "SELECT COUNT(*) FROM AuditLog a" : "PRAGMA data_version");
    if (sqlite3_trace_v2(reader.handle(), SQLITE_TRACE_STMT, TraceGate::callback, &trace) != SQLITE_OK)
        return 55;
    std::atomic<unsigned> idle_hints{0};
    owner.set_on_xproc_idle(&idle_hints, [](void* context) {
        static_cast<std::atomic<unsigned>*>(context)->fetch_add(1);
    }, nullptr);
    std::exception_ptr worker_error, parent_error;
    int64_t result = -1;
    std::thread worker([&] {
        try {
            if (kind == 0) owner.handle_cross_process_notification();
            else if (kind == 1) result = owner.pending_sync_entry_count();
            else result = owner.data_version();
        } catch (...) { worker_error = std::current_exception(); }
    });
    unsigned held = 99;
    try {
        trace.gate.wait_ready();
        owner.close_read_db();
        held = fixture.marker.destroyed.load();
    } catch (...) { parent_error = std::current_exception(); }
    trace.gate.finish(); worker.join();
    owner.set_on_xproc_idle(nullptr, nullptr);
    if (parent_error) std::rethrow_exception(parent_error);
    if (worker_error) std::rethrow_exception(worker_error);
    if (!trace.visited || trace.callback_failed || held != 0 || fixture.marker.destroyed.load() != 1)
        return 56;
    if (kind == 0) return idle_hints.load() == 1 ? 0 : 57;
    return (kind == 1 ? result == 0 : result >= 0) ? 0 : 58;
}

struct Reentry {
    lattice::lattice_db* owner;
    std::atomic<unsigned> destroyed{0}, succeeded{0};
    explicit Reentry(lattice::lattice_db& value) : owner(&value) {}
    static void destroy(void* raw) noexcept {
        auto& self = *static_cast<Reentry*>(raw);
        self.destroyed.fetch_add(1);
        try {
            auto rows = self.owner->query_read("SELECT value FROM ReaderLifetimeProbe WHERE id=1");
            self.owner->detach_alias("never-attached-reader-reentry");
            if (std::get<int64_t>(rows.at(0).at("value")) == 47) self.succeeded.fetch_add(1);
        } catch (...) {}
    }
};
int destructor_reentry(bool replace) {
    Fixture fixture;
    Reentry reentry(*fixture.owner);
    auto& reader = fixture.owner->read_db();
    if (sqlite3_create_function_v2(reader.handle(), "reader_reentry", 0, SQLITE_UTF8, &reentry,
            [](sqlite3_context* c, int, sqlite3_value**) { sqlite3_result_null(c); },
            nullptr, nullptr, Reentry::destroy) != SQLITE_OK) return 59;
    // Both successful paths must destroy this function before the stack state
    // goes away, including if the reentrant query fails. No callback may survive.
    std::exception_ptr error;
    try {
        if (replace) fixture.owner->reopen_read_db();
        else fixture.owner->close_read_db();
    } catch (...) { error = std::current_exception(); }
    fixture.owner->close_read_db();
    if (error) std::rethrow_exception(error);
    return reentry.destroyed.load() == 1 && reentry.succeeded.load() == 1 ? 0 : 60;
}

std::vector<lattice::database::row_t> attached_rows(lattice::database& db) {
    return db.query("SELECT value, _source FROM ReaderLifetimeProbe ORDER BY _source");
}
int attached_reopen() {
    Fixture primary, secondary;
    PairTeardown teardown{primary, secondary};
    secondary.owner->db().execute("UPDATE ReaderLifetimeProbe SET value=53 WHERE id=1");
    primary.owner->attach(*secondary.owner);
    const auto expected = attached_rows(primary.owner->db());
    if (expected.size() != 2 || attached_rows(primary.owner->read_db()) != expected) return 61;
    std::set<int64_t> values;
    bool saw_main = false, saw_attached = false;
    for (const auto& row : expected) {
        values.insert(std::get<int64_t>(row.at("value")));
        const auto& source = std::get<std::string>(row.at("_source"));
        saw_main |= source == "main";
        saw_attached |= source == "\"" + secondary.path.stem().string() + "\"";
    }
    if (values != std::set<int64_t>{47, 53} || !saw_main || !saw_attached) return 61;
    primary.owner->close_read_db();
    primary.owner->reopen_read_db();
    if (attached_rows(*primary.owner->borrow_read_connection()) != expected) return 62;
    primary.owner->close_write_db();
    // A still-published reader does not require a published writer to be usable.
    if (attached_rows(*primary.owner->borrow_read_connection()) != expected) return 63;
    primary.owner->reopen_write_db();
    if (attached_rows(primary.owner->db()) != expected) return 64;
    primary.owner->detach(*secondary.owner);
    return read_value(*primary.owner->borrow_read_connection()) == 47 ? 0 : 65;
}

// A per-child delegating VFS supplies the actual file-open boundary without
// requiring loadable/automatic extension support. All non-injected operations
// receive the original VFS pointer, preserving its private pAppData contract.
// No sqlite3_file/IO-method wrapper or production-only hook is introduced.
struct OpenControl {
    std::atomic<unsigned> calls{0}, attached_calls{0}, injected{0}, gated{0};
    std::atomic<bool> callback_failed{false}, cleanup_failed{false};
    unsigned fail_at = 0;
    bool gate_first = false, deny_attach = false;
    Gate gate;
};
struct OpenRegistration {
    using Symbol = void (*)(void);
    sqlite3_vfs wrapper{};
    sqlite3_vfs* parent = nullptr;
    OpenControl& control;
    std::string primary_path, attached_path;
    bool registered = false;
    static constexpr const char* name = "lattice-reader-lifetime-test-vfs";
    static OpenRegistration& self(sqlite3_vfs* vfs) noexcept {
        return *static_cast<OpenRegistration*>(vfs->pAppData);
    }
    std::string full_path(const std::string& path) {
        if (path.empty()) return {};
        // The fixtures use plain owned paths, never a URI or caller profile.
        // Normalize with the actual parent VFS used for all subsequent opens.
        if (parent->mxPathname <= 0 || parent->mxPathname > 1024 * 1024)
            throw std::runtime_error("unsupported fixture VFS pathname bound");
        std::vector<char> text(static_cast<size_t>(parent->mxPathname) + 1, '\0');
        if (parent->xFullPathname(parent, path.c_str(), static_cast<int>(text.size()), text.data()) != SQLITE_OK ||
            std::memchr(text.data(), '\0', text.size()) == nullptr)
            throw std::runtime_error("fixture VFS full pathname failed");
        return text.data();
    }
    static int open(sqlite3_vfs* vfs, const char* path, sqlite3_file* file,
                    int flags, int* output_flags) noexcept {
        auto& value = self(vfs);
        // Required even for xOpen failure: SQLite may inspect pMethods.
        file->pMethods = nullptr;
        if (output_flags) *output_flags = 0;
        try {
            const bool database_file = (flags & SQLITE_OPEN_MAIN_DB) != 0;
            const bool primary = database_file && path && value.primary_path == path;
            const bool attachment = database_file && path && !value.attached_path.empty() && value.attached_path == path;
            if (primary) {
                // Reopen must create read-only readers; fail closed on a route
                // change instead of silently injecting into a writer.
                if (!(flags & SQLITE_OPEN_READONLY) || (flags & SQLITE_OPEN_READWRITE)) {
                    value.control.callback_failed.store(true);
                    return SQLITE_CANTOPEN;
                }
                const auto call = value.control.calls.fetch_add(1) + 1;
                if (value.control.gate_first && call == 1) {
                    value.control.gated.fetch_add(1);
                    value.control.gate.enter();
                }
                if (value.control.fail_at == call) {
                    value.control.injected.fetch_add(1);
                    return SQLITE_CANTOPEN;
                }
            }
            if (attachment) {
                value.control.attached_calls.fetch_add(1);
                if (value.control.deny_attach) {
                    value.control.injected.fetch_add(1);
                    return SQLITE_CANTOPEN;
                }
            }
            return value.parent->xOpen(value.parent, path, file, flags, output_flags);
        } catch (...) {
            value.control.callback_failed.store(true);
            return SQLITE_IOERR;
        }
    }
    OpenRegistration(OpenControl& value, const std::string& primary,
                     const std::string& attachment = {}) : control(value) {
        parent = sqlite3_vfs_find(nullptr);
        if (!parent || parent->iVersion < 1 || parent->iVersion > 3 ||
            !parent->xOpen || !parent->xFullPathname || sqlite3_vfs_find(name))
            throw std::runtime_error("unsupported or occupied fixture VFS");
        primary_path = full_path(primary); attached_path = full_path(attachment);
        // Read only fields defined by the parent's advertised ABI version.
        // Every callback forwards the parent, never this wrapper, to its
        // original method. The SQLite-owned file retains the parent IO methods.
        wrapper.iVersion = parent->iVersion;
        wrapper.szOsFile = parent->szOsFile; wrapper.mxPathname = parent->mxPathname;
        wrapper.zName = name; wrapper.pAppData = this;
        wrapper.xOpen = open;
        wrapper.xDelete = [](sqlite3_vfs* v, const char* p, int sync) {
            auto* b = self(v).parent; return b->xDelete(b, p, sync);
        };
        wrapper.xAccess = [](sqlite3_vfs* v, const char* p, int flags, int* out) {
            auto* b = self(v).parent; return b->xAccess(b, p, flags, out);
        };
        wrapper.xFullPathname = [](sqlite3_vfs* v, const char* p, int n, char* out) {
            auto* b = self(v).parent; return b->xFullPathname(b, p, n, out);
        };
        if (parent->xDlOpen) wrapper.xDlOpen = [](sqlite3_vfs* v, const char* p) {
            auto* b = self(v).parent; return b->xDlOpen(b, p);
        };
        if (parent->xDlError) wrapper.xDlError = [](sqlite3_vfs* v, int n, char* out) {
            auto* b = self(v).parent; b->xDlError(b, n, out);
        };
        if (parent->xDlSym) wrapper.xDlSym = [](sqlite3_vfs* v, void* h, const char* p) -> Symbol {
            auto* b = self(v).parent; return b->xDlSym(b, h, p);
        };
        if (parent->xDlClose) wrapper.xDlClose = [](sqlite3_vfs* v, void* h) {
            auto* b = self(v).parent; b->xDlClose(b, h);
        };
        wrapper.xRandomness = [](sqlite3_vfs* v, int n, char* out) {
            auto* b = self(v).parent; return b->xRandomness(b, n, out);
        };
        wrapper.xSleep = [](sqlite3_vfs* v, int n) {
            auto* b = self(v).parent; return b->xSleep(b, n);
        };
        wrapper.xCurrentTime = [](sqlite3_vfs* v, double* out) {
            auto* b = self(v).parent; return b->xCurrentTime(b, out);
        };
        if (parent->xGetLastError) wrapper.xGetLastError = [](sqlite3_vfs* v, int n, char* out) {
            auto* b = self(v).parent; return b->xGetLastError(b, n, out);
        };
        if (parent->iVersion >= 2 && parent->xCurrentTimeInt64)
            wrapper.xCurrentTimeInt64 = [](sqlite3_vfs* v, sqlite3_int64* out) {
                auto* b = self(v).parent; return b->xCurrentTimeInt64(b, out);
            };
        if (parent->iVersion >= 3) {
            if (parent->xSetSystemCall) wrapper.xSetSystemCall = [](sqlite3_vfs* v, const char* p, sqlite3_syscall_ptr f) {
                auto* b = self(v).parent; return b->xSetSystemCall(b, p, f);
            };
            if (parent->xGetSystemCall) wrapper.xGetSystemCall = [](sqlite3_vfs* v, const char* p) {
                auto* b = self(v).parent; return b->xGetSystemCall(b, p);
            };
            if (parent->xNextSystemCall) wrapper.xNextSystemCall = [](sqlite3_vfs* v, const char* p) {
                auto* b = self(v).parent; return b->xNextSystemCall(b, p);
            };
        }
        const int rc = sqlite3_vfs_register(&wrapper, 1);
        if (rc != SQLITE_OK) throw std::runtime_error("fixture VFS registration failed");
        registered = true;
        if (sqlite3_vfs_find(nullptr) != &wrapper) {
            restore(); throw std::runtime_error("fixture VFS did not become default");
        }
    }
    void restore() noexcept {
        if (!registered) return;
        // Only after opener.join() and destruction of every unpublished staged
        // database. Existing fixture owners were opened through parent before
        // registration. No new reader is published in any injected case.
        const bool current = sqlite3_vfs_find(nullptr) == &wrapper;
        const int restore_rc = sqlite3_vfs_register(parent, 1);
        const int remove_rc = sqlite3_vfs_unregister(&wrapper);
        if (!current || restore_rc != SQLITE_OK || remove_rc != SQLITE_OK ||
            sqlite3_vfs_find(nullptr) != parent || sqlite3_vfs_find(name) != nullptr)
            control.cleanup_failed.store(true);
        registered = false;
    }
    ~OpenRegistration() { restore(); }
    OpenRegistration(const OpenRegistration&) = delete;
    OpenRegistration& operator=(const OpenRegistration&) = delete;
};
// Even an unexpected successful publication must not retain a connection
// referencing the stack VFS after unregistration. Evaluate the publication
// oracle first; then retire the test-owned pair on every exit.
struct RetireBeforeVfsRestore {
    lattice::lattice_db& owner;
    ~RetireBeforeVfsRestore() noexcept {
        // stop_listening on Darwin cancels future delivery but may leave an
        // already-copied callback. close() first disables/drains instance guard
        // holds and the scheduler before retiring any newly published reader.
        // No fixture observer borrows the new pair after this quiescence.
        try { owner.close(); owner.close_read_db(); }
        catch (...) {
            std::fputs("reader_lifetime VFS owner quiescence failed\n", stderr);
            _exit(93); // Do not unwind a stack VFS whose borrowers are unproven.
        }
    }
};
int failed_reopen(bool topology) {
    Fixture fixture, secondary;
    PairTeardown teardown{fixture, secondary};
    auto& owner = *fixture.owner;
    if (topology) owner.attach(*secondary.owner);
    auto ordinary = owner.borrow_read_connection();
    auto xproc = owner.borrow_xproc_read_connection();
    const auto expected = ordinary->query("SELECT value FROM ReaderLifetimeProbe");
    OpenControl control;
    control.fail_at = topology ? 0 : 2;
    control.deny_attach = topology;
    bool refused = false, unchanged = false;
    {
        OpenRegistration registration(control, fixture.path.string(), topology ? secondary.path.string() : std::string{});
        RetireBeforeVfsRestore retire{owner};
        try { owner.reopen_read_db(); }
        catch (const lattice::db_error&) { refused = true; }
        unchanged = owner.borrow_read_connection() == ordinary && owner.borrow_xproc_read_connection() == xproc &&
            ordinary->query("SELECT value FROM ReaderLifetimeProbe") == expected;
    }
    return refused && control.calls.load() == 2 && control.injected.load() == 1 &&
        control.attached_calls.load() == (topology ? 1u : 0u) && unchanged &&
        !control.callback_failed.load() && !control.cleanup_failed.load() ? 0 : 66;
}
int late_reopen(bool logical_close) {
    Fixture fixture;
    auto& owner = *fixture.owner;
    auto original = owner.borrow_read_connection();
    OpenControl control; control.gate_first = true;
    std::exception_ptr parent_error, worker_error;
    bool refused = false;
    int result = 67;
    {
        OpenRegistration registration(control, fixture.path.string());
        RetireBeforeVfsRestore retire{owner};
        std::thread opener([&] {
            try { owner.reopen_read_db(); }
            catch (const lattice::db_error& error) {
                refused = std::string(error.what()).find("invalidated by concurrent maintenance") != std::string::npos;
                if (!refused) worker_error = std::current_exception();
            } catch (...) { worker_error = std::current_exception(); }
        });
        try {
            control.gate.wait_ready();
            if (logical_close) static_cast<lattice::lattice_db&>(owner).close();
            else owner.close_read_db();
        } catch (...) { parent_error = std::current_exception(); }
        control.gate.finish(); opener.join();
        if (parent_error) std::rethrow_exception(parent_error);
        if (worker_error) std::rethrow_exception(worker_error);
        if (refused && control.calls.load() == 2) {
            if (logical_close) {
                result = owner.is_closed() && owner.borrow_read_connection() == original &&
                    owner.query_read("SELECT value FROM ReaderLifetimeProbe").empty() ? 0 : 68;
            } else {
                result = owner.borrow_read_connection().get() == &owner.db() && read_value(owner.db()) == 47 ? 0 : 69;
            }
        }
    }
    if (control.gated.load() != 1 || control.injected.load() != 0 ||
        control.callback_failed.load() || control.cleanup_failed.load()) return 67;
    return result;
}
int active_statement_checkpoint() {
    Fixture fixture;
    auto& owner = *fixture.owner;
    auto lease = owner.borrow_read_connection();
    sqlite3_stmt* raw = nullptr;
    if (sqlite3_prepare_v2(lease->handle(), "SELECT value FROM ReaderLifetimeProbe", -1, &raw, nullptr) != SQLITE_OK)
        return 70;
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(raw, sqlite3_finalize);
    if (sqlite3_step(statement.get()) != SQLITE_ROW) return 71;
    owner.db().execute("UPDATE ReaderLifetimeProbe SET value=59 WHERE id=1");
    owner.close_read_db();
    // A fixture-only short busy handler makes the real busy result bounded;
    // no production timeout, VACUUM result, or checkpoint status is rewritten.
    if (sqlite3_busy_timeout(owner.db().handle(), 1) != SQLITE_OK) return 72;
    const auto blocked = owner.db().query("PRAGMA wal_checkpoint(TRUNCATE)");
    if (std::get<int64_t>(blocked.at(0).at("busy")) != 1 ||
        sqlite3_column_int64(statement.get(), 0) != 47) return 73;
    statement.reset(); lease.reset();
    const auto completed = owner.db().query("PRAGMA wal_checkpoint(TRUNCATE)");
    return std::get<int64_t>(completed.at(0).at("busy")) == 0 && read_value(owner.db()) == 59 ? 0 : 74;
}
int busy_reopen(bool already_retired) {
    Fixture fixture, secondary;
    PairTeardown teardown{fixture, secondary};
    auto& owner = *fixture.owner;
    auto original = owner.borrow_read_connection();
    auto xproc = owner.borrow_xproc_read_connection();
    if (already_retired) owner.close_read_db();
    TraceGate trace("ATTACH DATABASE ");
    if (sqlite3_trace_v2(owner.db().handle(), SQLITE_TRACE_STMT, TraceGate::callback, &trace) != SQLITE_OK) return 78;
    std::exception_ptr worker_error, parent_error;
    std::thread attaching([&] {
        try { owner.attach(*secondary.owner); }
        catch (...) { worker_error = std::current_exception(); }
    });
    bool refused = false, unchanged = false;
    try {
        trace.gate.wait_ready();
        try { owner.reopen_read_db(); }
        catch (const lattice::db_error& error) {
            refused = std::string(error.what()).find("attachment topology is busy") != std::string::npos;
            if (!refused) throw;
        }
        unchanged = already_retired
            ? owner.borrow_read_connection().get() == &owner.db() &&
              owner.borrow_xproc_read_connection().get() == &owner.db()
            : owner.borrow_read_connection() == original && owner.borrow_xproc_read_connection() == xproc;
    } catch (...) { parent_error = std::current_exception(); }
    trace.gate.finish(); attaching.join();
    sqlite3_trace_v2(owner.db().handle(), 0, nullptr, nullptr);
    if (parent_error) std::rethrow_exception(parent_error);
    if (worker_error) std::rethrow_exception(worker_error);
    if (!refused || !unchanged || trace.callback_failed) return 79;
    owner.reopen_read_db();
    return attached_rows(*owner.borrow_read_connection()).size() == 2 ? 0 : 80;
}
int attach_keeps_snapshot() {
    Fixture fixture, secondary;
    PairTeardown teardown{fixture, secondary};
    auto& owner = *fixture.owner;
    install_marker(owner.read_db(), fixture.marker);
    TraceGate trace("ATTACH DATABASE ");
    if (sqlite3_trace_v2(owner.db().handle(), SQLITE_TRACE_STMT, TraceGate::callback, &trace) != SQLITE_OK) return 75;
    std::exception_ptr worker_error, parent_error;
    std::thread attaching([&] {
        try { owner.attach(*secondary.owner); }
        catch (...) { worker_error = std::current_exception(); }
    });
    unsigned held = 99;
    try {
        trace.gate.wait_ready();
        owner.close_read_db();
        held = fixture.marker.destroyed.load();
    } catch (...) { parent_error = std::current_exception(); }
    trace.gate.finish(); attaching.join();
    sqlite3_trace_v2(owner.db().handle(), 0, nullptr, nullptr);
    if (parent_error) std::rethrow_exception(parent_error);
    if (worker_error) std::rethrow_exception(worker_error);
    if (held != 0 || fixture.marker.destroyed.load() != 1 || trace.callback_failed) return 76;
    owner.reopen_read_db();
    return attached_rows(*owner.borrow_read_connection()).size() == 2 ? 0 : 77;
}
#endif

int bounded(int kind) {
    sigset_t mask; sigemptyset(&mask); sigaddset(&mask, SIGALRM);
    if (sigprocmask(SIG_UNBLOCK, &mask, nullptr) != 0) return 90;
    std::signal(SIGALRM, SIG_DFL); alarm(5);
    try {
        int result;
#ifndef LATTICE_READER_BASELINE_ADAPTER
        if (kind == 8) result = writer_fallback_retirement();
        else if (kind >= 9 && kind <= 11) result = actual_query_path(kind - 9);
        else if (kind == 12 || kind == 13) result = destructor_reentry(kind == 13);
        else if (kind == 14) result = attached_reopen();
        else if (kind == 15 || kind == 16) result = failed_reopen(kind == 16);
        else if (kind == 17 || kind == 18) result = late_reopen(kind == 18);
        else if (kind == 19) result = active_statement_checkpoint();
        else if (kind == 20) result = attach_keeps_snapshot();
        else if (kind == 21 || kind == 22) result = busy_reopen(kind == 22);
        else
#endif
        result = kind == 7 ? read_only_fallback() : kind == 6 ? transaction_route() :
            held_retirement((kind % 2) != 0, kind == 2 || kind == 3, kind == 4 || kind == 5);
        alarm(0); return result;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "reader_lifetime exception=%s\n", error.what()); return 91;
    } catch (...) { return 92; }
}
} // namespace

// Every blocking wait runs in a fresh exec child with a five-second alarm.
// A failed readiness/ownership test cannot wedge the full suite during join.
#define READER_CASE(NAME, INDEX) \
TEST(ReaderLifetime, NAME) { \
    ::testing::FLAGS_gtest_death_test_style = "threadsafe"; \
    ASSERT_EXIT({ _exit(bounded(INDEX)); }, ::testing::ExitedWithCode(0), ""); \
}
READER_CASE(OrdinaryBorrowSurvivesRetirement, 0)
READER_CASE(XprocBorrowSurvivesRetirement, 1)
READER_CASE(OrdinaryBorrowSurvivesVacuum, 2)
READER_CASE(XprocBorrowSurvivesVacuum, 3)
READER_CASE(MemoryReadFallbackRetainsWriter, 4)
READER_CASE(MemoryXprocFallbackRetainsWriter, 5)
READER_CASE(TransactionOwnerAndOtherThreadKeepTheirRoutes, 6)
READER_CASE(ReadOnlyReadAndXprocFallbackRetainPrimary, 7)
#ifndef LATTICE_READER_BASELINE_ADAPTER
READER_CASE(RetiredFallbackWriterRemainsOwnedUntilQueryRelease, 8)
READER_CASE(ActualXprocHandlerOwnsReaderDuringRetirement, 9)
READER_CASE(BridgePendingCountOwnsReaderDuringRetirement, 10)
READER_CASE(BridgeDataVersionOwnsReaderDuringRetirement, 11)
READER_CASE(RetiredReaderDestructorMayReenterQueryAndTopology, 12)
READER_CASE(ReplacedReaderDestructorMayReenterQueryAndTopology, 13)
READER_CASE(ReopenedReaderAndWriterRestoreAttachedViews, 14)
READER_CASE(FailedSecondOpenKeepsEntirePublishedReaderPair, 15)
READER_CASE(FailedTopologyRestoreKeepsEntirePublishedReaderPair, 16)
READER_CASE(ReaderRetirementInvalidatesStagedReopen, 17)
READER_CASE(LogicalCloseInvalidatesStagedReopen, 18)
READER_CASE(ActiveRetiredStatementPreservesBusyCheckpointResult, 19)
READER_CASE(AttachOwnsEveryViewHandleUntilTopologyUnlock, 20)
READER_CASE(BusyReopenPreservesPublishedPair, 21)
READER_CASE(BusyReopenPreservesAlreadyRetiredFallback, 22)
#endif
#undef READER_CASE
#endif
