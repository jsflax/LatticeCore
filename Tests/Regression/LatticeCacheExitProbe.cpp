#include <lattice.hpp>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <variant>
#include <unistd.h>

// A fresh standalone process is required: the verifier must be registered
// before the first cache access, so it runs after an old cache's destructor.
namespace {
struct Allocation {
    unsigned allocated = 0;
    unsigned deallocated = 0;
    unsigned objectsDestroyed = 0;
};

template<class T> struct TrackedAllocator {
    using value_type = T;
    Allocation* state;
    explicit TrackedAllocator(Allocation* value) : state(value) {}
    template<class U> TrackedAllocator(const TrackedAllocator<U>& other) : state(other.state) {}
    T* allocate(std::size_t count) {
        T* result = std::allocator<T>{}.allocate(count);
        ++state->allocated;
        return result;
    }
    void deallocate(T* pointer, std::size_t count) noexcept {
        ++state->deallocated;
        std::allocator<T>{}.deallocate(pointer, count);
    }
    template<class U> bool operator==(const TrackedAllocator<U>& other) const noexcept {
        return state == other.state;
    }
};

struct ProbeState {
    Allocation control, registered;
    std::string databasePath;
    std::shared_ptr<lattice::swift_lattice> live;
    bool ready = false;
};
// Intentionally process-lived: the exit verifier must not inspect dead state.
ProbeState* state = nullptr;

[[noreturn]] void finish(int code, const char* event) {
    std::fprintf(stdout, "{\"event\":\"%s\",\"exit\":%d}\n", event, code);
    std::fflush(stdout);
    std::_Exit(code);
}
void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

std::shared_ptr<lattice::swift_lattice> tracked(Allocation& allocation) {
    return std::shared_ptr<lattice::swift_lattice>(
        new lattice::swift_lattice(lattice::swift_configuration(":memory:"), lattice::SchemaVector{}),
        [marker = &allocation](lattice::swift_lattice* value) {
            delete value;
            ++marker->objectsDestroyed;
        }, TrackedAllocator<lattice::swift_lattice>(&allocation));
}

bool markerMatches(lattice::swift_lattice& value) {
    const auto rows = value.db().query("SELECT value FROM CacheExitProbe", {});
    return rows.size() == 1 && rows[0].contains("value") &&
        std::holds_alternative<int64_t>(rows[0].at("value")) &&
        std::get<int64_t>(rows[0].at("value")) == 731;
}

void verifyAfterStaticDestruction() noexcept {
    if (!state || !state->ready) finish(71, "setup_incomplete");
    const auto& registered = state->registered;
    // This marker, not undefined access to an already-destroyed cache, is the
    // negative oracle. The old static destructor releases its final weak ref.
    if (registered.deallocated != 0) finish(70, "weak_registry_destroyed");
    if (registered.allocated == 0 || registered.objectsDestroyed != 1)
        finish(72, "invalid_lifetime_control");
    try {
        // The actual database was read and closed before exit. Only resolve
        // its retained object identity here, without unrelated SQL/static state.
        auto resolved = lattice::detail::LatticeCache::instance().get_by_pointer(state->live.get());
        require(resolved && resolved == state->live, "late cache identity changed");
        finish(0, "late_cache_identity_passed");
    } catch (...) {
        finish(73, "late_cache_identity_failed");
    }
}

std::filesystem::path fixtureDirectory(int argc, char** argv) {
    require(argc == 2, "supply one existing caller-owned localdev artifact directory");
    const char* home = std::getenv("HOME");
    require(home && *home, "HOME is unavailable");
    const std::filesystem::path input(argv[1]);
    require(input.is_absolute(), "artifact directory must be absolute");
    const auto localdev = std::filesystem::canonical(std::filesystem::path(home) / "localdev");
    const auto root = std::filesystem::canonical(input);
    require(std::filesystem::is_directory(root), "artifact root is not a directory");
    const auto relative = root.lexically_relative(localdev);
    require(!relative.empty() && relative != "." && !relative.is_absolute(), "supply a directory below localdev");
    for (const auto& part : relative) require(part != "..", "artifact root is outside localdev");
    const auto fixture = root / ("cache-exit-" + std::to_string(::getpid()));
    require(std::filesystem::create_directory(fixture), "fixture directory must be new");
    return fixture;
}
} // namespace

int main(int argc, char** argv) {
    try {
        const auto directory = fixtureDirectory(argc, argv);
        state = new ProbeState;
        state->databasePath = (directory / "fixture.sqlite").string();
        lattice::set_log_level(lattice::log_level::off);
        require(std::atexit(verifyAfterStaticDestruction) == 0, "atexit registration failed");
        auto& cache = lattice::detail::LatticeCache::instance();

        // Healthy allocator control: an unregistered object's control block
        // must really be freed once its final strong reference is released.
        auto control = tracked(state->control);
        control.reset();
        require(state->control.objectsDestroyed == 1 && state->control.allocated > 0 &&
            state->control.deallocated == state->control.allocated, "unregistered allocator control failed");

        // The retained fixture proves normal create/read/close and gives the
        // late callback a pointer lookup without unrelated static teardown work.
        state->live = cache.get_or_create(lattice::swift_configuration(state->databasePath), lattice::SchemaVector{});
        require(bool(state->live), "fixture creation failed");
        // Ordinary model-table inserts resolve globalId by the SQLite rowid.
        state->live->db().execute("CREATE TABLE CacheExitProbe("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, globalId TEXT UNIQUE NOT NULL, value INTEGER NOT NULL)");
        state->live->db().execute("INSERT INTO CacheExitProbe(globalId, value) "
            "VALUES('c1498478-6c65-4a4c-843b-1e1050c318fb', 731)");
        require(markerMatches(*state->live), "normal SQLite read failed");
        state->live->close();
        require(cache.get_by_pointer(state->live.get()) == state->live, "closed live fixture lost its identity");

        auto sentinel = tracked(state->registered);
        cache.register_pointer(sentinel);
        require(cache.get_by_pointer(sentinel.get()) == sentinel, "registered identity missing");
        sentinel.reset();
        require(state->registered.objectsDestroyed == 1 && state->registered.allocated > 0 &&
            state->registered.deallocated == 0, "cache did not retain the weak control block");
        // Never resolve the expired sentinel again: that would reap its weak
        // entry before the exit oracle. No other explicit weak owner exists.
        state->ready = true;
        std::fputs("{\"event\":\"controls_passed\"}\n", stdout);
        std::fflush(stdout);
        return 0; // Normal exit invokes static destructors and then the verifier.
    } catch (const std::exception& error) {
        std::fprintf(stderr, "cache-exit probe setup failed: %s\n", error.what());
        finish(74, "setup_failed");
    } catch (...) {
        finish(75, "setup_failed_unknown");
    }
}
