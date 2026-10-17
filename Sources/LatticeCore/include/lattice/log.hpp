#pragma once

#ifdef __cplusplus

#include <cstdio>
#include <atomic>
#include <mutex>

namespace lattice {

enum class log_level : int {
    off = 0,
    error = 1,
    warn = 2,
    info = 3,
    debug = 4
};

/// Single global log level — defined in LatticeCore/src/lattice.cpp.
extern std::atomic<log_level> g_log_level;

inline void set_log_level(log_level level) {
    g_log_level.store(level, std::memory_order_relaxed);
}

inline log_level get_log_level() {
    return g_log_level.load(std::memory_order_relaxed);
}

}  // namespace lattice

#define LATTICE_LOG(level, tag, fmt, ...) \
    do { \
        if (static_cast<int>(level) <= static_cast<int>(lattice::g_log_level.load(std::memory_order_relaxed))) { \
            lattice::log_write("[" tag "] " fmt "\n", ##__VA_ARGS__); \
        } \
    } while(0)

namespace lattice {
/// When non-null, LATTICE_LOG writes to this file instead of stderr.
/// Set via set_log_file(). Caller owns the FILE* lifetime.
inline std::atomic<FILE*> g_log_file{nullptr};
// Intentionally leaked: prevents use-after-destroy during process teardown.
inline std::mutex& g_log_mutex() {
    static auto* m = new std::mutex();
    return *m;
}
inline void set_log_file(FILE* f) {
    std::lock_guard<std::mutex> lock(g_log_mutex());
    g_log_file.store(f, std::memory_order_release);
}
/// Thread-safe log write. Holds mutex to prevent races with set_log_file.
template<typename... Args>
inline void log_write(const char* fmt, Args... args) {
    std::lock_guard<std::mutex> lock(g_log_mutex());
    auto* f = g_log_file.load(std::memory_order_acquire);
    if (f) {
        std::fprintf(f, fmt, args...);
        std::fflush(f);
    } else {
        std::fprintf(stderr, fmt, args...);
    }
}
}

#define LOG_ERROR(tag, fmt, ...) LATTICE_LOG(lattice::log_level::error, tag, fmt, ##__VA_ARGS__)
#define LOG_WARN(tag, fmt, ...)  LATTICE_LOG(lattice::log_level::warn, tag, fmt, ##__VA_ARGS__)
#define LOG_INFO(tag, fmt, ...)  LATTICE_LOG(lattice::log_level::info, tag, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(tag, fmt, ...) LATTICE_LOG(lattice::log_level::debug, tag, fmt, ##__VA_ARGS__)

#endif // __cplusplus
