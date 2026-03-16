#pragma once

#ifdef __cplusplus

#include <cstdio>
#include <atomic>

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
            if (auto* _lf = lattice::g_log_file.load(std::memory_order_relaxed)) { \
                std::fprintf(_lf, "[%s] " fmt "\n", tag, ##__VA_ARGS__); \
                std::fflush(_lf); \
            } else { \
                std::fprintf(stderr, "[%s] " fmt "\n", tag, ##__VA_ARGS__); \
            } \
        } \
    } while(0)

namespace lattice {
/// When non-null, LATTICE_LOG writes to this file instead of stderr.
/// Set via set_log_file(). Caller owns the FILE* lifetime.
inline std::atomic<FILE*> g_log_file{nullptr};
inline void set_log_file(FILE* f) { g_log_file.store(f, std::memory_order_relaxed); }
}

#define LOG_ERROR(tag, fmt, ...) LATTICE_LOG(lattice::log_level::error, tag, fmt, ##__VA_ARGS__)
#define LOG_WARN(tag, fmt, ...)  LATTICE_LOG(lattice::log_level::warn, tag, fmt, ##__VA_ARGS__)
#define LOG_INFO(tag, fmt, ...)  LATTICE_LOG(lattice::log_level::info, tag, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(tag, fmt, ...) LATTICE_LOG(lattice::log_level::debug, tag, fmt, ##__VA_ARGS__)

#endif // __cplusplus
