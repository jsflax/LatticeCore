#pragma once

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

inline std::atomic<log_level>& current_log_level() {
    static std::atomic<log_level> level{log_level::off};
    return level;
}

inline void set_log_level(log_level level) {
    current_log_level().store(level, std::memory_order_relaxed);
}

inline log_level get_log_level() {
    return current_log_level().load(std::memory_order_relaxed);
}

}  // namespace lattice

#define LATTICE_LOG(level, tag, fmt, ...) \
    do { \
        if (static_cast<int>(level) <= static_cast<int>(lattice::current_log_level().load(std::memory_order_relaxed))) { \
            std::printf("[%s] " fmt "\n", tag, ##__VA_ARGS__); \
        } \
    } while(0)

#define LOG_ERROR(tag, fmt, ...) LATTICE_LOG(lattice::log_level::error, tag, fmt, ##__VA_ARGS__)
#define LOG_WARN(tag, fmt, ...)  LATTICE_LOG(lattice::log_level::warn, tag, fmt, ##__VA_ARGS__)
#define LOG_INFO(tag, fmt, ...)  LATTICE_LOG(lattice::log_level::info, tag, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(tag, fmt, ...) LATTICE_LOG(lattice::log_level::debug, tag, fmt, ##__VA_ARGS__)
