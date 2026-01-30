#pragma once

#ifdef __cplusplus

#include <cstdint>
#include <string>
#include <optional>
#include <vector>
#include <functional>
#include <memory>
#include <variant>
#include <chrono>
#include <array>
#include <random>
#include <sstream>
#include <iomanip>

namespace lattice {

// Timestamp type (milliseconds since Unix epoch, like Date in Swift)
using timestamp_t = std::chrono::system_clock::time_point;

// Primary key type
using primary_key_t = int64_t;

// UUID type (stored as TEXT, compatible with Swift UUID)
struct uuid_t {
    std::array<uint8_t, 16> bytes{};

    uuid_t() = default;

    explicit uuid_t(const std::array<uint8_t, 16>& b) : bytes(b) {}

    // Convert to lowercase hyphenated string (e.g., "550e8400-e29b-41d4-a716-446655440000")
    std::string to_string() const {
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (size_t i = 0; i < 16; ++i) {
            if (i == 4 || i == 6 || i == 8 || i == 10) ss << '-';
            ss << std::setw(2) << static_cast<int>(bytes[i]);
        }
        return ss.str();
    }

    // Parse from string (accepts with or without hyphens)
    static uuid_t from_string(const std::string& s) {
        uuid_t result;
        std::string hex;
        for (char c : s) {
            if (c != '-') hex += c;
        }
        if (hex.size() != 32) return result;  // Invalid, return empty
        for (size_t i = 0; i < 16; ++i) {
            result.bytes[i] = static_cast<uint8_t>(std::stoi(hex.substr(i * 2, 2), nullptr, 16));
        }
        return result;
    }

    // Generate a random UUID (v4)
    static uuid_t generate() {
        static thread_local std::random_device rd;
        static thread_local std::mt19937_64 gen(rd());
        static thread_local std::uniform_int_distribution<uint64_t> dis;

        uuid_t result;
        uint64_t a = dis(gen);
        uint64_t b = dis(gen);

        // Copy to bytes
        for (int i = 0; i < 8; ++i) {
            result.bytes[i] = static_cast<uint8_t>((a >> (56 - i * 8)) & 0xFF);
            result.bytes[8 + i] = static_cast<uint8_t>((b >> (56 - i * 8)) & 0xFF);
        }

        // Set version (4) and variant (RFC 4122)
        result.bytes[6] = (result.bytes[6] & 0x0F) | 0x40;  // Version 4
        result.bytes[8] = (result.bytes[8] & 0x3F) | 0x80;  // Variant 1

        return result;
    }

    bool operator==(const uuid_t& other) const { return bytes == other.bytes; }
    bool operator!=(const uuid_t& other) const { return bytes != other.bytes; }

    // Check if UUID is nil (all zeros)
    bool is_nil() const {
        for (auto b : bytes) if (b != 0) return false;
        return true;
    }
};

// Global ID for sync (UUID as string)
using global_id_t = std::string;

// Geographic bounding box (stored as 4 REAL columns, indexed via R*Tree)
struct geo_bounds {
    double min_lat = 0.0;
    double max_lat = 0.0;
    double min_lon = 0.0;
    double max_lon = 0.0;

    geo_bounds() = default;

    geo_bounds(double minLat, double maxLat, double minLon, double maxLon)
        : min_lat(minLat), max_lat(maxLat), min_lon(minLon), max_lon(maxLon) {}

    // Point convenience (min == max)
    static geo_bounds point(double lat, double lon) {
        return geo_bounds(lat, lat, lon, lon);
    }

    bool is_point() const {
        return min_lat == max_lat && min_lon == max_lon;
    }

    // Center point
    double center_lat() const { return (min_lat + max_lat) / 2.0; }
    double center_lon() const { return (min_lon + max_lon) / 2.0; }

    // Span
    double lat_span() const { return max_lat - min_lat; }
    double lon_span() const { return max_lon - min_lon; }

    bool operator==(const geo_bounds& other) const {
        return min_lat == other.min_lat && max_lat == other.max_lat &&
               min_lon == other.min_lon && max_lon == other.max_lon;
    }

    bool operator!=(const geo_bounds& other) const {
        return !(*this == other);
    }

    // Check if bounds contains a point
    bool contains(double lat, double lon) const {
        return lat >= min_lat && lat <= max_lat &&
               lon >= min_lon && lon <= max_lon;
    }

    // Check if bounds intersects another bounds
    bool intersects(const geo_bounds& other) const {
        return !(other.max_lat < min_lat || other.min_lat > max_lat ||
                 other.max_lon < min_lon || other.min_lon > max_lon);
    }
};

// Supported column types
using column_value_t = std::variant<
    std::nullptr_t,
    int64_t,
    double,
    std::string,
    std::vector<uint8_t>  // blob
>;

// Column type enumeration
enum class column_type {
    integer,
    real,
    text,
    blob
};

// Column definition for schema
struct column_def {
    std::string name;
    column_type type;
    bool nullable = false;
    bool is_primary_key = false;
    bool is_unique = false;
    std::optional<std::string> foreign_key_table;
    std::optional<std::string> foreign_key_column;
};

// Table schema
struct table_schema {
    std::string name;
    std::vector<column_def> columns;
    bool is_link_table = false;  // If true, skip id/globalId columns
};

// Change tracking for sync
enum class change_type {
    insert,
    update,
    remove
};

struct audit_entry {
    int64_t id;
    std::string table_name;
    change_type operation;
    primary_key_t row_id;
    global_id_t global_row_id;
    std::string changed_fields;  // JSON
    int64_t timestamp;
};

// ============================================================================
// Helper types and functions for property conversion
// ============================================================================

namespace detail {
    // Type traits
    template<typename T> struct is_optional : std::false_type {};
    template<typename T> struct is_optional<std::optional<T>> : std::true_type {};

    template<typename T>
    struct unwrap_optional { using type = T; };
    template<typename T>
    struct unwrap_optional<std::optional<T>> { using type = T; };

    // Convert C++ types to column_value_t
    inline column_value_t to_column_value(int64_t v) { return v; }
    inline column_value_t to_column_value(int v) { return static_cast<int64_t>(v); }
    inline column_value_t to_column_value(bool v) { return static_cast<int64_t>(v ? 1 : 0); }
    inline column_value_t to_column_value(double v) { return v; }
    inline column_value_t to_column_value(float v) { return static_cast<double>(v); }
    inline column_value_t to_column_value(const std::string& v) { return v; }
    inline column_value_t to_column_value(const std::vector<uint8_t>& v) { return v; }
    // Timestamp stored as double (seconds since epoch, like Swift Date)
    inline column_value_t to_column_value(timestamp_t v) {
        auto duration = v.time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        return static_cast<double>(millis) / 1000.0;
    }
    // UUID stored as TEXT (lowercase hyphenated string)
    inline column_value_t to_column_value(const uuid_t& v) {
        return v.to_string();
    }

    // Convert column_value_t back to C++ types
    template<typename T>
    T from_column_value(const column_value_t& v);

    template<> inline int64_t from_column_value<int64_t>(const column_value_t& v) {
        return std::get<int64_t>(v);
    }
    template<> inline int from_column_value<int>(const column_value_t& v) {
        return static_cast<int>(std::get<int64_t>(v));
    }
    template<> inline bool from_column_value<bool>(const column_value_t& v) {
        return std::get<int64_t>(v) != 0;
    }
    template<> inline double from_column_value<double>(const column_value_t& v) {
        return std::get<double>(v);
    }
    template<> inline float from_column_value<float>(const column_value_t& v) {
        return static_cast<float>(std::get<double>(v));
    }
    template<> inline std::string from_column_value<std::string>(const column_value_t& v) {
        return std::get<std::string>(v);
    }
    template<> inline std::vector<uint8_t> from_column_value<std::vector<uint8_t>>(const column_value_t& v) {
        return std::get<std::vector<uint8_t>>(v);
    }
    template<> inline timestamp_t from_column_value<timestamp_t>(const column_value_t& v) {
        double seconds = std::get<double>(v);
        auto millis = static_cast<int64_t>(seconds * 1000.0);
        return timestamp_t(std::chrono::milliseconds(millis));
    }
    template<> inline uuid_t from_column_value<uuid_t>(const column_value_t& v) {
        return uuid_t::from_string(std::get<std::string>(v));
    }

    // Template helper for getting property value (handles optional)
    template<typename T>
    column_value_t get_prop_value(const T& val) {
        return to_column_value(val);
    }

    template<typename T>
    column_value_t get_prop_value(const std::optional<T>& val) {
        if (!val.has_value()) return nullptr;
        return to_column_value(*val);
    }
} // namespace detail

} // namespace lattice

#endif // __cplusplus
