#pragma once

#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <filesystem>
#include <random>
#include <cstring>
#include <thread>
#include <atomic>
#include <condition_variable>

// ============================================================================
// RAII temp database — creates a unique temp file, cleans up on destruction
// ============================================================================

struct TempDB {
    std::filesystem::path path;

    TempDB(std::string name = "test")
        : path(std::filesystem::temp_directory_path()
               / (name + "_" + random_suffix() + ".sqlite")) {}

    ~TempDB() {
        std::filesystem::remove(path);
        std::filesystem::remove(path.string() + "-wal");
        std::filesystem::remove(path.string() + "-shm");
    }

    // Non-copyable, movable
    TempDB(const TempDB&) = delete;
    TempDB& operator=(const TempDB&) = delete;
    TempDB(TempDB&&) = default;
    TempDB& operator=(TempDB&&) = default;

    std::string str() const { return path.string(); }
    operator std::string() const { return str(); }

private:
    static std::string random_suffix() {
        static std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<uint32_t> dist;
        return std::to_string(dist(rng));
    }
};

// ============================================================================
// Global test environment — sets up file logging for all tests
// ============================================================================

class LatticeTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        log_file_ = fopen("/tmp/lattice_debug.log", "w");
        if (log_file_) {
            lattice::set_log_file(log_file_);
            lattice::set_log_level(lattice::log_level::debug);
        }
    }
    void TearDown() override {
        lattice::set_log_file(nullptr);
        lattice::set_log_level(lattice::log_level::off);
        if (log_file_) { fclose(log_file_); log_file_ = nullptr; }
    }
private:
    FILE* log_file_ = nullptr;
};

static auto* _lattice_env [[maybe_unused]] =
    ::testing::AddGlobalTestEnvironment(new LatticeTestEnv());

// ============================================================================
// Helpers
// ============================================================================

/// Pack a vector of floats into a byte vector (for vec0 BLOBs)
inline std::vector<uint8_t> pack_floats(const std::vector<float>& f) {
    std::vector<uint8_t> b(f.size() * sizeof(float));
    std::memcpy(b.data(), f.data(), b.size());
    return b;
}

/// Unpack bytes back to floats
inline std::vector<float> unpack_floats(const std::vector<uint8_t>& b) {
    std::vector<float> f(b.size() / sizeof(float));
    std::memcpy(f.data(), b.data(), b.size());
    return f;
}

/// Generate a simple fake UUID
inline std::string fake_uuid(int counter) {
    return "test-uuid-" + std::to_string(counter);
}

// ============================================================================
// Model Definitions
// ============================================================================

struct TestPerson {
    std::string name;
    int age;
    std::optional<std::string> email;
};
LATTICE_SCHEMA(TestPerson, name, age, email);

struct TestDog {
    std::string name;
    double weight;
    bool is_good_boy;
};
LATTICE_SCHEMA(TestDog, name, weight, is_good_boy);

struct TestTrip {
    std::string name;
    int days;
    std::optional<std::string> notes;
};
LATTICE_SCHEMA(TestTrip, name, days, notes);

struct TestAllTypes {
    int int_val;
    int64_t int64_val;
    double double_val;
    bool bool_val;
    std::string string_val;
    std::optional<int> optional_int;
    std::optional<std::string> optional_string;
};
LATTICE_SCHEMA(TestAllTypes, int_val, int64_val, double_val, bool_val, string_val, optional_int, optional_string);

struct TestPet {
    std::string name;
    double weight;
};
LATTICE_SCHEMA(TestPet, name, weight);

struct TestOwner {
    std::string name;
    TestPet* pet;
};
LATTICE_SCHEMA(TestOwner, name, pet);

struct TestPlace {
    std::string name;
    lattice::geo_bounds location;
};
LATTICE_SCHEMA(TestPlace, name, location);

struct TestLandmark {
    std::string name;
    std::optional<lattice::geo_bounds> bounds;
};
LATTICE_SCHEMA(TestLandmark, name, bounds);

struct TestRegion {
    std::string name;
    std::vector<lattice::geo_bounds> zones;
};
LATTICE_SCHEMA(TestRegion, name, zones);
