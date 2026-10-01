#pragma once
// Private, stock-SQLite memory-capture admission. Never a general SQL sandbox.
#include "projection_memory.hpp"

namespace lattice {
/// Owns only charged metadata and cursor pins; capture owns connection policies
/// and the mutex. Must be destroyed before capture and before budget.finish().
/// Catalog cursors establish progressive per-schema snapshots, not a single
/// atomic commit boundary across independent attached databases.
class projection_capture_policy {
public:
    projection_capture_policy(database_projection_capture&,
        const std::shared_ptr<projection_capture_budget>&, const projection_query&);
    ~projection_capture_policy() noexcept;
    projection_capture_policy(const projection_capture_policy&) = delete;
    projection_capture_policy& operator=(const projection_capture_policy&) = delete;
    void validate_columns(const projection_query&, bool& has_source);
    bool table_exists(const std::string& schema, const std::string& table) const;
    bool matches_schemas(const std::vector<std::string>& expected_physical_schemas) const;
    void prepare_read(const std::string& sql);
    int authorize(int, const char*, const char*, const char*, const char*) noexcept;
private:
    struct state;
    database_projection_capture& capture_;
    std::shared_ptr<projection_capture_budget> budget_;
    state* state_ = nullptr;
    void check() const;
    void cleanup() noexcept;
};
} // namespace lattice
