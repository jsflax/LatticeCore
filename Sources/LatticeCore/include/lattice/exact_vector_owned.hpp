#pragma once

#include "db.hpp"
#include <memory>
#include <string>
#include <vector>

namespace lattice {
struct exact_vector_owned_test_access;
namespace detail {

class exact_vector_busy : public db_error {
public:
    explicit exact_vector_busy(const char* message) : db_error(message) {}
};
class exact_vector_schema_error : public db_error {
public:
    explicit exact_vector_schema_error(const char* message) : db_error(message) {}
};

struct exact_vector_owned_arm {
    std::string schema; // raw SQLite schema name
    int64_t attachment_token = 0;
    std::shared_ptr<const void> model_metadata; // immutable typed attachment schema
    std::string filename; // canonical attachment filename; empty for memory
};

// Internal, same-thread/LIFO ownership. No topology lock survives construction.
// The shared managed frame keeps writer publication refused through hydration.
// Metadata and row SELECTs are not one cross-connection snapshot: the model
// schema must remain stable. The final row SELECT has SQLite statement semantics.
class exact_vector_read_lease {
public:
    database& connection() const noexcept { return *scope_.writer_owner_; }
    const std::shared_ptr<database>& writer() const noexcept { return scope_.writer_owner_; }
    const std::vector<exact_vector_owned_arm>& arms() const noexcept { return arms_; }
    ~exact_vector_read_lease() noexcept = default; // scope's no-drain exit cannot throw
    exact_vector_read_lease(const exact_vector_read_lease&) = delete;
    exact_vector_read_lease& operator=(const exact_vector_read_lease&) = delete;
private:
    friend class lattice::lattice_db;
    friend struct lattice::exact_vector_owned_test_access;
    exact_vector_read_lease(lattice_db&, std::shared_ptr<database>, uint64_t revision);
    managed_route_scope scope_; // destroyed last; releases writer off topology locks
    lattice_db* owner_;
    std::vector<exact_vector_owned_arm> arms_;
};
} // namespace detail
} // namespace lattice
