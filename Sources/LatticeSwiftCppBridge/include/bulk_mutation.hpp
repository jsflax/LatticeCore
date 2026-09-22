#pragma once

#ifdef __cplusplus
#include <dynamic_object.hpp>
#include <error.hpp>
#include <vector>

namespace lattice {

/// Retains explicit managed handles until execution. Metadata and operations
/// never become object fields, and execution never changes row-cache modes.
class selected_mutation_batch {
public:
    selected_mutation_batch() = default;

    void add_object(const dynamic_object_ref& object) SWIFT_NAME(addObject(_:)) {
        sealed([&] { objects_.push_back(object.shared()); });
        failed_ = failed_ || !last_bridge_error().empty();
    }
    /// V1 permits finite REAL values only (Swift restricts these to Date).
    void set(const std::string& column, const column_value_t& value)
        SWIFT_NAME(set(column:value:)) {
        sealed([&] { operations_.push_back({column, value, false}); });
        failed_ = failed_ || !last_bridge_error().empty();
    }
    void increment_int64(const std::string& column, int64_t delta)
        SWIFT_NAME(incrementInt64(column:by:)) {
        sealed([&] { operations_.push_back({column, column_value_t(delta), true}); });
        failed_ = failed_ || !last_bridge_error().empty();
    }

private:
    friend class swift_lattice;
    struct operation {
        std::string column;
        column_value_t value;
        bool increment;
    };
    std::vector<std::shared_ptr<dynamic_object>> objects_;
    std::vector<operation> operations_;
    // A later sealed builder call must not erase an earlier builder failure.
    bool failed_ = false;
};

} // namespace lattice
#endif
