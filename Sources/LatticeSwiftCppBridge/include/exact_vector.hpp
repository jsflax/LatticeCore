#pragma once
#ifdef __cplusplus

#include <dynamic_object.hpp>
#include <lattice/exact_vector_selection.hpp>
#include <memory>
#include <string>
#include <vector>

namespace lattice {
class swift_lattice;
class swift_lattice_ref;
struct exact_vector_live_state;

// Internal bridge only. No public Swift query/shape contract is added here.
enum class exact_vector_status : int32_t {
    success = 0, invalid_request = 1, invalid_schema = 2,
    resource_busy = 3, database_failure = 4, bridge_failure = 5
};

class exact_vector_request {
public:
    exact_vector_request() = default;
    exact_vector_request(const exact_vector_request&) noexcept;
    exact_vector_request& operator=(const exact_vector_request&) noexcept;
    exact_vector_request(exact_vector_request&&) noexcept = default;
    exact_vector_request& operator=(exact_vector_request&&) noexcept = default;
    void set_table(const std::string&) noexcept SWIFT_NAME(setTable(_:));
    void set_column(const std::string&) noexcept SWIFT_NAME(setColumn(_:));
    void add_component(float) noexcept SWIFT_NAME(addComponent(_:));
    void set_k(int64_t) noexcept SWIFT_NAME(setK(_:));
    void set_metric(int32_t) noexcept SWIFT_NAME(setMetric(_:));
    // Trusted internal expression with root alias m; positional bindings only.
    void set_predicate(const std::string&) noexcept SWIFT_NAME(setPredicate(_:));
    void add_parameter(const column_value_t&) noexcept SWIFT_NAME(addParameter(_:));
private:
    friend class swift_lattice;
    std::string table_, column_;
    std::vector<float> query_;
    detail::exact_vector_predicate predicate_;
    int64_t k_ = -1;
    int32_t metric_ = 0;
    exact_vector_status status_ = exact_vector_status::success;
    std::string error_;
    void fail(exact_vector_status, const char*) noexcept;
    void set_text(std::string&, const std::string&) noexcept;
};

// Copying a result shares already-owned payload; it does not copy managed rows.
// Accessors are same-thread, with sticky failure independent of TLS diagnostics.
// The state owns live objects, not the captured writer connection/read lease.
class exact_vector_live_result {
public:
    exact_vector_live_result() = default; // failed, never successful-empty
    exact_vector_live_result(const exact_vector_live_result&) noexcept = default;
    exact_vector_live_result& operator=(const exact_vector_live_result&) noexcept = default;
    exact_vector_live_result(exact_vector_live_result&&) noexcept = default;
    exact_vector_live_result& operator=(exact_vector_live_result&&) noexcept = default;
    int32_t status_code() const noexcept SWIFT_NAME(statusCode()) {
        return static_cast<int32_t>(status_);
    }
    int64_t row_count() const noexcept SWIFT_NAME(rowCount());
    double distance_at(int64_t) noexcept SWIFT_NAME(distanceAt(_:));
    std::string error_message() const noexcept SWIFT_NAME(errorMessage());
#if LATTICE_HAS_FRT
    dynamic_object_ref* object_at(int64_t) noexcept SWIFT_NAME(objectAt(_:)) SWIFT_RETURNS_UNRETAINED;
#else
    dynamic_object_ref object_at(int64_t) noexcept SWIFT_NAME(objectAt(_:));
#endif
private:
    friend class swift_lattice;
    friend class swift_lattice_ref;
    exact_vector_status status_ = exact_vector_status::bridge_failure;
    std::shared_ptr<const exact_vector_live_state> state_;
    std::shared_ptr<const std::string> error_;
    void fail(exact_vector_status, const char*) noexcept;
    bool valid_index(int64_t) noexcept;
};
} // namespace lattice
#endif
