#pragma once
#ifdef __cplusplus
#include <bridging.hpp>
#include <error.hpp>
#include <lattice/projection.hpp>

namespace lattice {
class swift_lattice_ref;

/// Bounded value request. Builder failure remains sticky until startProjection;
/// no oversized binding is cloned into this bridge or into the Core operation.
class projection_request {
public:
    projection_request() = default;
    void set_table(const std::string& value) SWIFT_NAME(setTable(_:));
    void add_column(const std::string& value) SWIFT_NAME(addColumn(_:));
    void set_where(const std::string& value) SWIFT_NAME(setWhere(_:));
    void set_order_by(const std::string& value) SWIFT_NAME(setOrderBy(_:));
    void add_order_column(const std::string& value) SWIFT_NAME(addOrderColumn(_:));
    void set_group_by(const std::string& value) SWIFT_NAME(setGroupBy(_:));
    void set_distinct_by(const std::string& value) SWIFT_NAME(setDistinctBy(_:));
    void set_has_bounds(bool value) SWIFT_NAME(setHasBounds(_:)) {
        query_.has_bounds = value;
        if (!value) query_.bounds.reset();
    }
    void set_bounds(const std::string& column, double min_lat, double max_lat,
                    double min_lon, double max_lon)
        SWIFT_NAME(setBounds(column:minLat:maxLat:minLon:maxLon:));
    void add_parameter(const column_value_t& value) SWIFT_NAME(addParameter(_:));
    void set_limit(int64_t value) SWIFT_NAME(setLimit(_:)) { query_.limit = value; }
    void set_offset(int64_t value) SWIFT_NAME(setOffset(_:)) { query_.offset = value; }
    void set_max_rows(int64_t value) SWIFT_NAME(setMaxRows(_:)) { query_.max_rows = value; }
    void set_max_copied_bytes(int64_t value) SWIFT_NAME(setMaxCopiedBytes(_:)) { query_.max_copied_bytes = value; }
    void set_timeout_milliseconds(int64_t value) SWIFT_NAME(setTimeoutMilliseconds(_:)) { query_.timeout_ms = value; }
private:
    friend class swift_lattice_ref;
    void set_text(std::string& target, const std::string& value);
    projection_query query_;
    size_t binding_bytes_ = 0;
    bool failed_ = false;
};

class projection_batch {
public:
    projection_batch() = default;
    explicit projection_batch(projection_read_batch value) : value_(std::move(value)) {}
    int32_t status_code() const SWIFT_NAME(statusCode()) { return failure_ ? 9 : value_.status_code(); }
    std::string error_message() const SWIFT_NAME(errorMessage());
    int64_t row_count() const SWIFT_NAME(rowCount()) { return value_.row_count(); }
    int64_t column_count() const SWIFT_NAME(columnCount()) { return value_.column_count(); }
    column_value_t value(int64_t row, int64_t column) const SWIFT_NAME(value(row:column:));
    int64_t cumulative_rows() const SWIFT_NAME(cumulativeRows()) { return value_.cumulative_rows(); }
    int64_t cumulative_copied_bytes() const SWIFT_NAME(cumulativeCopiedBytes()) { return value_.cumulative_copied_bytes(); }
    bool is_complete() const SWIFT_NAME(isComplete()) { return !failure_ && value_.is_complete(); }
private:
    friend class projection_operation;
    projection_read_batch value_;
    bool failure_ = false;
};

/// Copyable shared-PImpl handle on both FRT and legacy/value bridge platforms.
class projection_operation {
public:
    projection_operation() = default;
    explicit projection_operation(projection_read_operation value) : value_(std::move(value)) {}
    uint64_t operation_id() const SWIFT_NAME(operationId()) { return value_.operation_id(); }
    projection_batch next_batch(int64_t max_rows) const SWIFT_NAME(nextBatch(maxRows:));
    void cancel() const { value_.cancel(); }
    void close() const { value_.close(); }
    bool is_terminal() const SWIFT_NAME(isTerminal()) { return value_.is_terminal(); }
    bool has_resources() const SWIFT_NAME(hasResources()) { return value_.has_resources(); }
    bool when_released(void* context, void (*callback)(void*)) const noexcept
        SWIFT_NAME(whenReleased(context:callback:)) { return value_.when_released(context, callback); }
private:
    projection_read_operation value_;
};
} // namespace lattice
#endif
