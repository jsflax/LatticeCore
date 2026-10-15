#include <projection.hpp>
#include <stdexcept>

namespace lattice {
void projection_request::set_max_capture_bytes(int64_t value) { query_.max_capture_bytes = value; }
void projection_request::set_text(std::string& target, const std::string& value) {
    sealed([&] {
        if (value.size() > 65536) throw std::invalid_argument("projection SQL metadata exceeds bound");
        target = value;
    });
    failed_ = failed_ || !last_bridge_error().empty();
}
void projection_request::set_table(const std::string& value) { set_text(query_.table, value); }
void projection_request::set_where(const std::string& value) { set_text(query_.where_clause, value); }
void projection_request::set_order_by(const std::string& value) { set_text(query_.order_by, value); }
void projection_request::add_order_column(const std::string& value) {
    sealed([&] {
        if (query_.order_columns.size() >= 64 || value.size() > 65536)
            throw std::invalid_argument("projection order-column metadata exceeds bound");
        query_.order_columns.push_back(value);
    });
    failed_ = failed_ || !last_bridge_error().empty();
}
void projection_request::set_bounds(const std::string& column, double min_lat, double max_lat,
                                    double min_lon, double max_lon) {
    sealed([&] {
        if (column.size() > 65536)
            throw std::invalid_argument("projection bounds metadata exceeds bound");
        query_.bounds = projection_bounds{column, min_lat, max_lat, min_lon, max_lon};
        query_.has_bounds = true;
    });
    failed_ = failed_ || !last_bridge_error().empty();
}
void projection_request::set_group_by(const std::string& value) { set_text(query_.group_by, value); }
void projection_request::set_distinct_by(const std::string& value) { set_text(query_.distinct_by, value); }
void projection_request::add_column(const std::string& value) {
    sealed([&] {
        if (query_.columns.size() >= 64 || value.size() > 65536)
            throw std::invalid_argument("projection selected-column metadata exceeds bound");
        query_.columns.push_back(value);
    });
    failed_ = failed_ || !last_bridge_error().empty();
}
void projection_request::add_parameter(const column_value_t& value) {
    sealed([&] {
        const size_t bytes = std::visit([](const auto& cell) -> size_t {
            using T = std::decay_t<decltype(cell)>;
            if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::vector<uint8_t>>) return cell.size();
            else return 8;
        }, value);
        if (query_.parameters.size() >= 1024 || bytes > 1024 * 1024 - binding_bytes_)
            throw std::invalid_argument("projection binding admission bound exceeded");
        query_.parameters.push_back(value);
        binding_bytes_ += bytes;
    });
    failed_ = failed_ || !last_bridge_error().empty();
}
std::string projection_batch::error_message() const {
    return sealed([&] { return failure_ ? std::string("projection bridge allocation failed") : value_.error_message(); });
}
column_value_t projection_batch::value(int64_t row, int64_t column) const {
    return sealed([&] { return value_.value(row, column); });
}
projection_batch projection_operation::next_batch(int64_t max_rows) const {
    auto result = sealed([&] { return projection_batch(value_.next_batch(max_rows)); });
    if (!last_bridge_error().empty()) {
        value_.close();
        result.failure_ = true;
    }
    return result;
}
} // namespace lattice
