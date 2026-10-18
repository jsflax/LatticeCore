#pragma once
#include "observer_delivery_domain.hpp"
#include <lattice/detail/observer_delivery_values.hpp>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct sqlite3_value;

namespace lattice::detail {
// Private SQL meter building block. Admission/connection/Native original and
// delivery-tail wiring are separate required parts of this staged vertical.
// No mutable DTO, path spelling, callback or caller-supplied SQL grants credit.
using observer_delivery_meter_budget=observer_delivery_values::write_limits;
struct observer_delivery_meter_column {
    std::string name;
};
struct observer_delivery_meter_table {
    std::string name;
    std::vector<observer_delivery_meter_column> columns;
};
struct observer_delivery_meter_program {
    std::string name, table, sql;
};

class observer_delivery_meter {
public:
    static constexpr uint64_t integer_ceiling = UINT64_C(0x7fffffffffffffff);
    static constexpr size_t table_ceiling = 256, column_ceiling = 128;
    static constexpr size_t catalog_name_bytes = 65536, program_byte_ceiling = 1048576;
    // Includes both the declared structured-delivery liability and the actual
    // declaration's staging/prepare/bind workspace. Overflow never becomes a
    // double/REAL or a smaller admission. Fanout/capture weights must already
    // have been checked by the bounded registration/envelope producer.
    static observer_delivery_domain::units admission_charge(observer_delivery_meter_budget);
    // Conservative UTF8 JSON string representation without building JSON.
    // BLOB length/hex cost is distinct; integers/reals have fixed safe bounds.
    static uint64_t json_string_bytes(std::string_view);
    // Strict UTF8 profile; no conversion/allocation is used to validate input.
    // NUL is valid TEXT data and remains byte-counted by json_string_bytes.
    static bool valid_utf8(std::string_view)noexcept;
    static uint64_t row_bytes(std::string_view table,
        std::span<const std::string_view> column_names,
        std::span<sqlite3_value* const> values);
    // Canonical complete MAIN BEFORE programs. They meter before an AFTER
    // audit producer can allocate JSON. All identifier bytes are quoted;
    // duplicates/virtual/unadmitted catalog entries must fail before this API.
    static std::vector<observer_delivery_meter_program> programs(
        const std::vector<observer_delivery_meter_table>&);
    static std::string_view ledger_name() noexcept;
    static std::string_view ledger_ddl() noexcept;
};
}
