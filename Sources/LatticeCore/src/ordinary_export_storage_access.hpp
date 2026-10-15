#pragma once
#include "ordinary_export.hpp"

namespace lattice::detail {
// Source-private spelling bridge for a synchronous retained/owned view. The
// exact existing pointer creates no owner, turn, source grant or client escape.
class ordinary_export_storage_access final {
public:
    static uint64_t history_capacity_epoch(database& writer) {
        return ordinary_export_access::history_capacity_connection_epoch(writer);
    }
    static std::shared_ptr<void>& history_capacity_cell(database& writer) {
        return ordinary_export_access::history_capacity_connection_witness(writer);
    }
    static sqlite3* borrow(database& writer) noexcept {
        return ordinary_export_access::borrowed_storage_handle(writer);
    }
};
} // namespace lattice::detail
