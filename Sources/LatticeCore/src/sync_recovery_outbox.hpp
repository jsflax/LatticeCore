#pragma once
#include <cstddef>
#include <compare>
#include <cstdint>
#include <optional>
#include <map>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace lattice {
class lattice_db;
namespace detail {

// Private preservation input, not a replay policy or proof of server receipt.
// Every limit is supplied explicitly. Bytes are logical scalar bytes, not RSS;
// transient SQLite storage, container overhead and bounded key copies are extra.
struct recovery_outbox_limits {
    uint64_t audit_records, current_rows, tables, columns_per_table, total_columns;
    uint64_t fields, field_bytes, logical_bytes;
};
enum class recovery_outbox_error_code {
    transaction_required, invalid_argument, budget_exceeded,
    corrupt_state, unsupported_schema, sql_error
};
class recovery_outbox_error : public std::runtime_error {
public:
    recovery_outbox_error_code code;
    int sqlite_code;
    recovery_outbox_error(recovery_outbox_error_code c, const char* message, int rc = 0)
        : std::runtime_error(message), code(c), sqlite_code(rc) {}
};
using recovery_scalar = std::variant<std::nullptr_t, int64_t, double,
                                     std::string, std::vector<uint8_t>>;
struct recovery_outbox_audit {
    int64_t id, row_id;
    std::string global_id, table_name, operation, global_row_id;
    std::string changed_fields_json, changed_names_json;
    recovery_scalar timestamp;
    bool from_remote, globally_synchronized, synthesized;
    // Present only for the recovering channel. Explicit pending overrides the
    // global flag; absence plus global false is also pending. Other channels do
    // not remove local obligations. No upload floor or outbound filter is used.
    std::optional<int64_t> channel_synchronized;
};
struct recovery_outbox_column {
    std::string name, declared_type;
    std::optional<std::string> default_sql;
    int64_t ordinal, primary_key_position;
    bool not_null;
};
enum class recovery_outbox_table_kind { model, link, polymorphic_link };
struct recovery_outbox_table {
    std::string name, create_sql;
    std::vector<recovery_outbox_column> columns;
    recovery_outbox_table_kind kind;
    // The original exact metadata, including an explicitly empty flags value.
    std::optional<std::string> trigger_flags, internal_parent;
};
struct recovery_outbox_current_row {
    size_t table_index;
    std::string lookup_global_id;
    bool present = false;
    std::optional<int64_t> local_row_id; // physical model id / link rowid
    std::vector<recovery_scalar> values; // actual column order, all columns
};
struct recovery_outbox_capture {
    std::string sync_id;
    std::vector<recovery_outbox_audit> audit;
    std::vector<recovery_outbox_table> tables;
    std::vector<recovery_outbox_current_row> current_rows;
    uint64_t charged_fields = 0, charged_logical_bytes = 0;
};

// Caller MUST own the same explicit main write transaction and actual owner
// through capture, interpretation and durable persistence by the future
// installer. Admission/maintenance and owner retention are caller obligations.
// A returned C++ value confers no transaction lease: after COMMIT/ROLLBACK it is
// invalid as installation evidence, even if another transaction is then begun.
// No SQL writes, ACK, floor, checkpoint, schema repair or historical payload
// rewriting occurs here. Any refusal yields no partial capture. A captured
// pending identity may already be committed remotely (lost ACK); do not mint a
// replacement ID or infer a winner from this local bookkeeping.
recovery_outbox_capture capture_pending_outbox(lattice_db& owner,
    const std::string& sync_id, const recovery_outbox_limits& limits);

struct recovery_row_key {
    std::string table, global_id;
    auto operator<=>(const recovery_row_key&) const = default;
};
// Private bounded reuse of the same strict schema/current-value reader. Empty
// key lists still read the requested table's schema. No SQL writes occur.
recovery_outbox_capture capture_recovery_rows(lattice_db& owner,
    const std::map<std::string, std::vector<std::string>>& tables_and_keys,
    const recovery_outbox_limits& limits);
// The predicate is bound into SQL BEFORE copying audit bodies or inspecting
// their model tables. Unrelated pending targets remain outside this capture.
// An orphan obligation without its audit body remains unclassifiable/refused.
// SQLite's finite bind-variable/SQL limits additionally bound this first slice.
recovery_outbox_capture capture_pending_outbox_for_targets(lattice_db& owner,
    const std::string& sync_id, const std::vector<recovery_row_key>& targets,
    const recovery_outbox_limits& limits);

} // namespace detail
} // namespace lattice
