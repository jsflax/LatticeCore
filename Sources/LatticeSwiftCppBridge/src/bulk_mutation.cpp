#include <lattice.hpp>
#include <bulk_mutation.hpp>
#include <cmath>
#include <limits>
#include <map>
#include <set>

namespace lattice {
namespace {
std::string quote_identifier(const std::string& value) {
    std::string result = "\"";
    for (char c : value) { result += c; if (c == '"') result += '"'; }
    return result + '"';
}

[[noreturn]] void invalid_batch(const std::string& reason) {
    throw std::runtime_error("selected batch mutation: " + reason);
}

// database opens SQLITE_OPEN_FULLMUTEX. Its connection mutex is recursive,
// including nested prepare/step/finalize and normal execute hook bookkeeping.
// Keep preflight + writes together against users of this same connection.
struct connection_guard {
    sqlite3_mutex* mutex;
    explicit connection_guard(sqlite3* handle) : mutex(sqlite3_db_mutex(handle)) {
        if (!mutex) invalid_batch("writer connection is not serialized");
        sqlite3_mutex_enter(mutex);
    }
    ~connection_guard() { sqlite3_mutex_leave(mutex); }
    connection_guard(const connection_guard&) = delete;
    connection_guard& operator=(const connection_guard&) = delete;
};

std::string placeholders(size_t count) {
    std::string result;
    for (size_t i = 0; i < count; ++i) {
        if (i) result += ',';
        result += '?';
    }
    return result;
}

bool same_database_file(const std::string& actual, const std::string& expected) {
    if (configuration::path_is_memory(expected)) return actual.empty();
    if (actual.empty()) return false;
    std::error_code actual_error, expected_error;
    const auto actual_path = std::filesystem::weakly_canonical(actual, actual_error);
    const auto expected_path = std::filesystem::weakly_canonical(expected, expected_error);
    return !actual_error && !expected_error && actual_path == expected_path;
}
} // namespace

int64_t swift_lattice::apply_selected_mutations(const selected_mutation_batch& batch) {
    if (batch.failed_) invalid_batch("a batch builder operation failed");
    // Match attach/detach lock order. No callbacks are delivered by execute
    // while this explicit transaction remains open; commit belongs to Swift.
    std::lock_guard<std::mutex> topology_guard(attach_mutex_);
    if (is_closed() || db().is_closed()) invalid_batch("lattice is closed");
    connection_guard writer_guard(db().internal_handle());
    if (!owns_write_transaction())
        invalid_batch("requires a Core transaction owned by the calling thread");

    std::set<std::string> operation_columns;
    std::vector<column_value_t> assignment_params;
    std::vector<column_value_t> guard_params;
    std::string assignments, integer_guards;
    for (const auto& operation : batch.operations_) {
        const auto& column = operation.column;
        if (column.empty() || column == "id" || column == "globalId" ||
            column == "_source" || column == "_lattice_attach_token" ||
            !operation_columns.insert(column).second)
            invalid_batch("duplicate or reserved mutation column: " + column);
        const std::string quoted = quote_identifier(column);
        if (!assignments.empty()) assignments += ", ";
        assignments += quoted + " = ";
        if (operation.increment) {
            if (!std::holds_alternative<int64_t>(operation.value))
                invalid_batch("increment requires Int64");
            assignments += quoted + " + ?";
            // Recheck at UPDATE time as well: an earlier chunk's trigger may
            // change another selected row after the complete preflight.
            integer_guards += " AND typeof(" + quoted + ") = 'integer'";
            const auto delta = std::get<int64_t>(operation.value);
            if (delta > 0) {
                integer_guards += " AND " + quoted + " <= ?";
                guard_params.emplace_back(std::numeric_limits<int64_t>::max() - delta);
            } else if (delta < 0) {
                integer_guards += " AND " + quoted + " >= ?";
                guard_params.emplace_back(std::numeric_limits<int64_t>::min() - delta);
            }
        } else {
            if (!std::holds_alternative<double>(operation.value) ||
                !std::isfinite(std::get<double>(operation.value)))
                invalid_batch("set requires a finite REAL Date value");
            assignments += '?';
        }
        assignment_params.push_back(operation.value);
    }
    if (batch.operations_.empty()) invalid_batch("no mutation operations");

    struct selected_row { int64_t id; std::string global_id; };
    struct physical_group {
        std::string schema;
        std::string table;
        std::map<int64_t, std::string> selected;
        std::vector<selected_row> rows;
    };
    std::map<std::string, physical_group> groups;
    std::map<std::string, std::string> uuid_stores;
    std::string selected_table;

    for (const auto& object : batch.objects_) {
        if (!object || object->lattice.get() != this || object->deleted_)
            invalid_batch("unmanaged, deleted, or foreign lattice handle");
        const auto& managed = object->managed_;
        if (managed.db_ != &db() || managed.lattice_ != this ||
            managed.id_ == 0 || managed.global_id_.empty())
            invalid_batch("handle is not bound to this writer");

        std::string schema = "main";
        if (managed.attachment_token_ != 0) {
            bool found = false;
            for (const auto& [alias, token] : attached_route_tokens_) {
                if (token == managed.attachment_token_) { schema = alias; found = true; break; }
            }
            if (!found) invalid_batch("detached or stale attachment handle");
        }
        const std::string route_prefix = schema == "main" ? "main." : quote_identifier(schema) + '.';
        const SwiftSchema* properties = nullptr;
        std::string table;
        const auto* route_schemas = &schemas_;
        if (managed.attachment_token_ != 0) {
            auto source_schemas = attached_route_metadata_.find(managed.attachment_token_);
            if (source_schemas == attached_route_metadata_.end())
                invalid_batch("attachment has no captured Swift model schema");
            route_schemas = static_cast<const attached_schema_map*>(source_schemas->second.get());
        }
        for (const auto& [name, candidate] : *route_schemas) {
            if (managed.table_name_ == route_prefix + name ||
                (schema == "main" && managed.table_name_ == name)) {
                properties = &candidate;
                table = name;
                break;
            }
        }
        if (!properties) invalid_batch("unrecognized physical table route");
        if (!selected_table.empty() && selected_table != table)
            invalid_batch("selected handles have different model schemas");
        selected_table = table;
        for (const auto& operation : batch.operations_) {
            auto it = properties->find(operation.column);
            if (it == properties->end()) invalid_batch("unknown stored column: " + operation.column);
            const auto& property = it->second;
            if (property.kind != property_kind::primitive || property.is_vector ||
                property.is_geo_bounds || property.is_union ||
                (!property.column_name.empty() && property.column_name != operation.column) ||
                property.type != (operation.increment ? column_type::integer : column_type::real))
                invalid_batch("incompatible stored column: " + operation.column);
        }
        auto [uuid_it, uuid_inserted] = uuid_stores.emplace(managed.global_id_, schema);
        if (!uuid_inserted && uuid_it->second != schema)
            invalid_batch("same globalId selected from different physical stores");
        auto& group = groups[schema];
        group.schema = schema;
        group.table = table;
        auto [row_it, inserted] = group.selected.emplace(managed.id_, managed.global_id_);
        if (!inserted && row_it->second != managed.global_id_)
            invalid_batch("conflicting identities for the same physical row");
    }
    if (groups.empty()) return 0;

    const int variable_limit = sqlite3_limit(db().internal_handle(), SQLITE_LIMIT_VARIABLE_NUMBER, -1);
    const size_t mutation_binds = assignment_params.size() + guard_params.size();
    if (variable_limit <= 0 || mutation_binds + 2 > static_cast<size_t>(variable_limit))
        invalid_batch("mutation exceeds SQLite parameter limit");
    const size_t chunk_capacity = std::min<size_t>(512,
        (static_cast<size_t>(variable_limit) - mutation_binds) / 2);

    // Read attachment binding from the same writer, never a read connection.
    std::map<std::string, std::string> database_files;
    for (const auto& row : db().query("PRAGMA database_list")) {
        auto name = row.find("name"), file = row.find("file");
        if (name != row.end() && file != row.end() &&
            std::holds_alternative<std::string>(name->second) &&
            std::holds_alternative<std::string>(file->second))
            database_files.emplace(std::get<std::string>(name->second), std::get<std::string>(file->second));
    }

    // Complete every schema/row/type/overflow check before the first UPDATE.
    for (auto& [_, group] : groups) {
        const std::string qualified = quote_identifier(group.schema) + '.' + quote_identifier(group.table);
        if (group.schema != "main") {
            auto binding = std::find_if(attached_dbs_.begin(), attached_dbs_.end(),
                [&](const auto& entry) { return entry.first == group.schema; });
            auto actual = database_files.find(group.schema);
            if (binding == attached_dbs_.end() || actual == database_files.end() ||
                !same_database_file(actual->second, binding->second))
                invalid_batch("attachment no longer matches its physical database");
        }
        const auto physical_table = db().query("SELECT name FROM " + quote_identifier(group.schema) +
            ".sqlite_master WHERE type = 'table' AND name = ?", {group.table});
        if (physical_table.size() != 1) invalid_batch("physical table is missing");
        std::map<std::string, std::string> actual_types;
        for (const auto& row : db().query("PRAGMA " + quote_identifier(group.schema) +
                                        ".table_info(" + quote_identifier(group.table) + ')')) {
            actual_types.emplace(std::get<std::string>(row.at("name")), std::get<std::string>(row.at("type")));
        }
        for (const auto& operation : batch.operations_) {
            auto type = actual_types.find(operation.column);
            const std::string expected = operation.increment ? "INTEGER" : "REAL";
            if (type == actual_types.end() || type->second != expected)
                invalid_batch("physical column type does not match model schema");
        }
        for (const auto& [id, gid] : group.selected) group.rows.push_back({id, gid});
        std::string projection = "id, globalId";
        for (const auto& operation : batch.operations_)
            if (operation.increment) projection += ", " + quote_identifier(operation.column);
        for (size_t offset = 0; offset < group.rows.size(); offset += chunk_capacity) {
            const size_t count = std::min(chunk_capacity, group.rows.size() - offset);
            std::vector<column_value_t> ids;
            for (size_t i = 0; i < count; ++i) ids.emplace_back(group.rows[offset + i].id);
            const auto rows = db().query("SELECT " + projection + " FROM " + qualified +
                " WHERE id IN (" + placeholders(count) + ')', ids);
            if (rows.size() != count) invalid_batch("selected row is missing");
            for (const auto& row : rows) {
                auto id = row.find("id"), gid = row.find("globalId");
                if (id == row.end() || gid == row.end() ||
                    !std::holds_alternative<int64_t>(id->second) ||
                    !std::holds_alternative<std::string>(gid->second))
                    invalid_batch("invalid stored row identity");
                auto expected = group.selected.find(std::get<int64_t>(id->second));
                if (expected == group.selected.end() || expected->second != std::get<std::string>(gid->second))
                    invalid_batch("selected row identity changed");
                for (const auto& operation : batch.operations_) {
                    if (!operation.increment) continue;
                    auto cell = row.find(operation.column);
                    if (cell == row.end() || !std::holds_alternative<int64_t>(cell->second))
                        invalid_batch("increment requires an existing INTEGER value");
                    const int64_t current = std::get<int64_t>(cell->second);
                    const int64_t delta = std::get<int64_t>(operation.value);
                    if ((delta > 0 && current > std::numeric_limits<int64_t>::max() - delta) ||
                        (delta < 0 && current < std::numeric_limits<int64_t>::min() - delta))
                        invalid_batch("Int64 increment overflow");
                }
            }
        }
    }

    int64_t affected = 0;
    for (const auto& [_, group] : groups) {
        const std::string qualified = quote_identifier(group.schema) + '.' + quote_identifier(group.table);
        for (size_t offset = 0; offset < group.rows.size(); offset += chunk_capacity) {
            if (!owns_write_transaction() || db().is_closed()) invalid_batch("writer transaction ended");
            const size_t count = std::min(chunk_capacity, group.rows.size() - offset);
            std::vector<column_value_t> params;
            std::vector<column_value_t> ids;
            std::string selected_values;
            for (size_t i = 0; i < count; ++i) {
                const auto& row = group.rows[offset + i];
                if (i) selected_values += ',';
                selected_values += "(?,?)";
                params.emplace_back(row.id);
                params.emplace_back(row.global_id);
                ids.emplace_back(row.id);
            }
            params.insert(params.end(), assignment_params.begin(), assignment_params.end());
            params.insert(params.end(), guard_params.begin(), guard_params.end());
            // Preserve a row-id lookup for large batches, with a second guard
            // matching the identity captured by each explicit selected handle.
            db().execute("WITH selected(id, gid) AS (VALUES " + selected_values +
                         ") UPDATE " + qualified + " SET " + assignments +
                         " WHERE id IN (SELECT id FROM selected)" +
                         " AND (id, globalId) IN (SELECT id, gid FROM selected)" +
                         integer_guards, params);
            if (db().changes() != static_cast<int64_t>(count))
                invalid_batch("selected row disappeared or no longer accepts the mutation");
            // SQLite may collect UPDATE rowids before running row triggers.
            // Detect a trigger changing a later row's identity/type within the
            // same statement, too; the checked outer scope must roll it back.
            std::string projection = "id, globalId";
            for (const auto& operation : batch.operations_)
                if (operation.increment) projection += ", " + quote_identifier(operation.column);
            const auto written = db().query("SELECT " + projection + " FROM " + qualified +
                " WHERE id IN (" + placeholders(count) + ')', ids);
            if (written.size() != count) invalid_batch("trigger removed a selected row");
            for (const auto& row : written) {
                auto id = row.find("id"), gid = row.find("globalId");
                if (id == row.end() || gid == row.end() ||
                    !std::holds_alternative<int64_t>(id->second) ||
                    !std::holds_alternative<std::string>(gid->second))
                    invalid_batch("trigger changed a selected identity");
                auto expected = group.selected.find(std::get<int64_t>(id->second));
                if (expected == group.selected.end() || expected->second != std::get<std::string>(gid->second))
                    invalid_batch("trigger changed a selected identity");
                for (const auto& operation : batch.operations_)
                    if (operation.increment && !std::holds_alternative<int64_t>(row.at(operation.column)))
                        invalid_batch("trigger changed an incremented INTEGER value");
            }
            affected += static_cast<int64_t>(count);
        }
    }
    return affected;
}
} // namespace lattice
