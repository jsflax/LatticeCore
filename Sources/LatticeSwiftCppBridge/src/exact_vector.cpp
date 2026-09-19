#include <lattice.hpp>
#include <exact_vector.hpp>
#include <lattice/exact_vector_rows.hpp>
#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <tuple>

namespace lattice {
struct exact_vector_live_state {
    struct entry { std::shared_ptr<dynamic_object> object; double distance; };
    std::vector<entry> rows;
};

exact_vector_request::exact_vector_request(const exact_vector_request& other) noexcept {
    if (other.status_ != exact_vector_status::success) {
        status_ = other.status_;
        try { error_ = other.error_; } catch (...) {}
        return;
    }
    try {
        table_ = other.table_;
        column_ = other.column_;
        query_ = other.query_;
        predicate_ = other.predicate_;
        k_ = other.k_;
        metric_ = other.metric_;
    } catch (...) {
        fail(exact_vector_status::bridge_failure, "exact request copy allocation failed");
    }
}
exact_vector_request& exact_vector_request::operator=(const exact_vector_request& other) noexcept {
    if (this != &other) {
        exact_vector_request copy(other);
        *this = std::move(copy);
    }
    return *this;
}

void exact_vector_request::fail(exact_vector_status status, const char* message) noexcept {
    if (status_ != exact_vector_status::success) return;
    status_ = status; // diagnostic allocation cannot erase failure
    try { error_ = message; } catch (...) {}
}
void exact_vector_request::set_text(std::string& target, const std::string& value) noexcept {
    if (status_ != exact_vector_status::success) return;
    if (value.find('\0') != std::string::npos) {
        fail(exact_vector_status::invalid_request, "exact request contains NUL");
        return;
    }
    try { target = value; }
    catch (...) { fail(exact_vector_status::bridge_failure, "exact request allocation failed"); }
}
void exact_vector_request::set_table(const std::string& value) noexcept { set_text(table_, value); }
void exact_vector_request::set_column(const std::string& value) noexcept { set_text(column_, value); }
void exact_vector_request::set_predicate(const std::string& value) noexcept { set_text(predicate_.sql, value); }
void exact_vector_request::add_component(float value) noexcept {
    if (status_ != exact_vector_status::success) return;
    if (!std::isfinite(value) || query_.size() >= 8192) {
        fail(exact_vector_status::invalid_request, "invalid exact query component or dimensions");
        return;
    }
    try { query_.push_back(value); }
    catch (...) { fail(exact_vector_status::bridge_failure, "exact query allocation failed"); }
}
void exact_vector_request::set_k(int64_t value) noexcept {
    if (status_ != exact_vector_status::success) return;
    if (value < 0) fail(exact_vector_status::invalid_request, "exact k must be nonnegative");
    else k_ = value;
}
void exact_vector_request::set_metric(int32_t value) noexcept {
    if (status_ != exact_vector_status::success) return;
    if (value < 0 || value > 2) fail(exact_vector_status::invalid_request, "invalid exact metric");
    else metric_ = value;
}
void exact_vector_request::add_parameter(const column_value_t& value) noexcept {
    if (status_ != exact_vector_status::success) return;
    try { predicate_.bindings.push_back(value); }
    catch (...) { fail(exact_vector_status::bridge_failure, "exact binding allocation failed"); }
}

void exact_vector_live_result::fail(exact_vector_status status, const char* message) noexcept {
    // A default result is already failure; this setter also initializes that
    // failure's category. Accessors only call it from the successful state.
    status_ = status;
    state_.reset();
    try { error_ = std::make_shared<const std::string>(message); } catch (...) {}
}
int64_t exact_vector_live_result::row_count() const noexcept {
    return status_ == exact_vector_status::success && state_ ? static_cast<int64_t>(state_->rows.size()) : 0;
}
bool exact_vector_live_result::valid_index(int64_t index) noexcept {
    if (status_ != exact_vector_status::success) return false;
    if (!state_ || index < 0 || static_cast<uint64_t>(index) >= state_->rows.size()) {
        fail(exact_vector_status::invalid_request, "exact result index out of bounds");
        return false;
    }
    return true;
}
double exact_vector_live_result::distance_at(int64_t index) noexcept {
    return valid_index(index) ? state_->rows[static_cast<size_t>(index)].distance : 0;
}
std::string exact_vector_live_result::error_message() const noexcept {
    try { return error_ ? *error_ : std::string{}; } catch (...) { return {}; }
}
#if LATTICE_HAS_FRT
dynamic_object_ref* exact_vector_live_result::object_at(int64_t index) noexcept {
#else
dynamic_object_ref exact_vector_live_result::object_at(int64_t index) noexcept {
#endif
    if (valid_index(index)) {
        try { return dynamic_object_ref::_make(state_->rows[static_cast<size_t>(index)].object); }
        catch (...) { fail(exact_vector_status::bridge_failure, "exact object wrapper allocation failed"); }
    }
#if LATTICE_HAS_FRT
    return nullptr;
#else
    return dynamic_object_ref{};
#endif
}

namespace {
using detail::exact_vector_schema_error;
using detail::exact_vector_identifier;
using detail::exact_vector_rows_access;

std::string text_cell(const database::row_t& row, const char* name) {
    const auto value = row.find(name);
    if (value == row.end() || !std::holds_alternative<std::string>(value->second))
        throw exact_vector_schema_error("invalid exact schema metadata");
    return std::get<std::string>(value->second);
}
int64_t integer_cell(const database::row_t& row, const char* name) {
    const auto value = row.find(name);
    if (value == row.end() || !std::holds_alternative<int64_t>(value->second))
        throw exact_vector_schema_error("invalid exact schema metadata");
    return std::get<int64_t>(value->second);
}
const char* type_name(column_type type) {
    switch (type) {
        case column_type::integer: return "INTEGER";
        case column_type::real: return "REAL";
        case column_type::text: return "TEXT";
        case column_type::blob: return "BLOB";
    }
    throw exact_vector_schema_error("unknown registered exact column type");
}
bool same_properties(const SwiftSchema& first, const SwiftSchema& other) {
    if (first.size() != other.size()) return false;
    for (const auto& [name, a] : first) {
        const auto it = other.find(name);
        if (it == other.end()) return false;
        const auto& b = it->second;
        if (std::tie(a.name, a.type, a.kind, a.target_table, a.link_table, a.nullable,
                     a.is_vector, a.is_geo_bounds, a.column_name, a.is_union) !=
            std::tie(b.name, b.type, b.kind, b.target_table, b.link_table, b.nullable,
                     b.is_vector, b.is_geo_bounds, b.column_name, b.is_union)) return false;
        if (a.union_desc.union_table_name != b.union_desc.union_table_name ||
            a.union_desc.cases.size() != b.union_desc.cases.size()) return false;
        for (size_t i = 0; i < a.union_desc.cases.size(); ++i) {
            const auto& x = a.union_desc.cases[i]; const auto& y = b.union_desc.cases[i];
            if (x.case_name != y.case_name || x.values.size() != y.values.size()) return false;
            for (size_t j = 0; j < x.values.size(); ++j) {
                const auto& p = x.values[j]; const auto& q = y.values[j];
                if (std::tie(p.param_name, p.type, p.is_link, p.link_target) !=
                    std::tie(q.param_name, q.type, q.is_link, q.link_target)) return false;
            }
        }
    }
    return true;
}

// Authenticate physical layout by registered storage shape, not just the
// existence of a similarly named BLOB. Include machinery columns in the
// complete row image; they are not new user properties.
std::vector<std::string> physical_columns(database& db, const std::string& schema,
    const std::string& model, const SwiftSchema& properties) {
    std::map<std::string, std::string> expected{{"id", "INTEGER"}, {"globalId", "TEXT"}};
    std::set<std::string> possible_shadows;
    const auto add = [&](const std::string& name, const std::string& type) {
        (void)exact_vector_identifier(name);
        if (!expected.emplace(name, type).second)
            throw exact_vector_schema_error("duplicate registered exact column");
    };
    for (const auto& [name, property] : properties) {
        if (name != property.name || (!property.column_name.empty() && property.column_name != name))
            throw exact_vector_schema_error("exact read does not support remapped schema properties");
        if (property.kind == property_kind::link) possible_shadows.insert(name + "__link_gid");
        if (property.kind == property_kind::union_type) add(name, "TEXT");
        else if (property.kind == property_kind::primitive) {
            if (property.is_geo_bounds) {
                for (const auto* suffix : {"_minLat", "_maxLat", "_minLon", "_maxLon"}) add(name + suffix, "REAL");
            } else add(name, type_name(property.type));
        }
    }
    // table-valued PRAGMA preserves bound table/schema names, including quotes.
    auto rows = exact_vector_rows_access::collect(db,
        {"SELECT name, type, hidden, pk FROM pragma_table_xinfo(?, ?)", {model, schema}});
    std::set<std::string> seen;
    std::vector<std::string> columns;
    size_t primary_key_columns = 0;
    bool id_is_primary_key = false;
    for (const auto& row : rows) {
        const auto name = text_cell(row, "name");
        const auto type = text_cell(row, "type");
        if (integer_cell(row, "hidden") != 0 || !seen.insert(name).second)
            throw exact_vector_schema_error("exact read requires ordinary physical columns");
        const auto pk = integer_cell(row, "pk");
        if (pk != 0) {
            ++primary_key_columns;
            id_is_primary_key = name == "id" && pk == 1;
        }
        auto declared = expected.find(name);
        if (declared == expected.end()) {
            if (!possible_shadows.count(name) || type != "TEXT")
                throw exact_vector_schema_error("exact read has an unregistered physical column");
        } else if (declared->second != type) {
            throw exact_vector_schema_error("exact read physical column type differs from registration");
        }
        columns.push_back(name);
    }
    for (const auto& [name, _] : expected) if (!seen.count(name))
        throw exact_vector_schema_error("exact read is missing a registered physical column");
    // Live managed fields address WHERE id = ?. A payload join by (id,gid)
    // alone cannot establish that this later live access names the same row.
    if (primary_key_columns != 1 || !id_is_primary_key)
        throw exact_vector_schema_error("exact live rows require id as the sole INTEGER primary key");
    // Stable order across stores created with different unordered-map orders.
    std::sort(columns.begin(), columns.end());
    return columns;
}
} // namespace

exact_vector_live_result swift_lattice::exact_nearest_rows(const exact_vector_request& request,
    const std::shared_ptr<swift_lattice>& parent) noexcept {
    exact_vector_live_result result;
    try {
        if (parent.get() != this) throw db_error("exact read has no matching owning lattice");
        if (request.status_ != exact_vector_status::success) {
            result.fail(request.status_, request.error_.c_str());
            return result;
        }
        (void)exact_vector_identifier(request.table_);
        (void)exact_vector_identifier(request.column_);
        if (request.k_ < 0 || static_cast<uint64_t>(request.k_) > std::numeric_limits<size_t>::max() ||
            request.query_.empty() || request.query_.size() > 8192 ||
            (request.predicate_.sql.empty() && !request.predicate_.bindings.empty()))
            throw std::invalid_argument("invalid exact request");
        const auto metric = static_cast<detail::exact_vector_metric>(request.metric_);
        if (metric == detail::exact_vector_metric::cosine &&
            std::all_of(request.query_.begin(), request.query_.end(), [](float v) { return v == 0; }))
            throw std::invalid_argument("undefined exact cosine query");

        auto lease = acquire_exact_vector_read();
        std::vector<detail::exact_vector_row_arm> arms;
        std::map<std::string, const SwiftSchema*> schemas_by_source;
        std::vector<std::string> columns;
        const SwiftSchema* baseline = nullptr;
        for (const auto& arm : lease->arms()) {
            const attached_schema_map* schemas = &schemas_;
            if (arm.schema != "main") {
                if (!arm.model_metadata)
                    throw exact_vector_schema_error("exact attachment lacks captured Swift schemas");
                schemas = static_cast<const attached_schema_map*>(arm.model_metadata.get());
            }
            auto registered = schemas->find(request.table_);
            auto tables = exact_vector_rows_access::collect(lease->connection(),
                {"SELECT type FROM " + exact_vector_identifier(arm.schema) +
                 ".sqlite_master WHERE name = ? COLLATE BINARY", {request.table_}});
            if (registered == schemas->end()) {
                if (!tables.empty()) throw exact_vector_schema_error("unregistered physical exact model");
                continue; // a known store without this model contributes no arm
            }
            if (tables.size() != 1 || text_cell(tables.front(), "type") != "table")
                throw exact_vector_schema_error("registered exact model is not a physical table");
            const auto& properties = registered->second;
            auto vector = properties.find(request.column_);
            if (vector == properties.end() || vector->second.kind != property_kind::primitive ||
                !vector->second.is_vector || vector->second.type != column_type::blob ||
                vector->second.is_geo_bounds || vector->second.is_union)
                throw exact_vector_schema_error("exact property is not a registered vector BLOB");
            if (baseline && !same_properties(*baseline, properties))
                throw exact_vector_schema_error("exact model registration differs across physical stores");
            auto actual_columns = physical_columns(lease->connection(), arm.schema, request.table_, properties);
            if (baseline && columns != actual_columns)
                throw exact_vector_schema_error("exact physical column sets differ across stores");
            if (!baseline) { baseline = &properties; columns = std::move(actual_columns); }
            arms.push_back({arm.schema, arm.attachment_token, request.predicate_});
            schemas_by_source.emplace(arm.schema == "main" ? arm.schema : exact_vector_identifier(arm.schema), &properties);
        }
        if (arms.empty()) throw exact_vector_schema_error("exact model is not registered in any participating store");
        auto rows = detail::select_exact_vector_rows(lease->connection(), request.table_, request.column_,
            columns, arms, request.query_, static_cast<size_t>(request.k_), metric);
        auto state = std::make_shared<exact_vector_live_state>();
        state->rows.reserve(rows.size());
        for (auto& row : rows) {
            auto object = hydrate_exact_row<swift_dynamic_object>(row.row, request.table_, *lease);
            const auto schema = schemas_by_source.find(text_cell(row.row, "_source"));
            if (schema == schemas_by_source.end()) throw exact_vector_schema_error("unknown exact winning source");
            object.properties_ = *schema->second;
            object.source.properties = *schema->second;
            object.query_row_image_ = std::make_shared<const database::row_t>(std::move(row.row));
            // Construct final owning dynamic object while writer publication
            // is still refused. objectAt later creates only a reference wrapper.
            state->rows.push_back({std::shared_ptr<dynamic_object>(new dynamic_object(object, parent)), row.distance});
        }
        result.state_ = std::move(state);
        result.status_ = exact_vector_status::success;
    } catch (const detail::exact_vector_busy& error) {
        result.fail(exact_vector_status::resource_busy, error.what());
    } catch (const exact_vector_schema_error& error) {
        result.fail(exact_vector_status::invalid_schema, error.what());
    } catch (const std::invalid_argument& error) {
        result.fail(exact_vector_status::invalid_request, error.what());
    } catch (const db_error& error) {
        result.fail(exact_vector_status::database_failure, error.what());
    } catch (const std::exception& error) {
        result.fail(exact_vector_status::bridge_failure, error.what());
    } catch (...) {
        result.fail(exact_vector_status::bridge_failure, "unknown exact bridge failure");
    }
    return result;
}
} // namespace lattice
