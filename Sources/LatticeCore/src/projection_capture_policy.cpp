#include "projection_capture_policy.hpp"
#include <algorithm>
#include <cstring>
#include <string_view>

namespace lattice {
namespace {
using text = std::basic_string<char, std::char_traits<char>, capture_allocator<char>>;
template<class T> using items = std::vector<T, capture_allocator<T>>;
[[noreturn]] void refuse(const char* message) {
    throw projection_capture_failure(projection_status::unsupported, message);
}
[[noreturn]] void changed(const char* message) {
    throw projection_capture_failure(projection_status::schema_changed, message);
}
std::string_view view(const text& value) noexcept { return {value.data(), value.size()}; }
bool equal(std::string_view left, std::string_view right) noexcept {
    if (left.size() != right.size()) return false;
    for (size_t i = 0; i < left.size(); ++i) {
        unsigned char a = left[i], b = right[i];
        if (a >= 'A' && a <= 'Z') a += 'a' - 'A';
        if (b >= 'A' && b <= 'Z') b += 'a' - 'A';
        if (a != b) return false;
    }
    return true;
}
bool starts(std::string_view value, std::string_view prefix) noexcept {
    return value.size() >= prefix.size() && equal(value.substr(0, prefix.size()), prefix);
}
std::string quoted_name(std::string_view name) {
    // Call sites supply bounded admitted schema/object names, never SQL text.
    std::string result = "\"";
    for (char c : name) { if (c == '"') result += '"'; result += c; }
    result += '"';
    return result;
}
struct statement_owner {
    sqlite3_stmt* value = nullptr;
    ~statement_owner() { if (value) sqlite3_finalize(value); }
};
struct token {
    text value;
    enum kind { identifier, literal, punctuation, parameter, number } type;
    token(const std::shared_ptr<projection_capture_budget>& budget, kind type)
        : value(capture_allocator<char>(budget)), type(type) {}
};
bool id_start(unsigned char c) noexcept {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c >= 128;
}
bool id_part(unsigned char c) noexcept { return id_start(c) || (c >= '0' && c <= '9') || c == '$'; }
bool space(unsigned char c) noexcept { return c == ' ' || (c >= '\t' && c <= '\r'); }
} // namespace

struct projection_capture_policy::state {
    static constexpr size_t max_schemas = 32, max_objects = 4096;
    static constexpr size_t max_catalog_bytes = 4 * 1024 * 1024;
    static constexpr size_t max_sql_bytes = 1024 * 1024, max_tokens = 32768, max_depth = 64;
    enum class phase { catalog, rtree_connect, projected, metadata } mode = phase::catalog;
    struct object {
        text schema, type, name, parent, sql;
        bool visited = false, visiting = false, permitted = false, spatial = false, shadow = false, statistics = false;
        explicit object(const std::shared_ptr<projection_capture_budget>& budget)
            : schema(capture_allocator<char>(budget)), type(capture_allocator<char>(budget)),
              name(capture_allocator<char>(budget)), parent(capture_allocator<char>(budget)),
              sql(capture_allocator<char>(budget)) {}
    };
    projection_capture_policy& owner;
    std::shared_ptr<projection_capture_budget> budget;
    items<text> schemas, modules;
    items<object> objects;
    items<sqlite3_stmt*> guards;
    size_t catalog_bytes = 0;
    bool json_each = false, denied = false;
    object* connecting = nullptr;

    state(projection_capture_policy& owner, const std::shared_ptr<projection_capture_budget>& budget)
        : owner(owner), budget(budget), schemas(capture_allocator<text>(budget)),
          modules(capture_allocator<text>(budget)), objects(capture_allocator<object>(budget)),
          guards(capture_allocator<sqlite3_stmt*>(budget)) {}
    sqlite3* db() const noexcept { return owner.capture_.handle(); }
    void check() const { owner.check(); }
    text copy(std::string_view value) {
        return text(value.data(), value.size(), capture_allocator<char>(budget));
    }
    text cell(sqlite3_stmt* stmt, int column, size_t limit, bool nullable = false) {
        check();
        const int type = sqlite3_column_type(stmt, column);
        if (nullable && type == SQLITE_NULL) return text(capture_allocator<char>(budget));
        if (type != SQLITE_TEXT) refuse("capture catalog field is not text");
        const int count = sqlite3_column_bytes(stmt, column);
        if (count < 0 || static_cast<size_t>(count) > limit) refuse("capture catalog field exceeds bound");
        if (static_cast<size_t>(count) > max_catalog_bytes - catalog_bytes)
            refuse("capture catalog byte bound exceeded");
        const auto* bytes = reinterpret_cast<const char*>(sqlite3_column_text(stmt, column));
        if (!bytes) throw db_error("capture catalog text conversion failed");
        if (std::memchr(bytes, 0, static_cast<size_t>(count))) refuse("capture catalog contains NUL");
        catalog_bytes += static_cast<size_t>(count);
        return copy({bytes, static_cast<size_t>(count)});
    }
    void prepare(statement_owner& stmt, const std::string& sql, unsigned flags = SQLITE_PREPARE_NO_VTAB) {
        check();
        database::record_statement();
        const int rc = sqlite3_prepare_v3(db(), sql.c_str(), static_cast<int>(sql.size() + 1), flags, &stmt.value, nullptr);
        check();
        if (rc != SQLITE_OK) {
            if ((rc & 0xff) == SQLITE_BUSY || (rc & 0xff) == SQLITE_LOCKED)
                throw projection_capture_failure(projection_status::admission_rejected, "capture catalog is busy");
            if (denied) refuse("capture catalog or module action denied");
            throw db_error("capture catalog prepare failed: " + std::string(sqlite3_errmsg(db())));
        }
        if (!stmt.value || !sqlite3_stmt_readonly(stmt.value)) refuse("capture metadata must be readonly");
    }
    int step(sqlite3_stmt* stmt) {
        check();
        const int rc = sqlite3_step(stmt);
        check();
        if ((rc & 0xff) == SQLITE_BUSY || (rc & 0xff) == SQLITE_LOCKED)
            throw projection_capture_failure(projection_status::admission_rejected, "capture catalog is busy");
        if (rc != SQLITE_ROW && rc != SQLITE_DONE)
            throw db_error("capture catalog step failed: " + std::string(sqlite3_errmsg(db())));
        return rc;
    }
    items<token> lex(std::string_view sql) {
        check();
        if (sql.size() > max_sql_bytes || sql.find('\0') != std::string_view::npos)
            refuse("capture SQL lexical bound exceeded");
        items<token> result{capture_allocator<token>(budget)};
        size_t cursor = 0;
        while (cursor < sql.size()) {
            check();
            unsigned char c = sql[cursor];
            if (space(c)) { ++cursor; continue; }
            if (c == '-' && cursor + 1 < sql.size() && sql[cursor + 1] == '-') {
                cursor += 2; while (cursor < sql.size() && sql[cursor] != '\n') ++cursor; continue;
            }
            if (c == '/' && cursor + 1 < sql.size() && sql[cursor + 1] == '*') {
                const size_t end = sql.find("*/", cursor + 2);
                if (end == std::string_view::npos) refuse("unterminated capture SQL comment");
                cursor = end + 2; continue;
            }
            if (result.size() >= max_tokens) refuse("capture token count exceeds bound");
            if (c == '"' || c == '\'' || c == '`' || c == '[') {
                const char end = c == '[' ? ']' : static_cast<char>(c);
                token item(budget, c == '\'' ? token::literal : token::identifier);
                ++cursor; bool closed = false;
                while (cursor < sql.size()) {
                    const char value = sql[cursor++];
                    if (value == end) {
                        if (end != ']' && cursor < sql.size() && sql[cursor] == end) {
                            ++cursor; item.value.push_back(end); continue;
                        }
                        closed = true; break;
                    }
                    item.value.push_back(value);
                }
                if (!closed) refuse("unterminated capture SQL quote");
                result.push_back(std::move(item)); continue;
            }
            if (id_start(c)) {
                token item(budget, token::identifier);
                do { item.value.push_back(sql[cursor++]); } while (cursor < sql.size() && id_part(sql[cursor]));
                result.push_back(std::move(item)); continue;
            }
            if (c == '?' || c == ':' || c == '@' || c == '$') {
                token item(budget, token::parameter);
                item.value.push_back(sql[cursor++]);
                while (cursor < sql.size() && (id_part(sql[cursor]) || sql[cursor] == ':')) item.value.push_back(sql[cursor++]);
                result.push_back(std::move(item)); continue;
            }
            if (c >= '0' && c <= '9') {
                token item(budget, token::number);
                do { item.value.push_back(sql[cursor++]); } while (cursor < sql.size() &&
                    (id_part(sql[cursor]) || sql[cursor] == '.'));
                result.push_back(std::move(item)); continue;
            }
            token item(budget, token::punctuation); item.value.push_back(sql[cursor++]);
            result.push_back(std::move(item));
        }
        return result;
    }
    bool is(const token& value, std::string_view expected) const noexcept { return equal(view(value.value), expected); }
    object* find(std::string_view schema, std::string_view name) {
        for (auto& item : objects)
            if ((equal(view(item.type), "table") || equal(view(item.type), "view")) &&
                equal(view(item.schema), schema) && equal(view(item.name), name)) return &item;
        return nullptr;
    }
    void inventory() {
        // Constant PRAGMAs do not invoke a table-valued/eponymous module.
        statement_owner listing;
        prepare(listing, "PRAGMA database_list");
        schemas.push_back(copy("temp"));
        while (step(listing.value) == SQLITE_ROW) {
            auto name = cell(listing.value, 1, 255);
            bool present = false;
            for (const auto& prior : schemas) if (equal(view(prior), view(name))) present = true;
            if (!present) {
                if (schemas.size() >= max_schemas) refuse("capture schema count exceeds bound");
                schemas.push_back(std::move(name));
            }
        }
        // A stock count is required before any aggregate cursor is stepped.
        if (!owner.capture_.builtin_functions_.count("count")) refuse("capture snapshot count builtin is unavailable");
        for (const auto& schema : schemas) {
            statement_owner guard;
            prepare(guard, "SELECT count(*) FROM " + quoted_name(view(schema)) + ".sqlite_schema");
            if (step(guard.value) != SQLITE_ROW || !sqlite3_stmt_busy(guard.value))
                refuse("capture catalog cursor did not retain a snapshot");
            guards.push_back(guard.value); guard.value = nullptr;
        }
        // All schema guards are installed before catalog definitions are copied.
        for (const auto& schema : schemas) {
            statement_owner catalog;
            prepare(catalog, "SELECT type,name,tbl_name,sql FROM " + quoted_name(view(schema)) + ".sqlite_schema");
            while (step(catalog.value) == SQLITE_ROW) {
                if (objects.size() >= max_objects) refuse("capture catalog object count exceeds bound");
                object item(budget);
                item.schema = copy(view(schema));
                item.type = cell(catalog.value, 0, 32);
                item.name = cell(catalog.value, 1, 1024);
                item.parent = cell(catalog.value, 2, 1024);
                item.sql = cell(catalog.value, 3, max_sql_bytes, true);
                objects.push_back(std::move(item));
            }
        }
        statement_owner module_list;
        prepare(module_list, "PRAGMA module_list");
        while (step(module_list.value) == SQLITE_ROW) {
            if (modules.size() >= 256) refuse("capture module inventory exceeds bound");
            modules.push_back(cell(module_list.value, 0, 255));
        }
    }
    // Parse exact CREATE table header, not arbitrary SQL. Quoted names preserve
    // their bytes; keyword matching is SQLite's ASCII folding.
    size_t create_header(const items<token>& tokens, const object& item, bool virtual_table) {
        size_t i = 0;
        auto take = [&](std::string_view word) { if (i >= tokens.size() || !is(tokens[i++], word)) refuse("capture spatial schema is not stock"); };
        take("create");
        if (virtual_table) take("virtual");
        take("table");
        if (i < tokens.size() && is(tokens[i], "if")) { take("if"); take("not"); take("exists"); }
        if (i >= tokens.size()) refuse("capture spatial table name missing");
        if (i + 2 < tokens.size() && is(tokens[i + 1], ".")) {
            if (!is(tokens[i], view(item.schema))) refuse("capture spatial schema mismatch");
            i += 2;
        }
        if (!is(tokens[i++], view(item.name))) refuse("capture spatial table mismatch");
        return i;
    }
    void exact_shadow(object& item, std::string_view first, std::string_view second) {
        if (!equal(view(item.type), "table") || item.sql.empty()) refuse("capture RTree shadow is not ordinary");
        auto tokens = lex(view(item.sql));
        size_t i = create_header(tokens, item, false);
        const std::string_view expected[]{"(", first, "integer", "primary", "key", ",", second, ")"};
        for (auto part : expected) if (i >= tokens.size() || !is(tokens[i++], part)) refuse("capture RTree shadow shape is unsupported");
        if (i < tokens.size() && is(tokens[i], ";")) ++i;
        if (i != tokens.size()) refuse("capture RTree shadow has extra schema clauses");
        for (const auto& other : objects)
            if (equal(view(other.schema), view(item.schema)) && equal(view(other.type), "trigger") &&
                equal(view(other.parent), view(item.name))) refuse("capture RTree shadow has triggers");
        item.permitted = item.visited = item.shadow = true;
    }
    void approve_spatial(const projection_query& query) {
        if (!query.bounds) return;
        const std::string base = "_" + query.table + "_" + query.bounds->column;
        const std::string name = base + "_rtree";
        for (const auto& schema : schemas) {
            if (equal(view(schema), "temp")) continue;
            auto* model = find(view(schema), query.table);
            if (!model || !equal(view(model->type), "table")) continue;
            auto* spatial = find(view(schema), name);
            if (!spatial || spatial->sql.empty()) changed("capture bounds index is missing");
            auto tokens = lex(view(spatial->sql));
            size_t i = create_header(tokens, *spatial, true);
            const std::string_view expected[]{"using", "rtree", "(", "id", ",", "minLat", ",", "maxLat", ",", "minLon", ",", "maxLon", ")"};
            for (auto part : expected) if (i >= tokens.size() || !is(tokens[i++], part)) refuse("capture RTree arguments are not the Core spatial layout");
            if (i < tokens.size() && is(tokens[i], ";")) ++i;
            if (i != tokens.size()) refuse("capture RTree has unsupported arguments");
            for (const auto& suffix : {"_node", "_parent", "_rowid"}) {
                auto* shadow = find(view(schema), name + suffix);
                if (!shadow) refuse("capture RTree shadow is absent");
                exact_shadow(*shadow, std::string_view(suffix) == "_rowid" ? "rowid" : "nodeno",
                    std::string_view(suffix) == "_node" ? "data" : (std::string_view(suffix) == "_parent" ? "parentnode" : "nodeno"));
            }
            if (auto* statistics = find(view(schema), "sqlite_stat1")) {
                auto shape = lex(view(statistics->sql));
                size_t at = create_header(shape, *statistics, false);
                for (auto part : {"(", "tbl", ",", "idx", ",", "stat", ")"})
                    if (at >= shape.size() || !is(shape[at++], part)) refuse("capture RTree statistics shape is unsupported");
                if (at < shape.size() && is(shape[at], ";")) ++at;
                if (at != shape.size()) refuse("capture RTree statistics has extra clauses");
                for (const auto& other : objects)
                    if (equal(view(other.schema), view(schema)) && equal(view(other.type), "trigger") &&
                        equal(view(other.parent), "sqlite_stat1")) refuse("capture RTree statistics has triggers");
                statistics->permitted = statistics->visited = statistics->statistics = true;
            }
            spatial->permitted = spatial->visited = spatial->spatial = true;
            // The optional list sidecar and model are normal dependency roots.
            if (auto* list = find(view(schema), base)) visit(*list, 0);
        }
    }
    void scan(std::string_view sql, size_t depth) {
        auto tokens = lex(sql);
        for (size_t i = 0; i < tokens.size(); ++i) {
            check();
            const auto& current = tokens[i];
            if (current.type != token::identifier && current.type != token::literal) continue;
            const auto name = view(current.value);
            if (starts(name, "pragma_")) refuse("capture dynamic PRAGMA modules are unsupported");
            // Stock SQLite may register builtin json_each lazily: it can be
            // absent from PRAGMA module_list until this query is prepared.
            // Admit only the already-approved table-function syntax, without
            // widening authorizer actions or trusting another module name.
            const bool approved_json = equal(name, "json_each") && i > 0 && i + 1 < tokens.size() &&
                (is(tokens[i - 1], "from") || is(tokens[i - 1], "join") || is(tokens[i - 1], ",")) &&
                is(tokens[i + 1], "(");
            if (equal(name, "json_each")) {
                if (!approved_json) refuse("capture SQL names an unapproved module");
                json_each = true;
            }
            for (const auto& module : modules) {
                if (!equal(name, view(module))) continue;
                if (approved_json) break;
                refuse("capture SQL names an unapproved module");
            }
            // Deliberately conservative: all catalog objects matching a token
            // are visited, including possible legacy single-quoted identifiers.
            // Bound parameters are never scanned. A string literal equal to an
            // unsafe object can be refused; this is an explicit API boundary.
            for (auto& item : objects) {
                check();
                if ((equal(view(item.type), "table") || equal(view(item.type), "view")) &&
                    equal(view(item.name), name)) visit(item, depth + 1);
            }
        }
    }
    void visit(object& item, size_t depth) {
        if (item.visited || item.visiting) return;
        if (depth > max_depth) refuse("capture dependency depth exceeds bound");
        if (item.sql.empty()) refuse("capture relevant object has no schema SQL");
        auto tokens = lex(view(item.sql));
        if (tokens.size() >= 2 && is(tokens[0], "create") && is(tokens[1], "virtual"))
            refuse("capture virtual table is not an approved bounds RTree");
        if (!equal(view(item.type), "table") && !equal(view(item.type), "view")) refuse("capture dependency is not readable");
        item.visiting = true;
        scan(view(item.sql), depth);
        item.visiting = false;
        item.visited = item.permitted = true;
    }
    void roots(const projection_query& query) {
        bool found = false;
        for (auto& item : objects)
            if ((equal(view(item.type), "table") || equal(view(item.type), "view")) && equal(view(item.name), query.table)) {
                visit(item, 0); found = true;
            }
        if (!found) changed("capture model table is absent");
        for (const auto* sql : {&query.where_clause, &query.order_by, &query.group_by, &query.distinct_by}) scan(*sql, 0);
        // Output and sort dependency names are identifiers, not arbitrary SQL.
        for (const auto& name : query.columns) for (auto& item : objects)
            if (equal(view(item.name), name)) visit(item, 0);
    }
    bool allowed(std::string_view schema, std::string_view name) const noexcept {
        for (const auto& item : objects)
            if (item.permitted && equal(view(item.schema), schema) && equal(view(item.name), name)) return true;
        return false;
    }
    bool allowed_without_schema(std::string_view name) const noexcept {
        bool found = false;
        for (const auto& item : objects) {
            if (!(equal(view(item.type), "table") || equal(view(item.type), "view")) ||
                !equal(view(item.name), name)) continue;
            if (!item.permitted) return false;
            found = true;
        }
        return found;
    }
    bool connect_relation(std::string_view schema, std::string_view name) const noexcept {
        if (!connecting || !equal(view(connecting->schema), schema)) return false;
        if (equal(view(connecting->name), name)) return true;
        for (const auto& item : objects) {
            if (!item.shadow || !equal(view(item.schema), schema) || !equal(view(item.name), name) ||
                !starts(name, view(connecting->name))) continue;
            const auto suffix = name.substr(connecting->name.size());
            if (equal(suffix, "_node") || equal(suffix, "_parent") || equal(suffix, "_rowid")) return true;
        }
        return false;
    }
    void connect_spatial() {
        mode = phase::rtree_connect;
        for (auto& item : objects) if (item.spatial) {
            connecting = &item;
            statement_owner probe;
            prepare(probe, "SELECT id FROM " + quoted_name(view(item.schema)) + "." + quoted_name(view(item.name)) + " LIMIT 0", 0);
            // Preparing connects the validated module; stepping only this fixed
            // readonly LIMIT-0 SELECT cannot execute its cached write programs.
            if (step(probe.value) != SQLITE_DONE) refuse("capture RTree initializer returned rows");
        }
        connecting = nullptr;
        mode = phase::projected;
    }
};

projection_capture_policy::projection_capture_policy(database_projection_capture& capture,
    const std::shared_ptr<projection_capture_budget>& budget, const projection_query& query)
    : capture_(capture), budget_(budget) {
    if (!budget_ || capture_.policy_ || capture_.statement_)
        throw projection_capture_failure(projection_status::invalid_request, "capture policy requires an empty owned scope");
    budget_->charge(sizeof(state));
    try { state_ = new state(*this, budget_); }
    catch (...) { budget_->release(sizeof(state)); throw; }
    capture_.policy_ = this;
    try {
        state_->inventory();
        state_->approve_spatial(query);
        state_->roots(query);
        state_->connect_spatial();
    } catch (...) { cleanup(); throw; }
}
void projection_capture_policy::check() const { capture_.check(); }
void projection_capture_policy::cleanup() noexcept {
    if (capture_.statement_) { sqlite3_finalize(capture_.statement_); capture_.statement_ = nullptr; }
    if (state_) {
        for (auto* cursor : state_->guards) sqlite3_finalize(cursor);
        state_->guards.clear();
    }
    if (capture_.policy_ == this) capture_.policy_ = nullptr;
    if (state_) { delete state_; state_ = nullptr; budget_->release(sizeof(state)); }
}
projection_capture_policy::~projection_capture_policy() noexcept { cleanup(); }
bool projection_capture_policy::table_exists(const std::string& schema, const std::string& table) const {
    check();
    auto* item = state_->find(schema, table);
    return item && equal(view(item->type), "table") && view(item->name) == std::string_view(table);
}
bool projection_capture_policy::matches_schemas(const std::vector<std::string>& expected) const {
    check();
    size_t matched = 0;
    for (const auto& schema : state_->schemas) {
        if (equal(view(schema), "temp")) continue;
        size_t count = 0;
        for (const auto& name : expected) if (equal(view(schema), name)) ++count;
        if (count != 1) return false;
        ++matched;
    }
    return matched == expected.size();
}
void projection_capture_policy::validate_columns(const projection_query& query, bool& has_source) {
    check();
    if (state_->denied) refuse("capture policy is terminal after a denied prepare");
    state_->mode = state::phase::metadata;
    try {
        statement_owner metadata;
        state_->prepare(metadata, "PRAGMA table_info(" + quoted_name(query.table) + ")", 0);
        items<text> names{capture_allocator<text>(budget_)};
        has_source = false;
        while (state_->step(metadata.value) == SQLITE_ROW) {
            if (names.size() >= 4096) refuse("capture model column count exceeds bound");
            auto name = state_->cell(metadata.value, 1, 1024);
            if (view(name) == "_source") has_source = true;
            names.push_back(std::move(name));
        }
        const auto contains = [&](const std::string& name) {
            for (const auto& item : names) if (view(item) == name) return true;
            return false;
        };
        for (const auto& name : query.columns) if (!contains(name)) changed("projected stored column is missing");
        for (const auto& name : query.order_columns) if (!contains(name)) changed("projection sort dependency is missing");
        for (const auto* name : {&query.group_by, &query.distinct_by})
            if (!name->empty() && !contains(*name)) changed("projection grouping dependency is missing");
        state_->mode = state::phase::projected;
    } catch (...) { state_->mode = state::phase::projected; throw; }
}
void projection_capture_policy::prepare_read(const std::string& sql) {
    check();
    if (capture_.statement_ || state_->denied) refuse("capture policy is terminal after a denied prepare");
    try {
        state_->scan(sql, 0);
        const char* tail = nullptr;
        database::record_statement();
        const int rc = sqlite3_prepare_v3(capture_.handle(), sql.c_str(), static_cast<int>(sql.size() + 1),
            0, &capture_.statement_, &tail);
        check();
        if (rc != SQLITE_OK) {
            if ((rc & 0xff) == SQLITE_BUSY || (rc & 0xff) == SQLITE_LOCKED)
                throw projection_capture_failure(projection_status::admission_rejected, "capture projected source is busy");
            if (state_->denied) refuse("capture projected callback or relation denied");
            throw db_error("capture projected prepare failed: " + std::string(sqlite3_errmsg(capture_.handle())));
        }
        while (tail && *tail && space(static_cast<unsigned char>(*tail))) ++tail;
        if (!capture_.statement_ || (tail && *tail) || !sqlite3_stmt_readonly(capture_.statement_))
            throw projection_capture_failure(projection_status::invalid_request, "capture requires one readonly SELECT");
    } catch (...) { state_->denied = true; capture_.denied_ = true; throw; }
}
int projection_capture_policy::authorize(int action, const char* first, const char* second,
                                          const char* schema, const char* trigger) noexcept {
    if (!state_ || capture_.control_->stopped()) return SQLITE_DENY;
    auto& policy = *state_;
    const std::string_view name = first ? first : "", physical = schema ? schema : "";
    bool allowed = false;
    if (action == SQLITE_SELECT || action == SQLITE_RECURSIVE) allowed = true;
    else if (action == SQLITE_FUNCTION && second) {
        for (const auto& builtin : capture_.builtin_functions_)
            if (equal(builtin, second)) { allowed = true; break; }
    } else if (policy.mode == state::phase::catalog) {
        if (action == SQLITE_PRAGMA)
            allowed = equal(name, "database_list") || equal(name, "module_list");
        if (action == SQLITE_READ)
            allowed = equal(name, "sqlite_master") || equal(name, "sqlite_schema") ||
                (equal(physical, "temp") && equal(name, "sqlite_temp_master"));
    } else if (policy.mode == state::phase::metadata && action == SQLITE_PRAGMA) {
        allowed = equal(name, "table_info");
    } else if (action == SQLITE_READ) {
        allowed = policy.allowed(physical, name) ||
            (policy.json_each && equal(name, "json_each"));
        // SQLite can additionally check table access with an empty column and
        // no schema for count(*) or an INTEGER PRIMARY KEY-only SELECT. Do not
        // guess its schema: every same-name relation in the pinned catalog must
        // already be admitted, and at least one must exist.
        if (!allowed && policy.mode == state::phase::projected && first && *first && !schema &&
            second && !*second)
            allowed = policy.allowed_without_schema(name);
        if (policy.mode == state::phase::rtree_connect)
            allowed = policy.connect_relation(physical, name) || equal(name, "sqlite_master") || equal(name, "sqlite_schema") ||
                (equal(name, "sqlite_stat1") && policy.connecting &&
                 equal(view(policy.connecting->schema), physical) && policy.allowed(physical, name));
    } else if (policy.mode == state::phase::rtree_connect &&
               (action == SQLITE_INSERT || action == SQLITE_UPDATE || action == SQLITE_DELETE)) {
        // Exact validated shadow tables only. Any trigger context is denied.
        allowed = !trigger && policy.connect_relation(physical, name) && policy.connecting &&
            !equal(name, view(policy.connecting->name));
    }
    if (!allowed) { policy.denied = true; capture_.denied_ = true; }
    return allowed ? SQLITE_OK : SQLITE_DENY;
}
} // namespace lattice
