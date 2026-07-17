#include <stdio.h>
#include <dynamic_object.hpp>
#include <list.hpp>
#include <geo_bounds.hpp>
#include <lattice.hpp>
#include <nlohmann/json.hpp>
#include <optional>
#include <set>

// std::format / std::vformat parse the format string at runtime, so libc++
// unconditionally instantiates the floating-point formatter, which depends on
// std::to_chars(float). On Apple platforms that symbol is only available at
// iOS 16.3 / macOS 13.3 / tvOS 16.3 / watchOS 9.3. When the deployment target
// is lower we fall back to manual string concatenation.
#if defined(__cpp_lib_format)
#  if defined(__APPLE__)
#    include <Availability.h>
#    if (defined(__IPHONE_OS_VERSION_MIN_REQUIRED) && __IPHONE_OS_VERSION_MIN_REQUIRED >= 160300) || \
        (defined(__MAC_OS_X_VERSION_MIN_REQUIRED)  && __MAC_OS_X_VERSION_MIN_REQUIRED  >= 130300) || \
        (defined(__TV_OS_VERSION_MIN_REQUIRED)     && __TV_OS_VERSION_MIN_REQUIRED     >= 160300) || \
        (defined(__WATCH_OS_VERSION_MIN_REQUIRED)  && __WATCH_OS_VERSION_MIN_REQUIRED  >=  90300)
#      define LATTICE_HAS_STD_FORMAT 1
#    endif
#  else
#    define LATTICE_HAS_STD_FORMAT 1
#  endif
#endif

#if defined(LATTICE_HAS_STD_FORMAT)
#  include <format>
#endif

// helper type for the visitor #4
template<class... Ts>
struct overloaded : Ts... { using Ts::operator()...; };
// explicit deduction guide (not needed as of C++20)
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

namespace lattice {

std::string dynamic_object::debug_description() const {
    auto is_managed = lattice != nullptr;
    std::string table_name;
    if (lattice) {
        table_name = managed_.table_name_;
    } else {
        table_name = unmanaged_.table_name;
    }

    std::stringstream list_ss;
    std::stringstream value_ss;
    std::stringstream managed_value_ss;

    if (is_managed) {
        // Print managed values by iterating through properties
        managed_value_ss << "{ \n";
        managed_value_ss << "\t\tid: " << managed_.id_ << ", \n";
        managed_value_ss << "\t\tglobalId: " << managed_.global_id_ << ", \n";
        for (const auto& [name, prop] : managed_.properties_) {
            if (prop.kind == property_kind::primitive) {
                managed_value_ss << "\t\t" << name << ": ";
                switch (prop.type) {
                    case column_type::integer:
                        managed_value_ss << managed_.get_int(name);
                        break;
                    case column_type::real:
                        managed_value_ss << managed_.get_double(name);
                        break;
                    case column_type::text:
                        managed_value_ss << "\"" << managed_.get_string(name) << "\"";
                        break;
                    case column_type::blob:
                        managed_value_ss << "<blob>";
                        break;
                }
                managed_value_ss << ", \n";
            } else if (prop.kind == property_kind::link) {
                managed_value_ss << "\t\t" << name << ": <link to " << prop.target_table << ">, \n";
            } else if (prop.kind == property_kind::list) {
                managed_value_ss << "\t\t" << name << ": <list to " << prop.target_table << ">, \n";
            }
        }
        managed_value_ss << "\t}";
        value_ss << "{ } (see managed values)";
        list_ss << "(see managed values)";
    } else {
        const auto& list_values = unmanaged_.list_values;
        value_ss << "{ \n";
        for (const auto& [name, value] : unmanaged_.values) {
            value_ss << "\t\t" << name << ": ";
            std::visit(overloaded {
                [&](auto arg) { value_ss << arg; },
                [&](std::nullptr_t) { value_ss << "null"; },
                [&](std::vector<unsigned char> arg) {
                    for (auto& c: arg) { value_ss << c << ","; }
                }
            }, value);
            value_ss << ", \n";
        }
        value_ss << "\t}";
        for (const auto& [name, list] : list_values) {
            list_ss << name;
            list_ss << ": ";
            list_ss << &list;
            list_ss << " count: ";
            list_ss << list->size();
        }
        managed_value_ss << "{ } (not managed)";
    }

    std::string list_s = list_ss.str();
    std::string val_s = value_ss.str();
    std::string managed_val_s = managed_value_ss.str();

#if defined(LATTICE_HAS_STD_FORMAT)
    return std::vformat(R"(
    table_name: {}
    is managed: {}
    unmanaged values: {}
    unmanaged list values: {}
    managed values: {}
    )", std::make_format_args(table_name, is_managed, val_s, list_s, managed_val_s));
#else
    std::string out;
    out += "\n    table_name: ";            out += table_name;
    out += "\n    is managed: ";            out += (is_managed ? "true" : "false");
    out += "\n    unmanaged values: ";      out += val_s;
    out += "\n    unmanaged list values: "; out += list_s;
    out += "\n    managed values: ";        out += managed_val_s;
    out += "\n    ";
    return out;
#endif
}

dynamic_object::dynamic_object(const dynamic_object& o) : lattice(o.lattice) {
    if (lattice) {
        new (&managed_) managed<swift_dynamic_object>(o.managed_);
    } else {
        new (&unmanaged_) swift_dynamic_object(o.unmanaged_);
    }
}

dynamic_object& dynamic_object::operator=(const dynamic_object &o) {
    if (this != &o) {
        // Destroy current
        if (lattice) {
            managed_.~managed();
        } else {
            unmanaged_.~swift_dynamic_object();
        }
        // Copy new
        lattice = o.lattice;
        if (lattice) {
            new (&managed_) managed<swift_dynamic_object>(o.managed_);
        } else {
            new (&unmanaged_) swift_dynamic_object(o.unmanaged_);
        }
    }
    return *this;
}

void dynamic_object::set_object(const std::string &name, const dynamic_object_ref& value) {
    // impl_-> gives non-const access to the pointee even through a const ref
    // (shared_ptr::operator-> is const but yields a non-const T*), so a const
    // ref suffices and avoids the Swift inout-& asymmetry between the FRT (class)
    // and value-type imports.
    if (lattice) {
        // Use the pointer specialization which handles nil fields safely
        managed<swift_dynamic_object *> field = managed_.get_managed_field<swift_dynamic_object *>(name);
        if (value.impl_->lattice) {
            // Child is already managed — assign the managed object pointer
            field = &value.impl_->managed_;
        } else {
            // Child is unmanaged — assign unmanaged value (will be auto-managed)
            field = value.impl_->unmanaged_;
            value.impl_->manage(field.value());
        }
    } else {
        unmanaged_.link_values[name] = value.impl_;
    }
}


dynamic_object dynamic_object::get_object(const std::string &name) const SWIFT_NAME(getObject(named:)) SWIFT_RETURNS_INDEPENDENT_VALUE {
    if (lattice) {
        managed<swift_dynamic_object*> m;
        m.assign(managed_.db_,
                 this->managed_.lattice_,
                 this->managed_.table_name_,
                 name,
                 this->managed_.id_);
        const property_descriptor& property = managed_.properties_.at(name);
        auto base = static_cast<model_base>(managed_);
        m.bind_to_parent(&base, property);
        // Nil to-one link: no related row exists. get_value() is null here, so
        // m->table_name() (and the std::move(m) below) would dereference a null
        // pointer. Return an empty (unmanaged) object instead — callers test
        // is_managed()/hasLattice and treat that as "no linked object".
        if (!m.has_value()) {
            return dynamic_object{};
        }
        // Target table may be absent from the schema (e.g. a partially
        // reconstructed dynamic schema); guard the deref.
        const SwiftSchema* schema = lattice->get_properties_for_table(m->table_name());
        if (schema) {
            for (auto& [col_name, column_type] : *schema) {
                m->properties_[col_name] = column_type;
                m->property_types_[col_name] = column_type.type;
                m->property_names_.push_back(col_name);
            }
        }
        return std::move(m);
    } else {
        if (unmanaged_.link_values.count(name)) {
            return std::move(*unmanaged_.link_values.at(name));
        }
    }
    // Unmanaged with no staged link value: no object.
    return dynamic_object{};
}

// ============================================================================
// dynamic_object_ref — union delegates
// ============================================================================

union_value dynamic_object_ref::get_union(const std::string& name) const {
    return impl_->get_union(name);
}

void dynamic_object_ref::set_union(const std::string& name, const union_value& value) const {
    impl_->set_union(name, value);
}

// ============================================================================
// union_value
// ============================================================================

std::vector<std::string> union_value::all_keys() const {
    std::vector<std::string> keys;
    for (const auto& [k, _] : string_fields_) keys.push_back(k);
    for (const auto& [k, _] : int_fields_) keys.push_back(k);
    for (const auto& [k, _] : double_fields_) keys.push_back(k);
    for (const auto& [k, _] : blob_fields_) keys.push_back(k);
    return keys;
}

column_value_t union_value::field_as_column_value(const std::string& key) const {
    if (has_string(key)) return get_string(key);
    if (has_int(key)) return get_int(key);
    if (has_double(key)) return get_double(key);
    if (has_blob(key)) return get_blob(key);
    return nullptr;
}

// ============================================================================
// dynamic_object — union accessors
// ============================================================================

union_value dynamic_object::get_union(const std::string& name) const {
    if (lattice) {
        // Managed: read globalId from parent column, query union table
        std::string union_gid = managed_.get_string(name);
        if (union_gid.empty()) {
            return union_value{};
        }

        // Look up the union table name from the property descriptor
        auto prop_it = managed_.properties_.find(name);
        if (prop_it == managed_.properties_.end() || !prop_it->second.is_union) {
            return union_value{};
        }
        const auto& union_table = prop_it->second.union_desc.union_table_name;

        // Query the union row by globalId
        auto rows = managed_.db_->query(
            "SELECT * FROM " + union_table + " WHERE globalId = ?",
            {union_gid});
        if (rows.empty()) {
            return union_value{};
        }

        const auto& row = rows[0];
        union_value result;

        // Extract "case" discriminator
        auto case_it = row.find("case");
        if (case_it != row.end() && std::holds_alternative<std::string>(case_it->second)) {
            result.case_name = std::get<std::string>(case_it->second);
        }

        // Build col_name → field_key mapping from the active case in the descriptor
        const auto& udesc = prop_it->second.union_desc;
        std::unordered_map<std::string, std::string> col_to_key;  // DB col → macro key
        std::unordered_map<std::string, bool> col_is_link;
        std::unordered_map<std::string, std::string> col_link_target;
        for (const auto& c : udesc.cases) {
            if (c.case_name != result.case_name) continue;
            for (size_t vi = 0; vi < c.values.size(); ++vi) {
                const auto& v = c.values[vi];
                std::string key = v.param_name.empty()
                    ? ("_" + std::to_string(vi)) : v.param_name;
                std::string col = (c.values.size() == 1 && v.param_name.empty())
                    ? c.case_name : c.case_name + "__" + key;
                col_to_key[col] = key;
                col_is_link[col] = v.is_link;
                if (v.is_link) col_link_target[col] = v.link_target;
            }
            break;
        }

        // Extract non-null value columns, storing under field keys (not column names)
        for (const auto& [col, val] : row) {
            if (col == "id" || col == "globalId" || col == "case") continue;
            if (std::holds_alternative<std::nullptr_t>(val)) continue;
            auto key_it = col_to_key.find(col);
            if (key_it == col_to_key.end()) continue;
            const auto& key = key_it->second;

            std::visit(overloaded{
                [&](std::nullptr_t) {},
                [&](int64_t v) { result.set_int(key, v); },
                [&](double v) { result.set_double(key, v); },
                [&](const std::string& v) { result.set_string(key, v); },
                [&](const std::vector<uint8_t>& v) { result.set_blob(key, v); },
            }, val);

            // Hydrate link fields into link_refs via object_by_global_id
            if (col_is_link[col]) {
                std::string link_gid = result.get_string(key);
                if (!link_gid.empty()) {
                    auto obj = lattice->object_by_global_id(link_gid, col_link_target[col]);
                    if (obj) {
                        result.link_refs()[key] = std::make_shared<dynamic_object>(std::move(*obj));
                    }
                }
            }
        }

        return result;
    } else {
        // Unmanaged: read from local map
        auto it = unmanaged_.union_values.find(name);
        if (it != unmanaged_.union_values.end() && it->second) {
            return *it->second;
        }
        return union_value{};
    }
}

void dynamic_object::set_union(const std::string& name, const union_value& value) {
    if (lattice) {
        // Managed: update or insert union row, then update parent FK
        auto prop_it = managed_.properties_.find(name);
        if (prop_it == managed_.properties_.end() || !prop_it->second.is_union) return;

        const auto& union_table = prop_it->second.union_desc.union_table_name;
        const auto& union_desc = prop_it->second.union_desc;
        std::string existing_gid = managed_.get_string(name);

        if (value.case_name.empty()) {
            // Clearing the union (for optional fields)
            if (!existing_gid.empty()) {
                managed_.db_->execute(
                    "DELETE FROM " + union_table + " WHERE globalId = ?",
                    {existing_gid});
                managed_.set_string(name, "");
            }
            return;
        }

        // Build col_name ↔ field_key mapping from the descriptor
        struct col_info { std::string case_name; std::string col_name; std::string field_key; };
        std::vector<col_info> all_cols;
        for (const auto& c : union_desc.cases) {
            if (c.values.empty()) continue;
            if (c.values.size() == 1 && c.values[0].param_name.empty()) {
                all_cols.push_back({c.case_name, c.case_name, "_0"});
            } else {
                for (size_t vi = 0; vi < c.values.size(); ++vi) {
                    const auto& v = c.values[vi];
                    std::string key = v.param_name.empty()
                        ? ("_" + std::to_string(vi)) : v.param_name;
                    all_cols.push_back({c.case_name, c.case_name + "__" + key, key});
                }
            }
        }

        if (!existing_gid.empty()) {
            // UPDATE existing union row — only set values for the active case, NULL others
            std::string sql = "UPDATE " + union_table + " SET \"case\" = ?";
            std::vector<column_value_t> params;
            params.push_back(value.case_name);

            for (const auto& ci : all_cols) {
                sql += ", " + ci.col_name + " = ?";
                if (ci.case_name == value.case_name && value.has_field(ci.field_key)) {
                    params.push_back(value.field_as_column_value(ci.field_key));
                } else {
                    params.push_back(nullptr);
                }
            }
            sql += " WHERE globalId = ?";
            params.push_back(existing_gid);
            managed_.db_->execute(sql, params);
        } else {
            // INSERT new union row
            auto gid_rows = managed_.db_->query(
                "SELECT lower(hex(randomblob(4))) || '-' || "
                "lower(hex(randomblob(2))) || '-' || "
                "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
                "substr('89ab', 1 + (abs(random()) % 4), 1) || "
                "substr(lower(hex(randomblob(2))),2) || '-' || "
                "lower(hex(randomblob(6))) AS gid");
            std::string new_gid = std::get<std::string>(gid_rows[0].at("gid"));

            std::string cols = "globalId, \"case\"";
            std::string placeholders = "?, ?";
            std::vector<column_value_t> params;
            params.push_back(new_gid);
            params.push_back(value.case_name);

            for (const auto& ci : all_cols) {
                cols += ", " + ci.col_name;
                placeholders += ", ?";
                if (ci.case_name == value.case_name && value.has_field(ci.field_key)) {
                    params.push_back(value.field_as_column_value(ci.field_key));
                } else {
                    params.push_back(nullptr);
                }
            }

            managed_.db_->execute(
                "INSERT INTO " + union_table + " (" + cols + ") VALUES (" + placeholders + ")",
                params);

            // Update parent FK to point to the new union row's globalId
            managed_.set_string(name, new_gid);
        }
    } else {
        // Unmanaged: store in local map
        unmanaged_.union_values[name] = std::make_shared<union_value>(value);
    }
}

// getLattice mints a Swift-facing handle from the object's shared db wrapper.
// Defined here (not in the header) because swift_lattice_ref is a complete
// type only after lattice.hpp. FRT path: a fresh unretained heap ref that
// Swift retains and frees — the object itself keeps the db alive via its
// shared_ptr member. Value path: a by-value copy (empty when unmanaged).
#if LATTICE_HAS_FRT
swift_lattice_ref* dynamic_object_ref::getLattice() const {
    return swift_lattice_ref::_make(impl_->lattice);
}
#else
swift_lattice_ref dynamic_object_ref::getLattice() const {
    return swift_lattice_ref::_make(impl_->lattice);
}

// Value path: the ref types are complete here, so the by-value wrappers can be
// defined (they are forward-declared at their call site in dynamic_object.hpp).
link_list_ref dynamic_object_ref::get_link_list(const std::string& name) const {
    return impl_->get_link_list(name);
}

geo_bounds_list_ref dynamic_object_ref::get_geo_bounds_list(const std::string& name) const {
    return impl_->get_geo_bounds_list(name);
}
#endif

}

#if LATTICE_HAS_FRT
// Implement retain/release for Swift shared reference. Below the FRT floor
// (iOS 15) dynamic_object_ref is a plain value type whose inner shared_ptr owns
// the impl, so these hooks are not needed.
void retainDynamicObjectRef(lattice::dynamic_object_ref* p) {
    if (p) {
        p->retain();
    }
}

void releaseDynamicObjectRef(lattice::dynamic_object_ref* p) {
    if (p) {
        if (p->release()) {
            delete p;
        }
    }
}
#endif

// MARK: - Row cache

void lattice::dynamic_object::refresh_row_cache() {
    if (!lattice) return;
    auto* db = managed_.db_;
    if (!db) return;
    // table_name_ may be schema-qualified for attached/UNION objects
    // ("alias".Table) — valid SQL, use as-is. One SELECT * replaces the
    // per-column reads the live path would otherwise issue.
    auto rows = db->query(
        "SELECT * FROM " + managed_.table_name_ + " WHERE id = ?",
        {managed_.id_});
    if (rows.empty()) return;
    for (auto& [key, value] : rows[0]) {
        if (key == "id") continue;
        managed_.source.values[key] = value;
    }
}

// ============================================================================
// MARK: - Object-graph → JSON (dynamic_object::to_json)
//
// Backs the C ABI's lattice_object_to_json (and Swift-reachable
// toJson(maxDepth:)). The output contract is PINNED to Swift's
// DynamicObject.jsonObject(maxDepth:includeVectors:false) — lattice repo,
// Sources/Lattice/Dynamic/DynamicObject+JSON.swift — so a sibling SDK's
// Detached is a one-liner over the same shape:
//
// - "globalId" and "id" are emitted when present.
// - primitives by declared column type: integer → JSON number (bools are
//   0/1), real → JSON number, blob → base64 string, text → if the string's
//   first character is '[' or '{' AND it parses as JSON it is INLINED as
//   that value, else the raw string. NULL/absent values omit the key.
// - vector columns are OMITTED (Swift's includeVectors default).
// - geo_bounds values: {"lat","lon"} for a point (min == max), else
//   {"minLat","maxLat","minLon","maxLon"}; absent → key omitted.
// - to-one links (link/virtual_link): unmanaged/absent target → key
//   omitted; depth > 0 and target not already visited → recursed with
//   depth-1; otherwise a {"globalId": gid} stub (key omitted entirely when
//   the target has no globalId to stub with).
// - to-many lists (list/virtual_list): geo-bounds lists collapse to
//   {"geoBoundsCount": n}; depth > 0 → array of recursed elements
//   (already-visited elements collapse to {"globalId": gid} stubs);
//   depth <= 0 → {"count": n}.
// - unions: {"unionRef": "<union row globalId>"} when set.
// - cycle guard: a visited set keyed "<table>:<globalId>", inserted when an
//   object with a globalId is serialized and NEVER removed — matching
//   Swift, a DAG diamond collapses its second occurrence to a stub.
// - key ORDER inside objects is unspecified (nlohmann sorts, Swift
//   dictionaries hash); consumers must parse, not string-compare.
// ============================================================================

namespace {

std::string lattice_json_base64(const std::vector<uint8_t>& data) {
    static constexpr char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((data.size() + 2) / 3) * 4);
    size_t i = 0;
    for (; i + 3 <= data.size(); i += 3) {
        const uint32_t n = (uint32_t(data[i]) << 16) |
                           (uint32_t(data[i + 1]) << 8) | data[i + 2];
        out += tbl[(n >> 18) & 63];
        out += tbl[(n >> 12) & 63];
        out += tbl[(n >> 6) & 63];
        out += tbl[n & 63];
    }
    const size_t rem = data.size() - i;
    if (rem == 1) {
        const uint32_t n = uint32_t(data[i]) << 16;
        out += tbl[(n >> 18) & 63];
        out += tbl[(n >> 12) & 63];
        out += "==";
    } else if (rem == 2) {
        const uint32_t n = (uint32_t(data[i]) << 16) | (uint32_t(data[i + 1]) << 8);
        out += tbl[(n >> 18) & 63];
        out += tbl[(n >> 12) & 63];
        out += tbl[(n >> 6) & 63];
        out += '=';
    }
    return out;
}

// Swift's inlineText: embedded JSON arrays/objects inline; anything else is
// the raw string (no whitespace trimming — first character only, as Swift).
nlohmann::json lattice_json_text_value(const std::string& text) {
    if (!text.empty() && (text.front() == '[' || text.front() == '{')) {
        auto parsed = nlohmann::json::parse(text, nullptr, /*allow_exceptions=*/false);
        if (!parsed.is_discarded()) return parsed;
    }
    return text;
}

}  // namespace

lattice::link_list lattice::dynamic_object::list_backing(const std::string& name) const {
    if (this->lattice) {
        // The same binding get_link_list performs, minus the ref wrapper (the
        // walker needs raw shared_ptr element access on both ref paths).
        managed<std::vector<swift_dynamic_object*>> m;
        const property_descriptor& property = managed_.properties_.at(name);
        model_base* base = const_cast<model_base*>(static_cast<const model_base*>(&managed_));
        m.bind_to_parent(base, property);
        return link_list(m);
    }
    // Unmanaged: a list that was never staged reads as empty (the raw
    // accessor would throw std::out_of_range on the absent key).
    if (unmanaged_.list_values.count(name)) {
        if (auto sp = unmanaged_.get_link_list(name)) return *sp;
    }
    return link_list{};
}

void lattice::dynamic_object::json_walk(const dynamic_object& obj, int64_t depth,
                                        std::set<std::string>& visited,
                                        void* out_json) {
    auto& out = *static_cast<nlohmann::json*>(out_json);
    out = nlohmann::json::object();

    const bool is_managed = obj.lattice != nullptr;
    const std::string table = obj.get_table_name();

    std::optional<std::string> global_id;
    if (obj.has_value("globalId")) global_id = obj.get_string("globalId");
    if (global_id) out["globalId"] = *global_id;
    if (obj.has_value("id")) out["id"] = obj.get_int("id");
    // Cycle guard keyed by identity (only objects with a globalId are
    // guarded — same as Swift; maxDepth is the backstop for the rest).
    if (global_id) visited.insert(table + ":" + *global_id);

    const auto& props = is_managed ? obj.managed_.properties_ : obj.unmanaged_.properties;
    for (const auto& [name, prop] : props) {
        switch (prop.kind) {
        case property_kind::primitive: {
            if (prop.is_vector) break;  // omitted (includeVectors=false)
            if (prop.is_geo_bounds) {
                if (obj.has_geo_bounds(name)) {
                    const geo_bounds gb = obj.get_geo_bounds(name);
                    nlohmann::json g = nlohmann::json::object();
                    if (gb.is_point()) {
                        g["lat"] = gb.min_lat;
                        g["lon"] = gb.min_lon;
                    } else {
                        g["minLat"] = gb.min_lat;
                        g["maxLat"] = gb.max_lat;
                        g["minLon"] = gb.min_lon;
                        g["maxLon"] = gb.max_lon;
                    }
                    out[name] = std::move(g);
                }
                break;
            }
            if (!obj.has_value(name)) break;
            switch (prop.type) {
            case column_type::integer: out[name] = obj.get_int(name); break;
            case column_type::real:    out[name] = obj.get_double(name); break;
            case column_type::blob:    out[name] = lattice_json_base64(obj.get_data(name)); break;
            case column_type::text:    out[name] = lattice_json_text_value(obj.get_string(name)); break;
            }
            break;
        }
        case property_kind::link:
        case property_kind::virtual_link: {
            const dynamic_object child = obj.get_object(name);
            if (!child.lattice) break;  // no linked row → key omitted
            std::optional<std::string> child_gid;
            if (child.has_value("globalId")) child_gid = child.get_string("globalId");
            const bool already_visited =
                child_gid && visited.count(child.get_table_name() + ":" + *child_gid) > 0;
            if (depth > 0 && !already_visited) {
                nlohmann::json sub;
                json_walk(child, depth - 1, visited, &sub);
                out[name] = std::move(sub);
            } else if (child_gid) {
                out[name] = nlohmann::json{{"globalId", *child_gid}};
            }
            break;
        }
        case property_kind::list:
        case property_kind::virtual_list: {
            const link_list list = obj.list_backing(name);
            if (prop.is_geo_bounds) {
                // Geo-bounds list lives in a sidecar table; expose its count
                // through the same list access Swift uses.
                out[name] = nlohmann::json{{"geoBoundsCount", list.size()}};
                break;
            }
            if (depth > 0) {
                nlohmann::json arr = nlohmann::json::array();
                const size_t n = list.size();
                for (size_t i = 0; i < n; i++) {
                    const std::shared_ptr<dynamic_object> el = list[i].object;
                    if (!el) continue;
                    std::optional<std::string> el_gid;
                    if (el->has_value("globalId")) el_gid = el->get_string("globalId");
                    if (el_gid && visited.count(el->get_table_name() + ":" + *el_gid) > 0) {
                        arr.push_back(nlohmann::json{{"globalId", *el_gid}});
                    } else {
                        nlohmann::json sub;
                        json_walk(*el, depth - 1, visited, &sub);
                        arr.push_back(std::move(sub));
                    }
                }
                out[name] = std::move(arr);
            } else {
                out[name] = nlohmann::json{{"count", list.size()}};
            }
            break;
        }
        case property_kind::union_type: {
            // Stored as the union row's globalId in the parent column; the
            // raw reference is surfaced (rich case decoding is a follow-up,
            // matching Swift).
            if (obj.has_value(name)) {
                out[name] = nlohmann::json{{"unionRef", obj.get_string(name)}};
            }
            break;
        }
        }
    }
}

std::string lattice::dynamic_object::to_json(int64_t max_depth) const {
    nlohmann::json out;
    std::set<std::string> visited;
    json_walk(*this, max_depth, visited, &out);
    return out.dump();
}
