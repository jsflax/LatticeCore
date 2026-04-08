#include <stdio.h>
#include <dynamic_object.hpp>
#include <format>
#include <list.hpp>
#include <lattice.hpp>

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

    return std::vformat(R"(
    table_name: {}
    is managed: {}
    unmanaged values: {}
    unmanaged list values: {}
    managed values: {}
    )", std::make_format_args(table_name, is_managed, val_s, list_s, managed_val_s));
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

void dynamic_object::set_object(const std::string &name, dynamic_object_ref& value) {
    if (lattice) {
        // Use the pointer specialization which handles nil fields safely
        managed<swift_dynamic_object *> field = managed_.get_managed_field<swift_dynamic_object *>(name);
        if (value.get()->lattice) {
            // Child is already managed — assign the managed object pointer
            field = &value.get()->managed_;
        } else {
            // Child is unmanaged — assign unmanaged value (will be auto-managed)
            field = value.get()->unmanaged_;
            value.get()->manage(field.value());
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
        const auto& schema = lattice->get()->get_properties_for_table(m->table_name());
        for (auto& [name, column_type] : *schema) {
            m->properties_[name] = column_type;
            m->property_types_[name] = column_type.type;
            m->property_names_.push_back(name);
        }
        return std::move(m);
    } else {
        if (unmanaged_.link_values.count(name)) {
            return std::move(*unmanaged_.link_values.at(name));
        }
    }
}

// ============================================================================
// dynamic_object_ref — union delegates
// ============================================================================

union_value dynamic_object_ref::get_union(const std::string& name) const {
    return impl_->get_union(name);
}

void dynamic_object_ref::set_union(const std::string& name, const union_value& value) {
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
                    auto obj = lattice->get()->object_by_global_id(link_gid, col_link_target[col]);
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

}

// Implement retain/release for Swift shared reference
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
