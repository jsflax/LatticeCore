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
    // if the new link is managed
    if (value.get()->lattice) {
        if (lattice) {
            managed<swift_dynamic_object> field = managed_.get_managed_field<swift_dynamic_object>(name);
            field = value.get()->managed_;
        } else {
            unmanaged_.link_values[name] = value.impl_;
        }
    } else {
        // if not managd but this object is managed
        if (lattice) {
            managed<swift_dynamic_object *> field = managed_.get_managed_field<swift_dynamic_object *>(name);
            field = value.get()->unmanaged_;
            value.get()->manage(field.value());
        } else {
            unmanaged_.link_values[name] = value.impl_;
        }
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

}

// Implement retain/release for Swift shared reference
void retainDynamicObjectRef(lattice::dynamic_object_ref* p) {
    if (p) {
//        std::cout << "RETAIN " << p << " count: " << p->ref_count_ << " -> " << (p->ref_count_ + 1) << std::endl;
        p->retain();
    }
}

void releaseDynamicObjectRef(lattice::dynamic_object_ref* p) {
    if (p ) {
//        std::cout << "RELEASE " << p << " count: " << p->ref_count_ << " -> " << (p->ref_count_ - 1) << std::endl;
        if (p->release()) {
//            std::cout << "DELETE " << p << std::endl;
            delete p;
        }
    }
}
