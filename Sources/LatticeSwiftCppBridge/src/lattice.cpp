#include <lattice.hpp>
#include <list.hpp>
#include <unmanaged_object.hpp>
#include <dynamic_object.hpp>
#include <geo_bounds.hpp>
#include <util.hpp>

// Log level control
void lattice_set_log_level(lattice::log_level level) {
    lattice::set_log_level(level);
}

lattice::log_level lattice_get_log_level() {
    return lattice::get_log_level();
}

// swift_lattice_ref retain/release for SWIFT_SHARED_REFERENCE
// The ref counting is managed by swift_lattice_ref's atomic counter

void retainSwiftLatticeRef(lattice::swift_lattice_ref* ptr) {
    // std::cerr << "[DEBUG] retainSwiftLatticeRef ptr=" << ptr << std::endl;
    if (!ptr) return;
    ptr->retain();
}

void releaseSwiftLatticeRef(lattice::swift_lattice_ref* ptr) {
    // std::cerr << "[DEBUG] releaseSwiftLatticeRef ptr=" << ptr << std::endl;
    if (!ptr) return;
    if (ptr->release()) {
        // std::cerr << "[DEBUG] releaseSwiftLatticeRef deleting" << std::endl;
        delete ptr;
    }
}

// swift_migration_context_ref retain/release for SWIFT_SHARED_REFERENCE
void retainSwiftMigrationContextRef(lattice::swift_migration_context_ref* ptr) {
    if (!ptr) return;
    ptr->retain();
}

void releaseSwiftMigrationContextRef(lattice::swift_migration_context_ref* ptr) {
    if (!ptr) return;
    if (ptr->release()) {
        delete ptr;
    }
}

lattice::swift_lattice_ref* lattice::managed<lattice::swift_dynamic_object>::lattice_ref() const {
    return lattice::swift_lattice_ref::get_ref_for_lattice(this->lattice_);
}

lattice::detail::LatticeCache& lattice::detail::LatticeCache::instance() {
   static LatticeCache inst;
   return inst;
}

// MARK: Dynamic Object
namespace lattice {

bool managed<swift_dynamic_object>::has_value(const std::string& name) const {
    if (name == "id" || name == "globalId") {
        return true;
    }
    if (!properties_.count(name)) {
        return false;
    }
    const auto& property_desc = this->properties_.at(name);

    if (property_desc.kind == property_kind::link) {
        managed<swift_dynamic_object *> m;
        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
        auto base = static_cast<model_base>(*this);
        m.bind_to_parent(&base, property_desc);
        return m.has_value();
    } else {
        managed<std::optional<std::string>> m;
        m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
        return m.has_value();
    }
}

void dynamic_object::set_int(const std::string& name, int64_t value) {
    set_field(name, value);
}

link_list_ref* dynamic_object::get_link_list(const std::string &name) const {
    if (lattice) {
        // Create a fresh managed vector bound to this object's property
        managed<std::vector<swift_dynamic_object *>> m;
        const property_descriptor& property = managed_.properties_.at(name);
        model_base* base = const_cast<model_base*>(static_cast<const model_base*>(&managed_));
        m.bind_to_parent(base, property);
        // Return an owning link_list_ref - no caching needed since managed<> is backed by DB
        return link_list_ref::create(m);
    } else {
        // Unmanaged: list_values in swift_dynamic_object is the source of truth
        // TODO: Consider if link_list_ref should extend lifetime of swift_dynamic_object
        return link_list_ref::wrap(unmanaged_.get_link_list(name));
    }
}

geo_bounds_list_ref* dynamic_object::get_geo_bounds_list(const std::string &name) const {
    if (lattice) {
        // Create a fresh managed vector bound to this object's property
        managed<std::vector<geo_bounds*>> m;
        const property_descriptor& property = managed_.properties_.at(name);
        model_base* base = const_cast<model_base*>(static_cast<const model_base*>(&managed_));
        m.bind_to_parent(base, property);
        // Return an owning geo_bounds_list_ref
        return geo_bounds_list_ref::create(m);
    } else {
        // Unmanaged: return empty list for now
        // TODO: Add geo_bounds list support to unmanaged objects
        return geo_bounds_list_ref::create();
    }
}

void dynamic_object::manage(managed<swift_dynamic_object> o) {
    unmanaged_.~swift_dynamic_object();
    new (&managed_) managed(o);
    lattice = lattice::swift_lattice_ref::get_ref_for_lattice(o.lattice_);
}

// MARK: Swift Lattice
// Construct with swift_configuration (includes row migration callback)
swift_lattice::swift_lattice(const swift_configuration& config, const SchemaVector& schemas)
    : lattice_db(config), swift_config_(config) {
    if (!config.read_only) {
        ensure_swift_tables(schemas);
    } else {
        // In read-only mode, just store schemas without creating tables
        for (const auto& entry : schemas) {
            schemas_[entry.table_name] = entry.properties;
            constraints_[entry.table_name] = entry.constraints;
        }
    }
}

swift_lattice::swift_lattice(swift_configuration&& config, const SchemaVector& schemas)
    : lattice_db(config), swift_config_(std::move(config)) {
    if (!swift_config_.read_only) {
        ensure_swift_tables(schemas);
    } else {
        // In read-only mode, just store schemas without creating tables
        for (const auto& entry : schemas) {
            schemas_[entry.table_name] = entry.properties;
            constraints_[entry.table_name] = entry.constraints;
        }
    }
}

void swift_lattice::add(dynamic_object &obj) {
    if (obj.lattice) {
        throw std::runtime_error("Cannot add already managed object");
    }
    if (obj.deleted_) {
        throw std::runtime_error("Cannot add a deleted object");
    }
    auto& unmanaged_obj = obj.unmanaged_;
    const std::string& table_name = unmanaged_obj.table_name;
    auto upsert_cols = get_upsert_columns(table_name);

    // Add with optional conflict columns for upsert
    const managed<swift_dynamic_object> object = lattice_db::add(
        std::move(unmanaged_obj), unmanaged_obj.instance_schema(), upsert_cols);

    for (auto& [name, link] : unmanaged_obj.link_values) {
        if (!link->lattice) {
            add(*link);
        }
        managed<swift_dynamic_object*> link_field = object.get_managed_field<swift_dynamic_object*>(name);
        link_field = link->managed_.as_link();
    }
    for (auto& [name, list] : unmanaged_obj.list_values) {
        auto link_list_field = object.get_managed_field<std::vector<swift_dynamic_object*>>(name);
        for (auto& obj_ptr : list->unmanaged_) {
            if (!obj_ptr->lattice) {
                add(*obj_ptr);
            }
            link_list_field.push_back(&obj_ptr->managed_);
        }
    }
    // Handle geo_bounds lists
    for (auto& [name, bounds_list] : unmanaged_obj.geo_bounds_lists) {
        auto geo_list_field = object.get_managed_field<std::vector<geo_bounds>>(name);
        for (const auto& bounds : bounds_list) {
            const_cast<managed<std::vector<geo_bounds>>&>(geo_list_field).push_back(bounds);
        }
    }
    obj.manage(object);
}

void swift_lattice::add_bulk(std::vector<dynamic_object_ref*>& objects) {
    if (objects.empty()) {
        return;
    }
    for (auto& o : objects) {
        if (o->impl_->deleted_) {
            throw std::runtime_error("Cannot add a deleted object");
        }
    }
    // Use schema from first object (all objects in the batch should have the same schema)
    auto schema = objects[0]->impl_->unmanaged_.instance_schema();
    auto upsert_cols = get_upsert_columns(schema.table_name);
    std::vector<swift_dynamic_object> v;
    for (auto& o : objects) { v.push_back(o->impl_->unmanaged_); }
    auto managed_vector = lattice_db::add_bulk_with_schema(std::move(v), schema, upsert_cols);
    for (size_t i = 0; i < managed_vector.size(); i++) {
        auto unmanaged_obj = v[i];
        objects[i]->impl_->manage(managed_vector[i]);
        for (auto& [name, link] : unmanaged_obj.link_values) {
            if (!link->lattice) {
                add(*link);
            }

            managed<swift_dynamic_object*> link_field = objects[i]->impl_->managed_.get_managed_field<swift_dynamic_object*>(name);
            link_field = link->managed_.as_link();
        }
    }
}

void swift_lattice::add_bulk(std::vector<dynamic_object*>& objects) {
    if (objects.empty()) {
        return;
    }
    // Check that first object is not already managed
    if (objects[0]->lattice) {
        throw std::runtime_error("Cannot add already managed object");
    }
    // Use schema from first object (all objects in the batch should have the same schema)
    auto schema = objects[0]->unmanaged_.instance_schema();
    auto upsert_cols = get_upsert_columns(schema.table_name);
    std::vector<swift_dynamic_object> v;
    for (auto& o : objects) {
        if (o->lattice) {
            throw std::runtime_error("Cannot add already managed object");
        }
        if (o->deleted_) {
            throw std::runtime_error("Cannot add a deleted object");
        }
        v.push_back(o->unmanaged_);
    }
    auto managed_vector = lattice_db::add_bulk_with_schema(std::move(v), schema, upsert_cols);
    for (size_t i = 0; i < managed_vector.size(); i++) {
        auto unmanaged_obj = v[i];
        objects[i]->manage(managed_vector[i]);
        for (auto& [name, link] : unmanaged_obj.link_values) {
            if (!link->lattice) {
                add(*link);
            }

            managed<swift_dynamic_object*> link_field = objects[i]->managed_.get_managed_field<swift_dynamic_object*>(name);
            link_field = link->managed_.as_link();
        }
    }
}


std::optional<managed<swift_dynamic_object>> swift_lattice::object(int64_t primary_key, const std::string& table_name) {
   auto result = lattice_db::find<swift_dynamic_object>(primary_key, table_name);
   if (result) {
       if (auto* props = get_properties_for_table(table_name)) {
           result->properties_ = *props;
           result->source.properties = *props;
       }
   }
   return result;
}

void swift_lattice::ensure_swift_tables(const SchemaVector &schemas)  {
    auto transaction = lattice::transaction(this->db());
    defer([&transaction] {
        transaction.commit();
    });
    // Build model_schema list and identify new vs existing tables
    // Note: Don't update schemas_ yet - we need old schemas for migration
    std::vector<model_schema> all_schemas;
    std::vector<std::string> new_tables;
    std::vector<std::string> existing_tables;

    for (const auto& entry : schemas) {
        // Store constraints (these don't affect migration)
        constraints_[entry.table_name] = entry.constraints;

        // Convert SwiftSchema to model_schema for table operations
        model_schema schema;
        schema.table_name = entry.table_name;
        for (const auto& [name, desc] : entry.properties) {
            if (desc.kind == property_kind::primitive ||
                (desc.kind == property_kind::list && desc.is_geo_bounds)) {
                schema.properties.push_back(desc);
            }
        }
        all_schemas.push_back(std::move(schema));

        if (!db().table_exists(entry.table_name)) {
            new_tables.push_back(entry.table_name);
        } else {
            existing_tables.push_back(entry.table_name);
        }
    }

    // Create new tables first (no migration needed)
    for (const auto& schema : all_schemas) {
        if (std::find(new_tables.begin(), new_tables.end(), schema.table_name) != new_tables.end()) {
            create_model_table_public(schema);
        }
    }

    // Incremental migration loop
    int current_version = get_schema_version();
    int target_version = swift_config_.target_schema_version;

    for (int version = current_version + 1; version <= target_version; version++) {
        migration_context migration_ctx(db());

        // For each existing table, check if it needs migration at this version
        for (const auto& table_name : existing_tables) {
            // Ask Swift for old/new schema pair for this table at this version
            auto schema_pair_opt = swift_config_.get_schema_pair_block
                ? swift_config_.get_schema_pair_block(table_name, version)
                : std::nullopt;

            if (!schema_pair_opt) {
                // No migration defined for this table at this version
                continue;
            }

            const auto& [old_schema, new_schema] = *schema_pair_opt;

            // Temporarily set schemas_ to OLD schema for hydration
            schemas_[table_name] = old_schema;

            // Query all rows and migrate them
            auto rows = db().query("SELECT id FROM " + table_name);
            LOG_INFO("swift_lattice", "migrating %zu rows for table %s", rows.size(), table_name.c_str());
            size_t migration_count = 0;
            size_t total = rows.size();
            for (const auto& row : rows) {
                defer([&migration_count, total, &table_name]() {
                    migration_count += 1;
                    if (migration_count % 100 == 0 || migration_count == total) {
                        LOG_INFO("swift_lattice", "migrated %zu/%zu for %s", migration_count, total, table_name.c_str());
                    }
                });
                int64_t row_id = std::get<int64_t>(row.at("id"));
                
                // Hydrate with OLD schema
                auto obj_opt = this->object(row_id, table_name);
                if (!obj_opt) continue;

                // Detach to get unmanaged copy with old schema values
                swift_dynamic_object old_obj = obj_opt->detach();

                // Create new object with NEW schema
                swift_dynamic_object new_obj(table_name, new_schema);
                // Copy id and globalId
                new_obj.set_int("id", old_obj.get_int("id"));
                new_obj.set_string("globalId", old_obj.get_string("globalId"));

                // Copy unchanged fields from old to new (fields that exist in both schemas)
                for (const auto& [prop_name, new_prop] : new_schema) {
                    if (prop_name == "id" || prop_name == "globalId") continue;
                    // Check if this field exists in old schema with same type
                    auto old_it = old_schema.find(prop_name);
                    if (old_it != old_schema.end() && old_it->second.type == new_prop.type) {
                        // Copy value from old to new
                        if (old_obj.has_value(prop_name)) {
                            new_obj.values[prop_name] = old_obj.values[prop_name];
                        }
                    }
                }

                // Wrap for Swift callback
                auto old_ref = dynamic_object_ref::wrap(std::make_shared<dynamic_object>(old_obj));
                auto new_ref = dynamic_object_ref::wrap(std::make_shared<dynamic_object>(new_obj));

                old_ref->retain();
                new_ref->retain();
                defer([&old_ref, &new_ref] {
                    old_ref->release();
                    new_ref->release();
                });
                
                // Call Swift migration callback
                if (swift_config_.row_migration_block) {
                    swift_config_.row_migration_block(table_name, old_ref, new_ref);
                }

                // Collect new values for update
                migration_row new_row;
                for (const auto& [prop_name, prop_desc] : new_schema) {
                    if (prop_name == "id" || prop_name == "globalId") continue;
                    if (prop_desc.kind != property_kind::primitive) continue;

                    // Handle geo_bounds specially - expands to 4 columns
                    if (prop_desc.is_geo_bounds) {
                        if (new_ref->has_geo_bounds(prop_name)) {
                            geo_bounds bounds = new_ref->get_geo_bounds(prop_name);
                            new_row[prop_name + "_minLat"] = bounds.min_lat;
                            new_row[prop_name + "_maxLat"] = bounds.max_lat;
                            new_row[prop_name + "_minLon"] = bounds.min_lon;
                            new_row[prop_name + "_maxLon"] = bounds.max_lon;
                        }
                        continue;
                    }

                    if (!new_ref->has_value(prop_name)) continue;

                    switch (prop_desc.type) {
                        case column_type::integer:
                            new_row[prop_name] = new_ref->get_int(prop_name);
                            break;
                        case column_type::real:
                            new_row[prop_name] = new_ref->get_double(prop_name);
                            break;
                        case column_type::text:
                            new_row[prop_name] = new_ref->get_string(prop_name);
                            break;
                        case column_type::blob:
                            new_row[prop_name] = new_ref->get_data(prop_name);
                            break;
                        default:
                            break;
                    }
                }

                migration_ctx.queue_row_update(table_name, row_id, std::move(new_row));
            }

            // Now update schemas_ to NEW schema and apply table migration
            schemas_[table_name] = new_schema;

            // Find the model_schema for this table and migrate
            for (const auto& schema : all_schemas) {
                if (schema.table_name == table_name) {
                    migrate_model_table_public(schema);
                    break;
                }
            }
        }

        // Apply queued row updates for this version
        migration_ctx.apply_pending_updates();

        // Update version in _lattice_meta
        set_schema_version(version);
    }

    // Finally, store all final schemas and handle any tables that didn't need migration
    for (const auto& entry : schemas) {
        schemas_[entry.table_name] = entry.properties;

        // Migrate tables that weren't touched by versioned migration
        if (std::find(existing_tables.begin(), existing_tables.end(), entry.table_name) != existing_tables.end()) {
            for (const auto& schema : all_schemas) {
                if (schema.table_name == entry.table_name) {
                    migrate_model_table_public(schema);
                    break;
                }
            }
        }
    }

    // Phase 6: Ensure geo_bounds rtree tables exist with correct data
    // This is called AFTER all migration updates are applied, so rtrees are created
    // with the correct (migrated) data rather than default values.
    ensure_geo_bounds_rtrees(all_schemas);

    // Phase 6b: Ensure FTS5 tables exist for full-text indexed columns
    ensure_fts5_tables(all_schemas);

    // Phase 7: Create UNIQUE indexes for constraints
    for (const auto& entry : schemas) {
        for (size_t i = 0; i < entry.constraints.size(); ++i) {
            const auto& constraint = entry.constraints[i];
            if (constraint.columns.empty()) continue;

            std::ostringstream idx_name;
            idx_name << "unique_" << entry.table_name << "_" << i;

            std::ostringstream sql;
            sql << "CREATE UNIQUE INDEX IF NOT EXISTS " << idx_name.str()
                << " ON " << entry.table_name << "(";
            for (size_t j = 0; j < constraint.columns.size(); ++j) {
                if (j > 0) sql << ", ";
                sql << constraint.columns[j];
            }
            sql << ")";
            db().execute(sql.str());
        }
    }
}

}

void lattice::swift_lattice::add_bulk(std::vector<dynamic_object>& objects) {
    if (objects.empty()) {
        return;
    }
    // Check that first object is not already managed
    if (objects[0].lattice) {
        throw std::runtime_error("Cannot add already managed object");
    }
    // Use schema from first object (all objects in the batch should have the same schema)
    auto schema = objects[0].unmanaged_.instance_schema();
    auto upsert_cols = get_upsert_columns(schema.table_name);
    std::vector<swift_dynamic_object> v;
    for (auto& o : objects) {
        if (o.lattice) {
            throw std::runtime_error("Cannot add already managed object");
        }
        if (o.deleted_) {
            throw std::runtime_error("Cannot add a deleted object");
        }
        v.push_back(o.unmanaged_);
    }
    auto managed_vector = lattice_db::add_bulk_with_schema(std::move(v), schema, upsert_cols);
    for (size_t i = 0; i < managed_vector.size(); i++) {
        objects[i].unmanaged_.~swift_dynamic_object();
        objects[i].managed_ = managed_vector[i];
        objects[i].lattice = lattice::swift_lattice_ref::get_ref_for_lattice(this);
    }
}

bool lattice::swift_lattice::remove(dynamic_object &&obj) {
    if (!obj.lattice) return false;
    lattice_db::remove(obj.managed_, obj.managed_.table_name());
    obj.lattice = nullptr;
    return true;
}

bool lattice::swift_lattice::remove(dynamic_object_ref* obj) {
    if (!obj->is_managed())
        return false;
    const auto table_name = obj->impl_->managed_.table_name();
    const auto properties = obj->impl_->managed_.properties_;
    lattice_db::remove(obj->impl_->managed_, obj->impl_->managed_.table_name());
    obj->impl_->lattice = nullptr;
    // Properly transition union from managed to unmanaged:
    // 1. Destroy managed_ (while lattice is still set so destructor works correctly)
    obj->impl_->managed_.~managed();
    // 2. Construct unmanaged_ in the same storage
    new (&obj->impl_->unmanaged_) swift_dynamic_object(table_name, properties);
    // 3. NOW clear lattice to indicate unmanaged state
    obj->impl_->lattice = nullptr;
    // 4. Mark as deleted so link list append can detect stale objects
    obj->impl_->deleted_ = true;

    return true;
}

void lattice::swift_lattice::attach(swift_lattice &lattice) {
    lattice_db::attach(lattice);
}

// enumerate_objects is now defined inline in lattice.hpp
