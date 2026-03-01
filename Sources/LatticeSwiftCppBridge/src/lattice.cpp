#include <lattice.hpp>
#include <list.hpp>
#include <unmanaged_object.hpp>
#include <dynamic_object.hpp>
#include <geo_bounds.hpp>
#include <util.hpp>

// Thread-local state for migration lookup functions
static thread_local lattice::swift_lattice* g_migration_lattice = nullptr;
static thread_local std::shared_ptr<lattice::dynamic_object> g_migration_lookup_result;

// Thread-local state for row migration callback refs
// Set before calling the row migration callback, read by Swift via accessor functions.
static thread_local lattice::dynamic_object_ref* g_migration_old_row = nullptr;
static thread_local lattice::dynamic_object_ref* g_migration_new_row = nullptr;

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
    LOG_DEBUG("swift_lattice", "ctor start path=%s schemas=%zu read_only=%d", config.path.c_str(), schemas.size(), config.read_only);
    if (!config.read_only) {
        LOG_DEBUG("swift_lattice", "ensure_swift_tables");
        ensure_swift_tables(schemas);
        LOG_DEBUG("swift_lattice", "ensure_swift_tables done");
    } else {
        // In read-only mode, just store schemas without creating tables
        for (const auto& entry : schemas) {
            schemas_[entry.table_name] = entry.properties;
            constraints_[entry.table_name] = entry.constraints;
        }
    }
    LOG_DEBUG("swift_lattice", "ctor done");
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
        LOG_ERROR("swift_lattice", "Cannot add already managed object (table: %s)", obj.unmanaged_.table_name.c_str());
        throw std::runtime_error("Cannot add already managed object");
    }
    if (obj.deleted_) {
        LOG_ERROR("swift_lattice", "Cannot add a deleted object (table: %s)", obj.unmanaged_.table_name.c_str());
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

void swift_lattice::add_preserving_global_id(dynamic_object &obj, const std::string& preserved_global_id) {
    if (obj.lattice) {
        LOG_ERROR("swift_lattice", "Cannot add already managed object (table: %s)", obj.unmanaged_.table_name.c_str());
        throw std::runtime_error("Cannot add already managed object");
    }
    if (obj.deleted_) {
        LOG_ERROR("swift_lattice", "Cannot add a deleted object (table: %s)", obj.unmanaged_.table_name.c_str());
        throw std::runtime_error("Cannot add a deleted object");
    }
    auto& unmanaged_obj = obj.unmanaged_;
    const std::string& table_name = unmanaged_obj.table_name;
    auto upsert_cols = get_upsert_columns(table_name);

    // Add with optional conflict columns for upsert, preserving globalId
    const managed<swift_dynamic_object> object = lattice_db::add(
        std::move(unmanaged_obj), unmanaged_obj.instance_schema(), upsert_cols, preserved_global_id);

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
            LOG_ERROR("swift_lattice", "Cannot add a deleted object in add_bulk (ref)");
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
        LOG_ERROR("swift_lattice", "Cannot add already managed object in add_bulk(ptr)");
        throw std::runtime_error("Cannot add already managed object");
    }
    // Use schema from first object (all objects in the batch should have the same schema)
    auto schema = objects[0]->unmanaged_.instance_schema();
    auto upsert_cols = get_upsert_columns(schema.table_name);
    std::vector<swift_dynamic_object> v;
    for (auto& o : objects) {
        if (o->lattice) {
            LOG_ERROR("swift_lattice", "Cannot add already managed object in add_bulk(ptr) loop");
            throw std::runtime_error("Cannot add already managed object");
        }
        if (o->deleted_) {
            LOG_ERROR("swift_lattice", "Cannot add a deleted object in add_bulk(ptr) loop");
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
    LOG_INFO("swift_lattice", "ensure_swift_tables: acquiring exclusive transaction");
    auto transaction = lattice::transaction(this->db(), /*exclusive=*/true);
    LOG_INFO("swift_lattice", "ensure_swift_tables: exclusive transaction acquired");
    // NOTE: No unconditional defer{commit} — if an exception occurs during
    // migration, the transaction destructor will rollback, preventing
    // partial migration state (table rebuilt but version not bumped).
    // Build model_schema list and identify new vs existing tables
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

    LOG_INFO("swift_lattice", "ensure_swift_tables: creating %zu new tables, %zu existing", new_tables.size(), existing_tables.size());

    // Create new tables first (no migration needed)
    for (const auto& schema : all_schemas) {
        if (std::find(new_tables.begin(), new_tables.end(), schema.table_name) != new_tables.end()) {
            LOG_INFO("swift_lattice", "ensure_swift_tables: creating table %s", schema.table_name.c_str());
            create_model_table_public(schema);
        }
    }

    // Pre-populate schemas_ for ALL tables so Migration.lookup() can hydrate
    // objects from non-migrating tables (e.g. looking up Memory by id during
    // an Edge migration). The migration loop will override schemas_ with
    // old_schema for tables that ARE being migrated.
    for (const auto& entry : schemas) {
        schemas_[entry.table_name] = entry.properties;
    }

    // Incremental migration loop
    int current_version = get_schema_version();
    int target_version = swift_config_.target_schema_version;
    LOG_INFO("swift_lattice", "migration: current_version=%d, target_version=%d", current_version, target_version);

    if (current_version > target_version) {
        LOG_ERROR("swift_lattice", "Database schema version (%d) is newer than this binary supports (%d). "
                  "Update the application to a version that supports schema v%d.",
                  current_version, target_version, current_version);
        throw std::runtime_error(
            "Database schema version (" + std::to_string(current_version) +
            ") is newer than this binary supports (" + std::to_string(target_version) +
            "). Update the application.");
    }

    for (int version = current_version + 1; version <= target_version; version++) {
        LOG_INFO("swift_lattice", "migration: running version %d", version);
        migration_context migration_ctx(db());

        // Set migration lattice for Migration.lookup() calls from Swift
        g_migration_lattice = this;

        // For each existing table, check if it needs migration at this version
        for (const auto& table_name : existing_tables) {
            // Look up old/new schema pair for this table at this version
            auto schema_pair_opt = swift_config_.lookupMigrationSchema(table_name, version);

            if (!schema_pair_opt) {
                LOG_DEBUG("swift_lattice", "  no migration for %s at version %d", table_name.c_str(), version);
                // No migration defined for this table at this version
                continue;
            }
            LOG_INFO("swift_lattice", "  found migration for %s at version %d", table_name.c_str(), version);

            const auto& [old_schema, new_schema] = *schema_pair_opt;

            // Guard against partial migration: if the table was already rebuilt
            // (e.g. previous migration crashed after DDL but before version bump),
            // the old schema columns won't exist. Detect this and skip row migration.
            auto actual_cols = db().get_table_info(table_name);
            bool old_schema_matches = true;
            for (const auto& [col_name, col_prop] : old_schema) {
                if (col_name == "id" || col_name == "globalId") continue;
                // Links and lists are stored in separate tables, not as columns
                if (col_prop.kind != property_kind::primitive) continue;
                if (actual_cols.find(col_name) == actual_cols.end()) {
                    old_schema_matches = false;
                    LOG_INFO("swift_lattice", "  table %s missing old column '%s' — already rebuilt",
                             table_name.c_str(), col_name.c_str());
                    break;
                }
            }

            if (!old_schema_matches) {
                // Table already has new schema from a previous partial migration.
                // Just update internal schema and rebuild triggers/indexes, skip row migration.
                LOG_INFO("swift_lattice", "  skipping row migration for %s (table already rebuilt)", table_name.c_str());
                schemas_[table_name] = new_schema;
                for (const auto& s : all_schemas) {
                    if (s.table_name == table_name) {
                        migrate_model_table_public(s);
                        break;
                    }
                }
                continue;
            }

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
                if (swift_config_.row_migration_fn_) {
                    g_migration_old_row = old_ref;
                    g_migration_new_row = new_ref;
                    swift_config_.row_migration_fn_(table_name, old_ref, new_ref);
                    g_migration_old_row = nullptr;
                    g_migration_new_row = nullptr;
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

                // Process link_values set by the migration callback (FK-to-Link)
                std::string parent_gid = new_ref->get_string("globalId");
                for (auto& [link_name, link_obj_ptr] : new_ref->impl_->unmanaged_.link_values) {
                    if (!link_obj_ptr) continue;

                    auto prop_it = new_schema.find(link_name);
                    if (prop_it == new_schema.end()) continue;
                    const auto& prop = prop_it->second;
                    if (prop.kind != property_kind::link) continue;

                    std::string child_gid = link_obj_ptr->get_string("globalId");
                    if (parent_gid.empty() || child_gid.empty()) continue;

                    std::string link_table = "_" + table_name + "_" + prop.target_table + "_" + link_name;
                    ensure_link_table(link_table, table_name);
                    db().execute("INSERT OR REPLACE INTO " + link_table +
                        " (lhs, rhs) VALUES ('" + parent_gid + "', '" + child_gid + "')");
                }

                // Process list_values set by the migration callback (FK-to-List)
                for (auto& [list_name, list_ptr] : new_ref->impl_->unmanaged_.list_values) {
                    if (!list_ptr) continue;

                    auto prop_it = new_schema.find(list_name);
                    if (prop_it == new_schema.end()) continue;
                    const auto& prop = prop_it->second;
                    if (prop.kind != property_kind::list || prop.is_geo_bounds) continue;

                    std::string link_table = "_" + table_name + "_" + prop.target_table + "_" + list_name;
                    ensure_link_table(link_table, table_name);

                    for (auto& elem_ptr : list_ptr->unmanaged_) {
                        if (!elem_ptr) continue;
                        std::string child_gid = elem_ptr->get_string("globalId");
                        if (parent_gid.empty() || child_gid.empty()) continue;
                        db().execute("INSERT OR REPLACE INTO " + link_table +
                            " (lhs, rhs) VALUES ('" + parent_gid + "', '" + child_gid + "')");
                    }
                }
            }

            // Now update schemas_ to NEW schema and apply table migration
            LOG_INFO("swift_lattice", "  setting new schema for %s, calling migrate_model_table", table_name.c_str());
            schemas_[table_name] = new_schema;

            // Find the model_schema for this table and migrate
            for (const auto& schema : all_schemas) {
                if (schema.table_name == table_name) {
                    migrate_model_table_public(schema);
                    break;
                }
            }
            LOG_INFO("swift_lattice", "  migrate_model_table done for %s", table_name.c_str());
        }

        // Apply queued row updates for this version
        LOG_INFO("swift_lattice", "  applying pending updates for version %d", version);
        migration_ctx.apply_pending_updates();

        // Update version in _lattice_meta
        LOG_INFO("swift_lattice", "  setting schema version to %d", version);
        set_schema_version(version);

        // Clear migration lattice pointer
        g_migration_lattice = nullptr;
        g_migration_lookup_result.reset();
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

    LOG_INFO("swift_lattice", "ensure_swift_tables: tables created/migrated, doing rtrees");
    // Phase 6: Ensure geo_bounds rtree tables exist with correct data
    // This is called AFTER all migration updates are applied, so rtrees are created
    // with the correct (migrated) data rather than default values.
    ensure_geo_bounds_rtrees(all_schemas);

    LOG_INFO("swift_lattice", "ensure_swift_tables: doing fts5 (%zu schemas)", all_schemas.size());
    // Phase 6b: Ensure FTS5 tables exist for full-text indexed columns
    ensure_fts5_tables(all_schemas);
    LOG_INFO("swift_lattice", "ensure_swift_tables: fts5 done");

    LOG_INFO("swift_lattice", "ensure_swift_tables: creating indexes");
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

    // Phase 7b: Create non-unique indexes for @Indexed properties
    for (const auto& schema : all_schemas) {
        for (const auto& prop : schema.properties) {
            if (!prop.is_indexed) continue;
            std::ostringstream sql;
            sql << "CREATE INDEX IF NOT EXISTS idx_" << schema.table_name << "_" << prop.name
                << " ON " << schema.table_name << "(" << prop.name << ")";
            db().execute(sql.str());
        }
    }

    // All migrations and schema setup succeeded — commit the transaction.
    // If anything above threw, the transaction destructor rolls back instead.
    LOG_INFO("swift_lattice", "ensure_swift_tables: committing transaction");
    transaction.commit();
    LOG_INFO("swift_lattice", "ensure_swift_tables done");
}

} // namespace lattice

void lattice::swift_lattice::add_bulk(std::vector<dynamic_object>& objects) {
    if (objects.empty()) {
        return;
    }
    // Check that first object is not already managed
    if (objects[0].lattice) {
        LOG_ERROR("swift_lattice", "Cannot add already managed object in add_bulk(val)");
        throw std::runtime_error("Cannot add already managed object");
    }
    // Use schema from first object (all objects in the batch should have the same schema)
    auto schema = objects[0].unmanaged_.instance_schema();
    auto upsert_cols = get_upsert_columns(schema.table_name);
    std::vector<swift_dynamic_object> v;
    for (auto& o : objects) {
        if (o.lattice) {
            LOG_ERROR("swift_lattice", "Cannot add already managed object in add_bulk(val) loop");
            throw std::runtime_error("Cannot add already managed object");
        }
        if (o.deleted_) {
            LOG_ERROR("swift_lattice", "Cannot add a deleted object in add_bulk(val) loop");
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

// MARK: - Migration Lookup Functions

bool lattice::migration_lookup(const std::string& table_name, int64_t primary_key) {
    if (!g_migration_lattice) {
        g_migration_lookup_result = nullptr;
        return false;
    }
    auto obj = g_migration_lattice->object(primary_key, table_name);
    if (!obj) {
        g_migration_lookup_result = nullptr;
        return false;
    }
    swift_dynamic_object detached = obj->detach();
    g_migration_lookup_result = std::make_shared<dynamic_object>(detached);
    return true;
}

bool lattice::migration_lookup_by_global_id(const std::string& table_name, const std::string& global_id) {
    if (!g_migration_lattice) {
        g_migration_lookup_result = nullptr;
        return false;
    }
    auto obj = g_migration_lattice->object_by_global_id(global_id, table_name);
    if (!obj) {
        g_migration_lookup_result = nullptr;
        return false;
    }
    swift_dynamic_object detached = obj->detach();
    g_migration_lookup_result = std::make_shared<dynamic_object>(detached);
    return true;
}

lattice::dynamic_object_ref* lattice::migration_take_lookup_result() {
    if (!g_migration_lookup_result) {
        return dynamic_object_ref::create();
    }
    auto result = g_migration_lookup_result;
    g_migration_lookup_result = nullptr;
    return dynamic_object_ref::wrap(result);
}

lattice::dynamic_object_ref* lattice::migration_get_old_row() {
    return g_migration_old_row;
}

lattice::dynamic_object_ref* lattice::migration_get_new_row() {
    return g_migration_new_row;
}

void lattice::swift_lattice::update_sync_filter(const SyncFilterVector& filter) {
    lattice_db::update_sync_filter(std::vector<sync_filter_entry>(filter.begin(), filter.end()));
}

void lattice::swift_lattice::clear_sync_filter() {
    lattice_db::clear_sync_filter();
}
