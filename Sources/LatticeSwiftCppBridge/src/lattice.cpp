#include <lattice.hpp>
#include <list.hpp>
#include <unmanaged_object.hpp>
#include <dynamic_object.hpp>
#include <geo_bounds.hpp>
#include <util.hpp>
#include <LatticeBridge.hpp>
#include <nlohmann/json.hpp>  // bundled in ../LatticeCore/include (header search path)

// Thread-local state for migration lookup functions
static thread_local lattice::swift_lattice* g_migration_lattice = nullptr;
static thread_local std::shared_ptr<lattice::dynamic_object> g_migration_lookup_result;
static thread_local std::vector<std::shared_ptr<lattice::dynamic_object>> g_migration_backlink_results;

// Thread-local state for row migration callback refs
// Set before calling the row migration callback, read by Swift via accessor functions.
static thread_local lattice::dynamic_object_ref* g_migration_old_row = nullptr;
static thread_local lattice::dynamic_object_ref* g_migration_new_row = nullptr;

// Normalize a DynamicObjectRefPtrVector element to a dynamic_object_ref&: the
// FRT vector holds pointers, the value-path vector holds values.
#if LATTICE_HAS_FRT
#  define LATTICE_DEREF_REF(x) (*(x))
#else
#  define LATTICE_DEREF_REF(x) (x)
#endif

// The schema version step currently being migrated. The incremental migration
// loop walks current+1...target one version at a time; Swift must dispatch the
// row callback to THAT version's Migration, not the target's (a v1→v3 walk
// would otherwise send v2 tables' rows to the v3 blocks and crash).
static thread_local int g_migration_current_version = 0;

// Log level control
void lattice_set_log_level(lattice::log_level level) {
    lattice::set_log_level(level);
}

lattice::log_level lattice_get_log_level() {
    return lattice::get_log_level();
}

void _lattice_post_cross_process_notification(const std::string& db_path) {
    auto notifier = lattice::make_cross_process_notifier(db_path);
    if (notifier) {
        notifier->post_notification();
    }
}

// swift_lattice_ref retain/release for SWIFT_SHARED_REFERENCE
// The ref counting is managed by swift_lattice_ref's atomic counter.
// FRT-only: below the floor swift_lattice_ref is a value type (shared_ptr owns).
#if LATTICE_HAS_FRT
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
#endif

// swift_migration_context_ref retain/release for SWIFT_SHARED_REFERENCE
// FRT-only: below the floor it's a plain value type (stack-allocated).
#if LATTICE_HAS_FRT
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
#endif

std::shared_ptr<lattice::swift_lattice> lattice::managed<lattice::swift_dynamic_object>::lattice_shared() const {
    return lattice::swift_lattice_ref::shared_for_lattice(this->lattice_);
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
    if (properties_.count(name)) {
        const auto& property_desc = this->properties_.at(name);
        if (property_desc.kind == property_kind::link) {
            managed<swift_dynamic_object *> m;
            m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
            auto base = static_cast<model_base>(*this);
            m.bind_to_parent(&base, property_desc);
            return m.has_value();
        }
    }
    // Fall through to DB check for non-link properties (or properties
    // not in the schema, e.g. AuditLog queried without being in init)
    managed<std::optional<std::string>> m;
    m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_);
    return m.has_value();
}

void dynamic_object::set_int(const std::string& name, int64_t value) {
    set_field(name, value);
}

#if LATTICE_HAS_FRT
link_list_ref* dynamic_object::get_link_list(const std::string &name) const {
#else
link_list_ref dynamic_object::get_link_list(const std::string &name) const {
#endif
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

#if LATTICE_HAS_FRT
geo_bounds_list_ref* dynamic_object::get_geo_bounds_list(const std::string &name) const {
#else
geo_bounds_list_ref dynamic_object::get_geo_bounds_list(const std::string &name) const {
#endif
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
    lattice = o.lattice_shared();
}

// MARK: Swift Lattice
// Construct with swift_configuration (includes row migration callback)
swift_lattice::swift_lattice(const swift_configuration& config, const SchemaVector& schemas)
    : lattice_db(config, /*defer_sync=*/true), swift_config_(config) {
    LOG_DEBUG("swift_lattice", "ctor start path=%s schemas=%zu read_only=%d", config.path.c_str(), schemas.size(), config.read_only);
    if (!config.read_only) {
        LOG_DEBUG("swift_lattice", "ensure_swift_tables");
        ensure_swift_tables(schemas);
        LOG_DEBUG("swift_lattice", "ensure_swift_tables done");
        // Sync setup after all tables exist (Swift tables created by ensure_swift_tables)
        lattice_db::setup_sync_if_configured();
        lattice_db::setup_ipc_if_configured();
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
    : lattice_db(config, /*defer_sync=*/true), swift_config_(std::move(config)) {
    if (!swift_config_.read_only) {
        ensure_swift_tables(schemas);
        lattice_db::setup_sync_if_configured();
        lattice_db::setup_ipc_if_configured();
    } else {
        // In read-only mode, just store schemas without creating tables
        for (const auto& entry : schemas) {
            schemas_[entry.table_name] = entry.properties;
            constraints_[entry.table_name] = entry.constraints;
        }
    }
}

// A `@Unique(..., allowsUpsert: true)` whose conflict set includes a to-one link
// uses the `<link>__link_gid` shadow column (Phase 8a). That column is normally
// filled by the post-insert link-table trigger — too late for `ON CONFLICT` to
// see the conflict — so write the link's globalId into the shadow column as part
// of the row INSERT here. (Already-managed targets only; if the target isn't yet
// persisted the shadow stays NULL, i.e. distinct — same as before.)
void swift_lattice::materialize_link_shadow_conflict_cols(swift_dynamic_object& obj,
                                                         const std::vector<std::string>& upsert_cols) {
    static const std::string suffix = "__link_gid";
    for (const auto& cc : upsert_cols) {
        if (cc.size() <= suffix.size() ||
            cc.compare(cc.size() - suffix.size(), suffix.size(), suffix) != 0) continue;
        auto it = obj.link_values.find(cc.substr(0, cc.size() - suffix.size()));
        if (it == obj.link_values.end() || !it->second || !it->second->lattice) continue;
        obj.properties[cc] = property_descriptor{cc, column_type::text};   // shadow col isn't in the Swift schema
        obj.values[cc] = it->second->managed_.global_id();
    }
}

void swift_lattice::add(dynamic_object &obj) {
    if (lattice_db::is_closed()) return;   // writes are multi-step (insert+manage+notify); no-op whole op when closed
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
    materialize_link_shadow_conflict_cols(unmanaged_obj, upsert_cols);

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
    // Handle union values — insert union rows and update parent FK
    persist_union_values(unmanaged_obj, table_name, object.id());
    obj.manage(object);
}

void swift_lattice::add_preserving_global_id(dynamic_object &obj, const std::string& preserved_global_id) {
    if (lattice_db::is_closed()) return;
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
    materialize_link_shadow_conflict_cols(unmanaged_obj, upsert_cols);

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
    // Handle union values
    persist_union_values(unmanaged_obj, table_name, object.id());
    obj.manage(object);
}

void swift_lattice::add_bulk(DynamicObjectRefPtrVector& objects) {
    if (lattice_db::is_closed()) return;
    if (objects.empty()) {
        return;
    }
    for (auto& o : objects) {
        if (LATTICE_DEREF_REF(o).impl_->deleted_) {
            LOG_ERROR("swift_lattice", "Cannot add a deleted object in add_bulk (ref)");
            throw std::runtime_error("Cannot add a deleted object");
        }
    }
    // Use schema from first object (all objects in the batch should have the same schema)
    auto schema = LATTICE_DEREF_REF(objects[0]).impl_->unmanaged_.instance_schema();
    auto upsert_cols = get_upsert_columns(schema.table_name);
    std::vector<swift_dynamic_object> v;
    for (auto& o : objects) { v.push_back(LATTICE_DEREF_REF(o).impl_->unmanaged_); }
    auto managed_vector = lattice_db::add_bulk_with_schema(std::move(v), schema, upsert_cols);
    for (size_t i = 0; i < managed_vector.size(); i++) {
        auto unmanaged_obj = v[i];
        LATTICE_DEREF_REF(objects[i]).impl_->manage(managed_vector[i]);
        for (auto& [name, link] : unmanaged_obj.link_values) {
            if (!link->lattice) {
                add(*link);
            }

            managed<swift_dynamic_object*> link_field = LATTICE_DEREF_REF(objects[i]).impl_->managed_.get_managed_field<swift_dynamic_object*>(name);
            link_field = link->managed_.as_link();
        }
    }
}

void swift_lattice::add_bulk(std::vector<dynamic_object*>& objects) {
    if (lattice_db::is_closed()) return;
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

void swift_lattice::persist_union_values(swift_dynamic_object& unmanaged_obj,
                                         const std::string& table_name,
                                         int64_t parent_id) {
    for (auto& [name, uval_ptr] : unmanaged_obj.union_values) {
        if (!uval_ptr || uval_ptr->case_name.empty()) continue;

        auto prop_it = unmanaged_obj.properties.find(name);
        if (prop_it == unmanaged_obj.properties.end() || !prop_it->second.is_union) continue;

        const auto& union_table = prop_it->second.union_desc.union_table_name;
        auto udesc_it = union_schemas_.find(union_table);
        if (udesc_it == union_schemas_.end()) continue;
        const auto& udesc = udesc_it->second;

        // Persist any unmanaged linked objects and fill in their globalIds
        for (const auto& [field_key, link_obj] : uval_ptr->link_refs()) {
            if (link_obj && !link_obj->lattice) {
                add(*link_obj);
            }
            // Now the linked object is managed — grab its globalId
            if (link_obj && link_obj->lattice) {
                uval_ptr->set_string(field_key, link_obj->managed_.global_id());
            }
        }

        // Generate globalId for the union row
        auto gid_rows = db().query(
            "SELECT lower(hex(randomblob(4))) || '-' || "
            "lower(hex(randomblob(2))) || '-' || "
            "'4' || substr(lower(hex(randomblob(2))),2) || '-' || "
            "substr('89ab', 1 + (abs(random()) % 4), 1) || "
            "substr(lower(hex(randomblob(2))),2) || '-' || "
            "lower(hex(randomblob(6))) AS gid");
        std::string new_gid = std::get<std::string>(gid_rows[0].at("gid"));

        // Build column info: for each case, map field_key → column_name
        struct col_info {
            std::string case_name;  // which case this column belongs to
            std::string col_name;   // DB column name
            std::string field_key;  // key used by macro in union_value
        };
        std::vector<col_info> all_cols;
        for (const auto& c : udesc.cases) {
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

        // Build INSERT — only read values for the ACTIVE case, NULL for all others
        std::string cols = "globalId, \"case\"";
        std::string placeholders = "?, ?";
        std::vector<column_value_t> params;
        params.push_back(new_gid);
        params.push_back(uval_ptr->case_name);

        for (const auto& ci : all_cols) {
            cols += ", " + ci.col_name;
            placeholders += ", ?";
            if (ci.case_name == uval_ptr->case_name && uval_ptr->has_field(ci.field_key)) {
                params.push_back(uval_ptr->field_as_column_value(ci.field_key));
            } else {
                params.push_back(nullptr);
            }
        }

        db().execute(
            "INSERT INTO " + union_table + " (" + cols + ") VALUES (" + placeholders + ")",
            params);

        // Update parent FK to the new union row's globalId
        db().execute(
            "UPDATE " + table_name + " SET " + name + " = ? WHERE id = ?",
            {new_gid, parent_id});
    }
}

void swift_lattice::ensure_swift_tables(const SchemaVector &schemas)  {
    // FAST PATH: when the fingerprint marker matches the schema cookie AND the
    // stored schema version equals this binary's target, every DDL statement
    // below is provably a no-op — populate in-memory state only and return
    // without taking the write lock. The marker + cookie + version are read
    // under one read transaction so the triple can't be torn by concurrent DDL.
    const std::string fp_key = compute_swift_fingerprint_key(schemas);
    {
        bool fast = false;
        int current_version = 0;
        const int target_version = swift_config_.target_schema_version;
        bool db_newer = false;
        try {
            db().execute("BEGIN");
            if (fingerprint_marker_valid_in_txn(fp_key)) {
                current_version = get_schema_version();
                if (current_version > target_version) {
                    db_newer = true;
                } else if (current_version == target_version) {
                    fast = true;
                }
            }
            db().execute("COMMIT");
        } catch (const db_error&) {
            try { db().execute("ROLLBACK"); } catch (...) {}
            fast = false;
            db_newer = false;
        }
        if (db_newer) {
            // Preserve the version guard: a no-DDL row-transform migration by a
            // newer binary leaves the cookie (and thus the marker) intact, but
            // this binary must still refuse to operate on the newer database.
            LOG_ERROR("swift_lattice", "Database schema version (%d) is newer than this binary supports (%d). "
                      "Update the application to a version that supports schema v%d.",
                      current_version, target_version, current_version);
            throw std::runtime_error(
                "Database schema version (" + std::to_string(current_version) +
                ") is newer than this binary supports (" + std::to_string(target_version) +
                "). Update the application.");
        }
        if (fast) {
            populate_swift_in_memory_state(schemas);
            LOG_INFO("swift_lattice", "ensure_swift_tables: fast path (fingerprint match)");
            dispatch_vec0_reconcile(schemas);
            return;
        }
    }

    LOG_INFO("swift_lattice", "ensure_swift_tables: acquiring exclusive transaction");
    auto transaction = lattice::transaction(this->db(), /*exclusive=*/true);
    LOG_INFO("swift_lattice", "ensure_swift_tables: exclusive transaction acquired");

    // Double-checked: another process may have completed this exact ensure
    // pass between our probe above and acquiring the write lock.
    if (fingerprint_marker_valid_in_txn(fp_key) &&
        get_schema_version() == swift_config_.target_schema_version) {
        populate_swift_in_memory_state(schemas);
        transaction.commit();
        LOG_INFO("swift_lattice", "ensure_swift_tables: fast path after lock (sibling completed)");
        dispatch_vec0_reconcile(schemas);
        return;
    }
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
            if (desc.kind == property_kind::union_type) {
                // Union FK column: TEXT storing the union row's globalId
                auto col_desc = desc;
                col_desc.type = column_type::text;
                col_desc.nullable = true;
                schema.properties.push_back(col_desc);
            } else if (desc.kind == property_kind::primitive ||
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
            lattice_db::create_model_table_public(schema);
        }
    }

    // Pre-populate schemas_ for ALL tables so Migration.lookup() can hydrate
    // objects from non-migrating tables (e.g. looking up Memory by id during
    // an Edge migration). The migration loop will override schemas_ with
    // old_schema for tables that ARE being migrated.
    for (const auto& entry : schemas) {
        schemas_[entry.table_name] = entry.properties;
    }

    // Create union tables before migration so persist_union_values works during migration callbacks
    for (const auto& entry : schemas) {
        for (const auto& [name, desc] : entry.properties) {
            if (desc.is_union) {
                lattice_db::ensure_union_table(desc.union_desc.union_table_name, desc.union_desc,
                                   entry.table_name, name);
                lattice_db::create_union_cascade_trigger(entry.table_name, name,
                                             desc.union_desc.union_table_name);
                union_schemas_[desc.union_desc.union_table_name] = desc.union_desc;
            }
        }
    }

    // Incremental migration loop
    // Stash for union values set during migration callbacks (persisted after table rebuild)
    struct pending_union_ref {
        std::string table_name;
        int64_t row_id;
        swift_dynamic_object unmanaged;
    };
    std::vector<pending_union_ref> pending_migration_union_refs;

    int current_version = lattice_db::get_schema_version();
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
        g_migration_current_version = version;

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
                        lattice_db::migrate_model_table_public(s);
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

                // Wrap for Swift callback. On the FRT path `wrap` returns a heap
                // `dynamic_object_ref*`; on the value path it returns a value. Bind
                // uniform `dynamic_object_ref*` aliases (old_ptr/new_ptr) so the
                // rest of this body — and the thread-local globals — use `->`
                // identically regardless of path. On the value path the pointers
                // address the stack-local refs, which outlive the synchronous
                // callback (the globals are cleared before they go out of scope).
                auto old_ref = dynamic_object_ref::wrap(std::make_shared<dynamic_object>(old_obj));
                auto new_ref = dynamic_object_ref::wrap(std::make_shared<dynamic_object>(new_obj));

#if LATTICE_HAS_FRT
                old_ref->retain();
                new_ref->retain();
                defer([&old_ref, &new_ref] {
                    // Mirror releaseDynamicObjectRef: delete when the count
                    // hits zero, otherwise these heap refs leak once per
                    // migrated row (Swift's retain/release nets to zero here).
                    if (old_ref->release()) delete old_ref;
                    if (new_ref->release()) delete new_ref;
                });
                dynamic_object_ref* old_ptr = old_ref;
                dynamic_object_ref* new_ptr = new_ref;
#else
                dynamic_object_ref* old_ptr = &old_ref;
                dynamic_object_ref* new_ptr = &new_ref;
#endif

                // Call Swift migration callback
                if (swift_config_.row_migration_fn_) {
                    g_migration_old_row = old_ptr;
                    g_migration_new_row = new_ptr;
                    swift_config_.row_migration_fn_(table_name, old_ptr, new_ptr);
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
                        if (new_ptr->has_geo_bounds(prop_name)) {
                            geo_bounds bounds = new_ptr->get_geo_bounds(prop_name);
                            new_row[prop_name + "_minLat"] = bounds.min_lat;
                            new_row[prop_name + "_maxLat"] = bounds.max_lat;
                            new_row[prop_name + "_minLon"] = bounds.min_lon;
                            new_row[prop_name + "_maxLon"] = bounds.max_lon;
                        }
                        continue;
                    }

                    if (!new_ptr->has_value(prop_name)) continue;

                    switch (prop_desc.type) {
                        case column_type::integer:
                            new_row[prop_name] = new_ptr->get_int(prop_name);
                            break;
                        case column_type::real:
                            new_row[prop_name] = new_ptr->get_double(prop_name);
                            break;
                        case column_type::text:
                            new_row[prop_name] = new_ptr->get_string(prop_name);
                            break;
                        case column_type::blob:
                            new_row[prop_name] = new_ptr->get_data(prop_name);
                            break;
                        default:
                            break;
                    }
                }

                migration_ctx.queue_row_update(table_name, row_id, std::move(new_row));

                // Process link_values set by the migration callback (FK-to-Link)
                std::string parent_gid = new_ptr->get_string("globalId");
                for (auto& [link_name, link_obj_ptr] : new_ptr->impl_->unmanaged_.link_values) {
                    if (!link_obj_ptr) continue;

                    auto prop_it = new_schema.find(link_name);
                    if (prop_it == new_schema.end()) continue;
                    const auto& prop = prop_it->second;
                    if (prop.kind != property_kind::link) continue;

                    std::string child_gid = link_obj_ptr->get_string("globalId");
                    if (parent_gid.empty() || child_gid.empty()) continue;

                    std::string link_table = "_" + table_name + "_" + prop.target_table + "_" + link_name;
                    lattice_db::ensure_link_table(link_table, table_name, prop.target_table);
                    db().execute("INSERT OR REPLACE INTO " + link_table +
                        " (lhs, rhs) VALUES ('" + parent_gid + "', '" + child_gid + "')");
                }

                // Process list_values set by the migration callback (FK-to-List)
                for (auto& [list_name, list_ptr] : new_ptr->impl_->unmanaged_.list_values) {
                    if (!list_ptr) continue;

                    auto prop_it = new_schema.find(list_name);
                    if (prop_it == new_schema.end()) continue;
                    const auto& prop = prop_it->second;
                    if (prop.kind != property_kind::list || prop.is_geo_bounds) continue;

                    std::string link_table = "_" + table_name + "_" + prop.target_table + "_" + list_name;
                    lattice_db::ensure_link_table(link_table, table_name, prop.target_table);

                    for (auto& elem_ptr : list_ptr->unmanaged_) {
                        if (!elem_ptr) continue;
                        std::string child_gid = elem_ptr->get_string("globalId");
                        if (parent_gid.empty() || child_gid.empty()) continue;
                        db().execute("INSERT OR REPLACE INTO " + link_table +
                            " (lhs, rhs) VALUES ('" + parent_gid + "', '" + child_gid + "')");
                    }
                }

                // Stash union values for persistence after table rebuild.
                // Can't persist now — the table still has the old schema.
                if (!new_ptr->impl_->unmanaged_.union_values.empty()) {
                    pending_migration_union_refs.push_back({table_name, row_id, new_ptr->impl_->unmanaged_});
                }
            }

            // Now update schemas_ to NEW schema and apply table migration
            LOG_INFO("swift_lattice", "  setting new schema for %s, calling migrate_model_table", table_name.c_str());
            schemas_[table_name] = new_schema;

            // Find the model_schema for this table and migrate
            for (const auto& schema : all_schemas) {
                if (schema.table_name == table_name) {
                    lattice_db::migrate_model_table_public(schema);
                    break;
                }
            }
            LOG_INFO("swift_lattice", "  migrate_model_table done for %s", table_name.c_str());
        }

        // Apply queued row updates for this version
        LOG_INFO("swift_lattice", "  applying pending updates for version %d", version);
        migration_ctx.apply_pending_updates();

        // Persist union values set during migration callbacks (table now has new schema)
        for (auto& ref : pending_migration_union_refs) {
            persist_union_values(ref.unmanaged, ref.table_name, ref.row_id);
        }
        pending_migration_union_refs.clear();

        // Update version in _lattice_meta
        LOG_INFO("swift_lattice", "  setting schema version to %d", version);
        lattice_db::set_schema_version(version);

        // Clear migration lattice pointer
        g_migration_lattice = nullptr;
        g_migration_lookup_result.reset();
        g_migration_backlink_results.clear();
        g_migration_current_version = 0;
    }

    // Finally, store all final schemas and handle any tables that didn't need migration
    for (const auto& entry : schemas) {
        schemas_[entry.table_name] = entry.properties;

        // Migrate tables that weren't touched by versioned migration
        if (std::find(existing_tables.begin(), existing_tables.end(), entry.table_name) != existing_tables.end()) {
            for (const auto& schema : all_schemas) {
                if (schema.table_name == entry.table_name) {
                    lattice_db::migrate_model_table_public(schema);
                    break;
                }
            }
        }
    }

    LOG_INFO("swift_lattice", "ensure_swift_tables: tables created/migrated, doing rtrees");
    // Phase 6: Ensure geo_bounds rtree tables exist with correct data
    // This is called AFTER all migration updates are applied, so rtrees are created
    // with the correct (migrated) data rather than default values.
    lattice_db::ensure_geo_bounds_rtrees(all_schemas);

    // Phase 6b: Ensure vec0 tables exist for vector columns
    // vec0 is normally created lazily on first add(), but sync-only DBs
    // receive data without calling add(), so vec0 must be created here.
    for (const auto& schema : all_schemas) {
        for (const auto& prop : schema.properties) {
            if (!prop.is_vector || prop.type != column_type::blob) continue;
            std::string vec_table = "_" + schema.table_name + "_" + prop.name + "_vec";
            bool existed = db().table_exists(vec_table);
            if (!existed) {
                // Infer dimensions from existing data
                auto rows = db().query(
                    "SELECT length(" + prop.name + ") AS len FROM " + schema.table_name +
                    " WHERE " + prop.name + " IS NOT NULL AND length(" + prop.name + ") > 0 LIMIT 1");
                if (rows.empty()) continue;
                int bytes = static_cast<int>(std::get<int64_t>(rows[0].at("len")));
                int dimensions = bytes / static_cast<int>(sizeof(float));
                if (dimensions <= 0) continue;
                lattice_db::ensure_vec0_table(schema.table_name, prop.name, dimensions);
                // Backfill vec0 from existing data
                db().execute(
                    "INSERT OR REPLACE INTO " + vec_table + "(global_id, embedding) "
                    "SELECT globalId, " + prop.name + " FROM " + schema.table_name +
                    " WHERE " + prop.name + " IS NOT NULL AND length(" + prop.name + ") > 0");
            } else {
                // Table exists — ensure triggers are up to date (pass 0 dimensions, ignored)
                lattice_db::ensure_vec0_table(schema.table_name, prop.name, 0);
                // Gap reconciliation (rows inserted by other connections whose
                // vec0 triggers fired elsewhere) moved off the open path — it
                // scans the model table. dispatch_vec0_reconcile() heals gaps
                // in the background after the transaction commits.
            }
        }
    }

    LOG_INFO("swift_lattice", "ensure_swift_tables: doing fts5 (%zu schemas)", all_schemas.size());
    // Phase 6c: Ensure FTS5 tables exist for full-text indexed columns
    lattice_db::ensure_fts5_tables(all_schemas);
    LOG_INFO("swift_lattice", "ensure_swift_tables: fts5 done");

    LOG_INFO("swift_lattice", "ensure_swift_tables: creating indexes");

    // Phase 7: Eagerly create junction tables and register internal table →
    // parent mappings. This MUST run before the unique-index phase because
    // a compound `@Unique` that references a link field needs the link
    // table to exist first (shadow-column triggers fire on link inserts).
    //
    // Without eager creation, polymorphic VirtualList/VirtualLink tables
    // only exist after a local write — breaking receive() on relay/downstream
    // nodes that never write locally (e.g. SyncRelay pattern).
    for (const auto& entry : schemas) {
        for (const auto& [name, desc] : entry.properties) {
            std::string link_table_name;
            if (desc.kind == property_kind::virtual_list || desc.kind == property_kind::virtual_link) {
                link_table_name = "_" + entry.table_name + "_" + name;
                lattice_db::ensure_virtual_link_table(link_table_name, entry.table_name);
            } else if (desc.kind == property_kind::link && !desc.target_table.empty()) {
                link_table_name = "_" + entry.table_name + "_" + std::string(desc.target_table) + "_" + name;
                lattice_db::ensure_link_table(link_table_name, entry.table_name, std::string(desc.target_table));
            } else if (desc.kind == property_kind::list && !desc.is_geo_bounds && !desc.target_table.empty()) {
                link_table_name = "_" + entry.table_name + "_" + std::string(desc.target_table) + "_" + name;
                lattice_db::ensure_link_table(link_table_name, entry.table_name, std::string(desc.target_table));
            }
            if (!link_table_name.empty()) {
                // Store "parent_table:property_name" so flush_changes can
                // build changed_fields_names for per-object observer dispatch.
                db().execute(
                    "INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES(?, ?)",
                    {"internal_table:" + link_table_name, entry.table_name + ":" + name}
                );
            }
        }
    }

    // Phase 8: Create UNIQUE indexes for constraints.
    //
    // A compound `@Unique` may reference `Member?`-style link fields whose
    // storage is a link table, not a column on the parent. SQLite's UNIQUE
    // INDEX can only target columns on a single table, so for each link
    // column in a constraint we materialize a shadow TEXT column
    // `<field>__link_gid` on the parent table and keep it in sync via
    // triggers on the link table:
    //
    //   AFTER INSERT on link table → UPDATE parent SET shadow = NEW.rhs
    //   AFTER DELETE on link table → UPDATE parent SET shadow = NULL
    //
    // The UNIQUE INDEX then targets the shadow column(s). Shadow cols
    // aren't in the swift schema, so the parent's audit trigger (built
    // from schema-declared cols only) ignores them — no sync pollution.
    //
    // If @Unique was added after rows were already inserted, deduplicate
    // before creating the index (keeps the newest row per unique group).
    for (const auto& entry : schemas) {
        for (size_t i = 0; i < entry.constraints.size(); ++i) {
            const auto& constraint = entry.constraints[i];
            if (constraint.columns.empty()) continue;

            // Resolve each constraint column. Link-kind cols are replaced
            // with their shadow column name after ensuring the shadow col
            // and its maintenance triggers exist.
            std::vector<std::string> resolved_cols;
            resolved_cols.reserve(constraint.columns.size());

            for (const auto& col : constraint.columns) {
                auto prop_it = entry.properties.find(col);
                if (prop_it == entry.properties.end()) {
                    // Not in the schema map — treat as a literal column name
                    // (matches existing behavior for primitive fields).
                    resolved_cols.push_back(col);
                    continue;
                }
                const auto& desc = prop_it->second;

                if (desc.kind == property_kind::link && !desc.target_table.empty()) {
                    const std::string shadow_col = col + "__link_gid";
                    const std::string link_table =
                        "_" + entry.table_name + "_" + std::string(desc.target_table) + "_" + col;

                    // 1) Add shadow column if missing. ALTER TABLE ADD COLUMN
                    //    can't be wrapped in IF NOT EXISTS, so gate on
                    //    PRAGMA table_info.
                    auto cols_map = db().get_table_info(entry.table_name);
                    const bool shadow_added = cols_map.find(shadow_col) == cols_map.end();
                    if (shadow_added) {
                        db().execute("ALTER TABLE " + entry.table_name +
                                     " ADD COLUMN " + shadow_col + " TEXT");
                    }

                    // 2) Shadow-maintenance triggers on the link table.
                    //    Not gated by sync_disabled() — shadow is derived
                    //    state, not user-owned data; safe to update during
                    //    sync-apply too (the original link INSERT audit is
                    //    what syncs; peers recompute their own shadow).
                    const std::string ins_trig =
                        "LinkShadow_" + link_table + "_Insert_" + shadow_col;
                    db().execute(
                        "CREATE TRIGGER IF NOT EXISTS " + ins_trig +
                        " AFTER INSERT ON " + link_table +
                        " BEGIN"
                        "   UPDATE " + entry.table_name +
                        "   SET " + shadow_col + " = NEW.rhs"
                        "   WHERE globalId = NEW.lhs;"
                        " END");

                    const std::string del_trig =
                        "LinkShadow_" + link_table + "_Delete_" + shadow_col;
                    // Guard with `shadow = OLD.rhs` so a stale delete doesn't
                    // clobber a freshly re-assigned link.
                    db().execute(
                        "CREATE TRIGGER IF NOT EXISTS " + del_trig +
                        " AFTER DELETE ON " + link_table +
                        " BEGIN"
                        "   UPDATE " + entry.table_name +
                        "   SET " + shadow_col + " = NULL"
                        "   WHERE globalId = OLD.lhs"
                        "     AND " + shadow_col + " = OLD.rhs;"
                        " END");

                    // 3) Backfill from the current link-table state so rows
                    //    inserted before the constraint was added participate
                    //    in the unique index. Only when the shadow column was
                    //    just created — on revalidation passes the triggers
                    //    have been maintaining it, and this UPDATE scans the
                    //    whole parent table.
                    if (shadow_added) {
                        db().execute(
                            "UPDATE " + entry.table_name +
                            " SET " + shadow_col + " ="
                            " (SELECT rhs FROM " + link_table +
                            " WHERE lhs = " + entry.table_name + ".globalId LIMIT 1)"
                            " WHERE " + shadow_col + " IS NULL");
                    }

                    resolved_cols.push_back(shadow_col);
                } else {
                    resolved_cols.push_back(col);
                }
            }

            std::ostringstream idx_name;
            idx_name << "unique_" << entry.table_name << "_" << i;

            std::ostringstream cols_ss;
            for (size_t j = 0; j < resolved_cols.size(); ++j) {
                if (j > 0) cols_ss << ", ";
                cols_ss << resolved_cols[j];
            }
            std::string col_list = cols_ss.str();

            std::ostringstream sql;
            sql << "CREATE UNIQUE INDEX IF NOT EXISTS " << idx_name.str()
                << " ON " << entry.table_name << "(" << col_list << ")";
            try {
                db().execute(sql.str());
            } catch (...) {
                // Duplicate data exists — deduplicate (keep newest row per group).
                LOG_WARN("swift_lattice", "Deduplicating %s for unique constraint on (%s)",
                         entry.table_name.c_str(), col_list.c_str());
                db().execute(
                    "DELETE FROM " + entry.table_name + " WHERE id NOT IN ("
                    "SELECT MAX(id) FROM " + entry.table_name + " GROUP BY " + col_list + ")");
                // Retry index creation
                db().execute(sql.str());
            }
        }
    }

    // Phase 8b: Create non-unique indexes for @Indexed properties
    for (const auto& schema : all_schemas) {
        for (const auto& prop : schema.properties) {
            if (!prop.is_indexed) continue;
            std::ostringstream sql;
            sql << "CREATE INDEX IF NOT EXISTS idx_" << schema.table_name << "_" << prop.name
                << " ON " << schema.table_name << "(" << prop.name << ")";
            db().execute(sql.str());
        }
    }

    // Union tables already created before migration loop (see above)

    // Stamp both layers' fingerprint markers inside the transaction, AFTER all
    // DDL, so the captured schema cookie reflects this pass. The core-layer
    // marker is re-stamped because this pass's DDL bumped the cookie and would
    // otherwise leave it permanently one-open stale.
    store_fingerprint_marker(fp_key);
    store_fingerprint_marker(compute_core_fingerprint_key());

    // Persist the full Swift schema so a later dynamic (no-types) open can
    // rebuild it. Inside this exclusive transaction so it commits atomically
    // with the DDL it describes.
    store_swift_schema_snapshot(schemas);

    // All migrations and schema setup succeeded — commit the transaction.
    // If anything above threw, the transaction destructor rolls back instead.
    LOG_INFO("swift_lattice", "ensure_swift_tables: committing transaction");
    transaction.commit();
    dispatch_vec0_reconcile(schemas);

    // TODO: Dispatch background IVF training for untrained vec0 tables.
    // Disabled pending investigation of IVF+int8 dimension mismatch.
    // vec0_training_future_ = std::async(std::launch::async, [this]() {
    //     train_untrained_vec0_tables();
    // });

    LOG_INFO("swift_lattice", "ensure_swift_tables done");
}

std::string swift_lattice::compute_swift_fingerprint_key(const SchemaVector& schemas) const {
    std::ostringstream out;
    out << "swift\n"
        << "epoch:" << kLatticeSchemaFormatEpoch << '\n'
        << "target_schema_version:" << swift_config_.target_schema_version << '\n';
    // Sort tables and property names: SchemaVector order follows the Swift
    // call site and SwiftSchema is an unordered_map — neither is stable.
    std::vector<const swift_schema_entry*> sorted;
    sorted.reserve(schemas.size());
    for (const auto& e : schemas) sorted.push_back(&e);
    std::sort(sorted.begin(), sorted.end(),
              [](const swift_schema_entry* a, const swift_schema_entry* b) {
                  return a->table_name < b->table_name;
              });
    for (const auto* entry : sorted) {
        out << "table:" << entry->table_name << '\n';
        std::vector<std::string> prop_names;
        prop_names.reserve(entry->properties.size());
        for (const auto& [name, desc] : entry->properties) prop_names.push_back(name);
        std::sort(prop_names.begin(), prop_names.end());
        for (const auto& name : prop_names) {
            out << "prop:" << name << '\x01';
            serialize_property_for_fingerprint(out, entry->properties.at(name));
        }
        // Constraints drive Phase 8 DDL (unique indexes, link-shadow columns
        // and triggers); allows_upsert affects conflict-clause generation.
        for (const auto& c : entry->constraints) {
            out << "constraint:";
            for (const auto& col : c.columns) out << col << '\x01';
            out << (c.allows_upsert ? 1 : 0) << '\n';
        }
    }
    std::ostringstream hex;
    hex << std::hex << std::setfill('0') << std::setw(16) << fnv1a_hash(out.str());
    return "schema_fingerprint:" + hex.str();
}

void swift_lattice::populate_swift_in_memory_state(const SchemaVector& schemas) {
    for (const auto& entry : schemas) {
        schemas_[entry.table_name] = entry.properties;
        constraints_[entry.table_name] = entry.constraints;
        for (const auto& [name, desc] : entry.properties) {
            if (desc.is_union) {
                union_schemas_[desc.union_desc.union_table_name] = desc.union_desc;
            }
            // Mirror Phase 7's link-table naming with in-memory-only helpers.
            if (desc.kind == property_kind::virtual_list ||
                desc.kind == property_kind::virtual_link) {
                note_virtual_link_table("_" + entry.table_name + "_" + name);
            } else if (desc.kind == property_kind::list && desc.is_geo_bounds) {
                note_geo_list_table(entry.table_name, "_" + entry.table_name + "_" + name);
            } else if ((desc.kind == property_kind::link ||
                        desc.kind == property_kind::list) &&
                       !desc.target_table.empty()) {
                note_link_table("_" + entry.table_name + "_" +
                                std::string(desc.target_table) + "_" + name,
                                std::string(desc.target_table));
            }
        }
    }
}

// ===========================================================================
// Dynamic schema snapshot: persist the full Swift schema (property descriptors
// + constraints) so a later no-compile-time-types open can rebuild it. nlohmann
// gives correct JSON escaping; enums are stored as stable string names so a
// future reordering of the C++ enums can't corrupt an existing snapshot.
// ===========================================================================
namespace {

const char* ct_name(column_type t) {
    switch (t) {
        case column_type::integer: return "integer";
        case column_type::real:    return "real";
        case column_type::text:    return "text";
        case column_type::blob:    return "blob";
    }
    return "text";
}
column_type ct_from(const std::string& s) {
    if (s == "integer") return column_type::integer;
    if (s == "real")    return column_type::real;
    if (s == "blob")    return column_type::blob;
    return column_type::text;
}
const char* pk_name(property_kind k) {
    switch (k) {
        case property_kind::primitive:    return "primitive";
        case property_kind::link:         return "link";
        case property_kind::list:         return "list";
        case property_kind::virtual_list: return "virtual_list";
        case property_kind::virtual_link: return "virtual_link";
        case property_kind::union_type:   return "union_type";
    }
    return "primitive";
}
property_kind pk_from(const std::string& s) {
    if (s == "link")         return property_kind::link;
    if (s == "list")         return property_kind::list;
    if (s == "virtual_list") return property_kind::virtual_list;
    if (s == "virtual_link") return property_kind::virtual_link;
    if (s == "union_type")   return property_kind::union_type;
    return property_kind::primitive;
}

nlohmann::json descriptor_to_json(const property_descriptor& d) {
    nlohmann::json j;
    j["type"] = ct_name(d.type);
    j["kind"] = pk_name(d.kind);
    j["nullable"] = d.nullable;
    if (!d.target_table.empty()) j["target_table"] = d.target_table;
    if (!d.link_table.empty())   j["link_table"]   = d.link_table;
    if (d.is_vector)     j["is_vector"] = true;
    if (d.is_geo_bounds) j["is_geo_bounds"] = true;
    if (d.is_full_text)  j["is_full_text"] = true;
    if (d.is_indexed)    j["is_indexed"] = true;
    if (d.is_unique)     j["is_unique"] = true;
    if (!d.column_name.empty()) j["column_name"] = d.column_name;
    if (d.is_union) {
        j["is_union"] = true;
        nlohmann::json u;
        u["union_table_name"] = d.union_desc.union_table_name;
        nlohmann::json cases = nlohmann::json::array();
        for (const auto& c : d.union_desc.cases) {
            nlohmann::json cj;
            cj["case_name"] = c.case_name;
            nlohmann::json vals = nlohmann::json::array();
            for (const auto& v : c.values) {
                nlohmann::json vj;
                vj["param_name"] = v.param_name;
                vj["type"] = ct_name(v.type);
                vj["is_link"] = v.is_link;
                if (!v.link_target.empty()) vj["link_target"] = v.link_target;
                vals.push_back(std::move(vj));
            }
            cj["values"] = std::move(vals);
            cases.push_back(std::move(cj));
        }
        u["cases"] = std::move(cases);
        j["union_desc"] = std::move(u);
    }
    return j;
}

property_descriptor descriptor_from_json(const std::string& name, const nlohmann::json& j) {
    property_descriptor d;
    d.name = name;
    d.type = ct_from(j.value("type", std::string("text")));
    d.kind = pk_from(j.value("kind", std::string("primitive")));
    d.nullable = j.value("nullable", false);
    d.target_table = j.value("target_table", std::string());
    d.link_table = j.value("link_table", std::string());
    d.is_vector = j.value("is_vector", false);
    d.is_geo_bounds = j.value("is_geo_bounds", false);
    d.is_full_text = j.value("is_full_text", false);
    d.is_indexed = j.value("is_indexed", false);
    d.is_unique = j.value("is_unique", false);
    d.column_name = j.value("column_name", std::string());
    d.is_union = j.value("is_union", false);
    if (d.is_union && j.contains("union_desc")) {
        const auto& u = j["union_desc"];
        d.union_desc.union_table_name = u.value("union_table_name", std::string());
        if (u.contains("cases")) {
            for (const auto& cj : u["cases"]) {
                union_case c;
                c.case_name = cj.value("case_name", std::string());
                if (cj.contains("values")) {
                    for (const auto& vj : cj["values"]) {
                        union_case_value v;
                        v.param_name = vj.value("param_name", std::string());
                        v.type = ct_from(vj.value("type", std::string("text")));
                        v.is_link = vj.value("is_link", false);
                        v.link_target = vj.value("link_target", std::string());
                        c.values.push_back(std::move(v));
                    }
                }
                d.union_desc.cases.push_back(std::move(c));
            }
        }
    }
    return d;
}

} // anonymous namespace

void swift_lattice::store_swift_schema_snapshot(const SchemaVector& schemas) {
    nlohmann::json root;
    root["format"] = 1;
    root["version"] = swift_config_.target_schema_version;
    nlohmann::json tables = nlohmann::json::array();
    for (const auto& entry : schemas) {
        nlohmann::json t;
        t["name"] = entry.table_name;
        nlohmann::json props = nlohmann::json::object();
        for (const auto& [name, desc] : entry.properties) {
            props[name] = descriptor_to_json(desc);
        }
        t["properties"] = std::move(props);
        nlohmann::json constraints = nlohmann::json::array();
        for (const auto& c : entry.constraints) {
            nlohmann::json cj;
            cj["columns"] = c.columns;
            cj["allows_upsert"] = c.allows_upsert;
            constraints.push_back(std::move(cj));
        }
        t["constraints"] = std::move(constraints);
        tables.push_back(std::move(t));
    }
    root["tables"] = std::move(tables);
    try {
        db().execute(
            "INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES('lattice_swift_schema', ?)",
            { root.dump() });
    } catch (const std::exception& e) {
        LOG_WARN("swift_lattice", "store_swift_schema_snapshot failed: %s", e.what());
    }
}

SchemaVector swift_lattice::reconstruct_swift_schema_from_db() {
    std::string raw;
    try {
        auto rows = db().query("SELECT value FROM _lattice_meta WHERE key = 'lattice_swift_schema'");
        if (!rows.empty()) {
            const auto& v = rows[0].at("value");
            if (std::holds_alternative<std::string>(v)) raw = std::get<std::string>(v);
        }
    } catch (const std::exception& e) {
        LOG_WARN("swift_lattice", "schema snapshot read failed: %s", e.what());
    }

    if (!raw.empty()) {
        try {
            nlohmann::json root = nlohmann::json::parse(raw);
            SchemaVector out;
            if (root.contains("tables")) {
                for (const auto& t : root["tables"]) {
                    swift_schema_entry entry;
                    entry.table_name = t.value("name", std::string());
                    if (t.contains("properties")) {
                        for (auto it = t["properties"].begin(); it != t["properties"].end(); ++it) {
                            entry.properties[it.key()] = descriptor_from_json(it.key(), it.value());
                        }
                    }
                    if (t.contains("constraints")) {
                        for (const auto& cj : t["constraints"]) {
                            swift_constraint c;
                            c.columns = cj.value("columns", std::vector<std::string>{});
                            c.allows_upsert = cj.value("allows_upsert", false);
                            entry.constraints.push_back(std::move(c));
                        }
                    }
                    out.push_back(std::move(entry));
                }
            }
            if (!out.empty()) return out;
        } catch (const std::exception& e) {
            LOG_WARN("swift_lattice", "schema snapshot parse failed: %s; using fallback", e.what());
        }
    }
    return reconstruct_swift_schema_fallback();
}

SchemaVector swift_lattice::reconstruct_swift_schema_fallback() {
    // Best-effort for databases written before the snapshot existed: enumerate
    // user model tables and expose their columns as PRIMITIVE properties. Link /
    // embedded / union distinctions are NOT recoverable here (documented
    // limitation) — a single open by a snapshot-aware build writes the snapshot
    // and upgrades subsequent dynamic opens to full fidelity.
    SchemaVector out;
    std::vector<std::string> tables;
    try {
        auto rows = db().query("SELECT name FROM sqlite_master WHERE type='table'");
        for (const auto& row : rows) {
            auto it = row.find("name");
            if (it == row.end() || !std::holds_alternative<std::string>(it->second)) continue;
            const std::string& name = std::get<std::string>(it->second);
            if (name.empty() || name[0] == '_') continue;        // internal + sidecar tables
            if (name == "AuditLog") continue;                    // audit log
            if (name.rfind("sqlite_", 0) == 0) continue;         // sqlite internal
            tables.push_back(name);
        }
    } catch (const std::exception& e) {
        LOG_WARN("swift_lattice", "fallback table enumeration failed: %s", e.what());
        return out;
    }
    for (const auto& table : tables) {
        swift_schema_entry entry;
        entry.table_name = table;
        try {
            auto info = db().get_table_info(table);
            for (const auto& [col, sql_type] : info) {
                property_descriptor d;
                d.name = col;
                d.kind = property_kind::primitive;
                if (sql_type == "INTEGER")   d.type = column_type::integer;
                else if (sql_type == "REAL") d.type = column_type::real;
                else if (sql_type == "BLOB") d.type = column_type::blob;
                else                         d.type = column_type::text;
                d.nullable = true;
                entry.properties[col] = std::move(d);
            }
        } catch (const std::exception& e) {
            LOG_WARN("swift_lattice", "fallback PRAGMA failed for %s: %s", table.c_str(), e.what());
        }
        out.push_back(std::move(entry));
    }
    return out;
}

void swift_lattice::dispatch_vec0_reconcile(const SchemaVector& schemas) {
    // Collect (table, vector-column) pairs; other connections (e.g. IPC sync)
    // may have inserted model rows whose vec0 triggers fired on their
    // connection but not ours. Heal in the background — the scan is O(table)
    // and must not block open. The future member blocks destruction until the
    // task drains, so `this` stays valid.
    std::vector<std::pair<std::string, std::string>> vec_props;
    for (const auto& entry : schemas) {
        for (const auto& [name, desc] : entry.properties) {
            if (desc.is_vector && desc.type == column_type::blob) {
                vec_props.emplace_back(entry.table_name, name);
            }
        }
    }
    if (vec_props.empty()) return;
#ifdef __EMSCRIPTEN__
    // No threads in the browser build — std::launch::async would abort (and
    // hangs the opening tab). Run inline: fresh DBs return immediately
    // (vec table missing or counts match), and wasm datasets are small.
    for (const auto& [table, prop] : vec_props) {
        reconcile_vec0_gaps_for(table, prop);
    }
#else
    vec0_reconcile_future_ = std::async(std::launch::async, [this, vec_props]() {
        for (const auto& [table, prop] : vec_props) {
            reconcile_vec0_gaps_for(table, prop);
        }
    });
#endif
}

void swift_lattice::reconcile_vec0_gaps_for(const std::string& table, const std::string& prop) {
    std::string vec_table = "_" + table + "_" + prop + "_vec";
    try {
        if (!db().table_exists(vec_table)) return;
        // Qualify the model table with `main.`: this task runs async after
        // open, and attach() may have installed a UNION ALL TEMP view that
        // SHADOWS the bare table name on this connection. Counting the view
        // (local + attached rows) against the local-only vec0 index would
        // "heal" the attached rows into main's vec0 — permanent orphans.
        // The vec0 index only indexes main's rows, so reconcile must only
        // ever read main's model table.
        auto mc = db().query(
            "SELECT COUNT(*) as cnt FROM main." + table +
            " WHERE " + prop + " IS NOT NULL AND length(" + prop + ") > 0");
        auto vc = db().query(
            "SELECT COUNT(*) as cnt FROM " + vec_table);
        int64_t m = mc.empty() ? 0 : std::get<int64_t>(mc[0].at("cnt"));
        int64_t v = vc.empty() ? 0 : std::get<int64_t>(vc[0].at("cnt"));
        if (m <= v) return;
        LOG_INFO("swift_lattice", "vec0 reconcile: model=%lld vec0=%lld, filling gaps in %s",
                 (long long)m, (long long)v, vec_table.c_str());
        // Find gaps using the _rowids shadow table (regular indexed table)
        // instead of the vec0 virtual table. Insert missing rows one at a
        // time since vec0 doesn't support OR IGNORE.
        std::string rowids_table = vec_table + "_rowids";
        auto gaps = db().query(
            "SELECT m.globalId, m." + prop +
            " FROM main." + table + " m"
            " LEFT JOIN " + rowids_table + " r ON r.id = m.globalId"
            " WHERE m." + prop + " IS NOT NULL"
            " AND length(m." + prop + ") > 0"
            " AND r.id IS NULL");
        for (const auto& row : gaps) {
            auto gid_it = row.find("globalId");
            auto vec_it = row.find(prop);
            if (gid_it == row.end() || vec_it == row.end()) continue;
            if (!std::holds_alternative<std::string>(gid_it->second)) continue;
            if (!std::holds_alternative<std::vector<uint8_t>>(vec_it->second)) continue;
            try {
                db().execute(
                    "INSERT INTO " + vec_table + "(global_id, embedding) VALUES (?, ?)",
                    {std::get<std::string>(gid_it->second),
                     std::get<std::vector<uint8_t>>(vec_it->second)});
            } catch (...) {}
        }
        if (!gaps.empty()) {
            LOG_INFO("swift_lattice", "vec0 reconcile: filled %zu gaps in %s",
                     gaps.size(), vec_table.c_str());
        }
    } catch (const std::exception& e) {
        LOG_WARN("swift_lattice", "vec0 reconcile failed for %s: %s", vec_table.c_str(), e.what());
    }
}

} // namespace lattice

void lattice::swift_lattice::add_bulk(std::vector<dynamic_object>& objects) {
    if (lattice_db::is_closed()) return;
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
        objects[i].lattice = lattice::swift_lattice_ref::shared_for_lattice(this);
    }
}

// Cascade-clean union table rows whose linked-case payload pointed at
// `gid` in `deleted_table`. Unlike link tables (handled by
// `lattice_db::remove`'s `WHERE rhs = ?` loop), union tables store the
// linked entity's globalId in a case-specific column — `<case>` for
// single-value cases or `<case>__<param>` for multi-value cases — so
// rhs-based cleanup misses them entirely.
//
// Without this, deleting a model leaves union rows whose payload
// references a non-existent row. Reading the union later re-loads the
// missing target via `find_by_global_id` and SIGSEGVs in
// `dynamic_object::get_object` the same way the link-table orphan
// case did.
//
// The matching DELETE on the union table fires its AuditDelete
// trigger, so peers receive the cleanup over sync just like primary
// model deletes.
static void cascade_clean_union_tables(
    lattice::swift_lattice& self,
    const std::unordered_map<std::string, lattice::union_descriptor>& union_schemas,
    const std::string& deleted_table,
    const std::string& gid)
{
    for (const auto& [union_table, desc] : union_schemas) {
        if (!self.db().table_exists(union_table)) continue;
        for (const auto& c : desc.cases) {
            for (const auto& v : c.values) {
                if (!v.is_link || v.link_target != deleted_table) continue;
                std::string col = v.param_name.empty()
                    ? c.case_name
                    : c.case_name + "__" + v.param_name;
                try {
                    self.db().execute(
                        "DELETE FROM " + union_table + " WHERE \"" + col + "\" = ?",
                        {gid});
                } catch (...) {
                    // Column may not exist on older schemas — skip silently.
                }
            }
        }
    }
}

bool lattice::swift_lattice::remove(dynamic_object &&obj) {
    if (lattice_db::is_closed()) return false;
    if (!obj.lattice) return false;
    auto gid = obj.managed_.global_id();
    auto table = obj.managed_.table_name();
    lattice_db::remove(obj.managed_, table);
    if (!gid.empty()) {
        cascade_clean_union_tables(*this, union_schemas_, table, gid);
    }
    obj.lattice = nullptr;
    return true;
}

bool lattice::swift_lattice::remove(const dynamic_object_ref& obj) {
    if (lattice_db::is_closed()) return false;
    if (!obj.is_managed())
        return false;
    const auto table_name = obj.impl_->managed_.table_name();
    const auto properties = obj.impl_->managed_.properties_;
    auto gid = obj.impl_->managed_.global_id();
    lattice_db::remove(obj.impl_->managed_, obj.impl_->managed_.table_name());
    if (!gid.empty()) {
        cascade_clean_union_tables(*this, union_schemas_, table_name, gid);
    }
    obj.impl_->lattice = nullptr;
    // Properly transition union from managed to unmanaged:
    // 1. Destroy managed_ (while lattice is still set so destructor works correctly)
    obj.impl_->managed_.~managed();
    // 2. Construct unmanaged_ in the same storage
    new (&obj.impl_->unmanaged_) swift_dynamic_object(table_name, properties);
    // 3. NOW clear lattice to indicate unmanaged state
    obj.impl_->lattice = nullptr;
    // 4. Mark as deleted so link list append can detect stale objects
    obj.impl_->deleted_ = true;

    return true;
}

bool lattice::swift_lattice::attach(swift_lattice &lattice) {
    // C++ exceptions must NOT propagate across the Swift/C++ boundary — that
    // causes std::terminate() → SIGTRAP. Catch here, surface via accessor
    // (same contract as receive_sync_data/last_receive_error).
    { std::lock_guard<std::mutex> lock(attach_error_mutex_); last_attach_error_.reset(); }
    try {
        lattice_db::attach(lattice);
        return true;
    } catch (const std::exception& e) {
        { std::lock_guard<std::mutex> lock(attach_error_mutex_); last_attach_error_ = std::string(e.what()); }
        LOG_ERROR("attach", "Exception: %s", e.what());
        return false;
    } catch (...) {
        { std::lock_guard<std::mutex> lock(attach_error_mutex_); last_attach_error_ = std::string("unknown C++ exception in attach"); }
        LOG_ERROR("attach", "Unknown exception");
        return false;
    }
}

bool lattice::swift_lattice::detach(swift_lattice &lattice) {
    { std::lock_guard<std::mutex> lock(attach_error_mutex_); last_attach_error_.reset(); }
    try {
        lattice_db::detach(lattice);
        return true;
    } catch (const std::exception& e) {
        { std::lock_guard<std::mutex> lock(attach_error_mutex_); last_attach_error_ = std::string(e.what()); }
        LOG_ERROR("detach", "Exception: %s", e.what());
        return false;
    } catch (...) {
        { std::lock_guard<std::mutex> lock(attach_error_mutex_); last_attach_error_ = std::string("unknown C++ exception in detach"); }
        LOG_ERROR("detach", "Unknown exception");
        return false;
    }
}

// enumerate_objects is now defined inline in lattice.hpp

// MARK: - Migration Lookup Functions

/// Filter a managed object's properties to only include columns that actually
/// exist in the database table. During migration, the registered schema may
/// include columns not yet added (e.g. additive schema changes applied after
/// row migrations).
static void filter_properties_to_existing_columns(
    lattice::managed<lattice::swift_dynamic_object>& obj,
    const std::string& table_name) {
    auto actual_cols = g_migration_lattice->db().get_table_info(table_name);
    lattice::SwiftSchema filtered;
    for (const auto& [name, desc] : obj.properties_) {
        if (name == "id" || name == "globalId" || actual_cols.count(name)) {
            filtered[name] = desc;
        }
    }
    obj.properties_ = filtered;
    obj.source.properties = filtered;
}

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
    filter_properties_to_existing_columns(*obj, table_name);
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
    filter_properties_to_existing_columns(*obj, table_name);
    swift_dynamic_object detached = obj->detach();
    g_migration_lookup_result = std::make_shared<dynamic_object>(detached);
    return true;
}

#if LATTICE_HAS_FRT
lattice::dynamic_object_ref* lattice::migration_take_lookup_result() {
    if (!g_migration_lookup_result) {
        // nullptr imports as nil — the same "absent" answer the value path
        // gives via an empty ref (isValid()==false).
        return nullptr;
    }
    auto result = g_migration_lookup_result;
    g_migration_lookup_result = nullptr;
    return dynamic_object_ref::wrap(result);
}


lattice::dynamic_object_ref* lattice::migration_take_backlink_result(int64_t index) {
    if (index < 0 || index >= static_cast<int64_t>(g_migration_backlink_results.size())) {
        return dynamic_object_ref::create();
    }
    return dynamic_object_ref::wrap(g_migration_backlink_results[static_cast<size_t>(index)]);
}

lattice::dynamic_object_ref* lattice::migration_get_old_row() {
    return g_migration_old_row;
}

lattice::dynamic_object_ref* lattice::migration_get_new_row() {
    return g_migration_new_row;
}
#else
lattice::dynamic_object_ref lattice::migration_take_lookup_result() {
    if (!g_migration_lookup_result) {
        return dynamic_object_ref::wrap(std::shared_ptr<dynamic_object>(nullptr));  // empty: isValid()==false
    }
    auto result = g_migration_lookup_result;
    g_migration_lookup_result = nullptr;
    return dynamic_object_ref::wrap(result);
}

lattice::dynamic_object_ref lattice::migration_get_old_row() {
    return g_migration_old_row ? *g_migration_old_row
                               : dynamic_object_ref::wrap(std::shared_ptr<dynamic_object>(nullptr));
}

lattice::dynamic_object_ref lattice::migration_get_new_row() {
    return g_migration_new_row ? *g_migration_new_row
                               : dynamic_object_ref::wrap(std::shared_ptr<dynamic_object>(nullptr));
}

lattice::dynamic_object_ref lattice::migration_take_backlink_result(int64_t index) {
    if (index < 0 || index >= static_cast<int64_t>(g_migration_backlink_results.size())) {
        return dynamic_object_ref::wrap(std::shared_ptr<dynamic_object>(nullptr));  // empty: isValid()==false
    }
    return dynamic_object_ref::wrap(g_migration_backlink_results[static_cast<size_t>(index)]);
}
#endif

int64_t lattice::migration_lookup_backlinks(const std::string& child_table,
                                            const std::string& parent_table,
                                            const std::string& link_property,
                                            const std::string& parent_global_id) {
    g_migration_backlink_results.clear();
    if (!g_migration_lattice) {
        return 0;
    }
    auto& database = g_migration_lattice->db();

    // Single links store the linked row's globalId in a shadow column
    // `<link>__link_gid` on the child table when the link is referenced by a
    // @Unique constraint; otherwise only the `_<Child>_<Parent>_<link>`
    // junction has the association. Prefer the shadow column (no stale rows);
    // the junction can carry rows for since-deleted children, which the JOIN
    // filters out. COLLATE NOCASE: stored gids are lowercase hex, Swift's
    // UUID.uuidString is uppercase.
    std::vector<int64_t> child_ids;
    auto cols = database.get_table_info(child_table);
    std::string shadow_col = link_property + "__link_gid";
    if (cols.count(shadow_col)) {
        auto rows = database.query(
            "SELECT id FROM " + child_table +
            " WHERE " + shadow_col + " = ? COLLATE NOCASE",
            {parent_global_id});
        for (const auto& row : rows) {
            child_ids.push_back(std::get<int64_t>(row.at("id")));
        }
    } else {
        std::string link_table = "_" + child_table + "_" + parent_table + "_" + link_property;
        if (!database.table_exists(link_table)) {
            return 0;
        }
        auto rows = database.query(
            "SELECT c.id AS id FROM " + link_table + " j"
            " JOIN " + child_table + " c ON c.globalId = j.lhs"
            " WHERE j.rhs = ? COLLATE NOCASE",
            {parent_global_id});
        for (const auto& row : rows) {
            child_ids.push_back(std::get<int64_t>(row.at("id")));
        }
    }

    for (int64_t child_id : child_ids) {
        auto obj = g_migration_lattice->object(child_id, child_table);
        if (!obj) {
            continue;
        }
        filter_properties_to_existing_columns(*obj, child_table);
        swift_dynamic_object detached = obj->detach();
        g_migration_backlink_results.push_back(std::make_shared<dynamic_object>(detached));
    }
    return static_cast<int64_t>(g_migration_backlink_results.size());
}

int lattice::migration_get_current_version() {
    return g_migration_current_version;
}

void lattice::swift_lattice::update_sync_filter(const SyncFilterVector& filter) {
    lattice_db::update_sync_filter(std::vector<sync_filter_entry>(filter.begin(), filter.end()));
}

void lattice::swift_lattice::clear_sync_filter() {
    lattice_db::clear_sync_filter();
}

#if LATTICE_HAS_FRT
lattice::swift_lattice_ref* lattice::swift_lattice_ref::create(const swift_configuration& config,
                                                               const SchemaVector& schemas,
                                                               cxx_error& err)
SWIFT_NAME(create(swiftConfig:schemas:error:)) {
    auto ref = new swift_lattice_ref();
    try {
        LOG_DEBUG("swift_lattice_ref", "create() start path=%s schemas=%zu", config.path.c_str(), schemas.size());
        LOG_DEBUG("swift_lattice_ref", "create() calling get_or_create_shared");
        ref->impl_ = get_or_create_shared(config, schemas);
        LOG_DEBUG("swift_lattice_ref", "create() done, impl=%p", ref->impl_.get());
    } catch (const std::exception& e) {
        auto msg = e.what();
        LOG_ERROR("swift_lattice", "%s", e.what());
        last_error_ = e;
        err = e;
    }
    return ref;
}
#else
lattice::swift_lattice_ref lattice::swift_lattice_ref::create(const swift_configuration& config,
                                                              const SchemaVector& schemas,
                                                              cxx_error& err)
SWIFT_NAME(create(swiftConfig:schemas:error:)) {
    std::shared_ptr<swift_lattice> impl;
    try {
        LOG_DEBUG("swift_lattice_ref", "create() start path=%s schemas=%zu", config.path.c_str(), schemas.size());
        impl = get_or_create_shared(config, schemas);
    } catch (const std::exception& e) {
        LOG_ERROR("swift_lattice", "%s", e.what());
        last_error_ = e;
        err = e;
    }
    return _make(impl);
}
#endif
