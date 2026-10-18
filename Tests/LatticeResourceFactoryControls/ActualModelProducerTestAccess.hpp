#pragma once
#include <lattice/lattice.hpp>
#include <type_traits>

namespace lattice {
// Existing lattice_db test friend only. The exact actual owner still executes
// the original protected producer; this does not issue an Open or SQL purpose.
struct observer_delivery_custody_test_access {
    static void create_model(lattice_db& owner,const model_schema& schema) {
        owner.create_model_table_public(schema);
    }
    static void require_model_registries(lattice_db& owner,const detail::recovery_owner_schema& catalog) {
        owner.require_pending_model_registries_(catalog);
    }
    static const detail::recovery_owner_schema& actual_model_catalog(const lattice_db& owner) {
        owner.require_creator_process();return owner.recovery_declarations();
    }
    static bool actual_derived_registry_binding(lattice_db& owner) {
        owner.require_creator_process();const auto& binding=owner.process_resources_.get().pending_model_registry_checker_;
        return binding.check&&binding.owner==&owner&&binding.catalog==&owner.recovery_schemas_&&
            binding.creator==detail::database_native_process();
    }
    static auto& known_link_registry(lattice_db& owner){return owner.link_tables_by_rhs_target_;}
    static auto& unknown_link_registry(lattice_db& owner){return owner.link_tables_unknown_target_;}
    static auto& virtual_link_registry(lattice_db& owner){return owner.virtual_link_tables_;}
    static auto& geo_list_registry(lattice_db& owner){return owner.list_tables_by_parent_;}
    static auto core_registry_snapshot(lattice_db& owner) {
        owner.require_creator_process();return owner.capture_mutation_registry_();
    }
    static void replace_core_registry_data(lattice_db& owner,const detail::recovery_owner_schema& catalog) {
        owner.replace_core_model_registry_data_(catalog);
    }
    template<class Owner>
    static void replace_swift_registry_data(Owner& owner,const detail::recovery_owner_schema& catalog) {
        static_assert(std::is_same_v<Owner,swift_lattice>,"actual Swift owner required");
        owner.replace_swift_model_registry_data_(catalog);
    }
    static void set_schema_version(lattice_db& owner,int version) {
        owner.set_schema_version(version);
    }
    static int get_schema_version(lattice_db& owner) {
        return owner.get_schema_version();
    }
    static bool prove_current_model_catalog(lattice_db& owner) {
        return owner.initialize_current_model_catalog_if_pending_();
    }
    static void migrate_current_model(lattice_db& owner,const model_schema& model) {
        owner.migrate_model_table(model);
    }
};
} // namespace lattice
