#pragma once
#include "recovery_obligation_producer.hpp"
#include "recovery_writer_access.hpp"

namespace lattice::detail {
// Explicit incoming whole-model grant CLAIM for this inactive qualification
// slice. Neither the caller bytes nor their digest authenticate scope/authority.
// Complete producer/export coverage is deliberately not constructible here.
struct recovery_local_producer_grant {
    recovery_obligation_address address;
    std::vector<std::string> models;
    std::vector<uint8_t> incoming_grant_receipt;
};
namespace recovery_local_producer_test_hooks {
// Private deterministic seams. Authorizer faults may only restrict admission;
// that C callback must not allocate, throw, run SQL, or call user code.
struct authorizer_fault {
    const lattice_db* owner;
    int (*restrict_action)(int,const char*,const char*,const char*) noexcept;
};
extern thread_local const authorizer_fault* fault;
// Runs inside the bootstrap's private retained read snapshot, for one bounded
// test-owned sibling schedule. Production leaves it null.
extern thread_local void (*after_inventory)();
}
struct recovery_local_producer_test_access;
// Private, bounded export facts from an already admitted immutable descriptor.
// No source authority, origin upgrade or all-route completeness is conferred.
struct recovery_local_export_table {
    std::string name;
    std::vector<std::pair<std::string,column_type>> columns;
    std::set<std::string> no_history;
    bool regular_link=false;
};
struct recovery_local_export_scope {
    recovery_obligation_scope contribution;
    int64_t program_revision=0;
    std::string program_digest;
    std::vector<recovery_local_export_table> tables;
};
struct recovery_local_export_inventory {
    bool continuous=false;
    recovery_obligation_producer_discovery_limits limits{};
    std::vector<recovery_local_export_scope> scopes;
};
// Only the first nonblocking SQLite mutex probe can construct this outcome.
// It says nothing about durable absence, profile validity, or later effects.
class export_discovery_busy final : public db_error {
    friend class recovery_local_producer_adapter;
    export_discovery_busy():db_error("export discovery writer is busy"){}
public:
    export_discovery_busy(const export_discovery_busy&)=default;
};
class recovery_local_producer_adapter {
    struct context;
    struct descriptor;
    struct management;
    static thread_local management* management_;
    static std::shared_ptr<database> retained_writer_for_test(lattice_db&);
    static descriptor describe(lattice_db&, database&, const recovery_local_producer_grant&, bool initial_inventory);
    static void compile_continuous(lattice_db&,context&);
    static void compile_programs(lattice_db&, descriptor&, const recovery_obligation_producer_program&);
    static void register_context(database&, const std::shared_ptr<context>&);
    static std::shared_ptr<context> bootstrap(lattice_db&, const std::shared_ptr<database>&);
    static void validate_custody(lattice_db&, database&);
    static void publish(lattice_db&, database&) noexcept;
    friend struct recovery_local_producer_test_access;
    friend class recovery_continuous_producer;
    friend bool prepare_recovery_local_producer(lattice_db&, const std::shared_ptr<database>&);
    friend void publish_recovery_local_producer(lattice_db&, database&) noexcept;
    friend bool preserve_recovery_local_producer_relation(database&, const std::string&);
    friend void require_recovery_local_producer_maintenance_absent(database&);
public:
    // Owns setup/retirement transactions. A committed return establishes only
    // atomic local origin stamps, not dispatch or source receipt authority.
    static recovery_install_result enroll_for_qualification(std::shared_ptr<lattice_db>,
        const recovery_local_producer_grant&, const recovery_obligation_producer_discovery_limits&);
    static recovery_install_result retire_for_qualification(std::shared_ptr<lattice_db>,
        const recovery_obligation_address&, const recovery_obligation_producer_discovery_limits&);
    // Bounded durable/admitted profile facts under the actual owned WRITE.
    // Absence is not evidence that no legacy or external export route exists.
    static std::vector<recovery_obligation_producer_profile> profiles_for_owned_write(
        std::shared_ptr<lattice_db>, const recovery_obligation_producer_discovery_limits&);
    // Fresh read-only classification, including owners opened before a sibling
    // enrolled. Never reserves/drains notification delivery or grants write,
    // send, origin, or all-route authority. Stale READ snapshots refuse; false
    // must be rechecked inside the actual transaction before legacy mutations.
    static bool export_protection_required(std::shared_ptr<lattice_db>);
    // Actual owned WRITE; indexed fixed-profile checks only. Enrollment/open
    // performed the full audit. Does not copy manifests or scan retained rows.
    static recovery_local_export_inventory export_inventory_for_owned_write(std::shared_ptr<lattice_db>);
    static constexpr bool all_route_capability = false;
};
} // namespace lattice::detail
