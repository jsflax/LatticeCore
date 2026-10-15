#pragma once
#include "ordinary_managed_history.hpp"
#include "ordinary_target_scope_root_tail.hpp"
#include <memory>
namespace lattice::detail {
class ordinary_export_access;
class ordinary_target_staging_connection;
// Actual original preparatory custody, not an open/current/model capability.
// No public path, cap, VFS, SQL, cleanup, refund or construction API exists.
class ordinary_target_staging_io final {
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
    friend struct ordinary_target_reclamation_fixture;
#endif
    friend class ordinary_export_access;
    friend class ordinary_target_staging_connection;
    friend class ::lattice::database;
    struct state;
    struct environmental_phase;
    std::shared_ptr<state> state_;
    explicit ordinary_target_staging_io(std::shared_ptr<state>);
    static void prepare(const std::shared_ptr<const ordinary_unpublished_group_target>&,
        std::shared_ptr<ordinary_target_staging_io>&,ordinary_launch::deadline);
    static void require_bound_original(const std::shared_ptr<const ordinary_unpublished_group_target>&,
        ordinary_launch::deadline);
    void require_original(ordinary_launch::deadline)const;
    void require_same_target(const std::shared_ptr<const ordinary_unpublished_group_target>&,
        ordinary_launch::deadline)const;
    const char* registered_vfs_name(ordinary_launch::deadline)const;
    std::int64_t maximum_page_count(ordinary_launch::deadline)const;
    void require_preopen_constructor(ordinary_launch::deadline)const;
    void bind_descriptor_connection(const std::shared_ptr<ordinary_target_staging_connection>&,
        ordinary_launch::deadline);
    bool charge_descriptor(const ordinary_target_staging_connection&,bool,std::int64_t);
    void begin_descriptor_phase(const ordinary_target_staging_connection&,std::shared_ptr<environmental_phase>&);
    void settle_descriptor_phase(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&);
    void settle_descriptor_callback(const ordinary_target_staging_connection&);
    bool callbacks_joined(const ordinary_target_staging_connection&)const noexcept;
    bool prepare_terminal_scope_environment_history(const ordinary_target_staging_connection&,
        const std::shared_ptr<environmental_phase>&,const std::shared_ptr<ordinary_resource_reservation<true>>&,
        std::shared_ptr<ordinary_target_scope_environment_record>&)const noexcept;
    bool bind_terminal_scope_environment_history(const ordinary_target_staging_connection&,
        const std::shared_ptr<environmental_phase>&,const std::shared_ptr<ordinary_target_scope_environment_record>&)noexcept;
    bool terminal_environment_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept;
    bool end_terminal_environment(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)noexcept;
    bool terminal_environment_ended(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept;
    bool terminal_environment_historical_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept;
    std::int64_t terminal_descriptor_extent_limit(const ordinary_target_staging_connection&,const void*)const noexcept;
    bool terminal_environment_after_vfs_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept;
    std::int64_t terminal_descriptor_extent_limit_after_vfs(const ordinary_target_staging_connection&,const void*)const noexcept;
    bool terminal_environment_after_scope_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&)const noexcept;
    std::int64_t terminal_descriptor_extent_limit_after_scope(const ordinary_target_staging_connection&,const void*)const noexcept;
    bool terminal_registration_after_scope_known(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&,const void*,std::uint64_t)const noexcept;
    bool retire_terminal_registration(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&,const void*,std::uint64_t)noexcept;
    bool terminal_registration_ended(const ordinary_target_staging_connection&,const std::shared_ptr<environmental_phase>&,const void*,std::uint64_t)const noexcept;
    bool retire_descriptor_registration(const ordinary_target_staging_connection&)noexcept;
public:
    ~ordinary_target_staging_io();
    ordinary_target_staging_io(const ordinary_target_staging_io&)=delete;
    ordinary_target_staging_io& operator=(const ordinary_target_staging_io&)=delete;
};
} // namespace lattice::detail
