#pragma once
#include "ordinary_owned_launch.hpp"
#include "ordinary_installation_manifest.hpp"
#include <lattice/store_resource_context.hpp>
#include <memory>
#include <exception>
#include <sqlite3.h>

namespace lattice::detail {
class store_resource_open;
class recovery_continuous_producer;
class store_physical_vfs;
class store_physical_open_original;
class store_deletion_operation;
namespace ordinary_installation {
class retained_resource_cold_restart;

// Separate protocol and fixed OS-account installation anchor. Primary MCP
// normal_receiver and all of its source/SQL contracts remain untouched.
struct resource_normal_receiver final {
    // Channel3; installation directory4; then logical/generation/control
    // directory triples5... . The exact actual parent/child incarnations,
    // loaded image vnodes, installed anchor and every directory are checked.
    static void receive(ordinary_launch::deadline);
};

// A live actual upgraded application, not a decoded receipt or ready bit.
class resource_application_child final {
    friend class retained_resource_normal_host;
    ordinary_launch::owned_child child_;
    explicit resource_application_child(ordinary_launch::owned_child child):child_(std::move(child)){}
public:
    resource_application_child(resource_application_child&&)noexcept=default;
    resource_application_child(const resource_application_child&)=delete;
    auto identity()const{return child_.identity();}
    auto observe_terminal(ordinary_launch::deadline end){return child_.observe_terminal(end);}
    auto stop_and_join(ordinary_launch::deadline end){return child_.stop_and_join(end);}
};

// Normal launches consume an already installed resource epoch. Initial epoch
// activation belongs only to the actual retained cold-restart operation, which
// must permanently deny old aliases and observe old original retirement first.
// The same user/package-mutation trust boundary as ordinary installation
// applies; a received wire record is never an alternative installation root.
class retained_resource_normal_host final {
    friend class retained_resource_cold_restart;
    struct implementation;
    implementation* impl_=nullptr;
    explicit retained_resource_normal_host(implementation*)noexcept;
public:
    static retained_resource_normal_host open_installed(ordinary_launch::deadline);
    ~retained_resource_normal_host();
    retained_resource_normal_host(retained_resource_normal_host&&)noexcept;
    retained_resource_normal_host(const retained_resource_normal_host&)=delete;
    // Role, argv, environment, actual image and working directory come only
    // from the independently installed record. No runtime command substitution.
    resource_application_child launch(const std::string& installed_role,ordinary_launch::deadline);
};

// Dedicated one-time installer host. open_staged reads the independently
// staged fixed OS-account plan; it is not constructible from decoded data.
// Activation actually observes every selected old incarnation, permanently
// exchanges each old MAIN alias for a refusal, preserves its real sidecars,
// and publishes the resource epoch only after all real effects/receipts.
class retained_resource_cold_restart final {
    struct implementation;
    implementation* impl_=nullptr;
    explicit retained_resource_cold_restart(implementation*)noexcept;
public:
    // Explicit dedicated installer command only. The approved package record
    // must be beside this actual loaded host image; this performs real root
    // inventory/preparation, never a decoded completion-to-authority shortcut.
    static void prepare_package(const std::string& actual_package_directory,ordinary_launch::deadline);
    static retained_resource_cold_restart open_staged(ordinary_launch::deadline);
    ~retained_resource_cold_restart();
    retained_resource_cold_restart(retained_resource_cold_restart&&)noexcept;
    retained_resource_cold_restart(const retained_resource_cold_restart&)=delete;
    retained_resource_normal_host retire_and_activate(ordinary_launch::deadline);
};
}

// Private actual issuer original for one continuous factory attempt. It is
// created only by the installed root/member producer and retains real guard
// descriptions through the real container lock and Native open. No SQL grant.
class store_resource_continuous_startup final {
    friend class recovery_continuous_producer;
    struct implementation;implementation* impl_=nullptr;
    explicit store_resource_continuous_startup(implementation*)noexcept;
    static store_resource_continuous_startup acquire_(const std::string&,
        std::chrono::steady_clock::time_point);
    bool managed_()const;
    const std::string& actual_path_()const;
    // Returns an origin enum only after consuming THIS genuine producer's
    // exact retained original. A file's shape/record cannot issue this enum.
    enum class first_start : std::uint8_t {existing,created,recreated};
    first_start claim_container_(const std::string&,int actual_parent,int actual_container);
    void before_native_open_()const;
    void finish_native_open_(const std::string&,std::uint64_t actual_device,std::uint64_t actual_inode);
public:
    store_resource_continuous_startup()noexcept=default;
    ~store_resource_continuous_startup()noexcept;
    store_resource_continuous_startup(store_resource_continuous_startup&&)noexcept;
    store_resource_continuous_startup(const store_resource_continuous_startup&)=delete;
};

// The actual open adapter consumes this pre-Native owner. It retains entry SH
// through real open publication and gives each VFS original its OWN physical
// SH file description. Existing WAL/SHM continuations do not reacquire entry.
class store_resource_open final {
    friend class store_physical_api;
    friend class store_deletion_operation;
    friend class store_resource_factory_admission;
    friend store_resource_file_metadata store_resource_file_metadata_at(const std::string&,
        std::chrono::steady_clock::time_point);
    struct implementation;
    implementation* impl_=nullptr;
    explicit store_resource_open(implementation*)noexcept;
    static store_resource_open acquire_(const std::string&,bool create,std::chrono::steady_clock::time_point,bool actual_uri_enabled=true,bool capture_creation=false);
    store_resource_initialization_original take_for_builder_();
    void require_no_unclaimed_creation_()const;
    int open_(sqlite3_vfs*,sqlite3**,int,store_physical_open_original* issued=nullptr);
    int open_initializing_(sqlite3_vfs*,sqlite3**,int,store_physical_open_original&,
        store_resource_initialization_original&,store_resource_initialization_original*);
    store_physical_vfs* enroll_(store_physical_vfs&,sqlite3*,const std::string&,
        const ::lattice::physical_store_identity&,std::int64_t,int actual_attached_limit);
public:
    store_resource_open()noexcept=default;
    ~store_resource_open();
    store_resource_open(store_resource_open&&)noexcept;
    store_resource_open(const store_resource_open&)=delete;
};

enum class store_deletion_stage : std::uint8_t {
    none,physical_wait,admin_open,admin_work,admin_close,admin_fallback,
    admin_ending,filesystem,foreign_return
};
struct store_deletion_outcome final {
    store_deletion_stage first=store_deletion_stage::none;
    int first_code=0,open=-1,work=-1,close=-1,fallback=-1;
    bool open_returned=false,work_returned=false,close_returned=false,fallback_returned=false;
    bool ending_observed=false,physical_ended=false,foreign_return=false;
    bool namespace_attempted=false,namespace_effect=false;
};
class store_deletion_failure final : public std::exception {
    store_deletion_outcome actual_;
    std::exception_ptr first_native_;
public:
    explicit store_deletion_failure(store_deletion_outcome actual,std::exception_ptr first={})noexcept
        :actual_(actual),first_native_(std::move(first)){}
    const char* what()const noexcept override{return "store deletion did not establish its complete ending and filesystem result";}
    const store_deletion_outcome& actual()const noexcept{return actual_;}
    const std::exception_ptr& first_native()const noexcept{return first_native_;}
};

// Core-only filesystem operation. This captures the original entry fence;
// paths/receipts returned to Swift never carry an unlink permit. Actual owner
// retirement is a separate prerequisite before finish may acquire physical EX.
class store_deletion_operation final {
    struct implementation;
    implementation* impl_=nullptr;
    explicit store_deletion_operation(implementation*)noexcept;
public:
    static store_deletion_operation begin(const std::string& logical_path,
        std::chrono::steady_clock::time_point original_end);
    ~store_deletion_operation()noexcept;
    store_deletion_operation(store_deletion_operation&&)noexcept;
    store_deletion_operation(const store_deletion_operation&)=delete;
    const std::string& actual_path()const;
    store_deletion_outcome outcome()const noexcept;
    // Acquires actual physical EX, checkpoints with a separately enrolled
    // administrative original, proves its real close/API/VFS ending, then
    // performs the complete journaled unlink while both fences remain held.
    // Throws on pending/unknown. A partial unlink is retained as durable debt.
    void finish();
};
}
