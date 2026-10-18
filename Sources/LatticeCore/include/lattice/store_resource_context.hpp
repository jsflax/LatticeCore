#pragma once
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

namespace lattice { class database; class lattice_db; class swift_lattice; struct physical_store_identity; }
namespace lattice::detail {
class LatticeCache;class store_resource_open;class store_physical_api;class store_physical_open_original;
class observer_delivery_open_startup;struct database_initialization_budget_test_access;
// Resource-only startup/factory boundary. No canonical SQL/history/recovery
// permission follows from this operation. The actual cold epoch and image-checked
// normal issuer (or genuine child receiver) produce retained roots. A path only
// selects that custody; an incomplete installed profile refuses file opens.
// In a genuinely uninstalled process, existing ordinary paths remain exact.
std::string store_resource_factory_path(const std::string& requested,
    std::chrono::steady_clock::time_point original_end);
void store_resource_before_cache_return(const std::string& actual,
    std::chrono::steady_clock::time_point original_end);
bool store_resource_profile_active() noexcept;
// Metadata from the enrolled original while entry/physical custody is held.
// An uninstalled legacy process returns enrolled=false. Installed refusal
// throws; callers must never fall back to the logical refusal stub in that case.
struct store_resource_file_metadata final {
    bool enrolled=false;
    std::uint64_t size=0;
    std::int64_t modified_seconds=0;
    std::uint32_t modified_nanoseconds=0;
};
store_resource_file_metadata store_resource_file_metadata_at(const std::string&,
    std::chrono::steady_clock::time_point original_end);
// Fixed real factory callsites supply their own code addresses. The Native
// issuer checks executable mappings + independently approved image bodies;
// no address is dereferenced or treated as an authority-bearing pointer.
void store_resource_bind_library_code(std::uintptr_t actual_bridge,std::uintptr_t actual_sdk,
    std::chrono::steady_clock::time_point original_end);
// Defined in the genuine database constructor TU, included in the complete
// Native consumer rebuild. This marker has no policy or mutable state.
void store_resource_database_code_anchor() noexcept;

// Genuine one-use normal factory/Native handoff. Only the actual issuer can
// populate this opaque move-only owner; a path/READY record cannot create it.
// Directory/entry/physical descriptions stay creator fenced until real ending.
// It never substitutes for the independently issued Native physical original.
class store_resource_initialization_original final {
    friend class store_resource_factory_admission;
    friend class store_resource_open;
    friend class store_physical_api;
    friend class observer_delivery_open_startup;
    friend class ::lattice::database;
    friend class ::lattice::lattice_db;
    friend struct database_initialization_budget_test_access;
    struct implementation;implementation* impl_=nullptr;
    std::uint64_t creator_=0;std::thread::id builder_{};
    std::chrono::steady_clock::time_point end_{};
    explicit store_resource_initialization_original(implementation*,std::chrono::steady_clock::time_point)noexcept;
    void transfer_to_(store_resource_initialization_original&);
    bool transport_current_()const noexcept;
    std::chrono::steady_clock::time_point original_deadline_()const;
    void finish_unselected_open_(const ::lattice::database&,const store_physical_open_original&);
    void bind_open_(::lattice::database&,const observer_delivery_open_startup&,const store_physical_open_original&);
    const store_resource_initialization_original* fresh_for_(const ::lattice::database&,
        const observer_delivery_open_startup&)const;
    void finish_initialization_(const ::lattice::database&,const observer_delivery_open_startup&);
public:
    store_resource_initialization_original()noexcept=default;
    ~store_resource_initialization_original()noexcept;
    store_resource_initialization_original(store_resource_initialization_original&&)noexcept;
    store_resource_initialization_original(const store_resource_initialization_original&)=delete;
    store_resource_initialization_original& operator=(const store_resource_initialization_original&)=delete;
};

// Capture before any cache leaf. The entry SH and independent temporary SH
// are owned through the factory's actual cache hit/build return. They never
// substitute for the individual Native/VFS originals opened by that builder.
class store_resource_factory_admission final {
    friend class LatticeCache;
    friend struct database_initialization_budget_test_access;
    struct implementation;
    implementation* impl_=nullptr;
    store_resource_initialization_original take_for_builder_();
    void require_no_unclaimed_creation_()const; // denial-only cache hit/wait path
public:
    store_resource_factory_admission(const std::string&,bool allow_new,
        std::chrono::steady_clock::time_point original_end);
    ~store_resource_factory_admission();
    store_resource_factory_admission(const store_resource_factory_admission&)=delete;
    store_resource_factory_admission(store_resource_factory_admission&&)noexcept;
    const std::string& actual_path()const;
};

// Name observation from an ACTUAL retained Native MAIN identity. This is
// notification DATA and directory custody, never a physical-ending/deletion
// permit. No extra descriptor for MAIN is opened or later closed.
class store_resource_name_observation final {
    friend class ::lattice::database;
    struct implementation;
    std::uint64_t creator_=0;
    implementation* impl_=nullptr;
    static store_resource_name_observation capture_(const ::lattice::physical_store_identity&,
        std::chrono::steady_clock::time_point);
public:
    store_resource_name_observation()noexcept=default;
    store_resource_name_observation(const store_resource_name_observation&)noexcept;
    store_resource_name_observation(store_resource_name_observation&&)noexcept;
    store_resource_name_observation& operator=(const store_resource_name_observation&)noexcept;
    store_resource_name_observation& operator=(store_resource_name_observation&&)noexcept;
    ~store_resource_name_observation()noexcept;
    bool current()const noexcept;
    int duplicate_directory_for_events()const noexcept;
};
}
