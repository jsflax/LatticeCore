#include <lattice.hpp>
#include "../../LatticeCore/src/ordinary_initialization_storage.hpp"
#include "mutation_operation_test_probe.hpp"
#include <filesystem>
#include <array>
#include <cstdint>
#include <atomic>
#include <mutex>
#include "../../LatticeCore/src/database_retirement.hpp"
#if defined(__APPLE__) || defined(__linux__)
#include <sys/stat.h>
#endif
#include <list.hpp>
#include <unmanaged_object.hpp>
#include <dynamic_object.hpp>
#include <geo_bounds.hpp>
#include <util.hpp>
#include <LatticeBridge.hpp>
#include <nlohmann/json.hpp>  // bundled in ../LatticeCore/include (header search path)
#include "../../LatticeCore/src/vendor/picosha2/picosha2.h"

#include <resource_startup.hpp>
#include <lattice/store_resource_context.hpp>

bool lattice::bind_store_resource_code(std::uintptr_t actual_sdk) noexcept {
    return sealed([&] {
        detail::store_resource_bind_library_code(
            reinterpret_cast<std::uintptr_t>(&bind_store_resource_code), actual_sdk,
            std::chrono::steady_clock::now()+std::chrono::seconds(30));
        return true;
    });
}
lattice::detail::store_resource_file_metadata lattice::resource_store_file_metadata(const std::string& logical_path) noexcept {
    return sealed([&] {
        return detail::store_resource_file_metadata_at(logical_path,
            std::chrono::steady_clock::now()+std::chrono::seconds(30));
    });
}

namespace lattice {
namespace {
// Fixed scratch and finite traversal; no SQL, owner calls or error-bank writes.
class ordinary_error_diagnostic {
    static constexpr std::size_t limit = 2048;
    static constexpr unsigned depth_limit = 4, node_limit = 16;
    std::array<char, limit + 1> text_{};
    std::array<const std::exception*, depth_limit> active_{};
    std::array<const char*, depth_limit + 1> labels_{};
    std::size_t size_ = 0;
    unsigned nodes_ = 0;
    bool truncated_ = false;

    void append(const char* value) noexcept {
        if (!value) value = "[message-unknown]";
        while (*value) {
            if (size_ == limit) { truncated_ = true; return; }
            text_[size_++] = *value++;
        }
    }
    bool full() const noexcept { return size_ == limit; }
    void presence(const char* label, const std::exception_ptr& error) noexcept {
        append(","); append(label); append(error ? "=present" : "=absent");
    }
    void number(int value) noexcept {
        const auto signed_value = static_cast<std::int64_t>(value);
        auto magnitude = signed_value < 0 ? std::uint64_t{0} - static_cast<std::uint64_t>(signed_value)
                                          : static_cast<std::uint64_t>(signed_value);
        std::array<char, 24> digits{};
        std::size_t count = 0;
        do { digits[count++] = static_cast<char>('0' + magnitude % 10); magnitude /= 10; } while (magnitude);
        if (signed_value < 0) append("-");
        while (count) { const char digit[]{digits[--count], '\0'}; append(digit); }
    }
    static const char* phase(detail::recovery_install_state value) noexcept {
        switch (value) {
            case detail::recovery_install_state::refused: return "refused";
            case detail::recovery_install_state::rolled_back: return "rolled_back";
            case detail::recovery_install_state::committed: return "committed";
            case detail::recovery_install_state::unsettled: return "unsettled";
            case detail::recovery_install_state::ownership_lost: return "ownership_lost";
        }
        return "[phase-unknown]";
    }
    static const char* deferred(detail::recovery_install_deferred value) noexcept {
        switch (value) {
            case detail::recovery_install_deferred::none: return "none";
            case detail::recovery_install_deferred::connection_mutex: return "connection_mutex";
            case detail::recovery_install_deferred::engine_read: return "engine_read";
            case detail::recovery_install_deferred::store_gate: return "store_gate";
            case detail::recovery_install_deferred::core_write: return "core_write";
            case detail::recovery_install_deferred::schema_registry: return "schema_registry";
        }
        return "[deferred-unknown]";
    }
    void path(unsigned depth) noexcept {
        if (!depth) { append("root"); return; }
        for (unsigned at = 1; at <= depth; ++at) {
            if (at != 1) append(".");
            append(labels_[at]);
        }
    }
    void marker(const char* value, unsigned depth, bool messages) noexcept {
        if (messages) { append(";"); path(depth); append(":"); }
        append(value);
    }
    void nested(const char* label, const std::exception_ptr& error, unsigned depth, bool messages) noexcept {
        if (!error) return;
        if (full()) { truncated_ = true; return; }
        labels_[depth] = label; // depth is at most depth_limit, before refusal below.
        if (!messages) { append(";"); append(label); append(":"); }
        if (depth >= depth_limit) { marker("[depth-limit;further-errors-unknown]", depth, messages); return; }
        if (nodes_ >= node_limit) { marker("[node-limit;further-errors-unknown]", depth, messages); return; }
        try { std::rethrow_exception(error); }
        catch (const std::exception& value) { describe(value, depth, messages); }
        catch (...) { ++nodes_; marker("[non-std-exception;message-unknown]", depth, messages); }
    }
    void describe(const std::exception& error, unsigned depth, bool messages) noexcept {
        if (full()) { truncated_ = true; return; }
        if (nodes_ >= node_limit) { marker("[node-limit;further-errors-unknown]", depth, messages); return; }
        for (unsigned at = 0; at < depth; ++at)
            if (active_[at] == &error) { marker("[cycle;further-errors-unknown]", depth, messages); return; }
        ++nodes_;
        active_[depth] = &error;
        if (const auto* owned = dynamic_cast<const detail::ordinary_order_write_error*>(&error)) {
            const auto& value = owned->settlement;
            if (!messages) {
                append("ordinary-owned-error{phase="); append(phase(value.state));
                append(",deferred="); append(deferred(value.deferred));
                append(value.unexpected_commit_observed ? ",unexpected_commit=true" : ",unexpected_commit=false");
                presence("primary", value.primary_error); presence("cleanup", value.cleanup_error);
                presence("postcommit", value.postcommit_error); presence("notification", value.notification_error);
                append("}");
            }
            nested("primary", value.primary_error, depth + 1, messages);
            nested("cleanup", value.cleanup_error, depth + 1, messages);
            nested("postcommit", value.postcommit_error, depth + 1, messages);
            nested("notification", value.notification_error, depth + 1, messages);
        } else if (const auto* checked = dynamic_cast<const detail::ordinary_checked_open_error*>(&error)) {
            if (!messages) {
                append("ordinary-checked-open-error{retirement="); append(checked->retirement ? "retained" : "absent");
                append(",context="); append(checked->retained_context ? "retained" : "absent");
                if (checked->retirement) {
                    const auto observed = checked->retirement->state();
                    append(",retirement_phase_observed=");
                    using retirement_phase = detail::database_retirement_state::phase;
                    append(observed == retirement_phase::closed ? "closed" :
                           observed == retirement_phase::close_unproved ? "close_unproved" :
                           observed == retirement_phase::live ? "live" : "[phase-unknown]");
                    append(",checked_close="); number(checked->retirement->checked_close_result());
                    append(",fallback_close="); number(checked->retirement->fallback_close_result());
                    append(checked->retirement->raw_handle_escaped() ? ",raw_escape=true" : ",raw_escape=false");
                    append(checked->retirement->owning_wrapper_moved() ? ",wrapper_moved=true" : ",wrapper_moved=false");
                }
                presence("primary", checked->primary_error); presence("cleanup", checked->cleanup_error); append("}");
            }
            nested("primary", checked->primary_error, depth + 1, messages);
            nested("cleanup", checked->cleanup_error, depth + 1, messages);
        } else if (messages) {
            append(";"); path(depth); append(":");
            const auto* what = error.what();
            append(what && *what ? what : "[empty-exception-message]");
        } else {
            append("std-exception");
        }
        active_[depth] = nullptr;
    }
public:
    void format(const std::exception& error) noexcept {
        if (!dynamic_cast<const detail::ordinary_order_write_error*>(&error) &&
            !dynamic_cast<const detail::ordinary_checked_open_error*>(&error)) {
            append(error.what()); // A valid empty unrelated what() remains exactly empty.
        } else {
            describe(error, 0, false); // All bounded structured facts precede cause text.
            append(";causes"); nodes_ = 0; active_.fill(nullptr);
            describe(error, 0, true);
        }
        if (truncated_) {
            constexpr char suffix[] = "[truncated]";
            for (std::size_t at = 0; at < sizeof(suffix) - 1; ++at)
                text_[limit - (sizeof(suffix) - 1) + at] = suffix[at];
        }
        text_[size_] = '\0';
    }
    const char* data() const noexcept { return text_.data(); }
    std::size_t size() const noexcept { return size_; }
};
}
bool assign_ordinary_bridge_exception_checked(std::string& destination, const std::exception& error) noexcept {
    try {
        ordinary_error_diagnostic diagnostic; diagnostic.format(error);
        destination.assign(diagnostic.data(), diagnostic.size());
        return true;
    } catch (...) {
        try { destination = "[diagnostic-unavailable]"; return true; } catch (...) { return false; }
    }
}
void assign_ordinary_bridge_exception(std::string& destination, const std::exception& error) noexcept {
    (void)assign_ordinary_bridge_exception_checked(destination, error);
}
bool record_ordinary_bridge_exception(const std::exception& error) noexcept {
    if (!dynamic_cast<const detail::ordinary_order_write_error*>(&error) &&
        !dynamic_cast<const detail::ordinary_checked_open_error*>(&error)) return false;
    detail::record_bridge_failure([&](std::string& destination) {
        return assign_ordinary_bridge_exception_checked(destination, error);
    });
    return true; // Recognized failure, independently of diagnostic allocation.
}
} // namespace lattice

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

// Native cache owner process is bound before any function-local cache
// initialization guard may be inherited. It conveys no store admission.
static const pid_t native_lattice_cache_creator_pid=::getpid();
lattice::detail::LatticeCache& lattice::detail::LatticeCache::instance() {
   if(native_lattice_cache_creator_pid != ::getpid()) throw std::runtime_error("inherited native LatticeCache belongs to another process");
   static LatticeCache inst;
   inst.require_creator_process();
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
            m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_, this->attachment_token_, this->attachment_writer_);
            auto base = static_cast<model_base>(*this);
            m.bind_to_parent(&base, property_desc);
            return m.has_value();
        }
    }
    // Fall through to DB check for non-link properties (or properties
    // not in the schema, e.g. AuditLog queried without being in init)
    managed<std::optional<std::string>> m;
    m.assign(this->db_, this->lattice_, this->table_name_, name, this->id_, this->attachment_token_, this->attachment_writer_);
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
        return link_list_ref::create(m, lattice);
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

namespace {
#if defined(__APPLE__) || defined(__linux__)
// One bounded opening/failure fence, not a second export quota or an issuer.
// A failed close's actual physical/retirement evidence survives exception-to-
// string conversion. Only process restart can clear an unproved native opening.
struct mounted_server_open_fact {
    const uint64_t creator=static_cast<uint64_t>(::getpid());
    const physical_store_identity physical;
    const std::shared_ptr<detail::database_retirement_state> retirement;
    bool native_attempted=false, published=false, owner_cleanup_unproved=false;
    mounted_server_open_fact(const physical_store_identity& value,
        std::shared_ptr<detail::database_retirement_state> close):physical(value),retirement(std::move(close)){}
};
struct mounted_server_open_registry {
    std::mutex leaf;
    std::array<mounted_server_open_fact*,4> slots{};
};
mounted_server_open_registry& mounted_server_open_registry_for_creator() {
    static std::atomic<uint64_t> creator{0};
    const auto current=static_cast<uint64_t>(::getpid());uint64_t empty=0;
    creator.compare_exchange_strong(empty,current,std::memory_order_acq_rel);
    if(creator.load(std::memory_order_acquire)!=current)
        throw db_error("mounted server opening registry inherited across fork");
    // Native PID refusal precedes a potentially inherited initialization guard.
    // Intentionally retained process storage includes poisoned failure records.
    static auto* value=new mounted_server_open_registry();return *value;
}
class mounted_server_open_attempt {
    mounted_server_open_registry* registry_=nullptr;
    mounted_server_open_fact* fact_=nullptr;
    size_t slot_=0;
public:
    mounted_server_open_attempt(const physical_store_identity& physical,
        const std::shared_ptr<detail::database_retirement_state>& retirement) {
        auto& registry=mounted_server_open_registry_for_creator();
        auto fact=std::make_unique<mounted_server_open_fact>(physical,retirement);
        bool conflict=false;size_t free=registry.slots.size();
        {std::lock_guard lock(registry.leaf);
            for(size_t n=0;n<registry.slots.size();++n){const auto* prior=registry.slots[n];
                if(!prior){if(free==registry.slots.size())free=n;continue;}
                if(prior->physical.filename==physical.filename||
                   (prior->physical.device==physical.device&&prior->physical.inode==physical.inode))conflict=true;
            }
            if(!conflict&&free<registry.slots.size())registry.slots[free]=fact.get();
        }
        if(conflict||free==registry.slots.size())
            throw db_error("mounted server opening or failed native close retains physical custody");
        registry_=&registry;slot_=free;fact_=fact.release();
    }
    mounted_server_open_attempt(const mounted_server_open_attempt&)=delete;
    ~mounted_server_open_attempt() noexcept {
        if(!fact_)return;
        // Quarantine inherited storage BEFORE its mutex or automatic referents.
        if(fact_->creator!=static_cast<uint64_t>(::getpid()))return;
        const bool unproved=fact_->native_attempted&&!fact_->published&&
            (fact_->owner_cleanup_unproved||fact_->retirement->state()!=detail::database_retirement_state::phase::closed||
             fact_->retirement->raw_handle_escaped());
        if(unproved)return; // exact record remains in its original bounded slot
        auto* released=fact_;
        {std::lock_guard lock(registry_->leaf);registry_->slots[slot_]=nullptr;}
        delete released; // all referent destruction off the registry leaf
    }
    void will_open_native() noexcept {fact_->native_attempted=true;}
    void published() noexcept {fact_->published=true;}
    void observed_owner_cleanup_unproved() noexcept {fact_->owner_cleanup_unproved=true;}
};
#endif
}

namespace {
template<class Actual>bool pending_registry_union_equal_(const Actual& actual,const union_descriptor& expected)noexcept {
    if(actual.union_table_name!=expected.union_table_name||actual.cases.size()!=expected.cases.size())return false;
    for(std::size_t index=0;index<expected.cases.size();++index) {
        const auto& left=actual.cases[index];const auto& right=expected.cases[index];
        if(left.case_name!=right.case_name||left.values.size()!=right.values.size())return false;
        for(std::size_t field=0;field<right.values.size();++field) {
            const auto& a=left.values[field];const auto& e=right.values[field];
            if(a.param_name!=e.param_name||a.type!=e.type||a.is_link!=e.is_link||a.link_target!=e.link_target)return false;
        }
    }
    return true;
}
template<class Actual>bool pending_registry_property_equal_(const Actual& actual,const property_descriptor& expected)noexcept {
    return actual.name==expected.name&&actual.type==expected.type&&actual.kind==expected.kind&&
        actual.target_table==expected.target_table&&actual.link_table==expected.link_table&&actual.nullable==expected.nullable&&
        actual.is_vector==expected.is_vector&&actual.is_geo_bounds==expected.is_geo_bounds&&actual.is_full_text==expected.is_full_text&&
        actual.is_indexed==expected.is_indexed&&actual.is_unique==expected.is_unique&&actual.column_name==expected.column_name&&
        actual.is_union==expected.is_union&&actual.no_history==expected.no_history&&pending_registry_union_equal_(actual.union_desc,expected.union_desc);
}
}
void swift_lattice::bind_pending_model_registry_checker_() {
    require_creator_process();
    if(!swift_resources_.in_creator_process())throw db_error("inherited Swift registry binding");
    auto& binding=process_resources_.get().pending_model_registry_checker_;
    if(binding.check||binding.owner||binding.catalog||binding.creator)
        throw db_error("Swift model registry checker already bound");
    // A fixed compiled target only; no caller function or opaque payload enters.
    binding.check=&swift_lattice::require_pending_model_registries_data_;
    binding.owner=this;binding.catalog=&recovery_schemas_;binding.creator=detail::database_native_process();
}
void swift_lattice::require_pending_model_registries_data_(const lattice_db& base,
    const detail::recovery_owner_schema& catalog) {
    // Only the real constructor installs this target with this same owner.
    // Base checked creator/owner/catalog before dispatch; no SQL or user target.
    const auto& owner=static_cast<const swift_lattice&>(base);owner.require_creator_process();
    if(!owner.swift_resources_.in_creator_process()||&catalog!=&owner.recovery_schemas_||!catalog.valid())
        throw db_error("Swift registry validation lost its actual owner catalog");
    if(owner.schemas_.size()!=catalog.swift_models.size()||owner.constraints_.size()!=catalog.swift_models.size()||
       catalog.swift_constraints.size()!=catalog.swift_models.size()||catalog.swift_order.size()!=catalog.swift_models.size())
        throw db_error("Swift model registry table corpus differs");
    for(std::size_t index=0;index<catalog.swift_order.size();++index) {
        const auto& name=catalog.swift_order[index];
        if(catalog.swift_models.find(name)==catalog.swift_models.end())throw db_error("Swift registry declaration order has an undeclared table");
        for(std::size_t prior=0;prior<index;++prior)if(catalog.swift_order[prior]==name)
            throw db_error("Swift registry declaration order repeats a table");
    }
    for(const auto& name:catalog.swift_models) {
        const auto model=catalog.models.find(name);const auto schema=owner.schemas_.find(name);
        const auto declared_constraints=catalog.swift_constraints.find(name);const auto constraints=owner.constraints_.find(name);
        if(model==catalog.models.end()||model->second.table_name!=name||schema==owner.schemas_.end()||
           schema->second.size()!=model->second.properties.size()||declared_constraints==catalog.swift_constraints.end()||
           constraints==owner.constraints_.end()||constraints->second.size()!=declared_constraints->second.size())
            throw db_error("Swift model registry is missing its exact captured table");
        for(const auto& property:model->second.properties) {
            const auto actual=schema->second.find(property.name);
            if(actual==schema->second.end()||actual->first!=actual->second.name||!pending_registry_property_equal_(actual->second,property))
                throw db_error("Swift property registry differs from its complete captured descriptor");
        }
        for(std::size_t index=0;index<declared_constraints->second.size();++index) {
            const auto& actual=constraints->second[index];const auto& expected=declared_constraints->second[index];
            if(actual.allows_upsert!=expected.allows_upsert||actual.columns.size()!=expected.columns.size())
                throw db_error("Swift constraint registry differs from its captured ordered values");
            for(std::size_t column=0;column<expected.columns.size();++column)if(std::string_view(actual.columns[column])!=std::string_view(expected.columns[column]))
                throw db_error("Swift constraint registry differs from its captured ordered values");
        }
        for(const auto& property:model->second.properties)if(property.is_union) {
            const auto actual=owner.union_schemas_.find(property.union_desc.union_table_name);
            if(actual==owner.union_schemas_.end()||!pending_registry_union_equal_(actual->second,property.union_desc))
                throw db_error("Swift union registry is missing its complete captured descriptor");
        }
    }
    if(owner.union_schemas_.size()>detail::recovery_owner_schema::max_properties)
        throw db_error("Swift union registry corpus exceeds its captured bound");
    for(const auto& [name,descriptor]:owner.union_schemas_) {
        bool declared=false;
        for(const auto& model_name:catalog.swift_models) {
            const auto model=catalog.models.find(model_name);
            for(const auto& property:model->second.properties)if(property.is_union&&property.union_desc.union_table_name==name&&
                pending_registry_union_equal_(descriptor,property.union_desc))declared=true;
        }
        if(!declared)throw db_error("Swift union registry has an undeclared descriptor");
    }
    owner.require_creator_process();
}

// MARK: Swift Lattice
// Actual managed parent is retained by the bridge factory throughout this
// unpublished construction. Reuse immutable declarations and in-memory Swift
// descriptors, without applying caller-provided migrations or schema edits.
swift_lattice::swift_lattice(const swift_configuration& config,swift_lattice& parent)
    :lattice_db(config,true,detail::ordinary_export_access::managed_clone_catalog(parent,config),{},{},true),swift_resources_(config) {
    bind_pending_model_registry_checker_();
    if(delivery_startup_)populate_pending_swift_model_registries_();
    else {
        // Ordinary clone copies only allocator-aware runtime DATA. No input
        // SchemaVector rebuild or standard-container mirror is retained.
        require_creator_process();parent.require_creator_process();
        schemas_=parent.schemas_;constraints_=parent.constraints_;union_schemas_=parent.union_schemas_;
        auto cascade=parent.capture_mutation_registry_();restore_mutation_registry_(cascade);
    }
    if(delivery_startup_) {
        finish_observer_delivery_startup_();
        publish_observer_delivery_facilities_(false); // Original Base defer_sync=true.
    }
}

// Construct with swift_configuration (includes row migration callback)
swift_lattice::swift_lattice(const swift_configuration& config,const SchemaVector& schemas,
    detail::recovery_owner_schema catalog,std::shared_ptr<database> writer,lattice_db::checked_managed_existing_key key)
    :lattice_db(config,std::move(catalog),std::move(writer),key),swift_resources_(config) {
    bind_pending_model_registry_checker_();
    // Mirror the genuine READ fast path; no ensure/heal/migration/background work.
    populate_swift_in_memory_state(schemas);
}

std::shared_ptr<swift_lattice> swift_lattice::open_managed_ordinary(const swift_configuration& config,
    const SchemaVector& schemas,const detail::ordinary_context& context,
    const std::shared_ptr<const detail::ordinary_managed_child_admission>& admission,
    const detail::ordinary_launch::deadline* dormant_end,bool* dormant_claimed) {
    detail::ordinary_require_creator(context);
    if(!context||config.ordinary_context!=context)throw db_error("checked open requires exact receiver context");
    if(!dormant_end)detail::ordinary_before_open(config.path,context);
    const auto issued_wal_limit=context->wal_extent_limit_bytes(config.path);
    if(!config.wal_extent_limit_bytes||*config.wal_extent_limit_bytes!=issued_wal_limit)
        throw db_error("checked open requires its normalized receiver WAL policy");
    if(config.is_in_memory()||config.read_only||config.is_sync_enabled()||config.is_ipc_enabled()||
       config.audit_retention_seconds!=0||config.migration_block||config.row_migration_fn_||
       !config.migration_schemas_.empty()||config.managed_product_credentials||config.target_schema_version<=0||
       config.ordinary_policy!=ordinary_sync_policy::legacy_provisional||!config.ordinary_model_profile.empty()||
       config.busy_timeout_ms<0||config.busy_timeout_ms>30000)
        throw db_error("checked open requires quiescent existing configuration");
    detail::ordinary_export_access::validate_checked_open_admission(context,admission,config.path);
    auto catalog=recovery_catalog(config,schemas);
    if(!catalog.valid()||catalog.models.empty()||catalog.swift_fingerprint.empty())
        throw db_error("checked open full native and Swift declarations required");
#if defined(__APPLE__) || defined(__linux__)
    if(config.path.empty()||config.path.size()>4096||config.path.find('\0')!=std::string::npos||
       config.path.starts_with("file:")||!std::filesystem::path(config.path).is_absolute())
        throw db_error("checked open bounded existing file required");
    // Actual no-follow main metadata plus the issued context. This is selection
    // of a retained existing file, never a seeded birth or a new source issuer.
    const auto requested=std::filesystem::path(config.path);
    const auto name=requested.filename().string();
    if(name.empty()||name=="."||name=="..")throw db_error("checked open regular main required");
    std::error_code error;const auto parent=std::filesystem::canonical(requested.parent_path(),error);
    if(error)throw db_error("checked open existing parent unavailable");
    const auto physical_path=parent/name;struct stat physical{};
    if(::lstat(physical_path.c_str(),&physical)!=0||!S_ISREG(physical.st_mode)||physical.st_nlink!=1)
        throw db_error("checked open no-follow existing regular main required");
    auto expected=std::make_shared<physical_store_identity>();
    expected->device=physical.st_dev;expected->inode=physical.st_ino;expected->filename=physical_path.string();
    if(dormant_end){
        if(!dormant_claimed||*dormant_claimed)throw db_error("dormant GROUP opening identity already consumed");
        detail::ordinary_export_access::claim_managed_group_open(context,admission,config.path,
            expected->device,expected->inode,catalog,*dormant_end);
        *dormant_claimed=true; // Nonthrowing first fact immediately after the actual shared CAS.
    }
    else context->validate_physical(expected->device,expected->inode);
    auto retirement=std::make_shared<detail::database_retirement_state>();
    std::shared_ptr<database> writer;std::shared_ptr<swift_lattice> owner;
    try {
        // The unpublished dormant owner has no concurrent public query/sync
        // route. Keep one original control across open, schema and archive SQL;
        // do not leave an expired control on a successful later request owner.
        std::shared_ptr<database_read_control> preparation_control;
        if(dormant_end){preparation_control=std::make_shared<database_read_control>();preparation_control->deadline=*dormant_end;}
        writer=std::make_shared<database>(config.path,database::open_mode::read_write,config.busy_timeout_ms,
            preparation_control,database::initialization_key(expected,context,retirement),context,config.wal_extent_limit_bytes);
        if(dormant_end)detail::ordinary_export_access::validate_managed_group_open(context,admission,config.path,
            expected->device,expected->inode,*dormant_end);
        else detail::ordinary_before_open(config.path,context);
        owner=std::shared_ptr<swift_lattice>(new swift_lattice(config,schemas,std::move(catalog),writer,
            lattice_db::checked_managed_existing_key{}));
        detail::ordinary_export_access::validate_checked_managed_schema(*owner,
            detail::ordinary_export_access::checked_reference_phase::before_enrollment,
            dormant_end?admission:std::shared_ptr<const detail::ordinary_managed_child_admission>{},
            dormant_end?*dormant_end:detail::ordinary_launch::deadline{});
        if(dormant_end)detail::ordinary_export_access::validate_managed_group_open(context,admission,config.path,
            expected->device,expected->inode,*dormant_end);
        else detail::ordinary_before_open(config.path,context);
        return owner; // Still unpublished; real enrollment and revalidation follow in cache.
    }catch(...){
        const auto primary=std::current_exception();std::exception_ptr cleanup;
        // All wrapper/target/scheduler destruction is outside SQLite/cache leaves.
        owner.reset();
        if(writer){
            try{writer->close();}catch(...){cleanup=std::current_exception();}
            writer->retire_connection_();
        }
        if(!cleanup&&retirement->state()==detail::database_retirement_state::phase::close_unproved)
            cleanup=std::make_exception_ptr(db_error("checked existing native close unproved"));
        // This error retains the actual context and recorded close evidence.
        // It cannot manufacture a normal/history/source capability.
        throw detail::ordinary_checked_open_error(primary,cleanup,context,retirement);
    }
#else
    (void)admission;throw db_error("checked existing managed open platform unsupported");
#endif
}

swift_lattice::swift_lattice(const swift_configuration& config,const SchemaVector& schemas,
    detail::recovery_owner_schema catalog,std::shared_ptr<database> writer,lattice_db::checked_mounted_server_existing_key key)
    :lattice_db(config,std::move(catalog),std::move(writer),key),swift_resources_(config) {
    bind_pending_model_registry_checker_();
    populate_swift_in_memory_state(schemas);
}


void swift_lattice::open_managed_group_receiver(const swift_configuration& supplied,const SchemaVector& schemas,
    const detail::ordinary_context& context,const std::shared_ptr<const detail::ordinary_managed_child_admission>& admission,
    detail::ordinary_launch::deadline end,std::shared_ptr<swift_lattice>& retained,bool& opening_claimed) {
    detail::ordinary_require_creator(context);
    if(retained||opening_claimed||!context||!admission||std::chrono::steady_clock::now()>=end)
        throw db_error("dormant GROUP requires its fresh original preparation");
    auto config=supplied;
    if(config.ordinary_context&&config.ordinary_context!=context)throw db_error("dormant GROUP configuration context differs");
    config.ordinary_context=context;
    config.wal_extent_limit_bytes=detail::ordinary_select_wal_extent_limit(config.path,context,config.wal_extent_limit_bytes);
    retained=open_managed_ordinary(config,schemas,context,admission,&end,&opening_claimed);
    // Real owner retention precedes any hook installation or later preservation
    // failure. No normal update buffering, observer registration, notifier,
    // background maintenance or source enrollment is published here.
    detail::ordinary_export_access::validate_checked_managed_schema(*retained,
        detail::ordinary_export_access::checked_reference_phase::before_enrollment,admission,end);
    retained->setup_administrative_transaction_hooks();
}

void swift_lattice::finish_managed_group_preparation_control(detail::ordinary_launch::deadline end) {
    require_creator_process();
    std::shared_ptr<database> writer;
    {std::lock_guard lock(connection_ownership_mutex_);
        if(closed_.load()||!db_)throw db_error("dormant GROUP original owner unavailable after archive");
        writer=db_;}
    writer->finish_checked_preparation_read_control_(end);
}

struct swift_managed_group_receiver::state {
    struct resources {
        const detail::ordinary_context context;
        const std::shared_ptr<const detail::ordinary_managed_child_admission> admission;
        const detail::ordinary_launch::deadline end;
        const std::thread::id thread=std::this_thread::get_id();
        std::shared_ptr<swift_lattice> owner;
        std::shared_ptr<const detail::ordinary_managed_receiver_preservation> archive;
        std::exception_ptr first_failure;
        bool opening_claimed=false;
        bool complete=false;
        resources(detail::ordinary_context c,std::shared_ptr<const detail::ordinary_managed_child_admission> a,
            detail::ordinary_launch::deadline e):context(std::move(c)),admission(std::move(a)),end(e){}
    };
    detail::process_bound_resource_owner<resources> custody;
    state(detail::ordinary_context c,std::shared_ptr<const detail::ordinary_managed_child_admission> a,
        detail::ordinary_launch::deadline e):custody(std::move(c),std::move(a),e){}
};
namespace {
void group_receiver_error(cxx_error& error,std::exception_ptr first) noexcept {
    // Diagnostics are fallible DATA. The bool and original exception custody
    // remain authoritative for success/failure even if text cannot allocate.
    error.name.clear();error.msg.clear();
    try{if(first)std::rethrow_exception(first);throw db_error("dormant GROUP operation failed");}
    catch(const std::exception& e){try{error.name=typeid(e).name();error.msg=e.what();}catch(...){error.name.clear();error.msg.clear();}}
    catch(...){try{error.name="native";error.msg="unknown dormant GROUP failure";}catch(...){error.name.clear();error.msg.clear();}}
}
}
bool swift_lattice_ref::prepare_managed_group_receiver(const swift_configuration& config,const SchemaVector& schemas,
    const swift_managed_history_startup& startup,const swift_ordinary_open_context& registered,
    swift_managed_group_receiver& held,cxx_error& error) noexcept {
    error.name.clear();error.msg.clear();bool registered_holder=false;
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
    try {
        if(held.value_)throw db_error("dormant GROUP preparation cannot be replaced or retried");
        if(!startup.opens_||!startup.admission_||!registered.context_||registered.admission_!=startup.admission_)
            throw db_error("dormant GROUP requires its actual startup and registered context");
        detail::ordinary_require_creator(registered.context_);
        detail::ordinary_export_access::validate_checked_open_admission(registered.context_,startup.admission_,config.path);
        auto& cache=detail::LatticeCache::instance();
        const auto slot=cache.reserve_group_receiver_preparation();
        held.value_=std::make_shared<swift_managed_group_receiver::state>(registered.context_,startup.admission_,end);
        cache.retain_group_receiver_preparation(slot,held.value_);registered_holder=true;
        auto& original=held.value_->custody.get();
        swift_lattice::open_managed_group_receiver(config,schemas,original.context,original.admission,original.end,original.owner,original.opening_claimed);
        original.archive=detail::ordinary_export_access::preserve_managed_receiver(original.owner,original.admission,original.end);
        original.owner->finish_managed_group_preparation_control(original.end);
        if(std::chrono::steady_clock::now()>=original.end)throw db_error("dormant GROUP original publication deadline expired");
        original.complete=true;return true;
    }catch(...){
        const auto first=std::current_exception();
        if(registered_holder&&held.value_->custody.in_creator_process()){
            auto& original=held.value_->custody.get();if(!original.first_failure)original.first_failure=first;
            // This original cannot mint another archive/source if metadata later
            // disappears. Hold the real native open/transaction/cleanup error.
            if(original.opening_claimed)detail::ordinary_export_access::remember_managed_open_failure(
                original.admission,config.path,original.first_failure);
        }
        group_receiver_error(error,first);return false;
    }
}
bool swift_managed_group_receiver::preservation(swift_owned_receiver_preservation& held,cxx_error& error) const noexcept {
    error.name.clear();error.msg.clear();
    try{
        if(!value_||!value_->custody.in_creator_process())throw db_error("dormant GROUP creator custody unavailable");
        const auto& original=value_->custody.get();
        if(original.thread!=std::this_thread::get_id()||original.first_failure||!original.complete||!original.archive||
           !original.owner||original.owner->is_closed()||std::chrono::steady_clock::now()>=original.end)
            throw db_error("dormant GROUP original preservation unavailable");
        // The existing native archive validator still owns current/directory
        // admission. This is a SAME strong capability copy, never a DATA factory.
        held=swift_owned_receiver_preservation(original.archive);return true;
    }catch(...){group_receiver_error(error,std::current_exception());return false;}
}

std::shared_ptr<swift_lattice> swift_lattice::open_checked_mounted_server(
    const swift_configuration& config,const SchemaVector& schemas) {
#if defined(__APPLE__) || defined(__linux__)
    (void)mounted_server_open_registry_for_creator();
    // This is a distinct no-context owner, never a receiver/admin fallback.
    if(config.ordinary_context||config.managed_product_credentials||config.is_in_memory()||config.read_only||
       config.is_sync_enabled()||config.is_ipc_enabled()||config.audit_retention_seconds!=0||
       config.migration_block||config.row_migration_fn_||!config.migration_schemas_.empty()||
       config.ordinary_policy!=ordinary_sync_policy::legacy_provisional||!config.ordinary_model_profile.empty()||
       !config.recovery_source_expectation.empty()||config.target_schema_version<=0||
       config.busy_timeout_ms<0||config.busy_timeout_ms>30000)
        throw db_error("checked mounted server quiescent existing configuration required");
    if(config.path.empty()||config.path.size()>4096||config.path.find('\0')!=std::string::npos||
       config.path.starts_with("file:")||!std::filesystem::path(config.path).is_absolute())
        throw db_error("checked mounted server bounded existing file required");
    const auto requested=std::filesystem::path(config.path);const auto name=requested.filename().string();
    if(name.empty()||name=="."||name=="..")throw db_error("checked mounted server regular main required");
    std::error_code error;const auto parent=std::filesystem::canonical(requested.parent_path(),error);
    if(error)throw db_error("checked mounted server existing parent unavailable");
    const auto physical_path=parent/name;struct stat physical{};
    if(::lstat(physical_path.c_str(),&physical)!=0||!S_ISREG(physical.st_mode)||physical.st_nlink!=1)
        throw db_error("checked mounted server no-follow existing regular main required");
    auto expected=std::make_shared<physical_store_identity>();
    expected->device=physical.st_dev;expected->inode=physical.st_ino;expected->filename=physical_path.string();
    auto retirement=std::make_shared<detail::database_retirement_state>();
    mounted_server_open_attempt attempt(*expected,retirement);
    auto catalog=recovery_catalog(config,schemas);
    if(!catalog.valid()||catalog.models.empty()||catalog.swift_models.empty()||catalog.swift_fingerprint.empty())
        throw db_error("checked mounted server full native and Swift declarations required");
    // Catalog identity validates every real descriptor/constraint before opening.
    (void)managed_catalog_identity(catalog);
    std::shared_ptr<database> writer;std::shared_ptr<swift_lattice> owner;
    try {
        attempt.will_open_native();
        writer=std::make_shared<database>(config.path,database::open_mode::read_write,config.busy_timeout_ms,
            std::shared_ptr<database_read_control>{},database::initialization_key(expected,retirement,
                database::initialization_key::mounted_server_key{}),detail::ordinary_context{},config.wal_extent_limit_bytes);
        owner=std::shared_ptr<swift_lattice>(new swift_lattice(config,schemas,std::move(catalog),writer,
            lattice_db::checked_mounted_server_existing_key{}));
        detail::ordinary_export_access::validate_checked_mounted_server_schema(*owner,*expected);
        owner->publish_checked_existing_facilities();
        detail::ordinary_export_access::validate_checked_mounted_server_schema(*owner,*expected);
        // Pointer resolution only; no reuse of a generic cached/bootstrap owner.
        detail::LatticeCache::instance().register_pointer(owner);
        attempt.published();
        return owner; // shared_ptr move is nonthrowing; FRT shell already allocated
    }catch(...){
        const auto primary=std::current_exception();std::exception_ptr cleanup;
        if(owner){
            const auto closed=owner->close_checked();
            cleanup=closed.cleanup_error?closed.cleanup_error:closed.error;
            if(!closed.cleanup_complete||closed.cleanup_error)attempt.observed_owner_cleanup_unproved();
        }
        owner.reset();
        if(writer){
            try{writer->close();}catch(...){if(!cleanup)cleanup=std::current_exception();}
            writer->retire_connection_();
        }
        if(!cleanup&&retirement->state()!=detail::database_retirement_state::phase::closed)
            cleanup=std::make_exception_ptr(db_error("checked mounted server native close unproved"));
        // Failure fence disposition occurs AFTER all actual native retire attempts.
        // A poisoned physical record survives this error's sealed string boundary.
        throw detail::ordinary_checked_open_error(primary,cleanup,{},retirement);
    }
#else
    (void)config;(void)schemas;throw db_error("checked mounted server open platform unsupported");
#endif
}

namespace {
// Schema-bearing read-only opens must enforce the same upper version bound
// without entering ensure_swift_tables, migration, DDL or background repair.
void require_read_only_swift_version(database& connection,int target_version) {
    const auto rows=connection.query("SELECT value FROM _lattice_meta WHERE key = 'schema_version'");
    int current_version=1; // Same explicit legacy missing-key default as get_schema_version().
    if(!rows.empty()) {
        if(rows.size()!=1)throw db_error("read-only schema version metadata is not singular");
        const auto& value=std::get<std::string>(rows.front().at("value"));
        if(value.empty()||value.find_first_not_of("0123456789")!=std::string::npos)
            throw db_error("read-only schema version metadata is invalid");
        // Query/type/range failures propagate; none become a permissive default.
        current_version=std::stoi(value);
        if(current_version<=0)throw db_error("read-only schema version metadata is invalid");
    }
    if(current_version>target_version)
        throw std::runtime_error("Database schema version ("+std::to_string(current_version)+
            ") is newer than this binary supports ("+std::to_string(target_version)+
            "). Update the application.");
}
}

swift_lattice::swift_lattice(const swift_configuration& config, const SchemaVector& schemas)
    :swift_lattice(config,schemas,normal_factory_key(nullptr)) {}

swift_lattice::swift_lattice(const swift_configuration& config,const SchemaVector& schemas,
    normal_factory_key key)
    : lattice_db(config, /*defer_sync=*/true,recovery_catalog(config,schemas),{}, {}, /*defer_delivery_publication=*/true,key.original_), swift_resources_(config) {
    bind_pending_model_registry_checker_();
    LOG_DEBUG("swift_lattice", "ctor start path=%s schemas=%zu read_only=%d", config.path.c_str(), schemas.size(), config.read_only);
    if (!config.read_only) {
        LOG_DEBUG("swift_lattice", "ensure_swift_tables");
        ensure_swift_tables(schemas,!delivery_startup_);
        LOG_DEBUG("swift_lattice", "ensure_swift_tables done");
        if(delivery_startup_) {
            finish_observer_delivery_startup_();
            publish_observer_delivery_facilities_();
            dispatch_vec0_reconcile(schemas);
            return;
        }
        // Sync setup after all tables exist (Swift tables created by ensure_swift_tables)
        lattice_db::setup_sync_if_configured();
        lattice_db::setup_ipc_if_configured();
    } else {
        // The actual pending owner cannot issue public metadata SQL. Its
        // mandatory finish_open_ proves the exact captured current version,
        // catalog and marker under its private READ before any publication.
        if(!delivery_startup_&&!schemas.empty())require_read_only_swift_version(db(),swift_config_.target_schema_version);
        // Retain all descriptor/constraint/union and cascade families without DDL.
        populate_swift_in_memory_state(schemas);
        if(delivery_startup_) {
            finish_observer_delivery_startup_();
            publish_observer_delivery_facilities_();
        }
    }
    LOG_DEBUG("swift_lattice", "ctor done");
}

swift_lattice::swift_lattice(swift_configuration&& config, const SchemaVector& schemas)
    : lattice_db(config, /*defer_sync=*/true,recovery_catalog(config,schemas),{}, {}, /*defer_delivery_publication=*/true), swift_resources_(std::move(config)) {
    bind_pending_model_registry_checker_();
    if (!swift_config_.read_only) {
        ensure_swift_tables(schemas,!delivery_startup_);
        if(delivery_startup_) {
            finish_observer_delivery_startup_();
            publish_observer_delivery_facilities_();
            dispatch_vec0_reconcile(schemas);
            return;
        }
        lattice_db::setup_sync_if_configured();
        lattice_db::setup_ipc_if_configured();
    } else {
        // The actual pending owner cannot issue public metadata SQL. Its
        // mandatory finish_open_ proves the exact captured current version,
        // catalog and marker under its private READ before any publication.
        if(!delivery_startup_&&!schemas.empty())require_read_only_swift_version(db(),swift_config_.target_schema_version);
        // Retain all descriptor/constraint/union and cascade families without DDL.
        populate_swift_in_memory_state(schemas);
        if(delivery_startup_) {
            finish_observer_delivery_startup_();
            publish_observer_delivery_facilities_();
        }
    }
}

// Private unpublished derived construction. Enrollment and every publication
// occur in the common retained-owner factory after this constructor returns.
swift_lattice::swift_lattice(const swift_configuration& config,const SchemaVector& schemas,
    const std::shared_ptr<detail::recovery_continuous_admission>& admission)
    :lattice_db(config,true,recovery_catalog(config,schemas),admission),swift_resources_(config) {
    bind_pending_model_registry_checker_();
    ensure_swift_tables(schemas,false);
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

// One finite, pre-effect binding journal. Its strong graph census owns every
// cloned container and every target until AFTER Native/store leaves and all
// adoption steps. SQL rollback never repairs user objects by allocating copies.
struct swift_lattice::graph_mutation_plan {
    struct node {
        dynamic_object* original = nullptr; // caller root or retained child
        std::shared_ptr<dynamic_object> retained_original;
        std::shared_ptr<dynamic_object> staged;
    };
    struct list_copy { std::shared_ptr<link_list> original, staged; };
    struct union_copy { std::shared_ptr<union_value> original, staged; };
    std::shared_ptr<swift_lattice> owner;
    graph_objects root_holds, roots;
    std::vector<node> nodes;
    std::vector<list_copy> lists;
    std::vector<union_copy> unions;
    std::unordered_map<const dynamic_object*, size_t> seen;
    std::unordered_map<const link_list*, size_t> seen_lists;
    std::unordered_map<const union_value*, size_t> seen_unions;
    // Closed INSERT outputs are allocated/materialized only after the input
    // plan admitted their original. The graph journal retains both sides of
    // every final swap until Native/store leaves and delivery have ended.
    std::vector<managed<swift_dynamic_object>> closed_outputs;
    const uint64_t creator = static_cast<uint64_t>(::getpid());
    bool adopted = false;

    graph_mutation_plan()=default;
    explicit graph_mutation_plan(std::shared_ptr<swift_lattice> actual, graph_objects held)
        : owner(std::move(actual)), root_holds(std::move(held)) {}
    static bool same_owner(const std::shared_ptr<swift_lattice>& a,
                           const std::shared_ptr<swift_lattice>& b) noexcept {
        return a && b && a.get() == b.get() && !a.owner_before(b) && !b.owner_before(a);
    }
    std::shared_ptr<dynamic_object> clone(dynamic_object* original,
                                          std::shared_ptr<dynamic_object> held = {}) {
        if (!original || original->deleted_) throw db_error("graph mutation requires a live object");
        if (original->lattice) {
            if (!same_owner(original->lattice, owner))
                throw db_error("graph mutation requires the exact physical owner");
            if (!held) throw db_error("managed graph target requires retained ownership");
            root_holds.push_back(held);
            return held;
        }
        if (auto found = seen.find(original); found != seen.end()) {
            auto& prior = nodes[found->second];
            if (!prior.retained_original && held) prior.retained_original = std::move(held);
            return prior.staged;
        }
        auto staged = std::make_shared<dynamic_object>(original->unmanaged_);
        const auto index = nodes.size();
        nodes.push_back({original, std::move(held), staged});
        seen.emplace(original, index); // publish BEFORE recursion; cycles/shared nodes have one row
        for (auto& [name, target] : staged->unmanaged_.link_values) {
            if (!target) throw db_error("graph mutation contains an empty link");
            target = clone(target.get(), target);
        }
        for (auto& [name, list] : staged->unmanaged_.list_values) {
            if (!list || list->lattice) throw db_error("graph mutation requires an unmanaged list value");
            auto found = seen_lists.find(list.get());
            if (found != seen_lists.end()) { list = lists[found->second].staged; continue; }
            auto original_list = list;
            auto copied = std::make_shared<link_list>(*list);
            seen_lists.emplace(list.get(), lists.size()); lists.push_back({original_list, copied});
            list = copied;
            for (auto& target : copied->unmanaged_) {
                if (!target) throw db_error("graph mutation contains an empty list target");
                target = clone(target.get(), target);
            }
        }
        for (auto& [name, value] : staged->unmanaged_.union_values) {
            if (!value) continue;
            auto found = seen_unions.find(value.get());
            if (found != seen_unions.end()) { value = unions[found->second].staged; continue; }
            auto original_value = value;
            auto copied = std::make_shared<union_value>(*value);
            seen_unions.emplace(value.get(), unions.size()); unions.push_back({original_value, copied});
            value = copied;
            for (auto& [key, target] : copied->link_refs()) {
                if (!target) throw db_error("graph mutation contains an empty union target");
                target = clone(target.get(), target);
            }
        }
        return staged;
    }
    static bool same_property(const property_descriptor& a, const property_descriptor& b) noexcept {
        if (a.name != b.name || a.type != b.type || a.kind != b.kind || a.target_table != b.target_table ||
            a.link_table != b.link_table || a.column_name != b.column_name || a.nullable != b.nullable ||
            a.is_vector != b.is_vector || a.is_geo_bounds != b.is_geo_bounds || a.is_full_text != b.is_full_text ||
            a.is_indexed != b.is_indexed || a.is_unique != b.is_unique || a.is_union != b.is_union ||
            a.no_history != b.no_history || a.union_desc.union_table_name != b.union_desc.union_table_name ||
            a.union_desc.cases.size() != b.union_desc.cases.size()) return false;
        for (size_t i = 0; i != a.union_desc.cases.size(); ++i) {
            const auto& x = a.union_desc.cases[i]; const auto& y = b.union_desc.cases[i];
            if (x.case_name != y.case_name || x.values.size() != y.values.size()) return false;
            for (size_t j = 0; j != x.values.size(); ++j) {
                const auto& u = x.values[j]; const auto& v = y.values[j];
                if (u.param_name != v.param_name || u.type != v.type || u.is_link != v.is_link ||
                    u.link_target != v.link_target) return false;
            }
        }
        return true;
    }
    static bool same_schema(const swift_dynamic_object& a, const swift_dynamic_object& b) noexcept {
        if (a.table_name != b.table_name || a.properties.size() != b.properties.size()) return false;
        for (const auto& [name, property] : a.properties) {
            const auto found = b.properties.find(name);
            if (found == b.properties.end() || !same_property(property, found->second)) return false;
        }
        return true;
    }
    // All object rows precede relationship rows, so cyclic graphs are finite
    // and a link can never recursively publish an original binding mid-unit.
    void insert_rows(const std::string& preserved_global_id) {
        for (size_t index = 0; index != nodes.size();) {
            auto& first = *nodes[index].staged;
            auto upsert = owner->get_upsert_columns(first.unmanaged_.table_name);
            owner->materialize_link_shadow_conflict_cols(first.unmanaged_, upsert);
            auto schema = first.unmanaged_.instance_schema();
            if (index == 0 && !preserved_global_id.empty()) {
                owner->materialize_link_shadow_conflict_cols(first.unmanaged_, upsert);
                auto result = owner->lattice_db::add(first.unmanaged_, schema, upsert, preserved_global_id);
                first.unmanaged_.~swift_dynamic_object();
                new (&first.managed_) managed<swift_dynamic_object>(std::move(result));
                first.lattice = owner; ++index; continue;
            }
            size_t end = index + 1;
            // Dynamic schema-bearing adds remain supported. A different full
            // descriptor starts another group, never silently uses root zero.
            while (end != nodes.size()) {
                auto& candidate = nodes[end].staged->unmanaged_;
                owner->materialize_link_shadow_conflict_cols(candidate, owner->get_upsert_columns(candidate.table_name));
                if (!same_schema(first.unmanaged_, candidate)) break;
                ++end;
            }
            std::vector<swift_dynamic_object> values; values.reserve(end - index);
            for (size_t at = index; at != end; ++at) values.push_back(nodes[at].staged->unmanaged_);
            auto results = owner->lattice_db::add_bulk_with_schema(std::move(values), schema, upsert);
            if (results.size() != end - index) throw db_error("graph bulk result count differs");
            for (size_t at = index; at != end; ++at) {
                auto& staged = *nodes[at].staged;
                staged.unmanaged_.~swift_dynamic_object();
                new (&staged.managed_) managed<swift_dynamic_object>(std::move(results[at-index]));
                staged.lattice = owner;
            }
            index = end;
        }
        for (auto& n : nodes) {
            auto& source = n.staged->managed_.source;
            auto& result = n.staged->managed_;
            for (auto& [name, target] : source.link_values) {
                auto field = result.get_managed_field<swift_dynamic_object*>(name);
                field = target->managed_.as_link();
            }
            for (auto& [name, list] : source.list_values) {
                auto field = result.get_managed_field<std::vector<swift_dynamic_object*>>(name);
                for (const auto& target : list->unmanaged_) field.push_back(&target->managed_);
            }
            for (auto& [name, bounds] : source.geo_bounds_lists) {
                auto field = result.get_managed_field<std::vector<geo_bounds>>(name);
                for (const auto& value : bounds) field.push_back(value);
            }
            owner->persist_union_values(source, result.table_name_, result.id());
        }
    }
    std::shared_ptr<dynamic_object> original_target(const std::shared_ptr<dynamic_object>& staged) const noexcept {
        // Existing managed targets were retained unchanged. Cloned targets have
        // a real retained original (every graph edge owns one).
        for (const auto& n : nodes) if (n.staged == staged) return n.retained_original;
        return staged;
    }
    void prepare_adoption() {
        // Rewrite only private clones. Every lookup and storage already exists;
        // fail BEFORE RELEASE/COMMIT if an impossible alias has no root owner.
        for (auto& n : nodes) {
            auto& source = n.staged->managed_.source;
            for (auto& [key, target] : source.link_values) {
                auto original = original_target(target);
                if (!original) throw db_error("graph root edge requires retained ownership");
                target = std::move(original);
            }
            for (auto& [key, list] : source.list_values)
                for (const auto& pair : lists) if (pair.staged == list) { list = pair.original; break; }
            for (auto& [key, value] : source.union_values)
                for (const auto& pair : unions) if (pair.staged == value) { value = pair.original; break; }
        }
        for (auto& pair : unions) for (auto& [key, target] : pair.staged->link_refs()) {
            auto original = original_target(target);
            if (!original) throw db_error("graph union root edge requires retained ownership");
            target = std::move(original);
        }
    }
    static uint64_t closed_output_workspace(
        const detail::observer_delivery_swift_input::input& input,
        const detail::observer_delivery_write& write) {
        using detail::observer_delivery_value_detail::add_structured_bytes;
        using detail::observer_delivery_value_detail::add_structured_product;
        using values=detail::observer_delivery_values;
        if(write.row_count()!=input.size())throw db_error("delivery Swift row count differs");
        uint64_t bytes=sizeof(graph_mutation_plan);
        add_structured_product(bytes,input.size(),sizeof(managed<swift_dynamic_object>));
        if(input.has_originals())add_structured_product(bytes,input.size(),sizeof(node)+sizeof(dynamic_object));
        values::field_buffer scratch;
        for(size_t index=0;index<input.size();++index) {
            const auto& source=input.source(index);
            const auto& schema=write.schema(index); // exact retained immutable postimage
            if(schema.properties.size()!=source.properties.size())
                throw db_error("delivery Swift INSERT requires its exact admitted schema");
            // Compare without copying. All later counts/copies use this SAME
            // retained catalog, selected by admitted row.table, never a later
            // caller table or mutable owner registry lookup.
            for(const auto& property:schema.properties) {
                const auto actual=source.properties.find(property.name);
                if(actual==source.properties.end()||!same_property(property,actual->second))
                    throw db_error("delivery Swift input descriptor differs from its admitted schema");
                if(property.union_desc.cases.size()>values::column_ceiling)
                    throw db_error("delivery Swift descriptor case bound");
                uint64_t descriptor=sizeof(std::pair<const std::string,property_descriptor>);
                add_structured_bytes(descriptor,property.name.size()+1); // map key
                add_structured_bytes(descriptor,property.name.size()+1);
                add_structured_bytes(descriptor,property.target_table.size()+1);
                add_structured_bytes(descriptor,property.link_table.size()+1);
                add_structured_bytes(descriptor,property.column_name.size()+1);
                add_structured_bytes(descriptor,property.union_desc.union_table_name.size()+1);
                for(const auto& branch:property.union_desc.cases) {
                    if(branch.values.size()>values::column_ceiling)
                        throw db_error("delivery Swift descriptor parameter bound");
                    add_structured_bytes(descriptor,sizeof(branch)+branch.case_name.size()+1);
                    for(const auto& parameter:branch.values)
                        add_structured_bytes(descriptor,sizeof(parameter)+parameter.param_name.size()+1+parameter.link_target.size()+1);
                }
                // source.properties and properties_; actual entries are capped
                // separately. These are structured bytes, not allocator/RSS.
                add_structured_product(bytes,2,descriptor);
            }
            const auto row=write.row(index,scratch);
            add_structured_product(bytes,2,row.table.size()+1);
            add_structured_bytes(bytes,write.global_id(index).size()+1);
            add_structured_bytes(bytes,detail::observer_delivery_identity::allocation_bytes());
            add_structured_bytes(bytes,row.table.size()+1);
            add_structured_bytes(bytes,write.global_id(index).size()+1);
            for(const auto& field:row.fields) {
                uint64_t value_bytes=sizeof(std::pair<const std::string,column_value_t>)+field.name.size()+1;
                if(const auto* text=std::get_if<std::string_view>(&field.contents))
                    add_structured_bytes(value_bytes,text->size()+1);
                if(const auto* blob=std::get_if<std::span<const uint8_t>>(&field.contents))
                    add_structured_bytes(value_bytes,blob->size());
                // Immutable source cache and model_base value cache.
                add_structured_product(bytes,2,value_bytes);
            }
        }
        return bytes;
    }
    void materialize_closed(const detail::observer_delivery_swift_input::input& input,
                            const detail::observer_delivery_write& write) {
        using values=detail::observer_delivery_values;
        static_assert(std::is_nothrow_move_constructible_v<managed<swift_dynamic_object>>);
        closed_outputs.reserve(write.row_count());
        if(input.has_originals())nodes.reserve(write.row_count());
        owner->require_creator_process();
        values::field_buffer scratch;
        for(size_t index=0;index<write.row_count();++index) {
            auto* original=input.original(index);
            if(input.has_originals())for(const auto& prior:nodes)
                if(prior.original==original)throw db_error("graph roots must identify distinct objects");
            closed_outputs.emplace_back();
            auto& output=closed_outputs.back();
            const auto row=write.row(index,scratch);
            output.source.table_name=std::string(row.table);
            const auto& schema=write.schema(index); // retained through actual write ending
            output.source.properties.reserve(schema.properties.size());
            output.properties_.reserve(schema.properties.size());
            owner->require_creator_process();
            for(const auto& property:schema.properties) {
                output.source.properties.emplace(property.name,property);
                owner->require_creator_process();
                output.properties_.emplace(property.name,property);
                owner->require_creator_process();
            }
            output.source.values.reserve(row.fields.size());
            output.unmanaged_values_.reserve(row.fields.size());
            owner->require_creator_process();
            // Only builtin admitted representations are copied. No source
            // value, collector, graph clone or JSON encoder is invoked here.
            for(const auto& field:row.fields) {
                const auto own=[](const values::value_view& borrowed)->column_value_t {
                    return std::visit([](const auto& value)->column_value_t {
                    using T=std::decay_t<decltype(value)>;
                    if constexpr(std::is_same_v<T,std::string_view>)return std::string(value);
                    else if constexpr(std::is_same_v<T,std::span<const uint8_t>>)
                        return std::vector<uint8_t>(value.begin(),value.end());
                    else return value;
                    },borrowed);
                };
                output.source.values.emplace(std::string(field.name),own(field.contents));
                owner->require_creator_process();
                output.unmanaged_values_.emplace(std::string(field.name),own(field.contents));
                owner->require_creator_process();
            }
            output.db_=&owner->db();output.lattice_=owner.get();
            output.table_name_=std::string(row.table);
            output.global_id_=std::string(write.global_id(index));
            owner->require_creator_process();
            owner->prepare_observer_delivery_identity_(output,row.table,write.global_id(index));
            if(input.has_originals()) {
                nodes.emplace_back();nodes.back().original=original;
                nodes.back().retained_original=input.retained(index);
                owner->require_creator_process();
                nodes.back().staged=std::make_shared<dynamic_object>();
                owner->require_creator_process();
                auto& staged=*nodes.back().staged;
                staged.unmanaged_.~swift_dynamic_object();
                new (&staged.managed_) managed<swift_dynamic_object>(std::move(output));
                staged.lattice=owner;
            }
        }
    }
    void adopt_closed(std::span<const detail::observer_delivery_write::inserted_row> inserted) noexcept {
        // The closed executor validates exact count and actual positive rowids
        // INSIDE the operation before its successful RELEASE/COMMIT. This tail
        // performs only fixed assignments/swaps and cannot report a late error.
        if(creator!=static_cast<uint64_t>(::getpid()))return;
        for(size_t index=0;index<inserted.size();++index) {
            if(nodes.empty()) {
                closed_outputs[index].id_=inserted[index].rowid;
                owner->publish_observer_delivery_identity_(closed_outputs[index],inserted[index].rowid);
            } else {
                auto& managed=nodes[index].staged->managed_;
                managed.id_=inserted[index].rowid;
                owner->publish_observer_delivery_identity_(managed,inserted[index].rowid);
            }
        }
        if(!nodes.empty())adopt();
    }
    void adopt() noexcept {
        // No allocation, SQL, callback or final graph release. This runs only
        // after actual SQL success and after every Native/store leaf ended.
        for (auto& pair : unions) pair.original->swap_graph_value(*pair.staged);
        for (auto& n : nodes) n.original->swap_graph_state(*n.staged);
        adopted = true;
    }
    bool prepare_native_target_destruction(pid_t actual_creator) noexcept {
        const auto current = [&] { return static_cast<uint64_t>(actual_creator) == creator &&
            actual_creator == ::getpid(); };
        if (!current()) return false;
        // Break only privately-owned cycles before dropping any census hold.
        // All final Native/model owners outlive this first loop.
        for (auto& pair : lists) { pair.staged->unmanaged_.clear(); if (!current()) return false; }
        for (auto& pair : unions) { pair.staged->link_refs().clear(); if (!current()) return false; }
        for (auto& n : nodes) {
            if(!n.staged)continue; // preallocated closed node may fail before staging
            auto& source = n.staged->lattice ? n.staged->managed_.source : n.staged->unmanaged_;
            source.link_values.clear(); source.list_values.clear(); source.union_values.clear();
            if (!current()) return false;
        }
        for (auto& root : roots) { root.reset(); if (!current()) return false; }
        for (auto& pair : lists) {
            pair.staged.reset(); if (!current()) return false;
            pair.original.reset(); if (!current()) return false;
        }
        for (auto& pair : unions) {
            pair.staged.reset(); if (!current()) return false;
            pair.original.reset(); if (!current()) return false;
        }
        for (auto& n : nodes) {
            n.staged.reset(); if (!current()) return false;
            n.retained_original.reset(); if (!current()) return false;
        }
        for (auto& hold : root_holds) { hold.reset(); if (!current()) return false; }
        owner.reset(); // last actual Native owner, no graph/target remains in automatic storage
        return current();
    }

};

void swift_lattice::add_observer_delivery(const detail::observer_delivery_swift_input::input& input) {
    require_creator_process();
    if(!input.size())return;
    auto write=detail::observer_delivery_write::prepare(*this,detail::observer_delivery_swift_input::cursor(input));
    const auto workspace=graph_mutation_plan::closed_output_workspace(input,write);
    write.reserve_output_workspace(workspace);
    detail::process_bound_resource_owner<graph_mutation_plan> journal;
    require_creator_process();
    auto& plan=journal.get();plan.owner=swift_lattice_ref::shared_for_lattice(this);
    require_creator_process();
    if(!plan.owner||plan.owner.get()!=this)throw db_error("delivery Swift INSERT requires its retained owner");
    plan.materialize_closed(input,write);
    write.execute(*this,{&plan,[](void* raw,bool committed,
        std::span<const detail::observer_delivery_write::inserted_row> rows)noexcept {
        auto& plan=*static_cast<graph_mutation_plan*>(raw);
        if(committed)plan.adopt_closed(rows);
    }});
    require_creator_process();
}
std::vector<managed<swift_dynamic_object>> swift_lattice::add_observer_delivery_values(
    const detail::observer_delivery_swift_input::input& input) {
    require_creator_process();
    if(!input.size())return {};
    auto write=detail::observer_delivery_write::prepare(*this,detail::observer_delivery_swift_input::cursor(input));
    const auto workspace=graph_mutation_plan::closed_output_workspace(input,write);
    write.reserve_output_workspace(workspace);
    detail::process_bound_resource_owner<graph_mutation_plan> journal;
    require_creator_process();
    auto& plan=journal.get();plan.owner=swift_lattice_ref::shared_for_lattice(this);
    require_creator_process();
    if(!plan.owner||plan.owner.get()!=this)throw db_error("delivery Swift values require their retained owner");
    plan.materialize_closed(input,write);
    write.execute(*this,{&plan,[](void* raw,bool committed,
        std::span<const detail::observer_delivery_write::inserted_row> rows)noexcept {
        if(committed)static_cast<graph_mutation_plan*>(raw)->adopt_closed(rows);
    }});
    require_creator_process();
    return std::move(plan.closed_outputs);
}

void swift_lattice::mutate_graph(const std::vector<dynamic_object*>& originals,
    graph_objects retained, const std::function<void(const graph_objects&)>& finish_body,
    const std::function<void()>& publish, const std::string& preserved_global_id) {
    require_creator_process();
    if(detail::observer_delivery_enabled(*this))
        throw db_error("delivery graph mutation requires its closed relationship operator");
    auto actual_owner = swift_lattice_ref::shared_for_lattice(this);
    if (!actual_owner || actual_owner.get() != this) throw db_error("graph mutation requires its retained owner");
    // Owner and journal exist BEFORE the operation's SQL admission/Native leaf.
    // No library-owned public value carries this private lifetime after return.
    detail::process_bound_resource_owner<graph_mutation_plan> journal(std::move(actual_owner), std::move(retained));
    auto& plan = journal.get();
    std::unordered_set<const dynamic_object*> root_census;
    for (auto* original : originals) {
        if (!root_census.insert(original).second) throw db_error("graph roots must identify distinct objects");
        std::shared_ptr<dynamic_object> held;
        for (const auto& candidate : plan.root_holds) if (candidate.get() == original) { held = candidate; break; }
        if (!original || original->lattice) throw db_error("cannot add an already managed or empty graph root");
        plan.roots.push_back(plan.clone(original, std::move(held)));
    }
    if (detail::mutation_operation_test_hooks::after_graph_prepared)
        detail::mutation_operation_test_hooks::after_graph_prepared(*this);
    require_creator_process();
    with_mutation_operation([&](database&) {
        plan.insert_rows(preserved_global_id);
        if (finish_body) finish_body(plan.roots);
        plan.prepare_adoption();
    }, [&](bool committed) noexcept {
        if (committed) { plan.adopt(); if (publish) publish(); }
    });
}

namespace {
// Creator-only heap custody for the existing managed operand's pre-call state.
// It is distinct from graph adoption, and is retained through detached delivery.
struct graph_managed_operand {
    std::shared_ptr<swift_lattice> owner;
    std::shared_ptr<dynamic_object> object;
    std::optional<managed<std::vector<swift_dynamic_object*>>> list;
    std::vector<std::shared_ptr<managed<swift_dynamic_object>>> retired_list_cache;
    std::optional<union_value> value;
    explicit graph_managed_operand(std::shared_ptr<swift_lattice> actual) : owner(std::move(actual)) {}
    bool prepare_native_target_destruction(pid_t creator) noexcept {
        if (creator != ::getpid()) return false;
        object.reset(); if (creator != ::getpid()) return false;
        list.reset(); if (creator != ::getpid()) return false;
        for (auto& entry : retired_list_cache) { entry.reset(); if (creator != ::getpid()) return false; }
        value.reset(); if (creator != ::getpid()) return false;
        owner.reset(); return creator == ::getpid();
    }
};
}

void swift_lattice::mutate_link(dynamic_object& parent, const std::string& name,
                               const std::shared_ptr<dynamic_object>& target) {
    require_creator_process();
    if (!graph_mutation_plan::same_owner(parent.lattice, swift_lattice_ref::shared_for_lattice(this)))
        throw db_error("link mutation requires its exact parent owner");
    detail::process_bound_resource_owner<graph_managed_operand> operand(parent.lattice);
    auto& state = operand.get(); state.object = std::make_shared<dynamic_object>(parent);
    state.object->deleted_ = parent.deleted_; state.object->row_cache_enabled_ = parent.row_cache_enabled_;
    if (!target || target->deleted_) throw db_error("link mutation requires a live target");
    if (target->lattice && !graph_mutation_plan::same_owner(target->lattice, state.owner))
        throw db_error("link mutation cannot borrow a foreign owner");
    std::vector<dynamic_object*> roots;
    if (!target->lattice) roots.push_back(target.get());
    mutate_graph(roots, {target}, [&](const graph_objects& staged) {
        const auto& actual = target->lattice ? target : staged.at(0);
        auto field = state.object->managed_.get_managed_field<swift_dynamic_object*>(name);
        field = actual->managed_.as_link();
    }, [&]() noexcept { parent.swap_graph_state(*state.object); });
}

void swift_lattice::mutate_list(link_list& parent, const std::shared_ptr<dynamic_object>& target,
                               std::optional<size_t> replacement) {
    require_creator_process();
    auto actual_owner = swift_lattice_ref::shared_for_lattice(this);
    const auto identity = swift_lattice_ref::identity_for_owner(actual_owner);
    if (parent.lattice != this || !parent.owner_identity_.matches(identity))
        throw db_error("list mutation requires its captured parent owner");
    detail::process_bound_resource_owner<graph_managed_operand> operand(std::move(actual_owner));
    auto& state = operand.get(); state.list.emplace(parent.managed_);
    if (!target || target->deleted_) throw db_error("list mutation requires a live target");
    if (target->lattice && !graph_mutation_plan::same_owner(target->lattice, state.owner))
        throw db_error("list mutation cannot borrow a foreign owner");
    std::vector<dynamic_object*> roots;
    if (!target->lattice) roots.push_back(target.get());
    mutate_graph(roots, {target}, [&](const graph_objects& staged) {
        const auto& actual = target->lattice ? target : staged.at(0);
        if (replacement) {
            if (*replacement >= state.list->size()) throw db_error("list replacement index out of range");
            const auto& previous = static_cast<const managed<std::vector<swift_dynamic_object*>>&>(*state.list)[*replacement];
            // The actual ordinary link recipe is lhs/rhs (plus rhs_type for
            // virtual lists). Do not use the legacy generic replacement's
            // nonexistent parent_global_id/child_global_id columns.
            const auto& table = state.list->link_table_;
            if (state.list->is_virtual_) state.owner->db().execute(
                "UPDATE " + table + " SET rhs=?,rhs_type=? WHERE lhs=? COLLATE NOCASE AND rhs=? COLLATE NOCASE AND rhs_type=?",
                {actual->managed_.global_id(), actual->managed_.table_name_, state.list->parent_global_id_, previous.global_id(), previous.table_name_});
            else state.owner->db().execute(
                "UPDATE " + table + " SET rhs=? WHERE lhs=? COLLATE NOCASE AND rhs=? COLLATE NOCASE",
                {actual->managed_.global_id(), state.list->parent_global_id_, previous.global_id()});
        } else state.list->push_back(&actual->managed_);
        // No cloned child wrapper may escape in the list cache after its
        // original binding swap. Retire the real temporary cache off-leaf.
        state.list->cached_objects_.swap(state.retired_list_cache);
        state.list->loaded_ = false;
    }, [&]() noexcept {
        static_assert(std::is_nothrow_move_constructible_v<managed<std::vector<swift_dynamic_object*>>>);
        static_assert(std::is_nothrow_move_assignable_v<managed<std::vector<swift_dynamic_object*>>>);
        std::swap(parent.managed_, *state.list);
    });
}

void swift_lattice::mutate_list_remove(link_list& parent, std::optional<size_t> index) {
    require_creator_process();
    auto actual_owner = swift_lattice_ref::shared_for_lattice(this);
    const auto identity = swift_lattice_ref::identity_for_owner(actual_owner);
    if (parent.lattice != this || !parent.owner_identity_.matches(identity))
        throw db_error("list removal requires its captured parent owner");
    detail::process_bound_resource_owner<graph_managed_operand> operand(std::move(actual_owner));
    auto& state = operand.get(); state.list.emplace(parent.managed_);
    mutate_graph({}, {}, [&](const graph_objects&) {
        if (!index) state.list->clear();
        else if (*index < state.list->size()) {
            auto object = (*state.list)[*index];
            state.list->erase(&object.operator managed<swift_dynamic_object>&());
        }
        state.list->cached_objects_.swap(state.retired_list_cache);
        state.list->loaded_ = false;
    }, [&]() noexcept { std::swap(parent.managed_, *state.list); });
}

void swift_lattice::mutate_union(dynamic_object& parent, const std::string& name, const union_value& value) {
    require_creator_process();
    if (!graph_mutation_plan::same_owner(parent.lattice, swift_lattice_ref::shared_for_lattice(this)))
        throw db_error("union mutation requires its exact parent owner");
    detail::process_bound_resource_owner<graph_managed_operand> operand(parent.lattice);
    auto& state = operand.get(); state.object = std::make_shared<dynamic_object>(parent);
    state.object->deleted_ = parent.deleted_; state.object->row_cache_enabled_ = parent.row_cache_enabled_;
    state.value.emplace(value);
    graph_objects holds;
    std::vector<dynamic_object*> roots;
    std::vector<std::pair<std::string, size_t>> staged_fields;
    std::unordered_map<const dynamic_object*, size_t> root_indices;
    for (const auto& [key, target] : value.link_refs()) {
        if (!target || target->deleted_) throw db_error("union mutation requires a live target");
        holds.push_back(target);
        if (target->lattice) {
            if (!graph_mutation_plan::same_owner(target->lattice, state.owner))
                throw db_error("union mutation cannot borrow a foreign owner");
        } else {
            const auto [where, inserted] = root_indices.try_emplace(target.get(), roots.size());
            if (inserted) roots.push_back(target.get());
            staged_fields.emplace_back(key, where->second);
        }
    }
    mutate_graph(roots, std::move(holds), [&](const graph_objects& staged) {
        for (const auto& [key, index] : staged_fields) state.value->link_refs().at(key) = staged.at(index);
        for (const auto& [key, target] : state.value->link_refs())
            state.value->set_string(key, target->managed_.global_id());
        state.object->set_union_body(name, *state.value);
    }, [&]() noexcept { parent.swap_graph_state(*state.object); });
}

void swift_lattice::add(dynamic_object& object) {
    if(detail::observer_delivery_enabled(*this)) {
        dynamic_object* root=&object;
        add_observer_delivery(detail::observer_delivery_swift_input::objects({&root,1}));return;
    }
    mutate_graph({&object}, {});
}
void swift_lattice::add_preserving_global_id(dynamic_object& object, const std::string& gid) {
    mutate_graph({&object}, {}, {}, {}, gid);
}
void swift_lattice::add_bulk(DynamicObjectRefPtrVector& objects) {
    if(detail::observer_delivery_enabled(*this)) {
        add_observer_delivery(detail::observer_delivery_swift_input::refs(objects));return;
    }
    std::vector<dynamic_object*> roots; graph_objects holds;
    roots.reserve(objects.size()); holds.reserve(objects.size());
    for (auto& ref : objects) {
        const auto& actual = LATTICE_DEREF_REF(ref).impl_;
        if (!actual) throw db_error("bulk add requires a live object ref");
        roots.push_back(actual.get()); holds.push_back(actual);
    }
    if (!roots.empty()) mutate_graph(roots, std::move(holds));
}
void swift_lattice::add_bulk(std::vector<dynamic_object*>& objects) {
    if(detail::observer_delivery_enabled(*this)) {
        add_observer_delivery(detail::observer_delivery_swift_input::objects(objects));return;
    }
    if (!objects.empty()) mutate_graph(objects, {});
}


std::optional<managed<swift_dynamic_object>> swift_lattice::object(int64_t primary_key, const std::string& table_name) {
   auto result = lattice_db::find<swift_dynamic_object>(primary_key, table_name);
   if (result) {
       if (auto* props = get_properties_for_table(table_name)) {
           result->properties_ = copy_runtime_properties_(*props);
           result->source.properties = copy_runtime_properties_(*props);
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
        require_runtime_schema_ordinary_();
        const auto udesc = udesc_it->second.ordinary_copy();

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

void swift_lattice::ensure_swift_tables(const SchemaVector &schemas,bool publish_background)  {
    if(initialize_current_model_catalog_if_pending_(true)){populate_pending_swift_model_registries_();return;}
    if(recovery_producer_bootstrapped()) {
        // Base bootstrap has already compared complete declared schema and
        // exact enrolled programs in its retained view. Never enter ordinary
        // migration/trigger recreation for this admitted physical connection.
        if(schemas.empty()&&recovery_declarations().swift_digest.empty())return;
        const auto fp=compute_swift_fingerprint_key(schemas);
        if(fp!=recovery_declarations().swift_fingerprint)
            throw db_error("local producer Swift schema requires explicit migration");
        if(schema_initializing_(db())) {
            // The immutable marker/version getters retain the SAME actual
            // private read snapshot. fingerprint_marker_valid() would end its
            // own episode before version reads, so use its in-transaction body.
            auto snapshot=begin_schema_initialization_(db(),schema_startup_transaction::deferred);
            const bool current=fingerprint_marker_valid_in_txn(fp)&&
                get_schema_version()==swift_config_.target_schema_version;
            snapshot.commit();
            if(!current)throw db_error("local producer Swift schema requires explicit migration");
        }else if(!fingerprint_marker_valid(fp)||get_schema_version()!=swift_config_.target_schema_version)
            throw db_error("local producer Swift schema requires explicit migration");
        populate_swift_in_memory_state(schemas);
        return;
    }
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
        std::optional<database::initialization_transaction> startup_snapshot;
        // Preserve the private route decision even if issuing its BEGIN fails
        // before the optional acquires an episode. The helper revalidates the
        // actual pending open before any effect.
        const bool private_snapshot=schema_initializing_(db());
        try {
            if(private_snapshot)
                startup_snapshot.emplace(begin_schema_initialization_(db(),schema_startup_transaction::deferred));
            else db().execute("BEGIN");
            if (fingerprint_marker_valid_in_txn(fp_key)) {
                current_version = get_schema_version();
                if (current_version > target_version) {
                    db_newer = true;
                } else if (current_version == target_version) {
                    fast = true;
                }
            }
            if(startup_snapshot)startup_snapshot->commit();
            else db().execute("COMMIT");
        } catch (const db_error&) {
            // Preserve the first private failure. Its exact prepaid rollback
            // runs on unwind, including sticky failure/deadline exhaustion.
            if(private_snapshot)throw;
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
            if(publish_background)dispatch_vec0_reconcile(schemas);
            return;
        }
    }

    LOG_INFO("swift_lattice", "ensure_swift_tables: acquiring exclusive transaction");
    std::optional<lattice::transaction> legacy_transaction;
    std::optional<database::initialization_transaction> startup_transaction;
    if(schema_initializing_(db()))
        startup_transaction.emplace(begin_schema_initialization_(db(),schema_startup_transaction::exclusive));
    else legacy_transaction.emplace(this->db(), /*exclusive=*/true);
    const auto commit_schema=[&]{
        if(startup_transaction)startup_transaction->commit();
        else legacy_transaction->commit();
    };
    LOG_INFO("swift_lattice", "ensure_swift_tables: exclusive transaction acquired");

    // Double-checked: another process may have completed this exact ensure
    // pass between our probe above and acquiring the write lock.
    if (fingerprint_marker_valid_in_txn(fp_key) &&
        get_schema_version() == swift_config_.target_schema_version) {
        populate_swift_in_memory_state(schemas);
        commit_schema();
        LOG_INFO("swift_lattice", "ensure_swift_tables: fast path after lock (sibling completed)");
        if(publish_background)dispatch_vec0_reconcile(schemas);
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
        model_schema schema=detail::ordinary_schema_recipe::swift_physical_model(entry.table_name,entry.properties);
        all_schemas.push_back(std::move(schema));

        if (!schema_table_exists_(entry.table_name)) {
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
                    auto cols_map = schema_table_info_(entry.table_name);
                    const bool shadow_added = cols_map.find(shadow_col) == cols_map.end();
                    if (shadow_added) {
                        execute_model_schema_(detail::ordinary_schema_recipe::shadow_column(entry.table_name,shadow_col));
                    }

                    // 2) Shadow-maintenance triggers on the link table.
                    //    Not gated by sync_disabled() — shadow is derived
                    //    state, not user-owned data; safe to update during
                    //    sync-apply too (the original link INSERT audit is
                    //    what syncs; peers recompute their own shadow).
                    const std::string ins_trig =
                        "LinkShadow_" + link_table + "_Insert_" + shadow_col;
                    execute_model_schema_(detail::ordinary_schema_recipe::shadow_insert(entry.table_name,link_table,shadow_col,ins_trig));

                    const std::string del_trig =
                        "LinkShadow_" + link_table + "_Delete_" + shadow_col;
                    // Guard with `shadow = OLD.rhs` so a stale delete doesn't
                    // clobber a freshly re-assigned link.
                    execute_model_schema_(detail::ordinary_schema_recipe::shadow_delete(entry.table_name,link_table,shadow_col,del_trig));

                    // 3) Backfill from the current link-table state so rows
                    //    inserted before the constraint was added participate
                    //    in the unique index. Only when the shadow column was
                    //    just created — on revalidation passes the triggers
                    //    have been maintaining it, and this UPDATE scans the
                    //    whole parent table.
                    if (shadow_added) {
                        db().execute(detail::ordinary_schema_recipe::shadow_backfill(entry.table_name,link_table,shadow_col));
                    }

                    resolved_cols.push_back(shadow_col);
                } else {
                    resolved_cols.push_back(col);
                }
            }

            std::string col_list=detail::ordinary_schema_recipe::column_list(resolved_cols);
            const auto sql=detail::ordinary_schema_recipe::unique_index(entry.table_name,i,col_list);
            if(schema_initializing_(db())) {
                execute_model_schema_(sql);
            }else try {
                db().execute(sql);
            } catch (...) {
                // Duplicate data exists — deduplicate (keep newest row per group).
                LOG_WARN("swift_lattice", "Deduplicating %s for unique constraint on (%s)",
                         entry.table_name.c_str(), col_list.c_str());
                db().execute(
                    "DELETE FROM " + entry.table_name + " WHERE id NOT IN ("
                    "SELECT MAX(id) FROM " + entry.table_name + " GROUP BY " + col_list + ")");
                // Retry index creation
                db().execute(sql);
            }
        }
    }

    // Phase 8b: Create non-unique indexes for @Indexed properties
    for (const auto& schema : all_schemas) {
        for (const auto& prop : schema.properties) {
            if (!prop.is_indexed) continue;
            execute_model_schema_(detail::ordinary_schema_recipe::nonunique_index(schema,prop));
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
    commit_schema();
    if(publish_background)dispatch_vec0_reconcile(schemas);

    // TODO: Dispatch background IVF training for untrained vec0 tables.
    // Disabled pending investigation of IVF+int8 dimension mismatch.
    // vec0_training_future_ = std::async(std::launch::async, [this]() {
    //     train_untrained_vec0_tables();
    // });

    LOG_INFO("swift_lattice", "ensure_swift_tables done");
}

std::string swift_lattice::compute_swift_fingerprint_key(const SchemaVector& schemas) const {
    return compute_swift_fingerprint_key_for(swift_config_.target_schema_version,schemas);
}
std::string swift_lattice::compute_swift_fingerprint_key_for(int32_t target_version,const SchemaVector& schemas) {
    std::ostringstream out;
    out << "swift\n"
        << "epoch:" << kLatticeSchemaFormatEpoch << '\n'
        << "target_schema_version:" << target_version << '\n';
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
    require_creator_process();
    if(delivery_startup_&&schema_initializing_(db())){populate_pending_swift_model_registries_();return;}
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

namespace detail {
void require_swift_runtime_ordinary_copy_() {
    if(ordinary_initialization_storage_scope::debit_scope_active())
        throw ordinary_initialization_storage_error("pending Swift runtime cannot materialize an ordinary owning schema");
}
}
void swift_lattice::replace_swift_model_registry_data_(const detail::recovery_owner_schema& catalog,
    const detail::observer_delivery_open_startup* pending) {
    require_creator_process();
    if(!swift_resources_.in_creator_process()||&catalog!=&recovery_schemas_||!catalog.valid()||is_closed()||
       !detail::ordinary_initialization_storage_scope::debit_scope_active())
        throw db_error("Swift registry population requires its same live owner catalog and actual storage scope");
    const auto* actual_pending=db_?db_->delivery_startup_.get():nullptr;
    if(pending!=actual_pending)throw db_error("Swift registry population pending original differs");
    if(pending){
        if(!detail::ordinary_initialization_storage_scope::matches_debit_scope(pending,db_.get(),
            &detail::observer_delivery_open_startup::debit_initialization_storage_))
            throw db_error("Swift registry population lacks its exact original debit scope");
        pending->require_work_current_(*db_);if(&pending->final_catalog_(*db_)!=&catalog)
            throw db_error("Swift registry population catalog original differs");
    }
    const auto creator=detail::database_native_process();
    using custody=detail::native_retirement_storage<detail::swift_runtime_registry_stage>;
    detail::debit_core_model_registry_storage_(custody::allocation_bytes());custody held;
    auto& stage=held.get<0>();std::size_t properties=0;
    for(const auto& name:catalog.swift_order) {
        require_creator_process();if(pending)pending->require_work_current_(*db_);
        const auto model=catalog.models.find(name);const auto constraints=catalog.swift_constraints.find(name);
        if(!catalog.swift_models.count(name)||model==catalog.models.end()||model->second.table_name!=name||
           constraints==catalog.swift_constraints.end()||stage.schemas.find(name)!=stage.schemas.end()||
           model->second.properties.size()>detail::recovery_owner_schema::max_properties-properties)
            throw db_error("Swift registry population exact declaration corpus differs");
        properties+=model->second.properties.size();
        RuntimeSwiftSchema schema;schema.reserve(model->second.properties.size());
        for(const auto& property:model->second.properties) {
            if(!schema.emplace(detail::swift_runtime_text_from_(property.name),detail::swift_runtime_property_descriptor(property)).second)
                throw db_error("Swift registry population duplicates a property");
            if(property.is_union){
                auto inserted=stage.unions.emplace(detail::swift_runtime_text_from_(property.union_desc.union_table_name),
                    detail::swift_runtime_union_descriptor(property.union_desc));
                if(!inserted.second&&!pending_registry_union_equal_(inserted.first->second,property.union_desc))
                    throw db_error("Swift registry population union declarations differ");
            }
        }
        stage.schemas.emplace(detail::swift_runtime_text_from_(name),std::move(schema));
        stage.constraints.emplace(detail::swift_runtime_text_from_(name),RuntimeConstraintVector(constraints->second));
    }
    if(stage.schemas.size()!=catalog.swift_models.size()||catalog.swift_order.size()!=catalog.swift_models.size()||
       catalog.swift_constraints.size()!=catalog.swift_models.size())throw db_error("Swift registry population table corpus differs");
    require_creator_process();
    const auto swap=[&]()noexcept{schemas_.swap(stage.schemas);constraints_.swap(stage.constraints);union_schemas_.swap(stage.unions);};
    swap();
    try {
        require_pending_model_registries_(catalog);require_creator_process();
        if(pending){pending->require_work_current_(*db_);if(&pending->final_catalog_(*db_)!=&catalog)
            throw db_error("Swift registry population catalog changed");}
    }catch(...){if(creator==detail::database_native_process())swap();throw;}
    // No allocation/Native/callback in the swaps; displaced DATA ends through
    // creator-fenced custody. Denied real allocations never publish partial DATA.
}
void swift_lattice::populate_pending_swift_model_registries_() {
    require_creator_process();auto* open=db_?db_->delivery_startup_.get():nullptr;
    if(!delivery_startup_||!open)throw db_error("Swift registry population lacks its actual pending Open");
    const auto creator=detail::database_native_process();
    try {
        open->require_work_current_(*db_);const auto& catalog=open->final_catalog_(*db_);
        if(&catalog!=&recovery_schemas_)throw db_error("Swift registry population original catalog differs");
        detail::ordinary_initialization_storage_scope storage(open,db_.get(),
            &detail::observer_delivery_open_startup::debit_initialization_storage_);
        replace_swift_model_registry_data_(catalog,open);
    }catch(...){if(creator==detail::database_native_process())open->fail_initialization_();throw;}
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

namespace {
nlohmann::json ordinary_swift_schema_snapshot(int32_t version,const SchemaVector& schemas) {
    nlohmann::json root;
    root["format"] = 1;
    root["version"] = version;
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
    return root;
}
}

detail::recovery_owner_schema swift_lattice::recovery_catalog(const swift_configuration& config,const SchemaVector& schemas) {
    auto result=detail::recovery_owner_schema::capture_native();
    if(!result.valid()||schemas.size()>detail::recovery_owner_schema::max_models-result.models.size()) {
        result.refuse();return result;
    }
    if(schemas.empty())return result; // native-only bridge retains its original profile bytes
    // Check every nested count and field before copying or serializing it.
    // Retain full descriptors even for currently unsupported recovery kinds.
    for(const auto& entry:schemas) {
        if(result.models.count(entry.table_name)||entry.properties.size()>256||
           !result.admit_field(entry.table_name)||!result.admit_count(entry.constraints.size(),64,32)) {
            result.refuse();return result;
        }
        for(const auto& [name,p]:entry.properties)
            if(name!=p.name||!result.admit_field(name)||!result.admit_property(p)) {
                result.refuse();return result;
            }
        for(const auto& constraint:entry.constraints) {
            if(!result.admit_count(constraint.columns.size(),64,16))return result;
            for(const auto& name:constraint.columns)if(!result.admit_field(name))return result;
        }
        if(!result.admit_count(1,detail::recovery_owner_schema::max_models,sizeof(std::string))||
           !result.admit_field(entry.table_name)) {result.refuse();return result;}
        result.swift_order.push_back(entry.table_name); // Retain actual original order before later generation.
        model_schema model;model.table_name=entry.table_name;model.properties.reserve(entry.properties.size());
        for(const auto& [name,p]:entry.properties)model.properties.push_back(p);
        result.models.emplace(entry.table_name,std::move(model));result.swift_models.insert(entry.table_name);
        // Complete bounded preflight above precedes every constraint copy,
        // including an explicit empty vector for each declared Swift table.
        auto& constraints=result.swift_constraints[entry.table_name];constraints.reserve(entry.constraints.size());
        for(const auto& constraint:entry.constraints)
            constraints.push_back({std::vector<std::string>(constraint.columns.begin(),constraint.columns.end()),constraint.allows_upsert});
    }
    // Stable complete declaration digest includes constraints, unsupported
    // property flags, relationships and the target version. A schema cookie or
    // the ordinary noncryptographic fast-open fingerprint is not this identity.
    nlohmann::json root;root["format"]="swift-recovery-declarations-v1";root["version"]=config.target_schema_version;
    auto tables=nlohmann::json::object();
    for(const auto& entry:schemas) {
        auto props=nlohmann::json::object();
        for(const auto& [name,p]:entry.properties) {
            props[name]=descriptor_to_json(p);
            props[name]["no_history"]=p.no_history;
        }
        auto constraints=nlohmann::json::array();
        for(const auto& c:entry.constraints)constraints.push_back({{"columns",c.columns},{"allows_upsert",c.allows_upsert}});
        tables[entry.table_name]={{"properties",std::move(props)},{"constraints",std::move(constraints)}};
    }
    root["tables"]=std::move(tables);
    result.swift_digest=picosha2::hash256_hex_string(root.dump());
    result.swift_fingerprint=compute_swift_fingerprint_key_for(config.target_schema_version,schemas);
    auto snapshot=ordinary_swift_schema_snapshot(config.target_schema_version,schemas).dump();
    if(!result.admit_count(snapshot.size(),detail::recovery_owner_schema::max_bytes,1)) {result.refuse();return result;}
    result.swift_snapshot=std::move(snapshot); // Actual normal producer bytes, no current/SQL grant.
    return result;
}

void swift_lattice::store_swift_schema_snapshot(const SchemaVector& schemas) {
    const auto snapshot=ordinary_swift_schema_snapshot(swift_config_.target_schema_version,schemas);
    if(schema_initializing_(db())) {
        store_model_metadata_("lattice_swift_schema",snapshot.dump());
    }else try {
        db().execute(
            "INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES('lattice_swift_schema', ?)",
            { snapshot.dump() });
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
        with_vec0_maintenance("reconcile", [&] {
            if (!db().table_exists(vec_table)) {
                // Missing table + present vector data is the sync-only lifecycle:
                // the DB was CREATED empty (open's Phase 6b had no rows to infer
                // dims from), then hydrated by sync-apply — which never creates
                // vec0 or its triggers. Every later open takes the fast path, so
                // bailing here would leave the DB semantically unsearchable
                // forever. Create the index (dims from a sample row); the gap
                // fill below then backfills every row.
                auto sample = db().query(
                    "SELECT length(" + prop + ") as len FROM main." + table +
                    " WHERE " + prop + " IS NOT NULL AND length(" + prop + ") > 0 LIMIT 1");
                if (sample.empty()) return;
                auto len_it = sample[0].find("len");
                if (len_it == sample[0].end() ||
                    !std::holds_alternative<int64_t>(len_it->second)) return;
                int dims = static_cast<int>(std::get<int64_t>(len_it->second) / sizeof(float));
                if (dims <= 0) return;
                ensure_vec0_table(table, prop, dims);
            }
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
            notify_vec0_maintenance_test_hook("reconcile", "before-fill");
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
        });
    } catch (const std::exception& e) {
        LOG_WARN("swift_lattice", "vec0 reconcile failed for %s: %s", vec_table.c_str(), e.what());
    }
}

} // namespace lattice

void lattice::swift_lattice::add_bulk(std::vector<dynamic_object>& objects) {
    if(detail::observer_delivery_enabled(*this)) {
        add_observer_delivery(detail::observer_delivery_swift_input::objects(objects));return;
    }
    std::vector<dynamic_object*> roots; roots.reserve(objects.size());
    for (auto& object : objects) roots.push_back(&object);
    if (!roots.empty()) mutate_graph(roots, {});
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
    const lattice::detail::swift_runtime_map<lattice::detail::swift_runtime_union_descriptor>& union_schemas,
    const std::string& deleted_table,
    const std::string& gid)
{
    self.require_runtime_schema_ordinary_();
    for (const auto& [stored_table, stored_desc] : union_schemas) {
        const auto union_table=lattice::detail::swift_runtime_ordinary_text_(stored_table);
        const auto desc=stored_desc.ordinary_copy();
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
        // Capture only the source store's own schema before topology changes.
        // Core binds this immutable metadata to the attachment token under its
        // topology lock and erases it whenever that token is invalidated.
        require_runtime_schema_ordinary_();lattice.require_runtime_schema_ordinary_();
        auto metadata = std::make_shared<const attached_schema_map>(lattice.schemas_);
        attach_with_metadata(lattice, std::move(metadata));
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

void lattice::swift_lattice::update_sync_filter_for_channel(const std::string& channel,
                                                            const SyncFilterVector& filter) {
    lattice_db::update_sync_filter(channel,
                                   std::vector<sync_filter_entry>(filter.begin(), filter.end()));
}

void lattice::swift_lattice::clear_sync_filter_for_channel(const std::string& channel) {
    lattice_db::clear_sync_filter(channel);
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
