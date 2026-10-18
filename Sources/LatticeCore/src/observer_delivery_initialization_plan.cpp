#include "observer_delivery_initialization_plan.hpp"
#include "ordinary_schema_projection.hpp"
#include "ordinary_schema_validation.hpp"
#include "observer_delivery_schema.hpp"
#include "recovery_obligation_bootstrap_statement.hpp"
#include "database_initialization_budget.hpp"
#include "ordinary_initialization_storage.hpp"
#include <lattice/ordinary_schema_recipe.hpp>
#include <lattice/native_retirement_storage.hpp>
#include <lattice/db.hpp>
#include <algorithm>
#include <limits>
#include <optional>
#include <thread>

namespace lattice::detail {
namespace {
[[noreturn]] void plan_refuse(const char* message){throw db_error(message);}
template<class Names>bool plan_contains_name_(const Names& names,std::string_view wanted)noexcept {
    for(const auto& name:names)if(std::string_view(name)==wanted)return true;
    return false;
}
}
struct observer_delivery_initialization_plan::state final {
    const std::uint64_t creator;
    const void* const generation;
    ordinary_schema_projection projection;
    ordinary_schema_projection preimage_projection;
    ordinary_schema_projection final_projection; // exact normal full schema+meter, never an enrolled substitute
    ordinary_schema_projection static_sync_projection; // separate family, never widens bootstrap/metadata scope
    enum class model_kind {none,table,trigger,index,shadow,rtree,fts5,metadata_store,metadata_read,version,meter_table,meter_seed,meter_trigger,meter_marker};
    struct approved {
        observer_delivery_initialization_sql::recipe recipe;
        ordinary_initialization_text sql;
        model_kind model=model_kind::none;bool fresh_only=false;
        ordinary_initialization_text model_owner,model_object,model_value;
        ordinary_initialization_vector<ordinary_initialization_text> implicit_indexes,module_tables,module_indexes;
    };
    std::vector<approved,ordinary_initialization_allocator<approved>> entries;
    std::uint64_t stored_bytes=0;
    state(std::uint64_t issuer,const void* actual)noexcept:creator(issuer),generation(actual){}
};
struct observer_delivery_initialization_meter::state final {
    const std::uint64_t creator=database_native_process();
    const std::thread::id builder=std::this_thread::get_id();
    std::shared_ptr<const observer_delivery_initialization_plan> plan;
    std::shared_ptr<const observer_delivery_open_startup> open;
    std::shared_ptr<const database_initialization_episode> snapshot;
    std::shared_ptr<const observer_delivery_initialization_preimage> entry_proof;
    database* actual=nullptr;sqlite3* native=nullptr;const void* runtime=nullptr;
    std::uint64_t revision=0;
    ordinary_initialization_vector<std::size_t> operations;
    std::size_t position=0;
    bool active=false,failed=false;
    std::array<char,35> marker{};
    std::int64_t old_cookie=0;
    ordinary_initialization_text new_cookie;
};
observer_delivery_initialization_meter::observer_delivery_initialization_meter(std::uint64_t issuer,state* held)noexcept
    :creator_(issuer),state_(held){}
observer_delivery_initialization_meter::~observer_delivery_initialization_meter()noexcept {
    if(creator_==database_native_process())delete state_;
}
void observer_delivery_initialization_meter::destroy_(observer_delivery_initialization_meter* held)noexcept {
    if(held&&held->creator_==database_native_process())delete held;
}
observer_delivery_initialization_plan::observer_delivery_initialization_plan(std::uint64_t issuer,state* value)noexcept
    :creator_(issuer),state_(value){}
observer_delivery_initialization_plan::~observer_delivery_initialization_plan()noexcept {
    if(creator_==database_native_process()&&state_)delete state_;
}
void observer_delivery_initialization_plan::destroy(const observer_delivery_initialization_plan* value)noexcept {
    if(value&&value->creator_==database_native_process())delete value;
}
std::shared_ptr<const observer_delivery_initialization_plan>
observer_delivery_initialization_plan::build_(const observer_delivery_open_startup& open,
    database& actual) {
    try {
    open.require_initializing_(actual);
    ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
        &observer_delivery_open_startup::debit_initialization_storage_);
    const auto& catalog=open.initialization_catalog_(actual);
    if(!catalog.valid())plan_refuse("initialization immutable catalog unavailable");
    const auto creator=database_native_process();
    // The existing closed projection caps retained logical programs at16MiB;
    // reserve the complete requested payload BEFORE invoking its emitters.
    // A separate fixed headroom covers one bounded per-table program while it
    // is generated. This is structured storage/count, not allocator/RSS.
    constexpr std::uint64_t projection_limit=16*1024*1024;
    constexpr std::uint64_t alternate_limit=16*1024*1024;
    constexpr std::uint64_t program_limit=1024*1024;
    open.claim_workspace_(actual,sizeof(state)+sizeof(observer_delivery_initialization_plan)+
        2*projection_limit+alternate_limit+program_limit);
    ordinary_initialization_storage_scope::charge(native_retirement_storage<
        std::shared_ptr<const observer_delivery_initialization_plan>>::allocation_bytes());
    native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_plan>> result;
    if(creator!=database_native_process())plan_refuse("inherited initialization plan custody");
    auto* held=new state(creator,open.catalog_generation_());
    if(creator!=database_native_process())plan_refuse("inherited initialization plan allocation");
    try {
        // All prospective state is now only in creator-fenced heap. No new
        // Native owner or user callback is an automatic construction argument.
        if(!catalog.models.empty())held->projection=describe_ordinary_projection(catalog,{});
        if(creator!=database_native_process())plan_refuse("inherited initialization projection return");
        if(held->projection.logical_bytes>projection_limit)
            plan_refuse("initialization projection exceeded its existing bound");
        using recipe=observer_delivery_initialization_sql::recipe;
        std::uint64_t count=24+6*catalog.models.size()+3+3*(held->projection.tables.size()+1);
        // The exact finite meter table/guard corpus is included BEFORE reserve,
        // never grown after the approved-entry allocation has been admitted.
        for(const auto& [name,table]:held->projection.tables) {
            count+=1+table.shadow_columns.size()+table.audit_programs.size()+table.other_programs.size()+table.index_programs.size();
            if(table.role!=ordinary_projection_role::model)count+=2; // exact closed fresh internal metadata read/store
            for(const auto& bundle:table.alternate_audit_programs)count+=bundle.size();
        }
        for(const auto& derived:held->projection.derived)
            count+=(!derived.create_sql.empty()?1:0)+derived.programs.size();
        if(count>65536||count>alternate_limit/sizeof(state::approved))
            plan_refuse("initialization approved recipe count declaration");
        held->entries.reserve(size_t(count));
        if(creator!=database_native_process())plan_refuse("inherited recipe vector allocation");
        if(held->entries.capacity()>65536||held->entries.capacity()>alternate_limit/sizeof(state::approved))
            plan_refuse("initialization actual recipe vector capacity");
        held->stored_bytes=held->entries.capacity()*sizeof(state::approved);
        const auto add=[&](recipe id,std::string_view sql) {
            if(creator!=database_native_process())plan_refuse("inherited initialization recipe source");
            if(sql.empty()||sql.size()>program_limit)
                plan_refuse("initialization generated program bound");
            const auto bytes=sql.size()+1;
            if(bytes>alternate_limit-held->stored_bytes||held->entries.size()>=held->entries.capacity())
                plan_refuse("initialization approved recipe aggregate bound");
            held->stored_bytes+=bytes;
            held->entries.push_back({id,ordinary_initialization_text(sql.data(),sql.size())});
        };
        const auto add_model=[&](state::model_kind kind,recipe id,std::string_view sql,
                std::string_view owner,std::string_view object) {
            // Parsing is pure DATA over THIS emitted immutable library recipe.
            // The predeclared one-program headroom covers its bounded temporary.
            auto indexes=kind==state::model_kind::table?
                ordinary_initialization_schema_validation::compiled_implicit_indexes_(sql,object):ordinary_initialization_vector<ordinary_initialization_text>{};
            if(creator!=database_native_process())plan_refuse("inherited model operator description return");
            if(owner.empty()||owner.size()>256||object.empty()||object.size()>256||indexes.size()>2)
                plan_refuse("model operator identity exceeds compiled bound");
            ordinary_initialization_vector<ordinary_initialization_text> modules,module_indexes;
            if(kind==state::model_kind::rtree)for(const char* suffix:{"_node","_parent","_rowid"})modules.emplace_back(ordinary_program_storage_text<ordinary_initialization_text>(object)+suffix);
            else if(kind==state::model_kind::fts5) {
                for(const char* suffix:{"_data","_idx","_docsize","_config"})modules.emplace_back(ordinary_program_storage_text<ordinary_initialization_text>(object)+suffix);
                module_indexes.push_back(ordinary_initialization_text("sqlite_autoindex_")+ordinary_program_storage_text<ordinary_initialization_text>(object)+"_idx_1");
                module_indexes.push_back(ordinary_initialization_text("sqlite_autoindex_")+ordinary_program_storage_text<ordinary_initialization_text>(object)+"_config_1");
            }
            std::uint64_t bytes=owner.size()+object.size()+2+
                (indexes.capacity()+modules.capacity()+module_indexes.capacity())*sizeof(ordinary_initialization_text);
            for(const auto* names:{&indexes,&modules,&module_indexes})for(const auto& name:*names)bytes+=name.size()+1;
            if(bytes>alternate_limit-held->stored_bytes)plan_refuse("model operator aggregate identity bound");
            held->stored_bytes+=bytes;
            add(id,sql);auto& entry=held->entries.back();entry.model=kind;
            entry.model_owner.assign(owner.data(),owner.size());entry.model_object.assign(object.data(),object.size());
            const auto retain_names=[](auto& retained,const auto& names) {
                retained.reserve(names.size());
                for(const auto& name:names)retained.emplace_back(name.data(),name.size());
            };
            // Parser output, module names and retained copies all debit the
            // same actual Open through the current synchronous storage scope.
            retain_names(entry.implicit_indexes,indexes);retain_names(entry.module_tables,modules);
            retain_names(entry.module_indexes,module_indexes);
            if(creator!=database_native_process())plan_refuse("inherited model operator identity copy");
        };
        const auto quoted_size=[&](std::string_view value) {
            if(value.size()>program_limit-2)plan_refuse("model metadata literal bound");
            std::uint64_t bytes=value.size()+2;for(const auto ch:value)if(ch==char(39)){if(bytes==program_limit)plan_refuse("model metadata quote expansion bound");++bytes;}return bytes;
        };
        const auto literal=[&](std::string_view value) {
            ordinary_initialization_text out;out.reserve(size_t(quoted_size(value)));out.push_back(char(39));
            for(const auto ch:value){if(ch==char(39))out.push_back(char(39));out.push_back(ch);}out.push_back(char(39));return out;
        };
        const auto add_metadata=[&](std::string_view key,std::string_view value,bool fresh_only=false) {
            if(key.empty()||key.size()>512||value.size()>recovery_owner_schema::max_bytes)
                plan_refuse("catalog model metadata key/value bound");
            const auto key_bytes=quoted_size(key),value_bytes=quoted_size(value);
            if(key_bytes+value_bytes+96>program_limit)plan_refuse("model metadata compiled SQL bound");
            for(bool read:{false,true}) {
                const ordinary_initialization_text sql=read?"SELECT value FROM main._lattice_meta WHERE key="+literal(key)+" LIMIT 1":
                    "INSERT OR REPLACE INTO main._lattice_meta(key,value) VALUES("+literal(key)+","+literal(value)+")";
                if(creator!=database_native_process())plan_refuse("inherited model metadata emission");
                const auto bytes=sizeof(std::string)*3+key.size()+value.size()+16;
                if(bytes>alternate_limit-held->stored_bytes)plan_refuse("catalog metadata aggregate retained bound");
                held->stored_bytes+=bytes;add(read?recipe::model_metadata_read:recipe::model_metadata_store,sql);
                auto& entry=held->entries.back();entry.model=read?state::model_kind::metadata_read:state::model_kind::metadata_store;entry.fresh_only=fresh_only;
                entry.model_owner="_lattice_meta";entry.model_object.assign(key.data(),key.size());entry.model_value.assign(value.data(),value.size());
                if(creator!=database_native_process())plan_refuse("inherited model metadata identity/value copy");
            }
        };
        for(const auto& [name,model]:catalog.models) {
            ordinary_initialization_text_set no_history;
            for(const auto& property:model.properties)
                if(property.kind==property_kind::primitive&&!property.is_geo_bounds&&property.no_history)no_history.emplace(property.name.data(),property.name.size());
            ordinary_initialization_text marker;for(const auto& name:no_history){if(!marker.empty())marker+=',';marker+=name;}
            add_metadata(ordinary_initialization_text("trigger_flags:")+ordinary_program_storage_text<ordinary_initialization_text>(name),marker);
        }
        if(!catalog.swift_snapshot.empty())add_metadata("lattice_swift_schema",catalog.swift_snapshot);
        // Deterministic fresh output uses the actual full captured membership.
        // Existing rows retain their original final-proof set semantics.
        for(const auto& [key,value]:ordinary_fresh_catalog_internal_metadata_(catalog,held->projection))
            add_metadata(key,value,true);

        add(recipe::model_version_write,"UPDATE main._lattice_meta SET value=? WHERE key='schema_version'");
        held->entries.back().model=state::model_kind::version;
        held->entries.back().model_owner="_lattice_meta";held->entries.back().model_object="schema_version";
        // Exact library-generated complete families; no observed SQL is ever
        // inserted into this plan. Alternate audit bundles are complete real
        // normal producers, and are still validated as a bundle at seal.
        for(const auto& [name,table]:held->projection.tables) {
            add_model(state::model_kind::table,recipe::model_table,table.create_sql,name,name);
            for(const auto& column:table.shadow_columns)add_model(state::model_kind::shadow,recipe::model_shadow_column,
                ordinary_schema_recipe::shadow_column_storage<ordinary_initialization_text>(name,column),name,column);
            const auto trigger=[&](const auto& sql){add_model(state::model_kind::trigger,recipe::model_program,sql,name,
                ordinary_initialization_schema_validation::compiled_object_(sql,"trigger"));};
            for(const auto& sql:table.audit_programs)trigger(sql);
            for(const auto& bundle:table.alternate_audit_programs)for(const auto& sql:bundle)trigger(sql);
            for(const auto& sql:table.other_programs)trigger(sql);
            for(const auto& sql:table.index_programs)add_model(state::model_kind::index,recipe::model_index,sql,name,
                ordinary_initialization_schema_validation::compiled_object_(sql,"index"));
        }
        for(const auto& derived:held->projection.derived) {
            if(!derived.create_sql.empty())add_model(derived.kind==ordinary_derived_kind::geographic?state::model_kind::rtree:state::model_kind::fts5,
                recipe::model_table,derived.create_sql,derived.model,derived.table);
            for(const auto& sql:derived.programs)add_model(state::model_kind::trigger,recipe::model_program,sql,derived.model,
                ordinary_initialization_schema_validation::compiled_object_(sql,"trigger"));
        }
        // Static machinery uses the ACTUAL shared normal recipe definitions,
        // rather than a second hand-written approximation of their DDL.
        add(recipe::metadata_table,ordinary_schema_recipe::metadata_table());
        add(recipe::metadata_initial_version,ordinary_schema_recipe::metadata_initial_version());
        add(recipe::schema_marker,ordinary_schema_recipe::metadata_marker_store());
        add(recipe::schema_marker_prune,ordinary_schema_recipe::metadata_marker_prune());
        add(recipe::sync_control_table,ordinary_schema_recipe::sync_control_table());
        add(recipe::sync_control_initial,ordinary_schema_recipe::sync_control_initial());
        add(recipe::audit_table,ordinary_schema_recipe::audit_table());
        add(recipe::audit_pending_index,ordinary_schema_recipe::audit_pending_index());
        add(recipe::audit_lookup_index,ordinary_schema_recipe::audit_lookup_index());
        add(recipe::receive_store,ordinary_schema_recipe::receive_store());
        add(recipe::receive_channel,ordinary_schema_recipe::receive_channel());
        add(recipe::receive_index,ordinary_schema_recipe::receive_index());
        add(recipe::sync_set,ordinary_schema_recipe::sync_set());
        add(recipe::sync_state,ordinary_schema_recipe::sync_state());
        add(recipe::sync_state_pending,ordinary_schema_recipe::sync_state_pending());
        add(recipe::replication_slots,ordinary_schema_recipe::replication_slots());
        add(recipe::replication_upload_floor,ordinary_schema_recipe::replication_upload_floor());
        // Preserve the actual original Core descriptor order and Swift
        // physical-column recipe too, in addition to the logical projection.
        // These variants are generated ONLY after the complete projection
        // validated closed names/property/column bounds above.
        for(const auto& [name,model]:catalog.models) {
            // Preserve the actual normal Core catalog order and the exact
            // Swift property filter/union override without cloning descriptor
            // maps, closures or unrelated relationship DATA.
            ordinary_projection_model_view physical;
            physical.table_name.assign(name.data(),name.size());
            const bool swift=catalog.swift_models.count(name);
            for(const auto& property:model.properties) {
                if(swift&&property.kind!=property_kind::union_type&&
                   property.kind!=property_kind::primitive&&
                   !(property.kind==property_kind::list&&property.is_geo_bounds))continue;
                physical.properties.push_back({ordinary_initialization_text(property.name.data(),property.name.size()),
                    swift&&property.kind==property_kind::union_type?column_type::text:property.type,
                    property.kind,swift&&property.kind==property_kind::union_type?true:property.nullable,
                    property.is_geo_bounds,property.no_history});
            }
            ordinary_initialization_vector<std::pair<ordinary_initialization_text,column_type>> columns;
            const auto sql=ordinary_schema_recipe::model_table_storage<ordinary_initialization_text>(physical,columns);
            add_model(state::model_kind::table,recipe::model_table,sql,name,name);
            ordinary_initialization_text_set no_history;
            for(const auto& property:model.properties)
                if(property.kind==property_kind::primitive&&!property.is_geo_bounds&&property.no_history)
                    no_history.emplace(property.name.data(),property.name.size());
            const ordinary_initialization_text audit_owner(name.data(),name.size());
            ordinary_model_audit_programs_storage(audit_owner,columns,no_history,ordinary_initialization_text{},[&](const auto& emitted){
                add_model(state::model_kind::trigger,recipe::model_program,std::string_view(emitted),name,
                    ordinary_initialization_schema_validation::compiled_object_(emitted,"trigger"));});
        }
        // A separate initialization-only scope proves genuine internal tables
        // before the bootstrap can read their rows. These are ACTUAL shared
        // library definitions, never a remembered name/observed SQL grant.
        // The copy is covered by the predeclared projection headroom above.
        held->preimage_projection=held->projection;
        const auto internal=[&](const char* name,const char* ddl,
                std::initializer_list<const char*> indexes) {
            ordinary_projection_table table;table.name=name;table.create_sql=ddl;
            table.role=ordinary_projection_role::link;table.local_id=false;
            for(const auto* sql:indexes)table.index_programs.emplace_back(sql);
            if(!held->preimage_projection.tables.emplace(name,std::move(table)).second)
                plan_refuse("initialization internal/model table namespace collision");
        };
        internal("AuditLog",ordinary_schema_recipe::audit_table(),
            {ordinary_schema_recipe::audit_pending_index(),ordinary_schema_recipe::audit_lookup_index()});
        internal("_lattice_meta",ordinary_schema_recipe::metadata_table(),{});
        internal("_SyncControl",ordinary_schema_recipe::sync_control_table(),{});
        const auto static_internal=[&](const char* name,const char* ddl) {
            ordinary_projection_table table;table.name=name;table.create_sql=ddl;
            table.role=ordinary_projection_role::link;table.local_id=false;
            if(!held->static_sync_projection.tables.emplace(name,std::move(table)).second)
                plan_refuse("compiled static sync family identity collision");
        };
        static_internal("_lattice_sync_set",ordinary_schema_recipe::sync_set());
        static_internal("_lattice_sync_state",ordinary_schema_recipe::sync_state());
        static_internal("_lattice_replication_slots",ordinary_schema_recipe::replication_slots());
        held->final_projection=held->preimage_projection;
        const auto final_internal=[&](const char* name,std::string_view ddl,std::initializer_list<const char*> indexes) {
            ordinary_projection_table table;table.name=name;table.create_sql.assign(ddl.data(),ddl.size());
            table.role=ordinary_projection_role::link;table.local_id=false;
            for(const auto* sql:indexes)table.index_programs.emplace_back(sql);
            if(!held->final_projection.tables.emplace(name,std::move(table)).second)
                plan_refuse("final schema internal identity overlaps actual model catalog");
        };
        final_internal("_lattice_sync_set",ordinary_schema_recipe::sync_set(),{});
        final_internal("_lattice_sync_state",ordinary_schema_recipe::sync_state(),{ordinary_schema_recipe::sync_state_pending()});
        final_internal("_lattice_replication_slots",ordinary_schema_recipe::replication_slots_after_upload_floor_storage<ordinary_initialization_text>(),{});
        final_internal("_lattice_receive_guard_store",ordinary_schema_recipe::receive_store(),{});
        final_internal("_lattice_receive_guard",ordinary_schema_recipe::receive_channel(),{ordinary_schema_recipe::receive_index()});
        final_internal("_lattice_observer_delivery_meter_v1",observer_delivery_meter::ledger_ddl(),{});
        const auto add_meter=[&](state::model_kind kind,recipe id,std::string_view sql,std::string_view owner,std::string_view object) {
            const auto bytes=owner.size()+object.size()+2;
            if(bytes>alternate_limit-held->stored_bytes)plan_refuse("meter operator aggregate identity bound");
            held->stored_bytes+=bytes;
            add(id,sql);auto& entry=held->entries.back();entry.model=kind;
            entry.model_owner.assign(owner.data(),owner.size());
            if(creator!=database_native_process())plan_refuse("inherited meter operator owner copy");
            entry.model_object.assign(object.data(),object.size());
            if(creator!=database_native_process())plan_refuse("inherited meter operator identity copy");
        };
        add_meter(state::model_kind::meter_table,recipe::delivery_ledger,observer_delivery_meter::ledger_ddl(),observer_delivery_meter::ledger_name(),observer_delivery_meter::ledger_name());
        add_meter(state::model_kind::meter_seed,recipe::delivery_initial_seed,observer_delivery_meter::initialization_seed_(),observer_delivery_meter::ledger_name(),observer_delivery_meter::ledger_name());
        add_meter(state::model_kind::meter_marker,recipe::delivery_marker_cas,observer_delivery_meter::initialization_marker_cas_(),"_lattice_meta","schema_fingerprint:");
        const auto meter_tables=observer_delivery_schema::tables_storage_<ordinary_initialization_text>(held->projection);
        const auto meter_programs=observer_delivery_meter::programs_storage_<ordinary_initialization_text>(meter_tables);
        for(const auto& program:meter_programs) {
            const auto target=held->final_projection.tables.find(std::string_view(program.table));
            if(target==held->final_projection.tables.end())plan_refuse("actual complete meter guard owner is outside final catalog");
            target->second.other_programs.push_back(program.sql);
            add_meter(state::model_kind::meter_trigger,recipe::delivery_trigger,program.sql,program.table,program.name);
        }
        // Sorting the private lookup table does not change any real schema
        // statement order. Avoid quadratic duplicate/search scans at startup.
        std::sort(held->entries.begin(),held->entries.end(),[](const auto& a,const auto& b){
            return a.sql<b.sql||(a.sql==b.sql&&static_cast<unsigned>(a.recipe)<static_cast<unsigned>(b.recipe));
        });
        const auto end=std::unique(held->entries.begin(),held->entries.end(),[](const auto& a,const auto& b){return a.sql==b.sql;});
        held->entries.erase(end,held->entries.end());
        if(creator!=database_native_process())plan_refuse("inherited initialization plan build return");
        auto* raw=new observer_delivery_initialization_plan(creator,held);
        if(creator!=database_native_process())plan_refuse("inherited initialization plan owner allocation");
        held=nullptr;
        result.get<0>()=std::shared_ptr<const observer_delivery_initialization_plan>(raw,destroy,
            ordinary_initialization_allocator<observer_delivery_initialization_plan>{});
        if(creator!=database_native_process())plan_refuse("inherited initialization plan publication");
        return std::move(result.get<0>());
    }catch(...) {if(creator==database_native_process())delete held;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
void observer_delivery_initialization_plan::entry_(std::size_t actual,
    const observer_delivery_open_startup& open,database& owner,
    observer_delivery_initialization_sql::recipe& id,std::string_view& sql)const {
    try {
    open.require_initializing_(owner);
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       state_->generation!=open.catalog_generation_())
        plan_refuse("initialization immutable recipe generation differs");
    if(actual>=state_->entries.size())plan_refuse("initialization entry is not owned by its actual catalog plan");
    const auto& entry=state_->entries[actual];
    if(entry.fresh_only&&!open.fresh_catalog_active_for_(owner))plan_refuse("fresh metadata recipe requires its exact first-start WRITE original");
    id=entry.recipe;sql=entry.sql;
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::select_(
    const observer_delivery_open_startup& open,database& owner,std::string_view sql,
    const std::vector<column_value_t>& parameters)const {
    try {
    open.require_initializing_(owner);
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       state_->generation!=open.catalog_generation_())
        plan_refuse("initialization recipe selector lost its catalog generation");
    // This first closed family consists only of parameterless generated DDL.
    // Marker/migration parameters require their actual producer plan entry;
    // they never fall through to a generic purpose/string constructor.
    if(!parameters.empty())plan_refuse("initialization parameter recipe is not implemented");
    const auto selected=std::lower_bound(state_->entries.begin(),state_->entries.end(),sql,
        [](const auto& entry,std::string_view wanted){return std::string_view(entry.sql)<wanted;});
    if(selected==state_->entries.end()||std::string_view(selected->sql)!=sql)
        plan_refuse("initialization statement has no actual immutable library recipe");
    return observer_delivery_initialization_sql::from_plan_(*this,
        std::size_t(selected-state_->entries.begin()),open,owner);
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::model_metadata_recipe_(
    const observer_delivery_open_startup& open,database& actual,bool read,std::string_view key,std::string_view value)const {
    try {
        open.require_work_current_(actual);
        if(creator_!=database_native_process()||!state_||state_->creator!=creator_||state_->generation!=open.catalog_generation_())
            plan_refuse("model metadata lost its immutable catalog Plan");
        const auto kind=read?state::model_kind::metadata_read:state::model_kind::metadata_store;
        for(std::size_t index=0;index<state_->entries.size();++index) {
            const auto& entry=state_->entries[index];
            if(entry.model==kind&&std::string_view(entry.model_object)==key&&std::string_view(entry.model_value)==value)
                return observer_delivery_initialization_sql::from_plan_(*this,index,open,actual);
        }
        plan_refuse("model metadata key/value is not an exact retained catalog producer");
    }catch(...) {open.fail_initialization_();throw;}
}
void observer_delivery_initialization_plan::require_current_model_metadata_(
    const observer_delivery_open_startup& open,database& actual)const {
    try {
        open.require_work_current_(actual);
        if(creator_!=database_native_process()||!state_||state_->creator!=creator_||state_->generation!=open.catalog_generation_())
            plan_refuse("current model metadata lost its immutable catalog Plan");
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        using custody=native_retirement_storage<std::vector<database::row_t>>;
        ordinary_initialization_storage_scope::charge(custody::allocation_bytes());custody rows;
        if(creator_!=database_native_process())plan_refuse("inherited current model metadata custody");
        for(std::size_t index=0;index<state_->entries.size();++index) {
            const auto& entry=state_->entries[index];
            if(entry.model!=state::model_kind::metadata_read||entry.fresh_only)continue;
            // Actual trigger-flags and Swift snapshot bytes were generated
            // once into this immutable Plan. No schema/JSON regeneration or
            // standard owning key/value copy occurs on this current route.
            auto issued=observer_delivery_initialization_sql::from_plan_(*this,index,open,actual);
            rows.get<0>()=actual.query_initialization_(issued);
            if(creator_!=database_native_process())plan_refuse("inherited current model metadata read");
            open.require_work_current_(actual);
            if(rows.get<0>().size()!=1||rows.get<0>().front().size()!=1)
                plan_refuse("current model metadata requires its real typed producer");
            const column_value_t* cell=nullptr;
            for(const auto& item:rows.get<0>().front())if(std::string_view(item.first)=="value")cell=&item.second;
            const auto* value=cell?std::get_if<std::string>(cell):nullptr;
            if(!value||std::string_view(*value)!=std::string_view(entry.model_value))
                plan_refuse("current model metadata differs from its retained producer");
        }
        open.require_work_current_(actual);
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::model_version_recipe_(
    const observer_delivery_open_startup& open,database& actual,std::int64_t next)const {
    try {
        open.require_work_current_(actual);
        if(creator_!=database_native_process()||!state_||state_->creator!=creator_||state_->generation!=open.catalog_generation_()||
           next<=0||next>open.initialization_schema_version_(actual))plan_refuse("model version target is outside actual pending Plan");
        for(std::size_t index=0;index<state_->entries.size();++index)if(state_->entries[index].model==state::model_kind::version) {
            ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
                &observer_delivery_open_startup::debit_initialization_storage_);
            const auto value=ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(next));
            if(creator_!=database_native_process())plan_refuse("inherited version parameter emission");
            return observer_delivery_initialization_sql::from_plan_(*this,index,open,actual,{},{},value);
        }
        plan_refuse("actual model version Plan operator absent");
    }catch(...) {open.fail_initialization_();throw;}
}
bool observer_delivery_initialization_plan::model_operator_for_(std::size_t index,
    const observer_delivery_open_startup& open,const database& actual,
    observer_delivery_initialization_sql::recipe id)const noexcept {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       state_->generation!=open.catalog_generation_()||!open.pending_for_(actual)||
       index>=state_->entries.size()||state_->entries[index].recipe!=id)return false;
    if(state_->entries[index].fresh_only&&!open.fresh_catalog_active_for_(actual))return false;
    // The newly closed meter family NEVER inherits the old model issuer.
    // Preserve the exact previously admitted model-kind set explicitly.
    switch(state_->entries[index].model) {
      case state::model_kind::table:case state::model_kind::trigger:case state::model_kind::index:
      case state::model_kind::shadow:case state::model_kind::rtree:case state::model_kind::fts5:
      case state::model_kind::metadata_store:case state::model_kind::metadata_read:case state::model_kind::version:return true;
      default:return false;
    }
}
int observer_delivery_initialization_plan::model_authorize_(std::size_t index,
    const observer_delivery_open_startup& open,const database& actual,
    observer_delivery_initialization_sql::recipe id,int action,const char* first,
    const char* second,const char* schema)const noexcept {
    if(!model_operator_for_(index,open,actual,id))return SQLITE_DENY;
    const auto& entry=state_->entries[index];
    const auto same=[](const char* text,std::string_view expected)noexcept {
        if(!text)return expected.empty();std::size_t i=0;
        const auto lower=[](unsigned char c){return c>='A'&&c<='Z'?static_cast<unsigned char>(c+('a'-'A')):c;};
        for(;text[i]&&i<expected.size();++i)if(lower(static_cast<unsigned char>(text[i]))!=lower(static_cast<unsigned char>(expected[i])))return false;
        return i==expected.size()&&!text[i];
    };
    const auto member=[&](const auto& values,const char* name)noexcept {
        for(const auto& value:values)if(same(name,value))return true;return false;
    };
    const bool module=entry.model==state::model_kind::rtree||entry.model==state::model_kind::fts5;
    // The SQL entry is an exact immutable library program. Function names
    // below cannot select an expression or SQL body; all user origins remain
    // refused by the enclosing Request/descriptor before this private leaf.
    if(action==SQLITE_ALTER_TABLE)
        return entry.model==state::model_kind::shadow&&same(first,"main")&&same(second,entry.model_owner)&&
            (!schema||same(schema,"main"))?SQLITE_OK:SQLITE_DENY;
    if(action==SQLITE_FUNCTION) {
        if(entry.model==state::model_kind::shadow&&same(second,"printf"))return SQLITE_OK;
        for(const char* name:{"randomblob","hex","lower","substr","abs","random","unixepoch","json_object","json_array"})
            if(same(second,name))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(action==SQLITE_SELECT)return module||entry.model==state::model_kind::metadata_read||entry.model==state::model_kind::shadow?SQLITE_OK:SQLITE_DENY;
    if(!same(schema,"main"))return SQLITE_DENY;
    if(entry.model==state::model_kind::metadata_store||entry.model==state::model_kind::metadata_read||entry.model==state::model_kind::version) {
        if(!same(first,"_lattice_meta"))return SQLITE_DENY;
        if(action==SQLITE_READ)return same(second,"key")||same(second,"value")?SQLITE_OK:SQLITE_DENY;
        if(entry.model==state::model_kind::metadata_store&&action==SQLITE_INSERT)return SQLITE_OK;
        if(entry.model==state::model_kind::version&&action==SQLITE_UPDATE&&same(second,"value"))return SQLITE_OK;
        return SQLITE_DENY;
    }
    const bool catalog=same(first,"sqlite_master")||same(first,"sqlite_schema");
    if(action==SQLITE_CREATE_TABLE) {
        if(entry.model==state::model_kind::table&&same(first,entry.model_object))return SQLITE_OK;
        return module&&member(entry.module_tables,first)?SQLITE_OK:SQLITE_DENY;
    }
    if(action==SQLITE_CREATE_VTABLE)
        return module&&same(first,entry.model_object)&&same(second,entry.model==state::model_kind::rtree?"rtree":"fts5")?SQLITE_OK:SQLITE_DENY;
    if(action==SQLITE_CREATE_TRIGGER)
        return entry.model==state::model_kind::trigger&&same(first,entry.model_object)&&same(second,entry.model_owner)?SQLITE_OK:SQLITE_DENY;
    if(action==SQLITE_REINDEX)
        return entry.model==state::model_kind::index&&same(first,entry.model_object)?SQLITE_OK:SQLITE_DENY;
    if(action==SQLITE_CREATE_INDEX) {
        if(entry.model==state::model_kind::index&&same(first,entry.model_object)&&same(second,entry.model_owner))return SQLITE_OK;
        if(entry.model==state::model_kind::table&&member(entry.implicit_indexes,first)&&same(second,entry.model_object))return SQLITE_OK;
        return module&&member(entry.module_indexes,first)&&member(entry.module_tables,second)?SQLITE_OK:SQLITE_DENY;
    }
    if(action==SQLITE_INSERT&&catalog)return SQLITE_OK;
    if(action==SQLITE_UPDATE&&catalog) {
        for(const char* column:{"type","name","tbl_name","rootpage","sql"})if(same(second,column))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(action==SQLITE_READ) {
        if(catalog) {for(const char* column:{"type","name","tbl_name","rootpage","sql","ROWID"})if(same(second,column))return SQLITE_OK;return SQLITE_DENY;}
        if(module&&member(entry.module_tables,first))return SQLITE_OK;
        if((entry.model!=state::model_kind::index&&entry.model!=state::model_kind::table&&!module)||!same(first,entry.model_owner))return SQLITE_DENY;
        const auto table=state_->projection.tables.find(std::string_view(entry.model_owner));
        if(table==state_->projection.tables.end())return SQLITE_DENY;
        if(table->second.local_id&&same(second,"id"))return SQLITE_OK;
        for(const auto& column:table->second.columns)if(same(second,column.name))return SQLITE_OK;
        return member(table->second.shadow_columns,second)?SQLITE_OK:SQLITE_DENY;
    }
    if(module&&(action==SQLITE_INSERT||action==SQLITE_UPDATE||action==SQLITE_DELETE)&&member(entry.module_tables,first))return SQLITE_OK;
    return SQLITE_DENY;
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::metadata_table_recipe_(
    const observer_delivery_open_startup& open,database& actual)const {
    return select_(open,actual,ordinary_schema_recipe::metadata_table(),{});
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::metadata_initial_version_recipe_(
    const observer_delivery_open_startup& open,database& actual)const {
    return select_(open,actual,ordinary_schema_recipe::metadata_initial_version(),{});
}
bool observer_delivery_initialization_plan::metadata_operator_for_(std::size_t index,
    const observer_delivery_open_startup& open,const database& actual,
    observer_delivery_initialization_sql::recipe id)const noexcept {
    using recipe=observer_delivery_initialization_sql::recipe;
    if(id!=recipe::metadata_table&&id!=recipe::metadata_initial_version&&
       id!=recipe::schema_marker&&id!=recipe::schema_marker_prune)return false;
    return creator_==database_native_process()&&state_&&state_->creator==creator_&&
        state_->generation==open.catalog_generation_()&&open.pending_for_(actual)&&
        index<state_->entries.size()&&state_->entries[index].recipe==id;
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::metadata_marker_recipe_(
    const observer_delivery_open_startup& open,database& actual,bool prune,
    std::string_view key,std::string_view cookie)const {
    try {
        open.require_work_current_(actual);
        if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
           state_->generation!=open.catalog_generation_())
            plan_refuse("marker Plan lost its immutable catalog generation");
        const std::string_view text=prune?ordinary_schema_recipe::metadata_marker_prune():ordinary_schema_recipe::metadata_marker_store();
        const auto selected=std::lower_bound(state_->entries.begin(),state_->entries.end(),text,
            [](const auto& entry,std::string_view wanted){return std::string_view(entry.sql)<wanted;});
        using recipe=observer_delivery_initialization_sql::recipe;
        const auto id=prune?recipe::schema_marker_prune:recipe::schema_marker;
        if(selected==state_->entries.end()||std::string_view(selected->sql)!=text||selected->recipe!=id)
            plan_refuse("marker has no exact immutable library operator");
        return observer_delivery_initialization_sql::from_plan_(*this,
            std::size_t(selected-state_->entries.begin()),open,actual,key,cookie);
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::sync_control_table_recipe_(
    const observer_delivery_open_startup& open,database& actual)const {
    return select_(open,actual,ordinary_schema_recipe::sync_control_table(),{});
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::sync_control_initial_recipe_(
    const observer_delivery_open_startup& open,database& actual)const {
    return select_(open,actual,ordinary_schema_recipe::sync_control_initial(),{});
}
bool observer_delivery_initialization_plan::sync_control_operator_for_(std::size_t index,
    const observer_delivery_open_startup& open,const database& actual,
    observer_delivery_initialization_sql::recipe id)const noexcept {
    using recipe=observer_delivery_initialization_sql::recipe;
    if(id!=recipe::sync_control_table&&id!=recipe::sync_control_initial)return false;
    return creator_==database_native_process()&&state_&&state_->creator==creator_&&
        state_->generation==open.catalog_generation_()&&open.pending_for_(actual)&&
        index<state_->entries.size()&&state_->entries[index].recipe==id;
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::static_sync_recipe_(
    const observer_delivery_open_startup& open,database& actual,observer_delivery_initialization_sql::recipe id)const {
    using recipe=observer_delivery_initialization_sql::recipe;
    switch(id) {
      case recipe::sync_set:return select_(open,actual,ordinary_schema_recipe::sync_set(),{});
      case recipe::sync_state:return select_(open,actual,ordinary_schema_recipe::sync_state(),{});
      case recipe::sync_state_pending:return select_(open,actual,ordinary_schema_recipe::sync_state_pending(),{});
      case recipe::replication_slots:return select_(open,actual,ordinary_schema_recipe::replication_slots(),{});
      case recipe::replication_upload_floor:return select_(open,actual,ordinary_schema_recipe::replication_upload_floor(),{});
      default:plan_refuse("not an actual closed static sync operator");
    }
}
bool observer_delivery_initialization_plan::static_sync_operator_for_(std::size_t index,
    const observer_delivery_open_startup& open,const database& actual,observer_delivery_initialization_sql::recipe id)const noexcept {
    using recipe=observer_delivery_initialization_sql::recipe;
    if(id!=recipe::sync_set&&id!=recipe::sync_state&&id!=recipe::sync_state_pending&&
       id!=recipe::replication_slots&&id!=recipe::replication_upload_floor)return false;
    return creator_==database_native_process()&&state_&&state_->creator==creator_&&
        state_->generation==open.catalog_generation_()&&open.pending_for_(actual)&&
        index<state_->entries.size()&&state_->entries[index].recipe==id;
}
observer_delivery_initialization_sql observer_delivery_initialization_plan::audit_recipe_(
    const observer_delivery_open_startup& open,database& actual,observer_delivery_initialization_sql::recipe id)const {
    using recipe=observer_delivery_initialization_sql::recipe;
    switch(id) {
      case recipe::audit_table:return select_(open,actual,ordinary_schema_recipe::audit_table(),{});
      case recipe::audit_pending_index:return select_(open,actual,ordinary_schema_recipe::audit_pending_index(),{});
      case recipe::audit_lookup_index:return select_(open,actual,ordinary_schema_recipe::audit_lookup_index(),{});
      default:plan_refuse("not an actual closed AuditLog operator");
    }
}
bool observer_delivery_initialization_plan::audit_operator_for_(std::size_t index,
    const observer_delivery_open_startup& open,const database& actual,observer_delivery_initialization_sql::recipe id)const noexcept {
    using recipe=observer_delivery_initialization_sql::recipe;
    std::string_view expected;
    switch(id) {
      case recipe::audit_table:expected=ordinary_schema_recipe::audit_table();break;
      case recipe::audit_pending_index:expected=ordinary_schema_recipe::audit_pending_index();break;
      case recipe::audit_lookup_index:expected=ordinary_schema_recipe::audit_lookup_index();break;
      default:return false;
    }
    // audit_table ALSO labels model audit-trigger programs in the old plan.
    // The enum alone can never select this actual AuditLog CREATE operator.
    return creator_==database_native_process()&&state_&&state_->creator==creator_&&
        state_->generation==open.catalog_generation_()&&open.pending_for_(actual)&&
        index<state_->entries.size()&&state_->entries[index].recipe==id&&
        std::string_view(state_->entries[index].sql)==expected;
}
struct observer_delivery_initialization_preimage::state final {
    const std::uint64_t creator;
    const observer_delivery_initialization_plan* plan_owner=nullptr; // comparison only, retained same startup owns Plan
    database* actual=nullptr; // comparison only; never owns a Native wrapper
    std::shared_ptr<const observer_delivery_open_startup> open;
    std::shared_ptr<const database_initialization_episode> snapshot;
    ordinary_schema_preimage facts;
    ordinary_schema_projection scope;
    std::uint64_t schema_revision=0;
    std::optional<bool> meter_present; // strictly proved old-or-complete entry, never postimage/grant
    const recovery_obligation_bootstrap_reader* enrollment_reader=nullptr; // comparison DATA only
    std::optional<observer_delivery_open_startup::enrollment_ledger_phase> enrollment_phase;
    // Pure immutable compiled names prepared BEFORE any descriptor authorizer.
    ordinary_initialization_vector<std::pair<ordinary_initialization_text,ordinary_initialization_text>> enrollment_indexes;
    std::optional<observer_delivery_open_startup::receive_preimage_phase> receive_phase;
    std::optional<observer_delivery_open_startup::metadata_preimage_phase> metadata_phase;
    std::optional<observer_delivery_open_startup::model_preimage_phase> model_phase;
    // Correspondence only: assigned after this actual capture and all guarded
    // row/metadata checks. Stage creation independently classifies full DATA.
    std::optional<observer_delivery_schema::model_stage_family> model_stage_family;
    std::optional<observer_delivery_open_startup::sync_control_preimage_phase> sync_control_phase;
    std::optional<observer_delivery_open_startup::static_sync_preimage_phase> static_sync_phase;
    bool static_sync_upload_current=false;
    std::optional<observer_delivery_open_startup::audit_preimage_phase> audit_phase;
    ordinary_initialization_vector<ordinary_initialization_text> plain_tables;
    explicit state(std::uint64_t issuer)noexcept:creator(issuer){}
};
struct observer_delivery_initialization_stage::state final {
    const std::uint64_t creator;
    const observer_delivery_initialization_plan* const plan;
    const void* const generation;
    database* const actual;
    const observer_delivery_open_startup* const original; // comparison only, owning Open retains this DATA
    std::weak_ptr<const database_initialization_episode> episode; // control-block identity only; no Open cycle
    const observer_delivery_schema::model_stage_family family;
    using text=std::basic_string<char,std::char_traits<char>,ordinary_initialization_allocator<char>>;
    struct columns {
        text table;
        std::vector<text,ordinary_initialization_allocator<text>> present;
        explicit columns(std::string_view name):table(name.data(),name.size()){}
    };
    std::vector<columns,ordinary_initialization_allocator<columns>> physical;
    state(std::uint64_t issuer,const observer_delivery_initialization_plan* source,const void* catalog,database* owner,
        const observer_delivery_open_startup* open,const std::shared_ptr<const database_initialization_episode>& snapshot,
        observer_delivery_schema::model_stage_family selected)
        :creator(issuer),plan(source),generation(catalog),actual(owner),original(open),episode(snapshot),family(selected){}
};
observer_delivery_initialization_stage::observer_delivery_initialization_stage(std::uint64_t creator,state* value)noexcept
    :creator_(creator),state_(value){}
observer_delivery_initialization_stage::~observer_delivery_initialization_stage()noexcept {
    if(creator_==database_native_process())delete state_;
}
void observer_delivery_initialization_stage::destroy_(const observer_delivery_initialization_stage* value)noexcept {
    if(value&&value->creator_==database_native_process())delete value;
}
bool observer_delivery_initialization_plan::model_stage_for_(const observer_delivery_initialization_stage& stage,
    const observer_delivery_initialization_preimage& proof)const noexcept {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||stage.creator_!=creator_||
       !stage.state_||stage.state_->creator!=creator_||stage.state_->plan!=this||stage.state_->generation!=state_->generation||
       proof.creator_!=creator_||!proof.state_||proof.state_->creator!=creator_||proof.state_->plan_owner!=this||
       proof.state_->actual!=stage.state_->actual||!proof.state_->snapshot||!proof.state_->open||proof.state_->open.get()!=stage.state_->original||
       proof.state_->open->catalog_generation_()!=state_->generation||!proof.state_->model_stage_family||
       *proof.state_->model_stage_family!=stage.state_->family||
       (stage.state_->family==observer_delivery_schema::model_stage_family::complete_meter&&!stage.state_->physical.empty()))return false;
    // Same control block, even after an old weak episode expires; no pointer
    // ABA, weak lock, strong Open cycle or ownership transfer is involved.
    return !stage.state_->episode.owner_before(proof.state_->snapshot)&&
        !proof.state_->snapshot.owner_before(stage.state_->episode);
}
std::shared_ptr<const observer_delivery_initialization_stage>
observer_delivery_initialization_plan::model_stage_from_(const observer_delivery_open_startup& open,database& actual,
    const observer_delivery_initialization_preimage& proof)const {
    try {
        open.require_work_current_(actual);
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        if(creator_!=database_native_process()||!state_||state_->creator!=creator_||state_->generation!=open.catalog_generation_()||
           proof.creator_!=creator_||!proof.state_||proof.state_->plan_owner!=this||proof.state_->actual!=&actual||
           proof.state_->open.get()!=&open||!proof.model_phase_for_(actual,observer_delivery_open_startup::model_preimage_phase::construction)||
           !proof.state_->model_stage_family)
            plan_refuse("model physical stage lacks its actual current Plan/Open/episode proof");
        // A table-count/name flag is never the guarded-family proof. Regenerate
        // its complete compiled alternative under the SAME Open budget and
        // compare every retained projection field plus exact actual objects.
        const auto family=observer_delivery_schema::model_stage_scope_(state_->projection,state_->preimage_projection,
            proof.state_->scope,proof.state_->facts);
        if(creator_!=database_native_process())plan_refuse("inherited model physical stage family return");
        if(family!=*proof.state_->model_stage_family)plan_refuse("model physical stage family differs from actual capture");
        const auto& physical_scope=family==observer_delivery_schema::model_stage_family::complete_meter?
            state_->preimage_projection:proof.state_->scope;
        // Complete guarded catalogs have every declared shadow. Borrow their
        // immutable complete model view; no ledger or variable stage grants
        // extra model DDL/metadata authority.
        std::size_t tables=0,names=0;std::uint64_t bytes=0;
        for(const auto& [name,full]:state_->preimage_projection.tables) {
            const auto at=physical_scope.tables.find(name);
            if(at==physical_scope.tables.end())plan_refuse("model physical stage lost declared table");
            const auto& selected=at->second;std::size_t ordinal=0;
            for(const auto& column:selected.shadow_columns) {
                while(ordinal<full.shadow_columns.size()&&full.shadow_columns[ordinal]!=column)++ordinal;
                if(ordinal==full.shadow_columns.size())plan_refuse("model stage contains undeclared/duplicate shadow column");
                ++ordinal;
            }
            if(selected.shadow_columns==full.shadow_columns)continue;
            if(full.role!=ordinary_projection_role::model||full.shadow_columns.empty()||name.size()>256)
                plan_refuse("model stage may vary only declared model shadow columns");
            ++tables;names+=selected.shadow_columns.size();bytes+=name.size()+1;
            for(const auto& column:selected.shadow_columns)bytes+=column.size()+1;
        }
        if(tables>recovery_owner_schema::max_models||names>recovery_owner_schema::max_properties||bytes>recovery_owner_schema::max_bytes)
            plan_refuse("model physical stage declaration bound");
        // The same retained Open is the ONLY debit sink. Nested Plan/stage
        // work uses this exact callback identity; no scope/sink is retained by
        // the returned allocator and deallocation never refunds this account.
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_stage>>;
        open.claim_workspace_(actual,sizeof(observer_delivery_initialization_stage)+sizeof(observer_delivery_initialization_stage::state)+
            custody::allocation_bytes());
        custody result;if(creator_!=database_native_process())plan_refuse("inherited model stage custody");
        auto* held=new observer_delivery_initialization_stage::state(creator_,this,state_->generation,&actual,&open,proof.state_->snapshot,family);
        if(creator_!=database_native_process())plan_refuse("inherited model stage state allocation");
        observer_delivery_initialization_stage* wrapper=nullptr;bool owned=false;
        try {
            held->physical.reserve(tables);
            if(creator_!=database_native_process())plan_refuse("inherited model stage vector allocation");
            for(const auto& [name,full]:state_->preimage_projection.tables) {
                const auto& selected=physical_scope.tables.at(name);
                if(selected.shadow_columns==full.shadow_columns)continue;
                held->physical.emplace_back(std::string_view(name));
                if(creator_!=database_native_process())plan_refuse("inherited model stage name copy");
                auto& columns=held->physical.back().present;columns.reserve(selected.shadow_columns.size());
                if(creator_!=database_native_process())plan_refuse("inherited model stage column allocation");
                for(const auto& column:selected.shadow_columns){columns.emplace_back(column.data(),column.size());if(creator_!=database_native_process())plan_refuse("inherited model stage column copy");}
            }
            wrapper=new observer_delivery_initialization_stage(creator_,held);owned=true;
            if(creator_!=database_native_process())plan_refuse("inherited model stage wrapper allocation");
            const auto* entrusted=wrapper;wrapper=nullptr;
            result.get<0>()=std::shared_ptr<const observer_delivery_initialization_stage>(entrusted,
                &observer_delivery_initialization_stage::destroy_,ordinary_initialization_allocator<observer_delivery_initialization_stage>{});
            if(creator_!=database_native_process())plan_refuse("inherited model stage publication");
            return std::move(result.get<0>());
        }catch(...) {if(creator_==database_native_process()){if(wrapper)delete wrapper;else if(!owned)delete held;}throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
observer_delivery_initialization_preimage::observer_delivery_initialization_preimage(
    std::uint64_t issuer,state* value)noexcept:creator_(issuer),state_(value){}
observer_delivery_initialization_preimage::~observer_delivery_initialization_preimage()noexcept {
    if(creator_==database_native_process())delete state_;
}
void observer_delivery_initialization_preimage::destroy_(const observer_delivery_initialization_preimage* actual)noexcept {
    if(actual&&actual->creator_==database_native_process())delete actual;
}
bool observer_delivery_initialization_preimage::current_snapshot_(const database& actual)const noexcept {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
       state_->actual!=&actual||!state_->open||!state_->snapshot)return false;
    std::uint64_t revision=0;
    return actual.initialization_schema_revision_(*state_->snapshot,*state_->open,revision)&&
        revision==state_->schema_revision;
}
bool observer_delivery_initialization_preimage::enrollment_ledger_for_(const database& actual,
    const recovery_obligation_bootstrap_reader* reader,observer_delivery_open_startup::enrollment_ledger_operator op,
    std::string_view table,std::string_view index)const noexcept {
    if(!current_snapshot_(actual)||!reader||state_->enrollment_reader!=reader||!state_->enrollment_phase)return false;
    bool declared=false;for(const auto& item:state_->scope.tables)if(item.first==table)declared=true;
    if(!declared)return false;
    // All names are fixed output of the genuine compiled ledger supplier.
    // No observed SQL/name, purpose flag or digest can enter this family.
    bool ledger=false;
    for(const char* name:{"_lattice_install_store","_lattice_install_channel","_lattice_obligation_store",
        "_lattice_obligation_scope","_lattice_obligation_entry","_lattice_obligation_producer_store",
        "_lattice_obligation_producer_profile","_lattice_obligation_producer_stamp"})if(table==name)ledger=true;
    if(!ledger)return false;
    const bool absent=std::binary_search(state_->facts.absent_tables.begin(),state_->facts.absent_tables.end(),table);
    using operation=observer_delivery_open_startup::enrollment_ledger_operator;
    using phase=observer_delivery_open_startup::enrollment_ledger_phase;
    if(op==operation::bind_receiver)return *state_->enrollment_phase==phase::complete&&!absent&&index.empty()&&
        (table=="_lattice_install_store"||table=="_lattice_install_channel");
    if(op==operation::seed_store)return *state_->enrollment_phase==phase::complete&&!absent&&index.empty()&&
        (table=="_lattice_install_store"||table=="_lattice_obligation_store"||table=="_lattice_obligation_producer_store");
    if(*state_->enrollment_phase!=phase::construction)return false;
    if(op==operation::create_table)return absent&&index.empty();
    if(op!=operation::create_index||absent||index.empty())return false;
    bool expected=false;
    for(const auto& item:state_->enrollment_indexes)if(item.first==table&&item.second==index)expected=true;
    if(!expected)return false;
    const ordinary_identifier_less less;
    for(const auto& object:state_->facts.main_objects)
        if(!less(object.name,index)&&!less(index,object.name))return false;
    return true;
}
ordinary_initialization_vector<ordinary_initialization_text> observer_delivery_initialization_plan::enrollment_implicit_indexes_(
    std::string_view sql,std::string_view table) {
    // The sole caller supplies the same closed regenerated ledger CREATE.
    // Parser output is comparison DATA; no SQL or Native grant is returned.
    auto result=ordinary_initialization_schema_validation::compiled_implicit_indexes_(sql,table);
    if(result.size()>3)plan_refuse("compiled ledger implicit-index count exceeded");
    return result;
}
bool observer_delivery_initialization_preimage::receive_phase_for_(const database& actual,
    observer_delivery_open_startup::receive_preimage_phase phase)const noexcept {
    return current_snapshot_(actual)&&state_->receive_phase&&*state_->receive_phase==phase;
}
bool observer_delivery_initialization_preimage::model_phase_for_(const database& actual,
    observer_delivery_open_startup::model_preimage_phase phase)const noexcept {
    return current_snapshot_(actual)&&state_->model_phase&&*state_->model_phase==phase;
}
bool observer_delivery_initialization_preimage::metadata_phase_for_(const database& actual,
    observer_delivery_open_startup::metadata_preimage_phase phase)const noexcept {
    return current_snapshot_(actual)&&state_->metadata_phase&&*state_->metadata_phase==phase;
}
bool observer_delivery_initialization_preimage::sync_control_phase_for_(const database& actual,
    observer_delivery_open_startup::sync_control_preimage_phase phase)const noexcept {
    return current_snapshot_(actual)&&state_->sync_control_phase&&*state_->sync_control_phase==phase;
}
bool observer_delivery_initialization_preimage::static_sync_phase_for_(const database& actual,
    observer_delivery_open_startup::static_sync_preimage_phase phase)const noexcept {
    return current_snapshot_(actual)&&state_->static_sync_phase&&*state_->static_sync_phase==phase;
}
bool observer_delivery_initialization_preimage::static_sync_upload_current_for_(const database& actual)const noexcept {
    return current_snapshot_(actual)&&state_->static_sync_phase&&state_->static_sync_upload_current;
}
bool observer_delivery_initialization_preimage::audit_phase_for_(const database& actual,
    observer_delivery_open_startup::audit_preimage_phase phase)const noexcept {
    return current_snapshot_(actual)&&state_->audit_phase&&*state_->audit_phase==phase;
}
bool observer_delivery_initialization_preimage::plain_table_for_(
    const database& actual,std::string_view table)const noexcept {
    if(!current_snapshot_(actual))return false;
    const ordinary_identifier_less less;
    const auto at=std::lower_bound(state_->plain_tables.begin(),state_->plain_tables.end(),table,less);
    return at!=state_->plain_tables.end()&&!less(*at,table)&&!less(table,*at);
}
bool observer_delivery_initialization_preimage::shape_for_(const database& actual,
    std::string_view table,std::uint64_t& columns,bool& without_rowid)const noexcept {
    columns=0;without_rowid=false;
    if(!plain_table_for_(actual,table))return false;
    const ordinary_identifier_less less;
    const auto at=std::find_if(state_->facts.plain_shapes.begin(),state_->facts.plain_shapes.end(),
        [&](const auto& value){return !less(value.name,table)&&!less(table,value.name);});
    if(at==state_->facts.plain_shapes.end())return false;
    columns=at->columns;without_rowid=at->without_rowid;return true;
}
bool observer_delivery_initialization_preimage::model_table_present_for_(const database& actual,std::string_view table)const {
    if(!model_phase_for_(actual,observer_delivery_open_startup::model_preimage_phase::construction))
        plan_refuse("model inventory lost its actual construction snapshot");
    const ordinary_identifier_less less;const auto same=[&](std::string_view a,std::string_view b){return !less(a,b)&&!less(b,a);};
    bool selected=false;
    for(const auto& [name,unused]:state_->scope.tables)if(same(name,table)){selected=true;break;}
    if(!selected)for(const auto& derived:state_->scope.derived)if(same(derived.table,table)){selected=true;break;}
    if(!selected)plan_refuse("model inventory target is outside immutable catalog projection");
    for(const auto& name:state_->facts.absent_tables)if(same(name,table))return false;
    return true;
}
struct observer_delivery_initialization_columns::state final {
    const std::uint64_t creator;
    std::shared_ptr<const observer_delivery_initialization_preimage> preimage;
    struct column {std::string_view name;std::array<char,17> type{};std::uint8_t bytes=0;};
    ordinary_initialization_vector<column> values;
    explicit state(std::uint64_t issuer):creator(issuer){}
};
observer_delivery_initialization_columns::observer_delivery_initialization_columns(std::uint64_t issuer,state* value)noexcept
    :creator_(issuer),state_(value){}
observer_delivery_initialization_columns::observer_delivery_initialization_columns(observer_delivery_initialization_columns&& other)noexcept
    :creator_(other.creator_),state_(other.creator_==database_native_process()?std::exchange(other.state_,nullptr):nullptr){}
observer_delivery_initialization_columns::~observer_delivery_initialization_columns()noexcept {
    if(creator_==database_native_process()&&state_&&state_->creator==creator_)delete state_;
}
std::size_t observer_delivery_initialization_columns::size()const {
    if(creator_!=database_native_process()||!state_||state_->creator!=creator_)
        plan_refuse("inherited model column DATA");
    return state_->values.size();
}
std::size_t observer_delivery_initialization_columns::count(std::string_view name)const {
    (void)size();for(const auto& column:state_->values)if(column.name==name)return 1;return 0;
}
std::string_view observer_delivery_initialization_columns::at(std::string_view name)const {
    (void)size();for(const auto& column:state_->values)if(column.name==name)
        return {column.type.data(),column.bytes};
    throw std::out_of_range("model column DATA key absent");
}
observer_delivery_initialization_columns
observer_delivery_initialization_preimage::model_table_info_for_(const database& actual,std::string_view table,
    const std::shared_ptr<const observer_delivery_initialization_preimage>& original)const {
    if(creator_!=database_native_process())plan_refuse("inherited model column DATA request");
    if(original.get()!=this||!model_table_present_for_(actual,table)||!plain_table_for_(actual,table))
        plan_refuse("model columns require their same actual retained present-table preimage");
    const ordinary_identifier_less less;
    const auto at=std::find_if(state_->facts.plain_shapes.begin(),state_->facts.plain_shapes.end(),
        [&](const auto& shape){return !less(shape.name,table)&&!less(table,shape.name);});
    if(at==state_->facts.plain_shapes.end()||at->physical_columns.size()!=at->columns||
       at->columns>ordinary_sync_limits::columns_per_table+1)
        plan_refuse("model column facts are not the same validated physical shape");
    for(const auto& column:at->physical_columns)
        if(column.name.size()>256||column.type.size()>16)plan_refuse("model column DATA exceeds actual validated bounds");
    ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(state_->open.get()),
        state_->actual,&observer_delivery_open_startup::debit_initialization_storage_);
    ordinary_initialization_storage_scope::charge(sizeof(observer_delivery_initialization_columns::state));
    const auto creator=database_native_process();
    auto* held=new observer_delivery_initialization_columns::state(creator);
    if(creator_!=database_native_process())plan_refuse("inherited model column DATA allocation");
    try {
        held->preimage=original;held->values.reserve(size_t(at->columns));
        if(creator_!=database_native_process())plan_refuse("inherited model column DATA reserve");
        for(const auto& column:at->physical_columns) {
            observer_delivery_initialization_columns::state::column value;
            value.name={column.name.data(),column.name.size()};value.bytes=std::uint8_t(column.type.size());
            for(size_t i=0;i<column.type.size();++i) {
                const auto c=column.type[i];value.type[i]=c>='a'&&c<='z'?char(c-'a'+'A'):c;
            }
            held->values.push_back(value);
            if(creator_!=database_native_process())plan_refuse("inherited model column DATA append");
        }
        if(!current_snapshot_(actual))plan_refuse("model column return lost its actual view");
        return observer_delivery_initialization_columns(creator,held);
    }catch(...) {if(creator==database_native_process())delete held;throw;}
}

bool observer_delivery_initialization_plan::model_preimage_for_(std::size_t index,
    const observer_delivery_open_startup& open,const database& actual,
    const observer_delivery_initialization_preimage& proof)const noexcept {
    if(creator_!=database_native_process()||!state_||index>=state_->entries.size()||
       !model_operator_for_(index,open,actual,state_->entries[index].recipe)||
       !proof.model_phase_for_(actual,observer_delivery_open_startup::model_preimage_phase::construction))return false;
    const auto& entry=state_->entries[index];
    if(entry.model==state::model_kind::table)return true; // exact absent/current CREATE proof
    if(entry.model==state::model_kind::metadata_store||entry.model==state::model_kind::metadata_read||entry.model==state::model_kind::version)
        return proof.plain_table_for_(actual,"_lattice_meta");
    if(!proof.plain_table_for_(actual,entry.model_owner))return false;
    if(entry.model==state::model_kind::shadow) {
        const auto full=state_->preimage_projection.tables.find(std::string_view(entry.model_owner));
        const auto selected=proof.state_->scope.tables.find(std::string_view(entry.model_owner));
        return full!=state_->preimage_projection.tables.end()&&selected!=proof.state_->scope.tables.end()&&
            plan_contains_name_(full->second.shadow_columns,std::string_view(entry.model_object))&&
            !plan_contains_name_(selected->second.shadow_columns,std::string_view(entry.model_object));
    }
    return true;
}
bool observer_delivery_initialization_plan::model_postimage_for_(std::size_t index,
    const observer_delivery_open_startup& open,const database& actual,
    const observer_delivery_initialization_preimage& proof)const noexcept {
    if(creator_!=database_native_process()||!state_||index>=state_->entries.size()||
       !model_operator_for_(index,open,actual,state_->entries[index].recipe)||
       !proof.model_phase_for_(actual,observer_delivery_open_startup::model_preimage_phase::construction)||
       !proof.state_)return false;
    const auto& entry=state_->entries[index];
    const ordinary_identifier_less less;const auto same=[&](std::string_view a,std::string_view b)noexcept{return !less(a,b)&&!less(b,a);};
    if(entry.model==state::model_kind::metadata_store||entry.model==state::model_kind::metadata_read||entry.model==state::model_kind::version)
        return proof.plain_table_for_(actual,"_lattice_meta");
    if(entry.model==state::model_kind::shadow) {
        const auto shape=std::find_if(proof.state_->facts.plain_shapes.begin(),proof.state_->facts.plain_shapes.end(),
            [&](const auto& value){return same(value.name,entry.model_owner);});
        if(shape==proof.state_->facts.plain_shapes.end())return false;
        for(const auto& column:shape->physical_columns)if(same(column.name,entry.model_object))
            return column.type=="text"&&column.not_null==0&&column.primary_key==0&&column.hidden==0&&!column.default_sql;
        return false;
    }
    const char* kind=entry.model==state::model_kind::trigger?"trigger":entry.model==state::model_kind::index?"index":"table";
    const auto& owner=entry.model==state::model_kind::trigger||entry.model==state::model_kind::index?entry.model_owner:entry.model_object;
    for(const auto& object:proof.state_->facts.main_objects)
        if(same(object.name,entry.model_object))return object.type==kind&&same(object.owner,owner);
    return false;
}
bool observer_delivery_initialization_preimage::system_metadata_for_(const database& actual,
    std::string_view name)const noexcept {
    if(!current_snapshot_(actual))return false;
    // No global table_list capability exists. Its unrelated module/view
    // xConnect/capture path is avoided by actual targeted shape witnesses.
    if(name!="pragma_table_xinfo"&&name!="pragma_database_list")return false;
    const ordinary_identifier_less less;
    for(const auto* objects:{&state_->facts.main_objects,&state_->facts.temp_objects})
        for(const auto& object:*objects)
            if(!less(object.name,name)&&!less(name,object.name))return false;
    // This proof covers actual namespace + retained view only. Builtin module
    // registration history is the separate unpublished constructor contract.
    return true;
}
std::shared_ptr<const observer_delivery_initialization_preimage>
observer_delivery_initialization_plan::capture_preimage_(const observer_delivery_open_startup& open,
    database& actual,const recovery_obligation_bootstrap_reader* bootstrap,
    preimage_selection selection,const observer_delivery_initialization_stage* model_stage,
    const observer_delivery_initialization_preimage* predecessor,std::size_t after_operator)const {
    try {
        open.require_work_current_(actual);
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
           state_->generation!=open.catalog_generation_())
            plan_refuse("initialization preimage catalog generation differs");
        // WHOLE bounded metadata procedure is reserved before any prospective
        // statement/result/validator/token copy. Separate fixed count ceilings
        // bound containers; this is requested structured storage, not RSS or a
        // portable claim about opaque allocator bookkeeping.
        const ordinary_metadata_limits caps{65536,262144,32*1024*1024,256*1024};
        constexpr std::uint64_t metadata_payload=32*1024*1024;
        constexpr std::uint64_t metadata_objects=65536;
        constexpr std::uint64_t metadata_rows=262144;
        const auto envelope=metadata_payload*3+
            metadata_objects*(sizeof(ordinary_object_metadata)+sizeof(std::string)*8)+
            metadata_rows*sizeof(database::row_t)+16*1024*1024+sizeof(observer_delivery_initialization_preimage::state);
        open.claim_workspace_(actual,envelope);
        ordinary_initialization_storage_scope::charge(native_retirement_storage<
            std::shared_ptr<const observer_delivery_initialization_preimage>>::allocation_bytes());
        native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>> result;
        if(creator_!=database_native_process())plan_refuse("inherited preimage procedure custody");
        ordinary_initialization_storage_scope::charge(sizeof(observer_delivery_initialization_preimage::state));
        auto* held=new observer_delivery_initialization_preimage::state(creator_);
        if(creator_!=database_native_process())plan_refuse("inherited preimage state allocation");
        try {
            held->plan_owner=this;held->actual=&actual;held->open=open.initialization_original_(actual);
            held->snapshot=actual.capture_initialization_snapshot_(open);
            if(creator_!=database_native_process())plan_refuse("inherited actual snapshot capture");
            if(!actual.initialization_schema_revision_(*held->snapshot,open,held->schema_revision))
                plan_refuse("initialization preimage schema revision unavailable");
            using sync_phase=observer_delivery_open_startup::static_sync_preimage_phase;
            std::optional<sync_phase> static_sync;
            switch(selection) {
              case preimage_selection::static_sync_set_create:static_sync=sync_phase::set_create;break;
              case preimage_selection::static_sync_set_present:static_sync=sync_phase::set_present;break;
              case preimage_selection::static_sync_state_create:static_sync=sync_phase::state_create;break;
              case preimage_selection::static_sync_state_present:static_sync=sync_phase::state_present;break;
              case preimage_selection::static_sync_state_index_create:static_sync=sync_phase::state_index_create;break;
              case preimage_selection::static_sync_state_index_present:static_sync=sync_phase::state_index_present;break;
              case preimage_selection::static_sync_slots_create:static_sync=sync_phase::slots_create;break;
              case preimage_selection::static_sync_slots_present:static_sync=sync_phase::slots_present;break;
              case preimage_selection::static_sync_upload_floor:static_sync=sync_phase::upload_floor;break;
              case preimage_selection::static_sync_upload_floor_present:static_sync=sync_phase::upload_floor_present;break;
              default:break;
            }
            using audit_phase=observer_delivery_open_startup::audit_preimage_phase;
            std::optional<audit_phase> audit;
            switch(selection) {
              case preimage_selection::audit_create:audit=audit_phase::table_create;break;
              case preimage_selection::audit_present:audit=audit_phase::table_present;break;
              case preimage_selection::audit_pending_create:audit=audit_phase::pending_create;break;
              case preimage_selection::audit_pending_present:audit=audit_phase::pending_present;break;
              case preimage_selection::audit_lookup_create:audit=audit_phase::lookup_create;break;
              case preimage_selection::audit_lookup_present:audit=audit_phase::lookup_present;break;
              default:break;
            }
            bool model_initial=selection==preimage_selection::model_construction_prefix;
            const bool model_selection=model_initial||selection==preimage_selection::model_required_present;
            bool existing_model_meter=false;
            if(selection!=preimage_selection::model_construction_prefix&&(model_stage||predecessor||after_operator!=std::size_t(-1)))
                plan_refuse("physical model stage cannot select a bootstrap/metadata/finish scope");
            bool audit_pending=false,audit_lookup=false;
            bool static_pending_index=false,static_upload=false,static_observer=false;
            const bool receive=selection==preimage_selection::receive_read_existing||
                selection==preimage_selection::receive_before_store||selection==preimage_selection::receive_before_channel||
                selection==preimage_selection::receive_before_index||selection==preimage_selection::receive_strict_seed;
            if(selection==preimage_selection::final_required_present||selection==preimage_selection::meter_install_entry) {
                if(bootstrap||model_stage||predecessor||after_operator!=std::size_t(-1))
                    plan_refuse("final normal proof cannot inherit bootstrap/model stage authority");
                held->scope=state_->final_projection;
                // Actual constructor requires cursor+upload_floor. The real
                // observer column is added later by its normal runtime producer,
                // so both complete compiled families remain compatible. Stored
                // SQL selects only a candidate; strict physical proof follows.
                auto inventory=observer_delivery_initialization_sql::fixed_(open,actual,
                    observer_delivery_initialization_sql::recipe::static_sync_inventory);
                using final_inventory_custody=native_retirement_storage<std::vector<database::row_t>>;
                open.claim_workspace_(actual,final_inventory_custody::allocation_bytes());final_inventory_custody rows;
                if(creator_!=database_native_process())plan_refuse("inherited final static inventory custody");
                rows.get<0>()=actual.query_initialization_(inventory);
                if(creator_!=database_native_process())plan_refuse("inherited final static inventory return");
                for(const auto& row:rows.get<0>()) {
                    const auto name=row.find("name"),sql=row.find("sql");
                    if(name==row.end()||!std::holds_alternative<std::string>(name->second))plan_refuse("final static inventory shape differs");
                    const auto& actual_name=std::get<std::string>(name->second);
                    if(sqlite3_stricmp(actual_name.c_str(),"_lattice_sync_set_v1")==0)plan_refuse("final historical rebuild is unfinished");
                    if(sqlite3_stricmp(actual_name.c_str(),"_lattice_replication_slots")!=0)continue;
                    if(sql==row.end()||!std::holds_alternative<std::string>(sql->second))plan_refuse("final slots declaration missing");
                    const auto& observed=std::get<std::string>(sql->second);constexpr std::string_view marker="is_observer";
                    if(std::search(observed.begin(),observed.end(),marker.begin(),marker.end(),[](unsigned char a,unsigned char b){return (a>='A'&&a<='Z'?a+32:a)==b;})!=observed.end())
                        held->scope.tables.at("_lattice_replication_slots").create_sql=ordinary_schema_recipe::replication_slots_after_upload_floor_and_observer_storage<ordinary_initialization_text>();
                }
                if(selection==preimage_selection::meter_install_entry) {
                    // Inventory selects only absent-versus-complete candidates;
                    // full physical/program/module/namespace proof below is
                    // mandatory before the installer may hold a continuation.
                    using inventory_custody=native_retirement_storage<std::vector<database::row_t>>;
                    open.claim_workspace_(actual,inventory_custody::allocation_bytes());inventory_custody objects;
                    if(creator_!=database_native_process())plan_refuse("inherited meter entry inventory custody");
                    auto emitted=ordinary_initialization_schema_validation::final_statement_(ordinary_initialization_schema_validation::final_read::meter_namespace);
                    auto descriptor=observer_delivery_initialization_sql::from_preimage_(emitted,open,actual);
                    objects.get<0>()=actual.query_initialization_(descriptor);
                    if(creator_!=database_native_process())plan_refuse("inherited meter entry inventory return");
                    const ordinary_identifier_less less;
                    const auto same=[&](std::string_view a,std::string_view b){return !less(a,b)&&!less(b,a);};
                    const auto field=[](const database::row_t& row,const char* name)->std::string_view {
                        const auto at=row.find(name);if(at==row.end()||!std::holds_alternative<std::string>(at->second))plan_refuse("meter entry inventory identity missing");
                        return std::get<std::string>(at->second);
                    };
                    bool present=false,reserved=false;
                    constexpr std::string_view prefix="_lattice_observer_delivery_";
                    const auto reserved_name=[&](std::string_view value){return value.size()>=prefix.size()&&same(value.substr(0,prefix.size()),prefix);};
                    for(const auto& row:objects.get<0>()) {
                        const auto name=field(row,"name"),owner=field(row,"owner");
                        if(same(name,observer_delivery_meter::ledger_name()))present=true;
                        if(reserved_name(name)||reserved_name(owner))reserved=true;
                    }
                    if(!present) {
                        if(reserved)plan_refuse("absent meter ledger has unknown/orphan reserved occupancy");
                        const auto ledger=held->scope.tables.find(observer_delivery_meter::ledger_name());
                        if(ledger==held->scope.tables.end())plan_refuse("immutable final meter declaration missing");held->scope.tables.erase(ledger);
                        for(auto& [name,table]:held->scope.tables)
                            for(auto it=table.other_programs.begin();it!=table.other_programs.end();) {
                                const auto trigger=ordinary_initialization_schema_validation::compiled_object_(*it,"trigger");
                                if(trigger.starts_with("_lattice_observer_delivery_before_v1_"))it=table.other_programs.erase(it);else ++it;
                            }
                    }
                    held->meter_present=present;
                }
            }else if(audit) {
                if(bootstrap)plan_refuse("AuditLog proof is not a bootstrap family proof");
                auto inventory=observer_delivery_initialization_sql::fixed_(open,actual,
                    observer_delivery_initialization_sql::recipe::audit_inventory);
                using inventory_custody=native_retirement_storage<std::vector<database::row_t>>;
                open.claim_workspace_(actual,inventory_custody::allocation_bytes());inventory_custody rows;
                if(creator_!=database_native_process())plan_refuse("inherited AuditLog inventory custody");
                rows.get<0>()=actual.query_initialization_(inventory);
                if(creator_!=database_native_process())plan_refuse("inherited AuditLog inventory return");
                if(rows.get<0>().size()>3)plan_refuse("AuditLog inventory exceeds its fixed family");
                const ordinary_identifier_less less;
                const auto same=[&](std::string_view a,std::string_view c){return !less(a,c)&&!less(c,a);};
                for(const auto& row:rows.get<0>()) {
                    const auto n=row.find("name");
                    if(n==row.end()||!std::holds_alternative<std::string>(n->second))
                        plan_refuse("AuditLog inventory result shape differs");
                    const auto& name=std::get<std::string>(n->second);
                    if(same(name,"idx_audit_log_pending_sync"))audit_pending=true;
                    if(same(name,"idx_audit_log_change_lookup"))audit_lookup=true;
                }
                const auto original=state_->preimage_projection.tables.find("AuditLog");
                if(original==state_->preimage_projection.tables.end())plan_refuse("actual compiled AuditLog family unavailable");
                auto table=original->second;table.index_programs.clear();
                if(audit_pending)table.index_programs.emplace_back(ordinary_schema_recipe::audit_pending_index());
                if(audit_lookup)table.index_programs.emplace_back(ordinary_schema_recipe::audit_lookup_index());
                held->scope.tables.emplace(original->first,std::move(table));
                // Inventory chooses only one compiled missing-index prefix.
                // The complete actual DDL/index/FK/namespace checks below are
                // mandatory; no observed SQL becomes an expected program.
            }else if(static_sync) {
                if(bootstrap)plan_refuse("static sync proof is not bootstrap family proof");
                auto inventory=observer_delivery_initialization_sql::fixed_(open,actual,
                    observer_delivery_initialization_sql::recipe::static_sync_inventory);
                using inventory_custody=native_retirement_storage<std::vector<database::row_t>>;
                open.claim_workspace_(actual,inventory_custody::allocation_bytes());inventory_custody inventory_rows;
                if(creator_!=database_native_process())plan_refuse("inherited static sync inventory custody");
                inventory_rows.get<0>()=actual.query_initialization_(inventory);
                if(creator_!=database_native_process())plan_refuse("inherited static sync inventory return");
                const auto& rows=inventory_rows.get<0>();
                // This bounded inventory selects only a compiled candidate.
                // Full strict metadata/index/FK/namespace proof below remains
                // mandatory. Observed SQL is never stored as an approved entry.
                const ordinary_identifier_less less;
                const auto same=[&](std::string_view a,std::string_view b){return !less(a,b)&&!less(b,a);};
                const auto has_floor=[](const std::string& sql)noexcept {
                    constexpr std::string_view word="upload_floor";
                    return std::search(sql.begin(),sql.end(),word.begin(),word.end(),[](unsigned char a,unsigned char b){
                        return (a>='A'&&a<='Z'?a+('a'-'A'):a)==b;
                    })!=sql.end();
                };
                const auto has_observer=[](const std::string& sql)noexcept {
                    constexpr std::string_view word="is_observer";
                    return std::search(sql.begin(),sql.end(),word.begin(),word.end(),[](unsigned char a,unsigned char b){
                        return (a>='A'&&a<='Z'?a+('a'-'A'):a)==b;
                    })!=sql.end();
                };
                if(rows.size()>5)plan_refuse("static sync inventory exceeds its fixed family");
                for(const auto& row:rows) {
                    const auto n=row.find("name"),s=row.find("sql");
                    if(n==row.end()||s==row.end()||!std::holds_alternative<std::string>(n->second))
                        plan_refuse("static sync inventory result shape differs");
                    const auto& name=std::get<std::string>(n->second);
                    if(same(name,"_lattice_sync_set_v1"))plan_refuse("unfinished historical sync-set rebuild cannot be adopted");
                    if(same(name,"idx_sync_state_pending"))static_pending_index=true;
                    if(same(name,"_lattice_replication_slots")) {
                        if(!std::holds_alternative<std::string>(s->second))plan_refuse("static sync slots declaration unavailable");
                        static_upload=has_floor(std::get<std::string>(s->second));
                        static_observer=has_observer(std::get<std::string>(s->second));
                        if(static_observer&&!static_upload)
                            plan_refuse("observer slots without the ordinary upload-floor prefix are unselected");
                    }
                }
                for(const char* name:{"_lattice_sync_set","_lattice_sync_state","_lattice_replication_slots"}) {
                    const auto table=state_->static_sync_projection.tables.find(name);
                    if(table==state_->static_sync_projection.tables.end())plan_refuse("actual compiled static sync family missing");
                    held->scope.tables.emplace(table->first,table->second);
                }
                if(static_pending_index)held->scope.tables.at("_lattice_sync_state").index_programs.emplace_back(ordinary_schema_recipe::sync_state_pending());
                if(static_observer)held->scope.tables.at("_lattice_replication_slots").create_sql=ordinary_schema_recipe::replication_slots_after_upload_floor_and_observer_storage<ordinary_initialization_text>();
                else if(static_upload)held->scope.tables.at("_lattice_replication_slots").create_sql=ordinary_schema_recipe::replication_slots_after_upload_floor_storage<ordinary_initialization_text>();
            }else if(receive) {
                if(bootstrap)plan_refuse("receive proof is not bootstrap family proof");
                const auto add=[&](const char* name,const char* ddl) ->ordinary_projection_table& {
                    ordinary_projection_table table;table.name=name;table.create_sql=ddl;
                    table.role=ordinary_projection_role::link;table.local_id=false;
                    return held->scope.tables.emplace(name,std::move(table)).first->second;
                };
                add("_lattice_receive_guard_store",ordinary_schema_recipe::receive_store());
                auto& channel=add("_lattice_receive_guard",ordinary_schema_recipe::receive_channel());
                if(selection!=preimage_selection::receive_before_index)
                    channel.index_programs.emplace_back(ordinary_schema_recipe::receive_index());
            }else if(selection==preimage_selection::sync_control_create||selection==preimage_selection::sync_control_seed) {
                if(bootstrap)plan_refuse("sync-control proof is not a bootstrap family proof");
                const auto control=state_->preimage_projection.tables.find("_SyncControl");
                if(control==state_->preimage_projection.tables.end())plan_refuse("actual compiled sync-control recipe unavailable");
                held->scope.tables.emplace(control->first,control->second);
            }else if(selection==preimage_selection::schema_metadata_read||
                     selection==preimage_selection::schema_metadata_create||selection==preimage_selection::schema_metadata_seed||
                     selection==preimage_selection::schema_metadata_marker_present) {
                if(bootstrap)plan_refuse("schema metadata proof is not a bootstrap family proof");
                const auto meta=state_->preimage_projection.tables.find("_lattice_meta");
                if(meta==state_->preimage_projection.tables.end())plan_refuse("actual compiled metadata recipe unavailable");
                // Only the actual compiled metadata declaration enters this
                // SELECT-only proof. A model needing migration is not mistaken
                // for authority to read its rows, nor a reason to skip setup.
                held->scope.tables.emplace(meta->first,meta->second);
            }else {
                held->scope=state_->preimage_projection;
                if(selection==preimage_selection::model_construction_prefix) {
                    if(predecessor||after_operator!=std::size_t(-1)) {
                        if(!predecessor||after_operator>=state_->entries.size()||predecessor->creator_!=creator_||
                           !predecessor->state_||predecessor->state_->creator!=creator_||predecessor->state_->plan_owner!=this||
                           predecessor->state_->actual!=&actual||predecessor->state_->open.get()!=&open||
                           predecessor->state_->snapshot!=held->snapshot||!predecessor->state_->model_phase||
                           *predecessor->state_->model_phase!=observer_delivery_open_startup::model_preimage_phase::construction||
                           predecessor->state_->schema_revision==std::numeric_limits<std::uint64_t>::max()||
                           held->schema_revision!=predecessor->state_->schema_revision+1)
                            plan_refuse("model postimage is not one actual attempt after SAME Plan/Open/raw episode predecessor");
                        const auto& entry=state_->entries[after_operator];
                        if(entry.model!=state::model_kind::table&&entry.model!=state::model_kind::trigger&&entry.model!=state::model_kind::index&&
                           entry.model!=state::model_kind::rtree&&entry.model!=state::model_kind::fts5&&entry.model!=state::model_kind::shadow)
                            plan_refuse("non-DDL operator cannot derive physical construction stage");
                        held->scope=predecessor->state_->scope;model_initial=false;
                        if(entry.model==state::model_kind::table&&plan_contains_name_(predecessor->state_->facts.absent_tables,std::string_view(entry.model_object))) {
                            const auto at=held->scope.tables.find(std::string_view(entry.model_object));
                            const auto full=state_->preimage_projection.tables.find(std::string_view(entry.model_object));
                            if(at==held->scope.tables.end()||full==state_->preimage_projection.tables.end())plan_refuse("new model table lost its immutable declaration");
                            // Actual original CREATE contains no later unique-link
                            // shadow columns. This precise target transition may
                            // expect its base image; unrelated tables do not vary.
                            at->second.shadow_columns.clear();
                        }else if(entry.model==state::model_kind::shadow) {
                            const auto at=held->scope.tables.find(std::string_view(entry.model_owner));
                            const auto full=state_->preimage_projection.tables.find(std::string_view(entry.model_owner));
                            if(at==held->scope.tables.end()||full==state_->preimage_projection.tables.end()||
                               !plan_contains_name_(full->second.shadow_columns,std::string_view(entry.model_object))||
                               plan_contains_name_(at->second.shadow_columns,std::string_view(entry.model_object)))
                                plan_refuse("shadow ADD lacks exact declared absent predecessor column");
                            auto selected=at->second.shadow_columns;selected.emplace_back(entry.model_object.data(),entry.model_object.size());at->second.shadow_columns.clear();
                            for(const auto& column:full->second.shadow_columns)
                                if(std::find(selected.begin(),selected.end(),column)!=selected.end())at->second.shadow_columns.push_back(column);
                        }
                    }else if(model_stage) {
                        if(model_stage->creator_!=creator_||!model_stage->state_||model_stage->state_->creator!=creator_||
                           model_stage->state_->plan!=this||model_stage->state_->generation!=state_->generation||model_stage->state_->actual!=&actual||model_stage->state_->original!=&open)
                            plan_refuse("model physical stage belongs to another actual Plan/Open database");
                        if(!model_stage->state_->episode.owner_before(held->snapshot)&&!held->snapshot.owner_before(model_stage->state_->episode)) {
                            for(const auto& columns:model_stage->state_->physical) {
                                const auto at=std::find_if(held->scope.tables.begin(),held->scope.tables.end(),[&](const auto& table){
                                    return std::string_view(table.first)==std::string_view(columns.table.data(),columns.table.size());});
                                if(at==held->scope.tables.end()||at->second.role!=ordinary_projection_role::model)
                                    plan_refuse("model physical stage target left retained declaration");
                                // Select an ordered subset of the ALREADY copied
                                // full declaration without new std::string or
                                // vector allocations. Erasure only retires DATA.
                                auto& expected=at->second.shadow_columns;std::size_t wanted=0;
                                for(auto column=expected.begin();column!=expected.end();) {
                                    if(wanted<columns.present.size()&&std::string_view(*column)==
                                       std::string_view(columns.present[wanted].data(),columns.present[wanted].size())) {
                                        ++wanted;++column;
                                    }else column=expected.erase(column);
                                }
                                if(wanted!=columns.present.size())plan_refuse("retained model stage is not an ordered declared subset");
                            }
                            model_initial=false;
                        }
                        // An old weak episode is only DATA. A successor must
                        // earn the exact initial base/current proof anew; its
                        // old intermediate state cannot survive an ending.
                    }
                }
            }
            if(model_selection) {
                // Presence is only a candidate selector. A guarded store must
                // prove ONE complete canonical family below; a partial table,
                // familiar prefix or observed program never enters expected SQL.
                using inventory_custody=native_retirement_storage<std::vector<database::row_t>>;
                open.claim_workspace_(actual,inventory_custody::allocation_bytes());inventory_custody rows;
                if(creator_!=database_native_process())plan_refuse("inherited model meter inventory custody");
                auto emitted=ordinary_initialization_schema_validation::final_statement_(ordinary_initialization_schema_validation::final_read::meter_namespace);
                auto descriptor=observer_delivery_initialization_sql::from_preimage_(emitted,open,actual);
                rows.get<0>()=actual.query_initialization_(descriptor);
                if(creator_!=database_native_process())plan_refuse("inherited model meter inventory return");
                std::uint64_t revision=0;
                if(!actual.initialization_schema_revision_(*held->snapshot,open,revision)||revision!=held->schema_revision)
                    plan_refuse("model meter inventory left its SAME actual BEGIN/revision");
                const ordinary_identifier_less less;
                const auto same=[&](std::string_view a,std::string_view b){return !less(a,b)&&!less(b,a);};
                constexpr std::string_view prefix="_lattice_observer_delivery_";
                const auto reserved=[&](std::string_view name){return name.size()>=prefix.size()&&same(name.substr(0,prefix.size()),prefix);};
                const auto field=[](const database::row_t& row,const char* key)->std::string_view {
                    const auto at=row.find(key);if(at==row.end()||!std::holds_alternative<std::string>(at->second))plan_refuse("model meter inventory identity missing");
                    return std::get<std::string>(at->second);
                };
                bool occupied=false;
                for(const auto& row:rows.get<0>()) {
                    const auto name=field(row,"name"),owner=field(row,"owner");
                    if(same(name,observer_delivery_meter::ledger_name()))existing_model_meter=true;
                    if(reserved(name)||reserved(owner))occupied=true;
                }
                if(!existing_model_meter&&occupied)plan_refuse("model prefix refuses orphan/unknown meter occupancy");
                if(existing_model_meter) {
                    // Existing guarded catalogs are current complete families,
                    // not physical construction prefixes. Ignore no missing
                    // shadow/index/table via an old stage or predecessor.
                    held->scope=observer_delivery_schema::model_meter_variant_(state_->projection,state_->preimage_projection);
                    if(creator_!=database_native_process())plan_refuse("inherited complete model meter variant generation");
                    model_initial=false;
                }
            }
            const bool enrollment=selection==preimage_selection::enrollment_ledger_prefix||
                selection==preimage_selection::enrollment_ledger_complete;
            if(enrollment) {
                if(!bootstrap||!bootstrap->enrollment_transaction_live_())
                    plan_refuse("ledger proof requires the actual retained enrollment WRITE original");
                bootstrap->require_enrollment_current_();
            }
            if(creator_!=database_native_process())plan_refuse("inherited preimage scope copy");
            // Only the actual private bootstrap recipe issuer can append its
            // genuine complete compiled table/index families, never observed
            // definitions, supplied expected values, SQL text or grant flags.
            if(bootstrap) {
                if(enrollment)bootstrap->append_enrollment_ledger_tables_(held->scope);
                else bootstrap->append_preimage_tables_(held->scope);
            }
            if(creator_!=database_native_process())plan_refuse("inherited bootstrap family recipe return");
            const auto query=
                [&](const std::shared_ptr<const ordinary_preimage_statement>& statement) {
                    std::uint64_t revision=0;
                    if(creator_!=database_native_process()||
                       !actual.initialization_schema_revision_(*held->snapshot,open,revision)||revision!=held->schema_revision)
                        plan_refuse("initialization preimage lost its retained actual view");
                    auto entry=observer_delivery_initialization_sql::from_preimage_(statement,open,actual);
                    auto result=actual.query_initialization_(entry);
                    if(static_sync&&statement->kind_id()==ordinary_preimage_statement::kind::table_columns) {
                        const auto name=statement->parameter_count()==1?statement->parameter_text(0):std::string_view{};
                        if(name.empty()||!(static_observer?ordinary_schema_recipe::static_sync_observer_columns_match(name,result):
                            ordinary_schema_recipe::static_sync_columns_match(name,static_upload,result)))
                            plan_refuse("static sync actual physical column order differs from its compiled family");
                    }
                    if(audit&&statement->kind_id()==ordinary_preimage_statement::kind::table_columns) {
                        const auto name=statement->parameter_count()==1?statement->parameter_text(0):std::string_view{};
                        if(name!="AuditLog"||!ordinary_schema_recipe::audit_columns_match(result))
                            plan_refuse("AuditLog actual physical column order differs from its compiled producer");
                    }
                    return result;
                };
            // Only the explicit bootstrap reader may select READ-only plain
            // table evidence before it regenerates enrolled trigger bundles.
            // This result carries no trigger origins or DML/seal authority.
            if(existing_model_meter||selection==preimage_selection::final_required_present||selection==preimage_selection::meter_install_entry)
                held->facts=ordinary_initialization_schema_validation::required_present_(held->scope,query,caps);
            else if(selection==preimage_selection::enrollment_ledger_prefix)
                held->facts=ordinary_initialization_schema_validation::construction_prefix_(held->scope,query,caps);
            else if(selection==preimage_selection::enrollment_ledger_complete)
                held->facts=ordinary_initialization_schema_validation::required_present_(held->scope,query,caps);
            else if(selection==preimage_selection::model_construction_prefix) {
                if(model_initial)held->facts=ordinary_initialization_schema_validation::construction_initial_(held->scope,query,caps);
                else held->facts=ordinary_initialization_schema_validation::construction_prefix_(held->scope,query,caps);
            }
            else if(selection==preimage_selection::model_required_present)
                held->facts=ordinary_initialization_schema_validation::required_present_(held->scope,query,caps);
            else
            held->facts=(bootstrap||selection==preimage_selection::schema_metadata_read||
                selection==preimage_selection::schema_metadata_create||selection==preimage_selection::sync_control_create||
                (receive&&selection!=preimage_selection::receive_strict_seed))?
                ordinary_initialization_schema_validation::read_tables_(held->scope,query,caps):
                ordinary_initialization_schema_validation::preexisting_(held->scope,query,caps);
            std::uint64_t final_revision=0;
            if(creator_!=database_native_process()||
               !actual.initialization_schema_revision_(*held->snapshot,open,final_revision)||final_revision!=held->schema_revision)
                plan_refuse("initialization preimage returned outside its actual view/revision");
            if(existing_model_meter) {
                // The ordinary required-present proof closes every actual
                // declaration/index/FK/program/TEMP namespace. Require exact
                // canonical meter bytes too; token equivalence or a count
                // alone is not an existing guard registration/proof.
                observer_delivery_schema::require_model_meter_objects_(state_->projection,held->facts);
                if(creator_!=database_native_process())plan_refuse("inherited exact existing model meter proof");
                using rows_custody=native_retirement_storage<std::vector<database::row_t>,std::vector<database::row_t>,std::array<char,35>>;
                ordinary_initialization_storage_scope::charge(rows_custody::allocation_bytes());rows_custody rows;
                if(creator_!=database_native_process())plan_refuse("inherited existing model meter metadata custody");
                const auto current=[&] {
                    open.require_work_current_(actual);std::uint64_t revision=0;
                    if(creator_!=database_native_process()||!actual.initialization_schema_revision_(*held->snapshot,open,revision)||revision!=held->schema_revision)
                        plan_refuse("existing model meter metadata lost SAME actual BEGIN/revision");
                };
                current();
                {
                    store_physical_native_scope physical(actual);if(!physical.entered())plan_refuse("existing model meter Native original unavailable");
                    database::catalog_native_admission_ leaf(actual);current();auto* native=actual.db_;
                    if(!native||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE)
                        plan_refuse("existing model meter requires its actual privately issued WRITE");
                    current();
                }
                rows.get<2>()=open.model_meter_marker_key_(actual);
                if(creator_!=database_native_process())plan_refuse("inherited actual model meter marker producer");
                const auto& catalog=open.initialization_catalog_(actual);const auto target=open.initialization_schema_version_(actual);
                if(!catalog.valid()||target<=0||target>INT32_MAX||held->facts.schema_cookie<0||held->facts.schema_cookie>INT32_MAX)
                    plan_refuse("existing model meter catalog/target/cookie domain differs");
                using read=ordinary_initialization_schema_validation::final_read;
                const auto query_rows=[&](read kind,std::vector<database::row_t>& output) {
                    current();auto emitted=ordinary_initialization_schema_validation::final_statement_(kind);
                    if(creator_!=database_native_process())plan_refuse("inherited existing model metadata recipe");
                    auto descriptor=observer_delivery_initialization_sql::from_preimage_(emitted,open,actual);
                    output=actual.query_initialization_(descriptor);
                    if(creator_!=database_native_process())plan_refuse("inherited actual existing model metadata return");current();
                };
                const auto cell=[](const database::row_t& row,const char* key)->const column_value_t& {
                    const auto at=row.find(key);if(at==row.end())plan_refuse("existing model meter result field missing");return at->second;
                };
                const auto integer=[&](const database::row_t& row,const char* key) {
                    const auto* value=std::get_if<std::int64_t>(&cell(row,key));if(!value)plan_refuse("existing model meter requires actual INTEGER");return *value;
                };
                const auto text=[&](const database::row_t& row,const char* key)->std::string_view {
                    const auto* value=std::get_if<std::string>(&cell(row,key));
                    if(!value||value->find('\0')!=std::string::npos)plan_refuse("existing model meter requires actual NUL-free TEXT");return *value;
                };
                query_rows(read::meter_row,rows.get<0>());
                const auto& meter=rows.get<0>();
                if(meter.size()!=1||meter.front().size()!=5||integer(meter.front(),"id")!=1||text(meter.front(),"token_type")!="blob"||
                   integer(meter.front(),"token_bytes")!=32||integer(meter.front(),"events")<0||integer(meter.front(),"bytes")<0)
                    plan_refuse("existing model meter row/types/counters differ");
                query_rows(read::metadata_rows,rows.get<1>());
                const auto cookie=ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(held->facts.schema_cookie));
                const auto version=ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(target));
                const std::string_view core_marker(rows.get<2>().data(),rows.get<2>().size());
                ordinary_initialization_text_map<const state::approved*> expected_metadata;
                for(const auto& entry:state_->entries) {
                    current();
                    if(entry.model==state::model_kind::metadata_read&&!entry.fresh_only&&!expected_metadata.emplace(entry.model_object,&entry).second)
                        plan_refuse("compiled existing model metadata key collision");
                }
                bool seen_version=false;std::size_t markers=0;ordinary_initialization_text_set seen_metadata;
                for(const auto& row:rows.get<1>()) {
                    current();const auto key=text(row,"key");
                    if(key=="schema_version") {
                        if(seen_version||text(row,"value")!=std::string_view(version))plan_refuse("existing guarded model requires actual current target version");
                        seen_version=true;continue;
                    }
                    if(key.starts_with("schema_fingerprint:")) {
                        if(++markers!=1||(key!=core_marker&&(catalog.swift_fingerprint.empty()||key!=catalog.swift_fingerprint))||
                           text(row,"value")!=std::string_view(cookie))plan_refuse("existing guarded model surviving marker is missing/foreign/stale");
                        continue;
                    }
                    const auto expected=expected_metadata.find(key);
                    if(expected!=expected_metadata.end()&&(text(row,"value")!=std::string_view(expected->second->model_value)||!seen_metadata.emplace(key.data(),key.size()).second))
                        plan_refuse("existing guarded model metadata differs from same captured catalog");
                }
                if(!seen_version||markers!=1)plan_refuse("existing guarded model current version/marker missing");
                if(seen_metadata.size()!=expected_metadata.size())plan_refuse("existing guarded model required retained metadata row missing");
                current();
            }
            held->plain_tables.reserve(held->scope.tables.size());
            if(creator_!=database_native_process())plan_refuse("inherited preimage table vector allocation");
            if(held->plain_tables.capacity()>ordinary_sync_limits::projection_tables+16)
                plan_refuse("initialization preimage table vector capacity bound");
            const ordinary_identifier_less less;
            for(const auto& [name,unused]:held->scope.tables) {
                // The exact validator proved every PRESENT expected ordinary
                // table's real closed DDL/engine metadata/program/FK closure.
                // An unrelated inventory object or module cannot enter this set.
                if(std::binary_search(held->facts.absent_tables.begin(),held->facts.absent_tables.end(),name))continue;
                held->plain_tables.push_back(name);
            }
            std::sort(held->plain_tables.begin(),held->plain_tables.end(),less);
            if(enrollment) {
                held->enrollment_reader=bootstrap;
                held->enrollment_phase=selection==preimage_selection::enrollment_ledger_complete?
                    observer_delivery_open_startup::enrollment_ledger_phase::complete:
                    observer_delivery_open_startup::enrollment_ledger_phase::construction;
                for(const auto& [name,table]:held->scope.tables)
                    for(const auto& sql:table.index_programs) {
                        if(held->enrollment_indexes.size()==65536)plan_refuse("ledger proof compiled-index corpus bound");
                        held->enrollment_indexes.emplace_back(name,ordinary_initialization_schema_validation::compiled_object_(sql,"index"));
                    }
                if(creator_!=database_native_process())plan_refuse("inherited ledger compiled-index corpus");
            }
            if(selection==preimage_selection::model_construction_prefix||selection==preimage_selection::model_required_present) {
                held->model_phase=selection==preimage_selection::model_required_present?
                    observer_delivery_open_startup::model_preimage_phase::complete:observer_delivery_open_startup::model_preimage_phase::construction;
                // Reached only after full actual scope validation, SAME raw
                // view/revision and (for guarded stores) exact ledger/program,
                // row/counter/current-version/surviving-marker checks above.
                held->model_stage_family=existing_model_meter?
                    observer_delivery_schema::model_stage_family::complete_meter:
                    observer_delivery_schema::model_stage_family::ordinary_construction;
            }
            if(audit) {
                const bool present=std::binary_search(held->plain_tables.begin(),held->plain_tables.end(),std::string_view("AuditLog"),less);
                switch(*audit) {
                  case audit_phase::table_create:break;
                  case audit_phase::table_present:
                    if(!present)plan_refuse("AuditLog CREATE postimage missing");break;
                  case audit_phase::pending_create:case audit_phase::lookup_create:
                    if(!present)plan_refuse("AuditLog index requires its actual current table");break;
                  case audit_phase::pending_present:
                    if(!present||!audit_pending)plan_refuse("AuditLog pending-index postimage missing");break;
                  case audit_phase::lookup_present:
                    if(!present||!audit_lookup)plan_refuse("AuditLog lookup-index postimage missing");break;
                }
                held->audit_phase=*audit;
            }
            if(static_sync) {
                const auto has=[&](const char* name){return std::binary_search(held->plain_tables.begin(),held->plain_tables.end(),std::string_view(name),less);};
                for(const auto* objects:{&held->facts.main_objects,&held->facts.temp_objects})for(const auto& object:*objects)
                    if((!less(object.name,"_lattice_sync_set_v1")&&!less("_lattice_sync_set_v1",object.name))||
                       (!less(object.owner,"_lattice_sync_set_v1")&&!less("_lattice_sync_set_v1",object.owner)))
                        plan_refuse("static sync historical rebuild namespace is not a completed family");
                switch(*static_sync) {
                  case sync_phase::set_present:if(!has("_lattice_sync_set"))plan_refuse("static sync set postimage missing");break;
                  case sync_phase::state_present:if(!has("_lattice_sync_state"))plan_refuse("static sync state postimage missing");break;
                  case sync_phase::state_index_create:if(!has("_lattice_sync_state"))plan_refuse("static sync index requires its actual state table");break;
                  case sync_phase::state_index_present:if(!has("_lattice_sync_state")||!static_pending_index)plan_refuse("static sync partial-index postimage missing");break;
                  case sync_phase::slots_present:case sync_phase::upload_floor:
                    if(!has("_lattice_replication_slots"))plan_refuse("static sync slots pre/postimage missing");break;
                  case sync_phase::upload_floor_present:
                    if(!has("_lattice_replication_slots")||!static_upload)plan_refuse("static sync upload-floor postimage missing");break;
                  default:break;
                }
                held->static_sync_phase=*static_sync;
                held->static_sync_upload_current=static_upload&&has("_lattice_replication_slots");
            }
            if(selection==preimage_selection::schema_metadata_create||selection==preimage_selection::schema_metadata_seed||
               selection==preimage_selection::schema_metadata_marker_present) {
                using phase=observer_delivery_open_startup::metadata_preimage_phase;
                const bool present=std::binary_search(held->plain_tables.begin(),held->plain_tables.end(),std::string_view("_lattice_meta"),less);
                if(selection!=preimage_selection::schema_metadata_create&&!present)
                    plan_refuse("metadata write requires an actual strict present table");
                held->metadata_phase=selection==preimage_selection::schema_metadata_marker_present?phase::marker_write:
                    selection==preimage_selection::schema_metadata_seed?phase::seed_version:phase::create_table;
            }
            if(selection==preimage_selection::sync_control_create||selection==preimage_selection::sync_control_seed) {
                using phase=observer_delivery_open_startup::sync_control_preimage_phase;
                const bool present=std::binary_search(held->plain_tables.begin(),held->plain_tables.end(),std::string_view("_SyncControl"),less);
                if(selection==preimage_selection::sync_control_seed&&!present)
                    plan_refuse("sync-control seed requires an actual strict present table");
                held->sync_control_phase=selection==preimage_selection::sync_control_seed?phase::seed_row:phase::create_table;
            }
            if(receive) {
                const auto has=[&](const char* name){return std::binary_search(held->plain_tables.begin(),held->plain_tables.end(),std::string_view(name),less);};
                const bool store=has("_lattice_receive_guard_store"),channel=has("_lattice_receive_guard");
                using phase=observer_delivery_open_startup::receive_preimage_phase;
                switch(selection) {
                  case preimage_selection::receive_before_store:
                    if(store||channel)plan_refuse("receive fresh family was not actually absent");
                    held->receive_phase=phase::before_create_store;break;
                  case preimage_selection::receive_before_channel:
                    if(!store||channel)plan_refuse("receive store-only prefix differs");
                    held->receive_phase=phase::before_create_channel;break;
                  case preimage_selection::receive_before_index:
                    if(!store||!channel)plan_refuse("receive pre-index prefix differs");
                    held->receive_phase=phase::before_create_index;break;
                  case preimage_selection::receive_strict_seed:
                    if(!store||!channel)plan_refuse("receive strict complete seed preimage missing");
                    held->receive_phase=phase::strict_seed;break;
                  case preimage_selection::receive_read_existing:
                    if(!store||!channel)plan_refuse("receive existing complete plain family missing");
                    held->receive_phase=phase::read_existing;break;
                  default:plan_refuse("receive preimage selector differs");
                }
            }
            ordinary_initialization_storage_scope::charge(sizeof(observer_delivery_initialization_preimage));
            auto* wrapper=new observer_delivery_initialization_preimage(creator_,held);
            if(creator_!=database_native_process())plan_refuse("inherited preimage receipt allocation");
            held=nullptr;
            result.get<0>()=std::shared_ptr<const observer_delivery_initialization_preimage>(wrapper,
                &observer_delivery_initialization_preimage::destroy_,
                ordinary_initialization_allocator<observer_delivery_initialization_preimage>{});
            if(creator_!=database_native_process())plan_refuse("inherited preimage receipt publication");
            return std::move(result.get<0>());
        }catch(...) {if(creator_==database_native_process())delete held;throw;}
    }catch(...) {open.fail_initialization_();throw;}
}
bool observer_delivery_initialization_plan::meter_current_for_(const observer_delivery_initialization_meter& original,
    const observer_delivery_open_startup& open,const database& actual,std::size_t index)const noexcept {
    if(creator_!=database_native_process()||original.creator_!=creator_||!original.state_||
       original.state_->creator!=creator_||!state_||state_->creator!=creator_||!actual.creator_current_())return false;
    const auto& held=*original.state_;
    if(held.builder!=std::this_thread::get_id()||held.failed||!held.active||held.plan.get()!=this||
       held.open.get()!=&open||held.actual!=&actual||!held.snapshot||!held.entry_proof||
       held.position>=held.operations.size()||held.operations[held.position]!=index||index>=state_->entries.size()||
       actual.closed_.load(std::memory_order_acquire)||actual.raw_handle_escaped_.load(std::memory_order_acquire)||
       actual.catalog_quarantined_()||actual.db_!=held.native||actual.observer_delivery_sql_.get()!=held.runtime||
       state_->generation!=open.catalog_generation_()||!open.pending_for_(actual))return false;
    std::uint64_t revision=0;
    return actual.initialization_schema_revision_(*held.snapshot,open,revision)&&revision==held.revision;
}
void observer_delivery_initialization_plan::meter_entry_(const observer_delivery_initialization_meter& original,
    std::size_t index,const observer_delivery_open_startup& open,database& actual,
    observer_delivery_initialization_sql::recipe& recipe,std::string_view& sql)const {
    open.require_work_current_(actual);
    if(!meter_current_for_(original,open,actual,index))plan_refuse("meter entry lost its genuine active continuation");
    const auto& entry=state_->entries[index];
    using kind=state::model_kind;
    if(entry.model!=kind::meter_table&&entry.model!=kind::meter_seed&&entry.model!=kind::meter_trigger&&entry.model!=kind::meter_marker)
        plan_refuse("meter continuation selected a different compiler family");
    recipe=entry.recipe;sql=entry.sql;
}
void observer_delivery_initialization_plan::meter_parameters_(const observer_delivery_initialization_meter& original,
    std::size_t index,const observer_delivery_open_startup& open,database& actual,
    ordinary_initialization_vector<ordinary_initialization_text>& output)const {
    open.require_work_current_(actual);
    if(!meter_current_for_(original,open,actual,index)||!output.empty())plan_refuse("meter parameters lost actual empty retained entry");
    if(state_->entries[index].model!=state::model_kind::meter_marker)return;
    const auto& held=*original.state_;
    if(held.new_cookie.empty()||held.new_cookie.size()>10||held.old_cookie<0||held.old_cookie>INT32_MAX)
        plan_refuse("meter CAS lacks its real same-Native schema cookie");
    output.reserve(3);
    if(creator_!=database_native_process())plan_refuse("inherited meter CAS parameter storage");
    output.push_back(held.new_cookie);
    if(creator_!=database_native_process())plan_refuse("inherited meter CAS new-cookie copy");
    output.emplace_back(held.marker.data(),held.marker.size());
    if(creator_!=database_native_process())plan_refuse("inherited meter CAS surviving-key copy");
    output.push_back(ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(held.old_cookie)));
    if(creator_!=database_native_process())plan_refuse("inherited meter CAS old-cookie copy");
}
int observer_delivery_initialization_plan::meter_authorize_(const observer_delivery_initialization_meter& original,
    std::size_t index,const observer_delivery_open_startup& open,const database& actual,
    int action,const char* first,const char* second,const char* schema)const noexcept {
    if(!meter_current_for_(original,open,actual,index))return SQLITE_DENY;
    const auto same=[](const char* value,std::string_view expected)noexcept {
        if(!value)return expected.empty();std::size_t at=0;
        for(;at<expected.size();++at){if(!value[at])return false;unsigned char a=value[at],b=expected[at];
            if(a>='A'&&a<='Z')a+=32;if(b>='A'&&b<='Z')b+=32;if(a!=b)return false;}
        return !value[at];
    };
    const auto& entry=state_->entries[index];using kind=state::model_kind;
    if(entry.model!=kind::meter_table&&entry.model!=kind::meter_seed&&entry.model!=kind::meter_trigger&&entry.model!=kind::meter_marker)return SQLITE_DENY;
    if(action==SQLITE_FUNCTION&&(!schema||!*schema||same(schema,"main"))&&(!first||!*first)) {
        if(entry.model==kind::meter_table&&same(second,"typeof"))return SQLITE_OK;
        if(entry.model==kind::meter_seed&&(same(second,"typeof")||same(second,"zeroblob")))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(!same(schema,"main"))return SQLITE_DENY;
    const bool catalog=same(first,"sqlite_master")||same(first,"sqlite_schema");
    if(entry.model==kind::meter_marker) {
        if(!same(first,"_lattice_meta"))return SQLITE_DENY;
        if(action==SQLITE_UPDATE&&same(second,"value"))return SQLITE_OK;
        if(action==SQLITE_READ&&(same(second,"key")||same(second,"value")))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(entry.model==kind::meter_seed) {
        if(action==SQLITE_INSERT&&same(first,observer_delivery_meter::ledger_name()))return SQLITE_OK;
        if(action==SQLITE_READ&&same(first,observer_delivery_meter::ledger_name())&&
           (same(second,"id")||same(second,"events")||same(second,"bytes")))return SQLITE_OK;
        return SQLITE_DENY;
    }
    if(entry.model==kind::meter_table&&action==SQLITE_CREATE_TABLE&&same(first,entry.model_object))return SQLITE_OK;
    if(entry.model==kind::meter_trigger&&action==SQLITE_CREATE_TRIGGER&&same(first,entry.model_object)&&same(second,entry.model_owner))return SQLITE_OK;
    if(action==SQLITE_INSERT&&catalog)return SQLITE_OK;
    if(action==SQLITE_UPDATE&&catalog)for(const char* column:{"type","name","tbl_name","rootpage","sql"})if(same(second,column))return SQLITE_OK;
    if(action==SQLITE_READ&&catalog)for(const char* column:{"type","name","tbl_name","rootpage","sql","ROWID"})if(same(second,column))return SQLITE_OK;
    if(entry.model==kind::meter_table&&action==SQLITE_READ&&same(first,observer_delivery_meter::ledger_name())&&
       (same(second,"id")||same(second,"events")||same(second,"bytes")))return SQLITE_OK;
    return SQLITE_DENY;
}
void observer_delivery_initialization_plan::install_final_meter_(const observer_delivery_open_startup& open,
    database& actual,const recovery_owner_schema& catalog,std::int64_t target,std::string_view core_marker)const {
    try {
        open.require_work_current_(actual);
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        using custody=native_retirement_storage<meter_entry_evidence,std::shared_ptr<observer_delivery_initialization_meter>,std::vector<database::row_t>>;
        ordinary_initialization_storage_scope::charge(custody::allocation_bytes());custody retained;
        if(creator_!=database_native_process())plan_refuse("inherited meter installation custody");
        // The real leaves remain held across every checked Native operation
        // and continuation transition. No user callback, external owner lock,
        // guessed handle pin or intermediate publication supplies this custody.
        store_physical_native_scope physical(actual);if(!physical.entered())plan_refuse("meter physical original unavailable");
        // Establish the actual Core SQL ancestor BEFORE taking Native. Nested
        // requests cannot enter topology waiting while this outer Native leaf
        // is held; any earlier admission uses the SAME original finite end.
        database::sql_entry_scope request_family(actual,{},open.original_deadline_());
        database::catalog_native_admission_ leaf(actual);
        require_normal_state_(open,actual,catalog,target,core_marker,&retained.get<0>());
        if(creator_!=database_native_process())plan_refuse("inherited actual meter entry proof return");
        if(retained.get<0>().present)return; // complete exact family; no healing/reseed/CAS
        open.require_final_meter_writer_(actual);
        auto* native=actual.db_;
        if(!native||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE)
            plan_refuse("meter installation requires its actual privately issued WRITE original");
        ordinary_initialization_storage_scope::charge(sizeof(observer_delivery_initialization_meter::state));
        auto* held=new observer_delivery_initialization_meter::state;
        if(creator_!=database_native_process())plan_refuse("inherited meter continuation allocation");
        observer_delivery_initialization_meter* wrapper=nullptr;bool wrapped=false;
        try {
            held->plan=open.initialization_plan_(actual);
            if(held->plan.get()!=this)plan_refuse("meter installer differs from the actual retained Plan");
            held->open=open.initialization_original_(actual);held->actual=&actual;held->native=native;
            held->runtime=actual.observer_delivery_sql_.get();held->entry_proof=retained.get<0>().proof;
            held->snapshot=actual.capture_initialization_snapshot_(open);held->marker=retained.get<0>().marker;held->old_cookie=retained.get<0>().cookie;
            if(!held->snapshot||!held->entry_proof||!held->entry_proof->state_->meter_present||*held->entry_proof->state_->meter_present||
               !held->entry_proof->current_snapshot_(actual)||held->entry_proof->state_->plan_owner!=this||
               !actual.initialization_schema_revision_(*held->snapshot,open,held->revision))
                plan_refuse("meter continuation lost actual strict absence/BEGIN/revision proof");
            const auto append_one=[&](state::model_kind kind) {
                std::size_t found=state_->entries.size();
                for(std::size_t index=0;index<state_->entries.size();++index)if(state_->entries[index].model==kind) {
                    if(found!=state_->entries.size())plan_refuse("duplicate compiled singleton meter operator");found=index;
                }
                if(found==state_->entries.size())plan_refuse("actual immutable meter operator missing");held->operations.push_back(found);
            };
            held->operations.reserve(3+3*observer_delivery_meter::table_ceiling);
            if(creator_!=database_native_process())plan_refuse("inherited meter operation corpus allocation");
            append_one(state::model_kind::meter_table);append_one(state::model_kind::meter_seed);
            for(std::size_t index=0;index<state_->entries.size();++index)
                if(state_->entries[index].model==state::model_kind::meter_trigger)held->operations.push_back(index);
            append_one(state::model_kind::meter_marker);
            if(held->operations.size()<6||held->operations.size()>3+3*observer_delivery_meter::table_ceiling)
                plan_refuse("compiled complete meter continuation corpus bound");
            ordinary_initialization_storage_scope::charge(sizeof(observer_delivery_initialization_meter));
            wrapper=new observer_delivery_initialization_meter(creator_,held);wrapped=true;
            if(creator_!=database_native_process())plan_refuse("inherited meter continuation wrapper");
            auto* entrusted=wrapper;wrapper=nullptr;
            retained.get<1>()=std::shared_ptr<observer_delivery_initialization_meter>(entrusted,
                &observer_delivery_initialization_meter::destroy_,ordinary_initialization_allocator<observer_delivery_initialization_meter>{});
            if(creator_!=database_native_process())plan_refuse("inherited meter continuation publication");
        }catch(...) {
            if(creator_==database_native_process()){if(wrapper)delete wrapper;else if(!wrapped)delete held;}throw;
        }
        auto& original=*retained.get<1>();auto& sequence=*original.state_;
        while(sequence.position<sequence.operations.size()) {
            open.require_final_meter_writer_(actual);
            if(sequence.creator!=database_native_process()||sequence.builder!=std::this_thread::get_id()||sequence.failed||sequence.active||
               actual.db_!=native||actual.raw_handle_escaped_.load(std::memory_order_acquire)||actual.catalog_quarantined_()||
               !actual.initialization_snapshot_current_(*sequence.snapshot,open)||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE)
                plan_refuse("meter transition lost actual held Native/WRITE original");
            std::uint64_t revision=0;
            if(!actual.initialization_schema_revision_(*sequence.snapshot,open,revision)||revision!=sequence.revision||revision==UINT64_MAX)
                plan_refuse("meter continuation cannot retarget or renew schema revision");
            const auto index=sequence.operations[sequence.position];const auto kind=state_->entries[index].model;
            if(kind==state::model_kind::meter_marker) {
                auto descriptor=observer_delivery_initialization_sql::fixed_(open,actual,observer_delivery_initialization_sql::recipe::schema_cookie);
                retained.get<2>()=actual.query_initialization_(descriptor);
                if(creator_!=database_native_process())plan_refuse("inherited actual post-install schema cookie return");
                const auto& rows=retained.get<2>();
                if(rows.size()!=1||rows.front().size()!=1||!std::holds_alternative<std::int64_t>(rows.front().begin()->second))
                    plan_refuse("actual post-install schema cookie shape differs");
                const auto cookie=std::get<std::int64_t>(rows.front().begin()->second);
                if(cookie<0||cookie>INT32_MAX||cookie==sequence.old_cookie)plan_refuse("actual meter DDL cookie did not advance within SQLite domain");
                sequence.new_cookie=ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(cookie));
                if(creator_!=database_native_process())plan_refuse("inherited actual meter cookie copy");
            }
            sequence.active=true;
            try {
                auto descriptor=observer_delivery_initialization_sql::from_meter_(retained.get<1>(),index,open,actual);
                actual.execute_initialization_(descriptor);
                if(creator_!=database_native_process())plan_refuse("inherited actual meter effect return");
                // DONE + exact finalization are already proved by the real
                // execute channel; only that actual successful return can
                // advance this one finite same-episode compiler continuation.
                if(actual.db_!=native||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE||
                   !actual.initialization_schema_revision_(*sequence.snapshot,open,revision)||revision!=sequence.revision+1)
                    plan_refuse("meter effect did not return exactly one actual retired schema attempt");
                if((kind==state::model_kind::meter_seed||kind==state::model_kind::meter_marker)&&sqlite3_changes64(native)!=1)
                    plan_refuse("meter seed/CAS lost its exact actual row preimage");
                sequence.revision=revision;++sequence.position;sequence.active=false;
            }catch(...) {
                if(creator_==database_native_process()){sequence.active=false;sequence.failed=true;}throw;
            }
        }
        // No completion/grant is returned. The sole closed finish caller
        // immediately requires the actual full strict postimage/row/marker
        // proof on this SAME actual private WRITE before its real COMMIT.
        // A produced-step counter or successful CREATE is not that proof.
    }catch(...) {open.fail_initialization_();throw;}
}
void observer_delivery_initialization_plan::require_final_(const observer_delivery_open_startup& open,
    database& actual,const recovery_owner_schema& catalog,std::int64_t target,std::string_view core_marker)const {
    require_normal_state_(open,actual,catalog,target,core_marker,nullptr);
}
void observer_delivery_initialization_plan::require_normal_state_(const observer_delivery_open_startup& open,
    database& actual,const recovery_owner_schema& catalog,std::int64_t target,std::string_view core_marker,meter_entry_evidence* entry)const {
    try {
        open.require_work_current_(actual);
        ordinary_initialization_storage_scope storage(const_cast<observer_delivery_open_startup*>(&open),&actual,
            &observer_delivery_open_startup::debit_initialization_storage_);
        if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
           state_->generation!=open.catalog_generation_()||!catalog.valid()||target<=0||target>INT32_MAX||
           core_marker.size()!=35||!core_marker.starts_with("schema_fingerprint:"))
            plan_refuse("final proof lost its actual immutable Plan/catalog/target");
        using custody=native_retirement_storage<std::shared_ptr<const observer_delivery_initialization_preimage>,
            std::vector<database::row_t>,ordinary_schema_metadata>;
        ordinary_initialization_storage_scope::charge(custody::allocation_bytes());custody held;
        if(creator_!=database_native_process())plan_refuse("inherited final proof custody");
        held.get<0>()=capture_preimage_(open,actual,nullptr,entry?preimage_selection::meter_install_entry:preimage_selection::final_required_present);
        if(creator_!=database_native_process())plan_refuse("inherited required-present final proof return");
        const bool present=!entry||(held.get<0>()&&held.get<0>()->state_->meter_present&&*held.get<0>()->state_->meter_present);
        if(entry&&(!held.get<0>()||!held.get<0>()->state_->meter_present))plan_refuse("meter entry lacks actual old-or-complete snapshot fact");
        const auto current=[&] {
            open.require_work_current_(actual);
            if(creator_!=database_native_process()||!held.get<0>()||
               !held.get<0>()->current_snapshot_(actual)||held.get<0>()->state_->plan_owner!=this||
               (present&&!held.get<0>()->plain_table_for_(actual,observer_delivery_meter::ledger_name()))||
               !held.get<0>()->plain_table_for_(actual,"_lattice_meta"))
                plan_refuse("final metadata lost its SAME required-present snapshot/revision");
        };
        current();
        const ordinary_identifier_less meter_less;
        const auto exact_object=[&](std::string_view name,std::string_view type,std::string_view owner,std::string_view sql) {
            const auto& objects=held.get<0>()->state_->facts.main_objects;
            const auto found=std::find_if(objects.begin(),objects.end(),[&](const auto& object){return !meter_less(object.name,name)&&!meter_less(name,object.name);});
            if(found==objects.end()||std::string_view(found->type)!=type||std::string_view(found->name)!=name||
               std::string_view(found->owner)!=owner||std::string_view(found->sql)!=sql)
                plan_refuse("final exact versioned meter object identity/program differs");
        };
        if(present)exact_object(observer_delivery_meter::ledger_name(),"table",observer_delivery_meter::ledger_name(),observer_delivery_meter::ledger_ddl());
        if(present)for(const auto& [name,table]:state_->final_projection.tables)for(const auto& sql:table.other_programs) {
            const auto trigger=ordinary_initialization_schema_validation::compiled_object_(sql,"trigger");
            if(trigger.starts_with("_lattice_observer_delivery_before_v1_"))exact_object(trigger,"trigger",name,sql);
        }
        // required_present_ proves pre-connect declarations first. This
        // unchanged strict complete validator then performs actual selected
        // xConnect/xinfo/shadow/config checks on the SAME retained snapshot.
        // The second complete procedure has its own prospective envelope;
        // neither global reservation is removed by this following.
        open.claim_workspace_(actual,32*1024*1024*3+65536*(sizeof(ordinary_object_metadata)+sizeof(std::string)*8)+
            262144*sizeof(database::row_t)+16*1024*1024);
        const auto complete_query=[&](const std::shared_ptr<const ordinary_preimage_statement>& statement) {
            current();auto descriptor=observer_delivery_initialization_sql::from_preimage_(statement,open,actual);
            auto result=actual.query_initialization_(descriptor);
            if(creator_!=database_native_process())plan_refuse("inherited actual complete module metadata return");
            current();return result;
        };
        held.get<2>()=ordinary_initialization_schema_validation::complete_(held.get<0>()->state_->scope,complete_query,
            ordinary_metadata_limits{65536,262144,32*1024*1024,256*1024});
        if(creator_!=database_native_process())plan_refuse("inherited complete schema/module proof return");
        if(held.get<2>().schema_cookie!=held.get<0>()->state_->facts.schema_cookie||
           held.get<2>().temp_schema_cookie!=held.get<0>()->state_->facts.temp_schema_cookie)
            plan_refuse("complete module/schema proof changed retained cookies");
        current();
        using read=ordinary_initialization_schema_validation::final_read;
        const auto query=[&](read selected) ->const std::vector<database::row_t>& {
            current();
            auto statement=ordinary_initialization_schema_validation::final_statement_(selected);
            if(creator_!=database_native_process())plan_refuse("inherited closed final descriptor return");
            auto descriptor=observer_delivery_initialization_sql::from_preimage_(statement,open,actual);
            held.get<1>()=actual.query_initialization_(descriptor);
            if(creator_!=database_native_process())plan_refuse("inherited final metadata Native return");
            current();return held.get<1>();
        };
        const auto cell=[](const database::row_t& row,const char* key)->const column_value_t& {
            const auto at=row.find(key);if(at==row.end())plan_refuse("final metadata result field missing");return at->second;
        };
        const auto integer=[&](const database::row_t& row,const char* key) {
            const auto* value=std::get_if<std::int64_t>(&cell(row,key));
            if(!value)plan_refuse("final metadata requires actual INTEGER");return *value;
        };
        const auto text=[&](const database::row_t& row,const char* key)->std::string_view {
            const auto* value=std::get_if<std::string>(&cell(row,key));
            if(!value||value->find('\0')!=std::string::npos)plan_refuse("final metadata requires actual NUL-free TEXT");return *value;
        };
        const auto one_integer=[&](read kind,const char* key,std::int64_t expected) {
            const auto& rows=query(kind);
            if(rows.size()!=1||rows.front().size()!=1||integer(rows.front(),key)!=expected)
                plan_refuse("final connection policy differs from its actual startup declaration");
        };
        const auto& encoding=query(read::encoding);
        if(encoding.size()!=1||encoding.front().size()!=1||text(encoding.front(),"encoding")!="UTF-8")
            plan_refuse("final database encoding differs from supported UTF-8 profile");
        one_integer(read::recursive_triggers,"recursive_triggers",1);
        one_integer(read::foreign_keys,"foreign_keys",1);
        const auto& databases=query(read::databases);bool main=false,temp=false;
        for(const auto& row:databases) {
            const auto name=text(row,"name");
            if(name=="main"){if(main)plan_refuse("duplicate final MAIN database");main=true;}
            else if(name=="temp"){if(temp)plan_refuse("duplicate final TEMP database");temp=true;}
            else plan_refuse("final database has an unapproved attachment");
        }
        if(!main)plan_refuse("final actual MAIN database missing");
        const auto& sync_control=query(read::sync_control_row);
        if(sync_control.size()!=1||sync_control.front().size()!=2||integer(sync_control.front(),"id")!=1||
           integer(sync_control.front(),"disabled")<0||integer(sync_control.front(),"disabled")>1)
            plan_refuse("final actual sync-control policy row differs");
        if(present) {
            const auto& meter=query(read::meter_row);
            if(meter.size()!=1||meter.front().size()!=5||integer(meter.front(),"id")!=1||
               text(meter.front(),"token_type")!="blob"||integer(meter.front(),"token_bytes")!=32||
               integer(meter.front(),"events")<0||integer(meter.front(),"bytes")<0)
                plan_refuse("final actual meter ledger row/types/counters differ");
        }
        // All expected metadata comes from this retained immutable catalog and
        // genuine compiled producer. Observed keys/values select no SQL grant.
        const auto fold=[](std::string_view value){ordinary_initialization_text out(value.data(),value.size());for(auto& c:out)if(c>='A'&&c<='Z')c+=32;return out;};
        const auto identifier=[](std::string_view value) {
            if(value.empty()||value.size()>256||(value.front()>='0'&&value.front()<='9'))plan_refuse("final internal parent identifier bound");
            for(const auto c:value)if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_'))
                plan_refuse("final internal parent identifier spelling");
        };
        using parent_set=ordinary_initialization_text_set;
        ordinary_initialization_text_map<parent_set> parents;
        ordinary_initialization_text_set selected;
        for(const auto& [name,model]:catalog.models) {
            selected.insert(fold(name));
            for(const auto& property:model.properties) {
                ordinary_initialization_text table,parent(name.data(),name.size());
                if(property.kind==property_kind::union_type||property.is_union){table.assign(property.union_desc.union_table_name);parent+=':';parent.append(property.name);}
                else if(property.kind==property_kind::virtual_link||property.kind==property_kind::virtual_list){table="_"+ordinary_initialization_text(name.data(),name.size())+"_"+ordinary_initialization_text(property.name.data(),property.name.size());if(catalog.swift_models.count(name)){parent+=':';parent.append(property.name);}}
                else if((property.kind==property_kind::link||property.kind==property_kind::list)&&!property.is_geo_bounds&&!property.target_table.empty()){
                    table="_"+ordinary_initialization_text(name.data(),name.size())+"_"+ordinary_initialization_text(property.target_table.data(),property.target_table.size())+"_"+ordinary_initialization_text(property.name.data(),property.name.size());
                    if(catalog.swift_models.count(name)){parent+=':';parent.append(property.name);}}
                else if(property.kind==property_kind::list&&property.is_geo_bounds)table="_"+ordinary_initialization_text(name.data(),name.size())+"_"+ordinary_initialization_text(property.name.data(),property.name.size());
                if(table.empty()||!state_->projection.tables.count(std::string_view(table)))continue;
                identifier(table);parents[table].insert(std::move(parent));
            }
        }
        for(const auto& [name,table]:state_->projection.tables)
            if(table.role!=ordinary_projection_role::model&&!parents.count(std::string_view(name)))
                plan_refuse("final declared sidecar lacks its actual catalog parent");
        ordinary_initialization_text_set seen_parents,seen_metadata;
        const auto target_text=ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(target));
        const auto cookie=held.get<0>()->state_->facts.schema_cookie;
        if(cookie<0||cookie>INT32_MAX)plan_refuse("final actual schema cookie outside bounded SQLite domain");
        const auto cookie_text=ordinary_program_storage_decimal<ordinary_initialization_text>(static_cast<unsigned long long>(cookie));
        bool version=false;size_t marker_count=0;
        const auto& rows=query(read::metadata_rows);
        for(const auto& row:rows) {
            const auto key=text(row,"key");
            if(key=="schema_version") {
                if(version||text(row,"value")!=std::string_view(target_text))plan_refuse("final actual target version differs");version=true;continue;
            }
            if(key.starts_with("schema_fingerprint:")) {
                if(++marker_count!=1||(key!=core_marker&&(catalog.swift_fingerprint.empty()||key!=catalog.swift_fingerprint))||
                   text(row,"value")!=std::string_view(cookie_text))plan_refuse("final surviving actual Core/Swift marker differs or is stale");
                if(entry) {
                    if(key.size()!=entry->marker.size())plan_refuse("actual surviving marker extent differs");
                    std::copy(key.begin(),key.end(),entry->marker.begin());
                }
                continue;
            }
            const auto compiled=std::find_if(state_->entries.begin(),state_->entries.end(),[&](const auto& entry) {
                return entry.model==state::model_kind::metadata_read&&!entry.fresh_only&&std::string_view(entry.model_object)==key;
            });
            if(compiled!=state_->entries.end()) {
                if(text(row,"value")!=std::string_view(compiled->model_value)||!seen_metadata.emplace(key.data(),key.size()).second)
                    plan_refuse("final actual retained model metadata differs");
            }
            if(!key.starts_with("internal_table:"))continue;
            const auto table=key.substr(15);identifier(table);const auto value=text(row,"value");
            parent_set actual_parents;size_t begin=0;
            if(!value.empty())for(;;) {
                const auto end=value.find(';',begin);const auto part=value.substr(begin,(end==std::string_view::npos?value.size():end)-begin);
                if(part.empty()||part.size()>513||actual_parents.size()>=ordinary_sync_limits::projection_tables)plan_refuse("final internal parent membership bound");
                const auto colon=part.find(':');identifier(part.substr(0,colon));if(colon!=std::string_view::npos)identifier(part.substr(colon+1));
                if(!actual_parents.emplace(part.data(),part.size()).second)plan_refuse("final duplicate internal parent membership");
                if(end==std::string_view::npos)break;begin=end+1;
            }
            const auto expected=parents.find(table);bool relevant=expected!=parents.end();
            for(const auto& parent:actual_parents)if(selected.count(fold(std::string_view(parent).substr(0,parent.find(':')))))relevant=true;
            for(const auto& [name,unused]:parents)if(fold(name)==fold(table))relevant=true;
            if(relevant&&(expected==parents.end()||expected->second!=actual_parents||!seen_parents.emplace(table.data(),table.size()).second))
                plan_refuse("final internal parent membership differs from same catalog");
        }
        if(!version||marker_count!=1||seen_parents.size()!=parents.size())plan_refuse("final actual version/marker/internal metadata missing");
        for(const auto& entry:state_->entries)
            if(entry.model==state::model_kind::metadata_read&&!entry.fresh_only&&!seen_metadata.count(std::string_view(entry.model_object)))
                plan_refuse("final required retained model metadata row missing");
        current(); // every actual row and final proof belongs to SAME snapshot/revision
        if(entry) {entry->cookie=cookie;entry->present=present;entry->proof=held.get<0>();}
    }catch(...) {open.fail_initialization_();throw;}
}
void observer_delivery_initialization_plan::require_fresh_catalog_empty_(const observer_delivery_open_startup& open,database& actual)const {
    try {
        open.require_work_current_(actual);
        auto empty=observer_delivery_initialization_sql::fixed_(open,actual,
            observer_delivery_initialization_sql::recipe::fresh_catalog_schema_presence);
        using custody=native_retirement_storage<std::vector<database::row_t>,std::shared_ptr<const observer_delivery_initialization_preimage>>;
        open.claim_workspace_(actual,custody::allocation_bytes());custody held;
        held.get<0>()=actual.query_initialization_(empty);
        if(creator_!=database_native_process())plan_refuse("inherited actual empty catalog observation");
        if(!held.get<0>().empty())plan_refuse("genuine fresh original already has durable schema; no reseed permitted");
        held.get<1>()=open.capture_model_preimage_(actual);
        open.require_model_write_(actual,held.get<1>(),observer_delivery_open_startup::model_preimage_phase::construction);
        if(!held.get<1>()||!held.get<1>()->current_snapshot_(actual)||!held.get<1>()->state_->facts.temp_objects.empty())
            plan_refuse("genuine fresh catalog requires its same empty TEMP/MAIN WRITE view");
    }catch(...){open.fail_initialization_();throw;}
}
void observer_delivery_initialization_plan::initialize_fresh_catalog_families_(const observer_delivery_open_startup& open,
    lattice_db& owner,database& actual)const {
    try {
        open.require_model_owner_(owner,actual);
        if(creator_!=database_native_process()||!state_||state_->creator!=creator_||
           state_->generation!=open.catalog_generation_()||!open.fresh_catalog_active_for_(actual))
            plan_refuse("fresh model families require their actual first-start WRITE original");
        const auto current=[&]{open.require_model_owner_(owner,actual);if(!open.fresh_catalog_active_for_(actual))
            plan_refuse("fresh model family lost its same WRITE original");};
        // The shared pure iterator selects no grant. EACH actual callback
        // still captures and proves its own same-Open/WRITE typed transition.
        ordinary_fresh_catalog_programs_(state_->projection,[&](std::string_view sql){
            current();open.initialize_model_recipe_(owner,actual,sql);
        },[&](std::string_view name,std::string_view column){
            current();const auto sql=ordinary_schema_recipe::shadow_column_storage<ordinary_initialization_text>(name,column);
            open.initialize_model_recipe_(owner,actual,sql);
        });
        for(const auto& entry:state_->entries)if(entry.model==state::model_kind::metadata_store){current();
            open.initialize_model_metadata_(owner,actual,entry.model_object,entry.model_value);}
        current();open.require_model_catalog_present_(owner,actual);
    }catch(...){open.fail_initialization_();throw;}
}

} // namespace lattice::detail
