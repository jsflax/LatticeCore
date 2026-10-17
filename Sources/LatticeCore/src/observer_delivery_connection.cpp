#include "observer_delivery_connection.hpp"
#include "observer_delivery_schema.hpp"
#include "observer_delivery_original.hpp"
#include <lattice/native_retirement_storage.hpp>
#include <lattice/lattice.hpp>
#include <sqlite3.h>
#include <algorithm>
#include <climits>
#include <limits>
#include <type_traits>

namespace lattice::detail {
namespace {
[[noreturn]] void refuse(const char* message){throw db_error(message);}
const std::string& only_text(const std::vector<database::row_t>& rows,const char* key) {
    if(rows.size()!=1)refuse("delivery enrollment exact metadata row missing");
    const auto at=rows[0].find(key);
    if(at==rows[0].end()||!std::holds_alternative<std::string>(at->second))
        refuse("delivery enrollment exact TEXT metadata required");
    return std::get<std::string>(at->second);
}
void require_storage_type(const observer_delivery_values::field_view& field,const property_descriptor& property) {
    if(std::holds_alternative<std::nullptr_t>(field.contents)) {
        if(!property.nullable)refuse("delivery INSERT NULL differs from actual declared field");
        return;
    }
    const bool accepted=property.type==column_type::integer?std::holds_alternative<int64_t>(field.contents):
        property.type==column_type::real?(std::holds_alternative<double>(field.contents)||std::holds_alternative<int64_t>(field.contents)):
        property.type==column_type::text?std::holds_alternative<std::string_view>(field.contents):
        property.type==column_type::blob?std::holds_alternative<std::span<const uint8_t>>(field.contents):false;
    if(!accepted)refuse("delivery INSERT requires exact closed scalar storage");
}
}
observer_delivery_connection::observer_delivery_connection(uint64_t issuer,lattice_db& owner,database& writer,
    recovery_owner_schema catalog,policy config,observer_delivery_store store,observer_delivery_store::client client)
    :creator_(issuer),owner_(&owner),writer_(&writer),catalog_(std::move(catalog)),
     policy_(config),store_(std::move(store)),client_(std::move(client)){}
void observer_delivery_connection::destroy(const observer_delivery_connection* value)noexcept {
    if(value&&value->creator_==database_native_process())delete value;
}
bool observer_delivery_connection::current(const lattice_db& owner,const database& writer)const noexcept {
    if(creator_!=database_native_process()||owner_!=&owner||writer_!=&writer||
       !owner.owns_creator_process()||owner.closed_.load(std::memory_order_acquire)||writer.is_closed())return false;
    // Exact atomic publication is DATA only; actual Native scope revalidates
    // the writer and issued original under its normal serialized leaf.
    return std::atomic_load(&owner.observer_delivery_connection_).get()==this;
}
void observer_delivery_connection::validate_row(const observer_delivery_values::row_view& row)const {
    if(creator_!=database_native_process())refuse("inherited delivery connection");
    // The retained catalog is already bounded. Linear borrowed lookup avoids
    // creating a temporary table-name string before admission.
    const auto found=std::find_if(catalog_.models.begin(),catalog_.models.end(),
        [&](const auto& entry){return entry.first==row.table;});
    const auto* model=found==catalog_.models.end()?nullptr:&found->second;
    if(!model||model->table_name!=row.table)refuse("delivery INSERT target differs from actual retained catalog");
    const auto constraints=std::find_if(catalog_.swift_constraints.begin(),catalog_.swift_constraints.end(),
        [&](const auto& entry){return entry.first==row.table;});
    if(constraints!=catalog_.swift_constraints.end()&&std::any_of(constraints->second.begin(),constraints->second.end(),
       [](const auto& constraint){return constraint.allows_upsert;}))
        refuse("delivery INSERT target requires the unfinished closed UPSERT operator");
    // Initial closed INSERT operator. Relationship, derived/module, upsert,
    // update/delete and opaque collectors require their own closed operators;
    // they cannot pass through a generic legacy body in the bounded profile.
    for(const auto& property:model->properties)
        if(property.kind!=property_kind::primitive||property.is_vector||property.is_geo_bounds||
           property.is_full_text||property.is_union)
            refuse("delivery INSERT target needs an unsupported closed operator");
    const auto required=std::count_if(model->properties.begin(),model->properties.end(),
        [](const auto& property){return property.name!="globalId";});
    if(row.fields.size()<size_t(required)||row.fields.size()>size_t(required)+1)
        refuse("delivery INSERT requires every exact declared scalar field");
    for(const auto& property:model->properties) {
        if(property.name=="globalId")continue; // Default add generates it after admission.
        const auto at=std::find_if(row.fields.begin(),row.fields.end(),[&](const auto& field){return field.name==property.name;});
        if(at==row.fields.end())refuse("delivery INSERT declared field is missing");
        require_storage_type(*at,property);
    }
    for(const auto& field:row.fields) {
        if(field.name=="globalId") {
            // Existing default add ignores a supplied identity. The separate
            // preserving operator is not admitted by this initial INSERT API.
            continue;
        }else if(std::none_of(model->properties.begin(),model->properties.end(),[&](const auto& property){return property.name==field.name;}))
            refuse("delivery INSERT has an extra or differently spelled field");
    }
}
void observer_delivery_connection::validate_plan(const observer_delivery_plan& plan)const {
    observer_delivery_values::field_buffer scratch;
    for(const auto& row:plan.rows()) {
        scratch.reset();
        for(const auto& field:row.fields)
            std::visit([&](const auto& value){scratch.append_scalar(field.name,value);},field.contents);
        validate_row({row.table,scratch.fields()});
    }
    // The immutable postimage, not a prior borrowed DTO, is the final actual
    // catalog/storage proof. This performs no SQL or owning value copies.
}
observer_delivery_plan observer_delivery_connection::prepare(const observer_delivery_values::cursor& input)const {
    if(!input.size()||input.size()>policy_.original.events/2)
        refuse("delivery INSERT request exceeds its original event declaration");
    observer_delivery_values::field_buffer scratch;
    for(size_t i=0;i<input.size();++i)validate_row(input.read(i,scratch));
    const auto deadline=observer_delivery_store::clock::now()+policy_.maximum_admission_wait;
    auto result=observer_delivery_plan::prepare_insert(client_,policy_.original,deadline,input);
    if(result.code!=observer_delivery_store::status::admitted)
        refuse("delivery INSERT admission refused before input copies/effects");
    validate_plan(result.plan);
    return std::move(result.plan);
}
observer_delivery_plan observer_delivery_connection::prepare(std::span<const observer_delivery_values::row_view> input)const {
    if(input.empty()||input.size()>policy_.original.events/2)
        refuse("delivery INSERT request exceeds its original event declaration");
    for(const auto& row:input)validate_row(row);
    const auto deadline=observer_delivery_store::clock::now()+policy_.maximum_admission_wait;
    auto result=observer_delivery_plan::prepare_insert(client_,policy_.original,deadline,input);
    if(result.code!=observer_delivery_store::status::admitted)
        refuse("delivery INSERT admission refused before input copies/effects");
    validate_plan(result.plan);
    return std::move(result.plan);
}
observer_delivery_plan observer_delivery_connection::prepare(const observer_delivery_values::cursor& input,
    const std::shared_ptr<const observer_delivery_original>& original)const {
    if(!input.size()||input.size()>policy_.original.events/2)
        refuse("delivery INSERT request exceeds its original event declaration");
    observer_delivery_values::field_buffer scratch;
    for(size_t i=0;i<input.size();++i)validate_row(input.read(i,scratch));
    auto result=observer_delivery_plan::prepare_insert(client_,policy_.original,input,original);
    if(result.code!=observer_delivery_store::status::admitted)
        refuse("delivery INSERT subplan exceeds its existing BEGIN envelope");
    validate_plan(result.plan);
    return std::move(result.plan);
}
observer_delivery_plan observer_delivery_connection::prepare(std::span<const observer_delivery_values::row_view> input,
    const std::shared_ptr<const observer_delivery_original>& original)const {
    if(input.empty()||input.size()>policy_.original.events/2)
        refuse("delivery INSERT request exceeds its original event declaration");
    for(const auto& row:input)validate_row(row);
    auto result=observer_delivery_plan::prepare_insert(client_,policy_.original,input,original);
    if(result.code!=observer_delivery_store::status::admitted)
        refuse("delivery INSERT subplan exceeds its existing BEGIN envelope");
    validate_plan(result.plan);
    return std::move(result.plan);
}
void observer_delivery_connection::enroll(lattice_db& owner,policy config,activation_key) {
    owner.require_creator_process();
    const auto issuer=database_native_process();
    if(!owner.recovery_schemas_.valid()||owner.config_.read_only||owner.config_.target_schema_version<=0||
       owner.recovery_continuous_||config.original.workspace_bytes<observer_delivery_plan::minimum_workspace_bytes()||
       config.maximum_admission_wait.count()<=0||
       config.maximum_admission_wait>std::chrono::minutes(1)||config.original.event_bytes>uint64_t(INT_MAX))
        refuse("delivery enrollment requires its explicit current bounded profile");
    (void)observer_delivery_meter::admission_charge(config.original);
    native_retirement_storage<std::shared_ptr<database>,std::shared_ptr<observer_delivery_connection>> held;
    if(!owner.owns_creator_process())refuse("inherited delivery enrollment storage");
    {
        std::lock_guard lock(owner.connection_ownership_mutex_);
        if(owner.closed_.load(std::memory_order_acquire)||!owner.db_||
           std::atomic_load(&owner.observer_delivery_connection_))
            refuse("delivery enrollment owner closed or already published");
        held.get<0>()=owner.db_;
    }
    const auto& writer=held.get<0>();
    // Real per-physical binding/client admission occurs BEFORE the owned
    // maintenance transaction or its Native/store leaves; no path-only key.
    auto store=observer_delivery_store::bind(*writer,config.domain);
    auto client=store.register_client();
    if(client.code!=observer_delivery_store::status::admitted)
        refuse("delivery enrollment client capacity unavailable");
    held.get<1>()=std::shared_ptr<observer_delivery_connection>(new observer_delivery_connection(issuer,owner,*writer,
        owner.recovery_schemas_,config,std::move(store),std::move(client.value)),destroy);
    if(!owner.owns_creator_process())refuse("inherited delivery profile allocation");
    const auto programs=observer_delivery_schema::programs(owner.recovery_schemas_);
    owner.with_audit_prune_transaction_([&]() -> int64_t {
        if(owner.db_!=writer||writer->raw_handle_escaped_.load(std::memory_order_acquire)||writer->observer_delivery_sql_)
            refuse("delivery enrollment physical writer changed or escaped");
        const auto encoding=writer->query("PRAGMA main.encoding");
        if(only_text(encoding,"encoding")!="UTF-8")refuse("delivery enrollment requires actual UTF8 storage");
        const auto version=writer->query("SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB)) BETWEEN 1 AND 10 THEN value END AS value FROM main._lattice_meta WHERE key='schema_version' LIMIT 2");
        if(only_text(version,"value")!=std::to_string(owner.config_.target_schema_version))
            refuse("delivery enrollment requires exact current schema version");
        const auto old_cookie=owner.read_schema_cookie();
        if(old_cookie<0)refuse("delivery enrollment old schema cookie unavailable");
        const auto old_value=std::to_string(old_cookie);
        std::vector<std::string> keys{owner.compute_core_fingerprint_key()};
        if(!owner.recovery_schemas_.swift_fingerprint.empty()&&owner.recovery_schemas_.swift_fingerprint!=keys.front())
            keys.push_back(owner.recovery_schemas_.swift_fingerprint);
        for(const auto& key:keys) {
            const auto rows=writer->query("SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB)) BETWEEN 1 AND 10 THEN value END AS value FROM main._lattice_meta WHERE key=? LIMIT 2",{key});
            if(only_text(rows,"value")!=old_value)refuse("delivery enrollment refuses stale/missing fingerprint; no repair");
        }
        // Same retained actual WRITE view; existing strict ordinary/internal
        // ownership and complete old/new guard validation precede every DDL.
        observer_delivery_schema::validate_database(*writer,owner.recovery_schemas_);
        const auto present=writer->query("SELECT name FROM main.sqlite_schema WHERE name='_lattice_observer_delivery_meter_v1' COLLATE NOCASE LIMIT 2");
        if(present.empty()) {
            writer->execute(std::string(observer_delivery_meter::ledger_ddl()));
            writer->execute("INSERT INTO main._lattice_observer_delivery_meter_v1(id,token,events,bytes) VALUES(1,zeroblob(32),0,0)");
            for(const auto& program:programs)writer->execute(program.sql);
        }
        observer_delivery_schema::validate_database(*writer,owner.recovery_schemas_);
        const auto new_cookie=owner.read_schema_cookie();
        if(new_cookie<0)refuse("delivery enrollment final schema cookie unavailable");
        if(new_cookie!=old_cookie)for(const auto& key:keys) {
            writer->execute("UPDATE main._lattice_meta SET value=? WHERE key=? AND value=?",{std::to_string(new_cookie),key,old_value});
            const auto changed=writer->query("SELECT changes() AS n");
            if(changed.size()!=1||!std::holds_alternative<int64_t>(changed[0].at("n"))||std::get<int64_t>(changed[0].at("n"))!=1)
                refuse("delivery enrollment marker lost its exact preimage");
        }
        return 0;
    });
    if(!owner.owns_creator_process())refuse("inherited delivery enrollment completion");
    held.get<1>()->runtime_=observer_delivery_sql_runtime::install(*writer);
    {
        std::lock_guard lock(owner.connection_ownership_mutex_);
        if(owner.closed_.load(std::memory_order_acquire)||owner.db_!=writer||
           std::atomic_load(&owner.observer_delivery_connection_))
            refuse("delivery profile publication lost exact live owner");
        std::atomic_store(&owner.observer_delivery_connection_,
            std::shared_ptr<const observer_delivery_connection>(held.get<1>()));
    }
}
}
