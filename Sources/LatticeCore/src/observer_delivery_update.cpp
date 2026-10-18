#include <lattice/detail/observer_delivery_update.hpp>
#include <lattice/lattice.hpp>
#include "observer_delivery_connection.hpp"
#include <algorithm>
#include <optional>
#include <utility>

namespace lattice::detail {
struct observer_delivery_update::payload {
    const uint64_t creator;
    lattice_db* owner=nullptr; // compare only; actual calls retain their owner
    std::shared_ptr<database> writer;
    std::shared_ptr<const observer_delivery_connection> connection;
    std::shared_ptr<instance_guard> lifetime; // DATA, no Native/owner authority
    observer_delivery_identity identity;
    observer_delivery_plan input;
    std::optional<observer_delivery_sql_runtime::prepared> prepared;
    bool supplied=false,attempted=false;
    explicit payload(uint64_t issuer):creator(issuer){}
    static payload& checked(payload* value) {
        if(!value||value->creator!=database_native_process())
            throw db_error("delivery UPDATE token is inherited or unavailable");
        return *value;
    }
    static void destroy(payload* value)noexcept {
        if(!value||value->creator!=database_native_process())return;
        value->prepared.reset();if(value->creator!=database_native_process())return;
        value->input={};if(value->creator!=database_native_process())return;
        value->identity={};if(value->creator!=database_native_process())return;
        value->lifetime.reset();if(value->creator!=database_native_process())return;
        value->connection.reset();if(value->creator!=database_native_process())return;
        value->writer.reset();if(value->creator!=database_native_process())return;
        delete value;
    }
};
observer_delivery_update::observer_delivery_update(payload* value)noexcept:value_(value){}
observer_delivery_update::observer_delivery_update(observer_delivery_update&& other)noexcept
    :value_(std::exchange(other.value_,nullptr)){}
observer_delivery_update::~observer_delivery_update()noexcept {payload::destroy(value_);}
observer_delivery_update observer_delivery_update::prepare(lattice_db& owner,
    const observer_delivery_target& target,value_kind kind,uint64_t maximum) {
    static_assert(sizeof(payload)<=observer_delivery_plan::write_storage_bytes);
    owner.require_creator_process();
    const auto issuer=database_native_process();
    auto* value=new payload(issuer);
    try {
        if(issuer!=database_native_process()||!owner.owns_creator_process())
            throw db_error("inherited delivery UPDATE allocation");
        if(!target.valid()||target.identity_.owner_address()!=&owner||target.attachment_generation()!=0)
            throw db_error("delivery UPDATE needs its actual primary-store binding");
        value->owner=&owner;value->identity=target.identity_;
        // Every weak lock is assigned directly into existing creator-fenced
        // heap custody. Weak expiry refuses; it proves neither ending nor refund.
        value->lifetime=value->identity.owner_lifetime().lock();
        value->writer=value->identity.writer_identity().lock();
        value->connection=value->identity.profile_identity().lock();
        if(issuer!=database_native_process())throw db_error("inherited delivery UPDATE provenance");
        if(!value->lifetime||value->lifetime!=owner.guard_||
           value->lifetime.owner_before(owner.guard_)||owner.guard_.owner_before(value->lifetime)||
           !value->lifetime->alive.load(std::memory_order_acquire)||!value->writer||!value->connection)
            throw db_error("delivery UPDATE binding original is unavailable");
        {
            std::lock_guard lock(owner.connection_ownership_mutex_);
            if(owner.closed_.load(std::memory_order_acquire)||owner.db_!=value->writer||
               owner.db_.owner_before(value->writer)||value->writer.owner_before(owner.db_))
                throw db_error("delivery UPDATE writer differs from exact binding");
        }
        if(!value->connection->current(owner,*value->writer))
            throw db_error("delivery UPDATE profile differs from exact binding");
        const auto table=value->identity.table(),column=target.column();
        const auto found=std::find_if(value->connection->catalog_->models.begin(),value->connection->catalog_->models.end(),
            [&](const auto& entry){return entry.first==table;});
        if(found==value->connection->catalog_->models.end()||found->second.table_name!=table||column=="id"||column=="globalId")
            throw db_error("delivery UPDATE target differs from retained scalar catalog");
        const auto property=std::find_if(found->second.properties.begin(),found->second.properties.end(),
            [&](const auto& actual){return actual.name==column;});
        if(property==found->second.properties.end()||property->kind!=property_kind::primitive||property->is_vector||
           property->is_geo_bounds||property->is_full_text||property->is_union)
            throw db_error("delivery UPDATE needs an unsupported closed field operator");
        const bool accepts=kind==value_kind::null_value?property->nullable:
            property->type==column_type::integer?kind==value_kind::integer:
            property->type==column_type::real?(kind==value_kind::real||kind==value_kind::integer):
            property->type==column_type::text?kind==value_kind::text:
            property->type==column_type::blob?kind==value_kind::blob:false;
        if(!accepts)throw db_error("delivery UPDATE builtin kind differs from retained field");
        if(database::sql_entry_scope::active_for(value->writer.get())||
           database::mutation_sql_scope::active_for(value->writer.get()))
            throw db_error("delivery UPDATE conversion must precede Native operation leaves");
        // This temporary admission ends before any possible domain wait. A
        // current caller original is DATA copied only into the plan after this
        // leaf, never a strong Native pin on a fork-crossing stack.
        std::shared_ptr<const observer_delivery_original> original; // DATA only
        {
            database::catalog_native_admission_ admitted(*value->writer);
            if(sqlite3_get_autocommit(value->writer->db_)==0) {
                const auto& actual=value->writer->core_transaction_original_;
                if(!actual||!actual->current_for(*value->writer)||!actual->observer_delivery||
                   !actual->observer_delivery->explicit_envelope_||
                   !value->connection->runtime_->has_original(actual->observer_delivery))
                    throw db_error("delivery UPDATE caller BEGIN lacks its actual admitted envelope");
                original=actual->observer_delivery;
            }
        }
        const auto deadline=observer_delivery_store::clock::now()+value->connection->policy_.maximum_admission_wait;
        auto admitted=observer_delivery_plan::prepare_update(value->connection->client_,value->connection->policy_.original,
            deadline,table,column,value->identity.rowid(),value->identity.global_id(),size_t(kind),maximum,original);
        if(admitted.code!=observer_delivery_store::status::admitted)
            throw db_error("delivery UPDATE admission refused before conversion/effects");
        value->input=std::move(admitted.plan);
        if(issuer!=database_native_process())throw db_error("inherited delivery UPDATE admission return");
        // Metadata is now copied under the actual charge. It must still match
        // the exact immutable binding/catalog after admission/allocation.
        const auto& row=value->input.rows().at(0);
        if(row.table!=table||row.target_rowid!=value->identity.rowid()||row.target_global_id!=value->identity.global_id()||
           row.fields.size()!=1||row.fields[0].name!=column||!value->connection->current(owner,*value->writer))
            throw db_error("delivery UPDATE final admitted metadata differs");
        return observer_delivery_update(value);
    }catch(...){payload::destroy(value);throw;}
}
void observer_delivery_update::supply(const observer_delivery_values::value_view& input) {
    auto& value=payload::checked(value_);
    if(value.supplied||value.attempted)throw db_error("delivery UPDATE supply is one use");
    value.input.supply_update(input);
    if(value.creator!=database_native_process())throw db_error("inherited delivery UPDATE supplied value");
    value.supplied=true;
    value.prepared.emplace(value.connection->runtime_->prepare(*value.writer,value.input));
    if(value.creator!=database_native_process())throw db_error("inherited delivery UPDATE preparation");
}
observer_delivery_values::value_view observer_delivery_update::value()const {
    const auto& value=payload::checked(value_);
    if(!value.supplied)throw db_error("delivery UPDATE supplied value unavailable");
    return std::visit([](const auto& actual)->observer_delivery_values::value_view {
        using T=std::decay_t<decltype(actual)>;
        if constexpr(std::is_same_v<T,std::string>)return std::string_view(actual);
        else if constexpr(std::is_same_v<T,std::vector<uint8_t>>)return std::span<const uint8_t>(actual);
        else return actual;
    },value.input.rows()[0].fields[0].contents);
}
void observer_delivery_update::reserve_output_workspace(uint64_t bytes) {
    auto& value=payload::checked(value_);
    if(value.attempted)throw db_error("delivery UPDATE cache workspace must precede execution");
    value.input.reserve_output_workspace(bytes);
}
uint64_t observer_delivery_update::declared_output_workspace()const {
    return payload::checked(value_).input.remaining_workspace();
}
void observer_delivery_update::execute(lattice_db& owner,adoption_view adoption) {
    auto& value=payload::checked(value_);owner.require_creator_process();
    if(value.attempted||!value.supplied||!value.prepared||value.owner!=&owner||
       !value.connection->current(owner,*value.writer)||!value.lifetime->alive.load(std::memory_order_acquire))
        throw db_error("delivery UPDATE requires its unused actual binding/owner");
    value.attempted=true;
    bool present=false;
    const auto perform=[&]{owner.with_mutation_operation([&](database& writer) {
        if(&writer!=value.writer.get()||!value.connection->current(owner,writer))
            throw db_error("delivery UPDATE lost its admitted writer");
        value.prepared->execute(writer);present=value.input.update_row_present();
    },[&](bool committed)noexcept {
        if(value.creator!=database_native_process())return;
        if(adoption.publish)adoption.publish(adoption.context,committed,present);
    });};
    if(value.input.borrowed_original())perform();
    else {observer_delivery_original::begin_offer offer(*value.writer,value.input);perform();}
    if(value.creator!=database_native_process())throw db_error("inherited delivery UPDATE completion");
}
bool observer_delivery_update::row_present()const {
    return payload::checked(value_).input.update_row_present();
}
}
