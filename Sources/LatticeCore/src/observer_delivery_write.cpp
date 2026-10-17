#include <lattice/detail/observer_delivery_write.hpp>
#include <lattice/lattice.hpp>
#include "observer_delivery_connection.hpp"
#include <optional>
#include <utility>

namespace lattice::detail {
struct observer_delivery_write::payload {
    const uint64_t creator;
    lattice_db* owner=nullptr;
    // Actual Native pins live only in this creator-fenced allocation, never a
    // stack shared_ptr crossing allocation, adoption or delivery callbacks.
    std::shared_ptr<database> writer;
    std::shared_ptr<const observer_delivery_connection> connection;
    std::shared_ptr<const observer_delivery_original> original;
    observer_delivery_plan input;
    std::optional<observer_delivery_sql_runtime::prepared> prepared;
    bool attempted=false;
    explicit payload(uint64_t issuer):creator(issuer){}
    static payload& checked(payload* value) {
        if(!value||value->creator!=database_native_process())
            throw db_error("delivery write is inherited or unavailable");
        return *value;
    }
    static void destroy(payload* value)noexcept {
        if(!value||value->creator!=database_native_process())return;
        value->prepared.reset();if(value->creator!=database_native_process())return;
        value->input={};if(value->creator!=database_native_process())return;
        value->original.reset();if(value->creator!=database_native_process())return;
        value->connection.reset();if(value->creator!=database_native_process())return;
        value->writer.reset();if(value->creator!=database_native_process())return;
        delete value;
    }
};
observer_delivery_write::observer_delivery_write(payload* value)noexcept:value_(value){}
observer_delivery_write::observer_delivery_write(observer_delivery_write&& other)noexcept
    :value_(std::exchange(other.value_,nullptr)){}
observer_delivery_write::~observer_delivery_write()noexcept {payload::destroy(value_);}
bool observer_delivery_write::enabled(const lattice_db& owner)noexcept {
    if(!owner.owns_creator_process())return false;
    // Publication is immutable DATA. No Native admission/owner is inferred
    // from this selector; execute still checks the actual current writer.
    return bool(std::atomic_load(&owner.observer_delivery_connection_));
}
template<typename Input>
observer_delivery_write observer_delivery_write::prepare_input(lattice_db& owner,const Input& input) {
    static_assert(sizeof(payload)<=observer_delivery_plan::write_storage_bytes);
    owner.require_creator_process();
    const auto issuer=database_native_process();
    auto* value=new payload(issuer);
    try {
        if(value->creator!=database_native_process()||!owner.owns_creator_process())
            throw db_error("inherited delivery write allocation");
        value->owner=&owner;
        value->connection=std::atomic_load(&owner.observer_delivery_connection_);
        if(!value->connection)throw db_error("delivery write requires an actually enrolled owner profile");
        {
            std::lock_guard lock(owner.connection_ownership_mutex_);
            if(owner.closed_.load(std::memory_order_acquire)||!owner.db_)
                throw db_error("delivery write owner is closed");
            value->writer=owner.db_;
        }
        if(!value->connection->current(owner,*value->writer))
            throw db_error("delivery write physical original changed");
        // Classify the actual Core original BEFORE reading the borrowed
        // cursor. A same-caller BEGIN can only spend its already admitted
        // immutable envelope. Foreign originals keep existing serialization;
        // no Native admission remains held by this selector during preflight.
        if(database::sql_entry_scope::active_for(value->writer.get())||
           database::mutation_sql_scope::active_for(value->writer.get()))
            throw db_error("delivery INSERT cannot prepare inside an active Native operation");
        {
            database::catalog_native_admission_ admitted(*value->writer);
            if(sqlite3_get_autocommit(value->writer->db_)==0) {
                const auto& actual=value->writer->core_transaction_original_;
                if(!actual||!actual->current_for(*value->writer)||!actual->observer_delivery||
                   !actual->observer_delivery->explicit_envelope_||
                   !value->connection->runtime_->has_original(actual->observer_delivery))
                    throw db_error("delivery INSERT caller BEGIN has no exact admitted envelope");
                value->original=actual->observer_delivery; // DATA, never a Native pin
            }
        }
        // Actual input validation/admission occurs without any Native/store
        // leaf. No collector, result copy or SQL body has run at this point.
        value->input=value->original?value->connection->prepare(input,value->original):value->connection->prepare(input);
        if(value->creator!=database_native_process())throw db_error("inherited delivery input admission");
        value->prepared.emplace(value->connection->runtime_->prepare(*value->writer,value->input));
        if(value->creator!=database_native_process())throw db_error("inherited delivery prepared custody");
        if(!value->connection->current(owner,*value->writer))
            throw db_error("delivery write owner changed during input preparation");
        return observer_delivery_write(value);
    }catch(...) {payload::destroy(value);throw;}
}
observer_delivery_write observer_delivery_write::prepare(lattice_db& owner,const observer_delivery_values::cursor& input) {
    return prepare_input(owner,input);
}
observer_delivery_write observer_delivery_write::prepare(lattice_db& owner,std::span<const observer_delivery_values::row_view> input) {
    return prepare_input(owner,input);
}
size_t observer_delivery_write::row_count()const {return payload::checked(value_).input.rows().size();}
observer_delivery_values::row_view observer_delivery_write::row(size_t index,observer_delivery_values::field_buffer& output)const {
    const auto& rows=payload::checked(value_).input.rows();
    if(index>=rows.size())throw std::out_of_range("delivery admitted row index");
    output.reset();
    for(const auto& field:rows[index].fields)
        std::visit([&](const auto& actual){output.append_scalar(field.name,actual);},field.contents);
    return {rows[index].table,output.fields()};
}
std::string_view observer_delivery_write::global_id(size_t index)const {return payload::checked(value_).input.global_id(index);}
const model_schema& observer_delivery_write::schema(size_t index)const {
    const auto& value=payload::checked(value_);const auto& rows=value.input.rows();
    if(index>=rows.size()||!value.writer||value.writer->is_closed()||!value.connection||
       value.connection->creator_!=value.creator||value.connection->writer_!=value.writer.get()||
       (value.original&&value.original->ended_.load(std::memory_order_acquire)))
        throw db_error("delivery schema requires its current admitted input");
    // No raw owner dereference: a retained DATA view is not a strong owner
    // lifetime pin. Actual execute still takes/revalidates the real owner.
    const auto found=value.connection->catalog_.models.find(rows[index].table);
    if(found==value.connection->catalog_.models.end()||found->second.table_name!=rows[index].table)
        throw db_error("delivery immutable catalog target differs");
    return found->second;
}
void observer_delivery_write::reserve_output_workspace(uint64_t bytes) {
    auto& value=payload::checked(value_);
    if(value.attempted)throw db_error("delivery output workspace must precede actual execution");
    value.input.reserve_output_workspace(bytes);
}
uint64_t observer_delivery_write::declared_output_workspace()const {return payload::checked(value_).input.remaining_workspace();}
std::span<const observer_delivery_write::inserted_row> observer_delivery_write::inserted_rows()const {
    return payload::checked(value_).input.inserted_rows();
}
void observer_delivery_write::execute(lattice_db& owner,adoption_view adoption) {
    auto& value=payload::checked(value_);owner.require_creator_process();
    if(value.attempted||value.owner!=&owner||!value.prepared||!value.writer||
       !value.connection->current(owner,*value.writer))
        throw db_error("delivery write requires its unused exact owner original");
    value.attempted=true; // body/adoption/failed SQL never silently retries
    const auto receipts=value.input.inserted_rows(); // POD borrowed span, before Native/adoption
    if(receipts.size()!=value.input.rows().size())
        throw db_error("delivery INSERT receipt count differs before execution");
    const auto perform=[&]{owner.with_mutation_operation([&](database& writer) {
        if(&writer!=value.writer.get()||!value.connection->current(owner,writer))
            throw db_error("delivery write lost its actual admitted Native writer");
        value.prepared->execute(writer);
        // Validate all fallible output facts BEFORE outer RELEASE/COMMIT.
        // The later noexcept journal adoption only publishes these proved IDs.
        for(const auto& row:receipts)
            if(row.rowid<=0||row.global_id.size()!=36)
                throw db_error("delivery INSERT lacks a proved actual row identity");
    },[&](bool committed)noexcept {
        if(value.creator!=database_native_process())return;
        if(adoption.publish)adoption.publish(adoption.context,committed,receipts);
    });};
    if(value.original)perform(); // Existing BEGIN/savepoint; no acquire/renewal.
    else {
        observer_delivery_original::begin_offer offer(*value.writer,value.input);
        perform();
    }
    if(value.creator!=database_native_process())throw db_error("inherited delivery write completion");
}
}
