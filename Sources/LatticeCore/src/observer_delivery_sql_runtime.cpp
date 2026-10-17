#include "observer_delivery_sql_runtime.hpp"
#include "observer_delivery_plan.hpp"
#include <lattice/db.hpp>
#include <sqlite3.h>
#include <array>
#include <limits>
#include <stdexcept>
#include <utility>
#include <type_traits>

namespace lattice::detail {
struct observer_delivery_sql_runtime::state {
    const uint64_t creator;
    database* const owner;
    sqlite3* const native;
    // DATA only; does not retain the writer, original Native ownership or a
    // callback. SQLite function registrations keep the state valid through a
    // zombie connection; first checked-close debt is never rewritten here.
    std::shared_ptr<const observer_delivery_original> original;
    sqlite3_stmt* statement=nullptr;
    bool registration_failed=false,control=false;
    state(uint64_t issuer,database* value,sqlite3* handle):creator(issuer),owner(value),native(handle){}
    static void destroy(state* value)noexcept {
        if(value&&value->creator==database_native_process())delete value;
    }
    bool permitted(sqlite3_context* context)const noexcept {
        if(creator!=database_native_process()||registration_failed||control||!statement||
           sqlite3_context_db_handle(context)!=native||!original||
           original->creator_!=creator||!original->began_.load(std::memory_order_acquire)||
           original->ended_.load(std::memory_order_acquire)||!sqlite3_stmt_busy(statement))return false;
        // No Native query/lock through owner: the executor's live statement
        // scope and actual BEGIN/end hooks own this continuously serialized
        // SQLite turn. No function can publish/change that scope.
        return true;
    }
    static state* require(sqlite3_context* context)noexcept {
        auto* holder=static_cast<std::shared_ptr<state>*>(sqlite3_user_data(context));
        auto* value=holder?holder->get():nullptr;
        if(!value||!value->permitted(context)) {
            sqlite3_result_error(context,"observer delivery closed original/statement required",-1);
            return nullptr;
        }
        return value;
    }
    static void release(void* raw)noexcept {
        // Only a DATA shared holder; state's deleter fences inherited fields.
        delete static_cast<std::shared_ptr<state>*>(raw);
    }
    static void permit(sqlite3_context* context,int,sqlite3_value**)noexcept {
        if(require(context))sqlite3_result_int(context,1);
    }
    static void token(sqlite3_context* context,int,sqlite3_value**)noexcept {
        if(auto* value=require(context))sqlite3_result_blob(context,value->original->token_.data(),
            int(value->original->token_.size()),SQLITE_STATIC);
    }
    static void event_limit(sqlite3_context* context,int,sqlite3_value**)noexcept {
        if(auto* value=require(context))sqlite3_result_int64(context,sqlite3_int64(value->original->budget_.events));
    }
    static void byte_limit(sqlite3_context* context,int,sqlite3_value**)noexcept {
        if(auto* value=require(context))sqlite3_result_int64(context,sqlite3_int64(value->original->budget_.event_bytes));
    }
    static void add(sqlite3_context* context,int count,sqlite3_value** args)noexcept {
        if(!require(context))return;
        if(count!=2||sqlite3_value_type(args[0])!=SQLITE_INTEGER||sqlite3_value_type(args[1])!=SQLITE_INTEGER) {
            sqlite3_result_error(context,"observer delivery byte sum type",-1);return;
        }
        const auto left=sqlite3_value_int64(args[0]),right=sqlite3_value_int64(args[1]);
        if(left<0||right<0||right>INT64_MAX-left){sqlite3_result_error(context,"observer delivery byte sum overflow",-1);return;}
        sqlite3_result_int64(context,left+right);
    }
    static void row(sqlite3_context* context,int count,sqlite3_value** args)noexcept {
        if(!require(context))return;
        // Generator caps each call at60 pairs (historical127-arg SQLite).
        if(count<3||count>121||(count&1)==0||sqlite3_value_type(args[0])!=SQLITE_TEXT) {
            sqlite3_result_error(context,"observer delivery row argument shape",-1);return;
        }
        std::array<std::string_view,60> names{};std::array<sqlite3_value*,60> values{};
        const auto text=[](sqlite3_value* value)->std::string_view {
            if(sqlite3_value_type(value)!=SQLITE_TEXT)throw std::length_error("observer delivery compiled identifier type");
            const auto size=sqlite3_value_bytes(value);const auto* bytes=sqlite3_value_text(value);
            if(size<=0||size>255||!bytes)throw std::length_error("observer delivery compiled identifier bound");
            return {reinterpret_cast<const char*>(bytes),size_t(size)};
        };
        try {
            const auto table=text(args[0]);const auto fields=size_t((count-1)/2);
            for(size_t index=0;index<fields;++index){names[index]=text(args[1+2*index]);values[index]=args[2+2*index];}
            const auto bytes=observer_delivery_meter::row_bytes(table,
                {names.data(),fields},{values.data(),fields});
            sqlite3_result_int64(context,sqlite3_int64(bytes));
        } catch(...) {sqlite3_result_error(context,"observer delivery row byte bound",-1);}
    }
};
observer_delivery_sql_runtime::observer_delivery_sql_runtime(std::shared_ptr<state> value):state_(std::move(value)){}
std::shared_ptr<observer_delivery_sql_runtime> observer_delivery_sql_runtime::install(database& writer) {
    writer.require_creator_();
    if(!writer.db_||writer.closed_.load(std::memory_order_acquire)||
       writer.raw_handle_escaped_.load(std::memory_order_acquire)||writer.observer_delivery_sql_||sqlite3_get_autocommit(writer.db_)==0)
        throw db_error("delivery SQL registration requires idle unescaped physical writer");
    const auto creator=database_native_process();
    auto* const original=writer.db_; // Capture before either allocation.
    auto value=std::shared_ptr<state>(new state(creator,&writer,original),state::destroy);
    if(creator!=database_native_process())throw db_error("inherited delivery SQL registration allocation");
    auto result=std::shared_ptr<observer_delivery_sql_runtime>(new observer_delivery_sql_runtime(value));
    struct function {const char* name;int args;void(*call)(sqlite3_context*,int,sqlite3_value**);};
    const std::array<function,6> functions{{
        {"lattice_observer_delivery_permit_v1",0,state::permit},
        {"lattice_observer_delivery_token_v1",0,state::token},
        {"lattice_observer_delivery_event_limit_v1",0,state::event_limit},
        {"lattice_observer_delivery_byte_limit_v1",0,state::byte_limit},
        {"lattice_observer_delivery_add_bytes_v1",2,state::add},
        {"lattice_observer_delivery_row_bytes_v1",-1,state::row}}};
    // All six DATA holder allocations precede first function publication.
    std::array<std::unique_ptr<std::shared_ptr<state>>,6> holders;
    for(auto& holder:holders)holder=std::make_unique<std::shared_ptr<state>>(value);
    if(creator!=database_native_process())throw db_error("inherited delivery SQL function allocation");
    // Complete allocating DATA holders before entering Native. Recheck the
    // exact original under its serialized admission before publishing any UDF.
    database::catalog_native_admission_ admitted(writer);
    if(writer.db_!=value->native||writer.closed_.load(std::memory_order_acquire)||
       writer.raw_handle_escaped_.load(std::memory_order_acquire)||writer.observer_delivery_sql_||sqlite3_get_autocommit(writer.db_)==0)
        throw db_error("delivery SQL registration original changed");
    if(sqlite3_next_stmt(writer.db_,nullptr))
        throw db_error("delivery SQL registration refuses preexisting prepared statements");
    sqlite3_stmt* existing=nullptr;
    int rc=sqlite3_prepare_v2(writer.db_,
        "SELECT 1 FROM pragma_function_list WHERE name GLOB 'lattice_observer_delivery_*' LIMIT 1",-1,&existing,nullptr);
    if(rc!=SQLITE_OK){sqlite3_finalize(existing);throw db_error("delivery SQL reserved function inspection failed");}
    rc=sqlite3_step(existing);const int ended=sqlite3_finalize(existing);
    if(rc!=SQLITE_DONE||ended!=SQLITE_OK)
        throw db_error("delivery SQL reserved function namespace already used or unavailable");
    for(size_t index=0;index<functions.size();++index) {
        const auto& function=functions[index];auto* holder=holders[index].release();
        const int rc=sqlite3_create_function_v2(writer.db_,function.name,function.args,SQLITE_UTF8,
            holder,function.call,nullptr,nullptr,state::release);
        if(rc!=SQLITE_OK){value->registration_failed=true;throw db_error("delivery SQL function registration failed");}
    }
    writer.observer_delivery_sql_=result; // actual field publication only after all six registrations
    return result;
}
sqlite3* observer_delivery_sql_runtime::native(database& writer)const {
    writer.require_creator_();
    if(!state_||state_->creator!=database_native_process()||state_->registration_failed||
       state_->owner!=&writer||writer.db_!=state_->native||writer.is_closed()||writer.observer_delivery_sql_.get()!=this)
        throw db_error("delivery SQL writer original differs");
    return state_->native;
}
bool observer_delivery_sql_runtime::admits(database& writer,sqlite3_stmt* statement)const noexcept {
    if(!state_||state_->creator!=database_native_process()||state_->owner!=&writer||
       writer.db_!=state_->native||!statement||sqlite3_db_handle(statement)!=state_->native)return false;
    // A real readonly result remains diagnostic even while an original is
    // fenced/unsettled. Transaction commands/ATTACH have zero result columns
    // and do not enter through this exception.
    if(sqlite3_stmt_readonly(statement)&&sqlite3_column_count(statement)>0)return true;
    return !state_->registration_failed&&state_->statement==statement;
}
bool observer_delivery_sql_runtime::has_original(const std::shared_ptr<const observer_delivery_original>& original)const noexcept {
    return state_&&state_->creator==database_native_process()&&!state_->registration_failed&&original&&
        state_->original==original&&!state_->original.owner_before(original)&&!original.owner_before(state_->original);
}
observer_delivery_sql_runtime::control_scope::control_scope(observer_delivery_sql_runtime& runtime,
    database& writer,sqlite3_stmt* statement)
    :creator_(database_native_process()),state_(runtime.state_),statement_(statement) {
    if(!state_||state_->creator!=creator_||state_->registration_failed||state_->statement||
       state_->owner!=&writer||writer.db_!=state_->native||!statement||
       sqlite3_db_handle(statement)!=state_->native||!sqlite3_stmt_readonly(statement)||
       sqlite3_column_count(statement)!=0||!state_->original)
        throw db_error("delivery SQL actual Core control admission differs");
    const auto& original=writer.core_transaction_original_;
    if(!original||!original->current_for(writer)||original->observer_delivery!=state_->original||
       original->observer_delivery.owner_before(state_->original)||state_->original.owner_before(original->observer_delivery))
        throw db_error("delivery SQL Core control lost its actual original");
    state_->control=true;state_->statement=statement;
}
observer_delivery_sql_runtime::control_scope::~control_scope()noexcept {
    if(creator_==database_native_process()&&state_->creator==creator_&&state_->statement==statement_) {
        state_->statement=nullptr;state_->control=false;
    }
}
void observer_delivery_sql_runtime::moved()noexcept {
    if(state_&&state_->creator==database_native_process())state_->registration_failed=true;
}
void observer_delivery_sql_runtime::original_preparing(std::shared_ptr<const observer_delivery_original> original) {
    const auto& value=state_;
    if(!value||value->creator!=database_native_process()||value->registration_failed||value->statement||value->original||
       !original||original->creator_!=value->creator||!original->claimed_.load(std::memory_order_acquire)||
       original->began_.load(std::memory_order_acquire)||original->ended_.load(std::memory_order_acquire))
        throw db_error("delivery SQL original pre-BEGIN publication differs");
    value->original=std::move(original);
}
void observer_delivery_sql_runtime::original_unissued(const std::shared_ptr<const observer_delivery_original>& original) {
    const auto& value=state_;
    if(!value||value->creator!=database_native_process()||value->statement||value->original!=original||
       value->original.owner_before(original)||original.owner_before(value->original)||
       !original->unissued_.load(std::memory_order_acquire)||original->began_.load(std::memory_order_acquire))
        throw db_error("delivery SQL known-unissued original differs");
    value->original.reset(); // already-admitted offer/original still owns it
}
void observer_delivery_sql_runtime::original_captured(const std::shared_ptr<const observer_delivery_original>& original) {
    const auto& value=state_;
    if(!value||value->creator!=database_native_process()||value->statement||value->original!=original||
       value->original.owner_before(original)||original.owner_before(value->original)||
       !original->captured_.load(std::memory_order_acquire))throw db_error("delivery SQL actual capture differs");
    value->original.reset(); // actual capture/context pin the same DATA outside this slot
}
observer_delivery_sql_runtime::statement_scope::statement_scope(observer_delivery_sql_runtime& runtime,database& writer,sqlite3_stmt* statement)
    :creator_(database_native_process()),state_(runtime.state_),statement_(statement) {
    if(!state_||state_->creator!=creator_||state_->registration_failed||state_->owner!=&writer||
       writer.db_!=state_->native||writer.observer_delivery_sql_.get()!=&runtime||state_->statement||!statement||
       sqlite3_db_handle(statement)!=state_->native||sqlite3_stmt_readonly(statement)||!state_->original||
       !state_->original->began_.load(std::memory_order_acquire)||state_->original->ended_.load(std::memory_order_acquire))
        throw db_error("delivery SQL closed statement admission differs");
    const auto& native_original=writer.core_transaction_original_;
    if(!native_original||!native_original->current_for(writer)||
       native_original->observer_delivery!=state_->original||
       native_original->observer_delivery.owner_before(state_->original)||
       state_->original.owner_before(native_original->observer_delivery)||
       sqlite3_get_autocommit(state_->native)!=0)
        throw db_error("delivery SQL statement lost its genuine current Native original");
    state_->statement=statement_;
}
observer_delivery_sql_runtime::statement_scope::~statement_scope()noexcept {retire();}
void observer_delivery_sql_runtime::statement_scope::retire()noexcept {
    if(creator_==database_native_process()&&state_->creator==creator_&&state_->statement==statement_)
        state_->statement=nullptr;
}
struct observer_delivery_sql_runtime::prepared::payload {
    const uint64_t creator;
    // Destroy opaque retained failure first, Native stmt before immutable
    // STATIC-bind input, and actual input before the final delivery credit.
    observer_delivery_plan input;
    std::shared_ptr<observer_delivery_sql_runtime> runtime;
    sqlite3_stmt* statement=nullptr;
    std::exception_ptr first;
    bool issued=false;
    explicit payload(uint64_t issuer):creator(issuer){}
    static void destroy(payload* value)noexcept {
        if(!value||value->creator!=database_native_process())return;
        if(value->statement) {
            database::finalize_mutation_statement_(std::exchange(value->statement,nullptr));
            if(value->creator!=database_native_process())return;
        }
        value->first={};if(value->creator!=database_native_process())return;
        value->runtime.reset();if(value->creator!=database_native_process())return;
        value->input={};if(value->creator!=database_native_process())return;
        delete value;
    }
};
observer_delivery_sql_runtime::prepared observer_delivery_sql_runtime::prepare(database& writer,
    const observer_delivery_plan& plan) {
    if(!plan.valid()||database::sql_entry_scope::active_for(&writer)||
       database::mutation_sql_scope::active_for(&writer))
        throw db_error("delivery prepared input must precede Native operation leaves");
    {
        database::catalog_native_admission_ admitted(writer);
        (void)native(writer);
        const auto& borrowed=plan.borrowed_original();
        if(borrowed) {
            const auto& actual=writer.core_transaction_original_;
            if(!actual||!actual->current_for(writer)||actual->observer_delivery!=borrowed||
               !has_original(borrowed)||!borrowed->explicit_envelope_||sqlite3_get_autocommit(writer.db_)!=0)
                throw db_error("delivery subplan lost its actual admitted caller original");
        }else if(sqlite3_get_autocommit(writer.db_)==0)
            throw db_error("delivery fresh plan cannot adopt a caller original");
    }
    static_assert(sizeof(prepared::payload)<=observer_delivery_plan::prepared_storage_bytes);
    const auto creator=database_native_process();
    plan.claim_preparation();prepared::payload* value=nullptr;
    try {
        value=new prepared::payload(creator);
        if(creator!=database_native_process())throw db_error("inherited delivery prepared allocation");
        value->input=plan;value->runtime=writer.observer_delivery_sql_;
        if(value->runtime.get()!=this)throw db_error("delivery prepared runtime changed");
        return prepared(value);
    }catch(...) {
        if(creator==database_native_process()) {
            prepared::payload::destroy(value);plan.release_unissued_preparation();
        }
        throw;
    }
}
observer_delivery_sql_runtime::prepared::prepared(prepared&& other)noexcept
    :value_(std::exchange(other.value_,nullptr)){}
observer_delivery_sql_runtime::prepared::~prepared()noexcept {payload::destroy(value_);}
void observer_delivery_sql_runtime::prepared::execute(database& writer) {
    auto* const value=value_;
    if(!value||value->creator!=database_native_process()||!value->runtime)
        throw db_error("delivery prepared input is inherited or unavailable");
    if(value->issued||!database::mutation_sql_scope::active_for(&writer))writer.refuse_delivery_statement_();
    auto& runtime=*value->runtime;auto* native=runtime.native(writer);
    const auto& actual=writer.core_transaction_original_;
    if(!actual||!actual->current_for(writer)||!actual->observer_delivery)
        writer.refuse_delivery_statement_();
    const auto& borrowed=value->input.borrowed_original();
    if(borrowed) {
        if(actual->observer_delivery!=borrowed||!runtime.has_original(borrowed)||!borrowed->explicit_envelope_)
            writer.refuse_delivery_statement_();
    }else {
        const auto input=value->input.input_identity();
        if(actual->observer_delivery->input_identity_.expired()||
           input.owner_before(actual->observer_delivery->input_identity_)||
           actual->observer_delivery->input_identity_.owner_before(input))
            writer.refuse_delivery_statement_();
    }
    value->issued=true; // no retry/renewal, including a later caught refusal
    try {
        size_t row_index=0;
        for(const auto& row:value->input.rows()) {
            if(row.is_update) {
                // Bound identity is provenance; this same-unit readonly query
                // only classifies absent vs same vs a reused-id successor. It
                // never grants an identity from a current row/path lookup.
                const char* tail=nullptr;database::record_statement();
                int rc=sqlite3_prepare_v2(native,row.identity_sql.c_str(),int(row.identity_sql.size()+1),&value->statement,&tail);
                if(value->creator!=database_native_process())throw db_error("inherited delivery identity prepare");
                if(rc!=SQLITE_OK||!value->statement||!tail||*tail||!sqlite3_stmt_readonly(value->statement)||
                   sqlite3_column_count(value->statement)!=1||sqlite3_bind_parameter_count(value->statement)!=2)
                    throw db_error("delivery generated identity comparison differs");
                rc=sqlite3_bind_text64(value->statement,1,row.target_global_id.data(),row.target_global_id.size(),SQLITE_STATIC,SQLITE_UTF8);
                if(value->creator!=database_native_process())throw db_error("inherited delivery identity bind");
                if(rc!=SQLITE_OK||sqlite3_bind_int64(value->statement,2,row.target_rowid)!=SQLITE_OK)
                    throw db_error("delivery identity comparison bind failed");
                rc=writer.step_statement_(value->statement);
                if(value->creator!=database_native_process())throw db_error("inherited delivery identity step");
                const bool present=rc==SQLITE_ROW;
                bool same=false;
                if(present) {
                    if(sqlite3_column_type(value->statement,0)!=SQLITE_INTEGER)
                        throw db_error("delivery current identity is not a known scalar result");
                    const auto comparison=sqlite3_column_int(value->statement,0);
                    if(comparison!=0&&comparison!=1)throw db_error("delivery identity comparison result differs");
                    same=comparison==1;
                    rc=writer.step_statement_(value->statement);
                    if(value->creator!=database_native_process())throw db_error("inherited delivery identity ending");
                }
                if(rc!=SQLITE_DONE) {
                    try{throw db_error("delivery identity comparison did not end exactly");}
                    catch(...){value->first=std::current_exception();}
                    database::finalize_failed_mutation_statement_(std::exchange(value->statement,nullptr),rc,value->first);
                    if(value->creator!=database_native_process())throw db_error("inherited delivery identity failed finalize");
                    std::rethrow_exception(value->first);
                }
                const int ended=database::finalize_mutation_statement_(std::exchange(value->statement,nullptr));
                if(value->creator!=database_native_process())throw db_error("inherited delivery identity finalize");
                if(ended!=SQLITE_OK)throw db_error("delivery identity comparison finalize failed");
                if(!present){value->input.record_update_present(false);++row_index;continue;}
                if(!same)throw db_error("delivery UPDATE target id now belongs to a different globalId");
            }
            const char* tail=nullptr;database::record_statement();
            int rc=sqlite3_prepare_v2(native,row.insert_sql.c_str(),int(row.insert_sql.size()+1),&value->statement,&tail);
            if(value->creator!=database_native_process())throw db_error("inherited delivery prepare return");
            if(rc!=SQLITE_OK||!value->statement||!tail||*tail||
               sqlite3_column_count(value->statement)!=0||sqlite3_stmt_readonly(value->statement)||
               sqlite3_bind_parameter_count(value->statement)!=int(row.fields.size()+(row.is_update?2:0)))
                throw db_error("delivery generated statement prepare differs");
            statement_scope admitted(runtime,writer,value->statement);
            for(size_t index=0;index<row.fields.size();++index) {
                rc=std::visit([&](const auto& contents)->int {
                    using T=std::decay_t<decltype(contents)>;const int slot=int(index+1);
                    if constexpr(std::is_same_v<T,std::nullptr_t>)return sqlite3_bind_null(value->statement,slot);
                    else if constexpr(std::is_same_v<T,int64_t>)return sqlite3_bind_int64(value->statement,slot,contents);
                    else if constexpr(std::is_same_v<T,double>)return sqlite3_bind_double(value->statement,slot,contents);
                    else if constexpr(std::is_same_v<T,std::string>)return sqlite3_bind_text64(value->statement,slot,contents.data(),contents.size(),SQLITE_STATIC,SQLITE_UTF8);
                    else return contents.empty()?sqlite3_bind_zeroblob64(value->statement,slot,0):
                        sqlite3_bind_blob64(value->statement,slot,contents.data(),contents.size(),SQLITE_STATIC);
                },row.fields[index].contents);
                if(value->creator!=database_native_process())throw db_error("inherited delivery bind return");
                if(rc!=SQLITE_OK)throw db_error("delivery static parameter bind failed");
            }
            if(row.is_update) {
                rc=sqlite3_bind_int64(value->statement,2,row.target_rowid);
                if(value->creator!=database_native_process())throw db_error("inherited delivery UPDATE id bind");
                if(rc!=SQLITE_OK)throw db_error("delivery UPDATE id bind failed");
                rc=sqlite3_bind_text64(value->statement,3,row.target_global_id.data(),row.target_global_id.size(),SQLITE_STATIC,SQLITE_UTF8);
                if(value->creator!=database_native_process())throw db_error("inherited delivery UPDATE identity bind");
                if(rc!=SQLITE_OK)throw db_error("delivery UPDATE identity bind failed");
            }
            rc=writer.step_statement_(value->statement);
            if(value->creator!=database_native_process())throw db_error("inherited delivery step return");
            if(rc!=SQLITE_DONE) {
                try{throw db_error(std::string("delivery generated step failed: ")+sqlite3_errmsg(native));}
                catch(...){value->first=std::current_exception();}
                admitted.retire();
                database::finalize_failed_mutation_statement_(std::exchange(value->statement,nullptr),rc,value->first);
                if(value->creator!=database_native_process())throw db_error("inherited delivery failed finalize return");
                std::rethrow_exception(value->first);
            }
            if(!value->input.inserted_rows().empty())
                value->input.record_inserted(row_index,sqlite3_last_insert_rowid(native));
            admitted.retire();
            const int ended=database::finalize_mutation_statement_(std::exchange(value->statement,nullptr));
            if(value->creator!=database_native_process())throw db_error("inherited delivery finalize return");
            if(ended!=SQLITE_OK)throw db_error("delivery generated statement finalize failed");
            if(row.is_update)value->input.record_update_present(true);
            ++row_index;
        }
    }catch(...) {
        if(value->creator!=database_native_process())throw;
        if(!value->first)value->first=std::current_exception();
        // Every remaining statement is finalized while the actual operation
        // still owns its original/Native leaf, BEFORE its rollback/RELEASE.
        if(value->statement)database::finalize_mutation_statement_(std::exchange(value->statement,nullptr));
        if(value->creator!=database_native_process())throw;
        // Finalization got the first opportunity to retain genuinely different
        // cleanup debt. If none exists, latch the SAME primary so a body that
        // catches a bind/prepare/profile refusal still cannot commit this unit.
        writer.latch_delivery_failure_(value->first);
        std::rethrow_exception(value->first);
    }
}

}
