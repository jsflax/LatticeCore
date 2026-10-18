#include "ordinary_history_storage_recipe.hpp"
#include "ordinary_target_raw_history_storage_recipe.hpp"
// Fixed normal metadata validators on the SAME request/shell read snapshot.
// No public SQL callback/handle or fingerprint/header-derived native grant.
#include "ordinary_export.hpp"
#include <cmath>
#include <algorithm>
#include <type_traits>
namespace lattice {
struct database::target_staging_state::history_projection_native {
    terminal_native_static_tail terminal_static; // FIRST: rows/SQL and actual programme/scope references die before this.
    database* const shell;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const std::weak_ptr<detail::ordinary_group_target_schema_original> schema;
    const std::shared_ptr<detail::ordinary_group_target_schema_original::projection_program> program;
    std::unique_ptr<detail::ordinary_target_staging_connection::database_scope> scope;
    static constexpr std::size_t statements_limit=388,normal_calls=32768,total_calls=33024;
    static constexpr std::size_t begin=0,anchor=1,commit=386,rollback=387;
    struct statement {
        std::string sql;
        std::vector<column_value_t> bindings;
        std::vector<std::string> keys;
        std::vector<database::row_t> rows; // Actual observations survive validator/copy/ending failure.
        sqlite3_stmt* held=nullptr;sqlite3_stmt* prepared_identity=nullptr;sqlite3_stmt* finalized_identity=nullptr;
        bool prepare_entered=false,prepare_returned=false,readonly_returned=false,terminal_returned=false;
        bool finalize_entered=false,finalize_returned=false,destruction_known=false;
        bool first_step_returned=false;int prepare_rc=SQLITE_MISUSE,readonly_value=0,first_step_rc=SQLITE_MISUSE,terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
    };
    enum class kind {prepare,readonly,parameters,bind_text,bind_integer,columns,name,step,type,integer,real,bytes,text,blob,finalize,autocommit,txn_state};
    struct native_call {kind type=kind::prepare;std::size_t statement=0,column=0;bool entered=false,returned=false;
        std::int64_t integer=SQLITE_MISUSE;double real=0;const void* pointer=nullptr;};
    std::array<statement,statements_limit> statements{};
    std::array<native_call,total_calls> calls{};
    std::size_t call_count=0,queries_issued=0,logical_bytes=sizeof(history_projection_native);
    bool ending=false,left=false,completed=false,snapshot_observed=false,header_checked=false;
    bool ending_autocommit_returned=false;int ending_autocommit=0;
    int snapshot_state=SQLITE_TXN_NONE;
    std::exception_ptr first_failure;
    history_projection_native(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
        const std::shared_ptr<detail::ordinary_group_target_schema_original>& original,
        const std::shared_ptr<detail::ordinary_group_target_schema_original::projection_program>& input)
        :terminal_static(actual,this,original.get(),2,receiving),shell(&actual),owner(receiving),schema(original),program(input) {
        // All statement/call/ending capacity precedes the genuine BEGIN. Existing
        // header/latest-page originals share the unchanged 24 MiB staging pool.
        const auto held=actual.target_staging_;
        if(!held||!held->history_header)throw db_error("GROUP projection actual header capacity missing");
        add_stage(held->history_header->logical_bytes);
        if(held->history_page)add_stage(held->history_page->logical_bytes);
        terminal_static.constructor_returned();
    }
    void add_stage(std::size_t bytes) {
        if(bytes>ordinary_history_limits::stage_scratch_bytes-logical_bytes)
            throw db_error("GROUP projection combined staging capacity refused");
        logical_bytes+=bytes;
    }
    void require(database& actual,bool unwind=false)const {
        const auto state=actual.target_staging_;
        if(!state||!state->current_creator()||shell!=&actual||state->history_projection.get()!=this||!scope||left||
           !actual.db_||state->retirement_requested||actual.retirement_->owning_wrapper_moved()||actual.retirement_->raw_handle_escaped())
            throw db_error("GROUP projection actual Native shell/scope unavailable");
        scope->require_projection_role(actual);
        if(unwind){if(!ending)throw db_error("GROUP projection original ending unavailable");scope->require_current(actual);return;}
        if(ending||first_failure)throw db_error("GROUP projection Native work sealed");
        const auto receiving=owner.lock();const auto original=schema.lock();const auto connection=state->connection.lock();
        if(!receiving||!original||!connection||receiving->require_header_schema(*connection)!=original||
           original->require_projection(*connection,state->end)!=program)
            throw db_error("GROUP projection actual request/schema original retired");
        actual.require_target_history_complete_(*connection,*receiving);scope->require_schema_work(actual);
    }
    native_call& reserve(kind type,std::size_t index,bool unwind=false,std::size_t column=0) {
        if(index>=statements_limit||call_count>=(unwind?total_calls:normal_calls))
            throw db_error("GROUP projection fixed Native ledger refused");
        auto& call=calls[call_count++];call.type=type;call.statement=index;call.column=column;return call;
    }
    template<class Value> static void returned(native_call& call,Value value)noexcept {
        if constexpr(std::is_pointer_v<Value>)call.pointer=value;
        else if constexpr(std::is_floating_point_v<Value>)call.real=value;
        else call.integer=static_cast<std::int64_t>(value);
        call.returned=true; // Actual returned primitive/pointer before current/error conversion.
    }
    template<class Fn> auto observe(database& actual,kind type,std::size_t index,Fn invoke,bool unwind=false,std::size_t column=0) {
        require(actual,unwind);auto& call=reserve(type,index,unwind,column);require(actual,unwind);call.entered=true;
        const auto value=invoke();returned(call,value);require(actual,unwind);return value;
    }
    void retain_statement(std::size_t index,const std::string& sql,const std::vector<column_value_t>& bindings) {
        auto& held=statements.at(index);
        if(held.prepare_entered||!held.sql.empty()||sql.empty()||sql.size()>256*1024||sql.find('\0')!=std::string::npos||bindings.size()>16)
            throw db_error("GROUP projection generated statement shape refused");
        held.sql=sql;add_stage(held.sql.capacity());held.bindings=bindings;
        add_stage(held.bindings.capacity()*sizeof(column_value_t));
        for(const auto& value:held.bindings) {
            if(const auto* text=std::get_if<std::string>(&value)) {
                if(text->size()>256)throw db_error("GROUP projection normal metadata binding refused");add_stage(text->capacity());
            }else if(!std::holds_alternative<std::int64_t>(value))throw db_error("GROUP projection generated binding storage refused");
        }
    }
    void prepare(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];const auto& sql=statement.sql;
        if(statement.prepare_entered||sql.empty())throw db_error("GROUP projection statement reissue refused");
        require(actual,unwind);auto& call=reserve(kind::prepare,index,unwind);require(actual,unwind);
        const char* tail=nullptr;statement.prepare_entered=true;call.entered=true;
        statement.prepare_rc=::lattice::detail::store_physical_prepare_v2(actual.db_,sql.data(),static_cast<int>(sql.size()),&statement.held,&tail);
        statement.prepared_identity=statement.held;statement.prepare_returned=true;returned(call,statement.prepare_rc);
        require(actual,unwind);
        if(statement.prepare_rc!=SQLITE_OK||!statement.held||tail!=sql.data()+sql.size())throw db_error("GROUP projection actual Native preparation failed/changed");
        const int readonly=observe(actual,kind::readonly,index,[&]{return sqlite3_stmt_readonly(statement.held);},unwind);
        statement.readonly_value=readonly;statement.readonly_returned=true;
        if(!readonly)throw db_error("GROUP projection generated VM is not actually read-only");
        const int parameters=observe(actual,kind::parameters,index,[&]{return sqlite3_bind_parameter_count(statement.held);},unwind);
        if(parameters!=static_cast<int>(statement.bindings.size()))throw db_error("GROUP projection generated Native parameter count changed");
        for(std::size_t n=0;n<statement.bindings.size();++n) {
            int rc=SQLITE_MISUSE;
            if(const auto* value=std::get_if<std::string>(&statement.bindings[n]))
                rc=observe(actual,kind::bind_text,index,[&]{return sqlite3_bind_text(statement.held,static_cast<int>(n+1),value->data(),static_cast<int>(value->size()),SQLITE_STATIC);},unwind,n);
            else if(const auto* value=std::get_if<std::int64_t>(&statement.bindings[n]))
                rc=observe(actual,kind::bind_integer,index,[&]{return sqlite3_bind_int64(statement.held,static_cast<int>(n+1),*value);},unwind,n);
            if(rc!=SQLITE_OK)throw db_error("GROUP projection generated binding failed");
        }
    }
    int step(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];require(actual,unwind);auto& call=reserve(kind::step,index,unwind);require(actual,unwind);call.entered=true;
        const int rc=::lattice::detail::store_physical_step(statement.held);returned(call,rc);
        if(!statement.first_step_returned){statement.first_step_rc=rc;statement.first_step_returned=true;}
        if(rc!=SQLITE_ROW){statement.terminal_rc=rc;statement.terminal_returned=true;}
        if(index==begin)scope->record_schema_begin(actual,rc);
        else if(index==commit)scope->record_schema_commit(actual,rc);
        else if(index==rollback)scope->record_schema_rollback(actual,rc);
        require(actual,unwind);return rc;
    }
    void finish(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];if(!statement.held||statement.finalize_entered)return;
        require(actual,unwind);auto& call=reserve(kind::finalize,index,unwind);require(actual,unwind);
        statement.finalized_identity=statement.held;statement.finalize_entered=true;call.entered=true;
        statement.finalize_rc=::lattice::detail::store_physical_finalize(statement.held);statement.finalize_returned=true;returned(call,statement.finalize_rc);
        if((statement.finalize_rc&0xff)!=SQLITE_MISUSE){statement.destruction_known=true;statement.held=nullptr;}
        scope->record_schema_finalized(actual,statement.finalize_rc);require(actual,unwind);
        if(statement.finalize_rc!=SQLITE_OK||!statement.destruction_known)throw db_error("GROUP projection actual VM ending failed/unknown");
    }
    void control(database& actual,std::size_t index,const char* fixed,bool unwind=false) {
        retain_statement(index,fixed,{});prepare(actual,index,unwind);
        if(step(actual,index,unwind)!=SQLITE_DONE)throw db_error("GROUP projection actual fixed Native ending failed");finish(actual,index,unwind);
    }
    template<class T> void metadata_reserve(std::vector<T>& values,std::size_t capacity) {
        if(capacity<=values.capacity())return;
        if(capacity>program->maximum_bytes/sizeof(T))throw db_error("GROUP projection metadata vector capacity refused");
        const auto old=values.capacity();program->add((capacity-old)*sizeof(T));values.reserve(capacity);
        if(values.capacity()>capacity)program->add((values.capacity()-capacity)*sizeof(T));
    }
    std::vector<database::row_t> read(database& actual,std::size_t index,const std::string& sql,
        const std::vector<column_value_t>& bindings,std::size_t limit) {
        if(!limit||limit>262144||(!snapshot_observed&&index!=anchor))throw db_error("GROUP projection bounded retained snapshot unavailable");
        require(actual);retain_statement(index,sql,bindings);prepare(actual,index);auto& statement=statements[index];
        const int columns=observe(actual,kind::columns,index,[&]{return sqlite3_column_count(statement.held);});
        if(columns<1||columns>16)throw db_error("GROUP projection normal metadata column limit refused");
        metadata_reserve(statement.keys,static_cast<std::size_t>(columns));
        for(int c=0;c<columns;++c) {
            const char* name=observe(actual,kind::name,index,[&]{return sqlite3_column_name(statement.held,c);},false,c);
            if(!name)throw db_error("GROUP projection actual column name unavailable");
            std::size_t bytes=0;while(bytes<=256&&name[bytes])++bytes;
            if(!bytes||bytes>256)throw db_error("GROUP projection actual column name bound refused");
            program->add(bytes);statement.keys.emplace_back(name,bytes);
            if(statement.keys.back().capacity()>bytes)program->add(statement.keys.back().capacity()-bytes);
            if(std::find(statement.keys.begin(),statement.keys.end()-1,statement.keys.back())!=statement.keys.end()-1)
                throw db_error("GROUP projection duplicate column name refused");
        }
        for(;;) {
            const int rc=step(actual,index);if(rc==SQLITE_DONE)break;if(rc!=SQLITE_ROW)throw db_error("GROUP projection actual metadata step failed");
            if(statement.rows.size()==limit)throw db_error("GROUP projection metadata row limit refused before copy");
            if(statement.rows.size()==statement.rows.capacity())metadata_reserve(statement.rows,std::min(limit,std::max<std::size_t>(1,statement.rows.capacity()*2)));
            statement.rows.emplace_back();auto& row=statement.rows.back();
            // The actual row is retained before scalar observations/copies. Logical
            // node/bucket accounting does not assert allocator/RSS workspace bounds.
            row.reserve(static_cast<std::size_t>(columns));program->add(row.bucket_count()*sizeof(void*));
            for(int c=0;c<columns;++c) {
                program->cell();const auto& key=statement.keys[static_cast<std::size_t>(c)];
                program->add(sizeof(database::row_t::value_type)+2*sizeof(void*)+key.size());
                const int type=observe(actual,kind::type,index,[&]{return sqlite3_column_type(statement.held,c);},false,c);
                column_value_t value=nullptr;
                if(type==SQLITE_INTEGER)value=observe(actual,kind::integer,index,[&]{return sqlite3_column_int64(statement.held,c);},false,c);
                else if(type==SQLITE_FLOAT) {
                    const auto number=observe(actual,kind::real,index,[&]{return sqlite3_column_double(statement.held,c);},false,c);
                    if(!std::isfinite(number))throw db_error("GROUP projection nonfinite metadata observation refused");value=number;
                }else if(type==SQLITE_TEXT||type==SQLITE_BLOB) {
                    const int bytes=observe(actual,kind::bytes,index,[&]{return sqlite3_column_bytes(statement.held,c);},false,c);
                    if(bytes<0)throw db_error("GROUP projection negative metadata bytes refused");program->add(static_cast<std::size_t>(bytes));
                    if(type==SQLITE_TEXT) {
                        const auto* data=observe(actual,kind::text,index,[&]{return sqlite3_column_text(statement.held,c);},false,c);
                        if(!data&&bytes)throw db_error("GROUP projection actual text observation unavailable");
                        value=bytes?std::string(reinterpret_cast<const char*>(data),static_cast<std::size_t>(bytes)):std::string{};
                        const auto capacity=std::get<std::string>(value).capacity();if(capacity>static_cast<std::size_t>(bytes))program->add(capacity-static_cast<std::size_t>(bytes));
                    }else {
                        const auto* data=static_cast<const std::uint8_t*>(observe(actual,kind::blob,index,[&]{return sqlite3_column_blob(statement.held,c);},false,c));
                        if(!data&&bytes)throw db_error("GROUP projection actual blob observation unavailable");
                        value=bytes?std::vector<std::uint8_t>(data,data+bytes):std::vector<std::uint8_t>{};
                        const auto capacity=std::get<std::vector<std::uint8_t>>(value).capacity();if(capacity>static_cast<std::size_t>(bytes))program->add(capacity-static_cast<std::size_t>(bytes));
                    }
                }else if(type!=SQLITE_NULL)throw db_error("GROUP projection actual metadata storage refused");
                row.emplace(key,std::move(value));require(actual);
            }
        }
        finish(actual,index);
        // Preserve the actual first observations on this original. The private
        // validator gets a bounded copy, charged before that output allocation.
        std::size_t copy=statement.rows.size()*sizeof(database::row_t);
        if(copy>program->maximum_bytes)throw db_error("GROUP projection output vector bound refused");
        for(const auto& row:statement.rows) {
            const auto buckets=row.bucket_count()*sizeof(void*);
            if(buckets>program->maximum_bytes-copy)throw db_error("GROUP projection output buckets bound refused");copy+=buckets;
            for(const auto& [key,value]:row) {
                const auto extra=sizeof(database::row_t::value_type)+2*sizeof(void*)+key.capacity()+
                    (std::get_if<std::string>(&value)?std::get<std::string>(value).capacity():
                     std::get_if<std::vector<std::uint8_t>>(&value)?std::get<std::vector<std::uint8_t>>(value).capacity():0);
                if(extra>program->maximum_bytes-copy)throw db_error("GROUP projection output copy bound refused");copy+=extra;
            }
        }
        program->add(copy);auto result=statement.rows;require(actual);return result;
    }
    void enter_ending(database& actual) {
        if(!ending){scope->begin_schema_ending(actual);ending=true;}
    }
    bool ending_known()const noexcept {
        if(first_failure||ending||!snapshot_observed||snapshot_state!=SQLITE_TXN_READ||!header_checked||
           !program->validation_returned||program->first_failure||!program->metadata||!program->internal)return false;
        for(std::size_t n=0;n<call_count;++n)if(calls[n].entered&&!calls[n].returned)return false;
        for(std::size_t n=0;n<statements.size();++n)if(statements[n].prepare_entered) {
            const auto& statement=statements[n];if(!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||
                !statement.readonly_returned||!statement.readonly_value||
                !statement.terminal_returned||statement.terminal_rc!=SQLITE_DONE||!statement.finalize_returned||
                statement.finalize_rc!=SQLITE_OK||!statement.destruction_known||statement.held)return false;
        }
        return statements[begin].terminal_returned&&statements[begin].terminal_rc==SQLITE_DONE&&
            statements[commit].terminal_returned&&statements[commit].terminal_rc==SQLITE_DONE;
    }
    bool cleanup_known()const noexcept {
        for(std::size_t n=0;n<call_count;++n)if(calls[n].entered&&!calls[n].returned)return false;
        for(const auto& statement:statements)if(statement.prepare_entered) {
            if(!statement.prepare_returned)return false;
            if(statement.prepared_identity&&(!statement.finalize_returned||!statement.destruction_known||statement.held))return false;
        }
        const auto& started=statements[begin];const auto& ended=statements[commit];const auto& undone=statements[rollback];
        if(started.terminal_returned&&started.terminal_rc==SQLITE_DONE&&!(ended.terminal_returned&&ended.terminal_rc==SQLITE_DONE)&&
           !(undone.terminal_returned&&undone.terminal_rc==SQLITE_DONE)&&!(ending_autocommit_returned&&ending_autocommit!=0))return false;
        return true; // Known original Native endings only; first failure remains retained.
    }
    void settle_failed(database& actual)noexcept {
        try {
            enter_ending(actual);
            for(std::size_t n=0;n<statements.size();++n)if(statements[n].held&&!statements[n].finalize_entered)finish(actual,n,true);
            if(statements[begin].terminal_returned&&statements[begin].terminal_rc==SQLITE_DONE&&
               !(statements[commit].terminal_returned&&statements[commit].terminal_rc==SQLITE_DONE)) {
                require(actual,true);auto& call=reserve(kind::autocommit,rollback,true);require(actual,true);call.entered=true;
                ending_autocommit=sqlite3_get_autocommit(actual.db_);ending_autocommit_returned=true;returned(call,ending_autocommit);
                require(actual,true);if(!ending_autocommit)control(actual,rollback,"ROLLBACK",true);
            }
            left=scope->leave(actual); // Refusal is retained debt, never completion/current credit.
        }catch(...){}
    }
};
void database::require_target_history_complete_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    require_target_history_header_ending_(connection,receiving);const auto state=target_staging_;
    const auto& header=receiving.require_header_original(connection);
    if(header.pages) {
        const auto progress=target_history_page_progress_(connection,receiving);
        if(!progress.complete||progress.next_page!=header.pages||progress.rows!=header.rows||
           progress.known!=header.known||progress.unknown!=header.unknown)throw db_error("GROUP actual final staged page Native ending missing");
    }else if(header.rows||header.known||header.unknown||state->history_page||header.content_digest!=picosha2::hash256_hex_string(std::string{}))
        throw db_error("GROUP actual empty history/header content differs");
}
bool database::target_history_projection_cleanup_ending_()const noexcept {
    const auto state=target_staging_;if(!state||!state->current_creator()||state->terminal_native_statics_attempted)return false;
    return !state->history_projection||state->history_projection->cleanup_known();
}
bool database::target_history_projection_native_ending_()const noexcept {
    const auto state=target_staging_;const auto native=state?state->history_projection:nullptr;
    return state&&state->current_creator()&&!state->terminal_native_statics_attempted&&native&&native->shell==this&&native->ending_known();
}
void database::require_target_history_projection_ending_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    require_target_history_complete_(connection,receiving);const auto state=target_staging_;const auto native=state->history_projection;
    if(state->connection.lock().get()!=&connection||!native||native->owner.lock().get()!=&receiving||
       native->schema.lock()!=receiving.require_header_schema(connection)||!native->completed||!native->left||
       !native->scope||!native->scope->leave(*this)||!target_history_projection_native_ending_())throw db_error("GROUP projection actual Native read ending missing");
}
void database::validate_target_history_projection_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving) {
    const auto state=target_staging_;if(!state||!state->current_creator()||!receiving||state->history_projection_attempted||state->history_projection)
        throw db_error("GROUP projection actual one-shot original unavailable");
    const auto connection=state->require_original();require_target_history_complete_(*connection,*receiving);
    const bool raw_storage=target_history_raw_storage_(*connection,*receiving); // Actual retained header mode, before SQL scope.
    const auto schema=receiving->require_header_schema(*connection);const auto input=schema->prepare_projection(*connection,state->end);
    state->history_projection_attempted=true;
    try {
        state->history_projection=std::make_shared<target_staging_state::history_projection_native>(*this,receiving,schema,input);
        const auto native=state->history_projection; // Hold before scope effects/postchecks/diagnostics.
        connection->begin_database_projection(*this,receiving,native->scope,state->end);
        native->require(*this);native->control(*this,native->begin,"BEGIN DEFERRED");
        const auto anchor=native->read(*this,native->anchor,"SELECT COUNT(*) AS n FROM main.sqlite_schema",{},1);
        if(anchor.size()!=1||anchor[0].size()!=1||!std::get_if<std::int64_t>(&anchor[0].at("n"))||std::get<std::int64_t>(anchor[0].at("n"))<0)
            throw db_error("GROUP projection actual read anchor malformed");
        native->snapshot_state=native->observe(*this,target_staging_state::history_projection_native::kind::txn_state,native->anchor,[&]{return sqlite3_txn_state(db_,"main");});
        native->snapshot_observed=true;
        if(native->snapshot_state!=SQLITE_TXN_READ)throw db_error("GROUP projection actual main read snapshot missing");
        const detail::ordinary_metadata_query query=[&](const std::string& sql,const std::vector<column_value_t>& bindings,std::size_t limit) {
            native->require(*this);if(native->queries_issued==384)throw db_error("GROUP projection fixed generated query count refused");
            const auto at=2+native->queries_issued++;return native->read(*this,at,sql,bindings,limit);
        };
        // Target-owned page format is observed in this genuine SAME READ
        // snapshot even for empty history. The generic internal schema recipe
        // continues to validate only its own normal internal tables.
        const auto format=query("SELECT name,sql FROM main.sqlite_schema WHERE type='table' AND name IN ('_lattice_ordered_history_page_v1','_lattice_ordered_history_raw_page_v1') ORDER BY name",{},2);
        const char* wanted_name=raw_storage?detail::ordinary_target_raw_history_storage_recipe::page_name:"_lattice_ordered_history_page_v1";
        const char* wanted_sql=raw_storage?detail::ordinary_target_raw_history_storage_recipe::page:detail::ordinary_history_storage_recipe::page;
        if(format.size()!=1||format[0].size()!=2||format[0].find("name")==format[0].end()||format[0].find("sql")==format[0].end()||
           format[0].at("name")!=column_value_t(std::string(wanted_name))||format[0].at("sql")!=column_value_t(std::string(wanted_sql)))
            throw db_error("GROUP projection actual selected raw/BLOB page format differs or mixed");
        schema->validate_projection_read(*connection,query,state->end);
        const auto& header=receiving->require_header_original(*connection);
        if(input->logical->models!=header.projection.models||input->logical->scope!=header.projection.scope||
           input->logical->schema!=header.projection.schema||input->logical->tables.size()!=header.projection.tables.size())
            throw db_error("GROUP actual generated logical projection/header differs");
        for(std::size_t n=0;n<input->logical->tables.size();++n) {
            const auto& generated=input->logical->tables[n];const auto& observed=header.projection.tables[n];
            if(generated.name!=observed.name||generated.columns!=observed.columns||generated.types!=observed.types)
                throw db_error("GROUP actual generated table/column/type header differs");
        }
        native->header_checked=true; // Recorded comparison DATA; not native role or accepted-cut authority.
        native->control(*this,native->commit,"COMMIT");
        if(!target_history_projection_native_ending_())throw db_error("GROUP projection actual Native observations/endings not joined");
        native->left=native->scope->leave(*this);if(!native->left)throw db_error("GROUP projection original Native read scope leave refused");
        state->require_original();receiving->require_header_original(*connection);native->completed=true;
    }catch(...) {
        const auto native=state->history_projection;if(native&&!native->first_failure)native->first_failure=std::current_exception();
        if(!state->first_failure)state->first_failure=std::current_exception();
        if(native&&native->scope)native->settle_failed(*this);retire_target_staging_();throw;
    }
}
} // namespace lattice
