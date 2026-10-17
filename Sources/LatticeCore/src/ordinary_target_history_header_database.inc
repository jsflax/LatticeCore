// Target-only passive header staging. Included after the real catalog channel.
// No supplied SQL/raw handle, old-owner WRITE or source-cut factory is used.
#include "ordinary_history_receive.hpp"
#include "ordinary_target_raw_history_storage_recipe.hpp"
#include "ordinary_export_fingerprint.hpp"
#include "vendor/picosha2/picosha2.h"
namespace lattice {
struct database::target_staging_state::history_header_native {
    using arguments=std::vector<column_value_t>;
    terminal_native_static_tail terminal_static; // FIRST: actual member death, not source-null inference.
    database* const shell;
    const bool raw_pages; // Selected only by the private unmounted target programme.
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const std::weak_ptr<detail::ordinary_group_target_schema_original> schema;
    std::unique_ptr<detail::ordinary_target_staging_connection::database_scope> scope;
    static constexpr std::size_t statements_limit=32,normal_calls=512,total_calls=640;
    struct statement {
        sqlite3_stmt* held=nullptr;sqlite3_stmt* prepared_identity=nullptr;sqlite3_stmt* finalized_identity=nullptr;
        bool prepare_entered=false,prepare_returned=false,terminal_returned=false,finalize_entered=false,finalize_returned=false;
        bool destruction_known=false;int prepare_rc=SQLITE_MISUSE,first_step_rc=SQLITE_MISUSE,terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
        bool first_step_returned=false;
    };
    enum class kind {prepare,parameters,bind_integer,bind_text,bind_blob,step,finalize,columns,type,integer,bytes,text,blob,changes,autocommit,register_guard,context,result,error_result};
    struct native_call {kind type=kind::prepare;std::size_t statement=0,column=0;bool entered=false,returned=false;std::int64_t value=SQLITE_MISUSE;sqlite3_context* context=nullptr;};
    std::array<statement,statements_limit> statements{};std::array<native_call,total_calls> calls{};
    std::size_t call_count=0,completed_operations=0,guard_invocations=0,logical_bytes=0;
    static constexpr std::size_t begin=0,absence=1,ddl_first=2,stage_insert=18,meta_insert=19,stage_read=20,meta_read=21,commit=22,rollback=23;
    const std::string begin_sql="BEGIN IMMEDIATE",commit_sql="COMMIT",rollback_sql="ROLLBACK";
    const std::string absence_sql="SELECT 1 FROM main.sqlite_schema WHERE name GLOB '_lattice_ordered_history*' OR tbl_name GLOB '_lattice_ordered_history*' LIMIT 1";
    const std::string stage_insert_sql="INSERT INTO main._lattice_ordered_history_stage_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    const std::string meta_insert_sql="INSERT INTO main._lattice_ordered_history_receive_v1 VALUES(?,?,?,?,?,?,?,?,?)";
    const std::string stage_read_sql="SELECT stage,role,source,epoch,cut,scope,schema,device,inode,state,header,rows,known,unknown,pages,bytes,content,seal FROM main._lattice_ordered_history_stage_v1 WHERE stage=?";
    const std::string meta_read_sql="SELECT stage,channel,records_limit,pages_limit,page_limit,records_per_page,bytes_limit,last_table,seal FROM main._lattice_ordered_history_receive_v1 WHERE stage=?";
    std::array<std::string,16> ddl;
    arguments stage,meta,stage_key;
    bool ending=false,left=false,completed=false,guard_registration_entered=false,guard_registration_returned=false;
    bool guard_destroy_entered=false,guard_destroy_returned=false;
    int first_guard_registration_rc=SQLITE_MISUSE;
    bool ending_autocommit_entered=false,ending_autocommit_returned=false;int first_ending_autocommit=0;
    std::exception_ptr first_failure;
    struct guard_userdata {
        const std::shared_ptr<target_staging_state> original;
        history_header_native* const operation;
        database* const shell;sqlite3* const native;
        guard_userdata(std::shared_ptr<target_staging_state> value,history_header_native& input,database& actual)
            :original(std::move(value)),operation(&input),shell(&actual),native(actual.db_){}
        static void destroy(void* raw)noexcept {
            auto* held=static_cast<guard_userdata*>(raw);
            if(!held||!held->original->current_creator()||held->original->history_header.get()!=held->operation||held->operation->userdata!=held)return;
            const auto original=held->original;auto* actual=held->operation;actual->guard_destroy_entered=true;
            delete held;actual->guard_destroy_returned=true;
        }
        static void invoke(sqlite3_context* context,int,sqlite3_value**)noexcept {
            auto* held=static_cast<guard_userdata*>(sqlite3_user_data(context));
            if(!held||!held->original->current_creator()||held->original->history_header.get()!=held->operation||held->operation->userdata!=held)return;
            const auto original=held->original;auto* actual=held->operation;
            try {
                actual->require(*held->shell);
                if(actual->guard_invocations==16)throw db_error("target header guard callback capacity exhausted");
                ++actual->guard_invocations;
                auto& native=actual->reserve(kind::context,stage_insert);actual->require(*held->shell);native.context=context;native.entered=true;
                const auto* db=sqlite3_context_db_handle(context);returned(native,static_cast<std::int64_t>(reinterpret_cast<std::uintptr_t>(db)));
                actual->require(*held->shell);if(db!=held->native||db!=held->shell->db_)throw db_error("target header guard native context differs");
                auto& result=actual->reserve(kind::result,stage_insert);actual->require(*held->shell);result.context=context;result.entered=true;
                sqlite3_result_int(context,1);returned(result,0);actual->require(*held->shell);
            }catch(...) {
                if(!actual->first_failure)actual->first_failure=std::current_exception();if(!original->first_failure)original->first_failure=std::current_exception();
                try {actual->enter_ending(*held->shell);actual->require(*held->shell,true);
                    auto& result=actual->reserve(kind::error_result,stage_insert,true);actual->require(*held->shell,true);
                    result.context=context;result.entered=true;sqlite3_result_error(context,"target same-original header staging guard failed",-1);
                    returned(result,0);actual->require(*held->shell,true);
                }catch(...){} // No failed current join is converted into result/ending permission.
            }
        }
    };
    guard_userdata* userdata=nullptr;
    // The unchanged source sealing grammar is factored below before this
    // constructor is selected; static binding copies are held before effects.
    history_header_native(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
        std::shared_ptr<detail::ordinary_group_target_schema_original> input,arguments stage_row,arguments meta_row,bool raw)
        :terminal_static(actual,this,input.get(),1,receiving),shell(&actual),raw_pages(raw),owner(receiving),schema(std::move(input)),stage(std::move(stage_row)),meta(std::move(meta_row)),stage_key{stage.at(0)} {
        const std::array<const char*,4> names{"_lattice_ordered_history_stage_v1",raw_pages?detail::ordinary_target_raw_history_storage_recipe::page_name:"_lattice_ordered_history_page_v1","_lattice_ordered_history_receive_v1","_lattice_ordered_history_receive_key_v1"};
        const std::array<const char*,4> create{detail::ordinary_history_storage_recipe::stage,raw_pages?detail::ordinary_target_raw_history_storage_recipe::page:detail::ordinary_history_storage_recipe::page,detail::ordinary_history_storage_recipe::meta,detail::ordinary_history_storage_recipe::key};
        std::size_t at=0;for(std::size_t table=0;table<names.size();++table) {
            ddl[at++]=create[table];
            for(const auto* operation:{"INSERT","UPDATE","DELETE"})
                ddl[at++]=std::string("CREATE TRIGGER ")+names[table]+'_'+operation+" BEFORE "+operation+" ON "+names[table]+" BEGIN SELECT lattice_ordered_export_owned_v1(); END";
        }
        std::size_t retained=sizeof(*this);
        const auto charge=[&](std::size_t bytes){if(bytes>ordinary_history_limits::stage_scratch_bytes-retained)
            throw db_error("target header actual fixed input/ledger logical capacity bound");retained+=bytes;};
        for(const auto& sql:ddl)charge(sql.capacity());
        for(const auto* sql:{&begin_sql,&commit_sql,&rollback_sql,&absence_sql,&stage_insert_sql,&meta_insert_sql,&stage_read_sql,&meta_read_sql})charge(sql->capacity());
        for(const auto* args:{&stage,&meta,&stage_key}) {
            if(args->capacity()>ordinary_history_limits::stage_scratch_bytes/sizeof(column_value_t))throw db_error("target header argument capacity bound");
            charge(args->capacity()*sizeof(column_value_t));
            for(const auto& value:*args) {
                if(const auto* text=std::get_if<std::string>(&value))charge(text->capacity());
                else if(const auto* blob=std::get_if<std::vector<std::uint8_t>>(&value))charge(blob->capacity());
            }
        }
        logical_bytes=retained;
        terminal_static.constructor_returned();
    }
    void require(database& actual,bool unwind=false)const {
        const auto state=actual.target_staging_;
        if(!state||!state->current_creator()||shell!=&actual||state->history_header.get()!=this||!scope||left||
           !actual.db_||state->retirement_requested||actual.retirement_->owning_wrapper_moved()||actual.retirement_->raw_handle_escaped())
            throw db_error("target header actual native shell/scope unavailable");
        scope->require_history_role(actual);
        if(unwind){if(!ending)throw db_error("target header admitted ending role missing");scope->require_current(actual);return;}
        if(ending||first_failure)throw db_error("target header fixed generated work sealed");
        const auto held=owner.lock();const auto connection=state->connection.lock();
        if(!held||!connection||held->require_header_schema(*connection)!=schema.lock())
            throw db_error("target header actual retained receiving owner/schema unavailable");
        (void)held->require_header_original(*connection);scope->require_schema_work(actual);
    }
    native_call& reserve(kind type,std::size_t statement,bool unwind=false,std::size_t column=0) {
        if(call_count>=(unwind?total_calls:normal_calls)||statement>=statements_limit)
            throw db_error("target header fixed native ledger exhausted");
        auto& actual=calls[call_count++];actual.type=type;actual.statement=statement;actual.column=column;return actual;
    }
    static void returned(native_call& actual,std::int64_t value)noexcept {actual.value=value;actual.returned=true;}
    void prepare(database& actual,std::size_t index,const std::string& sql,bool unwind=false) {
        auto& statement=statements[index];if(statement.prepare_entered||sql.empty()||sql.find('\0')!=std::string::npos||sql.size()>4096)
            throw db_error("target header fixed statement unavailable");
        require(actual,unwind);auto& call=reserve(kind::prepare,index,unwind);require(actual,unwind);
        const char* tail=nullptr;statement.prepare_entered=true;call.entered=true;
        statement.prepare_rc=sqlite3_prepare_v2(actual.db_,sql.c_str(),static_cast<int>(sql.size()+1),&statement.held,&tail);
        statement.prepared_identity=statement.held;statement.prepare_returned=true;returned(call,statement.prepare_rc);
        if(statement.prepare_rc!=SQLITE_OK||!statement.held)throw db_error("target header preparation failed");
        require(actual,unwind);if(!tail||tail<sql.c_str()||tail>sql.c_str()+sql.size())throw db_error("target header native tail unavailable");
        for(const auto* at=tail;at<sql.c_str()+sql.size();++at)if(*at!=' '&&*at!='\t'&&*at!='\r'&&*at!='\n')throw db_error("target header native extra SQL tail");
    }
    void bind(database& actual,std::size_t index,const arguments& args) {
        auto* statement=statements[index].held;require(actual);auto& count=reserve(kind::parameters,index);require(actual);count.entered=true;
        const int parameters=sqlite3_bind_parameter_count(statement);returned(count,parameters);require(actual);
        if(parameters<0||static_cast<std::size_t>(parameters)!=args.size())throw db_error("target header parameter count differs");
        for(std::size_t at=0;at<args.size();++at) {
            int rc=SQLITE_MISUSE;
            if(const auto* number=std::get_if<std::int64_t>(&args[at])) {
                auto& call=reserve(kind::bind_integer,index,false,at);require(actual);call.entered=true;
                rc=sqlite3_bind_int64(statement,static_cast<int>(at+1),*number);returned(call,rc);require(actual);
            }else if(const auto* text=std::get_if<std::string>(&args[at])) {
                if(text->size()>ordinary_sync_limits::frame_bytes)throw db_error("target header text input bound");
                auto& call=reserve(kind::bind_text,index,false,at);require(actual);call.entered=true;
                rc=sqlite3_bind_text(statement,static_cast<int>(at+1),text->data(),static_cast<int>(text->size()),SQLITE_STATIC);returned(call,rc);require(actual);
            }else if(const auto* bytes=std::get_if<std::vector<std::uint8_t>>(&args[at])) {
                if(bytes->empty()||bytes->size()>ordinary_sync_limits::frame_bytes)throw db_error("target header BLOB input bound");
                auto& call=reserve(kind::bind_blob,index,false,at);require(actual);call.entered=true;
                rc=sqlite3_bind_blob(statement,static_cast<int>(at+1),bytes->data(),static_cast<int>(bytes->size()),SQLITE_STATIC);returned(call,rc);require(actual);
            }else throw db_error("target header unsupported generated argument");
            if(rc!=SQLITE_OK)throw db_error("target header fixed argument binding failed");
        }
    }
    int step(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];if(!statement.held||statement.finalize_entered)throw db_error("target header actual VM unavailable");
        require(actual,unwind);auto& call=reserve(kind::step,index,unwind);require(actual,unwind);call.entered=true;
        const int rc=sqlite3_step(statement.held);returned(call,rc);
        if(!statement.first_step_returned){statement.first_step_returned=true;statement.first_step_rc=rc;}
        if(rc!=SQLITE_ROW){if(statement.terminal_returned)throw db_error("target header native terminal reissued");statement.terminal_returned=true;statement.terminal_rc=rc;}
        if(index==begin)scope->record_schema_begin(actual,rc);if(index==commit)scope->record_schema_commit(actual,rc);if(index==rollback)scope->record_schema_rollback(actual,rc);
        require(actual,unwind);return rc;
    }
    void finish(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];if(!statement.held||statement.finalize_entered)return;
        require(actual,unwind);auto& call=reserve(kind::finalize,index,unwind);require(actual,unwind);
        statement.finalized_identity=statement.held;statement.finalize_entered=true;call.entered=true;
        statement.finalize_rc=sqlite3_finalize(statement.held);statement.finalize_returned=true;returned(call,statement.finalize_rc);
        statement.destruction_known=(statement.finalize_rc&0xff)!=SQLITE_MISUSE;if(statement.destruction_known)statement.held=nullptr;
        scope->record_schema_finalized(actual,statement.finalize_rc);
        if(statement.finalize_rc!=SQLITE_OK)throw db_error("target header native finalization failed");require(actual,unwind);
    }
    void execute(database& actual,std::size_t index,const std::string& sql,const arguments& args={},bool unwind=false) {
        prepare(actual,index,sql,unwind);if(!unwind)bind(actual,index,args);
        if(step(actual,index,unwind)!=SQLITE_DONE)throw db_error("target header fixed operation did not finish");finish(actual,index,unwind);
    }
    void changes(database& actual,std::size_t index) {
        require(actual);auto& call=reserve(kind::changes,index);require(actual);call.entered=true;
        const int changed=sqlite3_changes(actual.db_);returned(call,changed);require(actual);
        if(changed!=1)throw db_error("target header generated INSERT not applied exactly once");
    }
    void verify(database& actual,std::size_t index,const std::string& sql,const arguments& expected) {
        prepare(actual,index,sql);bind(actual,index,stage_key);if(step(actual,index)!=SQLITE_ROW)throw db_error("target header actual postimage missing");
        auto* stmt=statements[index].held;auto& count=reserve(kind::columns,index);require(actual);count.entered=true;
        const int columns=sqlite3_column_count(stmt);returned(count,columns);require(actual);
        if(columns<0||static_cast<std::size_t>(columns)!=expected.size())throw db_error("target header postimage column shape differs");
        for(std::size_t at=0;at<expected.size();++at) {
            auto& type=reserve(kind::type,index,false,at);require(actual);type.entered=true;const int storage=sqlite3_column_type(stmt,static_cast<int>(at));returned(type,storage);require(actual);
            if(const auto* number=std::get_if<std::int64_t>(&expected[at])) {
                if(storage!=SQLITE_INTEGER)throw db_error("target header actual INTEGER storage differs");
                auto& call=reserve(kind::integer,index,false,at);require(actual);call.entered=true;const auto value=sqlite3_column_int64(stmt,static_cast<int>(at));returned(call,value);require(actual);
                if(value!=*number)throw db_error("target header actual INTEGER value differs");
            }else {
                const auto* text=std::get_if<std::string>(&expected[at]);const auto* blob=std::get_if<std::vector<std::uint8_t>>(&expected[at]);
                if((text&&storage!=SQLITE_TEXT)||(blob&&storage!=SQLITE_BLOB)||(!text&&!blob))throw db_error("target header actual byte storage differs");
                auto& size=reserve(kind::bytes,index,false,at);require(actual);size.entered=true;const int bytes=sqlite3_column_bytes(stmt,static_cast<int>(at));returned(size,bytes);require(actual);
                const auto wanted=text?text->size():blob->size();if(bytes<0||static_cast<std::size_t>(bytes)!=wanted)throw db_error("target header actual byte length differs");
                auto& pointer=reserve(text?kind::text:kind::blob,index,false,at);require(actual);pointer.entered=true;
                const void* value=text?static_cast<const void*>(sqlite3_column_text(stmt,static_cast<int>(at))):sqlite3_column_blob(stmt,static_cast<int>(at));
                returned(pointer,static_cast<std::int64_t>(reinterpret_cast<std::uintptr_t>(value)));require(actual);
                const void* expected_bytes=text?static_cast<const void*>(text->data()):static_cast<const void*>(blob->data());
                if(bytes&&(!value||std::memcmp(value,expected_bytes,static_cast<std::size_t>(bytes))))throw db_error("target header actual retained byte value differs");
            }
        }
        if(step(actual,index)!=SQLITE_DONE)throw db_error("target header postimage not unique");finish(actual,index);
    }
    void enter_ending(database& actual) {if(!scope||left)throw db_error("target header admitted ending unavailable");scope->begin_schema_ending(actual);ending=true;}
    void settle_failed(database& actual)noexcept {
        try {enter_ending(actual);
            for(std::size_t i=0;i<statements.size();++i)if(statements[i].held&&!statements[i].finalize_entered)try{finish(actual,i,true);}catch(...){}
            if(statements[begin].terminal_returned&&statements[begin].terminal_rc==SQLITE_DONE&&
               !(statements[commit].terminal_returned&&statements[commit].terminal_rc==SQLITE_DONE)) {
                auto& call=reserve(kind::autocommit,rollback,true);require(actual,true);ending_autocommit_entered=true;call.entered=true;
                first_ending_autocommit=sqlite3_get_autocommit(actual.db_);ending_autocommit_returned=true;returned(call,first_ending_autocommit);require(actual,true);
                if(!first_ending_autocommit)execute(actual,rollback,rollback_sql,{},true);
            }
            if(userdata&&!guard_registration_entered&&!guard_destroy_entered)guard_userdata::destroy(userdata);
            left=scope->leave(actual);
        }catch(...){} // First native/VM/fd/ending debt is held; no reissue or COMMIT inference.
    }
};
void database::require_target_catalog_ending_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_schema_original& original)const {
    const auto state=target_staging_;const auto actual=state?state->catalog:nullptr;
    require_target_metadata_ending_(connection,original);
    if(!actual||actual->shell!=this||!actual->scope||!actual->left||!actual->completed||actual->ending||actual->first_failure||
       !actual->module_entered||!actual->module_returned||actual->first_module_rc!=SQLITE_OK||
       !actual->cookie_observed||actual->first_cookie<0||actual->cookie_text.empty()||!actual->udf_queries||
       actual->completed_operations!=actual->generated->operations.size()||
       actual->generated!=original.require_catalog(connection,state->end))
        throw db_error("target history requires actual SAME completed catalog native original");
    actual->scope->require_catalog_ending(*this,db_);
    const auto& begin=actual->statements[0];const auto& commit=actual->statements[actual->commit_index];
    if(!begin.terminal_returned||begin.terminal_rc!=SQLITE_DONE||!commit.terminal_returned||commit.terminal_rc!=SQLITE_DONE)
        throw db_error("target history catalog actual transaction ending missing");
    for(std::size_t at=0;at<actual->call_count;++at)if(actual->calls[at].entered&&!actual->calls[at].returned)
        throw db_error("target history catalog retains actual native return debt");
    for(const auto& statement:actual->statements)if(statement.prepare_entered&&
       (!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||statement.held||!statement.terminal_returned||
        statement.terminal_rc!=SQLITE_DONE||!statement.finalize_entered||!statement.finalize_returned||!statement.finalize_destruction_known||
        !statement.prepared_identity||statement.finalized_identity!=statement.prepared_identity||statement.finalize_rc!=SQLITE_OK))
        throw db_error("target history catalog retains actual VM ending debt");
}
bool database::target_history_header_native_ending_()const noexcept {
    try {
        const auto state=target_staging_;const auto actual=state?state->history_header:nullptr;
        if(!state||!state->current_creator()||state->terminal_native_statics_attempted||!actual||actual->shell!=this||!actual->scope||actual->ending||actual->first_failure||
           !actual->guard_registration_entered||!actual->guard_registration_returned||actual->first_guard_registration_rc!=SQLITE_OK||
           !actual->userdata||actual->guard_destroy_entered||actual->guard_destroy_returned||actual->completed_operations!=22||actual->guard_invocations!=2)
            return false;
        actual->scope->require_history_role(*this);
        const auto& begin=actual->statements[actual->begin];const auto& commit=actual->statements[actual->commit];
        if(!begin.terminal_returned||begin.terminal_rc!=SQLITE_DONE||!commit.terminal_returned||commit.terminal_rc!=SQLITE_DONE)return false;
        for(std::size_t at=0;at<actual->call_count;++at)if(actual->calls[at].entered&&!actual->calls[at].returned)return false;
        for(const auto& statement:actual->statements)if(statement.prepare_entered&&
           (!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||statement.held||!statement.terminal_returned||
            statement.terminal_rc!=SQLITE_DONE||!statement.finalize_entered||!statement.finalize_returned||!statement.destruction_known||
            !statement.prepared_identity||statement.finalized_identity!=statement.prepared_identity||statement.finalize_rc!=SQLITE_OK))return false;
        return true;
    }catch(...){return false;}
}
void database::require_target_history_header_ending_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    const auto state=target_staging_;const auto actual=state?state->history_header:nullptr;
    const auto& header=receiving.require_header_original(connection);
    const auto& schema=receiving.require_header_schema(connection);
    require_target_catalog_ending_(connection,*schema);
    if(!state||state->connection.lock().get()!=&connection||!actual||actual->owner.lock().get()!=&receiving||
       actual->schema.lock()!=schema||!actual->left||!actual->completed||actual->ending||actual->first_failure||
       actual->stage.at(0)!=column_value_t(header.stage))
        throw db_error("target header lacks actual same-receiver durable ending");
    const auto* raw=std::get_if<std::vector<std::uint8_t>>(&actual->stage.at(10));
    if(!raw||raw->size()!=header.header.size()||std::memcmp(raw->data(),header.header.data(),raw->size()))
        throw db_error("target header actual retained native input differs");
    // Actual leave/native proof is retained on the original, not a caller DTO.
    if(!actual->scope->leave(*this))throw db_error("target header original leave did not join");
}
bool database::target_history_raw_storage_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    require_target_history_header_ending_(connection,receiving);
    const auto state=target_staging_;const auto actual=state?state->history_header:nullptr;
    if(!state||!state->current_creator()||!actual||state->first_failure||state->retirement_requested||
       state->connection.lock().get()!=&connection||actual->owner.lock().get()!=&receiving||
       actual->schema.lock()!=receiving.require_header_schema(connection))
        throw db_error("target history actual retained storage recipe unavailable");
    return actual->raw_pages; // Observed DATA from SAME actual header programme; never an operation permit.
}
bool database::target_history_guard_destroyed_()const noexcept {
    const auto state=target_staging_;const auto actual=state?state->history_header:nullptr;
    if(!state||!state->current_creator()||state->terminal_native_statics_attempted)return false;
    if(!target_history_page_guard_destroyed_())return false;
    if(!actual)return true; // Native work is only after this exact field is retained.
    if(!actual->userdata)return !actual->guard_registration_entered;
    return actual->guard_destroy_entered&&actual->guard_destroy_returned;
}
void database::stage_target_history_header_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving) {
    stage_target_history_header_program_(receiving,false);
}
void database::stage_target_raw_history_header_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving) {
    stage_target_history_header_program_(receiving,true); // Unmounted until both raw readers compose.
}
void database::stage_target_history_header_program_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,bool raw) {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||!receiving||state->history_header_attempted||state->history_header||state->retirement_requested)
        throw db_error("target passive history header original already consumed/unavailable");
    const auto connection=state->require_original();const auto& header=receiving->require_header_original(*connection);
    const auto schema=receiving->require_header_schema(*connection);require_target_catalog_ending_(*connection,*schema);
    state->history_header_attempted=true; // No failed construction/native phase can be replaced or reset.
    try {
        using native=target_staging_state::history_header_native;
        const ordinary_history_limits limits;const auto identity=connection->history_main_identity(state->end);
        if(header.header.empty()||header.header.size()>ordinary_sync_limits::frame_bytes||header.header.size()>limits.durable_stage_bytes||
           header.rows>limits.records||header.known>limits.records-header.rows||header.unknown>limits.records-header.rows-header.known)
            throw db_error("target header fixed passive input bound");
        const std::string empty_content;
        if(!header.pages&&picosha2::hash256_hex_string(empty_content.begin(),empty_content.end())!=header.content_digest)
            throw db_error("target empty history actual content digest differs");
        native::arguments stage{header.stage,std::int64_t{2},header.projection.source,header.projection.epoch,header.projection.cut,
            header.projection.scope,header.projection.schema,std::to_string(identity.first),std::to_string(identity.second),
            std::int64_t(header.pages==0),std::vector<std::uint8_t>(header.header.begin(),header.header.end()),
            std::int64_t{0},std::int64_t{0},std::int64_t{0},std::int64_t{0},std::int64_t(header.header.size()),header.content_digest};
        stage.emplace_back(detail::ordinary_history_storage_seal("ordinary-paged-stage-v1",stage,17));
        native::arguments meta{header.stage,receiving->require_header_channel(*connection),std::int64_t(limits.records),std::int64_t(limits.pages),
            std::int64_t(limits.page_bytes),std::int64_t(limits.records_per_page),std::int64_t(limits.durable_stage_bytes),std::int64_t{-1}};
        meta.emplace_back(detail::ordinary_history_storage_seal("ordinary-history-receive-meta-v1",meta,8));
        state->history_header=std::make_shared<native>(*this,receiving,schema,std::move(stage),std::move(meta),raw);
        const auto actual=state->history_header; // Actual fixed inputs/ledgers retained BEFORE scope/native effects.
        connection->begin_database_history_header(*this,receiving,actual->scope,state->end);actual->require(*this);
        actual->userdata=new native::guard_userdata(state,*actual,*this); // Exact object before Native registration diagnostics.
        actual->require(*this);auto& registration=actual->reserve(native::kind::register_guard,native::stage_insert);actual->require(*this);
        actual->guard_registration_entered=true;registration.entered=true;
        actual->first_guard_registration_rc=sqlite3_create_function_v2(db_,"lattice_ordered_export_owned_v1",0,SQLITE_UTF8,
            actual->userdata,&native::guard_userdata::invoke,nullptr,nullptr,&native::guard_userdata::destroy);
        actual->guard_registration_returned=true;native::returned(registration,actual->first_guard_registration_rc);
        if(actual->first_guard_registration_rc!=SQLITE_OK)throw db_error("target passive header guard registration failed");actual->require(*this);
        actual->execute(*this,native::begin,actual->begin_sql);++actual->completed_operations;
        actual->prepare(*this,native::absence,actual->absence_sql);actual->bind(*this,native::absence,{});
        if(actual->step(*this,native::absence)!=SQLITE_DONE)throw db_error("target existing history object cannot be adopted");actual->finish(*this,native::absence);++actual->completed_operations;
        for(std::size_t at=0;at<actual->ddl.size();++at){actual->execute(*this,native::ddl_first+at,actual->ddl[at]);++actual->completed_operations;}
        actual->execute(*this,native::stage_insert,actual->stage_insert_sql,actual->stage);actual->changes(*this,native::stage_insert);++actual->completed_operations;
        actual->execute(*this,native::meta_insert,actual->meta_insert_sql,actual->meta);actual->changes(*this,native::meta_insert);++actual->completed_operations;
        actual->verify(*this,native::stage_read,actual->stage_read_sql,actual->stage);++actual->completed_operations;
        actual->verify(*this,native::meta_read,actual->meta_read_sql,actual->meta);++actual->completed_operations;
        actual->execute(*this,native::commit,actual->commit_sql);
        if(!target_history_header_native_ending_())throw db_error("target header fixed native ending not complete");
        actual->left=actual->scope->leave(*this);if(!actual->left)throw db_error("target header actual scope did not join");
        state->require_original();receiving->require_header_original(*connection);actual->completed=true;
        // Passive raw/header indexes only. No peer table/model row, old Audit,
        // original receipt/debt, source binding/checkpoint/cut or publisher is changed.
    }catch(...) {
        if(state->history_header&&!state->history_header->first_failure)state->history_header->first_failure=std::current_exception();
        if(!state->first_failure)state->first_failure=std::current_exception();
        if(state->history_header)state->history_header->settle_failed(*this);
        retire_target_staging_();throw;
    }
}
} // namespace lattice
