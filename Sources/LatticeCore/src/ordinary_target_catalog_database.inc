// Included once after the target database state/constructor/metadata bodies.
// No ordinary/public database accessor or caller-selected SQL channel exists.

#include <sqlite-vec.h>
namespace lattice {
struct database::target_staging_state::catalog_native {
    using program=detail::ordinary_group_target_schema_original::catalog_program;
    terminal_native_static_tail terminal_static; // FIRST: real member tail follows payload/scope destruction.
    database* const shell;
    const std::weak_ptr<detail::ordinary_group_target_schema_original> original;
    const std::shared_ptr<program> generated;
    std::unique_ptr<detail::ordinary_target_staging_connection::database_scope> scope;
    static constexpr std::size_t maximum_statements=4164,normal_calls=65536,total_calls=73728,maximum_udf_queries=64;
    struct statement {
        sqlite3_stmt* held=nullptr;
        sqlite3_stmt* prepared_identity=nullptr;sqlite3_stmt* finalized_identity=nullptr;
        bool finalize_destruction_known=false;
        bool prepare_entered=false,prepare_returned=false,step_entered=false,first_step_returned=false;
        bool terminal_returned=false,finalize_entered=false,finalize_returned=false;
        int prepare_rc=SQLITE_MISUSE,first_step_rc=SQLITE_MISUSE,terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
    };
    enum class kind {prepare,parameter_count,bind_integer,bind_text,step,finalize,column_count,column_type,column_integer,module,autocommit,result,error_result};
    struct call {kind type=kind::prepare;std::size_t statement=0;bool entered=false,returned=false;std::int64_t value=SQLITE_MISUSE;sqlite3_context* context_identity=nullptr;};
    std::array<statement,maximum_statements> statements{};
    std::array<call,total_calls> calls{};
    std::size_t call_count=0,udf_queries=0,completed_operations=0;
    const std::size_t commit_index,rollback_index,probe_index,udf_first_index;
    // Fallible fixed-text ownership is prepared before any module/SQL effect;
    // failed ending never allocates a replacement SQL input original.
    const std::string begin_sql="BEGIN IMMEDIATE",commit_sql="COMMIT",rollback_sql="ROLLBACK";
    const std::string probe_sql="SELECT sync_disabled()",control_sql="SELECT disabled FROM _SyncControl WHERE id=1";
    bool left=false,completed=false,ending=false,module_entered=false,module_returned=false;
    int first_module_rc=SQLITE_MISUSE;
    bool cookie_observed=false;std::int64_t first_cookie=-1;std::string cookie_text;
    bool ending_autocommit_entered=false,ending_autocommit_returned=false;int first_ending_autocommit=0;
    std::exception_ptr first_failure;
    catalog_native(database& actual,const std::shared_ptr<detail::ordinary_group_target_schema_original>& source,
        std::shared_ptr<program> input)
        :terminal_static(actual,this,source.get(),0),shell(&actual),original(source),generated(std::move(input)),commit_index(generated->operations.size()+1),
         rollback_index(commit_index+1),probe_index(commit_index+2),udf_first_index(commit_index+3) {
        if(generated->operations.empty()||generated->operations.size()>program::maximum_operations||
           udf_first_index+maximum_udf_queries>maximum_statements)
            throw db_error("target catalog fixed native capacity unavailable");
        terminal_static.constructor_returned();
    }
    void require(database& actual,bool unwind=false)const {
        const auto state=actual.target_staging_;
        if(shell!=&actual||!state||!state->current_creator()||state->catalog.get()!=this||!scope||left||
           !actual.db_||state->retirement_requested||actual.retirement_->owning_wrapper_moved()||actual.retirement_->raw_handle_escaped())
            throw db_error("target catalog actual shell/native custody unavailable");
        scope->require_catalog_role(actual);
        if(unwind) {if(!ending)throw db_error("target catalog ending role unavailable");scope->require_current(actual);return;}
        if(ending||first_failure)throw db_error("target catalog generated work permanently sealed");
        const auto held=original.lock();const auto connection=state->connection.lock();
        if(!held||!connection||held->require_catalog(*connection,state->end)!=generated)
            throw db_error("target catalog SAME generated original unavailable");
        scope->require_schema_work(actual);
    }
    call& reserve(kind type,std::size_t index,bool unwind=false) {
        if(call_count>=(unwind?total_calls:normal_calls)||index>=maximum_statements)
            throw db_error("target fixed catalog native ledger exhausted");
        auto& result=calls[call_count++];result.type=type;result.statement=index;return result;
    }
    static void returned(call& actual,std::int64_t value)noexcept {actual.value=value;actual.returned=true;}
    void finish(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];if(!statement.held||statement.finalize_entered)return;
        require(actual,unwind);auto& record=reserve(kind::finalize,index,unwind);require(actual,unwind);
        statement.finalized_identity=statement.held;statement.finalize_entered=true;record.entered=true;
        statement.finalize_rc=::lattice::detail::store_physical_finalize(statement.held);statement.finalize_returned=true;
        returned(record,statement.finalize_rc);
        // SQLITE_MISUSE can return before sqlite3VdbeDelete. Preserve the SAME
        // outstanding VM and first identities; never retry an unknown ending.
        statement.finalize_destruction_known=(statement.finalize_rc&0xff)!=SQLITE_MISUSE;
        if(statement.finalize_destruction_known)statement.held=nullptr;
        scope->record_schema_finalized(actual,statement.finalize_rc);
        if(statement.finalize_rc!=SQLITE_OK)throw db_error("target catalog native finalization failed");
        require(actual,unwind);
    }
    void prepare(database& actual,std::size_t index,const std::string& sql,bool unwind=false) {
        auto& statement=statements[index];if(statement.prepare_entered||sql.empty()||sql.size()>program::maximum_bytes||sql.find('\0')!=std::string::npos)
            throw db_error("target catalog generated native statement unavailable");
        require(actual,unwind);auto& record=reserve(kind::prepare,index,unwind);require(actual,unwind);
        const char* tail=nullptr;statement.prepare_entered=true;record.entered=true;
        statement.prepare_rc=::lattice::detail::store_physical_prepare_v2(actual.db_,sql.c_str(),static_cast<int>(sql.size()+1),&statement.held,&tail);
        statement.prepared_identity=statement.held;statement.prepare_returned=true;returned(record,statement.prepare_rc);
        if(statement.prepare_rc!=SQLITE_OK||!statement.held)throw db_error("target catalog native preparation failed");
        require(actual,unwind);
        if(!tail||tail<sql.c_str()||tail>sql.c_str()+sql.size())throw db_error("target generated tail unavailable");
        for(const char* value=tail;value<sql.c_str()+sql.size();++value)
            if(*value!=' '&&*value!='\t'&&*value!='\r'&&*value!='\n')throw db_error("target generated statement has extra native tail");
    }
    int step(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];if(!statement.held||statement.finalize_entered)throw db_error("target catalog native statement not active");
        require(actual,unwind);auto& record=reserve(kind::step,index,unwind);require(actual,unwind);
        statement.step_entered=true;record.entered=true;const int rc=::lattice::detail::store_physical_step(statement.held);returned(record,rc);
        if(!statement.first_step_returned){statement.first_step_rc=rc;statement.first_step_returned=true;}
        if(rc!=SQLITE_ROW){if(statement.terminal_returned)throw db_error("target catalog native terminal already observed");statement.terminal_rc=rc;statement.terminal_returned=true;}
        if(index==0)scope->record_schema_begin(actual,rc);
        if(index==commit_index)scope->record_schema_commit(actual,rc);
        if(index==rollback_index)scope->record_schema_rollback(actual,rc);
        require(actual,unwind);return rc;
    }
    std::int64_t integer(database& actual,std::size_t index) {
        auto& statement=statements[index];
        if(!statement.held||!statement.first_step_returned||statement.first_step_rc!=SQLITE_ROW||statement.terminal_returned)
            throw db_error("target catalog scalar lacks actual native row");
        require(actual);auto& count=reserve(kind::column_count,index);require(actual);count.entered=true;
        const int columns=sqlite3_column_count(statement.held);returned(count,columns);require(actual);
        if(columns!=1)throw db_error("target catalog scalar column count malformed");
        auto& type=reserve(kind::column_type,index);require(actual);type.entered=true;
        const int storage=sqlite3_column_type(statement.held,0);returned(type,storage);require(actual);
        if(storage!=SQLITE_INTEGER)throw db_error("target catalog scalar storage malformed");
        auto& value=reserve(kind::column_integer,index);require(actual);value.entered=true;
        const auto result=sqlite3_column_int64(statement.held,0);returned(value,result);require(actual);return result;
    }
    void bind(database& actual,std::size_t index,const program::operation& operation) {
        auto& statement=statements[index];require(actual);auto& count=reserve(kind::parameter_count,index);require(actual);count.entered=true;
        const int parameters=sqlite3_bind_parameter_count(statement.held);returned(count,parameters);require(actual);
        if(parameters<0||static_cast<std::size_t>(parameters)!=operation.bindings.size())throw db_error("target generated parameter count differs");
        for(std::size_t i=0;i<operation.bindings.size();++i) {
            const auto& input=operation.bindings[i];
            if(const auto* number=std::get_if<std::int64_t>(&input)) {
                auto& record=reserve(kind::bind_integer,index);require(actual);record.entered=true;
                const int rc=sqlite3_bind_int64(statement.held,static_cast<int>(i+1),*number);returned(record,rc);require(actual);
                if(rc!=SQLITE_OK)throw db_error("target generated integer binding failed");
            }else {
                const auto& text=operation.kind==program::purpose::fingerprint_cookie&&i==1?cookie_text:std::get<std::string>(input);
                if(operation.kind==program::purpose::fingerprint_cookie&&(!cookie_observed||first_cookie<0||cookie_text.empty()))
                    throw db_error("target fingerprint lacks actual SAME schema cookie");
                if(text.size()>program::maximum_bytes)throw db_error("target generated bound text too large");
                auto& record=reserve(kind::bind_text,index);require(actual);record.entered=true;
                const int rc=sqlite3_bind_text(statement.held,static_cast<int>(i+1),text.data(),static_cast<int>(text.size()),SQLITE_STATIC);
                returned(record,rc);require(actual);if(rc!=SQLITE_OK)throw db_error("target generated text binding failed");
            }
        }
    }
    void fixed(database& actual,std::size_t index,const std::string& sql,bool unwind=false) {
        // Fixed native BEGIN/COMMIT/ROLLBACK only, never supplied by a caller.
        prepare(actual,index,sql,unwind);
        if(step(actual,index,unwind)!=SQLITE_DONE)throw db_error("target fixed catalog transaction ending failed");finish(actual,index,unwind);
    }
    void enter_ending(database& actual) {
        if(!scope||left)throw db_error("target catalog actual admitted ending missing");
        scope->begin_schema_ending(actual);ending=true; // No generation/module/write rearm.
    }
    void settle_failed(database& actual)noexcept {
        try {
            enter_ending(actual);
            for(std::size_t i=0;i<statements.size();++i)if(statements[i].held&&!statements[i].finalize_entered)
                try{finish(actual,i,true);}catch(...){} // Preserve each first failure/result, continue only actual endings.
            const auto& begin=statements[0];const auto& commit=statements[commit_index];
            if(begin.terminal_returned&&begin.terminal_rc==SQLITE_DONE&&!(commit.terminal_returned&&commit.terminal_rc==SQLITE_DONE)) {
                auto& record=reserve(kind::autocommit,rollback_index,true);require(actual,true);
                ending_autocommit_entered=true;record.entered=true;first_ending_autocommit=sqlite3_get_autocommit(actual.db_);
                ending_autocommit_returned=true;returned(record,first_ending_autocommit);require(actual,true);
                if(!first_ending_autocommit)fixed(actual,rollback_index,rollback_sql,true);
            }
            left=scope->leave(actual);
        }catch(...){} // SAME failed native/scope/callback/transaction debt stays held.
    }
};
void database::require_target_metadata_ending_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_schema_original& original)const {
    const auto state=target_staging_;
    require_target_fixed_constructor_(connection);
    if(!state||state->schema_original.lock().get()!=&original||!state->schema_attempted||!state->schema_left||
       !state->metadata_bootstrap_done||!state->schema_scope||state->first_failure||
       !state->udf_registration_attempted||!state->udf_registration_returned||state->first_udf_registration_result!=SQLITE_OK||
       !state->userdata_original||state->userdata_original->shell!=this||state->userdata_original->native!=db_||
       state->udf_destroy_entered||state->udf_destroy_returned)
        throw db_error("target catalog lacks actual metadata/UDF original ending");
    state->schema_scope->require_metadata_ending(*this,db_);
    for(std::size_t i=0;i<state->schema_call_count;++i)
        if(state->schema_calls[i].entered&&!state->schema_calls[i].returned)throw db_error("target metadata retains native return debt");
    for(const auto& statement:state->schema_statements)if(statement.prepare_attempted&&
        (!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||statement.held||
         !statement.finalize_attempted||!statement.finalize_returned||statement.finalize_rc!=SQLITE_OK))
        throw db_error("target metadata retains actual statement ending debt");
    original.require_connection(connection,state->end);
}
void database::target_staging_state::invoke_catalog_control(sync_control_userdata* issued,sqlite3_context* context)noexcept {
    const auto state=issued->original;const auto actual=state->catalog;
    if(!actual||!state->current_creator()||state->userdata_original!=issued||actual->shell!=issued->shell||
       issued->native!=issued->shell->db_)return; // No foreign SQLite/result ending.
    std::size_t index=0;bool admitted=false;
    try {
        actual->require(*issued->shell);
        if(actual->udf_queries==catalog_native::maximum_udf_queries)throw db_error("target catalog actual UDF ledger exhausted");
        index=actual->udf_first_index+actual->udf_queries++;admitted=true;
        actual->prepare(*issued->shell,index,actual->control_sql);
        if(actual->step(*issued->shell,index)!=SQLITE_ROW)throw db_error("target actual UDF control row missing");
        const auto value=actual->integer(*issued->shell,index);
        if(value!=0&&value!=1)throw db_error("target actual UDF control integer malformed");
        if(actual->step(*issued->shell,index)!=SQLITE_DONE)throw db_error("target actual UDF control row not unique");
        actual->finish(*issued->shell,index);
        actual->require(*issued->shell);auto& result=actual->reserve(catalog_native::kind::result,index);actual->require(*issued->shell);
        result.context_identity=context;result.entered=true;sqlite3_result_int(context,static_cast<int>(value));catalog_native::returned(result,0);
        actual->require(*issued->shell);
    }catch(...) {
        if(!actual->first_failure)actual->first_failure=std::current_exception();if(!state->first_failure)state->first_failure=std::current_exception();
        if(state->current_creator()) {
            try {
                // The actual pre-issued userdata/context is on this native
                // invocation. Failure is sealed work, not permission: only its
                // genuinely retained creator-bound ending scope can deliver.
                actual->enter_ending(*issued->shell);
                if(admitted)try{actual->finish(*issued->shell,index,true);}catch(...){}
                actual->require(*issued->shell,true);
                auto& result=actual->reserve(catalog_native::kind::error_result,index,true);
                actual->require(*issued->shell,true);result.context_identity=context;result.entered=true;
                sqlite3_result_error(context,"target same-original catalog control read failed",-1);
                catalog_native::returned(result,0); // Native void return before any later ending join.
                actual->require(*issued->shell,true);
            }catch(...){} // No creator/scope/result return becomes invented ending credit.
        }
    }
}
bool database::target_catalog_native_ending_()const noexcept {
    try {
        const auto state=target_staging_;const auto actual=state?state->catalog:nullptr;
        if(!state||!state->current_creator()||state->terminal_native_statics_attempted||!actual||actual->shell!=this||!actual->scope||actual->ending||
           actual->first_failure||!actual->module_entered||!actual->module_returned||actual->first_module_rc!=SQLITE_OK||
           !actual->cookie_observed||actual->first_cookie<0||actual->cookie_text.empty()||
           actual->completed_operations!=actual->generated->operations.size()||!actual->udf_queries)
            return false;
        actual->scope->require_catalog_role(*this);
        const auto& begin=actual->statements[0];const auto& commit=actual->statements[actual->commit_index];
        if(!begin.terminal_returned||begin.terminal_rc!=SQLITE_DONE||!commit.terminal_returned||commit.terminal_rc!=SQLITE_DONE)return false;
        for(std::size_t i=0;i<actual->call_count;++i)if(actual->calls[i].entered&&!actual->calls[i].returned)return false;
        for(const auto& statement:actual->statements)if(statement.prepare_entered&&
           (!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||statement.held||!statement.terminal_returned||
            statement.terminal_rc!=SQLITE_DONE||!statement.finalize_entered||!statement.finalize_returned||!statement.finalize_destruction_known||
            !statement.prepared_identity||statement.finalized_identity!=statement.prepared_identity||statement.finalize_rc!=SQLITE_OK))return false;
        return true; // Actual same original/native operation ending only, never caller data/current/publication.
    }catch(...){return false;}
}
void database::install_target_catalog_(const std::shared_ptr<detail::ordinary_group_target_schema_original>& original) {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||!original||state->catalog_attempted||state->catalog||state->retirement_requested)
        throw db_error("target complete catalog one-shot original unavailable");
    const auto connection=state->require_original();require_target_metadata_ending_(*connection,*original);
    const auto generated=original->require_catalog(*connection,state->end);
    state->catalog_attempted=true; // No reissue/reset after failed allocation or native work.
    try {
        state->catalog=std::make_shared<target_staging_state::catalog_native>(*this,original,generated);
        const auto actual=state->catalog; // Held before scope/output/current or native diagnostics.
        connection->begin_database_catalog(*this,original,actual->scope,state->end);actual->require(*this);
        auto& module=actual->reserve(target_staging_state::catalog_native::kind::module,0);actual->require(*this);
        actual->module_entered=true;module.entered=true;actual->first_module_rc=sqlite3_vec_init(db_,nullptr,nullptr);
        actual->module_returned=true;target_staging_state::catalog_native::returned(module,actual->first_module_rc);
        if(actual->first_module_rc!=SQLITE_OK)throw db_error("target actual vec provider registration failed");actual->require(*this);
        actual->fixed(*this,0,actual->begin_sql);
        // Same pre-issued userdata, not replacement UDF registration/diagnostic permission.
        {const auto index=actual->probe_index;actual->prepare(*this,index,actual->probe_sql);
         if(actual->step(*this,index)!=SQLITE_ROW||actual->integer(*this,index)!=0)throw db_error("target catalog actual UDF probe failed");
         if(actual->step(*this,index)!=SQLITE_DONE)throw db_error("target catalog UDF result not unique");actual->finish(*this,index);}
        using purpose=target_staging_state::catalog_native::program::purpose;
        for(std::size_t i=0;i<generated->operations.size();++i) {
            const auto index=i+1;const auto& operation=generated->operations[i];actual->prepare(*this,index,operation.sql);actual->bind(*this,index,operation);
            const int rc=actual->step(*this,index);
            if(operation.kind==purpose::schema_cookie) {
                if(rc!=SQLITE_ROW||actual->cookie_observed)throw db_error("target catalog actual schema cookie missing/reissued");
                actual->first_cookie=actual->integer(*this,index);actual->cookie_observed=true;
                if(actual->first_cookie<0)throw db_error("target catalog actual schema cookie malformed");
                actual->cookie_text=std::to_string(actual->first_cookie); // First scalar already retained before allocation.
                if(actual->step(*this,index)!=SQLITE_DONE)throw db_error("target catalog schema cookie not unique");
            }else if(rc!=SQLITE_DONE) {
                if(operation.kind==purpose::require_absent_object)throw db_error("target generated schema object already exists; no adoption");
                if(operation.kind==purpose::require_empty_vector_model)throw db_error("target vector dimensions require actual later imported data; no guessed sidecar");
                throw db_error("target generated operation did not complete");
            }
            actual->finish(*this,index);++actual->completed_operations;
        }
        actual->fixed(*this,actual->commit_index,actual->commit_sql);actual->require(*this);
        if(!target_catalog_native_ending_())throw db_error("target complete catalog retains native result/ending debt");
        actual->left=actual->scope->leave(*this);if(!actual->left)throw db_error("target complete catalog scope did not join");
        state->require_original();original->require_connection(*connection,state->end);actual->completed=true;
        // Schema only. No source-cut/checkpoint rows imported, no active old Audit,
        // owner publication/current writer/active WAL route or recovery completion.
    }catch(...) {
        if(state->catalog&&!state->catalog->first_failure)state->catalog->first_failure=std::current_exception();
        if(!state->first_failure)state->first_failure=std::current_exception();
        if(state->catalog)state->catalog->settle_failed(*this);
        retire_target_staging_();throw; // Exact first results/old histories/partial native ending stay held.
    }
}
} // namespace lattice
