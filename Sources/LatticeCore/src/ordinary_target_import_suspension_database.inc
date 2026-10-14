// Exact normal trigger/index suspension on the SAME unpublished target.
// This private writer changes no model row and supplies no accepted-cut,
// source/current/ACK, local-conflict disposition or publication authority.
#include <type_traits>
namespace lattice {
struct database::target_staging_state::import_suspension_native {
    using program_type=detail::ordinary_group_target_schema_original::import_suspension_program;
    using purpose=program_type::purpose;
    database* const shell;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const std::weak_ptr<detail::ordinary_group_target_schema_original> schema;
    const std::shared_ptr<program_type> program;
    std::unique_ptr<detail::ordinary_target_staging_connection::database_scope> scope;
    struct statement {
        const std::string* sql=nullptr;const std::string* binding=nullptr;
        sqlite3_stmt* held=nullptr;sqlite3_stmt* prepared_identity=nullptr;sqlite3_stmt* finalized_identity=nullptr;
        bool prepare_entered=false,prepare_returned=false,readonly_returned=false,terminal_returned=false;
        bool first_step_returned=false,finalize_entered=false,finalize_returned=false,destruction_known=false;
        bool count_returned=false,count_checked=false;
        std::int64_t count_value=0;
        int prepare_rc=SQLITE_MISUSE,readonly_value=0,first_step_rc=SQLITE_MISUSE,terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
    };
    enum class kind {prepare,readonly,parameters,bind_text,columns,name,type,integer,step,finalize,autocommit,txn_state};
    struct native_call {kind type=kind::prepare;std::size_t statement=0;bool entered=false,returned=false;
        std::int64_t integer=SQLITE_MISUSE;const void* pointer=nullptr;};
    const std::string begin_sql="BEGIN IMMEDIATE",commit_sql="COMMIT",rollback_sql="ROLLBACK";
    std::vector<statement> statements;
    std::vector<native_call> calls;
    const std::size_t begin=0,commit,rollback;
    std::size_t call_count=0,normal_calls=0,logical_bytes=sizeof(import_suspension_native);
    bool ending=false,left=false,completed=false,transaction_observed=false;
    int transaction_state=SQLITE_TXN_NONE;
    bool ending_autocommit_returned=false;int ending_autocommit=0;
    std::exception_ptr first_failure;
    import_suspension_native(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
        const std::shared_ptr<detail::ordinary_group_target_schema_original>& original,const std::shared_ptr<program_type>& input)
        :shell(&actual),owner(receiving),schema(original),program(input),commit(input->operations.size()+1),rollback(commit+1) {
        const auto state=actual.target_staging_;
        if(!state||!state->history_chain||!state->history_chain->active||!program||
           program->operations.size()>program_type::maximum_operations)
            throw db_error("GROUP import suspension actual chain/input absent");
        // The chain arena already includes the actual header/latest-page and
        // projection SQL originals. Metadata retains its separate 32 MiB pool.
        add(state->history_chain->active->storage.bytes);add(program->bytes);
        reserve_capacity(statements,program->operations.size()+3);
        statements.resize(program->operations.size()+3);
        normal_calls=program->operations.size()*20+32;
        reserve_capacity(calls,normal_calls+program->operations.size()+32);
        calls.resize(normal_calls+program->operations.size()+32);
        add(begin_sql.capacity()+commit_sql.capacity()+rollback_sql.capacity());
        statements[begin].sql=&begin_sql;statements[commit].sql=&commit_sql;statements[rollback].sql=&rollback_sql;
        for(std::size_t n=0;n<program->operations.size();++n) {
            auto& statement=statements[n+1];const auto& operation=program->operations[n];statement.sql=&operation.sql;
            if(operation.kind==purpose::require_absent)statement.binding=&operation.name;
        }
        // Every VM/call/rollback slot is charged and retained before BEGIN.
    }
    void add(std::size_t n) {
        if(n>ordinary_history_limits::stage_scratch_bytes-logical_bytes)
            throw db_error("GROUP import suspension shared staging bound refused");
        logical_bytes+=n;
    }
    template<class T> void reserve_capacity(std::vector<T>& values,std::size_t wanted) {
        if(wanted>ordinary_history_limits::stage_scratch_bytes/sizeof(T))
            throw db_error("GROUP import suspension fixed capacity refused");
        add(wanted*sizeof(T));values.reserve(wanted);
        if(values.capacity()>wanted)add((values.capacity()-wanted)*sizeof(T));
    }
    void require(database& actual,bool unwind=false)const {
        const auto state=actual.target_staging_;
        if(!state||!state->current_creator()||state->import_suspension.get()!=this||shell!=&actual||!scope||left||
           !actual.db_||state->retirement_requested||actual.retirement_->owning_wrapper_moved()||actual.retirement_->raw_handle_escaped())
            throw db_error("GROUP import suspension SAME actual Native original unavailable");
        scope->require_import_suspension_role(actual);
        if(unwind){if(!ending)throw db_error("GROUP import suspension original ending absent");scope->require_current(actual);return;}
        if(ending||first_failure||completed)throw db_error("GROUP import suspension actual work sealed");
        const auto connection=state->connection.lock();const auto receiving=owner.lock();const auto original=schema.lock();
        if(!connection||!receiving||!original||receiving->require_header_schema(*connection)!=original||
           original->require_import_suspension(*connection,state->end)!=program)
            throw db_error("GROUP import suspension actual request/schema retired");
        actual.require_target_history_chain_ending_(*connection,*receiving);scope->require_schema_work(actual);
    }
    native_call& reserve(kind type,std::size_t index,bool unwind=false) {
        if(index>=statements.size()||call_count>=(unwind?calls.size():normal_calls))
            throw db_error("GROUP import suspension Native ledger refused");
        auto& call=calls[call_count++];call.type=type;call.statement=index;return call;
    }
    template<class Value> static void returned(native_call& call,Value value)noexcept {
        if constexpr(std::is_pointer_v<Value>)call.pointer=value;else call.integer=static_cast<std::int64_t>(value);
        call.returned=true;
    }
    template<class Fn> auto observe(database& actual,kind type,std::size_t index,Fn invoke,bool unwind=false) {
        require(actual,unwind);auto& call=reserve(type,index,unwind);require(actual,unwind);call.entered=true;
        const auto value=invoke();returned(call,value);require(actual,unwind);return value;
    }
    void prepare(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements.at(index);const auto* sql=statement.sql;
        if(statement.prepare_entered||!sql||sql->empty()||sql->size()>256*1024||sql->find('\0')!=std::string::npos)
            throw db_error("GROUP import suspension actual generated statement reissue/shape refused");
        require(actual,unwind);auto& call=reserve(kind::prepare,index,unwind);require(actual,unwind);
        const char* tail=nullptr;statement.prepare_entered=true;call.entered=true;
        statement.prepare_rc=sqlite3_prepare_v2(actual.db_,sql->data(),static_cast<int>(sql->size()),&statement.held,&tail);
        statement.prepared_identity=statement.held;statement.prepare_returned=true;returned(call,statement.prepare_rc);
        require(actual,unwind);
        if(statement.prepare_rc!=SQLITE_OK||!statement.held||tail!=sql->data()+sql->size())
            throw db_error("GROUP import suspension actual Native preparation failed/changed");
        const int readonly=observe(actual,kind::readonly,index,[&]{return sqlite3_stmt_readonly(statement.held);},unwind);
        statement.readonly_value=readonly;statement.readonly_returned=true;
        if(index>begin&&index<commit) {
            const auto tag=program->operations[index-1].kind;
            const bool read=tag==purpose::require_empty||tag==purpose::require_absent;
            if((readonly!=0)!=read)throw db_error("GROUP import suspension actual generated VM purpose differs");
        }
        const int parameters=observe(actual,kind::parameters,index,[&]{return sqlite3_bind_parameter_count(statement.held);},unwind);
        if(parameters!=(statement.binding?1:0))throw db_error("GROUP import suspension actual parameter shape differs");
        if(statement.binding) {
            const auto& name=*statement.binding;
            if(name.size()>256||name.find('\0')!=std::string::npos)throw db_error("GROUP import suspension actual object name refused");
            const int rc=observe(actual,kind::bind_text,index,[&]{return sqlite3_bind_text(statement.held,1,name.data(),static_cast<int>(name.size()),SQLITE_STATIC);},unwind);
            if(rc!=SQLITE_OK)throw db_error("GROUP import suspension actual object binding failed");
        }
    }
    int step(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];require(actual,unwind);auto& call=reserve(kind::step,index,unwind);require(actual,unwind);call.entered=true;
        const int rc=sqlite3_step(statement.held);returned(call,rc);
        if(!statement.first_step_returned){statement.first_step_returned=true;statement.first_step_rc=rc;}
        if(rc!=SQLITE_ROW){statement.terminal_returned=true;statement.terminal_rc=rc;}
        if(index==begin)scope->record_schema_begin(actual,rc);
        else if(index==commit)scope->record_schema_commit(actual,rc);
        else if(index==rollback)scope->record_schema_rollback(actual,rc);
        require(actual,unwind);return rc;
    }
    void finish(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];if(!statement.held||statement.finalize_entered)return;
        require(actual,unwind);auto& call=reserve(kind::finalize,index,unwind);require(actual,unwind);
        statement.finalized_identity=statement.held;statement.finalize_entered=true;call.entered=true;
        statement.finalize_rc=sqlite3_finalize(statement.held);statement.finalize_returned=true;returned(call,statement.finalize_rc);
        if((statement.finalize_rc&0xff)!=SQLITE_MISUSE){statement.destruction_known=true;statement.held=nullptr;}
        scope->record_schema_finalized(actual,statement.finalize_rc);require(actual,unwind);
        // A returned non-MISUSE finalize has destroyed this SAME VM even
        // when it returns the prior step error. Preserve that rc/first failure,
        // but use preheld ending capacity to roll back the admitted transaction.
        if(!statement.destruction_known||(!unwind&&statement.finalize_rc!=SQLITE_OK))
            throw db_error("GROUP import suspension actual VM ending failed/unknown");
    }
    void control(database& actual,std::size_t index,bool unwind=false) {
        prepare(actual,index,unwind);if(step(actual,index,unwind)!=SQLITE_DONE)
            throw db_error("GROUP import suspension actual transaction control failed");finish(actual,index,unwind);
    }
    void require_zero(database& actual,std::size_t index) {
        auto& statement=statements[index];prepare(actual,index);
        if(step(actual,index)!=SQLITE_ROW)throw db_error("GROUP import suspension actual count row missing");
        const int columns=observe(actual,kind::columns,index,[&]{return sqlite3_column_count(statement.held);});
        if(columns!=1)throw db_error("GROUP import suspension actual count shape differs");
        const char* name=observe(actual,kind::name,index,[&]{return sqlite3_column_name(statement.held,0);});
        if(!name||name[0]!='n'||name[1]!='\0')throw db_error("GROUP import suspension actual count label differs");
        const int type=observe(actual,kind::type,index,[&]{return sqlite3_column_type(statement.held,0);});
        if(type!=SQLITE_INTEGER)throw db_error("GROUP import suspension actual count storage differs");
        require(actual);auto& call=reserve(kind::integer,index);require(actual);call.entered=true;
        statement.count_value=sqlite3_column_int64(statement.held,0);statement.count_returned=true;returned(call,statement.count_value);
        require(actual);if(statement.count_value!=0)throw db_error("GROUP import suspension actual nonempty model/object refused");
        if(step(actual,index)!=SQLITE_DONE)throw db_error("GROUP import suspension actual count did not end uniquely");
        statement.count_checked=true;finish(actual,index);
    }
    bool native_ending_known()const noexcept {
        if(first_failure||ending||!transaction_observed||transaction_state!=SQLITE_TXN_WRITE)return false;
        for(std::size_t n=0;n<call_count;++n)if(calls[n].entered&&!calls[n].returned)return false;
        for(std::size_t n=0;n<statements.size();++n)if(statements[n].prepare_entered) {
            const auto& statement=statements[n];
            if(!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||!statement.readonly_returned||
               !statement.terminal_returned||statement.terminal_rc!=SQLITE_DONE||!statement.finalize_returned||
               statement.finalize_rc!=SQLITE_OK||!statement.destruction_known||statement.held)return false;
            if(n>begin&&n<commit) {
                const auto tag=program->operations[n-1].kind;
                if((tag==purpose::require_empty||tag==purpose::require_absent)&&
                   (!statement.count_returned||!statement.count_checked||statement.count_value!=0))return false;
            }
        }
        for(std::size_t n=begin;n<=commit;++n)if(!statements[n].terminal_returned||statements[n].terminal_rc!=SQLITE_DONE)return false;
        return true;
    }
    bool cleanup_known()const noexcept {
        for(std::size_t n=0;n<call_count;++n)if(calls[n].entered&&!calls[n].returned)return false;
        for(const auto& statement:statements)if(statement.prepare_entered) {
            if(!statement.prepare_returned)return false;
            if(statement.prepared_identity&&(!statement.finalize_returned||!statement.destruction_known||statement.held))return false;
        }
        const auto& start=statements[begin];const auto& done=statements[commit];const auto& undo=statements[rollback];
        if(start.terminal_returned&&start.terminal_rc==SQLITE_DONE&&!(done.terminal_returned&&done.terminal_rc==SQLITE_DONE)&&
           !(undo.terminal_returned&&undo.terminal_rc==SQLITE_DONE)&&!(ending_autocommit_returned&&ending_autocommit!=0))return false;
        return true; // No row/current credit; first failed results remain immutable.
    }
    void settle_failed(database& actual)noexcept {
        try {
            if(!ending){scope->begin_schema_ending(actual);ending=true;}
            for(std::size_t n=0;n<statements.size();++n)if(statements[n].held&&!statements[n].finalize_entered)finish(actual,n,true);
            if(statements[begin].terminal_returned&&statements[begin].terminal_rc==SQLITE_DONE&&
               !(statements[commit].terminal_returned&&statements[commit].terminal_rc==SQLITE_DONE)) {
                require(actual,true);auto& call=reserve(kind::autocommit,rollback,true);require(actual,true);call.entered=true;
                ending_autocommit=sqlite3_get_autocommit(actual.db_);ending_autocommit_returned=true;returned(call,ending_autocommit);
                require(actual,true);if(!ending_autocommit)control(actual,rollback,true);
            }
            left=scope->leave(actual);
        }catch(...){} // The first failure and exact VM/transaction debt remain retained.
    }
    void run(database& actual) {
        control(actual,begin);
        transaction_state=observe(actual,kind::txn_state,begin,[&]{return sqlite3_txn_state(actual.db_,"main");});transaction_observed=true;
        if(transaction_state!=SQLITE_TXN_WRITE)throw db_error("GROUP import suspension actual write transaction absent");
        for(std::size_t n=0;n<program->operations.size();++n) {
            const auto tag=program->operations[n].kind;const auto index=n+1;
            if(tag==purpose::require_empty||tag==purpose::require_absent)require_zero(actual,index);
            else {prepare(actual,index);if(step(actual,index)!=SQLITE_DONE)throw db_error("GROUP import suspension generated DDL failed");finish(actual,index);}
        }
        control(actual,commit);
        if(!native_ending_known())throw db_error("GROUP import suspension actual Native ending absent");
        left=scope->leave(actual);if(!left)throw db_error("GROUP import suspension actual descriptor leave refused");
        completed=true;
    }
};
bool database::target_import_suspension_native_ending_()const noexcept {
    const auto state=target_staging_;const auto original=state?state->import_suspension:nullptr;
    return state&&state->current_creator()&&original&&original->shell==this&&original->native_ending_known();
}
bool database::target_import_suspension_cleanup_ending_()const noexcept {
    const auto state=target_staging_;if(!state||!state->current_creator())return false;
    return !state->import_suspension||state->import_suspension->cleanup_known();
}
void database::require_target_import_suspension_issued_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    require_target_history_chain_ending_(connection,receiving);const auto original=target_staging_->import_suspension;
    if(!original||original->shell!=this||original->owner.lock().get()!=&receiving||original->scope||original->first_failure||original->completed)
        throw db_error("GROUP import suspension scope lacks its actual retained programme");
}
void database::require_target_import_suspension_ending_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    require_target_history_chain_ending_(connection,receiving);const auto original=target_staging_->import_suspension;
    if(!original||original->shell!=this||original->owner.lock().get()!=&receiving||!original->completed||original->first_failure||
       !original->scope||!original->left||!original->native_ending_known()||!original->scope->leave(*this))
        throw db_error("GROUP import suspension actual ending unavailable");
}
void database::suspend_target_import_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving) {
    const auto state=target_staging_;if(!state||!state->current_creator()||!receiving||state->import_suspension_attempted||state->import_suspension)
        throw db_error("GROUP import suspension one-shot original unavailable");
    const auto connection=state->require_original();require_target_history_chain_ending_(*connection,*receiving);
    state->import_suspension_attempted=true;
    try {
        const auto schema=receiving->require_header_schema(*connection);const auto program=schema->prepare_import_suspension(*connection,state->end);
        state->import_suspension=std::make_shared<target_staging_state::import_suspension_native>(*this,receiving,schema,program);
        const auto original=state->import_suspension;
        connection->begin_database_import_suspension(*this,receiving,original->scope,state->end);
        original->run(*this);require_target_import_suspension_ending_(*connection,*receiving);
        // Restore/rebuild/integrity and actual bounded row insertion are later
        // positive stages. Original local namespace remains separately retained.
    }catch(...) {
        const auto original=state->import_suspension;
        if(original&&!original->first_failure)original->first_failure=std::current_exception();
        if(!state->first_failure)state->first_failure=std::current_exception();
        if(original&&original->scope)original->settle_failed(*this);
        retire_target_staging_();throw;
    }
}
} // namespace lattice
