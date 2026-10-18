// Included only by db.cpp, inside namespace lattice. All entry points are
// private; an immutable startup plan entry is the only issuance authority.
void database::note_initialization_end_locked_()const noexcept {
    if(!creator_current_()||!initialization_episode_||!db_||sqlite3_get_autocommit(db_)==0)return;
    auto actual=std::atomic_load(&initialization_episode_);
    if(!actual||actual->creator_!=detail::database_native_process())return;
    actual->live_.store(false,std::memory_order_release);
    actual->ended_.store(true,std::memory_order_release);
    std::atomic_compare_exchange_strong(&initialization_episode_,&actual,
        std::shared_ptr<detail::database_initialization_episode>{});
}
int database::step_initialization_(sqlite3_stmt* statement,detail::database_sql_request& request,
    const std::shared_ptr<detail::database_initialization_episode>& episode,bool begin,bool cleanup) {
    require_creator_();request.check();
    sql_entry_scope entry(*this);
    auto* mutex=sqlite3_db_mutex(db_);sqlite3_mutex_enter(mutex);
    detail::store_physical_mutex_return held(*this,mutex);
    if(!cleanup)require_catalog_clean_locked_();
    if(!request.matches(statement))throw db_error("initialization step lost its exact prepared statement");
    if(episode) {
        if(episode->creator_!=detail::database_native_process()||episode->owner_!=this)
            throw db_error("initialization transaction original differs");
        auto actual=std::atomic_load(&initialization_episode_);
        if(begin) {
            if(actual||sqlite3_get_autocommit(db_)==0)
                throw db_error("initialization BEGIN cannot adopt an existing Native transaction");
        }else if(!actual||actual!=episode||!episode->live_.load(std::memory_order_acquire)||
                 episode->ended_.load(std::memory_order_acquire)||sqlite3_get_autocommit(db_)!=0)
            throw db_error("initialization ending lost its actual issued original");
    }
    detail::database_sql_request::binding selected(*this,&request,statement,cleanup);
    if(observer_delivery_sql_&&!observer_delivery_sql_->admits(*this,statement))refuse_delivery_statement_();
    if(!cleanup&&(channel_reset_unsettled_.load(std::memory_order_acquire)||
       mutation_operation_unsettled_.load(std::memory_order_acquire)))
        throw db_error("initialization mutation is unsettled; exact rollback required");
    note_history_mutation_(statement);
    const int rc=detail::store_physical_step(statement);
    require_creator_(); // Real callbacks/allocator may have returned in a child.
    request.note_initialization_api_error_(rc,cleanup);
    note_core_transaction_end_locked_(rc); // Existing real provenance/tail bookkeeping.
    if(begin) {
        if(rc==SQLITE_DONE&&sqlite3_get_autocommit(db_)==0) {
            episode->live_.store(true,std::memory_order_release);
            std::atomic_store(&initialization_episode_,episode);
        }else if(sqlite3_get_autocommit(db_)==0) {
            // No successful BEGIN was issued. Retain actual unknown debt; do
            // not guess ownership/rollback authority from Native liveness.
            note_observer_policy_unsettled_();
        }
    }
    if(cleanup&&rc==SQLITE_DONE&&sqlite3_get_autocommit(db_)!=0) {
        channel_reset_unsettled_.store(false,std::memory_order_release);
        mutation_operation_unsettled_.store(false,std::memory_order_release);
    }
    if(rc!=SQLITE_ROW&&rc!=SQLITE_DONE&&episode) {
        auto& error=cleanup?episode->cleanup_api_error_:episode->first_api_error_;
        int none=SQLITE_OK;error.compare_exchange_strong(none,rc,std::memory_order_acq_rel);
    }
    return rc;
}
sqlite3_stmt* database::prepare_initialization_statement_(
    const detail::observer_delivery_initialization_sql& descriptor,
    detail::database_sql_request& request,engine_query_scope& issued,bool query_result) {
    require_creator_();request.check();
    if(request.owner_!=this||&issued.owner!=this||issued.request!=&request||
       !issued.active||issued.statement||engine_query_scope::current!=&issued||
       !request.initialization_entry_matches_(descriptor))
        throw db_error("initialization prepare requires its exact retained entry/request/node");
    const auto sql=descriptor.sql_();
    if(sql.size()>std::size_t(INT_MAX))throw db_error("initialization SQL length exceeds Native count");
    g_statement_count.fetch_add(1,std::memory_order_relaxed);++t_statement_count;
    sqlite3_stmt* statement=nullptr;const char* tail=nullptr;
    const int prepared=prepare_catalog_checked_(sql.data(),int(sql.size()),&statement,&tail,&request);
    issued.adopt(statement);request.note_initialization_api_error_(prepared);
    if(prepared!=SQLITE_OK||!statement)throw db_error(query_result?"failed to prepare exact initialization query":
        "failed to prepare exact initialization entry");
    // One immutable entry is one actual statement, never an exec tail permit.
    const auto* end=sql.data()+sql.size();
    while(tail&&tail<end&&(*tail==' '||*tail=='\t'||*tail=='\n'||*tail=='\r'||*tail==';'))++tail;
    if(tail&&tail!=end)throw db_error(query_result?"initialization query contains an unissued SQL tail":
        "initialization entry contains an unissued SQL tail");
    int index=1;
    if(descriptor.bootstrap_parameters_()) {
        for(std::size_t actual_index=0;actual_index<descriptor.bootstrap_parameter_count_();++actual_index) {
            const auto kind=descriptor.bootstrap_parameter_kind_(actual_index);int bound;
            if(kind==2) {
                const auto value=descriptor.bootstrap_parameter_blob_(actual_index);
                request.parameter_blob_before_copy(value);require_creator_();
                {catalog_native_admission_ admission(*this);
                 bound=sqlite3_bind_blob64(statement,index++,value.data(),
                    static_cast<sqlite3_uint64>(value.size()),SQLITE_TRANSIENT);}
            }else {
                column_value_t value=nullptr;
                if(kind==1)value=descriptor.bootstrap_parameter_integer_(actual_index);
                else if(kind!=0)throw db_error("unissued receiver parameter type");
                request.parameter_before_copy(value);bound=bind_value_checked_(statement,index++,value);
            }
            require_creator_();request.note_initialization_api_error_(bound);
            if(bound!=SQLITE_OK)throw db_error("initialization binding failed before step");
        }
    }else if(descriptor.preimage_parameters_()) {
        for(std::size_t actual_index=0;actual_index<descriptor.preimage_parameter_count_();++actual_index) {
            const auto value=descriptor.preimage_parameter_text_(actual_index);
            request.parameter_text_before_copy(value);
            int bound;require_creator_();
            {catalog_native_admission_ admission(*this);
             bound=sqlite3_bind_text64(statement,index++,value.data(),
                static_cast<sqlite3_uint64>(value.size()),SQLITE_TRANSIENT,SQLITE_UTF8);}
            require_creator_();request.note_initialization_api_error_(bound);
            if(bound!=SQLITE_OK)throw db_error("initialization binding failed before step");
        }
    }else for(const auto& param:descriptor.parameters_()) {
        request.parameter_before_copy(param);
        const int bound=bind_value_checked_(statement,index++,param);
        require_creator_();request.note_initialization_api_error_(bound);
        if(bound!=SQLITE_OK)throw db_error("initialization binding failed before step");
    }
    return statement;
}
void database::execute_initialization_engine_(const detail::observer_delivery_initialization_sql& descriptor,
    const std::shared_ptr<detail::database_initialization_episode>& episode,bool begin,bool cleanup) {
    require_creator_();
    detail::database_sql_request request(*this,descriptor,episode,cleanup);
    const auto sql=descriptor.sql_();
    if(sql.size()>std::size_t(INT_MAX))throw db_error("initialization SQL length exceeds Native count");
    sql_entry_scope entry(*this);
    engine_query_scope issued(*this,false,&request);
    request.arm_initialization_schema_attempt_();
    auto* statement=prepare_initialization_statement_(descriptor,request,issued);
    int rc;
    do {
        rc=step_initialization_(statement,request,episode,begin,cleanup);
        if(rc==SQLITE_ROW)request.row(statement,false);
    }while(rc==SQLITE_ROW);
    // Finalize the exact owned VM while the retained descriptor/request is
    // still present. Existing reset performs real end observation under Native.
    issued.reset();request.note_initialization_api_error_(issued.finalization_result,true);
    if(issued.finalization_result!=SQLITE_OK&&(rc==SQLITE_DONE||issued.finalization_result!=rc)) {
        note_observer_policy_unsettled_();
        if(rc==SQLITE_DONE)rc=issued.finalization_result;
    }
    const bool schema_retired=request.retire_initialization_schema_attempt_();
    entry.release();
    if(rc!=SQLITE_DONE) {
        discard_if_rolled_back();
        throw db_error(cleanup?"exact initialization rollback failed; original debt retained":
            "exact initialization statement failed");
    }
    if(!schema_retired)
        throw db_error("initialization schema revision invalidation failed; original debt retained");
    drain_if_settled();
}
void database::execute_initialization_(const detail::observer_delivery_initialization_sql& descriptor) {
    require_creator_();
    if(detail::database_sql_request::transaction_recipe_(descriptor))
        throw db_error("initialization transaction must preissue exact ending capacity");
    execute_initialization_engine_(descriptor,{},false,false);
}
std::vector<database::row_t> database::query_initialization_(
    const detail::observer_delivery_initialization_sql& descriptor) {
    require_creator_();
    if(detail::database_sql_request::transaction_recipe_(descriptor))
        throw db_error("initialization query cannot issue a transaction control");
    if(descriptor.schema_may_change_())
        throw db_error("initialization schema-changing recipe requires its exact execute channel");
    detail::database_sql_request request(*this,descriptor);
    const auto sql=descriptor.sql_();
    if(sql.size()>std::size_t(INT_MAX))throw db_error("initialization SQL length exceeds Native count");
    sql_entry_scope entry(*this);engine_query_scope issued(*this,false,&request);
    auto* statement=prepare_initialization_statement_(descriptor,request,issued,true);
    request.columns(statement);
    const auto count=sqlite3_column_count(statement);
    std::vector<std::string> names;names.reserve(std::size_t(count));
    for(int column=0;column<count;++column) {
        const auto* name=sqlite3_column_name(statement,column);
        if(!name)throw db_error("initialization column name unavailable");
        names.emplace_back(name);
    }
    std::vector<row_t> rows;int rc;
    while((rc=step_initialization_(statement,request,{},false,false))==SQLITE_ROW) {
        request.row(statement);row_t row;
        for(int column=0;column<count;++column)row[names[std::size_t(column)]]=extract_column(statement,column);
        rows.push_back(std::move(row));
    }
    issued.reset();request.note_initialization_api_error_(issued.finalization_result,true);
    if(issued.finalization_result!=SQLITE_OK&&(rc==SQLITE_DONE||issued.finalization_result!=rc)) {
        note_observer_policy_unsettled_();
        if(rc==SQLITE_DONE)rc=issued.finalization_result;
    }
    entry.release();
    if(rc!=SQLITE_DONE){discard_if_rolled_back();throw db_error("exact initialization query failed");}
    drain_if_settled();
    // Bare owning-container transfer: Core no longer owns these results, but
    // its SAME monotonic setup charge stays for retained startup/catalog tails.
    return rows;
}
struct database::initialization_transaction::fields final {
    const std::uint64_t creator;
    database* const owner; // constructor-owned comparison/dispatch only
    std::shared_ptr<detail::database_initialization_episode> episode;
    std::optional<detail::observer_delivery_initialization_sql> commit,rollback;
    bool rollback_failed=false; // actual same-creator failure, never liveness inference
    fields(std::uint64_t issuer,database* actual)noexcept:creator(issuer),owner(actual){}
    static void destroy(fields* value)noexcept {
        if(value&&value->creator==detail::database_native_process())delete value;
    }
};
database::initialization_transaction::initialization_transaction(database& owner,
    const detail::observer_delivery_initialization_sql& begin,
    const detail::observer_delivery_initialization_sql& commit,
    const detail::observer_delivery_initialization_sql& rollback)
    :initialization_transaction(owner,begin,commit,rollback,nullptr) {}
database::initialization_transaction::initialization_transaction(database& owner,
    const detail::observer_delivery_initialization_sql& begin,
    const detail::observer_delivery_initialization_sql& commit,
    const detail::observer_delivery_initialization_sql& rollback,std::exception_ptr* actual_cleanup_error)
    :creator_(detail::database_native_process()) {
    owner.require_creator_();
    begin.check_for_(owner);commit.check_for_(owner);rollback.check_for_(owner);
    auto* budget=owner.delivery_startup_->initialization_budget_(owner).get();
    fields* actual=nullptr;
    try {
        budget->claim_workspace_(sizeof(fields));
        if(detail::database_initialization_test_probe::consume(detail::database_initialization_test_probe::allocation_stage::transaction))
            throw std::bad_alloc();
        actual=new fields(creator_,&owner);
        if(creator_!=detail::database_native_process())throw db_error("inherited initialization transaction custody");
        actual->episode=detail::database_initialization_episode::prepare_(owner,begin,commit,rollback);
        actual->commit.emplace(commit.retain_());actual->rollback.emplace(rollback.retain_());
        if(creator_!=detail::database_native_process())throw db_error("inherited initialization ending entries");
        value_=actual;owner.execute_initialization_engine_(begin,actual->episode,true,false);
    }catch(...) {
        if(creator_==detail::database_native_process()) {
            budget->fail_();
            if(value_&&actual&&actual->episode&&actual->episode->live_.load(std::memory_order_acquire)) {
                try {this->rollback();}catch(...){
                    // Retain the genuine same-creator secondary in the outer
                    // owned result before this catch ends. Preserve its first
                    // debt; no callback, allocation, fabricated code or retry.
                    if(creator_==detail::database_native_process()&&actual_cleanup_error&&!*actual_cleanup_error)
                        *actual_cleanup_error=std::current_exception();
                    owner.note_observer_policy_unsettled_();
                }
            }
            value_=nullptr;fields::destroy(actual);
        }
        throw;
    }
}
database::initialization_transaction::initialization_transaction(initialization_transaction&& other)noexcept
    :creator_(other.creator_),value_(other.creator_==detail::database_native_process()?std::exchange(other.value_,nullptr):nullptr){}
database::initialization_transaction::~initialization_transaction()noexcept {
    if(creator_!=detail::database_native_process()||!value_)return;
    auto* actual=std::exchange(value_,nullptr);
    if(actual->episode&&actual->episode->live_.load(std::memory_order_acquire)&&
       !actual->episode->ended_.load(std::memory_order_acquire)) {
        // Keep the exact field alive while rollback executes. Failure is
        // persistent actual-open policy debt, never a guessed ending/refund.
        value_=actual;
        try {rollback();}catch(...){if(creator_==detail::database_native_process())actual->owner->note_observer_policy_unsettled_();}
        value_=nullptr;
    }
    fields::destroy(actual);
}
void database::initialization_transaction::commit() {
    if(creator_!=detail::database_native_process()||!value_||!value_->episode)
        throw db_error("inherited or empty initialization transaction");
    value_->owner->execute_initialization_engine_(*value_->commit,value_->episode,false,false);
}
void database::initialization_transaction::rollback() {
    if(creator_!=detail::database_native_process()||!value_||!value_->episode)
        throw db_error("inherited or empty initialization transaction");
    if(!value_->episode->budget_||!value_->episode->budget_->current_()) {
        value_->rollback_failed=true;
        throw db_error("initialization rollback creator/builder differs");
    }
    if(value_->episode->ended_.load(std::memory_order_acquire))return;
    try {value_->owner->execute_initialization_engine_(*value_->rollback,value_->episode,false,true);}
    catch(...) {
        if(creator_==detail::database_native_process()) {
            value_->rollback_failed=true;value_->owner->note_observer_policy_unsettled_();
        }
        throw;
    }
}
bool database::initialization_transaction::retire_failed_rollback_()noexcept {
    if(creator_!=detail::database_native_process()||!value_||value_->creator!=creator_||
       !value_->rollback_failed)return false;
    auto* actual=std::exchange(value_,nullptr);
    // The outer enrollment retains the actual writer/Open and this reader's
    // same budget/episode. Retire only already-consumed ending descriptors;
    // the real live episode, Native state and persistent policy debt remain.
    actual->owner->note_observer_policy_unsettled_();
    fields::destroy(actual);return true;
}
database::initialization_transaction database::begin_initialization_(
    const detail::observer_delivery_initialization_sql& begin,
    const detail::observer_delivery_initialization_sql& commit,
    const detail::observer_delivery_initialization_sql& rollback) {
    return initialization_transaction(*this,begin,commit,rollback);
}

database::initialization_transaction database::begin_initialization_(
    const detail::observer_delivery_initialization_sql& begin,
    const detail::observer_delivery_initialization_sql& commit,
    const detail::observer_delivery_initialization_sql& rollback,std::exception_ptr& actual_cleanup_error) {
    return initialization_transaction(*this,begin,commit,rollback,&actual_cleanup_error);
}
