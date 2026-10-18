// SAME authenticated receiving original, fixed per-page passive staging only.
// Included after header channel. No supplied SQL/path/old-owner/cut factory.
#include "ordinary_history_record_key_recipe.hpp"
#include "ordinary_target_history_spool.hpp"
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_FIXTURE)
#if !defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
#error "Receiving history fault qualification requires the debug macOS owner fixture"
#endif
#include "ordinary_receiver_reclamation_fixture.hpp"
#endif
namespace lattice {
struct database::target_staging_state::history_page_native {
    using arguments=std::vector<column_value_t>;
    terminal_native_page_tail terminal_page; // FIRST: runs after every actual page member dies.
    database* const shell;
    // SAME IO original is retained before SQL scope/native registration. The
    // numeric pair is its checked DATA; it cannot substitute for this original.
    const std::shared_ptr<const detail::ordinary_target_history_page_append> raw_append;
    const std::pair<std::uint64_t,std::uint64_t> raw_span;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const std::weak_ptr<detail::ordinary_group_target_schema_original> schema;
    std::shared_ptr<history_page_native> previous;
    std::unique_ptr<detail::ordinary_target_staging_connection::database_scope> scope;
    static constexpr std::size_t statements_limit=144,normal_calls=8192,total_calls=8448;
    struct statement {
        sqlite3_stmt* held=nullptr;sqlite3_stmt* prepared_identity=nullptr;sqlite3_stmt* finalized_identity=nullptr;
        bool prepare_entered=false,prepare_returned=false,terminal_returned=false,finalize_entered=false,finalize_returned=false;
        bool destruction_known=false;int prepare_rc=SQLITE_MISUSE,first_step_rc=SQLITE_MISUSE,terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
        bool first_step_returned=false;
    };
    enum class kind {prepare,parameters,bind_integer,bind_text,bind_blob,step,finalize,columns,type,integer,bytes,text,blob,changes,autocommit,register_guard,context,result,error_result};
    struct native_call {kind type=kind::prepare;std::size_t statement=0,column=0;bool entered=false,returned=false;std::int64_t value=SQLITE_MISUSE;sqlite3_context* context=nullptr;};
    std::array<statement,statements_limit> statements{};std::array<native_call,total_calls> calls{};
    std::size_t call_count=0,completed_operations=0,guard_invocations=0,logical_bytes=0,active_statement=statements_limit;
    std::uint64_t durable_bytes=0;
    static constexpr std::size_t begin=0,stage_before=1,meta_before=2,key_first=3,key_verify_first=67,
        page_insert=131,page_read=132,stage_update=133,meta_update=134,stage_after=135,meta_after=136,counts=137,commit=138,rollback=139;
    const std::string begin_sql="BEGIN IMMEDIATE",commit_sql="COMMIT",rollback_sql="ROLLBACK";
    const std::string stage_read_sql="SELECT stage,role,source,epoch,cut,scope,schema,device,inode,state,header,rows,known,unknown,pages,bytes,content,seal FROM main._lattice_ordered_history_stage_v1 WHERE stage=?";
    const std::string meta_read_sql="SELECT stage,channel,records_limit,pages_limit,page_limit,records_per_page,bytes_limit,last_table,seal FROM main._lattice_ordered_history_receive_v1 WHERE stage=?";
    const std::string key_insert_sql="INSERT INTO main._lattice_ordered_history_receive_key_v1 VALUES(?,?,?,?,?,?,?)";
    const std::string key_read_sql="SELECT stage,role,model,gid,page,ordinal,seal FROM main._lattice_ordered_history_receive_key_v1 WHERE stage=? AND page=? AND ordinal=?";
    const std::string page_insert_sql=raw_append?
        "INSERT INTO main._lattice_ordered_history_raw_page_v1 VALUES(?,?,?,?,?,?,?,?,?,?)":
        "INSERT INTO main._lattice_ordered_history_page_v1 VALUES(?,?,?,?,?,?,?,?,?)";
    const std::string page_read_sql=raw_append?
        "SELECT stage,page,previous,digest,raw_offset,raw_length,rows,known,unknown,seal FROM main._lattice_ordered_history_raw_page_v1 WHERE stage=? AND page=?":
        "SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?";
    const std::string stage_update_sql="UPDATE main._lattice_ordered_history_stage_v1 SET state=?,rows=?,known=?,unknown=?,pages=?,bytes=?,seal=? WHERE stage=?";
    const std::string meta_update_sql="UPDATE main._lattice_ordered_history_receive_v1 SET last_table=?,seal=? WHERE stage=?";
    // The genuine prior program and its typed postimage establish the prefix.
    // Count this new page only; unique stage/page indexes bound the new scan.
    // Cold/reconstructed chains still require their own complete validation.
    const std::string counts_sql=raw_append?
        "SELECT (SELECT COUNT(*) FROM main._lattice_ordered_history_raw_page_v1 WHERE stage=? AND page=?),(SELECT COUNT(*) FROM main._lattice_ordered_history_receive_key_v1 WHERE stage=? AND page=?)":
        "SELECT (SELECT COUNT(*) FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?),(SELECT COUNT(*) FROM main._lattice_ordered_history_receive_key_v1 WHERE stage=? AND page=?)";
    arguments stage,meta,page,stage_key,page_key,stage_update_values,meta_update_values,count_key,count_values;
    std::vector<arguments> keys,key_queries;
    detail::ordinary_history_progress progress;
    picosha2::hash256_one_by_one content;
    bool ending=false,left=false,completed=false,guard_registration_entered=false,guard_registration_returned=false;
    bool guard_destroy_entered=false,guard_destroy_returned=false,previous_destroy_joined=false;
    int first_guard_registration_rc=SQLITE_MISUSE;
    bool ending_autocommit_entered=false,ending_autocommit_returned=false;int first_ending_autocommit=0;
    std::exception_ptr first_failure;
    struct guard_userdata {
        const std::shared_ptr<target_staging_state> original;
        const std::shared_ptr<history_page_native> operation;
        database* const shell;sqlite3* const native;
        guard_userdata(std::shared_ptr<target_staging_state> value,std::shared_ptr<history_page_native> input,database& actual)
            :original(std::move(value)),operation(std::move(input)),shell(&actual),native(actual.db_){}
        static void destroy(void* raw)noexcept {
            auto* held=static_cast<guard_userdata*>(raw);
            if(!held||!held->original->current_creator()||held->operation->userdata!=held||held->operation->shell!=held->shell)return;
            const auto original=held->original;const auto actual=held->operation;actual->guard_destroy_entered=true;
            delete held;actual->guard_destroy_returned=true; // SAME issued object survives actual destruction-return.
        }
        static void invoke(sqlite3_context* context,int,sqlite3_value**)noexcept {
            auto* held=static_cast<guard_userdata*>(sqlite3_user_data(context));
            if(!held||!held->original->current_creator()||held->original->history_page!=held->operation||held->operation->userdata!=held)return;
            const auto original=held->original;const auto actual=held->operation;
            try {
                actual->require(*held->shell);
                if(actual->guard_invocations>=67||actual->active_statement>=statements_limit||
                   !actual->statements[actual->active_statement].held)throw db_error("target page guard actual VM/capacity unavailable");
                ++actual->guard_invocations;const auto at=actual->active_statement;
                auto& native=actual->reserve(kind::context,at);actual->require(*held->shell);native.context=context;native.entered=true;
                const auto* db=sqlite3_context_db_handle(context);returned(native,static_cast<std::int64_t>(reinterpret_cast<std::uintptr_t>(db)));
                actual->require(*held->shell);if(db!=held->native||db!=held->shell->db_)throw db_error("target page guard actual native context differs");
                auto& result=actual->reserve(kind::result,at);actual->require(*held->shell);result.context=context;result.entered=true;
                sqlite3_result_int(context,1);returned(result,0);actual->require(*held->shell);
            }catch(...) {
                if(!actual->first_failure)actual->first_failure=std::current_exception();if(!original->first_failure)original->first_failure=std::current_exception();
                try {actual->enter_ending(*held->shell);actual->require(*held->shell,true);
                    auto& result=actual->reserve(kind::error_result,actual->active_statement,true);actual->require(*held->shell,true);
                    result.context=context;result.entered=true;sqlite3_result_error(context,"target same-original page guard failed",-1);
                    returned(result,0);actual->require(*held->shell,true);
                }catch(...){}
            }
        }
    };
    guard_userdata* userdata=nullptr;
    history_page_native(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
        const std::shared_ptr<detail::ordinary_group_target_schema_original>& original,const detail::ordinary_history_descriptor& header,
        const detail::ordinary_history_decoded_page& input,const std::shared_ptr<history_page_native>& prior,
        const arguments& before_stage,const arguments& before_meta,const detail::ordinary_history_progress& before_progress,
        const picosha2::hash256_one_by_one& before_content,
        std::shared_ptr<const detail::ordinary_target_history_page_append> append,std::pair<std::uint64_t,std::uint64_t> span)
        :terminal_page(actual,receiving,this,input.index),shell(&actual),raw_append(std::move(append)),raw_span(span),owner(receiving),schema(original),previous(prior),stage(before_stage),meta(before_meta),progress(before_progress),content(before_content) {
        const ordinary_history_limits limits;
        if(input.index!=progress.next_page||input.previous!=progress.previous||progress.complete||input.records.empty()||input.records.size()>64||
           input.raw.empty()||input.raw.size()>limits.page_bytes||std::get<std::int64_t>(stage.at(15))<0||
           input.raw.size()>limits.durable_stage_bytes-static_cast<std::uint64_t>(std::get<std::int64_t>(stage.at(15))))
            throw db_error("target page actual prior progress/input/capacity differs");
        if(raw_append&&(raw_span.first!=std::uint64_t(std::get<std::int64_t>(stage.at(15)))||raw_span.second!=input.raw.size()||
           raw_span.first>limits.durable_stage_bytes||raw_span.second>limits.durable_stage_bytes-raw_span.first||
           raw_span.first>std::uint64_t(INT64_MAX)||raw_span.second>std::uint64_t(INT64_MAX)))
            throw db_error("target raw locator differs from actual append/prior prefix");
        durable_bytes=prior?prior->durable_bytes:
            header.stage.size()+header.projection.source.size()+header.projection.scope.size()+header.projection.schema.size()+
            std::get<std::string>(before_stage.at(7)).size()+std::get<std::string>(before_stage.at(8)).size()+80+64+
            header.header.size()+header.content_digest.size()+header.stage.size()+std::get<std::string>(before_meta.at(1)).size()+48+64;
        if(durable_bytes>limits.durable_stage_bytes)throw db_error("target genuine prior passive durable accounting exceeded");
        const auto reserve_durable=[&](std::uint64_t bytes){if(bytes>limits.durable_stage_bytes-durable_bytes)
            throw db_error("target shared passive durable logical quota refused before Native writes");durable_bytes+=bytes;};
        keys.reserve(input.records.size());key_queries.reserve(input.records.size());
        for(std::size_t at=0;at<input.records.size();++at) {
            keys.push_back(detail::ordinary_history_record_key_recipe::key(header,input,at,progress));
            const auto& key=keys.back();reserve_durable(header.stage.size()+std::get<std::string>(key.at(2)).size()+std::get<std::string>(key.at(3)).size()+24+64);
            key_queries.push_back(arguments{header.stage,std::int64_t(input.index),std::int64_t(at)});
        }
        // Raw bytes have the separately retained actual spool extent charge.
        // This passive MAIN accounting retains locator/key/bootstrap metadata;
        // it is not a physical SQLite model/index fit proof.
        reserve_durable(header.stage.size()+input.previous.size()+input.full_digest.size()+(raw_append?48:input.raw.size()+32)+64);
        ++progress.next_page;progress.previous=input.full_digest;progress.complete=progress.next_page==header.pages;
        if(progress.rows>header.rows||progress.known>header.known||progress.unknown>header.unknown||progress.next_page>header.pages||
           (progress.complete&&(progress.rows!=header.rows||progress.known!=header.known||progress.unknown!=header.unknown)))
            throw db_error("target page actual accumulated counts differ");
        page={header.stage,std::int64_t(input.index),input.previous,input.full_digest};
        if(raw_append){page.emplace_back(std::int64_t(raw_span.first));page.emplace_back(std::int64_t(raw_span.second));}
        else page.emplace_back(std::vector<std::uint8_t>(input.raw.begin(),input.raw.end()));
        page.emplace_back(std::int64_t(progress.rows-before_progress.rows));
        page.emplace_back(std::int64_t(progress.known-before_progress.known));
        page.emplace_back(std::int64_t(progress.unknown-before_progress.unknown));
        page.emplace_back(detail::ordinary_history_storage_seal(raw_append?
            detail::ordinary_target_raw_history_storage_recipe::seal_domain:"ordinary-paged-page-v1",page,raw_append?9:8));
        stage[9]=std::int64_t(progress.complete);stage[11]=std::int64_t(progress.rows);stage[12]=std::int64_t(progress.known);stage[13]=std::int64_t(progress.unknown);
        stage[14]=std::int64_t(progress.next_page);stage[15]=std::get<std::int64_t>(stage[15])+std::int64_t(input.raw.size());
        stage[17]=detail::ordinary_history_storage_seal("ordinary-paged-stage-v1",stage,17);
        meta[7]=progress.last_table;meta[8]=detail::ordinary_history_storage_seal("ordinary-history-receive-meta-v1",meta,8);
        stage_key={header.stage};page_key={header.stage,std::int64_t(input.index)};
        stage_update_values={stage[9],stage[11],stage[12],stage[13],stage[14],stage[15],stage[17],stage[0]};
        meta_update_values={meta[7],meta[8],meta[0]};
        count_key={header.stage,std::int64_t(input.index),header.stage,std::int64_t(input.index)};
        count_values={std::int64_t{1},std::int64_t(keys.size())};
        // Preserve exact concatenated wire bytes; chunk the unchanged hash producer.
        // Its opaque library allocator workspace is not a native RSS/capacity proof.
        for(std::size_t at=0;at<input.raw.size();) {
            const auto count=std::min<std::size_t>(65536,input.raw.size()-at);
            content.process(input.raw.begin()+at,input.raw.begin()+at+count);at+=count;
        }
        if(progress.complete){auto finished=content;finished.finish();if(picosha2::get_hash_hex_string(finished)!=header.content_digest)
            throw db_error("target final passive bytes content digest differs");}
        std::size_t retained=sizeof(*this);const auto charge=[&](std::size_t bytes){
            if(bytes>ordinary_history_limits::stage_scratch_bytes-retained)throw db_error("target page combined staging logical capacity bound");retained+=bytes;};
        for(const auto* sql:{&begin_sql,&commit_sql,&rollback_sql,&stage_read_sql,&meta_read_sql,&key_insert_sql,&key_read_sql,&page_insert_sql,&page_read_sql,&stage_update_sql,&meta_update_sql,&counts_sql})charge(sql->capacity());
        const auto args_charge=[&](const arguments& args){if(args.capacity()>ordinary_history_limits::stage_scratch_bytes/sizeof(column_value_t))
            throw db_error("target page actual argument capacity bound");charge(args.capacity()*sizeof(column_value_t));
            for(const auto& value:args){if(const auto* text=std::get_if<std::string>(&value))charge(text->capacity());else if(const auto* blob=std::get_if<std::vector<std::uint8_t>>(&value))charge(blob->capacity());}};
        for(const auto* args:{&stage,&meta,&page,&stage_key,&page_key,&stage_update_values,&meta_update_values,&count_key,&count_values})args_charge(*args);
        charge((keys.capacity()+key_queries.capacity())*sizeof(arguments));for(const auto& args:keys)args_charge(args);for(const auto& args:key_queries)args_charge(args);
        charge(progress.previous.capacity());logical_bytes=retained;
        const auto header_original=actual.target_staging_->history_header;
        if(!header_original)throw db_error("target page actual header program missing");charge(header_original->logical_bytes);
        if(previous)charge(previous->logical_bytes); // Fixed two-page overlap; no successive allocation after unknown ending.
        terminal_page.constructor_returned(); // Final constructor action, never a partial-page receipt.
    }
    void require(database& actual,bool unwind=false)const {
        const auto state=actual.target_staging_;
        if(!state||!state->current_creator()||shell!=&actual||state->history_page.get()!=this||!scope||left||!actual.db_||
           state->retirement_requested||actual.retirement_->owning_wrapper_moved()||actual.retirement_->raw_handle_escaped())
            throw db_error("target page actual scope/shell unavailable");
        scope->require_history_page_role(actual);
        if(unwind){if(!ending)throw db_error("target page admitted ending unavailable");scope->require_current(actual);return;}
        if(ending||first_failure)throw db_error("target page generated Native work sealed");
        const auto held=owner.lock();const auto connection=state->connection.lock();
        if(!held||!connection||held->require_header_schema(*connection)!=schema.lock())throw db_error("target page actual owner/schema unavailable");
        (void)held->require_page_original(*connection);scope->require_schema_work(actual);
    }
    native_call& reserve(kind type,std::size_t statement,bool unwind=false,std::size_t column=0) {
        if(call_count>=(unwind?total_calls:normal_calls)||statement>=statements_limit)
            throw db_error("target page fixed native ledger exhausted");
        auto& actual=calls[call_count++];actual.type=type;actual.statement=statement;actual.column=column;return actual;
    }
    static void returned(native_call& actual,std::int64_t value)noexcept {actual.value=value;actual.returned=true;}
    void prepare(database& actual,std::size_t index,const std::string& sql,bool unwind=false) {
        auto& statement=statements[index];if(statement.prepare_entered||sql.empty()||sql.find('\0')!=std::string::npos||sql.size()>4096)
            throw db_error("target page fixed statement unavailable");
        require(actual,unwind);auto& call=reserve(kind::prepare,index,unwind);require(actual,unwind);
        const char* tail=nullptr;statement.prepare_entered=true;call.entered=true;
        statement.prepare_rc=::lattice::detail::store_physical_prepare_v2(actual.db_,sql.c_str(),static_cast<int>(sql.size()+1),&statement.held,&tail);
        statement.prepared_identity=statement.held;statement.prepare_returned=true;returned(call,statement.prepare_rc);
        if(statement.prepare_rc!=SQLITE_OK||!statement.held)throw db_error("target page preparation failed");
        require(actual,unwind);if(!tail||tail<sql.c_str()||tail>sql.c_str()+sql.size())throw db_error("target page native tail unavailable");
        for(const auto* at=tail;at<sql.c_str()+sql.size();++at)if(*at!=' '&&*at!='\t'&&*at!='\r'&&*at!='\n')throw db_error("target page native extra SQL tail");
    }
    void bind(database& actual,std::size_t index,const arguments& args) {
        auto* statement=statements[index].held;require(actual);auto& count=reserve(kind::parameters,index);require(actual);count.entered=true;
        const int parameters=sqlite3_bind_parameter_count(statement);returned(count,parameters);require(actual);
        if(parameters<0||static_cast<std::size_t>(parameters)!=args.size())throw db_error("target page parameter count differs");
        for(std::size_t at=0;at<args.size();++at) {
            int rc=SQLITE_MISUSE;
            if(const auto* number=std::get_if<std::int64_t>(&args[at])) {
                auto& call=reserve(kind::bind_integer,index,false,at);require(actual);call.entered=true;
                rc=sqlite3_bind_int64(statement,static_cast<int>(at+1),*number);returned(call,rc);require(actual);
            }else if(const auto* text=std::get_if<std::string>(&args[at])) {
                if(text->size()>ordinary_sync_limits::frame_bytes)throw db_error("target page text input bound");
                auto& call=reserve(kind::bind_text,index,false,at);require(actual);call.entered=true;
                rc=sqlite3_bind_text(statement,static_cast<int>(at+1),text->data(),static_cast<int>(text->size()),SQLITE_STATIC);returned(call,rc);require(actual);
            }else if(const auto* bytes=std::get_if<std::vector<std::uint8_t>>(&args[at])) {
                if(bytes->empty()||bytes->size()>ordinary_sync_limits::frame_bytes)throw db_error("target page BLOB input bound");
                auto& call=reserve(kind::bind_blob,index,false,at);require(actual);call.entered=true;
                rc=sqlite3_bind_blob(statement,static_cast<int>(at+1),bytes->data(),static_cast<int>(bytes->size()),SQLITE_STATIC);returned(call,rc);require(actual);
            }else throw db_error("target page unsupported generated argument");
            if(rc!=SQLITE_OK)throw db_error("target page fixed argument binding failed");
        }
    }
    int step(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];if(!statement.held||statement.finalize_entered)throw db_error("target page actual VM unavailable");
        require(actual,unwind);auto& call=reserve(kind::step,index,unwind);require(actual,unwind);call.entered=true;
        active_statement=index;const int rc=::lattice::detail::store_physical_step(statement.held);returned(call,rc);active_statement=statements_limit;
        if(!statement.first_step_returned){statement.first_step_returned=true;statement.first_step_rc=rc;}
        if(rc!=SQLITE_ROW){if(statement.terminal_returned)throw db_error("target page native terminal reissued");statement.terminal_returned=true;statement.terminal_rc=rc;}
        if(index==begin)scope->record_schema_begin(actual,rc);if(index==commit)scope->record_schema_commit(actual,rc);if(index==rollback)scope->record_schema_rollback(actual,rc);
        require(actual,unwind);return rc;
    }
    void finish(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];if(!statement.held||statement.finalize_entered)return;
        require(actual,unwind);auto& call=reserve(kind::finalize,index,unwind);require(actual,unwind);
        statement.finalized_identity=statement.held;statement.finalize_entered=true;call.entered=true;
        statement.finalize_rc=::lattice::detail::store_physical_finalize(statement.held);statement.finalize_returned=true;returned(call,statement.finalize_rc);
        statement.destruction_known=(statement.finalize_rc&0xff)!=SQLITE_MISUSE;if(statement.destruction_known)statement.held=nullptr;
        scope->record_schema_finalized(actual,statement.finalize_rc);
        if(statement.finalize_rc!=SQLITE_OK)throw db_error("target page native finalization failed");require(actual,unwind);
    }
    void execute(database& actual,std::size_t index,const std::string& sql,const arguments& args={},bool unwind=false) {
        prepare(actual,index,sql,unwind);if(!unwind)bind(actual,index,args);
        if(step(actual,index,unwind)!=SQLITE_DONE)throw db_error("target page fixed operation did not finish");finish(actual,index,unwind);
    }
    void changes(database& actual,std::size_t index) {
        require(actual);auto& call=reserve(kind::changes,index);require(actual);call.entered=true;
        const int changed=sqlite3_changes(actual.db_);returned(call,changed);require(actual);
        if(changed!=1)throw db_error("target page generated INSERT not applied exactly once");
    }
    void verify(database& actual,std::size_t index,const std::string& sql,const arguments& bindings,const arguments& expected) {
        prepare(actual,index,sql);bind(actual,index,bindings);if(step(actual,index)!=SQLITE_ROW)throw db_error("target page actual postimage missing");
        auto* stmt=statements[index].held;auto& count=reserve(kind::columns,index);require(actual);count.entered=true;
        const int columns=sqlite3_column_count(stmt);returned(count,columns);require(actual);
        if(columns<0||static_cast<std::size_t>(columns)!=expected.size())throw db_error("target page postimage column shape differs");
        for(std::size_t at=0;at<expected.size();++at) {
            auto& type=reserve(kind::type,index,false,at);require(actual);type.entered=true;const int storage=sqlite3_column_type(stmt,static_cast<int>(at));returned(type,storage);require(actual);
            if(const auto* number=std::get_if<std::int64_t>(&expected[at])) {
                if(storage!=SQLITE_INTEGER)throw db_error("target page actual INTEGER storage differs");
                auto& call=reserve(kind::integer,index,false,at);require(actual);call.entered=true;const auto value=sqlite3_column_int64(stmt,static_cast<int>(at));returned(call,value);require(actual);
                if(value!=*number)throw db_error("target page actual INTEGER value differs");
            }else {
                const auto* text=std::get_if<std::string>(&expected[at]);const auto* blob=std::get_if<std::vector<std::uint8_t>>(&expected[at]);
                if((text&&storage!=SQLITE_TEXT)||(blob&&storage!=SQLITE_BLOB)||(!text&&!blob))throw db_error("target page actual byte storage differs");
                auto& size=reserve(kind::bytes,index,false,at);require(actual);size.entered=true;const int bytes=sqlite3_column_bytes(stmt,static_cast<int>(at));returned(size,bytes);require(actual);
                const auto wanted=text?text->size():blob->size();if(bytes<0||static_cast<std::size_t>(bytes)!=wanted)throw db_error("target page actual byte length differs");
                auto& pointer=reserve(text?kind::text:kind::blob,index,false,at);require(actual);pointer.entered=true;
                const void* value=text?static_cast<const void*>(sqlite3_column_text(stmt,static_cast<int>(at))):sqlite3_column_blob(stmt,static_cast<int>(at));
                returned(pointer,static_cast<std::int64_t>(reinterpret_cast<std::uintptr_t>(value)));require(actual);
                const void* expected_bytes=text?static_cast<const void*>(text->data()):static_cast<const void*>(blob->data());
                if(bytes&&(!value||std::memcmp(value,expected_bytes,static_cast<std::size_t>(bytes))))throw db_error("target page actual retained byte value differs");
            }
        }
        if(step(actual,index)!=SQLITE_DONE)throw db_error("target page postimage not unique");finish(actual,index);
    }
    void enter_ending(database& actual) {if(!scope||left)throw db_error("target page admitted ending unavailable");scope->begin_schema_ending(actual);ending=true;}
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
    bool sealed(const database& actual)const noexcept {
        try {
            const auto state=actual.target_staging_;
            if(!state||!state->current_creator()||shell!=&actual||!scope||!left||!completed||ending||first_failure||
               !guard_registration_entered||!guard_registration_returned||first_guard_registration_rc!=SQLITE_OK||
               completed_operations!=10+2*keys.size()||guard_invocations!=keys.size()+3||!previous_destroy_joined)
                return false;
            if(!scope->leave(actual))return false;
            const auto& first=statements[begin];const auto& last=statements[commit];
            if(!first.terminal_returned||first.terminal_rc!=SQLITE_DONE||!last.terminal_returned||last.terminal_rc!=SQLITE_DONE)return false;
            for(std::size_t at=0;at<call_count;++at)if(calls[at].entered&&!calls[at].returned)return false;
            for(const auto& statement:statements)if(statement.prepare_entered&&
               (!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||statement.held||!statement.terminal_returned||statement.terminal_rc!=SQLITE_DONE||
                !statement.finalize_entered||!statement.finalize_returned||!statement.destruction_known||!statement.prepared_identity||
                statement.finalized_identity!=statement.prepared_identity||statement.finalize_rc!=SQLITE_OK))return false;
            return true;
        }catch(...){return false;}
    }
};
bool database::target_history_page_guard_destroyed_()const noexcept {
    const auto state=target_staging_;const auto actual=state?state->history_page:nullptr;
    if(!state||!state->current_creator()||state->terminal_native_pages_attempted)return false;if(!actual)return true;
    if(!actual->userdata)return !actual->guard_registration_entered;
    return actual->guard_destroy_entered&&actual->guard_destroy_returned;
}
bool database::target_history_page_native_ending_()const noexcept {
    try {
        const auto state=target_staging_;const auto actual=state?state->history_page:nullptr;
        if(!state||!state->current_creator()||state->terminal_native_pages_attempted||!actual||actual->shell!=this||!actual->scope||actual->ending||actual->first_failure||
           !actual->guard_registration_entered||!actual->guard_registration_returned||actual->first_guard_registration_rc!=SQLITE_OK||
           !actual->userdata||actual->guard_destroy_entered||actual->guard_destroy_returned||!actual->previous_destroy_joined||
           actual->completed_operations!=10+2*actual->keys.size()||actual->guard_invocations!=actual->keys.size()+3)return false;
        actual->scope->require_history_page_role(*this);
        const auto& begin=actual->statements[actual->begin];const auto& commit=actual->statements[actual->commit];
        if(!begin.terminal_returned||begin.terminal_rc!=SQLITE_DONE||!commit.terminal_returned||commit.terminal_rc!=SQLITE_DONE)return false;
        for(std::size_t at=0;at<actual->call_count;++at)if(actual->calls[at].entered&&!actual->calls[at].returned)return false;
        for(const auto& statement:actual->statements)if(statement.prepare_entered&&
           (!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||statement.held||!statement.terminal_returned||statement.terminal_rc!=SQLITE_DONE||
            !statement.finalize_entered||!statement.finalize_returned||!statement.destruction_known||!statement.prepared_identity||
            statement.finalized_identity!=statement.prepared_identity||statement.finalize_rc!=SQLITE_OK))return false;
        return true;
    }catch(...){return false;}
}
detail::ordinary_history_progress database::target_history_page_progress_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    const auto state=target_staging_;const auto actual=state?state->history_page:nullptr;
    require_target_history_header_ending_(connection,receiving);
    if(!actual||actual->owner.lock().get()!=&receiving||actual->schema.lock()!=receiving.require_header_schema(connection)||
       state->connection.lock().get()!=&connection||!actual->sealed(*this))throw db_error("target page actual durable Native ending unavailable");
    if(actual->raw_append) {
        require_target_raw_history_page_observation_(connection,receiving,*actual->raw_append);
        actual->raw_append->require_locator_joined(connection,receiving,state->end);
    }
    return actual->progress; // Copied observed progress, not a cut/ACK/import authority.
}
uint64_t database::target_history_page_append_before_bytes_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving,detail::ordinary_history_progress& before)const {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||
       state->history_page_active||state->retirement_requested||state->first_failure)
        throw db_error("target raw page prior actual shell/Conn ending unavailable");
    require_target_history_header_ending_(connection,receiving);
    const auto& header=receiving.require_header_original(connection);
    const auto& input=receiving.require_page_original(connection);
    const auto schema=receiving.require_header_schema(connection);
    const auto prior=state->history_page;const auto actual_header=state->history_header;
    if(!actual_header)throw db_error("target raw page actual prior header missing");
    detail::ordinary_history_progress progress;progress.complete=header.pages==0;
    if(prior) {
        if(prior->owner.lock().get()!=&receiving||prior->schema.lock()!=schema||!prior->sealed(*this)||
           prior->first_failure||prior->guard_destroy_entered||prior->guard_destroy_returned)
            throw db_error("target raw page prior Native/VM/leave/guard ending unavailable");
        progress=prior->progress;
    }else if(actual_header->guard_destroy_entered||actual_header->guard_destroy_returned)
        throw db_error("target raw first page header guard original already consumed");
    if(input.index!=progress.next_page||input.previous!=progress.previous||progress.complete||
       state->history_pages_issued!=progress.next_page)
        throw db_error("target raw page does not follow actual retained prior progress");
    const auto& stage=prior?prior->stage:actual_header->stage;
    const auto* bytes=stage.size()==18?std::get_if<std::int64_t>(&stage.at(15)):nullptr;
    const ordinary_history_limits limits;
    if(!bytes||*bytes<0||std::uint64_t(*bytes)>limits.durable_stage_bytes||
       std::uint64_t(*bytes)<header.header.size()||(!prior&&std::uint64_t(*bytes)!=header.header.size()))
        throw db_error("target raw page actual prior wire byte total differs");
    before=progress;return std::uint64_t(*bytes); // DATA copied from actual ended program; no new SQL role.
}
void database::require_target_raw_history_page_ending_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving,const detail::ordinary_target_history_page_append& append)const {
    const auto state=target_staging_;const auto actual=state?state->history_page:nullptr;
    require_target_history_header_ending_(connection,receiving);
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||state->history_page_active||state->first_failure||
       !state->history_header->raw_pages||!actual||actual->raw_append.get()!=&append||actual->owner.lock().get()!=&receiving||
       actual->schema.lock()!=receiving.require_header_schema(connection)||!actual->sealed(*this)||actual->first_failure)
        throw db_error("target raw locator lacks SAME actual SQL/VM/COMMIT/leave ending");
    const auto& input=receiving.require_page_original(connection);
    const auto span=append.require_durable_span(connection,receiving,input,state->end);
    const auto& page=actual->page;const auto& stage=actual->stage;
    if(page.size()!=10||stage.size()!=18||actual->raw_span!=span||page.at(0)!=stage.at(0)||
       page.at(1)!=column_value_t(std::int64_t(input.index))||page.at(2)!=column_value_t(input.previous)||page.at(3)!=column_value_t(input.full_digest)||
       page.at(4)!=column_value_t(std::int64_t(span.first))||page.at(5)!=column_value_t(std::int64_t(span.second))||
       page.at(9)!=column_value_t(detail::ordinary_history_storage_seal(detail::ordinary_target_raw_history_storage_recipe::seal_domain,page,9))||
       stage.at(14)!=column_value_t(std::int64_t(input.index+1))||stage.at(15)!=column_value_t(std::int64_t(span.first+span.second))||
       actual->progress.next_page!=input.index+1||actual->progress.previous!=input.full_digest||state->history_pages_issued!=input.index+1)
        throw db_error("target raw locator exact committed postimage/prefix differs");
}
void database::require_target_raw_history_page_observation_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving,const detail::ordinary_target_history_page_append& append)const {
    const auto state=target_staging_;const auto actual=state?state->history_page:nullptr;
    require_target_history_header_ending_(connection,receiving);
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||state->history_page_active||state->first_failure||
       !state->history_header->raw_pages||!actual||actual->raw_append.get()!=&append||actual->owner.lock().get()!=&receiving||
       actual->schema.lock()!=receiving.require_header_schema(connection)||!actual->sealed(*this)||actual->first_failure)
        throw db_error("target raw locator historical SAME SQL ending unavailable");
    const auto& page=actual->page;const auto& stage=actual->stage;
    const auto* index=page.size()==10?std::get_if<std::int64_t>(&page.at(1)):nullptr;
    const auto* previous=page.size()==10?std::get_if<std::string>(&page.at(2)):nullptr;
    const auto* digest=page.size()==10?std::get_if<std::string>(&page.at(3)):nullptr;
    if(!index||*index<0||std::uint64_t(*index)>=ordinary_history_limits{}.pages||!previous||!digest||stage.size()!=18)
        throw db_error("target raw locator historical retained shape differs");
    // Observe the genuine first raw+SQL join after input destruction. This is
    // no renewed decoded alias, active reader, SQL role or acknowledgement.
    const auto span=append.require_committed_span(connection,receiving,std::uint64_t(*index),*previous,*digest,state->end);
    if(actual->raw_span!=span||page.at(0)!=stage.at(0)||
       page.at(4)!=column_value_t(std::int64_t(span.first))||page.at(5)!=column_value_t(std::int64_t(span.second))||
       page.at(9)!=column_value_t(detail::ordinary_history_storage_seal(detail::ordinary_target_raw_history_storage_recipe::seal_domain,page,9))||
       stage.at(14)!=column_value_t(std::int64_t(*index+1))||stage.at(15)!=column_value_t(std::int64_t(span.first+span.second))||
       actual->progress.next_page!=std::uint64_t(*index)+1||actual->progress.previous!=*digest||state->history_pages_issued!=std::uint64_t(*index)+1)
        throw db_error("target raw locator historical committed prefix differs");
}
void database::stage_target_history_page_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving) {
    stage_target_history_page_program_(receiving,{}); // Existing legacy BLOB writer.
}
void database::stage_target_raw_history_page_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
    const std::shared_ptr<const detail::ordinary_target_history_page_append>& append) {
    if(!append)throw db_error("target raw page requires actual durable append original");
    stage_target_history_page_program_(receiving,append); // Private and unmounted.
}
void database::stage_target_history_page_program_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
    const std::shared_ptr<const detail::ordinary_target_history_page_append>& append) {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||!receiving||state->history_page_active||state->retirement_requested)
        throw db_error("target page actual original already active/sealed");
    const auto connection=state->require_original();const auto& header=receiving->require_header_original(*connection);
    const auto& input=receiving->require_page_original(*connection);const auto schema=receiving->require_header_schema(*connection);
    require_target_history_header_ending_(*connection,*receiving);
    const auto prior=state->history_page;
    detail::ordinary_history_progress progress;progress.complete=header.pages==0;
    picosha2::hash256_one_by_one content;
    const auto actual_header=state->history_header;
    if(!actual_header||actual_header->raw_pages!=bool(append))throw db_error("target page actual header/storage recipe differs");
    const auto span=append?append->require_durable_span(*connection,*receiving,input,state->end):std::pair<std::uint64_t,std::uint64_t>{};
    if(prior) {
        if(!prior->sealed(*this)||prior->first_failure||prior->guard_destroy_entered||prior->guard_destroy_returned)
            throw db_error("target prior page Native/VM/guard ending debt cannot be replaced");
        progress=prior->progress;content=prior->content;
    }
    if(input.index!=progress.next_page||input.previous!=progress.previous||progress.complete||state->history_pages_issued!=progress.next_page)
        throw db_error("target page does not follow genuine retained prior ending");
    using native=target_staging_state::history_page_native;
    std::shared_ptr<native> next;
    state->history_page_active=true;++state->history_pages_issued; // Distinct attempt; never reset/reissue after failure.
    try {
        const auto& before_stage=prior?prior->stage:actual_header->stage;const auto& before_meta=prior?prior->meta:actual_header->meta;
        next=std::make_shared<native>(*this,receiving,schema,header,input,prior,before_stage,before_meta,progress,content,append,span);
        state->history_page=next; // SAME program + prior Native original are retained before scope/registration/effects.
        connection->begin_database_history_page(*this,receiving,next->scope,state->end);next->require(*this);
        next->userdata=new native::guard_userdata(state,next,*this);
        auto& registration=next->reserve(native::kind::register_guard,native::key_first);next->require(*this);
        next->guard_registration_entered=true;registration.entered=true;
        next->first_guard_registration_rc=::lattice::detail::store_physical_create_function_v2(db_,"lattice_ordered_export_owned_v1",0,SQLITE_UTF8,
            next->userdata,&native::guard_userdata::invoke,nullptr,nullptr,&native::guard_userdata::destroy);
        next->guard_registration_returned=true;native::returned(registration,next->first_guard_registration_rc);
        if(next->first_guard_registration_rc!=SQLITE_OK)throw db_error("target page guard first Native replacement failed");next->require(*this);
        if(prior) {
            if(!prior->sealed(*this)||!prior->guard_destroy_entered||!prior->guard_destroy_returned)
                throw db_error("target prior page actual userdata destruction-return not joined");
        }else if(!actual_header->guard_destroy_entered||!actual_header->guard_destroy_returned)
            throw db_error("target prior header actual userdata destruction-return not joined");
        next->previous_destroy_joined=true;
        next->execute(*this,native::begin,next->begin_sql);++next->completed_operations;
        next->verify(*this,native::stage_before,next->stage_read_sql,next->stage_key,before_stage);++next->completed_operations;
        next->verify(*this,native::meta_before,next->meta_read_sql,next->stage_key,before_meta);++next->completed_operations;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_FIXTURE)
        if(next->terminal_page.page_index==1&&next->previous) {
            next->require(*this); // SAME actual page/owner/currentness; no new role or deadline.
            if(detail::ordinary_receiver_reclamation_fixture::history_page2_failure_requested(*receiving))
                throw db_error("injected receiving history page2 pre-predecessor-release failure");
        }
#endif
        // Prior static bindings and actual guard/callback/VM ending are joined; release the old program only now.
        next->previous.reset();
        for(std::size_t at=0;at<next->keys.size();++at) {
            next->execute(*this,native::key_first+at,next->key_insert_sql,next->keys[at]);next->changes(*this,native::key_first+at);++next->completed_operations;
        }
        next->execute(*this,native::page_insert,next->page_insert_sql,next->page);next->changes(*this,native::page_insert);++next->completed_operations;
        next->verify(*this,native::page_read,next->page_read_sql,next->page_key,next->page);++next->completed_operations;
        for(std::size_t at=0;at<next->keys.size();++at) {
            next->verify(*this,native::key_verify_first+at,next->key_read_sql,next->key_queries[at],next->keys[at]);++next->completed_operations;
        }
        next->execute(*this,native::stage_update,next->stage_update_sql,next->stage_update_values);next->changes(*this,native::stage_update);++next->completed_operations;
        next->execute(*this,native::meta_update,next->meta_update_sql,next->meta_update_values);next->changes(*this,native::meta_update);++next->completed_operations;
        next->verify(*this,native::stage_after,next->stage_read_sql,next->stage_key,next->stage);++next->completed_operations;
        next->verify(*this,native::meta_after,next->meta_read_sql,next->stage_key,next->meta);++next->completed_operations;
        next->verify(*this,native::counts,next->counts_sql,next->count_key,next->count_values);++next->completed_operations;
        next->execute(*this,native::commit,next->commit_sql);
        if(!target_history_page_native_ending_())throw db_error("target page actual Native ending did not join");
        next->left=next->scope->leave(*this);if(!next->left)throw db_error("target page original scope leave refused");
        state->require_original();receiving->require_page_original(*connection);next->completed=true;state->history_page_active=false;
    }catch(...) {
        if(next&&!next->first_failure)next->first_failure=std::current_exception();
        if(!state->first_failure)state->first_failure=std::current_exception();
        if(next)next->settle_failed(*this); // Construction refusal must not rewrite the prior completed page original.
        retire_target_staging_();throw;
    }
}
} // namespace lattice
