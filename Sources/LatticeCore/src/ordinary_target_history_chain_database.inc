// SAME request-owned durable pages, one actual SQLite read phase at a time.
// Full-chain verification is a private producer for later typed model import;
// it does not issue accepted-cut/ACK/source/current/publication authority.
#include "ordinary_history_record_key_recipe.hpp"
#include "ordinary_target_history_spool.hpp"
#include "ordinary_target_raw_history_storage_recipe.hpp"
namespace lattice {
struct database::target_staging_state::history_chain_native {
    terminal_native_outer_tail terminal_outer; // First: follows ALL actual outer member destruction.
    using arguments=std::vector<column_value_t>;
    struct phase;
    database* const shell;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const std::weak_ptr<detail::ordinary_group_target_schema_original> schema;
    const bool raw_storage;
    const arguments final_stage,final_meta; // Actual previous Native postimages; corroboration only.
    detail::ordinary_history_progress checked;
    picosha2::hash256_one_by_one content;
    std::shared_ptr<phase> active;
    std::uint64_t verified_phases=0,bytes=0;
    bool completed=false;
    std::exception_ptr first_failure;
    history_chain_native(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving)
        :terminal_outer(actual,receiving,this,0),shell(&actual),owner(receiving),schema(receiving->require_header_schema(*actual.target_staging_->connection.lock())),
         raw_storage(actual.target_history_raw_storage_(*actual.target_staging_->connection.lock(),*receiving)),
         final_stage(actual.target_staging_->history_page?actual.target_staging_->history_page->stage:actual.target_staging_->history_header->stage),
         final_meta(actual.target_staging_->history_page?actual.target_staging_->history_page->meta:actual.target_staging_->history_header->meta) {
        const auto connection=actual.target_staging_->connection.lock();
        const auto& header=receiving->require_header_original(*connection);bytes=header.header.size();
        terminal_outer.constructor_returned(); // Actual final constructor operation.
    }
};
struct database::target_staging_state::history_chain_native::phase {
    terminal_native_phase_tail terminal_payload; // First: follows every actual phase member destruction.
    // Declared before payload: failed/unreturned decoder work retains the SAME slot
    // through actual phase member destruction. Successful decoder retirement
    // releases only this phase reference after real destruction/Native/current
    // ending; separately charged Native observations and receipts stay held.
    std::shared_ptr<detail::ordinary_resource_reservation<true>> decoder_charge;
    std::shared_ptr<detail::ordinary_target_history_page_reader> raw_reader;
    using arguments=history_chain_native::arguments;
    database* const shell;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const std::weak_ptr<detail::ordinary_group_target_schema_original> schema;
    const std::uint64_t page_index,raw_before_bytes;
    const bool raw_storage;
    bool raw_metadata_checked=false;
    std::unique_ptr<detail::ordinary_target_staging_connection::database_scope> scope;
    static constexpr std::size_t statements_limit=8,normal_calls=4096,total_calls=4352;
    static constexpr std::size_t begin=0,stage_read=1,meta_read=2,page_read=3,key_read=4,counts=5,commit=6,rollback=7;
    struct statement {
        std::string sql;arguments bindings;std::vector<std::string> keys;
        std::vector<database::row_t> rows; // Held before typed copying/verification/diagnostics.
        sqlite3_stmt* held=nullptr;sqlite3_stmt* prepared_identity=nullptr;sqlite3_stmt* finalized_identity=nullptr;
        bool prepare_entered=false,prepare_returned=false,readonly_returned=false,terminal_returned=false;
        bool finalize_entered=false,finalize_returned=false,destruction_known=false,first_step_returned=false;
        int prepare_rc=SQLITE_MISUSE,readonly_value=0,first_step_rc=SQLITE_MISUSE,terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
    };
    enum class kind {prepare,readonly,parameters,bind_text,bind_integer,columns,name,step,type,integer,real,bytes,text,blob,finalize,autocommit,txn_state};
    struct native_call {kind type=kind::prepare;std::size_t statement=0,column=0;bool entered=false,returned=false;
        std::int64_t integer=SQLITE_MISUSE;double real=0;const void* pointer=nullptr;};
    std::array<statement,statements_limit> statements{};std::array<native_call,total_calls> calls{};
    struct arena {
        static constexpr std::size_t maximum_bytes=ordinary_history_limits::stage_scratch_bytes;
        std::size_t bytes=0,cells=0;
        void add(std::size_t n){if(n>maximum_bytes-bytes)throw db_error("GROUP cold-chain combined staging capacity refused");bytes+=n;}
        void cell(){if(cells==1024)throw db_error("GROUP cold-chain fixed observed cell bound refused");++cells;}
    } storage;
    arena* const program=&storage;
    std::size_t call_count=0;
    bool decoder_retirement_entered=false,decoder_retirement_returned=false;
    bool ending=false,left=false,data_checked=false,snapshot_observed=false;
    int snapshot_state=SQLITE_TXN_NONE;
    bool ending_autocommit_returned=false;int ending_autocommit=0;
    bool decode_entered=false,decode_returned=false,decode_exception_returned=false;
    std::optional<detail::ordinary_history_decoded_page> decoded;
    detail::ordinary_history_progress next;
    picosha2::hash256_one_by_one next_content;
    std::uint64_t next_bytes=0;
    std::exception_ptr first_failure;
    phase(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
        const history_chain_native& original):terminal_payload(actual,receiving,this,0),decoder_charge(receiving->require_decoder_charge(*actual.target_staging_->connection.lock())),
        shell(&actual),owner(receiving),schema(original.schema),page_index(original.checked.next_page),raw_before_bytes(original.bytes),raw_storage(original.raw_storage),
        next(original.checked),next_content(original.content),next_bytes(original.bytes) {
        storage.add(sizeof(*this));
        const auto state=actual.target_staging_;
        if(!state||!state->history_projection)throw db_error("GROUP cold-chain original projection capacity absent");
        storage.add(state->history_projection->logical_bytes); // Contains actual header/latest-page SQL originals once.
        for(const auto* args:{&original.final_stage,&original.final_meta}) {
            storage.add(args->capacity()*sizeof(column_value_t));
            for(const auto& value:*args) {
                if(const auto* text=std::get_if<std::string>(&value))storage.add(text->capacity());
                if(const auto* blob=std::get_if<std::vector<std::uint8_t>>(&value))storage.add(blob->capacity());
            }
        }
        storage.add(original.checked.previous.capacity()+next.previous.capacity());
        // Decoder owns the existing distinct 56 MiB row reservation on the
        // actual receiving original. The SQL arena never claims JSON/RSS bounds.
        terminal_payload.constructor_returned(); // Actual last constructor operation.
    }
    void require(database& actual,bool unwind=false)const {
        const auto state=actual.target_staging_;const auto original=state?state->history_chain:nullptr;
        if(!state||!state->current_creator()||!original||original->shell!=&actual||original->active.get()!=this||
           shell!=&actual||!scope||left||!actual.db_||state->retirement_requested||
           actual.retirement_->owning_wrapper_moved()||actual.retirement_->raw_handle_escaped())
            throw db_error("GROUP cold-chain SAME actual Native phase unavailable");
        scope->require_history_chain_role(actual);
        if(unwind){if(!ending)throw db_error("GROUP cold-chain original ending unavailable");scope->require_current(actual);return;}
        if(ending||first_failure||original->first_failure||original->completed)
            throw db_error("GROUP cold-chain original sealed");
        const auto receiving=owner.lock();const auto connection=state->connection.lock();
        if(!receiving||!connection||original->owner.lock()!=receiving||schema.lock()!=receiving->require_header_schema(*connection))
            throw db_error("GROUP cold-chain actual receiving/schema original unavailable");
        actual.require_target_history_projection_ending_(*connection,*receiving);scope->require_schema_work(actual);
    }
    void add_stage(std::size_t bytes){storage.add(bytes);}
    native_call& reserve(kind type,std::size_t index,bool unwind=false,std::size_t column=0) {
        if(index>=statements_limit||call_count>=(unwind?total_calls:normal_calls))
            throw db_error("GROUP cold-chain fixed Native ledger refused");
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
            throw db_error("GROUP cold-chain generated statement shape refused");
        held.sql=sql;add_stage(held.sql.capacity());held.bindings=bindings;
        add_stage(held.bindings.capacity()*sizeof(column_value_t));
        for(const auto& value:held.bindings) {
            if(const auto* text=std::get_if<std::string>(&value)) {
                if(text->size()>256)throw db_error("GROUP cold-chain normal metadata binding refused");add_stage(text->capacity());
            }else if(!std::holds_alternative<std::int64_t>(value))throw db_error("GROUP cold-chain generated binding storage refused");
        }
    }
    void prepare(database& actual,std::size_t index,bool unwind=false) {
        auto& statement=statements[index];const auto& sql=statement.sql;
        if(statement.prepare_entered||sql.empty())throw db_error("GROUP cold-chain statement reissue refused");
        require(actual,unwind);auto& call=reserve(kind::prepare,index,unwind);require(actual,unwind);
        const char* tail=nullptr;statement.prepare_entered=true;call.entered=true;
        statement.prepare_rc=::lattice::detail::store_physical_prepare_v2(actual.db_,sql.data(),static_cast<int>(sql.size()),&statement.held,&tail);
        statement.prepared_identity=statement.held;statement.prepare_returned=true;returned(call,statement.prepare_rc);
        require(actual,unwind);
        if(statement.prepare_rc!=SQLITE_OK||!statement.held||tail!=sql.data()+sql.size())throw db_error("GROUP cold-chain actual Native preparation failed/changed");
        const int readonly=observe(actual,kind::readonly,index,[&]{return sqlite3_stmt_readonly(statement.held);},unwind);
        statement.readonly_value=readonly;statement.readonly_returned=true;
        if(!readonly)throw db_error("GROUP cold-chain generated VM is not actually read-only");
        const int parameters=observe(actual,kind::parameters,index,[&]{return sqlite3_bind_parameter_count(statement.held);},unwind);
        if(parameters!=static_cast<int>(statement.bindings.size()))throw db_error("GROUP cold-chain generated Native parameter count changed");
        for(std::size_t n=0;n<statement.bindings.size();++n) {
            int rc=SQLITE_MISUSE;
            if(const auto* value=std::get_if<std::string>(&statement.bindings[n]))
                rc=observe(actual,kind::bind_text,index,[&]{return sqlite3_bind_text(statement.held,static_cast<int>(n+1),value->data(),static_cast<int>(value->size()),SQLITE_STATIC);},unwind,n);
            else if(const auto* value=std::get_if<std::int64_t>(&statement.bindings[n]))
                rc=observe(actual,kind::bind_integer,index,[&]{return sqlite3_bind_int64(statement.held,static_cast<int>(n+1),*value);},unwind,n);
            if(rc!=SQLITE_OK)throw db_error("GROUP cold-chain generated binding failed");
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
        if(statement.finalize_rc!=SQLITE_OK||!statement.destruction_known)throw db_error("GROUP cold-chain actual VM ending failed/unknown");
    }
    void control(database& actual,std::size_t index,const char* fixed,bool unwind=false) {
        retain_statement(index,fixed,{});prepare(actual,index,unwind);
        if(step(actual,index,unwind)!=SQLITE_DONE)throw db_error("GROUP cold-chain actual fixed Native ending failed");finish(actual,index,unwind);
    }
    template<class T> void metadata_reserve(std::vector<T>& values,std::size_t capacity) {
        if(capacity<=values.capacity())return;
        if(capacity>program->maximum_bytes/sizeof(T))throw db_error("GROUP cold-chain metadata vector capacity refused");
        const auto old=values.capacity();program->add((capacity-old)*sizeof(T));values.reserve(capacity);
        if(values.capacity()>capacity)program->add((values.capacity()-capacity)*sizeof(T));
    }
    const std::vector<database::row_t>& read(database& actual,std::size_t index,const std::string& sql,
        const std::vector<column_value_t>& bindings,std::size_t limit) {
        if(!limit||limit>64||(!snapshot_observed&&index!=stage_read))throw db_error("GROUP cold-chain bounded retained snapshot unavailable");
        require(actual);retain_statement(index,sql,bindings);prepare(actual,index);auto& statement=statements[index];
        const int columns=observe(actual,kind::columns,index,[&]{return sqlite3_column_count(statement.held);});
        if(columns<1||columns>18)throw db_error("GROUP cold-chain normal metadata column limit refused");
        metadata_reserve(statement.keys,static_cast<std::size_t>(columns));
        for(int c=0;c<columns;++c) {
            const char* name=observe(actual,kind::name,index,[&]{return sqlite3_column_name(statement.held,c);},false,c);
            if(!name)throw db_error("GROUP cold-chain actual column name unavailable");
            std::size_t bytes=0;while(bytes<=256&&name[bytes])++bytes;
            if(!bytes||bytes>256)throw db_error("GROUP cold-chain actual column name bound refused");
            program->add(bytes);statement.keys.emplace_back(name,bytes);
            if(statement.keys.back().capacity()>bytes)program->add(statement.keys.back().capacity()-bytes);
            if(std::find(statement.keys.begin(),statement.keys.end()-1,statement.keys.back())!=statement.keys.end()-1)
                throw db_error("GROUP cold-chain duplicate column name refused");
        }
        for(;;) {
            const int rc=step(actual,index);if(rc==SQLITE_DONE)break;if(rc!=SQLITE_ROW)throw db_error("GROUP cold-chain actual metadata step failed");
            if(statement.rows.size()==limit)throw db_error("GROUP cold-chain metadata row limit refused before copy");
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
                    if(!std::isfinite(number))throw db_error("GROUP cold-chain nonfinite metadata observation refused");value=number;
                }else if(type==SQLITE_TEXT||type==SQLITE_BLOB) {
                    const int bytes=observe(actual,kind::bytes,index,[&]{return sqlite3_column_bytes(statement.held,c);},false,c);
                    if(bytes<0)throw db_error("GROUP cold-chain negative metadata bytes refused");program->add(static_cast<std::size_t>(bytes));
                    if(type==SQLITE_TEXT) {
                        const auto* data=observe(actual,kind::text,index,[&]{return sqlite3_column_text(statement.held,c);},false,c);
                        if(!data&&bytes)throw db_error("GROUP cold-chain actual text observation unavailable");
                        value=bytes?std::string(reinterpret_cast<const char*>(data),static_cast<std::size_t>(bytes)):std::string{};
                        const auto capacity=std::get<std::string>(value).capacity();if(capacity>static_cast<std::size_t>(bytes))program->add(capacity-static_cast<std::size_t>(bytes));
                    }else {
                        const auto* data=static_cast<const std::uint8_t*>(observe(actual,kind::blob,index,[&]{return sqlite3_column_blob(statement.held,c);},false,c));
                        if(!data&&bytes)throw db_error("GROUP cold-chain actual blob observation unavailable");
                        value=bytes?std::vector<std::uint8_t>(data,data+bytes):std::vector<std::uint8_t>{};
                        const auto capacity=std::get<std::vector<std::uint8_t>>(value).capacity();if(capacity>static_cast<std::size_t>(bytes))program->add(capacity-static_cast<std::size_t>(bytes));
                    }
                }else if(type!=SQLITE_NULL)throw db_error("GROUP cold-chain actual metadata storage refused");
                row.emplace(key,std::move(value));require(actual);
            }
        }
        finish(actual,index);
        // No copied DTO: the private verifier consumes these SAME retained
        // first observations while the original read phase remains current.
        require(actual);return statement.rows;
    }

    void enter_ending(database& actual){if(!ending){scope->begin_schema_ending(actual);ending=true;}}
    bool native_ending_known()const noexcept {
        if(first_failure||ending||!data_checked||!snapshot_observed||snapshot_state!=SQLITE_TXN_READ)return false;
        if((raw_reader&&!raw_reader->io_ending_known())||(raw_storage&&decode_entered&&!raw_reader))return false;
        for(std::size_t n=0;n<call_count;++n)if(calls[n].entered&&!calls[n].returned)return false;
        for(const auto& held:statements)if(held.prepare_entered) {
            if(!held.prepare_returned||held.prepare_rc!=SQLITE_OK||!held.readonly_returned||!held.readonly_value||
               !held.terminal_returned||held.terminal_rc!=SQLITE_DONE||!held.finalize_returned||held.finalize_rc!=SQLITE_OK||
               !held.destruction_known||held.held)return false;
        }
        return statements[begin].terminal_returned&&statements[begin].terminal_rc==SQLITE_DONE&&
               statements[commit].terminal_returned&&statements[commit].terminal_rc==SQLITE_DONE;
    }
    bool cleanup_known()const noexcept {
        if((decode_entered&&!decode_returned&&!decode_exception_returned)||(raw_reader&&!raw_reader->cleanup_known()))return false;
        for(std::size_t n=0;n<call_count;++n)if(calls[n].entered&&!calls[n].returned)return false;
        for(const auto& held:statements)if(held.prepare_entered) {
            if(!held.prepare_returned)return false;
            if(held.prepared_identity&&(!held.finalize_returned||!held.destruction_known||held.held))return false;
        }
        const auto& start=statements[begin];const auto& done=statements[commit];const auto& undo=statements[rollback];
        if(start.terminal_returned&&start.terminal_rc==SQLITE_DONE&&!(done.terminal_returned&&done.terminal_rc==SQLITE_DONE)&&
           !(undo.terminal_returned&&undo.terminal_rc==SQLITE_DONE)&&!(ending_autocommit_returned&&ending_autocommit!=0))return false;
        return true; // Ending facts only; first failure is never cleared.
    }
    void settle_failed(database& actual)noexcept {
        try {
            enter_ending(actual);
            for(std::size_t n=0;n<statements_limit;++n)if(statements[n].held&&!statements[n].finalize_entered)finish(actual,n,true);
            if(statements[begin].terminal_returned&&statements[begin].terminal_rc==SQLITE_DONE&&
               !(statements[commit].terminal_returned&&statements[commit].terminal_rc==SQLITE_DONE)) {
                require(actual,true);auto& call=reserve(kind::autocommit,rollback,true);require(actual,true);call.entered=true;
                ending_autocommit=sqlite3_get_autocommit(actual.db_);ending_autocommit_returned=true;returned(call,ending_autocommit);
                require(actual,true);if(!ending_autocommit)control(actual,rollback,"ROLLBACK",true);
            }
            left=scope->leave(actual);
        }catch(...){}
    }
    static const column_value_t& cell(const database::row_t& row,const char* name) {
        const auto at=row.find(name);if(at==row.end())throw db_error("GROUP cold-chain fixed observation missing");return at->second;
    }
    static void exact(const database::row_t& row,const arguments& wanted,std::initializer_list<const char*> names) {
        if(row.size()!=wanted.size()||names.size()!=wanted.size())throw db_error("GROUP cold-chain fixed observation shape differs");
        std::size_t at=0;for(const auto* name:names)if(cell(row,name)!=wanted[at++])throw db_error("GROUP cold-chain actual retained postimage differs");
    }
    void verify(database& actual) {
        const auto original=actual.target_staging_->history_chain;const auto receiving=owner.lock();
        const auto connection=actual.target_staging_->connection.lock();const auto& header=receiving->require_header_original(*connection);
        control(actual,begin,"BEGIN DEFERRED");
        const auto& stage=read(actual,stage_read,"SELECT stage,role,source,epoch,cut,scope,schema,device,inode,state,header,rows,known,unknown,pages,bytes,content,seal FROM main._lattice_ordered_history_stage_v1 WHERE stage=?",{header.stage},1);
        if(stage.size()!=1)throw db_error("GROUP cold-chain actual stage missing/duplicate");
        exact(stage[0],original->final_stage,{"stage","role","source","epoch","cut","scope","schema","device","inode","state","header","rows","known","unknown","pages","bytes","content","seal"});
        snapshot_state=observe(actual,kind::txn_state,stage_read,[&]{return sqlite3_txn_state(actual.db_,"main");});snapshot_observed=true;
        if(snapshot_state!=SQLITE_TXN_READ)throw db_error("GROUP cold-chain actual MAIN snapshot absent");
        const auto& meta=read(actual,meta_read,"SELECT stage,channel,records_limit,pages_limit,page_limit,records_per_page,bytes_limit,last_table,seal FROM main._lattice_ordered_history_receive_v1 WHERE stage=?",{header.stage},1);
        if(meta.size()!=1)throw db_error("GROUP cold-chain actual metadata missing/duplicate");
        exact(meta[0],original->final_meta,{"stage","channel","records_limit","pages_limit","page_limit","records_per_page","bytes_limit","last_table","seal"});
        if(header.pages) {
            const auto& pages=read(actual,page_read,(raw_storage?"SELECT stage,page,previous,digest,raw_offset,raw_length,rows,known,unknown,seal FROM main._lattice_ordered_history_raw_page_v1 WHERE stage=? AND page=?":"SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?"),{header.stage,std::int64_t(page_index)},1);
            if(pages.size()!=1||pages[0].size()!=(raw_storage?10u:9u))throw db_error("GROUP cold-chain actual durable page missing/duplicate");
            const auto& row=pages[0];const ordinary_history_limits limits;
            const std::vector<std::uint8_t>* raw=nullptr;
            if(cell(row,"stage")!=column_value_t(header.stage)||cell(row,"page")!=column_value_t(std::int64_t(page_index)))
                throw db_error("GROUP cold-chain actual durable page identity differs");
            if(raw_storage) {
                const auto* offset=std::get_if<std::int64_t>(&cell(row,"raw_offset"));
                const auto* length=std::get_if<std::int64_t>(&cell(row,"raw_length"));
                const auto* total=std::get_if<std::int64_t>(&original->final_stage.at(15));
                if(!offset||!length||!total||*offset<0||*length<=0||*total<0||
                   std::uint64_t(*offset)!=raw_before_bytes||raw_before_bytes!=next_bytes||
                   std::uint64_t(*length)>limits.page_bytes||std::uint64_t(*total)>limits.durable_stage_bytes||
                   std::uint64_t(*offset)>std::uint64_t(*total)||std::uint64_t(*length)>std::uint64_t(*total)-std::uint64_t(*offset))
                    throw db_error("GROUP cold-chain actual raw page contiguous span differs");
            }else {
                raw=std::get_if<std::vector<std::uint8_t>>(&cell(row,"payload"));
                if(!raw||raw->empty()||raw->size()>limits.page_bytes)
                    throw db_error("GROUP cold-chain actual BLOB page shape differs");
            }
            arguments seal_values;seal_values.reserve(raw_storage?9u:8u);storage.add(seal_values.capacity()*sizeof(column_value_t));
            const auto seal_cell=[&](const char* name) {
                const auto& value=cell(row,name);
                if(const auto* text=std::get_if<std::string>(&value))storage.add(text->size());
                if(const auto* blob=std::get_if<std::vector<std::uint8_t>>(&value))storage.add(blob->size());
                seal_values.push_back(value);
                if(const auto* text=std::get_if<std::string>(&seal_values.back());text&&text->capacity()>text->size())storage.add(text->capacity()-text->size());
                if(const auto* blob=std::get_if<std::vector<std::uint8_t>>(&seal_values.back());blob&&blob->capacity()>blob->size())storage.add(blob->capacity()-blob->size());
            };
            if(raw_storage)for(const auto* name:{"stage","page","previous","digest","raw_offset","raw_length","rows","known","unknown"})seal_cell(name);
            else for(const auto* name:{"stage","page","previous","digest","payload","rows","known","unknown"})seal_cell(name);
            storage.add(128); // Fixed hexadecimal seal result/comparison before allocation.
            if(cell(row,"seal")!=column_value_t(detail::ordinary_history_storage_seal(raw_storage?detail::ordinary_target_raw_history_storage_recipe::seal_domain:"ordinary-paged-page-v1",seal_values,raw_storage?9u:8u)))
                throw db_error("GROUP cold-chain actual page storage seal differs");
            if(raw_storage) {
                raw_metadata_checked=true; // Exact ten-cell retained row and nine-cell seal returned before issue.
                const auto length=std::uint64_t(std::get<std::int64_t>(cell(row,"raw_length")));
                storage.add(detail::ordinary_target_history_page_reader::retained_bytes(length));
                connection->read_history_page(actual,receiving,*scope,raw_reader,actual.target_staging_->end);
                raw=&raw_reader->bytes(actual,scope.get(),actual.target_staging_->end);
            }
            decode_entered=true;
            try {
                decoded=detail::decode_ordinary_history_page(std::string_view(reinterpret_cast<const char*>(raw->data()),raw->size()),header,next,limits);
                decode_returned=true;
            }catch(...) {
                // This SAME synchronous decoder/output call has genuinely
                // unwound into its immediate caller. Preserve its exception;
                // this ending permits only actual failed-target cleanup.
                decode_exception_returned=true;throw;
            }
            require(actual);
            if(cell(row,"previous")!=column_value_t(decoded->previous)||cell(row,"digest")!=column_value_t(decoded->full_digest))
                throw db_error("GROUP cold-chain exact page digest/previous differs");
            const auto before=next;
            const auto& keys=read(actual,key_read,"SELECT stage,role,model,gid,page,ordinal,seal FROM main._lattice_ordered_history_receive_key_v1 WHERE stage=? AND page=? ORDER BY ordinal",{header.stage,std::int64_t(page_index)},64);
            if(keys.size()!=decoded->records.size())throw db_error("GROUP cold-chain exact page key count differs");
            // One ephemeral exact key programme at a time; charge its fixed
            // decoder-proven identity envelope before its normal recipe allocates.
            storage.add(14*sizeof(column_value_t)+36+256+4096+64+32);
            for(std::size_t n=0;n<keys.size();++n) {
                auto wanted=detail::ordinary_history_record_key_recipe::key(header,*decoded,n,next);
                if(wanted.capacity()>14)throw db_error("GROUP cold-chain exact key capacity refused");
                exact(keys[n],wanted,{"stage","role","model","gid","page","ordinal","seal"});require(actual);
            }
            if(cell(row,"rows")!=column_value_t(std::int64_t(next.rows-before.rows))||
               cell(row,"known")!=column_value_t(std::int64_t(next.known-before.known))||
               cell(row,"unknown")!=column_value_t(std::int64_t(next.unknown-before.unknown)))
                throw db_error("GROUP cold-chain actual per-page counters differ");
            ++next.next_page;next.previous=decoded->full_digest;
            if(raw->size()>limits.durable_stage_bytes-next_bytes)throw db_error("GROUP cold-chain actual byte total bound refused");
            next_bytes+=raw->size();
            for(std::size_t at=0;at<raw->size();) {
                require(actual);const auto count=std::min<std::size_t>(65536,raw->size()-at);
                next_content.process(raw->begin()+at,raw->begin()+at+count);at+=count;
            }
        }
        next.complete=next.next_page==header.pages;
        if(next.complete) {
            auto finished=next_content;finished.finish();
            if(next.rows!=header.rows||next.known!=header.known||next.unknown!=header.unknown||
               next.last_table!=std::get<std::int64_t>(original->final_meta.at(7))||
               next_bytes!=static_cast<std::uint64_t>(std::get<std::int64_t>(original->final_stage.at(15)))||
               picosha2::get_hash_hex_string(finished)!=header.content_digest)
                throw db_error("GROUP cold-chain complete actual content/counters differ");
            const auto& totals=read(actual,counts,(raw_storage?"SELECT (SELECT COUNT(*) FROM main._lattice_ordered_history_raw_page_v1) AS pages,(SELECT COUNT(*) FROM main._lattice_ordered_history_receive_key_v1) AS records":"SELECT (SELECT COUNT(*) FROM main._lattice_ordered_history_page_v1) AS pages,(SELECT COUNT(*) FROM main._lattice_ordered_history_receive_key_v1) AS records"),{},1);
            if(totals.size()!=1)throw db_error("GROUP cold-chain actual total inventory missing");
            exact(totals[0],{std::int64_t(header.pages),std::int64_t(header.rows+header.known+header.unknown)},{"pages","records"});
        }
        data_checked=true;control(actual,commit,"COMMIT");
        if(!native_ending_known())throw db_error("GROUP cold-chain actual Native read ending missing");
        left=scope->leave(actual);if(!left)throw db_error("GROUP cold-chain actual read phase leave refused");
    }
};
bool database::target_history_chain_native_ending_()const noexcept {
    const auto state=target_staging_;const auto original=state?state->history_chain:nullptr;
    return state&&state->current_creator()&&original&&original->shell==this&&original->active&&original->active->native_ending_known();
}
bool database::target_history_chain_cleanup_ending_()const noexcept {
    const auto state=target_staging_;if(!state||!state->current_creator())return false;
    return !state->history_chain||!state->history_chain->active||state->history_chain->active->cleanup_known();
}
void database::require_target_history_chain_issued_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    require_target_history_projection_ending_(connection,receiving);const auto original=target_staging_->history_chain;
    if(!original||original->shell!=this||original->owner.lock().get()!=&receiving||original->first_failure||original->completed||
       !original->active||original->active->scope||original->active->page_index!=original->checked.next_page)
        throw db_error("GROUP cold-chain next phase lacks SAME retained original");
}
void database::require_target_history_chain_ending_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const {
    require_target_history_projection_ending_(connection,receiving);const auto original=target_staging_->history_chain;
    const auto& header=receiving.require_header_original(connection);
    if(!original||original->shell!=this||original->owner.lock().get()!=&receiving||original->first_failure||!original->completed||
       !original->checked.complete||original->checked.next_page!=header.pages||!original->active||
       !original->active->left||!original->active->native_ending_known()||!original->active->scope||!original->active->scope->leave(*this)||
       original->verified_phases!=(header.pages?header.pages:1))
        throw db_error("GROUP whole durable-chain original read ending unavailable");
}
void database::verify_target_history_chain_(const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving) {
    const auto state=target_staging_;if(!state||!state->current_creator()||!receiving||state->history_chain_attempted||state->history_chain)
        throw db_error("GROUP durable chain one-shot actual original unavailable");
    const auto connection=state->require_original();require_target_history_projection_ending_(*connection,*receiving);
    state->history_chain_attempted=true;
    try {
        state->history_chain=std::make_shared<target_staging_state::history_chain_native>(*this,receiving);
        const auto original=state->history_chain; // SAME full-chain owner retained before postchecks/effects.
        const auto& header=receiving->require_header_original(*connection);
        do {
            if(original->active)throw db_error("GROUP cold-chain previous phase retained unknown debt");
            original->active=std::make_shared<target_staging_state::history_chain_native::phase>(*this,receiving,*original);
            const auto phase=original->active;
            connection->begin_database_history_chain(*this,receiving,phase->scope,state->end);
            phase->verify(*this);
            if(!phase->left||!phase->native_ending_known()||!phase->scope->leave(*this))
                throw db_error("GROUP cold-chain exact previous phase ending absent");
            // Retire only a fully ended actual phase. The next phase cannot be
            // allocated after uncertainty; failed rows/VM identity stay held.
            original->checked=phase->next;original->content=phase->next_content;original->bytes=phase->next_bytes;
            ++original->verified_phases;
            state->require_original();receiving->require_header_original(*connection);
            if(!original->checked.complete) {
                if(phase->decoder_retirement_entered||phase->decoder_retirement_returned||!phase->decoder_charge||
                   phase->decoder_charge!=receiving->require_decoder_charge(*connection))
                    throw db_error("GROUP cold-chain decoder retirement original already claimed or absent");
                phase->decoder_retirement_entered=true;phase->decoded.reset();phase->decoder_retirement_returned=true;
                state->require_original();receiving->require_header_original(*connection);
                if(!phase->left||!phase->native_ending_known()||!phase->scope->leave(*this)||
                   !phase->decoder_retirement_entered||!phase->decoder_retirement_returned||phase->decoded||
                   phase->decoder_charge!=receiving->require_decoder_charge(*connection))
                    throw db_error("GROUP cold-chain decoder actual destruction ending unavailable");
                if(phase->raw_reader) {
                    phase->raw_reader->end_input(*connection,*receiving,*this,phase->scope.get(),state->end);
                    if(!phase->raw_reader->input_ending_known())throw db_error("GROUP cold-chain raw byte borrower ending unavailable");
                }
                phase->decoder_charge.reset();
                original->active.reset();
            } // Final actual decoder/reader ending remains held for importer handoff.
        }while(original->checked.next_page<header.pages);
        original->completed=true;require_target_history_chain_ending_(*connection,*receiving);
        // No accepted-row BLOBs or model effects. Actual same-chain server rows
        // will feed a later bounded writer that retains uncertain old originals.
    }catch(...) {
        const auto original=state->history_chain;
        if(original&&!original->first_failure)original->first_failure=std::current_exception();
        if(original&&original->active&&!original->active->first_failure)original->active->first_failure=std::current_exception();
        if(!state->first_failure)state->first_failure=std::current_exception();
        if(original&&original->active&&original->active->scope)original->active->settle_failed(*this);
        retire_target_staging_();throw;
    }
}
detail::ordinary_target_history_read_span database::target_history_chain_raw_span_(
    const detail::ordinary_target_staging_connection& connection,const detail::ordinary_group_target_receiving_owner& receiving,
    const void* scope,const void* output,const detail::ordinary_target_history_page_reader* reader)const {
    const auto state=target_staging_;const auto original=state?state->history_chain:nullptr;
    const auto phase=original?original->active:nullptr;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||state->retirement_requested||
       !original||original->shell!=this||original->owner.lock().get()!=&receiving||original->first_failure||original->completed||
       !phase||phase->shell!=this||phase->owner.lock().get()!=&receiving||phase->scope.get()!=scope||
       output!=&phase->raw_reader||(reader?phase->raw_reader.get()!=reader:bool(phase->raw_reader))||
       !phase->raw_storage||!phase->raw_metadata_checked||phase->first_failure||phase->left||phase->ending||
       !phase->snapshot_observed||phase->snapshot_state!=SQLITE_TXN_READ||
       phase->page_index!=original->checked.next_page||phase->raw_before_bytes!=original->bytes)
        throw db_error("GROUP history_chain raw read lacks SAME checked active phase/row/output");
    const auto& header=receiving.require_header_original(connection);
    const auto& statement=phase->statements[phase->page_read];
    if(!statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||!statement.terminal_returned||statement.terminal_rc!=SQLITE_DONE||
       !statement.finalize_returned||statement.finalize_rc!=SQLITE_OK||!statement.destruction_known||statement.held||
       statement.rows.size()!=1||statement.rows[0].size()!=10||original->final_stage.size()!=18||
       phase->page_index>=header.pages)
        throw db_error("GROUP history_chain raw read actual retained Native row/VM ending absent");
    const auto& row=statement.rows[0];
    const auto* offset=std::get_if<std::int64_t>(&phase->cell(row,"raw_offset"));
    const auto* length=std::get_if<std::int64_t>(&phase->cell(row,"raw_length"));
    const auto* total=std::get_if<std::int64_t>(&original->final_stage[15]);
    const ordinary_history_limits limits;
    if(!offset||!length||!total||*offset<0||*length<=0||*total<0||
       phase->cell(row,"stage")!=column_value_t(header.stage)||phase->cell(row,"page")!=column_value_t(std::int64_t(phase->page_index))||
       std::uint64_t(*offset)!=phase->raw_before_bytes||std::uint64_t(*offset)<header.header.size()||
       std::uint64_t(*length)>limits.page_bytes||std::uint64_t(*total)>limits.durable_stage_bytes||
       std::uint64_t(*offset)>std::uint64_t(*total)||std::uint64_t(*length)>std::uint64_t(*total)-std::uint64_t(*offset))
        throw db_error("GROUP history_chain raw read exact immutable prefix/span differs");
    return {phase->page_index,std::uint64_t(*offset),std::uint64_t(*length),std::uint64_t(*total)};
}
void database::require_target_history_chain_raw_retired_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving,const void* scope,
    const detail::ordinary_target_history_page_reader& reader)const {
    const auto state=target_staging_;const auto original=state?state->history_chain:nullptr;
    const auto phase=original?original->active:nullptr;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&connection||state->retirement_requested||
       !original||original->shell!=this||original->owner.lock().get()!=&receiving||original->first_failure||
       !phase||phase->shell!=this||phase->owner.lock().get()!=&receiving||phase->scope.get()!=scope||phase->raw_reader.get()!=&reader||
       !phase->raw_storage||!phase->raw_metadata_checked||!phase->left||!phase->native_ending_known()||!phase->scope->leave(*this)||
       !phase->decode_entered||!phase->decode_returned||!phase->decoder_retirement_entered||!phase->decoder_retirement_returned||
       phase->decoded||!phase->decoder_charge||phase->decoder_charge!=receiving.require_decoder_charge(connection))
        throw db_error("GROUP history_chain raw byte borrower/decoder actual destruction ending absent");
}
} // namespace lattice
