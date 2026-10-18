// Included once, outside db.cpp's lattice namespace. This is a closed
// complementary source leaf, not an ordinary/public target opener.
#include "ordinary_target_staging_connection.hpp"
#include "ordinary_target_staging_io.hpp"
#include "ordinary_group_target_schema.hpp"
#include "ordinary_group_target_receiving_owner.hpp"
#include "ordinary_history_storage_recipe.hpp"
#include "../include/lattice/ordinary_schema_recipe.hpp"
#include <array>
#include <cstring>

namespace lattice {
namespace {
class target_staging_open_ledger final : public detail::database_open_ledger {
public:
    ~target_staging_open_ledger() = default;
};
static_assert(sizeof(target_staging_open_ledger)==sizeof(detail::database_open_ledger));
std::shared_ptr<detail::database_open_ledger> target_staging_ledger() {
    auto* actual=new target_staging_open_ledger;
    return std::shared_ptr<detail::database_open_ledger>(actual,[](auto* owner) noexcept {
        if(owner->in_creator_process())delete static_cast<target_staging_open_ledger*>(owner);
        // Inherited destruction cannot erase the actual parent ledger.
    });
}
}

struct database::target_staging_state {
    detail::ordinary_target_scope_root_tail terminal_receiving_tail_{this,7}; // First: ALL actual Native first banks end before this tail.
    const uint64_t creator=detail::database_native_process();
    const std::thread::id thread=std::this_thread::get_id();
    const detail::ordinary_launch::deadline end;
    std::weak_ptr<detail::ordinary_target_staging_connection> connection;
    std::shared_ptr<detail::ordinary_target_staging_io> io;
    std::shared_ptr<detail::database_open_ledger> ledger;
    const size_t slot;
    const uint64_t serial;
    bool initialization_attempted=false,native_open_attempted=false,native_open_returned=false;
    int first_open_result=detail::database_retirement_state::not_attempted;
    bool configured=false,retirement_requested=false,close_returned=false,fallback_returned=false;
    int first_close_result=detail::database_retirement_state::not_attempted;
    int first_fallback_result=detail::database_retirement_state::not_attempted;
    std::exception_ptr first_failure;
    bool terminal_environment_ending_attempted=false,terminal_environment_ending_returned=false,terminal_environment_ended=false;
    bool terminal_raw_ending_attempted=false,terminal_raw_ending_returned=false;
    detail::ordinary_target_staging_connection::terminal_raw_retirement_result terminal_raw_result=
        detail::ordinary_target_staging_connection::terminal_raw_retirement_result::retained_debt;
    bool terminal_files_ending_attempted=false,terminal_files_ending_returned=false;
    detail::ordinary_target_staging_connection::terminal_files_retirement_result terminal_files_result=
        detail::ordinary_target_staging_connection::terminal_files_retirement_result::retained_debt;
    bool terminal_vfs_ending_attempted=false,terminal_vfs_ending_returned=false;
    detail::ordinary_target_staging_connection::terminal_vfs_retirement_result terminal_vfs_result=
        detail::ordinary_target_staging_connection::terminal_vfs_retirement_result::retained_debt;
    bool terminal_scope_ending_attempted=false,terminal_scope_ending_returned=false;
    detail::ordinary_target_staging_connection::terminal_scope_retirement_result terminal_scope_result=
        detail::ordinary_target_staging_connection::terminal_scope_retirement_result::retained_debt;
    struct terminal_native_static_record;
    struct terminal_native_static_tail;
    struct terminal_native_static_ending;
    bool terminal_native_statics_attempted=false,terminal_native_statics_allocation_returned=false,terminal_native_statics_allocation_exception_returned=false;
    std::exception_ptr terminal_native_statics_first_failure;
    std::shared_ptr<terminal_native_static_ending> terminal_native_statics_original;
    struct terminal_native_page_record;
    struct terminal_native_page_tail;
    struct terminal_native_page_ending;
    bool terminal_native_pages_attempted=false,terminal_native_pages_allocation_returned=false,terminal_native_pages_allocation_exception_returned=false;
    std::exception_ptr terminal_native_pages_first_failure;
    std::shared_ptr<terminal_native_page_ending> terminal_native_pages_original;
    struct terminal_native_outer_record;
    struct terminal_native_outer_tail;
    struct terminal_native_outer_ending;
    bool terminal_native_outer_attempted=false,terminal_native_outer_allocation_returned=false,terminal_native_outer_allocation_exception_returned=false;
    std::exception_ptr terminal_native_outer_first_failure;
    std::shared_ptr<terminal_native_outer_ending> terminal_native_outer_original;
    struct terminal_native_phase_record;
    struct terminal_native_phase_tail;
    struct terminal_native_phase_ending;
    bool terminal_native_phase_attempted=false,terminal_native_phase_allocation_returned=false,terminal_native_phase_allocation_exception_returned=false;
    std::exception_ptr terminal_native_phase_first_failure;
    std::shared_ptr<terminal_native_phase_ending> terminal_native_phase_original;
    struct terminal_decoder_ending;
    bool terminal_decoder_attempted=false,terminal_decoder_allocation_returned=false,terminal_decoder_allocation_exception_returned=false;
    std::exception_ptr terminal_decoder_first_failure;
    std::shared_ptr<terminal_decoder_ending> terminal_decoder_original;
    std::unique_ptr<detail::ordinary_target_staging_connection::database_scope> constructor_scope,cleanup_scope;
    std::unique_ptr<detail::ordinary_target_staging_connection::database_scope> schema_scope;
    std::weak_ptr<detail::ordinary_group_target_schema_original> schema_original;
    bool schema_attempted=false,schema_left=false,metadata_bootstrap_done=false;
    struct sync_control_userdata;
    struct catalog_native;
    std::shared_ptr<catalog_native> catalog;
    bool catalog_attempted=false;
    struct history_header_native;
    std::shared_ptr<history_header_native> history_header;
    bool history_header_attempted=false;
    struct row_import_native;
    std::shared_ptr<row_import_native> row_import;
    bool row_import_attempted=false;
    struct import_suspension_native;
    std::shared_ptr<import_suspension_native> import_suspension;
    bool import_suspension_attempted=false;
    struct history_chain_native;
    std::shared_ptr<history_chain_native> history_chain;
    bool history_chain_attempted=false;
    struct history_projection_native;
    std::shared_ptr<history_projection_native> history_projection;
    bool history_projection_attempted=false;
    struct history_page_native;
    std::shared_ptr<history_page_native> history_page;
    bool history_page_active=false;
    std::uint64_t history_pages_issued=0;
    static void invoke_catalog_control(sync_control_userdata*,sqlite3_context*)noexcept;
    sync_control_userdata* userdata_original=nullptr; // Exact issued object identity, never a current grant.
    bool udf_registration_attempted=false,udf_registration_returned=false,udf_destroy_entered=false,udf_destroy_returned=false;
    int first_udf_registration_result=detail::database_retirement_state::not_attempted;
    struct schema_statement {
        sqlite3_stmt* held=nullptr;
        sqlite3_stmt* prepared_identity=nullptr;sqlite3_stmt* finalized_identity=nullptr;
        bool finalize_destruction_known=false;
        bool prepare_attempted=false,prepare_returned=false,step_attempted=false,step_returned=false;
        bool finalize_attempted=false,finalize_returned=false;
        int prepare_rc=SQLITE_MISUSE,step_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
    };
    // BEGIN, seven exact fixed recipes, COMMIT, ROLLBACK, fixed UDF probe and bounded UDF reads.
    std::array<schema_statement,32> schema_statements{};
    std::size_t udf_queries=0;
    enum class schema_native_kind {prepare,step,finalize,register_udf,autocommit,column_count,column_type,column_integer};
    struct schema_native_call {
        schema_native_kind kind=schema_native_kind::prepare;std::size_t statement=0;
        bool entered=false,returned=false;int64_t result=SQLITE_MISUSE;
    };
    static constexpr std::size_t normal_schema_calls=96,total_schema_calls=112;
    std::array<schema_native_call,total_schema_calls> schema_calls{};
    std::size_t schema_call_count=0;
    bool ending_autocommit_attempted=false,ending_autocommit_returned=false;
    int ending_autocommit=0;
    schema_native_call& reserve_schema(schema_native_kind kind,std::size_t statement,bool ending) {
        if(!current_creator()||schema_call_count>=(ending?total_schema_calls:normal_schema_calls))
            throw db_error("target fixed schema native ledger exhausted");
        auto& call=schema_calls[schema_call_count++];call.kind=kind;call.statement=statement;return call;
    }
    static void returned_schema(schema_native_call& call,int64_t result)noexcept {
        call.result=result;call.returned=true; // Before any current/error/string conversion.
    }
    void require_schema_current(const database& actual)const {
        if(!current_creator()||!schema_scope||schema_left)throw db_error("target actual schema scope unavailable");
        schema_scope->require_current(actual);
    }
    void require_schema_work(const database& actual)const {
        if(!current_creator()||!schema_scope||schema_left)throw db_error("target generated work lacks original");
        schema_scope->require_schema_work(actual);
    }
    int64_t read_fixed_control_integer(const database& actual,schema_statement& statement,std::size_t index) {
        require_schema_work(actual);
        if(index>=schema_statements.size()||&schema_statements[index]!=&statement||!statement.held||
           !statement.prepare_returned||statement.prepare_rc!=SQLITE_OK||
           !statement.step_returned||statement.step_rc!=SQLITE_ROW)
            throw db_error("target actual fixed control statement unavailable");
        auto& count=reserve_schema(schema_native_kind::column_count,index,false);
        require_schema_work(actual);count.entered=true;
        const int columns=sqlite3_column_count(statement.held);returned_schema(count,columns);
        require_schema_work(actual);
        if(columns!=1)throw db_error("target fixed control column count malformed");
        auto& type=reserve_schema(schema_native_kind::column_type,index,false);
        require_schema_work(actual);type.entered=true;
        const int storage=sqlite3_column_type(statement.held,0);returned_schema(type,storage);
        require_schema_work(actual);
        if(storage!=SQLITE_INTEGER)throw db_error("target fixed control storage malformed");
        auto& integer=reserve_schema(schema_native_kind::column_integer,index,false);
        require_schema_work(actual);integer.entered=true;
        const int64_t value=sqlite3_column_int64(statement.held,0);returned_schema(integer,value);
        require_schema_work(actual);return value;
    }
    struct sync_control_userdata {
        const std::shared_ptr<target_staging_state> original;
        database* const shell;
        sqlite3* const native;
        sync_control_userdata(std::shared_ptr<target_staging_state> value,database& owner,sqlite3* db)
            :original(std::move(value)),shell(&owner),native(db) {}
        static void destroy(void* raw)noexcept {
            auto* held=static_cast<sync_control_userdata*>(raw);
            if(!held||!held->original->current_creator()||held->original->userdata_original!=held)return;
            // Inherited/foreign disposal is no original creator ending.
            auto original=held->original;original->udf_destroy_entered=true;
            delete held;original->udf_destroy_returned=true;
        }
        static void invoke(sqlite3_context* context,int,sqlite3_value**)noexcept {
            auto* held=static_cast<sync_control_userdata*>(sqlite3_user_data(context));
            if(!held||!held->original->current_creator()||held->original->userdata_original!=held)return;
            // No inherited SQLite/result operation or ending credit.
            const auto original=held->original;
            if(original->catalog){invoke_catalog_control(held,context);return;}
            schema_statement* query=nullptr;std::size_t index=0;
            try {
                original->require_schema_work(*held->shell);
                if(held->native!=held->shell->db_||original->udf_queries==original->schema_statements.size()-11)
                    throw db_error("target bounded actual UDF connection unavailable");
                index=11+original->udf_queries++;query=&original->schema_statements[index];
                auto& prepare=original->reserve_schema(schema_native_kind::prepare,index,false);
                original->require_schema_work(*held->shell);query->prepare_attempted=true;prepare.entered=true;
                query->prepare_rc=::lattice::detail::store_physical_prepare_v2(held->native,"SELECT disabled FROM _SyncControl WHERE id=1",-1,&query->held,nullptr);
                query->prepared_identity=query->held;
                query->prepare_returned=true;returned_schema(prepare,query->prepare_rc);
                if(query->prepare_rc!=SQLITE_OK||!query->held)throw db_error("target UDF read preparation failed");
                original->require_schema_work(*held->shell);
                auto& step=original->reserve_schema(schema_native_kind::step,index,false);
                original->require_schema_work(*held->shell);query->step_attempted=true;step.entered=true;
                query->step_rc=::lattice::detail::store_physical_step(query->held);query->step_returned=true;returned_schema(step,query->step_rc);
                if(query->step_rc!=SQLITE_ROW)throw db_error("target UDF actual control row unavailable");
                const auto value=original->read_fixed_control_integer(*held->shell,*query,index);
                if(value!=0&&value!=1)throw db_error("target UDF actual control value malformed");
                original->require_schema_work(*held->shell);
                auto& done=original->reserve_schema(schema_native_kind::step,index,false);
                original->require_schema_work(*held->shell);done.entered=true;
                const int terminal=::lattice::detail::store_physical_step(query->held);returned_schema(done,terminal);
                if(terminal!=SQLITE_DONE)throw db_error("target UDF control row not unique/complete");
                original->require_schema_work(*held->shell);
                auto& finalize=original->reserve_schema(schema_native_kind::finalize,index,false);
                original->require_schema_work(*held->shell);query->finalized_identity=query->held;query->finalize_attempted=true;finalize.entered=true;
                query->finalize_rc=::lattice::detail::store_physical_finalize(query->held);query->finalize_returned=true;
                returned_schema(finalize,query->finalize_rc);
                query->finalize_destruction_known=(query->finalize_rc&0xff)!=SQLITE_MISUSE;
                if(query->finalize_destruction_known)query->held=nullptr;
                if(query->finalize_rc!=SQLITE_OK)throw db_error("target UDF actual finalization failed");
                original->require_schema_work(*held->shell);sqlite3_result_int(context,static_cast<int>(value));
            }catch(...) {
                if(!original->first_failure)original->first_failure=std::current_exception();
                try {
                    if(original->schema_scope)original->schema_scope->begin_schema_ending(*held->shell);
                    if(query&&query->held&&!query->finalize_attempted) {
                        auto& finalize=original->reserve_schema(schema_native_kind::finalize,index,true);
                        query->finalized_identity=query->held;query->finalize_attempted=true;finalize.entered=true;
                        query->finalize_rc=::lattice::detail::store_physical_finalize(query->held);query->finalize_returned=true;
                        returned_schema(finalize,query->finalize_rc);
                        query->finalize_destruction_known=(query->finalize_rc&0xff)!=SQLITE_MISUSE;
                        if(query->finalize_destruction_known)query->held=nullptr;
                    }
                }catch(...) {} // Actual first callback failure/native debt remains retained.
                sqlite3_result_error(context,"target sync control actual read/ending failed",-1);
            }
        }
    };
    bool constructor_left=false,cleanup_attempted=false,cleanup_admitted=false,cleanup_admission_returned=false;
    bool cleanup_left=false,native_files_join_attempted=false,native_files_join_returned=false,native_files_joined=false;
    struct native_call {bool attempted=false,returned=false;int result=-1;};
    std::array<native_call,20> configuration_calls{};
    size_t configuration_attempts=0;
    target_staging_state(const std::shared_ptr<detail::ordinary_target_staging_connection>& owner,
        const std::shared_ptr<detail::ordinary_target_staging_io>& original,
        const std::shared_ptr<detail::database_open_ledger>& custody,size_t n,uint64_t sequence)
        :end(owner->original_deadline()),connection(owner),io(original),ledger(custody),slot(n),serial(sequence) {terminal_receiving_tail_.construction_returned_=true;}
    bool current_creator()const noexcept {
        return creator==detail::database_native_process()&&thread==std::this_thread::get_id();
    }
    std::shared_ptr<detail::ordinary_target_staging_connection> require_original()const {
        if(!current_creator())throw db_error("target database original creator required");
        auto held=connection.lock();
        if(!held||!io||!ledger)throw db_error("target database original custody unavailable");
        held->require_original(end);
        if(held->io_original(end)!=io)throw db_error("target database I/O original changed");
        io->require_original(end);
        return held;
    }
    void require_constructor_current(const database& shell)const {
        require_original();
        if(!constructor_scope)throw db_error("target database constructor scope unavailable");
        constructor_scope->require_current(shell);
    }
    native_call& begin_configuration_call() {
        if(configuration_attempts==configuration_calls.size())throw db_error("target fixed configuration exhausted");
        auto& value=configuration_calls[configuration_attempts++];value.attempted=true;return value;
    }
};

bool database::target_staging_creator_current_()const noexcept {
    return target_staging_&&target_staging_->current_creator();
}

std::shared_ptr<database> database::make_target_staging_shell(
    const std::shared_ptr<detail::ordinary_target_staging_connection>& connection) {
    if(!connection)throw db_error("target database requires its actual connection original");
    const auto end=connection->original_deadline();
    // The genuine connection charges this one-shot attempt before allocation.
    // Refusal/failure cannot produce a replacement shell on the same original.
    connection->claim_database_shell(end);
    auto io=connection->io_original(end);
    auto ledger=target_staging_ledger();
    const auto admitted=ledger->reserve_open();
    database_open_builder_tail tail{ledger,admitted.slot,admitted.serial};
    auto fact=std::make_shared<detail::database_retirement_state>();
    ledger->bind_retirement(admitted.slot,admitted.serial,fact);
    initialization_key key(connection,io,fact,ledger,admitted.slot,admitted.serial);
    auto* actual=new database(key,target_staging_deferred_t{});
    tail.object_created=true;
    // Existing control-block failure semantics call this exact owning deleter.
    // Constructor has made NO native SQLite/file/registration call.
    return std::shared_ptr<database>(actual,owned_connection_deleter{ledger,admitted.slot,admitted.serial});
}

database::database(initialization_key key,target_staging_deferred_t)
    :retirement_(key.checked_retirement_),mode_(open_mode::read_write),busy_timeout_ms_(0) {
    auto connection=key.target_connection_.lock();
    if(!connection||!key.target_io_||!key.open_ledger_||!retirement_)
        throw db_error("target unopened shell lacks actual retained custody");
    auto* actual=new target_staging_state(connection,key.target_io_,key.open_ledger_,key.open_slot_,key.open_serial_);
    target_staging_=std::shared_ptr<target_staging_state>(actual,[](auto* state) noexcept {
        if(state->current_creator())delete state;
        // Failed/inherited foreign destruction retains original child debt.
    });
    auto held=target_staging_->require_original();
    path_=held->main_path(target_staging_->end); // Native role-derived fixed MAIN only.
    if(path_.empty()||path_[0]!='/'||path_.find('\0')!=std::string::npos)
        throw db_error("target MAIN path unavailable");
    suppress_destructor_optimize_=true;
    // No public query/CRUD/raw route is created by fixed initialization.
    closed_.store(true,std::memory_order_release);
    target_terminal_shell_tail_.construction_returned_=true;
}

void database::initialize_target_staging_() {
    auto state=target_staging_;
    if(!state||!state->current_creator()||state->initialization_attempted||state->retirement_requested||db_||
       retirement_->owning_wrapper_moved()||retirement_->raw_handle_escaped())
        throw db_error("target database initialization original unavailable");
    state->initialization_attempted=true; // Never reset or replay this body.
    try {
        auto held=state->require_original();
        if(held->main_path(state->end)!=path_||held->main_descriptor(state->end)<0)
            throw db_error("target exclusive MAIN original changed");
        // The same connection publishes the actual scope directly into this
        // retained shell before post-current errors. No copied path or VFS
        // name supplies callback/constructor admission.
        held->begin_database_constructor(*this,state->constructor_scope,state->end);
        state->require_constructor_current(*this);
        // The coupled counterparts must prove this exact same-shell scope,
        // native resource ledger and descriptor provider before ANY open.
        held->require_preopen_constructor(state->end);
        state->io->require_preopen_constructor(state->end);
        const char* vfs=state->io->registered_vfs_name(state->end);
        const auto pages=state->io->maximum_page_count(state->end);
        if(!vfs||!*vfs||pages<=0||pages>0x3fffffff)
            throw db_error("target fixed native profile unavailable");
        const auto pages_text=std::to_string(pages); // Held original precedes allocation.
        int flags=SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX;
#ifdef SQLITE_OPEN_NOFOLLOW
        flags|=SQLITE_OPEN_NOFOLLOW;
#else
        throw db_error("target SQLite requires native no-follow support");
#endif
        // MAIN was born exclusively by the genuine connection. No CREATE,
        // URI, legacy path, ordinary WAL fallback or caller-supplied VFS exists.
        state->require_constructor_current(*this);
        // Issue from this already-admitted target constructor BEFORE actual
        // open. Normal Host/resource acquisition has no authority over it.
        if(!physical_open_original_.issue_exclusive_target_(state->creator))
            throw db_error("target constructor physical-open original already issued or inherited");
        state->require_constructor_current(*this);
        state->ledger->native_open_entered(state->slot,state->serial);
        state->native_open_attempted=true;
        state->first_open_result=physical_open_original_.open_exclusive_target_(path_.c_str(),&db_,flags,vfs,state->end);
        state->native_open_returned=true; // Immediate nonthrowing first-result capture.
        state->constructor_scope->record_sqlite_open(*this,db_,state->first_open_result);
        if(!db_)retirement_->record_close(SQLITE_OK,detail::database_retirement_state::not_attempted);
        if(state->first_open_result!=SQLITE_OK)throw db_error("target native SQLite open failed");
        if(!db_)throw db_error("target native SQLite returned no connection");
        if(!physical_open_original_.matches_open_(db_))
            throw db_error("target fixed sqlite open lost its issued actual original");
        state->require_constructor_current(*this); // Postchecks cannot erase native rcOK.
        struct echo {
            const char* expected;size_t rows=0;bool exact=true;
            static int row(void* raw,int count,char** values,char**)noexcept {
                auto& self=*static_cast<echo*>(raw);
                if(count!=1||!values||!values[0]||self.rows++!=0||std::strcmp(values[0],self.expected)!=0){
                    self.exact=false;return 1;
                }
                return 0;
            }
        };
        const auto exec=[&](const char* sql,const char* expected=nullptr){
            state->require_constructor_current(*this);
            auto& call=state->begin_configuration_call();
            echo result{expected};
            call.result=::lattice::detail::store_physical_exec(db_,sql,expected?&echo::row:nullptr,expected?&result:nullptr,nullptr);
            call.returned=true; // First native terminal result precedes current/error conversion.
            state->constructor_scope->record_configuration_result(*this,call.result);
            if(call.result!=SQLITE_OK||(expected&&(!result.exact||result.rows!=1)))
                throw db_error("target fixed configuration failed");
            state->require_constructor_current(*this);
        };
        {state->require_constructor_current(*this);auto& call=state->begin_configuration_call();
            call.result=sqlite3_busy_timeout(db_,0);call.returned=true;
            state->constructor_scope->record_configuration_result(*this,call.result);
            if(call.result!=SQLITE_OK)throw db_error("target no-wait busy policy unavailable");
            state->require_constructor_current(*this);}
#ifdef SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE
        {state->require_constructor_current(*this);auto& call=state->begin_configuration_call();
            call.result=sqlite3_db_config(db_,SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,1,nullptr);call.returned=true;
            state->constructor_scope->record_configuration_result(*this,call.result);
            if(call.result!=SQLITE_OK)throw db_error("target close policy unavailable");
            state->require_constructor_current(*this);}
#else
        throw db_error("target SQLite requires no-checkpoint-on-close support");
#endif
        exec("PRAGMA main.page_size=4096");exec("PRAGMA main.page_size","4096");
        const auto limit="PRAGMA main.max_page_count="+pages_text;
        exec(limit.c_str(),pages_text.c_str());exec("PRAGMA main.max_page_count",pages_text.c_str());
        exec("PRAGMA main.journal_mode=DELETE","delete");exec("PRAGMA main.journal_mode","delete");
        exec("PRAGMA main.synchronous=FULL");exec("PRAGMA main.synchronous","2");
        exec("PRAGMA main.mmap_size=0");exec("PRAGMA main.mmap_size","0");
        exec("PRAGMA temp_store=MEMORY");exec("PRAGMA temp_store","2");
        exec("PRAGMA cache_spill=OFF");exec("PRAGMA cache_spill","0");
        exec("PRAGMA foreign_keys=ON");exec("PRAGMA foreign_keys","1");
        state->require_constructor_current(*this);
        state->constructor_left=state->constructor_scope->leave(*this);
        if(!state->constructor_left)throw db_error("target database constructor scope did not join");
        state->require_original();
        state->configured=true; // Fixed connection setup only; no import/current/model grant.
    }catch(...){
        if(!state->first_failure)state->first_failure=std::current_exception();
        if(state->constructor_scope&&!state->constructor_left)
            state->constructor_left=state->constructor_scope->leave(*this);
        retire_target_staging_(); // No new SQL or original preparation on cleanup.
        throw;
    }
}

std::exception_ptr database::target_staging_failure_original_(
    const detail::ordinary_target_staging_connection& original)const noexcept {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&original||
       state->retirement_requested||state->cleanup_attempted||state->close_returned||state->fallback_returned||
       !retirement_||retirement_->raw_handle_escaped()||retirement_->owning_wrapper_moved())return {};
    return state->first_failure; // SAME first exception, never a copied reason grant.
}

void database::retire_target_staging_()noexcept {
    auto state=target_staging_;
    if(!state)return;
    if(!state->current_creator()){
        if(!db_)return;
        db_=nullptr;
        if(retirement_)retirement_->record_close(detail::database_retirement_state::not_attempted,
                                               detail::database_retirement_state::not_attempted);
        return; // No inherited SQLite, guard retirement or cleanup credit.
    }
    auto held=state->connection.lock();
    // Every constructor/native-phase catch reaches this boundary with its
    // actual first failure before any first automatic descriptor retirement.
    // A failed/missing election retains db_ and every original, never closes.
    if((state->first_failure||(held&&held->terminal_cleanup_pending(*this)))&&
       (!held||!held->elect_terminal_database_failure(*this))) {
        state->retirement_requested=true;closed_.store(true,std::memory_order_release);return;
    }
    state->retirement_requested=true;
    closed_.store(true,std::memory_order_release);
    if(!db_)return; // Actual unopened shell; no SQLite close is manufactured.
    if(state->cleanup_attempted)return; // No replacement cleanup/close attempt.
    state->cleanup_attempted=true;
    if(!held)return; // SAME actual shell/connection custody is required even for cleanup.
    state->cleanup_admitted=held->begin_database_cleanup(*this,state->cleanup_scope);
    state->cleanup_admission_returned=true; // Output is held before interpreting the actual result.
    if(!state->cleanup_admitted||!state->cleanup_scope)return; // Keep db_ and all native debt.
    // Only the private exact-connection sync-control UDF may be installed.
    // SQLite invokes its genuine xDestroy; no user/model observer is mounted.
    // Real SQLite close and fallback have independent immutable first results.
    auto* actual=std::exchange(db_,nullptr);
    state->first_close_result=::lattice::detail::store_physical_close_owned(physical_open_original_,actual);
    state->close_returned=true;
    state->cleanup_scope->record_sqlite_close(*this,state->first_close_result);
    if(state->first_close_result!=SQLITE_OK){
        state->first_fallback_result=::lattice::detail::store_physical_close_v2_owned(physical_open_original_,actual);
        state->fallback_returned=true;
    }
    if(retirement_)retirement_->record_close(state->first_close_result,state->first_fallback_result);
    if(held->terminal_cleanup_pending(*this)) {
        state->terminal_environment_ending_attempted=true;
        state->terminal_environment_ended=held->end_terminal_environment(*this);
        state->terminal_environment_ending_returned=true; // Actual same-scope provider ending result only.
        if(state->terminal_environment_ended) {
            state->terminal_raw_ending_attempted=true;
            state->terminal_raw_result=held->retire_terminal_raw(*this);
            state->terminal_raw_ending_returned=true; // RAW receipt/not-issued DATA only, never full target cleanup.
            if(state->terminal_raw_result!=detail::ordinary_target_staging_connection::terminal_raw_retirement_result::retained_debt) {
                state->terminal_files_ending_attempted=true;
                state->terminal_files_result=held->retire_terminal_files(*this);
                state->terminal_files_ending_returned=true; // Actual files-only first result, never native_files_joined.
                if(state->terminal_files_result==detail::ordinary_target_staging_connection::terminal_files_retirement_result::disposed) {
                    state->terminal_vfs_ending_attempted=true;
                    state->terminal_vfs_result=held->retire_terminal_vfs(*this);
                    state->terminal_vfs_ending_returned=true; // Actual registration ending only, same cleanup remains debt.
                    if(state->terminal_vfs_result==detail::ordinary_target_staging_connection::terminal_vfs_retirement_result::disposed) {
                        state->terminal_scope_ending_attempted=true;
                        state->terminal_scope_result=held->detach_terminal_scope(*this);
                        state->terminal_scope_ending_returned=true; // Binding-only result; no leave/payload/lease/owner join.
                    }
                }
            }
        }
        // Retain THIS cleanup scope and all first close/error/callback banks.
        // The separate terminal route may detach its active binding only after
        // actual files and VFS disposal. Ordinary leave/native_files_joined and
        // payload/input/resource/lease/shell/owner/FIFO endings remain debt.
        return;
    }
    // Only the genuine same-shell cleanup scope can join the exact original
    // borrower/locks/fds and guard ending. Failed close_v2 never substitutes
    // for the actual first SQLite close or enables this join.
    if(state->first_close_result==SQLITE_OK&&target_staging_native_cleanup_known_(*held)&&target_row_import_cleanup_ending_()&&target_import_suspension_cleanup_ending_()&&target_history_chain_cleanup_ending_()&&target_history_projection_cleanup_ending_()&&
       (!state->udf_registration_attempted||state->udf_destroy_returned)){
        // First SQLite close and genuine UDF userdata destruction are distinct
        // actual endings. Neither substitutes for original descriptor/VFS join.
        state->native_files_join_attempted=true;
        state->native_files_joined=state->cleanup_scope->join_native_files(*this);
        state->native_files_join_returned=true;
    }
    state->cleanup_left=state->cleanup_scope->leave(*this);
    // These returned facts grant no import/model/current/active WAL route,
    // registry refund or physical-space credit. Unknown native ending remains held.
}

bool database::target_staging_cleanup_joined_(const detail::ordinary_target_staging_connection& original)const noexcept {
    const auto state=target_staging_;
    return state&&state->current_creator()&&state->connection.lock().get()==&original&&target_staging_native_cleanup_known_(original)&&
        state->retirement_requested&&state->cleanup_attempted&&state->cleanup_admission_returned&&
        state->cleanup_admitted&&state->cleanup_scope&&state->close_returned&&state->first_close_result==SQLITE_OK&&
        (!state->udf_registration_attempted||state->udf_destroy_returned)&&target_history_guard_destroyed_()&&target_row_import_cleanup_ending_()&&target_import_suspension_cleanup_ending_()&&target_history_chain_cleanup_ending_()&&target_history_projection_cleanup_ending_()&&state->native_files_join_attempted&&
        state->native_files_join_returned&&state->native_files_joined&&state->cleanup_left&&retirement_&&
        retirement_->state()==detail::database_retirement_state::phase::closed&&
        retirement_->checked_close_result()==SQLITE_OK&&!retirement_->raw_handle_escaped()&&!retirement_->owning_wrapper_moved();
}

void database::require_target_fixed_constructor_(const detail::ordinary_target_staging_connection& original)const {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||state->connection.lock().get()!=&original||!db_||
       !retirement_||retirement_->owning_wrapper_moved()||retirement_->raw_handle_escaped()||
       !state->initialization_attempted||!state->native_open_attempted||!state->native_open_returned||
       state->first_open_result!=SQLITE_OK||!physical_open_original_.exclusive_target_||!physical_open_original_.matches_open_(db_)||
       !state->configured||!state->constructor_left||
       !state->constructor_scope||!state->constructor_scope->leave(*this)||state->retirement_requested||
       state->cleanup_attempted||state->configuration_attempts!=18)
        throw db_error("target schema requires actual fixed constructor ending");
    state->constructor_scope->require_constructor_ending(*this,db_);
    for(const auto& call:state->configuration_calls)if(call.attempted&&(!call.returned||call.result!=SQLITE_OK))
        throw db_error("target schema fixed constructor retains native debt");
    state->require_original(); // Actual original join; configured alone is no schema permission.
}
void database::bootstrap_target_metadata_(const std::shared_ptr<detail::ordinary_group_target_schema_original>& original) {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||!original||state->schema_attempted||state->retirement_requested)
        throw db_error("target metadata one-shot original unavailable");
    auto connection=state->require_original();
    require_target_fixed_constructor_(*connection);
    state->schema_attempted=true;state->schema_original=original; // Preserve attempted custody before effects/errors.
    const auto finalize=[&](std::size_t index,bool ending) {
        auto& statement=state->schema_statements[index];
        if(!statement.held||statement.finalize_attempted)return;
        if(ending)state->require_schema_current(*this);else state->require_schema_work(*this);
        auto& call=state->reserve_schema(target_staging_state::schema_native_kind::finalize,index,ending);
        if(ending)state->require_schema_current(*this);else state->require_schema_work(*this);statement.finalized_identity=statement.held;statement.finalize_attempted=true;call.entered=true;
        statement.finalize_rc=::lattice::detail::store_physical_finalize(statement.held);statement.finalize_returned=true;
        target_staging_state::returned_schema(call,statement.finalize_rc);
        statement.finalize_destruction_known=(statement.finalize_rc&0xff)!=SQLITE_MISUSE;
        if(statement.finalize_destruction_known)statement.held=nullptr;
        state->schema_scope->record_schema_finalized(*this,statement.finalize_rc);
        if(!ending&&statement.finalize_rc!=SQLITE_OK)throw db_error("target generated statement finalization failed");
        if(ending)state->require_schema_current(*this);else state->require_schema_work(*this);
    };
    const auto run=[&](std::size_t index,const char* sql,bool ending) {
        auto& statement=state->schema_statements[index];
        if(statement.prepare_attempted||!sql||std::strlen(sql)>16384)
            throw db_error("target generated fixed statement unavailable");
        if(ending)state->require_schema_current(*this);else state->require_schema_work(*this);
        auto& prepare=state->reserve_schema(target_staging_state::schema_native_kind::prepare,index,ending);
        if(ending)state->require_schema_current(*this);else state->require_schema_work(*this);statement.prepare_attempted=true;prepare.entered=true;
        const char* tail=nullptr;
        statement.prepare_rc=::lattice::detail::store_physical_prepare_v2(db_,sql,-1,&statement.held,&tail);statement.prepare_returned=true;
        statement.prepared_identity=statement.held;
        target_staging_state::returned_schema(prepare,statement.prepare_rc);
        if(statement.prepare_rc!=SQLITE_OK||!statement.held)throw db_error("target generated statement preparation failed");
        if(tail)for(const char* suffix=tail;*suffix;++suffix)
            if(*suffix!=' '&&*suffix!='\n'&&*suffix!='\r'&&*suffix!='\t')throw db_error("target generated statement has an extra native tail");
        if(ending)state->require_schema_current(*this);else state->require_schema_work(*this);
        auto& step=state->reserve_schema(target_staging_state::schema_native_kind::step,index,ending);
        if(ending)state->require_schema_current(*this);else state->require_schema_work(*this);statement.step_attempted=true;step.entered=true;
        statement.step_rc=::lattice::detail::store_physical_step(statement.held);statement.step_returned=true;
        target_staging_state::returned_schema(step,statement.step_rc);
        if(index==0)state->schema_scope->record_schema_begin(*this,statement.step_rc);
        if(index==8)state->schema_scope->record_schema_commit(*this,statement.step_rc);
        if(index==9)state->schema_scope->record_schema_rollback(*this,statement.step_rc);
        if(statement.step_rc!=SQLITE_DONE)throw db_error("target generated statement did not complete");
        if(ending)state->require_schema_current(*this);else state->require_schema_work(*this);finalize(index,ending);
    };
    const auto probe_sync_control=[&] {
        constexpr std::size_t index=10;auto& statement=state->schema_statements[index];
        state->require_schema_work(*this);
        auto& prepare=state->reserve_schema(target_staging_state::schema_native_kind::prepare,index,false);
        state->require_schema_work(*this);statement.prepare_attempted=true;prepare.entered=true;
        statement.prepare_rc=::lattice::detail::store_physical_prepare_v2(db_,"SELECT sync_disabled()",-1,&statement.held,nullptr);
        statement.prepared_identity=statement.held;
        statement.prepare_returned=true;target_staging_state::returned_schema(prepare,statement.prepare_rc);
        if(statement.prepare_rc!=SQLITE_OK||!statement.held)throw db_error("target actual UDF probe preparation failed");
        state->require_schema_work(*this);
        auto& step=state->reserve_schema(target_staging_state::schema_native_kind::step,index,false);
        state->require_schema_work(*this);statement.step_attempted=true;step.entered=true;
        statement.step_rc=::lattice::detail::store_physical_step(statement.held);statement.step_returned=true;
        target_staging_state::returned_schema(step,statement.step_rc);
        if(statement.step_rc!=SQLITE_ROW)throw db_error("target actual UDF/control result unavailable");
        if(state->read_fixed_control_integer(*this,statement,index)!=0)
            throw db_error("target actual UDF/control result unavailable");
        state->require_schema_work(*this);
        auto& done=state->reserve_schema(target_staging_state::schema_native_kind::step,index,false);
        state->require_schema_work(*this);done.entered=true;
        const int terminal=::lattice::detail::store_physical_step(statement.held);target_staging_state::returned_schema(done,terminal);
        if(terminal!=SQLITE_DONE)throw db_error("target actual UDF probe did not complete uniquely");
        state->require_schema_work(*this);finalize(index,false);
    };
    try {
        connection->begin_database_schema(*this,original,state->schema_scope,state->end);
        state->require_schema_work(*this);
        auto userdata=std::make_unique<target_staging_state::sync_control_userdata>(state,*this,db_);
        auto& registration=state->reserve_schema(target_staging_state::schema_native_kind::register_udf,0,false);
        state->require_schema_work(*this);
        state->userdata_original=userdata.get(); // SAME owned object before native ownership transfer.
        state->udf_registration_attempted=true;auto* actual=userdata.release();registration.entered=true;
        state->first_udf_registration_result=::lattice::detail::store_physical_create_function_v2(db_,"sync_disabled",0,SQLITE_UTF8,actual,
            &target_staging_state::sync_control_userdata::invoke,nullptr,nullptr,
            &target_staging_state::sync_control_userdata::destroy);
        state->udf_registration_returned=true;
        target_staging_state::returned_schema(registration,state->first_udf_registration_result);
        if(state->first_udf_registration_result!=SQLITE_OK)throw db_error("target actual sync-control UDF registration failed");
        state->require_schema_work(*this);
        run(0,"BEGIN IMMEDIATE",false);
        // Exact literals shared with the normal owner producers. Only fresh
        // metadata/control/Audit objects are installed; compatibility ALTER,
        // sync state/cursor, models/all-kind relations/indexes remain next.
        const std::array<const char*,7> statements{
            detail::ordinary_schema_recipe::metadata_table(),detail::ordinary_schema_recipe::metadata_initial_version(),
            detail::ordinary_schema_recipe::sync_control_table(),detail::ordinary_schema_recipe::sync_control_initial(),
            detail::ordinary_schema_recipe::audit_table(),detail::ordinary_schema_recipe::audit_pending_index(),
            detail::ordinary_schema_recipe::audit_lookup_index()};
        for(std::size_t index=0;index<statements.size();++index) {
            run(index+1,statements[index],false);
            if(index==3)probe_sync_control(); // Actual same native userdata/control read, not an authority receipt.
        }
        run(8,"COMMIT",false);
        state->require_schema_work(*this);
        state->schema_left=state->schema_scope->leave(*this);
        if(!state->schema_left)throw db_error("target metadata schema scope did not join");
        state->require_original();
        state->metadata_bootstrap_done=true; // Partial native stage only, NOT full catalog/version/fingerprint.
    }catch(...) {
        if(!state->first_failure)state->first_failure=std::current_exception();
        try {
            if(state->schema_scope&&!state->schema_left) {
                state->schema_scope->begin_schema_ending(*this); // Same retained ending capacity; no fresh DDL/current grant.
                for(std::size_t index=0;index<state->schema_statements.size();++index)finalize(index,true);
                const auto& begin=state->schema_statements[0];const auto& commit=state->schema_statements[8];
                if(begin.step_returned&&begin.step_rc==SQLITE_DONE&&!(commit.step_returned&&commit.step_rc==SQLITE_DONE)) {
                    auto& ending=state->reserve_schema(target_staging_state::schema_native_kind::autocommit,9,true);
                    state->require_schema_current(*this);state->ending_autocommit_attempted=true;ending.entered=true;
                    state->ending_autocommit=sqlite3_get_autocommit(db_);state->ending_autocommit_returned=true;
                    target_staging_state::returned_schema(ending,state->ending_autocommit);
                    if(!state->ending_autocommit)run(9,"ROLLBACK",true);
                }
                state->schema_left=state->schema_scope->leave(*this);
            }
        }catch(...) {} // First failure and all partial statements/endings remain SAME held debt.
        retire_target_staging_();
        throw;
    }
}
} // namespace lattice

#include "ordinary_target_terminal_static_tail_database.inc"
#include "ordinary_target_catalog_database.inc"

#include "ordinary_target_history_header_database.inc"

#include "ordinary_target_terminal_page_tail_database.inc"
#include "ordinary_target_history_page_database.inc"

#include "ordinary_target_history_projection_database.inc"

#include "ordinary_target_terminal_phase_tail_database.inc"
#include "ordinary_target_terminal_outer_tail_database.inc"
#include "ordinary_target_history_chain_database.inc"
#include "ordinary_target_import_suspension_database.inc"
#include "ordinary_target_history_row_import_database.inc"

#include "ordinary_target_abandonment_database.inc"
#include "ordinary_target_terminal_decoder_database.inc"
#include "ordinary_target_terminal_phase_database.inc"
#include "ordinary_target_terminal_outer_database.inc"
#include "ordinary_target_terminal_page_database.inc"
#include "ordinary_target_terminal_static_database.inc"
#include "ordinary_target_terminal_programme_database.inc"

#include "ordinary_target_terminal_scope_roots_database.inc"
#include "ordinary_target_terminal_shell_database.inc"
