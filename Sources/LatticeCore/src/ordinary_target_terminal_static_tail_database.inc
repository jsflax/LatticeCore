// Four remaining actual heap Native families only. No SQL/binding/row/call
// payload or shared programme/scope owner is copied into the ending history.
#include "../include/lattice/native_retirement_storage.hpp"
#include <array>
#include <memory>
#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>
namespace lattice {
struct database::target_staging_state::terminal_native_static_record {
    struct vm_fact {
        std::size_t index=0;
        const void* original=nullptr;
        sqlite3_stmt* held=nullptr;sqlite3_stmt* prepared=nullptr;sqlite3_stmt* finalized=nullptr;
        bool prepare_entered=false,prepare_returned=false,step_entered=false,first_step_returned=false,terminal_returned=false;
        bool readonly_returned=false,finalize_entered=false,finalize_returned=false,destruction_known=false;
        bool count_returned=false,count_checked=false;
        int prepare_rc=SQLITE_MISUSE,readonly_value=0,first_step_rc=SQLITE_MISUSE,terminal_rc=SQLITE_MISUSE,finalize_rc=SQLITE_MISUSE;
        std::int64_t count_value=0;
        bool issued()const noexcept {
            return held||prepared||finalized||prepare_entered||prepare_returned||step_entered||first_step_returned||terminal_returned||
                readonly_returned||finalize_entered||finalize_returned||destruction_known||count_returned||count_checked||
                prepare_rc!=SQLITE_MISUSE||readonly_value||first_step_rc!=SQLITE_MISUSE||terminal_rc!=SQLITE_MISUSE||
                finalize_rc!=SQLITE_MISUSE||count_value;
        }
        bool known()const noexcept {
            if(!original||held||(prepare_entered&&!prepare_returned)||(!prepare_entered&&(prepare_returned||prepared)))return false;
            if(prepared)return prepare_entered&&prepare_returned&&finalize_entered&&finalize_returned&&destruction_known&&
                (finalize_rc&0xff)!=SQLITE_MISUSE&&prepared==finalized;
            return !finalize_entered&&!finalized;
        }
    };
    static constexpr std::array<std::size_t,4> vm_bounds{4164,32,388,16387};
    static constexpr std::array<std::size_t,4> call_bounds{73728,640,33024,344128};
    template<typename Statement> static vm_fact snapshot_vm(const Statement& input,std::size_t at)noexcept {
        vm_fact value;value.index=at;value.original=&input;value.held=input.held;
        value.prepared=input.prepared_identity;value.finalized=input.finalized_identity;
        value.prepare_entered=input.prepare_entered;value.prepare_returned=input.prepare_returned;
        value.first_step_returned=input.first_step_returned;value.terminal_returned=input.terminal_returned;
        value.finalize_entered=input.finalize_entered;value.finalize_returned=input.finalize_returned;
        if constexpr(requires {input.step_entered;})value.step_entered=input.step_entered;
        if constexpr(requires {input.readonly_returned;}) {value.readonly_returned=input.readonly_returned;value.readonly_value=input.readonly_value;}
        if constexpr(requires {input.destruction_known;})value.destruction_known=input.destruction_known;
        else value.destruction_known=input.finalize_destruction_known;
        if constexpr(requires {input.count_returned;}) {value.count_returned=input.count_returned;
            value.count_checked=input.count_checked;value.count_value=input.count_value;}
        value.prepare_rc=input.prepare_rc;value.first_step_rc=input.first_step_rc;value.terminal_rc=input.terminal_rc;value.finalize_rc=input.finalize_rc;
        return value;
    }
    template<typename Original> static std::size_t begin_index(const Original& input)noexcept {
        if constexpr(requires {input.begin;})return input.begin;else return 0;
    }
    template<typename Original> static std::size_t commit_index(const Original& input)noexcept {
        if constexpr(requires {input.commit;})return input.commit;else return input.commit_index;
    }
    template<typename Original> static std::size_t rollback_index(const Original& input)noexcept {
        if constexpr(requires {input.rollback;})return input.rollback;else return input.rollback_index;
    }
    template<typename Original> static bool extra_known(const Original& input)noexcept {
        if constexpr(requires {input.module_entered;})if(input.module_entered&&!input.module_returned)return false;
        if constexpr(requires {input.guard_registration_entered;}) {
            if(input.guard_registration_entered&&!input.guard_registration_returned)return false;
            if(input.userdata&&(!input.guard_destroy_entered||!input.guard_destroy_returned))return false;
        }
        if constexpr(requires {input.ending_autocommit_entered;})
            if(input.ending_autocommit_entered&&!input.ending_autocommit_returned)return false;
        return true;
    }
    template<typename Original> static int autocommit(const Original& input)noexcept {
        if constexpr(requires {input.first_ending_autocommit;})return input.first_ending_autocommit;
        else return input.ending_autocommit;
    }
    template<typename Original> static bool source_known(const Original& input,database& actual,
        const detail::ordinary_target_staging_connection& connection,const detail::ordinary_group_target_receiving_owner& receiving,
        const void* scope,std::uint64_t generation,std::size_t kind)noexcept {
        if constexpr(requires {input.generated;}) {if(!input.generated)return false;}
        else if constexpr(requires {input.program;}) {if(!input.program)return false;}
        if(kind>=4||input.statements.size()>vm_bounds[kind]||input.calls.size()>call_bounds[kind]||
           input.call_count>input.calls.size()||!extra_known(input))return false;
        if(input.scope) {
            if(!input.scope->terminal_static_detached_history(actual,connection,receiving,scope,generation,kind).common.detached_known)return false;
        }else if(input.call_count)return false;
        for(std::size_t at=0;at<input.call_count;++at)if(input.calls[at].entered&&!input.calls[at].returned)return false;
        for(std::size_t at=0;at<input.statements.size();++at) {
            const auto vm=snapshot_vm(input.statements[at],at);
            if(!vm.known()||(!input.scope&&vm.issued()))return false;
        }
        const auto begin=begin_index(input),commit=commit_index(input),rollback=rollback_index(input);
        if(begin>=input.statements.size()||commit>=input.statements.size()||rollback>=input.statements.size())return false;
        const auto& started=input.statements[begin];const auto& done=input.statements[commit];const auto& undo=input.statements[rollback];
        return !started.terminal_returned||started.terminal_rc!=SQLITE_DONE||
            (done.terminal_returned&&done.terminal_rc==SQLITE_DONE)||(undo.terminal_returned&&undo.terminal_rc==SQLITE_DONE)||
            (input.ending_autocommit_returned&&autocommit(input)!=0);
    }
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot,kind;
    const std::uint64_t serial,generation;
    const std::weak_ptr<target_staging_state> state;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const database* const shell;
    const detail::ordinary_target_staging_connection* const connection;
    const void* const cleanup_scope;
    const void* const object_original;
    const void* const tail_original;
    const void* const schema_original;
    const void* const programme_original;
    const void* const scope_wrapper_original;
    const void* const statements_original;
    const void* const calls_original;
    const std::exception_ptr first_native_failure,first_programme_failure;
    const detail::ordinary_target_staging_connection::database_scope::terminal_static_scope_history scope_history;
    const bool constructor_returned,source_returns_known,completed,left,ending;
    const std::size_t statement_slots,call_slots,call_count,begin,commit,rollback;
    const bool ending_autocommit_returned;
    const int ending_autocommit;
    const bool module_entered,module_returned;
    const int first_module_rc;
    const void* const userdata_original;
    const bool guard_registration_entered,guard_registration_returned,guard_destroy_entered,guard_destroy_returned;
    const int first_guard_registration_rc;
    const bool transaction_state_observed;
    const int transaction_state;
    const bool cookie_observed;
    const std::int64_t first_cookie;
    const std::size_t cookie_text_bytes;
    // Only actual issued Native first-result facts are retained. Every omitted
    // slot was checked to have default/unissued Native facts, not copied payload.
    const std::vector<vm_fact> statements;
    bool binding_attempted=false,binding_returned=false,extraction_attempted=false,extraction_returned=false;
    bool release_attempted=false,release_returned=false;
    std::atomic<bool> members_destroyed{false},partial_constructor_destroyed{false};
    std::thread::id ending_thread;
    template<typename Original>
    terminal_native_static_record(const std::shared_ptr<target_staging_state>& actual,database& actual_shell,
        const detail::ordinary_target_staging_connection& conn,const detail::ordinary_group_target_receiving_owner& receiving,
        const void* cleanup,std::uint64_t gen,std::size_t selected,const Original& input)
        :creator(actual->creator),thread(actual->thread),end(actual->end),slot(actual->slot),kind(selected),serial(actual->serial),generation(gen),
         state(actual),owner(receiving.terminal_retirement_wake_original(conn)),shell(&actual_shell),connection(&conn),cleanup_scope(cleanup),
         object_original(&input),tail_original(&input.terminal_static),schema_original(input.terminal_static.schema_original),
         programme_original([&]() -> const void* {if constexpr(requires {input.generated;})return input.generated.get();else if constexpr(requires {input.program;})return input.program.get();else return nullptr;}()),
         scope_wrapper_original(input.scope.get()),statements_original(&input.statements),calls_original(&input.calls),first_native_failure(input.first_failure),
         first_programme_failure([&]() -> std::exception_ptr {if constexpr(requires {input.generated;})return input.generated?input.generated->first_failure:std::exception_ptr{};
             else if constexpr(requires {input.program;})return input.program?input.program->first_failure:std::exception_ptr{};else return {};}()),
         scope_history(input.scope?input.scope->terminal_static_detached_history(actual_shell,conn,receiving,cleanup,gen,selected):
             detail::ordinary_target_staging_connection::database_scope::terminal_static_scope_history{}),
         constructor_returned(input.terminal_static.construction_returned),source_returns_known(source_known(input,actual_shell,conn,receiving,cleanup,gen,selected)),
         completed(input.completed),left(input.left),ending(input.ending),statement_slots(input.statements.size()),call_slots(input.calls.size()),
         call_count(input.call_count),begin(begin_index(input)),commit(commit_index(input)),rollback(rollback_index(input)),
         ending_autocommit_returned(input.ending_autocommit_returned),ending_autocommit(autocommit(input)),
         module_entered([&] {if constexpr(requires {input.module_entered;})return input.module_entered;else return false;}()),
         module_returned([&] {if constexpr(requires {input.module_returned;})return input.module_returned;else return false;}()),
         first_module_rc([&] {if constexpr(requires {input.first_module_rc;})return input.first_module_rc;else return SQLITE_MISUSE;}()),
         userdata_original([&]() -> const void* {if constexpr(requires {input.userdata;})return input.userdata;else return nullptr;}()),
         guard_registration_entered([&] {if constexpr(requires {input.guard_registration_entered;})return input.guard_registration_entered;else return false;}()),
         guard_registration_returned([&] {if constexpr(requires {input.guard_registration_returned;})return input.guard_registration_returned;else return false;}()),
         guard_destroy_entered([&] {if constexpr(requires {input.guard_destroy_entered;})return input.guard_destroy_entered;else return false;}()),
         guard_destroy_returned([&] {if constexpr(requires {input.guard_destroy_returned;})return input.guard_destroy_returned;else return false;}()),
         first_guard_registration_rc([&] {if constexpr(requires {input.first_guard_registration_rc;})return input.first_guard_registration_rc;else return SQLITE_MISUSE;}()),
         transaction_state_observed([&] {if constexpr(requires {input.snapshot_observed;})return input.snapshot_observed;
             else if constexpr(requires {input.transaction_observed;})return input.transaction_observed;else return false;}()),
         transaction_state([&] {if constexpr(requires {input.snapshot_state;})return input.snapshot_state;
             else if constexpr(requires {input.transaction_state;})return input.transaction_state;else return SQLITE_TXN_NONE;}()),
         cookie_observed([&] {if constexpr(requires {input.cookie_observed;})return input.cookie_observed;else return false;}()),
         first_cookie([&]() -> std::int64_t {if constexpr(requires {input.first_cookie;})return input.first_cookie;else return -1;}()),
         cookie_text_bytes([&]() -> std::size_t {if constexpr(requires {input.cookie_text;})return input.cookie_text.size();else return 0;}()),
         statements([&] {
             if(selected>=4||input.statements.size()>vm_bounds[selected])throw db_error("terminal static Native VM history bound");
             std::size_t used=0;for(std::size_t at=0;at<input.statements.size();++at)if(snapshot_vm(input.statements[at],at).issued())++used;
             std::vector<vm_fact> copy;copy.reserve(used);
             if(copy.capacity()>input.statements.size())throw db_error("terminal static Native first-result capacity bound");
             for(std::size_t at=0;at<input.statements.size();++at) {auto vm=snapshot_vm(input.statements[at],at);if(vm.issued())copy.push_back(vm);}
             return copy;}()) {}
    bool native_returns_known()const noexcept {
        if(kind>=4||!constructor_returned||!source_returns_known||!schema_original||statement_slots>vm_bounds[kind]||
           call_slots>call_bounds[kind]||call_count>call_slots||statements.capacity()>statement_slots||
           statements.size()>statement_slots||begin>=statement_slots||commit>=statement_slots||rollback>=statement_slots||
           (module_entered&&!module_returned)||(guard_registration_entered&&!guard_registration_returned)||
           (userdata_original&&(!guard_destroy_entered||!guard_destroy_returned)))return false;
        if(scope_wrapper_original) {
            const auto& held=scope_history.common;
            if(scope_history.kind!=kind||!held.detached_known||!held.scope_original||!held.retirement_original||
               held.active_callback_slot!=64||held.active_callback_generation||held.settling_callback_slot!=64||held.settling_callback_generation)return false;
            if(held.published) {if(!held.left||!held.generation||held.generation>=generation||!held.ledger_original)return false;}
            else if(held.generation||held.ledger_original||held.environment_original||held.left||held.leave_returned||held.leave_joined)return false;
        }else if(scope_history.common.detached_known||scope_history.common.scope_original||call_count||!statements.empty()||
            module_entered||guard_registration_entered||userdata_original)return false;
        std::size_t previous=0;bool first=true;
        for(const auto& vm:statements) {
            if(vm.index>=statement_slots||(!first&&vm.index<=previous)||!vm.issued()||!vm.known())return false;
            previous=vm.index;first=false;
        }
        const auto find=[&](std::size_t index) -> const vm_fact* {
            const auto at=std::lower_bound(statements.begin(),statements.end(),index,[](const auto& vm,std::size_t value){return vm.index<value;});
            return at==statements.end()||at->index!=index?nullptr:&*at;
        };
        const auto* started=find(begin);const auto* done=find(commit);const auto* undo=find(rollback);
        return !started||!started->terminal_returned||started->terminal_rc!=SQLITE_DONE||
            (done&&done->terminal_returned&&done->terminal_rc==SQLITE_DONE)||
            (undo&&undo->terminal_returned&&undo->terminal_rc==SQLITE_DONE)||(ending_autocommit_returned&&ending_autocommit!=0);
    }
};
struct database::target_staging_state::terminal_native_static_tail {
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot,kind;
    const std::uint64_t serial;
    const std::weak_ptr<target_staging_state> state;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const database* const shell;
    const void* const object_original;
    const void* const schema_original;
    bool construction_returned=false,binding_attempted=false;
    std::shared_ptr<terminal_native_static_record> first;
    terminal_native_static_tail(database& actual,const void* original,const void* schema,std::size_t selected,
        const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving={})
        :creator(actual.target_staging_->creator),thread(actual.target_staging_->thread),end(actual.target_staging_->end),
         slot(actual.target_staging_->slot),kind(selected),serial(actual.target_staging_->serial),state(actual.target_staging_),owner(receiving),
         shell(&actual),object_original(original),schema_original(schema) {
        if(!actual.target_staging_->current_creator()||!original||!schema||kind>=4||(kind!=0&&!receiving))
            throw db_error("actual static Native constructor identity unavailable");
    }
    terminal_native_static_tail(const terminal_native_static_tail&)=delete;
    terminal_native_static_tail& operator=(const terminal_native_static_tail&)=delete;
    void constructor_returned()noexcept {
        if(creator==detail::database_native_process()&&thread==std::this_thread::get_id())construction_returned=true;
    }
    bool bind(const std::shared_ptr<terminal_native_static_record>& issued)noexcept {
        if(creator!=detail::database_native_process()||thread!=std::this_thread::get_id()||binding_attempted||first)return false;
        binding_attempted=true;
        if(!construction_returned||!issued||issued->creator!=creator||issued->thread!=thread||issued->end!=end||issued->slot!=slot||
           issued->serial!=serial||issued->kind!=kind||issued->shell!=shell||issued->object_original!=object_original||
           issued->tail_original!=this||issued->schema_original!=schema_original||issued->state.lock()!=state.lock()||
           (kind!=0&&issued->owner.lock()!=owner.lock())||!issued->native_returns_known())return false;
        issued->binding_attempted=true;first=issued;issued->binding_returned=true;return true;
    }
    ~terminal_native_static_tail()noexcept {
        if(creator!=detail::database_native_process()||!first)return;
        first->ending_thread=std::this_thread::get_id();
        if(construction_returned&&first->binding_returned)first->members_destroyed.store(true,std::memory_order_release);
        else first->partial_constructor_destroyed.store(true,std::memory_order_release);
        database::wake_target_terminal_native_static_(state,first->owner,first->connection);
    }
};
struct database::target_staging_state::terminal_native_static_ending {
    using custody=detail::native_retirement_storage<std::shared_ptr<catalog_native>,std::shared_ptr<history_header_native>,
        std::shared_ptr<history_projection_native>,std::shared_ptr<import_suspension_native>>;
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot;
    const std::uint64_t serial,generation;
    const database* const shell;
    const detail::ordinary_target_staging_connection* const connection;
    const detail::ordinary_group_target_receiving_owner* const owner;
    const void* const scope;
    const std::exception_ptr source_first_failure;
    const std::shared_ptr<terminal_native_page_ending> pages_original;
    const std::array<bool,4> original_issuance_attempted;
    std::array<const void*,4> prepared_roots{};
    std::array<bool,4> record_allocation_attempted{},record_allocation_returned{},record_allocation_exception_returned{};
    std::array<std::shared_ptr<terminal_native_static_record>,4> records{};
    std::array<terminal_native_statics_result,4> slot_results{terminal_native_statics_result::retained_debt,
        terminal_native_statics_result::retained_debt,terminal_native_statics_result::retained_debt,terminal_native_statics_result::retained_debt};
    std::array<bool,4> slot_returned{};
    custody held; // FOUR empty typed slots exist before any first tail binding/extraction.
    bool preparation_returned=false,extraction_attempted=false,extraction_returned=false,returned=false;
    terminal_native_statics_result result=terminal_native_statics_result::retained_debt;
    std::exception_ptr first_failure;
    terminal_native_static_ending(const std::shared_ptr<target_staging_state>& actual,database& actual_shell,
        const detail::ordinary_target_staging_connection& conn,const detail::ordinary_group_target_receiving_owner& receiving,
        const void* cleanup,std::uint64_t gen)
        :creator(actual->creator),thread(actual->thread),end(actual->end),slot(actual->slot),serial(actual->serial),generation(gen),
         shell(&actual_shell),connection(&conn),owner(&receiving),scope(cleanup),source_first_failure(actual->first_failure),
         pages_original(actual->terminal_native_pages_original),original_issuance_attempted{actual->catalog_attempted,
             actual->history_header_attempted,actual->history_projection_attempted,actual->import_suspension_attempted} {}
};
} // namespace lattice
