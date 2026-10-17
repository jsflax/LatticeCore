// Complete state, before the actual chain/row outer definitions. No normal
// phase receipt/allocation is added. These tails follow actual member death.
#include <algorithm>
namespace lattice {
struct database::target_staging_state::terminal_native_outer_record {
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
    const void* const outer_original;
    const void* const tail_original;
    const void* const stage_original;
    const void* const meta_original;
    const void* const progress_original;
    const void* const hash_original;
    const void* const programme_original;
    const std::exception_ptr first_native_failure;
    const bool constructor_returned,completed,progress_complete,raw_storage;
    const std::uint64_t next_page,rows,known,unknown,bytes,ended_phases,inserted;
    const std::int64_t last_table;
    const std::size_t stage_cells,meta_cells,previous_size;
    // At most the actual fixed digest bytes. No argument/string/vector/hash
    // object or equivalent staging payload graph moves into this history.
    std::array<char,64> previous_digest{};
    bool binding_attempted=false,binding_returned=false;
    bool extraction_attempted=false,extraction_returned=false;
    bool release_attempted=false,release_returned=false;
    std::atomic<bool> members_destroyed{false},partial_constructor_destroyed{false};
    std::thread::id ending_thread;
    template<typename Original>
    terminal_native_outer_record(const std::shared_ptr<target_staging_state>& actual,database& actual_shell,
        const detail::ordinary_target_staging_connection& conn,const void* scope,std::uint64_t gen,
        std::size_t selected,const Original& input)
        :creator(actual->creator),thread(actual->thread),end(actual->end),slot(actual->slot),kind(selected),
         serial(actual->serial),generation(gen),state(actual),owner(input.owner),shell(&actual_shell),connection(&conn),
         cleanup_scope(scope),outer_original(&input),tail_original(&input.terminal_outer),stage_original(&input.final_stage),
         meta_original(&input.final_meta),progress_original(&input.checked),hash_original(&input.content),
         programme_original([&]() -> const void* {if constexpr(requires {input.program;})return input.program.get();else return nullptr;}()),
         first_native_failure(input.first_failure),constructor_returned(input.terminal_outer.construction_returned),
         completed(input.completed),progress_complete(input.checked.complete),raw_storage(input.raw_storage),
         next_page(input.checked.next_page),rows(input.checked.rows),known(input.checked.known),unknown(input.checked.unknown),
         bytes(input.bytes),ended_phases([&] {if constexpr(requires {input.ended_phases;})return input.ended_phases;else return input.verified_phases;}()),
         inserted([&]() -> std::uint64_t {if constexpr(requires {input.inserted;})return input.inserted;else return 0;}()),
         last_table(input.checked.last_table),stage_cells(input.final_stage.size()),meta_cells(input.final_meta.size()),
         previous_size(input.checked.previous.size()) {
        if(previous_size>previous_digest.size())throw db_error("terminal outer fixed progress history bound");
        std::copy(input.checked.previous.begin(),input.checked.previous.end(),previous_digest.begin());
    }
};
struct database::target_staging_state::terminal_native_outer_tail {
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot,kind;
    const std::uint64_t serial;
    const std::weak_ptr<target_staging_state> state;
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
    const database* const shell;
    const void* const outer_original;
    bool construction_returned=false,binding_attempted=false;
    std::shared_ptr<terminal_native_outer_record> first;
    terminal_native_outer_tail(database& actual,const std::shared_ptr<detail::ordinary_group_target_receiving_owner>& receiving,
        const void* original,std::size_t selected)
        :creator(actual.target_staging_->creator),thread(actual.target_staging_->thread),end(actual.target_staging_->end),
         slot(actual.target_staging_->slot),kind(selected),serial(actual.target_staging_->serial),state(actual.target_staging_),
         owner(receiving),shell(&actual),outer_original(original) {
        if(!actual.target_staging_->current_creator()||!receiving||!original||kind>1)
            throw db_error("actual Native outer constructor identity unavailable");
    }
    terminal_native_outer_tail(const terminal_native_outer_tail&)=delete;
    terminal_native_outer_tail& operator=(const terminal_native_outer_tail&)=delete;
    void constructor_returned()noexcept {
        if(creator==detail::database_native_process()&&thread==std::this_thread::get_id())construction_returned=true;
    }
    bool bind(const std::shared_ptr<terminal_native_outer_record>& issued)noexcept {
        if(creator!=detail::database_native_process()||thread!=std::this_thread::get_id()||binding_attempted||first)return false;
        binding_attempted=true;
        if(!construction_returned||!issued||issued->creator!=creator||issued->thread!=thread||issued->end!=end||
           issued->slot!=slot||issued->serial!=serial||issued->kind!=kind||issued->shell!=shell||
           issued->outer_original!=outer_original||issued->tail_original!=this||issued->state.lock()!=state.lock()||
           issued->owner.lock()!=owner.lock()||!issued->constructor_returned)return false;
        issued->binding_attempted=true;first=issued;issued->binding_returned=true;return true;
    }
    ~terminal_native_outer_tail()noexcept {
        if(creator!=detail::database_native_process()||!first)return;
        first->ending_thread=std::this_thread::get_id();
        if(construction_returned&&first->binding_returned)first->members_destroyed.store(true,std::memory_order_release);
        else first->partial_constructor_destroyed.store(true,std::memory_order_release);
        // Actual final_stage/final_meta/progress/hash and row programme reference
        // have died BEFORE this first member. Shared object death is separate.
        database::wake_target_terminal_native_outer_(state,owner,first->connection);
    }
};
} // namespace lattice
