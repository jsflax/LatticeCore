// Included after actual chain/row/Native cleanup definitions. This finite
// terminal producer ends only their decoded values and SAME charge references.
// Native phases, readers, scopes, programmes, first calls/errors remain held.
#include "../include/lattice/native_retirement_storage.hpp"
#include <type_traits>
#include <atomic>
#include <optional>
#include <memory>
#include <thread>
#include <array>
namespace lattice {
struct database::target_staging_state::terminal_decoder_ending {
    struct capture_record {
        const std::uint64_t creator;
        const std::thread::id thread;
        const detail::ordinary_launch::deadline end;
        const std::size_t slot;
        const std::uint64_t serial;
        const std::weak_ptr<target_staging_state> state;
        const std::weak_ptr<detail::ordinary_group_target_receiving_owner> owner;
        const database* const shell;
        const detail::ordinary_target_staging_connection* const connection;
        const void* const cleanup_scope;
        const std::uint64_t generation;
        const std::size_t kind;
        const void* const phase;
        const void* const phase_scope;
        const std::uint64_t page_index;
        const void* const optional_original;
        const void* const charge_original;
        const bool held_decoded,held_charge;
        const bool decode_entered,decode_returned,decode_exception_returned;
        const bool ordinary_retirement_entered,ordinary_retirement_returned;
        bool allocation_attempted=false,allocation_returned=false,allocation_exception_returned=false;
        bool construction_entered=false,construction_returned=false;
        const void* capsule_original=nullptr;
        bool transfer_attempted=false,transfer_returned=false,source_reset_attempted=false,source_reset_returned=false;
        bool release_attempted=false,release_returned=false;
        std::atomic<bool> payload_members_destroyed{false},partial_constructor_destroyed{false};
        std::thread::id ending_thread;
        std::exception_ptr first_failure;
        template<typename Phase>
        capture_record(const std::shared_ptr<target_staging_state>& original,database& actual,
            const detail::ordinary_target_staging_connection& actual_connection,const void* scope,std::uint64_t gen,
            std::size_t selected,const Phase& input)
            :creator(original->creator),thread(original->thread),end(original->end),slot(original->slot),serial(original->serial),
             state(original),owner(input.owner),shell(&actual),connection(&actual_connection),cleanup_scope(scope),generation(gen),
             kind(selected),phase(&input),phase_scope(input.scope.get()),page_index(input.page_index),
             optional_original(&input.decoded),charge_original(input.decoder_charge.get()),
             held_decoded(input.decoded.has_value()),held_charge(bool(input.decoder_charge)),
             decode_entered(input.decode_entered),decode_returned(input.decode_returned),decode_exception_returned(input.decode_exception_returned),
             ordinary_retirement_entered(input.decoder_retirement_entered),ordinary_retirement_returned(input.decoder_retirement_returned){}
    };
    struct payload_tail {
        const std::shared_ptr<capture_record> original;
        payload_tail(std::shared_ptr<capture_record> issued,const void* capsule):original(std::move(issued)) {
            if(!original||original->creator!=detail::database_native_process()||original->thread!=std::this_thread::get_id()||
               !original->allocation_attempted||original->construction_entered)
                throw db_error("terminal Native decoder actual constructor differs");
            original->construction_entered=true;original->capsule_original=capsule;
        }
        payload_tail(const payload_tail&)=delete;
        payload_tail& operator=(const payload_tail&)=delete;
        ~payload_tail()noexcept {
            if(original->creator!=detail::database_native_process())return;
            original->ending_thread=std::this_thread::get_id();
            if(original->construction_returned)
                original->payload_members_destroyed.store(true,std::memory_order_release);
            else original->partial_constructor_destroyed.store(true,std::memory_order_release);
            // Charge-reference destruction may wake earlier. This actual
            // member-tail publication precedes the existing collector wake.
            database::wake_target_terminal_decoder_payload_(original->state,original->owner,original->connection);
        }
    };
    struct payload_capsule {
        payload_tail ending; // First member: tail follows decoded + charge members.
        std::shared_ptr<detail::ordinary_resource_reservation<true>> charge;
        std::optional<detail::ordinary_history_decoded_page> decoded;
        explicit payload_capsule(std::shared_ptr<capture_record> issued):ending(std::move(issued),this){}
    };
    using custody=detail::native_retirement_storage<std::shared_ptr<payload_capsule>,std::shared_ptr<payload_capsule>>;
    const std::uint64_t creator;
    const std::thread::id thread;
    const detail::ordinary_launch::deadline end;
    const std::size_t slot;
    const std::uint64_t serial;
    const database* const shell;
    const detail::ordinary_target_staging_connection* const connection;
    const detail::ordinary_group_target_receiving_owner* const owner;
    const void* const scope;
    const std::uint64_t generation;
    // Fixed first record-allocation outcomes precede fallible receipt allocation.
    std::array<bool,2> record_allocation_attempted{},record_allocation_returned{},record_allocation_exception_returned{};
    std::array<const void*,2> prepared_phases{},prepared_optionals{},prepared_charges{};
    std::array<std::shared_ptr<capture_record>,2> records{};
    custody held; // Empty ownership prepared BEFORE any actual transfer.
    bool evidence_returned=false,evidence_known=false,preparation_returned=false;
    bool extraction_attempted=false,extraction_returned=false,returned=false;
    std::size_t held_values=0,held_charge_references=0;
    terminal_decoder_payload_result result=terminal_decoder_payload_result::retained_debt;
    std::exception_ptr first_failure;
    terminal_decoder_ending(const std::shared_ptr<target_staging_state>& original,database& actual,
        const detail::ordinary_target_staging_connection& conn,const detail::ordinary_group_target_receiving_owner& receiving,
        const void* cleanup_scope,std::uint64_t gen)
        :creator(original->creator),thread(original->thread),end(original->end),slot(original->slot),serial(original->serial),
         shell(&actual),connection(&conn),owner(&receiving),scope(cleanup_scope),generation(gen){}
};
void database::wake_target_terminal_decoder_payload_(const std::weak_ptr<target_staging_state>& weak_state,
    const std::weak_ptr<detail::ordinary_group_target_receiving_owner>& weak_owner,
    const detail::ordinary_target_staging_connection* connection_original)noexcept {
    try {
        const auto state=weak_state.lock();
        if(!state||!state->current_creator()||state->thread!=std::this_thread::get_id())return;
        const auto connection=state->connection.lock();const auto receiving=weak_owner.lock();
        if(connection&&connection.get()==connection_original&&receiving&&receiving->terminal_inputs_ended(*connection))
            receiving->schedule_collection(); // Existing coalescer, outside all leaves; no result/credit.
    }catch(...){}
}
database::terminal_decoder_payload_result database::retire_target_terminal_decoder_payloads_(
    const detail::ordinary_target_staging_connection& connection,const detail::ordinary_group_target_receiving_owner& receiving)noexcept {
    const auto state=target_staging_;
    if(!state||!state->current_creator()||state->thread!=std::this_thread::get_id()||state->connection.lock().get()!=&connection)
        return terminal_decoder_payload_result::retained_debt;
    if(state->terminal_decoder_attempted) {
        const auto& first=state->terminal_decoder_original;
        if(!first||!first->returned)return terminal_decoder_payload_result::retained_debt;
        if(first->result==terminal_decoder_payload_result::not_held)return first->result; // Unissued DATA only.
        return target_terminal_decoder_payloads_ended_(connection,receiving)?terminal_decoder_payload_result::captures_ended:
            terminal_decoder_payload_result::retained_debt; // Historical reobservation only; first effects never retry.
    }
    const void* scope=nullptr;std::uint64_t generation=0;
    if(!connection.terminal_native_decoder_context_known(receiving,scope,generation)||!scope||!generation||
       !target_staging_native_cleanup_known_(connection))return terminal_decoder_payload_result::retained_debt;
    const auto eligible=[&](const auto& phase) {
        if(!phase)return true; // No issued phase DATA, never a payload ending.
        if(phase.use_count()!=1||phase->shell!=this||phase->owner.lock().get()!=&receiving||
           (phase->decode_entered&&!phase->decode_returned&&!phase->decode_exception_returned)||
           (phase->decoder_retirement_entered&&!phase->decoder_retirement_returned))return false;
        if(phase->decoded&&(!phase->decode_entered||!phase->decode_returned||phase->decode_exception_returned||!phase->decoder_charge))return false;
        return !phase->decoder_charge||receiving.terminal_decoder_charge_identity_matches(connection,phase->decoder_charge.get());
    };
    if((state->history_chain&&!eligible(state->history_chain->active))||
       (state->row_import&&!eligible(state->row_import->active)))return terminal_decoder_payload_result::retained_debt;
    state->terminal_decoder_attempted=true; // First allocation/transfer issuer; no reset even on failure.
    try {
        using ending=target_staging_state::terminal_decoder_ending;
        using capsule=ending::payload_capsule;
        using record=ending::capture_record;
        static_assert(std::is_nothrow_move_constructible_v<detail::ordinary_history_decoded_page>);
        static_assert(std::is_nothrow_move_assignable_v<detail::ordinary_history_decoded_page>);
        static_assert(std::is_nothrow_move_assignable_v<std::optional<detail::ordinary_history_decoded_page>>);
        auto first=std::make_shared<ending>(state,*this,connection,receiving,scope,generation);
        state->terminal_decoder_original=first;state->terminal_decoder_allocation_returned=true;
        first->evidence_returned=true;first->evidence_known=true;
        const auto prepare=[&]<std::size_t Index>(const auto& phase) {
            if(!phase||(!phase->decoded&&!phase->decoder_charge))return;
            first->prepared_phases[Index]=phase.get();first->prepared_optionals[Index]=&phase->decoded;
            first->prepared_charges[Index]=phase->decoder_charge.get();first->record_allocation_attempted[Index]=true;
            std::shared_ptr<record> receipt;
            try {receipt=std::make_shared<record>(state,*this,connection,scope,generation,Index,*phase);
                first->records[Index]=receipt;first->record_allocation_returned[Index]=true;}
            catch(...) {first->record_allocation_exception_returned[Index]=true;throw;}
            receipt->allocation_attempted=true;
            try {
                auto value=std::shared_ptr<capsule>(new capsule(receipt),[](capsule* actual)noexcept {
                    if(actual&&actual->ending.original->creator==detail::database_native_process())delete actual;
                });
                receipt->construction_returned=true;receipt->allocation_returned=true;
                first->held.template get<Index>()=std::move(value);
                first->held_values+=std::size_t(receipt->held_decoded);
                first->held_charge_references+=std::size_t(receipt->held_charge);
            }catch(...) {receipt->allocation_exception_returned=true;if(!receipt->first_failure)receipt->first_failure=std::current_exception();throw;}
        };
        if(state->history_chain)prepare.template operator()<0>(state->history_chain->active);
        if(state->row_import)prepare.template operator()<1>(state->row_import->active);
        first->preparation_returned=true; // BOTH empty capsules exist before any original reference moves.
        first->extraction_attempted=true;
        const auto transfer=[&]<std::size_t Index>(const auto& phase) {
            const auto& receipt=first->records[Index];if(!receipt)return;
            auto& value=first->held.template get<Index>();
            if(!phase||phase.get()!=receipt->phase||&phase->decoded!=receipt->optional_original||
               phase->decoder_charge.get()!=receipt->charge_original||!value||value.use_count()!=1)
                throw db_error("terminal Native decoder actual extraction identity changed");
            receipt->transfer_attempted=true;
            value->decoded=std::move(phase->decoded);value->charge=std::move(phase->decoder_charge);
            receipt->transfer_returned=true;
            if(receipt->held_decoded) {
                // optional move keeps the source engaged. Its actual moved-
                // from value must die too; no NULL/copy stands in for reset.
                receipt->source_reset_attempted=true;phase->decoded.reset();receipt->source_reset_returned=true;
            }
        };
        if(state->history_chain)transfer.template operator()<0>(state->history_chain->active);
        if(state->row_import)transfer.template operator()<1>(state->row_import->active);
        first->extraction_returned=true;
        const auto release=[&]<std::size_t Index>() {
            const auto& receipt=first->records[Index];if(!receipt)return true;
            receipt->release_attempted=true;
            const bool returned=first->held.template release<Index>(); // Actual decoded/charge members die outside ALL leaves.
            if(!state->current_creator()||!returned)return false;
            receipt->release_returned=true;
            return receipt->payload_members_destroyed.load(std::memory_order_acquire)&&receipt->ending_thread==state->thread&&
                !receipt->partial_constructor_destroyed.load(std::memory_order_acquire);
        };
        if(!release.template operator()<0>()||!release.template operator()<1>())return terminal_decoder_payload_result::retained_debt;
        if(!connection.terminal_receiver_input_history_matches(receiving,scope,generation))return terminal_decoder_payload_result::retained_debt;
        first->result=(first->held_values||first->held_charge_references)?terminal_decoder_payload_result::captures_ended:
            terminal_decoder_payload_result::not_held;
        first->returned=true;return first->result;
    }catch(...) {
        if(state->current_creator()) {
            if(!state->terminal_decoder_allocation_returned)state->terminal_decoder_allocation_exception_returned=true;
            if(!state->terminal_decoder_first_failure)state->terminal_decoder_first_failure=std::current_exception();
            if(state->terminal_decoder_original&&!state->terminal_decoder_original->first_failure)
                state->terminal_decoder_original->first_failure=std::current_exception();
        }
        // Remaining real custody stays in the same staging-state bank. No
        // ordinary flags, first calls/errors, deadline, phase or charge reset.
        return terminal_decoder_payload_result::retained_debt;
    }
}
bool database::target_terminal_decoder_payloads_ended_(const detail::ordinary_target_staging_connection& connection,
    const detail::ordinary_group_target_receiving_owner& receiving)const noexcept {
    try {
        const auto state=target_staging_;const auto first=state?state->terminal_decoder_original:nullptr;
        if(!state||!state->current_creator()||state->thread!=std::this_thread::get_id()||state->connection.lock().get()!=&connection||
           !state->terminal_decoder_attempted||!state->terminal_decoder_allocation_returned||state->terminal_decoder_allocation_exception_returned||
           state->terminal_decoder_first_failure||!first||first->creator!=state->creator||first->thread!=state->thread||
           first->end!=state->end||first->slot!=state->slot||first->serial!=state->serial||first->shell!=this||
           first->connection!=&connection||first->owner!=&receiving||!first->scope||!first->generation||
           !first->evidence_returned||!first->evidence_known||!first->preparation_returned||!first->extraction_attempted||
           !first->extraction_returned||!first->returned||first->result!=terminal_decoder_payload_result::captures_ended||first->first_failure||
           (!first->held_values&&!first->held_charge_references)||!receiving.terminal_inputs_ended(connection)||
           !connection.terminal_receiver_input_history_matches(receiving,first->scope,first->generation))return false;
        const auto known=[&]<std::size_t Index>(const auto& phase) {
            const auto& receipt=first->records[Index];if(!receipt)return true; // Unissued-slot DATA, no positive capture.
            if(!phase||!first->record_allocation_attempted[Index]||!first->record_allocation_returned[Index]||
               first->record_allocation_exception_returned[Index]||first->prepared_phases[Index]!=receipt->phase||
               first->prepared_optionals[Index]!=receipt->optional_original||first->prepared_charges[Index]!=receipt->charge_original||
               phase.get()!=receipt->phase||phase->shell!=this||phase->owner.lock().get()!=&receiving||
               phase->scope.get()!=receipt->phase_scope||phase->page_index!=receipt->page_index||&phase->decoded!=receipt->optional_original||
               receipt->creator!=state->creator||receipt->thread!=state->thread||receipt->end!=state->end||receipt->slot!=state->slot||
               receipt->serial!=state->serial||receipt->shell!=this||receipt->connection!=&connection||receipt->cleanup_scope!=first->scope||
               receipt->generation!=first->generation||receipt->kind!=Index||receipt->state.lock()!=state||receipt->owner.lock().get()!=&receiving||
               !receipt->allocation_attempted||!receipt->allocation_returned||receipt->allocation_exception_returned||
               !receipt->construction_entered||!receipt->construction_returned||!receipt->capsule_original||receipt->first_failure||
               !receipt->transfer_attempted||!receipt->transfer_returned||!receipt->release_attempted||!receipt->release_returned||
               (receipt->held_decoded&&(!receipt->source_reset_attempted||!receipt->source_reset_returned))||
               !receipt->payload_members_destroyed.load(std::memory_order_acquire)||receipt->ending_thread!=state->thread||
               receipt->partial_constructor_destroyed.load(std::memory_order_acquire)||
               phase->decode_entered!=receipt->decode_entered||phase->decode_returned!=receipt->decode_returned||
               phase->decode_exception_returned!=receipt->decode_exception_returned||
               phase->decoder_retirement_entered!=receipt->ordinary_retirement_entered||
               phase->decoder_retirement_returned!=receipt->ordinary_retirement_returned||phase->decoded||phase->decoder_charge)return false;
            return !receipt->held_charge||receiving.terminal_decoder_charge_identity_matches(connection,receipt->charge_original);
        };
        if(state->history_chain) {if(!known.template operator()<0>(state->history_chain->active))return false;}
        else if(first->records[0])return false;
        if(state->row_import) {if(!known.template operator()<1>(state->row_import->active))return false;}
        else if(first->records[1])return false;
        return true; // Only these actual decoded values/charge references; no Native/owner/credit join.
    }catch(...){return false;}
}
} // namespace lattice
