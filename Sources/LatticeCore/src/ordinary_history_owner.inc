// Included only by ordinary_export.cpp. Actual owning child survives all page
// leases, and its public native resource slot is released last.
namespace {
std::uint64_t history_creator() noexcept {
#if defined(__APPLE__) || defined(__linux__)
    return static_cast<std::uint64_t>(::getpid());
#else
    return 0; // Non-fork platforms retain existing generic capture behavior.
#endif
}
}
// Optional per-reservation DATA. No callback, owner, payload, registry or
// permission is retained; an observer can outlive the Native tail harmlessly.
struct ordinary_history_refund_cell {
    const std::uint64_t creator;
    explicit ordinary_history_refund_cell(std::uint64_t original_creator):creator(original_creator){}
    std::atomic<ordinary_history_refund_status> phase{ordinary_history_refund_status::held};
};
static_assert(sizeof(ordinary_history_refund_cell)<=64);
ordinary_history_refund_status ordinary_history_refund_observation::status()const noexcept {
    if(!cell_)return ordinary_history_refund_status::unavailable;
    if(cell_->creator!=history_creator())return ordinary_history_refund_status::inherited;
    return cell_->phase.load(std::memory_order_acquire);
}
namespace {
using history_refund_cell=std::shared_ptr<ordinary_history_refund_cell>;
history_refund_cell history_refund_load(const history_refund_cell& source)noexcept {
    return std::atomic_load_explicit(&source,std::memory_order_acquire);
}
history_refund_cell history_refund_observe(history_refund_cell& source,std::uint64_t original_creator) {
    if(original_creator!=history_creator())throw db_error("inherited history refund observation");
    auto observed=history_refund_load(source);
    if(observed&&observed->creator!=original_creator)throw db_error("history refund observation creator differs");
    if(!observed){
        // Explicit observer allocation only, outside every Native ownership
        // leaf. A losing candidate contains DATA only and issues no authority.
        auto candidate=std::make_shared<ordinary_history_refund_cell>(original_creator);
        // A process allocator can cross a fork. Never publish into inherited
        // Native storage or bind the cell to the allocator continuation PID.
        if(original_creator!=history_creator())throw db_error("history refund allocation crossed creator");
        if(std::atomic_compare_exchange_strong_explicit(&source,&observed,candidate,
            std::memory_order_acq_rel,std::memory_order_acquire))observed=std::move(candidate);
    }
    if(original_creator!=history_creator())throw db_error("history refund publication crossed creator");
    return observed;
}
void history_refund_quarantine(const history_refund_cell& observed)noexcept {
    if(!observed||observed->creator!=history_creator())return;
    auto expected=ordinary_history_refund_status::held;
    (void)observed->phase.compare_exchange_strong(expected,ordinary_history_refund_status::quarantined,
        std::memory_order_release,std::memory_order_relaxed);
}
void history_refund_returned(const history_refund_cell& observed)noexcept {
    if(!observed||observed->creator!=history_creator())return;
    // A contradictory/quarantined outcome never becomes success. Only the
    // actual original reservation's returned destructor reaches this helper.
    auto expected=ordinary_history_refund_status::held;
    (void)observed->phase.compare_exchange_strong(expected,ordinary_history_refund_status::refunded,
        std::memory_order_release,std::memory_order_relaxed);
}
}

namespace { history_refund_cell history_refund_for_cut(const ordinary_history_export_state&)noexcept; }
struct ordinary_mounted_history_source_state {
    // Nil unless a source-private test explicitly observes this SAME slot.
    history_refund_cell refund_observation;
    // Optional makes callback-capable release explicit, last and off-leaf.
    std::optional<ordinary_resource_reservation<false>> reservation;
    std::shared_ptr<lattice_db> owner;
    ordinary_history_limits limits;
    std::vector<std::string> mounted;
    std::string scope,schema,device,inode;
    const std::uint64_t creator=history_creator();
    std::mutex leaf;
    std::weak_ptr<ordinary_history_export_state> active;
    ordinary_history_export_state* retained_cut=nullptr; // Actual cut object/native fields remain occupied through destruction.
    const void* retained_construction=nullptr; // Real failure prefix may retain the SAME mounted admission.
    bool capturing=false;
    ordinary_mounted_history_source_state(ordinary_resource_reservation<false>&& charge,std::shared_ptr<lattice_db> held,
        const ordinary_history_limits& policy,std::vector<std::string> roots)
        :reservation(std::move(charge)),owner(std::move(held)),limits(policy),mounted(std::move(roots)){}
    static void dispose(ordinary_mounted_history_source_state* value)noexcept {
        if(!value||value->creator!=history_creator())return; // Reuse the inherited record as quarantine.
        auto observed=std::move(value->refund_observation); // Strong-zero: no observer factory can still access this state.
        value->owner.reset();if(value->creator!=history_creator())return;
        std::vector<std::string>{}.swap(value->mounted);std::string{}.swap(value->scope);std::string{}.swap(value->schema);std::string{}.swap(value->device);std::string{}.swap(value->inode);
        value->reservation.reset();if(value->creator!=history_creator())return;
        delete value;
        history_refund_returned(observed); // Actual common reservation reset AND storage destruction returned.
    }
};
// Preallocated final custody uses one real quota/registration; raw marker clears
// only after all admitted factory/deleter tails and native closes are proved.
struct ordinary_history_destruction_tail {
    // Standalone reservations move this exact DATA cell with their real tail.
    history_refund_cell refund_observation;
    const std::uint64_t creator=history_creator();
    std::optional<ordinary_resource_reservation<false>> reservation;
    std::shared_ptr<ordinary_mounted_history_source_state> mounted;
    const void* construction=nullptr;
    ordinary_history_export_state* cut=nullptr;
    static void finish(void* raw)noexcept {
        auto* value=static_cast<ordinary_history_destruction_tail*>(raw);
        if(!value||value->creator!=history_creator())return;
        auto observed=std::move(value->refund_observation);
        value->reservation.reset();if(value->creator!=history_creator())return;
        auto mounted=std::move(value->mounted);const auto creator=value->creator;
        const auto construction=value->construction;const auto cut=value->cut;
        delete value; // Tail storage and all other real fields are gone before marker publication.
        if(creator!=history_creator())return;
        if(mounted){
            {std::lock_guard lock(mounted->leaf);
                if(construction&&mounted->retained_construction==construction)mounted->retained_construction=nullptr;
                if(cut&&mounted->retained_cut==cut)mounted->retained_cut=nullptr;}
            mounted.reset(); // Registration alone publishes its common refund, not this cut.
        } else history_refund_returned(observed); // Exact standalone reset + tail storage destruction returned.
    }
    static void dispose(ordinary_history_destruction_tail* value)noexcept {finish(value);}
};
struct ordinary_history_construction_tail {
    const std::uint64_t creator=history_creator();
    std::optional<ordinary_resource_reservation<false>> reservation;
    std::shared_ptr<ordinary_mounted_history_source_state> mounted;
    std::shared_ptr<ordinary_history_child_connections> connections;
    ordinary_history_destruction_tail* tail;
    bool published=false;
    ordinary_history_construction_tail(std::optional<ordinary_resource_reservation<false>>&& charge,
        std::shared_ptr<ordinary_mounted_history_source_state> registration,ordinary_history_destruction_tail* retained)
        :reservation(std::move(charge)),mounted(std::move(registration)),tail(retained){}
    static void dispose(ordinary_history_construction_tail* value)noexcept {
        if(!value||value->creator!=history_creator())return;
        if(value->published){
            auto* unused=value->tail;value->tail=nullptr;
            value->connections.reset();if(value->creator!=history_creator())return;
            auto mounted=std::move(value->mounted);const auto completed=value;const auto creator=value->creator;
            delete value;ordinary_history_destruction_tail::dispose(unused);
            if(creator!=history_creator())return;
            if(mounted){ {std::lock_guard lock(mounted->leaf);if(mounted->retained_construction==completed)mounted->retained_construction=nullptr;}mounted.reset();}
            return;
        }
        auto* tail=value->tail;tail->construction=value;
        tail->mounted=std::move(value->mounted);
        if(value->reservation){tail->reservation.emplace(std::move(*value->reservation));value->reservation.reset();}
        auto connections=std::move(value->connections);
        if(connections&&!connections->seal_fixed_scope())return;
        delete value; // All prefix temporaries and child unwind preceded this deleter.
        if(tail->creator!=history_creator())return;
        if(connections)connections->handoff(tail,&ordinary_history_destruction_tail::finish);
        else ordinary_history_destruction_tail::finish(tail); // Factory never entered native allocation without a ledger.
        // Local ledger release is creator-aware; no subsequent leaf access.
    }
};
using history_construction_custody=std::unique_ptr<ordinary_history_construction_tail,decltype(&ordinary_history_construction_tail::dispose)>;
history_construction_custody history_retain_construction(std::optional<ordinary_resource_reservation<false>>&& charge,
    const std::shared_ptr<ordinary_mounted_history_source_state>& mounted){
    std::unique_ptr<ordinary_history_destruction_tail,decltype(&ordinary_history_destruction_tail::dispose)> tail(
        new ordinary_history_destruction_tail(),&ordinary_history_destruction_tail::dispose);
    history_construction_custody result(new ordinary_history_construction_tail(std::move(charge),mounted,tail.get()),&ordinary_history_construction_tail::dispose);
    tail.release();
    if(result->creator!=history_creator())refuse("inherited history construction custody");
    if(mounted){std::lock_guard lock(mounted->leaf);mounted->retained_construction=result.get();}
    return result;
}
struct ordinary_history_export_state {
    // Mounted cuts use mounted->refund_observation; never a second allowance.
    history_refund_cell refund_observation;
    std::optional<ordinary_resource_reservation<false>> reservation;
    std::shared_ptr<ordinary_mounted_history_source_state> mounted;

    std::shared_ptr<lattice_db> owner;
    ordinary_history_limits limits;
    ordinary_history_stage_record record;
    std::optional<ordinary_history_prior_source_observation> prior;
    std::string mounted_relation; // Written once before clean publication; passive, cut-owned.
    const std::uint64_t creator=history_creator();
    std::mutex leaf;
    std::weak_ptr<const ordinary_history_page_state> outstanding;
    const ordinary_history_page_state* retained_page=nullptr;
    // Retained actual close facts poison this SAME occupied cut on uncertainty.
    std::shared_ptr<ordinary_history_child_connections> child_connections;
    std::exception_ptr disposal_error,disposal_cleanup_error;
    ordinary_history_destruction_tail* destruction_tail=nullptr;
    bool reading=false;
    ordinary_history_export_state(ordinary_history_destruction_tail* tail,std::optional<ordinary_resource_reservation<false>>&& charge,
        std::shared_ptr<ordinary_mounted_history_source_state> registration,std::shared_ptr<lattice_db> retained,
        ordinary_history_limits policy,ordinary_history_stage_record captured,
        std::shared_ptr<ordinary_history_child_connections> connections,
        std::optional<ordinary_history_prior_source_observation> old=std::nullopt,std::string relation={})
        :reservation(std::move(charge)),mounted(std::move(registration)),owner(std::move(retained)),limits(policy),record(std::move(captured)),prior(std::move(old)),mounted_relation(std::move(relation)),child_connections(std::move(connections)),destruction_tail(tail){}
    static void dispose(ordinary_history_export_state* value)noexcept {
        if(!value||value->creator!=history_creator())return;
        // A strong-zero/expired weak pointer is not child/native destruction.
        // Keep the actual state + same charge on any unproved native close.
        const auto observed=history_refund_for_cut(*value);
        if(!value->child_connections){history_refund_quarantine(observed);return;} // Missing real witnesses never refund.
        // Seal before close: deny new factories, while already admitted native
        // operations retain their exact entry through actual later destruction.
        if(!value->child_connections->seal_fixed_scope()){history_refund_quarantine(observed);return;}
        auto closed=value->owner->close_checked();
        // Moves leave the stack report empty even if a callback forked; no
        // last exception target is automatically released in the child.
        value->disposal_error=std::move(closed.error);
        value->disposal_cleanup_error=std::move(closed.cleanup_error);
        if(value->creator!=history_creator())return;
        if(!closed.cleanup_complete||value->disposal_cleanup_error){history_refund_quarantine(observed);return;}
        value->owner.reset();if(value->creator!=history_creator())return;
        // Pending off-list natives keep the preallocated SAME tail, then reclaim on their genuine last-deleter return.
        // Clear any callback-capable witness/exception targets separately, and
        // recheck native PID after EACH release before further inherited work.
        value->disposal_error=nullptr;if(value->creator!=history_creator())return;
        value->disposal_cleanup_error=nullptr;if(value->creator!=history_creator())return;
        auto connections=std::move(value->child_connections);
        auto* tail=value->destruction_tail;
        tail->mounted=std::move(value->mounted);
        if(value->reservation){tail->reservation.emplace(std::move(*value->reservation));value->reservation.reset();}
        tail->refund_observation=std::move(value->refund_observation);
        tail->cut=value;
        // Child logical cleanup returned, but actual off-list wrappers may
        // remain live. All cut metadata/object storage dies before handoff.
        delete value;
        if(tail->creator!=history_creator())return;
        connections->handoff(tail,&ordinary_history_destruction_tail::finish);
    }

};

namespace {
history_refund_cell history_refund_for_cut(const ordinary_history_export_state& original)noexcept {
    return original.mounted?history_refund_load(original.mounted->refund_observation):history_refund_load(original.refund_observation);
}
}
ordinary_history_refund_observation ordinary_export_access::observe_history_refund(const ordinary_history_export& original) {
    const auto state=original.state_; // Actual strong owner fences observation attachment against disposal.
    if(!state||state->creator!=history_creator())throw db_error("history refund observation lacks creator-owned cut");
    auto observed=state->mounted?history_refund_observe(state->mounted->refund_observation,state->creator):history_refund_observe(state->refund_observation,state->creator);
    return ordinary_history_refund_observation(std::move(observed));
}
ordinary_history_refund_observation ordinary_export_access::observe_history_refund(const ordinary_mounted_history_source& original) {
    const auto state=original.state_;
    if(!state||state->creator!=history_creator())throw db_error("history refund observation lacks creator-owned registration");
    return ordinary_history_refund_observation(history_refund_observe(state->refund_observation,state->creator));
}
bool ordinary_export_access::history_has_refund_observation(const ordinary_history_export& original)noexcept {
    const auto state=original.state_;
    return state&&state->creator==history_creator()&&bool(history_refund_for_cut(*state));
}
lattice_close_result ordinary_export_access::close_history_child_for_test(const ordinary_history_export& original) {
    const auto state=original.state_;
    if(!state||state->creator!=history_creator()||!state->owner)throw db_error("actual history test child unavailable");
    return state->owner->close_checked();
}
std::shared_ptr<database> ordinary_export_access::history_child_xproc_for_test(const ordinary_history_export& original) {
    const auto state=original.state_;
    if(!state||state->creator!=history_creator()||!state->owner)throw db_error("actual history test child unavailable");
    return state->owner->borrow_xproc_read_connection();
}
catalog_stream_operation ordinary_export_access::history_child_catalog_for_test(const ordinary_history_export& original,
    const std::string& request,int64_t budget) {
    const auto state=original.state_;
    if(!state||state->creator!=history_creator()||!state->owner)throw db_error("actual history test child unavailable");
    return make_catalog_stream_operation(state->owner,request,budget);
}

struct ordinary_history_page_state {
    // Real immutable cut ownership survives page bytes on every final thread.
    std::shared_ptr<ordinary_history_export_state> owner;
    uint64_t index=0;
    std::vector<uint8_t> bytes;
    const std::uint64_t creator=history_creator();
    explicit ordinary_history_page_state(std::shared_ptr<ordinary_history_export_state> retained,uint64_t at)
        :owner(std::move(retained)),index(at){}
    static void dispose(ordinary_history_page_state* value)noexcept {
        if(!value||value->creator!=history_creator())return;
        auto owner=std::move(value->owner);const auto creator=value->creator;const auto completed=value;
        delete value; // actual vector and ALL page members/object storage are gone
        if(creator!=history_creator())return; // cut deleter quarantines inherited owner
        {std::lock_guard lock(owner->leaf);
            if(owner->retained_page==completed)owner->retained_page=nullptr;}
        owner.reset(); // may finally dispose the actual cut; no later leaf access
    }
};
namespace {
// Allocation is the real supplied-allocator shared_ptr control-block boundary.
// A fault here executes that constructor's armed deleter exception guard.
// Production's null hook delegates directly to std::allocator, no fake receipt.
template<class T> struct history_cut_control_allocator {
    using value_type=T;
    const std::uint64_t creator=history_creator();
    history_cut_control_allocator()=default;
    template<class U> history_cut_control_allocator(const history_cut_control_allocator<U>& other)noexcept:creator(other.creator){}
    T* allocate(size_t count) {
        if(creator!=history_creator())refuse("inherited history control-block allocation");
        if(ordinary_wire_test_hooks::before_history_cut_control_block_allocate)ordinary_wire_test_hooks::before_history_cut_control_block_allocate();
        if(creator!=history_creator())refuse("history control-block allocation crossed creator");
        return std::allocator<T>{}.allocate(count);
    }
    void deallocate(T* value,size_t count)noexcept {
        if(creator==history_creator())std::allocator<T>{}.deallocate(value,count);
    }
    template<class U> bool operator==(const history_cut_control_allocator<U>& other)const noexcept{return creator==other.creator;}
};
void history_process(const ordinary_history_export_state& state){if(state.creator!=history_creator())refuse("inherited history cut");}
std::shared_ptr<ordinary_history_export_state> history_owned_state(
    ordinary_history_construction_tail& construction,std::shared_ptr<ordinary_mounted_history_source_state> mounted,
    std::shared_ptr<lattice_db> owner,const ordinary_history_limits& limits,ordinary_history_stage_record record,
    std::shared_ptr<ordinary_history_child_connections> connections,
    std::optional<ordinary_history_prior_source_observation> prior=std::nullopt,std::string mounted_relation={}){
    // Bound the SUM of native registration + current-cut retention, rather
    // than giving each owner a separate eight-MiB allowance on one slot.
    size_t bytes=history_capacity_retained_payload+sizeof(ordinary_history_refund_cell)+sizeof(ordinary_history_export_state)+sizeof(ordinary_history_destruction_tail)+sizeof(ordinary_history_page_state)+sizeof(ordinary_history_construction_tail)+sizeof(ordinary_history_child_connections)+database_open_ledger::maximum_opens*(sizeof(database_retirement_state)+1024)+record.header.size()+record.identity.stage.size()+record.identity.source.size()+record.identity.scope.size()+record.identity.schema.size()+record.identity.device.size()+record.identity.inode.size()+record.content_digest.size()+record.seal.size()+4096;
    if(prior)bytes+=prior->scope.size()+prior->source.size()+prior->schema_digest.size()+sizeof(*prior);
    if(mounted_relation.size()>262144)refuse("mounted relation retained byte bound");bytes+=mounted_relation.capacity();
    if(mounted){bytes+=history_capacity_retained_payload+sizeof(*mounted)+mounted->scope.size()+mounted->schema.size()+mounted->device.size()+mounted->inode.size()+mounted->mounted.size()*sizeof(std::string);for(const auto& name:mounted->mounted)bytes+=name.size();}
    if(bytes>ordinary_history_limits::owner_bytes)refuse("combined history owner retention bound");
    if(construction.creator!=history_creator()||construction.published||!construction.tail)
        refuse("history cut lacks its real unpublished construction custody");
    // Keep the original prefix tail and SAME charge until all actual cut
    // fields are constructed. Allocation/partial construction failure here
    // leaves the prefix as the sole physical-retirement completion owner.
    std::unique_ptr<ordinary_history_export_state,decltype(&ordinary_history_export_state::dispose)> actual(
        new ordinary_history_export_state(construction.tail,{},std::move(mounted),std::move(owner),limits,std::move(record),std::move(connections),std::move(prior),std::move(mounted_relation)),
        &ordinary_history_export_state::dispose);
    // All operations in this transfer are nonthrowing: the resource move is
    // noexcept, and reset destroys only its now-inactive reservation. Mark the
    // prefix transferred BEFORE fallible mounted publication/shared_ptr setup.
    if(construction.reservation){actual->reservation.emplace(std::move(*construction.reservation));construction.reservation.reset();}
    construction.tail=nullptr;construction.published=true;
    bool denied=false;
    if(actual->mounted){
        {std::lock_guard lock(actual->mounted->leaf);
            denied=actual->mounted->retained_cut!=nullptr;
            if(!denied)actual->mounted->retained_cut=actual.get();}
    }
    if(denied)refuse("mounted cut already retains native destruction custody");
    // release() precedes constructor entry; its real allocator failure guard
    // invokes the SAME cut deleter exactly once. The transferred prefix may
    // clear its own raw construction marker only after prefix storage dies.
    return {actual.release(),&ordinary_history_export_state::dispose,history_cut_control_allocator<ordinary_history_export_state>{}};
}
ordinary_history_stage_record history_decode_stage(const values& row,const ordinary_history_limits& limits){
    if(row.size()!=18||integer(row[1])!=1||integer(row[9])!=1)refuse("history source stage is not complete");
    ordinary_history_stage_record record;
    record.identity={text(row[0]),text(row[2]),text(row[5]),text(row[6]),text(row[7]),text(row[8]),integer(row[3]),integer(row[4]),false};
    if(!canonical_uuid(record.identity.stage)||!canonical_uuid(record.identity.source)||record.identity.epoch<=0||record.identity.cut<=0)refuse("history stage coordinate invalid");
    for(size_t n=11;n<=15;++n)if(integer(row[n])<0)refuse("history stage count invalid");
    record.totals={uint64_t(integer(row[11])),uint64_t(integer(row[12])),uint64_t(integer(row[13])),uint64_t(integer(row[14])),uint64_t(integer(row[15]))};
    if(record.totals.rows>limits.records||record.totals.known>limits.records-record.totals.rows||record.totals.unknown>limits.records-record.totals.rows-record.totals.known||record.totals.pages>limits.pages||record.totals.bytes>limits.durable_stage_bytes)refuse("history resumed stage exceeds local profile");
    const auto* header=std::get_if<std::vector<uint8_t>>(&row[10]);if(!header||header->empty()||header->size()>limits.page_bytes)refuse("history stage header unavailable");
    if(header->size()+row.size()*sizeof(column_value_t)>ordinary_history_limits::owner_bytes)refuse("history owner retention bound");
    record.header.assign(reinterpret_cast<const char*>(header->data()),header->size());record.content_digest=text(row[16]);record.seal=text(row[17]);record.sealed=true;return record;
}
void history_source_context(database& writer,const ordinary_history_stage_record& record,budget& scratch){
    original_view(writer,true);
    if(!history_storage(ordinary_export_storage_access::borrow(writer),scratch)||!inspect_storage(ordinary_export_storage_access::borrow(writer),scratch))refuse("history source storage unavailable");
    const auto physical=writer.physical_identity("main",{},true);if(!physical||record.identity.device!=std::to_string(physical->device)||record.identity.inode!=std::to_string(physical->inode))refuse("history source file replaced");
    const auto binding=read(ordinary_export_storage_access::borrow(writer),"SELECT source,epoch,cut,device,inode,schema FROM main._lattice_ordered_source_v1 WHERE scope=?",{record.identity.scope},1,scratch);
    if(binding.size()!=1||binding[0][0]!=column_value_t(record.identity.source)||binding[0][1]!=column_value_t(record.identity.epoch)||integer(binding[0][2])<record.identity.cut||binding[0][3]!=column_value_t(record.identity.device)||binding[0][4]!=column_value_t(record.identity.inode)||binding[0][5]!=column_value_t(record.identity.schema))refuse("history stage source epoch/schema continuity unavailable");
    const auto actual=history_stage_row(writer,record.identity.stage,scratch);if(text(actual[17])!=record.seal)refuse("history stage changed after issue");
}
}
std::shared_ptr<const ordinary_history_page_state> ordinary_export_access::history_page(
    const std::shared_ptr<ordinary_history_export_state>& state,uint64_t index){
    if(!state)refuse("history page index unavailable");history_process(*state);
    if(index>state->record.totals.pages)refuse("history page index unavailable");
    std::shared_ptr<const ordinary_history_page_state> existing;bool refused=false;
    {
        std::lock_guard lock(state->leaf);existing=state->outstanding.lock();
        if(existing)refused=existing->index!=index;
        else if(state->reading||state->retained_page)refused=true;
        else state->reading=true;
    }
    // The temporary retained page and its captures are released off the leaf.
    if(refused)refuse("history page lease/read already admitted");if(existing)return existing;
    struct read_end {ordinary_history_export_state& state;~read_end(){if(state.creator==history_creator()){std::lock_guard lock(state.leaf);state.reading=false;}}} settle{*state};
    auto* actual=new ordinary_history_page_state(state,index);
    {std::lock_guard lock(state->leaf);state->retained_page=actual;}
    // Control-block failure invokes the same real destruction-return deleter.
    auto result=std::shared_ptr<ordinary_history_page_state>(actual,&ordinary_history_page_state::dispose);
    const auto committed=recovery_writer_access::ordinary_owned_write(*state->owner,[&](database& writer){
        require_managed_profile_if_present(*state->owner,writer,&state->limits);
        budget scratch{history_stage_limits};history_source_context(writer,state->record,scratch);
        if(index==0){scratch.add(state->record.header.size());result->bytes.assign(state->record.header.begin(),state->record.header.end());}
        else {
            const auto pages=read(ordinary_export_storage_access::borrow(writer),"SELECT stage,page,previous,digest,payload,rows,known,unknown,seal FROM main._lattice_ordered_history_page_v1 WHERE stage=? AND page=?",{state->record.identity.stage,int64_t(index-1)},1,scratch);
            if(pages.size()!=1)refuse("history page disappeared");const auto& page=pages[0];const auto* bytes=std::get_if<std::vector<uint8_t>>(&page[4]);
            if(!bytes||bytes->empty()||bytes->size()>state->limits.page_bytes||text(page[8])!=history_seal("ordinary-paged-page-v1",page,8)||text(page[3])!=picosha2::hash256_hex_string(bytes->begin(),bytes->end()))refuse("history page content changed");
            scratch.add(bytes->size());result->bytes=*bytes;
        }
        history_source_context(writer,state->record,scratch);
    });
    if(committed.state!=recovery_install_state::committed||committed.primary_error||committed.cleanup_error||committed.postcommit_error||committed.notification_error)throw ordinary_order_write_error(committed);
    // No native ownership/SQLite leaf is held during future socket use. The
    // buffer remains owned until the caller releases all copies after send.
    history_process(*state);
    {std::lock_guard lock(state->leaf);state->outstanding=result;}
    return result;
}
