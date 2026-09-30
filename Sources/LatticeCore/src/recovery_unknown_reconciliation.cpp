#include "recovery_unknown_reconciliation.hpp"
#include "recovery_receiver_controller.hpp"
#include "recovery_receiver_source.hpp"
#include <algorithm>
#include <limits>

namespace lattice::detail {
namespace {
[[noreturn]] void refuse(const char* message){throw db_error(message);}
recovery_export_preparation pending(){recovery_export_preparation result;result.protected_store=true;return result;}
void settled(const recovery_install_result& result){
    for(const auto& error:{result.primary_error,result.cleanup_error,result.postcommit_error,result.notification_error})
        if(error)std::rethrow_exception(error);
    if(result.state!=recovery_install_state::committed||result.unexpected_commit_observed)
        refuse("reconciliation first-claim transaction did not prove its own COMMIT");
}
}

recovery_unknown_reconciliation::recovery_unknown_reconciliation(
    const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor)
    :descriptor_(descriptor),contributions_(descriptor->contributions_.size()) {
    const auto cap=descriptor->limits_.obligations.records;
    if(cap<=0||cap>100000)refuse("reconciliation first-claim capacity differs");
    size_t count=0;
    for(size_t n=0;n<descriptor->contributions_.size();++n){
        const auto size=descriptor->contributions_[n].journal.entries.size();
        if(size>static_cast<size_t>(cap)-count)refuse("reconciliation first-claim inventory exceeds capacity");
        claim_offsets_[n]=count;count+=size;
    }
    claim_offsets_[descriptor->contributions_.size()]=count;
    auto claims=std::make_shared<std::vector<int64_t>>();claims->reserve(count);
    for(const auto& part:descriptor->contributions_)for(const auto& entry:part.journal.entries){
        const auto first=entry.first_export_claim.value_or(0);
        if(first<0||(entry.first_export_claim&&first==0))refuse("reconciliation descriptor first claim invalid");
        claims->push_back(first);
    }
    claim_capacity_=claims->capacity();
    first_claims_=std::move(claims);
}

std::shared_ptr<recovery_unknown_reconciliation> recovery_unknown_reconciliation::acquire(
    const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor) {
    if(!descriptor||descriptor->contributions_.empty()||descriptor->contributions_.size()>16)
        refuse("reconciliation descriptor contribution capacity differs");
    {std::lock_guard lock(descriptor->worker_mutex_);
        if(descriptor->worker_)return descriptor->worker_;
        // Defer before allocation: competing routes cannot retain duplicate
        // first-claim vectors while the one admitted constructor runs off leaf.
        if(descriptor->worker_initializing_)return {};
        descriptor->worker_initializing_=true;}
    std::shared_ptr<recovery_unknown_reconciliation> candidate;
    try{candidate=std::shared_ptr<recovery_unknown_reconciliation>(new recovery_unknown_reconciliation(descriptor));}
    catch(...){std::lock_guard lock(descriptor->worker_mutex_);descriptor->worker_initializing_=false;throw;}
    {std::lock_guard lock(descriptor->worker_mutex_);descriptor->worker_=candidate;descriptor->worker_initializing_=false;}
    return candidate;
}
void recovery_unknown_reconciliation::require_live_locked()const {
    if(failure_)std::rethrow_exception(failure_);
    if(abandoned_)refuse("reconciliation window abandoned; current source generation remains closed");
}
void recovery_unknown_reconciliation::fail(std::exception_ptr error)noexcept {
    std::lock_guard lock(mutex_);if(!failure_)failure_=std::move(error);settling_=false;
}
std::shared_ptr<recovery_reconciliation_operation> recovery_unknown_reconciliation::begin_operation(bool claims) {
    auto operation=std::shared_ptr<recovery_reconciliation_operation>(new recovery_reconciliation_operation);
    operation->worker_=shared_from_this();operation->descriptor_=descriptor_.lock();
    if(!operation->descriptor_)refuse("reconciliation continuity descriptor expired");
    {
        std::lock_guard lock(mutex_);require_live_locked();
        if(active_operation_||settling_||completed_)return {};
        if(next_operation_==std::numeric_limits<uint64_t>::max())refuse("reconciliation continuity sequence exhausted");
        operation->before_=first_claims_;operation->sequence_=++next_operation_;active_operation_=operation->sequence_;
    }
    if(claims){
        operation->candidate_=std::make_shared<std::vector<int64_t>>(*operation->before_);
        // A later candidate must fit the capacity already charged for it.
        if(operation->candidate_->capacity()>claim_capacity_)refuse("reconciliation candidate capacity exceeds retained charge");
    }
    return operation;
}
std::shared_ptr<const std::vector<int64_t>> recovery_unknown_reconciliation::refreeze_claims() {
    std::lock_guard lock(mutex_);require_live_locked();
    if(!settling_||active_operation_)refuse("reconciliation refreeze lacks settled first-claim witness");
    return first_claims_;
}
size_t recovery_unknown_reconciliation::continuity_bytes(size_t cap)const noexcept {
    // Charge the baseline plus the sole candidate even for a parked frame.
    const auto count=claim_capacity_;
    const size_t fixed=sizeof(*this)+sizeof(recovery_reconciliation_operation)+2*sizeof(std::vector<int64_t>);
    if(fixed>cap||count>(cap-fixed)/(2*sizeof(int64_t)))return cap+1;
    return fixed+count*2*sizeof(int64_t);
}
recovery_reconciliation_operation::~recovery_reconciliation_operation(){
    // Destroy the old/candidate payload before admitting a successor, off leaf.
    before_.reset();candidate_.reset();
    if(worker_&&sequence_){std::lock_guard lock(worker_->mutex_);
        if(worker_->active_operation_==sequence_)worker_->active_operation_=0;}
}
void recovery_reconciliation_operation::require_live()const {
    std::lock_guard lock(worker_->mutex_);worker_->require_live_locked();
    if(!sequence_||worker_->active_operation_!=sequence_)refuse("reconciliation continuity operation expired");
}
void recovery_reconciliation_operation::fail(std::exception_ptr error)noexcept {worker_->fail(std::move(error));}
void recovery_reconciliation_operation::require_settled(const recovery_install_result& result)const {settled(result);require_live();}
void recovery_reconciliation_operation::verify(database& db,bool candidate)const {
    require_live();const auto& expected=candidate?*candidate_:*before_;
    for(size_t c=0;c<descriptor_->contributions_.size();++c){const auto& part=descriptor_->contributions_[c];
        // `original` is the canonical UUID key written by put_entry, distinct
        // from the case-preserving `actual_original` spelling. Compare the
        // typed key exactly; existing provenance/receipt audits stay in place.
        const auto rows=db.query("SELECT CASE WHEN typeof(original)='blob' AND length(original)=36 THEN CAST(original AS TEXT) END AS original,"
            "CASE WHEN typeof(audit_id)='integer' THEN audit_id END AS audit_id,"
            "CASE WHEN first_export IS NULL THEN 0 WHEN typeof(first_export)='integer' AND first_export>0 THEN first_export END AS first_export "
            "FROM main._lattice_obligation_entry WHERE channel=CAST(? AS BLOB) AND stage IS NOT 2 ORDER BY audit_id LIMIT ?",
            {part.journal.scope.address.channel,static_cast<int64_t>(part.journal.entries.size()+1)});
        if(rows.size()!=part.journal.entries.size())refuse("restricted handoff validated first claim changed");
        for(size_t n=0;n<rows.size();++n){const auto& original=part.journal.entries[n];
            const auto* id=std::get_if<std::string>(&rows[n].at("original"));
            const auto* audit=std::get_if<int64_t>(&rows[n].at("audit_id"));
            const auto* first=std::get_if<int64_t>(&rows[n].at("first_export"));
            if(!id||!audit||!first||*id!=original.canonical_original_id||*audit!=original.record.audit_id||
                *first!=expected[worker_->claim_offsets_[c]+n])refuse("restricted handoff validated first claim changed");
        }
    }
    require_live();
}
void recovery_reconciliation_operation::claimed(const recovery_obligation_export_ticket& ticket){
    require_live();if(!candidate_||ticket.sequence<=0)refuse("reconciliation continuity lacks private claim candidate");
    size_t c=0;for(;c<descriptor_->contributions_.size();++c)if(descriptor_->contributions_[c].journal.scope.address==ticket.address)break;
    if(c==descriptor_->contributions_.size())refuse("reconciliation claim outside retained contribution");
    const auto& entries=descriptor_->contributions_[c].journal.entries;
    size_t next=0;
    for(const auto& id:ticket.canonical_original_ids){
        while(next<entries.size()&&entries[next].canonical_original_id!=id)++next;
        if(next==entries.size())refuse("reconciliation claim outside ordered retained original inventory");
        auto& first=(*candidate_)[worker_->claim_offsets_[c]+next++];
        if(!first)first=ticket.sequence;
    }
}
void recovery_reconciliation_operation::publish(){
    if(!candidate_)refuse("reconciliation continuity publication lacks candidate");
    std::shared_ptr<const std::vector<int64_t>> old;
    {std::lock_guard lock(worker_->mutex_);worker_->require_live_locked();
        if(!sequence_||worker_->active_operation_!=sequence_||worker_->first_claims_!=before_)
            refuse("reconciliation continuity publication identity changed");
        old=std::move(worker_->first_claims_);worker_->first_claims_=candidate_;}
}
void recovery_unknown_reconciliation::release(size_t contribution,uint64_t reservation,
    size_t begin,size_t count,bool handed_off,bool retry)noexcept {
    std::lock_guard lock(mutex_);
    if(contribution>=contributions_.size())return;
    auto& current=contributions_[contribution];
    // Old disposal cannot erase a replacement window. No owner, SQL, transport
    // or user callback is touched under this passive leaf.
    if(!reservation||current.active!=reservation||current.next!=begin)return;
    current.active=0;
    if(handed_off&&count)current.next+=count;
    else if(!retry)abandoned_=true;
}
recovery_reconciliation_export::~recovery_reconciliation_export(){
    if(!released_)if(auto worker=worker_.lock())worker->release(contribution_,reservation_,begin_,0,false,false);
}
void recovery_reconciliation_export::release_for_retry()noexcept {
    if(released_)return;released_=true;
    if(auto worker=worker_.lock())worker->release(contribution_,reservation_,begin_,0,false,true);
}
void recovery_reconciliation_export::did_handoff()noexcept {
    if(released_)return;released_=true;
    if(auto worker=worker_.lock())worker->release(contribution_,reservation_,begin_,selected_.size(),true,false);
}
size_t recovery_reconciliation_export::retained_bytes(size_t cap)const noexcept {
    size_t bytes=sizeof(*this);
    const auto add=[&](size_t count,size_t width=1){if(bytes>cap||count>(cap-bytes)/width)bytes=cap+1;else bytes+=count*width;};
    const auto text=[&](const std::string& value){add(value.capacity());add(1);};
    if(auto worker=worker_.lock())add(worker->continuity_bytes(cap));
    text(address_.channel);add(requested_.capacity(),sizeof(recovery_obligation_entry));
    for(const auto& entry:requested_){
        for(const auto* value:{&entry.record.original_id,&entry.record.table,&entry.record.target_id,&entry.canonical_original_id,&entry.canonical_target_id})text(*value);
        if(entry.acknowledged){text(entry.acknowledged->original_id);text(entry.acknowledged->receipt_namespace);}
    }
    for(const auto* values:{&originals_,&selected_}){add(values->capacity(),sizeof(std::string));for(const auto& value:*values)text(value);}
    return bytes;
}

std::shared_ptr<recovery_reconciliation_export> recovery_unknown_reconciliation::reserve(
    const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor,
    const std::shared_ptr<recovery_receiver_route>& route,size_t count,const std::vector<int64_t>& in_flight) {
    if(!count||count>1000||in_flight.size()>2000||descriptor_.lock()!=descriptor||descriptor->phase_!=4)
        refuse("reconciliation window lacks exact restricted descriptor");
    size_t index=descriptor->contributions_.size();
    for(size_t n=0;n<descriptor->contributions_.size();++n)if(descriptor->contributions_[n].route.lock()==route){
        if(index!=descriptor->contributions_.size())refuse("reconciliation physical route is ambiguous");index=n;
    }
    if(index==descriptor->contributions_.size())refuse("reconciliation physical route is not admitted");
    const auto& part=descriptor->contributions_[index];
    if(part.unknown_originals.size()>8192)refuse("reconciliation original capacity exceeded");
    auto grant=std::shared_ptr<recovery_reconciliation_export>(new recovery_reconciliation_export);
    grant->descriptor_=descriptor;grant->worker_=shared_from_this();grant->contribution_=index;grant->address_=part.journal.scope.address;
    {
        std::lock_guard lock(mutex_);require_live_locked();auto& current=contributions_[index];
        if(completed_||settling_||current.active||current.next==part.unknown_originals.size())return {};
        if(current.next>part.unknown_originals.size()||next_reservation_==std::numeric_limits<uint64_t>::max())
            refuse("reconciliation window sequence exhausted");
        grant->begin_=current.next;grant->reservation_=++next_reservation_;current.active=grant->reservation_;
    }
    const auto end=grant->begin_+std::min(count,part.unknown_originals.size()-grant->begin_);
    grant->originals_.assign(part.unknown_originals.begin()+grant->begin_,part.unknown_originals.begin()+end);
    // Descriptor entries are already in exact audit order. Select only the
    // controller's retained unresolved-Q subset, regardless of legacy ACK bits.
    size_t next=0;
    for(const auto& entry:part.journal.entries)if(next<grant->originals_.size()&&entry.canonical_original_id==grant->originals_[next]){
        if(entry.stage!=recovery_obligation_stage::open)refuse("reconciliation selected a settled or acknowledged original");
        // A real previous send may still own volatile ACK tracking. Wait at
        // that original; do not skip it, treat it as accepted or acquire a
        // competing pre-send exclusion. Persisted legacy ACK bits are ignored.
        if(std::find(in_flight.begin(),in_flight.end(),entry.record.audit_id)!=in_flight.end()){
            grant->originals_.resize(next);break;
        }
        grant->requested_.push_back(entry);++next;
    }
    if(next!=grant->originals_.size())refuse("reconciliation original missing from exact retained journal");
    if(!next){grant->release_for_retry();return {};}
    return grant;
}

void recovery_unknown_reconciliation::cancel(const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor) {
    {
        std::lock_guard lock(mutex_);require_live_locked();if(settling_||completed_)return;settling_=true;
    }
    try {
        const auto owner=descriptor->owner();if(!owner||owner->is_closed()||descriptor->phase_!=2)refuse("reconciliation cancellation owner retired");
        bool coordinator_busy=false;
        auto result=recovery_receiver_controller::controller_reconcile_owned(descriptor,recovery_reconciliation_step::cancelled,[&](database&){
            const auto& caps=descriptor->limits_;
            canonical_range_staging stages(owner,caps.install.installations,caps.codec,caps.staging);
            recovery_obligation_store journal(owner,caps.obligations,caps.install.installations);
            for(const auto& part:descriptor->contributions_){
                const auto q=canonical_range::decode(part.framing.request_frame,caps.codec);
                const auto m=canonical_range::decode(part.framing.manifest_frame,caps.codec);
                const auto* manifest=std::get_if<canonical_range::manifest>(&m.body);
                if(!std::holds_alternative<canonical_range::request>(q.body)||!manifest||q.logical!=m.logical||part.framing.route<=0)
                    refuse("reconciliation cancellation framing differs");
                stages.abandon_active(q.logical,manifest->manifest_digest,static_cast<uint64_t>(part.framing.route));
                journal.cancel_frozen_for_retry(part.journal.scope.address,descriptor->attempt_,part.journal.scope.revision);
            }
        },&coordinator_busy);
        if(coordinator_busy){std::lock_guard lock(mutex_);settling_=false;return;}
        recovery_export_adapter::require_committed(result.settlement());
        recovery_receiver_controller::controller_reconcile_publish(std::move(result));
        std::lock_guard lock(mutex_);completed_=true;settling_=false;
    }catch(...){fail(std::current_exception());throw;}
}
bool recovery_unknown_reconciliation::refreeze_if_complete(
    const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor) {
    {
        std::lock_guard lock(mutex_);require_live_locked();if(completed_||settling_)return true;
        if(active_operation_)return false;
        for(size_t n=0;n<contributions_.size();++n)
            if(contributions_[n].active||contributions_[n].next!=descriptor->contributions_[n].unknown_originals.size())return false;
        settling_=true;
    }
    try {
        // These cursors prove only actual handoff of the selected originals.
        // The next Q must ask the source again; this body creates no receipt,
        // clears no obligation and opens neither ordinary DML nor upload.
        bool coordinator_busy=false;
        auto result=recovery_receiver_controller::controller_reconcile_owned(descriptor,
            recovery_reconciliation_step::ready_for_refreeze,[](database&){},&coordinator_busy);
        if(coordinator_busy){std::lock_guard lock(mutex_);settling_=false;return true;}
        settled(result.settlement());
        recovery_receiver_controller::controller_reconcile_publish(std::move(result));
        std::lock_guard lock(mutex_);completed_=true;settling_=false;return true;
    }catch(...){fail(std::current_exception());throw;}
}

std::optional<recovery_export_preparation> recovery_unknown_reconciliation::prepare(
    const std::shared_ptr<recovery_receiver_route>& receiver,
    const std::shared_ptr<recovery_continuous_route>& continuous,
    const std::shared_ptr<receiver_source_binding>& source,
    std::shared_ptr<lattice_db> owner,uint64_t physical,size_t count,const std::vector<int64_t>& in_flight,
    std::shared_ptr<const receiver_upload_view> upload,bool* discovery_busy) {
    if(!receiver)return pending();
    const auto descriptor=receiver->pending_reconciliation();if(!descriptor)return pending();
    if(!owner||descriptor->owner()!=owner||owner->is_closed())refuse("reconciliation actual owner differs");
    const auto worker=acquire(descriptor);
    if(!worker){if(discovery_busy){*discovery_busy=true;return std::nullopt;}return pending();}
    if(descriptor->phase_==2){worker->cancel(descriptor);return pending();}
    if(descriptor->phase_!=4)refuse("reconciliation descriptor phase differs");
    if(worker->refreeze_if_complete(descriptor))return pending();
    if(!count||!upload||!upload->current())return pending();
    bool matched=false;
    for(const auto& part:descriptor->contributions_)if(part.route.lock()==receiver){
        if(matched||part.source!=source||upload->binding_.lock()!=source||upload->record_!=part.view.value||
           !source->recovery_live(part.view)||source->recovery_lifecycle(part.view)!=physical)
            refuse("reconciliation negotiated upload source differs");
        matched=true;
    }
    if(!matched)refuse("reconciliation actual contribution missing");
    auto operation=worker->begin_operation(true);
    if(!operation){if(discovery_busy){*discovery_busy=true;return std::nullopt;}return pending();}
    auto grant=worker->reserve(descriptor,receiver,std::min(count,upload->entries_),in_flight);if(!grant)return pending();
    try {
        auto work=recovery_continuous_producer::admit_reconciliation_work(descriptor,continuous,owner,physical);
        bool busy=false;
        auto result=recovery_export_adapter::prepare_reconciliation(std::move(owner),std::move(work),grant,physical,
            std::move(upload),discovery_busy?&busy:nullptr,operation);
        if(busy){*discovery_busy=true;grant->release_for_retry();return std::nullopt;}
        if(!result.frame){
            if(!result.blocked_original.empty())throw db_error(result.blocked_original);
            refuse("reconciliation retained window produced no frame");
        }
        return result;
    }catch(...){worker->fail(std::current_exception());throw;}
}
} // namespace lattice::detail
