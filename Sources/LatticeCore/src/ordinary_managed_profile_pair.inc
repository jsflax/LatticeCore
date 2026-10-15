// Included only by ordinary_export.cpp after the actual profile implementation.
// No raw/configuration/DTO conversion issues this retained pair.
namespace {
std::uint64_t managed_pair_creator() noexcept {
#if defined(__APPLE__) || defined(__linux__)
    return static_cast<std::uint64_t>(::getpid());
#else
    return 0;
#endif
}
void managed_pair_deadline(ordinary_launch::deadline end) {
    if(std::chrono::steady_clock::now()>=end)refuse("managed pair deadline expired");
}
}
struct ordinary_managed_profile_pair::implementation {
    std::shared_ptr<lattice_db> source,recipient;
    std::shared_ptr<const ordinary_managed_profile> source_profile,recipient_profile;
    std::shared_ptr<const ordinary_managed_child_admission> child;
    physical_store_identity source_physical,recipient_physical;
    std::string source_member,recipient_member,generation;
    static void dispose(implementation* value,std::uint64_t creator) noexcept {
        // Reuse this already allocated record as the quarantine capsule. No
        // allocation, mutex, new authority or native cleanup claim is needed.
        if(!value||creator==0||creator!=managed_pair_creator())return;
        value->child.reset();
        if(creator!=managed_pair_creator())return;
        value->recipient_profile.reset();
        if(creator!=managed_pair_creator())return;
        value->source_profile.reset();
        if(creator!=managed_pair_creator())return;
        value->recipient.reset();
        if(creator!=managed_pair_creator())return;
        value->source.reset();
        if(creator!=managed_pair_creator())return;
        // All callback-capable members are empty. Only value metadata remains.
        delete value;
    }
};
ordinary_managed_profile_pair::ordinary_managed_profile_pair(std::uint64_t creator,std::unique_ptr<implementation> value)
    :creator_(creator),impl_(std::move(value)){}
ordinary_managed_profile_pair::~ordinary_managed_profile_pair(){
    // Detach before the first arbitrary finalizer, so reentry cannot access a
    // partially retired pair and inherited continuation cannot default-delete.
    const auto creator=std::exchange(creator_,0);
    implementation::dispose(impl_.release(),creator);
}
ordinary_managed_profile_pair::ordinary_managed_profile_pair(ordinary_managed_profile_pair&& other) noexcept
    :creator_(std::exchange(other.creator_,0)),impl_(std::move(other.impl_)){}
ordinary_managed_profile_pair& ordinary_managed_profile_pair::operator=(ordinary_managed_profile_pair&& other) noexcept {
    if(this!=&other){
        const auto caller=managed_pair_creator();
        const auto creator=std::exchange(creator_,0);
        auto* retired=impl_.release();
        if((creator!=0&&creator!=caller)||(other.creator_!=0&&other.creator_!=caller)){
            implementation::dispose(retired,creator);return *this;
        }
        // Publish into the already empty wrapper before any finalizer runs.
        // Reentrant assignment therefore follows this same guarded path; no
        // later default unique_ptr deletion can bypass the creator veto.
        impl_=std::move(other.impl_);creator_=std::exchange(other.creator_,0);
        implementation::dispose(retired,creator);
        // No wrapper or incoming owner is touched after arbitrary teardown.
    }
    return *this;
}
void ordinary_managed_profile_pair::require_creator()const{
    if(creator_==0||creator_!=managed_pair_creator())refuse("managed pair inherited or unavailable");
    if(!impl_)refuse("managed pair was moved or retired");
}
const std::shared_ptr<lattice_db>& ordinary_managed_profile_pair::source_owner()const{require_creator();return impl_->source;}
const std::shared_ptr<lattice_db>& ordinary_managed_profile_pair::recipient_owner()const{require_creator();return impl_->recipient;}
const std::shared_ptr<const ordinary_managed_profile>& ordinary_managed_profile_pair::source_profile()const{require_creator();return impl_->source_profile;}
const std::shared_ptr<const ordinary_managed_profile>& ordinary_managed_profile_pair::recipient_profile()const{require_creator();return impl_->recipient_profile;}
const std::string& ordinary_managed_profile_pair::source_member()const{require_creator();return impl_->source_member;}
const std::string& ordinary_managed_profile_pair::recipient_member()const{require_creator();return impl_->recipient_member;}
const std::string& ordinary_managed_profile_pair::generation()const{require_creator();return impl_->generation;}
void ordinary_managed_profile_pair::validate(ordinary_launch::deadline end)const{
    require_creator();ordinary_export_access::validate_managed_pair(*this,end);
}

ordinary_managed_profile_pair ordinary_export_access::retain_managed_pair(
    std::shared_ptr<lattice_db> source,std::shared_ptr<lattice_db> recipient,ordinary_launch::deadline end){
    managed_pair_deadline(end);
    const auto creator=managed_pair_creator();
    if(creator==0||!source||!recipient||source.get()==recipient.get())refuse("managed pair requires two actual owners");
    // These private actual-context calls verify creator before entering any
    // native ownership/SQL leaf. A normal-primary open is not enough.
    if(!source->config_.ordinary_context||!recipient->config_.ordinary_context||
        !source->config_.ordinary_context->is_managed_history_view()||
        !recipient->config_.ordinary_context->is_managed_history_view())
        refuse("managed pair requires actual historical cohort views");
    if(source->is_closed()||recipient->is_closed())refuse("managed pair owner closed");
    auto ap=source->ordinary_managed_profile_.load(std::memory_order_acquire);
    auto bp=recipient->ordinary_managed_profile_.load(std::memory_order_acquire);
    if(!ap||!bp||!ap->impl_||!bp->impl_||!ap->impl_->admission||
        ap->impl_->admission!=bp->impl_->admission||
        (ap->impl_->physical.device==bp->impl_->physical.device&&ap->impl_->physical.inode==bp->impl_->physical.inode))
        refuse("managed pair lacks distinct enrolled stores in the same actual child");
    ordinary_managed_profile_pair result(creator,
        std::make_unique<ordinary_managed_profile_pair::implementation>());
    auto& state=*result.impl_;
    // Install actual owners into the already guarded empty record before any
    // fallible identity copies, binding checks or owned-write delivery. These
    // moves destroy no prior capture, and leave no parallel local owner copies.
    state.source=std::move(source);state.recipient=std::move(recipient);
    state.source_profile=std::move(ap);state.recipient_profile=std::move(bp);
    state.child=state.source_profile->impl_->admission;
    state.source_physical=state.source_profile->impl_->physical;
    state.recipient_physical=state.recipient_profile->impl_->physical;
    const auto a=state.child->ipc_member_binding(state.source_profile->impl_->path,state.source_physical.device,state.source_physical.inode,end);
    const auto b=state.child->ipc_member_binding(state.recipient_profile->impl_->path,state.recipient_physical.device,state.recipient_physical.inode,end);
    if(a.first==b.first||a.first.empty()||b.first.empty()||a.second!=b.second)refuse("managed pair member binding changed");
    state.source_member=a.first;state.recipient_member=b.first;state.generation=a.second;
    // Nothing escapes before both actual owner views commit and their deferred
    // delivery completes without a primary, cleanup or post-COMMIT error.
    validate_managed_pair(result,end);
    return result;
}
void ordinary_export_access::validate_managed_pair(const ordinary_managed_profile_pair& pair,ordinary_launch::deadline end){
    pair.require_creator();managed_pair_deadline(end);
    const auto& state=*pair.impl_;
    const auto check=[&](const std::shared_ptr<lattice_db>& owner,
        const std::shared_ptr<const ordinary_managed_profile>& expected,
        const physical_store_identity& physical,const std::string& member){
        managed_pair_deadline(end);
        if(!owner||!expected||!owner->config_.ordinary_context||
            !owner->config_.ordinary_context->is_managed_history_view()||owner->is_closed())
            refuse("managed pair actual owner unavailable");
        const auto settlement=recovery_writer_access::ordinary_owned_write(*owner,[&](database& writer){
            if(owner->ordinary_managed_profile_.load(std::memory_order_acquire)!=expected)
                refuse("managed pair actual enrollment replaced");
            validate_managed_profile_until(*expected,*owner,writer,end);
            const auto actual=writer.physical_identity("main",{},true);
            if(!actual||*actual!=physical)refuse("managed pair physical store changed");
            const auto child=expected->attachment_admission(owner->config_.ordinary_context,owner->config_.path,
                actual->device,actual->inode,end);
            if(child!=state.child)refuse("managed pair actual child changed");
            const auto binding=child->ipc_member_binding(owner->config_.path,actual->device,actual->inode,end);
            if(binding.first!=member||binding.second!=state.generation)refuse("managed pair retained member changed");
            managed_pair_deadline(end);
        });
        if(settlement.state!=recovery_install_state::committed||settlement.primary_error||settlement.cleanup_error||
            settlement.postcommit_error||settlement.notification_error)throw ordinary_order_write_error(settlement);
        if(owner->is_closed()||owner->ordinary_managed_profile_.load(std::memory_order_acquire)!=expected)
            refuse("managed pair owner retired during delivery");
        managed_pair_deadline(end);
    };
    check(state.source,state.source_profile,state.source_physical,state.source_member);
    check(state.recipient,state.recipient_profile,state.recipient_physical,state.recipient_member);
    if(state.source->is_closed()||state.recipient->is_closed()||
        state.source->ordinary_managed_profile_.load(std::memory_order_acquire)!=state.source_profile||
        state.recipient->ordinary_managed_profile_.load(std::memory_order_acquire)!=state.recipient_profile)
        refuse("managed pair owner changed before publication");
    managed_pair_deadline(end);
}
