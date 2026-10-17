// Included after actual profile/pair implementations in ordinary_export.cpp.
// Pair001's small header/state additions and peer-claim-deadline003 are required.
struct ordinary_managed_peer_owner::implementation {
    const std::uint64_t creator=managed_pair_creator();
    std::shared_ptr<lattice_db> owner;
    std::shared_ptr<const ordinary_managed_profile> profile;
    std::shared_ptr<ordinary_managed_cohort_link> link;
    physical_store_identity physical;
    std::string member,generation;
    ordinary_managed_cohort_link_facts transport;
    int descriptor=-1; // stable borrow only; the exact link owns close
    std::atomic<bool> stopped{false};
};
ordinary_managed_peer_owner::ordinary_managed_peer_owner(std::unique_ptr<implementation> value):impl_(std::move(value)){}
ordinary_managed_peer_owner::~ordinary_managed_peer_owner(){
    if(impl_&&impl_->creator!=managed_pair_creator()){(void)impl_.release();return;}
    request_stop();
    // The registered connection retains this object until actual joins and
    // explicit link settlement. Destruction is never positive close evidence.
    auto* held=impl_.release();if(!held)return;
    held->link.reset();if(held->creator!=managed_pair_creator())return;
    held->profile.reset();if(held->creator!=managed_pair_creator())return;
    held->owner.reset();if(held->creator!=managed_pair_creator())return;
    delete held;
}
void ordinary_managed_peer_owner::validate_lifetime()const{
    if(!impl_||impl_->creator==0||impl_->creator!=managed_pair_creator())refuse("managed peer inherited or absent");
    if(impl_->stopped.load(std::memory_order_acquire)||!impl_->owner||impl_->owner->is_closed()||
        !ordinary_export_access::managed_peer_owner_current(*this)||!impl_->link)
        refuse("managed peer actual owner retired");
}
void ordinary_managed_peer_owner::validate(ordinary_launch::deadline end)const{
    validate_lifetime();ordinary_export_access::validate_managed_peer_owner(*this,end);
}
const ordinary_managed_cohort_link_facts& ordinary_managed_peer_owner::facts()const{validate_lifetime();return impl_->transport;}
int ordinary_managed_peer_owner::descriptor()const{
    // Stop requires this stable borrow for shutdown. No borrow outlives the
    // connection's actual operation/terminal-use counts or explicit settlement.
    if(!impl_||impl_->creator!=managed_pair_creator()||impl_->descriptor<0)refuse("managed peer descriptor unavailable");
    return impl_->descriptor;
}
void ordinary_managed_peer_owner::request_stop()noexcept{
    if(!impl_||impl_->creator!=managed_pair_creator())return;
    impl_->stopped.store(true,std::memory_order_release);
    ordinary_managed_ipc_access::stop_cohort_link(impl_->link);
}
ordinary_managed_cohort_link_settlement ordinary_managed_peer_owner::settle(ordinary_launch::deadline end)noexcept{
    using phase=ordinary_managed_cohort_link_settlement_state;
    if(!impl_||impl_->creator!=managed_pair_creator())return {phase::inherited,0,false};
    impl_->stopped.store(true,std::memory_order_release);
    const auto result=ordinary_managed_ipc_access::settle_cohort_link(impl_->link,end);
    if(result.state==phase::clean||result.state==phase::uncertain)impl_->descriptor=-1;
    return result;
}

void ordinary_export_access::retain_managed_peer_owner(
    std::shared_ptr<lattice_db> owner,ordinary_launch::deadline end,std::shared_ptr<ordinary_managed_peer_owner>& reserved_slot){
    if(reserved_slot)refuse("managed peer receiving slot already occupied");
    managed_pair_deadline(end);
    if(managed_pair_creator()==0||!owner||!owner->config_.ordinary_context||
       !owner->config_.ordinary_context->is_managed_history_view()||owner->is_closed())
        refuse("managed peer requires actual enrolled owner");
    auto profile=owner->ordinary_managed_profile_.load(std::memory_order_acquire);
    if(!profile||!profile->impl_||!profile->impl_->admission)refuse("managed peer actual profile unavailable");
    auto state=std::make_unique<ordinary_managed_peer_owner::implementation>();
    state->owner=std::move(owner);state->profile=std::move(profile);
    state->physical=state->profile->impl_->physical;
    const auto creator=state->creator;
    auto result=std::shared_ptr<ordinary_managed_peer_owner>(new ordinary_managed_peer_owner(std::move(state)),
        [creator](ordinary_managed_peer_owner* value){if(creator==managed_pair_creator())delete value;});
    reserved_slot=result; // exact pre-reserved connection owns unwind/uncertainty
    // Keep the new result in the reserved connection record before one-shot
    // handoff. The caller supplies a fixed receiving slot, not a callback.
    const auto settled=recovery_writer_access::ordinary_owned_write(*result->impl_->owner,[&](database& writer){
        auto& actual=*result->impl_;
        if(actual.owner->ordinary_managed_profile_.load(std::memory_order_acquire)!=actual.profile)
            refuse("managed peer profile replaced before claim");
        validate_managed_profile_until(*actual.profile,*actual.owner,writer,end);
        const auto physical=writer.physical_identity("main",{},true);
        if(!physical||*physical!=actual.physical)refuse("managed peer physical owner changed");
        const auto binding=actual.profile->impl_->admission->ipc_member_binding(
            actual.owner->config_.path,physical->device,physical->inode,end);
        if(binding.first!="memory.sqlite")refuse("managed peer requires actual primary member");
        actual.member=binding.first;actual.generation=binding.second;
        // Constructor/result storage already exists before consuming FD10.
        actual.link=ordinary_managed_ipc_access::claim_cohort_link(*actual.profile,*actual.owner,writer,end);
        actual.transport=ordinary_managed_ipc_access::cohort_link_facts(actual.link,end);
        actual.descriptor=ordinary_managed_ipc_access::cohort_link_descriptor(actual.link,end);
        validate_managed_profile_until(*actual.profile,*actual.owner,writer,end);
        managed_pair_deadline(end);
    });
    // Failed/uncertain COMMIT remains in the actual reserved record. Its
    // constructor unwind attempts explicit settlement; ambiguity stays charged.
    managed_clean_turn(settled);
    result->validate(end);
}
bool ordinary_export_access::managed_peer_owner_current(const ordinary_managed_peer_owner& peer)noexcept{
    return peer.impl_&&peer.impl_->creator==managed_pair_creator()&&peer.impl_->owner&&
        peer.impl_->owner->ordinary_managed_profile_.load(std::memory_order_acquire)==peer.impl_->profile;
}
void ordinary_export_access::validate_managed_peer_owner(const ordinary_managed_peer_owner& peer,ordinary_launch::deadline end){
    peer.validate_lifetime();managed_pair_deadline(end);
    const auto& state=*peer.impl_;
    ordinary_managed_ipc_access::validate_cohort_link(state.link,end);
    const auto settled=recovery_writer_access::ordinary_owned_write(*state.owner,[&](database& writer){
        peer.validate_lifetime();validate_managed_profile_until(*state.profile,*state.owner,writer,end);
        const auto physical=writer.physical_identity("main",{},true);
        if(!physical||*physical!=state.physical)refuse("managed peer physical identity changed");
        const auto binding=state.profile->impl_->admission->ipc_member_binding(state.owner->config_.path,physical->device,physical->inode,end);
        if(binding.first!=state.member||binding.second!=state.generation)refuse("managed peer member or generation changed");
        peer.validate_lifetime();managed_pair_deadline(end);
    });managed_clean_turn(settled);
    peer.validate_lifetime();ordinary_managed_ipc_access::validate_cohort_link(state.link,end);managed_pair_deadline(end);
}
