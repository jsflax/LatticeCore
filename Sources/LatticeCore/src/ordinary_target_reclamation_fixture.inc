// Included after real receiver definitions, within lattice::detail.
// These methods are compiled only in the dedicated private fixture image.
int ordinary_target_reclamation_fixture::run_memory(ordinary_launch::deadline end) {
    auto startup=ordinary_installation::managed_history_receiver::receive_paired(end);
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    const auto native=startup.admission->impl_;
    if(!native||!native->admitted||native->daemon_entry||!native->paired_runtime||!native->channel)
        normal_fail("constructor fixture sibling has no actual paired admission");
    native->verify(end,true);
    auto complete=native->offer;complete["phase"]=7;
    complete["fixture"]="lattice-target-constructor-reclamation-v1";
    const auto raw=complete.dump();
    if(native->channel->receive(end)!=ordinary_launch::frame(raw.begin(),raw.end()))
        normal_fail("constructor fixture sibling completion exchange mismatch");
    // No new Native work or peer-live authority is requested after the final
    // joint stop frame; siblings may now exit in either order.
    return 0;
#else
    normal_fail("constructor fixture platform unavailable");
#endif
}
int ordinary_target_reclamation_fixture::run_daemon(ordinary_launch::deadline end) {
    auto startup=ordinary_installation::managed_history_receiver::receive_daemon(end);
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    const auto native=startup.admission->impl_;
    if(!native||!native->admitted||!native->daemon_entry||!native->paired_runtime||!native->channel||
       native->target_count!=1)normal_fail("constructor fixture requires one actual issued GROUP target");
    auto begin=native->offer;begin["phase"]=5;
    begin["fixture"]="lattice-target-constructor-reclamation-v1";
    const auto initial=begin.dump();
    if(native->channel->receive(end)!=ordinary_launch::frame(initial.begin(),initial.end()))
        normal_fail("constructor fixture actual controller start mismatch");
    native->verify(end,true);
    const auto& issued=native->targets[0];
    const auto source=std::find_if(native->stores.begin(),native->stores.end(),[&](const auto& value){
        return value.relative==issued.old_member&&value.main==issued.old_main;
    });
    if(source==native->stores.end())normal_fail("constructor fixture issued source no longer exists");
    std::shared_ptr<const ordinary_unpublished_group_target> target;
    // The SAME actual admitted daemon claims the SAME issued old-member role.
    // Internal observations are checked again by normal retain_group_target.
    startup.admission->retain_group_target(startup.admission,source->path,source->main.device,
        source->main.inode,end,target);
    auto observation=construct_and_abandon(target,end);
    native->verify(end,true); // Existing target census observes the real empty receipt.
    auto reply=native->offer;reply["phase"]=6;
    reply["fixture"]="lattice-target-constructor-reclamation-v1";
    reply["observation"]=std::move(observation);
    const auto raw=reply.dump();
    if(raw.size()>ordinary_launch::maximum_frame_bytes)normal_fail("constructor fixture receipt too large");
    native->channel->send(ordinary_launch::frame(raw.begin(),raw.end()),end);
    auto complete=native->offer;complete["phase"]=7;
    complete["fixture"]="lattice-target-constructor-reclamation-v1";
    const auto final=complete.dump();
    if(native->channel->receive(end)!=ordinary_launch::frame(final.begin(),final.end()))
        normal_fail("constructor fixture daemon completion exchange mismatch");
    // No new Native work or peer-live authority is requested after the final
    // joint stop frame; siblings may now exit in either order.
    return 0;
#else
    normal_fail("constructor fixture platform unavailable");
#endif
}
