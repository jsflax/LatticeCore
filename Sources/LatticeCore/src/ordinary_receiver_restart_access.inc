// Native closed archive access, compiled after its complete private state.
// Values returned here are observations for the fixed issuer; callers never
// create a positive cap from a member/deadline/physical tuple.
std::shared_ptr<lattice_db> ordinary_export_access::receiver_restart_parent(const ordinary_managed_receiver_preservation& cap){
    const auto& state=cap.state_;if(!state||state->creator!=history_creator()||!state->owner||state->owner->is_closed())refuse("restart receiver parent unavailable");return state->owner;
}
ordinary_launch::deadline ordinary_export_access::receiver_restart_deadline(const ordinary_managed_receiver_preservation& cap){
    const auto& state=cap.state_;if(!state||state->creator!=history_creator())refuse("restart receiver deadline unavailable");return state->admission_end;
}
std::string ordinary_export_access::receiver_restart_member(const ordinary_managed_receiver_preservation& cap){
    const auto parent=receiver_restart_parent(cap);const auto& state=cap.state_;std::string member;
    const auto settled=recovery_writer_access::ordinary_owned_write(*parent,[&](database& writer){
        validate_receiver_clone_admission(cap,*parent,writer);const auto physical=writer.physical_identity("main",{},true);if(!physical)refuse("restart receiver member has no actual file");
        const auto actual=state->admission->ipc_member_binding(state->path,physical->device,physical->inode,state->admission_end);
        if(actual.second!=state->record.generation)refuse("restart receiver actual member generation changed");member=actual.first;
    });if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
    if(parent->is_closed()||std::chrono::steady_clock::now()>=state->admission_end)refuse("restart receiver member publication expired");return member;
}
void ordinary_export_access::retain_receiver_group_target(const ordinary_managed_receiver_preservation& cap,
    std::shared_ptr<const ordinary_unpublished_group_target>& retained,ordinary_launch::deadline end){
    const auto parent=receiver_restart_parent(cap);const auto& state=cap.state_;
    if(!state->admission||end>state->admission_end||std::chrono::steady_clock::now()>=end)refuse("original GROUP target admission unavailable");
    state->admission->retain_group_target(state->admission,state->path,state->physical.device,state->physical.inode,end,retained);
    if(parent->is_closed())refuse("original GROUP parent retired during target association");
}
void ordinary_export_access::prepare_receiver_group_target_io(
    const std::shared_ptr<const ordinary_unpublished_group_target>& original,
    std::shared_ptr<ordinary_target_staging_io>& retained,ordinary_launch::deadline end){
    ordinary_target_staging_io::prepare(original,retained,end);
    // Preparation/registration is storage custody only. The same request
    // next retains its actual connection before the closed constructor gate.
}
void ordinary_export_access::prepare_receiver_group_target_connection(
    const std::shared_ptr<const ordinary_unpublished_group_target>& original,
    const std::shared_ptr<ordinary_target_staging_io>& io,
    std::shared_ptr<ordinary_target_staging_connection>& retained,ordinary_launch::deadline end){
    ordinary_target_staging_connection::prepare(original,io,retained,end);
    // The coupled producer has already initialized and left its SAME shell
    // constructor scope. A pre-open scope check here would reject every such
    // return. Rejoin actual connection/target/IO originals without granting
    // schema SQL, model access or an ordinary historical-file child.
    retained->require_original(end);
    retained->require_target_identity(original.get(),end);
    if(retained->io_original(end)!=io)refuse("target returned connection I/O original differs");
}
void ordinary_export_access::validate_receiver_restart_until(const ordinary_managed_receiver_preservation& cap,ordinary_launch::deadline until){
    const auto parent=receiver_restart_parent(cap);const auto& state=cap.state_;const auto now=std::chrono::steady_clock::now();
    const auto retained=state->episode_end_millis.load(std::memory_order_acquire);
    const auto actual=retained>0?ordinary_launch::deadline(std::chrono::milliseconds(retained)):state->admission_end;
    if(until>actual||now>=until)refuse("restart receiver attempt cannot renew actual native deadline");
    const auto settled=recovery_writer_access::ordinary_owned_write(*parent,[&](database& writer){validate_receiver_preservation(cap,*parent,writer,until);});
    if(settled.state!=recovery_install_state::committed||settled.primary_error||settled.cleanup_error||settled.postcommit_error||settled.notification_error)throw ordinary_order_write_error(settled);
}
void ordinary_export_access::publish_receiver_restart_child(const std::shared_ptr<const ordinary_managed_receiver_preservation>& cap,
    const std::shared_ptr<const ordinary_receiver_wss_request>& request,lattice_db& parent,lattice_db& child){
    if(!cap||!request)refuse("restart receiver child has no actual issuer");validate_receiver_clone_publication(*cap,parent,child);
    if(child.ordinary_receiver_preservation_.snapshot().lock()!=cap)refuse("restart receiver child archive association differs");
    child.ordinary_receiver_wss_.publish(request);
}
std::shared_ptr<const ordinary_receiver_wss_request> ordinary_export_access::receiver_wss_request(lattice_db& owner){
    // Reject foreign/unsupported processes BEFORE entering inherited atomic
    // weak-pointer machinery, which may contain a held implementation lock.
    if(!owner.owns_creator_process()||!ordinary_receiver_wss_access::creator_current())return {};
    return owner.ordinary_receiver_wss_.snapshot().lock();
}
