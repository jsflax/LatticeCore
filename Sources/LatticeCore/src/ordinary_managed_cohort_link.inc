// Included only after complete managed child admission, inside lattice::detail.
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
struct ordinary_managed_cohort_link::implementation {
    fd_owner socket;
    std::shared_ptr<const ordinary_managed_child_admission> admission;
    ordinary_managed_cohort_link_facts observed;
    std::atomic<bool> stopped{false};
    std::atomic<ordinary_managed_cohort_link_settlement_state> settlement{
        ordinary_managed_cohort_link_settlement_state::pending};
    int close_error=0;
    bool close_in_time=false;
};
#else
struct ordinary_managed_cohort_link::implementation {};
#endif
ordinary_managed_cohort_link::ordinary_managed_cohort_link(std::unique_ptr<implementation> state)
    :impl_(std::move(state)){}
ordinary_managed_cohort_link::~ordinary_managed_cohort_link() {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_)return;
    if(::getpid()!=impl_->observed.local.pid){(void)impl_.release();return;}
    request_stop();
    if(impl_->settlement.load(std::memory_order_acquire)!=ordinary_managed_cohort_link_settlement_state::clean)
        (void)impl_.release(); // No implicit close or destruction-as-retirement.
    // Actual clean close leaves fd_owner empty. Releasing admission here is
    // allowed only in the original process and after the collector's true join.
#endif
}
void ordinary_managed_cohort_link::validate(ordinary_launch::deadline end)const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_||::getpid()!=impl_->observed.local.pid||
        ordinary_launch::current_process()!=impl_->observed.local||
        impl_->stopped.load(std::memory_order_acquire)||impl_->socket.value<0||!impl_->admission)
        normal_fail("managed cohort peer transport unavailable");
    impl_->admission->impl_->verify(end,true);
#else
    (void)end;normal_fail("managed cohort peer unavailable");
#endif
}
const ordinary_managed_cohort_link_facts& ordinary_managed_cohort_link::facts(ordinary_launch::deadline end)const {
    validate(end);
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    return impl_->observed;
#else
    normal_fail("managed cohort peer unavailable");
#endif
}
int ordinary_managed_cohort_link::descriptor(ordinary_launch::deadline end)const {
    validate(end);
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    return impl_->socket.value;
#else
    normal_fail("managed cohort peer unavailable");
#endif
}
void ordinary_managed_cohort_link::request_stop()noexcept {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    try {
        if(!impl_||::getpid()!=impl_->observed.local.pid||
            ordinary_launch::current_process()!=impl_->observed.local)return;
        impl_->stopped.store(true,std::memory_order_release);
        if(impl_->socket.value>=0)(void)::shutdown(impl_->socket.value,SHUT_RDWR);
    }catch(...){} // Cancellation cannot supply successful retirement evidence.
#endif
}
ordinary_managed_cohort_link_settlement ordinary_managed_cohort_link::settle_descriptor(
    ordinary_launch::deadline end)noexcept {
    using phase=ordinary_managed_cohort_link_settlement_state;
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    // A PID mismatch veto precedes any lock, fd access or inherited disposal.
    if(!impl_||::getpid()!=impl_->observed.local.pid)return {phase::inherited,0,false};
    auto current=impl_->settlement.load(std::memory_order_acquire);
    const auto observed=[&](phase value) {
        if(value==phase::clean||value==phase::uncertain)
            return ordinary_managed_cohort_link_settlement{value,impl_->close_error,impl_->close_in_time};
        return ordinary_managed_cohort_link_settlement{value,0,false};
    };
    if(current!=phase::pending)return observed(current);
    if(std::chrono::steady_clock::now()>=end)return observed(phase::pending);
    if(!impl_->settlement.compare_exchange_strong(current,phase::in_progress,
            std::memory_order_acq_rel,std::memory_order_acquire))return observed(current);
    impl_->stopped.store(true,std::memory_order_release);
    // The actual collector owns exclusive settlement after all counted uses.
    // Remove the numeric fd from every future stop/destructor path BEFORE the
    // consuming syscall, so EINTR/other ambiguity cannot close a reused fd.
    const int descriptor=std::exchange(impl_->socket.value,-1);
    int result=-1,error=EBADF;
    if(descriptor>=0){result=::close(descriptor);error=result==0?0:errno;}
    impl_->close_error=error;
    impl_->close_in_time=std::chrono::steady_clock::now()<end;
    current=result==0?phase::clean:phase::uncertain;
    impl_->settlement.store(current,std::memory_order_release);
    return observed(current);
#else
    (void)end;return {phase::uncertain,0,false};
#endif
}
std::shared_ptr<ordinary_managed_cohort_link> ordinary_managed_child_admission::claim_cohort_link(
    std::shared_ptr<const ordinary_managed_child_admission> self,ordinary_launch::deadline end)const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(self.get()!=this||!impl_||!impl_->admitted||!impl_->paired_runtime)
        normal_fail("actual paired child transport required");
    impl_->verify(end,true);
    // Prepare all allocation/copies before consuming the sole inherited end.
    // Failure before CAS leaves its real owner intact, with no handoff issued.
    auto value=std::make_unique<ordinary_managed_cohort_link::implementation>();
    value->admission=std::move(self);
    value->observed.local=impl_->process;value->observed.peer=impl_->peer;
    value->observed.supervisor=impl_->supervisor;
    value->observed.generation=managed_identity(impl_->offer.at("generation"));
    value->observed.cohort=managed_identity(impl_->offer.at("cohort"));
    value->observed.local_image=impl_->memory.identity;value->observed.peer_image=impl_->peer_image.identity;
    value->observed.local_image_digest=impl_->memory.content;value->observed.peer_image_digest=impl_->peer_image.content;
    value->observed.nonce=impl_->peer_nonce;value->observed.local_is_daemon=impl_->daemon_entry;
    value->settlement.store(ordinary_managed_cohort_link_settlement_state::clean,std::memory_order_relaxed);
    auto result=std::shared_ptr<ordinary_managed_cohort_link>(new ordinary_managed_cohort_link(std::move(value)));
    if(std::chrono::steady_clock::now()>=end)normal_fail("managed cohort peer handoff expired");
    bool available=false;
    if(!impl_->peer_claimed.compare_exchange_strong(available,true,std::memory_order_acq_rel))
        normal_fail("managed cohort peer already handed off");
    result->impl_->socket=std::move(impl_->peer_socket);
    result->impl_->settlement.store(ordinary_managed_cohort_link_settlement_state::pending,std::memory_order_release);
    if(result->impl_->socket.value<0)normal_fail("managed cohort peer descriptor absent");
    return result; // Still transport custody only; product issuance is separate.
#else
    (void)self;(void)end;normal_fail("managed cohort peer unavailable");
#endif
}
