// Included in the real authenticated request issuer, after its native state.
// Neither a caller-selected path nor a copied decoder/header can issue this owner.
namespace {
struct group_receiving_slot {bool charged=false;std::uint64_t serial=0;std::shared_ptr<ordinary_group_target_receiving_owner> held;};
auto* const group_receiving_registry=new std::array<group_receiving_slot,64>;
std::mutex group_receiving_registry_leaf;
std::uint64_t group_receiving_serial=0;
struct group_receiving_construction {
    std::size_t slot=64;std::uint64_t serial=0;bool published=false;
    group_receiving_construction() {
        creator_check();std::lock_guard lock(group_receiving_registry_leaf);
        require(group_receiving_serial<UINT64_MAX,"GROUP receiving construction identity exhausted");
        for(std::size_t i=0;i<64;++i)if(!(*group_receiving_registry)[i].charged) {
            auto& value=(*group_receiving_registry)[i];value.charged=true;value.serial=++group_receiving_serial;
            slot=i;serial=value.serial;break;
        }
        require(slot<64,"GROUP receiving construction capacity exhausted before capture/allocation");
    }
    ~group_receiving_construction() {
        if(published||creator()!=registry_creator||slot>=64)return;
        std::lock_guard lock(group_receiving_registry_leaf);auto& value=(*group_receiving_registry)[slot];
        // No worker/native/factory effect can precede this actual publication.
        if(value.charged&&value.serial==serial&&!value.held)value.charged=false;
    }
};
}
struct ordinary_group_target_receiving_owner::state {
    const std::uint64_t process=creator();
    const std::weak_ptr<const ordinary_receiver_wss_request> request;
    std::weak_ptr<ordinary_group_target_receiving_owner> receiving_original; // Actual published owner; advisory wake only.
    const std::shared_ptr<lattice_db> application;
    const std::shared_ptr<const ordinary_managed_receiver_preservation> archive;
    const std::shared_ptr<ordinary_managed_product_credentials> credentials;
    const std::shared_ptr<const ordinary_receiver_history_expectation> expectation;
    const clock::time_point end;
    const std::string endpoint,model_profile;
    const std::size_t service_slot;
    const std::uint64_t service_serial;
    std::shared_ptr<const ordinary_unpublished_group_target> target;
    std::shared_ptr<ordinary_target_staging_io> io;
    std::shared_ptr<ordinary_target_staging_connection> connection;
    std::shared_ptr<ordinary_group_target_schema_original> schema;
    std::shared_ptr<configured_retirement_registry> registry;
    std::optional<configured_retirement_registry::reservation> reservation;
    platform_retirement_receipt receipt;
    std::shared_ptr<configured_attempt_custody> custody;
    std::optional<sync_retirement_lane::reservation> native_lane;
    std::shared_ptr<std_thread_scheduler> worker;
    std::shared_ptr<network_factory> factory;
    std::shared_ptr<sync_transport> transport;
    owned_platform_sync_transport* typed_transport=nullptr; // SAME retained transport only.
    platform_transport_callbacks endpoint_original;
    std::thread::id creator_thread;
    std::string token,model_request,history_request;
    const std::uint64_t lifecycle=1; // One actual connect, never a redial.
    clock::time_point negotiation_end{};
    std::mutex leaf;
    std::condition_variable changed;
    std::exception_ptr first_failure;
    bool terminal_observed=false,terminal_error=false,terminal_reason_retained=false;
    int terminal_code=0;std::size_t terminal_reason_bytes=0;
    platform_transport_callbacks terminal_endpoint;
    std::shared_ptr<const std::string> terminal_reason;
    bool stop=false,cleanup_queued=false,cleanup_entered=false,collector_queued=false;
    bool collector_active=false,collector_external_wake=false;
    bool first_worker_join_entered=false,first_worker_join_returned=false,first_worker_join_result=false,worker_join_failure=false,worker_join_active=false;
    std::size_t worker_join_calls=0;
    configured_attempt_custody::observation first_worker_join_input{},first_worker_join_output{};
    bool preparation_active=true,preparation_returned=false,native_lane_reservation_entered=false,native_lane_reservation_returned=false;
    bool never_started_entered=false;
    bool startup_active=false,startup_returned=false,worker_creation_entered=false,worker_creation_returned=false;
    bool timer_launch_entered=false,timer_launch_returned=false,initial_queue_entered=false,initial_queue_returned=false;
    bool target_preparation_entered=false,target_preparation_returned=false,never_started_ending=false,service_released=false;
    bool worker_entered=false,worker_joined=false,native_lane_bound=false,native_lane_returned=false;
    bool factory_entered=false,factory_returned=false,connect_entered=false,connect_returned=false;
    bool model_send_entered=false,model_send_returned=false,model_send_result=false,model_agreed=false;
    bool history_send_entered=false,history_send_returned=false,history_send_result=false;
    bool sqlite_and_files_joined=false,native_completion_entered=false,native_completion_returned=false;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
    bool qualification_checkpoint=false,qualification_disposition_returned=false,qualification_fallback_entered=false;
    ordinary_target_staging_connection::abandoned_retirement_result qualification_disposition=
        ordinary_target_staging_connection::abandoned_retirement_result::retained_debt;
    std::uint64_t qualification_native_observation=0;
#endif
    bool collected=false,collection_entered=false,collection_returned=false,first_collection_result=false;
    sync_retirement_result lane_result{};
    std::size_t callbacks=0,queued=0,logical_bytes=0;
    static constexpr std::size_t maximum_queued=8,maximum_frame=ordinary_sync_limits::frame_bytes,
        maximum_logical_bytes=4*ordinary_sync_limits::frame_bytes;
    // Private actual payload-destructor receipts. They keep no strong payload
    // or state backedge and have no public positive factory.
    struct capture_return_record {
        enum class kind {packet,callable,page,page_alias,decoded};
        const std::uint64_t process;
        const state* const owner_original;
        const std::weak_ptr<state> owner;
        const std::size_t service_slot;
        const std::uint64_t service_serial,lifecycle;
        const clock::time_point deadline;
        const kind type;
        const void* const parent_original;
        const void* capsule_original=nullptr; // Set by the actual first constructor.
        std::atomic<bool> allocation_entered{false},allocation_returned{false},allocation_exception_returned{false};
        std::atomic<bool> construction_entered{false},construction_returned{false};
        std::atomic<bool> body_entered{false},body_returned{false},body_reentered{false};
        std::atomic<bool> payload_members_destroyed{false},partial_constructor_destroyed{false};
        std::thread::id ending_thread;
        capture_return_record(const std::shared_ptr<state>& original,kind issued,const void* parent)
            :process(original->process),owner_original(original.get()),owner(original),
             service_slot(original->service_slot),service_serial(original->service_serial),lifecycle(original->lifecycle),
             deadline(original->end),type(issued),parent_original(parent){}
    };
    struct capture_issue_first {
        bool attempted=false,returned=false,exception_returned=false;
        std::shared_ptr<capture_return_record> original,first_failure_original;
        std::exception_ptr first_failure;
    };
    std::array<capture_issue_first,5> first_capture_issues{};
    std::uint64_t capture_tail_generation=0;
    bool capture_tail_generation_overflow=false;
    template<typename Capsule,typename... Arguments>
    static std::shared_ptr<Capsule> issue_capture(const std::shared_ptr<state>& owner,
        capture_return_record::kind type,const void* parent,Arguments&&... arguments) {
        require(owner&&owner->process==creator(),"GROUP capture issuer inherited process");
        const auto index=static_cast<std::size_t>(type);bool first=false;
        {std::lock_guard lock(owner->leaf);auto& entry=owner->first_capture_issues[index];
            if(!entry.attempted){entry.attempted=true;first=true;}}
        std::shared_ptr<capture_return_record> record;
        try {
            record=std::make_shared<capture_return_record>(owner,type,parent);
            if(first){std::lock_guard lock(owner->leaf);owner->first_capture_issues[index].original=record;}
            record->allocation_entered.store(true,std::memory_order_release);
            auto value=std::make_shared<Capsule>(record,std::forward<Arguments>(arguments)...);
            record->construction_returned.store(true,std::memory_order_release);
            record->allocation_returned.store(true,std::memory_order_release);
            if(first){std::lock_guard lock(owner->leaf);owner->first_capture_issues[index].returned=true;}
            return value;
        }catch(...) {
            const auto failure=std::current_exception();
            if(record)record->allocation_exception_returned.store(true,std::memory_order_release);
            if(owner->process==creator()) {std::lock_guard lock(owner->leaf);auto& entry=owner->first_capture_issues[index];
                if(first)entry.exception_returned=true;
                if(!entry.first_failure){entry.first_failure=failure;entry.first_failure_original=record;}}
            throw; // No absent or partial capsule receives a successful ending.
        }
    }
    struct capture_tail {
        const std::shared_ptr<capture_return_record> original;
        capture_tail(std::shared_ptr<capture_return_record> issued,const void* capsule):original(std::move(issued)) {
            require(original&&original->process==creator()&&original->allocation_entered.load(std::memory_order_acquire)&&
                !original->construction_entered.exchange(true,std::memory_order_acq_rel),"GROUP capture constructor differs");
            original->capsule_original=capsule;
        }
        capture_tail(const capture_tail&)=delete;
        capture_tail& operator=(const capture_tail&)=delete;
        ~capture_tail()noexcept {
            if(original->process!=creator())return;
            original->ending_thread=std::this_thread::get_id();
            if(original->construction_returned.load(std::memory_order_acquire))
                original->payload_members_destroyed.store(true,std::memory_order_release);
            else original->partial_constructor_destroyed.store(true,std::memory_order_release);
            // First member tail runs AFTER the real capsule members, including
            // packet lease and FIFO data/packet captures. Its wake is later
            // than that publication; the lease's earlier wake is insufficient.
            try {
                const auto data=original->owner.lock();
                if(!data||data.get()!=original->owner_original||data->process!=creator()||
                   data->service_slot!=original->service_slot||data->service_serial!=original->service_serial)return;
                {std::lock_guard lock(data->leaf);
                    if(data->capture_tail_generation<UINT64_MAX)++data->capture_tail_generation;
                    else data->capture_tail_generation_overflow=true;}
                const auto owner=data->receiving_original.lock();
                if(owner&&owner->state_==data)owner->schedule_collection(); // Existing queued collector, outside leaves.
            }catch(...){} // Unknown advisory wake grants no ending or credit.
        }
    };
    struct packet_body_return {
        const std::shared_ptr<capture_return_record> original;
        explicit packet_body_return(std::shared_ptr<capture_return_record> issued):original(std::move(issued)) {
            if(original->process!=creator())return;
            if(original->body_entered.exchange(true,std::memory_order_acq_rel))
                original->body_reentered.store(true,std::memory_order_release);
        }
        ~packet_body_return()noexcept {
            if(original->process==creator()&&!original->body_reentered.load(std::memory_order_acquire))
                original->body_returned.store(true,std::memory_order_release);
            // Actual body return/unwind only. A distinct callable tail must
            // still observe destruction of the FIFO's real payload captures.
        }
    };
    struct receiver_input_ending_record {
        bool observation_attempted=false,first_readiness_refused=false;
        std::size_t readiness_observations=0,observed_callbacks=0;
        std::uint64_t observed_tail_generation=0;
        bool attempted=false,evidence_returned=false,evidence_known=false;
        bool storage_attempted=false,storage_returned=false,storage_exception_returned=false;
        bool extraction_attempted=false,extraction_returned=false,returned=false;
        const ordinary_target_staging_connection* connection_original=nullptr;
        const void* scope_original=nullptr;std::uint64_t generation=0;
        const void* storage_original=nullptr;
        std::array<std::shared_ptr<capture_return_record>,3> packets{},callables{};
        std::shared_ptr<capture_return_record> page,page_alias,decoded,page_packet,page_callable;
        std::array<bool,5> held{},release_attempted{},release_returned{};
        std::size_t held_captures=0,held_packets=0;
        terminal_input_retirement_result result=terminal_input_retirement_result::retained_debt;
        std::exception_ptr first_failure;
    } receiver_input_ending;
    // Actual charged inputs remain held through decoding/native staging.
    // Their coordinates are DATA, not accepted cut or import authority.
    struct ingress {
        // Tail runs after lease destruction, which follows actual wire/charge.
        capture_tail ending;
        configured_attempt_custody::lease lease;
        const std::weak_ptr<state> original;
        const std::size_t bytes;
        std::shared_ptr<const transport_message> copy;
        std::shared_ptr<capture_return_record> callable_original;
        ingress(std::shared_ptr<capture_return_record> issued,configured_attempt_custody::lease value,
            const std::shared_ptr<state>& owner,std::size_t count)
            :ending(std::move(issued),this),lease(std::move(value)),original(owner),bytes(count){}
        ~ingress() {
            copy.reset(); // No physical/logical wire credit before destruction.
            if(auto data=original.lock();data&&data->process==creator()) {
                {std::lock_guard lock(data->leaf);--data->queued;data->logical_bytes-=bytes;}
                data->changed.notify_all();
            }
        }
    };
    // Same process receive reservation precedes codec/retained-output allocation.
    // It is declared before outputs and dies after their real destruction.
    // Downstream Native phases share this SAME slot until their decoded
    // captures are destroyed, including retained failed/unknown originals.
    std::shared_ptr<ordinary_resource_reservation<true>> history_charge;
    struct decoded_history {
        capture_tail ending; // First member publishes after relation/header/progress captures die.
        explicit decoded_history(std::shared_ptr<capture_return_record> issued):ending(std::move(issued),this){}
        const ordinary_history_limits limits{};
        std::optional<ordinary_history_lineage_relation> relation;
        std::optional<ordinary_history_descriptor> header;
        bool offer_returned=false,relation_attempted=false,relation_returned=false;
        bool header_attempted=false,header_returned=false;
        bool stage_entered=false,stage_returned=false,progress_send_entered=false,progress_send_returned=false,progress_send_result=false;
        ordinary_history_progress progress; // Observed progress; next Native stage rejoins its actual retained ending.
        // No current/accepted-cut authority is issued.
    };
    struct page_original {
        capture_tail ending; // First member publishes after decoded/wire/page-alias receipt fields die.
        explicit page_original(std::shared_ptr<capture_return_record> issued):ending(std::move(issued),this){}
        std::shared_ptr<ingress> packet;platform_transport_callbacks endpoint;std::uint64_t lifecycle=0;
        std::shared_ptr<capture_return_record> packet_history,callable_history;
        std::optional<ordinary_history_decoded_page> decoded;
        bool decode_entered=false,decode_returned=false,stage_entered=false,stage_returned=false;
        bool send_entered=false,send_returned=false,send_result=false;
        bool input_pin_issued=false,input_ending_attempted=false,input_ending_returned=false;
        std::weak_ptr<const ordinary_history_decoded_page> input_pin;
        std::shared_ptr<capture_return_record> input_alias_original;
    };
    // One separately identifiable lease controls the actual page/packet alias.
    // It is not a copied decoder/page and cannot grant SQL or transport work.
    struct page_input_lease {
        capture_tail ending; // First member runs after the const actual page reference ends.
        const std::shared_ptr<page_original> page;
        page_input_lease(std::shared_ptr<capture_return_record> issued,std::shared_ptr<page_original> original)
            :ending(std::move(issued),this),page(std::move(original)){}
    };
    struct queued_packet_capture {
        capture_tail ending; // Publishes after every actual FIFO capture below dies.
        const std::shared_ptr<state> data;
        const platform_transport_callbacks endpoint;
        const std::shared_ptr<ingress> packet;
        const std::weak_ptr<ordinary_group_target_receiving_owner> owner;
        const bool opened;
        queued_packet_capture(std::shared_ptr<capture_return_record> issued,std::shared_ptr<state> value,
            platform_transport_callbacks original_endpoint,std::shared_ptr<ingress> original_packet,
            std::weak_ptr<ordinary_group_target_receiving_owner> original_owner,bool was_opened)
            :ending(std::move(issued),this),data(std::move(value)),endpoint(std::move(original_endpoint)),
             packet(std::move(original_packet)),owner(std::move(original_owner)),opened(was_opened){}
    };
    using input_capture_storage=native_retirement_storage<std::shared_ptr<ingress>,std::shared_ptr<ingress>,
        std::shared_ptr<ingress>,std::shared_ptr<page_original>,std::shared_ptr<decoded_history>>;
    // At most one empty storage is allocated before extraction. Unknown first
    // releases leave their remaining actual originals here, with this owner.
    std::unique_ptr<input_capture_storage> terminal_input_storage;
    std::shared_ptr<page_original> page;
    std::shared_ptr<decoded_history> decoded;
    std::array<std::shared_ptr<ingress>,3> first_frames;
    std::array<platform_transport_callbacks,3> first_frame_endpoints;
    std::array<std::uint64_t,3> first_frame_lifecycles{};
    std::size_t first_frame_count=0;
    std::function<void()> cleanup_work,collection_work;
    state(const std::shared_ptr<const ordinary_receiver_wss_request>& original,
        const std::shared_ptr<ordinary_receiver_wss_state>& value,std::size_t slot,std::uint64_t serial)
        :request(original),application(value->application),archive(value->archive),credentials(value->credentials),
         expectation(value->expectation),end(value->deadline),endpoint(value->endpoint),model_profile(value->model_profile),service_slot(slot),service_serial(serial){}
    static void dispose(state* value)noexcept {
        if(!value||value->process!=creator())return;
        const auto process=value->process;const auto slot=value->service_slot;const auto serial=value->service_serial;
        // End every actual state capture BEFORE refunding construction/lifetime
        // credit. A joined wrapper retained by the request still owns its slot.
        delete value;
        if(process!=creator())return; // Actual capture destruction may have crossed a fork.
        std::lock_guard lock(group_receiving_registry_leaf);auto& original=(*group_receiving_registry)[slot];
        if(original.charged&&original.serial==serial&&!original.held)original.charged=false;
    }
};
ordinary_group_target_receiving_owner::ordinary_group_target_receiving_owner(std::shared_ptr<state> value):state_(std::move(value)){}
ordinary_group_target_receiving_owner::~ordinary_group_target_receiving_owner()=default;
void ordinary_group_target_receiving_owner::prepare(
    const std::shared_ptr<const ordinary_receiver_wss_request>& request,std::shared_ptr<ordinary_group_target_receiving_owner>& retained) {
    creator_check();ordinary_receiver_wss_access::require_group_receiving_current(request);
    require(!std::atomic_load(&retained)&&!request->state_->unpublished_target&&!request->state_->staging_io&&!request->state_->staging_connection,
        "GROUP receiving owner cannot transfer an existing creator-bound target");
    group_receiving_construction credit; // Actual lifetime credit BEFORE retained captures/owner allocation.
    auto data=std::shared_ptr<state>(new state(request,request->state_,credit.slot,credit.serial),&state::dispose);
    auto original=std::shared_ptr<ordinary_group_target_receiving_owner>(new ordinary_group_target_receiving_owner(data),
        [](auto* value)noexcept {if(value->state_->process==creator())delete value;});
    data->receiving_original=original; // Weak exact owner binding before any capture issuance.
    // Registry and request both retain SAME original before reserve/factory/thread effects.
    {std::lock_guard lock(group_receiving_registry_leaf);auto& slot=(*group_receiving_registry)[credit.slot];
        require(slot.charged&&slot.serial==credit.serial&&!slot.held,"GROUP receiving actual construction credit changed");
        slot.held=original;credit.published=true;}
    std::atomic_store(&retained,std::move(original));
    const auto published=std::atomic_load(&retained);
    try {
        ordinary_receiver_wss_access::require_group_receiving_current(request);
        data->registry=configured_retirement_registry::instance();
        data->reservation.emplace(data->registry->reserve());
        data->receipt=data->reservation->begin_attempt();
        data->custody=data->registry->attempt_custody(data->receipt);
        require(bool(data->custody),"GROUP receiving configured attempt custody unavailable");
        ordinary_receiver_wss_access::require_group_receiving_current(request);
        data->native_lane_reservation_entered=true;
        data->native_lane.emplace(sync_retirement_lane::instance()->reserve_empty());
        data->native_lane_reservation_returned=true;
        const auto weak=std::weak_ptr<ordinary_group_target_receiving_owner>(published);
        // Fixed one cleanup and one coalesced collector capture are prepared
        // before any worker or foreign allocation. No payload/ticket minting API.
        data->cleanup_work=[weak]{if(auto owner=weak.lock())owner->retire_on_worker();};
        data->collection_work=[weak]{if(auto owner=weak.lock())owner->collect_on_worker();};
        data->custody->bind_wakeup([weak]{if(auto owner=weak.lock())owner->schedule_collection();});
        ordinary_receiver_wss_access::require_group_receiving_current(request);
        {std::lock_guard lock(data->leaf);data->preparation_active=false;data->preparation_returned=true;}
        data->changed.notify_all();
    }catch(...) {
        {std::lock_guard lock(data->leaf);data->preparation_active=false;data->preparation_returned=true;}
        published->remember_failure(std::current_exception());published->request_close();throw;
    }
}
void ordinary_group_target_receiving_owner::remember_failure(std::exception_ptr error)noexcept {
    const auto data=state_;if(!data||data->process!=creator())return;
    {std::lock_guard lock(data->leaf);if(!data->first_failure)data->first_failure=std::move(error);}data->changed.notify_all();
}
void ordinary_group_target_receiving_owner::start(const std::string& token) {
    const auto data=state_;require(data&&data->process==creator(),"GROUP receiving creator unavailable");
    const auto original=data->request.lock();ordinary_receiver_wss_access::require_group_receiving_current(original);
    {std::lock_guard lock(data->leaf);require(!data->stop&&!data->startup_active&&!data->worker_creation_entered,
        "GROUP receiving start already consumed or stopped");data->startup_active=true;}
    try {
        require(token.size()<=16384,"GROUP receiving credential capture exceeds fixed bound");
        auto initial=data->custody->admit(configured_attempt_custody::kind::payload);
        auto timer=data->custody->admit(configured_attempt_custody::kind::payload);
        require(bool(initial)&&bool(timer),"GROUP receiving initial command capacity unavailable");
        data->token=token;
        ordinary_receiver_wss_access::require_group_receiving_current(original);
        {std::lock_guard lock(data->leaf);require(!data->stop,"GROUP receiving stopped before worker creation");data->worker_creation_entered=true;}
        auto worker=std::make_shared<std_thread_scheduler>();
        // The actual returned worker is published under the SAME cancellation
        // leaf; off-leaf native calls always use a strong local snapshot.
        {std::lock_guard lock(data->leaf);data->worker=worker;data->worker_creation_returned=true;}
        ordinary_receiver_wss_access::require_group_receiving_current(original);
        const auto weak=std::weak_ptr<ordinary_group_target_receiving_owner>(std::atomic_load(&original->state_->group_receiver));
        {std::lock_guard lock(data->leaf);require(!data->stop,"GROUP receiving stopped before timer admission");data->timer_launch_entered=true;}
        data->custody->launch_worker(std::move(timer),[weak,data]{
            if(data->process!=creator())return;
            {std::unique_lock lock(data->leaf);data->changed.wait_until(lock,data->end,[&]{return data->stop;});}
            if(data->process!=creator())return;
            if(auto owner=weak.lock())owner->request_close();
        });
        {std::lock_guard lock(data->leaf);data->timer_launch_returned=true;}
        auto work=retain_configured_payload(std::move(initial),[weak]{if(auto owner=weak.lock())owner->prepare_on_worker();});
        {std::lock_guard lock(data->leaf);require(!data->stop,"GROUP receiving stopped before target worker admission");data->initial_queue_entered=true;}
        worker->invoke(std::move(work));
        {std::lock_guard lock(data->leaf);data->initial_queue_returned=true;data->startup_active=false;data->startup_returned=true;}
        data->changed.notify_all();schedule_collection();
    }catch(...) {
        {std::lock_guard lock(data->leaf);data->startup_active=false;data->startup_returned=true;}
        remember_failure(std::current_exception());request_close();throw;
    }
}
void ordinary_group_target_receiving_owner::require_worker_current()const {
    const auto data=state_;require(data&&data->process==creator(),"GROUP receiving inherited native entry");
    // This identity was observed INSIDE the actual retained worker, not copied
    // from scheduler.can_invoke/is_on_thread or a caller supplied Boolean.
    {std::lock_guard lock(data->leaf);require(data->worker_entered&&data->creator_thread==std::this_thread::get_id()&&!data->stop,
        "GROUP receiving native entry is not its live creator worker");}
    const auto original=data->request.lock();ordinary_receiver_wss_access::require_group_receiving_current(original);
    require(std::atomic_load(&original->state_->group_receiver).get()==this&&original->state_->application==data->application&&
        original->state_->archive==data->archive&&original->state_->credentials==data->credentials&&
        original->state_->expectation==data->expectation&&original->state_->deadline==data->end&&clock::now()<data->end,
        "GROUP receiving actual original association changed");
}
std::shared_ptr<const ordinary_group_target_receiving_owner>
ordinary_group_target_receiving_owner::terminal_abandonment_original(
    const ordinary_target_staging_connection& connection,
    const std::shared_ptr<const ordinary_unpublished_group_target>& target,
    const std::shared_ptr<ordinary_target_staging_io>& io)const {
    const auto data=state_;require(data&&data->process==creator(),"GROUP terminal inherited owner");
    // Actual creator check precedes the inherited mutex or mutable custody.
    {std::lock_guard lock(data->leaf);
        require(data->worker_entered&&data->creator_thread==std::this_thread::get_id()&&
            data->stop&&data->cleanup_entered&&data->target_preparation_entered&&
            data->connection.get()==&connection&&data->target==target&&data->io==io,
            "GROUP terminal actual worker/target stop association differs");}
    const auto request=data->request.lock();
    require(request&&request->state_&&request->state_->process==data->process&&
        request->state_->cancelled.load(std::memory_order_acquire),"GROUP terminal actual request not creator-cancelled");
    const auto owner=std::atomic_load(&request->state_->group_receiver);
    require(owner.get()==this&&request->state_->application==data->application&&request->state_->archive==data->archive&&
        request->state_->credentials==data->credentials&&request->state_->expectation==data->expectation&&
        request->state_->deadline==data->end&&connection.original_deadline()==data->end&&
        request->state_->staging_connection.get()==&connection&&request->state_->unpublished_target==target&&
        request->state_->staging_io==io,"GROUP terminal actual request originals differ");
    return owner; // Actual retained owner producer; no live admission or new end.
}

void ordinary_group_target_receiving_owner::prepare_on_worker() {
    const auto data=state_;if(!data||data->process!=creator())return;
    {std::lock_guard lock(data->leaf);if(data->worker_entered||data->stop)return;
        data->creator_thread=std::this_thread::get_id();data->worker_entered=true;}
    try {
        require_worker_current();const auto original=data->request.lock();
        {std::lock_guard lock(data->leaf);require(!data->stop,"GROUP receiving stopped before target preparation");data->target_preparation_entered=true;}
        try {ordinary_receiver_wss_access::prepare_group_on_receiving_worker(original);
            {std::lock_guard lock(data->leaf);data->target_preparation_returned=true;}}
        catch(...) {
            // Each producer publishes directly into the real request before
            // postchecks. Preserve partial originals before propagation.
            data->target=original->state_->unpublished_target;data->io=original->state_->staging_io;
            data->connection=original->state_->staging_connection;data->schema=original->state_->target_schema;throw;
        }
        data->target=original->state_->unpublished_target;data->io=original->state_->staging_io;
        data->connection=original->state_->staging_connection;data->schema=original->state_->target_schema;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
        if(data->qualification_checkpoint) {
            // The private checkpoint already repeated actual request/archive/
            // credential/worker fences. Only terminal FIFO cleanup follows.
            request_close();return;
        }
#endif
        require_worker_current();data->factory=get_network_factory();
        auto* factory=dynamic_cast<configured_platform_factory*>(data->factory.get());
        require(data->factory&&factory,"GROUP receiving requires actual typed configured factory");
        require_worker_current();std::shared_ptr<std_thread_scheduler> worker;
        {std::lock_guard lock(data->leaf);require(!data->stop,"GROUP receiving stopped before factory entry");worker=data->worker;data->factory_entered=true;}
        auto returned=factory->create_configured_sync_transport(worker,data->receipt);
        if(data->process!=creator()) {returned.release();return;} // Inherited explicit native custody is quarantined.
        data->factory_returned=true;
        data->transport=std::shared_ptr<sync_transport>(std::move(returned));
        // Retain actual provider result before current/type diagnostics.
        require(data->transport&&data->registry->retain_transport(data->receipt,data->transport),"GROUP receiving actual transport retention failed");
        require_worker_current();data->typed_transport=dynamic_cast<owned_platform_sync_transport*>(data->transport.get());
        require(data->typed_transport,"GROUP receiving requires owned system-TLS transport");
        const auto weak=std::weak_ptr<ordinary_group_target_receiving_owner>(std::atomic_load(&original->state_->group_receiver));
        data->native_lane->bind(data->transport,{},[weak](sync_retirement_result result){
            if(auto owner=weak.lock()) {const auto held=owner->state_;if(held->process!=creator())return;
                {std::lock_guard lock(held->leaf);held->lane_result=result;held->native_lane_returned=true;}
                owner->schedule_collection();held->changed.notify_all();}
        });
        data->native_lane_bound=true;data->native_lane->publish();
        HeadersMap headers;headers["Authorization"]="Bearer "+data->token;
        require_worker_current();data->connect_entered=true;
        data->typed_transport->connect_with_attempt_handlers(data->endpoint,headers,
            [weak](const auto& endpoint){if(auto owner=weak.lock())owner->enqueue_event(endpoint,nullptr,true,false);},
            [weak](const auto& endpoint,const auto& message){if(auto owner=weak.lock())owner->enqueue_event(endpoint,&message,false,false);},
            [weak](const auto& endpoint,const std::string& reason){if(auto owner=weak.lock())owner->enqueue_event(endpoint,nullptr,false,true,&reason,0,true);},
            [weak](const auto& endpoint,int code,const std::string& reason){if(auto owner=weak.lock())owner->enqueue_event(endpoint,nullptr,false,true,&reason,code,false);});
        data->connect_returned=true;require_worker_current();
    }catch(...) {remember_failure(std::current_exception());request_close();}
}
void ordinary_group_target_receiving_owner::enqueue_event(const platform_transport_callbacks& endpoint,
    const transport_message* message,bool opened,bool terminal,const std::string* reason,int code,bool error)noexcept {
    const auto data=state_;if(!data||data->process!=creator())return;
    bool entered=false;const std::size_t bytes=message?message->data.size():0;
    try {
        {std::lock_guard lock(data->leaf);if(data->stop)return;++data->callbacks;entered=true;}
        if(terminal) {
            bool first=false;
            {std::lock_guard lock(data->leaf);if(!data->terminal_observed) {
                data->terminal_observed=true;data->terminal_error=error;data->terminal_code=code;
                data->terminal_endpoint=endpoint;data->terminal_reason_bytes=reason?reason->size():0;first=true;}}
            if(first) {
                // One fixed16KiB diagnostic slot was reserved with this owner.
                // Actual endpoint/code/length precede any fallible text copy.
                require(!reason||reason->size()<=16384,"GROUP first native terminal reason exceeds bounded retention");
                const auto retained=std::make_shared<const std::string>(reason?*reason:std::string{});
                {std::lock_guard lock(data->leaf);data->terminal_reason=retained;data->terminal_reason_retained=true;}
                throw db_error(*retained);
            }
            request_close();
        } else {
        require(endpoint.current_system_tls_for_owner(),"GROUP receiving callback lacks actual system-TLS endpoint");
        require(!message||(message->msg_type==transport_message::type::binary&&bytes&&bytes<=state::maximum_frame),
            "GROUP receiving ingress shape/bound");
        const auto original=data->request.lock();
        require(bool(original),"GROUP receiving actual request retired before ingress");
        auto use=data->custody->admit(configured_attempt_custody::kind::payload);
        require(bool(use),"GROUP receiving native ingress custody unavailable");
        {std::lock_guard lock(data->leaf);require(!data->stop&&data->queued<state::maximum_queued&&
            bytes<=state::maximum_logical_bytes-data->logical_bytes,"GROUP receiving logical ingress capacity exhausted");
            ++data->queued;data->logical_bytes+=bytes;}
        std::shared_ptr<state::ingress> packet;
        try {packet=state::issue_capture<state::ingress>(data,state::capture_return_record::kind::packet,nullptr,
            std::move(use),data,bytes);}
        catch(...) {std::lock_guard lock(data->leaf);--data->queued;data->logical_bytes-=bytes;throw;}
        // Actual charge/count precedes both the wire copy and queued captures.
        if(message)packet->copy=std::make_shared<const transport_message>(*message);
        const auto weak=std::weak_ptr<ordinary_group_target_receiving_owner>(std::atomic_load(&original->state_->group_receiver));
        std::shared_ptr<std_thread_scheduler> worker;
        {std::lock_guard lock(data->leaf);require(!data->stop,"GROUP receiving stopped before retained ingress enqueue");worker=data->worker;}
        require(bool(worker),"GROUP receiving actual worker unavailable at ingress");
        auto captures=state::issue_capture<state::queued_packet_capture>(data,state::capture_return_record::kind::callable,
            packet.get(),data,endpoint,packet,weak,opened);
        packet->callable_original=captures->ending.original;
        worker->invoke([captures]{
            const auto& data=captures->data;const auto& endpoint=captures->endpoint;const auto& packet=captures->packet;
            const auto& weak=captures->owner;const bool opened=captures->opened;
            if(data->process!=creator())return;
            state::packet_body_return body(packet->ending.original);
            if(auto owner=weak.lock())try {
                owner->require_worker_current();
                if(opened)owner->opened_on_worker(endpoint);
                else {
                    if(data->model_agreed) {
                        // SAME charged native endpoint/message is retained BEFORE
                        // any decoder/native-stage allocation or diagnostics.
                        if(data->first_frame_count<data->first_frames.size()) {
                            const auto index=data->first_frame_count++;
                            data->first_frames[index]=packet;data->first_frame_endpoints[index]=endpoint;
                            data->first_frame_lifecycles[index]=data->lifecycle;
                        }else {
                            require(data->decoded&&data->decoded->stage_returned&&data->decoded->progress_send_returned&&
                                data->decoded->progress_send_result&&!data->decoded->progress.complete,
                                "GROUP page ingress precedes actual durable header/progress or follows complete stage");
                            if(data->page)require(data->page->stage_returned&&data->page->send_returned&&data->page->send_result,
                                "GROUP previous actual page retains decoding/native/send debt");
                            auto next=state::issue_capture<state::page_original>(data,state::capture_return_record::kind::page,nullptr);
                            next->packet=packet;next->endpoint=endpoint;next->lifecycle=data->lifecycle;
                            next->packet_history=packet->ending.original;next->callable_history=packet->callable_original;
                            data->page=std::move(next); // Charged actual ingress before parse/native errors.
                        }
                    }
                    owner->message_on_worker(endpoint,*packet->copy);
                }
            }catch(...) {owner->remember_failure(std::current_exception());owner->request_close();}
        });
        }
    }catch(...) {remember_failure(std::current_exception());request_close();}
    if(entered&&data->process==creator()) {
        {std::lock_guard lock(data->leaf);--data->callbacks;}data->changed.notify_all();schedule_collection();
    }
}
void ordinary_group_target_receiving_owner::opened_on_worker(const platform_transport_callbacks& endpoint) {
    require_worker_current();const auto data=state_;
    require(endpoint.current_system_tls_for_owner()&& !data->model_send_entered,
        "GROUP receiving endpoint replaced/reopened");
    data->endpoint_original=endpoint;data->model_request=uuid_t::generate().to_string();data->history_request=uuid_t::generate().to_string();
    data->negotiation_end=std::min(data->end,clock::now()+std::chrono::seconds(5));
    const auto wire=nlohmann::json{{"latticeOrdinarySync",{{"version",2},{"kind","modelRequest"},
        {"request",data->model_request},{"modelProfile",data->model_profile}}}}.dump();
    auto message=transport_message::from_string(wire);message.msg_type=transport_message::type::binary;
    require_worker_current();require(endpoint.current_system_tls_for_owner(),"GROUP model send endpoint retired");
    data->model_send_entered=true;
    data->model_send_result=data->typed_transport->send_to_attempt(endpoint,message);
    data->model_send_returned=true;
    require_worker_current();require(data->model_send_result&&endpoint.current_system_tls_for_owner(),"GROUP model send refused/retired");
}
void ordinary_group_target_receiving_owner::message_on_worker(const platform_transport_callbacks& endpoint,const transport_message& message) {
    require_worker_current();const auto data=state_;
    require(endpoint.matches(data->endpoint_original)&&endpoint.current_system_tls_for_owner()&&data->connect_returned&&
        data->model_send_returned&&data->model_send_result,"GROUP receiving endpoint/lifecycle is not its actual opened attempt");
    if(!data->model_agreed) {
        require(clock::now()<data->negotiation_end&&message.data.size()<=4096,"GROUP model negotiation expired/oversized");
        const auto envelope=parse(message.data.data(),message.data.size(),4096);
        require(envelope.is_object()&&envelope.size()==1&&envelope.contains("latticeOrdinarySync"),"GROUP model envelope shape");
        const auto& value=envelope.at("latticeOrdinarySync");
        require(value.is_object()&&value.size()==4&&value.contains("version")&&value.at("version").is_number_integer()&&value.at("version")==2&&
            value.contains("kind")&&value.at("kind")=="modelOffer"&&value.contains("request")&&value.at("request")==data->model_request&&
            value.contains("modelProfile")&&value.at("modelProfile")==data->model_profile,"GROUP model offer correlation differs");
        require_worker_current();require(clock::now()<data->negotiation_end&&endpoint.current_system_tls_for_owner(),"GROUP model offer expired during parse");
        data->model_agreed=true;
        const auto wire=nlohmann::json{{"latticeOrdinarySync",{{"version",2},{"kind","request"},{"request",data->history_request},
            {"profile","paged-history-v1"},{"relation","mounted-lineage-frontier-v1"}}}}.dump();
        auto message=transport_message::from_string(wire);message.msg_type=transport_message::type::binary;
        require_worker_current();require(clock::now()<data->negotiation_end&&endpoint.current_system_tls_for_owner(),"GROUP history request expired before send");
        data->history_send_entered=true;
        data->history_send_result=data->typed_transport->send_to_attempt(endpoint,message);
        data->history_send_returned=true;
        require_worker_current();require(data->history_send_result&&endpoint.current_system_tls_for_owner(),"GROUP history request refused/retired");
        return;
    }
    require(data->history_send_returned&&data->history_send_result&&data->first_frame_count&&
        data->first_frame_count<=data->first_frames.size()&&data->target_preparation_returned,
        "GROUP history ingress precedes actual request/catalog return");
    if(data->page&&data->page->packet&&data->page->packet->copy.get()==&message) {
        receive_page(endpoint,message);return;
    }
    const auto at=data->first_frame_count-1;
    const auto packet=data->first_frames[at];
    require(packet&&packet->copy.get()==&message&&data->first_frame_endpoints[at].matches(endpoint)&&
        data->first_frame_lifecycles[at]==data->lifecycle,"GROUP history input is not its actual retained native ingress");
    const auto request=data->request.lock();ordinary_receiver_wss_access::require_group_history_ingress(request);
    if(!data->decoded) {
        ordinary_resource_reservation<true> charge; // Reserve before holder/output allocation.
        data->history_charge=std::make_shared<ordinary_resource_reservation<true>>(std::move(charge));
        data->decoded=state::issue_capture<state::decoded_history>(data,state::capture_return_record::kind::decoded,nullptr);
    }
    const auto decoded=data->decoded;
    const std::string_view raw(reinterpret_cast<const char*>(message.data.data()),message.data.size());
    if(at==0) {
        require(!decoded->offer_returned&&clock::now()<data->negotiation_end,"GROUP history offer already consumed/expired");
        ordinary_group_history_codec::offer(raw,data->history_request);
        decoded->offer_returned=true;
        require(clock::now()<data->negotiation_end,"GROUP history offer expired during parse");
    }else if(at==1) {
        require(decoded->offer_returned&&!decoded->relation_attempted,"GROUP history relation is out of order");
        decoded->relation_attempted=true;
        decoded->relation=ordinary_group_history_codec::relation(raw,*data->expectation,data->history_request,
            data->expectation->channel,data->model_profile);
        decoded->relation_returned=true;
    }else {
        require(decoded->offer_returned&&decoded->relation_returned&&decoded->relation&&!decoded->header_attempted,
            "GROUP history header is out of order");
        decoded->header_attempted=true;
        decoded->header=ordinary_group_history_codec::header(raw,decoded->limits);
        // Preserve the actual decoded output before any matching/current failure.
        decoded->header_returned=true;
        ordinary_group_history_codec::match(*decoded->relation,*decoded->header);
        require(decoded->header->projection.models==std::vector<std::string>{"Edge","GroupProjectMap","Memory"},
            "GROUP history root selector differs from actual fixed GROUP catalog");
    }
    ordinary_receiver_wss_access::require_group_history_ingress(request);
    require_worker_current();require(endpoint.matches(data->endpoint_original)&&endpoint.current_system_tls_for_owner(),
        "GROUP history ingress retired during decode");
    if(at==2) {
        require(!decoded->stage_entered,"GROUP history header stage already attempted");
        decoded->stage_entered=true;
        const auto same=std::atomic_load(&request->state_->group_receiver);
        data->connection->stage_raw_history_header(same,data->end);
        decoded->stage_returned=true; // Actual Native ending already verified inside the same Conn.
        decoded->progress.complete=decoded->header->pages==0;
        if(decoded->header->pages==0)data->connection->validate_history_projection(same,data->end);
        send_header_progress(endpoint);
        if(decoded->progress.complete) {
            data->connection->verify_history_chain(same,data->end);
            data->connection->suspend_import(same,data->end);
            data->connection->import_history_rows(same,data->end);
        }
    }

}
const ordinary_history_descriptor& ordinary_group_target_receiving_owner::require_header_original(const ordinary_target_staging_connection& connection)const {
    require_worker_current();const auto data=state_;const auto request=data->request.lock();
    ordinary_receiver_wss_access::require_group_history_ingress(request);
    require(data->connection.get()==&connection&&request->state_->staging_connection==data->connection&&
        data->decoded&&data->decoded->header_attempted&&data->decoded->header_returned&&data->decoded->header&&
        data->decoded->relation_returned&&data->decoded->relation&&data->first_frame_count==3,
        "GROUP history header is not its actual same-worker original");
    const auto packet=data->first_frames[2];const auto& header=*data->decoded->header;
    require(packet&&packet->copy&&data->first_frame_lifecycles[2]==data->lifecycle&&
        data->first_frame_endpoints[2].matches(data->endpoint_original)&&data->endpoint_original.current_system_tls_for_owner()&&
        header.header.size()==packet->copy->data.size()&&
        !std::memcmp(packet->copy->data.data(),header.header.data(),header.header.size()),
        "GROUP history header raw/endpoint/lifecycle original changed");
    return header; // Borrowed DATA; the native writer must still enter its exact private role.
}
const std::shared_ptr<ordinary_resource_reservation<true>>& ordinary_group_target_receiving_owner::require_decoder_charge(
    const ordinary_target_staging_connection& connection)const {
    (void)require_header_original(connection);
    require(bool(state_->history_charge),"GROUP decoder SAME actual receiving reservation unavailable");
    return state_->history_charge; // Strong lifetime custody; no new slot or role is issued.
}
const std::string& ordinary_group_target_receiving_owner::require_header_channel(const ordinary_target_staging_connection& connection)const {
    (void)require_header_original(connection);return state_->expectation->channel;
}
const std::shared_ptr<ordinary_group_target_schema_original>& ordinary_group_target_receiving_owner::require_header_schema(const ordinary_target_staging_connection& connection)const {
    (void)require_header_original(connection);return state_->schema;
}
void ordinary_group_target_receiving_owner::send_header_progress(const platform_transport_callbacks& endpoint) {
    const auto data=state_;const auto decoded=data->decoded;
    const auto& header=require_header_original(*data->connection);data->connection->require_history_header_ending(*this,data->end);
    if(header.pages==0)data->connection->require_history_projection_ending(*this,data->end);
    require(decoded->stage_returned&&!decoded->progress_send_entered,"GROUP history header progress already attempted/not staged");
    const auto wire=nlohmann::json{{"latticeOrdinarySync",{{"version",2},{"profile","paged-history-v1"},{"kind","historyProgress"},
        {"source",header.projection.source},{"epoch",std::to_string(header.projection.epoch)},{"cut",std::to_string(header.projection.cut)},
        {"stage",header.stage},{"nextPage","0"},{"previousDigest",""},{"stagedComplete",header.pages==0}}}}.dump();
    auto message=transport_message::from_string(wire);message.msg_type=transport_message::type::binary;
    require_header_original(*data->connection);data->connection->require_history_header_ending(*this,data->end);
    require(endpoint.matches(data->endpoint_original)&&endpoint.current_system_tls_for_owner(),"GROUP staged header endpoint retired before send");
    decoded->progress_send_entered=true;
    decoded->progress_send_result=data->typed_transport->send_to_attempt(endpoint,message);
    decoded->progress_send_returned=true; // SAME first transport return before any later diagnostics.
    require_header_original(*data->connection);
    require(decoded->progress_send_result&&endpoint.current_system_tls_for_owner(),"GROUP durable header progress refused/retired");
    // This is passive header staging only. It is no accepted-cut receipt,
    // original ACK/prune instruction, model import or selected-store publication.
}
const ordinary_history_decoded_page& ordinary_group_target_receiving_owner::require_page_original(const ordinary_target_staging_connection& connection)const {
    (void)require_header_original(connection);const auto data=state_;const auto page=data->page;
    require(page&&page->packet&&page->packet->copy&&page->decoded&&page->decode_entered&&page->decode_returned&&
        page->lifecycle==data->lifecycle&&page->endpoint.matches(data->endpoint_original)&&page->endpoint.current_system_tls_for_owner()&&
        page->decoded->raw.size()==page->packet->copy->data.size()&&
        !std::memcmp(page->decoded->raw.data(),page->packet->copy->data.data(),page->decoded->raw.size()),
        "GROUP page is not SAME decoded actual charged ingress/endpoint/lifecycle");
    return *page->decoded; // DATA only; Native caller still joins prior real ending and enters its exact page role.
}
std::shared_ptr<const ordinary_history_decoded_page> ordinary_group_target_receiving_owner::require_page_input_pin(
    const ordinary_target_staging_connection& connection)const {
    const auto& input=require_page_original(connection);const auto page=state_->page;
    require(page&&page->decoded&&(&*page->decoded==&input),"GROUP raw input pin lost actual page original");
    require(!page->input_pin_issued&&!page->input_ending_attempted,"GROUP raw input lease cannot be reissued");
    page->input_pin_issued=true; // No reset after lease construction failure.
    const auto lease=state::issue_capture<state::page_input_lease>(state_,state::capture_return_record::kind::page_alias,page.get(),page);
    page->input_alias_original=lease->ending.original;
    auto pin=std::shared_ptr<const ordinary_history_decoded_page>(lease,&input);
    page->input_pin=pin;return pin; // Alias owns SAME page/packet/decoded bytes.
}
void ordinary_group_target_receiving_owner::end_page_input_pin(const ordinary_target_staging_connection& connection,
    std::shared_ptr<const ordinary_history_decoded_page>& pin)const {
    const auto& input=require_page_original(connection);const auto data=state_;const auto page=data->page;
    require(page&&pin.get()==&input&&page->input_pin_issued&&!page->input_ending_attempted&&
        page->stage_entered&&page->stage_returned&&page->send_entered&&page->send_returned&&page->send_result&&
        page->endpoint.matches(data->endpoint_original)&&page->endpoint.current_system_tls_for_owner(),
        "GROUP raw input lacks actual Native/send return/endpoint ending");
    const auto progress=connection.history_page_progress(*this,data->end);
    require(progress.next_page==input.index+1&&progress.previous==input.full_digest,
        "GROUP raw input differs from SAME joined SQL/raw progress");
    // Lock the actual issued control block, not another alias into page. Two
    // references are this lock and the SAME append's pin; any extra borrower
    // refuses destruction. This count grants no IO, SQL, ACK or replacement.
    auto actual=page->input_pin.lock();
    require(actual&&actual.get()==pin.get()&&!actual.owner_before(pin)&&!pin.owner_before(actual)&&pin.use_count()==2,
        "GROUP raw input lease still has a borrower/does not match actual issuer");
    page->input_ending_attempted=true;
    actual.reset();pin.reset(); // Actual lease destruction returns before member destruction.
    require(page->input_pin.expired(),"GROUP raw input alias destruction did not end");
    page->decoded.reset();page->packet.reset(); // End actual decoded/wire captures before reservation release.
    page->input_ending_returned=true;
}
void ordinary_group_target_receiving_owner::receive_page(const platform_transport_callbacks& endpoint,const transport_message& message) {
    const auto data=state_;const auto page=data->page;const auto decoded=data->decoded;
    const auto& header=require_header_original(*data->connection);
    require(page&&page->packet&&page->packet->copy.get()==&message&&endpoint.matches(page->endpoint)&&
        !page->decode_entered&&!decoded->progress.complete,"GROUP page decode original/order unavailable");
    page->decode_entered=true;
    page->decoded=ordinary_group_history_codec::page(
        std::string_view(reinterpret_cast<const char*>(message.data.data()),message.data.size()),header,decoded->progress,decoded->limits);
    page->decode_returned=true; // Actual typed output is held before post-current/matching errors.
    require_page_original(*data->connection);
    const auto request=data->request.lock();ordinary_receiver_wss_access::require_group_history_ingress(request);
    const auto same=std::atomic_load(&request->state_->group_receiver);
    require(same.get()==this,"GROUP page actual request owner changed before Native stage");
    page->stage_entered=true;data->connection->stage_raw_history_page(same,data->end);page->stage_returned=true;
    decoded->progress=data->connection->history_page_progress(*this,data->end);
    if(decoded->progress.complete)data->connection->validate_history_projection(same,data->end);
    send_page_progress(endpoint);
    if(decoded->progress.complete) {
        data->connection->verify_history_chain(same,data->end);
        data->connection->suspend_import(same,data->end);
        data->connection->import_history_rows(same,data->end);
    }
}
void ordinary_group_target_receiving_owner::send_page_progress(const platform_transport_callbacks& endpoint) {
    const auto data=state_;const auto page=data->page;const auto& header=require_header_original(*data->connection);
    (void)require_page_original(*data->connection);const auto progress=data->connection->history_page_progress(*this,data->end);
    if(progress.complete)data->connection->require_history_projection_ending(*this,data->end);
    require(page->stage_returned&&!page->send_entered,"GROUP page progress lacks actual durable original/was attempted");
    const auto wire=nlohmann::json{{"latticeOrdinarySync",{{"version",2},{"profile","paged-history-v1"},{"kind","historyProgress"},
        {"source",header.projection.source},{"epoch",std::to_string(header.projection.epoch)},{"cut",std::to_string(header.projection.cut)},
        {"stage",header.stage},{"nextPage",std::to_string(progress.next_page)},{"previousDigest",progress.previous},{"stagedComplete",progress.complete}}}}.dump();
    auto message=transport_message::from_string(wire);message.msg_type=transport_message::type::binary;
    require_page_original(*data->connection);(void)data->connection->history_page_progress(*this,data->end);
    require(endpoint.matches(data->endpoint_original)&&endpoint.current_system_tls_for_owner(),"GROUP page progress endpoint retired");
    page->send_entered=true;page->send_result=data->typed_transport->send_to_attempt(endpoint,message);page->send_returned=true;
    require_page_original(*data->connection);require(page->send_result&&endpoint.current_system_tls_for_owner(),"GROUP staged page progress refused/retired");
    // Actual input/decoded capacities are ended after the real Native VM/COMMIT/leave
    // and actual send return. Its Native static binding copies remain on original.
    // The exact raw append original may still own an alias into this optional.
    // A later replacement/reset of data->page drops only the receiver reference;
    // the alias retains the actual decoded member and ingress until its ending.
    if(page->input_pin_issued)data->connection->end_raw_history_page_input(*this,data->end);
    else {page->decoded.reset();page->packet.reset();}
}
void ordinary_group_target_receiving_owner::request_close()noexcept {
    const auto data=state_;if(!data||data->process!=creator())return;
    if(const auto original=data->request.lock())original->state_->cancelled.store(true,std::memory_order_release);
    std::function<void()> work;std::shared_ptr<std_thread_scheduler> worker;bool never_entered=false;
    try {
        {std::lock_guard lock(data->leaf);data->stop=true;worker=data->worker;
            if(worker&&!data->cleanup_queued){data->cleanup_queued=true;work=data->cleanup_work;}
            never_entered=!data->preparation_active&&!data->startup_active&&!data->worker_creation_entered&&!data->target_preparation_entered&&!data->factory_entered;}
        data->changed.notify_all();if(work)worker->invoke(std::move(work));
        if(!worker&&never_entered)finish_never_started();
        if(worker)schedule_collection();
    }catch(...) {remember_failure(std::current_exception());}
}
void ordinary_group_target_receiving_owner::finish_never_started()noexcept {
    const auto data=state_;if(!data||data->process!=creator())return;
    try {
        {std::lock_guard lock(data->leaf);
            if(!data->stop||data->preparation_active||data->startup_active||data->worker_creation_entered||data->target_preparation_entered||
               data->factory_entered||data->never_started_entered||data->collected||
               (data->native_lane_reservation_entered&&!data->native_lane_reservation_returned))return;
            data->never_started_entered=true;}
        // These are the same owner's actual effect-entry records. No worker,
        // target producer or foreign factory can enter after its elected stop.
        if(data->custody) {const auto counts=data->custody->snapshot();
            if(counts.commands||counts.payloads||counts.workers||counts.first_error)return;}
        data->native_lane.reset(); // Actual still-empty native reservation ends.
        bool collected=true;
        if(data->registry&&data->receipt.valid()) {
            data->registry->request_retirement(data->receipt);
            data->receipt.complete_adapter_cleanup(0); // Foreign factory NEVER entered.
            data->registry->complete_native_cleanup(data->receipt,0);
            collected=data->registry->collect_completed(data->receipt);
        }
        {std::lock_guard lock(data->leaf);if(collected){data->never_started_ending=true;data->collected=true;}}
        if(collected)release_service_custody();data->changed.notify_all();
    }catch(...) {remember_failure(std::current_exception());}
}
void ordinary_group_target_receiving_owner::release_service_custody()noexcept {
    const auto data=state_;if(!data||data->process!=creator())return;
    {std::lock_guard lock(data->leaf);
        if(data->service_released||!data->collected||(!data->worker_joined&&!data->never_started_ending)||
            (data->target_preparation_entered&&!data->sqlite_and_files_joined)||data->callbacks||data->queued)return;
        data->service_released=true;}
    // Actual Native collection plus original worker ending releases service
    // retention only. Lifetime credit stays charged until state::dispose has
    // destroyed the actual retained captures/diagnostics/worker wrapper.
    // Unknown/open/debt objects never reach this exact-object join.
    data->reservation.reset();
    std::shared_ptr<ordinary_group_target_receiving_owner> released;
    {std::lock_guard lock(group_receiving_registry_leaf);auto& slot=(*group_receiving_registry)[data->service_slot];
        if(slot.charged&&slot.serial==data->service_serial&&slot.held.get()==this)released=std::move(slot.held);}
    released.reset(); // Off registration leaf, caller/request still retain it.
}
ordinary_group_target_receiving_owner::terminal_input_retirement_result
ordinary_group_target_receiving_owner::retire_terminal_inputs(const ordinary_target_staging_connection& connection)noexcept {
    const auto data=state_;if(!data||data->process!=creator())return terminal_input_retirement_result::retained_debt;
    {
        std::lock_guard lock(data->leaf);
        if(!data->worker_entered||data->creator_thread!=std::this_thread::get_id()||!data->stop||!data->cleanup_entered||
           data->connection.get()!=&connection||data->callbacks)return terminal_input_retirement_result::retained_debt;
        // Stop seals admission. Callback quiescence precedes any bounded
        // readiness observation, and its existing returned callback wake
        // supplies the later retry without charging intermediate counts.
        const auto& first=data->receiver_input_ending;
        if(first.attempted||first.storage_attempted)return first.returned?first.result:terminal_input_retirement_result::retained_debt;
    }
    const void* scope=nullptr;std::uint64_t generation=0;
    // Old predicates are called only while the actual input originals still
    // exist, before any extraction. No new SQL, lease, deadline or admission.
    if(!connection.terminal_receiver_input_context_known(*this,scope,generation))return terminal_input_retirement_result::retained_debt;
    const auto valid=[&](const auto& record,state::capture_return_record::kind type,const void* capsule,const void* parent) {
        return record&&record->process==data->process&&record->owner_original==data.get()&&
            record->service_slot==data->service_slot&&record->service_serial==data->service_serial&&
            record->lifecycle==data->lifecycle&&record->deadline==data->end&&record->type==type&&
            record->capsule_original&&record->capsule_original==capsule&&record->parent_original==parent&&
            record->allocation_entered.load(std::memory_order_acquire)&&record->allocation_returned.load(std::memory_order_acquire)&&
            !record->allocation_exception_returned.load(std::memory_order_acquire)&&
            record->construction_entered.load(std::memory_order_acquire)&&record->construction_returned.load(std::memory_order_acquire)&&
            !record->partial_constructor_destroyed.load(std::memory_order_acquire);
    };
    const auto ended=[&](const auto& record) {
        return record&&record->payload_members_destroyed.load(std::memory_order_acquire)&&
            record->ending_thread!=std::thread::id{}&&
            (record->type==state::capture_return_record::kind::callable||record->type==state::capture_return_record::kind::packet||record->ending_thread==data->creator_thread);
    };
    const auto ready_packet=[&](const auto& packet) {
        if(!packet||packet.use_count()!=1||!packet->copy||
           !valid(packet->ending.original,state::capture_return_record::kind::packet,packet.get(),nullptr))return false;
        const auto& original=packet->ending.original;const auto& callable=packet->callable_original;
        // Exclusivity refuses aliases; only the actual callable destructor
        // tail, after payload member destruction, proves FIFO captures ended.
        return original->body_entered.load(std::memory_order_acquire)&&original->body_returned.load(std::memory_order_acquire)&&
            !original->body_reentered.load(std::memory_order_acquire)&&!ended(original)&&
            callable&&valid(callable,state::capture_return_record::kind::callable,callable->capsule_original,packet.get())&&
            ended(callable);
    };
    bool ready=true;std::size_t held=0,packets=0;
    {
        std::lock_guard lock(data->leaf);auto& first=data->receiver_input_ending;
        if(first.attempted||first.storage_attempted)return first.returned?first.result:terminal_input_retirement_result::retained_debt;
        if(data->capture_tail_generation_overflow||data->startup_active||!scope||!generation)return terminal_input_retirement_result::retained_debt;
        if(!first.observation_attempted) {
            first.observation_attempted=true;first.connection_original=&connection;first.scope_original=scope;first.generation=generation;
        } else if(first.connection_original!=&connection||first.scope_original!=scope||first.generation!=generation)
            return terminal_input_retirement_result::retained_debt;
        else if(first.observed_tail_generation==data->capture_tail_generation&&first.observed_callbacks==std::size_t(data->callbacks==0))
            return terminal_input_retirement_result::retained_debt; // No fresh actual ending, no polling retry.
        // At most eight admitted packets can remain behind the sealed stop.
        // Actual tail/callback observations have fixed private storage and a
        // eighteen-observation ceiling (two tails per packet and initial/
        // final observations). Callback quiescence is already required; no
        // intermediate callback decrement consumes this bound. RAW alias
        // return/tail precedes the unchanged post-scope gate above.
        if(first.readiness_observations>=2*state::maximum_queued+2)return terminal_input_retirement_result::retained_debt;
        ++first.readiness_observations;first.observed_tail_generation=data->capture_tail_generation;first.observed_callbacks=std::size_t(data->callbacks==0);
        if(data->callbacks||data->first_frame_count>data->first_frames.size())ready=false;
        for(std::size_t i=0;i<data->first_frames.size();++i) {
            const auto& packet=data->first_frames[i];
            if(bool(packet)!=(i<data->first_frame_count))ready=false;
            if(!packet)continue;
            ++held;++packets;
            if(!ready_packet(packet)||data->first_frame_lifecycles[i]!=data->lifecycle)ready=false;
            for(std::size_t prior=0;prior<i;++prior)if(data->first_frames[prior].get()==packet.get())ready=false;
        }
        if(data->page) {
            ++held;const auto& page=data->page;
            if(page.use_count()!=1||!valid(page->ending.original,state::capture_return_record::kind::page,page.get(),nullptr)||
               ended(page->ending.original)||page->lifecycle!=data->lifecycle||
               (page->input_ending_attempted&&!page->input_ending_returned))ready=false;
            if(page->packet) {
                ++packets;
                if(!ready_packet(page->packet)||page->packet_history!=page->packet->ending.original||
                   page->callable_history!=page->packet->callable_original)ready=false;
                for(const auto& packet:data->first_frames)if(packet.get()==page->packet.get())ready=false;
            } else {
                // The unchanged ordinary ending may already have destroyed
                // this packet. Exact retained first tails, never NULL, prove it.
                const auto& packet=page->packet_history;const auto& callable=page->callable_history;
                if(!page->input_ending_attempted||!page->input_ending_returned||!packet||!callable||
                   !valid(packet,state::capture_return_record::kind::packet,packet->capsule_original,nullptr)||!ended(packet)||
                   !packet->body_entered.load(std::memory_order_acquire)||!packet->body_returned.load(std::memory_order_acquire)||
                   packet->body_reentered.load(std::memory_order_acquire)||
                   !valid(callable,state::capture_return_record::kind::callable,callable->capsule_original,packet->capsule_original)||
                   !ended(callable))ready=false;
            }
            if(page->input_pin_issued) {
                const auto& alias=page->input_alias_original;
                if(!alias||!valid(alias,state::capture_return_record::kind::page_alias,alias->capsule_original,page.get())||!ended(alias))ready=false;
                // This proves only the real alias reference ended. The page
                // itself is still held here and needs its separate first tail.
            } else if(page->input_alias_original)ready=false;
        }
        if(data->decoded) {
            ++held;
            if(data->decoded.use_count()!=1||!valid(data->decoded->ending.original,state::capture_return_record::kind::decoded,
                data->decoded.get(),nullptr)||ended(data->decoded->ending.original))ready=false;
        }
        // Membership is an extra refusal constraint, never a lease/destructor
        // proof. A queued-behind-cleanup packet keeps this count inconsistent
        // until its genuine callable/packet tails publish and wake afterward.
        if(data->queued!=packets)ready=false;
        if(!ready){first.first_readiness_refused=true;return terminal_input_retirement_result::retained_debt;}
        first.storage_attempted=true; // The first storage allocation is never retried if unknown/failed.
    }
    try {
        auto storage=std::make_unique<state::input_capture_storage>(); // Empty, before any extraction.
        if(data->process!=creator())return terminal_input_retirement_result::retained_debt;
        {
            std::lock_guard lock(data->leaf);auto& first=data->receiver_input_ending;
            first.storage_returned=true;first.attempted=true;first.evidence_returned=true;first.evidence_known=true;
            data->terminal_input_storage=std::move(storage);first.storage_original=data->terminal_input_storage.get();
            first.held_captures=held;first.held_packets=packets;
            for(std::size_t i=0;i<data->first_frames.size();++i)if(data->first_frames[i]) {
                first.held[i]=true;first.packets[i]=data->first_frames[i]->ending.original;
                first.callables[i]=data->first_frames[i]->callable_original;
            }
            if(data->page) {
                first.held[3]=true;first.page=data->page->ending.original;first.page_alias=data->page->input_alias_original;
                first.page_packet=data->page->packet_history;first.page_callable=data->page->callable_history;
            }
            if(data->decoded){first.held[4]=true;first.decoded=data->decoded->ending.original;}
            first.extraction_attempted=true;
            data->terminal_input_storage->get<0>()=std::move(data->first_frames[0]);
            data->terminal_input_storage->get<1>()=std::move(data->first_frames[1]);
            data->terminal_input_storage->get<2>()=std::move(data->first_frames[2]);
            data->terminal_input_storage->get<3>()=std::move(data->page);
            data->terminal_input_storage->get<4>()=std::move(data->decoded);
            first.extraction_returned=true;
        }
        const auto release=[&]<std::size_t Index>() {
            bool held=false;
            {std::lock_guard lock(data->leaf);auto& first=data->receiver_input_ending;held=first.held[Index];
                if(held)first.release_attempted[Index]=true;}
            if(!held)return true; // Unissued slot DATA; never a positive destruction receipt.
            const bool returned=data->terminal_input_storage->release<Index>(); // Actual originals die OUTSIDE all leaves.
            if(data->process!=creator()||!returned)return false;
            {std::lock_guard lock(data->leaf);data->receiver_input_ending.release_returned[Index]=true;}
            return true;
        };
        // Destroy header/relation, then page optional and its actual packet,
        // then first frames. The SAME shared decoder charge stays held.
        if(!release.template operator()<4>()||!release.template operator()<3>()||
           !release.template operator()<0>()||!release.template operator()<1>()||!release.template operator()<2>())
            return terminal_input_retirement_result::retained_debt;
        if(!connection.terminal_receiver_input_history_matches(*this,scope,generation))return terminal_input_retirement_result::retained_debt;
        {
            std::lock_guard lock(data->leaf);auto& first=data->receiver_input_ending;
            const auto payload_ended=[&](const auto& record) {return record&&ended(record)&&
                record->construction_returned.load(std::memory_order_acquire)&&!record->partial_constructor_destroyed.load(std::memory_order_acquire);};
            for(std::size_t i=0;i<3;++i)if(first.held[i]&&(!first.release_attempted[i]||!first.release_returned[i]||
                !payload_ended(first.packets[i])||!payload_ended(first.callables[i])))return terminal_input_retirement_result::retained_debt;
            if(first.held[3]&&(!first.release_attempted[3]||!first.release_returned[3]||!payload_ended(first.page)||
                !payload_ended(first.page_packet)||!payload_ended(first.page_callable)||
                (first.page_alias&&!payload_ended(first.page_alias))))return terminal_input_retirement_result::retained_debt;
            if(first.held[4]&&(!first.release_attempted[4]||!first.release_returned[4]||!payload_ended(first.decoded)))
                return terminal_input_retirement_result::retained_debt;
            if(data->callbacks||data->queued)return terminal_input_retirement_result::retained_debt;
            first.result=held?terminal_input_retirement_result::captures_ended:terminal_input_retirement_result::not_held;
            first.returned=true;return first.result;
        }
    }catch(...) {
        if(data->process==creator()) {std::lock_guard lock(data->leaf);auto& first=data->receiver_input_ending;
            if(first.storage_attempted&&!first.storage_returned)first.storage_exception_returned=true;
            if(!first.first_failure)first.first_failure=std::current_exception();}
        // Actual storage remains owner-held after any unknown partial release.
        // Neither first errors nor extraction/release bits are cleared/retried.
        return terminal_input_retirement_result::retained_debt;
    }
}
bool ordinary_group_target_receiving_owner::terminal_inputs_ended(const ordinary_target_staging_connection& connection)const noexcept {
    const auto data=state_;if(!data||data->process!=creator())return false;
    const void* scope=nullptr;std::uint64_t generation=0;
    {
        std::lock_guard lock(data->leaf);const auto& first=data->receiver_input_ending;
        if(!data->worker_entered||data->creator_thread!=std::this_thread::get_id()||!data->stop||!data->cleanup_entered||
           data->connection.get()!=&connection||!first.attempted||!first.evidence_returned||!first.evidence_known||
           !first.storage_attempted||!first.storage_returned||first.storage_exception_returned||!first.extraction_attempted||
           !first.extraction_returned||!first.returned||first.result!=terminal_input_retirement_result::captures_ended||
           !first.held_captures||first.first_failure||first.connection_original!=&connection||
           !data->terminal_input_storage||data->terminal_input_storage.get()!=first.storage_original)return false;
        scope=first.scope_original;generation=first.generation;
    }
    // A distinct historical association, not the pre-input Native/payload gate.
    if(!connection.terminal_receiver_input_history_matches(*this,scope,generation))return false;
    const auto known=[&](const auto& original,state::capture_return_record::kind type,const void* parent) {
        return original&&original->process==data->process&&original->owner_original==data.get()&&
            original->service_slot==data->service_slot&&original->service_serial==data->service_serial&&
            original->lifecycle==data->lifecycle&&original->deadline==data->end&&original->type==type&&
            original->capsule_original&&original->parent_original==parent&&
            original->allocation_entered.load(std::memory_order_acquire)&&original->allocation_returned.load(std::memory_order_acquire)&&
            !original->allocation_exception_returned.load(std::memory_order_acquire)&&
            original->construction_entered.load(std::memory_order_acquire)&&original->construction_returned.load(std::memory_order_acquire)&&
            !original->partial_constructor_destroyed.load(std::memory_order_acquire)&&
            original->payload_members_destroyed.load(std::memory_order_acquire)&&original->ending_thread!=std::thread::id{}&&
            (original->type==state::capture_return_record::kind::callable||original->type==state::capture_return_record::kind::packet||original->ending_thread==data->creator_thread);
    };
    std::lock_guard lock(data->leaf);const auto& first=data->receiver_input_ending;
    if(data->callbacks||data->queued||data->page||data->decoded)return false; // Supplementary association, not destruction proof.
    for(std::size_t i=0;i<3;++i) {
        if(data->first_frames[i])return false;
        if(first.held[i]&&(!first.release_attempted[i]||!first.release_returned[i]||
            !known(first.packets[i],state::capture_return_record::kind::packet,nullptr)||
            !known(first.callables[i],state::capture_return_record::kind::callable,first.packets[i]->capsule_original)||
            !first.packets[i]->body_entered.load(std::memory_order_acquire)||!first.packets[i]->body_returned.load(std::memory_order_acquire)||
            first.packets[i]->body_reentered.load(std::memory_order_acquire)))return false;
    }
    if(first.held[3]&&(!first.release_attempted[3]||!first.release_returned[3]||
        !known(first.page,state::capture_return_record::kind::page,nullptr)||
        !known(first.page_packet,state::capture_return_record::kind::packet,nullptr)||
        !known(first.page_callable,state::capture_return_record::kind::callable,first.page_packet->capsule_original)||
        !first.page_packet->body_entered.load(std::memory_order_acquire)||!first.page_packet->body_returned.load(std::memory_order_acquire)||
        first.page_packet->body_reentered.load(std::memory_order_acquire)||
        (first.page_alias&&!known(first.page_alias,state::capture_return_record::kind::page_alias,first.page->capsule_original))))return false;
    if(first.held[4]&&(!first.release_attempted[4]||!first.release_returned[4]||
        !known(first.decoded,state::capture_return_record::kind::decoded,nullptr)))return false;
    return true; // Selected receiver captures only; no Native/charge/shell/owner/registry join.
}
bool ordinary_group_target_receiving_owner::terminal_decoder_charge_identity_matches(
    const ordinary_target_staging_connection& connection,const void* charge_original)const noexcept {
    const auto data=state_;if(!data||data->process!=creator()||!charge_original)return false;
    // The live decoder accessor requires the now-ended header. This distinct
    // private history observer issues only SAME retained reservation identity.
    if(!terminal_inputs_ended(connection))return false;
    std::lock_guard lock(data->leaf);
    return data->worker_entered&&data->creator_thread==std::this_thread::get_id()&&data->stop&&data->cleanup_entered&&
        data->connection.get()==&connection&&data->history_charge&&data->history_charge.get()==charge_original;
}
void ordinary_group_target_receiving_owner::retire_on_worker()noexcept {
    const auto data=state_;if(!data||data->process!=creator())return;
    {std::lock_guard lock(data->leaf);if(data->cleanup_entered)return;
        if(data->worker_entered&&data->creator_thread!=std::this_thread::get_id())return;
        if(!data->worker_entered){data->worker_entered=true;data->creator_thread=std::this_thread::get_id();}
        data->cleanup_entered=true;}
    // Cleanup is creator/exact original only. An expired admission is never
    // renewed; no new target, journal, SQL work, write or model grant is admitted.
    // Pending general disposition preserves actual ingress/decoded inputs
    // and their lease. Their independent known destruction remains required.
    bool joined=false;
    if(data->connection) {
        using disposition=ordinary_target_staging_connection::abandoned_retirement_result;
        // Preserve constructor-only disposition. Its not_applicable result
        // elects the actual owner stop before first close; pending general
        // debt cannot fall through to ordinary retirement or successful collection.
        switch(
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
            ordinary_receiver_reclamation_fixture::observe_disposition(*this,data->connection->retire_abandoned_receiving_shell())
#else
            data->connection->retire_abandoned_receiving_shell()
#endif
        ) {
            case disposition::not_applicable:
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
                ordinary_receiver_reclamation_fixture::observe_ordinary_fallback(*this);
#endif
                joined=data->connection->retire_terminal_receiving_shell(*this)==disposition::joined;break;
            case disposition::joined:
                joined=true;break;
            case disposition::retained_debt:
                break; // SAME original remains held; sqlite/files fact stays false.
        }
    }
    if(data->process!=creator())return;
    if(joined) {
        // Only the unchanged constructor-only successful ending reaches here.
        for(auto& message:data->first_frames)message.reset();
        data->page.reset();data->decoded.reset();data->history_charge.reset();
    }
    {std::lock_guard lock(data->leaf);data->sqlite_and_files_joined=joined;}
    if(!joined&&data->connection) {
        // Actual post-scope receiver captures only. This separate first result
        // never advances the old target/native/collection/worker join bits.
        (void)retire_terminal_inputs(*data->connection);
    }
    if(data->registry)data->registry->request_retirement(data->receipt);
    if(data->process!=creator())return; // Actual foreign retirement request may cross process.
    if(data->native_lane_bound)data->native_lane->retire();
    else if(!data->factory_entered) {
        // Actual factory boundary was NEVER entered after this worker's FIFO
        // target turn ended. Cancel the real empty reservation, not a fake close.
        data->native_lane.reset();data->receipt.complete_adapter_cleanup(0);
        {std::lock_guard lock(data->leaf);data->native_lane_returned=true;data->lane_result.unstarted_reservation_canceled=true;}
    }
    schedule_collection();
}
void ordinary_group_target_receiving_owner::schedule_collection()noexcept {
    const auto data=state_;if(!data||data->process!=creator())return;
    std::function<void()> work;std::shared_ptr<std_thread_scheduler> worker;
    try {{std::lock_guard lock(data->leaf);
        if(!data->worker||!data->stop||!data->cleanup_entered||data->collected)return;
        if(data->collector_active) {
            // Actual join/seal/complete producers synchronously wake on this
            // SAME worker even on no-op/refusal. They cannot enqueue themselves.
            // A genuine other-thread ending during the turn remains pending.
            if(std::this_thread::get_id()!=data->creator_thread)data->collector_external_wake=true;
            return;
        }
        if(data->collector_queued)return;
        data->collector_queued=true;work=data->collection_work;worker=data->worker;}
        worker->invoke(std::move(work));
    }catch(...) {remember_failure(std::current_exception());}
}
void ordinary_group_target_receiving_owner::collect_on_worker()noexcept {
    const auto data=state_;if(!data||data->process!=creator())return;
    sync_retirement_result lane_result;
    {std::lock_guard lock(data->leaf);data->collector_queued=false;
        if(data->collected||data->collector_active||!data->worker_entered||data->creator_thread!=std::this_thread::get_id())return;
        data->collector_active=true;
        lane_result=data->lane_result;}
    struct actual_turn_ending {
        ordinary_group_target_receiving_owner* owner;std::shared_ptr<state> data;
        ~actual_turn_ending() {
            if(data->process!=creator())return;
            bool external=false;
            {std::lock_guard lock(data->leaf);data->collector_active=false;external=std::exchange(data->collector_external_wake,false);}
            if(external)owner->schedule_collection();
        }
    } turn{this,data}; // Actual function custody, never native/current permission.
    if(data->connection) {
        // Real later destructor/callback wakes may reobserve readiness before
        // the first extraction. Once storage/extraction starts it never retries.
        (void)retire_terminal_inputs(*data->connection);
        if(terminal_inputs_ended(*data->connection)) {
            (void)data->connection->retire_terminal_native_decoder_payloads(*this);
            (void)data->connection->retire_terminal_native_phases(*this);
        }
    }
    {std::lock_guard lock(data->leaf);
        if(data->startup_active||!data->cleanup_entered||!data->native_lane_returned||data->callbacks||data->queued||
            (data->target_preparation_entered&&!data->sqlite_and_files_joined)||data->worker_join_failure||data->worker_join_active||
            (data->first_worker_join_entered&&!data->first_worker_join_returned))return;}
    auto counts=data->custody->snapshot();
    if(counts.commands||counts.payloads||counts.first_error)return;
    if(counts.finished_workers) {
        // Never call the unconditional-wake join producer on a no-op snapshot.
        // Every later join requires new actual finished handles; failed/unknown
        // first joins remain held rather than automatically reissuing them.
        {std::lock_guard lock(data->leaf);
            if(data->worker_join_calls>=configured_attempt_custody::maximum_payloads)return;
            ++data->worker_join_calls;data->worker_join_active=true;
            if(!data->first_worker_join_entered){data->first_worker_join_input=counts;data->first_worker_join_entered=true;}}
        const bool result=data->custody->join_finished_workers();
        if(data->process!=creator())return;
        {std::lock_guard lock(data->leaf);
            if(!data->first_worker_join_returned){data->first_worker_join_result=result;data->first_worker_join_returned=true;}
            if(!result)data->worker_join_failure=true;data->worker_join_active=false;}
        const auto returned=data->custody->snapshot(); // Actual post-join counts.
        {std::lock_guard lock(data->leaf);if(data->worker_join_calls==1)data->first_worker_join_output=returned;}
        if(!result)return;counts=returned;
    }
    if(counts.commands||counts.payloads||counts.workers||counts.first_error)return;
    if(lane_result.first_error||(!lane_result.disconnect_returned&&!lane_result.unstarted_reservation_canceled))return;
    // Null sync_callback_lifetime in the transport lane supplies no callback
    // proof: actual own ingress count, FIFO drain and custody joins above do.
    if(!data->native_completion_entered) {
        data->native_completion_entered=true;
        const bool result=data->registry->complete_native_cleanup(data->receipt,0);
        data->native_completion_returned=result;
    }
    if(!data->native_completion_returned)return;
    const auto status=data->registry->snapshot(data->receipt);
    if(!status.adapter_complete||status.first_error)return;
    data->collection_entered=true;const bool collected=data->registry->collect_completed(data->receipt);
    if(data->process!=creator())return;
    if(!data->collection_returned){data->first_collection_result=collected;data->collection_returned=true;}
    if(collected) {
        data->transport.reset();if(data->process!=creator())return;
        data->typed_transport=nullptr;data->factory.reset();if(data->process!=creator())return;
        data->token.clear();
    }
    {std::lock_guard lock(data->leaf);data->collected=collected;}
    data->changed.notify_all();
}
bool ordinary_group_target_receiving_owner::cleanup_complete()const noexcept {
    const auto data=state_;if(!data||data->process!=creator())return false;
    std::lock_guard lock(data->leaf);return data->collected&&(data->worker_joined||data->never_started_ending)&&
        (!data->target_preparation_entered||data->sqlite_and_files_joined);
}
lattice_close_result ordinary_group_target_receiving_owner::close(clock::time_point until)noexcept {
    lattice_close_result result;result.cleanup_complete=false;result.sync=sync_drain_state::deadline_pending;
    const auto data=state_;if(!data||data->process!=creator())return result;
    request_close();
    try {
        {std::unique_lock lock(data->leaf);
            if(data->worker_entered&&data->creator_thread==std::this_thread::get_id()) {result.sync=sync_drain_state::reentrant_pending;result.error=data->first_failure;return result;}
            if(!data->changed.wait_until(lock,until,[&]{return data->collected;})) {result.error=data->first_failure;return result;}
            result.error=data->first_failure;}
        // Only this explicit off-worker close joins the actual native worker.
        // stop/deinit, deadlinePending or disconnect is not a scheduler join.
        std::shared_ptr<std_thread_scheduler> worker;
        {std::lock_guard lock(data->leaf);worker=data->worker;}
        if(worker)worker->shutdown();
        if(data->process!=creator())return result;
        {std::lock_guard lock(data->leaf);if(worker)data->worker_joined=true;result.error=data->first_failure;}
        release_service_custody();
        result.cleanup_complete=cleanup_complete();result.sync=result.cleanup_complete?sync_drain_state::retired:sync_drain_state::failed;
    }catch(...) {result.remember(std::current_exception());remember_failure(std::current_exception());}
    return result;
}
