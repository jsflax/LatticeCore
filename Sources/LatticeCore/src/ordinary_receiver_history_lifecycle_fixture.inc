// Included only in the separate debug history fixture, after the actual
// receiving owner/request state. This publishes DATA, never a native grant.
namespace {
std::uint64_t history_fixture_flags(std::initializer_list<bool> facts)noexcept {
    std::uint64_t out=0,bit=1;
    for(const bool fact:facts){if(fact)out|=bit;bit<<=1;}
    return out;
}
nlohmann::json history_fixture_root(const ordinary_target_lifetime_observation::root_fact& value) {
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_EXTRA_ALIAS)
    // Case-only short keys are lossless; the paired Swift CodingKeys keep
    // all seven mandatory UInt64 scalars and every original predicate.
    return nlohmann::json{{"f",history_fixture_flags({
        value.materialized,value.source_materialized,value.constructor_observed,value.source_constructor_observed,
        value.constructor_returned,value.source_constructor_returned,value.owner_count_observed,
        value.source_failure_observed,value.source_failure,value.outstanding_native_call,
        value.binding_attempted,value.binding_returned,value.extraction_attempted,value.extraction_returned,
        value.source_reset_attempted,value.source_reset_returned,value.release_attempted,value.release_returned,
        value.members_destroyed,value.partial_constructor_destroyed,value.native_returns_observed,value.native_returns_known,
        value.scope_history_observed,value.scope_published,value.scope_left,value.scope_failed,
        value.scope_leave_returned,value.scope_leave_joined,value.scope_detached})},
        {"o",value.original},{"s",value.source_original},{"p",value.previous_original},
        {"i",value.page_index},{"c",value.owner_count},{"n",value.native_call_count}};
#else
    // Bit positions are the exact ordered POD facts, not inferred absence.
    return nlohmann::json{{"flags",history_fixture_flags({
        value.materialized,value.source_materialized,value.constructor_observed,value.source_constructor_observed,
        value.constructor_returned,value.source_constructor_returned,value.owner_count_observed,
        value.source_failure_observed,value.source_failure,value.outstanding_native_call,
        value.binding_attempted,value.binding_returned,value.extraction_attempted,value.extraction_returned,
        value.source_reset_attempted,value.source_reset_returned,value.release_attempted,value.release_returned,
        value.members_destroyed,value.partial_constructor_destroyed,value.native_returns_observed,value.native_returns_known,
        value.scope_history_observed,value.scope_published,value.scope_left,value.scope_failed,
        value.scope_leave_returned,value.scope_leave_joined,value.scope_detached})},
        {"original",value.original},{"sourceOriginal",value.source_original},{"previousOriginal",value.previous_original},
        {"pageIndex",value.page_index},{"ownerCount",value.owner_count},{"nativeCallCount",value.native_call_count}};
#endif
}
nlohmann::json history_fixture_family(const ordinary_target_lifetime_observation::family_fact& value) {
    return nlohmann::json{{"flags",history_fixture_flags({value.attempted,value.allocation_returned,
        value.allocation_exception_returned,value.first_failure,value.preparation_returned,
        value.extraction_attempted,value.extraction_returned,value.returned,value.history_checked,value.history_known})},
        {"result",value.result},{"roots",nlohmann::json::array({history_fixture_root(value.roots[0]),history_fixture_root(value.roots[1])})}};
}
nlohmann::json history_fixture_raw(const ordinary_target_raw_object_observation& value) {
    auto objects=nlohmann::json::array();
    for(const auto& object:value.objects)objects.push_back({{"flags",object.flags},{"original",object.original},
        {"stateOriginal",object.state_original},{"ownerCount",object.owner_count},{"stateOwnerCount",object.state_owner_count},
        {"kind",object.kind},{"aliasOf",object.alias_of}});
    return nlohmann::json{{"version",1},{"flags",history_fixture_flags({value.creator_worker,value.receiving_owner_bound,
        value.attempted,value.allocation_returned,value.allocation_exception_returned,value.first_failure,
        value.preparation_returned,value.returned,value.history_checked,value.history_known,
        value.not_issued_observed,value.not_issued,value.physical_close_observed})},
        {"result",value.result},{"physicalCloseResult",value.physical_close_result},
        {"physicalCloseError",value.physical_close_error},{"objects",std::move(objects)}};
}
nlohmann::json history_fixture_programmes(const ordinary_target_programme_observation& value) {
    auto objects=nlohmann::json::array();
    for(const auto& object:value.objects)objects.push_back({{"flags",object.flags},{"original",object.original},
        {"projectionOriginal",object.projection_original},{"ownerCount",object.owner_count},{"bytes",object.bytes},
        {"items",object.items},{"cells",object.cells},{"bindings",object.bindings}});
    return nlohmann::json{{"version",1},{"flags",history_fixture_flags({value.creator_worker,value.receiving_owner_bound,
        value.attempted,value.allocation_returned,value.allocation_exception_returned,value.first_failure,
        value.preparation_returned,value.returned,value.history_checked,value.history_known,
        value.source_failure_observed,value.source_failure,value.projection_payloads_observed})},
        {"schemaOriginal",value.schema_original},{"result",value.result},{"objects",std::move(objects)},
        {"projectionPayloads",value.projection_payloads},{"projectionPayloadOwnerCounts",value.projection_payload_owner_counts}};
}
nlohmann::json history_fixture_schema_roots(const ordinary_target_schema_root_observation& value) {
    auto roots=nlohmann::json::array();
    for(const auto& root:value.roots)roots.push_back({{"flags",root.flags},{"original",root.original},{"ownerCount",root.owner_count}});
    return nlohmann::json{{"version",1},{"flags",history_fixture_flags({value.creator_worker,value.receiving_owner_bound,value.originals_observed,
        value.attempted,value.allocation_returned,value.allocation_exception_returned,value.first_failure,value.preparation_returned,
        value.reset_attempted,value.reset_returned,value.ending_observation_attempted,value.ending_observation_returned,
        value.ending_known,value.returned,value.history_checked,value.history_known,value.source_failure_observed,value.source_failure})},
        {"result",value.result},{"roots",std::move(roots)},{"sources",value.sources},
        {"receivingStateOriginal",value.receiving_state_original},{"requestOriginal",value.request_original},
        {"requestStateOriginal",value.request_state_original},{"connectionOriginal",value.connection_original}};
}
nlohmann::json history_fixture_scope(const ordinary_target_scope_root_observation& value) {
    auto roots=nlohmann::json::array();
    for(const auto& root:value.roots)roots.push_back(nlohmann::json::array({root.original,root.flags,root.aliases}));
    // Compact ordered DATA keeps the same 16KiB envelope; no old field changes.
    return nlohmann::json::array({value.flags,value.peak_bytes,value.scope_count,value.ledger_count,value.resource_count,
        value.environment_count,std::move(roots),value.sources,value.ledger_sources});
}
nlohmann::json history_fixture_snapshot(const ordinary_target_lifetime_observation& value,
    const ordinary_target_raw_object_observation& raw,const ordinary_target_programme_observation& programmes,
    const ordinary_target_schema_root_observation& schema,const ordinary_target_scope_root_observation& scope) {
    return nlohmann::json{{"flags",history_fixture_flags({value.creator_worker,value.receiving_owner_bound,
        value.inputs_ended,value.retirement_requested,value.state_first_failure,value.history_page_active,value.unexpected_predecessor,
        value.state_first_failure_observed,value.state_first_failure_is_page2_injection})},
        {"historyPagesIssued",value.history_pages_issued},{"families",{
            {"decoder",history_fixture_family(value.decoder)},{"phases",history_fixture_family(value.phases)},
            {"outer",history_fixture_family(value.outer)},{"pages",history_fixture_family(value.pages)}}},
        {"staticNative",nlohmann::json::array({history_fixture_family(value.static_native[0]),history_fixture_family(value.static_native[1]),
            history_fixture_family(value.static_native[2]),history_fixture_family(value.static_native[3])})},
        {"staticProgrammeFailureObserved",value.static_programme_failure_observed},
        {"staticProgrammeFailure",value.static_programme_failure},{"rawShared",history_fixture_raw(raw)},
        {"programmeShared",history_fixture_programmes(programmes)},{"schemaShared",history_fixture_schema_roots(schema)},{"scopeShared",history_fixture_scope(scope)}};
}
bool history_fixture_constructed(const ordinary_target_lifetime_observation::root_fact& value)noexcept {
    return value.materialized&&value.original&&value.constructor_observed&&value.constructor_returned&&
        !value.partial_constructor_destroyed;
}
bool history_fixture_raw_constructed(const ordinary_target_raw_object_observation::object_fact& value)noexcept {
    constexpr auto required=(std::uint64_t{1}<<0)|(std::uint64_t{1}<<1)|(std::uint64_t{1}<<2)|
        (std::uint64_t{1}<<3)|(std::uint64_t{1}<<4)|(std::uint64_t{1}<<5)|(std::uint64_t{1}<<6);
    constexpr auto refused=(std::uint64_t{1}<<7)|(std::uint64_t{1}<<18)|(std::uint64_t{1}<<19);
    return value.original&&value.state_original&&(value.flags&required)==required&&!(value.flags&refused)&&value.state_owner_count==1;
}
bool history_fixture_raw_alias(const ordinary_target_raw_object_observation& value,unsigned kind)noexcept {
    for(const auto& object:value.objects)if(object.kind==kind&&history_fixture_raw_constructed(object)) {
        std::uint64_t internal=0;
        for(const auto& other:value.objects)if(other.original==object.original)++internal;
        // A duplicated internal slot alone is not evidence of an outside
        // genuine Native user. These are observed counts, never a new alias.
        if(object.owner_count>internal)return true;
    }
    return false;
}
bool history_fixture_programme_constructed(const ordinary_target_programme_observation::object_fact& value)noexcept {
    constexpr auto required=(std::uint64_t{1}<<0)|(std::uint64_t{1}<<1)|(std::uint64_t{1}<<2)|
        (std::uint64_t{1}<<3)|(std::uint64_t{1}<<4)|(std::uint64_t{1}<<5)|(std::uint64_t{1}<<6)|
        (std::uint64_t{1}<<18)|(std::uint64_t{1}<<19);
    constexpr auto refused=(std::uint64_t{1}<<7)|(std::uint64_t{1}<<15);
    return value.original&&(value.flags&required)==required&&!(value.flags&refused);
}
bool history_fixture_programme_goal(const ordinary_target_programme_observation& value)noexcept {
    if(!value.creator_worker||!value.receiving_owner_bound||!value.schema_original||value.attempted||value.first_failure||
       !value.source_failure_observed||value.source_failure||!history_fixture_programme_constructed(value.objects[0])||
       value.objects[0].owner_count<=1)return false;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_PAIR_FAILURE)
    for(std::size_t at=1;at<4;++at) {
        const auto& object=value.objects[at];
        if(object.flags!=1||object.original||object.projection_original||object.owner_count||object.bytes||object.items||object.cells||object.bindings)return false;
    }
    return !value.projection_payloads_observed;
#else
    for(std::size_t at=1;at<4;++at)if(!history_fixture_programme_constructed(value.objects[at])||
        value.objects[at].owner_count<=(at==1?3u:1u))return false;
    if(!value.projection_payloads_observed)return false;
    for(std::size_t at=0;at<4;++at)if(!value.projection_payloads[at]||value.projection_payload_owner_counts[at]!=1)return false;
    return value.objects[2].projection_original==value.objects[1].original&&value.objects[3].projection_original==value.objects[1].original;
#endif
}
bool history_fixture_schema_goal(const ordinary_target_schema_root_observation& value)noexcept {
    if(!value.creator_worker||!value.receiving_owner_bound||!value.originals_observed||value.attempted||value.first_failure||
       !value.source_failure_observed||value.source_failure||!value.receiving_state_original||!value.request_original||
       !value.request_state_original||!value.connection_original)return false;
    for(std::size_t at=0;at<2;++at)if(value.roots[at].flags!=15||!value.roots[at].original||
        value.roots[at].owner_count!=(at==0?2u:1u)||value.sources[at])return false;
    return value.roots[0].original!=value.roots[1].original;
}
bool history_fixture_scope_goal(const ordinary_target_scope_root_observation& value)noexcept {
    if(value.flags!=3||value.scope_count<2||value.scope_count>4||value.ledger_count<2||value.ledger_count>3)return false;
    for(std::size_t at=0;at<2;++at)if(!value.roots[at].original||value.roots[at].flags!=15||value.roots[at].aliases!=1)return false;
    if(value.roots[2].original?(value.roots[2].flags!=15||value.roots[2].aliases!=1):(value.roots[2].flags||value.roots[2].aliases))return false;
    for(std::size_t at=0;at<4;++at) {
        const auto& root=value.roots[3+at];
        if(at<value.scope_count?(!root.original||root.flags!=15||!root.aliases):(root.original||root.flags||root.aliases))return false;
    }
    for(std::size_t at=0;at<3;++at) {
        const auto& root=value.roots[7+at];
        if(at<value.ledger_count?(!root.original||root.flags!=15||!root.aliases):(root.original||root.flags||root.aliases))return false;
    }
    return true; // Only observed genuine pre-attempt constructors/counts, no ending grant.
}
bool history_fixture_goal(const ordinary_target_lifetime_observation& value,
    const ordinary_target_raw_object_observation& raw,const ordinary_target_programme_observation& programmes,
    const ordinary_target_schema_root_observation& schema,const ordinary_target_scope_root_observation& scope)noexcept {
    if(!value.creator_worker||!value.receiving_owner_bound||value.unexpected_predecessor||
       !raw.creator_worker||!raw.receiving_owner_bound||raw.attempted||raw.first_failure||
       !history_fixture_raw_constructed(raw.objects[0])||!history_fixture_raw_alias(raw,1)||
       !history_fixture_programme_goal(programmes)||!history_fixture_schema_goal(schema)||!history_fixture_scope_goal(scope))return false;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_PAIR_FAILURE)
    const auto& now=value.pages.roots[0];const auto& prior=value.pages.roots[1];
    return history_fixture_constructed(now)&&history_fixture_constructed(prior)&&now.original!=prior.original&&
        now.previous_original==prior.original&&history_fixture_constructed(value.static_native[0].roots[0])&&
        history_fixture_constructed(value.static_native[1].roots[0]);
#else
    for(const auto& family:value.static_native)if(!history_fixture_constructed(family.roots[0]))return false;
    return history_fixture_raw_alias(raw,2)&&history_fixture_constructed(value.pages.roots[0])&&
        history_fixture_constructed(value.phases.roots[0])&&history_fixture_constructed(value.phases.roots[1])&&
        history_fixture_constructed(value.outer.roots[0])&&history_fixture_constructed(value.outer.roots[1]);
#endif
}
}
bool ordinary_receiver_reclamation_fixture::history_page2_failure_requested(
    const ordinary_group_target_receiving_owner& owner)noexcept {
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_PAIR_FAILURE)
    try {owner.require_worker_current();return true;}catch(...){return false;}
#else
    (void)owner;return false;
#endif
}
void ordinary_receiver_reclamation_fixture::observe_history_worker(
    ordinary_group_target_receiving_owner& owner)noexcept {
    try {
        const auto data=owner.state_;
        if(!data||data->process!=creator())return; // Before any inherited leaf.
        bool stopped=false,failed=false;
        {std::lock_guard lock(data->leaf);
            if(!data->worker_entered||data->creator_thread!=std::this_thread::get_id())return;
            stopped=data->stop;failed=bool(data->first_failure);}
        // The last full family snapshot was published before first payload
        // extraction. Never replace it with zeros or call a dead/live fallback.
        if(!data->connection||data->connection->terminal_receiving_payloads_selected()) {
            auto& held=original();if(held.process!=creator())return;
            {std::lock_guard lock(held.leaf);
                if(held.history_owner_original!=&owner||held.history_state_original!=data.get())return;
                held.history_stopped=held.history_stopped||stopped;
                held.history_owner_failure=held.history_owner_failure||failed;}
            held.history_changed.notify_all();return;
        }
        // No Native/owner leaf is held while complete private records are read.
        const auto sample=data->connection?data->connection->qualification_lifetime_observation(owner):
            ordinary_target_lifetime_observation{};
        const auto raw=data->connection?data->connection->qualification_raw_object_observation(owner):
            ordinary_target_raw_object_observation{};
        const auto programmes=data->connection?data->connection->qualification_programme_observation(owner):
            ordinary_target_programme_observation{};
        const auto schema=data->connection?data->connection->qualification_schema_root_observation(owner):
            ordinary_target_schema_root_observation{};
        const auto scope=data->connection?data->connection->qualification_scope_root_observation(owner):ordinary_target_scope_root_observation{};
        auto& held=original();if(held.process!=creator())return;
        {std::lock_guard lock(held.leaf);
            if((held.history_owner_original&&held.history_owner_original!=&owner)||
               (held.history_state_original&&held.history_state_original!=data.get())||
               (held.capture_returned&&held.owner.get()!=&owner))return;
            // A fast real worker may finish before start() returns to capture.
            // Record only its exact addresses; close still requires the later
            // actual captured request/owner to match them. No owner is minted.
            if(!held.history_owner_original){held.history_owner_original=&owner;held.history_state_original=data.get();}
            held.history_latest=sample;held.history_raw_latest=raw;held.history_programme_latest=programmes;
            held.history_schema_latest=schema;held.history_scope_latest=scope;held.history_observed=true;
            held.history_stopped=held.history_stopped||stopped;
            held.history_owner_failure=held.history_owner_failure||failed;
            if(!held.history_ready&&history_fixture_goal(sample,raw,programmes,schema,scope)) {
                held.history_ready_original=sample;held.history_raw_ready_original=raw;
                held.history_programme_ready_original=programmes;held.history_schema_ready_original=schema;held.history_scope_ready_original=scope;held.history_ready=true;
            }}
        held.history_changed.notify_all();
    }catch(...) {
        try {auto& held=original();if(held.process!=creator())return;
            std::lock_guard lock(held.leaf);if(!held.history_bridge_first_failure)held.history_bridge_first_failure=std::current_exception();
            held.history_changed.notify_all();}catch(...){}
    }
}
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_EXTRA_ALIAS)
void ordinary_receiver_reclamation_fixture::before_terminal_collection(ordinary_group_target_receiving_owner& owner)noexcept {
    try {
        const auto data=owner.state_;if(!data||data->process!=creator())return;
        const auto request=data->request.lock();
        if(!request||!request->state_||request->state_->process!=creator()||
           !request->state_->cancelled.load(std::memory_order_acquire)||std::atomic_load(&request->state_->group_receiver).get()!=&owner)return;
        {std::lock_guard lock(data->leaf);
            if(!data->stop||!data->cleanup_entered||!data->collector_active||!data->worker_entered||
               data->creator_thread!=std::this_thread::get_id()||data->terminal_collection_selected||clock::now()>=data->end)return;}
        auto& held=original();if(held.process!=creator())return;
        {std::lock_guard lock(held.leaf);
            if(held.history_alias_armed||(held.capture_returned&&(held.owner.get()!=&owner||held.request!=request))||
               held.history_owner_original!=&owner||held.history_state_original!=data.get()||!data->connection)return;
            held.history_extra_alias=data->connection; // One REAL extra control-block owner.
            held.history_alias_original=held.history_extra_alias.get();
            held.history_alias_control_known=held.history_extra_alias.get()==data->connection.get()&&
                !held.history_extra_alias.owner_before(data->connection)&&!data->connection.owner_before(held.history_extra_alias);
            held.history_alias_armed=true;}
        const auto before=ordinary_target_staging_connection::qualification_terminal_collection_preflight(owner,
            data->connection,request->state_->staging_connection,data->io,request->state_->staging_io,
            data->target,request->state_->unpublished_target,data->schema_roots_original,data->history_charge,
            data->terminal_collection_selected,data->terminal_collection_original,data->terminal_collection_first_failure);
        if(data->process!=creator())return;
        {std::lock_guard lock(held.leaf);held.history_alias_before=before;}
    }catch(...) {try {auto& held=original();if(held.process!=creator())return;std::lock_guard lock(held.leaf);
        if(!held.history_bridge_first_failure)held.history_bridge_first_failure=std::current_exception();}catch(...) {}}
}
void ordinary_receiver_reclamation_fixture::terminal_collection_returned(ordinary_group_target_receiving_owner& owner,bool ended)noexcept {
    try {
        const auto data=owner.state_;if(!data||data->process!=creator())return;
        {std::lock_guard lock(data->leaf);if(!data->worker_entered||data->creator_thread!=std::this_thread::get_id())return;}
        auto& held=original();if(held.process!=creator())return;
        {std::lock_guard lock(held.leaf);if(!held.history_alias_armed||held.history_alias_call_returned||
            held.history_owner_original!=&owner||held.history_state_original!=data.get())return;}
        const auto request=data->request.lock();if(!request||!request->state_||request->state_->process!=creator())return;
        const auto after=ordinary_target_staging_connection::qualification_terminal_collection_preflight(owner,
            data->connection,request->state_->staging_connection,data->io,request->state_->staging_io,
            data->target,request->state_->unpublished_target,data->schema_roots_original,data->history_charge,
            data->terminal_collection_selected,data->terminal_collection_original,data->terminal_collection_first_failure);
        if(data->process!=creator())return;
        {std::lock_guard lock(held.leaf);held.history_alias_after=after;held.history_alias_call_ended=ended;held.history_alias_call_returned=true;}
    }catch(...) {try {auto& held=original();if(held.process!=creator())return;std::lock_guard lock(held.leaf);
        if(!held.history_bridge_first_failure)held.history_bridge_first_failure=std::current_exception();}catch(...) {}}
}
void ordinary_receiver_reclamation_fixture::history_collection_turn_returned(ordinary_group_target_receiving_owner& owner)noexcept {
    try {
        const auto data=owner.state_;if(!data||data->process!=creator())return;
        const auto request=data->request.lock();if(!request||!request->state_||request->state_->process!=creator())return;
        std::array<std::uint64_t,6> ending{};
        {std::lock_guard lock(data->leaf);
            if(data->collector_active||!data->stop||!data->worker_entered||data->creator_thread!=std::this_thread::get_id())return;
            ending={data->terminal_collection_selected,data->terminal_graph_joined,data->collected,data->service_released,
                bool(data->history_charge),bool(request->state_->staging_connection)};}
        auto& held=original();if(held.process!=creator())return;
        {std::lock_guard lock(held.leaf);
            if(!held.history_alias_armed||!held.history_alias_call_returned||held.history_alias_turn_returned||
               held.history_owner_original!=&owner||held.history_state_original!=data.get())return;
            held.history_alias_turn=ending;held.history_alias_turn_returned=true;}
        held.history_changed.notify_all();
    }catch(...) {try {auto& held=original();if(held.process!=creator())return;std::lock_guard lock(held.leaf);
        if(!held.history_bridge_first_failure)held.history_bridge_first_failure=std::current_exception();held.history_changed.notify_all();}catch(...) {}}
}
#endif
std::size_t ordinary_receiver_reclamation_fixture::close_history_json(void* output,std::size_t capacity)noexcept {
    try {
        if(!output||capacity<16384)return 0;
        auto& held=original();if(held.process!=creator())return 0;
        std::shared_ptr<ordinary_group_target_receiving_owner> owner;
        std::shared_ptr<const ordinary_receiver_wss_request> request;
        {std::lock_guard lock(held.leaf);
            if(held.history_close_entered||!held.capture_returned||!held.owner||!held.request)return 0;
            owner=held.owner;request=held.request;held.history_close_entered=true;}
        const auto data=owner->state_;
        if(!data||data->process!=creator()||!request->state_||request->state_->process!=creator()||
           data->request.lock()!=request||std::atomic_load(&request->state_->group_receiver)!=owner)return 0;
        {std::lock_guard lock(data->leaf);
            if(data->worker_entered&&data->creator_thread==std::this_thread::get_id())return 0;}
        // Reserve only the existing two-second close interval from this same
        // original admission; neither phase nor native deadline is restarted.
        const auto reserve=std::chrono::seconds(2);
        const auto wait_end=data->end-reserve;
        ordinary_target_lifetime_observation before,after;
        ordinary_target_raw_object_observation raw_before,raw_after;
        ordinary_target_programme_observation programme_before,programme_after;
        ordinary_target_schema_root_observation schema_before,schema_after;
        ordinary_target_scope_root_observation scope_before,scope_after;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_EXTRA_ALIAS)
        nlohmann::json extra_alias;
#endif
        {std::unique_lock lock(held.leaf);
            held.history_changed.wait_until(lock,wait_end,[&]{return held.history_ready||held.history_stopped||
                held.history_owner_failure||held.history_bridge_first_failure;});
            if(held.history_owner_original==owner.get()&&held.history_state_original==data.get()) {
                before=held.history_ready?held.history_ready_original:held.history_latest;
                raw_before=held.history_ready?held.history_raw_ready_original:held.history_raw_latest;
                programme_before=held.history_ready?held.history_programme_ready_original:held.history_programme_latest;
                schema_before=held.history_ready?held.history_schema_ready_original:held.history_schema_latest;
                scope_before=held.history_ready?held.history_scope_ready_original:held.history_scope_latest;
            }
            // Unknown/missing publication still closes the actual captured
            // owner. It never turns another owner's snapshot into evidence.
        }
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_EXTRA_ALIAS)
        const auto close_end=std::min(data->end,clock::now()+reserve); // Issued ONCE, before either phase.
        owner->request_close();
        std::shared_ptr<ordinary_target_staging_connection>* alias_source=nullptr;
        {std::unique_lock lock(held.leaf);
            held.history_changed.wait_until(lock,close_end,[&]{return held.history_alias_turn_returned||held.history_bridge_first_failure;});
            const auto& pre=held.history_alias_before;const auto& post=held.history_alias_after;
            const bool unselected=pre==post&&pre[0]==65535&&pre[6]==4&&pre[7]==1&&pre[10]>0&&
                std::all_of(pre.begin()+17,pre.begin()+26,[](auto value){return value==0;})&&pre[27]==0;
            if(!held.history_bridge_first_failure&&held.history_alias_armed&&held.history_alias_control_known&&
               held.history_alias_call_returned&&!held.history_alias_call_ended&&held.history_alias_turn_returned&&unselected&&
               held.history_alias_turn==std::array<std::uint64_t,6>{0,0,0,0,1,1}&&held.history_extra_alias&&
               !held.history_alias_release_entered&&clock::now()<close_end) {
                held.history_alias_release_entered=true;alias_source=&held.history_extra_alias;}
        }
        if(alias_source&&data->process==creator()) {
            // All case-only worker callbacks are one-shot and now cease
            // reading/writing this cell. The static fixture retains its cell
            // across fork; no local strong destructor can run in a child.
            alias_source->reset(); // ACTUAL release, outside every owner/registry/fixture leaf.
            if(data->process!=creator())return 0;
            {std::lock_guard lock(held.leaf);held.history_alias_release_returned=true;held.history_alias_empty_after=!*alias_source;}
            owner->schedule_collection(); // Existing actual preselection wake, same admission/end.
        }
        const auto actual=owner->close(close_end);
#else
        const auto actual=owner->close(std::min(data->end,clock::now()+reserve));
#endif
        if(data->process!=creator())return 0;
        bool collected=false,worker_joined=false,service_released=false,terminal_joined=false,cancel_entered=false,cancel_returned=false;
        std::array<std::uint64_t,10> terminal{};
        {std::lock_guard lock(data->leaf);collected=data->collected;worker_joined=data->worker_joined;service_released=data->service_released;
            terminal_joined=data->terminal_graph_joined;cancel_entered=data->terminal_cancel_entered;cancel_returned=data->terminal_cancel_returned;
            terminal=data->terminal_collection_observation;}
        {std::lock_guard lock(held.leaf);
            if(held.history_bridge_first_failure)return 0;
            if(held.history_owner_original==owner.get()&&held.history_state_original==data.get()) {
                after=held.history_latest;raw_after=held.history_raw_latest;programme_after=held.history_programme_latest;schema_after=held.history_schema_latest;scope_after=held.history_scope_latest;
            }
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_EXTRA_ALIAS)
            extra_alias={{"armed",held.history_alias_armed},{"controlKnown",held.history_alias_control_known},
                {"original",reinterpret_cast<std::uintptr_t>(held.history_alias_original)},
                {"callReturned",held.history_alias_call_returned},{"callEnded",held.history_alias_call_ended},
                {"turnReturned",held.history_alias_turn_returned},{"releaseEntered",held.history_alias_release_entered},
                {"releaseReturned",held.history_alias_release_returned},{"aliasEmptyAfter",held.history_alias_empty_after},
                {"before",held.history_alias_before},{"after",held.history_alias_after},{"turn",held.history_alias_turn}};
#endif
            held.history_close_returned=true;}
        const auto result=nlohmann::json{{"version",1},{"captured",true},
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_HISTORY_EXTRA_ALIAS)
            {"extraAlias",extra_alias},
#endif
            {"beforeClose",history_fixture_snapshot(before,raw_before,programme_before,schema_before,scope_before)},
            {"afterClose",history_fixture_snapshot(after,raw_after,programme_after,schema_after,scope_after)},
            {"terminalShared",terminal},{"terminalRequest",{{"cancelEntered",cancel_entered},{"cancelReturned",cancel_returned},
                {"cancelled",request->state_->cancelled.load(std::memory_order_acquire)},{"graphJoined",terminal_joined}}},
            {"close",{{"cleanupComplete",actual.cleanup_complete},{"failed",bool(actual.error)},
                {"cleanupFailed",bool(actual.cleanup_error)},{"collected",collected},
                {"workerJoined",worker_joined},{"serviceReleased",service_released},{"sync",static_cast<int>(actual.sync)}}}}.dump();
        if(result.empty()||result.size()>16384||result.size()>capacity)return 0;
        std::memcpy(output,result.data(),result.size());return result.size();
    }catch(...) {
        try {auto& held=original();if(held.process!=creator())return 0;
            std::lock_guard lock(held.leaf);if(!held.history_bridge_first_failure)held.history_bridge_first_failure=std::current_exception();}catch(...){}
        return 0;
    }
}
extern "C" std::size_t lattice_receiver_history_fixture_close_json_v1(void* output,std::size_t capacity)noexcept {
    return ordinary_receiver_reclamation_fixture::close_history_json(output,capacity);
}
