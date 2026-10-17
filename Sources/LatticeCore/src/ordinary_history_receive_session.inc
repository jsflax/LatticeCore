// Called only through the actual configured ordinary session. Parsing stays
// bounded under the ingress leaf; all SQLite work is on discovery after it.
bool ordinary_history_receive_state::current()const{
    const auto life=lifetime_.lock();return (!managed_binding_||managed_binding_->configuration_current())&&!failed_.load(std::memory_order_acquire)&&life&&life->current(lifecycle_)&&endpoint_.current_system_tls_for_owner()&&std::chrono::steady_clock::now()<episode_end_;
}
bool ordinary_receiver_session::receive_history(const platform_transport_callbacks& endpoint,uint64_t lifecycle,std::string_view raw,std::shared_ptr<ordinary_history_receive_operation>* completed){
    if(!completed)reject("history profile requires its dedicated queued output");
    std::shared_ptr<record> connection;std::shared_ptr<ordinary_history_receive_state> held;std::shared_ptr<ordinary_history_receive_operation> operation;
    {std::unique_lock lock(mutex_,std::try_to_lock);if(!lock.owns_lock())reject("concurrent history ingress refused");connection=current_;
        if(!connection||!connection->endpoint.matches(endpoint)||connection->lifecycle!=lifecycle||!live(connection))reject("stale or unauthenticated history frame");
        try{
            if(!connection->effect_request_sent)reject("history before actual effect request");
            held=connection->history;
            if(!held){stage_reservation charge;held=std::shared_ptr<ordinary_history_receive_state>(new ordinary_history_receive_state(std::move(charge),{},channel_));held->endpoint_=endpoint;held->lifecycle_=lifecycle;held->lifetime_=lifetime_;held->managed_binding_=managed_binding_;}
            if(!connection->offered){
                if(raw.size()>4096)reject("history offer byte bound");auto value=history_receive_codec::parse_frame(raw);keys(value,{"version","profile","kind","request"});
                if(value["kind"]!="offer"||value["request"]!=connection->request)reject("history negotiation correlation differs");
                connection->history=held;begin_pre_header_admission(connection);return true;
            }
            if(connection->relation_required&&!held->lineage_relation_){
                if(!held->descriptor_.header.empty()||!held->current()||held->command_pending_.load(std::memory_order_acquire)||!managed_binding_)reject("history relation admission out of order or retired");
                const auto& expected=managed_binding_->expected_history_relation();
                auto observation=history_lineage_codec::decode(raw,expected,connection->request,channel_,model_profile_);
                // Count actual retained capacities with the original owner slice;
                // SAX/DOM used the existing pre-reserved parser workspace. No
                // extra slot, SQL command, progress output or deadline refresh.
                const auto retained=history_lineage_codec::retained(observation);
                if(sizeof(*held)>ordinary_history_limits::owner_bytes||held->channel_.capacity()>ordinary_history_limits::owner_bytes-sizeof(*held)||retained>ordinary_history_limits::owner_bytes-sizeof(*held)-held->channel_.capacity())reject("history relation combined owner byte bound");
                if(!live(connection)||!held->current()||!managed_binding_->has_receiver_history_expectation())reject("history relation retired during decode");
                held->lineage_relation_=std::move(observation);connection->history=held;return true;
            }
            if(!held->current()||held->command_pending_.exchange(true,std::memory_order_acq_rel))reject("history command already pending or retired");
            operation=std::shared_ptr<ordinary_history_receive_operation>(new ordinary_history_receive_operation(held));
            if(held->descriptor_.header.empty()){
                held->descriptor_=decode_ordinary_history_descriptor(raw,held->limits_);
                if(!model_profile_.empty()&&held->descriptor_.projection.models!=expected_models_)reject("history root scope differs from actual selected store");
                if(connection->relation_required){
                    if(!held->lineage_relation_)reject("required history relation absent");
                    history_lineage_codec::match_header(*held->lineage_relation_,held->descriptor_);
                    if(!managed_binding_||!managed_binding_->has_receiver_history_expectation()||!live(connection))reject("history relation header retired");
                }
                connection->expires=clock_millis()+300000;held->episode_end_=std::chrono::steady_clock::time_point(std::chrono::milliseconds(connection->expires.load()));deadline_millis_=connection->expires.load();
            }else {
                if(held->progress_.complete)reject("history complete staging awaits managed final installation");
                operation->page_=decode_ordinary_history_page(raw,held->descriptor_,held->progress_,held->limits_);
            }
            connection->history=held;*completed=operation;
        }catch(...){connection->failed=true;if(held)held->failed_=true;connection->history.reset();deadline_millis_=INT64_MAX;throw;}
    }
    return true;
}
void ordinary_receiver_session::install_history(const std::shared_ptr<ordinary_history_receive_operation>& operation,owned_platform_sync_transport& transport){
    if(!operation||operation->consumed_.exchange(true,std::memory_order_acq_rel))reject("history operation already consumed");const auto state=operation->state_;std::shared_ptr<record> connection;
    {std::lock_guard lock(mutex_);connection=current_;if(!connection||connection->history!=state||!connection->endpoint.matches(state->endpoint_)||connection->lifecycle!=state->lifecycle_)reject("history operation lost its actual session");}
    if(!live(connection)||!state->current()){closed(connection->endpoint,connection->lifecycle);return;}
    const auto owner=owner_.lock();if(!owner){closed(connection->endpoint,connection->lifecycle);return;}
    try{
        // This private method returns only after actual owned COMMIT. It
        // stages bytes and indexes; it cannot publish a canonical receipt or
        // mutate any active user row, baseline, original or ACK/debt state.
        const auto progress=ordinary_export_access::stage_history(*owner,operation);
        if(!live(connection)||!state->current()){closed(connection->endpoint,connection->lifecycle);return;}
        operation->page_.reset(); // off-leaf, before a synchronous next-page callback
        {std::lock_guard lock(mutex_);if(current_!=connection||connection->history!=state)reject("staged history lost its current session");state->progress_=progress;state->command_pending_=false;}
        if(progress.complete&&state->restart_){
            ordinary_export_access::install_history_accepted(*owner,state);
            if(!live(connection)||!state->current()){closed(connection->endpoint,connection->lifecycle);return;}
        }
        const auto& header=state->descriptor_;const auto& frame=header.projection;
        const auto response=json{{"latticeOrdinarySync",{{"version",2},{"profile","paged-history-v1"},{"kind","historyProgress"},{"source",frame.source},{"epoch",std::to_string(frame.epoch)},{"cut",std::to_string(frame.cut)},{"stage",header.stage},{"nextPage",std::to_string(progress.next_page)},{"previousDigest",progress.previous},{"stagedComplete",progress.complete}}}}.dump();
        // This is expressly NOT historyReceipt, a known-original ACK, or an
        // instruction to prune source history. Actual final install is gated
        // on a private managed profile which this staging slice cannot issue.
        if(!transport.send_to_attempt(connection->endpoint,transport_message::from_string(response)))reject("durable history progress send refused");
    }catch(...){state->failed_=true;state->command_pending_=false;connection->failed=true;{std::lock_guard lock(mutex_);if(current_==connection)deadline_millis_=INT64_MAX;}throw;}
}
