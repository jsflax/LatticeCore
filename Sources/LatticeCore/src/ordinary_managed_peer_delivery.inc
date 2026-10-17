// Fixed actual daemon->MCP observation protocol, included after peer_request.inc.
// It grants no model materialization, privacy removal, history installation,
// partition completeness, upload ACK or repair completion.
namespace {
enum class peer_message : std::uint8_t { begin=1,data=2,end=3,ready=4,authorize=5,result=6 };
constexpr std::size_t peer_header_bytes=88;
struct peer_frame_view {
    peer_message kind;
    std::int64_t request;
    std::size_t total,offset;
    std::string digest;
    std::span<const std::uint8_t> payload;
};
peer_frame_view peer_frame_read(std::span<const std::uint8_t> bytes,const ordinary_managed_cohort_link_facts& link){
    if(bytes.size()<peer_header_bytes||bytes.size()>ordinary_sync_limits::frame_bytes||
       !std::equal(bytes.begin(),bytes.begin()+8,"LMPOBS01"))refuse("managed peer frame shape");
    if(bytes[8]<1||bytes[8]>6||!std::all_of(bytes.begin()+9,bytes.begin()+16,[](auto c){return c==0;}))refuse("managed peer frame version");
    if(!std::equal(bytes.begin()+16,bytes.begin()+32,link.nonce.begin()))refuse("managed peer link nonce differs");
    std::size_t at=32;
    const auto number=[&](){std::uint64_t n=0;for(unsigned i=0;i<8;++i)n=(n<<8)|bytes[at++];return n;};
    const auto request=number(),total=number(),offset=number();
    if(request==0||request>INT64_MAX||total==0||total>managed_peer_request_bytes||offset>total)refuse("managed peer frame coordinates");
    return {static_cast<peer_message>(bytes[8]),static_cast<std::int64_t>(request),static_cast<std::size_t>(total),static_cast<std::size_t>(offset),
        picosha2::bytes_to_hex_string(bytes.begin()+56,bytes.begin()+88),bytes.subspan(peer_header_bytes)};
}
std::vector<std::uint8_t> peer_frame_write(peer_message kind,std::int64_t request,std::size_t total,std::size_t offset,
    const std::string& digest,std::span<const std::uint8_t> payload,const ordinary_managed_cohort_link_facts& link){
    if(request<=0||total==0||total>managed_peer_request_bytes||offset>total||payload.size()>ordinary_sync_limits::frame_bytes-peer_header_bytes)
        refuse("managed peer output bound");
    original_digest(digest);
    // One fixed framing copy at most 4MiB, charged by the retained source or
    // receive payload reservation. No arbitrary output callback constructs it.
    std::vector<std::uint8_t> out;out.reserve(peer_header_bytes+payload.size());
    for(const char c:std::string_view("LMPOBS01"))out.push_back(static_cast<std::uint8_t>(c));
    out.push_back(static_cast<std::uint8_t>(kind));for(unsigned i=0;i<7;++i)out.push_back(0);
    out.insert(out.end(),link.nonce.begin(),link.nonce.end());
    const auto number=[&](std::uint64_t value){for(unsigned i=0;i<8;++i)out.push_back(static_cast<std::uint8_t>(value>>(56-8*i)));};
    number(static_cast<std::uint64_t>(request));number(total);number(offset);
    const auto hex=[](char c)->unsigned{return c<='9'?static_cast<unsigned>(c-'0'):static_cast<unsigned>(c-'a'+10);};
    for(std::size_t i=0;i<64;i+=2)out.push_back(static_cast<std::uint8_t>((hex(digest[i])<<4)|hex(digest[i+1])));
    out.insert(out.end(),payload.begin(),payload.end());return out;
}
}
struct ordinary_managed_ipc_delivery::state {
    const std::uint64_t creator=managed_pair_creator();
    const std::shared_ptr<ordinary_managed_peer_owner> peer;
    std::atomic<bool> stopping{false};
    std::timed_mutex turn;
    // Fixed actual in-flight custody, not a second request lane. Protected
    // fields are pinned while SQL/notifications run without this leaf.
    std::atomic<bool> external{false};
    struct outside_turn {
        state& owner;
        bool complete=false;
        explicit outside_turn(state& value):owner(value){
            if(owner.external.exchange(true,std::memory_order_acq_rel))refuse("managed peer external turn already admitted");
        }
        ~outside_turn(){
            if(owner.creator!=managed_pair_creator())return;
            if(!complete)owner.stopping.store(true,std::memory_order_release);
            owner.external.store(false,std::memory_order_release);
        }
        void reacquire(std::unique_lock<std::timed_mutex>& lock,ordinary_ipc_deadline end){
            // A callback or a final context destructor may have forked. There
            // is no owned lock to unlock and no inherited leaf to enter here.
            if(owner.creator!=managed_pair_creator())refuse("managed peer external turn inherited");
            if(!lock.try_lock_until(end))refuse("managed peer external settlement admission expired");
            owner.live();
        }
    };
    struct route_cursor {std::weak_ptr<const ordinary_managed_ipc_local_route> route;std::string scope,gid;};
    std::array<route_cursor,64> routes{}; // same existing 64-role ceiling, no payloads
    std::size_t next_route=0;
    std::int64_t next_request=1;
    std::unique_ptr<ordinary_managed_peer_request> outgoing;
    std::unique_ptr<ordinary_managed_peer_staged> incoming;
    enum class send_phase { idle,begin,data,end,ready,result } phase=send_phase::idle;
    std::size_t offset=0;
    explicit state(std::shared_ptr<ordinary_managed_peer_owner> value):peer(std::move(value)){}
    void live()const{if(creator!=managed_pair_creator()||stopping.load(std::memory_order_acquire))refuse("managed peer delivery stopped or inherited");peer->validate_lifetime();}
    // Caller owns turn. Pointer selection and both deadlines change only
    // under that leaf; outside_turn pins them while real SQL/callbacks run.
    ordinary_ipc_deadline pending_deadline(ordinary_ipc_deadline upper)const {
        auto result=upper;
        if(outgoing)result=std::min(result,outgoing->impl_->deadline);
        // apply_managed_peer_request writes applied outside turn. While that
        // work is charged, do not read applied at all: retain the original
        // deadline even after COMMIT but before off-leaf settlement returns.
        // false is observed with acquire after outside_turn's release store.
        // Starting a new outside turn also requires this same held leaf.
        if(incoming&&(external.load(std::memory_order_acquire)||!incoming->impl_->applied))
            result=std::min(result,incoming->impl_->deadline);
        return result;
    }
};
struct ordinary_managed_ipc_output::state {
    ordinary_managed_ipc_delivery* owner;
    peer_message kind;
    std::int64_t request;
    std::vector<std::uint8_t> bytes;
    const ordinary_ipc_deadline expires;
    state(ordinary_managed_ipc_delivery* o,peer_message k,std::int64_t id,std::vector<std::uint8_t>&& b,ordinary_ipc_deadline end):owner(o),kind(k),request(id),bytes(std::move(b)),expires(end){}
};
ordinary_managed_ipc_output::ordinary_managed_ipc_output(std::unique_ptr<state> value):state_(std::move(value)){}
ordinary_managed_ipc_output::~ordinary_managed_ipc_output()=default;
ordinary_managed_ipc_output::ordinary_managed_ipc_output(ordinary_managed_ipc_output&&)noexcept=default;
ordinary_managed_ipc_output& ordinary_managed_ipc_output::operator=(ordinary_managed_ipc_output&&)noexcept=default;
std::span<const std::uint8_t> ordinary_managed_ipc_output::frame()const{if(!state_)refuse("managed peer output moved");return state_->bytes;}
ordinary_ipc_deadline ordinary_managed_ipc_output::deadline(ordinary_ipc_deadline upper)const{
    if(!state_)refuse("managed peer output moved");return std::min(upper,state_->expires);
}
void ordinary_managed_ipc_output::validate_before_send(const ordinary_managed_ipc_role& role,ordinary_ipc_deadline end){
    if(!state_||!state_->owner)refuse("managed peer output unavailable");state_->owner->validate_output(role,*this,end);
}
ordinary_managed_ipc_delivery::ordinary_managed_ipc_delivery(std::unique_ptr<state> value):state_(std::move(value)){}
ordinary_managed_ipc_delivery::~ordinary_managed_ipc_delivery(){
    if(state_&&state_->creator!=managed_pair_creator()){(void)state_.release();return;}request_stop();
    // Failed or unfinished work is owned debt. The explicit retire path must
    // remove captures off-lock before this shell can be safely destroyed.
    if(state_&&(state_->external.load(std::memory_order_acquire)||state_->outgoing||state_->incoming)){
        (void)state_.release();return;
    }
}
std::unique_ptr<ordinary_managed_ipc_delivery> ordinary_managed_ipc_delivery::for_peer(
    const std::shared_ptr<ordinary_managed_peer_owner>& peer){
    if(!peer)refuse("managed peer delivery owner absent");peer->validate_lifetime();
    return std::unique_ptr<ordinary_managed_ipc_delivery>(new ordinary_managed_ipc_delivery(std::make_unique<state>(peer)));
}
void ordinary_managed_ipc_delivery::bind_route(std::shared_ptr<const ordinary_managed_ipc_local_route> route,ordinary_ipc_deadline end){
    state_->live();if(!route||!state_->peer->facts().local_is_daemon)refuse("managed peer route requires actual daemon");
    route->validate_lifetime();const auto& d=route->description();
    if(d.recipient_member!="memory.sqlite"||d.generation!=state_->peer->impl_->generation||
       &route->recipient_owner()!=state_->peer->impl_->owner.get()||&route->recipient_profile()!=state_->peer->impl_->profile.get())
        refuse("managed peer route primary/profile mismatch");
    std::unique_lock lock(state_->turn,std::defer_lock);if(!lock.try_lock_until(end))refuse("managed peer route admission expired");
    state_->live();if(state_->external.load(std::memory_order_acquire))refuse("managed peer route admission busy");
    for(const auto& cursor:state_->routes)if(!cursor.route.owner_before(route)&&!route.owner_before(cursor.route))return;
    auto empty=std::find_if(state_->routes.begin(),state_->routes.end(),[](const auto& x){return x.route.expired();});
    if(empty==state_->routes.end())refuse("managed peer registered route bound");
    empty->scope.clear();empty->gid.clear();empty->route=route;
    lock.unlock(); // argument/final owner destruction never under delivery leaf
}
void ordinary_managed_ipc_delivery::submit(std::shared_ptr<const ordinary_managed_ipc_local_route> route,
    const std::string& scope,const std::string& gid,ordinary_ipc_deadline end){
    state_->live();if(!state_->peer->facts().local_is_daemon)refuse("managed peer recipient cannot issue upstream request");
    std::unique_lock lock(state_->turn,std::defer_lock);if(!lock.try_lock_until(end))refuse("managed peer submission admission expired");
    state_->live();if(state_->external.load(std::memory_order_acquire)||state_->outgoing||state_->phase!=state::send_phase::idle||state_->next_request==INT64_MAX)
        refuse("managed peer pending request retains admission");
    // No capture or payload allocation occurs before the one actual request
    // lane is reserved. Fixed protocol code alone performs the source capture.
    state::outside_turn custody(*state_);const auto sequence=state_->next_request;
    lock.unlock();
    auto request=ordinary_export_access::make_managed_peer_request(state_->peer,route,scope,gid,sequence,end);
    custody.reacquire(lock,end);
    state_->outgoing=std::move(request);
    ++state_->next_request;state_->phase=state::send_phase::begin;state_->offset=0;
    custody.complete=true;
    lock.unlock(); // final route/temporary capture destruction stays off-leaf
}
ordinary_managed_ipc_output ordinary_managed_ipc_delivery::output(std::uint8_t kind,std::int64_t request,
    std::size_t total,std::size_t offset,const std::string& digest,std::span<const std::uint8_t> payload){
    auto actual=static_cast<peer_message>(kind);
    const auto expires=state_->outgoing&&state_->outgoing->impl_->request==request?state_->outgoing->impl_->deadline:
        state_->incoming&&state_->incoming->impl_->request==request?state_->incoming->impl_->deadline:ordinary_ipc_deadline{};
    managed_pair_deadline(expires);
    return ordinary_managed_ipc_output(std::make_unique<ordinary_managed_ipc_output::state>(this,actual,request,
        peer_frame_write(actual,request,total,offset,digest,payload,state_->peer->facts()),expires));
}
std::optional<ordinary_managed_ipc_output> ordinary_managed_ipc_delivery::prepare_output(
    const ordinary_managed_ipc_role& role,ordinary_ipc_deadline end){
    state_->live();if(role.peer_owner()!=state_->peer)refuse("managed peer output role changed");
    std::unique_lock lock(state_->turn,std::defer_lock);if(!lock.try_lock_until(end))refuse("managed peer output turn expired");
    state_->live();if(state_->external.load(std::memory_order_acquire))return std::nullopt;
    if(!state_->outgoing&&state_->peer->facts().local_is_daemon){
        if(state_->next_request==INT64_MAX)refuse("managed peer request sequence exhausted");
        std::size_t selected=state_->routes.size();
        for(std::size_t n=0;n<state_->routes.size();++n){const auto i=(state_->next_route+n)%state_->routes.size();if(!state_->routes[i].route.expired()){selected=i;break;}}
        if(selected!=state_->routes.size()){
            auto& cursor=state_->routes[selected];state_->next_route=(selected+1)%state_->routes.size();
            state::outside_turn custody(*state_);lock.unlock();
            // Idle registration is only a weak scheduling hint. Actual native
            // promotion and current validation precede every SQL/request. The
            // endpoint's submitted request then owns its whole strong route.
            auto route=cursor.route.lock();
            if(!route||!ordinary_managed_ipc_access::route_current(*route)){
                // Credential/policy revocation closes new publication. Remove
                // the actual stale owner outside the leaf; no new authority.
                cursor.route.reset();route.reset();
                if(state_->creator!=managed_pair_creator())refuse("managed peer route finalizer inherited");
                custody.complete=true;return std::nullopt;
            }
            std::unique_ptr<ordinary_managed_peer_request> request;
            try{request=ordinary_export_access::next_managed_peer_request(state_->peer,route,cursor.scope,cursor.gid,state_->next_request,end);}
            catch(const ordinary_export_capacity_exhausted&){
                // Only the typed actual common-pool refusal is idle. Generic
                // semantic/SQL/authority failures keep their failed outcome.
                route.reset();if(state_->creator!=managed_pair_creator())refuse("managed peer capacity finalizer inherited");
                custody.complete=true;return std::nullopt;
            }
            route.reset();if(state_->creator!=managed_pair_creator())refuse("managed peer source finalizer inherited");
            custody.reacquire(lock,end);
            if(request){state_->outgoing=std::move(request);++state_->next_request;state_->phase=state::send_phase::begin;state_->offset=0;}
            custody.complete=true;
        }
    }
    if(!state_->outgoing)return std::nullopt;
    auto& request=*state_->outgoing->impl_;
    managed_pair_deadline(std::min(end,request.deadline));
    if(state_->phase==state::send_phase::begin){
        state_->phase=state::send_phase::data;
        return output(1,request.request,request.bytes.size(),0,request.digest,{});
    }
    if(state_->phase==state::send_phase::data){
        const auto offset=state_->offset;
        const auto count=std::min(ordinary_sync_limits::frame_bytes-peer_header_bytes,request.bytes.size()-offset);
        auto result=output(2,request.request,request.bytes.size(),offset,request.digest,std::span<const std::uint8_t>(request.bytes).subspan(offset,count));
        state_->offset+=count;if(state_->offset==request.bytes.size())state_->phase=state::send_phase::end;
        return result;
    }
    if(state_->phase==state::send_phase::end){
        // Allow a real response before the writer's successful-return callback.
        // The actual peer cannot produce READY before consuming this END.
        state_->phase=state::send_phase::ready;
        return output(3,request.request,request.bytes.size(),request.bytes.size(),request.digest,{});
    }
    return std::nullopt;
}
void ordinary_managed_ipc_delivery::validate_output(const ordinary_managed_ipc_role& role,
    ordinary_managed_ipc_output& value,ordinary_ipc_deadline end){
    managed_pair_deadline(value.deadline(end));
    state_->live();if(role.peer_owner()!=state_->peer||!value.state_||value.state_->owner!=this)refuse("managed peer exact output owner changed");
    std::unique_lock lock(state_->turn,std::defer_lock);if(!lock.try_lock_until(end))refuse("managed peer output authorization expired");
    state_->live();if(state_->external.load(std::memory_order_acquire))refuse("managed peer output turn busy");
    if(value.state_->kind==peer_message::authorize){
        if(!state_->outgoing||state_->outgoing->impl_->request!=value.state_->request||state_->phase!=state::send_phase::result)
            refuse("managed peer authorization no longer selected");
        // This final producer check occurs immediately before actual socket
        // send, not while a response waits in the endpoint's queue.
        state::outside_turn custody(*state_);auto* actual=state_->outgoing.get();
        lock.unlock();
        ordinary_export_access::authorize_managed_peer_request(*actual);
        custody.reacquire(lock,end);
        if(state_->outgoing.get()!=actual)refuse("managed peer authorized request replaced");
        custody.complete=true;
        lock.unlock();
    }else if(state_->outgoing&&value.state_->request==state_->outgoing->impl_->request){
        auto& request=*state_->outgoing->impl_;request.route->validate_lifetime();
        managed_peer_capture_live(request.route->product_authority(),request.capture().impl_->evidence);
        managed_pair_deadline(std::min(end,request.deadline));
    }
}
std::optional<ordinary_managed_ipc_output> ordinary_managed_ipc_delivery::consume_input(
    const ordinary_managed_ipc_role& role,std::span<const std::uint8_t> bytes,ordinary_ipc_deadline end){
    state_->live();if(role.peer_owner()!=state_->peer)refuse("managed peer input role changed");
    std::unique_lock lock(state_->turn,std::defer_lock);if(!lock.try_lock_until(end))refuse("managed peer input turn expired");
    state_->live();if(state_->external.load(std::memory_order_acquire))refuse("managed peer input turn busy");
    const auto frame=peer_frame_read(bytes,state_->peer->facts());
    if(state_->peer->facts().local_is_daemon){
        if(!state_->outgoing)refuse("managed peer unsolicited reply");auto& request=*state_->outgoing->impl_;
        if(frame.request!=request.request||frame.total!=request.bytes.size()||frame.offset!=frame.total||frame.digest!=request.digest)
            refuse("managed peer reply correlation differs");
        if(frame.kind==peer_message::ready){
            if(state_->phase!=state::send_phase::ready||!frame.payload.empty())refuse("managed peer READY phase differs");
            state_->phase=state::send_phase::result;
            return output(5,frame.request,frame.total,frame.total,frame.digest,{});
        }
        if(frame.kind!=peer_message::result||state_->phase!=state::send_phase::result||!request.authorized)refuse("managed peer result before actual authorization");
        budget retained{wire_limits};const auto result=managed_peer_decode(frame.payload,retained,3);
        if(result.size()!=3)refuse("managed peer result shape");original_digest(text(result[0]));
        (void)managed_peer_boolean(result[1]);(void)managed_peer_boolean(result[2]);
        // Exact trusted recipient observed a clean passive-ledger COMMIT. This
        // consumes only this transport request. No delivery/repair flag changes.
        state::outside_turn custody(*state_);auto* selected=state_->outgoing.get();
        lock.unlock();ordinary_export_access::settle_managed_peer_request(*selected,text(result[0]),end);
        custody.reacquire(lock,end);
        if(state_->outgoing.get()!=selected)refuse("managed peer settled request replaced");
        auto retired=std::move(state_->outgoing);state_->phase=state::send_phase::idle;lock.unlock();
        retired.reset(); // actual route/original finalizers run without a leaf
        if(state_->creator!=managed_pair_creator())refuse("managed peer result finalizer inherited");
        custody.complete=true;return std::nullopt;
    }
    if(frame.kind==peer_message::begin){
        if(state_->next_request==INT64_MAX||frame.request!=state_->next_request||frame.offset!=0||!frame.payload.empty()||
            (state_->incoming&&!state_->incoming->impl_->applied))refuse("managed peer request admission already charged");
        auto stage=std::make_unique<ordinary_managed_peer_staged::implementation>(); // reservation before body allocation
        stage->peer=state_->peer;stage->request=frame.request;stage->digest=frame.digest;stage->deadline=end;
        stage->expected_bytes=frame.total;stage->bytes.reserve(frame.total);
        auto retired=std::move(state_->incoming);
        state_->incoming=std::unique_ptr<ordinary_managed_peer_staged>(new ordinary_managed_peer_staged(std::move(stage)));
        ++state_->next_request;
        state::outside_turn custody(*state_);lock.unlock();
        retired.reset();
        if(state_->creator!=managed_pair_creator())refuse("managed peer prior stage finalizer inherited");
        custody.complete=true;return std::nullopt;
    }
    if(!state_->incoming)refuse("managed peer body without actual begin");auto& stage=*state_->incoming->impl_;
    end=std::min(end,stage.deadline);managed_pair_deadline(end);
    if(frame.request!=stage.request||frame.digest!=stage.digest||frame.total!=stage.expected_bytes)refuse("managed peer staging coordinates changed");
    if(frame.kind==peer_message::data){
        if(stage.ready||frame.offset!=stage.bytes.size()||frame.payload.empty()||frame.payload.size()>frame.total-frame.offset)
            refuse("managed peer body order or bound");
        stage.bytes.insert(stage.bytes.end(),frame.payload.begin(),frame.payload.end());return std::nullopt;
    }
    if(frame.kind==peer_message::end){
        if(stage.ready||frame.offset!=frame.total||stage.bytes.size()!=frame.total||!frame.payload.empty()||
            picosha2::hash256_hex_string(stage.bytes)!=stage.digest)refuse("managed peer exact body incomplete");
        budget retained{wire_limits};stage.decoded.emplace(managed_peer_parse(stage.bytes,retained));
        if(stage.decoded->request!=stage.request||stage.decoded->nonce!=managed_peer_nonce(state_->peer->facts()))refuse("managed peer staged request differs");
        const auto expires=std::min(end,managed_peer_deadline(stage.decoded->expires));stage.deadline=expires;
        state::outside_turn custody(*state_);auto* actual=state_->incoming.get();
        lock.unlock();state_->peer->validate(expires);
        custody.reacquire(lock,expires);
        if(state_->incoming.get()!=actual)refuse("managed peer staged owner replaced");
        stage.ready=true;custody.complete=true;
        return output(4,frame.request,frame.total,frame.total,frame.digest,{});
    }
    if(frame.kind!=peer_message::authorize||!stage.ready||stage.authorized||frame.offset!=frame.total||!frame.payload.empty())
        refuse("managed peer authorization phase differs");
    stage.authorized=true; // one-shot; failed/uncertain COMMIT retains stage/debt
    state::outside_turn custody(*state_);auto* actual=state_->incoming.get();
    lock.unlock();const auto result=ordinary_export_access::apply_managed_peer_request(*actual,end);
    custody.reacquire(lock,end);
    if(state_->incoming.get()!=actual)refuse("managed peer applied owner replaced");
    budget retained{wire_limits};auto payload=encode({result.observation_seal,static_cast<std::int64_t>(result.exact_replay),static_cast<std::int64_t>(result.other_lineage_observed)},retained);
    custody.complete=true;
    return output(6,frame.request,frame.total,frame.total,frame.digest,payload);
}
void ordinary_managed_ipc_delivery::output_written(const ordinary_managed_ipc_role& role,ordinary_managed_ipc_output& output){
    if(state_->creator!=managed_pair_creator())return;
    if(role.peer_owner()!=state_->peer||!output.state_||output.state_->owner!=this)refuse("managed peer written output changed");
    // Publication already advanced the transmission cursor. A successful
    // write alone never consumes a request or produces a database receipt.
}
ordinary_ipc_deadline ordinary_managed_ipc_delivery::wait_deadline(ordinary_ipc_deadline upper){
    state_->live();std::unique_lock lock(state_->turn,std::defer_lock);
    if(!lock.try_lock_until(upper))refuse("managed peer waiting admission expired");
    state_->live();const auto result=state_->pending_deadline(upper);
    managed_pair_deadline(result);return result;
}
void ordinary_managed_ipc_delivery::request_stop()noexcept{
    if(state_&&state_->creator==managed_pair_creator())state_->stopping.store(true,std::memory_order_release);
}
bool ordinary_managed_ipc_delivery::retire(ordinary_ipc_deadline end){
    if(!state_||state_->creator!=managed_pair_creator()||end==ordinary_ipc_deadline::max())return false;
    request_stop();std::unique_lock lock(state_->turn,std::defer_lock);if(!lock.try_lock_until(end))return false;
    if(state_->external.load(std::memory_order_acquire))return false;
    // An unresolved source result transfers only to an actual committed
    // durable obligation; uncertain recipient SQL needs a clean real read
    // before physical retirement. Neither path fabricates observation success.
    if(state_->outgoing&&!state_->outgoing->impl_->settled&&!state_->outgoing->impl_->durable)return false;
    if(state_->incoming&&state_->incoming->impl_->authorized&&!state_->incoming->impl_->applied&&!state_->incoming->impl_->cleanup_reconciled){
        state::outside_turn proof(*state_);auto* selected=state_->incoming.get();lock.unlock();
        ordinary_export_access::reconcile_managed_peer_stage(*selected,end);
        // Admission is already stopped, so no live() re-admission here.
        if(state_->creator!=managed_pair_creator())return false;
        if(!lock.try_lock_until(end))return false;
        if(state_->incoming.get()!=selected||!selected->impl_->cleanup_reconciled)return false;
        proof.complete=true;
    }
    for(auto& cursor:state_->routes)cursor.route.reset(); // no strong payload/owner destruction
    auto incoming=std::move(state_->incoming);auto outgoing=std::move(state_->outgoing);
    state::outside_turn custody(*state_);lock.unlock();
    outgoing.reset();
    if(state_->creator!=managed_pair_creator()){(void)incoming.release();return false;}
    incoming.reset();
    if(state_->creator!=managed_pair_creator())return false;
    custody.complete=true;return std::chrono::steady_clock::now()<end;
}

ordinary_managed_peer_deadline_probe ordinary_export_access::exercise_managed_peer_deadline_for_test(
    lattice_db& owner,bool deny_commit){
    // Exercise exactly the production external marker, mutex and deadline
    // sampler. This storage-only fixture never constructs a live peer/role;
    // ready/authorized remain false and no stage escapes this fixed method.
    using delivery_state=ordinary_managed_ipc_delivery::state;
    delivery_state paused(std::shared_ptr<ordinary_managed_peer_owner>{});
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    const auto upper=end+std::chrono::seconds(1);
    auto stage=std::make_unique<ordinary_managed_peer_staged::implementation>();
    stage->deadline=end; // actual counted receive reservation, no received bytes
    paused.incoming=std::unique_ptr<ordinary_managed_peer_staged>(new ordinary_managed_peer_staged(std::move(stage)));
    ordinary_managed_peer_deadline_probe probe;
    auto* handle=ordinary_export_storage_access::borrow(owner.db());
    struct authorizer_restore {sqlite3* h;~authorizer_restore(){sqlite3_set_authorizer(h,nullptr,nullptr);}} restore{handle};
    if(deny_commit&&sqlite3_set_authorizer(handle,[](void* raw,int action,const char* first,const char*,const char*,const char*)noexcept{
        if(action==SQLITE_TRANSACTION&&first&&std::string_view(first)=="COMMIT"){
            ++static_cast<ordinary_managed_peer_deadline_probe*>(raw)->denied_commits;return SQLITE_DENY;
        }return SQLITE_OK;
    },&probe)!=SQLITE_OK)refuse("deadline fixture actual authorizer unavailable");
    std::mutex gate;std::condition_variable changed;
    bool entered=false,start_sql=false,sql_returned=false,release_external=false,finished=false,failed=false;
    std::thread worker([&]{
        try {
            std::unique_lock leaf(paused.turn,std::defer_lock);
            if(!leaf.try_lock_until(end))throw std::runtime_error("deadline fixture worker admission expired");
            delivery_state::outside_turn custody(paused);leaf.unlock();
            {
                std::unique_lock signal(gate);entered=true;changed.notify_all();
                if(!changed.wait_until(signal,end,[&]{return start_sql;}))throw std::runtime_error("deadline fixture SQL rendezvous expired");
            }
            const auto actual=recovery_writer_access::ordinary_owned_write(owner,[](database& writer){
                writer.execute("INSERT INTO TestPerson(globalId,name,age) VALUES('managed-deadline-turn','actual-owned-commit',7)");
            });
            probe.actual_commit_clean=actual.state==recovery_install_state::committed&&!actual.primary_error&&!actual.cleanup_error&&!actual.postcommit_error&&!actual.notification_error;
            probe.actual_rollback_observed=actual.state==recovery_install_state::rolled_back&&actual.primary_error&&!actual.cleanup_error;
            // Same off-leaf write position as apply_managed_peer_request,
            // but its truth here comes only from this real fixture SQL turn.
            // It is not an IPC receipt, authorization or cleanup grant.
            if(probe.actual_commit_clean)paused.incoming->impl_->applied=true;
            {
                std::unique_lock signal(gate);sql_returned=true;changed.notify_all();
                if(!changed.wait_until(signal,end,[&]{return release_external;}))throw std::runtime_error("deadline fixture return rendezvous expired");
            }
            custody.complete=probe.actual_commit_clean;
        }catch(...){std::lock_guard signal(gate);failed=true;}
        {std::lock_guard signal(gate);finished=true;}changed.notify_all();
    });
    const auto wait=[&](const auto& predicate){
        std::unique_lock signal(gate);return changed.wait_until(signal,end,predicate);
    };
    if(wait([&]{return entered||finished;})) {
        std::unique_lock leaf(paused.turn,std::defer_lock);if(!leaf.try_lock_until(end))std::abort();probe.external_entered=paused.external.load(std::memory_order_acquire);
        probe.held_before_sql=paused.pending_deadline(upper)==end;
        probe.caller_upper_preserved=paused.pending_deadline(end-std::chrono::seconds(1))==end-std::chrono::seconds(1);
    }else probe.deadline_failed=true;
    {std::lock_guard signal(gate);start_sql=true;}changed.notify_all();
    if(wait([&]{return sql_returned||finished;})) {
        std::unique_lock leaf(paused.turn,std::defer_lock);if(!leaf.try_lock_until(end))std::abort();
        // Deterministic regression: real COMMIT and applied=true have already
        // occurred, but external ownership remains held. Old001 unclamps here.
        probe.held_after_sql=paused.external.load(std::memory_order_acquire)&&paused.pending_deadline(upper)==end;
    }else probe.deadline_failed=true;
    {std::lock_guard signal(gate);release_external=true;}changed.notify_all();
    // An unfinished real worker cannot be detached or counted joined. This
    // failure-only process abort preserves the failed test, never completion.
    if(!wait([&]{return finished;}))std::abort();
    worker.join();probe.worker_joined=true;
    {std::unique_lock leaf(paused.turn,std::defer_lock);if(!leaf.try_lock_until(end))std::abort();
        probe.external_returned=!paused.external.load(std::memory_order_acquire);
        probe.released_after_success=probe.actual_commit_clean&&paused.pending_deadline(upper)==upper;
        probe.held_after_failed_turn=!probe.actual_commit_clean&&paused.pending_deadline(upper)==end;
    }
    probe.deadline_failed=probe.deadline_failed||failed||std::chrono::steady_clock::now()>=end;
    return probe;
}

ordinary_managed_peer_codec_probe exercise_managed_peer_codec(){
    // Passive bytes/facts solely exercise the strict codec. They never create
    // a retained peer, a ready/authorized stage or any actual SQL writer.
    ordinary_managed_peer_codec_probe result;
    ordinary_managed_cohort_link_facts spelling;for(std::size_t n=0;n<spelling.nonce.size();++n)spelling.nonce[n]=static_cast<std::uint8_t>(n+1);
    std::vector<std::uint8_t> body(managed_peer_request_bytes);for(std::size_t n=0;n<body.size();++n)body[n]=static_cast<std::uint8_t>((n*17)%251);
    const auto digest=picosha2::hash256_hex_string(body);result.input_bytes=body.size();
    std::vector<std::uint8_t> reconstructed;reconstructed.reserve(body.size());
    for(std::size_t offset=0;offset<body.size();){
        const auto count=std::min(ordinary_sync_limits::frame_bytes-peer_header_bytes,body.size()-offset);
        const auto encoded=peer_frame_write(peer_message::data,7,body.size(),offset,digest,std::span<const std::uint8_t>(body).subspan(offset,count),spelling);
        const auto actual=peer_frame_read(encoded,spelling);
        if(actual.offset!=offset||actual.total!=body.size()||actual.request!=7||actual.digest!=digest)refuse("test codec coordinates changed");
        reconstructed.insert(reconstructed.end(),actual.payload.begin(),actual.payload.end());
        result.largest_frame=std::max(result.largest_frame,encoded.size());++result.frames;offset+=count;
    }
    result.exact=reconstructed==body;
    auto first=peer_frame_write(peer_message::begin,7,body.size(),0,digest,{},spelling);
    first[16]^=1;try{(void)peer_frame_read(first,spelling);}catch(const db_error&){result.wrong_nonce_refused=true;}first[16]^=1;
    first[8]=7;try{(void)peer_frame_read(first,spelling);}catch(const db_error&){result.unknown_kind_refused=true;}
    first[8]=1;for(std::size_t n=40;n<48;++n)first[n]=0xff;
    try{(void)peer_frame_read(first,spelling);}catch(const db_error&){result.oversized_total_refused=true;}
    budget retained{wire_limits};auto scalar=encode({std::string("actual passive codec")},retained);scalar.push_back(0);
    try{(void)managed_peer_decode(scalar,retained);}catch(const db_error&){result.trailing_scalar_refused=true;}
    return result;
}
