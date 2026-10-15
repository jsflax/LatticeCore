// Included only in ordinary_namespace_retirement.cpp. This is a separate
// controller-owned restart route; generic historical reconciliation is exact.
#ifdef LATTICE_NAMESPACE_POSIX
retained_historical_generation::observation
engram_namespace_retirement::implementation::inspect_managed(
    const retained_managed_runtime_retirement::state& runtime, deadline end) const {
    if (!runtime.verify || !use_mutation_barrier) fail(code::incomplete);
    runtime.verify(end); owner(end); verify_barrier_complete(end);
    if ((held(data.n).st_mode&07777)!=data_original_mode || !recorded("old-paths.fenced",end))fail(code::changed);
    digest barrier{};
    picosha2::hash256(barrier_intent.begin(),barrier_intent.end(),barrier.begin(),barrier.end());
    if (runtime.historical != data_id || runtime.control != control_id ||
        runtime.generation != generation_id || runtime.barrier != barrier ||
        !recorded("deny.complete",end) || !recorded("preserved.unadopted",end)) fail(code::changed);
    retained_historical_generation::observation out;
    out.historical_parent=data_id; out.control=control_id; out.generation=generation_id; out.barrier=barrier;
    if (items.size()+sync_members.size()>256) fail(code::invalid);
    out.old_refusals.reserve(items.size()); out.members.reserve(256);
    std::set<std::string> old_names;
    for (const auto& item:items) {
        const auto old=named(data.n,item.leaf); check_kind(old,item.directory);
        if (identity(old)!=item.tombstone || (old.st_mode&07777)!=0) fail(code::changed);
        old_names.insert(item.leaf);
        out.old_refusals.push_back({item.leaf,item.original,item.tombstone,std::uint32_t(item.mode),item.directory});
    }
    for (const auto& name:list(data.n,end))
        if (store_shaped(name) && !old_names.contains(name)) fail(code::changed);

    // The old main identities remain invariant. Sidecar/scratch identities are
    // observed only after proved predecessor retirement, never from a row or
    // from a new permissive directory census. No new database name is admitted.
    std::map<std::string,const member*> mains;
    for (const auto& value:sync_members)
        if (value.leaf=="memory-synced.sqlite" || group_main(value.leaf)) mains.emplace(value.leaf,&value);
    if (mains.size()+1>64) fail(code::invalid);
    std::set<std::string> allowed_sync{"groups.json","groups.json.tmp","groups-refresh-requested"};
    for (const auto& [name,unused]:mains) {
        allowed_sync.insert(name);
        for (const auto* suffix:{"-wal","-shm","-journal"}) allowed_sync.insert(name+suffix);
    }
    struct held_member { int parent; std::string leaf; fd descriptor; struct stat before; };
    std::vector<held_member> retained; retained.reserve(256);
    std::set<std::pair<std::uint64_t,std::uint64_t>> physical;
    const auto stable_member=[](const struct stat& a,const struct stat& b) {
        if (identity(a)!=identity(b) || a.st_mode!=b.st_mode || a.st_uid!=b.st_uid ||
            a.st_nlink!=b.st_nlink || a.st_size!=b.st_size) return false;
#if defined(__APPLE__)
        return a.st_mtimespec.tv_sec==b.st_mtimespec.tv_sec && a.st_mtimespec.tv_nsec==b.st_mtimespec.tv_nsec &&
            a.st_ctimespec.tv_sec==b.st_ctimespec.tv_sec && a.st_ctimespec.tv_nsec==b.st_ctimespec.tv_nsec;
#else
        return a.st_mtim.tv_sec==b.st_mtim.tv_sec && a.st_mtim.tv_nsec==b.st_mtim.tv_nsec &&
            a.st_ctim.tv_sec==b.st_ctim.tv_sec && a.st_ctim.tv_nsec==b.st_ctim.tv_nsec;
#endif
    };
    auto observe=[&](int parent,const std::string& leaf,const std::string& relative,bool directory,
                     std::optional<file_identity> expected,std::optional<mode_t> mode,file_identity refusal) {
        bound(end); if (out.members.size()==256) fail(code::invalid);
        const auto before=named(parent,leaf); check_kind(before,directory);
        fd value(::openat(parent,leaf.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC|(directory?O_DIRECTORY:0)));
        const auto actual=held(value.n); check_kind(actual,directory); require_no_granting_acl(value.n);
        if (!stable_member(before,actual) || identity(actual).device!=data_id.device ||
            (expected && identity(actual)!=*expected) || (mode && (actual.st_mode&07777)!=*mode) ||
            !physical.emplace(identity(actual).device,identity(actual).inode).second) fail(code::changed);
        // Scratch remains data, not membership. Its content is not parsed.
        if ((leaf=="groups.json" || leaf=="groups.json.tmp" || leaf=="groups-refresh-requested") &&
            (actual.st_size<0 || std::uint64_t(actual.st_size)>1048576)) fail(code::invalid);
        out.members.push_back({relative,identity(actual),refusal,std::uint32_t(actual.st_mode&07777),directory});
        retained.push_back({parent,leaf,std::move(value),actual});
    };
    const auto root_names=list(generation.n,end);
    const auto primary=std::find_if(items.begin(),items.end(),[](const auto& item){return item.leaf=="memory.sqlite"&&!item.directory;});
    const auto sync=std::find_if(items.begin(),items.end(),[](const auto& item){return item.leaf=="sync"&&item.directory;});
    if (primary==items.end() || sync==items.end()) fail(code::changed);
    fd current_sync;
    std::vector<std::string> sync_names;
    for (const auto& name:root_names) {
        if (name=="memory.sqlite") observe(generation.n,name,name,false,primary->original,primary->mode,primary->tombstone);
        else if (name=="sync" && !empty_provisional) {
            observe(generation.n,name,name,true,sync->original,sync->mode,sync->tombstone);
            current_sync=fd(::openat(generation.n,"sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
            if(identity(held(current_sync.n))!=sync->original) fail(code::changed);
        } else if (name=="memory.sqlite-wal" || name=="memory.sqlite-shm" || name=="memory.sqlite-journal") {
            const auto original=std::find_if(items.begin(),items.end(),[&](const auto& item){return item.leaf==name;});
            observe(generation.n,name,name,false,{}, {},original==items.end()?file_identity{}:original->tombstone);
        } else fail(code::changed);
    }
    if (!std::binary_search(root_names.begin(),root_names.end(),"memory.sqlite") ||
        (!empty_provisional && current_sync.n<0)) fail(code::changed);
    if (empty_provisional) {
        if (list(empty_slot.n,end)!=std::vector<std::string>{"sync"}) fail(code::changed);
        const auto preserved=named(empty_slot.n,"sync");check_kind(preserved,true);
        if(identity(preserved)!=sync->original || (preserved.st_mode&07777)!=sync->mode)fail(code::changed);
    } else {
        sync_names=list(current_sync.n,end);
        for(const auto& name:sync_names) {
            if(!allowed_sync.contains(name))fail(code::changed);
            const auto main=mains.find(name);
            observe(current_sync.n,name,"sync/"+name,false,
                main==mains.end()?std::optional<file_identity>{}:main->second->original,
                main==mains.end()?std::optional<mode_t>{}:main->second->mode,{});
        }
        for(const auto& [name,unused]:mains)
            if(!std::binary_search(sync_names.begin(),sync_names.end(),name))fail(code::changed);
    }
    for(const auto& value:retained) {
        bound(end);require_no_granting_acl(value.descriptor.n);
        if(!stable_member(value.before,held(value.descriptor.n)) ||
            !stable_member(value.before,named(value.parent,value.leaf)))fail(code::changed);
    }
    if(list(generation.n,end)!=root_names || (current_sync.n>=0 && list(current_sync.n,end)!=sync_names))fail(code::changed);
    runtime.verify(end);owner(end);verify_barrier_complete(end);
    if ((held(data.n).st_mode&07777)!=data_original_mode || !recorded("old-paths.fenced",end))fail(code::changed);
    for(const auto& item:items)if(identity(named(data.n,item.leaf))!=item.tombstone ||
        (named(data.n,item.leaf).st_mode&07777)!=0)fail(code::changed);
    return out;
}
#endif

engram_namespace_retirement engram_namespace_retirement::resume_after_managed_runtime(
    int dp,const std::string& dl,int cp,const std::string& cl,
    const retained_managed_runtime_retirement& runtime,deadline end) {
#ifdef LATTICE_NAMESPACE_POSIX
    if(!runtime.state_ || !runtime.state_->verify)fail(code::incomplete);
    runtime.state_->verify(end);
    auto p=std::make_shared<implementation>(dp,dl,cp,cl);p->open_control(false,end);
    if(p->has_record("absent.start")) {
        p->decode_provisional(p->read_phase("absent.start",end));p->load_winner(end);
        p->decode_primary(p->read_phase("absent.primary",end));p->load_final_census(end);
        if(p->read_phase("barrier.intent",end)!=p->barrier_intent ||
            p->read_phase("intent.v1",end)!=p->intent)fail(code::changed);
    } else {
        p->decode_barrier(p->read_phase("barrier.intent",end));
        p->intent=p->read_phase("intent.v1",end);p->decode(p->intent);
        if(p->items.empty() || p->items.back().original!=p->barrier_sync_id ||
            p->items.back().mode!=p->sync_original_mode)fail(code::changed);
    }
    (void)p->inspect_managed(*runtime.state_,end);
    return engram_namespace_retirement(std::move(p));
#else
    (void)dp;(void)dl;(void)cp;(void)cl;(void)runtime;(void)end;fail(code::unavailable);
#endif
}

historical_namespace_binding engram_namespace_retirement::managed_original_binding(
    const retained_managed_runtime_retirement& runtime,bool preliminary,deadline end) const {
#ifdef LATTICE_NAMESPACE_POSIX
    if(!impl_ || !runtime.state_)fail(code::incomplete);
    (void)impl_->inspect_managed(*runtime.state_,end);
    historical_namespace_binding result;result.data=impl_->data_id;result.control=impl_->control_id;
    if(preliminary) {
        if(!impl_->provisional_root || impl_->primary_intent.empty() ||
            impl_->read_phase("absent.ready",end)!=impl_->primary_intent)fail(code::incomplete);
        picosha2::hash256(impl_->primary_intent.begin(),impl_->primary_intent.end(),result.barrier.begin(),result.barrier.end());
        reader input{impl_->primary_intent};input.at=impl_->provisional_intent.size();
        if(input.text()!="primary-members")fail(code::changed);
        const auto count=input.u64();if(count>256)fail(code::invalid);
        for(std::uint64_t n=0;n<count;++n){(void)input.text();result.originals.push_back(input.id());(void)input.u64();}
        result.originals.push_back(impl_->provisional_id);
    } else {
        picosha2::hash256(impl_->barrier_intent.begin(),impl_->barrier_intent.end(),result.barrier.begin(),result.barrier.end());
        for(const auto& value:impl_->items)result.originals.push_back(value.original);
        for(const auto& value:impl_->sync_members)result.originals.push_back(value.original);
    }
    return result;
#else
    (void)runtime;(void)preliminary;(void)end;fail(code::unavailable);
#endif
}

retained_historical_generation engram_namespace_retirement::retain_after_managed_runtime(
    const retained_managed_runtime_retirement& runtime,const historical_namespace_custody& custody,deadline end) {
#ifdef LATTICE_NAMESPACE_POSIX
    const auto binding=managed_original_binding(runtime,false,end);
    if(!custody.verify_ || custody.data_!=binding.data || custody.control_!=binding.control ||
        custody.barrier_!=binding.barrier)fail(code::incomplete);
    custody.verify_(end);
    auto owner=impl_;auto prior=runtime.state_;auto old=custody.verify_;
    auto state=std::make_shared<retained_historical_generation::implementation>();
    state->verify=[owner,prior,old](deadline until) {
        old(until);auto result=owner->inspect_managed(*prior,until);old(until);return result;
    };
    state->duplicate_generation=[owner,verify=state->verify](deadline until) {
        (void)verify(until);auto copy=duplicate(owner->generation.n);
        if(identity(held(copy.n))!=owner->generation_id)fail(code::changed);
        (void)verify(until);return std::exchange(copy.n,-1);
    };
    (void)state->verify(end);return retained_historical_generation(std::move(state));
#else
    (void)runtime;(void)custody;(void)end;fail(code::unavailable);
#endif
}
