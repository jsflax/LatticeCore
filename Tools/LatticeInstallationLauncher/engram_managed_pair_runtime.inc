// Fixed two-image runtime; included after the unchanged one-child entry.
int retained_engram_upgrade_session::run_managed_cohort_recovery(deadline end) {
#ifdef LATTICE_ENGRAM_UPGRADE_DARWIN
    if (!impl_) refused();
    auto owner = impl_;
    owner->verify(end);
    // Resolve the new runtime from the independently fixed installer stage,
    // then bind its real bytes to this launcher's embedded final signed image.
    const int runtime = owner->open_directory(owner->stage, "managed-runtime.v2", true, end);
    const auto image_record = owner->open_file(runtime, "image.json", 4096, nullptr, false, end);
    if ((owner->files[image_record].original.st_mode & 07777) != 0600 ||
        owner->files[image_record].original.st_uid != ::geteuid()) refused();
    const auto raw = read_file(owner->files[image_record].handle.value, 4096, end);
    const auto record = parse(raw);
    if (record.dump() + "\n" != raw || text(record, "format", 64) != "engram-managed-runtime-image-v2" ||
        number(record, "accountUID") != ::geteuid() || record.size() != 6 ||
        text(record, "provenanceHex", 256) != text(owner->record, "stagedProvenanceHex", 256)) refused();
    const auto image = owner->open_file(runtime, "memory", 512 * 1024 * 1024,
        &record.at("executable"), true, end);
    const auto& actual_image = owner->files[image];
    if ((actual_image.original.st_mode & 07777) != 0700 || actual_image.original.st_uid != ::geteuid() ||
        actual_image.digest != implementation::hex(engram_build_stamp::initializer_sha256.data(), 32)) refused();
    if(!engram_managed_pair_stamp::available)refused();
    const auto daemon=owner->open_file(runtime,"memory-sync",512*1024*1024,&record.at("daemonExecutable"),true,end);
    const auto& actual_daemon=owner->files[daemon];
    if((actual_daemon.original.st_mode&07777)!=0700||actual_daemon.original.st_uid!=::geteuid()||
        actual_daemon.digest!=implementation::hex(engram_managed_pair_stamp::daemon_sha256.data(),32))refused();
    const auto paired=text(record,"pairedProvenanceHex",320),legacy=text(record,"provenanceHex",256);
    if(paired.size()!=320||paired.substr(0,48)!="4c41544d5254320002000000000000000200000000000000"||
        paired.substr(48,144)!=legacy.substr(48,144)||paired.substr(192,64)!=actual_daemon.digest||
        paired.substr(256,64)!=legacy.substr(192,64))refused();
    const std::array<std::size_t,2> images{image,daemon};
    owner->verify(end);
    owner->paired_load(images,end);
    auto retired = [&]() -> retained_historical_generation {
        if(!owner->paired_head) return retire_and_retain(end);
        if(owner->role_phase_started)refused();owner->role_phase_started=true;
        const auto predecessor=owner->paired_head->at("current");
        owner->paired_verify_terminal(predecessor,images,end);
        const auto& launch=predecessor.at("expected").at("launch");
        auto proof=std::make_shared<retained_managed_runtime_retirement::state>();
        proof->historical=implementation::paired_id(launch.at("historicalParent"));
        proof->control=implementation::paired_id(launch.at("control"));
        proof->generation=implementation::paired_id(launch.at("generation"));
        proof->barrier=implementation::token(text(launch,"barrier",64));
        // The eventual generation closure retains owner. This verifier uses a
        // weak reference to avoid owner -> namespace -> verifier -> owner.
        const std::weak_ptr<implementation> weak=owner;
        proof->verify=[weak,predecessor,images](deadline until) {
            const auto live=weak.lock();if(!live)refused();
            live->paired_check_head(until);live->paired_verify_terminal(predecessor,images,until);
        };
        retained_managed_runtime_retirement runtime_retired(std::move(proof));
        // Fresh actual old-role observations remain mandatory. Saved completion
        // records never substitute for the fixed controller's kernel checks.
        for(const auto pid:owner->process_ids(end)) {
            const auto present=ordinary_launch::observed_process::inspect_files(pid,end);if(!present)continue;
            (void)owner->supported_image(owner->process_path(pid,end),end);
        }
        owner->namespace_transition.emplace(engram_namespace_retirement::resume_after_managed_runtime(
            owner->home,".claude",owner->stage,"namespace",runtime_retired,end));
        if(owner->namespace_transition->has_provisional_root()) {
            owner->namespace_binding=owner->namespace_transition->managed_original_binding(runtime_retired,true,end);
            owner->retire_phase(true,end);
        }
        owner->namespace_binding=owner->namespace_transition->managed_original_binding(runtime_retired,false,end);
        owner->retire_phase(false,end);
        const auto& binding=*owner->namespace_binding;
        historical_namespace_custody old(binding.data,binding.control,binding.barrier,
            [owner](deadline until){owner->verify_retired(until);});
        return owner->namespace_transition->retain_after_managed_runtime(runtime_retired,old,end);
    }();
    retired.drain_oversized_wals(end);
    const auto observed = retired.before_launch(end);
    fd generation(retired.duplicate_before_launch(end));
    const int control = owner->open_directory(owner->stage, "namespace", true, end);
    if (file_identity{std::uint64_t(metadata(control).st_dev),std::uint64_t(metadata(control).st_ino)} != observed.control)
        refused();
    // Cleanup is permitted only for the older, fully proved retired prior.
    // Its exact record remains durable cleanup debt until every known resource
    // is actually removed and flushed; unresolved current is never evicted.
    if(owner->paired_head && !owner->paired_head->at("prior").is_null()) {
        const auto prior=owner->paired_head->at("prior");owner->paired_verify_terminal(prior,images,end);
        auto next=*owner->paired_head;next["cleanup"]=prior;next["prior"]=nullptr;
        owner->paired_publish(std::move(next),end);owner->paired_cleanup_prior(images,end);
    }
    const auto previous_sequence=owner->paired_head?number(owner->paired_head->at("current"),"sequence"):0;
    if(previous_sequence==std::numeric_limits<std::uint64_t>::max())refused();
    const auto sequence=previous_sequence+1;const auto cohort_leaf=implementation::paired_leaf(sequence);
    const auto target_intent=owner->paired_target_plan(owner,observed,sequence,end);
    auto cohort = created_launch_cohort::create_before_child(owner->stage, cohort_leaf);
    cohort.retain_managed_custody(retired.impl_);
    fd cohort_root(cohort.duplicate_accepting_root());
    const auto cohort_stat = metadata(cohort_root.value);
    std::array<std::uint8_t,16> nonce{};
    if (::getentropy(nonce.data(), nonce.size()) ||
        std::all_of(nonce.begin(), nonce.end(), [](auto byte) { return byte == 0; })) refused();
    auto identity_json = [](file_identity value) {
        return json{{"device",value.device},{"inode",value.inode}};
    };
    auto process_json = [](const ordinary_launch::process_identity& value) {
        return json{{"pid",value.pid},{"parent",value.parent},{"birthMajor",value.birth_major},{"birthMinor",value.birth_minor}};
    };
    json members = json::array();
    for (const auto& value : observed.members) members.push_back({{"name",value.relative_name},
        {"identity",identity_json(value.identity)},{"refusal",identity_json(value.refusal)},
        {"mode",value.mode},{"directory",value.directory}});
    json refusals=json::array();
    for(const auto& value:observed.old_refusals)refusals.push_back({{"name",value.relative_name},
        {"identity",identity_json(value.identity)},{"refusal",identity_json(value.refusal)},
        {"mode",value.mode},{"directory",value.directory}});
    int endpoints[2]{-1,-1};
    if(::socketpair(AF_UNIX,SOCK_STREAM,0,endpoints))refused();
    std::array<fd,2> peer_ends{fd(endpoints[0]),fd(endpoints[1])};
    for(const auto& endpoint:peer_ends)if(::fcntl(endpoint.value,F_SETFD,FD_CLOEXEC))refused();
    std::array<std::uint8_t,16> peer_nonce{};
    if(::getentropy(peer_nonce.data(),peer_nonce.size())||
        std::all_of(peer_nonce.begin(),peer_nonce.end(),[](auto byte){return byte==0;}))refused();
    json peer_observations=json::array();
    for(const auto& endpoint:peer_ends) {const auto actual=metadata(endpoint.value);
        if(!S_ISSOCK(actual.st_mode))refused();
        peer_observations.push_back(identity_json({std::uint64_t(actual.st_dev),std::uint64_t(actual.st_ino)}));}
    json offer{{"format",target_intent.is_null()?"engram-managed-history-launch-v3":"engram-managed-history-launch-v4"},{"phase",1},
        {"cohortLeaf",cohort_leaf},{"launchSequence",sequence},{"oldRefusals",std::move(refusals)},
        {"nonce",implementation::hex(nonce.data(),nonce.size())},
        {"supervisor",process_json(ordinary_launch::current_process())},
        {"historicalParent",identity_json(observed.historical_parent)},
        {"control",identity_json(observed.control)},{"generation",identity_json(observed.generation)},
        {"cohort",identity_json({std::uint64_t(cohort_stat.st_dev),std::uint64_t(cohort_stat.st_ino)})},
        {"barrier",implementation::hex(observed.barrier.data(),observed.barrier.size())},
        {"members",std::move(members)},{"runtimeImage",record},
        {"peerLink",{{"nonce",implementation::hex(peer_nonce.data(),peer_nonce.size())},{"endpoints",peer_observations}}}};
    if(!target_intent.is_null())offer["unpublishedGroupTargets"]=target_intent;
    auto frame = [&](const json& value) {
        const auto bytes = value.dump();
        if (bytes.empty() || bytes.size() > ordinary_launch::maximum_frame_bytes) refused();
        return ordinary_launch::frame(bytes.begin(), bytes.end());
    };
    std::array<ordinary_launch::launch_specification,2> specifications;
    fd daemon_input(::open("/dev/null",O_RDONLY|O_CLOEXEC));if(daemon_input.value<0)refused();
    for(std::size_t index=0;index<2;++index) {
        auto& specification=specifications[index];
        specification.executable=owner->home_path+"/.engram-installation/upgrade-bootstrap.v1/managed-runtime.v2/"+
            (index==0?"memory":"memory-sync");
        specification.working_directory=owner->home_path+"/.engram-installation/upgrade-bootstrap.v1/namespace/generation";
        specification.arguments={index==0?"--lattice-managed-pair-history-v1":"--lattice-managed-daemon-history-v1"};
        specification.environment={"PATH=/usr/bin:/bin"};
        specification.inherited_directories={generation.value,cohort_root.value,owner->stage,runtime,owner->data,control};
        specification.inherited_channels={peer_ends[index].value}; // legacy fd10; v4 target dir moves this real peer to11
        if(index==1){specification.input=daemon_input.value;specification.output=STDERR_FILENO;}
    }
    std::array<std::optional<ordinary_launch::observed_process>,2> observers;
    std::array<std::optional<ordinary_launch::observed_incarnation>,2> incarnations;
    std::array<std::size_t,2> indices{};
    std::array<json,2> offers;
    json runtime_expected;
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
    json constructor_fixture_observation; // DATA; never retirement/admission authority.
#endif
    const auto cohort_snapshot=[&](deadline check_end){return owner->paired_snapshot(cohort_root.value,check_end);};
    json journal=owner->paired_head?*owner->paired_head:json{
        {"format","engram-managed-launch-journal-v3"},{"accountUID",::geteuid()},
        {"bootstrapSHA256",picosha2::hash256_hex_string(owner->record.dump()+"\n")},
        {"revision",0},{"current",nullptr},{"prior",nullptr},{"cleanup",nullptr}};
    journal["prior"]=journal.at("current");
    journal["current"]={{"sequence",sequence},{"leaf",cohort_leaf},
        {"directory",identity_json({std::uint64_t(cohort_stat.st_dev),std::uint64_t(cohort_stat.st_ino)})},
        {"expected",nullptr},{"complete",nullptr}};
    if(!target_intent.is_null())journal["current"]["targets"]=target_intent;
    owner->paired_publish(std::move(journal),end);
    // The exact intent is durable BEFORE mkdir/open/fsync of any target. Each
    // actual native result and resource remains in the same original ledger.
    if(!target_intent.is_null()) {
        const auto targets=owner->paired_create_targets(end);
        offer["unpublishedGroupTargets"]=targets;
        auto prepared=*owner->paired_head;prepared["current"]["targets"]=targets;
        owner->paired_publish(std::move(prepared),end);
        for(auto& specification:specifications)specification.inherited_directories.push_back(owner->unpublished_targets->root.value);
        // Genuine dual-runtime primitive: old dirs4...9, target10, peer11.
    }
    try {
        owner->verify(end);(void)retired.before_launch(end);
        if(!target_intent.is_null())owner->paired_verify_targets(end);
        // Both actual processes exist in this one retained cohort before either
        // phase-1 offer. Neither child can yet open the historical namespace.
        for(std::size_t index=0;index<2;++index)indices[index]=cohort.launch(specifications[index]);
        // Each actual owned child now holds only its intended inherited end.
        // Drop parent aliases so peer shutdown is not hidden by this controller.
        for(auto& endpoint:peer_ends)endpoint=fd();
        offer["children"]=json::array({process_json(cohort.child_identity(indices[0])),process_json(cohort.child_identity(indices[1]))});
        for(std::size_t index=0;index<2;++index) {
            offers[index]=offer;offers[index]["child"]=process_json(cohort.child_identity(indices[index]));
            cohort.send(indices[index],frame(offers[index]),end);
        }
        json expected_incarnations=json::array();
        for(std::size_t index=0;index<2;++index) {
            auto reply=offers[index];reply["phase"]=2;
            if(cohort.receive(indices[index],end)!=frame(reply))refused();
            observers[index].emplace(ordinary_launch::observed_process::attach(
                cohort.child_identity(indices[index]).pid,owner->files[images[index]].handle.value,end));
            incarnations[index]=observers[index]->incarnation(end);
            const auto child=cohort.child_identity(indices[index]);const auto& incarnation=*incarnations[index];
            if(incarnation.pid!=child.pid||child.parent!=owner->owner)refused();
            expected_incarnations.push_back({{"pid",incarnation.pid},
                {"auditToken",implementation::hex(incarnation.audit_token.data(),incarnation.audit_token.size())},
                {"imageDevice",incarnation.executable_device},{"imageInode",incarnation.executable_inode},
                {"imageSHA256",owner->files[images[index]].digest}});
        }
        owner->verify(end);(void)retired.before_launch(end);
        if(!target_intent.is_null())owner->paired_verify_targets(end);
        // Exactly one irreversible launch closure follows BOTH real phase-2
        // receivers and BOTH kernel observers. No child gets phase3 earlier.
        cohort.close_launch_admission(end);
        runtime_expected={{"format","engram-managed-runtime-expectation-v2"},
            {"accountUID",::geteuid()},{"bootstrapSHA256",picosha2::hash256_hex_string(owner->record.dump()+"\n")},
            {"launch",offers[0]},{"closedGateSnapshot",cohort_snapshot(end)},{"incarnations",expected_incarnations}};
        auto expected_head=*owner->paired_head;expected_head["current"]["expected"]=runtime_expected;
        owner->paired_publish(std::move(expected_head),end);
        owner->verify(end);(void)retired.before_launch(end);
        if(!target_intent.is_null())owner->paired_verify_targets(end);
        for(std::size_t index=0;index<2;++index)
            if(observers[index]->incarnation(end).audit_token!=incarnations[index]->audit_token)refused();
        for(std::size_t index=0;index<2;++index) {
            auto grant=offers[index];grant["phase"]=3;cohort.send_closed_child(indices[index],frame(grant),end);
        }
        for(std::size_t index=0;index<2;++index) {
            auto reply=offers[index];reply["phase"]=4;
            if(cohort.receive_closed_child(indices[index],end)!=frame(reply))refused();
        }
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
        // Private fixture images still passed every normal account/install,
        // old-writer retirement, image/incarnation and closed paired exchange.
        // No runtime flag/path grants a target or changes those authorities.
        if(target_intent.is_null()||!owner->unpublished_targets||owner->unpublished_targets->count!=1)refused();
        auto begin=offers[1];begin["phase"]=5;begin["fixture"]="lattice-target-constructor-reclamation-v1";
        cohort.send_closed_child(indices[1],frame(begin),end);
        const auto reply_bytes=cohort.receive_closed_child(indices[1],end);
        if(reply_bytes.size()>ordinary_launch::maximum_frame_bytes)refused();
        const auto received=parse(std::string(reply_bytes.begin(),reply_bytes.end()));
        auto expected=offers[1];expected["phase"]=6;expected["fixture"]="lattice-target-constructor-reclamation-v1";
        if(!received.is_object()||!received.contains("observation"))refused();
        constructor_fixture_observation=received.at("observation");
        auto stripped=received;stripped.erase("observation");if(stripped!=expected)refused();
        const auto& observed=constructor_fixture_observation;
        const auto& member=owner->unpublished_targets->members[0];
        if(text(observed,"format",64)!="lattice-target-constructor-reclamation-observation-v1"||
           number(observed,"memberDevice")!=member.identity.device||number(observed,"memberInode")!=member.identity.inode||
           number(observed,"mainDevice")!=member.identity.device||!number(observed,"mainInode")||
           number(observed,"linksBefore")!=1||number(observed,"linksAfter")!=0||
           number(observed,"entriesBefore")!=1||number(observed,"entriesAfter")!=0||
           number(observed,"sqliteOpenRC")!=SQLITE_OK||number(observed,"sqliteCloseRC")!=SQLITE_OK||
           number(observed,"configurations")!=18)refused();
        const auto checked_call=[&](const char* key,int rc,int error){
            const auto& call=observed.at(key);
            if(!call.is_object()||call.size()!=5||call.at("attempted")!=true||call.at("entered")!=true||
               call.at("returned")!=true||call.at("rc")!=rc||call.at("errno")!=error)refused();
        };
        checked_call("unlink",0,0);checked_call("absent",-1,ENOENT);
        checked_call("directorySync",0,0);checked_call("descriptorClose",0,0);
        for(const auto* key:{"nativeFilesJoined","constructorLeaveJoined","cleanupLeaveJoined"})
            if(observed.at(key)!=true)refused();
        for(const auto* key:{"schemaAttempted","catalogAttempted","historyAttempted","importAttempted","projectionAttempted"})
            if(observed.at(key)!=false)refused();
        const json expected_registration{{"attempted",true},{"returned",true},{"rc",SQLITE_OK},
            {"callbacks",0},{"registered",false},{"retired",true},{"retiring",false},{"uncertain",false}};
        if(observed.at("registration")!=expected_registration)refused();
        // Parent independently observes the exact retained member empty; the
        // child's first syscall observations do not stand in for this census.
        owner->verify(end);owner->paired_verify_targets(end);
        for(std::size_t index=0;index<2;++index)
            if(observers[index]->incarnation(end).audit_token!=incarnations[index]->audit_token)refused();
        for(std::size_t index=0;index<2;++index){auto complete=offers[index];complete["phase"]=7;
            complete["fixture"]="lattice-target-constructor-reclamation-v1";
            cohort.send_closed_child(indices[index],frame(complete),end);}
        std::array<bool,2> natural_exit{false,false};
        while(!natural_exit[0]||!natural_exit[1]) {
            bound(end); // Same original 30s admission deadline, no refreshed work budget.
            for(std::size_t index=0;index<2;++index)if(!natural_exit[index])
                natural_exit[index]=cohort.observe_terminal(indices[index],end).has_value();
            if(!natural_exit[0]||!natural_exit[1])std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
#endif
        // The actual runtime may be long-lived. Once either process ends, the
        // same owner closes the whole pair, never authorizing a concurrent
        // replacement while the sibling still owns stores or descriptors.
        bool terminal_seen=false;
        while(!terminal_seen) {
            const auto observe_end=std::chrono::steady_clock::now()+std::chrono::seconds(1);
            for(auto index:indices)if(cohort.observe_terminal(index,observe_end))terminal_seen=true;
            if(!terminal_seen)std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        const auto retirement_end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        const auto completed=cohort.close_and_join(retirement_end);
        if(completed.children.size()!=2)refused();
        json terminals=json::array();int result=0;
        for(std::size_t index=0;index<2;++index) {
            const auto& terminal=completed.children[index];const auto& expected=*incarnations[index];
            if(terminal.child!=cohort.child_identity(indices[index]))refused();
            const auto actual_exit=observers[index]->wait_for_exit(retirement_end);
            if(actual_exit.pid!=expected.pid||actual_exit.audit_token!=expected.audit_token||
                actual_exit.executable_device!=expected.executable_device||actual_exit.executable_inode!=expected.executable_inode)refused();
            terminals.push_back({{"child",process_json(terminal.child)},{"waitStatus",terminal.wait_status}});
            const auto code=WIFEXITED(terminal.wait_status)?WEXITSTATUS(terminal.wait_status):
                (WIFSIGNALED(terminal.wait_status)?128+WTERMSIG(terminal.wait_status):74);
            if(!result&&code)result=code;
        }
        owner->verify(retirement_end);
        if(!target_intent.is_null())owner->paired_verify_targets(retirement_end);
        const json complete{{"format","engram-managed-runtime-retirement-v2"},
            {"expectedSHA256",picosha2::hash256_hex_string(runtime_expected.dump()+"\n")},
            {"directory",identity_json(completed.directory)},{"gate",identity_json(completed.gate)},
            {"revision",completed.revision},{"terminalChildren",terminals},{"terminalSnapshot",cohort_snapshot(retirement_end)},
            {"actualKernelOutcome","both-note-exit-without-observed-fork-or-exec"},
            {"observationInterval","both-phase2-before-open-through-joint-owned-join"},{"restartAdmissionGranted",false}};
        auto complete_head=*owner->paired_head;complete_head["current"]["complete"]=complete;
        owner->paired_publish(std::move(complete_head),retirement_end);owner->verify(retirement_end);
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
        if(result)refused();
        const auto evidence=constructor_fixture_observation.dump();
        std::fputs("constructor-reclamation-qualified ",stdout);
        std::fputs(evidence.c_str(),stdout);std::fputc('\n',stdout);std::fflush(stdout);
#endif
        return result; // Preserve both real failure statuses in the receipt.
    } catch (...) {
        const auto primary=std::current_exception();
        try{(void)cohort.close_and_join(std::chrono::steady_clock::now()+std::chrono::seconds(5));}catch(...){}
        std::rethrow_exception(primary); // incomplete pair head remains debt
    }
#else
    (void)end;refused();
#endif
}
