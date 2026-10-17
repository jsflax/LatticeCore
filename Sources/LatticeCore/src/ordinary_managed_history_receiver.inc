// Included after the complete ordinary_open_context implementation.
namespace ordinary_installation {
managed_history_startup managed_history_receiver::receive(ordinary_launch::deadline end) {
    return receive_impl(end,false,false);
}
managed_history_startup managed_history_receiver::receive_paired(ordinary_launch::deadline end) {
    return receive_impl(end,false,true);
}
managed_history_startup managed_history_receiver::receive_daemon(ordinary_launch::deadline end) {
    return receive_impl(end,true,true);
}
managed_history_startup managed_history_receiver::receive_impl(ordinary_launch::deadline end,bool daemon_entry,bool paired_entry) {
#if defined(LATTICE_ORDINARY_NORMAL_POSIX) && defined(__APPLE__)
    auto& gate=policy();auto prior=policy_state::phase::untouched;
    if(!gate.state.compare_exchange_strong(prior,policy_state::phase::installing,std::memory_order_acq_rel))
        normal_fail("managed history receiver must precede all native opens");
    try {
        // Own these immediately, including every failing initial acquisition.
        fd_owner generation(4),cohort(5),stage(6),runtime(7),historical(8),control(9),target_root,peer_socket;
        // Own fd10 before inspection. Legacy paired peer is10; v4 carries the
        // real target directory10 and peer11. Never adopt an absent legacy11.
        if(paired_entry){fd_owner extra(10);struct stat actual{};
            if(::fstat(extra.value,&actual))normal_fail("managed pair inherited descriptor unavailable");
            if(S_ISDIR(actual.st_mode)){target_root=std::move(extra);peer_socket=fd_owner(11);}
            else if(S_ISSOCK(actual.st_mode))peer_socket=std::move(extra);
            else normal_fail("managed pair inherited descriptor has wrong kind");}

        ordinary_launch::inherited_channel channel(3);
        auto state=std::make_shared<ordinary_managed_child_admission::implementation>();
        state->generation=std::move(generation);state->cohort=std::move(cohort);state->stage=std::move(stage);
        state->runtime=std::move(runtime);state->historical=std::move(historical);state->control=std::move(control);
        state->peer_socket=std::move(peer_socket);state->target_root=std::move(target_root);state->daemon_entry=daemon_entry;
        if(state->target_root.value>=0&&::fcntl(state->target_root.value,F_SETFD,FD_CLOEXEC))
            normal_fail("managed target descriptor confinement failed");
        state->channel=std::make_unique<ordinary_launch::inherited_channel>(std::move(channel));
        for(int descriptor:{4,5,6,7,8,9})if(::fcntl(descriptor,F_SETFD,FD_CLOEXEC))
            normal_fail("managed history inherited descriptor confinement failed");
        // The independent account namespace is never supplied by a frame, URL,
        // environment variable or arbitrary application configuration.
        struct passwd account{},*found=nullptr;std::array<char,65536> account_buffer{};
        if(::getpwuid_r(::geteuid(),&account,account_buffer.data(),account_buffer.size(),&found) ||
            !found || account.pw_uid!=::geteuid() || !account.pw_dir || !*account.pw_dir)
            normal_fail("managed history OS account unavailable");
        state->home_path=account.pw_dir;
        if(state->home_path.size()>4096 || state->home_path.front()!='/' || state->home_path.back()=='/')
            normal_fail("managed history OS account home invalid");
        state->stage_path=state->home_path+"/.engram-installation/upgrade-bootstrap.v1";
        state->generation_path=state->stage_path+"/namespace/generation";
        auto fixed_stage=absolute_directory(state->stage_path);
        state->stage_identity=identity(state->stage.value,true);
        state->runtime_identity=identity(state->runtime.value,true);
        if(identity(fixed_stage.value,true)!=state->stage_identity)
            normal_fail("managed history inherited stage is not installed stage");
        state->bootstrap=managed_read_record(state->stage.value,"bootstrap.json",131072,end);
        auto checksum=managed_read_record(state->stage.value,"bootstrap.sha256",65,end);
        const auto bootstrap_hash=picosha2::bytes_to_hex_string(state->bootstrap.content.begin(),state->bootstrap.content.end());
        if(std::string(checksum.bytes.begin(),checksum.bytes.end())!=bootstrap_hash+"\n")
            normal_fail("managed history bootstrap pair incomplete");
        const auto bootstrap=managed_parse(state->bootstrap.bytes);
        if(bootstrap.dump()+"\n"!=std::string(state->bootstrap.bytes.begin(),state->bootstrap.bytes.end()) ||
            managed_text(bootstrap.at("format"),64)!="engram-upgrade-bootstrap-v1" ||
            managed_unsigned(bootstrap.at("accountUID"))!=::geteuid())
            normal_fail("managed history independent bootstrap invalid");
        for(const char* key:{"legacyBuildProvenanceObserved","coveredCachedAliases","wholeCohortRetired","historicalAdoptionAuthorized"})
            if(bootstrap.at(key)!=false)normal_fail("managed history bootstrap cannot grant retirement");
        state->image=managed_read_record(state->runtime.value,"image.json",4096,end);
        const auto image=managed_parse(state->image.bytes);
        const auto image_format=managed_text(image.at("format"),64);
        state->paired_runtime=image_format=="engram-managed-runtime-image-v2";
        state->runtime_leaf=state->paired_runtime?"managed-runtime.v2":"managed-runtime.v1";
        if(image.dump()+"\n"!=std::string(state->image.bytes.begin(),state->image.bytes.end()) ||
            (state->paired_runtime?image.size()!=6:(image_format!="engram-managed-runtime-image-v1"||image.size()!=4)) ||
            (paired_entry!=state->paired_runtime) || managed_unsigned(image.at("accountUID"))!=::geteuid())
            normal_fail("managed history installed runtime record invalid");
        named(state->stage.value,state->runtime_leaf.c_str(),state->runtime.value,state->runtime_identity,true);
        const auto provenance=managed_hex(managed_text(bootstrap.at("stagedProvenanceHex"),256),128);
        const std::array<uint8_t,24> prefix{'L','A','T','P','K','G','1',0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0};
        if(!std::equal(prefix.begin(),prefix.end(),provenance.begin()) ||
            managed_text(image.at("provenanceHex"),256)!=managed_text(bootstrap.at("stagedProvenanceHex"),256))
            normal_fail("managed history installed package disagreement");
        auto executable=[&](const managed_json& fact,const std::string& path,size_t hash_offset){
            executable_fact result;result.path=path;
            result.identity={managed_unsigned(fact.at("device")),managed_unsigned(fact.at("inode"))};
            const auto bytes=managed_hex(managed_text(fact.at("sha256"),64),32);
            if(!result.identity.inode || !std::equal(bytes.begin(),bytes.end(),provenance.begin()+hash_offset))
                normal_fail("managed history installed image digest disagreement");
            std::copy(bytes.begin(),bytes.end(),result.content.begin());return result;
        };
        const auto runtime_path=state->stage_path+"/"+state->runtime_leaf;
        state->memory=executable(image.at("executable"),runtime_path+"/memory",64);
        if(state->paired_runtime) {
            const auto paired=managed_hex(managed_text(image.at("pairedProvenanceHex"),320),160);
            const std::array<uint8_t,24> paired_prefix{'L','A','T','M','R','T','2',0,2,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0};
            if(!std::equal(paired_prefix.begin(),paired_prefix.end(),paired.begin()) ||
                !std::equal(provenance.begin()+24,provenance.begin()+96,paired.begin()+24) ||
                !std::equal(provenance.begin()+96,provenance.end(),paired.begin()+128))
                normal_fail("managed paired product provenance disagreement");
            executable_fact daemon;daemon.path=runtime_path+"/memory-sync";
            daemon.identity={managed_unsigned(image.at("daemonExecutable").at("device")),
                managed_unsigned(image.at("daemonExecutable").at("inode"))};
            const auto daemon_hash=managed_hex(managed_text(image.at("daemonExecutable").at("sha256"),64),32);
            if(!daemon.identity.inode || !std::equal(daemon_hash.begin(),daemon_hash.end(),paired.begin()+96))
                normal_fail("managed daemon product digest disagreement");
            std::copy(daemon_hash.begin(),daemon_hash.end(),daemon.content.begin());
            state->peer_image=daemon_entry?state->memory:daemon;
            if(daemon_entry)state->memory=std::move(daemon);
        }
        state->launcher=executable(bootstrap.at("stagedController"),state->stage_path+"/controller/memory-installation-launcher",96);
        state->process=ordinary_launch::current_process();state->supervisor=ordinary_launch::current_parent_process();
        loaded_image(state->process.pid,state->memory,end);loaded_image(state->supervisor.pid,state->launcher,end);
        state->offer=managed_parse(state->channel->receive(end));const auto& offer=state->offer;
        if(!offer.is_object())normal_fail("managed history launch is not an object");
        const auto version=managed_text(offer.at("format"),64);
        const bool targets_v4=version=="engram-managed-history-launch-v4";
        const bool paired_v3=version=="engram-managed-history-launch-v3"||targets_v4;
        const bool restart_v2=version=="engram-managed-history-launch-v2"||paired_v3;
        if(paired_v3!=state->paired_runtime || targets_v4!=(state->target_root.value>=0) ||
            (paired_v3?offer.size()!=(targets_v4?18u:17u):(restart_v2?offer.size()!=15:(version!="engram-managed-history-launch-v1"||offer.size()!=12))) ||
            managed_unsigned(offer.at("phase"))!=1 || managed_process(offer.at("child"))!=state->process ||
            managed_process(offer.at("supervisor"))!=state->supervisor || offer.at("runtimeImage")!=image)
            normal_fail("managed history actual launch exchange mismatch");
        if(paired_v3) {
            const auto& children=offer.at("children");
            if(!children.is_array()||children.size()!=2)normal_fail("managed pair child inventory invalid");
            for(size_t index=0;index<2;++index) {
                state->runtime_children[index]=managed_process(children[index]);
                if(state->runtime_children[index].parent!=state->supervisor.pid)
                    normal_fail("managed pair child parent mismatch");
            }
            const size_t own=daemon_entry?1:0;
            if(state->runtime_children[own]!=state->process || state->runtime_children[0]==state->runtime_children[1])
                normal_fail("managed pair actual image/child position mismatch");
            state->peer=state->runtime_children[1-own];
            loaded_image(state->peer.pid,state->peer_image,end);
            const auto& link=offer.at("peerLink");
            if(!link.is_object()||link.size()!=2||!link.at("endpoints").is_array()||link.at("endpoints").size()!=2)
                normal_fail("managed pair transport observation invalid");
            const auto nonce=managed_hex(managed_text(link.at("nonce"),32),16);
            if(std::all_of(nonce.begin(),nonce.end(),[](auto byte){return byte==0;}))
                normal_fail("managed pair transport nonce absent");
            std::copy(nonce.begin(),nonce.end(),state->peer_nonce.begin());
            state->prepare_peer(managed_identity(link.at("endpoints")[own]));
        }
        if(restart_v2) {
            const auto sequence=managed_unsigned(offer.at("launchSequence"));
            if(!sequence)normal_fail("managed history launch sequence invalid");
            state->cohort_leaf="managed-cohort."+std::to_string(sequence);
            if(managed_text(offer.at("cohortLeaf"),64)!=state->cohort_leaf)
                normal_fail("managed history cohort name mismatch");
            const auto& refusals=offer.at("oldRefusals");
            if(!refusals.is_array()||refusals.size()<2||refusals.size()>5)
                normal_fail("managed history original fence inventory invalid");
            std::set<std::string> old_names;std::set<std::pair<uint64_t,uint64_t>> old_ids;
            for(const auto& member:refusals) {
                if(!member.is_object()||member.size()!=5||!member.at("directory").is_boolean())
                    normal_fail("managed history original fence invalid");
                const auto name=managed_text(member.at("name"),160);
                if((name!="memory.sqlite"&&name!="sync"&&name!="memory.sqlite-wal"&&
                    name!="memory.sqlite-shm"&&name!="memory.sqlite-journal")||!old_names.insert(name).second||
                    member.at("directory").get<bool>()!=(name=="sync"))normal_fail("managed history original fence name invalid");
                const auto old=managed_identity(member.at("refusal")),original=managed_identity(member.at("identity"));
                const auto mode=managed_unsigned(member.at("mode"));
                if(!old.inode||!original.inode||old==original||old.device!=original.device||mode>0777||(mode&0022)||
                    !old_ids.emplace(old.device,old.inode).second)normal_fail("managed history original fence identity invalid");
            }
            if(!old_names.contains("memory.sqlite")||!old_names.contains("sync"))
                normal_fail("managed history original main fences missing");
        }
        const auto nonce=managed_hex(managed_text(offer.at("nonce"),32),16);
        if(std::all_of(nonce.begin(),nonce.end(),[](auto value){return value==0;}))normal_fail("managed history empty launch nonce");
        (void)managed_hex(managed_text(offer.at("barrier"),64),32);
        const auto& raw_members=offer.at("members");
        if(!raw_members.is_array() || raw_members.empty() || raw_members.size()>256)
            normal_fail("managed history store catalog bound exceeded");
        std::set<std::string> member_names;std::set<std::pair<uint64_t,uint64_t>> member_ids;
        file_identity sync_identity{};
        for(const auto& member:raw_members){
            if(!member.is_object() || member.size()!=5 || !member.at("directory").is_boolean())
                normal_fail("managed history catalog member invalid");
            const auto relative=managed_text(member.at("name"),160);
            if(relative=="." || relative==".." || relative.front()=='/' ||
                relative.find("..")!=std::string::npos || relative.find('\\')!=std::string::npos ||
                !member_names.insert(relative).second)normal_fail("managed history catalog member name invalid");
            const auto actual=managed_identity(member.at("identity"));
            if(!actual.inode || !member_ids.emplace(actual.device,actual.inode).second)
                normal_fail("managed history catalog identity repeated");
            const auto mode=managed_unsigned(member.at("mode"));
            if(mode>0777 || (mode&0022))normal_fail("managed history catalog mode invalid");
            const bool directory=member.at("directory").get<bool>();
            if(directory){if(relative!="sync")normal_fail("managed history unregistered directory");sync_identity=actual;}
            const bool nested=relative.starts_with("sync/");
            if(relative.find('/')!=std::string::npos && (!nested || relative.find('/',5)!=std::string::npos))
                normal_fail("managed history catalog traversal refused");
            if(!nested && relative!="sync" && relative!="memory.sqlite" && relative!="memory.sqlite-wal" &&
                relative!="memory.sqlite-shm" && relative!="memory.sqlite-journal")
                normal_fail("managed history top-level member unsupported");
            if(!nested && !restart_v2 && !managed_identity(member.at("refusal")).inode)
                normal_fail("managed history original path fence missing");
            if(nested && managed_identity(member.at("refusal"))!=file_identity{})
                normal_fail("managed history nested refusal fact invalid");
        }
        if(restart_v2)for(const auto& member:raw_members) {
            const auto relative=managed_text(member.at("name"),160);if(relative.find('/')!=std::string::npos)continue;
            const auto& refusals=offer.at("oldRefusals");
            const auto old=std::find_if(refusals.begin(),refusals.end(),[&](const auto& value){return value.at("name")==relative;});
            if(old==refusals.end()) {
                if(relative=="memory.sqlite"||relative=="sync"||managed_identity(member.at("refusal"))!=file_identity{})
                    normal_fail("managed history new main fence refused");
            } else if(member.at("refusal")!=old->at("refusal")||
                ((relative=="memory.sqlite"||relative=="sync")&&member.at("identity")!=old->at("identity")))
                normal_fail("managed history main continuity mismatch");
        }
        // Only exact actual store members become open views. Sidecars and
        // revoked group archives remain retained facts, not writable stores.
        state->stores.reserve(member_names.size());
        auto add_store=[&](const std::string& relative,const managed_json& member){
            file_identity parent=managed_identity(offer.at("generation"));
            if(relative.starts_with("sync/")){if(!sync_identity.inode)normal_fail("managed history sync root absent");parent=sync_identity;}
            state->stores.push_back({relative,state->generation_path+"/"+relative,parent,managed_identity(member.at("identity")),fd_owner{}});
        };
        const auto primary=std::find_if(raw_members.begin(),raw_members.end(),[](const auto& item){return item.at("name")=="memory.sqlite";});
        if(primary==raw_members.end() || primary->at("directory")!=false)normal_fail("managed history primary missing");
        add_store("memory.sqlite",*primary);
        for(const auto& member:raw_members){const auto relative=managed_text(member.at("name"),160);
            if(relative=="sync/memory-synced.sqlite" || (relative.starts_with("sync/") && managed_group_leaf(relative.substr(5)))){
                if(member.at("directory")!=false)normal_fail("managed history store is directory");add_store(relative,member);}}
        if(targets_v4) {
            const auto& targets=offer.at("unpublishedGroupTargets");
            if(!targets.is_object()||targets.size()!=8||managed_text(targets.at("format"),64)!="engram-unpublished-group-targets-v1"||
                managed_text(targets.at("state"),32)!="retained-empty"||managed_identity(targets.at("parent"))!=state->stage_identity||
                managed_identity(targets.at("generation"))!=managed_identity(offer.at("generation"))||targets.at("barrier")!=offer.at("barrier"))
                normal_fail("managed target role does not match actual historical launch");
            state->target_root_leaf="unpublished-group-targets."+std::to_string(managed_unsigned(offer.at("launchSequence")));
            if(managed_text(targets.at("leaf"),64)!=state->target_root_leaf)normal_fail("managed target root name differs");
            state->target_root_identity=managed_identity(targets.at("root"));
            named(state->stage.value,state->target_root_leaf.c_str(),state->target_root.value,state->target_root_identity,true);
            const auto& members=targets.at("members");
            if(!members.is_array()||members.empty()||members.size()>state->targets.size())normal_fail("managed target member count refused");
            std::set<std::string> old_names,target_names;
            for(const auto& member:members){
                if(!member.is_object()||member.size()!=4)normal_fail("managed target member shape differs");
                const auto old_name=managed_text(member.at("member"),64);
                const auto old=std::find_if(state->stores.begin(),state->stores.end(),[&](const auto& item){return item.relative==old_name;});
                if(old==state->stores.end()||!old_name.starts_with("sync/")||!managed_group_leaf(old_name.substr(5))||
                    managed_identity(member.at("oldMain"))!=old->main||!old_names.insert(old_name).second)
                    normal_fail("managed target has no exact old GROUP member");
                auto leaf=old_name.substr(5,42);for(auto& c:leaf)if(c>='A'&&c<='F')c+=('a'-'A');
                if(managed_text(member.at("leaf"),64)!=leaf||!target_names.insert(leaf).second)normal_fail("managed target member name differs");
                auto& held=state->targets[state->target_count++];held.old_member=old_name;held.old_main=old->main;held.leaf=leaf;
                held.directory_identity=managed_identity(member.at("directory"));
                held.directory=fd_owner(::openat(state->target_root.value,leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
                named(state->target_root.value,leaf.c_str(),held.directory.value,held.directory_identity,true);
            }
            std::size_t actual_groups=0;for(const auto& item:state->stores)if(item.relative.starts_with("sync/")&&managed_group_leaf(item.relative.substr(5)))++actual_groups;
            if(actual_groups!=state->target_count)normal_fail("managed target role omits an actual GROUP member");
        }
        // This check leaves no main/sync file descriptor retained before the
        // parent's final old-writer census. It cannot mistake our own new
        // child for an unqualified historical holder.
        state->verify(end,false);
        auto frame=[](const managed_json& value){const auto raw=value.dump();
            if(raw.size()>ordinary_launch::maximum_frame_bytes)normal_fail("managed history reply bound exceeded");
            return ordinary_launch::frame(raw.begin(),raw.end());};
        auto response=offer;response["phase"]=2;state->channel->send(frame(response),end);
        response["phase"]=3;
        if(state->channel->receive(end)!=frame(response))normal_fail("managed history grant exchange mismatch");
        state->verify(end,true);
        if(state->paired_runtime)state->exchange_peer(end);
        for(auto& value:state->stores){
            fd_owner parent;int fd=state->generation.value;auto leaf=value.relative;
            if(leaf.starts_with("sync/")){parent=fd_owner(::openat(fd,"sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));fd=parent.value;leaf.erase(0,5);}
            value.main_descriptor=fd_owner(::openat(fd,leaf.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
            named(fd,leaf.c_str(),value.main_descriptor.value,value.main,false);
        }
        state->admitted=true;state->verify(end,true);
        auto admission=std::shared_ptr<const ordinary_managed_child_admission>(new ordinary_managed_child_admission(state));
        auto view=std::make_shared<ordinary_open_context::implementation>();view->managed=admission;view->managed_path=admission->primary_path();
        auto context=ordinary_context(new ordinary_open_context(std::move(view)));
        gate.context=context;gate.state.store(policy_state::phase::managed,std::memory_order_release);
        response["phase"]=4;state->channel->send(frame(response),end);
        return {context,std::move(admission)};
    }catch(...){gate.state.store(policy_state::phase::failed,std::memory_order_release);throw;}
#else
    (void)end;(void)daemon_entry;(void)paired_entry;normal_fail("managed historical receiver unsupported");
#endif
}
}
