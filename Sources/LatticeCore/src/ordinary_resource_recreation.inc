// Included in the issuer's private helper namespace, before issue_new_member.
// Existing active members never reacquire root EX during their own builder's
// retained entry SH. Only real delete/recreate/metadata-debt states come here.
bool resource_member_needs_change(resource_root& root,const std::string& slot,deadline end){
    struct stat before{};
    if(::fstatat(root.control_fd.value,slot.c_str(),&before,AT_SYMLINK_NOFOLLOW)||!S_ISDIR(before.st_mode)||
       before.st_uid!=::geteuid()||(before.st_mode&07777&~0700))resource_fail();
    if((before.st_mode&07777)!=0700)return true; // DATA classification; root intent required before repair
    descriptor member(::openat(root.control_fd.value,slot.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
    const auto folder=metadata(member.value);struct stat named{};
    if(!same(before,folder)||::fstatat(root.control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(folder,named))resource_fail();
    for(const auto* name:{"member.v1","state.v1"}){
        if(::fstatat(member.value,name,&named,AT_SYMLINK_NOFOLLOW)){
            if(errno!=ENOENT)resource_fail();return true;
        }
    }
    const auto record=parse(read_record(member.value,"member.v1",end));
    if(record.contains("initialIntentSHA256")&&::fstatat(member.value,"creation-complete.v1",&named,AT_SYMLINK_NOFOLLOW)){
        if(errno!=ENOENT)resource_fail();return true;
    }
    for(const auto* name:{"deletion.v1","prior-control.v1"}){
        if(::fstatat(member.value,name,&named,AT_SYMLINK_NOFOLLOW)==0)return true;
        if(errno!=ENOENT)resource_fail();
    }
    return false; // DATA fast path only; active_member still validates under SH.
}
void resource_cleanup_prior(resource_root& root,int current,deadline end){
    const auto creator=pid();struct stat present{};
    if(::fstatat(current,"prior-control.v1",&present,AT_SYMLINK_NOFOLLOW)){
        if(errno==ENOENT)return;resource_fail();
    }
    const auto original=read_record(current,"prior-control.v1",end);const auto prior=parse(original);
    if(text(prior,"format")!="lattice-resource-prior-control-v1"||text(prior,"epoch",64)!=root.epoch||
       identity_json(prior.at("parent"))!=root.control_id)resource_fail();
    const auto name=text(prior,"directory",255);leaf(name);
    const auto expected=identity_json(prior.at("identity"));const auto old_hash=text(prior,"memberSHA256",64);
    if(name!="recreate-"+old_hash)resource_fail();
    struct stat named{};
    if(::fstatat(root.control_fd.value,name.c_str(),&named,AT_SYMLINK_NOFOLLOW)==0){
        if(!S_ISDIR(named.st_mode)||named.st_uid!=::geteuid()||(named.st_mode&07777)!=0700||id(named)!=expected)resource_fail();
        descriptor old(::openat(root.control_fd.value,name.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        if(!same(named,metadata(old.value)))resource_fail();
        const std::set<std::string> allowed{"entry.lock","physical.lock","member.v1","state.v1","intent.v1",
            "main.move.v1","wal.move.v1","shm.move.v1","journal.move.v1","deletion.v1",
            "deletion-checkpoint.v1","deletion-complete.v1","recreated-from.v1","recreation-intent.v1",
            "prepared-new.v1","creation-complete.v1"};
        std::vector<std::pair<std::string,struct stat>> files;files.reserve(allowed.size());
        descriptor cursor(::openat(old.value,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        DIR* raw=::fdopendir(cursor.release());if(!raw)resource_fail();
        struct directory_end {DIR* value;std::uint64_t creator;~directory_end(){if(creator==pid())::closedir(value);}} ending{raw,creator};
        for(;;){if(creator!=pid())resource_fail();bound(end);errno=0;auto* row=::readdir(raw);
            if(!row){if(errno)resource_fail();break;}const std::string leaf_name=row->d_name;
            if(leaf_name=="."||leaf_name=="..")continue;
            if(!allowed.contains(leaf_name)||files.size()==allowed.size())resource_fail();struct stat file{};
            if(::fstatat(old.value,leaf_name.c_str(),&file,AT_SYMLINK_NOFOLLOW)||!S_ISREG(file.st_mode)||
               file.st_uid!=::geteuid()||file.st_nlink!=1||(file.st_mode&07777)!=0600||
               file.st_size<0||static_cast<std::uint64_t>(file.st_size)>ordinary_launch::maximum_frame_bytes)resource_fail();
            if(leaf_name=="member.v1"&&digest_string(read_record(old.value,leaf_name,end))!=old_hash)resource_fail();
            files.push_back({leaf_name,file});
        }
        // Complete corpus before any unlink. Preserve the old member hash
        // witness until last; interrupted cleanup retains prior-control.v1.
        std::sort(files.begin(),files.end(),[](const auto& a,const auto& b){
            if(a.first=="member.v1")return false;if(b.first=="member.v1")return true;return a.first<b.first;});
        for(const auto& [leaf_name,before]:files){
            root.check(end);struct stat actual{};
            if(::fstatat(old.value,leaf_name.c_str(),&actual,AT_SYMLINK_NOFOLLOW)||!same(before,actual)||
               before.st_size!=actual.st_size||creator!=pid()||::unlinkat(old.value,leaf_name.c_str(),0))resource_fail();
        }
        if(creator!=pid()||::fsync(old.value)||::fstatat(root.control_fd.value,name.c_str(),&named,AT_SYMLINK_NOFOLLOW)||
           id(named)!=expected||::unlinkat(root.control_fd.value,name.c_str(),AT_REMOVEDIR)||::fsync(root.control_fd.value))resource_fail();
    }else if(errno!=ENOENT)resource_fail();
    // This also covers retry after actual rmdir followed by failed parent
    // fsync. Never clear the journal based only on an absent observed name.
    if(creator!=pid()||::fsync(root.control_fd.value))resource_fail();
    root.check(end);if(read_record(current,"prior-control.v1",end)!=original||creator!=pid()||
       ::unlinkat(current,"prior-control.v1",0)||::fsync(current))resource_fail();
}
std::optional<resource_creation_original> resource_recreate_completed_member(resource_root& root,const std::string& name,
    const std::string& slot,deadline end){
#if defined(__APPLE__)
    const auto creator=pid();root.check(end);
    descriptor old(::openat(root.control_fd.value,slot.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
    const auto old_folder=metadata(old.value);struct stat named{};
    if(!S_ISDIR(old_folder.st_mode)||old_folder.st_uid!=::geteuid()||(old_folder.st_mode&07777)!=0700||
       ::fstatat(root.control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(old_folder,named))resource_fail();
    const auto old_bytes=read_record(old.value,"member.v1",end);const auto previous=parse(old_bytes);
    if(text(previous,"format")!="lattice-resource-member-v1"||text(previous,"epoch",64)!=root.epoch||
       text(previous,"name",255)!=name||identity_json(previous.at("parent"))!=root.generation_id)resource_fail();
    auto old_entry=regular_control(old.value,"entry.lock",identity_json(previous.at("entry")));
    lock(old_entry.value,LOCK_EX,end);if(read_record(old.value,"member.v1",end)!=old_bytes)resource_fail();
    // A prior generation's metadata can never accumulate across another
    // reincarnation. It contains no SQLite file or physical ownership object.
    resource_cleanup_prior(root,old.value,end);
    if(::fstatat(old.value,"deletion.v1",&named,AT_SYMLINK_NOFOLLOW)){
        if(errno==ENOENT)return {};resource_fail();
    }
    auto old_physical=regular_control(old.value,"physical.lock",identity_json(previous.at("physical")));
    lock(old_physical.value,LOCK_EX,end);root.check(end);
    const auto old_hash=digest_string(old_bytes);const auto stage="recreate-"+old_hash;leaf(stage);
    const json binding={{"format","lattice-resource-delete-v1"},{"epoch",root.epoch},{"name",name},
        {"memberSHA256",old_hash},{"parent",identity_json(root.generation_id)},
        {"main",previous.at("main")},{"logicalStub",previous.at("logicalStub")}};
    for(const auto* record:{"deletion.v1","deletion-checkpoint.v1","deletion-complete.v1"})
        if(read_record(old.value,record,end)!=bytes(binding))resource_fail();
    const json intent={{"format","lattice-resource-recreate-intent-v1"},{"epoch",root.epoch},
        {"name",name},{"previousMemberSHA256",old_hash},{"previousDirectory",identity_json(id(old_folder))},
        {"stage",stage},{"control",identity_json(root.control_id)}};
    const auto exists=[&](int dir,const std::string& leaf_name,struct stat& value){
        if(::fstatat(dir,leaf_name.c_str(),&value,AT_SYMLINK_NOFOLLOW)==0)return true;
        if(errno!=ENOENT)resource_fail();return false;};
    const auto once=[&](int dir,const char* record,const json& expected){
        publish_record(dir,record,expected,creator,end);};
    once(old.value,"recreation-intent.v1",intent);
    auto next=resource_pending_directory(root,stage,end);
    const auto next_folder=metadata(next.value);
    for(const auto* file:{"entry.lock","physical.lock"}){
        if(!exists(next.value,file,named))create_control(next.value,file,creator,end);
        else {
            if(!S_ISREG(named.st_mode)||named.st_uid!=::geteuid()||named.st_nlink!=1||named.st_size||
               (named.st_mode&07777&~0600))resource_fail();
            if((named.st_mode&07777)!=0600&&
               (creator!=pid()||::fchmodat(next.value,file,0600,AT_SYMLINK_NOFOLLOW)))resource_fail();
            auto checked=regular_control(next.value,file);if(id(metadata(checked.value))!=id(named)||::fsync(checked.value))resource_fail();
        }
    }
    auto next_entry=regular_control(next.value,"entry.lock"),next_physical=regular_control(next.value,"physical.lock");
    lock(next_entry.value,LOCK_EX,end);lock(next_physical.value,LOCK_EX,end);
    struct stat main{},stub{};json prepared;
    if(exists(next.value,"recreated-from.v1",named)){
        prepared=parse(read_record(next.value,"recreated-from.v1",end));
        if(text(prepared,"format")!="lattice-resource-recreated-from-v1"||text(prepared,"epoch",64)!=root.epoch||
           text(prepared,"name",255)!=name||text(prepared,"previousMemberSHA256",64)!=old_hash||
           identity_json(prepared.at("directory"))!=id(next_folder)||
           identity_json(prepared.at("entry"))!=id(metadata(next_entry.value))||
           identity_json(prepared.at("physical"))!=id(metadata(next_physical.value)))resource_fail();
        once(next.value,"recreated-from.v1",prepared); // actual durable resume/own pending cleanup
    }else{
        for(const auto* suffix:{"","-wal","-shm","-journal"})if(exists(root.generation_fd.value,name+suffix,named))resource_fail();
        if(exists(root.logical_fd.value,name,named))resource_fail();
        const auto empty=[&](const char* leaf_name,mode_t mode){
            root.check(end);struct stat value{};
            if(!exists(next.value,leaf_name,value)){
                if(creator!=pid())resource_fail();bound(end);
                descriptor created(::openat(next.value,leaf_name,O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
                if(created.value<0||::fchmod(created.value,0600))resource_fail();
                value=metadata(created.value);
                if(creator!=pid()||::close(created.release()))resource_fail();
            }
            // Only this reserved unpublished zero-file slot may temporarily
            // become readable. No supported issuer opens it as SQLite MAIN.
            if(!S_ISREG(value.st_mode)||value.st_uid!=::geteuid()||value.st_nlink!=1||value.st_size||
               (value.st_mode&07777&~0600))resource_fail();
            if((value.st_mode&07777)!=0600){
                root.check(end);struct stat again{};
                if(creator!=pid()||::fstatat(next.value,leaf_name,&again,AT_SYMLINK_NOFOLLOW)||
                   !same(value,again)||::fchmodat(next.value,leaf_name,0600,AT_SYMLINK_NOFOLLOW))resource_fail();
            }
            descriptor pending(::openat(next.value,leaf_name,O_RDWR|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
            const auto opened=metadata(pending.value);struct stat named{};
            if(!S_ISREG(opened.st_mode)||opened.st_uid!=::geteuid()||opened.st_nlink!=1||opened.st_size||
               id(opened)!=id(value)||(opened.st_mode&07777)!=0600||creator!=pid()||
               ::fchmod(pending.value,mode)||::fsync(pending.value))resource_fail();
            value=metadata(pending.value);
            if((value.st_mode&07777)!=mode||::fstatat(next.value,leaf_name,&named,AT_SYMLINK_NOFOLLOW)||
               !same(value,named)||value.st_size!=named.st_size||creator!=pid()||
               ::close(pending.release())||::fsync(next.value))resource_fail();
            root.check(end);return value;
        };
        main=empty("pending-main",0600);stub=empty("pending-stub",0000);
        prepared={{"format","lattice-resource-recreated-from-v1"},{"epoch",root.epoch},{"name",name},
            {"previousMemberSHA256",old_hash},{"directory",identity_json(id(next_folder))},
            {"main",identity_json(id(main))},{"logicalStub",identity_json(id(stub))},
            {"entry",identity_json(id(metadata(next_entry.value)))},{"physical",identity_json(id(metadata(next_physical.value)))}};
        once(next.value,"recreated-from.v1",prepared);
    }
    const json member={{"format","lattice-resource-member-v1"},{"epoch",root.epoch},{"name",name},
        {"parent",identity_json(root.generation_id)},{"main",prepared.at("main")},
        {"logicalStub",prepared.at("logicalStub")},{"entry",prepared.at("entry")},{"physical",prepared.at("physical")},
        {"previousMemberSHA256",old_hash}};
    once(next.value,"member.v1",member);
    once(next.value,"state.v1",{{"phase","active"},{"epoch",root.epoch},{"main",prepared.at("main")}});
    once(next.value,"prior-control.v1",{{"format","lattice-resource-prior-control-v1"},{"epoch",root.epoch},
        {"parent",identity_json(root.control_id)},{"directory",stage},{"identity",identity_json(id(old_folder))},{"memberSHA256",old_hash}});
    const auto move=[&](const char* pending,int parent,file_identity expected,mode_t mode){
        struct stat source{},target{};const bool from=exists(next.value,pending,source),to=exists(parent,name,target);
        const auto valid=[&](const struct stat& value){return S_ISREG(value.st_mode)&&value.st_uid==::geteuid()&&
            value.st_nlink==1&&!value.st_size&&(value.st_mode&07777)==mode&&id(value)==expected;};
        if(from&&!to){
            if(!valid(source)||creator!=pid())resource_fail();root.check(end);
            if(::renameatx_np(next.value,pending,parent,name.c_str(),RENAME_EXCL))resource_fail();
        }else if(from||!to||!valid(target))resource_fail();
        // Resume after a completed rename must establish BOTH directory
        // durability facts too, even if the previous fsync never returned.
        root.check(end);if(creator!=pid()||::fsync(next.value)||::fsync(parent))resource_fail();
        root.check(end);if(!exists(parent,name,target)||!valid(target))resource_fail();
    };
    move("pending-main",root.generation_fd.value,identity_json(prepared.at("main")),0600);
    move("pending-stub",root.logical_fd.value,identity_json(prepared.at("logicalStub")),0000);
    // Complete bounded stage corpus after all known producer pending slots
    // have settled and before the directory can become active metadata.
    {
        const std::set<std::string> expected{"entry.lock","physical.lock","recreated-from.v1",
            "member.v1","state.v1","prior-control.v1"};
        std::set<std::string> actual;
        descriptor cursor(::openat(next.value,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        DIR* raw=::fdopendir(cursor.release());if(!raw)resource_fail();
        struct directory_end {DIR* value;std::uint64_t creator;~directory_end(){if(creator==pid())::closedir(value);}} ending{raw,creator};
        for(;;){if(creator!=pid())resource_fail();bound(end);errno=0;auto* row=::readdir(raw);
            if(!row){if(errno)resource_fail();break;}const std::string leaf_name=row->d_name;
            if(leaf_name=="."||leaf_name=="..")continue;
            if(!expected.contains(leaf_name)||!actual.insert(leaf_name).second||actual.size()>expected.size())resource_fail();
            struct stat file{};if(::fstatat(next.value,leaf_name.c_str(),&file,AT_SYMLINK_NOFOLLOW)||
               !S_ISREG(file.st_mode)||file.st_uid!=::geteuid()||file.st_nlink!=1||(file.st_mode&07777)!=0600||
               file.st_size<0||static_cast<std::uint64_t>(file.st_size)>ordinary_launch::maximum_frame_bytes)resource_fail();
        }
        if(actual!=expected)resource_fail();
    }
    root.check(end);
    if(read_record(old.value,"member.v1",end)!=old_bytes||
       ::fstatat(root.control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(old_folder,named)||
       ::fstatat(root.control_fd.value,stage.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(next_folder,named)||creator!=pid())resource_fail();
    // This single publication exchanges complete self-consistent directories.
    // Stale readers retain the old deletion marker and fail the named-dir test.
    if(::renameatx_np(root.control_fd.value,slot.c_str(),root.control_fd.value,stage.c_str(),RENAME_SWAP)||
       ::fsync(root.control_fd.value))resource_fail();
    if(creator!=pid())resource_fail();root.check(end);
    resource_cleanup_prior(root,next.value,end);
    return resource_creation_original(root,name,resource_creation_original::source::recreated,
        std::move(next),std::move(next_entry),std::move(next_physical),member,
        identity_json(prepared.at("main")),identity_json(prepared.at("logicalStub")));
#else
    // Cold activation currently relies on the actual Darwin observed-process
    // and atomic-name exchange contract. It cannot be emulated by unlink/move.
    (void)root;(void)name;(void)slot;(void)end;resource_fail();
#endif
}
