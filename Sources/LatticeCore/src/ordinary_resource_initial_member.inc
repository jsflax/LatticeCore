// Included in the issuer's private namespace after recreation helpers.
std::string resource_initial_intent_name(const std::string& slot){return "new-"+slot+".v1";}
json resource_initial_intent(resource_root& root,const std::string& name){
    return {{"format","lattice-resource-initial-create-v1"},{"epoch",root.epoch},{"name",name},
        {"control",identity_json(root.control_id)},{"generation",identity_json(root.generation_id)},
        {"logical",identity_json(root.logical_id)}};
}
json resource_initial_complete(resource_root& root,const std::string& name,file_identity directory,
    const std::vector<std::uint8_t>& member){
    return {{"format","lattice-resource-initial-complete-v1"},{"epoch",root.epoch},{"name",name},
        {"directory",identity_json(directory)},{"memberSHA256",digest_string(member)},
        {"initialIntentSHA256",digest_string(bytes(resource_initial_intent(root,name)))}};
}
void resource_initial_corpus(resource_root& root,int directory,const json& complete,bool committed,deadline end){
    root.check(end);const auto creator=root.creator;const auto parent=metadata(directory);
    const std::set<std::string> required{"entry.lock","physical.lock","intent.v1","prepared-new.v1","member.v1","state.v1"};
    auto allowed=required;allowed.insert("creation-complete.v1");
    const auto complete_bytes=bytes(complete);
    const json pending_binding={{"format","lattice-resource-record-publication-v1"},
        {"parent",identity_json(id(parent))},{"name","creation-complete.v1"},{"bodySHA256",digest_string(complete_bytes)}};
    const auto pending=std::string(".record-")+digest_string(bytes(pending_binding));
    if(!committed)allowed.insert(pending);
    descriptor cursor(::openat(directory,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
    DIR* raw=::fdopendir(cursor.release());if(!raw)resource_fail();
    struct directory_end {DIR* value;std::uint64_t creator;~directory_end(){if(creator==pid())::closedir(value);}} ending{raw,creator};
    std::set<std::string> actual;
    for(;;){if(creator!=pid())resource_fail();bound(end);errno=0;auto* row=::readdir(raw);
        if(!row){if(errno)resource_fail();break;}const std::string name=row->d_name;
        if(name=="."||name=="..")continue;
        if(!allowed.contains(name)||!actual.insert(name).second||actual.size()>allowed.size())resource_fail();
        struct stat file{};if(::fstatat(directory,name.c_str(),&file,AT_SYMLINK_NOFOLLOW)||!S_ISREG(file.st_mode)||
           file.st_uid!=::geteuid()||file.st_nlink!=1||file.st_size<0||
           static_cast<std::uint64_t>(file.st_size)>ordinary_launch::maximum_frame_bytes)resource_fail();
        if(name==pending){
            if((file.st_mode&07777&~0600)||static_cast<std::uint64_t>(file.st_size)>complete_bytes.size())resource_fail();
        }else if((file.st_mode&07777)!=0600)resource_fail();
    }
    for(const auto& name:required)if(!actual.contains(name))resource_fail();
    if(committed&&(!actual.contains("creation-complete.v1")||actual.size()!=required.size()+1))resource_fail();
    root.check(end);
}
void resource_initial_ready(resource_root& root,int directory,const std::string& name,
    const std::vector<std::uint8_t>& member,deadline end){
    const auto record=parse(member);
    if(!record.contains("initialIntentSHA256"))return; // unchanged cold/recreated members
    const auto expected=resource_initial_intent(root,name);const auto intent_hash=digest_string(bytes(expected));
    if(text(record,"initialIntentSHA256",64)!=intent_hash)resource_fail();
    const auto slot=digest_string(std::vector<std::uint8_t>(name.begin(),name.end()));
    if(read_record(root.control_fd.value,resource_initial_intent_name(slot),end)!=bytes(expected))resource_fail();
    const auto complete=resource_initial_complete(root,name,id(metadata(directory)),member);
    if(read_record(directory,"creation-complete.v1",end)!=bytes(complete))resource_fail();
    resource_initial_corpus(root,directory,complete,true,end);
    // A successful rename may precede a failed/crashed parent fsync. Every
    // valid READY resume must establish both real directory durability facts
    // before any Native open may grow the formerly empty MAIN.
    root.check(end);const auto creator=root.creator;const auto folder=metadata(directory);struct stat named{};
    if(!S_ISDIR(folder.st_mode)||folder.st_uid!=::geteuid()||(folder.st_mode&07777)!=0700||
       ::fstatat(root.control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(folder,named)||
       creator!=pid()||::fsync(directory)||::fsync(root.control_fd.value))resource_fail();
    root.check(end);
    if(creator!=pid()||::fstatat(root.control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||
       !same(folder,named)||!same(folder,metadata(directory)))resource_fail();
}
resource_creation_original resource_create_initial_member(resource_root& root,const std::string& name,const std::string& slot,
    bool already_present,deadline end){
#if defined(__APPLE__)
    const auto creator=root.creator;root.check(end);
    const auto root_intent=resource_initial_intent(root,name);const auto intent_name=resource_initial_intent_name(slot);
    if(already_present){
        // A partial directory is resumable only under the exact PREEXISTING
        // root-issued intent. Do not adopt an old unknown partial creation.
        if(read_record(root.control_fd.value,intent_name,end)!=bytes(root_intent))resource_fail();
    }else{
        struct stat absent{};
        for(const auto* suffix:{"","-wal","-shm","-journal"}){
            if(::fstatat(root.generation_fd.value,(name+suffix).c_str(),&absent,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)resource_fail();
        }
        if(::fstatat(root.logical_fd.value,name.c_str(),&absent,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)resource_fail();
    }
    // This publication completes before the FIRST directory/file effect.
    publish_record(root.control_fd.value,intent_name,root_intent,creator,end);
    auto member=resource_pending_directory(root,slot,end);const auto folder=metadata(member.value);
    resource_pending_control(root,member.value,"entry.lock",end);
    resource_pending_control(root,member.value,"physical.lock",end);
    auto entry=regular_control(member.value,"entry.lock"),physical=regular_control(member.value,"physical.lock");
    lock(entry.value,LOCK_EX,end);lock(physical.value,LOCK_EX,end);root.check(end);
    const auto intent_hash=digest_string(bytes(root_intent));
    const json intent={{"format","lattice-resource-new-member-intent-v1"},{"epoch",root.epoch},{"name",name},
        {"parent",identity_json(root.generation_id)},{"logicalParent",identity_json(root.logical_id)},
        {"directory",identity_json(id(folder))},{"initialIntentSHA256",intent_hash}};
    publish_record(member.value,"intent.v1",intent,creator,end);
    const auto exists=[&](int parent,const std::string& leaf_name,struct stat& value){
        if(::fstatat(parent,leaf_name.c_str(),&value,AT_SYMLINK_NOFOLLOW)==0)return true;
        if(errno!=ENOENT)resource_fail();return false;};
    struct stat found{};json prepared;
    if(exists(member.value,"prepared-new.v1",found)){
        prepared=parse(read_record(member.value,"prepared-new.v1",end));
        if(text(prepared,"format")!="lattice-resource-prepared-new-v1"||text(prepared,"epoch",64)!=root.epoch||
           text(prepared,"name",255)!=name||text(prepared,"initialIntentSHA256",64)!=intent_hash||
           identity_json(prepared.at("directory"))!=id(folder)||
           identity_json(prepared.at("entry"))!=id(metadata(entry.value))||
           identity_json(prepared.at("physical"))!=id(metadata(physical.value)))resource_fail();
        publish_record(member.value,"prepared-new.v1",prepared,creator,end);
    }else{
        for(const auto* suffix:{"","-wal","-shm","-journal"})if(exists(root.generation_fd.value,name+suffix,found))resource_fail();
        if(exists(root.logical_fd.value,name,found))resource_fail();
        const auto main=resource_pending_zero(root,member.value,"pending-main",0600,end);
        const auto stub=resource_pending_zero(root,member.value,"pending-stub",0000,end);
        prepared={{"format","lattice-resource-prepared-new-v1"},{"epoch",root.epoch},{"name",name},
            {"initialIntentSHA256",intent_hash},{"directory",identity_json(id(folder))},
            {"entry",identity_json(id(metadata(entry.value)))},{"physical",identity_json(id(metadata(physical.value)))},
            {"main",identity_json(id(main))},{"logicalStub",identity_json(id(stub))}};
        // Actual prepared identities are durable BEFORE either exclusive move.
        publish_record(member.value,"prepared-new.v1",prepared,creator,end);
    }
    const auto move=[&](const char* pending,int parent,file_identity expected,mode_t mode){
        root.check(end);struct stat source{},target{};
        const bool from=exists(member.value,pending,source),to=exists(parent,name,target);
        const auto valid=[&](const struct stat& value){return S_ISREG(value.st_mode)&&value.st_uid==::geteuid()&&
            value.st_nlink==1&&!value.st_size&&(value.st_mode&07777)==mode&&id(value)==expected;};
        if(from&&!to){
            if(!valid(source)||creator!=pid()||::renameatx_np(member.value,pending,parent,name.c_str(),RENAME_EXCL))resource_fail();
        }else if(from||!to||!valid(target))resource_fail();
        // Completed-syscall/failed-fsync retry still syncs BOTH actual parents.
        root.check(end);if(creator!=pid()||::fsync(member.value)||::fsync(parent))resource_fail();
        root.check(end);if(!exists(parent,name,target)||!valid(target))resource_fail();
    };
    move("pending-main",root.generation_fd.value,identity_json(prepared.at("main")),0600);
    move("pending-stub",root.logical_fd.value,identity_json(prepared.at("logicalStub")),0000);
    const json record={{"format","lattice-resource-member-v1"},{"epoch",root.epoch},{"name",name},
        {"parent",identity_json(root.generation_id)},{"main",prepared.at("main")},{"logicalStub",prepared.at("logicalStub")},
        {"entry",prepared.at("entry")},{"physical",prepared.at("physical")},{"initialIntentSHA256",intent_hash}};
    publish_record(member.value,"member.v1",record,creator,end);
    publish_record(member.value,"state.v1",{{"phase","active"},{"epoch",root.epoch},{"main",prepared.at("main")}},creator,end);
    const auto original=bytes(record);const auto complete=resource_initial_complete(root,name,id(folder),original);
    // Validate the WHOLE bounded corpus before final activation, including at
    // most the exact reserved incomplete producer for this final record.
    resource_initial_corpus(root,member.value,complete,false,end);
    publish_record(member.value,"creation-complete.v1",complete,creator,end);
    resource_initial_ready(root,member.value,name,original,end);
    root.check(end);struct stat named{};
    if(creator!=pid()||::fstatat(root.control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||
       !same(folder,named)||::fsync(member.value)||::fsync(root.control_fd.value))resource_fail();
    return resource_creation_original(root,name,resource_creation_original::source::initial,
        std::move(member),std::move(entry),std::move(physical),original,
        identity_json(prepared.at("main")),identity_json(prepared.at("logicalStub")));
#else
    (void)root;(void)name;(void)slot;(void)already_present;(void)end;resource_fail();
#endif
}
