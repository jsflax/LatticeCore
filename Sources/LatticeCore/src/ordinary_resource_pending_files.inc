// Included in the issuer's private namespace. Every caller already owns
// the actual root publication EX and has durably published its bound intent.
// None of these reserved pending names is an issued SQLite path.
descriptor resource_pending_directory(resource_root& root,const std::string& name,deadline end){
    const auto creator=root.creator;root.check(end);leaf(name);struct stat before{};
    if(::fstatat(root.control_fd.value,name.c_str(),&before,AT_SYMLINK_NOFOLLOW)){
        if(errno!=ENOENT||creator!=pid()||::mkdirat(root.control_fd.value,name.c_str(),0700)||
           ::fstatat(root.control_fd.value,name.c_str(),&before,AT_SYMLINK_NOFOLLOW))resource_fail();
    }
    if(!S_ISDIR(before.st_mode)||before.st_uid!=::geteuid()||(before.st_mode&07777&~0700))resource_fail();
    if((before.st_mode&07777)!=0700){
        root.check(end);struct stat again{};
        if(creator!=pid()||::fstatat(root.control_fd.value,name.c_str(),&again,AT_SYMLINK_NOFOLLOW)||
           !same(before,again)||::fchmodat(root.control_fd.value,name.c_str(),0700,AT_SYMLINK_NOFOLLOW))resource_fail();
    }
    descriptor result(::openat(root.control_fd.value,name.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
    const auto opened=metadata(result.value);struct stat named{};
    if(!S_ISDIR(opened.st_mode)||opened.st_uid!=::geteuid()||(opened.st_mode&07777)!=0700||
       id(opened)!=id(before)||::fstatat(root.control_fd.value,name.c_str(),&named,AT_SYMLINK_NOFOLLOW)||
       !same(opened,named)||creator!=pid()||::fsync(result.value)||::fsync(root.control_fd.value))resource_fail();
    root.check(end);return result;
}
void resource_pending_control(resource_root& root,int parent,const char* name,deadline end){
    const auto creator=root.creator;root.check(end);struct stat before{};
    if(::fstatat(parent,name,&before,AT_SYMLINK_NOFOLLOW)){
        if(errno!=ENOENT)resource_fail();create_control(parent,name,creator,end);
        if(::fstatat(parent,name,&before,AT_SYMLINK_NOFOLLOW))resource_fail();
    }
    if(!S_ISREG(before.st_mode)||before.st_uid!=::geteuid()||before.st_nlink!=1||before.st_size||
       (before.st_mode&07777&~0600))resource_fail();
    if((before.st_mode&07777)!=0600){
        struct stat again{};root.check(end);
        if(creator!=pid()||::fstatat(parent,name,&again,AT_SYMLINK_NOFOLLOW)||!same(before,again)||
           ::fchmodat(parent,name,0600,AT_SYMLINK_NOFOLLOW))resource_fail();
    }
    auto held=regular_control(parent,name,id(before));root.check(end);
    if(creator!=pid()||::fsync(held.value)||::fsync(parent))resource_fail();root.check(end);
}
struct stat resource_pending_zero(resource_root& root,int parent,const char* name,mode_t mode,deadline end){
    const auto creator=root.creator;root.check(end);struct stat value{};
    if(::fstatat(parent,name,&value,AT_SYMLINK_NOFOLLOW)){
        if(errno!=ENOENT||creator!=pid())resource_fail();bound(end);
        descriptor created(::openat(parent,name,O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
        if(created.value<0||::fchmod(created.value,0600))resource_fail();value=metadata(created.value);
        if(creator!=pid()||::close(created.release()))resource_fail();
    }
    if(!S_ISREG(value.st_mode)||value.st_uid!=::geteuid()||value.st_nlink!=1||value.st_size||
       (value.st_mode&07777&~0600))resource_fail();
    if((value.st_mode&07777)!=0600){
        struct stat again{};root.check(end);
        if(creator!=pid()||::fstatat(parent,name,&again,AT_SYMLINK_NOFOLLOW)||!same(value,again)||
           ::fchmodat(parent,name,0600,AT_SYMLINK_NOFOLLOW))resource_fail();
    }
    descriptor held(::openat(parent,name,O_RDWR|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
    const auto opened=metadata(held.value);struct stat named{};
    if(!S_ISREG(opened.st_mode)||opened.st_uid!=::geteuid()||opened.st_nlink!=1||opened.st_size||
       id(opened)!=id(value)||(opened.st_mode&07777)!=0600||creator!=pid()||
       ::fchmod(held.value,mode)||::fsync(held.value))resource_fail();
    value=metadata(held.value);
    if((value.st_mode&07777)!=mode||::fstatat(parent,name,&named,AT_SYMLINK_NOFOLLOW)||!same(value,named)||
       value.st_size!=named.st_size||creator!=pid()||::close(held.release())||::fsync(parent))resource_fail();
    root.check(end);return value;
}
