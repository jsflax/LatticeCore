// Included in recovery_local_producer.cpp. No public setter exposes either
// enrollment or metadata mutation admission. File descriptors below are ONLY
// directory descriptors, including failure cleanup (never main/WAL/SHM).
struct recovery_continuous_state {
    recovery_continuous_policy policy;
    blob manifest;
    std::string digest,path,parent;
    std::shared_ptr<const physical_store_identity> identity;
    std::mutex mutex;
    std::atomic<bool> dml_open{false};
    bool initialized=false, fresh=false, opening=true;
    size_t owners=0,routes=0,operations=0;
    uint64_t next_route=0,gate_revision=0;
    int64_t incarnation=0,barrier=0,attempt=0;
    std::set<std::string> generated_names;
    int64_t parent_device=0,parent_inode=0,container_device=0,container_inode=0;
#if defined(__APPLE__) || defined(__linux__)
    int parent_fd=-1,container_fd=-1;
    bool locked=false;
    ~recovery_continuous_state() {
        if(container_fd>=0){if(locked)::flock(container_fd,LOCK_UN);::close(container_fd);}
        if(parent_fd>=0)::close(parent_fd);
        // A refused or crashed enrollment leaves an orphan container. Never
        // recreate, remove, or silently adopt it on the next open.
    }
    static int64_t bounded(uint64_t n) {
        if(n>static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))refuse("continuous physical identity overflow");
        return static_cast<int64_t>(n);
    }
    void verify_file(database& writer)const {
        const auto current=writer.physical_identity("main",{},true);
        struct stat root{},root_path{},container{},container_path{},main{};
        if(!identity||!current||!(*identity==*current)||identity->filename!=current->filename||
           parent_fd<0||container_fd<0||!locked||::fstat(parent_fd,&root)||::lstat(parent.c_str(),&root_path)||
           !S_ISDIR(root.st_mode)||!S_ISDIR(root_path.st_mode)||root.st_dev!=root_path.st_dev||root.st_ino!=root_path.st_ino||
           ::fstat(container_fd,&container)||::lstat(std::filesystem::path(path).parent_path().c_str(),&container_path)||
           !S_ISDIR(container.st_mode)||!S_ISDIR(container_path.st_mode)||container.st_dev!=container_path.st_dev||container.st_ino!=container_path.st_ino||
           container.st_uid!=::geteuid()||(container.st_mode&07777)!=0700||
           ::fstatat(container_fd,"store.sqlite",&main,AT_SYMLINK_NOFOLLOW)||!S_ISREG(main.st_mode)||main.st_nlink!=1||
           bounded(root.st_dev)!=parent_device||bounded(root.st_ino)!=parent_inode||
           bounded(container.st_dev)!=container_device||bounded(container.st_ino)!=container_inode||
           bounded(main.st_dev)!=bounded(identity->device)||bounded(main.st_ino)!=bounded(identity->inode))
            refuse("continuous parent/container/main identity changed");
    }
#else
    void verify_file(database&)const {refuse("continuous directory custody platform unavailable");}
#endif
};
struct recovery_continuous_admission {
    std::shared_ptr<const recovery_continuous_producer::owner_recipe> recipe;
    std::shared_ptr<recovery_continuous_state> state;
    lattice_db* owner=nullptr;
    database* writer=nullptr;
    bool counted=false;
    ~recovery_continuous_admission(){if(counted){std::lock_guard<std::mutex> lock(state->mutex);--state->owners;}}
};
namespace {
constexpr const char* continuous_table="_lattice_producer_continuity";
constexpr const char* continuous_ddl="CREATE TABLE _lattice_producer_continuity(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,manifest BLOB NOT NULL,digest BLOB NOT NULL,main_device INTEGER NOT NULL,main_inode INTEGER NOT NULL,parent_device INTEGER NOT NULL,parent_inode INTEGER NOT NULL,container_device INTEGER NOT NULL,container_inode INTEGER NOT NULL,incarnation INTEGER NOT NULL,barrier INTEGER NOT NULL,attempt INTEGER NOT NULL,phase INTEGER NOT NULL) WITHOUT ROWID";
constexpr const char* continuous_query="SELECT CASE WHEN typeof(id)='integer' THEN id END AS id,CASE WHEN typeof(version)='integer' THEN version END AS version,CASE WHEN typeof(manifest)='blob' AND length(manifest)<=1048576 THEN manifest END AS manifest,CASE WHEN typeof(digest)='blob' AND length(digest)=64 THEN digest END AS digest,CASE WHEN typeof(main_device)='integer' THEN main_device END AS main_device,CASE WHEN typeof(main_inode)='integer' THEN main_inode END AS main_inode,CASE WHEN typeof(parent_device)='integer' THEN parent_device END AS parent_device,CASE WHEN typeof(parent_inode)='integer' THEN parent_inode END AS parent_inode,CASE WHEN typeof(container_device)='integer' THEN container_device END AS container_device,CASE WHEN typeof(container_inode)='integer' THEN container_inode END AS container_inode,CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(barrier)='integer' THEN barrier END AS barrier,CASE WHEN typeof(attempt)='integer' THEN attempt END AS attempt,CASE WHEN typeof(phase)='integer' THEN phase END AS phase FROM main._lattice_producer_continuity LIMIT 2";
struct continuous_frame {
    recovery_continuous_state* state;
    const database* writer;
    int operation; // 1 enrollment, 2 claim, 3 begin, 4 freeze, 5 cancel
    continuous_frame* previous;
    static thread_local continuous_frame* current;
    continuous_frame(recovery_continuous_state& s,database& w,int op):state(&s),writer(&w),operation(op),previous(current){current=this;}
    ~continuous_frame(){current=previous;}
};
thread_local continuous_frame* continuous_frame::current=nullptr;
bool continuous_owned(const recovery_continuous_admission& a)noexcept {
    for(auto* f=continuous_frame::current;f;f=f->previous)if(f->state==a.state.get()&&f->writer==a.writer)return true;
    return false;
}
bool continuous_authorize(const recovery_continuous_admission& a,int action,const char* table,const char* column,const char* origin)noexcept {
    if(action==SQLITE_PRAGMA&&column&&(same_ascii(table,"synchronous")||same_ascii(table,"journal_mode")))return false;
    const bool mutation=action==SQLITE_INSERT||action==SQLITE_UPDATE||action==SQLITE_DELETE;
    if(!mutation||!table)return true;
    if(std::strcmp(table,continuous_table)==0 || std::strncmp(table,"_lattice_obligation_",20)==0 ||
       std::strncmp(table,"_lattice_install_",17)==0) {
        for(auto* f=continuous_frame::current;f;f=f->previous)if(f->state==a.state.get()&&f->writer==a.writer) {
            if(std::strcmp(table,continuous_table)==0)return f->operation==1||f->operation==3||f->operation==4||f->operation==5;
            if(std::strncmp(table,"_lattice_install_",17)==0)return f->operation==1||f->operation==5;
            return f->operation==1||f->operation==2||f->operation==4||f->operation==5;
        }
        // Exact generated trigger names are independently reconstructed from
        // admitted immutable schema and stored on this custody session.
        if(origin&&std::strcmp(table,continuous_table)!=0)for(const auto& name:a.state->generated_names)if(name==origin)return true;
        return false;
    }
    return true;
}
programs continuous_guards() {
    programs out;
    for(const char* op:{"INSERT","UPDATE","DELETE"}) {
        const auto name=std::string("_lattice_producer_continuity_")+op;
        out.emplace(name,"CREATE TRIGGER "+name+" BEFORE "+op+" ON _lattice_producer_continuity BEGIN SELECT CASE WHEN lattice_recovery_continuity_owned_v1()=1 THEN 1 ELSE RAISE(ABORT,'continuous closed mutation required') END; END");
    }
    return out;
}
void continuous_inventory(database& db) {
    const auto rows=db.query("SELECT type,CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN length(CAST(sql AS BLOB))<=4096 THEN sql END AS sql FROM main.sqlite_schema WHERE name GLOB '_lattice_producer_continuity*' LIMIT 5");
    auto expected=continuous_guards();expected.emplace(continuous_table,continuous_ddl);
    if(rows.size()!=expected.size())refuse("continuous fixed schema inventory differs");
    for(const auto& row:rows){const auto name=text(row,"name");const auto found=expected.find(name);
        if(found==expected.end()||text(row,"sql")!=found->second||text(row,"type")!=(name==continuous_table?"table":"trigger"))refuse("continuous schema/guard changed");}
}
blob continuous_manifest(const recovery_continuous_policy& policy) {
    validate_limits(policy.limits);
    if(policy.contributions.empty()||policy.contributions.size()>16||policy.routes.empty()||policy.routes.size()>32||
       !policy.owners||policy.owners>64||!policy.physical_routes||policy.physical_routes>128||
       !policy.operations||policy.operations>256||!policy.frozen_entries||policy.frozen_entries>100000||
       !policy.frozen_bytes||policy.frozen_bytes>67108864)refuse("continuous policy outside independent caps");
    if(policy.contributions.size()>static_cast<size_t>(policy.limits.obligations.scopes)||policy.contributions.size()>static_cast<size_t>(policy.limits.installations.channels)||
       policy.contributions.size()>static_cast<size_t>(policy.limits.producers.profiles)||policy.frozen_entries<static_cast<size_t>(policy.limits.obligations.records)||
       policy.frozen_bytes<static_cast<uint64_t>(policy.limits.obligations.encoded_bytes))refuse("continuous Q policy must contain the complete bounded journal");
    blob out;field(out,std::string("lattice-continuous-generated-v1"));
    for(const auto n:{policy.limits.obligations.scopes,policy.limits.obligations.records,policy.limits.obligations.field_bytes,policy.limits.obligations.encoded_bytes,
        policy.limits.installations.channels,policy.limits.installations.field_bytes,policy.limits.installations.encoded_bytes,
        policy.limits.producers.profiles,policy.limits.producers.stamps,policy.limits.producers.field_bytes,policy.limits.producers.manifest_bytes,policy.limits.producers.encoded_bytes})number(out,n);
    for(const auto n:{static_cast<uint64_t>(policy.owners),static_cast<uint64_t>(policy.physical_routes),static_cast<uint64_t>(policy.operations),static_cast<uint64_t>(policy.frozen_entries),policy.frozen_bytes})number(out,n);
    number(out,policy.contributions.size());std::set<std::string> channels;
    for(const auto& c:policy.contributions) {
        const auto& b=c.profile.binding;
        for(const auto* value:{&b.channel,&b.authority,&b.source,&b.epoch,&b.scope,&b.schema,&c.profile.profile_digest,&c.profile.receipt_namespace}){
            if(value->empty()||value->size()>4096)refuse("continuous empty or oversized binding");field(out,*value);}
        if(!channels.insert(b.channel).second||c.models.empty()||c.models.size()>16||c.incoming_grant_claim.empty()||c.incoming_grant_claim.size()>65536)refuse("continuous contribution inventory differs");
        number(out,c.models.size());std::set<std::string> models;
        for(const auto& model:c.models){if(!identifier(model)||!models.insert(model).second)refuse("continuous model inventory invalid");field(out,model);}
        field(out,c.incoming_grant_claim);
    }
    number(out,policy.routes.size());std::set<std::string> routes;
    for(const auto& route:policy.routes) {
        if(route.sync_id.empty()||route.sync_id.size()>4096||route.endpoint.size()>4096||route.endpoint.compare(0,6,"wss://")||
           !routes.insert(route.sync_id).second)refuse("continuous bounded WSS route inventory invalid");
        field(out,route.sync_id);field(out,route.endpoint);
    }
    return out;
}
std::mutex continuous_sessions_mutex;
std::map<std::string,std::weak_ptr<recovery_continuous_state>> continuous_sessions;
bool continuous_reserved(const std::filesystem::path& path) {
    for(const auto& component:path)if(component.string().ends_with(".lattice-continuous"))return true;
    return false;
}
std::shared_ptr<recovery_continuous_admission> continuous_acquire(const configuration& config,const recovery_continuous_policy& policy) {
#if defined(__APPLE__) || defined(__linux__)
    if(config.path.size()>4096||config.read_only||config.is_in_memory()||config.is_ipc_enabled()||config.audit_retention_seconds||config.migration_block||
       config.path.find('?')!=std::string::npos||config.path.starts_with("file:"))refuse("continuous configuration topology unavailable");
    auto manifest=continuous_manifest(policy);
    auto normalized=policy; // First bounded validation above precedes copying.
    for(auto& contribution:normalized.contributions)std::sort(contribution.models.begin(),contribution.models.end());
    std::sort(normalized.contributions.begin(),normalized.contributions.end(),[](const auto& a,const auto& b){return a.profile.binding.channel<b.profile.binding.channel;});
    std::sort(normalized.routes.begin(),normalized.routes.end(),[](const auto& a,const auto& b){return a.sync_id<b.sync_id;});
    manifest=continuous_manifest(normalized);
    if(config.is_sync_enabled()) {
        bool match=false;for(const auto& route:policy.routes)if(route.endpoint==config.websocket_url&&route.sync_id=="wss:"+config.websocket_url)match=true;
        if(!match||config.sync_filter)refuse("continuous configured WSS route outside policy");
    }
    auto admission=std::make_shared<recovery_continuous_admission>();
    const auto lexical=std::filesystem::absolute(config.path).lexically_normal();
    if(lexical.filename()!="store.sqlite"||!lexical.parent_path().filename().string().ends_with(".lattice-continuous"))refuse("continuous explicit reserved container required");
    const auto outer=std::filesystem::canonical(lexical.parent_path().parent_path());
    if(outer!=lexical.parent_path().parent_path())refuse("continuous container ancestors must be canonical without aliases");
    const auto path=lexical.string();field(manifest,path);
    // Registry lock is only for bounded session publication. No SQLite, wait,
    // callback, transport teardown or owner lock occurs while it is held.
    std::shared_ptr<recovery_continuous_state> state;
    {std::lock_guard<std::mutex> lock(continuous_sessions_mutex);
     if(const auto found=continuous_sessions.find(path);found!=continuous_sessions.end())state=found->second.lock();}
    if(state){std::lock_guard<std::mutex> lock(state->mutex);
        if(!state->initialized||state->opening||state->manifest!=manifest||state->owners==policy.owners)refuse("continuous existing session unavailable or policy changed");
        ++state->owners;
    } else {
        state=std::make_shared<recovery_continuous_state>();state->policy=std::move(normalized);state->manifest=manifest;
        state->digest=picosha2::hash256_hex_string(manifest);state->path=path;state->parent=outer.string();
        constexpr int flags=O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC;
        state->parent_fd=::open(state->parent.c_str(),flags);
        if(state->parent_fd<0)refuse("continuous parent directory unavailable");
        struct stat root{},container{},main{};
        if(::fstat(state->parent_fd,&root)||!S_ISDIR(root.st_mode))refuse("continuous parent is not directory");
        const auto name=lexical.parent_path().filename().string();
        if(::mkdirat(state->parent_fd,name.c_str(),0700)==0)state->fresh=true;
        else if(errno!=EEXIST)refuse("continuous fresh container creation failed");
        state->container_fd=::openat(state->parent_fd,name.c_str(),flags);
        if(state->container_fd<0||::fstat(state->container_fd,&container)||!S_ISDIR(container.st_mode)||container.st_uid!=::geteuid()||(container.st_mode&07777)!=0700)
            refuse("continuous container missing, replaced or malformed");
        if(::flock(state->container_fd,LOCK_EX|LOCK_NB))refuse("continuous directory already owned or platform lock unavailable");
        state->locked=true;
        const int present=::fstatat(state->container_fd,"store.sqlite",&main,AT_SYMLINK_NOFOLLOW);
        if(state->fresh ? (present==0||errno!=ENOENT) : (present!=0||!S_ISREG(main.st_mode)||main.st_nlink!=1))refuse("continuous first-open/reopen identity invalid");
        state->parent_device=state->bounded(root.st_dev);state->parent_inode=state->bounded(root.st_ino);
        state->container_device=state->bounded(container.st_dev);state->container_inode=state->bounded(container.st_ino);
        if(::fsync(state->container_fd)||::fsync(state->parent_fd))refuse("continuous directory durability unavailable");
        state->owners=1;
        {std::lock_guard<std::mutex> lock(continuous_sessions_mutex);
         if(auto old=continuous_sessions[path].lock())refuse("continuous raced with another admitted session");
         for(auto it=continuous_sessions.begin();it!=continuous_sessions.end();)if(it->second.expired())it=continuous_sessions.erase(it);else ++it;
         if(continuous_sessions.size()>=64)refuse("continuous process session registry capacity");
         continuous_sessions[path]=state;}
    }
    admission->state=std::move(state);admission->counted=true;return admission;
#else
    (void)config;(void)policy;refuse("continuous native directory custody platform unavailable");
#endif
}
void continuous_verify_row(database& db,recovery_continuous_state& state,bool current=true) {
    state.verify_file(db);continuous_inventory(db);
    const auto rows=db.query(continuous_query);
    if(rows.size()!=1)refuse("continuous lineage row missing or duplicate");const auto& row=rows[0];
    const auto bytes=[&](const char* name)->const blob&{const auto at=row.find(name);if(at==row.end()||!std::holds_alternative<blob>(at->second))refuse("continuous invalid blob");return std::get<blob>(at->second);};
    if(integer(row,"id")!=1||integer(row,"version")!=1||bytes("manifest")!=state.manifest||bytes("digest")!=blob(state.digest.begin(),state.digest.end())||
       integer(row,"main_device")!=static_cast<int64_t>(state.identity->device)||integer(row,"main_inode")!=static_cast<int64_t>(state.identity->inode)||
       integer(row,"parent_device")!=state.parent_device||integer(row,"parent_inode")!=state.parent_inode||
       integer(row,"container_device")!=state.container_device||integer(row,"container_inode")!=state.container_inode||
       integer(row,"incarnation")<=0||integer(row,"barrier")<0||integer(row,"attempt")<0||integer(row,"phase")<0||integer(row,"phase")>2||(integer(row,"phase")!=0&&(integer(row,"barrier")==0||integer(row,"attempt")==0))||
       (current&&integer(row,"incarnation")!=state.incarnation))refuse("continuous lineage physical/policy binding differs");
    const auto mode=db.query("PRAGMA journal_mode"),durability=db.query("PRAGMA synchronous");
    if(mode.size()!=1||text(mode[0],"journal_mode")!="wal"||durability.size()!=1||integer(durability[0],"synchronous")<2)refuse("continuous requires WAL and FULL/EXTRA synchronous");
}
} // namespace
