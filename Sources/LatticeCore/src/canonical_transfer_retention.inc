// Included only inside canonical_writer_adapter.cpp after its private context.
// No public callback or writable TLS switch exposes the closed operation frame.
namespace {
constexpr int retention_bootstrap=1, retention_reserve=2, retention_release=3, retention_expire=4, retention_prune=5;
using retention_inventory=std::map<std::pair<std::string,std::string>,std::string>;
constexpr const char* retention_profile_query="SELECT CASE WHEN typeof(id)='integer' THEN id END AS id,CASE WHEN typeof(version)='integer' THEN version END AS version,CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(next_attempt)='integer' THEN next_attempt END AS next_attempt,CASE WHEN typeof(max_attempts)='integer' THEN max_attempts END AS max_attempts,CASE WHEN typeof(max_duration_ms)='integer' THEN max_duration_ms END AS max_duration_ms,CASE WHEN typeof(main_device)='integer' THEN main_device END AS main_device,CASE WHEN typeof(main_inode)='integer' THEN main_inode END AS main_inode,CASE WHEN typeof(parent_device)='integer' THEN parent_device END AS parent_device,CASE WHEN typeof(parent_inode)='integer' THEN parent_inode END AS parent_inode,CASE WHEN typeof(custody_device)='integer' THEN custody_device END AS custody_device,CASE WHEN typeof(custody_inode)='integer' THEN custody_inode END AS custody_inode FROM main._lattice_canonical_retention LIMIT 2";
constexpr const char* retention_attempts_query="SELECT CASE WHEN typeof(id)='integer' THEN id END AS id,CASE WHEN typeof(incarnation)='integer' THEN incarnation END AS incarnation,CASE WHEN typeof(base)='integer' THEN base END AS base,CASE WHEN typeof(deadline_ms)='integer' THEN deadline_ms END AS deadline_ms FROM main._lattice_canonical_attempt LIMIT 1025";
retention_inventory retention_schema(const canonical_writer_profile& p,const canonical_namespace_profile* namespaces=nullptr) {
    retention_inventory out;
    out[{"table","_lattice_canonical_store"}]="CREATE TABLE _lattice_canonical_store (id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,source BLOB NOT NULL,epoch BLOB NOT NULL,scope BLOB NOT NULL,schema_id BLOB NOT NULL,head INTEGER NOT NULL,floor INTEGER NOT NULL,markers INTEGER NOT NULL,marker_bytes INTEGER NOT NULL,receipts INTEGER NOT NULL,receipt_bytes INTEGER NOT NULL,max_markers INTEGER NOT NULL,max_marker_bytes INTEGER NOT NULL,max_receipts INTEGER NOT NULL,max_receipt_bytes INTEGER NOT NULL,max_batch INTEGER NOT NULL,max_identity INTEGER NOT NULL,max_operation INTEGER NOT NULL) WITHOUT ROWID";
    out[{"table","_lattice_canonical_touch"}]="CREATE TABLE _lattice_canonical_touch (relation BLOB NOT NULL,identity BLOB NOT NULL,position INTEGER NOT NULL,charge INTEGER NOT NULL,PRIMARY KEY(relation,identity)) WITHOUT ROWID";
    out[{"index","_lattice_canonical_touch_position"}]="CREATE INDEX _lattice_canonical_touch_position ON _lattice_canonical_touch(position,relation,identity)";
    out[{"table","_lattice_canonical_receipt"}]="CREATE TABLE _lattice_canonical_receipt (original_id BLOB PRIMARY KEY NOT NULL,position INTEGER NOT NULL,outcome INTEGER NOT NULL,relation BLOB,identity BLOB,charge INTEGER NOT NULL) WITHOUT ROWID";
    if(namespaces) {
        auto& receipt=out.at({"table","_lattice_canonical_receipt"});
        const auto at=receipt.find(") WITHOUT ROWID");receipt.insert(at,",namespace_id BLOB NOT NULL");
        out[{"table","_lattice_canonical_namespace"}]="CREATE TABLE _lattice_canonical_namespace(namespace_id BLOB PRIMARY KEY NOT NULL,coverage_id BLOB NOT NULL,revision INTEGER NOT NULL,status INTEGER NOT NULL,is_local INTEGER NOT NULL) WITHOUT ROWID";
        for(const auto* action:{"INSERT","UPDATE","DELETE"}) {
            const auto name=std::string("_lattice_canonical_namespace_retention_")+action;
            // Catalog is immutable after enrollment, even to an admitted writer.
            out[{"trigger",name}]="CREATE TRIGGER "+name+" BEFORE "+action+" ON _lattice_canonical_namespace BEGIN"+
                demand("0")+" END";
        }
    }
    out[{"table","_lattice_canonical_coverage"}]="CREATE TABLE _lattice_canonical_coverage(id INTEGER PRIMARY KEY CHECK(id=1),manifest BLOB NOT NULL) WITHOUT ROWID";
    out[{"table","_lattice_canonical_retention"}]=
        "CREATE TABLE _lattice_canonical_retention(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,incarnation INTEGER NOT NULL,next_attempt INTEGER NOT NULL,max_attempts INTEGER NOT NULL,max_duration_ms INTEGER NOT NULL,main_device INTEGER NOT NULL,main_inode INTEGER NOT NULL,parent_device INTEGER NOT NULL,parent_inode INTEGER NOT NULL,custody_device INTEGER NOT NULL,custody_inode INTEGER NOT NULL) WITHOUT ROWID";
    out[{"table","_lattice_canonical_attempt"}]=
        "CREATE TABLE _lattice_canonical_attempt(id INTEGER PRIMARY KEY,incarnation INTEGER NOT NULL,base INTEGER NOT NULL,deadline_ms INTEGER NOT NULL) WITHOUT ROWID";
    out[{"index","_lattice_canonical_attempt_deadline"}]=
        "CREATE INDEX _lattice_canonical_attempt_deadline ON _lattice_canonical_attempt(deadline_ms,id)";
    for(const auto* table:{"_lattice_canonical_store","_lattice_canonical_touch","_lattice_canonical_receipt",
                          "_lattice_canonical_coverage","_lattice_canonical_retention","_lattice_canonical_attempt"}) {
        const bool closed=std::strcmp(table,"_lattice_canonical_retention")==0 || std::strcmp(table,"_lattice_canonical_attempt")==0;
        const auto condition=closed?"lattice_canonical_retention_guard_v1()=1":
            "lattice_canonical_guard_v1("+literal(p.binding.source)+","+literal(p.binding.epoch)+","+
            literal(p.binding.scope)+","+literal(p.binding.schema)+")=1";
        for(const auto* action:{"INSERT","UPDATE","DELETE"}) {
            const auto name=std::string(table)+"_retention_"+action;
            out[{"trigger",name}]="CREATE TRIGGER "+name+" BEFORE "+action+" ON "+table+" BEGIN"+
                demand(condition)+" END";
        }
    }
    return out;
}
void changed_retention(database& writer,int64_t expected=1) {
    if(writer.changes()!=expected)refuse("canonical retention write was ignored or changed unexpected rows");
}
constexpr const char* retention_inventory_query="SELECT type,CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,"
        "CASE WHEN typeof(sql)='text' AND length(CAST(sql AS BLOB))<=8192 THEN sql END AS sql FROM main.sqlite_master WHERE "
        "(type='table' AND substr(name,1,19)='_lattice_canonical_') "
        "OR (type='trigger' AND substr(tbl_name,1,19)='_lattice_canonical_') "
        "OR (tbl_name IN ('_lattice_canonical_retention','_lattice_canonical_attempt','_lattice_canonical_store',"
        "'_lattice_canonical_touch','_lattice_canonical_receipt','_lattice_canonical_coverage','_lattice_canonical_namespace') AND sql IS NOT NULL) LIMIT 32";
void retention_inventory_matches(const std::vector<database::row_t>& rows,const canonical_writer_profile& p,const canonical_namespace_profile* namespaces=nullptr) {
    const auto expected=retention_schema(p,namespaces);
    if(rows.size()!=expected.size())refuse("canonical retention object inventory differs");
    retention_inventory actual;
    for(const auto& row:rows)actual.emplace(std::pair{string(row,"type"),string(row,"name")},string(row,"sql"));
    if(actual!=expected)refuse("canonical retention missing, altered or extra schema/guard");
}
void retention_inventory_matches(database& writer,const canonical_writer_profile& p,const canonical_namespace_profile* namespaces=nullptr) {
    retention_inventory_matches(writer.query(retention_inventory_query),p,namespaces);
}
}
struct canonical_writer_adapter::retention_session {
    canonical_retention_limits limits;
    std::shared_ptr<const physical_store_identity> identity;
    int64_t main_device=0,main_inode=0,parent_device=0,parent_inode=0,custody_device=0,custody_inode=0;
    int64_t incarnation=0;
    std::chrono::steady_clock::time_point started=std::chrono::steady_clock::now();
    std::recursive_mutex mutex;
    bool busy=false;
#if defined(__APPLE__) || defined(__linux__)
    // These are directory descriptors only, including every failure path.
    // Closing an independently opened SQLite main-file fd would release the
    // process's POSIX SQLite locks, even after a rejected competing attachment.
    int parent_fd=-1,custody_fd=-1;
    bool locked=false;
    std::string parent_path,main_name,custody_name;
    ~retention_session() {
        if(custody_fd>=0){if(locked)::flock(custody_fd,LOCK_UN);::close(custody_fd);}
        if(parent_fd>=0)::close(parent_fd);
        // Never unlink: an existing profile binds this persistent directory.
    }
    static int64_t bounded_identity(uint64_t value) {
        if(value>static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
            refuse("canonical retention physical identity exceeds durable integer bound");
        return static_cast<int64_t>(value);
    }
    static bool same_file(const struct stat& a,const struct stat& b) {
        return a.st_dev==b.st_dev && a.st_ino==b.st_ino;
    }
    void verify_file(database& writer)const {
        const auto current=writer.physical_identity("main",{},true);
        struct stat parent{},parent_path_now{},custody{},custody_path_now{},main{};
        if(!identity || !current || !(*identity==*current) || identity->filename!=current->filename ||
           parent_fd<0 || custody_fd<0 || !locked ||
           ::fstat(parent_fd,&parent)!=0 || !S_ISDIR(parent.st_mode) ||
           ::lstat(parent_path.c_str(),&parent_path_now)!=0 || !S_ISDIR(parent_path_now.st_mode) || !same_file(parent,parent_path_now) ||
           ::fstat(custody_fd,&custody)!=0 || !S_ISDIR(custody.st_mode) || custody.st_uid!=::geteuid() || (custody.st_mode&07777)!=0700 ||
           ::fstatat(parent_fd,custody_name.c_str(),&custody_path_now,AT_SYMLINK_NOFOLLOW)!=0 ||
           !S_ISDIR(custody_path_now.st_mode) || !same_file(custody,custody_path_now) ||
           ::fstatat(parent_fd,main_name.c_str(),&main,AT_SYMLINK_NOFOLLOW)!=0 || !S_ISREG(main.st_mode) || main.st_nlink!=1 ||
           bounded_identity(main.st_dev)!=main_device || bounded_identity(main.st_ino)!=main_inode ||
           bounded_identity(parent.st_dev)!=parent_device || bounded_identity(parent.st_ino)!=parent_inode ||
           bounded_identity(custody.st_dev)!=custody_device || bounded_identity(custody.st_ino)!=custody_inode)
            refuse("canonical retention main, parent or directory custody identity changed");
    }
#else
    void verify_file(database&)const {refuse("canonical retention directory custody platform unqualified");}
#endif
    void verify_binding(const database::row_t& row)const {
        if(integer(row,"main_device")!=main_device || integer(row,"main_inode")!=main_inode ||
           integer(row,"parent_device")!=parent_device || integer(row,"parent_inode")!=parent_inode ||
           integer(row,"custody_device")!=custody_device || integer(row,"custody_inode")!=custody_inode)
            refuse("canonical retention durable directory custody binding differs");
    }
    int64_t now()const {
        const auto value=std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-started).count();
        if(value<0 || value>std::numeric_limits<int64_t>::max()-86400000)refuse("canonical source clock exhausted");
        return value;
    }
};
struct canonical_writer_adapter::retention_frame {
    context* state;
    int operation;
    bool bootstrap;
    retention_frame* previous;
    static thread_local retention_frame* current;
    retention_frame(context& s,int op):state(&s),operation(op),bootstrap(op==retention_bootstrap),previous(current){current=this;}
    ~retention_frame(){current=previous;}
    retention_frame(const retention_frame&)=delete;
};
thread_local canonical_writer_adapter::retention_frame* canonical_writer_adapter::retention_frame::current=nullptr;
thread_local const canonical_upstream_test_hooks::authorizer_fault* canonical_retention_test_hooks::fault=nullptr;
void canonical_writer_adapter::context::admit_retention(sqlite3_context* sql,int count,sqlite3_value**) noexcept {
    auto& self=**static_cast<std::shared_ptr<context>*>(sqlite3_user_data(sql));
    const auto* frame=retention_frame::current;
    const bool valid=count==0 && frame && frame->state==&self && !self.retention.expired() &&
        sqlite3_context_db_handle(sql)==self.connection &&
        (frame->bootstrap || (self.active->load(std::memory_order_acquire) &&
            recovery_writer_access::active_install_for(self.owner,self.connection)));
    if(!valid)sqlite3_result_error(sql,"canonical retention closed operation required",-1);
    else sqlite3_result_int(sql,1);
}
bool canonical_writer_adapter::context::authorize_retention(context& self,int action,const char* table,const char* column) noexcept {
    const auto* frame=retention_frame::current;
    if(!frame || frame->state!=&self || frame->bootstrap ||
       !recovery_writer_access::active_install_for(self.owner,self.connection))return false;
    if(std::strcmp(table,"_lattice_canonical_attempt")==0)
        return (action==SQLITE_INSERT && frame->operation==retention_reserve) ||
            (action==SQLITE_DELETE && (frame->operation==retention_release || frame->operation==retention_expire || frame->operation==retention_prune));
    if(std::strcmp(table,"_lattice_canonical_retention")==0)
        return action==SQLITE_UPDATE && column && std::strcmp(column,"next_attempt")==0 && frame->operation==retention_reserve;
    if(frame->operation!=retention_prune)return false;
    if(std::strcmp(table,"_lattice_canonical_touch")==0)return action==SQLITE_DELETE;
    if(std::strcmp(table,"_lattice_canonical_store")!=0 || action!=SQLITE_UPDATE || !column)return false;
    for(const auto* field:{"head","floor","markers","marker_bytes","receipts","receipt_bytes"})if(std::strcmp(column,field)==0)return true;
    return false;
}
void canonical_writer_adapter::prepare_retention(lattice_db& owner,const canonical_retention_limits& limits) {
#if defined(__APPLE__) || defined(__linux__)
    if(limits.attempts<=0 || limits.attempts>1024 || limits.duration_ms<=0 || limits.duration_ms>86400000 ||
       owner.config_.is_in_memory() || owner.config_.read_only || owner.config_.audit_retention_seconds!=0)
        refuse("canonical retention requires bounded limits and a file writer without automatic audit retention");
    const auto mode=writer_->query("PRAGMA journal_mode");
    if(mode.size()!=1 || string(mode[0],"journal_mode")!="wal")refuse("canonical retention requires file WAL");
    auto session=std::make_shared<retention_session>();session->limits=limits;
    session->identity=writer_->physical_identity("main",{},true);
    if(!session->identity)refuse("canonical retention actual physical file identity unavailable");
    session->main_device=retention_session::bounded_identity(session->identity->device);
    session->main_inode=retention_session::bounded_identity(session->identity->inode);
    const std::filesystem::path path(session->identity->filename);
    session->parent_path=path.parent_path().string();session->main_name=path.filename().string();
    session->custody_name=".lattice-retention-"+std::to_string(session->main_device)+"-"+std::to_string(session->main_inode);
    const bool existing=writer_->table_exists("_lattice_canonical_retention");
    if(existing!=writer_->table_exists("_lattice_canonical_coverage"))
        refuse("canonical retention refuses implicit legacy profile transition");
    // O_DIRECTORY applies to every acquired fd, not just a post-open check.
    // A regular-file/hardlink/symlink substitution can never yield a main/WAL/
    // SHM fd that our error cleanup would close outside SQLite bookkeeping.
    constexpr int directory_flags=O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC;
    session->parent_fd=::open(session->parent_path.c_str(),directory_flags);
    if(session->parent_fd<0)refuse("canonical retention canonical parent directory unavailable");
    struct stat parent{},custody{};
    if(::fstat(session->parent_fd,&parent)!=0 || !S_ISDIR(parent.st_mode))
        refuse("canonical retention canonical parent is not a directory");
    session->parent_device=retention_session::bounded_identity(parent.st_dev);
    session->parent_inode=retention_session::bounded_identity(parent.st_ino);
    if(!existing && ::mkdirat(session->parent_fd,session->custody_name.c_str(),0700)!=0 && errno!=EEXIST)
        refuse("canonical retention first custody directory creation failed");
    // Existing profiles never recreate an absent artifact or adopt a new inode.
    session->custody_fd=::openat(session->parent_fd,session->custody_name.c_str(),directory_flags);
    if(session->custody_fd<0 || ::fstat(session->custody_fd,&custody)!=0 || !S_ISDIR(custody.st_mode) ||
       custody.st_uid!=::geteuid() || (custody.st_mode&07777)!=0700)
        refuse("canonical retention missing or malformed custody directory");
    session->custody_device=retention_session::bounded_identity(custody.st_dev);
    session->custody_inode=retention_session::bounded_identity(custody.st_ino);
    if(::flock(session->custody_fd,LOCK_EX|LOCK_NB)!=0)
        refuse("canonical retention source session already owned or directory locking unavailable");
    session->locked=true;
    session->verify_file(*writer_);
    if(existing) {
        retention_inventory_matches(*writer_,context_->profile,context_->namespace_profile());
        const auto rows=writer_->query(retention_profile_query);
        if(rows.size()!=1 || integer(rows[0],"id")!=1 || integer(rows[0],"version")!=2)
            refuse("canonical retention unsupported profile version; no migration or rebind");
        session->verify_binding(rows[0]);
    }
    // Runtime success is required on both admitted POSIX platforms. Unsupported
    // directory flock/fsync fails closed; source review is not qualification.
    // Re-sync a safe orphan from failed first enrollment before adopting it.
    if(::fsync(session->custody_fd)!=0 || ::fsync(session->parent_fd)!=0)
        refuse("canonical retention durable directory custody unavailable");
    session->verify_file(*writer_);
    retention_=std::move(session);context_->retention=retention_;
#else
    (void)owner;(void)limits;refuse("canonical retention file custody platform unqualified");
#endif
}
void canonical_writer_adapter::enroll_retention(lattice_db& owner,bool reopen) {
    auto& writer=*writer_;auto& session=*retention_;
    // A legacy coverage or v1 profile is never silently promoted. Profile v2
    // binds all three physical identities on first coverage enrollment only.
    const bool existing=writer.table_exists("_lattice_canonical_retention");
    if(reopen!=existing)refuse("canonical retention refuses implicit legacy profile transition");
    if(recovery_writer_access::active_writer(owner)!=&writer)refuse("canonical retention enrollment lost actual writer");
    if(!existing) {
        if(!writer.query("SELECT 1 FROM main.sqlite_master WHERE name IN ('_lattice_canonical_attempt','_lattice_canonical_attempt_deadline') "
            "OR (type='trigger' AND substr(tbl_name,1,19)='_lattice_canonical_') LIMIT 1").empty())
            refuse("canonical retention refuses partial enrollment");
        const auto schema=retention_schema(context_->profile,context_->namespace_profile());
        for(const auto& [key,sql]:schema)if(key.first=="table" && (key.second=="_lattice_canonical_retention" || key.second=="_lattice_canonical_attempt"))writer.execute(sql);
        for(const auto& [key,sql]:schema)if(key.first=="index" && key.second=="_lattice_canonical_attempt_deadline")writer.execute(sql);
        writer.execute("INSERT INTO main._lattice_canonical_retention VALUES(1,2,0,0,?,?,?,?,?,?,?,?)",{session.limits.attempts,session.limits.duration_ms,
            session.main_device,session.main_inode,session.parent_device,session.parent_inode,session.custody_device,session.custody_inode});
        changed_retention(writer);
        for(const auto& [key,sql]:schema)if(key.first=="trigger")writer.execute(sql);
    }
    retention_inventory_matches(writer,context_->profile,context_->namespace_profile());
    const auto rows=writer.query(retention_profile_query);
    if(rows.size()!=1 || integer(rows[0],"id")!=1 || integer(rows[0],"version")!=2 || integer(rows[0],"incarnation")<0 ||
       integer(rows[0],"incarnation")==std::numeric_limits<int64_t>::max() || integer(rows[0],"next_attempt")<0 ||
       integer(rows[0],"max_attempts")!=session.limits.attempts || integer(rows[0],"max_duration_ms")!=session.limits.duration_ms)
        refuse("canonical retention version, limits or incarnation mismatch");
    session.verify_file(writer);session.verify_binding(rows[0]);
    const auto prior=integer(rows[0],"incarnation");
    const auto stale=writer.query(retention_attempts_query);
    if(stale.size()>static_cast<size_t>(session.limits.attempts))refuse("canonical retention prior attempt capacity exceeded");
    const auto state=canonical_change_store(owner,context_->binding,context_->profile.limits,context_->namespace_profile()).state();
    for(const auto& row:stale)if(integer(row,"id")<=0 || integer(row,"id")>integer(rows[0],"next_attempt") ||
        integer(row,"incarnation")!=prior || integer(row,"base")<state.floor || integer(row,"base")>state.head || integer(row,"deadline_ms")<0)
        refuse("canonical retention prior attempt malformed");
    retention_frame authority(*context_,retention_bootstrap);
    // Exclusive bound directory custody proves the prior cooperating session (including its
    // in-flight captures) has ended. There are no READY/advertised rows here.
    writer.execute("DELETE FROM main._lattice_canonical_attempt");changed_retention(writer,static_cast<int64_t>(stale.size()));
    writer.execute("UPDATE main._lattice_canonical_retention SET incarnation=incarnation+1,next_attempt=0 WHERE id=1 AND incarnation=?",{prior});
    changed_retention(writer);session.incarnation=prior+1;
    verify_retention(writer,*context_,session);
}
void canonical_writer_adapter::verify_retention(database& writer,const context& state,const retention_session& session) {
    session.verify_file(writer);
    retention_inventory_matches(writer,state.profile,state.namespace_profile());
    const auto rows=writer.query(retention_profile_query);
    if(rows.size()!=1 || integer(rows[0],"id")!=1 || integer(rows[0],"version")!=2 || integer(rows[0],"incarnation")!=session.incarnation ||
       integer(rows[0],"next_attempt")<0 || integer(rows[0],"max_attempts")!=session.limits.attempts || integer(rows[0],"max_duration_ms")!=session.limits.duration_ms)
        refuse("canonical retention durable profile differs from this incarnation");
    session.verify_binding(rows[0]);
    const auto attempts=writer.query(retention_attempts_query);
    if(attempts.size()>static_cast<size_t>(session.limits.attempts))refuse("canonical retention attempt capacity exceeded");
    const auto bounds=canonical_change_store(*state.owner,state.binding,state.profile.limits,state.namespace_profile()).state();
    for(const auto& row:attempts)if(integer(row,"id")<=0 || integer(row,"id")>integer(rows[0],"next_attempt") ||
        integer(row,"incarnation")!=session.incarnation || integer(row,"base")<bounds.floor ||
        integer(row,"base")>bounds.head || integer(row,"deadline_ms")<0)
        refuse("canonical retention addressed registry state malformed");
}
recovery_install_result canonical_writer_adapter::retention_owned(std::shared_ptr<lattice_db> owner,
    std::shared_ptr<database> writer,std::shared_ptr<context> state,std::shared_ptr<retention_session> session,
    int operation,const std::function<void(database&,retention_session&)>& body) {
    recovery_install_result refused;
    try {
        if(!owner || !state || !session || state->owner!=owner.get())refuse("canonical retention requires this admitted actual owner");
        std::lock_guard<std::recursive_mutex> lock(session->mutex);
        if(session->busy)refuse("canonical retention nested operation or capture refused");
        session->verify_file(*writer); // refuse detected movement before BEGIN as well as under the owned turn
        session->busy=true;
        struct idle {retention_session& value;~idle(){value.busy=false;}} idle_guard{*session};
        return recovery_writer_access::install(owner,[&](database& actual){
            {
                std::lock_guard<std::mutex> publication(owner->connection_ownership_mutex_);
                if(owner->closed_.load() || owner->db_!=writer || &actual!=writer.get() || !state->active->load(std::memory_order_acquire) ||
                   !matches_connection(actual,state->connection))refuse("canonical retention source retired or writer replaced");
            }
            verify_retention(actual,*state,*session);
            retention_frame authority(*state,operation);
            body(actual,*session);
            verify_retention(actual,*state,*session);
            if(!state->active->load(std::memory_order_acquire))refuse("canonical retention retired before owned settlement");
        });
    } catch(...) {refused.primary_error=std::current_exception();return refused;}
}
std::unique_ptr<canonical_writer_adapter> canonical_writer_adapter::attach_retention_for_qualification(
    std::shared_ptr<lattice_db> owner,const canonical_writer_profile& profile,canonical_retention_limits limits) {
    if(!owner || profile.upstream_requested)refuse("canonical retention requires retained source; upstream/serving integration is unavailable");
    return std::unique_ptr<canonical_writer_adapter>(new canonical_writer_adapter(*owner,profile,nullptr,&limits));
}
canonical_retention_result canonical_writer_adapter::reserve_recovery_owned(std::shared_ptr<lattice_db> owner,
    std::optional<int64_t> base,int64_t duration_ms) {
    auto state=context_;auto writer=writer_;auto session=retention_;
    canonical_retention_result result;
    std::optional<canonical_retention_ticket> prepared;
    result.settlement=retention_owned(owner,writer,state,session,retention_reserve,[&](database& actual,retention_session& clock){
        if(duration_ms<=0 || duration_ms>clock.limits.duration_ms)refuse("canonical retention finite duration outside source policy");
        canonical_change_store store(*owner,state->binding,state->profile.limits,state->namespace_profile());store.audit();const auto bounds=store.state();
        const auto protection=base.value_or(bounds.head);store.require_base(protection);
        const auto prior=actual.query("SELECT CASE WHEN typeof(next_attempt)='integer' THEN next_attempt END AS next_attempt FROM main._lattice_canonical_retention WHERE id=1");
        const auto previous=integer(prior.at(0),"next_attempt");
        if(previous==std::numeric_limits<int64_t>::max())refuse("canonical retention attempt sequence exhausted");
        const auto count=actual.query("SELECT COUNT(*) AS n FROM main._lattice_canonical_attempt");
        if(integer(count.at(0),"n")>=clock.limits.attempts)refuse("canonical retention finite capacity exhausted; explicit expiry required");
        const auto id=previous+1, deadline=clock.now()+duration_ms;
        actual.execute("UPDATE main._lattice_canonical_retention SET next_attempt=? WHERE id=1 AND next_attempt=?",{id,previous});changed_retention(actual);
        actual.execute("INSERT INTO main._lattice_canonical_attempt VALUES(?,?,?,?)",{id,clock.incarnation,protection,deadline});changed_retention(actual);
        const auto rows=actual.query("SELECT id,incarnation,base,deadline_ms FROM main._lattice_canonical_attempt WHERE id=?",{id});
        if(rows.size()!=1 || integer(rows[0],"id")!=id || integer(rows[0],"incarnation")!=clock.incarnation ||
            integer(rows[0],"base")!=protection || integer(rows[0],"deadline_ms")!=deadline ||
            integer(actual.query("SELECT CASE WHEN typeof(next_attempt)='integer' THEN next_attempt END AS next_attempt FROM main._lattice_canonical_retention WHERE id=1").at(0),"next_attempt")!=id)
            refuse("canonical retention reservation postimage differs");
        prepared=canonical_retention_ticket{session,clock.incarnation,id,protection,deadline,base};
    });
    if(result.settlement.state==recovery_install_state::committed)result.reservation=std::move(prepared);
    return result;
}
recovery_install_result canonical_writer_adapter::release_recovery_owned(std::shared_ptr<lattice_db> owner,const canonical_retention_ticket& ticket) {
    auto session=retention_;
    return retention_owned(std::move(owner),writer_,context_,session,retention_release,[&](database& actual,retention_session& clock){
        if(ticket.session_.lock()!=session || ticket.incarnation_!=clock.incarnation)refuse("canonical retention old or foreign incarnation");
        actual.execute("DELETE FROM main._lattice_canonical_attempt WHERE id=? AND incarnation=? AND base=? AND deadline_ms=?",
            {ticket.id_,ticket.incarnation_,ticket.base_,ticket.deadline_});
        // Exact retries are harmless only when the addressed id is truly absent.
        if(actual.changes()>1 || !actual.query("SELECT 1 FROM main._lattice_canonical_attempt WHERE id=?",{ticket.id_}).empty())
            refuse("canonical retention release ignored or mismatched");
    });
}
namespace {
void expire_retention(database& writer,int64_t now) {
    const auto expected=integer(writer.query("SELECT COUNT(*) AS n FROM main._lattice_canonical_attempt WHERE deadline_ms<=?",{now}).at(0),"n");
    writer.execute("DELETE FROM main._lattice_canonical_attempt WHERE deadline_ms<=?",{now});changed_retention(writer,expected);
    if(!writer.query("SELECT 1 FROM main._lattice_canonical_attempt WHERE deadline_ms<=? LIMIT 1",{now}).empty())refuse("canonical retention expiry postimage differs");
}
}
recovery_install_result canonical_writer_adapter::expire_recovery_owned(std::shared_ptr<lattice_db> owner) {
    return retention_owned(std::move(owner),writer_,context_,retention_,retention_expire,[](database& actual,retention_session& clock){expire_retention(actual,clock.now());});
}
recovery_install_result canonical_writer_adapter::prune_recovery_owned(std::shared_ptr<lattice_db> owner,int64_t floor) {
    auto state=context_;
    return retention_owned(owner,writer_,state,retention_,retention_prune,[&](database& actual,retention_session& clock){
        expire_retention(actual,clock.now());
        canonical_change_store store(*owner,state->binding,state->profile.limits,state->namespace_profile());store.audit();
        const auto old=store.state();
        const auto rows=actual.query("SELECT COALESCE(MIN(base),?) AS ceiling FROM main._lattice_canonical_attempt",{old.head});
        // The only protection input is this registry's committed rows under the
        // same owned writer transaction. Receipts remain independently retained.
        store.advance_floor(floor,integer(rows.at(0),"ceiling"));
    });
}
sync_recovery::owned_canonical_capture canonical_writer_adapter::capture_reserved_owned(std::shared_ptr<lattice_db> owner,
    const canonical_retention_ticket& ticket,
    const std::vector<sync_recovery::canonical_capture_request>& requests,const sync_recovery::canonical_capture_limits& limits) {
    if(context_ && context_->namespaces)refuse("canonical namespaced capture requires admission");
    return capture_reserved_impl(std::move(owner),ticket,requests,limits,{});
}
sync_recovery::owned_canonical_capture canonical_writer_adapter::capture_reserved_namespaced_owned(std::shared_ptr<lattice_db> owner,
    const canonical_namespace_admission& admission,const canonical_retention_ticket& ticket,
    const std::vector<sync_recovery::canonical_capture_request>& requests,const sync_recovery::canonical_capture_limits& limits) {
    const auto held=admission;
    return capture_reserved_impl(std::move(owner),ticket,requests,limits,{},&held);
}
sync_recovery::owned_canonical_capture canonical_writer_adapter::capture_reserved_impl(std::shared_ptr<lattice_db> owner,
    const canonical_retention_ticket& ticket,const std::vector<sync_recovery::canonical_capture_request>& requests,
    const sync_recovery::canonical_capture_limits& limits,const std::function<void(size_t,uint64_t)>& after_batch,
    const canonical_namespace_admission* admission) {
    auto state=context_;auto session=retention_;auto writer=writer_;
    const auto requested_base=ticket.requested_base_;
    if(!session || !state || !owner || state->owner!=owner.get() || ticket.session_.lock()!=session || ticket.incarnation_!=session->incarnation)
        refuse("canonical reserved capture old or foreign source incarnation");
    std::lock_guard<std::recursive_mutex> lock(session->mutex);
    if(session->busy || session->now()>=ticket.deadline_)
        refuse("canonical reserved capture expired, nested or requested base changed");
    session->busy=true;struct idle {retention_session& value;~idle(){value.busy=false;}} idle_guard{*session};
    const auto validate=[&](uint64_t generation){
        if(session->now()>=ticket.deadline_)refuse("canonical reservation expired before capture");
        session->verify_file(*writer);
        const auto inventory=owner->query_at_generation(generation,retention_inventory_query);
        if(!inventory)refuse("canonical retention schema generation unavailable");
        retention_inventory_matches(*inventory,state->profile,state->namespace_profile());
        const auto profile=owner->query_at_generation(generation,retention_profile_query);
        if(!profile || profile->size()!=1 || integer(profile->at(0),"version")!=2 ||
           integer(profile->at(0),"incarnation")!=session->incarnation)
            refuse("canonical capture retention profile differs");
        session->verify_binding(profile->at(0));
        const auto rows=owner->query_at_generation(generation,"SELECT a.id FROM main._lattice_canonical_attempt a JOIN main._lattice_canonical_retention r "
            "ON r.id=1 AND r.version=2 AND r.incarnation=a.incarnation WHERE a.id=? AND a.incarnation=? AND a.base=? AND a.deadline_ms=?",
            {ticket.id_,ticket.incarnation_,ticket.base_,ticket.deadline_});
        if(!rows || rows->size()!=1)refuse("canonical capture has no exact committed reservation");
    };
    const auto before_decision=[&]{if(session->now()>=ticket.deadline_)refuse("canonical reservation expired during capture");};
    auto result=capture_recovery_impl(owner,state->binding,requested_base,requests,limits,after_batch,before_decision,{},validate,admission);
    if(!result.capture || result.head<ticket.base_)refuse("canonical reservation did not protect this coherent capture");
    return result; // actual generation custody ended before the session unlock
}
