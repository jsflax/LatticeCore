// Included only after the actual producer context/bootstrap implementation.
void recovery_local_producer_adapter::compile_continuous(lattice_db& owner,context& admitted) {
    std::map<std::string,std::vector<context::profile*>> relations;
    for(auto& p:admitted.profiles)for(const auto& [name,t]:p.schema.tables)relations[name].push_back(&p);
    size_t aggregate=0;
    for(auto& [name,contributions]:relations) {
        auto& first=contributions.front()->schema.tables.at(name);std::string tail,guards,install_guards;
        for(auto* contribution:contributions) {
            auto& table=contribution->schema.tables.at(name);const auto& p=contribution->stored;
            if(table.ddl!=first.ddl||table.columns!=first.columns||table.no_history!=first.no_history||table.link!=first.link||table.ordinary!=first.ordinary)
                refuse("continuous shared relation descriptor differs");
            const auto emitted=recovery_obligation_producer_store::compile(p,admitted.admitted_limits.obligations,admitted.admitted_limits.producers,continuous_receiver_enabled(*admitted.continuous->state)).emit_tail(name,table.link,max_sql);
            if(emitted.size()>max_sql-tail.size())refuse("continuous combined generated tail bound");tail+=emitted;
            if(!guards.empty())guards+=" AND ";
            if(!install_guards.empty())install_guards+=" AND ";
            install_guards+="lattice_recovery_producer_guard_v1("+literal(p.contribution.binding.channel)+","+std::to_string(p.contribution_incarnation)+","+
                std::to_string(p.program_revision)+","+literal(p.program_digest)+","+literal(name)+",2)=1";
            guards+="lattice_recovery_producer_guard_v1("+literal(p.contribution.binding.channel)+","+std::to_string(p.contribution_incarnation)+","+
                std::to_string(p.program_revision)+","+literal(p.program_digest)+","+literal(name)+",1)=1";
        }
        // Every contribution tail follows the SAME genuine AuditLog insert.
        // WITHOUT ROWID metadata preserves last_insert_rowid across tails.
        if(continuous_receiver_enabled(*admitted.continuous->state)) {
            // Every unresolved original owns at least one actual journal row.
            // A small physical row count therefore proves this same hard cap
            // without enumerating distinct originals. For larger journals use
            // the exact original test, including settled/shared-row semantics.
            // Both branches read this transaction's post-tail SQLite state;
            // no cached counter, memory flag, metadata claim or extra write.
            if(admitted.legacy_distinct_count_programs)
                tail+=require("(SELECT COUNT(*) FROM (SELECT DISTINCT original FROM _lattice_obligation_entry WHERE stage<>2 ORDER BY original LIMIT 8193))<=8192");
            else
                tail+=require("CASE WHEN (SELECT COUNT(*) FROM _lattice_obligation_entry)<=8192 THEN 1 ELSE (SELECT COUNT(*) FROM (SELECT DISTINCT original FROM _lattice_obligation_entry WHERE stage<>2 ORDER BY original LIMIT 8193))<=8192 END");
        }
        std::vector<std::string> emitted;
        if(first.link)owner.create_link_table_triggers(name,tail,&emitted);
        else owner.create_model_table_triggers(name,first.columns,first.no_history,tail,&emitted);
        for(const char* operation:{"INSERT","UPDATE","DELETE"}) {
            const std::string op(operation),key=op=="DELETE"?"OLD.globalId":"NEW.globalId";
            std::string check="sync_disabled()=0 AND ("+guards+") AND (SELECT phase FROM main._lattice_producer_continuity WHERE id=1)=0 AND lattice_recovery_producer_uuid_v1("+key+") IS NOT NULL";
            if(continuous_receiver_enabled(*admitted.continuous->state))check="(("+check+") OR (sync_disabled()=1 AND ("+install_guards+") AND lattice_recovery_producer_uuid_v1("+key+") IS NOT NULL))";
            if(op=="UPDATE")check+=" AND CAST(OLD.globalId AS BLOB) IS CAST(NEW.globalId AS BLOB)"+(first.link?std::string(" AND OLD.lhs IS NEW.lhs AND OLD.rhs IS NEW.rhs"):std::string(" AND OLD.id IS NEW.id"));
            emitted.push_back("CREATE TRIGGER _lattice_local_producer_"+name+"_"+op+" BEFORE "+op+" ON "+name+" BEGIN"+require(check)+" END");
        }
        programs complete;
        for(auto& sql:emitted){if(sql.size()>max_sql||sql.size()>max_manifest-aggregate)refuse("continuous generated SQL inventory bound");aggregate+=sql.size();const auto key=program_name(sql);complete.emplace(key,normalize_sql(std::move(sql)));}
        for(auto* contribution:contributions)contribution->schema.tables.at(name).enrolled=complete;
    }
}
void require_continuous_path_unowned(const std::string& value) {
    if(value.empty()||configuration::path_is_memory(value))return;
    std::string decoded=value;
    if(decoded.starts_with("file:")) {
        decoded=decoded.substr(5,decoded.find('?')==std::string::npos?std::string::npos:decoded.find('?')-5);
        if(decoded.starts_with("//localhost/"))decoded.erase(0,11);
        else if(decoded.starts_with("//"))refuse("continuous preflight unsupported URI authority");
        std::string path;path.reserve(decoded.size());
        const auto hex=[](char c)->int{if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;if(c>='A'&&c<='F')return c-'A'+10;return -1;};
        for(size_t i=0;i<decoded.size();++i){if(decoded[i]!='%'){path+=decoded[i];continue;}
            if(i+2>=decoded.size()||hex(decoded[i+1])<0||hex(decoded[i+2])<0)refuse("continuous malformed URI escape");
            const char c=static_cast<char>(hex(decoded[i+1])*16+hex(decoded[i+2]));if(!c)refuse("continuous URI NUL refused");path+=c;i+=2;}
        decoded=std::move(path);
    }
    const auto lexical=std::filesystem::absolute(decoded).lexically_normal();
    if(continuous_reserved(lexical)||continuous_reserved(std::filesystem::weakly_canonical(lexical)))
        refuse("continuous container requires retained owner factory");
}
void recovery_continuous_producer::classify_open(database& writer,bool admitted,bool writable) {
    // Writable open is unpublished, before ordinary pragma/schema effects.
    // Deferred read-only classification owns this connection's SQLite mutex.
    // The reserved-path check already precedes ordinary writable open.
    // There is NO online enrollment/upgrade of existing files, so an ordinary
    // legacy writer cannot acquire this profile later. Preserve its hot-path
    // mutex and transaction behavior; this state is denial-only, never proof.
    bool present=false;
    if(writer.delivery_startup_) {
        auto statement=observer_delivery_initialization_sql::fixed_(*writer.delivery_startup_,writer,
            observer_delivery_initialization_sql::recipe::continuous_schema_discovery);
        present=!writer.query_initialization_(statement).empty();
    }else {
        discovery_statement query(writer.internal_handle(),"SELECT 1 FROM main.sqlite_schema WHERE name='_lattice_producer_continuity' LIMIT 1");
        present=query.row();
    }
    bool reserved=false;
    try {require_continuous_path_unowned(writer.path_);}catch(const db_error&){reserved=true;}
    const bool protected_file=admitted||present||reserved;
    // Publish only after every read/path check succeeds. A failed probe leaves
    // unknown so a subsequent guarded boundary retries rather than assuming
    // absence. No read-only constructor/keeper query needs this probe.
    writer.continuous_file_.store(protected_file?database::continuous_classification::protected_file:
        database::continuous_classification::ordinary,std::memory_order_release);
    if(writable&&protected_file&&!admitted)refuse("continuous file requires retained constructor custody");
}
void recovery_continuous_producer::require_no_continuous_export(database& writer) {
    store_physical_native_scope physical_call(writer);
    if(!physical_call.entered())refuse("resource Native caller original unavailable");
    using classification=database::continuous_classification;
    auto known=writer.continuous_file_.load(std::memory_order_acquire);
    if(known==classification::unknown) {
        auto* db=writer.internal_handle();
        if(!db)refuse("continuous classification requires a live physical connection");
        auto* mutex=sqlite3_db_mutex(db);sqlite3_mutex_enter(mutex);
        struct unlock {sqlite3_mutex* mutex;~unlock(){sqlite3_mutex_leave(mutex);}} release{mutex};
        known=writer.continuous_file_.load(std::memory_order_acquire);
        if(known==classification::unknown) {
            classify_open(writer,false,false);
            known=writer.continuous_file_.load(std::memory_order_acquire);
        }
    }
    if(known!=classification::ordinary)refuse("continuous store refuses unclaimed legacy export/raw access");
}
void require_continuous_legacy_export_absent(database& writer){recovery_continuous_producer::require_no_continuous_export(writer);}
void require_continuous_raw_handle_absent(database& writer){recovery_continuous_producer::require_no_continuous_export(writer);}
bool recovery_continuous_producer::attached(const lattice_db& owner)noexcept{return static_cast<bool>(owner.recovery_continuous_);}
bool recovery_continuous_producer::shared_install_domains(const lattice_db& owner)noexcept{return owner.recovery_continuous_&&continuous_receiver_enabled(*owner.recovery_continuous_->state);}
void recovery_continuous_producer::require_no_continuous_route(lattice_db& owner) {
    if(owner.recovery_continuous_)refuse("continuous unsupported route before publication");
    require_no_continuous_export(*owner.db_);
}
void recovery_continuous_producer::append_initialization_projection_(ordinary_schema_projection& result,
    const recovery_obligation_bootstrap_reader& initialization) {
    // This is the SAME compiled durable table and guards used by enrollment,
    // not decoded stored SQL, observed names, a grant digest or caller recipe.
    // SELECT-only bootstrap evidence confers NO trigger-origin/DML authority.
    initialization.claim_workspace_(32768); // before any strings/columns/programs
    ordinary_projection_table table;table.name=continuous_table;table.create_sql=continuous_ddl;
    table.role=ordinary_projection_role::link;table.local_id=false;
    constexpr const char* columns[]={"id","version","manifest","digest","main_device","main_inode",
        "parent_device","parent_inode","container_device","container_inode","incarnation","barrier","attempt","phase"};
    table.columns.reserve(sizeof(columns)/sizeof(columns[0]));
    for(const auto* name:columns)table.columns.push_back({name,
        std::strcmp(name,"manifest")==0||std::strcmp(name,"digest")==0?column_type::blob:column_type::integer,false,false,{}});
    for(const auto& [name,program]:continuous_guards_storage<ordinary_initialization_text,ordinary_initialization_text_map<ordinary_initialization_text>>())table.other_programs.push_back(program);
    std::size_t bytes=table.name.size()+table.create_sql.size();
    for(const auto& column:table.columns)bytes+=column.name.size();
    for(const auto& program:table.other_programs)bytes+=program.size();
    if(bytes>32768||result.logical_bytes>std::numeric_limits<std::size_t>::max()-bytes||
       result.columns>std::numeric_limits<std::size_t>::max()-table.columns.size())
        refuse("continuous compiled bootstrap projection bound");
    result.logical_bytes+=bytes;result.columns+=table.columns.size();
    if(!result.tables.emplace(table.name,std::move(table)).second)
        refuse("continuous compiled bootstrap table overlaps actual catalog");
}
void recovery_continuous_producer::verify_initializing_lineage_(database& writer,recovery_continuous_state& state) {
    const auto issuer=database_native_process();
    writer.require_creator_();
    if(!writer.delivery_startup_)refuse("continuous private lineage read requires its actual pending Open");
    auto initialization=recovery_obligation_bootstrap_reader::create_(writer,discovery_caps,continuous_receiver_enabled(state));
    const auto current=[&]{writer.require_creator_();if(issuer!=database_native_process())refuse("inherited continuous lineage read return");};
    const auto autocommit=[&]{current();database::catalog_native_admission_ leaf(writer);return sqlite3_get_autocommit(writer.db_)!=0;};
    try {
        initialization->begin_();current();
        initialization->validated_family_();current();
        // Actual inventory, all fourteen typed durable cells, physical/policy
        // lineage, WAL and FULL checks stay in the original verifier. This
        // reader pins ONE genuine private raw snapshot through every read.
        continuous_verify_row(writer,state,false,initialization.get());current();
        initialization->rollback_();current();
        if(!autocommit())refuse("continuous lineage read snapshot did not settle");
    }catch(...) {
        // A process-only fence must precede ANY access/refcount of a parent's
        // error/reader/writer DATA. Foreign cleanup cannot inspect or preserve
        // the inaccessible parent primary locally, or infer an actual ending.
        if(issuer!=database_native_process())
            throw recovery_obligation_error(recovery_obligation_error_code::cleanup_failed,
                "inherited continuous lineage cleanup has unknown ending; parent primary is inaccessible");
        // Capture the real same-process primary BEFORE any throwing health or
        // Native ending observation. No recovery allocation is required here.
        struct primary_custody {
            using payload=std::exception_ptr;const std::uint64_t creator;
            alignas(payload) std::byte storage[sizeof(payload)];
            explicit primary_custody(std::uint64_t value)noexcept:creator(value){::new(static_cast<void*>(storage)) payload{};}
            payload& get()noexcept{return *std::launder(reinterpret_cast<payload*>(storage));}
            ~primary_custody(){if(creator==database_native_process())get().~payload();}
            primary_custody(const primary_custody&)=delete;
        } primary(issuer);
        primary.get()=std::current_exception();
        try {
            // ALL same-process health checks and actual Native observations
            // belong to this guarded cleanup attempt. A refused observation
            // is real secondary debt/unknown ending, never false autocommit.
            initialization->fail_();current();
            if(!autocommit()) {
                initialization->rollback_();current();
                if(!autocommit())refuse("continuous lineage read rollback remains live");
            }
        }catch(...) {
            // No throwing writer health check may replace either actual error.
            // If a cleanup return crossed processes, touch neither inherited
            // payload; the fixed stack holder and reader remain quarantined.
            if(issuer!=database_native_process())
                throw recovery_obligation_error(recovery_obligation_error_code::cleanup_failed,
                    "inherited continuous lineage cleanup has unknown ending; parent errors are inaccessible");
            throw recovery_obligation_error(recovery_obligation_error_code::cleanup_failed,
                "continuous lineage cleanup/ending observation failed; retained connection must be discarded",
                primary.get(),std::current_exception());
        }
        if(issuer!=database_native_process())
            throw recovery_obligation_error(recovery_obligation_error_code::cleanup_failed,
                "inherited continuous lineage cleanup return has unknown ending; parent primary is inaccessible");
        std::rethrow_exception(primary.get());
    }
}
void recovery_continuous_producer::initialize_connection_durability_(database& writer) {
    if(!writer.delivery_startup_) {writer.execute("PRAGMA synchronous=FULL");return;}
    using sql=observer_delivery_initialization_sql;using recipe=sql::recipe;
    auto set=sql::fixed_(*writer.delivery_startup_,writer,recipe::continuous_synchronous_full);
    writer.execute_initialization_(set);
    auto get=sql::fixed_(*writer.delivery_startup_,writer,recipe::continuous_synchronous_getter);
    const auto rows=writer.query_initialization_(get);
    if(rows.size()!=1||rows.front().size()!=1||integer(rows.front(),"synchronous")<2)
        refuse("continuous startup FULL durability unavailable");
}
bool recovery_continuous_producer::fresh_schema_present_(database& writer) {
    if(!writer.delivery_startup_)
        return !writer.query("SELECT 1 FROM main.sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1").empty();
    auto statement=observer_delivery_initialization_sql::fixed_(*writer.delivery_startup_,writer,
        observer_delivery_initialization_sql::recipe::continuous_fresh_schema_presence);
    return !writer.query_initialization_(statement).empty();
}
std::shared_ptr<observer_delivery_startup> recovery_continuous_producer::select_startup_(
    const configuration& config,const recovery_owner_schema& catalog,
    const std::shared_ptr<recovery_continuous_admission>& admission) {
    if(!admission||!admission->state||admission->state->path!=config.path||
       admission->pending_startup.creator!=database_native_process()||
       admission->pending_startup.builder!=std::this_thread::get_id()||admission->pending_startup.attempted||
       !admission->pending_startup.resource_startup)
        refuse("continuous startup selection lacks its actual pre-resource original");
    return observer_delivery_startup::select_with_deadline_(config,catalog,admission->pending_startup.resource_end);
}
std::shared_ptr<observer_delivery_startup> select_continuous_startup(const configuration& config,
    const recovery_owner_schema& catalog,const std::shared_ptr<recovery_continuous_admission>& admission) {
    return recovery_continuous_producer::select_startup_(config,catalog,admission);
}
bool recovery_continuous_producer::pending_startup_matches_(const lattice_db& owner,
    const observer_delivery_startup& startup)noexcept {
    if(!owner.owns_creator_process()||!owner.recovery_continuous_||!owner.db_||
       !owner.db_->creator_current_()||owner.db_->is_closed())return false;
    const auto& admission=*owner.recovery_continuous_;
    const auto& held=admission.pending_startup;
    if(held.creator!=database_native_process()||held.builder!=std::this_thread::get_id()||
       !held.attempted||held.resource_startup||held.catalog!=&owner.recovery_schemas_||
       held.startup.get()!=&startup||held.startup.get()!=owner.delivery_startup_.get()||
       held.startup.owner_before(owner.delivery_startup_)||owner.delivery_startup_.owner_before(held.startup)||
       !held.writer_open||held.writer_open.get()!=owner.db_->delivery_startup_.get()||
       admission.writer!=owner.db_.get()||admission.owner||!admission.retained_owner.expired()||
       !admission.recipe||!admission.state||admission.state->path!=owner.config_.path)return false;
    const auto& actual=owner.db_->delivery_startup_.shared_();
    return !actual.owner_before(held.writer_open)&&!held.writer_open.owner_before(actual);
}
namespace {
class fresh_catalog_identity_error final:public std::exception {
    int code_,error_;
public:
    fresh_catalog_identity_error(int code,int error)noexcept:code_(code),error_(error){}
    const char* what()const noexcept override{return "actual fresh catalog filesystem observation failed";}
    int actual_code()const noexcept{return code_;}int actual_errno()const noexcept{return error_;}
};
}
const recovery_continuous_admission* recovery_continuous_producer::pending_fresh_catalog_original_(
    const lattice_db& owner,database& actual,const observer_delivery_startup& startup) {
    if(!owner.owns_creator_process()||!actual.creator_current_())refuse("inherited fresh catalog provenance");
    if(!owner.recovery_continuous_)return nullptr;
    if(owner.db_.get()!=&actual||!pending_startup_matches_(owner,startup))
        refuse("fresh catalog lost its actual pre-enrollment owner/startup/writer");
    const auto* admission=owner.recovery_continuous_.get();const auto& state=*admission->state;
    if(!state.fresh||state.initialized)return nullptr;
    if(!state.opening||!state.identity||
       admission->pending_startup.resource_end==std::chrono::steady_clock::time_point::max()||
       std::chrono::steady_clock::now()>=admission->pending_startup.resource_end)
        refuse("fresh catalog original is not its same finite first-start container");
#if defined(__APPLE__) || defined(__linux__)
    using identity_custody=native_retirement_storage<std::shared_ptr<const physical_store_identity>>;
    actual.delivery_startup_->claim_workspace_(actual,identity_custody::allocation_bytes());identity_custody identity;
    if(admission->pending_startup.creator!=database_native_process())refuse("inherited fresh identity custody");
    identity.get<0>()=std::atomic_load(&actual.main_physical_identity_);
    if(admission->pending_startup.creator!=database_native_process())refuse("inherited fresh identity return");
    const auto& current=identity.get<0>();
    if(!current||!(*current==*state.identity)||current->filename!=state.path||state.identity->filename!=state.path)
        refuse("fresh catalog Native physical identity differs from its actual creation");
    if(!state.locked||state.parent_fd<0||state.container_fd<0)refuse("fresh catalog lost its actual directory custody");
    struct observation {struct stat parent{},container{},main{},named_parent{},named_container{};std::array<char,4097> path{};};
    actual.delivery_startup_->claim_workspace_(actual,sizeof(observation));observation observed;
    auto& parent=observed.parent;auto& container=observed.container;auto& main=observed.main;
    auto& named_parent=observed.named_parent;auto& named_container=observed.named_container;
    const auto observe=[](int rc){const int error=errno;if(rc)throw fresh_catalog_identity_error(rc,error);};
    observe(::fstat(state.parent_fd,&parent));observe(::fstat(state.container_fd,&container));
    observe(::fstatat(state.container_fd,"store.sqlite",&main,AT_SYMLINK_NOFOLLOW));
    observe(::lstat(state.parent.c_str(),&named_parent));
    const auto slash=std::string_view(state.path).rfind('/');
    if(slash==std::string_view::npos||slash>4096)refuse("fresh catalog retained container path bound");
    auto& path=observed.path;std::copy_n(state.path.data(),slash,path.data());
    observe(::lstat(path.data(),&named_container));
    if(!S_ISDIR(parent.st_mode)||!S_ISDIR(named_parent.st_mode)||!S_ISDIR(container.st_mode)||!S_ISDIR(named_container.st_mode)||
       !S_ISREG(main.st_mode)||main.st_nlink!=1||(main.st_mode&07777)!=0600||
       container.st_uid!=::geteuid()||(container.st_mode&07777)!=0700||main.st_uid!=::geteuid()||
       parent.st_dev!=named_parent.st_dev||parent.st_ino!=named_parent.st_ino||
       container.st_dev!=named_container.st_dev||container.st_ino!=named_container.st_ino||
       state.bounded(parent.st_dev)!=state.parent_device||state.bounded(parent.st_ino)!=state.parent_inode||
       state.bounded(container.st_dev)!=state.container_device||state.bounded(container.st_ino)!=state.container_inode||
       state.bounded(main.st_dev)!=state.bounded(current->device)||state.bounded(main.st_ino)!=state.bounded(current->inode)||
       std::chrono::steady_clock::now()>=admission->pending_startup.resource_end)
        refuse("fresh catalog actual parent/container/MAIN original changed");
    return admission; // comparison only; actual owner retains this original
#else
    refuse("fresh catalog original platform unsupported");
#endif
}
bool recovery_continuous_producer::enrollment_startup_matches_(const lattice_db& owner,
    const observer_delivery_startup& startup)noexcept {
    if(!owner.owns_creator_process()||!owner.recovery_continuous_||!owner.db_||
       !owner.db_->creator_current_()||owner.db_->is_closed()||owner.is_closed())return false;
    const auto& admission=*owner.recovery_continuous_;
    const auto& held=admission.pending_startup;
    if(held.creator!=database_native_process()||held.builder!=std::this_thread::get_id()||
       !held.attempted||held.resource_startup||held.catalog!=&owner.recovery_schemas_||
       held.startup.get()!=&startup||held.startup.get()!=owner.delivery_startup_.get()||
       held.startup.owner_before(owner.delivery_startup_)||owner.delivery_startup_.owner_before(held.startup)||
       !held.writer_open||held.writer_open.get()!=owner.db_->delivery_startup_.get()||
       admission.writer!=owner.db_.get()||admission.owner!=&owner||!admission.counted||
       !admission.recipe||!admission.state||admission.state->path!=owner.config_.path||
       held.resource_end==std::chrono::steady_clock::time_point::max()||
       std::chrono::steady_clock::now()>=held.resource_end)return false;
    const auto retained=admission.retained_owner.lock();
    if(retained.get()!=&owner)return false;
    const auto& actual=owner.db_->delivery_startup_.shared_();
    return !actual.owner_before(held.writer_open)&&!held.writer_open.owner_before(actual);
}
const recovery_continuous_admission* recovery_continuous_producer::enrollment_reader_binding_(
    const lattice_db& owner,const database& actual,recovery_obligation_producer_discovery_limits* caps,
    bool* shared)noexcept {
    // Fence process ownership BEFORE reading process_resources_ references or
    // any retained admission/shared_ptr payload, including the first factory call.
    if(!owner.owns_creator_process()||!actual.creator_current_())return nullptr;
    if(!owner.delivery_startup_||owner.db_.get()!=&actual||
       !enrollment_startup_matches_(owner,*owner.delivery_startup_))return nullptr;
    const auto* admission=owner.recovery_continuous_.get();
    if(caps)*caps=admission->state->policy.limits;
    if(shared)*shared=continuous_receiver_enabled(*admission->state);
    return admission;
}
bool recovery_continuous_producer::enrollment_reader_fresh_(const lattice_db& owner,const database& actual,
    const recovery_continuous_admission* original)noexcept {
    const auto* admission=enrollment_reader_binding_(owner,actual);
    return admission&&admission==original&&admission->state->fresh&&!admission->state->initialized;
}
const receive_install_binding& recovery_continuous_producer::enrollment_receiver_binding_(
    const lattice_db& owner,const database& actual,const recovery_continuous_admission* original,std::size_t index) {
    // Process-first before any process_resources/shared ownership payload.
    if(!owner.owns_creator_process()||!actual.creator_current_())refuse("inherited enrollment receiver binding");
    const auto* admission=enrollment_reader_binding_(owner,actual);
    if(!admission||admission!=original||index>=admission->state->policy.contributions.size())
        refuse("receiver binding is not the original actual enrollment contribution");
    return admission->state->policy.contributions[index].profile.binding;
}
std::shared_ptr<database> recovery_continuous_producer::open_writer(const configuration& config,
    const std::shared_ptr<recovery_continuous_admission>& admission,
    const std::shared_ptr<observer_delivery_startup>& startup,const recovery_owner_schema& catalog) {
    if(!admission||!admission->state||admission->state->path!=config.path)refuse("continuous writer lacks exact container custody");
    auto& held=admission->pending_startup;
    if(held.creator!=database_native_process()||held.builder!=std::this_thread::get_id()||
       held.attempted||held.startup||held.writer_open||held.catalog||admission->owner||admission->writer||
       !admission->recipe||!catalog.valid())refuse("continuous writer lost its unique pre-open constructor family");
    static_assert(sizeof(recovery_continuous_admission::pending_startup_handoff)+sizeof(void*)+
        sizeof(observer_delivery_startup_test_probe::continuous_handoff_observation)<=
        observer_delivery_startup::continuous_custody_charge_);
    held.attempted=true;held.catalog=&catalog;held.startup=startup;
    // Empty only on the existing genuinely uninstalled/profile-free route.
    // A present startup issues its actual one-use writer token NOW, before the
    // real constructor; both subsequent readers retain this same common state.
    if(startup)held.writer_open=startup->make_continuous_writer_(admission.get(),catalog,config.path);
    if(held.creator!=database_native_process())refuse("inherited continuous pre-open handoff");
    if(const auto hook=observer_delivery_startup_test_probe::after_continuous_handoff)
        hook({admission.get(),&catalog,held.startup.get(),held.writer_open.get()});
    if(held.creator!=database_native_process())refuse("inherited continuous handoff observation return");
    if(!held.resource_startup)refuse("continuous writer lost actual pre-open resource custody");
    held.resource_startup->before_native_open_();
    auto writer=std::make_shared<database>(config.path,database::open_mode::read_write,config.busy_timeout_ms,
        std::shared_ptr<database_read_control>{},database::initialization_key(admission,held.writer_open),config.ordinary_context,config.wal_extent_limit_bytes);
    writer->suppress_destructor_optimize_=true; // Immutable profile: refusal/close must not run optional ANALYZE.
    auto& state=*admission->state;
    const auto identity=writer->physical_identity("main",{},true);
    if(!identity)refuse("continuous SQLite physical file identity unavailable");
    {std::lock_guard<std::mutex> lock(state.mutex);
     if(state.identity&&(!(*state.identity==*identity)||state.identity->filename!=identity->filename))refuse("continuous facade file identity differs");
     if(!state.identity)state.identity=identity;}
    state.verify_file(*writer);
    // The actual Native/VFS writer now has its independently retained physical
    // original. End the genuine pre-open entry/physical guards only AFTER this
    // exact returned file/container identity has been proved.
    const bool managed_resources=held.resource_startup->managed_();
    held.resource_startup->finish_native_open_(config.path,identity->device,identity->inode);
    held.resource_startup.reset();
    if(const auto hook=observer_delivery_startup_test_probe::after_continuous_native_open)
        hook({admission.get(),held.startup.get(),held.writer_open.get(),identity->device,identity->inode,
            held.resource_end,managed_resources,state.fresh});
    if(held.creator!=database_native_process())refuse("inherited continuous Native observation return");
    initialize_connection_durability_(*writer);
    if(!state.fresh||state.initialized) {
        if(writer->delivery_startup_)verify_initializing_lineage_(*writer,state);
        else continuous_verify_row(*writer,state,false);
    }
    else if(fresh_schema_present_(*writer))
        refuse("continuous fresh container already has a durable schema");
    admission->writer=writer.get();
    return writer;
}
std::shared_ptr<database> open_continuous_writer(const configuration& config,
    const std::shared_ptr<recovery_continuous_admission>& admission,
    const std::shared_ptr<observer_delivery_startup>& startup,const recovery_owner_schema& catalog) {
    return recovery_continuous_producer::open_writer(config,admission,startup,catalog);
}
recovery_continuous_open_result recovery_continuous_producer::open(const configuration& requested,const recovery_continuous_policy& policy) {
    try {
        auto recipe=std::make_shared<owner_recipe>();
        recipe->construct=[](const configuration& config,const std::shared_ptr<recovery_continuous_admission>& admission){
            return std::shared_ptr<lattice_db>(new lattice_db(config,true,recovery_owner_schema::capture_native(),admission));
        };
        return open_owned(requested,policy,std::move(recipe));
    }catch(...){recovery_continuous_open_result output;output.settlement.primary_error=std::current_exception();return output;}
}
recovery_continuous_open_result recovery_continuous_producer::open_owned(const configuration& requested,const recovery_continuous_policy& policy,
    std::shared_ptr<const owner_recipe> recipe) {
    recovery_continuous_open_result output;
    std::shared_ptr<lattice_db> owner;
    try {
        const auto resource_end=std::chrono::steady_clock::now()+std::chrono::seconds(30);
        if(!recipe||!recipe->construct)refuse("continuous retained owner recipe unavailable");
        if(requested.path.empty()||requested.path.starts_with("file:")||configuration::path_is_memory(requested.path))refuse("continuous factory requires an explicit native file path");
        configuration config=requested;
        // Preserve original pure config/policy refusal BEFORE physical issuer
        // effects. acquire_ repeats validation on the final actual path; neither
        // preflight allocates a profile, token, authority or replacement clock.
        (void)continuous_request_preflight(config,policy);
        const auto lexical=std::filesystem::absolute(config.path).lexically_normal();
        config.path=(std::filesystem::canonical(lexical.parent_path().parent_path())/lexical.parent_path().filename()/lexical.filename()).string();
        auto resources=store_resource_continuous_startup::acquire_(config.path,resource_end);
        if(resources.managed_())config.path=resources.actual_path_();
        auto admission=acquire_(config,policy,std::move(resources),resource_end);
        admission->recipe=recipe;
        owner=recipe->construct(config,admission);
        if(!owner||owner->recovery_continuous_!=admission||owner->db_.get()!=admission->writer)
            refuse("continuous recipe did not retain the admitted physical owner");
        admission->owner=owner.get();
        admission->retained_owner=owner;
        output.settlement=enroll(owner);
        if(output.settlement.state!=recovery_install_state::committed||output.settlement.postcommit_error){
            try{owner->close();}catch(...){
                const auto actual_close_error=std::current_exception();
                if(!output.owner_close_error)output.owner_close_error=actual_close_error;
                if(!output.settlement.cleanup_error)output.settlement.cleanup_error=actual_close_error;
            }
            return output;
        }
        // The base constructor must NOT finish before genuine continuous
        // enrollment. Pending custody alone is no final schema/ledger proof;
        // publication still requires the real private finishing implementation.
        if(owner->delivery_startup_)owner->delivery_startup_->require_finished_(*owner);
        // A known enrollment COMMIT survives every later publication failure.
        // No callback, key-cache hit or borrowed pointer may grant admission.
        if(recipe->publish_pointer)recipe->publish_pointer(owner);
        instance_registry::instance().register_instance(config.path,owner.get(),owner->guard_);
        owner->adopt_path_wal_eviction_threshold();owner->setup_cross_process_notifier();
        owner->setup_sync_if_configured();
        output.owner=owner;
    } catch(...) {
        const auto error=std::current_exception();
        if(owner){try{owner->close();}catch(...){
                const auto actual_close_error=std::current_exception();
                if(!output.owner_close_error)output.owner_close_error=actual_close_error;
                if(!output.settlement.cleanup_error)output.settlement.cleanup_error=actual_close_error;
            }}
        output.owner.reset();
        if(output.settlement.state==recovery_install_state::committed)output.settlement.postcommit_error=error;
        else output.settlement.primary_error=error;
    }
    return output;
}
recovery_install_result recovery_continuous_producer::enroll(std::shared_ptr<lattice_db> owner) {
    const auto admission=owner->recovery_continuous_;auto state=admission->state;
    if(owner->delivery_startup_) {
        if(!enrollment_startup_matches_(*owner,*owner->delivery_startup_))
            refuse("continuous enrollment lost its exact retained pending owner/startup family");
        owner->delivery_startup_->require_continuous_enrollment_owner_(*owner,admission.get(),admission->pending_startup.resource_end);
    }
    std::unique_ptr<recovery_obligation_bootstrap_reader> initialization;
    if(owner->delivery_startup_)initialization=recovery_obligation_bootstrap_reader::create_continuous_enrollment_(*owner,*owner->db_);
    std::shared_ptr<recovery_local_producer_adapter::context> candidate;
    int64_t new_incarnation=0,phase=0,barrier=0,attempt=0;
    const auto result=recovery_writer_access::install_impl(owner,[&](database& db){
        state->verify_file(db);recovery_local_producer_adapter::validate_custody(*owner,db);
        recovery_local_producer_adapter::management management(db.internal_handle());continuous_frame changing(*state,db,1);
        if(initialization)initialization->validated_family_();
        const bool existing=initialization?!initialization->query_(recovery_obligation_bootstrap_reader::kind::continuous_presence,
            {std::string(continuous_table)}).empty():db.table_exists(continuous_table);
        if(state->fresh&&!state->initialized&&existing)refuse("continuous fresh file cannot adopt an existing profile");
        if(!existing) {
            if(!state->fresh||state->initialized)refuse("continuous orphan or missing lineage cannot be adopted");
            // Fresh owned WRITE is authoritative. No legacy AuditLog, receiver,
            // journal or producer history may be relabeled into coverage.
            if(!(initialization?initialization->query_(recovery_obligation_bootstrap_reader::kind::enrollment_audit_presence):
                    db.query("SELECT 1 FROM main.AuditLog LIMIT 1")).empty()||
               !(initialization?initialization->query_(recovery_obligation_bootstrap_reader::kind::enrollment_ledger_presence):
                    db.query("SELECT 1 FROM main.sqlite_schema WHERE name GLOB '_lattice_obligation_*' OR name GLOB '_lattice_install_*' LIMIT 1")).empty())
                refuse("continuous fresh enrollment found prior history");
            if(initialization)initialization->observe_continuous_enrollment_reads_(false);
            candidate=std::make_shared<recovery_local_producer_adapter::context>();candidate->connection=db.internal_handle();candidate->owner=owner.get();candidate->lifetime=owner->guard_;
            candidate->admitted_limits=state->policy.limits;candidate->continuous=admission;
            if(initialization)initialization->initialize_enrollment_ledgers_();
            receive_install_store receiver(owner,state->policy.limits.installations);if(!initialization)receiver.initialize();
            recovery_obligation_store journal(owner,state->policy.limits.obligations,state->policy.limits.installations);if(!initialization)journal.initialize();
            recovery_obligation_producer_store producer(owner,state->policy.limits.obligations,state->policy.limits.installations,state->policy.limits.producers);if(!initialization)producer.initialize();
            // The pending Reader completes its closed receiver family on this
            // SAME unpublished WRITE before the distinct journal producers.
            if(initialization)for(std::size_t index=0;index<state->policy.contributions.size();++index)
                initialization->bind_enrollment_receiver_(index);
            for(const auto& contribution:state->policy.contributions){
                if(!initialization)receiver.bind(contribution.profile.binding);
                const auto scope=journal.bind(contribution.profile);
                recovery_local_producer_grant grant{scope.address,contribution.models,contribution.incoming_grant_claim};
                auto description=recovery_local_producer_adapter::describe(*owner,db,grant,true);
                if(continuous_receiver_enabled(*state))for(const auto& prior:candidate->profiles) {
                    const auto& binding=prior.stored.contribution.binding;
                    if(binding.authority==contribution.profile.binding.authority&&binding.scope==contribution.profile.binding.scope) {
                        const auto previous=grant_from(prior.stored);
                        if(previous.models!=grant.models||previous.incoming_grant_receipt!=grant.incoming_grant_receipt||prior.schema.tables.size()!=description.tables.size())
                            refuse("continuous shared domain model/catalog contract differs");
                        for(const auto& [name,table]:description.tables){const auto found=prior.schema.tables.find(name);
                            if(found==prior.schema.tables.end()||found->second.link!=table.link||found->second.columns!=table.columns||found->second.no_history!=table.no_history)
                                refuse("continuous shared domain actual catalog differs");}
                    }
                }
                for(const auto& [name,table]:description.tables)if(actual_programs(db,name)!=table.ordinary)refuse("continuous initial generated inventory differs");
                recovery_obligation_producer_profile profile{scope.profile,scope.address.incarnation,continuous_receiver_enabled(*state)?3:2,description.digest,description.manifest};
                producer.enroll(profile);candidate->profiles.push_back({profile,std::move(description)});
            }
            recovery_local_producer_adapter::compile_continuous(*owner,*candidate);
            recovery_local_producer_adapter::register_context(db,candidate);
            db.execute(continuous_ddl);for(const auto& [name,sql]:continuous_guards())db.execute(sql);
            db.execute("INSERT INTO _lattice_producer_continuity VALUES(1,?,?,?,?,?,?,?,?,?,1,0,0,0)",
                {int64_t(continuous_receiver_enabled(*state)?2:1),state->manifest,blob(state->digest.begin(),state->digest.end()),static_cast<int64_t>(state->identity->device),static_cast<int64_t>(state->identity->inode),state->parent_device,state->parent_inode,state->container_device,state->container_inode});
            db.execute("PRAGMA recursive_triggers=ON");
            for(const auto& [name,sql]:retention_programs())db.execute(sql);
            std::set<std::string> installed;
            for(const auto& p:candidate->profiles)for(const auto& [name,table]:p.schema.tables)if(installed.insert(name).second){
                for(const auto& [key,sql]:table.ordinary)db.execute("DROP TRIGGER "+key);
                for(const auto& [key,sql]:table.enrolled)db.execute(sql);
            }
            if(continuous_receiver_enabled(*state))recovery_receiver_controller::initialize_owned(owner,state->policy);
            new_incarnation=1;owner->store_recovery_fingerprints();
        } else {
            continuous_verify_row(db,*state,false,initialization.get());
            const auto row=(initialization?initialization->query_(recovery_obligation_bootstrap_reader::kind::continuous_row):
                db.query(continuous_query)).at(0);const auto old=integer(row,"incarnation");
            if(initialization)initialization->observe_continuous_enrollment_reads_(true);
            if(old==std::numeric_limits<int64_t>::max())refuse("continuous physical incarnation exhausted");
            new_incarnation=state->initialized?state->incarnation:old+1;
            if(state->initialized&&old!=new_incarnation)refuse("continuous facade incarnation changed");

            phase=integer(row,"phase");barrier=integer(row,"barrier");attempt=integer(row,"attempt");
            candidate=std::static_pointer_cast<recovery_local_producer_adapter::context>(std::atomic_load(&db.local_producer_callback_custody_));
            if(!candidate||candidate->continuous!=admission||candidate->profiles.size()!=state->policy.contributions.size())refuse("continuous facade producer custody missing");
            for(size_t i=0;i<candidate->profiles.size();++i){const auto& p=candidate->profiles[i].stored;bool matched=false;
                for(const auto& c:state->policy.contributions)if(c.profile==p.contribution&&grant_from(p).models==c.models&&grant_from(p).incoming_grant_receipt==c.incoming_grant_claim)matched=true;
                if(!matched)refuse("continuous policy contribution differs from generated profile");}
            recovery_obligation_store journal(owner,state->policy.limits.obligations,state->policy.limits.installations);
            receive_install_store receiver(owner,state->policy.limits.installations);journal.audit();receiver.audit();
            if(journal.usage().scopes!=static_cast<int64_t>(state->policy.contributions.size())||receiver.usage().channels!=static_cast<int64_t>(state->policy.contributions.size()))refuse("continuous extra or missing journal/receiver contribution");
            // Capture marker currentness before the existing audited receiver
            // migration can legitimately perform DDL, under this same WRITE.
            // No foreign DDL can interleave with these owned validations.
            std::string upgrade_prior_cookie;
            std::set<std::string> upgrade_marker_keys;
            const auto schema_cookie=[&]() {
                const auto rows=db.query("PRAGMA main.schema_version");
                if(rows.size()!=1)refuse("continuous upgrade schema cookie unavailable");
                const auto value=integer(rows[0],"schema_version");
                if(value<0||value>std::numeric_limits<int32_t>::max())refuse("continuous upgrade schema cookie invalid");
                return std::to_string(value);
            };
            const auto marker=[&](const std::string& key) {
                const auto rows=db.query("SELECT CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=10 THEN value END AS value FROM main._lattice_meta WHERE key=? LIMIT 2",{key});
                if(rows.size()!=1)refuse("continuous upgrade expected marker missing");
                return text(rows[0],"value");
            };
            if(candidate->legacy_distinct_count_programs) {
                if(!continuous_receiver_enabled(*state)||!candidate->matches_complete_programs(db))
                    refuse("continuous historical bundle changed before upgrade");
                upgrade_prior_cookie=schema_cookie();upgrade_marker_keys.insert(owner->compute_core_fingerprint_key());
                if(!owner->recovery_schemas_.swift_fingerprint.empty())upgrade_marker_keys.insert(owner->recovery_schemas_.swift_fingerprint);
                for(const auto& key:upgrade_marker_keys)if(marker(key)!=upgrade_prior_cookie)
                    refuse("continuous upgrade expected marker stale or noncanonical");
            }
            if(continuous_receiver_enabled(*state)) {
                recovery_receiver_controller::validate_reopen_owned(owner,state->policy,phase,barrier,attempt);
            } else for(const auto& profile:candidate->profiles){
                const auto scope=journal.read(profile.stored.contribution.binding.channel);
                const auto received=receiver.read(profile.stored.contribution.binding.channel);
                const auto prior_attempt=phase==0?attempt:attempt-1;
                if(!scope||!received||prior_attempt<0||received->binding!=profile.stored.contribution.binding||received->active||received->last_installed||received->revision!=0||
                   received->frontier!=receive_install_frontier{}||received->last_sequence!=prior_attempt||
                   scope->mode!=(phase==2?recovery_obligation_mode::frozen:recovery_obligation_mode::recording)||
                   scope->last_attempt!=(phase==2?attempt:prior_attempt))
                    refuse("continuous lineage/journal/receiver restart state differs");
            }
            if(candidate->legacy_distinct_count_programs) {
                // Bootstrap was read-only. This existing owned enrollment WRITE
                // has now validated the full lineage, every journal/receiver and
                // the current physical file before any optional program upgrade.
                if(!continuous_receiver_enabled(*state)||!candidate->matches_complete_programs(db))
                    refuse("continuous historical bundle changed before upgrade");
                if(recovery_local_producer_test_hooks::before_count_program_upgrade)
                    recovery_local_producer_test_hooks::before_count_program_upgrade(owner.get());
                // Retain the admitted descriptor unchanged. The replacement
                // context is pending: its producer DML guard stays closed until
                // the existing known-COMMIT tail activates it. Failed endings
                // keep this unpublished owner fenced and follow normal cleanup.
                auto upgraded=std::make_shared<recovery_local_producer_adapter::context>();
                upgraded->connection=candidate->connection;upgraded->owner=candidate->owner;
                upgraded->lifetime=candidate->lifetime;upgraded->admitted_limits=candidate->admitted_limits;
                upgraded->continuous=candidate->continuous;upgraded->profiles=candidate->profiles;upgraded->previous=candidate;
                recovery_local_producer_adapter::compile_continuous(*owner,*upgraded);
                std::set<std::string> changed;
                for(size_t i=0;i<candidate->profiles.size();++i)for(const auto& [name,table]:candidate->profiles[i].schema.tables)if(changed.insert(name).second) {
                    for(const auto& [key,sql]:table.enrolled)db.execute("DROP TRIGGER "+key);
                    const auto& replacement=upgraded->profiles[i].schema.tables.at(name);
                    for(const auto& [key,sql]:replacement.enrolled)db.execute(sql);
                }
                if(!upgraded->matches_complete_programs(db))refuse("continuous upgrade complete postimage differs");
                const auto current_cookie=schema_cookie();
                for(const auto& key:upgrade_marker_keys) {
                    db.execute("UPDATE main._lattice_meta SET value=? WHERE key=? AND typeof(value)='text' AND value=? COLLATE BINARY",{current_cookie,key,upgrade_prior_cookie});
                    if(integer(db.query("SELECT changes() AS n").at(0),"n")!=1||marker(key)!=current_cookie)
                        refuse("continuous upgrade marker postimage differs");
                }
                // Existing setup-only registration may throw; that is still
                // inside this same transaction and follows ordinary rollback.
                candidate=std::move(upgraded);
                recovery_local_producer_adapter::register_context(db,candidate);
            }
            if(!state->initialized)db.execute("UPDATE _lattice_producer_continuity SET incarnation=? WHERE id=1 AND incarnation=?",{new_incarnation,old});
        }
        continuous_inventory(db);
        const auto final=db.query(continuous_query);
        if(final.size()!=1||integer(final[0],"incarnation")!=new_incarnation||integer(final[0],"phase")!=phase||integer(final[0],"barrier")!=barrier||integer(final[0],"attempt")!=attempt)
            refuse("continuous enrollment postimage differs");
        state->verify_file(db);
        if(!state->initialized)for(const auto& p:candidate->profiles)for(const auto& [name,t]:p.schema.tables)for(const auto& [key,sql]:t.ordinary)state->generated_names.insert(key);
    },[&]{
        candidate->status.store(1,std::memory_order_release);candidate->previous.reset();
        std::lock_guard<std::mutex> lock(state->mutex);
        if(!state->initialized) {
            state->incarnation=new_incarnation;state->barrier=barrier;state->attempt=attempt;
            state->initialized=true;state->opening=false;state->dml_open.store(phase==0,std::memory_order_release);
        }
    },{},nullptr,false,initialization.get());
    if(candidate&&result.state!=recovery_install_state::committed)candidate->active->store(false,std::memory_order_release);
    return result;
}
recovery_continuous_route::~recovery_continuous_route(){
    if(state_){std::lock_guard<std::mutex> lock(state_->mutex);--state_->routes;}
    if(configured_unregistered_)configured_unregistered_();
}
recovery_continuous_work::~recovery_continuous_work(){if(state_){std::lock_guard<std::mutex> lock(state_->mutex);--state_->operations;}}
std::shared_ptr<recovery_continuous_route> recovery_continuous_producer::admit_route(std::shared_ptr<lattice_db> owner,const sync_config& config,bool injected) {
    recovery_admission_test_hooks::route_entered();
    if(!owner)return {};
    if(!owner->recovery_continuous_){require_no_continuous_export(*owner->db_);return {};}
    auto state=owner->recovery_continuous_->state;
    if(injected||config.sync_filter)refuse("continuous injected/filtered export topology not implemented");
    bool allowed=false;for(const auto& route:state->policy.routes)if(route.sync_id==config.sync_id&&route.endpoint==config.websocket_url)allowed=true;
    if(!allowed)refuse("continuous WSS route absent from immutable policy");
    recovery_admission_test_hooks::route_point(recovery_admission_test_hooks::route_admission_point::policy_checked);
    recovery_admission_test_hooks::route_install_observation_scope observation;
    const auto checked=recovery_writer_access::install(owner,[&](database& db){continuous_verify_row(db,*state);recovery_local_producer_adapter::profiles_for_owned_write(owner,state->policy.limits);});
    observation.finish();
    if(auto* out=recovery_admission_test_hooks::route_observation){
        out->point=recovery_admission_test_hooks::route_admission_point::install_returned;
        out->install_state=static_cast<unsigned char>(checked.state);
        out->primary=bool(checked.primary_error);out->cleanup=bool(checked.cleanup_error);
        out->postcommit=bool(checked.postcommit_error);out->notification=bool(checked.notification_error);
        out->unexpected_commit=checked.unexpected_commit_observed;
    }
    if(checked.state!=recovery_install_state::committed){if(checked.primary_error){
        recovery_admission_test_hooks::route_point(recovery_admission_test_hooks::route_admission_point::primary_rethrow);
        std::rethrow_exception(checked.primary_error);
    }refuse("continuous route admission did not commit");}
    recovery_admission_test_hooks::route_point(recovery_admission_test_hooks::route_admission_point::committed);
    auto route=std::shared_ptr<recovery_continuous_route>(new recovery_continuous_route);
    {std::lock_guard<std::mutex> lock(state->mutex);
     if(!state->initialized||(!state->dml_open.load(std::memory_order_acquire)&&!continuous_receiver_enabled(*state))||state->routes==state->policy.physical_routes||state->next_route==std::numeric_limits<uint64_t>::max())refuse("continuous route gate closed or full");
     route->route_=++state->next_route;route->incarnation_=state->incarnation;route->owner_=owner;route->state_=state;++state->routes;}
    recovery_admission_test_hooks::route_point(recovery_admission_test_hooks::route_admission_point::registered);
    return route;
}
std::shared_ptr<recovery_continuous_work> recovery_continuous_producer::admit_work(const std::shared_ptr<recovery_continuous_route>& route,std::shared_ptr<lattice_db> owner,uint64_t physical) {
    if(!route){if(owner&&owner->recovery_continuous_)refuse("continuous export requires actual registered WSS route");return {};}
    if(!owner||route->owner_.lock()!=owner||owner->is_closed()||!physical)refuse("continuous work owner/physical route differs");
    auto state=route->state_;auto work=std::shared_ptr<recovery_continuous_work>(new recovery_continuous_work);
    {std::lock_guard<std::mutex> lock(state->mutex);
     if(!state->dml_open.load(std::memory_order_acquire)||state->incarnation!=route->incarnation_||state->operations==state->policy.operations)refuse("continuous work admission closed or full");
     work->owner_=std::move(owner);work->route_=route->route_;work->physical_=physical;work->incarnation_=state->incarnation;work->state_=state;++state->operations;}
    return work;
}
void recovery_continuous_producer::verify_work(const std::shared_ptr<recovery_continuous_work>& work,lattice_db& owner,database& writer) {
    if(!owner.recovery_continuous_){if(work)refuse("continuous work on legacy owner");return;}
    auto state=owner.recovery_continuous_->state;
    if(!work||work->reconciliation_||work->owner_.get()!=&owner||work->state_!=state||work->incarnation_!=state->incarnation||!work->route_||!work->physical_)refuse("continuous export lacks retained admission");
    continuous_verify_row(writer,*state);
    const auto row=writer.query(continuous_query).at(0);
    if(integer(row,"phase")>=2)refuse("continuous export arrived after frozen Q");
}
std::shared_ptr<recovery_continuous_work> recovery_continuous_producer::admit_reconciliation_work(
    const std::shared_ptr<const recovery_reconciliation_descriptor>& descriptor,const std::shared_ptr<recovery_continuous_route>& route,
    std::shared_ptr<lattice_db> owner,uint64_t physical) {
    recovery_receiver_controller::verify_reconciliation_route(descriptor,route,owner,physical);
    if(!owner->recovery_continuous_||route->owner_.lock()!=owner||route->state_!=owner->recovery_continuous_->state)refuse("restricted work actual continuous route differs");
    auto state=route->state_;auto work=std::shared_ptr<recovery_continuous_work>(new recovery_continuous_work);
    // Retain payloads before entering the leaf; failure/destruction runs off it.
    work->reconciliation_=descriptor;work->restricted_route_=route;work->owner_=owner;
    {std::lock_guard lock(state->mutex);if(!continuous_receiver_enabled(*state)||state->dml_open.load()||state->incarnation!=route->incarnation_||
        state->incarnation!=descriptor->physical_incarnation()||state->barrier!=descriptor->barrier()||state->attempt!=descriptor->attempt()||state->operations==state->policy.operations)
            refuse("restricted work gate changed or full");
        work->route_=route->route_;work->physical_=physical;work->incarnation_=state->incarnation;work->state_=state;++state->operations;}
    return work;
}
recovery_install_result recovery_continuous_producer::reconciliation_export_owned(std::shared_ptr<lattice_db> owner,
    const std::shared_ptr<recovery_continuous_work>& work,const std::vector<std::string>& originals,const std::function<void(database&)>& body,bool* admission_busy,bool automatic,
    const std::function<void()>& after_committed_unlock) {
    // Only initial admission before any owned body or SQL can report BUSY.
    return recovery_writer_access::install_impl(owner,[&](database& db){
        if(!work||!work->reconciliation_||work->owner_!=owner||!owner->recovery_continuous_||work->state_!=owner->recovery_continuous_->state)
            refuse("restricted export lost counted descriptor custody");
        auto state=work->state_;
        const auto verify=[&]{recovery_receiver_controller::verify_reconciliation_route(work->reconciliation_,work->restricted_route_,owner,work->physical_,&originals);
            continuous_verify_row(db,*state);const auto row=db.query(continuous_query).at(0);
            if(integer(row,"phase")!=4||integer(row,"incarnation")!=work->incarnation_||integer(row,"barrier")!=work->reconciliation_->barrier()||
                integer(row,"attempt")!=work->reconciliation_->attempt())refuse("restricted export durable generation changed");};
        verify();continuous_frame claim(*state,db,2);body(db);verify();
    },after_committed_unlock,{},automatic?nullptr:admission_busy,automatic);
}
recovery_install_result recovery_continuous_producer::export_owned(std::shared_ptr<lattice_db> owner,const std::shared_ptr<recovery_continuous_work>& work,const std::function<void(database&)>& body,bool* initial_admission_busy,bool automatic) {
    return recovery_writer_access::install_impl(owner,[&](database& db){
        verify_work(work,*owner,db);
        if(!work){body(db);return;}
        continuous_frame claim(*work->state_,db,2);body(db);verify_work(work,*owner,db);
    },{},{},automatic?nullptr:initial_admission_busy,automatic);
}
recovery_continuous_quiescence recovery_continuous_producer::begin(std::shared_ptr<lattice_db> owner,int64_t attempt) {
    return begin_impl(std::move(owner),attempt,false);
}
recovery_continuous_quiescence recovery_continuous_producer::begin_impl(std::shared_ptr<lattice_db> owner,int64_t attempt,bool controller_try) {
    recovery_continuous_quiescence output;std::shared_ptr<recovery_continuous_state> state;
    recovery_continuous_barrier token;
    output.settlement=recovery_writer_access::install_impl(owner,[&](database& db){
        if(!owner->recovery_continuous_||attempt<=0)refuse("continuous barrier requires admitted owner and positive logical attempt");
        state=owner->recovery_continuous_->state;continuous_verify_row(db,*state);
        const auto row=db.query(continuous_query).at(0);
        if(integer(row,"phase")!=0||attempt<=integer(row,"attempt")||integer(row,"barrier")==std::numeric_limits<int64_t>::max())refuse("continuous barrier stale attempt or already closed");
        receive_install_store receiver(owner,state->policy.limits.installations);
        const auto profiles=recovery_local_producer_adapter::profiles_for_owned_write(owner,state->policy.limits);
        for(const auto& profile:profiles){const auto current=receiver.read(profile.contribution.binding.channel);
            if(!current||current->binding!=profile.contribution.binding||current->active||current->last_sequence==std::numeric_limits<int64_t>::max()||attempt!=current->last_sequence+1)
                refuse("continuous barrier requires each receiver's exact next unstarted sequence");}
        if(profiles.size()!=state->policy.contributions.size())refuse("continuous begin contribution inventory changed");
        {std::lock_guard<std::mutex> lock(state->mutex);if(state->gate_revision==std::numeric_limits<uint64_t>::max())refuse("continuous runtime gate exhausted");++state->gate_revision;state->dml_open.store(false,std::memory_order_release);}
        // Never restore this flag on failure. A failed/unknown COMMIT requires
        // explicit retry/inspection under the actual writer before reopening.
        continuous_frame frame(*state,db,3);
        const auto barrier=integer(row,"barrier")+1;
        db.execute("UPDATE _lattice_producer_continuity SET barrier=?,attempt=?,phase=1 WHERE id=1 AND phase=0 AND barrier=?",{barrier,attempt,barrier-1});
        const auto after=db.query(continuous_query);
        if(after.size()!=1||integer(after[0],"barrier")!=barrier||integer(after[0],"attempt")!=attempt||integer(after[0],"phase")!=1)refuse("continuous barrier write ignored or changed");
        token.owner_=owner;token.state_=state;token.incarnation_=state->incarnation;token.barrier_=barrier;token.attempt_=attempt;
        state->verify_file(db);
    },{},{},nullptr,controller_try);
    if(output.settlement.state==recovery_install_state::committed){
        {std::lock_guard<std::mutex> lock(state->mutex);state->barrier=token.barrier_;state->attempt=token.attempt_;output.waiting=state->operations!=0;}
        output.barrier=token;
    }
    return output;
}
namespace {
bool same_snapshot(const recovery_obligation_snapshot& a,const recovery_obligation_snapshot& b){return a.scope==b.scope&&a.entries==b.entries;}
}
recovery_continuous_quiescence recovery_continuous_producer::finish(const recovery_continuous_barrier& token) {
    return finish_impl(token,false);
}
recovery_continuous_quiescence recovery_continuous_producer::finish_impl(const recovery_continuous_barrier& token,bool controller_try) {
    recovery_continuous_quiescence output;auto state=token.state_.lock();
    if(!state||!token.owner_){output.settlement.primary_error=std::make_exception_ptr(db_error("continuous barrier custody expired"));return output;}
    {std::lock_guard<std::mutex> lock(state->mutex);if(state->operations){output.waiting=true;output.barrier=token;return output;}}
    verified_unsent_set verified;
    output.settlement=recovery_writer_access::install_impl(token.owner_,[&](database& db){
        continuous_verify_row(db,*state);const auto row=db.query(continuous_query).at(0);
        if(token.owner_->recovery_continuous_->state!=state||token.incarnation_!=state->incarnation||integer(row,"barrier")!=token.barrier_||integer(row,"attempt")!=token.attempt_||integer(row,"phase")==0)
            refuse("continuous barrier changed before finite Q freeze");
        {std::lock_guard<std::mutex> lock(state->mutex);if(state->operations||state->dml_open.load(std::memory_order_acquire))refuse("continuous work not settled");}
        continuous_frame freeze(*state,db,4);
        const auto profiles=recovery_local_producer_adapter::profiles_for_owned_write(token.owner_,state->policy.limits);
        recovery_obligation_store journal(token.owner_,state->policy.limits.obligations,state->policy.limits.installations);
        journal.audit();const auto usage=journal.usage();
        if(usage.scopes!=static_cast<int64_t>(state->policy.contributions.size()))refuse("continuous frozen Q scope inventory differs");
        if(usage.records>static_cast<int64_t>(state->policy.frozen_entries)||usage.encoded_bytes>static_cast<int64_t>(state->policy.frozen_bytes))refuse("continuous frozen Q exceeds independent bounds");
        std::map<std::string,recovery_obligation_record> originals;std::set<std::string> claimed;
        for(const auto& profile:profiles){
            auto scope=journal.read(profile.contribution.binding.channel);
            if(!scope)refuse("continuous contribution disappeared");
            if(integer(row,"phase")==1)scope=journal.freeze(scope->address,token.attempt_);
            auto snapshot=journal.snapshot_for_install(scope->address,token.attempt_);
            for(const auto& entry:snapshot.entries){
                const auto stamp=recovery_obligation_producer_store::read_stamp(token.owner_,state->policy.limits.obligations,state->policy.limits.installations,state->policy.limits.producers,scope->address,entry.record.original_id);
                if(!stamp||stamp->contribution_incarnation!=scope->address.incarnation||stamp->program_revision!=(continuous_receiver_enabled(*state)?3:2)||stamp->program_digest!=profile.program_digest||stamp->audit_id!=entry.record.audit_id||stamp->record_sequence!=entry.sequence||entry.record.origin!=recovery_obligation_origin::local_candidate||entry.stage!=recovery_obligation_stage::open)
                    refuse("continuous finite Q lacks exact generated provenance");
                const auto [at,added]=originals.emplace(entry.canonical_original_id,entry.record);
                if(!added&&at->second!=entry.record)refuse("continuous shared original target/body identity differs");
                if(entry.first_export_claim)claimed.insert(entry.canonical_original_id);
            }
            verified.journals_.push_back(std::move(snapshot));
        }
        if(profiles.size()!=state->policy.contributions.size())refuse("continuous finite Q contribution inventory changed");
        for(const auto& [id,record]:originals)if(!claimed.count(id))verified.originals_.push_back(id);
        if(integer(row,"phase")==1)db.execute("UPDATE _lattice_producer_continuity SET phase=2 WHERE id=1 AND phase=1 AND barrier=? AND attempt=?",{token.barrier_,token.attempt_});
        const auto after=db.query(continuous_query);
        if(after.size()!=1||integer(after[0],"phase")!=2||integer(after[0],"barrier")!=token.barrier_||integer(after[0],"attempt")!=token.attempt_)refuse("continuous frozen Q transition ignored or changed");
        for(const auto& before:verified.journals_)if(!same_snapshot(before,journal.snapshot_for_install(before.scope.address,token.attempt_)))refuse("continuous Q changed during final verification");
        state->verify_file(db);verified.owner_=token.owner_;verified.state_=state;verified.incarnation_=token.incarnation_;verified.barrier_=token.barrier_;verified.policy_digest_=state->digest;
    },{},{},nullptr,controller_try);
    if(output.settlement.state==recovery_install_state::committed){output.barrier=token;output.unsent=std::move(verified);}
    return output;
}
recovery_continuous_quiescence recovery_continuous_producer::inspect(std::shared_ptr<lattice_db> owner) {
    return inspect_impl(std::move(owner),false);
}
recovery_continuous_quiescence recovery_continuous_producer::inspect_impl(std::shared_ptr<lattice_db> owner,bool controller_try) {
    recovery_continuous_quiescence output;recovery_continuous_barrier token;bool closed=false;
    output.settlement=recovery_writer_access::install_impl(owner,[&](database& db){
        if(!owner->recovery_continuous_)refuse("continuous inspection needs actual owner");auto state=owner->recovery_continuous_->state;
        continuous_verify_row(db,*state);const auto row=db.query(continuous_query).at(0);closed=integer(row,"phase")!=0;
        if(closed){token.owner_=owner;token.state_=state;token.incarnation_=state->incarnation;token.barrier_=integer(row,"barrier");token.attempt_=integer(row,"attempt");}
    },{},{},nullptr,controller_try);
    if(output.settlement.state==recovery_install_state::committed&&closed)output.barrier=token;
    return output;
}
recovery_install_result recovery_continuous_producer::cancel(const recovery_continuous_barrier& token) {
    auto state=token.state_.lock();uint64_t gate_revision=0;
    const auto result=recovery_writer_access::install(token.owner_,[&](database& db){
        if(!state||!token.owner_->recovery_continuous_||token.owner_->recovery_continuous_->state!=state)refuse("continuous cancel custody expired");
        if(continuous_receiver_enabled(*state))refuse("enabled controller owns canonical cancellation; public barrier cannot reopen its gate");
        continuous_verify_row(db,*state);const auto row=db.query(continuous_query).at(0);
        if(token.incarnation_!=state->incarnation||integer(row,"barrier")!=token.barrier_||integer(row,"attempt")!=token.attempt_||integer(row,"phase")==0)refuse("continuous cancel stale barrier");
        {std::lock_guard<std::mutex> lock(state->mutex);if(state->operations||state->dml_open.load(std::memory_order_acquire))refuse("continuous cancel has live work");gate_revision=state->gate_revision;}
        continuous_frame cancel(*state,db,5);recovery_obligation_store journal(token.owner_,state->policy.limits.obligations,state->policy.limits.installations);
        const auto profiles=recovery_local_producer_adapter::profiles_for_owned_write(token.owner_,state->policy.limits);
        for(const auto& profile:profiles){auto scope=journal.read(profile.contribution.binding.channel);if(!scope)refuse("continuous cancel missing journal");
            if(integer(row,"phase")==1){if(scope->mode!=recovery_obligation_mode::recording)refuse("continuous preparing cancel found changed journal");scope=journal.freeze(scope->address,token.attempt_);}
            journal.cancel_frozen_for_retry(scope->address,token.attempt_,scope->revision);}
        db.execute("UPDATE _lattice_producer_continuity SET phase=0 WHERE id=1 AND barrier=? AND attempt=?",{token.barrier_,token.attempt_});
        const auto after=db.query(continuous_query);if(after.size()!=1||integer(after[0],"phase")!=0||integer(after[0],"barrier")!=token.barrier_||integer(after[0],"attempt")!=token.attempt_)refuse("continuous cancellation postimage differs");
        state->verify_file(db);
    });
    if(result.state==recovery_install_state::committed){std::lock_guard<std::mutex> lock(state->mutex);if(state->gate_revision==gate_revision)state->dml_open.store(true,std::memory_order_release);}
    return result;
}
void recovery_continuous_producer::verify_for_owned_write(const verified_unsent_set& value) {
    const auto owner=value.owner_?value.owner_:value.controller_owner_.lock();
    auto state=value.state_.lock();if(!state||!owner||owner->is_closed()||!owner->recovery_continuous_||owner->recovery_continuous_->state!=state)refuse("continuous UNSENT custody expired");
    auto* writer=recovery_writer_access::active_writer(*owner);if(!writer)refuse("continuous UNSENT verification needs actual owned WRITE");
    continuous_verify_row(*writer,*state);const auto row=writer->query(continuous_query).at(0);
    if(value.incarnation_!=state->incarnation||value.policy_digest_!=state->digest||integer(row,"phase")!=2||integer(row,"barrier")!=value.barrier_)refuse("continuous UNSENT generation changed");
    {std::lock_guard<std::mutex> lock(state->mutex);if(state->operations||state->dml_open.load(std::memory_order_acquire))refuse("continuous UNSENT work not settled");}
    recovery_obligation_store journal(owner,state->policy.limits.obligations,state->policy.limits.installations);
    for(const auto& snapshot:value.journals_)if(!same_snapshot(snapshot,journal.snapshot_for_install(snapshot.scope.address,integer(row,"attempt"))))refuse("continuous UNSENT frozen Q changed");
}

void recovery_continuous_producer::setup_configured_route(lattice_db& owner) {
#ifndef __EMSCRIPTEN__
    if(!owner.recovery_continuous_||!owner.config_.is_sync_enabled())refuse("continuous configured route requires admitted owner");
    const auto state=owner.recovery_continuous_->state;
    sync_config config;config.websocket_url=owner.config_.websocket_url;config.authorization_token=owner.config_.authorization_token;
    config.sync_id="wss:"+config.websocket_url;config.sync_filter=owner.config_.sync_filter;config.is_observer=owner.config_.sync_is_observer;
    config.recovery_source_expectation=owner.config_.recovery_source_expectation;
    owner.config_.tuning.apply(config);
    for(const auto& route:state->policy.routes)config.all_active_sync_ids.push_back(route.sync_id);
    bool admitted=false;for(const auto& route:state->policy.routes)if(route.sync_id==config.sync_id&&route.endpoint==config.websocket_url)admitted=true;
    if(!admitted||config.sync_filter)refuse("continuous configured route outside immutable coverage");
    if(owner.synchronizer_)return;
    const auto admission=owner.recovery_continuous_;
    const auto actual_owner=admission->retained_owner.lock();
    if(!actual_owner||actual_owner.get()!=&owner||actual_owner->recovery_continuous_!=admission||owner.is_closed())
        refuse("configured route lost actual retained parent");
    const auto factory=get_network_factory();
    if(auto* typed=dynamic_cast<configured_platform_factory*>(factory.get())){
        {
            std::lock_guard<std::mutex> lock(admission->configured_mutex);
            if(admission->configured)return;
            if(admission->configuring)refuse("configured route construction already in progress");
            admission->configuring=true;
        }
        std::shared_ptr<configured_recovery_connection> configured;
        try{
            const auto path=owner.config_.path;const auto busy_timeout=owner.config_.busy_timeout_ms;
            const auto schema_version=owner.config_.target_schema_version;const auto recipe=admission->recipe;
            const auto wal_extent_limit_bytes=owner.config_.wal_extent_limit_bytes;
            const auto policy=state->policy;
            configured=configured_recovery_connection::create(actual_owner,config,factory,typed,[path,busy_timeout,schema_version,recipe,policy,wal_extent_limit_bytes]{
                configuration child(path,std::make_shared<std_thread_scheduler>());
                child.busy_timeout_ms=busy_timeout;child.target_schema_version=schema_version;
                child.wal_extent_limit_bytes=wal_extent_limit_bytes;
                auto opened=open_owned(child,policy,recipe);
                if(!opened.owner||opened.settlement.state!=recovery_install_state::committed||opened.settlement.postcommit_error){
                    if(opened.settlement.primary_error)std::rethrow_exception(opened.settlement.primary_error);
                    if(opened.settlement.postcommit_error)std::rethrow_exception(opened.settlement.postcommit_error);
                    refuse("configured dedicated facade open failed");
                }
                return opened.owner;
            });
            configured->set_state(owner.on_sync_state_change_);configured->set_error(owner.on_sync_error_);
            configured->set_progress(owner.on_sync_progress_);
            {std::lock_guard<std::mutex> lock(admission->configured_mutex);admission->configured=configured;admission->configuring=false;}
            configured->start();
        }catch(...){
            if(configured)configured->abandon_start(std::current_exception());
            {std::lock_guard<std::mutex> lock(admission->configured_mutex);admission->configuring=false;}
            throw;
        }
        return;
    }
    // Unsupported factories preserve their one physical/manual path. They
    // never acquire an automatic replacement grant or a new registry charge.
    configuration child(owner.config_.path,std::make_shared<std_thread_scheduler>());
    child.busy_timeout_ms=owner.config_.busy_timeout_ms;child.target_schema_version=owner.config_.target_schema_version;
    child.wal_extent_limit_bytes=owner.config_.wal_extent_limit_bytes;
    auto opened=open_owned(child,state->policy,owner.recovery_continuous_->recipe);
    if(!opened.owner||opened.settlement.state!=recovery_install_state::committed){if(opened.settlement.primary_error)std::rethrow_exception(opened.settlement.primary_error);refuse("continuous dedicated facade open failed");}
    // This private facade was just given a dedicated scheduler. Public shared
    // owners borrow theirs; only this internal creation transfers shutdown.
    owner.synchronizer_=std::unique_ptr<synchronizer>(new synchronizer(std::move(opened.owner),config,
        synchronizer::scheduler_ownership::owned));
    if(owner.on_sync_state_change_)owner.synchronizer_->set_on_state_change(owner.on_sync_state_change_);
    if(owner.on_sync_error_)owner.synchronizer_->set_on_error(owner.on_sync_error_);
    owner.synchronizer_->connect();
#else
    (void)owner;refuse("continuous browser route not implemented");
#endif
}

#ifndef __EMSCRIPTEN__
bool controller_publications_ended(const std::shared_ptr<recovery_continuous_state>& state,
    std::chrono::steady_clock::time_point end) noexcept {
    if(!state||state->controller_creator!=database_native_process())return false;
    try {
        std::unique_lock lock(state->mutex);
        if(!state->controller_deletion_retiring)return false;
        const bool observed=state->controller_publications_returned.wait_until(lock,end,[&]{
            return state->controller_publication_builders==0||state->controller_publication_unknown.load(std::memory_order_acquire);
        });
        return observed&&state->controller_publication_builders==0&&
            !state->controller_publication_unknown.load(std::memory_order_acquire);
    }catch(...){return false;}
}
controller_store_ending recovery_continuous_producer::retire_controller_for_deletion(
    const std::shared_ptr<lattice_db>& owner) {
    if(!owner)refuse("controller deletion requires its actual Native owner");
    owner->require_creator_process();
    if(!owner->is_closed())refuse("controller deletion ending requires prior logical close");
    controller_store_ending::owned_payload held(new controller_store_ending::payload); // before the producer leaf
    native_retirement_storage<std::shared_ptr<recovery_continuous_state>,
        std::shared_ptr<recovery_receiver_controller>> retiring; // admitted before effects
    if(!retiring.in_creator_process())refuse("controller deletion changed process during ending admission");
    owner->require_creator_process(); // allocation cannot authorize an inherited producer leaf
    auto& state=retiring.get<0>();auto& controller=retiring.get<1>();
    state=owner->recovery_continuous_?owner->recovery_continuous_->state:nullptr;
    if(state){
        if(state->controller_creator!=database_native_process())refuse("controller deletion producer belongs to another process");
        {
            std::lock_guard lock(state->mutex);
            state->controller_deletion_retiring=true;
            held->rows=state->controller_originals;
            held->first_failed_original=state->first_failed_controller_original;
            controller=state->controller.lock();
        }
        held->publication=state;
        if(controller)controller->retire_for_deletion(); // no producer/Native/store leaf
    }
    owner->require_creator_process(); // final off-leaf proof/writer release may have forked
    // Private successful freeze only. Empty is not inferred from weak expiry:
    // all actual candidates reserved rows before launching, and builders are
    // fenced/awaited separately, including unpublished/racing constructors.
    return controller_store_ending(held.release());
}
#endif
std::shared_ptr<recovery_receiver_route> recovery_continuous_producer::attach_receiver(
    const std::shared_ptr<recovery_continuous_route>& route,std::shared_ptr<lattice_db> owner,
    const std::shared_ptr<receiver_source_binding>& source,const std::shared_ptr<owned_platform_sync_transport>& transport,
    const std::shared_ptr<scheduler>& scheduler,const std::shared_ptr<sync_callback_lifetime>& lifetime) {
    if(!owner||!owner->recovery_continuous_)return {};
    owner->require_creator_process();
    auto state=owner->recovery_continuous_->state;
    if(!continuous_receiver_enabled(*state))return {};
    if(!route||route->state_!=state||route->owner_.lock()!=owner||route->incarnation_!=state->incarnation||
       !source||!transport||!scheduler||!lifetime)refuse("continuous receiver requires its actual authenticated platform route");
#ifndef __EMSCRIPTEN__
    if(state->controller_creator!=database_native_process())refuse("controller producer belongs to another process");
    struct publication_tail {
        const uint64_t creator=database_native_process();
        std::shared_ptr<recovery_continuous_state>* held;
        bool admitted=false;
        explicit publication_tail(const std::shared_ptr<recovery_continuous_state>& actual)
            :held(new std::shared_ptr<recovery_continuous_state>(actual)){}
        publication_tail(const publication_tail&)=delete;
        publication_tail& operator=(const publication_tail&)=delete;
        ~publication_tail(){
            if(creator!=database_native_process()){held=nullptr;return;}
            if(admitted){
                auto& state=**held;
                try {std::lock_guard lock(state.mutex);--state.controller_publication_builders;}
                catch(...) {state.controller_publication_unknown.store(true,std::memory_order_release);}
                state.controller_publications_returned.notify_all();
            }
            delete held; // final producer/callback ownership release OFF its leaf
        }
    } publication(state); // allocation precedes the actual counted admission
    {
        std::lock_guard lock(state->mutex);
        if(!state->controller_deletion_retiring&&state->controller_publication_builders!=SIZE_MAX){
            ++state->controller_publication_builders;publication.admitted=true;
        }
    }
    if(!publication.admitted)refuse("controller deletion retired publication admission");
    // No destructor, allocator or platform callback runs under the producer
    // leaf. Completed originals are observed first and retired outside it.
    std::shared_ptr<recovery_receiver_controller> controller;
    std::array<std::shared_ptr<controller_worker_original>,64> prior,retired;
    {std::lock_guard lock(state->mutex);controller=state->controller.lock();prior=state->controller_originals;}
    if(!controller){
        auto candidate_original=std::make_shared<controller_worker_original>();
        std::array<bool,64> joined{},failed{};
        for(size_t at=0;at<prior.size();++at)if(prior[at]){
            const auto observed=prior[at]->wait_until(std::chrono::steady_clock::now());
            joined[at]=observed.constructor_complete&&observed.actual==recovery_controller_executor::ending::disposition::joined;
            failed[at]=observed.constructor_failed;
        }
        size_t selected=prior.size();bool retiring=false;
        {
            std::lock_guard lock(state->mutex);
            retiring=state->controller_deletion_retiring;
            if(!retiring){
                for(size_t at=0;at<prior.size();++at)if(joined[at]&&state->controller_originals[at]==prior[at]){
                    if(failed[at]&&!state->first_failed_controller_original)
                        state->first_failed_controller_original=prior[at];
                    retired[at]=std::move(state->controller_originals[at]);
                }
                for(size_t at=0;at<prior.size();++at)if(!state->controller_originals[at]){
                    selected=at;state->controller_originals[at]=candidate_original;break;
                }
            }
        }
        if(retiring)refuse("controller deletion retired candidate construction");
        if(selected==prior.size())refuse("controller original ending custody capacity exhausted");
        std::shared_ptr<recovery_receiver_controller> candidate;
        try{candidate=std::shared_ptr<recovery_receiver_controller>(new recovery_receiver_controller(state->policy,candidate_original));}
        catch(...){candidate_original->failed_constructor(std::current_exception());throw;}
        if(state->controller_creator!=database_native_process())refuse("controller candidate belongs to another process");
        candidate_original->succeeded_constructor(); // shared_ptr construction/assignment actually returned
        {
            std::lock_guard lock(state->mutex);
            retiring=state->controller_deletion_retiring;
            controller=state->controller.lock();
            if(!retiring&&!controller){controller=candidate;state->controller=controller;}
        }
        if(retiring){candidate->retire_for_deletion();refuse("controller deletion refused late candidate publication");}
        // A racing candidate's actual worker retires off the producer leaf;
        // its independently reserved original remains until real reaper join.
    }
    return controller->attach(std::move(owner),route,source,transport,scheduler,lifetime);
#else
    refuse("continuous browser controller ending unavailable");
#endif
}
recovery_continuous_producer::cohort_admission recovery_continuous_producer::controller_cohort(
    const recovery_receiver_controller& controller,const std::shared_ptr<lattice_db>& owner,
    const std::shared_ptr<recovery_reconciliation_descriptor>& candidate) {
    if(!owner||owner->is_closed()||!owner->recovery_continuous_)refuse("controller cohort lost actual owner");
    const auto state=owner->recovery_continuous_->state;
    if(!continuous_receiver_enabled(*state))refuse("controller cohort requires enabled profile");
    // Only an empty off-lock shell can enter this private first registration.
    if(candidate){if(candidate->cohort_)refuse("controller cohort shell already registered");candidate->cohort_=std::make_shared<recovery_reconciliation_cohort>();}
    std::weak_ptr<const recovery_reconciliation_cohort> expired;
    {
        std::lock_guard lock(state->mutex);
        if(state->controller.lock().get()!=&controller)refuse("controller cohort physical registration changed");
        if(!state->reconciliation_cohort.expired())return cohort_admission::retained;
        if(candidate){expired=std::move(state->reconciliation_cohort);state->reconciliation_cohort=candidate->cohort_;}
    }
    // Even an expired weak control block is released outside the physical leaf.
    return cohort_admission::available;
}
bool recovery_continuous_producer::reconciliation_work_current(
    const std::shared_ptr<recovery_continuous_work>& work,const std::shared_ptr<lattice_db>& owner) {
    if(!work||!work->reconciliation_||work->owner_!=owner||!owner||!owner->recovery_continuous_||
       work->state_!=owner->recovery_continuous_->state)refuse("restricted export lost counted descriptor custody");
    return recovery_receiver_controller::reconciliation_route_current(work->reconciliation_,work->restricted_route_,owner,work->physical_);
}
namespace {
// One scoped producer context, never a callback or a generic publication hook.
// The common install retains the actual owner/writer throughout this frame.
struct controller_resume_commit_frame {
    static thread_local controller_resume_commit_frame* current;
    controller_resume_commit_frame* previous;
    const recovery_receiver_controller* controller;
    const lattice_db* owner;
    std::shared_ptr<recovery_continuous_admission> admission;
    std::shared_ptr<recovery_continuous_state> state;
    database* writer=nullptr;
    int64_t incarnation=0,barrier=0,attempt=0;
    uint64_t gate_revision=0;
    bool armed=false,published=false;
    controller_resume_commit_frame(const recovery_receiver_controller& c,const lattice_db* o)noexcept
        :previous(current),controller(&c),owner(o){current=this;}
    ~controller_resume_commit_frame(){current=previous;}
    controller_resume_commit_frame(const controller_resume_commit_frame&)=delete;
};
thread_local controller_resume_commit_frame* controller_resume_commit_frame::current=nullptr;
}
void publish_controller_resume_after_owned_commit(lattice_db& owner,database& writer) {
    auto* pending=controller_resume_commit_frame::current;
    if(!pending||pending->owner!=&owner||!pending->armed||pending->published)return;
    const auto& state=pending->state;
    if(pending->writer!=&writer||!state||!pending->admission||
       pending->admission->owner!=&owner||pending->admission->writer!=&writer||
       pending->admission->state!=state||owner.is_closed())
        refuse("controller committed resume lost exact owner/writer custody");
    // Same Native -> state-mutex order as controller_resume_owned. No SQL,
    // arbitrary callback, user payload or clock; successful publication does
    // not allocate. Refusal uses the existing bounded exception path.
    std::lock_guard lock(state->mutex);
    if(!state->initialized||state->controller.lock().get()!=pending->controller||
       state->incarnation!=pending->incarnation||state->barrier!=pending->barrier||
       state->attempt!=pending->attempt||state->gate_revision!=pending->gate_revision||
       state->operations||state->dml_open.load(std::memory_order_acquire))
        refuse("controller committed resume generation was revoked");
    state->dml_open.store(true,std::memory_order_release);pending->published=true;
}
recovery_install_result recovery_continuous_producer::controller_owned(const recovery_receiver_controller& controller,
    std::shared_ptr<lattice_db> owner,const std::function<void(database&)>& body,bool controller_try) {
    controller_resume_commit_frame publication(controller,owner.get());
    return recovery_writer_access::install_impl(owner,[&](database& db){
        if(!owner->recovery_continuous_)refuse("controller lost actual continuous owner");
        auto state=owner->recovery_continuous_->state;
        if(!continuous_receiver_enabled(*state))refuse("controller profile is not enabled");
        {std::lock_guard<std::mutex> lock(state->mutex);if(state->controller.lock().get()!=&controller)refuse("controller physical registration changed");}
        continuous_verify_row(db,*state);
        continuous_frame frame(*state,db,6);body(db);
        continuous_verify_row(db,*state);state->verify_file(db);
    },{},{},nullptr,controller_try);
}
void recovery_continuous_producer::controller_park_proof(verified_unsent_set& proof) {
    if(!proof.owner_||!proof.owner_->recovery_continuous_||!continuous_receiver_enabled(*proof.owner_->recovery_continuous_->state))refuse("controller proof parking requires actual enabled owner");
    proof.controller_owner_=proof.owner_;proof.owner_.reset();
}
int64_t recovery_continuous_producer::controller_next_attempt_owned(std::shared_ptr<lattice_db> owner) {
    if(!owner||!owner->recovery_continuous_)refuse("controller missing retained owner");
    const auto state=owner->recovery_continuous_->state;
    auto* writer=recovery_writer_access::active_writer(*owner);
    if(!writer||!continuous_install_owned(*owner->recovery_continuous_))refuse("controller sequence read outside checked operation");
    continuous_verify_row(*writer,*state);
    receive_install_store receiver(owner,state->policy.limits.installations);std::optional<int64_t> next;
    for(const auto& c:state->policy.contributions){const auto current=receiver.read(c.profile.binding.channel);
        if(!current||current->binding!=c.profile.binding||current->active||current->last_sequence==INT64_MAX)refuse("controller receiver cannot start next attempt");
        if(next&&*next!=current->last_sequence+1)refuse("controller contribution sequences differ");next=current->last_sequence+1;}
    if(!next)refuse("controller empty contribution inventory");return *next;
}
const recovery_owner_schema& recovery_continuous_producer::controller_catalog(const lattice_db& owner)noexcept {
    return owner.recovery_declarations();
}
void recovery_continuous_producer::controller_installed_owned(const recovery_receiver_controller& controller,const verified_unsent_set& proof) {
    auto owner=proof.owner_?proof.owner_:proof.controller_owner_.lock();auto state=proof.state_.lock();
    if(!owner||!state||!owner->recovery_continuous_||owner->recovery_continuous_->state!=state||
       !continuous_install_owned(*owner->recovery_continuous_)||proof.incarnation_!=state->incarnation||proof.policy_digest_!=state->digest)
        refuse("controller installation lost frozen physical custody");
    {std::lock_guard<std::mutex> lock(state->mutex);if(state->controller.lock().get()!=&controller||state->operations||state->dml_open.load())refuse("controller installation gate changed");}
    auto* writer=recovery_writer_access::active_writer(*owner);if(!writer)refuse("controller installation needs owned WRITE");
    continuous_verify_row(*writer,*state);const auto row=writer->query(continuous_query).at(0);
    if(integer(row,"phase")!=2||integer(row,"barrier")!=proof.barrier_)refuse("controller installation barrier changed");
    recovery_obligation_store journal(owner,state->policy.limits.obligations,state->policy.limits.installations);
    receive_install_store receiver(owner,state->policy.limits.installations);
    for(const auto& before:proof.journals_){const auto scope=journal.read(before.scope.address.channel);const auto actual=receiver.read(before.scope.address.channel);
        if(!scope||!actual||scope->address!=before.scope.address||scope->profile!=before.scope.profile||scope->mode!=recovery_obligation_mode::installed||
           !actual->last_installed||actual->active||scope->installed_sequence!=integer(row,"attempt")||scope->installed_sequence!=actual->last_installed->sequence||
           scope->installed_manifest!=actual->last_installed->manifest_digest||scope->installed_head!=actual->last_installed->head||scope->installed_revision!=actual->revision)
            refuse("controller missing exact all-contribution installed evidence");}
    writer->execute("UPDATE _lattice_producer_continuity SET phase=3 WHERE id=1 AND phase=2 AND barrier=? AND attempt=?",{proof.barrier_,integer(row,"attempt")});
    const auto after=writer->query(continuous_query).at(0);
    if(integer(after,"phase")!=3||integer(after,"barrier")!=proof.barrier_||integer(after,"attempt")!=integer(row,"attempt"))refuse("controller installed transition changed");
}
void recovery_continuous_producer::controller_resume_owned(const recovery_receiver_controller& controller,std::shared_ptr<lattice_db> owner,int64_t barrier,int64_t attempt) {
    if(!owner||!owner->recovery_continuous_||!continuous_install_owned(*owner->recovery_continuous_))refuse("controller resume outside checked WRITE");
    auto state=owner->recovery_continuous_->state;auto* writer=recovery_writer_access::active_writer(*owner);
    {std::lock_guard<std::mutex> lock(state->mutex);if(state->controller.lock().get()!=&controller||state->operations||state->dml_open.load())refuse("controller resume gate changed");}
    continuous_verify_row(*writer,*state);const auto row=writer->query(continuous_query).at(0);
    if(integer(row,"phase")!=3||integer(row,"barrier")!=barrier||integer(row,"attempt")!=attempt)refuse("controller resume durable barrier changed");
    recovery_obligation_store journal(owner,state->policy.limits.obligations,state->policy.limits.installations);
    receive_install_store receiver(owner,state->policy.limits.installations);
    for(const auto& c:state->policy.contributions){const auto scope=journal.read(c.profile.binding.channel);const auto actual=receiver.read(c.profile.binding.channel);
        if(!scope||scope->profile!=c.profile||scope->mode!=recovery_obligation_mode::recording||!actual||actual->active||!actual->last_installed||
           scope->installed_sequence!=attempt||actual->last_installed->sequence!=attempt||scope->installed_manifest!=actual->last_installed->manifest_digest||
           scope->installed_head!=actual->last_installed->head||scope->installed_revision!=actual->revision)refuse("controller resume lacks all exact resumed journals");}
    writer->execute("UPDATE _lattice_producer_continuity SET phase=0 WHERE id=1 AND phase=3 AND barrier=? AND attempt=?",{barrier,attempt});
    const auto after=writer->query(continuous_query).at(0);
    if(integer(after,"phase")!=0||integer(after,"barrier")!=barrier||integer(after,"attempt")!=attempt)refuse("controller resume transition changed");
    // Arm only after the actual resumed journals and exact phase-0 postimage.
    // A body failure/rollback/failed COMMIT discards this frame without opening.
    auto* publication=controller_resume_commit_frame::current;
    if(!publication||publication->owner!=owner.get()||publication->controller!=&controller||publication->armed)
        refuse("controller resume lacks exact scoped publication custody");
    std::lock_guard lock(state->mutex);
    if(state->controller.lock().get()!=&controller||state->operations||state->dml_open.load()||
       state->barrier!=barrier||state->attempt!=attempt||!state->initialized)
        refuse("controller resume publication generation changed");
    publication->admission=owner->recovery_continuous_;publication->state=state;publication->writer=writer;
    publication->incarnation=state->incarnation;publication->barrier=barrier;publication->attempt=attempt;
    publication->gate_revision=state->gate_revision;publication->armed=true;
}
void recovery_continuous_producer::controller_restart_terminal_owned(const recovery_receiver_controller& controller,
    std::shared_ptr<lattice_db> owner,int64_t physical_incarnation,int64_t barrier,int64_t attempt) {
    if(!owner||!owner->recovery_continuous_||!continuous_install_owned(*owner->recovery_continuous_))refuse("terminal restart lacks actual controller WRITE");
    auto state=owner->recovery_continuous_->state;auto& db=*recovery_writer_access::active_writer(*owner);
    {std::lock_guard lock(state->mutex);if(state->controller.lock().get()!=&controller||state->operations||state->dml_open.load()||
        !state->reconciliation_cohort.expired()||state->gate_revision==UINT64_MAX)refuse("terminal restart retained work or cohort remains live");}
    continuous_verify_row(db,*state);const auto row=db.query(continuous_query).at(0);
    if(integer(row,"phase")!=2||integer(row,"incarnation")!=physical_incarnation||integer(row,"barrier")!=barrier||integer(row,"attempt")!=attempt||
        barrier<=0||barrier==INT64_MAX||attempt<=0||attempt==INT64_MAX)refuse("terminal restart physical frozen attempt changed or exhausted");
    recovery_obligation_store journals(owner,state->policy.limits.obligations,state->policy.limits.installations);
    receive_install_store receiver(owner,state->policy.limits.installations);
    for(const auto& c:state->policy.contributions){const auto scope=journals.read(c.profile.binding.channel);const auto actual=receiver.read(c.profile.binding.channel);
        if(!scope||scope->profile!=c.profile||scope->mode!=recovery_obligation_mode::recording||scope->last_attempt!=attempt||
            scope->freeze_revision==INT64_MAX||scope->revision!=scope->freeze_revision+1||!actual||actual->binding!=c.profile.binding||
            actual->active||actual->last_sequence!=attempt||(actual->last_installed&&actual->last_installed->sequence>=attempt))
            refuse("terminal restart lacks every exact canceled journal/receiver");}
    db.execute("UPDATE _lattice_producer_continuity SET phase=1,barrier=?,attempt=? WHERE id=1 AND phase=2 AND barrier=? AND attempt=?",{barrier+1,attempt+1,barrier,attempt});
    const auto after=db.query(continuous_query);auto expected=row;expected["phase"]=int64_t(1);expected["barrier"]=barrier+1;expected["attempt"]=attempt+1;
    if(after.size()!=1||after[0]!=expected)refuse("terminal restart lineage postimage changed");state->verify_file(db);
}
void recovery_continuous_producer::controller_transition_reconcile_owned(const recovery_receiver_controller& controller,
    std::shared_ptr<lattice_db> owner,int64_t phase,int64_t barrier,int64_t attempt,int64_t next_phase,int64_t next_barrier,int64_t next_attempt) {
    if(!owner||!owner->recovery_continuous_||!continuous_install_owned(*owner->recovery_continuous_))refuse("reconciliation transition lacks actual controller WRITE");
    auto state=owner->recovery_continuous_->state;auto& db=*recovery_writer_access::active_writer(*owner);
    {std::lock_guard lock(state->mutex);if(state->controller.lock().get()!=&controller||state->operations||state->dml_open.load()||state->gate_revision==UINT64_MAX)refuse("reconciliation counted work remains live or runtime generation exhausted");}
    continuous_verify_row(db,*state);const auto row=db.query(continuous_query).at(0);
    if(integer(row,"phase")!=phase||integer(row,"barrier")!=barrier||integer(row,"attempt")!=attempt||
       !((phase==2&&next_phase==4&&barrier<INT64_MAX&&attempt<INT64_MAX&&next_barrier==barrier+1&&next_attempt==attempt+1)||
         (phase==4&&next_phase==1&&next_barrier==barrier&&next_attempt==attempt)))refuse("reconciliation phase transition differs");
    db.execute("UPDATE _lattice_producer_continuity SET phase=?,barrier=?,attempt=? WHERE id=1 AND phase=? AND barrier=? AND attempt=?",{next_phase,next_barrier,next_attempt,phase,barrier,attempt});
    const auto after=db.query(continuous_query);if(after.size()!=1)refuse("reconciliation lineage disappeared");
    auto expected=row;expected["phase"]=next_phase;expected["barrier"]=next_barrier;expected["attempt"]=next_attempt;
    if(after[0]!=expected)refuse("reconciliation lineage postimage changed");state->verify_file(db);
}
void recovery_continuous_producer::controller_publish_reconcile(const recovery_receiver_controller& controller,
    std::shared_ptr<lattice_db> owner,int64_t barrier,int64_t attempt) {
    if(!owner||owner->is_closed()||!owner->recovery_continuous_)refuse("reconciliation runtime owner retired");auto state=owner->recovery_continuous_->state;
    std::lock_guard lock(state->mutex);
    if(state->controller.lock().get()!=&controller||state->operations||state->dml_open.load())refuse("reconciliation runtime admission changed");
    if(state->gate_revision==UINT64_MAX)refuse("reconciliation runtime generation exhausted");
    ++state->gate_revision;state->barrier=barrier;state->attempt=attempt;
}
void recovery_continuous_producer::controller_publish_resume(const recovery_receiver_controller& controller,std::shared_ptr<lattice_db> owner,int64_t barrier,int64_t attempt) {
    if(!owner||owner->is_closed()||!owner->recovery_continuous_)return;
    const auto state=owner->recovery_continuous_->state;
    std::lock_guard<std::mutex> lock(state->mutex);
    // Offlock acknowledgement only. Opening authority was consumed at known
    // COMMIT under Native admission; this old tail cannot reopen a new barrier.
    if(state->controller.lock().get()==&controller&&state->barrier==barrier&&state->attempt==attempt&&
       !state->operations&&!state->dml_open.load(std::memory_order_acquire))
        refuse("controller resume was not published with known COMMIT");
}
