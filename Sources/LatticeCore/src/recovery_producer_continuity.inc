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
            // Count actual distinct unresolved originals after every generated
            // contribution has been inserted, in this same model transaction.
            // The 8,193-row result is bounded; SQLite may scan the admitted
            // journal's full finite raw-record cap (at most 100,000 records).
            tail+=require("(SELECT COUNT(*) FROM (SELECT DISTINCT original FROM _lattice_obligation_entry WHERE stage<>2 ORDER BY original LIMIT 8193))<=8192");
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
    discovery_statement query(writer.internal_handle(),"SELECT 1 FROM main.sqlite_schema WHERE name='_lattice_producer_continuity' LIMIT 1");
    const bool present=query.row();
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
std::shared_ptr<database> recovery_continuous_producer::open_writer(const configuration& config,const std::shared_ptr<recovery_continuous_admission>& admission) {
    if(!admission||!admission->state||admission->state->path!=config.path)refuse("continuous writer lacks exact container custody");
    auto writer=std::make_shared<database>(config.path,database::open_mode::read_write,config.busy_timeout_ms,
        std::shared_ptr<database_read_control>{},database::initialization_key(admission));
    writer->suppress_destructor_optimize_=true; // Immutable profile: refusal/close must not run optional ANALYZE.
    auto& state=*admission->state;
    const auto identity=writer->physical_identity("main",{},true);
    if(!identity)refuse("continuous SQLite physical file identity unavailable");
    {std::lock_guard<std::mutex> lock(state.mutex);
     if(state.identity&&(!(*state.identity==*identity)||state.identity->filename!=identity->filename))refuse("continuous facade file identity differs");
     if(!state.identity)state.identity=identity;}
    state.verify_file(*writer);
    writer->execute("PRAGMA synchronous=FULL");
    if(!state.fresh||state.initialized)continuous_verify_row(*writer,state,false);
    else if(!writer->query("SELECT 1 FROM main.sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1").empty())
        refuse("continuous fresh container already has a durable schema");
    admission->writer=writer.get();
    return writer;
}
std::shared_ptr<database> open_continuous_writer(const configuration& config,const std::shared_ptr<recovery_continuous_admission>& admission) {
    return recovery_continuous_producer::open_writer(config,admission);
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
        if(!recipe||!recipe->construct)refuse("continuous retained owner recipe unavailable");
        if(requested.path.empty()||requested.path.starts_with("file:")||configuration::path_is_memory(requested.path))refuse("continuous factory requires an explicit native file path");
        configuration config=requested;
        const auto lexical=std::filesystem::absolute(config.path).lexically_normal();
        config.path=(std::filesystem::canonical(lexical.parent_path().parent_path())/lexical.parent_path().filename()/lexical.filename()).string();
        auto admission=continuous_acquire(config,policy);
        admission->recipe=recipe;
        owner=recipe->construct(config,admission);
        if(!owner||owner->recovery_continuous_!=admission||owner->db_.get()!=admission->writer)
            refuse("continuous recipe did not retain the admitted physical owner");
        admission->owner=owner.get();
        admission->retained_owner=owner;
        output.settlement=enroll(owner);
        if(output.settlement.state!=recovery_install_state::committed||output.settlement.postcommit_error){
            try{owner->close();}catch(...){output.settlement.cleanup_error=std::current_exception();}
            return output;
        }
        // A known enrollment COMMIT survives every later publication failure.
        // No callback, key-cache hit or borrowed pointer may grant admission.
        if(recipe->publish_pointer)recipe->publish_pointer(owner);
        instance_registry::instance().register_instance(config.path,owner.get(),owner->guard_);
        owner->adopt_path_wal_eviction_threshold();owner->setup_cross_process_notifier();
        owner->setup_sync_if_configured();
        output.owner=owner;
    } catch(...) {
        const auto error=std::current_exception();
        if(owner){try{owner->close();}catch(...){output.settlement.cleanup_error=std::current_exception();}}
        output.owner.reset();
        if(output.settlement.state==recovery_install_state::committed)output.settlement.postcommit_error=error;
        else output.settlement.primary_error=error;
    }
    return output;
}
recovery_install_result recovery_continuous_producer::enroll(std::shared_ptr<lattice_db> owner) {
    const auto admission=owner->recovery_continuous_;auto state=admission->state;
    std::shared_ptr<recovery_local_producer_adapter::context> candidate;
    int64_t new_incarnation=0,phase=0,barrier=0,attempt=0;
    const auto result=recovery_writer_access::install_impl(owner,[&](database& db){
        state->verify_file(db);recovery_local_producer_adapter::validate_custody(*owner,db);
        recovery_local_producer_adapter::management management(db.internal_handle());continuous_frame changing(*state,db,1);
        const bool existing=db.table_exists(continuous_table);
        if(state->fresh&&!state->initialized&&existing)refuse("continuous fresh file cannot adopt an existing profile");
        if(!existing) {
            if(!state->fresh||state->initialized)refuse("continuous orphan or missing lineage cannot be adopted");
            // Fresh owned WRITE is authoritative. No legacy AuditLog, receiver,
            // journal or producer history may be relabeled into coverage.
            if(!db.query("SELECT 1 FROM main.AuditLog LIMIT 1").empty()||
               !db.query("SELECT 1 FROM main.sqlite_schema WHERE name GLOB '_lattice_obligation_*' OR name GLOB '_lattice_install_*' LIMIT 1").empty())
                refuse("continuous fresh enrollment found prior history");
            candidate=std::make_shared<recovery_local_producer_adapter::context>();candidate->connection=db.internal_handle();candidate->owner=owner.get();candidate->lifetime=owner->guard_;
            candidate->admitted_limits=state->policy.limits;candidate->continuous=admission;
            receive_install_store receiver(owner,state->policy.limits.installations);receiver.initialize();
            recovery_obligation_store journal(owner,state->policy.limits.obligations,state->policy.limits.installations);journal.initialize();
            recovery_obligation_producer_store producer(owner,state->policy.limits.obligations,state->policy.limits.installations,state->policy.limits.producers);producer.initialize();
            for(const auto& contribution:state->policy.contributions){
                receiver.bind(contribution.profile.binding);const auto scope=journal.bind(contribution.profile);
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
            continuous_verify_row(db,*state,false);
            const auto row=db.query(continuous_query).at(0);const auto old=integer(row,"incarnation");
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
    });
    if(candidate&&result.state!=recovery_install_state::committed)candidate->active->store(false,std::memory_order_release);
    return result;
}
recovery_continuous_route::~recovery_continuous_route(){
    if(state_){std::lock_guard<std::mutex> lock(state_->mutex);--state_->routes;}
    if(configured_unregistered_)configured_unregistered_();
}
recovery_continuous_work::~recovery_continuous_work(){if(state_){std::lock_guard<std::mutex> lock(state_->mutex);--state_->operations;}}
std::shared_ptr<recovery_continuous_route> recovery_continuous_producer::admit_route(std::shared_ptr<lattice_db> owner,const sync_config& config,bool injected) {
    if(!owner)return {};
    if(!owner->recovery_continuous_){require_no_continuous_export(*owner->db_);return {};}
    auto state=owner->recovery_continuous_->state;
    if(injected||config.sync_filter)refuse("continuous injected/filtered export topology not implemented");
    bool allowed=false;for(const auto& route:state->policy.routes)if(route.sync_id==config.sync_id&&route.endpoint==config.websocket_url)allowed=true;
    if(!allowed)refuse("continuous WSS route absent from immutable policy");
    const auto checked=recovery_writer_access::install(owner,[&](database& db){continuous_verify_row(db,*state);recovery_local_producer_adapter::profiles_for_owned_write(owner,state->policy.limits);});
    if(checked.state!=recovery_install_state::committed){if(checked.primary_error)std::rethrow_exception(checked.primary_error);refuse("continuous route admission did not commit");}
    auto route=std::shared_ptr<recovery_continuous_route>(new recovery_continuous_route);
    {std::lock_guard<std::mutex> lock(state->mutex);
     if(!state->initialized||(!state->dml_open.load(std::memory_order_acquire)&&!continuous_receiver_enabled(*state))||state->routes==state->policy.physical_routes||state->next_route==std::numeric_limits<uint64_t>::max())refuse("continuous route gate closed or full");
     route->route_=++state->next_route;route->incarnation_=state->incarnation;route->owner_=owner;route->state_=state;++state->routes;}
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
    const std::shared_ptr<recovery_continuous_work>& work,const std::vector<std::string>& originals,const std::function<void(database&)>& body,bool* admission_busy,bool automatic) {
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
    },{},{},automatic?nullptr:admission_busy,automatic);
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
            const auto policy=state->policy;
            configured=configured_recovery_connection::create(actual_owner,config,factory,typed,[path,busy_timeout,schema_version,recipe,policy]{
                configuration child(path,std::make_shared<std_thread_scheduler>());
                child.busy_timeout_ms=busy_timeout;child.target_schema_version=schema_version;
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

std::shared_ptr<recovery_receiver_route> recovery_continuous_producer::attach_receiver(
    const std::shared_ptr<recovery_continuous_route>& route,std::shared_ptr<lattice_db> owner,
    const std::shared_ptr<receiver_source_binding>& source,const std::shared_ptr<owned_platform_sync_transport>& transport,
    const std::shared_ptr<scheduler>& scheduler,const std::shared_ptr<sync_callback_lifetime>& lifetime) {
    if(!owner||!owner->recovery_continuous_)return {};
    auto state=owner->recovery_continuous_->state;
    if(!continuous_receiver_enabled(*state))return {};
    if(!route||route->state_!=state||route->owner_.lock()!=owner||route->incarnation_!=state->incarnation||
       !source||!transport||!scheduler||!lifetime)refuse("continuous receiver requires its actual authenticated platform route");
    // Construct outside the leaf: no destructor, allocator or platform callback
    // runs while a producer registration lock is held.
    auto candidate=std::shared_ptr<recovery_receiver_controller>(new recovery_receiver_controller(state->policy));
    std::shared_ptr<recovery_receiver_controller> controller;
    {std::lock_guard<std::mutex> lock(state->mutex);controller=state->controller.lock();if(!controller){controller=candidate;state->controller=controller;}}
    return controller->attach(std::move(owner),route,source,transport,scheduler,lifetime);
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
recovery_install_result recovery_continuous_producer::controller_owned(const recovery_receiver_controller& controller,
    std::shared_ptr<lattice_db> owner,const std::function<void(database&)>& body,bool controller_try) {
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
    if(state->controller.lock().get()==&controller&&state->barrier==barrier&&state->attempt==attempt&&!state->operations)
        state->dml_open.store(true,std::memory_order_release);
}
