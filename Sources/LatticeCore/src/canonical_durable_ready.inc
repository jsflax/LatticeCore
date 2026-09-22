// Included after the retained context/session definitions. No standalone
// package writer or caller-supplied protection proof is exposed.
namespace {
namespace cr=canonical_range;
constexpr int64_t ready_preparing=1,ready_complete=2;
constexpr uint64_t ready_route=1; // immutable stored spelling; not route authority
constexpr int64_t ready_key_limit=2048;
int64_t ready_add(int64_t a,int64_t b) {
    if(a<0 || b<0 || a>std::numeric_limits<int64_t>::max()-b)refuse("canonical READY charge overflow");return a+b;
}
std::string ready_blob(const database::row_t& row,const char* key) {
    const auto i=row.find(key);if(i==row.end() || !std::holds_alternative<blob>(i->second))refuse("canonical READY expected bounded bytes");
    const auto& value=std::get<blob>(i->second);return {value.begin(),value.end()};
}
std::string ready_i(const char* key) {return "CASE WHEN typeof("+std::string(key)+")='integer' THEN "+key+" END AS "+key;}
std::string ready_b(const char* key,int64_t cap) {
    return "CASE WHEN typeof("+std::string(key)+")='blob' AND length("+key+")<="+std::to_string(cap)+" THEN "+key+" END AS "+key;
}
void ready_part(std::string& out,const std::string& part) {out+=std::to_string(part.size())+":"+part;}
std::string ready_binding(const std::string& ns,const std::string& replica,const cr::attempt& attempt) {
    std::string key;for(const auto* part:{&ns,&replica,&attempt.receiver_incarnation,&attempt.channel_incarnation,&attempt.channel}) {
        if(part->empty() || part->size()>256)refuse("canonical READY binding outside bounds");ready_part(key,*part);
    }
    if(key.size()>ready_key_limit)refuse("canonical READY binding exceeds durable cap");return key;
}
cr::source_binding ready_source(const canonical_writer_profile& p,const canonical_ready_profile& ready) {
    return {ready.authority,p.binding.source,p.binding.epoch,p.binding.scope,p.binding.schema};
}
std::string ready_policy(const canonical_ready_profile& p) {
    std::string out="canonical-ready-policy-v3;";ready_part(out,p.authority);
    const auto number=[&](uint64_t n){out+=std::to_string(n)+";";};
    for(auto n:{p.transfers,p.bindings,p.charged_bytes,p.transfer_bytes})number(n);
    const auto& b=p.package.codec;const auto& w=b.maximum;
    for(auto n:{w.frame_bytes,w.payload_bytes,w.items_per_page,w.content_pages,w.content_identities,w.content_bytes,
                w.receipt_pages,w.receipts,w.receipt_bytes})number(n);
    for(auto n:{b.depth,b.nodes,b.string_bytes,b.request_entries,b.request_targets,b.request_target_bytes,b.restart_bytes})number(n);
    number(b.lease_ms);
    for(auto n:{b.values.raw_bytes,b.values.fields,b.values.name_bytes,b.values.value_bytes,b.values.decoded_bytes})number(n);
    number(p.package.retained_wire_bytes);number(p.package.frames);
    const auto& c=p.capture;const auto& s=c.rows;const auto& v=s.wire;
    for(auto n:{v.frame_bytes,v.depth,v.nodes,v.string_bytes,v.rows_per_page})number(n);
    number(v.pages);number(v.total_rows);number(v.total_bytes);
    for(auto n:{s.tables,s.columns_per_table,s.indexes_per_table})number(n);
    const auto& l=c.store;
    for(auto n:{l.markers,l.marker_bytes,l.receipts,l.receipt_bytes,l.batch_identities,l.identity_bytes,l.operation_bytes})number(n);
    number(c.requests);number(c.requested_targets);number(c.marker_batch);
    if(out.size()>4096)refuse("canonical READY policy serialization too large");return out;
}
void validate_ready_profile(const canonical_ready_profile& p,const canonical_writer_profile& source,const canonical_retention_limits& retention) {
    if(p.authority.empty() || p.authority.size()>256 || p.transfers<=0 || p.transfers>64 || p.bindings<=0 || p.bindings>1024 ||
       p.charged_bytes<=0 || p.charged_bytes>512LL*1024*1024 || p.transfer_bytes<4096 || p.transfer_bytes>p.charged_bytes ||
       p.package.retained_wire_bytes==0 || p.package.retained_wire_bytes>static_cast<uint64_t>(p.transfer_bytes) ||
       p.package.frames<2 || p.package.frames>65536 || p.package.codec.maximum.frame_bytes>1024*1024 ||
       p.package.codec.lease_ms>static_cast<uint64_t>(retention.duration_ms) || p.capture.store!=source.limits)
        refuse("canonical READY invalid bounded persistent policy");
    const auto& c=p.capture;const auto& s=c.rows;const auto& v=s.wire;
    if(!v.frame_bytes || v.frame_bytes>16*1024*1024 || v.string_bytes<64 || v.string_bytes>v.frame_bytes ||
       !v.rows_per_page || v.rows_per_page>4096 || !v.pages || v.pages>65536 || !v.total_rows || v.total_rows>1048576 ||
       !v.total_bytes || v.total_bytes>static_cast<uint64_t>(p.transfer_bytes) || !v.depth || v.depth>64 || !v.nodes || v.nodes>65536 ||
       !s.tables || s.tables>64 || !s.columns_per_table || s.columns_per_table>128 || !s.indexes_per_table || s.indexes_per_table>128 ||
       !c.requests || c.requests>4096 || !c.requested_targets || c.requested_targets>4096 || !c.marker_batch || c.marker_batch>4096)
        refuse("canonical READY capture policy outside hard caps");
    cr::attempt a{"00000000-0000-4000-8000-000000000001","00000000-0000-4000-8000-000000000002","qualification",1,
                  "00000000-0000-4000-8000-000000000003"};
    cr::request q;q.source=ready_source(source,p);q.budget=p.package.codec.maximum;
    q.request_digest=cr::request_sha256(a,q,p.package.codec);
    (void)cr::encode({a,ready_route,q},p.package.codec);(void)ready_policy(p);
}
void append_ready_schema(retention_inventory& out) {
    out[{"table","_lattice_canonical_ready_profile"}]=
        "CREATE TABLE _lattice_canonical_ready_profile(id INTEGER PRIMARY KEY CHECK(id=1),policy BLOB NOT NULL,active INTEGER NOT NULL,charged INTEGER NOT NULL,bindings INTEGER NOT NULL,next_lease INTEGER NOT NULL) WITHOUT ROWID";
    out[{"table","_lattice_canonical_ready_binding"}]=
        "CREATE TABLE _lattice_canonical_ready_binding(binding BLOB PRIMARY KEY,sequence INTEGER NOT NULL,charge INTEGER NOT NULL) WITHOUT ROWID";
    out[{"table","_lattice_canonical_ready_transfer"}]=
        "CREATE TABLE _lattice_canonical_ready_transfer(binding BLOB PRIMARY KEY,sequence INTEGER NOT NULL,request BLOB NOT NULL,request_sha BLOB NOT NULL,namespace_id BLOB NOT NULL,coverage_id BLOB NOT NULL,coverage_revision INTEGER NOT NULL,replica BLOB NOT NULL,descriptor BLOB NOT NULL,state INTEGER NOT NULL,reservation INTEGER NOT NULL,pin INTEGER NOT NULL,head INTEGER NOT NULL,floor INTEGER NOT NULL,frame_count INTEGER NOT NULL,wire_bytes INTEGER NOT NULL,frames_sha BLOB NOT NULL,manifest_sha BLOB NOT NULL,manifest_lease BLOB NOT NULL,manifest_duration INTEGER NOT NULL,lease_incarnation INTEGER NOT NULL,lease_sequence INTEGER NOT NULL,deadline_ms INTEGER NOT NULL) WITHOUT ROWID";
    out[{"table","_lattice_canonical_ready_frame"}]=
        "CREATE TABLE _lattice_canonical_ready_frame(binding BLOB NOT NULL,frame_index INTEGER NOT NULL,data BLOB NOT NULL,sha256 BLOB NOT NULL,PRIMARY KEY(binding,frame_index)) WITHOUT ROWID";
    for(const auto* table:{"_lattice_canonical_ready_profile","_lattice_canonical_ready_binding","_lattice_canonical_ready_transfer","_lattice_canonical_ready_frame"})
        for(const auto* action:{"INSERT","UPDATE","DELETE"}) {
            const auto name=std::string(table)+"_guard_"+action;
            out[{"trigger",name}]="CREATE TRIGGER "+name+" BEFORE "+action+" ON "+table+" BEGIN"+demand("lattice_canonical_retention_guard_v1()=1")+" END";
        }
}
const char* ready_inventory_query() {
    return "SELECT type,CASE WHEN length(CAST(name AS BLOB))<=128 THEN name END AS name,CASE WHEN typeof(sql)='text' AND length(CAST(sql AS BLOB))<=8192 THEN sql END AS sql FROM main.sqlite_master WHERE "
        "(type='table' AND substr(name,1,19)='_lattice_canonical_') OR (type='trigger' AND substr(tbl_name,1,19)='_lattice_canonical_') OR "
        "(substr(tbl_name,1,19)='_lattice_canonical_' AND sql IS NOT NULL) LIMIT 64";
}
bool ready_authorize(int op,int action,const char* table,const char* column)noexcept {
    const auto is=[&](const char* name){return std::strcmp(table,name)==0;};
    const bool dispose=op==ready_abandon || op==retention_expire || op==retention_prune;
    if(is("_lattice_canonical_attempt"))return (action==SQLITE_INSERT && op==ready_prepare) || (action==SQLITE_DELETE && (op==ready_publish || dispose));
    if(is("_lattice_canonical_retention"))return action==SQLITE_UPDATE && op==ready_prepare && column && std::strcmp(column,"next_attempt")==0;
    if(is("_lattice_canonical_ready_frame"))return (action==SQLITE_INSERT && op==ready_publish) || (action==SQLITE_DELETE && dispose);
    if(is("_lattice_canonical_ready_binding"))return op==ready_prepare && (action==SQLITE_INSERT ||
        (action==SQLITE_UPDATE && column && std::strcmp(column,"sequence")==0));
    if(is("_lattice_canonical_ready_profile")) {
        if(action!=SQLITE_UPDATE || !column)return false;
        if(std::strcmp(column,"next_lease")==0)return op==ready_prepare || op==ready_resume;
        if(std::strcmp(column,"bindings")==0)return op==ready_prepare;
        if(std::strcmp(column,"active")==0 || std::strcmp(column,"charged")==0)return op==ready_prepare || dispose;
        return false;
    }
    if(!is("_lattice_canonical_ready_transfer"))return false;
    if(action==SQLITE_INSERT)return op==ready_prepare;
    if(action==SQLITE_DELETE)return dispose;
    if(action!=SQLITE_UPDATE || !column)return false;
    if(op==ready_resume)return std::strcmp(column,"lease_incarnation")==0 || std::strcmp(column,"lease_sequence")==0 || std::strcmp(column,"deadline_ms")==0;
    if(op==ready_publish)for(const auto* allowed:{"state","reservation","pin","head","floor","frame_count","wire_bytes","frames_sha","manifest_sha"})
        if(std::strcmp(column,allowed)==0)return true;
    return false;
}
std::string ready_transfer_query(const canonical_ready_profile& p) {
    std::string sql="SELECT ";bool first=true;
    const auto add=[&](const std::string& field){if(!first)sql+=",";first=false;sql+=field;};
    for(const auto* n:{"sequence","coverage_revision","state","reservation","pin","head","floor","frame_count","wire_bytes",
                       "manifest_duration","lease_incarnation","lease_sequence","deadline_ms"})add(ready_i(n));
    add(ready_b("binding",ready_key_limit));add(ready_b("request",p.package.codec.maximum.frame_bytes));
    for(const auto* n:{"request_sha","descriptor","frames_sha","manifest_sha"})add(ready_b(n,64));
    for(const auto* n:{"namespace_id","coverage_id","replica","manifest_lease"})add(ready_b(n,256));
    return sql+" FROM main._lattice_canonical_ready_transfer";
}
database::row_t ready_profile_row(database& db,const canonical_ready_profile& p) {
    auto rows=db.query("SELECT "+ready_i("id")+","+ready_b("policy",4096)+","+ready_i("active")+","+ready_i("charged")+","+
        ready_i("bindings")+","+ready_i("next_lease")+" FROM main._lattice_canonical_ready_profile LIMIT 2");
    if(rows.size()!=1 || integer(rows[0],"id")!=1 || ready_blob(rows[0],"policy")!=ready_policy(p) ||
       integer(rows[0],"active")<0 || integer(rows[0],"active")>p.transfers || integer(rows[0],"bindings")<0 || integer(rows[0],"bindings")>p.bindings ||
       integer(rows[0],"charged")<0 || integer(rows[0],"charged")>p.charged_bytes || integer(rows[0],"next_lease")<0)
        refuse("canonical READY durable policy or counters differ");
    return rows[0];
}
database::row_t ready_row(database& db,const canonical_ready_profile& p,const std::string& key) {
    const auto rows=db.query(ready_transfer_query(p)+" WHERE binding=? LIMIT 2",{bytes(key)});
    if(rows.size()!=1)refuse("canonical READY transfer missing or terminal");return rows[0];
}
cr::frame ready_request(const database::row_t& row,const canonical_ready_profile& p) {
    const auto raw=ready_blob(row,"request");
    if(picosha2::hash256_hex_string(raw)!=ready_blob(row,"request_sha"))refuse("canonical READY frozen request hash differs");
    auto request=cr::decode(raw,p.package.codec);
    if(request.route_generation!=ready_route || !std::holds_alternative<cr::request>(request.body) || cr::encode(request,p.package.codec)!=raw)
        refuse("canonical READY request is not exact canonical spelling");
    return request;
}
void ready_hash_frame(picosha2::hash256_one_by_one& hash,const std::string& raw) {
    const auto size=std::to_string(raw.size())+":";hash.process(size.begin(),size.end());hash.process(raw.begin(),raw.end());
}
std::string ready_frame(database& db,const canonical_ready_profile& p,const std::string& key,int64_t index) {
    const auto rows=db.query("SELECT "+ready_b("data",p.package.codec.maximum.frame_bytes)+","+ready_b("sha256",64)+
        " FROM main._lattice_canonical_ready_frame WHERE binding=? AND frame_index=? LIMIT 2",{bytes(key),index});
    if(rows.size()!=1)refuse("canonical READY frame missing");
    const auto raw=ready_blob(rows[0],"data");if(raw.empty() || picosha2::hash256_hex_string(raw)!=ready_blob(rows[0],"sha256"))refuse("canonical READY frame hash differs");return raw;
}
int64_t ready_metadata_charge(const database::row_t& row) {
    int64_t count=512;
    for(const auto* name:{"binding","request","request_sha","namespace_id","coverage_id","replica","descriptor","manifest_lease"})
        count=ready_add(count,ready_blob(row,name).size());
    return ready_add(count,128); // fixed manifest/frame digests, including PREPARING reservation
}
cr::manifest audit_ready_frames(database& db,const canonical_ready_profile& p,const database::row_t& row,
    const std::function<void(const cr::receipt_item&)>& validate_receipt={}) {
    const auto request=ready_request(row,p);const auto& q=std::get<cr::request>(request.body);const auto key=ready_blob(row,"binding");
    const auto n=integer(row,"frame_count");if(n<2 || n>static_cast<int64_t>(p.package.frames))refuse("canonical READY frame count outside cap");
    const auto counts=db.query("SELECT COUNT(*) AS n FROM main._lattice_canonical_ready_frame WHERE binding=?",{bytes(key)});
    if(integer(counts.at(0),"n")!=n)refuse("canonical READY frame inventory differs");
    auto first=cr::decode(ready_frame(db,p,key,0),p.package.codec);
    const auto* m=std::get_if<cr::manifest>(&first.body);if(!m)refuse("canonical READY first frame is not manifest");
    const auto manifest=*m;
    if(manifest.head!=static_cast<uint64_t>(integer(row,"head")) || manifest.manifest_digest!=ready_blob(row,"manifest_sha") ||
       manifest.protection!=cr::lease{ready_blob(row,"manifest_lease"),static_cast<uint64_t>(integer(row,"manifest_duration"))})
        refuse("canonical READY immutable manifest binding differs");
    auto sequence=cr::begin(request.logical,q,manifest,p.package.codec);
    cr::stream_hasher content(manifest,cr::stream_kind::content,p.package.codec),receipts(manifest,cr::stream_kind::receipts,p.package.codec);
    picosha2::hash256_one_by_one hash;int64_t wire=0,charge=ready_metadata_charge(row);
    for(int64_t i=0;i<n;++i) {
        const auto raw=ready_frame(db,p,key,i);wire=ready_add(wire,raw.size());charge=ready_add(charge,ready_add(raw.size(),128));ready_hash_frame(hash,raw);
        if(wire>static_cast<int64_t>(p.package.retained_wire_bytes) || charge>p.transfer_bytes)refuse("canonical READY bytes exceed charged cap");
        const auto frame=cr::decode(raw,p.package.codec);
        if(frame.logical!=request.logical || frame.route_generation!=ready_route || cr::encode(frame,p.package.codec)!=raw)
            refuse("canonical READY frame association or canonical bytes differ");
        if(i)sequence=cr::propose(sequence,frame,p.package.codec);
        if(const auto* page=std::get_if<cr::content_page>(&frame.body))for(const auto& item:page->items)content.append(item);
        if(const auto* page=std::get_if<cr::receipt_page>(&frame.body))for(const auto& item:page->items) {
            if(std::holds_alternative<cr::not_committed>(item.value))refuse("canonical READY cannot prove negative receipt");
            if(const auto* committed=std::get_if<cr::committed>(&item.value))
                if(committed->namespace_id!=ready_blob(row,"namespace_id") || committed->coverage_id!=ready_blob(row,"coverage_id"))
                    refuse("canonical READY receipt coverage differs");
            receipts.append(item);
            if(validate_receipt)validate_receipt(item);
        }
    }
    hash.finish();if(sequence.status!=cr::phase::sequence_complete_unverified || wire!=integer(row,"wire_bytes") ||
       picosha2::get_hash_hex_string(hash)!=ready_blob(row,"frames_sha") || content.finish()!=manifest.content_digest || receipts.finish()!=manifest.receipt_digest)
        refuse("canonical READY exact capsule or whole-stream hashes differ");
    return manifest;
}
std::vector<std::string> ready_key_parts(const std::string& key) {
    std::vector<std::string> parts;size_t at=0;
    for(int i=0;i<5;++i) {
        if(at>=key.size() || key[at]<'1' || key[at]>'9')refuse("canonical READY malformed binding length");
        size_t length=0;
        while(at<key.size() && key[at]>='0' && key[at]<='9') {
            length=length*10+(key[at++]-'0');if(length>256)refuse("canonical READY binding field too large");
        }
        if(at>=key.size() || key[at++]!=':' || length>key.size()-at)refuse("canonical READY truncated binding");
        parts.push_back(key.substr(at,length));at+=length;
    }
    if(at!=key.size() || canonical_writer_adapter::uuid_key(parts[2])!=parts[2] || canonical_writer_adapter::uuid_key(parts[3])!=parts[3])
        refuse("canonical READY binding is not canonical");
    return parts;
}
void dispose_ready(database& db,const canonical_ready_profile& p,const database::row_t& row) {
    const auto key=ready_blob(row,"binding");const auto seq=integer(row,"sequence");
    const auto frames=integer(db.query("SELECT COUNT(*) AS n FROM main._lattice_canonical_ready_frame WHERE binding=?",{bytes(key)}).at(0),"n");
    db.execute("DELETE FROM main._lattice_canonical_ready_frame WHERE binding=?",{bytes(key)});changed_retention(db,frames);
    if(integer(row,"state")==ready_preparing) {
        db.execute("DELETE FROM main._lattice_canonical_attempt WHERE id=?",{integer(row,"reservation")});changed_retention(db);
    }
    db.execute("DELETE FROM main._lattice_canonical_ready_transfer WHERE binding=? AND sequence=?",{bytes(key),seq});changed_retention(db);
    db.execute("UPDATE main._lattice_canonical_ready_profile SET active=active-1,charged=charged-? WHERE id=1 AND active>0 AND charged>=?",
        {p.transfer_bytes,p.transfer_bytes});changed_retention(db);
    if(!db.query("SELECT 1 FROM main._lattice_canonical_ready_frame WHERE binding=? LIMIT 1",{bytes(key)}).empty() ||
       !db.query("SELECT 1 FROM main._lattice_canonical_ready_transfer WHERE binding=? LIMIT 1",{bytes(key)}).empty())
        refuse("canonical READY disposal postimage differs");
}
}
void canonical_writer_adapter::verify_ready_retention(database& writer,const context& state,const retention_session& session) {
    if(!state.ready || !state.namespaces)refuse("canonical READY profile missing");const auto& p=*state.ready;
    const auto sync=writer.query("PRAGMA main.synchronous"),mode=writer.query("PRAGMA main.journal_mode");
    if(sync.size()!=1 || integer(sync[0],"synchronous")<2 || integer(sync[0],"synchronous")>3 ||
       mode.size()!=1 || string(mode[0],"journal_mode")!="wal")refuse("canonical READY requires actual WAL and FULL/EXTRA commit durability");
    session.verify_file(writer);retention_inventory_matches(writer,state.profile,state.namespace_profile(),&p);
    const auto profiles=writer.query(retention_profile_query);
    if(profiles.size()!=1 || integer(profiles[0],"id")!=1 || integer(profiles[0],"version")!=3 ||
       integer(profiles[0],"incarnation")!=session.incarnation || integer(profiles[0],"next_attempt")<0 ||
       integer(profiles[0],"max_attempts")!=session.limits.attempts || integer(profiles[0],"max_duration_ms")!=session.limits.duration_ms)
        refuse("canonical READY retention incarnation or policy differs");
    session.verify_binding(profiles[0]);const auto policy=ready_profile_row(writer,p);
    canonical_change_store store(*state.owner,state.binding,state.profile.limits,state.namespace_profile());store.audit();const auto bounds=store.state();
    const auto attempts=writer.query(retention_attempts_query);
    if(attempts.size()>static_cast<size_t>(session.limits.attempts))refuse("canonical READY ordinary reservation capacity exceeded");
    std::map<int64_t,database::row_t> reservations;
    for(const auto& row:attempts) {
        const auto id=integer(row,"id");
        if(id<=0 || id>integer(profiles[0],"next_attempt") || integer(row,"incarnation")!=session.incarnation ||
           integer(row,"base")<bounds.floor || integer(row,"base")>bounds.head || integer(row,"deadline_ms")<0 || !reservations.emplace(id,row).second)
            refuse("canonical READY malformed ordinary reservation");
    }
    const auto bindings=writer.query("SELECT "+ready_b("binding",ready_key_limit)+","+ready_i("sequence")+","+ready_i("charge")+
        " FROM main._lattice_canonical_ready_binding ORDER BY binding LIMIT 1025");
    if(bindings.size()!=static_cast<size_t>(integer(policy,"bindings")) || bindings.size()>static_cast<size_t>(p.bindings))
        refuse("canonical READY high-water inventory differs");
    std::map<std::string,int64_t> high_water;int64_t charge=0;
    for(const auto& row:bindings) {
        const auto key=ready_blob(row,"binding");(void)ready_key_parts(key);
        if(integer(row,"sequence")<=0 || integer(row,"charge")!=ready_add(64,key.size()) || !high_water.emplace(key,integer(row,"sequence")).second)
            refuse("canonical READY invalid durable high water");
        charge=ready_add(charge,integer(row,"charge"));
    }
    const auto transfers=writer.query(ready_transfer_query(p)+" ORDER BY binding LIMIT 65");
    if(transfers.size()!=static_cast<size_t>(integer(policy,"active")) || transfers.size()>static_cast<size_t>(p.transfers))
        refuse("canonical READY active inventory differs");
    int64_t expected_frames=0;std::set<int64_t> used_reservations;
    for(const auto& row:transfers) {
        const auto key=ready_blob(row,"binding");const auto request=ready_request(row,p);const auto& q=std::get<cr::request>(request.body);
        const auto ns=ready_blob(row,"namespace_id"),replica=ready_blob(row,"replica");
        const auto sequence=integer(row,"sequence"),kind=integer(row,"state"),pin=integer(row,"pin");
        if(key!=ready_binding(ns,replica,request.logical) || sequence<=0 || static_cast<uint64_t>(sequence)!=request.logical.sequence ||
           !high_water.count(key) || high_water.at(key)!=sequence || q.source!=ready_source(state.profile,p) ||
           ready_blob(row,"descriptor")!=state.source_descriptor_digest || pin<bounds.floor || pin>bounds.head ||
           integer(row,"lease_incarnation")<=0 || integer(row,"lease_incarnation")>session.incarnation ||
           integer(row,"lease_sequence")<=0 || integer(row,"lease_sequence")>integer(policy,"next_lease") || integer(row,"deadline_ms")<0 ||
           integer(row,"manifest_duration")<=0 || integer(row,"manifest_duration")>static_cast<int64_t>(p.package.codec.lease_ms) ||
           ready_blob(row,"manifest_lease").empty() || ready_metadata_charge(row)>p.transfer_bytes)
            refuse("canonical READY immutable transfer binding or bounds differ");
        bool coverage=false;for(const auto& entry:state.namespaces->entries)
            if(entry.namespace_id==ns && entry.coverage_id==ready_blob(row,"coverage_id") && entry.revision==integer(row,"coverage_revision"))coverage=true;
        if(!coverage)refuse("canonical READY namespace coverage changed; explicit disposal required");
        for(const auto& requested:q.receipts)if(requested.namespace_id!=std::optional<std::string>{ns})refuse("canonical READY request namespace differs");
        if(kind==ready_preparing) {
            const auto id=integer(row,"reservation");
            if(!reservations.count(id) || !used_reservations.insert(id).second || integer(row,"lease_incarnation")!=session.incarnation ||
               integer(reservations.at(id),"base")!=pin || integer(reservations.at(id),"deadline_ms")!=integer(row,"deadline_ms") ||
               integer(row,"head")!=0 || integer(row,"floor")!=0 || integer(row,"frame_count")!=0 || integer(row,"wire_bytes")!=0 ||
               !ready_blob(row,"manifest_sha").empty() || !ready_blob(row,"frames_sha").empty())
                refuse("canonical READY preparation lost exact real source reservation");
            if(q.base && *q.base!=static_cast<uint64_t>(pin))refuse("canonical READY delta preparation base differs");
        } else if(kind==ready_complete) {
            if(integer(row,"reservation")!=0 || integer(row,"head")!=pin || integer(row,"floor")<0 || integer(row,"floor")>pin)
                refuse("canonical READY tail pin or captured bounds differ");
            (void)audit_ready_frames(writer,p,row,[&](const cr::receipt_item& item){
                const auto* positive=std::get_if<cr::committed>(&item.value);if(!positive)return;
                const auto actual=store.receipt(item.original_id);
                if(!actual || actual->original.namespace_id!=std::optional<std::string>{ns} ||
                   actual->position!=static_cast<int64_t>(positive->position) ||
                   static_cast<int>(actual->original.outcome)!=static_cast<int>(positive->outcome)+1 ||
                   bool(actual->original.target)!=bool(positive->accepted_target))refuse("canonical READY positive receipt lost source evidence");
                if(actual->original.target && (actual->original.target->table!=positive->accepted_target->table ||
                    actual->original.target->global_id!=positive->accepted_target->id))refuse("canonical READY accepted receipt target differs");
            });
            expected_frames=ready_add(expected_frames,integer(row,"frame_count"));
        } else refuse("canonical READY unsupported transfer state");
        charge=ready_add(charge,p.transfer_bytes);
    }
    if(charge!=integer(policy,"charged") || charge>p.charged_bytes ||
       integer(writer.query("SELECT COUNT(*) AS n FROM main._lattice_canonical_ready_frame").at(0),"n")!=expected_frames)
        refuse("canonical READY charge or frame inventory mismatch");
}
void canonical_writer_adapter::enroll_ready(lattice_db& owner,bool reopen) {
    auto& writer=*writer_;auto& session=*retention_;auto& state=*context_;const auto& p=*state.ready;
    const bool existing=writer.table_exists("_lattice_canonical_retention");
    if(reopen!=existing || recovery_writer_access::active_writer(owner)!=&writer)refuse("canonical READY refuses implicit profile transition");
    if(!existing) {
        const auto schema=retention_schema(state.profile,state.namespace_profile(),&p);
        for(const auto& [key,sql]:schema)if(key.first=="table" &&
            (key.second=="_lattice_canonical_retention" || key.second=="_lattice_canonical_attempt" || key.second.starts_with("_lattice_canonical_ready_")))writer.execute(sql);
        writer.execute(schema.at({"index","_lattice_canonical_attempt_deadline"}));
        writer.execute("INSERT INTO main._lattice_canonical_retention VALUES(1,3,0,0,?,?,?,?,?,?,?,?)",{session.limits.attempts,session.limits.duration_ms,
            session.main_device,session.main_inode,session.parent_device,session.parent_inode,session.custody_device,session.custody_inode});changed_retention(writer);
        writer.execute("INSERT INTO main._lattice_canonical_ready_profile VALUES(1,?,0,0,0,0)",{bytes(ready_policy(p))});changed_retention(writer);
        for(const auto& [key,sql]:schema)if(key.first=="trigger")writer.execute(sql);
    }
    const auto rows=writer.query(retention_profile_query);
    if(rows.size()!=1 || integer(rows[0],"version")!=3 || integer(rows[0],"incarnation")<0 ||
       integer(rows[0],"incarnation")==std::numeric_limits<int64_t>::max())refuse("canonical READY unsupported or exhausted incarnation");
    const auto prior=integer(rows[0],"incarnation");session.incarnation=prior;
    // Audit ALL persisted frames, charges, bindings and pins before any restart
    // cleanup. A malformed capsule cannot be repaired by discarding evidence.
    verify_ready_retention(writer,state,session);
    retention_frame authority(state,retention_bootstrap);
    const auto unfinished=writer.query(ready_transfer_query(p)+" WHERE state=1 ORDER BY binding LIMIT 65");
    for(const auto& row:unfinished)dispose_ready(writer,p,row);
    const auto remaining=integer(writer.query("SELECT COUNT(*) AS n FROM main._lattice_canonical_attempt").at(0),"n");
    writer.execute("DELETE FROM main._lattice_canonical_attempt");changed_retention(writer,remaining);
    writer.execute("UPDATE main._lattice_canonical_retention SET incarnation=incarnation+1,next_attempt=0 WHERE id=1 AND incarnation=?",{prior});changed_retention(writer);
    session.incarnation=prior+1;
    // READY rows and H pins remain. Their old lease incarnation is deliberately
    // orphaned; no prior-process monotonic deadline is interpreted or renewed.
    verify_ready_retention(writer,state,session);
}
void canonical_writer_adapter::expire_ready_rows(database& writer,const context& state,retention_session& session) {
    const auto rows=writer.query(ready_transfer_query(*state.ready)+" WHERE lease_incarnation=? AND deadline_ms<=? ORDER BY binding LIMIT 65",
        {session.incarnation,session.now()});
    for(const auto& row:rows)dispose_ready(writer,*state.ready,row);
}
canonical_ready_info canonical_writer_adapter::ready_info(database& writer,const context& state,const retention_session& session,const std::string& key) {
    const auto row=ready_row(writer,*state.ready,key);const auto request=ready_request(row,*state.ready);
    const auto& q=std::get<cr::request>(request.body);const bool ready=integer(row,"state")==ready_complete;
    canonical_ready_info info{canonical_ready_identity(key,integer(row,"sequence"),q.request_digest),ready_blob(row,"namespace_id"),ready_blob(row,"replica"),
        request.logical,ready,integer(row,"lease_incarnation")!=session.incarnation,integer(row,"pin"),
        static_cast<uint64_t>(integer(row,"frame_count")),static_cast<uint64_t>(integer(row,"wire_bytes")),{}};
    if(ready)info.manifest=std::get<cr::manifest>(cr::decode(ready_frame(writer,*state.ready,key,0),state.ready->package.codec).body);
    return info;
}
std::unique_ptr<canonical_writer_adapter> canonical_writer_adapter::attach_ready_for_qualification(
    std::shared_ptr<lattice_db> owner,const canonical_namespaced_writer_profile& p,
    canonical_upstream_limits upstream,canonical_retention_limits retention,const canonical_ready_profile& ready) {
    if(!owner || !p.writer.upstream_requested || !upstream.entries || upstream.field_bytes<256 || !upstream.delivery_bytes ||
       upstream.field_bytes>upstream.delivery_bytes || upstream.delivery_bytes>static_cast<size_t>(std::numeric_limits<int>::max()/8))
        refuse("canonical READY qualification requires bounded retained namespaced upstream owner");
    p.namespaces.validate();validate_ready_profile(ready,p.writer,retention);
    return std::unique_ptr<canonical_writer_adapter>(new canonical_writer_adapter(*owner,p.writer,&upstream,&retention,&p.namespaces,&ready));
}
namespace {
void ready_duration(const canonical_ready_profile& p,int64_t duration,uint64_t route) {
    if(duration<=0 || duration>static_cast<int64_t>(p.package.codec.lease_ms) || !route || route>static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        refuse("canonical READY finite lease or route spelling outside bounds");
}
std::vector<sync_recovery::canonical_capture_request> ready_capture_requests(const cr::request& q,const std::string& ns,
    const canonical_ready_profile& p) {
    if(q.receipts.size()>p.capture.requests)refuse("canonical READY capture request count exceeds policy");
    std::vector<sync_recovery::canonical_capture_request> out;uint64_t targets=0;
    for(const auto& requested:q.receipts) {
        if(requested.namespace_id!=std::optional<std::string>{ns} || canonical_writer_adapter::uuid_key(requested.original_id)!=requested.original_id ||
           requested.targets.size()>p.capture.requested_targets-targets)refuse("canonical READY request provenance or targets differ");
        targets+=requested.targets.size();sync_recovery::canonical_capture_request item{requested.original_id,{},ns};
        for(const auto& target:requested.targets) {
            if(canonical_writer_adapter::uuid_key(target.id)!=target.id)refuse("canonical READY target spelling is not normalized");
            item.targets.push_back({target.table,target.id});
        }
        out.push_back(std::move(item));
    }
    return out;
}
void ready_require_identity(const database::row_t& row,const canonical_ready_profile& p,const cr::attempt& attempt,
    const std::string& request_bytes,const canonical_namespace_entry& admission,const std::string& replica) {
    const auto frozen=ready_request(row,p);
    if(frozen.logical!=attempt || ready_blob(row,"request")!=request_bytes || ready_blob(row,"namespace_id")!=admission.namespace_id ||
       ready_blob(row,"coverage_id")!=admission.coverage_id || integer(row,"coverage_revision")!=admission.revision || ready_blob(row,"replica")!=replica)
        refuse("canonical READY logical request or coverage changed; abandon and use a new attempt");
}
}
canonical_ready_result canonical_writer_adapter::prepare_ready_owned(std::shared_ptr<lattice_db> owner,const canonical_namespace_admission& admission,
    const cr::attempt& attempt,const cr::request& request,int64_t duration,uint64_t route) {
    return prepare_ready_impl(std::move(owner),admission,attempt,request,duration,route,{},{});
}
canonical_ready_result canonical_writer_adapter::prepare_ready_impl(std::shared_ptr<lattice_db> owner,const canonical_namespace_admission& admission,
    const cr::attempt& attempt,const cr::request& request,int64_t duration,uint64_t route,
    const std::function<void()>& after_preparation,const std::function<void(size_t,uint64_t)>& after_batch) {
    // Retain every physical component before any callback. Subsequent stages
    // call the same static retained-capture implementation without this.
    auto state=context_;auto writer=writer_;auto session=retention_;canonical_ready_result result;
    try {
        const auto held_admission=admission;
        if(!state || !state->ready || !session)refuse("canonical READY v3 profile unavailable");
        const auto& p=*state->ready;validate_namespace_admission(owner,writer,state,held_admission);ready_duration(p,duration,route);
        if(request.source!=ready_source(state->profile,p))refuse("canonical READY request source differs from actual profile");
        const auto raw=cr::encode({attempt,ready_route,request},p.package.codec);
        const auto frozen=cr::decode(raw,p.package.codec);const auto& logical=frozen.logical;const auto& q=std::get<cr::request>(frozen.body);
        const auto ns=held_admission.namespace_.namespace_id,replica=held_admission.replica_;
        const auto key=ready_binding(ns,replica,logical),request_sha=picosha2::hash256_hex_string(raw);
        const auto requests=ready_capture_requests(q,ns,p);
        std::optional<canonical_retention_ticket> ticket;std::optional<canonical_ready_info> prepared;
        int64_t lease_sequence=0,deadline=0;std::string manifest_lease;
        bool needs_full=false;
        result.preparation=retention_owned(owner,writer,state,session,ready_prepare,[&](database& actual,retention_session& clock){
            validate_namespace_admission(owner,writer,state,held_admission);
            canonical_change_store store(*owner,state->binding,state->profile.limits,state->namespace_profile());store.audit();const auto bounds=store.state();
            if(!actual.query("SELECT 1 FROM main._lattice_canonical_ready_transfer WHERE binding=? LIMIT 1",{bytes(key)}).empty())
                refuse("canonical READY active logical binding requires resume or explicit abandonment");
            if(q.base && *q.base<static_cast<uint64_t>(bounds.floor)){needs_full=true;return;}
            const auto pin=q.base?static_cast<int64_t>(*q.base):bounds.head;store.require_base(pin);
            const auto policy=ready_profile_row(actual,p);
            const auto previous=actual.query("SELECT "+ready_i("sequence")+" FROM main._lattice_canonical_ready_binding WHERE binding=? LIMIT 2",{bytes(key)});
            const bool fresh=previous.empty();
            if(previous.size()>1 || (!fresh && static_cast<uint64_t>(integer(previous[0],"sequence"))>=logical.sequence))
                refuse("canonical READY logical attempt is stale or terminal");
            const auto added=ready_add(p.transfer_bytes,fresh?ready_add(64,key.size()):0);
            if(integer(policy,"active")>=p.transfers || (fresh && integer(policy,"bindings")>=p.bindings) ||
               added>p.charged_bytes-integer(policy,"charged") || integer(policy,"next_lease")==std::numeric_limits<int64_t>::max())
                refuse("canonical READY bounded spool or binding capacity exhausted");
            const auto registry=actual.query(retention_profile_query);const auto prior=integer(registry.at(0),"next_attempt");
            if(prior==std::numeric_limits<int64_t>::max() ||
               integer(actual.query("SELECT COUNT(*) AS n FROM main._lattice_canonical_attempt").at(0),"n")>=clock.limits.attempts)
                refuse("canonical READY real reservation capacity exhausted");
            const auto id=prior+1;lease_sequence=integer(policy,"next_lease")+1;deadline=ready_add(clock.now(),duration);
            manifest_lease="ready-v3-"+std::to_string(clock.incarnation)+"-"+std::to_string(lease_sequence);
            if(fresh)actual.execute("INSERT INTO main._lattice_canonical_ready_binding VALUES(?,?,?)",{bytes(key),static_cast<int64_t>(logical.sequence),ready_add(64,key.size())});
            else actual.execute("UPDATE main._lattice_canonical_ready_binding SET sequence=? WHERE binding=? AND sequence=?",
                {static_cast<int64_t>(logical.sequence),bytes(key),integer(previous[0],"sequence")});
            changed_retention(actual);
            actual.execute("INSERT INTO main._lattice_canonical_attempt VALUES(?,?,?,?)",{id,clock.incarnation,pin,deadline});changed_retention(actual);
            actual.execute("UPDATE main._lattice_canonical_retention SET next_attempt=? WHERE id=1 AND next_attempt=?",{id,prior});changed_retention(actual);
            actual.execute("UPDATE main._lattice_canonical_ready_profile SET active=active+1,charged=charged+?,bindings=bindings+?,next_lease=? WHERE id=1 AND next_lease=?",
                {added,int64_t(fresh),lease_sequence,integer(policy,"next_lease")});changed_retention(actual);
            actual.execute("INSERT INTO main._lattice_canonical_ready_transfer VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                {bytes(key),static_cast<int64_t>(logical.sequence),bytes(raw),bytes(request_sha),bytes(ns),bytes(held_admission.namespace_.coverage_id),
                 held_admission.namespace_.revision,bytes(replica),bytes(state->source_descriptor_digest),ready_preparing,id,pin,int64_t(0),int64_t(0),int64_t(0),int64_t(0),
                 blob{},blob{},bytes(manifest_lease),duration,clock.incarnation,lease_sequence,deadline});changed_retention(actual);
            ticket=canonical_retention_ticket(session,clock.incarnation,id,pin,deadline,q.base?std::optional<int64_t>(static_cast<int64_t>(*q.base)):std::nullopt);
            prepared=ready_info(actual,*state,clock,key);validate_namespace_admission(owner,writer,state,held_admission);
        });
        if(result.preparation.state!=recovery_install_state::committed)return result;
        result.requires_full_request=needs_full;if(needs_full)return result;
        result.transfer=prepared;
        if(!ticket || !prepared)refuse("canonical READY committed preparation lacks exact identity");
        try {
            if(after_preparation)after_preparation();
            const auto captured=capture_reserved_session(owner,writer,state,session,*ticket,requests,p.capture,after_batch,&held_admission);
            if(!captured.capture || captured.descriptor_digest!=state->source_descriptor_digest || captured.binding!=state->binding || captured.head<ticket->base_)
                refuse("canonical READY capture lacks exact retained source proof");
            std::vector<cr::content_item> content;content.reserve(captured.capture->rows.size());
            for(const auto& row:captured.capture->rows)content.push_back({{row.key.table,row.key.global_id},
                row.payload?std::variant<cr::present,cr::tombstone>(cr::present{*row.payload}):std::variant<cr::present,cr::tombstone>(cr::tombstone{})});
            std::vector<cr::receipt_item> receipts;receipts.reserve(captured.capture->receipts.size());
            for(const auto& receipt:captured.capture->receipts) {
                if(!receipt.stored){receipts.push_back({receipt.original_id,cr::unknown{cr::unknown_reason::unproved_provenance}});continue;}
                const auto& stored=*receipt.stored;
                if(stored.original.namespace_id!=std::optional<std::string>{ns})refuse("canonical READY capture receipt namespace differs");
                cr::committed accepted{ns,held_admission.namespace_.coverage_id,static_cast<cr::decision>(static_cast<int>(stored.original.outcome)-1),
                    static_cast<uint64_t>(stored.position),{}};
                if(stored.original.target)accepted.accepted_target=cr::identity{stored.original.target->table,stored.original.target->global_id};
                receipts.push_back({receipt.original_id,std::move(accepted)});
            }
            const auto package=cr::assemble_package(logical,ready_route,q,static_cast<uint64_t>(captured.head),
                {manifest_lease,static_cast<uint64_t>(duration)},content,receipts,p.package);
            std::optional<canonical_ready_info> published;
            result.publication=retention_owned(owner,writer,state,session,ready_publish,[&](database& actual,retention_session& clock){
                validate_namespace_admission(owner,writer,state,held_admission);
                if(clock.now()>=deadline || clock.incarnation!=ticket->incarnation_)refuse("canonical READY preparation lease expired or retired before publication");
                const auto row=ready_row(actual,p,key);ready_require_identity(row,p,logical,raw,held_admission.namespace_,replica);
                if(integer(row,"state")!=ready_preparing || integer(row,"reservation")!=ticket->id_ || integer(row,"pin")!=ticket->base_ ||
                   integer(row,"lease_sequence")!=lease_sequence || integer(row,"lease_incarnation")!=clock.incarnation || integer(row,"deadline_ms")!=deadline)
                    refuse("canonical READY publication lost exact preparation reservation");
                // Shared verification proved the real ordinary registry row
                // before this body. Its pin remains until the atomic H handoff.
                canonical_change_store store(*owner,state->binding,state->profile.limits,state->namespace_profile());const auto current=store.state();
                if(current.floor>ticket->base_ || captured.head>current.head || captured.floor>ticket->base_)
                    refuse("canonical READY source pin continuity lost");
                int64_t charge=ready_metadata_charge(row);picosha2::hash256_one_by_one hash;int64_t index=0;
                for(const auto& frame:package.frames()) {
                    charge=ready_add(charge,ready_add(frame.size(),128));if(charge>p.transfer_bytes)refuse("canonical READY capsule exceeds reserved charge");
                    ready_hash_frame(hash,frame);actual.execute("INSERT INTO main._lattice_canonical_ready_frame VALUES(?,?,?,?)",
                        {bytes(key),index++,bytes(frame),bytes(picosha2::hash256_hex_string(frame))});changed_retention(actual);
                }
                hash.finish();
                actual.execute("UPDATE main._lattice_canonical_ready_transfer SET state=2,reservation=0,pin=?,head=?,floor=?,frame_count=?,wire_bytes=?,frames_sha=?,manifest_sha=? WHERE binding=? AND state=1 AND reservation=?",
                    {captured.head,captured.head,captured.floor,index,static_cast<int64_t>(package.retained_wire_bytes()),bytes(picosha2::get_hash_hex_string(hash)),
                     bytes(package.offer().manifest_digest),bytes(key),ticket->id_});changed_retention(actual);
                actual.execute("DELETE FROM main._lattice_canonical_attempt WHERE id=? AND incarnation=? AND base=? AND deadline_ms=?",
                    {ticket->id_,clock.incarnation,ticket->base_,deadline});changed_retention(actual);
                if(!actual.query("SELECT 1 FROM main._lattice_canonical_attempt WHERE id=? LIMIT 1",{ticket->id_}).empty())refuse("canonical READY pin handoff postimage differs");
                published=ready_info(actual,*state,clock,key);validate_namespace_admission(owner,writer,state,held_admission);
                if(clock.now()>=deadline)refuse("canonical READY preparation expired during publication");
            });
            if(result.publication.state==recovery_install_state::committed) {
                result.transfer=std::move(published);
                if(result.transfer)result.lease=canonical_ready_lease(session,state,result.transfer->identity,ticket->incarnation_,lease_sequence,deadline,route);
            }
        } catch(...) {result.capture_error=std::current_exception();}
    } catch(...) {result.preparation.primary_error=std::current_exception();}
    return result;
}
canonical_ready_inspection canonical_writer_adapter::inspect_ready_owned(std::shared_ptr<lattice_db> owner) {
    auto state=context_;auto writer=writer_;auto session=retention_;canonical_ready_inspection result;
    std::vector<canonical_ready_info> found;
    result.settlement=retention_owned(std::move(owner),writer,state,session,ready_read,[&](database& actual,retention_session& clock){
        if(!state->ready)refuse("canonical READY v3 profile unavailable");
        const auto rows=actual.query("SELECT "+ready_b("binding",ready_key_limit)+" FROM main._lattice_canonical_ready_transfer ORDER BY binding LIMIT 65");
        for(const auto& row:rows)found.push_back(ready_info(actual,*state,clock,ready_blob(row,"binding")));
    });
    if(result.settlement.state==recovery_install_state::committed)result.transfers=std::move(found);return result;
}
canonical_ready_resume_result canonical_writer_adapter::resume_ready_owned(std::shared_ptr<lattice_db> owner,const canonical_namespace_admission& admission,
    const cr::attempt& attempt,const cr::request& request,int64_t duration,uint64_t route) {
    auto state=context_;auto writer=writer_;auto session=retention_;canonical_ready_resume_result result;
    try {
        const auto held=admission;if(!state || !state->ready || !session)refuse("canonical READY v3 profile unavailable");
        const auto& p=*state->ready;validate_namespace_admission(owner,writer,state,held);ready_duration(p,duration,route);
        if(request.source!=ready_source(state->profile,p))refuse("canonical READY resume source differs");
        const auto raw=cr::encode({attempt,ready_route,request},p.package.codec),key=ready_binding(held.namespace_.namespace_id,held.replica_,attempt);
        (void)ready_capture_requests(request,held.namespace_.namespace_id,p);
        std::optional<canonical_ready_info> resumed;int64_t sequence=0,deadline=0;
        result.settlement=retention_owned(owner,writer,state,session,ready_resume,[&](database& actual,retention_session& clock){
            validate_namespace_admission(owner,writer,state,held);const auto row=ready_row(actual,p,key);
            ready_require_identity(row,p,attempt,raw,held.namespace_,held.replica_);
            if(integer(row,"state")!=ready_complete)refuse("canonical READY incomplete preparation cannot resume");
            // Same-process expiry cannot silently renew. A source restart has
            // explicitly orphaned the old physical lease; fresh admission may
            // then request a new finite lease over the continuously retained H.
            if(integer(row,"lease_incarnation")==clock.incarnation && clock.now()>=integer(row,"deadline_ms"))
                refuse("canonical READY current lease expired; explicit disposal required");
            const auto policy=ready_profile_row(actual,p);
            if(integer(policy,"next_lease")==std::numeric_limits<int64_t>::max())refuse("canonical READY physical lease sequence exhausted");
            sequence=integer(policy,"next_lease")+1;deadline=ready_add(clock.now(),duration);
            actual.execute("UPDATE main._lattice_canonical_ready_profile SET next_lease=? WHERE id=1 AND next_lease=?",{sequence,integer(policy,"next_lease")});changed_retention(actual);
            actual.execute("UPDATE main._lattice_canonical_ready_transfer SET lease_incarnation=?,lease_sequence=?,deadline_ms=? WHERE binding=? AND sequence=? AND state=2",
                {clock.incarnation,sequence,deadline,bytes(key),static_cast<int64_t>(attempt.sequence)});changed_retention(actual);
            const auto postimage=ready_row(actual,p,key);
            if(integer(postimage,"lease_incarnation")!=clock.incarnation || integer(postimage,"lease_sequence")!=sequence || integer(postimage,"deadline_ms")!=deadline)
                refuse("canonical READY physical lease write ignored or postimage differs");
            resumed=ready_info(actual,*state,clock,key);validate_namespace_admission(owner,writer,state,held);
        });
        if(result.settlement.state==recovery_install_state::committed) {
            result.transfer=std::move(resumed);
            if(result.transfer)result.lease=canonical_ready_lease(session,state,result.transfer->identity,session->incarnation,sequence,deadline,route);
        }
    } catch(...) {result.settlement.primary_error=std::current_exception();}
    return result;
}
canonical_ready_frame_result canonical_writer_adapter::read_ready_frame_owned(std::shared_ptr<lattice_db> owner,const canonical_namespace_admission& admission,
    const canonical_ready_lease& lease,uint64_t index) {
    auto state=context_;auto writer=writer_;auto session=retention_;canonical_ready_frame_result result;
    std::optional<std::string> frame;
    try {
        const auto held=admission;
        if(!state || !state->ready || !session || lease.session_.lock()!=session || lease.context_.lock()!=state || lease.incarnation_!=session->incarnation)
            refuse("canonical READY old or foreign physical lease");
        validate_namespace_admission(owner,writer,state,held);
        result.settlement=retention_owned(owner,writer,state,session,ready_read,[&](database& actual,retention_session& clock){
            validate_namespace_admission(owner,writer,state,held);const auto& p=*state->ready;
            const auto row=ready_row(actual,p,lease.identity_.binding_);const auto frozen=ready_request(row,p);
            ready_require_identity(row,p,frozen.logical,ready_blob(row,"request"),held.namespace_,held.replica_);
            if(integer(row,"state")!=ready_complete || integer(row,"sequence")!=lease.identity_.sequence_ ||
               std::get<cr::request>(frozen.body).request_digest!=lease.identity_.request_digest_ ||
               integer(row,"lease_incarnation")!=lease.incarnation_ || integer(row,"lease_sequence")!=lease.sequence_ ||
               integer(row,"deadline_ms")!=lease.deadline_ || clock.now()>=lease.deadline_ || index>=static_cast<uint64_t>(integer(row,"frame_count")))
                refuse("canonical READY frame lease expired, superseded or outside capsule");
            auto decoded=cr::decode(ready_frame(actual,p,lease.identity_.binding_,static_cast<int64_t>(index)),p.package.codec);
            decoded.route_generation=lease.route_;frame=cr::encode(decoded,p.package.codec);
            if(frame->size()>std::get<cr::request>(frozen.body).budget.frame_bytes)
                refuse("canonical READY route envelope exceeds frozen request frame budget");
            // This byte return is still not a transport handoff. The future
            // controller must revalidate its authenticated route at send time.
            validate_namespace_admission(owner,writer,state,held);if(clock.now()>=lease.deadline_)refuse("canonical READY frame lease expired while reading");
        });
        if(result.settlement.state==recovery_install_state::committed)result.frame=std::move(frame);
    } catch(...) {result.settlement.primary_error=std::current_exception();}
    return result;
}
recovery_install_result canonical_writer_adapter::abandon_ready_owned(std::shared_ptr<lattice_db> owner,const canonical_ready_identity& identity) {
    auto state=context_;const auto held=identity;
    return retention_owned(std::move(owner),writer_,state,retention_,ready_abandon,[&](database& actual,retention_session&){
        if(!state->ready)refuse("canonical READY v3 profile unavailable");const auto row=ready_row(actual,*state->ready,held.binding_);
        if(integer(row,"sequence")!=held.sequence_ || std::get<cr::request>(ready_request(row,*state->ready).body).request_digest!=held.request_digest_)
            refuse("canonical READY source disposal identity differs");
        dispose_ready(actual,*state->ready,row);
    });
}
recovery_install_result canonical_writer_adapter::expire_ready_owned(std::shared_ptr<lattice_db> owner) {
    auto state=context_;
    return retention_owned(std::move(owner),writer_,state,retention_,retention_expire,[&](database& actual,retention_session& clock){
        if(!state->ready)refuse("canonical READY v3 profile unavailable");expire_ready_rows(actual,*state,clock);expire_retention(actual,clock.now());
    });
}
