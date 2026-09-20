#include "canonical_range_staging.hpp"
#include "recovery_writer_access.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <limits>
#include <type_traits>

namespace lattice::detail {
namespace {
namespace cr=canonical_range;
using code=canonical_staging_code;
using blob=std::vector<uint8_t>;
using row=database::row_t;
constexpr int64_t max_int=std::numeric_limits<int64_t>::max();
[[noreturn]] void fail(code c,const char* why){throw canonical_staging_error(c,why);}
blob bytes(const std::string& s){return {s.begin(),s.end()};}
int64_t integer(const row& r,const char* k){auto i=r.find(k);if(i==r.end()||!std::holds_alternative<int64_t>(i->second))fail(code::corrupt_state,"canonical staging expected integer");return std::get<int64_t>(i->second);}
std::string text(const row& r,const char* k){auto i=r.find(k);if(i==r.end()||!std::holds_alternative<blob>(i->second))fail(code::corrupt_state,"canonical staging expected bytes");const auto& b=std::get<blob>(i->second);return {b.begin(),b.end()};}
int64_t count(uint64_t n){if(n>static_cast<uint64_t>(max_int))fail(code::invalid_argument,"canonical staging unrepresentable count");return static_cast<int64_t>(n);}
int64_t size(const std::string& s){return count(s.size());}
bool fits(int64_t n,int64_t extra,int64_t cap){return n>=0&&extra>=0&&n<=cap&&extra<=cap-n;}
int64_t add(int64_t a,int64_t b){if(!fits(a,b,max_int))fail(code::capacity,"canonical staging byte/count overflow");return a+b;}
// SQLite INTEGER affinity does not constrain stored types. Never let the
// generic row decoder copy an arbitrary TEXT/BLOB from a numeric slot before
// integer() can refuse it; these projections return SQL NULL for bad types.
std::string integer_columns(std::initializer_list<const char*> keys){
    std::string result;for(const auto* key:keys){if(!result.empty())result+=",";result+="CASE WHEN typeof(";result+=key;result+=")='integer' THEN ";result+=key;result+=" END AS ";result+=key;}return result;
}
void changed(database& d){if(d.changes()!=1)fail(code::corrupt_state,"canonical staging expected write was ignored");}
template<class F> auto atomic(database& db,F&& f){
    db.execute("SAVEPOINT lattice_canonical_staging");
    try{auto r=f();db.execute("RELEASE lattice_canonical_staging");return r;}
    catch(...){auto primary=std::current_exception();if(!db.is_in_transaction())std::rethrow_exception(primary);
        try{db.execute("ROLLBACK TO lattice_canonical_staging");db.execute("RELEASE lattice_canonical_staging");}
        catch(...){throw canonical_staging_error(code::cleanup_failed,"canonical staging cleanup failed; abort owning transaction",primary,std::current_exception());}
        std::rethrow_exception(primary);}
}
std::vector<int64_t> fields(const canonical_staging_usage& u){return {u.channels,u.content_pages,u.identities,u.content_bytes,u.receipt_pages,u.receipts,u.receipt_bytes,u.stored_bytes};}
canonical_staging_usage read_usage(database& db){
    static const auto sql="SELECT "+integer_columns({"channels","content_pages","identities","content_bytes","receipt_pages","receipts","receipt_bytes","stored_bytes"})+" FROM main._lattice_range_store WHERE id=1";
    auto r=db.query(sql);
    if(r.size()!=1)fail(code::corrupt_state,"canonical staging usage missing");const auto& x=r[0];
    return {integer(x,"channels"),integer(x,"content_pages"),integer(x,"identities"),integer(x,"content_bytes"),integer(x,"receipt_pages"),integer(x,"receipts"),integer(x,"receipt_bytes"),integer(x,"stored_bytes")};
}
void write_usage(database& db,const canonical_staging_usage& old,const canonical_staging_usage& next){
    std::vector<column_value_t> args;for(auto n:fields(next))args.emplace_back(n);for(auto n:fields(old))args.emplace_back(n);
    db.execute("UPDATE main._lattice_range_store SET channels=?,content_pages=?,identities=?,content_bytes=?,receipt_pages=?,receipts=?,receipt_bytes=?,stored_bytes=? WHERE id=1 AND channels=? AND content_pages=? AND identities=? AND content_bytes=? AND receipt_pages=? AND receipts=? AND receipt_bytes=? AND stored_bytes=?",args);
    changed(db);if(read_usage(db)!=next)fail(code::corrupt_state,"canonical staging usage write was changed");
}
receive_install_binding binding(const cr::sequence_state& s){const auto& b=s.offer.source;return {s.logical.channel,b.authority,b.source_id,b.epoch,b.scope_digest,b.schema_digest};}
receive_install_identity identity(const cr::sequence_state& s){
    const auto& e=s.frozen_request.expected;const auto& m=s.offer;
    if(e.binding!=std::optional<cr::source_binding>{m.source})fail(code::stale_attempt,"canonical staging requires exact pre-bound expected source");
    receive_install_frontier f{e.base.kind==cr::frontier_kind::position?receive_frontier_kind::position:
        (e.base.kind==cr::frontier_kind::beginning_null?receive_frontier_kind::beginning_null:receive_frontier_kind::uninitialized),std::nullopt};
    if(e.base.value)f.position=count(*e.base.value);
    return {count(s.logical.sequence),count(e.revision),f,count(m.head),m.selection==cr::mode::full?receive_install_mode::full:receive_install_mode::delta,m.request_digest,m.receipt_digest,m.content_digest,m.manifest_digest};
}
canonical_staging_snapshot snapshot(cr::sequence_state s,uint64_t route,bool verified){auto b=binding(s);auto i=identity(s);return {std::move(s),route,verified,std::move(b),std::move(i)};}
int64_t stream(cr::stream_kind k){if(k==cr::stream_kind::content)return 0;if(k==cr::stream_kind::receipts)return 1;fail(code::invalid_argument,"invalid canonical page stream");}
std::pair<int64_t,uint64_t> page_key(const cr::frame& f){if(const auto* p=std::get_if<cr::content_page>(&f.body))return {0,p->index};if(const auto* p=std::get_if<cr::receipt_page>(&f.body))return {1,p->index};fail(code::invalid_argument,"canonical append requires a page");}
std::string manifest_id(const cr::frame& f){return std::visit([](const auto& p)->std::string{using T=std::decay_t<decltype(p)>;if constexpr(std::is_same_v<T,cr::content_page>||std::is_same_v<T,cr::receipt_page>||std::is_same_v<T,cr::end>)return p.manifest_digest;else fail(code::invalid_argument,"canonical page/end required");},f.body);}
cr::limits narrow(cr::limits b,const cr::wire_limits& w){b.maximum=w;b.string_bytes=std::min<uint64_t>(b.string_bytes,w.frame_bytes);return b;}
std::string logical_wire(cr::frame f,const cr::limits& b){(void)cr::encode(f,b);f.route_generation=1;return cr::encode(f,b);}
cr::sequence_state stored_state(const std::string& wire,const cr::attempt& a,const cr::limits& b){try{return cr::decode_state(wire,a,b);}catch(const cr::protocol_error&){fail(code::corrupt_state,"invalid canonical stored progress");}}
cr::frame stored_page(database& db,const std::string& channel,int64_t kind,uint64_t index,const cr::limits& b){
    auto shape=db.query("SELECT typeof(wire)='blob' AS encoded,length(wire) AS bytes FROM main._lattice_range_page WHERE channel=? AND stream=? AND page_index=?",{bytes(channel),kind,count(index)});
    if(shape.size()!=1||integer(shape[0],"encoded")!=1||integer(shape[0],"bytes")<=0||integer(shape[0],"bytes")>count(b.maximum.frame_bytes))fail(code::corrupt_state,"canonical page missing or oversized");
    auto wire=text(db.query("SELECT wire FROM main._lattice_range_page WHERE channel=? AND stream=? AND page_index=?",{bytes(channel),kind,count(index)}).at(0),"wire");
    try{auto f=cr::decode(wire,b);if((!std::holds_alternative<cr::content_page>(f.body)&&!std::holds_alternative<cr::receipt_page>(f.body))||f.route_generation!=1||page_key(f)!=std::pair<int64_t,uint64_t>{kind,index}||cr::encode(f,b)!=wire)fail(code::corrupt_state,"canonical stored page encoding/index differs");return f;}
    catch(const cr::protocol_error&){fail(code::corrupt_state,"canonical stored page fails framing/hash");}
}
}
canonical_range_staging::canonical_range_staging(std::shared_ptr<lattice_db> owner,receive_install_limits il,cr::limits codec,canonical_staging_limits limits)
    : owner_(std::move(owner)),installation_(owner_,il),install_limits_(il),codec_(codec),limits_(limits) {
    if(!owner_)fail(code::invalid_argument,"canonical staging needs retained owner");
    for(auto n:{limits.channels,limits.content_pages,limits.identities,limits.content_bytes,limits.receipt_pages,limits.receipts,limits.receipt_bytes,limits.stored_bytes})if(n<0)fail(code::invalid_argument,"negative canonical staging limit");
    const auto& b=codec.maximum;const auto& v=codec.values;
    if(!b.frame_bytes||b.frame_bytes>16*1024*1024||!b.payload_bytes||b.payload_bytes>b.frame_bytes||!b.items_per_page||b.items_per_page>4096||
        !codec.depth||codec.depth>64||!codec.nodes||codec.nodes>65536||codec.string_bytes<64||codec.string_bytes>b.frame_bytes||
        codec.request_entries>4096||codec.request_targets>4096||codec.request_target_bytes>16*1024*1024||!codec.restart_bytes||codec.restart_bytes>16*1024*1024||!codec.lease_ms||codec.lease_ms>static_cast<uint64_t>(max_int)||
        !v.raw_bytes||v.raw_bytes>16*1024*1024||!v.fields||v.fields>4096||!v.name_bytes||v.name_bytes>256||v.value_bytes>v.raw_bytes||v.decoded_bytes>v.raw_bytes)
        fail(code::invalid_argument,"invalid canonical staging codec limits");
    for(auto n:{b.content_pages,b.content_identities,b.content_bytes,b.receipt_pages,b.receipts,b.receipt_bytes})count(n);
}
database& canonical_range_staging::connection() const {auto* db=recovery_writer_access::active_writer(*owner_);if(!db)fail(code::transaction_required,"canonical staging requires owned main WRITE frame");return *db;}
std::string canonical_range_staging::configuration() const {
    const auto& b=codec_.maximum;const auto& v=codec_.values;
    return nlohmann::json{{"version",1},{"wire",{b.frame_bytes,b.payload_bytes,b.items_per_page,b.content_pages,b.content_identities,b.content_bytes,b.receipt_pages,b.receipts,b.receipt_bytes}},
        {"parse",{codec_.depth,codec_.nodes,codec_.string_bytes,codec_.request_entries,codec_.request_targets,codec_.request_target_bytes,codec_.restart_bytes,codec_.lease_ms}},
        {"values",{v.raw_bytes,v.fields,v.name_bytes,v.value_bytes,v.decoded_bytes}},
        {"install",{install_limits_.channels,install_limits_.field_bytes,install_limits_.encoded_bytes}},
        {"staging",{limits_.channels,limits_.content_pages,limits_.identities,limits_.content_bytes,limits_.receipt_pages,limits_.receipts,limits_.receipt_bytes,limits_.stored_bytes}}}.dump();
}
void canonical_range_staging::check_schema() const {
    auto& db=connection();auto tables=db.query("SELECT wr FROM pragma_table_list WHERE schema='main' AND name IN ('_lattice_range_store','_lattice_range_attempt','_lattice_range_page')");
    if(tables.size()!=3)fail(code::corrupt_state,"canonical staging schema incomplete");for(const auto& r:tables)if(integer(r,"wr")!=1)fail(code::corrupt_state,"canonical staging metadata must be WITHOUT ROWID");
    static const auto header_sql="SELECT "+integer_columns({"id","version"})+",typeof(configuration)='blob' AS encoded,length(configuration) AS bytes FROM main._lattice_range_store LIMIT 2";
    auto config=configuration();auto rows=db.query(header_sql);
    if(rows.size()!=1||integer(rows[0],"id")!=1||integer(rows[0],"version")!=1||integer(rows[0],"encoded")!=1)fail(code::corrupt_state,"canonical staging store header invalid");
    if(integer(rows[0],"bytes")!=size(config))fail(code::limits_mismatch,"canonical staging configuration differs");
    if(text(db.query("SELECT configuration FROM main._lattice_range_store WHERE id=1").at(0),"configuration")!=config)fail(code::limits_mismatch,"canonical staging configuration differs");
}
canonical_staging_usage canonical_range_staging::usage() const {
    check_schema();auto u=read_usage(connection());const auto used=fields(u);const int64_t caps[]={limits_.channels,limits_.content_pages,limits_.identities,limits_.content_bytes,limits_.receipt_pages,limits_.receipts,limits_.receipt_bytes,limits_.stored_bytes};
    for(size_t i=0;i<used.size();++i)if(!fits(0,used[i],caps[i]))fail(code::corrupt_state,"canonical staging counters exceed limits");
    if(u.stored_bytes<size(configuration()))fail(code::corrupt_state,"canonical staging configuration is uncharged");return u;
}
void canonical_range_staging::initialize(){
    auto& db=connection();installation_.audit();
    if(!db.query("SELECT 1 FROM main.sqlite_master WHERE name IN ('_lattice_range_store','_lattice_range_attempt','_lattice_range_page') LIMIT 1").empty()){audit();return;}
    const auto config=configuration();if(size(config)>limits_.stored_bytes)fail(code::capacity,"canonical staging configuration exceeds stored cap");
    atomic(db,[&]{
        db.execute("CREATE TABLE main._lattice_range_store(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,configuration BLOB NOT NULL,channels INTEGER NOT NULL,content_pages INTEGER NOT NULL,identities INTEGER NOT NULL,content_bytes INTEGER NOT NULL,receipt_pages INTEGER NOT NULL,receipts INTEGER NOT NULL,receipt_bytes INTEGER NOT NULL,stored_bytes INTEGER NOT NULL) WITHOUT ROWID");
        db.execute("CREATE TABLE main._lattice_range_attempt(channel BLOB PRIMARY KEY NOT NULL,logical BLOB NOT NULL,route INTEGER NOT NULL,state BLOB NOT NULL,verified INTEGER NOT NULL,content_pages INTEGER NOT NULL,identities INTEGER NOT NULL,content_bytes INTEGER NOT NULL,receipt_pages INTEGER NOT NULL,receipts INTEGER NOT NULL,receipt_bytes INTEGER NOT NULL,page_bytes INTEGER NOT NULL) WITHOUT ROWID");
        db.execute("CREATE TABLE main._lattice_range_page(channel BLOB NOT NULL,stream INTEGER NOT NULL,page_index INTEGER NOT NULL,wire BLOB NOT NULL,PRIMARY KEY(channel,stream,page_index)) WITHOUT ROWID");
        db.execute("INSERT INTO main._lattice_range_store VALUES(1,1,?,0,0,0,0,0,0,0,?)",{bytes(config),size(config)});changed(db);(void)usage();return true;
    });
}
void canonical_range_staging::audit_usage() const {
    const auto recorded=usage();auto& db=connection();
    if(!db.query("SELECT 1 FROM main._lattice_range_attempt WHERE typeof(channel)!='blob' OR length(channel) NOT BETWEEN 1 AND 256 OR typeof(logical)!='blob' OR length(logical) NOT BETWEEN 1 AND ? OR typeof(route)!='integer' OR route<=0 OR typeof(state)!='blob' OR length(state) NOT BETWEEN 1 AND ? OR typeof(verified)!='integer' OR verified NOT IN(0,1) OR typeof(content_pages)!='integer' OR content_pages<0 OR typeof(identities)!='integer' OR identities<0 OR typeof(content_bytes)!='integer' OR content_bytes<0 OR typeof(receipt_pages)!='integer' OR receipt_pages<0 OR typeof(receipts)!='integer' OR receipts<0 OR typeof(receipt_bytes)!='integer' OR receipt_bytes<0 OR typeof(page_bytes)!='integer' OR page_bytes<0 LIMIT 1",{count(codec_.maximum.frame_bytes),count(codec_.restart_bytes)}).empty())fail(code::corrupt_state,"canonical staging malformed attempt");
    if(!db.query("SELECT 1 FROM main._lattice_range_page p LEFT JOIN main._lattice_range_attempt a USING(channel) WHERE a.channel IS NULL OR typeof(p.stream)!='integer' OR p.stream NOT IN(0,1) OR typeof(p.page_index)!='integer' OR p.page_index<0 OR typeof(p.wire)!='blob' OR length(p.wire) NOT BETWEEN 1 AND ? LIMIT 1",{count(codec_.maximum.frame_bytes)}).empty())fail(code::corrupt_state,"canonical staging orphan or malformed page");
    if(!db.query("SELECT 1 FROM main._lattice_range_attempt a LEFT JOIN (SELECT channel,SUM(length(channel)+length(wire)) AS bytes FROM main._lattice_range_page GROUP BY channel) p USING(channel) WHERE a.page_bytes!=COALESCE(p.bytes,0) LIMIT 1").empty())fail(code::corrupt_state,"canonical staging page charges differ");
    const auto a=db.query("SELECT COUNT(*) AS channels,COALESCE(SUM(content_pages),0) AS content_pages,COALESCE(SUM(identities),0) AS identities,COALESCE(SUM(content_bytes),0) AS content_bytes,COALESCE(SUM(receipt_pages),0) AS receipt_pages,COALESCE(SUM(receipts),0) AS receipts,COALESCE(SUM(receipt_bytes),0) AS receipt_bytes,COALESCE(SUM(length(channel)+length(logical)+length(state)+page_bytes),0) AS stored_bytes FROM main._lattice_range_attempt").at(0);
    canonical_staging_usage actual{integer(a,"channels"),integer(a,"content_pages"),integer(a,"identities"),integer(a,"content_bytes"),integer(a,"receipt_pages"),integer(a,"receipts"),integer(a,"receipt_bytes"),add(size(configuration()),integer(a,"stored_bytes"))};
    if(actual!=recorded)fail(code::corrupt_state,"canonical staging actual reservations differ from counters");
}
canonical_staging_snapshot canonical_range_staging::addressed(const cr::attempt& a,const std::string& m,std::optional<uint64_t> route) const {
    // Validate caller strings/counters before SQL or owned input copies.
    const auto key_wire=cr::encode({a,1,cr::end{m}},codec_);if(route)count(*route);
    const auto u=usage();auto& db=connection();
    const auto shape=db.query("SELECT typeof(logical)='blob' AND length(logical) BETWEEN 1 AND ? AND typeof(route)='integer' AND route>0 AND typeof(state)='blob' AND length(state) BETWEEN 1 AND ? AND typeof(verified)='integer' AND verified IN(0,1) AND typeof(page_bytes)='integer' AND page_bytes>=0 AS valid,length(channel) AS key_bytes,length(logical) AS logical_bytes,length(state) AS state_bytes,CASE WHEN typeof(page_bytes)='integer' THEN page_bytes END AS page_bytes FROM main._lattice_range_attempt WHERE channel=?",{count(codec_.maximum.frame_bytes),count(codec_.restart_bytes),bytes(a.channel)});
    if(shape.size()!=1)fail(code::stale_attempt,"canonical staging attempt absent");if(integer(shape[0],"valid")!=1)fail(code::corrupt_state,"canonical staging addressed types/lengths invalid");
    auto remaining=u.stored_bytes-size(configuration());for(const auto* key:{"key_bytes","logical_bytes","state_bytes","page_bytes"}){const auto n=integer(shape[0],key);if(n<0||n>remaining)fail(code::corrupt_state,"canonical addressed bytes exceed counters");remaining-=n;}
    // logical/state BLOB lengths/types were checked above in this same owned
    // transaction. List them explicitly; an added corrupt column is never copied.
    static const auto row_sql="SELECT logical,state,"+integer_columns({"route","verified","content_pages","identities","content_bytes","receipt_pages","receipts","receipt_bytes","page_bytes"})+" FROM main._lattice_range_attempt WHERE channel=?";
    const auto row=db.query(row_sql,{bytes(a.channel)}).at(0);
    if(text(row,"logical")!=key_wire)fail(code::stale_attempt,"canonical logical attempt or manifest differs");
    const auto actual_route=integer(row,"route");if(route&&(*route==0||static_cast<uint64_t>(actual_route)!=*route))fail(code::stale_route,"canonical physical route is stale");
    auto state=stored_state(text(row,"state"),a,codec_);if(state.offer.manifest_digest!=m)fail(code::corrupt_state,"canonical state manifest differs");
    const auto& c=state.offer.counts;const auto verified=integer(row,"verified")==1;
    if(integer(row,"content_pages")!=count(c.content_pages)||integer(row,"identities")!=count(c.identities)||integer(row,"content_bytes")!=count(c.content_bytes)||integer(row,"receipt_pages")!=count(c.receipt_pages)||integer(row,"receipts")!=count(c.receipts)||integer(row,"receipt_bytes")!=count(c.receipt_bytes)||verified!=(state.status==cr::phase::sequence_complete_unverified))fail(code::corrupt_state,"canonical state/reservations disagree");
    const auto reserved=fields({1,count(c.content_pages),count(c.identities),count(c.content_bytes),count(c.receipt_pages),count(c.receipts),count(c.receipt_bytes),0});const auto used=fields(u);
    for(size_t i=0;i<reserved.size();++i)if(reserved[i]>used[i])fail(code::corrupt_state,"canonical attempt exceeds aggregate reservations");
    auto result=snapshot(std::move(state),actual_route,verified);
    const auto current=installation_.read(a.channel);if(!current||current->binding!=result.installation_binding||(current->active!=std::optional<receive_install_identity>{result.installation_identity}&&current->last_installed!=std::optional<receive_install_identity>{result.installation_identity}))fail(code::stale_attempt,"canonical attempt no longer has retained installation custody");
    return result;
}
canonical_staging_snapshot canonical_range_staging::verify_storage(const canonical_staging_snapshot& stored,bool whole) const {
    auto state=cr::begin(stored.state.logical,stored.state.frozen_request,stored.state.offer,codec_);
    const auto effective=narrow(codec_,state.frozen_request.budget);cr::stream_hasher content(state.offer,cr::stream_kind::content,effective),receipts(state.offer,cr::stream_kind::receipts,effective);
    auto& db=connection();const auto& channel=state.logical.channel;int64_t actual_bytes=0;
    for(const int64_t kind:{0,1}){
        const auto pages=kind==0?stored.state.next_content_page:stored.state.next_receipt_page;
        for(uint64_t index=0;index<pages;++index){auto page=stored_page(db,channel,kind,index,effective);
            const auto wire=cr::encode(page,effective);actual_bytes=add(actual_bytes,add(size(channel),size(wire)));
            if(const auto* p=std::get_if<cr::content_page>(&page.body))for(const auto& row:p->items)content.append(row);
            else for(const auto& row:std::get<cr::receipt_page>(page.body).items)receipts.append(row);
            state=cr::propose(state,page,codec_);
        }
        const auto summary=db.query("SELECT COUNT(*) AS n,COALESCE(MIN(CASE WHEN typeof(page_index)='integer' THEN page_index END),0) AS first,COALESCE(MAX(CASE WHEN typeof(page_index)='integer' THEN page_index END),-1) AS last FROM main._lattice_range_page WHERE channel=? AND stream=?",{bytes(channel),kind}).at(0);
        if(integer(summary,"n")!=count(pages)||integer(summary,"first")!=0||integer(summary,"last")!=count(pages)-1)fail(code::corrupt_state,"canonical exact page index set differs");
    }
    const auto charge=db.query("SELECT CASE WHEN typeof(page_bytes)='integer' THEN page_bytes END AS page_bytes FROM main._lattice_range_attempt WHERE channel=?",{bytes(channel)}).at(0);
    if(integer(charge,"page_bytes")!=actual_bytes)fail(code::corrupt_state,"canonical actual page bytes differ");
    if(whole||stored.content_verified){
        state=cr::propose(state,{state.logical,1,cr::end{state.offer.manifest_digest}},codec_);
        if(content.finish()!=state.offer.content_digest||receipts.finish()!=state.offer.receipt_digest)fail(code::digest_mismatch,"canonical retained whole C/E mismatch");
    }
    auto compare=state;if(whole&&!stored.content_verified)compare.status=cr::phase::receiving;
    if(compare!=stored.state)fail(code::corrupt_state,"canonical recorded progress differs from retained pages");
    return snapshot(std::move(state),stored.route_generation,whole||stored.content_verified);
}
void canonical_range_staging::audit() const {
    connection();installation_.audit();audit_usage();auto& db=connection();std::optional<std::string> previous;
    while(true){auto rows=previous?db.query("SELECT channel,logical FROM main._lattice_range_attempt WHERE channel>? ORDER BY channel LIMIT 1",{bytes(*previous)}):db.query("SELECT channel,logical FROM main._lattice_range_attempt ORDER BY channel LIMIT 1");if(rows.empty())break;
        const auto channel=text(rows[0],"channel");cr::frame logical;
        try{logical=cr::decode(text(rows[0],"logical"),codec_);}catch(const cr::protocol_error&){fail(code::corrupt_state,"canonical logical header invalid");}
        if(logical.logical.channel!=channel||logical.route_generation!=1||!std::holds_alternative<cr::end>(logical.body))fail(code::corrupt_state,"canonical logical header shape differs");
        (void)verify_storage(addressed(logical.logical,std::get<cr::end>(logical.body).manifest_digest,std::nullopt),false);previous=channel;
    }
}
canonical_staging_begin canonical_range_staging::begin(const cr::attempt& a,const cr::request& request,const cr::manifest& manifest,uint64_t route){
    auto& db=connection();const auto initial=cr::begin(a,request,manifest,codec_);const auto effective=narrow(codec_,request.budget);
    // Validate the real route envelope and SHA-prefix reservations at admission.
    (void)cr::encode({a,route,request},codec_);(void)cr::encode({a,route,manifest},effective);
    cr::stream_hasher content(manifest,cr::stream_kind::content,effective),receipts(manifest,cr::stream_kind::receipts,effective);
    const auto first=snapshot(initial,route,false);const auto image=cr::encode_state(initial,codec_);const auto key_wire=cr::encode({a,1,cr::end{manifest.manifest_digest}},codec_);
    const auto before=usage();
    return atomic(db,[&]{
        const auto admitted=installation_.begin(first.installation_binding,first.installation_identity);
        if(!db.query("SELECT 1 FROM main._lattice_range_attempt WHERE channel=?",{bytes(a.channel)}).empty()){
            const auto existing=resume(a,manifest.manifest_digest,route);
            if(existing.state.frozen_request!=request||existing.state.offer!=manifest)fail(code::stale_attempt,"canonical begin cannot replace frozen bytes");
            return canonical_staging_begin{admitted.disposition,existing};
        }
        if(admitted.disposition==receive_install_disposition::already_installed)return canonical_staging_begin{admitted.disposition,std::nullopt};
        const auto& c=manifest.counts;auto next=before;
        const int64_t extra[]={1,count(c.content_pages),count(c.identities),count(c.content_bytes),count(c.receipt_pages),count(c.receipts),count(c.receipt_bytes),add(size(a.channel),add(size(key_wire),size(image)))};
        const int64_t cap[]={limits_.channels,limits_.content_pages,limits_.identities,limits_.content_bytes,limits_.receipt_pages,limits_.receipts,limits_.receipt_bytes,limits_.stored_bytes};auto all=fields(before);
        for(size_t i=0;i<all.size();++i){if(!fits(all[i],extra[i],cap[i]))fail(code::capacity,"canonical declaration exceeds aggregate reservation");all[i]+=extra[i];}
        next={all[0],all[1],all[2],all[3],all[4],all[5],all[6],all[7]};
        db.execute("INSERT INTO main._lattice_range_attempt VALUES(?,?,?,?,0,?,?,?,?,?,?,0)",{bytes(a.channel),bytes(key_wire),count(route),bytes(image),count(c.content_pages),count(c.identities),count(c.content_bytes),count(c.receipt_pages),count(c.receipts),count(c.receipt_bytes)});changed(db);write_usage(db,before,next);
        const auto actual=addressed(a,manifest.manifest_digest,route);if(actual.state!=initial)fail(code::corrupt_state,"canonical begin state was changed");
        return canonical_staging_begin{admitted.disposition,actual};
    });
}
canonical_staging_snapshot canonical_range_staging::resume(const cr::attempt& a,const std::string& m,uint64_t route) const {connection();audit_usage();return verify_storage(addressed(a,m,route),false);}
canonical_staging_snapshot canonical_range_staging::append(const cr::frame& f){
    auto& db=connection();(void)cr::encode(f,codec_);const auto [kind,index]=page_key(f);const auto m=manifest_id(f);
    const auto old=addressed(f.logical,m,f.route_generation);const auto effective=narrow(codec_,old.state.frozen_request.budget);const auto wire=logical_wire(f,effective);
    if(!db.query("SELECT 1 FROM main._lattice_range_page WHERE channel=? AND stream=? AND page_index=?",{bytes(f.logical.channel),kind,count(index)}).empty()){
        const auto prior=stored_page(db,f.logical.channel,kind,index,effective);
        if(cr::encode(prior,effective)!=wire)fail(code::conflicting_page,"canonical page index has different retained bytes");
        if(old.content_verified){audit_usage();return verify_storage(old,true);}return old;
    }
    const auto next=cr::propose(old.state,f,codec_);const auto image=cr::encode_state(next,codec_);const auto u=usage();
    const auto r=db.query("SELECT length(state) AS bytes,CASE WHEN typeof(page_bytes)='integer' THEN page_bytes END AS page_bytes FROM main._lattice_range_attempt WHERE channel=?",{bytes(f.logical.channel)}).at(0);
    const auto prior_bytes=integer(r,"bytes"),prior_page_bytes=integer(r,"page_bytes");
    if(prior_bytes<0||prior_bytes>u.stored_bytes)fail(code::corrupt_state,"canonical state charge exceeds usage");
    const auto added=add(size(f.logical.channel),size(wire));const auto stored=add(size(image),added);
    if(!fits(u.stored_bytes-prior_bytes,stored,limits_.stored_bytes))fail(code::capacity,"canonical page/state exceeds stored-byte cap");
    const auto page_bytes=add(prior_page_bytes,added);auto after=u;after.stored_bytes=u.stored_bytes-prior_bytes+stored;
    return atomic(db,[&]{
        db.execute("INSERT INTO main._lattice_range_page VALUES(?,?,?,?)",{bytes(f.logical.channel),kind,count(index),bytes(wire)});changed(db);
        db.execute("UPDATE main._lattice_range_attempt SET state=?,page_bytes=? WHERE channel=?",{bytes(image),page_bytes,bytes(f.logical.channel)});changed(db);write_usage(db,u,after);
        const auto actual=addressed(f.logical,m,f.route_generation);
        if(actual.state!=next||integer(db.query("SELECT CASE WHEN typeof(page_bytes)='integer' THEN page_bytes END AS page_bytes FROM main._lattice_range_attempt WHERE channel=?",{bytes(f.logical.channel)}).at(0),"page_bytes")!=page_bytes||cr::encode(stored_page(db,f.logical.channel,kind,index,effective),effective)!=wire)fail(code::corrupt_state,"canonical page/state write was changed");
        return actual;
    });
}
canonical_staging_snapshot canonical_range_staging::verify_end(const cr::frame& f){
    auto& db=connection();(void)cr::encode(f,codec_);if(!std::holds_alternative<cr::end>(f.body))fail(code::invalid_argument,"canonical verification requires end frame");
    const auto m=manifest_id(f);const auto old=addressed(f.logical,m,f.route_generation);(void)cr::encode(f,narrow(codec_,old.state.frozen_request.budget));audit_usage();
    const auto next=verify_storage(old,true);if(old.content_verified)return next;
    const auto image=cr::encode_state(next.state,codec_);const auto u=usage();const auto prior=integer(db.query("SELECT length(state) AS bytes FROM main._lattice_range_attempt WHERE channel=?",{bytes(f.logical.channel)}).at(0),"bytes");
    if(prior<0||prior>u.stored_bytes)fail(code::corrupt_state,"canonical state charge exceeds usage");if(!fits(u.stored_bytes-prior,size(image),limits_.stored_bytes))fail(code::capacity,"canonical verified state exceeds stored-byte cap");
    auto after=u;after.stored_bytes=u.stored_bytes-prior+size(image);
    return atomic(db,[&]{db.execute("UPDATE main._lattice_range_attempt SET state=?,verified=1 WHERE channel=?",{bytes(image),bytes(f.logical.channel)});changed(db);write_usage(db,u,after);
        const auto actual=addressed(f.logical,m,f.route_generation);if(actual.state!=next.state||!actual.content_verified)fail(code::corrupt_state,"canonical verified write was changed");return actual;});
}
canonical_staging_snapshot canonical_range_staging::rebind(const cr::attempt& a,const std::string& m,uint64_t expected,uint64_t replacement){
    auto& db=connection();count(replacement);if(!replacement||replacement<=expected)fail(code::stale_route,"canonical replacement route must increase");const auto old=addressed(a,m,expected);
    // Only an active install may resume a different physical route. Retained
    // installed replies need no new page intake and remain exact store retries.
    const auto current=installation_.read(a.channel);if(!current||current->active!=std::optional<receive_install_identity>{old.installation_identity})fail(code::stale_attempt,"canonical rebind requires exact active installation");
    (void)cr::encode({a,replacement,old.state.frozen_request},codec_);
    (void)cr::encode({a,replacement,old.state.offer},narrow(codec_,old.state.frozen_request.budget));
    return atomic(db,[&]{db.execute("UPDATE main._lattice_range_attempt SET route=? WHERE channel=? AND route=?",{count(replacement),bytes(a.channel),count(expected)});changed(db);const auto actual=addressed(a,m,replacement);if(actual.state!=old.state)fail(code::corrupt_state,"canonical rebind changed frozen state");return actual;});
}
cr::message canonical_range_staging::read_verified_page(const cr::attempt& a,const std::string& m,uint64_t route,cr::stream_kind kind,uint64_t index) const {
    auto& db=connection();const auto old=addressed(a,m,route);if(!old.content_verified)fail(code::not_verified,"canonical retained stream has not been verified");
    const auto k=stream(kind);const auto total=k==0?old.state.offer.counts.content_pages:old.state.offer.counts.receipt_pages;if(index>=total)fail(code::invalid_argument,"canonical verified page index is outside manifest");
    auto f=stored_page(db,a.channel,k,index,narrow(codec_,old.state.frozen_request.budget));if(f.logical!=a||manifest_id(f)!=m)fail(code::corrupt_state,"canonical verified page belongs to another attempt");return std::move(f.body);
}
void canonical_range_staging::remove_staged(const canonical_staging_snapshot& s){
    auto& db=connection();audit_usage();const auto before=usage();const auto r=db.query("SELECT length(channel)+length(logical)+length(state)+page_bytes AS bytes FROM main._lattice_range_attempt WHERE channel=?",{bytes(s.state.logical.channel)}).at(0);
    const auto& c=s.state.offer.counts;const int64_t removed[]={1,count(c.content_pages),count(c.identities),count(c.content_bytes),count(c.receipt_pages),count(c.receipts),count(c.receipt_bytes),integer(r,"bytes")};auto remaining=fields(before);
    for(size_t i=0;i<remaining.size();++i){if(removed[i]<0||removed[i]>remaining[i])fail(code::corrupt_state,"canonical removal exceeds usage");remaining[i]-=removed[i];}
    canonical_staging_usage after{remaining[0],remaining[1],remaining[2],remaining[3],remaining[4],remaining[5],remaining[6],remaining[7]};
    db.execute("DELETE FROM main._lattice_range_page WHERE channel=?",{bytes(s.state.logical.channel)});
    if(!db.query("SELECT 1 FROM main._lattice_range_page WHERE channel=? LIMIT 1",{bytes(s.state.logical.channel)}).empty())fail(code::corrupt_state,"canonical page cleanup was ignored");
    db.execute("DELETE FROM main._lattice_range_attempt WHERE channel=?",{bytes(s.state.logical.channel)});changed(db);
    if(!db.query("SELECT 1 FROM main._lattice_range_attempt WHERE channel=?",{bytes(s.state.logical.channel)}).empty())fail(code::corrupt_state,"canonical attempt cleanup was ignored");
    write_usage(db,before,after);
}
void canonical_range_staging::abandon_active(const cr::attempt& a,const std::string& m,uint64_t route){
    auto& db=connection();const auto old=addressed(a,m,route);
    atomic(db,[&]{installation_.abandon_active(old.installation_binding,old.installation_identity);remove_staged(old);return true;});
}
void canonical_range_staging::release_installed(const cr::attempt& a,const std::string& m,uint64_t route){
    auto& db=connection();const auto old=addressed(a,m,route);const auto installed=installation_.read(a.channel);
    if(!installed||installed->binding!=old.installation_binding||installed->last_installed!=std::optional<receive_install_identity>{old.installation_identity})fail(code::stale_attempt,"canonical release requires exact retained installed identity");
    atomic(db,[&]{remove_staged(old);return true;});
}
} // namespace lattice::detail
