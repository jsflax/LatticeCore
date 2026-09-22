#include "sync_canonical_range.hpp"
#include "canonical_range_package.hpp"
#include "vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <limits>
#include <set>
#include <type_traits>

namespace lattice::detail::canonical_range {
namespace {
using json = nlohmann::json;
constexpr uint64_t maximum = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
constexpr uint64_t frame_max = 16 * 1024 * 1024;
void check(bool ok, const char* why) { if (!ok) throw protocol_error(why); }
uint64_t add(uint64_t a, uint64_t b) {
    check(a <= maximum && b <= maximum - a, "canonical count overflow"); return a + b;
}
bool bytes_less(std::string_view a, std::string_view b) {
    return std::lexicographical_compare(a.begin(),a.end(),b.begin(),b.end(),
        [](unsigned char x,unsigned char y){return x<y;});
}
bool less(const identity& a,const identity& b) {
    return a.table==b.table ? bytes_less(a.id,b.id) : bytes_less(a.table,b.table);
}
bool utf8(std::string_view s) {
    size_t i=0;
    while(i<s.size()) {
        const auto c=static_cast<unsigned char>(s[i++]);
        if(c<0x80) continue;
        unsigned n=0; uint32_t value=0,minimum=0;
        if(c>=0xc2 && c<=0xdf){n=1;value=c&31;minimum=0x80;}
        else if(c>=0xe0 && c<=0xef){n=2;value=c&15;minimum=0x800;}
        else if(c>=0xf0 && c<=0xf4){n=3;value=c&7;minimum=0x10000;}
        else return false;
        if(n>s.size()-i) return false;
        while(n--){const auto x=static_cast<unsigned char>(s[i++]);if((x&0xc0)!=0x80)return false;value=(value<<6)|(x&63);}
        if(value<minimum || value>0x10ffff || (value>=0xd800 && value<=0xdfff)) return false;
    }
    return true;
}
bool hex(char c){return (c>='0'&&c<='9')||(c>='a'&&c<='f');}
void digest(const std::string& s){check(s.size()==64 && std::all_of(s.begin(),s.end(),hex),"invalid canonical digest");}
void uuid(const std::string& s){
    check(s.size()==36,"invalid canonical UUID");
    for(size_t i=0;i<s.size();++i) check((i==8||i==13||i==18||i==23)?s[i]=='-':hex(s[i]),"invalid canonical UUID");
}
void name(const std::string& s,const limits& b){
    check(!s.empty() && s.size()<=std::min<size_t>(256,b.string_bytes) && utf8(s),"invalid canonical identifier");
    check(std::none_of(s.begin(),s.end(),[](unsigned char c){return c<32||c==127;}),"control byte in canonical identifier");
}
const char* spelling(mode m){switch(m){case mode::full:return "full";case mode::delta:return "delta";}throw protocol_error("invalid canonical mode");}
const char* spelling(frontier_kind k){switch(k){case frontier_kind::uninitialized:return "uninitialized";case frontier_kind::beginning_null:return "beginning_null";case frontier_kind::position:return "position";}throw protocol_error("invalid frontier kind");}
const char* spelling(decision d){switch(d){case decision::applied:return "applied";case decision::no_op:return "no_op";case decision::policy:return "policy";}throw protocol_error("invalid receipt decision");}
const char* spelling(unknown_reason r){switch(r){case unknown_reason::legacy:return "legacy";case unknown_reason::missing_coverage:return "missing_coverage";case unknown_reason::retired_coverage:return "retired_coverage";case unknown_reason::source_changed:return "source_changed";case unknown_reason::unproved_provenance:return "unproved_provenance";}throw protocol_error("invalid unknown reason");}
void wire_budget(const wire_limits& b){
    check(b.frame_bytes>0&&b.frame_bytes<=frame_max&&b.payload_bytes>0&&b.payload_bytes<=b.frame_bytes&&b.items_per_page>0&&b.items_per_page<=4096,"invalid canonical wire budget");
    for(auto n:{b.content_pages,b.content_identities,b.content_bytes,b.receipt_pages,b.receipts,b.receipt_bytes}) check(n<=maximum,"canonical budget overflow");
}
void budgets(const limits& b){
    wire_budget(b.maximum);
    check(b.depth>0&&b.depth<=64&&b.nodes>0&&b.nodes<=262144&&b.string_bytes>=64&&b.string_bytes<=b.maximum.frame_bytes,"invalid canonical parser budget");
    check(b.request_entries<=8192&&b.request_targets<=8192&&b.request_target_bytes<=frame_max&&b.restart_bytes>0&&b.restart_bytes<=frame_max&&b.lease_ms>0&&b.lease_ms<=maximum,"invalid canonical metadata budget");
    const auto& v=b.values;
    check(v.raw_bytes>0&&v.raw_bytes<=frame_max&&v.fields>0&&v.fields<=4096&&v.name_bytes>0&&v.name_bytes<=256&&v.value_bytes<=v.raw_bytes&&v.decoded_bytes<=v.raw_bytes,"invalid canonical value budget");
}
void within(const wire_limits& x,const wire_limits& b){
    wire_budget(x);
    check(x.frame_bytes<=b.frame_bytes&&x.payload_bytes<=b.payload_bytes&&x.items_per_page<=b.items_per_page&&x.content_pages<=b.content_pages&&x.content_identities<=b.content_identities&&x.content_bytes<=b.content_bytes&&x.receipt_pages<=b.receipt_pages&&x.receipts<=b.receipts&&x.receipt_bytes<=b.receipt_bytes,"request exceeds local canonical budget");
}
void valid(const attempt& a,const limits& b){uuid(a.receiver_incarnation);uuid(a.channel_incarnation);name(a.channel,b);uuid(a.attempt_id);check(a.sequence>0&&a.sequence<=maximum,"invalid logical sequence");}
void valid(const source_binding& s,const limits& b){name(s.authority,b);uuid(s.source_id);uuid(s.epoch);digest(s.scope_digest);digest(s.schema_digest);}
void valid(const identity& i,const limits& b){name(i.table,b);name(i.id,b);}
void valid(const frontier& f){
    (void)spelling(f.kind);
    check(f.kind==frontier_kind::position ? f.value&&*f.value<=maximum : !f.value,"contradictory canonical frontier");
}
void selection(mode m,const std::optional<uint64_t>& base,uint64_t head){
    (void)spelling(m);check(head<=maximum,"invalid canonical head");
    check(m==mode::full ? !base : base&&*base<=head,"invalid capture base");
}
uint64_t identity_bytes(const identity& i){return add(16,add(i.table.size(),i.id.size()));}
std::vector<identity> rebase(const request& r){
    std::vector<identity> result;
    for(const auto& q:r.receipts) for(const auto& t:q.targets) result.push_back(t);
    std::sort(result.begin(),result.end(),less);result.erase(std::unique(result.begin(),result.end()),result.end());return result;
}
void request_shape(const attempt& a,const request& r,const limits& b){
    budgets(b);valid(a,b);valid(r.source,b);within(r.budget,b.maximum);
    selection(r.selection,r.base,maximum);valid(r.expected.base);
    check(r.expected.revision<maximum,"installation revision exhausted");
    check((r.expected.base.kind==frontier_kind::position)==(r.expected.revision>0),"expected frontier contradicts installation revision");
    check(a.sequence>r.expected.revision,"logical attempt sequence must exceed expected revision");
    if(r.expected.binding){valid(*r.expected.binding,b);check(r.expected.binding->authority==r.source.authority&&r.expected.binding->scope_digest==r.source.scope_digest,"expected owner scope differs");}
    else check(r.expected.base.kind==frontier_kind::uninitialized,"initialized state requires expected binding");
    if(r.selection==mode::delta)check(r.expected.binding==std::optional<source_binding>{r.source}&&r.expected.base.kind==frontier_kind::position&&r.expected.base.value==r.base,"delta expected base differs");
    check(r.receipts.size()<=b.request_entries&&r.receipts.size()<=r.budget.receipts,"too many receipt requests");
    uint64_t targets=0,bytes=0;const std::string* last=nullptr;
    for(const auto& q:r.receipts){
        name(q.original_id,b);check(!last||bytes_less(*last,q.original_id),"duplicate or unordered receipt request");last=&q.original_id;
        if(q.namespace_id)name(*q.namespace_id,b);
        check(!q.targets.empty()&&q.targets.size()<=b.request_targets-targets,"too many receipt targets");
        targets+=q.targets.size();const identity* previous=nullptr;
        for(const auto& t:q.targets){valid(t,b);check(!previous||less(*previous,t),"duplicate or unordered receipt target");previous=&t;bytes=add(bytes,identity_bytes(t));check(bytes<=b.request_target_bytes,"receipt target bytes exceeded");}
    }
}
void stream_counts(uint64_t pages,uint64_t n,uint64_t bytes,uint64_t max_pages,uint64_t max_n,uint64_t max_bytes,const wire_limits& b){
    check(pages<=max_pages&&n<=max_n&&bytes<=max_bytes,"canonical stream exceeds budget");
    check((pages==0&&n==0&&bytes==0)||(pages>0&&n>=pages&&bytes>=n),"inconsistent canonical stream counts");
    if(n)check((n-1)/b.items_per_page<pages&&(bytes-1)/b.frame_bytes<pages,"impossible canonical stream counts");
}
void manifest_shape(const manifest& m,const limits& b){
    budgets(b);digest(m.request_digest);valid(m.source,b);selection(m.selection,m.base,m.head);
    name(m.protection.id,b);check(m.protection.duration_ms>0&&m.protection.duration_ms<=b.lease_ms,"invalid canonical lease duration");
    const auto& t=m.counts;const auto& w=b.maximum;
    check(t.identities==add(t.present,t.tombstones),"canonical tag totals differ");
    stream_counts(t.content_pages,t.identities,t.content_bytes,w.content_pages,w.content_identities,w.content_bytes,w);
    stream_counts(t.receipt_pages,t.receipts,t.receipt_bytes,w.receipt_pages,w.receipts,w.receipt_bytes,w);
    check(t.receipts<=b.request_entries&&t.rebase_identities<=b.request_targets&&t.rebase_identities<=t.identities&&t.rebase_bytes<=b.request_target_bytes,"invalid rebase totals");
    check((t.rebase_identities==0)==(t.rebase_bytes==0),"inconsistent rebase bytes");
    digest(m.content_digest);digest(m.receipt_digest);digest(m.rebase_digest);
}
void content_shape(const content_item& x,const limits& b){
    valid(x.key,b);check(!x.value.valueless_by_exception(),"missing canonical content tag");
    if(const auto* p=std::get_if<present>(&x.value)){
        check(!p->payload.empty()&&p->payload.size()<=b.maximum.payload_bytes&&p->payload.size()<=b.string_bytes,"canonical payload exceeds budget");
        (void)sync_recovery::decode_values(p->payload,b.values);
    }
}
void receipt_shape(const receipt_item& x,const limits& b){
    name(x.original_id,b);
    std::visit([&](const auto& v){using T=std::decay_t<decltype(v)>;
        if constexpr(std::is_same_v<T,unknown>)(void)spelling(v.reason);
        else {name(v.namespace_id,b);name(v.coverage_id,b);
            if constexpr(std::is_same_v<T,committed>){(void)spelling(v.outcome);check(v.position>0&&v.position<=maximum,"invalid receipt position");if(v.accepted_target)valid(*v.accepted_target,b);}}
    },x.value);
}

// Incremental SHA uses bounded input chunks: PicoSHA2 process() copies its range.
class hash_writer {
    picosha2::hash256_one_by_one hash_;
    uint64_t bytes_=0;
public:
    void raw(std::string_view s){
        check(s.size()<=std::numeric_limits<uint64_t>::max()/8-bytes_,"canonical SHA length overflow");bytes_+=s.size();
        while(!s.empty()){const auto n=std::min<size_t>(4096,s.size());hash_.process(s.begin(),s.begin()+n);s.remove_prefix(n);}
    }
    void byte(uint8_t c){const char v=static_cast<char>(c);raw(std::string_view(&v,1));}
    void u(uint64_t n){char bytes[8];for(int i=0;i<8;++i)bytes[7-i]=static_cast<char>(n>>(8*i));raw(std::string_view(bytes,8));}
    void s(std::string_view s){u(s.size());raw(s);}
    void d(const std::string& d){digest(d);auto nib=[](char c){return c<='9'?c-'0':c-'a'+10;};for(size_t i=0;i<64;i+=2)byte(static_cast<uint8_t>((nib(d[i])<<4)|nib(d[i+1])));}
    explicit hash_writer(std::string_view domain){s(std::string("lattice.canonical-range.v2/")+std::string(domain));}
    std::string finish(){hash_.finish();return picosha2::get_hash_hex_string(hash_);}
};
void write(hash_writer& h,const identity& x){h.s(x.table);h.s(x.id);}
void write(hash_writer& h,const source_binding& x){h.s(x.authority);h.s(x.source_id);h.s(x.epoch);h.d(x.scope_digest);h.d(x.schema_digest);}
void write(hash_writer& h,const attempt& x){h.s(x.receiver_incarnation);h.s(x.channel_incarnation);h.s(x.channel);h.u(x.sequence);h.s(x.attempt_id);}
void write(hash_writer& h,const std::optional<uint64_t>& x){h.byte(x?1:0);if(x)h.u(*x);}
void write(hash_writer& h,const wire_limits& x){for(auto n:{x.frame_bytes,x.payload_bytes,x.items_per_page,x.content_pages,x.content_identities,x.content_bytes,x.receipt_pages,x.receipts,x.receipt_bytes})h.u(n);}
void write(hash_writer& h,const expected_install& x){h.u(x.revision);h.byte(x.binding?1:0);if(x.binding)write(h,*x.binding);h.s(spelling(x.base.kind));if(x.base.value)h.u(*x.base.value);}
void write(hash_writer& h,const totals& x){for(auto n:{x.content_pages,x.identities,x.present,x.tombstones,x.content_bytes,x.receipt_pages,x.receipts,x.receipt_bytes,x.rebase_identities,x.rebase_bytes})h.u(n);}
void write(hash_writer& h,const content_item& x){h.byte(std::holds_alternative<present>(x.value)?1:2);write(h,x.key);if(const auto* p=std::get_if<present>(&x.value))h.s(p->payload);}
void write(hash_writer& h,const receipt_item& x){
    h.s(x.original_id);std::visit([&](const auto& v){using T=std::decay_t<decltype(v)>;
        if constexpr(std::is_same_v<T,unknown>){h.s("unknown");h.s(spelling(v.reason));}
        else {h.s(std::is_same_v<T,committed>?"committed":"not_committed");h.s(v.namespace_id);h.s(v.coverage_id);
            if constexpr(std::is_same_v<T,committed>){h.s(spelling(v.outcome));h.u(v.position);h.byte(v.accepted_target?1:0);if(v.accepted_target)write(h,*v.accepted_target);}}
    },x.value);
}
std::string request_hash(const attempt& a,const request& r){
    hash_writer h("request");write(h,a);write(h,r.source);h.s(spelling(r.selection));write(h,r.base);write(h,r.expected);write(h,r.budget);h.u(r.receipts.size());
    for(const auto& q:r.receipts){h.s(q.original_id);h.s(q.namespace_id?"negotiated":"unknown");if(q.namespace_id)h.s(*q.namespace_id);h.u(q.targets.size());for(const auto& i:q.targets)write(h,i);}return h.finish();
}
std::string anchor(const manifest& m){hash_writer h("anchor");h.d(m.request_digest);write(h,m.source);h.s(spelling(m.selection));write(h,m.base);h.u(m.head);h.s(m.protection.id);h.u(m.protection.duration_ms);return h.finish();}
std::string manifest_hash(const manifest& m){hash_writer h("manifest");h.d(anchor(m));write(h,m.counts);h.d(m.content_digest);h.d(m.receipt_digest);h.d(m.rebase_digest);return h.finish();}
void request_valid(const attempt& a,const request& r,const limits& b){request_shape(a,r,b);digest(r.request_digest);check(request_hash(a,r)==r.request_digest,"request digest mismatch");}
void manifest_valid(const manifest& m,const limits& b){manifest_shape(m,b);digest(m.manifest_digest);check(manifest_hash(m)==m.manifest_digest,"manifest digest mismatch");}

uint64_t content_size(const content_item& x){uint64_t n=add(1,identity_bytes(x.key));if(const auto* p=std::get_if<present>(&x.value))n=add(n,add(8,p->payload.size()));return n;}
uint64_t receipt_size(const receipt_item& x){
    uint64_t n=add(8,x.original_id.size());
    std::visit([&](const auto& v){using T=std::decay_t<decltype(v)>;
        if constexpr(std::is_same_v<T,unknown>)n=add(n,add(8+7,add(8,std::string_view(spelling(v.reason)).size())));
        else {n=add(n,8+(std::is_same_v<T,committed>?9:13));n=add(n,add(16,add(v.namespace_id.size(),v.coverage_id.size())));
            if constexpr(std::is_same_v<T,committed>){n=add(n,add(17,std::string_view(spelling(v.outcome)).size()));if(v.accepted_target)n=add(n,identity_bytes(*v.accepted_target));}}
    },x.value);return n;
}
template<class Page> void page_shape(const Page& p,const limits& b){
    budgets(b);digest(p.manifest_digest);const auto& w=b.maximum;
    constexpr bool content=std::is_same_v<Page,content_page>;
    check(p.index<(content?w.content_pages:w.receipt_pages)&&!p.items.empty()&&p.items.size()<=w.items_per_page&&p.count==p.items.size(),"invalid canonical page count/index");
    uint64_t bytes=0;
    for(size_t i=0;i<p.items.size();++i){const auto& item=p.items[i];
        if constexpr(content){content_shape(item,b);check(i==0||less(p.items[i-1].key,item.key),"duplicate or unordered content identity");bytes=add(bytes,content_size(item));}
        else {receipt_shape(item,b);check(i==0||bytes_less(p.items[i-1].original_id,item.original_id),"duplicate or unordered receipt identity");bytes=add(bytes,receipt_size(item));}
        check(bytes<=w.frame_bytes&&bytes<=(content?w.content_bytes:w.receipt_bytes),"canonical page bytes exceed budget");
    }
    check(bytes==p.bytes,"canonical page byte count mismatch");
}
template<class Page> std::string page_hash(const Page& p){
    hash_writer h(std::is_same_v<Page,content_page>?"content-page":"receipt-page");h.d(p.manifest_digest);h.u(p.index);h.u(p.count);h.u(p.bytes);for(const auto& x:p.items)write(h,x);return h.finish();
}
template<class Page> void page_valid(const Page& p,const limits& b){page_shape(p,b);digest(p.digest);check(page_hash(p)==p.digest,"canonical page digest mismatch");}

// Same safety pattern as v1, kept private so no v1 parser/ABI behavior changes.
struct bounded_sax : nlohmann::json_sax<json> {
    const limits& budget;size_t nodes=0;
    struct level{bool object;std::set<std::string> keys;};std::vector<level> stack;
    explicit bounded_sax(const limits& b):budget(b){}
    bool node(){return ++nodes<=budget.nodes;}
    bool null() override{return node();}bool boolean(bool) override{return node();}
    bool number_integer(number_integer_t) override{return node();}bool number_unsigned(number_unsigned_t) override{return node();}
    bool number_float(number_float_t,const string_t&) override{return false;}
    bool string(string_t& s) override{return s.size()<=budget.string_bytes&&node();}
    bool binary(binary_t&) override{return false;}
    bool start(bool object){if(!node()||stack.size()>=budget.depth)return false;stack.push_back({object,{}});return true;}
    bool start_object(std::size_t) override{return start(true);}bool start_array(std::size_t) override{return start(false);}
    bool key(string_t& s) override{return !stack.empty()&&stack.back().object&&s.size()<=budget.string_bytes&&node()&&stack.back().keys.insert(s).second;}
    bool end_object() override{stack.pop_back();return true;}bool end_array() override{stack.pop_back();return true;}
    bool parse_error(std::size_t,const std::string&,const nlohmann::detail::exception&) override{return false;}
};
json parse(std::string_view raw,const limits& b,uint64_t cap){
    budgets(b);check(!raw.empty()&&raw.size()<=cap,"raw canonical frame exceeds budget");
    const std::string owned(raw);bounded_sax sax(b);check(json::sax_parse(owned,&sax),"invalid or over-budget canonical JSON");
    return json::parse(owned);
}
void keys(const json& j,std::initializer_list<const char*> fields){check(j.is_object()&&j.size()==fields.size(),"missing or unknown canonical field");for(const auto* k:fields)check(j.contains(k),"missing canonical field");}
std::string text(const json& j){check(j.is_string(),"canonical string required");return j.get<std::string>();}
uint64_t number(const json& j){return sync_recovery::parse_position(text(j));}
std::string decimal(uint64_t n){check(n<=maximum,"canonical number overflow");return std::to_string(n);}
void version(const json& j){check(j.is_number_integer()&&j==2,"unsupported canonical version");}
json optional_number(const std::optional<uint64_t>& n){return n?json(decimal(*n)):json(nullptr);}
std::optional<uint64_t> read_optional_number(const json& j){return j.is_null()?std::nullopt:std::optional<uint64_t>{number(j)};}
mode read_mode(const json& j){const auto s=text(j);check(s=="full"||s=="delta","unknown canonical mode");return s=="full"?mode::full:mode::delta;}
json identity_json(const identity& i){return {{"table",i.table},{"id",i.id}};}
identity read_identity(const json& j){keys(j,{"table","id"});return {text(j.at("table")),text(j.at("id"))};}
json source_json(const source_binding& s){return {{"authority",s.authority},{"source_id",s.source_id},{"epoch",s.epoch},{"scope_digest",s.scope_digest},{"schema_digest",s.schema_digest}};}
source_binding read_source(const json& j){keys(j,{"authority","source_id","epoch","scope_digest","schema_digest"});return {text(j.at("authority")),text(j.at("source_id")),text(j.at("epoch")),text(j.at("scope_digest")),text(j.at("schema_digest"))};}
json attempt_json(const attempt& a){return {{"receiver_incarnation",a.receiver_incarnation},{"channel_incarnation",a.channel_incarnation},{"channel",a.channel},{"sequence",decimal(a.sequence)},{"attempt_id",a.attempt_id}};}
attempt read_attempt(const json& j){keys(j,{"receiver_incarnation","channel_incarnation","channel","sequence","attempt_id"});return {text(j.at("receiver_incarnation")),text(j.at("channel_incarnation")),text(j.at("channel")),number(j.at("sequence")),text(j.at("attempt_id"))};}
json frontier_json(const frontier& f){json j={{"kind",spelling(f.kind)}};if(f.value)j["value"]=decimal(*f.value);return j;}
frontier read_frontier(const json& j){
    check(j.is_object()&&j.contains("kind"),"frontier kind required");const auto k=text(j.at("kind"));
    if(k=="position"){keys(j,{"kind","value"});return {frontier_kind::position,number(j.at("value"))};}
    keys(j,{"kind"});check(k=="uninitialized"||k=="beginning_null","unknown frontier kind");return {k=="uninitialized"?frontier_kind::uninitialized:frontier_kind::beginning_null,std::nullopt};
}
json expected_json(const expected_install& e){return {{"revision",decimal(e.revision)},{"binding",e.binding?source_json(*e.binding):json(nullptr)},{"frontier",frontier_json(e.base)}};}
expected_install read_expected(const json& j){keys(j,{"revision","binding","frontier"});expected_install e;e.revision=number(j.at("revision"));if(!j.at("binding").is_null())e.binding=read_source(j.at("binding"));e.base=read_frontier(j.at("frontier"));return e;}
json budget_json(const wire_limits& b){return {{"frame_bytes",decimal(b.frame_bytes)},{"payload_bytes",decimal(b.payload_bytes)},{"items_per_page",decimal(b.items_per_page)},{"content_pages",decimal(b.content_pages)},{"content_identities",decimal(b.content_identities)},{"content_bytes",decimal(b.content_bytes)},{"receipt_pages",decimal(b.receipt_pages)},{"receipts",decimal(b.receipts)},{"receipt_bytes",decimal(b.receipt_bytes)}};}
wire_limits read_budget(const json& j){
    keys(j,{"frame_bytes","payload_bytes","items_per_page","content_pages","content_identities","content_bytes","receipt_pages","receipts","receipt_bytes"});
    return {number(j.at("frame_bytes")),number(j.at("payload_bytes")),number(j.at("items_per_page")),number(j.at("content_pages")),number(j.at("content_identities")),number(j.at("content_bytes")),number(j.at("receipt_pages")),number(j.at("receipts")),number(j.at("receipt_bytes"))};
}
json request_json(const request& r){
    json entries=json::array();for(const auto& e:r.receipts){json targets=json::array();for(const auto& i:e.targets)targets.push_back(identity_json(i));json p={{"kind",e.namespace_id?"negotiated":"unknown"}};if(e.namespace_id)p["namespace_id"]=*e.namespace_id;entries.push_back({{"original_id",e.original_id},{"provenance",p},{"targets",targets}});}
    return {{"source",source_json(r.source)},{"mode",spelling(r.selection)},{"base",optional_number(r.base)},{"expected_install",expected_json(r.expected)},{"limits",budget_json(r.budget)},{"receipt_requests",entries},{"request_digest",r.request_digest}};
}
request read_request(const json& j,const limits& b){
    keys(j,{"source","mode","base","expected_install","limits","receipt_requests","request_digest"});
    request r;r.source=read_source(j.at("source"));r.selection=read_mode(j.at("mode"));r.base=read_optional_number(j.at("base"));r.expected=read_expected(j.at("expected_install"));r.budget=read_budget(j.at("limits"));r.request_digest=text(j.at("request_digest"));
    const auto& entries=j.at("receipt_requests");check(entries.is_array()&&entries.size()<=b.request_entries,"too many receipt requests");
    size_t count=0;uint64_t bytes=0;
    for(const auto& e:entries){keys(e,{"original_id","provenance","targets"});receipt_request q;q.original_id=text(e.at("original_id"));
        const auto& p=e.at("provenance");check(p.is_object()&&p.contains("kind"),"provenance kind required");const auto kind=text(p.at("kind"));
        if(kind=="negotiated"){keys(p,{"kind","namespace_id"});q.namespace_id=text(p.at("namespace_id"));}else {keys(p,{"kind"});check(kind=="unknown","unknown provenance kind");}
        const auto& targets=e.at("targets");check(targets.is_array()&&!targets.empty()&&targets.size()<=b.request_targets-count,"too many receipt targets");count+=targets.size();
        for(const auto& x:targets){auto i=read_identity(x);valid(i,b);bytes=add(bytes,identity_bytes(i));check(bytes<=b.request_target_bytes,"receipt target bytes exceeded");q.targets.push_back(std::move(i));}
        r.receipts.push_back(std::move(q));
    }return r;
}
json totals_json(const totals& t){return {{"content_pages",decimal(t.content_pages)},{"identities",decimal(t.identities)},{"present",decimal(t.present)},{"tombstones",decimal(t.tombstones)},{"content_bytes",decimal(t.content_bytes)},{"receipt_pages",decimal(t.receipt_pages)},{"receipts",decimal(t.receipts)},{"receipt_bytes",decimal(t.receipt_bytes)},{"rebase_identities",decimal(t.rebase_identities)},{"rebase_bytes",decimal(t.rebase_bytes)}};}
totals read_totals(const json& j){
    keys(j,{"content_pages","identities","present","tombstones","content_bytes","receipt_pages","receipts","receipt_bytes","rebase_identities","rebase_bytes"});
    return {number(j.at("content_pages")),number(j.at("identities")),number(j.at("present")),number(j.at("tombstones")),number(j.at("content_bytes")),number(j.at("receipt_pages")),number(j.at("receipts")),number(j.at("receipt_bytes")),number(j.at("rebase_identities")),number(j.at("rebase_bytes"))};
}
json manifest_json(const manifest& m){return {{"request_digest",m.request_digest},{"source",source_json(m.source)},{"mode",spelling(m.selection)},{"base",optional_number(m.base)},{"head",decimal(m.head)},{"lease",{{"id",m.protection.id},{"duration_ms",decimal(m.protection.duration_ms)}}},{"totals",totals_json(m.counts)},{"content_digest",m.content_digest},{"receipt_digest",m.receipt_digest},{"rebase_digest",m.rebase_digest},{"manifest_digest",m.manifest_digest}};}
manifest read_manifest(const json& j){
    keys(j,{"request_digest","source","mode","base","head","lease","totals","content_digest","receipt_digest","rebase_digest","manifest_digest"});const auto& l=j.at("lease");keys(l,{"id","duration_ms"});
    manifest m;m.request_digest=text(j.at("request_digest"));m.source=read_source(j.at("source"));m.selection=read_mode(j.at("mode"));m.base=read_optional_number(j.at("base"));m.head=number(j.at("head"));m.protection={text(l.at("id")),number(l.at("duration_ms"))};m.counts=read_totals(j.at("totals"));m.content_digest=text(j.at("content_digest"));m.receipt_digest=text(j.at("receipt_digest"));m.rebase_digest=text(j.at("rebase_digest"));m.manifest_digest=text(j.at("manifest_digest"));return m;
}
json item_json(const content_item& x){json j=identity_json(x.key);if(const auto* p=std::get_if<present>(&x.value)){j["tag"]="present";j["payload"]=p->payload;}else j["tag"]="tombstone";return j;}
content_item read_content(const json& j){
    check(j.is_object()&&j.contains("tag"),"content tag required");const auto tag=text(j.at("tag"));
    if(tag=="present"){keys(j,{"table","id","tag","payload"});return {{text(j.at("table")),text(j.at("id"))},present{text(j.at("payload"))}};}
    keys(j,{"table","id","tag"});check(tag=="tombstone","unknown content tag");return {{text(j.at("table")),text(j.at("id"))},tombstone{}};
}
json item_json(const receipt_item& x){
    json j={{"original_id",x.original_id}};
    std::visit([&](const auto& v){using T=std::decay_t<decltype(v)>;
        if constexpr(std::is_same_v<T,unknown>){j["status"]="unknown";j["reason"]=spelling(v.reason);}
        else {j["status"]=std::is_same_v<T,committed>?"committed":"not_committed";j["namespace_id"]=v.namespace_id;j["coverage_id"]=v.coverage_id;
            if constexpr(std::is_same_v<T,committed>){j["decision"]=spelling(v.outcome);j["position"]=decimal(v.position);j["accepted_target"]=v.accepted_target?identity_json(*v.accepted_target):json(nullptr);}}
    },x.value);return j;
}
receipt_item read_receipt(const json& j){
    check(j.is_object()&&j.contains("status"),"receipt status required");const auto status=text(j.at("status"));
    if(status=="committed"){
        keys(j,{"original_id","status","namespace_id","coverage_id","decision","position","accepted_target"});const auto d=text(j.at("decision"));check(d=="applied"||d=="no_op"||d=="policy","unknown receipt decision");
        committed c{text(j.at("namespace_id")),text(j.at("coverage_id")),d=="applied"?decision::applied:(d=="no_op"?decision::no_op:decision::policy),number(j.at("position")),std::nullopt};
        if(!j.at("accepted_target").is_null())c.accepted_target=read_identity(j.at("accepted_target"));return {text(j.at("original_id")),c};
    }
    if(status=="not_committed"){keys(j,{"original_id","status","namespace_id","coverage_id"});return {text(j.at("original_id")),not_committed{text(j.at("namespace_id")),text(j.at("coverage_id"))}};}
    keys(j,{"original_id","status","reason"});check(status=="unknown","unknown receipt status");const auto reason=text(j.at("reason"));
    for(auto r:{unknown_reason::legacy,unknown_reason::missing_coverage,unknown_reason::retired_coverage,unknown_reason::source_changed,unknown_reason::unproved_provenance})if(reason==spelling(r))return {text(j.at("original_id")),unknown{r}};
    throw protocol_error("unknown receipt reason");
}
template<class Page> json page_json(const Page& p){json items=json::array();for(const auto& i:p.items)items.push_back(item_json(i));return {{"manifest_digest",p.manifest_digest},{"index",decimal(p.index)},{"count",decimal(p.count)},{"bytes",decimal(p.bytes)},{"digest",p.digest},{"items",items}};}
template<class Page> Page read_page(const json& j,const limits& b){
    keys(j,{"manifest_digest","index","count","bytes","digest","items"});const auto& items=j.at("items");check(items.is_array()&&!items.empty()&&items.size()<=b.maximum.items_per_page,"invalid page items");
    Page p;p.manifest_digest=text(j.at("manifest_digest"));p.index=number(j.at("index"));p.count=number(j.at("count"));p.bytes=number(j.at("bytes"));p.digest=text(j.at("digest"));
    for(const auto& x:items){if constexpr(std::is_same_v<Page,content_page>)p.items.push_back(read_content(x));else p.items.push_back(read_receipt(x));}return p;
}
void frame_valid(const frame& f,const limits& b){
    budgets(b);valid(f.logical,b);check(f.route_generation>0&&f.route_generation<=maximum,"invalid route generation spelling");
    std::visit([&](const auto& v){using T=std::decay_t<decltype(v)>;
        if constexpr(std::is_same_v<T,request>)request_valid(f.logical,v,b);
        else if constexpr(std::is_same_v<T,manifest>)manifest_valid(v,b);
        else if constexpr(std::is_same_v<T,end>)digest(v.manifest_digest);
        else page_valid(v,b);
    },f.body);
}
json frame_json(const frame& f){
    std::string kind;json body;
    std::visit([&](const auto& v){using T=std::decay_t<decltype(v)>;
        if constexpr(std::is_same_v<T,request>){kind="request";body=request_json(v);}
        else if constexpr(std::is_same_v<T,manifest>){kind="manifest";body=manifest_json(v);}
        else if constexpr(std::is_same_v<T,end>){kind="end";body={{"manifest_digest",v.manifest_digest}};}
        else {kind=std::is_same_v<T,content_page>?"content_page":"receipt_page";body=page_json(v);}
    },f.body);
    return {{"latticeCanonicalRange",{{"version",2},{"attempt",attempt_json(f.logical)},{"route_generation",decimal(f.route_generation)},{"kind",kind},{"body",body}}}};
}
std::string dump(const json& j,const limits& b,uint64_t cap){
    std::string raw;try{raw=j.dump();}catch(const json::exception&){throw protocol_error("invalid canonical JSON encoding");}
    (void)parse(raw,b,cap);return raw;
}
limits narrowed(const limits& b,const wire_limits& w){within(w,b.maximum);auto result=b;result.maximum=w;result.string_bytes=std::min<uint64_t>(b.string_bytes,w.frame_bytes);return result;}
void receipt_binding(const receipt_item& item,const receipt_request& asked,uint64_t head){
    check(item.original_id==asked.original_id,"receipt does not cover exact requested ID");
    std::visit([&](const auto& v){using T=std::decay_t<decltype(v)>;
        if constexpr(!std::is_same_v<T,unknown>){
            check(asked.namespace_id&&*asked.namespace_id==v.namespace_id,"receipt namespace is not negotiated request namespace");
            if constexpr(std::is_same_v<T,committed>){check(v.position<=head,"receipt accepted after captured head");if(v.accepted_target)check(std::find(asked.targets.begin(),asked.targets.end(),*v.accepted_target)!=asked.targets.end(),"stored receipt target differs from request");}
        }
    },item.value);
}
std::string rebase_hash(const request& r){hash_writer h("rebase");h.d(r.request_digest);const auto ids=rebase(r);h.u(ids.size());for(const auto& i:ids)write(h,i);return h.finish();}
void bound_offer(const attempt& a,const request& r,const manifest& m,const limits& b){
    request_valid(a,r,b);const auto narrow=narrowed(b,r.budget);manifest_valid(m,narrow);
    check(m.request_digest==r.request_digest&&m.source==r.source&&m.selection==r.selection&&m.base==r.base,"manifest request binding differs");
    if(r.expected.binding==std::optional<source_binding>{r.source}&&r.expected.base.value)check(m.head>=*r.expected.base.value,"same-source full head regresses installed frontier");
    const auto ids=rebase(r);uint64_t bytes=0;for(const auto& i:ids)bytes=add(bytes,identity_bytes(i));
    check(m.counts.receipts==r.receipts.size()&&m.counts.rebase_identities==ids.size()&&m.counts.rebase_bytes==bytes&&m.rebase_digest==rebase_hash(r),"receipt or rebase manifest coverage differs");
}
void progress(uint64_t pages,uint64_t rows,uint64_t bytes,uint64_t all_pages,uint64_t all_rows,uint64_t all_bytes,const wire_limits& w){
    check(pages<=all_pages&&rows<=all_rows&&bytes<=all_bytes,"sequence exceeds manifest");
    stream_counts(pages,rows,bytes,all_pages,all_rows,all_bytes,w);
    const auto left_pages=all_pages-pages,left_rows=all_rows-rows,left_bytes=all_bytes-bytes;
    stream_counts(left_pages,left_rows,left_bytes,all_pages,all_rows,all_bytes,w);
}
void state_valid(const sequence_state& s,const limits& b){
    bound_offer(s.logical,s.frozen_request,s.offer,b);
    // Restart DTOs must still be admissible as bounded frozen wire objects.
    // The minimal route spelling is not admission of any actual route.
    (void)encode(frame{s.logical,1,s.frozen_request},b);
    (void)encode(frame{s.logical,1,s.offer},narrowed(b,s.frozen_request.budget));
    check(s.status==phase::receiving||s.status==phase::sequence_complete_unverified,"unknown canonical sequence phase");
    const auto& t=s.offer.counts;const auto& w=s.frozen_request.budget;
    check(s.identities==add(s.present_count,s.tombstone_count)&&s.present_count<=t.present&&s.tombstone_count<=t.tombstones,"sequence tag totals differ");
    progress(s.next_content_page,s.identities,s.content_bytes,t.content_pages,t.identities,t.content_bytes,w);
    progress(s.next_receipt_page,s.receipt_count,s.receipt_bytes,t.receipt_pages,t.receipts,t.receipt_bytes,w);
    check(s.next_receipt_page==0||s.next_content_page==t.content_pages,"receipt sequence precedes content");
    check(s.last_identity.has_value()==(s.identities!=0),"sequence last identity missing or unexpected");if(s.last_identity)valid(*s.last_identity,b);
    const auto ids=rebase(s.frozen_request);check(s.rebase_seen.size()==ids.size(),"invalid rebase bitmap size");uint64_t seen=0;
    for(size_t i=0;i<ids.size();++i){check(s.rebase_seen[i]<=1,"invalid rebase bitmap flag");const bool passed=s.last_identity&&!less(*s.last_identity,ids[i]);check(static_cast<bool>(s.rebase_seen[i])==passed,"missing or impossible rebase identity");seen+=s.rebase_seen[i];}
    check(seen<=s.identities,"rebase coverage exceeds content");
    if(s.status==phase::sequence_complete_unverified)check(s.next_content_page==t.content_pages&&s.next_receipt_page==t.receipt_pages&&seen==ids.size(),"incomplete canonical terminal sequence");
}
json state_json(const sequence_state& s){
    std::string bitmap;bitmap.reserve(s.rebase_seen.size());for(auto c:s.rebase_seen)bitmap.push_back(c?'1':'0');
    return {{"latticeCanonicalRangeState",{{"version",2},{"attempt",attempt_json(s.logical)},{"request",request_json(s.frozen_request)},{"manifest",manifest_json(s.offer)},
        {"phase",s.status==phase::receiving?"receiving":"sequence_complete_unverified"},{"next_content_page",decimal(s.next_content_page)},{"next_receipt_page",decimal(s.next_receipt_page)},
        {"identities",decimal(s.identities)},{"present",decimal(s.present_count)},{"tombstones",decimal(s.tombstone_count)},{"content_bytes",decimal(s.content_bytes)},
        {"receipts",decimal(s.receipt_count)},{"receipt_bytes",decimal(s.receipt_bytes)},{"last_identity",s.last_identity?identity_json(*s.last_identity):json(nullptr)},{"rebase_seen",bitmap}}}};
}
} // namespace

std::string request_sha256(const attempt& a,const request& r,const limits& b){request_shape(a,r,b);return request_hash(a,r);}
std::string manifest_sha256(const manifest& m,const limits& b){manifest_shape(m,b);return manifest_hash(m);}
std::string rebase_sha256(const attempt& a,const request& r,const limits& b){request_valid(a,r,b);return rebase_hash(r);}
uint64_t content_record_bytes(const content_item& x,const limits& b){budgets(b);content_shape(x,b);return content_size(x);}
uint64_t receipt_record_bytes(const receipt_item& x,const limits& b){budgets(b);receipt_shape(x,b);return receipt_size(x);}
std::string page_sha256(const content_page& p,const limits& b){page_shape(p,b);return page_hash(p);}
std::string page_sha256(const receipt_page& p,const limits& b){page_shape(p,b);return page_hash(p);}
struct stream_hasher::state {
    manifest offer;
    limits budget;
    stream_kind kind;
    hash_writer hash;
    uint64_t count=0, bytes=0, present_count=0;
    std::optional<identity> last_identity;
    std::optional<std::string> last_original;
    bool finished=false;
    state(const manifest& m,stream_kind k,const limits& b)
        : offer(m),budget(b),kind(k),hash(k==stream_kind::content?"content":"receipts") {
        check(k==stream_kind::content||k==stream_kind::receipts,"invalid canonical stream kind");
        const uint64_t declared=k==stream_kind::content?m.counts.content_bytes:m.counts.receipt_bytes;
        // Prefix: S(domain), H(anchor), U(record count). Reserve the exact
        // prefix as well as record bytes in SHA's 64-bit bit-length bound.
        const auto label=k==stream_kind::content?"content":"receipts";
        const uint64_t prefix=8+std::string_view("lattice.canonical-range.v2/").size()+std::string_view(label).size()+32+8;
        check(declared<=std::numeric_limits<uint64_t>::max()/8-prefix,"canonical SHA length overflow");
        hash.d(anchor(m));hash.u(k==stream_kind::content?m.counts.identities:m.counts.receipts);
    }
};
stream_hasher::stream_hasher(const manifest& m,stream_kind k,const limits& b) {
    manifest_shape(m,b);state_=std::make_unique<state>(m,k,b);
}
stream_hasher::~stream_hasher()=default;
stream_hasher::stream_hasher(stream_hasher&&) noexcept=default;
stream_hasher& stream_hasher::operator=(stream_hasher&&) noexcept=default;
void stream_hasher::append(const content_item& item) {
    check(state_&&!state_->finished&&state_->kind==stream_kind::content,"canonical content hasher is not active");
    auto& s=*state_;content_shape(item,s.budget);
    check(!s.last_identity||less(*s.last_identity,item.key),"whole content duplicate or unordered identity");
    const auto next_bytes=add(s.bytes,content_size(item));
    check(s.count<s.offer.counts.identities&&next_bytes<=s.offer.counts.content_bytes,"whole content exceeds manifest");
    write(s.hash,item);++s.count;s.bytes=next_bytes;s.present_count+=std::holds_alternative<present>(item.value);s.last_identity=item.key;
}
void stream_hasher::append(const receipt_item& item) {
    check(state_&&!state_->finished&&state_->kind==stream_kind::receipts,"canonical receipt hasher is not active");
    auto& s=*state_;receipt_shape(item,s.budget);
    check(!s.last_original||bytes_less(*s.last_original,item.original_id),"whole receipt duplicate or unordered identity");
    if(const auto* c=std::get_if<committed>(&item.value))check(c->position<=s.offer.head,"receipt accepted after captured head");
    const auto next_bytes=add(s.bytes,receipt_size(item));
    check(s.count<s.offer.counts.receipts&&next_bytes<=s.offer.counts.receipt_bytes,"whole receipts exceed manifest");
    write(s.hash,item);++s.count;s.bytes=next_bytes;s.last_original=item.original_id;
}
std::string stream_hasher::finish() {
    check(state_&&!state_->finished,"canonical hasher already finished");auto& s=*state_;
    if(s.kind==stream_kind::content)
        check(s.count==s.offer.counts.identities&&s.bytes==s.offer.counts.content_bytes&&s.present_count==s.offer.counts.present&&s.count-s.present_count==s.offer.counts.tombstones,"whole content totals mismatch");
    else check(s.count==s.offer.counts.receipts&&s.bytes==s.offer.counts.receipt_bytes,"whole receipt totals mismatch");
    s.finished=true;return s.hash.finish();
}
std::string content_sha256(const manifest& m,const std::vector<content_item>& rows,const limits& b) {
    stream_hasher hash(m,stream_kind::content,b);check(rows.size()==m.counts.identities,"whole content count mismatch");
    for(const auto& row:rows)hash.append(row);return hash.finish();
}
std::string receipts_sha256(const manifest& m,const std::vector<receipt_item>& rows,const limits& b) {
    stream_hasher hash(m,stream_kind::receipts,b);check(rows.size()==m.counts.receipts,"whole receipt count mismatch");
    for(const auto& row:rows)hash.append(row);return hash.finish();
}
frame decode(std::string_view raw,const limits& b){
    const auto root=parse(raw,b,b.maximum.frame_bytes);keys(root,{"latticeCanonicalRange"});const auto& j=root.at("latticeCanonicalRange");keys(j,{"version","attempt","route_generation","kind","body"});version(j.at("version"));
    frame f;f.logical=read_attempt(j.at("attempt"));f.route_generation=number(j.at("route_generation"));const auto kind=text(j.at("kind"));const auto& body=j.at("body");
    if(kind=="request")f.body=read_request(body,b);
    else if(kind=="manifest")f.body=read_manifest(body);
    else if(kind=="content_page")f.body=read_page<content_page>(body,b);
    else if(kind=="receipt_page")f.body=read_page<receipt_page>(body,b);
    else if(kind=="end"){keys(body,{"manifest_digest"});f.body=end{text(body.at("manifest_digest"))};}
    else throw protocol_error("unknown canonical frame kind");
    frame_valid(f,b);if(const auto* r=std::get_if<request>(&f.body))check(raw.size()<=r->budget.frame_bytes,"request raw frame exceeds advertised budget");return f;
}
std::string encode(const frame& f,const limits& b){
    frame_valid(f,b);const auto* r=std::get_if<request>(&f.body);return dump(frame_json(f),b,r?r->budget.frame_bytes:b.maximum.frame_bytes);
}
sequence_state begin(const attempt& a,const request& r,const manifest& m,const limits& b){
    bound_offer(a,r,m,b);
    // Direct DTO callers receive the same raw/escaped/frame admission as decode.
    (void)encode(frame{a,1,r},b);(void)encode(frame{a,1,m},narrowed(b,r.budget));
    sequence_state s;s.logical=a;s.frozen_request=r;s.offer=m;s.rebase_seen.resize(rebase(r).size());
    (void)encode_state(s,b);return s;
}
sequence_state propose(const sequence_state& current,const frame& f,const limits& b){
    state_valid(current,b);const auto effective=narrowed(b,current.frozen_request.budget);(void)encode(f,effective);
    check(f.logical==current.logical,"frame logical attempt differs");check(current.status==phase::receiving,"canonical sequence already ended");
    auto next=current;const auto ids=rebase(current.frozen_request);const auto& m=current.offer;
    if(const auto* p=std::get_if<content_page>(&f.body)){
        check(p->manifest_digest==m.manifest_digest&&current.next_receipt_page==0&&p->index==current.next_content_page&&p->index<m.counts.content_pages,"duplicate or out-of-order content page");
        check(!current.last_identity||less(*current.last_identity,p->items.front().key),"content identity repeats across pages");
        check(p->count<=m.counts.identities-current.identities&&p->bytes<=m.counts.content_bytes-current.content_bytes,"content page exceeds remaining manifest");
        for(const auto& x:p->items){auto it=std::lower_bound(ids.begin(),ids.end(),x.key,less);const bool target=it!=ids.end()&&*it==x.key;
            if(m.selection==mode::delta&&m.base==std::optional<uint64_t>{m.head})check(target,"same-head refresh contains identity outside request union");
            if(target)next.rebase_seen[static_cast<size_t>(it-ids.begin())]=1;
            if(std::holds_alternative<present>(x.value))++next.present_count;
            else {check(m.selection==mode::delta||target,"full snapshot tombstone is not a requested refresh");++next.tombstone_count;}}
        ++next.next_content_page;next.identities+=p->count;next.content_bytes+=p->bytes;next.last_identity=p->items.back().key;
    }else if(const auto* p=std::get_if<receipt_page>(&f.body)){
        check(p->manifest_digest==m.manifest_digest&&current.next_content_page==m.counts.content_pages&&p->index==current.next_receipt_page&&p->index<m.counts.receipt_pages,"duplicate or out-of-order receipt page");
        check(p->count<=m.counts.receipts-current.receipt_count&&p->bytes<=m.counts.receipt_bytes-current.receipt_bytes,"receipt page exceeds remaining manifest");
        for(size_t i=0;i<p->items.size();++i)receipt_binding(p->items[i],current.frozen_request.receipts.at(static_cast<size_t>(current.receipt_count)+i),m.head);
        ++next.next_receipt_page;next.receipt_count+=p->count;next.receipt_bytes+=p->bytes;
    }else if(const auto* e=std::get_if<end>(&f.body)){
        check(e->manifest_digest==m.manifest_digest,"terminal manifest differs");next.status=phase::sequence_complete_unverified;
    }else throw protocol_error("request or manifest cannot replace active sequence");
    (void)encode_state(next,b);return next;
}
std::string encode_state(const sequence_state& s,const limits& b){state_valid(s,b);return dump(state_json(s),b,b.restart_bytes);}
sequence_state decode_state(std::string_view bytes,const attempt& expected,const limits& b){
    valid(expected,b);const auto root=parse(bytes,b,b.restart_bytes);keys(root,{"latticeCanonicalRangeState"});const auto& j=root.at("latticeCanonicalRangeState");
    keys(j,{"version","attempt","request","manifest","phase","next_content_page","next_receipt_page","identities","present","tombstones","content_bytes","receipts","receipt_bytes","last_identity","rebase_seen"});version(j.at("version"));
    sequence_state s;s.logical=read_attempt(j.at("attempt"));check(s.logical==expected,"restart logical attempt differs");s.frozen_request=read_request(j.at("request"),b);s.offer=read_manifest(j.at("manifest"));const auto status=text(j.at("phase"));check(status=="receiving"||status=="sequence_complete_unverified","unknown canonical sequence phase");s.status=status=="receiving"?phase::receiving:phase::sequence_complete_unverified;
    s.next_content_page=number(j.at("next_content_page"));s.next_receipt_page=number(j.at("next_receipt_page"));s.identities=number(j.at("identities"));s.present_count=number(j.at("present"));s.tombstone_count=number(j.at("tombstones"));s.content_bytes=number(j.at("content_bytes"));s.receipt_count=number(j.at("receipts"));s.receipt_bytes=number(j.at("receipt_bytes"));if(!j.at("last_identity").is_null())s.last_identity=read_identity(j.at("last_identity"));
    const auto bitmap=text(j.at("rebase_seen"));check(bitmap.size()<=b.request_targets,"restart bitmap exceeds budget");for(char c:bitmap){check(c=='0'||c=='1',"invalid restart bitmap");s.rebase_seen.push_back(c-'0');}
    state_valid(s,b);return s;
}
namespace {
// Count the codec's actual compact JSON representation without first dumping
// an escaped payload. DTOs produce only bounded, shallow JSON shapes here.
struct package_json_size { uint64_t bytes=0, nodes=0, depth=0; };
uint64_t package_string_bytes(const std::string& text) {
    uint64_t n=2;
    for(unsigned char c:text) {
        const uint64_t width=(c=='"'||c=='\\'||c=='\b'||c=='\f'||c=='\n'||c=='\r'||c=='\t')?2:(c<32?6:1);
        n=add(n,width);
    }
    return n;
}
package_json_size package_size(const json& value) {
    package_json_size result;result.nodes=1;
    if(value.is_string())result.bytes=package_string_bytes(value.get_ref<const std::string&>());
    else if(value.is_null())result.bytes=4;
    else if(value.is_boolean())result.bytes=value.get<bool>()?4:5;
    else if(value.is_number_integer()||value.is_number_unsigned())result.bytes=value.dump().size();
    else {
        check(value.is_structured(),"unsupported package JSON scalar");
        result.bytes=2;result.depth=1;bool first=true;
        for(auto it=value.begin();it!=value.end();++it) {
            if(!first)result.bytes=add(result.bytes,1);first=false;
            if(value.is_object()) {
                result.bytes=add(result.bytes,add(package_string_bytes(it.key()),1));
                result.nodes=add(result.nodes,1);
            }
            const auto child=package_size(it.value());
            result.bytes=add(result.bytes,child.bytes);result.nodes=add(result.nodes,child.nodes);
            result.depth=std::max(result.depth,add(child.depth,1));
        }
    }
    return result;
}
struct package_slice { size_t first=0,count=0;uint64_t logical_bytes=0; };
template<class Page,class Item>
std::vector<package_slice> package_partition(const attempt& a,uint64_t route,
    const std::vector<Item>& items,const limits& b,uint64_t maximum_pages) {
    Page envelope;envelope.manifest_digest=envelope.digest=std::string(64,'0');
    // The longest valid decimal spelling reserves overhead independent of the
    // final page/whole digests. Digests have fixed-width lowercase hex spelling.
    envelope.index=envelope.count=envelope.bytes=maximum;
    const auto overhead=package_size(frame_json({a,route,envelope}));
    std::vector<package_slice> result;
    if(items.empty())return result;
    check(overhead.bytes<b.maximum.frame_bytes&&overhead.nodes<b.nodes,
          "package page envelope exceeds budget");
    package_slice current;uint64_t bytes=overhead.bytes,nodes=overhead.nodes;
    for(size_t i=0;i<items.size();++i) {
        const auto item_size=package_size(item_json(items[i]));
        // root -> latticeCanonicalRange -> body -> items -> record.
        check(add(4,item_size.depth)<=b.depth,"package item depth exceeds budget");
        check(item_size.bytes<=b.maximum.frame_bytes-overhead.bytes&&item_size.nodes<=b.nodes-overhead.nodes,
              "package single item exceeds frame budget");
        const auto comma=current.count?1u:0u;
        if(current.count&&(current.count==b.maximum.items_per_page||
            add(item_size.bytes,comma)>b.maximum.frame_bytes-bytes||item_size.nodes>b.nodes-nodes)) {
            check(result.size()<maximum_pages,"package page count exceeds budget");
            result.push_back(current);current={i,0,0};bytes=overhead.bytes;nodes=overhead.nodes;
        }
        if(current.count)bytes=add(bytes,1);
        bytes=add(bytes,item_size.bytes);nodes=add(nodes,item_size.nodes);++current.count;
        if constexpr(std::is_same_v<Item,content_item>)current.logical_bytes=add(current.logical_bytes,content_size(items[i]));
        else current.logical_bytes=add(current.logical_bytes,receipt_size(items[i]));
    }
    if(current.count) {
        check(result.size()<maximum_pages,"package page count exceeds budget");
        result.push_back(current);
    }
    return result;
}
template<class Page,class Item>
Page package_page(const std::vector<Item>& items,const package_slice& slice,
                  uint64_t index,const std::string& manifest_digest,const limits& b) {
    Page result;result.manifest_digest=manifest_digest;result.index=index;
    result.count=slice.count;result.bytes=slice.logical_bytes;
    result.items.assign(items.begin()+slice.first,items.begin()+slice.first+slice.count);
    result.digest=page_sha256(result,b);return result;
}
} // namespace

encoded_package assemble_package(const attempt& a,uint64_t route,const request& r,
    uint64_t head,const lease& protection,const std::vector<content_item>& rows,
    const std::vector<receipt_item>& receipts,const package_limits& policy) {
    check(policy.retained_wire_bytes>0&&policy.retained_wire_bytes<=512u*1024u*1024u&&
          policy.frames>=2&&policy.frames<=65536,"invalid package output policy");
    const auto& local=policy.codec;
    request_valid(a,r,local);
    // Also validate route spelling and the complete incoming request's actual
    // wire/parser shape. Do not change its digest or narrow/rewrite frozen Q.
    (void)encode({a,route,r},local);
    const auto b=narrowed(local,r.budget);
    check(rows.size()<=b.maximum.content_identities&&receipts.size()==r.receipts.size(),
          "package input counts exceed request");
    check(add(rows.size(),receipts.size())<=policy.retained_wire_bytes,
          "package input count cannot fit retained output");
    encoded_package result;auto& m=result.offer_;
    m.request_digest=r.request_digest;m.source=r.source;m.selection=r.selection;m.base=r.base;
    m.head=head;m.protection=protection;
    selection(m.selection,m.base,m.head);name(protection.id,b);
    check(protection.duration_ms>0&&protection.duration_ms<=b.lease_ms,"invalid package lease spelling");
    m.content_digest=m.receipt_digest=m.rebase_digest=std::string(64,'0');
    m.counts.identities=rows.size();m.counts.receipts=receipts.size();
    const identity* previous=nullptr;
    for(const auto& row:rows) {
        content_shape(row,b);check(!previous||less(*previous,row.key),"package content duplicate or unordered identity");previous=&row.key;
        m.counts.content_bytes=add(m.counts.content_bytes,content_size(row));
        check(m.counts.content_bytes<=b.maximum.content_bytes,"package content bytes exceeded");
        if(std::holds_alternative<present>(row.value))++m.counts.present;else ++m.counts.tombstones;
    }
    for(size_t i=0;i<receipts.size();++i) {
        receipt_shape(receipts[i],b);receipt_binding(receipts[i],r.receipts[i],head);
        m.counts.receipt_bytes=add(m.counts.receipt_bytes,receipt_size(receipts[i]));
        check(m.counts.receipt_bytes<=b.maximum.receipt_bytes,"package receipt bytes exceeded");
    }
    const auto targets=rebase(r);m.counts.rebase_identities=targets.size();
    for(const auto& target:targets) {
        m.counts.rebase_bytes=add(m.counts.rebase_bytes,identity_bytes(target));
        const auto found=std::lower_bound(rows.begin(),rows.end(),target,
            [](const content_item& row,const identity& key){return less(row.key,key);});
        check(found!=rows.end()&&found->key==target,"package missing receipt-rebase target");
    }
    const auto content_pages=package_partition<content_page>(a,route,rows,b,
        std::min<uint64_t>(b.maximum.content_pages,policy.frames-2));
    const auto receipt_pages=package_partition<receipt_page>(a,route,receipts,b,
        std::min<uint64_t>(b.maximum.receipt_pages,policy.frames-2));
    m.counts.content_pages=content_pages.size();m.counts.receipt_pages=receipt_pages.size();
    check(add(add(content_pages.size(),receipt_pages.size()),2)<=policy.frames,"package total frames exceeded");
    m.rebase_digest=rebase_sha256(a,r,b);
    m.content_digest=content_sha256(m,rows,b);m.receipt_digest=receipts_sha256(m,receipts,b);
    m.manifest_digest=manifest_sha256(m,b);
    auto sequence=begin(a,r,m,local);
    const auto retain=[&](frame value) {
        // Check exact wire length BEFORE allocating the retained encoding.
        // At most one page DTO/JSON tree and codec temporaries exist beside
        // input vectors and retained output. Container/allocator costs excluded.
        const auto predicted=package_size(frame_json(value));
        check(predicted.bytes<=b.maximum.frame_bytes&&predicted.bytes<=policy.retained_wire_bytes-result.bytes_,
              "package retained wire bytes exceeded");
        check(result.frames_.size()<policy.frames,"package retained frame count exceeded");
        auto encoded=encode(value,b);
        check(encoded.size()==predicted.bytes,"package JSON size preflight disagrees with encoder");
        result.bytes_+=encoded.size();result.frames_.push_back(std::move(encoded));
    };
    retain({a,route,m});
    for(size_t i=0;i<content_pages.size();++i) {
        frame value{a,route,package_page<content_page>(rows,content_pages[i],i,m.manifest_digest,b)};
        sequence=propose(sequence,value,local);retain(std::move(value));
    }
    for(size_t i=0;i<receipt_pages.size();++i) {
        frame value{a,route,package_page<receipt_page>(receipts,receipt_pages[i],i,m.manifest_digest,b)};
        sequence=propose(sequence,value,local);retain(std::move(value));
    }
    frame terminal{a,route,end{m.manifest_digest}};
    sequence=propose(sequence,terminal,local);retain(std::move(terminal));
    check(sequence.status==phase::sequence_complete_unverified,"package sequence incomplete");
    return result;
}
} // namespace lattice::detail::canonical_range
