#include "recovery_receiver_source.hpp"
#include "sync_callback_lifetime.hpp"
#include <lattice/lattice.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <charconv>
#include <chrono>
#include <limits>
#include <set>
#include <string_view>

namespace lattice::detail {
namespace {
using json=nlohmann::json;
constexpr size_t max_bytes=65536;
[[noreturn]] void reject(const char* message){throw db_error(message);}
json bounded(const std::string& raw) {
    if(raw.empty()||raw.size()>max_bytes)reject("receiver source JSON byte bound");
    size_t nodes=0;std::vector<std::set<std::string>> keys;
    auto value=json::parse(raw,[&](int depth,json::parse_event_t event,json& v) {
        if(depth<0||depth>12||++nodes>8192)reject("receiver source JSON structural bound");
        if(v.is_string()&&v.get_ref<const std::string&>().size()>8192)reject("receiver source JSON scalar bound");
        if(event==json::parse_event_t::object_start)keys.emplace_back();
        if(event==json::parse_event_t::key&& (keys.empty()||!keys.back().insert(v.get<std::string>()).second))reject("receiver source duplicate JSON member");
        if(event==json::parse_event_t::object_end)keys.pop_back();return true;
    });
    if(!value.is_object())reject("receiver source object required");return value;
}
void shape(const json& j,std::initializer_list<const char*> names) {
    if(!j.is_object()||j.size()!=names.size())reject("receiver source object shape");
    for(const auto* key:names)if(!j.contains(key))reject("receiver source missing member");
}
std::string text(const json& j,const char* key,size_t cap=256) {
    const auto& v=j.at(key);if(!v.is_string())reject("receiver source text required");
    const auto& s=v.get_ref<const std::string&>();if(s.empty()||s.size()>cap||s.find('\0')!=std::string::npos)reject("receiver source text bound");return s;
}
int64_t number(const json& j,const char* key,int64_t hi=INT64_MAX,int64_t lo=1) {
    const auto& v=j.at(key);if(!v.is_number_integer()||(v.is_number_unsigned()&&v.get<uint64_t>()>uint64_t(hi)))reject("receiver source integer required");
    const auto n=v.get<int64_t>();if(n<lo||n>hi)reject("receiver source integer bound");return n;
}
int64_t decimal(const json& j,const char* key) {
    const auto raw=text(j,key,19);int64_t n=0;const auto parsed=std::from_chars(raw.data(),raw.data()+raw.size(),n);
    if(parsed.ec!=std::errc{}||parsed.ptr!=raw.data()+raw.size()||n<=0||std::to_string(n)!=raw)reject("receiver source normalized positive decimal required");return n;
}
void uuid(const json& j,const char* key) {
    const auto s=text(j,key,36);if(s.size()!=36)reject("receiver source UUID required");
    for(size_t i=0;i<s.size();++i)if(i==8||i==13||i==18||i==23 ? s[i]!='-' : !(s[i]>='0'&&s[i]<='9'||s[i]>='a'&&s[i]<='f'))reject("receiver source normalized UUID required");
}
void digest(const json& j,const char* key) {
    const auto s=text(j,key,64);if(s.size()!=64||!std::all_of(s.begin(),s.end(),[](char c){return c>='0'&&c<='9'||c>='a'&&c<='f';}))reject("receiver source digest required");
}
void scope(const json& j) {
    shape(j,{"models","relations","scopedLinkTables","catalogDigest"});digest(j,"catalogDigest");
    std::set<std::string> tables;
    const auto check=[&](const char* key,bool relation){const auto& list=j.at(key);
        if(!list.is_array()||list.size()>256)reject("receiver source catalog bound");
        for(const auto& item:list){if(relation)shape(item,{"table","lhsModel","rhsModel","incomingOperations"});else shape(item,{"table","incomingOperations"});
            if(!tables.insert(text(item,"table",64)).second)reject("receiver source duplicate table");
            if(relation){(void)text(item,"lhsModel",64);(void)text(item,"rhsModel",64);}
            const auto& operations=item.at("incomingOperations");if(!operations.is_array()||operations.size()>3)reject("receiver source operation bound");
            std::set<std::string> seen;for(const auto& op:operations){if(!op.is_string())reject("receiver source operation required");const auto s=op.get<std::string>();
                if((s!="INSERT"&&s!="UPDATE"&&s!="DELETE")||!seen.insert(s).second)reject("receiver source operation mask");}
        }};
    check("models",false);check("relations",true);if(j.at("models").empty())reject("receiver source model scope required");
    const auto& links=j.at("scopedLinkTables");if(!links.is_array()||links.size()>256)reject("receiver source link bound");
    std::set<std::string> seen;for(const auto& link:links)if(!link.is_string()||link.get_ref<const std::string&>().size()>64||!tables.count(link.get<std::string>())||!seen.insert(link.get<std::string>()).second)reject("receiver source link scope");
}
void profile(const json& j) {
    shape(j,{"name","wire","requestEntries","requestTargets","requestTargetBytes","parserDepth","parserNodes","scalarBytes","restartBytes","valueLimits","leaseMilliseconds","packageBytes","frames","transfers","bindings","durableBytes","transferBytes","captureRows","captureBytes","requestBytes","pendingRequests","pendingInputAndReplyBytes","pendingWorkspaceBytes"});
    (void)text(j,"name",64);
    const auto& wire=j.at("wire");shape(wire,{"frame_bytes","payload_bytes","items_per_page","content_pages","content_identities","content_bytes","receipt_pages","receipts","receipt_bytes"});
    for(auto it=wire.begin();it!=wire.end();++it)(void)decimal(wire,it.key().c_str());
    for(auto it=j.begin();it!=j.end();++it)if(it.key()!="name"&&it.key()!="wire"&&it.key()!="valueLimits")(void)number(j,it.key().c_str());
    const auto& values=j.at("valueLimits");shape(values,{"rawBytes","fields","nameBytes","valueBytes","decodedBytes"});
    for(auto it=values.begin();it!=values.end();++it)(void)number(values,it.key().c_str());
    // Recorded facts only. Remote bounds never allocate a package or override
    // local parser limits; profile negotiation/Q validation is a later gate.
}
// Allocation-bounded envelope discriminator, not a JSON validator. Skip string
// contents/escapes and nested values; decode at most a 256-byte top-level key.
// The real parser still validates every selected control. Ordinary frames keep
// their existing size limits and are not copied/parsed twice here.
bool reserved(std::string_view raw) {
    size_t depth=0;bool key=false,kind_value=false;
    for(size_t i=0;i<raw.size();++i) {
        const char c=raw[i];
        if(c=='"') {
            const auto start=i;bool closed=false;
            for(++i;i<raw.size();++i) {if(raw[i]=='\\'){if(i+1<raw.size())++i;continue;}if(raw[i]=='"'){closed=true;break;}}
            if(!closed)return false;
            if(depth==1&&(key||kind_value)) {
                json value;
                if(i-start+1<=256) {try {value=json::parse(raw.substr(start,i-start+1));}catch(...) {return false;}}
                if(key) {
                    if(value=="latticeCanonicalRange")return true;
                    kind_value=value=="kind";key=false;
                } else {if(value=="recoveryReady")return true;kind_value=false;}
            }
        } else if(c=='{'||c=='[') {if(depth==1)kind_value=false;++depth;if(depth==1){if(c!='{')return false;key=true;}}
        else if(c=='}'||c==']') {if(!depth)return false;--depth;if(!depth)return false;}
        else if(c==','&&depth==1){key=true;kind_value=false;}
        else if(kind_value&&c!=':'&&c!=' '&&c!='\t'&&c!='\r'&&c!='\n')kind_value=false;
    }
    return false;
}
int64_t now() {return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
std::string escape(const std::string& value) {
    constexpr char hex[]="0123456789ABCDEF";std::string result;
    for(unsigned char c:value)if(c>='A'&&c<='Z'||c>='a'&&c<='z'||c>='0'&&c<='9'||c=='-'||c=='_'||c=='.'||c=='~')result+=char(c);else {result+='%';result+=hex[c>>4];result+=hex[c&15];}return result;
}
void endpoint(const std::string& url) {
    if(url.size()>4096||url.rfind("wss://",0)!=0||url.find('#')!=std::string::npos||url.find('@')!=std::string::npos)reject("receiver source requires explicit WSS endpoint");
    // Disallow ambiguous percent-encoded query keys, not values. Native app
    // expectations use the same exact URL; the SDK separately compares actual
    // task host/port/path/query and system TLS, never just this spelling.
    const auto q=url.find('?');if(q==std::string::npos)return;
    for(size_t p=q+1;p<url.size();) {const auto end=url.find('&',p);const auto field=url.substr(p,end==std::string::npos?end:end-p);const auto key=field.substr(0,field.find('='));
        if(key.find('%')!=std::string::npos||key.rfind("recovery-",0)==0||key=="last-event-id")reject("receiver source reserved or encoded query key");
        if(end==std::string::npos)break;p=end+1;}
}
}
bool reserved_recovery_source_frame(std::string_view raw){return reserved(raw);}
struct receiver_source_binding::policy {json expected;std::string url;int64_t duration;};
struct receiver_source_binding::record {
    platform_transport_callbacks endpoint;uint64_t lifecycle=0;int64_t deadline=0;std::string request;
    bool described=false;json response; // immutable exact source/catalog/profile/physical generation
};
receiver_source_binding::receiver_source_binding(const std::shared_ptr<lattice_db>& owner,const std::shared_ptr<sync_callback_lifetime>& lifetime,const std::string& raw,const std::string& url)
    :owner_(owner),lifetime_(lifetime) {
    if(!owner||!lifetime)reject("receiver source requires actual retained owner");
    auto value=bounded(raw);shape(value,{"endpoint","source","incomingScope","peer","channel","validForMilliseconds"});
    endpoint(url);if(text(value,"endpoint",4096)!=url)reject("receiver source endpoint differs from actual owner configuration");
    const auto& source=value.at("source");shape(source,{"authority","sourceID","epoch","scopeDigest","schemaDigest","receiptNamespace","coverageID","coverageRevision","descriptorDigest"});
    (void)text(source,"authority");uuid(source,"sourceID");uuid(source,"epoch");digest(source,"scopeDigest");digest(source,"schemaDigest");digest(source,"descriptorDigest");
    (void)text(source,"receiptNamespace");(void)text(source,"coverageID");(void)number(source,"coverageRevision");scope(value.at("incomingScope"));
    const auto& peer=value.at("peer");shape(peer,{"replicaID","receiverIncarnation","channelIncarnation"});(void)text(peer,"replicaID");uuid(peer,"receiverIncarnation");uuid(peer,"channelIncarnation");(void)text(value,"channel",64);
    const auto duration=number(value,"validForMilliseconds",3600000);
    policy_=std::make_shared<policy>(policy{std::move(value),url,duration});
}
receiver_source_binding::~receiver_source_binding()=default;
std::string receiver_source_binding::dial_url()const {
    const auto& peer=policy_->expected.at("peer");auto url=policy_->url;
    url+=(url.find('?')==std::string::npos?"?":"&");url+="recovery-v=1&recovery-replica="+escape(text(peer,"replicaID"))+"&recovery-receiver="+text(peer,"receiverIncarnation",36)+"&recovery-channel="+text(peer,"channelIncarnation",36);
    if(url.size()>4096)reject("receiver source declared URL bound");return url;
}
bool receiver_source_binding::live(const std::shared_ptr<const record>& r)const {
    const auto owner=owner_.lock();const auto life=lifetime_.lock();return r&&owner&&!owner->is_closed()&&life&&now()<r->deadline&&life->current(r->lifecycle)&&r->endpoint.current_system_tls_for_owner();
}
void receiver_source_binding::invalidate(const std::shared_ptr<const record>& r) {
    std::shared_ptr<const record> retired;
    {std::lock_guard lock(mutex_);if(current_==r)retired.swap(current_);}
    // Last endpoint/provider/capture destruction must stay outside this leaf.
}
void receiver_source_binding::opened(const platform_transport_callbacks& attempt,uint64_t lifecycle,owned_platform_sync_transport& transport) {
    auto next=std::make_shared<record>();next->endpoint=attempt;next->lifecycle=lifecycle;
    const auto start=now();if(start>INT64_MAX-policy_->duration)reject("receiver source deadline overflow");next->deadline=start+policy_->duration;
    next->request=uuid_t::generate().to_string();
    if(!live(next))reject("receiver source requires same actual system-TLS attempt and owner");
    std::shared_ptr<const record> retired;
    {std::lock_guard lock(mutex_);if(!attempt.current_system_tls_for_owner())reject("receiver source attempt replaced before describe");retired.swap(current_);current_=next;}
    const auto wire=json{{"kind","recoveryReady"},{"version",1},{"operation","describe"},{"requestID",next->request}}.dump();
    try {if(!live(next)||!transport.send_to_attempt(attempt,transport_message::from_string(wire)))reject("receiver source describe retired before send");}
    catch(...){invalidate(next);throw;}
}
bool receiver_source_binding::receive(const platform_transport_callbacks& attempt,uint64_t lifecycle,const transport_message& message) {
    if(!attempt.current_system_tls_for_owner())reject("receiver source unverified physical message");
    const std::string_view raw(reinterpret_cast<const char*>(message.data.data()),message.data.size());if(!reserved(raw))return false;
    std::shared_ptr<const record> pending;
    {std::lock_guard lock(mutex_);pending=current_;}
    if(!pending||!pending->endpoint.matches(attempt)||pending->lifecycle!=lifecycle)return true;
    try {
        if(raw.size()>max_bytes)reject("receiver source control byte bound");
        const auto value=bounded(std::string(raw));
        const bool control=value.contains("kind")&&value.at("kind")=="recoveryReady";
        if(!control||pending->described||!live(pending))reject("receiver source unsolicited, repeated or expired recovery frame");
        shape(value,{"kind","version","operation","requestID","routeGeneration","source","incomingScope","peer","channel","profile","upload"});
        if(number(value,"version",1)!=1||value.at("operation")!="describe"||value.at("requestID")!=pending->request)reject("receiver source describe correlation differs");
        (void)decimal(value,"routeGeneration");profile(value.at("profile"));
        const auto& upload=value.at("upload");shape(upload,{"maximumEntries","maximumWireBytes","maximumScalarBytes","parserNodes","parserDepth","maximumDeletes"});
        for(auto it=upload.begin();it!=upload.end();++it)
            if(it.key()=="maximumDeletes")(void)number(upload,"maximumDeletes",256,0);
            else (void)number(upload,it.key().c_str());
        for(const auto* key:{"source","incomingScope","peer","channel"})if(value.at(key)!=policy_->expected.at(key))reject("receiver source differs from explicit application expectation");
        auto accepted=std::make_shared<record>(*pending);accepted->described=true;accepted->response=value;
        if(!live(accepted))reject("receiver source retired during describe validation");
        std::shared_ptr<const record> retired;
        {std::lock_guard lock(mutex_);
            if(current_!=pending) {
                if(current_&&current_->endpoint.matches(attempt)&&current_->lifecycle==lifecycle) {
                    retired.swap(current_);reject("receiver source concurrent repeated describe");
                }
                return true; // a different physical attempt owns its own pending record
            }
            if(!attempt.current_system_tls_for_owner())reject("receiver source replaced before describe publication");
            retired.swap(current_);current_=std::move(accepted);
        }
        return true;
    }catch(...){invalidate(pending);throw;}
}
bool receiver_source_binding::described()const {
    std::shared_ptr<const record> current;{std::lock_guard lock(mutex_);current=current_;}return current&&current->described&&live(current);
}
}
