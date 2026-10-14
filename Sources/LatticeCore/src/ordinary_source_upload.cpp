#include "ordinary_source_upload.hpp"
#include "ordinary_export_storage_access.hpp"
#include "ordinary_v2_property.hpp"
#include "ordinary_v2_identity.hpp"
#include <lattice/ordinary_sync.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <set>

namespace lattice::detail {
namespace {
using json=nlohmann::json;
using caps=ordinary_sync_limits;
[[noreturn]] void fail(const char* reason){throw db_error(std::string("ordinary source upload: ")+reason);}
std::atomic<size_t> uploads{0};
struct reservation {
    reservation(){auto n=uploads.load(std::memory_order_relaxed);do {
        if(n>=caps::simultaneous_receives)fail("intake capacity exhausted");
    }while(!uploads.compare_exchange_weak(n,n+1,std::memory_order_acq_rel));}
    ~reservation(){uploads.fetch_sub(1,std::memory_order_release);}
};
struct budget {
    size_t used=0;
    void add(size_t n){if(n>caps::decoded_receive_bytes-used)fail("retained byte bound");used+=n;}
    void tree(const json& value){add(128);if(value.is_string())add(value.get_ref<const std::string&>().size()+1);
        else if(value.is_object())for(auto it=value.begin();it!=value.end();++it){add(it.key().size()+1);tree(it.value());}
        else if(value.is_array())for(const auto& child:value)tree(child);}
};
struct sax final:nlohmann::json_sax<json> {
    size_t nodes=0,workspace;struct level{bool object;std::set<std::string> keys;};std::vector<level> stack;
    explicit sax(size_t wire_bytes):workspace(wire_bytes*caps::parser_wire_copies){}
    bool node(){if(++nodes>caps::parser_nodes||caps::parser_node_logical_bytes>caps::parser_workspace_bytes-workspace)return false;
        workspace+=caps::parser_node_logical_bytes;return true;}
    bool null()override{return node();}bool boolean(bool)override{return node();}
    bool number_integer(number_integer_t)override{return node();}bool number_unsigned(number_unsigned_t)override{return node();}
    bool number_float(number_float_t n,const string_t&)override{return std::isfinite(n)&&node();}
    bool string(string_t& s)override{return s.size()<=caps::scalar_bytes&&node();}
    bool binary(binary_t&)override{return false;}
    bool start(bool object){if(!node()||stack.size()>=caps::parser_depth)return false;stack.push_back({object,{}});return true;}
    bool start_object(size_t)override{return start(true);}bool start_array(size_t)override{return start(false);}
    bool key(string_t& s)override{return !stack.empty()&&stack.back().object&&s.size()<=256&&node()&&stack.back().keys.insert(s).second;}
    bool end_object()override{if(stack.empty()||!stack.back().object)return false;stack.pop_back();return true;}
    bool end_array()override{if(stack.empty()||stack.back().object)return false;stack.pop_back();return true;}
    bool parse_error(size_t,const std::string&,const nlohmann::detail::exception&)override{return false;}
};

void keys(const json& v,std::initializer_list<const char*> expected){
    if(!v.is_object()||v.size()!=expected.size())fail("unknown or missing fields");
    for(const auto* key:expected)if(!v.contains(key))fail("missing field");
}
const std::string& text(const json& v,size_t limit=256,bool empty=false){
    if(!v.is_string())fail("text required");const auto& s=v.get_ref<const std::string&>();
    if((s.empty()&&!empty)||s.size()>limit||s.find('\0')!=std::string::npos)fail("text bound");return s;
}
int64_t number(const json& v){if(!v.is_number_integer()||(v.is_number_unsigned()&&v.get<uint64_t>()>uint64_t(INT64_MAX)))fail("integer required");return v.get<int64_t>();}
json parse(std::string_view raw){
    if(raw.empty()||raw.size()>caps::frame_bytes)fail("frame bound");
    {sax check(raw.size());if(!json::sax_parse(raw.begin(),raw.end(),&check))fail("invalid, duplicate or over-budget JSON");}
    return json::parse(raw.begin(),raw.end());
}
std::vector<std::string> names(const json& value,size_t count=caps::columns_per_table,size_t length=256,bool sorted=false){
    if(!value.is_array()||value.size()>count)fail("name count bound");std::vector<std::string> out;out.reserve(value.size());
    for(const auto& item:value){const auto& name=text(item,length);
        if(std::find(out.begin(),out.end(),name)!=out.end()||(sorted&&!out.empty()&&out.back()>=name))fail("duplicate or unordered name");out.push_back(name);}return out;
}
std::shared_ptr<const ordinary_upload_original> observation(const json& v,budget& b){
    keys(v,{"version","fields","names"});if(number(v["version"])!=1)fail("observation version");
    b.tree(v);b.add(sizeof(ordinary_upload_original)+4*sizeof(void*));auto out=std::make_shared<ordinary_upload_original>();
    out->fields=names(v["fields"],caps::columns_per_table,256,true);out->names=names(v["names"]);return out;
}
audit_log_entry entry(const json& v,budget& b){
    if(!v.is_object())fail("entry object required");
    for(const auto* key:{"id","globalId","tableName","operation","rowId","globalRowId","changedFields","changedFieldsNames","timestamp","isFromRemote","isSynchronized","uploadOriginal"})if(!v.contains(key))fail("missing entry field");
    if(v.size()!=12+size_t(v.contains("synthesized"))+size_t(v.contains("originalIdentity"))+size_t(v.contains("deliveryVersion")))fail("unknown entry field");
    b.tree(v);audit_log_entry out;out.id=number(v["id"]);out.row_id=number(v["rowId"]);
    out.global_id=text(v["globalId"],4096);out.global_row_id=text(v["globalRowId"],4096);out.table_name=text(v["tableName"]);out.operation=text(v["operation"],16);
    out.timestamp=text(v["timestamp"],caps::scalar_bytes,true);
    if(out.operation!="INSERT"&&out.operation!="UPDATE"&&out.operation!="DELETE")fail("operation");
    for(const auto* key:{"isFromRemote","isSynchronized"})if(!v[key].is_boolean())fail("flag type");
    out.is_from_remote=v["isFromRemote"].get<bool>();out.is_synchronized=v["isSynchronized"].get<bool>();
    if(v.contains("synthesized")){if(!v["synthesized"].is_boolean()||!v["synthesized"].get<bool>())fail("synthesized flag");out.synthesized=true;}
    const auto& fields=v["changedFields"];if(!fields.is_object()||fields.size()>caps::columns_per_table)fail("field count");
    for(auto it=fields.begin();it!=fields.end();++it){if(it.key().empty()||it.key().size()>256||it.key().find('\0')!=std::string::npos)fail("field name");keys(it.value(),{"kind","value"});out.changed_fields.emplace(it.key(),decode_ordinary_v2_property(it.value(),caps::scalar_bytes));}
    out.changed_fields_names=names(v["changedFieldsNames"]);
    if(v.contains("originalIdentity")){
        const auto& identity=v["originalIdentity"];keys(identity,{"version","changedFieldsNames","digest"});if(number(identity["version"])!=1)fail("identity version");
        audit_original_identity id;id.changed_fields_names=names(identity["changedFieldsNames"],32,64);id.digest=text(identity["digest"],64);
        if(id.digest.size()!=64||!std::all_of(id.digest.begin(),id.digest.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}))fail("identity digest");out.original_identity=std::move(id);
    }
    if(v.contains("deliveryVersion")){const auto& version=text(v["deliveryVersion"],64);
        if(!ordinary_delivery_version_valid(version))fail("delivery version");out.ordinary_delivery_version=version;}
    out.upload_original=observation(v["uploadOriginal"],b);return out;
}
json observation_json(const ordinary_upload_original& v){return {{"version",1},{"fields",v.fields},{"names",v.names}};}
}
ordinary_source_upload_batch decode_ordinary_source_upload(std::string_view raw,bool require_versions){
    ordinary_source_upload_batch out;out.reservation=std::make_shared<reservation>();
    const auto root=parse(raw);keys(root,{"latticeOrdinarySync"});const auto& v=root["latticeOrdinarySync"];
    keys(v,{"version","kind","auditLog"});if(number(v["version"])!=2||v["kind"]!="upload")fail("upload protocol");
    const auto& entries=v["auditLog"];if(!entries.is_array()||entries.empty()||entries.size()>caps::records_per_page)fail("upload entry count");
    budget b;b.add(entries.size()*sizeof(audit_log_entry));out.entries.reserve(entries.size());std::set<std::string,ordinary_v2_nocase_less> ids;
    for(const auto& value:entries){auto decoded=entry(value,b);if(require_versions&&!decoded.ordinary_delivery_version)fail("versioned acknowledgment requires delivery version");b.add(decoded.global_id.size()+sizeof(std::string)+4*sizeof(void*));if(!ids.insert(decoded.global_id).second)fail("duplicate AuditLog identity");out.entries.push_back(std::move(decoded));}out.logical_bytes=b.used;return out;
}
std::string encode_ordinary_source_upload(const std::vector<audit_log_entry>& entries){
    if(entries.empty()||entries.size()>caps::records_per_page)fail("upload entry count");
    // A real slot precedes DOM/string construction. Binary sizing validates
    // exact kinds and precharges every retained input before JSON copies.
    auto hold=std::make_shared<reservation>();budget b;json values=json::array();
    for(const auto& item:entries){if(!item.upload_original)fail("missing actual pre-late-binding observation");
        if(item.changed_fields.size()>caps::columns_per_table||item.changed_fields_names.size()>caps::columns_per_table||item.upload_original->fields.size()>caps::columns_per_table||item.upload_original->names.size()>caps::columns_per_table)fail("encoded upload field count");
        b.add(128*(32+item.changed_fields.size()*6+item.changed_fields_names.size()*2+item.upload_original->fields.size()*2+item.upload_original->names.size()*2));
        (void)encode_ordinary_v2_retained_entry(item,[&](size_t n){b.add(n*8);});
        const auto observed=encode_ordinary_upload_observation(*item.upload_original,[&](size_t n){b.add(n*8);});
        auto value=json::parse(item.to_json());value["uploadOriginal"]=observation_json(*item.upload_original);
        if(item.ordinary_delivery_version){if(!ordinary_delivery_version_valid(*item.ordinary_delivery_version))fail("encoded delivery version");value["deliveryVersion"]=*item.ordinary_delivery_version;}values.push_back(std::move(value));}
    auto out=json{{"latticeOrdinarySync",{{"version",2},{"kind","upload"},{"auditLog",std::move(values)}}}}.dump();
    if(out.size()>caps::frame_bytes)fail("encoded frame bound");return out;
}
void capture_ordinary_upload_original(audit_log_entry& entry,std::string_view fields,std::string_view names,
    const ordinary_v2_retained_charge& charge,const ordinary_v2_workspace_acquire& workspace){
    auto values=parse_ordinary_v2_audit_values(fields,names,charge,workspace);
    charge(sizeof(ordinary_upload_original)+4*sizeof(void*)+(values.fields.size()+values.names.size())*sizeof(std::string));
    for(const auto& [name,value]:values.fields)charge(name.size()+1);for(const auto& name:values.names)charge(name.size()+1);
    auto observation=std::make_shared<ordinary_upload_original>();observation->fields.reserve(values.fields.size());
    for(const auto& [name,value]:values.fields)observation->fields.push_back(name);std::sort(observation->fields.begin(),observation->fields.end());observation->names=values.names;
    entry.changed_fields=std::move(values.fields);entry.changed_fields_names=std::move(values.names);entry.upload_original=std::move(observation);
}
std::vector<uint8_t> encode_ordinary_upload_observation(const ordinary_upload_original& v,const ordinary_v2_retained_charge& charge){
    // Reuse the local envelope's bounded name codec; no digest/source fields.
    size_t observed_bytes=sizeof(audit_log_entry)+(v.fields.size()+v.names.size())*(sizeof(std::string)+sizeof(any_property)+4*sizeof(void*));
    for(const auto* sequence:{&v.fields,&v.names})for(const auto& name:*sequence){if(name.size()>256||observed_bytes>caps::decoded_receive_bytes-name.size()-1)fail("observation byte bound");observed_bytes+=name.size()+1;}
    charge(observed_bytes);
    audit_log_entry carrier;carrier.global_id="ordinary-upload-observation-v1";carrier.table_name="observation";carrier.operation="UPDATE";carrier.global_row_id="observation";
    carrier.changed_fields_names=v.names;for(const auto& name:v.fields)carrier.changed_fields.emplace(name,any_property(nullptr));
    if(carrier.changed_fields.size()!=v.fields.size()||!std::is_sorted(v.fields.begin(),v.fields.end()))fail("observation fields");
    return encode_ordinary_v2_retained_entry(carrier,charge);
}
std::shared_ptr<const ordinary_upload_original> decode_ordinary_upload_observation(std::span<const uint8_t> bytes,const ordinary_v2_retained_charge& charge){
    auto carrier=decode_ordinary_v2_retained_entry(bytes,charge);
    if(carrier.id||carrier.row_id||carrier.global_id!="ordinary-upload-observation-v1"||carrier.table_name!="observation"||carrier.operation!="UPDATE"||carrier.global_row_id!="observation"||!carrier.timestamp.empty()||carrier.original_identity||carrier.synthesized||carrier.is_from_remote||carrier.is_synchronized)fail("observation envelope");
    charge(sizeof(ordinary_upload_original)+4*sizeof(void*)+carrier.changed_fields.size()*sizeof(std::string));auto out=std::make_shared<ordinary_upload_original>();out->names=std::move(carrier.changed_fields_names);
    for(const auto& [name,value]:carrier.changed_fields){if(value.kind!=any_property_kind::null_kind)fail("observation field value");charge(name.size()+1);out->fields.push_back(name);}std::sort(out->fields.begin(),out->fields.end());return out;
}
std::vector<database::row_t> read_ordinary_upload_rows(database& db,const std::string& sql,
    const std::vector<column_value_t>& args,size_t limit,const ordinary_v2_retained_charge& charge) {
    auto* h=ordinary_export_storage_access::borrow(db);if(!h||!limit||limit>caps::originals||sqlite3_txn_state(h,"main")==SQLITE_TXN_NONE)fail("bounded upload read view required");
    database::record_statement();sqlite3_stmt* raw=nullptr;const char* tail=nullptr;
    const auto rc=sqlite3_prepare_v2(h,sql.data(),int(sql.size()),&raw,&tail);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,&sqlite3_finalize);
    if(rc!=SQLITE_OK||!raw||!sqlite3_stmt_readonly(raw)||tail!=sql.data()+sql.size()||sqlite3_bind_parameter_count(raw)!=int(args.size()))fail("upload query shape");
    for(size_t i=0;i<args.size();++i){int result=SQLITE_MISUSE;
        if(const auto* v=std::get_if<std::string>(&args[i])){if(v->size()>4096)fail("upload query key bound");result=sqlite3_bind_text(raw,int(i+1),v->data(),int(v->size()),SQLITE_STATIC);}
        else if(const auto* v=std::get_if<int64_t>(&args[i]))result=sqlite3_bind_int64(raw,int(i+1),*v);
        if(result!=SQLITE_OK)fail("upload query bind");}
    const auto count=sqlite3_column_count(raw);if(count<1||count>32)fail("upload query column bound");
    std::vector<database::row_t> rows;int step;
    while((step=sqlite3_step(raw))==SQLITE_ROW){if(rows.size()==limit)fail("upload query row bound");charge(sizeof(database::row_t)+size_t(count)*(sizeof(database::row_t::value_type)+4*sizeof(void*)));
        database::row_t row;
        for(int c=0;c<count;++c){const auto* key=sqlite3_column_name(raw,c);if(!key)fail("upload column name");size_t length=0;while(length<=256&&key[length])++length;if(!length||length>256)fail("upload column name bound");charge(length+1);
            column_value_t value=nullptr;switch(sqlite3_column_type(raw,c)){
                case SQLITE_NULL:break;
                case SQLITE_INTEGER:value=int64_t(sqlite3_column_int64(raw,c));break;
                case SQLITE_FLOAT:{const auto n=sqlite3_column_double(raw,c);if(!std::isfinite(n))fail("upload nonfinite scalar");value=n;break;}
                case SQLITE_TEXT:{const auto n=sqlite3_column_bytes(raw,c);if(n<0||size_t(n)>caps::frame_bytes)fail("upload text bound");charge(size_t(n)+1);const auto* v=sqlite3_column_text(raw,c);if(!v&&n)fail("upload text unavailable");value=std::string(reinterpret_cast<const char*>(v?v:reinterpret_cast<const unsigned char*>("")),size_t(n));break;}
                default:fail("upload AuditLog storage class");}
            if(!row.emplace(std::string(key,length),std::move(value)).second)fail("duplicate upload column");}
        rows.push_back(std::move(row));}
    if(step!=SQLITE_DONE)fail("upload query incomplete");return rows;
}

std::shared_ptr<void> reserve_ordinary_upload_result(){return std::make_shared<reservation>();}
bool ordinary_delivery_version_valid(std::string_view value)noexcept {
    return value=="0"||(value.size()==64&&std::all_of(value.begin(),value.end(),[](char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}));
}
std::vector<ordinary_upload_ack> decode_ordinary_upload_ack(std::string_view raw) {
    auto hold=std::make_shared<reservation>();const auto root=parse(raw);keys(root,{"latticeOrdinarySync"});
    const auto& body=root["latticeOrdinarySync"];keys(body,{"version","kind","accepted"});
    if(number(body["version"])!=2||body["kind"]!="uploadAck")fail("acknowledgment protocol");
    const auto& accepted=body["accepted"];
    if(!accepted.is_array()||accepted.empty()||accepted.size()>caps::records_per_page)fail("acknowledgment count");
    budget b;b.add(accepted.size()*sizeof(ordinary_upload_ack));std::vector<ordinary_upload_ack> out;out.reserve(accepted.size());
    std::set<std::string,ordinary_v2_nocase_less> seen;
    for(const auto& item:accepted){keys(item,{"globalId","deliveryVersion"});const auto& gid=text(item["globalId"],4096);const auto& version=text(item["deliveryVersion"],64);
        if(!ordinary_delivery_version_valid(version))fail("acknowledgment version");b.add(2*gid.size()+version.size()+2*sizeof(std::string)+4*sizeof(void*));
        if(!seen.insert(gid).second)fail("duplicate acknowledgment identity");out.push_back({gid,version});}
    return out;
}
std::string encode_ordinary_upload_ack(const std::vector<audit_log_entry>& input,const std::vector<std::string>& accepted) {
    if(input.size()>caps::records_per_page||accepted.size()>input.size())fail("acknowledgment input count");
    auto hold=std::make_shared<reservation>();budget b;json values=json::array();std::set<std::string,ordinary_v2_nocase_less> seen;
    for(const auto& id:accepted){const auto found=std::find_if(input.begin(),input.end(),[&](const auto& e){return e.global_id==id;});
        if(found==input.end()||!found->ordinary_delivery_version||!ordinary_delivery_version_valid(*found->ordinary_delivery_version))fail("acknowledgment outside exact versioned intake");
        b.add(1024+id.size()*8+found->ordinary_delivery_version->size()*8);
        if(!seen.insert(id).second)fail("duplicate committed acknowledgment identity");
        values.push_back({{"globalId",id},{"deliveryVersion",*found->ordinary_delivery_version}});}
    if(accepted.empty())return {}; // No fabricated positive response after zero acceptance.
    auto out=json{{"latticeOrdinarySync",{{"version",2},{"kind","uploadAck"},{"accepted",std::move(values)}}}}.dump();
    if(out.size()>caps::frame_bytes)fail("acknowledgment frame bound");return out;
}

}

namespace lattice {
struct ordinary_upload_acknowledgment_result::payload final {
    std::shared_ptr<void> reservation;
    std::vector<std::string> ids;
    std::vector<uint8_t> acknowledgment;
    payload(std::shared_ptr<void> hold,std::vector<std::string> accepted,std::vector<uint8_t> bytes)
        :reservation(std::move(hold)),ids(std::move(accepted)),acknowledgment(std::move(bytes)){}
};
ordinary_upload_acknowledgment_result::ordinary_upload_acknowledgment_result(
    std::shared_ptr<void> hold,std::vector<std::string> accepted,std::vector<uint8_t> bytes) {
    if(!hold)throw db_error("ordinary source upload: missing result reservation");
    value_=std::make_shared<const payload>(std::move(hold),std::move(accepted),std::move(bytes));
}
bool ordinary_upload_acknowledgment_result::isValid()const noexcept{return bool(value_);}
const std::vector<std::string>& ordinary_upload_acknowledgment_result::ids()const noexcept {
    static const std::vector<std::string> empty;
    return value_?value_->ids:empty;
}
const std::vector<uint8_t>& ordinary_upload_acknowledgment_result::acknowledgment()const noexcept {
    static const std::vector<uint8_t> empty;
    return value_?value_->acknowledgment:empty;
}
size_t ordinary_upload_acknowledgment_result::acknowledgmentByteCount()const noexcept {
    return value_?value_->acknowledgment.size():0;
}
bool ordinary_upload_acknowledgment_result::copyAcknowledgmentTo(void* output,size_t count)const noexcept {
    if(!value_||count!=value_->acknowledgment.size()||(!output&&count))return false;
    if(count)std::memcpy(output,value_->acknowledgment.data(),count);
    return true;
}
ordinary_upload_acknowledgment_result ordinary_upload_acknowledgment_result::merged(const ordinary_upload_acknowledgment_result& other)const noexcept {
    try {
        if(!value_||!other.value_)return {};
        auto hold=detail::reserve_ordinary_upload_result(); // Before any merged storage.
        std::vector<detail::ordinary_upload_ack> pairs;pairs.reserve(ordinary_sync_limits::records_per_page);
        for(const auto* input:{this,&other}) {
            const auto& source=*input->value_;
            if(source.acknowledgment.empty()){if(!source.ids.empty())return {};continue;}
            const auto decoded=detail::decode_ordinary_upload_ack(std::string_view(reinterpret_cast<const char*>(source.acknowledgment.data()),source.acknowledgment.size()));
            if(decoded.size()!=source.ids.size())return {};
            for(size_t n=0;n<decoded.size();++n){const auto& pair=decoded[n];if(pair.gid!=source.ids[n])return {};
                const auto found=std::find_if(pairs.begin(),pairs.end(),[&](const auto& old){return !detail::ordinary_v2_nocase_less{}(old.gid,pair.gid)&&!detail::ordinary_v2_nocase_less{}(pair.gid,old.gid);});
                if(found!=pairs.end()){if(found->gid!=pair.gid||found->version!=pair.version)return {};continue;}
                if(pairs.size()==ordinary_sync_limits::records_per_page)return {};pairs.push_back(pair);
            }
        }
        std::vector<audit_log_entry> carriers;carriers.reserve(pairs.size());
        std::vector<std::string> accepted;accepted.reserve(pairs.size());
        for(const auto& pair:pairs){audit_log_entry entry;entry.global_id=pair.gid;entry.ordinary_delivery_version=pair.version;carriers.push_back(std::move(entry));accepted.push_back(pair.gid);}
        const auto bytes=detail::encode_ordinary_upload_ack(carriers,accepted);
        std::vector<uint8_t> encoded(bytes.begin(),bytes.end());
        return ordinary_upload_acknowledgment_result(std::move(hold),std::move(accepted),std::move(encoded));
    }catch(...){return {};}
}
}
