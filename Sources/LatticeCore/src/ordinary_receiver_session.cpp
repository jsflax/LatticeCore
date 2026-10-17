#include "ordinary_timestamp.hpp"
#include "ordinary_receiver_session.hpp"
#include "ordinary_managed_wss_binding.hpp"
#include "ordinary_wire.hpp"
#include "ordinary_history_receive.hpp"
#include "ordinary_group_history_codec.hpp"
#include "ordinary_managed_receiver_wss.hpp"
#include <bit>
#include "ordinary_export.hpp"
#include "ordinary_wire_test_hooks.hpp"
#include "ordinary_v2_property.hpp"
#include "ordinary_v2_identity.hpp"
#include "sync_callback_lifetime.hpp"
#include "vendor/picosha2/picosha2.h"
#include <lattice/lattice.hpp>
#include <nlohmann/json.hpp>
#include <atomic>
#include <charconv>
#include <cmath>
#include <limits>
#include <set>
#include <algorithm>

namespace lattice::detail {
thread_local std::function<void(lattice_db&)> ordinary_wire_test_hooks::after_child_open;
namespace {
using json=nlohmann::json;
using caps=ordinary_sync_limits;
[[noreturn]] void reject(const char* why){throw db_error(std::string("ordinary v2 receive: ")+why);}
int64_t clock_millis(){return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
// One phase after the genuine effect offer: existing relay cut admission30s
// plus first-frame admission30s. Delayed IO/send/SQL consumes this same bound;
// it is not a hard interrupt or a reset of5s negotiation/300s transfer.
constexpr int64_t pre_header_admission_millis=2*30*1000;
bool valid_model_profile(std::string_view id){
    if(id.empty()||id.size()>64)return false;
    return std::all_of(id.begin(),id.end(),[](unsigned char c){return (c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='-'||c=='.';});
}
using stage_reservation=ordinary_resource_reservation<true>;
struct sax final:nlohmann::json_sax<json> {
    size_t nodes=0,workspace;const size_t maximum_string;struct level{bool object;std::set<std::string> keys;};std::vector<level> stack;
    explicit sax(size_t wire_bytes,size_t string_limit=caps::scalar_bytes):workspace(wire_bytes*caps::parser_wire_copies),maximum_string(string_limit){}
    bool node(){if(++nodes>caps::parser_nodes||caps::parser_node_logical_bytes>caps::parser_workspace_bytes-workspace)return false;
        workspace+=caps::parser_node_logical_bytes;return true;}
    bool null()override{return node();}bool boolean(bool)override{return node();}
    bool number_integer(number_integer_t)override{return node();}bool number_unsigned(number_unsigned_t)override{return node();}
    bool number_float(number_float_t n,const string_t&)override{return std::isfinite(n)&&node();}
    bool string(string_t& s)override{return s.size()<=maximum_string&&node();}
    bool binary(binary_t&)override{return false;}
    bool start(bool object){if(!node()||stack.size()>=caps::parser_depth)return false;stack.push_back({object,{}});return true;}
    bool start_object(size_t)override{return start(true);}bool start_array(size_t)override{return start(false);}
    bool key(string_t& s)override{return !stack.empty()&&stack.back().object&&s.size()<=256&&node()&&stack.back().keys.insert(s).second;}
    bool end_object()override{if(stack.empty()||!stack.back().object)return false;stack.pop_back();return true;}
    bool end_array()override{if(stack.empty()||stack.back().object)return false;stack.pop_back();return true;}
    bool parse_error(size_t,const std::string&,const nlohmann::detail::exception&)override{return false;}
};
void keys(const json& value,std::initializer_list<const char*> fields){
    if(!value.is_object()||value.size()!=fields.size())reject("unknown, mixed or missing fields");
    for(const auto* field:fields)if(!value.contains(field))reject("missing field");
}
json parse(std::string_view raw){
    if(raw.empty()||raw.size()>caps::frame_bytes)reject("frame byte bound");
    // A full logical staging reservation already exists before SAX or DOM can
    // allocate. SAX refuses duplicate keys before any legacy parser sees them.
    {sax check(raw.size());if(!json::sax_parse(raw.begin(),raw.end(),&check))reject("invalid or over-budget JSON");}
    // Duplicate-key sets are destroyed before retaining a DOM. The per-frame
    // preflight accounts for the DOM and bounded original conversion scratch.
    auto result=json::parse(raw.begin(),raw.end());keys(result,{"latticeOrdinarySync"});
    auto value=std::move(result["latticeOrdinarySync"]);
    if(!value.is_object()||!value.contains("version")||!value["version"].is_number_integer()||value["version"]!=2)reject("unsupported version");
    return value;
}
const std::string& text(const json& value,size_t cap=caps::scalar_bytes,bool empty=false){
    if(!value.is_string())reject("text required");const auto& s=value.get_ref<const std::string&>();
    if((!empty&&s.empty())||s.size()>cap||s.find('\0')!=std::string::npos)reject("text bound or embedded NUL");return s;
}
int64_t decimal(const json& value,int64_t maximum=INT64_MAX,bool zero=false){
    const auto& s=text(value,20);int64_t n=0;const auto result=std::from_chars(s.data(),s.data()+s.size(),n);
    if(result.ec!=std::errc{}||result.ptr!=s.data()+s.size()||n<(zero?0:1)||n>maximum||std::to_string(n)!=s)reject("noncanonical decimal");return n;
}
int64_t signed_number(const json& value){
    if(!value.is_number_integer()||(value.is_number_unsigned()&&value.get<uint64_t>()>uint64_t(INT64_MAX)))reject("exact integer required");
    return value.get<int64_t>();
}
void unsigned_decimal(const json& value){
    const auto& s=text(value,20);uint64_t n=0;const auto result=std::from_chars(s.data(),s.data()+s.size(),n);
    if(result.ec!=std::errc{}||result.ptr!=s.data()+s.size()||std::to_string(n)!=s)reject("unsigned decimal spelling");
}
bool hex(char c){return(c>='0'&&c<='9')||(c>='a'&&c<='f');}
void digest_text(const std::string& value){if(value.size()!=64||!std::all_of(value.begin(),value.end(),hex))reject("digest spelling");}
void uuid(const std::string& value){if(value.size()!=36)reject("source UUID length");for(size_t i=0;i<value.size();++i)if((i==8||i==13||i==18||i==23)?value[i]!='-':!hex(value[i]))reject("source UUID spelling");}
std::string digest(std::string_view value){return picosha2::hash256_hex_string(value.begin(),value.end());}
void hash_bytes(picosha2::hash256_one_by_one& h,std::string_view value){
    while(!value.empty()){const auto n=std::min<size_t>(4096,value.size());h.process(value.begin(),value.begin()+n);value.remove_prefix(n);}
}
struct decoded_budget {
    size_t used=0,cells=0,maximum=caps::decoded_receive_bytes;
    void add(size_t amount){if(amount>maximum-used)reject("decoded retention byte bound");used+=amount;}
    void array(size_t count,size_t width){if(count>maximum/width)reject("decoded allocation product bound");add(count*width);}
    void row(size_t count){if(count>caps::decoded_cells-cells)reject("decoded cell bound");cells+=count;
        array(count,sizeof(column_value_t));add(2*sizeof(std::vector<column_value_t>));}
    std::string copy(const std::string& value){add(value.size()+1);return value;}
    // Original decoding allocates maps, vectors and immutable identity names.
    // Charge their complete input tree before the decoder allocates or copies;
    // 128 logical bytes/node includes the container slots and growth allowance.
    void original(const json& value){add(128);if(value.is_string())add(value.get_ref<const std::string&>().size()+1);
        else if(value.is_object())for(auto it=value.begin();it!=value.end();++it){add(it.key().size()+1);original(it.value());}
        else if(value.is_array())for(const auto& child:value)original(child);}
};
column_value_t scalar(const json& value,decoded_budget& budget){
    if(!value.is_array()||value.empty()||value.size()>2)reject("typed cell shape");const auto& kind=text(value[0],16);
    if(kind=="null"){if(value.size()!=1)reject("null cell shape");return nullptr;}
    if(value.size()!=2)reject("missing cell value");
    if(kind=="text"){if(!value[1].is_string())reject("text cell type");return budget.copy(value[1].get_ref<const std::string&>());}
    if(kind=="integer"){
        const auto& s=text(value[1],20);int64_t n=0;const auto parsed=std::from_chars(s.data(),s.data()+s.size(),n);
        if(parsed.ec!=std::errc{}||parsed.ptr!=s.data()+s.size()||std::to_string(n)!=s)reject("integer cell spelling");return n;
    }
    if(kind=="real"){if(!value[1].is_number()||!std::isfinite(value[1].get<double>()))reject("real cell value");return value[1].get<double>();}
    if(kind=="blob"){
        const auto& s=text(value[1],caps::scalar_bytes,true);if(s.size()%2||!std::all_of(s.begin(),s.end(),hex))reject("blob cell spelling");
        budget.add(s.size()/2);std::vector<uint8_t> bytes;bytes.reserve(s.size()/2);const auto nib=[](char c){return c<='9'?c-'0':c-'a'+10;};
        for(size_t i=0;i<s.size();i+=2)bytes.push_back(uint8_t(nib(s[i])*16+nib(s[i+1])));return bytes;
    }
    reject("unknown cell kind");
}
bool storage(const column_value_t& value,column_type type){
    if(std::holds_alternative<std::nullptr_t>(value))return true;
    if(type==column_type::integer)return std::holds_alternative<int64_t>(value);
    if(type==column_type::real)return std::holds_alternative<double>(value)||std::holds_alternative<int64_t>(value);
    if(type==column_type::text)return std::holds_alternative<std::string>(value);
    return type==column_type::blob&&std::holds_alternative<std::vector<uint8_t>>(value);
}
size_t original_column(const ordinary_export_table& table,const std::string& name){
    // The generated union program uses the quoted SQL spelling for this one
    // audit key. Decoded descriptor data cannot establish that it is a union:
    // actual local audit_names are checked by the owned installer later.
    const std::string_view physical=name=="\"case\""?std::string_view("case"):std::string_view(name);
    const auto found=std::find_if(table.columns.begin(),table.columns.end(),[&](const auto& column){return std::string_view(column)==physical;});
    if(found==table.columns.end())reject("original unknown projection column");return size_t(found-table.columns.begin());
}
any_property original_property(const json& encoded,column_type type){
    keys(encoded,{"kind","value"});auto result=decode_ordinary_v2_property(encoded,caps::scalar_bytes);
    // Actual model audit programs store BLOB values through SQLite hex(),
    // whereas late binding may use data-kind bytes. Preserve the original
    // kind and exact hex spelling; these values are not the row postimage.
    const auto* blob_hex=type==column_type::blob?std::get_if<std::string>(&result.value):nullptr;
    const bool audit_blob=blob_hex&&blob_hex->size()%2==0&&std::all_of(blob_hex->begin(),blob_hex->end(),[](char c){return hex(c)||(c>='A'&&c<='F');});
    if(!storage(result.value,type)&&!audit_blob)reject("original property storage differs from projection");return result;
}
audit_log_entry original_entry(const json& encoded,const std::vector<ordinary_export_table>& tables,decoded_budget& budget){
    if(!encoded.is_object())reject("original object required");
    for(const auto* name:{"id","globalId","tableName","operation","rowId","globalRowId","changedFields","changedFieldsNames","timestamp","isFromRemote","isSynchronized"})
        if(!encoded.contains(name))reject("original missing field");
    if(encoded.size()!=11+size_t(encoded.contains("synthesized"))+size_t(encoded.contains("originalIdentity")))reject("original unknown field");
    // Precharge the complete bounded object before copying any maps, strings,
    // vectors or identity. Legacy entry parsing and its32/64limits stay exact.
    budget.original(encoded);audit_log_entry result;
    result.id=signed_number(encoded["id"]);result.row_id=signed_number(encoded["rowId"]);
    result.global_id=text(encoded["globalId"],4096);result.global_row_id=text(encoded["globalRowId"],4096);
    result.table_name=text(encoded["tableName"],256);result.operation=text(encoded["operation"],16);result.timestamp=text(encoded["timestamp"],caps::scalar_bytes,true);
    (void)normalize_required_ordinary_timestamp(result.timestamp);
    if(result.id<=0||(result.operation!="INSERT"&&result.operation!="UPDATE"&&result.operation!="DELETE"))reject("original identity or operation");
    const auto found=std::find_if(tables.begin(),tables.end(),[&](const auto& table){return table.name==result.table_name;});
    if(found==tables.end())reject("original table outside projection");const auto& table=*found;
    for(const auto* flag:{"isFromRemote","isSynchronized"})if(!encoded[flag].is_boolean())reject("original flag type");
    result.is_from_remote=encoded["isFromRemote"].get<bool>();result.is_synchronized=encoded["isSynchronized"].get<bool>();
    if(encoded.contains("synthesized")){if(!encoded["synthesized"].is_boolean()||!encoded["synthesized"].get<bool>())reject("noncanonical synthesized flag");result.synthesized=true;}
    const auto names=[&](const json& values){
        if(!values.is_array()||values.size()>table.columns.size())reject("original property name count");std::vector<std::string> result;result.reserve(values.size());
        for(const auto& value:values){const auto& name=text(value,256);(void)original_column(table,name);
            if(std::find(result.begin(),result.end(),name)!=result.end())reject("duplicate original property name");result.push_back(name);}return result;};
    const auto& fields=encoded["changedFields"];if(!fields.is_object()||fields.size()>table.columns.size())reject("original changed field count");
    for(auto it=fields.begin();it!=fields.end();++it){if(it.key().empty()||it.key().size()>256||it.key().find('\0')!=std::string::npos)reject("original property name bound");
        const auto column=original_column(table,it.key());result.changed_fields.emplace(it.key(),original_property(it.value(),table.types[column]));}
    result.changed_fields_names=names(encoded["changedFieldsNames"]);
    if(encoded.contains("originalIdentity")){const auto& identity=encoded["originalIdentity"];keys(identity,{"version","changedFieldsNames","digest"});
        if(signed_number(identity["version"])!=1)reject("original identity version");audit_original_identity saved;saved.changed_fields_names=names(identity["changedFieldsNames"]);
        saved.digest=text(identity["digest"],64);digest_text(saved.digest);result.original_identity=std::move(saved);}
    return result;
}
}

size_t ordinary_wire_test_hooks::receive_slots()noexcept{return ordinary_receive_slots.load(std::memory_order_acquire);}
struct ordinary_receiver_session::record {
    platform_transport_callbacks endpoint;uint64_t lifecycle=0;std::string request,model_request;
    bool model_agreed=false,effect_request_sent=false;
    bool relation_required=false; // Only the retained actual receive-only issuer.
    std::atomic<int64_t> expires{0};
    bool offered=false,upload_observation=false,upload_acknowledgment=false;std::atomic<bool> failed{false};
    std::shared_ptr<ordinary_received_cut> stage;
    std::shared_ptr<ordinary_history_receive_state> history;
};
ordinary_audit_original_descriptor original_descriptor(const json& value,decoded_budget& budget) {
    keys(value,{"version","fields","names","digest"});
    if(signed_number(value["version"])!=1)reject("ordinary original descriptor version");
    for(const auto* key:{"fields","names"})if(!value[key].is_array()||value[key].size()>caps::columns_per_table)reject("ordinary original descriptor name bound");
    budget.original(value);ordinary_audit_original_descriptor result;
    result.fields.reserve(value["fields"].size());result.names.reserve(value["names"].size());
    for(const auto& field:value["fields"]){const auto& name=text(field,256);if(!result.fields.empty()&&result.fields.back()>=name)reject("ordinary original fields unordered");result.fields.push_back(name);}
    for(const auto& field:value["names"]){const auto& name=text(field,256);if(std::find(result.names.begin(),result.names.end(),name)!=result.names.end())reject("ordinary original duplicate name");result.names.push_back(name);}
    result.digest=text(value["digest"],64);digest_text(result.digest);return result;
}
#include "ordinary_history_decode.inc"
#include "ordinary_history_lineage_relation_decode.inc"

void ordinary_group_history_codec::offer(std::string_view raw,const std::string& nonce) {
    if(raw.size()>4096)reject("GROUP history offer byte bound");
    auto value=history_receive_codec::parse_frame(raw);keys(value,{"version","profile","kind","request"});
    if(value["kind"]!="offer"||value["request"]!=nonce)reject("GROUP history offer correlation differs");
}
ordinary_history_lineage_relation ordinary_group_history_codec::relation(std::string_view raw,
    const ordinary_receiver_history_expectation& expected,const std::string& nonce,
    const std::string& channel,const std::string& profile) {
    return history_lineage_codec::decode(raw,expected,nonce,channel,profile);
}
ordinary_history_descriptor ordinary_group_history_codec::header(std::string_view raw,const ordinary_history_limits& limits) {
    return decode_ordinary_history_descriptor(raw,limits);
}
ordinary_history_decoded_page ordinary_group_history_codec::page(std::string_view raw,const ordinary_history_descriptor& header,const ordinary_history_progress& progress,const ordinary_history_limits& limits) {
    return decode_ordinary_history_page(raw,header,progress,limits);
}
void ordinary_group_history_codec::match(const ordinary_history_lineage_relation& relation,const ordinary_history_descriptor& header) {
    history_lineage_codec::match_header(relation,header);
}


struct ordinary_received_cut {
    // Reservation dies last, after all retained decoded values and captures.
    stage_reservation charge;
    decoded_budget budget;
    std::shared_ptr<ordinary_export_frame> frame;
    std::weak_ptr<ordinary_receiver_session::record> connection;
    size_t bytes=0,pages=0,next_page=0,rows=0,originals=0,last_table=0;
    std::string expected_digest; picosha2::hash256_one_by_one content;
    std::set<std::string,ordinary_v2_nocase_less> original_ids;
    std::atomic<bool> consumed{false};
    ordinary_received_cut(stage_reservation&& reservation):charge(std::move(reservation)){
        budget.add(sizeof(ordinary_received_cut)+sizeof(ordinary_export_frame));frame=std::make_shared<ordinary_export_frame>();
        // Private authenticated v2 intake selects its finite resource profile;
        // this flag does not grant source or receiver authority.
        frame->wire_profile=true;
    }
    void descriptor(const json& value,size_t raw_bytes){
        keys(value,{"version","kind","source","epoch","cut","mode","base","scope","schema","catalog","captureDevice","captureInode","rows","originals","pages","contentDigest","models","tables"});
        if(value["kind"]!="cut"||value["mode"]!="full"||!value["base"].is_null())reject("unsupported cut mode");
        frame->source=budget.copy(text(value["source"],36));uuid(frame->source);frame->epoch=decimal(value["epoch"]);frame->cut=decimal(value["cut"]);
        if(frame->epoch!=1)reject("unproved source epoch transition");
        frame->scope=budget.copy(text(value["scope"],caps::models*260));frame->schema=budget.copy(text(value["catalog"],caps::schema_bytes));
        const auto& schema=text(value["schema"],64);digest_text(schema);if(digest(frame->schema)!=schema)reject("catalog digest mismatch");
        frame->device=budget.copy(text(value["captureDevice"],32));frame->inode=budget.copy(text(value["captureInode"],32));
        unsigned_decimal(value["captureDevice"]);unsigned_decimal(value["captureInode"]);
        rows=size_t(decimal(value["rows"],caps::rows,true));originals=size_t(decimal(value["originals"],caps::originals,true));
        pages=size_t(decimal(value["pages"],caps::frames-1,true));expected_digest=budget.copy(text(value["contentDigest"],64));digest_text(expected_digest);
        if((pages==0)!=(rows+originals==0)||pages>rows+originals||(rows+originals&&pages<(rows+originals+caps::records_per_page-1)/caps::records_per_page))reject("impossible page counts");
        const auto& tables=value["tables"];if(!tables.is_array()||tables.empty()||tables.size()>caps::projection_tables)reject("table count bound");
        const auto& models=value["models"];if(!models.is_array()||models.empty()||models.size()>caps::models)reject("root model count bound");
        budget.array(models.size(),sizeof(std::string));frame->models.reserve(models.size());
        budget.array(tables.size(),sizeof(ordinary_export_table));frame->tables.reserve(tables.size());
        budget.array(originals,sizeof(audit_log_entry));frame->entries.reserve(originals);
        budget.array(originals,sizeof(ordinary_audit_original_descriptor));frame->originals.reserve(originals);
        std::string scope;for(const auto& model:models){const auto& name=text(model,256);if(!frame->models.empty()&&frame->models.back()>=name)reject("unordered root models");frame->models.push_back(budget.copy(name));scope+=std::to_string(name.size())+':'+name;}
        size_t columns=0;
        for(const auto& item:tables){keys(item,{"name","columns","types"});ordinary_export_table table;table.name=budget.copy(text(item["name"],256));
            if(!frame->tables.empty()&&frame->tables.back().name>=table.name)reject("unordered or duplicate tables");
            const auto& names=item["columns"];const auto& types=item["types"];
            if(!names.is_array()||names.empty()||names.size()>caps::columns_per_table||!types.is_array()||types.size()!=names.size()||columns+names.size()>caps::projection_columns)reject("column count bound");
            columns+=names.size();std::set<std::string> unique;
            budget.array(names.size(),sizeof(std::string)+sizeof(column_type));table.columns.reserve(names.size());table.types.reserve(names.size());
            for(size_t i=0;i<names.size();++i){const auto& name=text(names[i],256);if(!unique.insert(name).second)reject("duplicate column");
                const auto type=signed_number(types[i]);if(type<static_cast<int>(column_type::integer)||type>static_cast<int>(column_type::blob))reject("unknown column type");
                table.columns.push_back(budget.copy(name));table.types.push_back(static_cast<column_type>(type));}
            if(table.columns[0]!="globalId"||table.types[0]!=column_type::text)reject("missing global identity column");
            frame->tables.push_back(std::move(table));
        }
        if(scope!=frame->scope)reject("scope differs from exact ordered root models");bytes=raw_bytes;
    }
    void page(const json& value,std::string_view raw){
        keys(value,{"version","kind","source","epoch","cut","index","records","digest"});
        if(value["kind"]!="page"||text(value["source"],36)!=frame->source||decimal(value["epoch"])!=frame->epoch||decimal(value["cut"])!=frame->cut||size_t(decimal(value["index"],caps::frames-1,true))!=next_page||next_page>=pages)reject("page source or sequence differs");
        if(raw.size()>caps::retained_wire_bytes-bytes)reject("cut wire byte bound");bytes+=raw.size();
        // The emitter's canonical suffix is part of v2 framing. Validate the
        // original bytes; reserializing a DTO must not bless an altered page.
        const auto& expected=text(value["digest"],64);digest_text(expected);
        const std::string suffix=",\"digest\":\""+expected+"\"}}";
        if(raw.size()<suffix.size()||raw.substr(raw.size()-suffix.size())!=suffix||digest(raw.substr(0,raw.size()-suffix.size()))!=expected)reject("page digest or framing differs");
        const auto& records=value["records"];if(!records.is_array()||records.empty()||records.size()>caps::records_per_page)reject("page record count bound");
        for(const auto& record:records){
            if(record.is_object()&&record.contains("row")){
                keys(record,{"row"});if(!frame->entries.empty()||frame->rows>=rows)reject("row count or phase differs");
                const auto& row=record["row"];keys(row,{"table","values"});const auto& name=text(row["table"],256);
                while(last_table<frame->tables.size()&&frame->tables[last_table].name!=name)++last_table;
                if(last_table==frame->tables.size())reject("row outside ordered table scope");auto& table=frame->tables[last_table];const auto& cells=row["values"];
                if(!cells.is_array()||cells.size()!=table.columns.size())reject("row column shape");
                budget.row(cells.size());std::vector<column_value_t> values;values.reserve(cells.size());
                for(size_t i=0;i<cells.size();++i){auto value=scalar(cells[i],budget);if(!storage(value,table.types[i]))reject("cell storage disagrees with descriptor");values.push_back(std::move(value));}
                const auto* id=std::get_if<std::string>(&values[0]);if(!id||id->empty()||id->size()>4096)reject("row identity bound");
                if(!table.rows.empty()&&std::get<std::string>(table.rows.back()[0])>=*id)reject("duplicate or unordered row identity");
                if(table.rows.size()==table.rows.capacity()){
                    const auto capacity=std::min(rows,table.rows.capacity()?table.rows.capacity()*2:size_t(1));
                    budget.array(capacity,sizeof(std::vector<column_value_t>));table.rows.reserve(capacity);
                }
                table.rows.push_back(std::move(values));++frame->rows;
            }else{
                keys(record,{"original","originalDescriptor"});if(frame->rows!=rows||frame->entries.size()>=originals)reject("original count or phase differs");
                auto decoded=original_entry(record["original"],frame->tables,budget);
                budget.add(sizeof(std::string)+4*sizeof(void*)+decoded.global_id.size()+1);
                if((!frame->entries.empty()&&frame->entries.back().id>=decoded.id)||!original_ids.insert(decoded.global_id).second)reject("original identity or order differs");
                auto descriptor=original_descriptor(record["originalDescriptor"],budget);
                frame->entries.push_back(std::move(decoded));frame->originals.push_back(std::move(descriptor));
            }
        }
        hash_bytes(content,raw);++next_page;
    }
    bool complete(){if(next_page!=pages)return false;content.finish();
        if(frame->rows!=rows||frame->entries.size()!=originals||picosha2::get_hash_hex_string(content)!=expected_digest)reject("incomplete or changed cut content");return true;}
};

std::vector<std::string> ordinary_receiver_session::issued_model_scope(const std::shared_ptr<lattice_db>& owner,const std::string& selector){
    if(selector.empty())return {}; // Exact old default contract.
    if(!owner||!valid_model_profile(selector))reject("invalid configured model selector");
    // Never use the process-wide native registry as the selected store scope.
    // These root declarations were captured from the actual Swift SchemaVector
    // passed when this opened store was issued, before any network frame.
    #ifndef __EMSCRIPTEN__
    const auto receiver=ordinary_receiver_wss_access::models_for_actual_child(*owner,selector);
    if(!receiver.empty())return receiver;
    #endif
    const auto& selected=owner->recovery_declarations().swift_models;
    if(selected.empty()||selected.size()>caps::models)reject("model profile requires actual immutable Swift store declarations");
    std::vector<std::string> result;result.reserve(selected.size());size_t bytes=0;
    for(const auto& name:selected){if(name.empty()||name.size()>256||bytes>65536-name.size())reject("configured root model bound");bytes+=name.size();result.push_back(name);}
    return result;
}
ordinary_receiver_session::ordinary_receiver_session(const std::shared_ptr<lattice_db>& owner,const std::shared_ptr<sync_callback_lifetime>& life,std::string channel,const std::string& url,const std::shared_ptr<ordinary_managed_product_credentials>& credentials,const std::string& token,bool history_profile,const std::string& model_profile)
    :owner_(owner),lifetime_(life),channel_(std::move(channel)),history_profile_(history_profile),model_profile_(model_profile),expected_models_(issued_model_scope(owner,model_profile)){
    if(!owner||!life||channel_.empty()||channel_.size()>4096||url.size()>4096||url.rfind("wss://",0)!=0||url.find('#')!=std::string::npos||url.find('@')!=std::string::npos)reject("configured retained owner and explicit WSS endpoint required");
    managed_binding_=ordinary_managed_wss_binding::attach(credentials,owner,life,url,token,channel_);
}
ordinary_receiver_session::~ordinary_receiver_session(){if(managed_binding_)managed_binding_->retire();}
bool ordinary_receiver_session::uploads_ready()const {
    std::lock_guard lock(mutex_);return !history_profile_&&current_&&current_->offered&&current_->upload_observation&&current_->upload_acknowledgment&&live(current_);
}
std::chrono::steady_clock::time_point ordinary_receiver_session::deadline()const noexcept{
    const auto value=deadline_millis_.load(std::memory_order_acquire);
    return value==INT64_MAX?std::chrono::steady_clock::time_point::max():std::chrono::steady_clock::time_point(std::chrono::milliseconds(value));
}
bool ordinary_receiver_session::expire(){
    std::shared_ptr<record> retired;
    {std::lock_guard lock(mutex_);
        if(!current_||clock_millis()<current_->expires.load(std::memory_order_acquire))return false;
        current_->failed=true;if(current_->history)current_->history->failed_=true;retired.swap(current_);deadline_millis_=INT64_MAX;
    }
    // The actual retained endpoint owns terminal delivery. Publish its phase
    // and TLS/live fences, then run the genuine owner+attempt+generation-guarded
    // error handler outside this leaf. That handler retires protected transport
    // and requests renewal, or uses the existing ordinary reconnect/backoff.
    // A replacement or physical terminal race refuses this old delivery;
    // never disconnect the whole adapter and accidentally close a new attempt.
    if(retired&&!retired->endpoint.trigger_on_error("ordinary v2 receive deadline expired"))
        if(managed_binding_)managed_binding_->closed(retired->endpoint,retired->lifecycle);
    // Retire incomplete stage outside the leaf. Already queued operations keep
    // their actual charge until settlement; terminal delivery is not cleanup.
    return true;
}
bool ordinary_receiver_session::live(const std::shared_ptr<record>& r)const{
    const auto owner=owner_.lock();const auto lifetime=lifetime_.lock();return (!managed_binding_||managed_binding_->configuration_current())&&r&&!r->failed.load(std::memory_order_acquire)&&owner&&!owner->is_closed()&&lifetime&&lifetime->current(r->lifecycle)&&r->endpoint.current_system_tls_for_owner()&&clock_millis()<r->expires.load(std::memory_order_acquire);
}
void ordinary_receiver_session::begin_pre_header_admission(const std::shared_ptr<record>& connection){
    // Correlation/shape is already checked under the receiver leaf. Recheck
    // the original negotiation/current endpoint before publishing the offer;
    // parse or scheduling delay cannot promote an expired5s negotiation.
    const auto offered_at=clock_millis();
    if(connection->offered||!live(connection))reject("effect offer admission expired or retired");
    connection->offered=true;
    connection->expires=offered_at+pre_header_admission_millis;
    deadline_millis_=connection->expires.load();
}
void ordinary_receiver_session::opened(const platform_transport_callbacks& endpoint,uint64_t lifecycle,owned_platform_sync_transport& transport){
    auto next=std::make_shared<record>();next->endpoint=endpoint;next->lifecycle=lifecycle;next->request=uuid_t::generate().to_string();next->expires=clock_millis()+5000;
    next->model_agreed=model_profile_.empty();next->effect_request_sent=model_profile_.empty();
    next->relation_required=history_profile_&&managed_binding_&&managed_binding_->has_receiver_history_expectation();
    if(!model_profile_.empty())next->model_request=uuid_t::generate().to_string();
    if(!live(next))reject("actual current system-TLS physical attempt required");std::shared_ptr<record> retired;
    {std::lock_guard lock(mutex_);if(!endpoint.current_system_tls_for_owner())reject("physical attempt retired before negotiation");retired.swap(current_);if(retired&&retired->history)retired->history->failed_=true;current_=next;deadline_millis_=next->expires.load();}
    if(managed_binding_)managed_binding_->opened(endpoint,lifecycle);
    auto request=json{{"version",2},{"kind","request"},{"request",next->request}};
    if(!model_profile_.empty())request=json{{"version",2},{"kind","modelRequest"},{"request",next->model_request},{"modelProfile",model_profile_}};
    else if(history_profile_){request["profile"]="paged-history-v1";if(next->relation_required)request["relation"]="mounted-lineage-frontier-v1";}
    const auto wire=json{{"latticeOrdinarySync",std::move(request)}}.dump();
    if(!transport.send_to_attempt(endpoint,transport_message::from_string(wire))){next->failed=true;closed(endpoint,lifecycle);reject("negotiation send refused");}
}
bool ordinary_receiver_session::receive_model_offer(const platform_transport_callbacks& endpoint,uint64_t lifecycle,std::string_view raw){
    std::shared_ptr<record> connection;
    {std::unique_lock lock(mutex_,std::try_to_lock);if(!lock.owns_lock())reject("concurrent model ingress refused");connection=current_;
        if(!connection||!connection->endpoint.matches(endpoint)||connection->lifecycle!=lifecycle||!live(connection))reject("stale model agreement");
        if(connection->model_agreed)return false;
        try{
            if(raw.size()>4096)reject("model offer byte bound");
            stage_reservation charge;auto value=parse(raw);keys(value,{"version","kind","request","modelProfile"});
            if(value["kind"]!="modelOffer"||value["request"]!=connection->model_request||value["modelProfile"]!=model_profile_)reject("model agreement correlation differs");
            connection->model_agreed=true;
            // Preserve the SAME original five-second physical-attempt budget.
            // This is not the effect offer and authorizes no bytes or uploads.
        }catch(...){connection->failed=true;deadline_millis_=INT64_MAX;throw;}
    }
    return true;
}
void ordinary_receiver_session::advance_model_negotiation(const platform_transport_callbacks& endpoint,uint64_t lifecycle,owned_platform_sync_transport& transport){
    if(model_profile_.empty())return;
    std::shared_ptr<record> connection;
    {std::unique_lock lock(mutex_,std::try_to_lock);if(!lock.owns_lock())reject("concurrent model advance refused");connection=current_;
        if(!connection||!connection->endpoint.matches(endpoint)||connection->lifecycle!=lifecycle||!live(connection))reject("retired model advance");
        if(!connection->model_agreed||connection->effect_request_sent)return;
        connection->effect_request_sent=true;
    }
    // Actual endpoint retained off-leaf. No controller mutex spans foreign send,
    // and a synchronous callback sees the original effect request as admitted.
    auto request=json{{"version",2},{"kind","request"},{"request",connection->request}};
    if(history_profile_){request["profile"]="paged-history-v1";if(connection->relation_required)request["relation"]="mounted-lineage-frontier-v1";}
    const auto wire=json{{"latticeOrdinarySync",std::move(request)}}.dump();
    if(!live(connection)||!transport.send_to_attempt(endpoint,transport_message::from_string(wire))){connection->failed=true;closed(endpoint,lifecycle);reject("effect negotiation send refused");}
}
void ordinary_receiver_session::closed(const platform_transport_callbacks& endpoint,uint64_t lifecycle){
    if(managed_binding_)managed_binding_->closed(endpoint,lifecycle);
    std::shared_ptr<record> retired;
    {std::lock_guard lock(mutex_);if(current_&&current_->lifecycle==lifecycle&&current_->endpoint.matches(endpoint)){current_->failed=true;if(current_->history)current_->history->failed_=true;retired.swap(current_);deadline_millis_=INT64_MAX;}}
    // Endpoint/provider and retained-cut destruction stays outside the leaf.
}
bool ordinary_receiver_session::receive(const platform_transport_callbacks& endpoint,uint64_t lifecycle,const transport_message& message,std::shared_ptr<ordinary_received_cut>& completed,std::vector<ordinary_upload_ack>* acknowledgment,std::shared_ptr<ordinary_history_receive_operation>* history){
    const std::string_view raw(reinterpret_cast<const char*>(message.data.data()),message.data.size());if(!reserved_ordinary_wire_frame(raw))return false;
    if(!model_profile_.empty()&&receive_model_offer(endpoint,lifecycle,raw))return true;
    if(history_profile_)return receive_history(endpoint,lifecycle,raw,history);
    std::shared_ptr<record> connection;std::shared_ptr<ordinary_received_cut> held;
    {std::unique_lock lock(mutex_,std::try_to_lock);if(!lock.owns_lock())reject("concurrent ingress refused");connection=current_;
        if(!connection||!connection->endpoint.matches(endpoint)||connection->lifecycle!=lifecycle||!live(connection))reject("stale or unauthenticated source frame");
        try{
            if(!connection->effect_request_sent)reject("effect frame before actual request");
            held=connection->stage;
            // Reserve before either JSON pass, and retain the same reservation
            // through the whole cut and its queued owned installation.
            if(!held){stage_reservation charge;held=std::make_shared<ordinary_received_cut>(std::move(charge));}
            auto value=parse(raw);const auto& kind=text(value.at("kind"),16);
            if(!connection->offered){
                if(value.contains("uploadObservation")) {
                    if(value.contains("uploadAcknowledgment")) {
                        keys(value,{"version","kind","request","uploadObservation","uploadAcknowledgment"});
                        if(signed_number(value["uploadAcknowledgment"])!=1)reject("unsupported upload acknowledgment");
                        connection->upload_acknowledgment=true;
                    } else keys(value,{"version","kind","request","uploadObservation"});
                    if(signed_number(value["uploadObservation"])!=1)reject("unsupported upload observation");
                    connection->upload_observation=true;
                } else keys(value,{"version","kind","request"});
                if(raw.size()>4096||kind!="offer"||value["request"]!=connection->request)reject("negotiation correlation differs");begin_pre_header_admission(connection);return true;
            }
            if(kind=="uploadAck") {
                if(!connection->upload_acknowledgment||!acknowledgment)reject("unnegotiated upload acknowledgment");
                *acknowledgment=decode_ordinary_upload_ack(raw);return true;
            }
            if(kind=="cut"){
                if(connection->stage)reject("overlapping cut or outstanding installation");held->descriptor(value,raw.size());
                if(!model_profile_.empty()&&held->frame->models!=expected_models_)reject("issued cut root scope differs from actual selected store");
                held->connection=connection;connection->stage=held;connection->expires=clock_millis()+300000;deadline_millis_=connection->expires.load();
            }else if(kind=="page"){
                if(!connection->stage)reject("page without current descriptor");held->page(value,raw);
            }else reject("unsolicited ordinary control");
            if(held->complete())completed=held;
        }catch(...){if(managed_binding_)managed_binding_->closed(endpoint,lifecycle);connection->failed=true;connection->stage.reset();deadline_millis_=INT64_MAX;throw;}
    }
    return true;
}
#include "ordinary_history_receive_session.inc"

void ordinary_receiver_session::install(const std::shared_ptr<ordinary_received_cut>& cut,owned_platform_sync_transport& transport){
    if(!cut||cut->consumed.exchange(true,std::memory_order_acq_rel))reject("cut installation already consumed");
    const auto connection=cut->connection.lock();if(!live(connection)){if(connection)closed(connection->endpoint,connection->lifecycle);return;}
    {std::lock_guard lock(mutex_);if(current_!=connection||connection->stage!=cut)reject("cut no longer belongs to physical session");}
    const auto owner=owner_.lock();if(!owner){closed(connection->endpoint,connection->lifecycle);return;}
    try{
        // Private constructor is reachable only after actual physical-session
        // admission and complete bounded validation. Public bytes/DTOs cannot
        // take this path; the owned engine independently checks local schema,
        // durable source/frontier, conflicts and actual COMMIT classification.
        owned_server_export issued(cut->frame);
        ordinary_managed_wss_acceptance accepted;
        (void)ordinary_export_access::apply_session(*owner,issued,channel_,managed_binding_?&accepted:nullptr);
        if(!live(connection)){closed(connection->endpoint,connection->lifecycle);return;}
        if(managed_binding_)managed_binding_->accepted(connection->endpoint,connection->lifecycle,std::move(accepted));
        // Publish completed installation before foreign send. A synchronous
        // receipt send may reenter with the next cut; it must see an idle
        // receiver while this local variable still retains the completed cut.
        {std::lock_guard lock(mutex_);if(current_!=connection||connection->stage!=cut)reject("installed cut lost its session");connection->stage.reset();connection->expires=INT64_MAX;deadline_millis_=INT64_MAX;}
        const auto& image=*cut->frame;
        const auto receipt=json{{"latticeOrdinarySync",{{"version",2},{"kind","receipt"},{"source",image.source},{"epoch",std::to_string(image.epoch)},{"cut",std::to_string(image.cut)},{"contentDigest",cut->expected_digest}}}}.dump();
        if(!transport.send_to_attempt(connection->endpoint,transport_message::from_string(receipt)))reject("committed cut receipt send refused");
    }catch(...){if(managed_binding_)managed_binding_->closed(connection->endpoint,connection->lifecycle);connection->failed=true;{std::lock_guard lock(mutex_);if(connection->stage==cut)connection->stage.reset();if(current_==connection)deadline_millis_=INT64_MAX;}throw;}
}
}
