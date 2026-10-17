#pragma once
#include "ordinary_history_receive.hpp"
#include "ordinary_export_fingerprint.hpp"
// Pure exact record-key/order/seal recipe. Bounded DATA; no native/source grant.
namespace lattice::detail {
struct ordinary_history_record_key_recipe final {
private:
    [[noreturn]] static void refuse(const char* reason){throw db_error(std::string("ordinary owned export: ")+reason);}
    static const std::string& text(const column_value_t& v){const auto* p=std::get_if<std::string>(&v);if(!p)refuse("expected exact text storage");return *p;}
public:
static std::vector<column_value_t> key(const ordinary_history_descriptor& header,const ordinary_history_decoded_page& page,size_t at,ordinary_history_progress& next){
    const auto& record=page.records.at(at);std::vector<column_value_t> key{header.stage,int64_t{0},std::string{},std::string{},int64_t(page.index),int64_t(at)};
    if(const auto* row=std::get_if<ordinary_history_row>(&record)){
        if(row->table>=header.projection.tables.size()||int64_t(row->table)<next.last_table||next.known||next.unknown)refuse("history staged row order differs");next.last_table=int64_t(row->table);
        key[2]=header.projection.tables[row->table].name;key[3]=text(row->values.at(0));++next.rows;
    }else if(const auto* known=std::get_if<ordinary_history_known>(&record)){
        if(next.rows!=header.rows)refuse("history original before complete rows");key[1]=int64_t{1};key[3]=known->entry.global_id;++next.known;
    }else {
        if(next.rows!=header.rows)refuse("history unknown before complete rows");key[1]=int64_t{2};key[3]=std::to_string(page.index)+':'+std::to_string(at);++next.unknown;
    }
    key.emplace_back(ordinary_history_storage_seal("ordinary-history-receive-key-v1",key,6));return key;
}
};
}
