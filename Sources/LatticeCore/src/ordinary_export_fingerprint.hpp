#pragma once
#include <lattice/sync.hpp>
#include "canonical_ready_sha256.hpp"
#include <array>
#include <bit>
#include <algorithm>
#include <string_view>
// Exact normal source fingerprint producer, extracted without output change.
namespace lattice::detail {
inline uint8_t ordinary_export_scalar_tag(const column_value_t& value) {
    if(std::holds_alternative<std::nullptr_t>(value))return 0;
    if(std::holds_alternative<int64_t>(value))return 1;
    if(std::holds_alternative<double>(value))return 2;
    if(std::holds_alternative<std::string>(value))return 3;
    return 4;
}
class ordinary_export_fingerprint {
    ready_sha256_state hash_;
public:
    void number(uint64_t value) {
        std::array<uint8_t,8> bytes{};
        for(unsigned i=0;i<8;++i)bytes[i]=static_cast<uint8_t>(value>>(56-8*i));
        hash_.process(std::string_view(reinterpret_cast<const char*>(bytes.data()),bytes.size()));
    }
    template<class T> void bytes(const T& value) {
        // Existing producers supply strings, string views or byte vectors.
        // Consume the borrowed range synchronously; only a partial block is copied.
        static_assert(sizeof(*value.data())==1);
        number(value.size());
        if(!value.empty())hash_.process(std::string_view(reinterpret_cast<const char*>(value.data()),value.size()));
    }
    void scalar(const column_value_t& value) {
        number(ordinary_export_scalar_tag(value));
        if(const auto* p=std::get_if<int64_t>(&value))number(static_cast<uint64_t>(*p));
        else if(const auto* p=std::get_if<double>(&value))number(std::bit_cast<uint64_t>(*p));
        else if(const auto* p=std::get_if<std::string>(&value))bytes(*p);
        else if(const auto* p=std::get_if<std::vector<uint8_t>>(&value))bytes(*p);
    }
    void entry(const audit_log_entry& value,bool include_timestamp=true) {
        bytes(value.global_id);bytes(value.table_name);bytes(value.operation);bytes(value.global_row_id);if(include_timestamp)bytes(value.timestamp);
        std::vector<const changed_fields_map::value_type*> fields;fields.reserve(value.changed_fields.size());
        for(const auto& field:value.changed_fields)fields.push_back(&field);
        std::sort(fields.begin(),fields.end(),[](const auto* a,const auto* b){return a->first<b->first;});
        number(fields.size());for(const auto* field:fields){bytes(field->first);number(static_cast<uint64_t>(field->second.kind));scalar(field->second.value);}
        number(value.changed_fields_names.size());for(const auto& name:value.changed_fields_names)bytes(name);
        number(value.synthesized?1:0);
    }
    void original_identity(const audit_log_entry& value) {
        number(value.original_identity.has_value());
        if(value.original_identity) {
            const auto& identity=*value.original_identity;
            number(static_cast<uint64_t>(identity.version));number(identity.changed_fields_names.size());
            for(const auto& name:identity.changed_fields_names)bytes(name);
            bytes(identity.digest);
        }
    }
    std::string finish(){return picosha2::bytes_to_hex_string(hash_.digest_bytes());}
};
inline std::string ordinary_history_storage_seal(const char* domain,const std::vector<column_value_t>& row,std::size_t count) {
    ordinary_export_fingerprint seal;seal.bytes(std::string_view(domain));seal.number(count);
    for(std::size_t n=0;n<count;++n)seal.scalar(row.at(n));return seal.finish();
}
}
