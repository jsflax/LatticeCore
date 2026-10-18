#pragma once
#include "receive_install_state.hpp"
#include <algorithm>
#include <charconv>
#include <limits>
#include <string_view>

namespace lattice::detail {
// Shared, closed receiver SQL text only. A generated string is DATA: only the
// actual enrollment Reader may bind it to its retained WRITE/operator/proof.
// No table/column identifier or caller SQL is accepted. Storage is selected by
// the real owning producer (standard for normal use, accounted during startup).
struct receive_install_initialization_recipe final {
    template<class Text>static Text decimal(std::int64_t value) {
        char digits[std::numeric_limits<std::int64_t>::digits10+3];
        const auto result=std::to_chars(digits,digits+sizeof(digits),value);
        if(result.ec!=std::errc{})throw receive_install_error(receive_install_error_code::invalid_argument,
            "receiver installation decimal conversion failed");
        return Text(digits,result.ptr);
    }
    template<class Text>static Text integer(std::string_view name) {
        const Text value(name.data(),name.size());
        return "CASE WHEN typeof("+value+")='integer' THEN "+value+" ELSE NULL END AS "+value;
    }
    template<class Text>static Text bounded(std::string_view name,std::int64_t maximum) {
        const Text value(name.data(),name.size());
        return "CASE WHEN typeof("+value+")='blob' AND length("+value+") BETWEEN 1 AND "+
            decimal<Text>(maximum)+" THEN "+value+" ELSE NULL END AS "+value;
    }
    template<class Text>static Text configuration() {
        Text sql="SELECT ";
        for(const char* name:{"id","version","max_channels","max_field_bytes","max_bytes","channels","bytes"}) {
            if(sql!="SELECT ")sql+=",";
            sql+=integer<Text>(name);
        }
        sql+=" FROM main._lattice_install_store LIMIT 2";return sql;
    }
    template<class Text>static Text row_sizes() {
        Text sql="SELECT ";
        for(const char* name:{"channel","authority","source","epoch","scope","schema_digest","active","last_install"}) {
            if(sql!="SELECT ")sql+=",";
            const Text column(name);
            const bool optional=column=="active"||column=="last_install";
            sql+="CASE ";
            if(optional)sql+="WHEN "+column+" IS NULL THEN 0 ";
            sql+="WHEN typeof("+column+")='blob' THEN length("+column+") ELSE -1 END AS "+column;
        }
        sql+=" FROM main._lattice_install_channel WHERE channel=? LIMIT 2";return sql;
    }
    template<class Text>static Text row_values(const receive_install_limits& limits) {
        const auto maximum=std::numeric_limits<std::int64_t>::max();
        if(limits.channels<0||limits.field_bytes<=0||limits.field_bytes>(maximum-176)/16||limits.encoded_bytes<0)
            throw receive_install_error(receive_install_error_code::invalid_argument,
                "receiver installation requires representable explicit limits");
        const auto identity_limit=std::min(std::int64_t(88+4*limits.field_bytes),limits.encoded_bytes);
        Text sql="SELECT active IS NULL AS active_null,last_install IS NULL AS last_null,typeof(frontier) AS frontier_type";
        for(const char* name:{"frontier_kind","frontier","revision","last_sequence","bytes"})sql+=","+integer<Text>(name);
        for(const char* name:{"channel","authority","source","epoch","scope","schema_digest"})sql+=","+bounded<Text>(name,limits.field_bytes);
        sql+=","+bounded<Text>("active",identity_limit)+","+bounded<Text>("last_install",identity_limit)+
            " FROM main._lattice_install_channel WHERE channel=? LIMIT 2";
        return sql;
    }
    template<class Text>static Text aliases(const receive_install_limits& limits) {
        return "SELECT "+bounded<Text>("channel",limits.field_bytes)+
            " FROM main._lattice_install_channel WHERE authority=? AND scope=? ORDER BY channel LIMIT 17";
    }
    static constexpr std::string_view insert_channel()noexcept {
        return "INSERT INTO main._lattice_install_channel VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
    }
    static constexpr std::string_view update_usage()noexcept {
        return "UPDATE main._lattice_install_store SET channels=?,bytes=? WHERE id=1 AND channels=? AND bytes=?";
    }
    static constexpr std::string_view affected_rows()noexcept{return "SELECT changes() AS n";}
};
}
