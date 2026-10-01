#pragma once

#include <lattice/lattice.hpp>
#include <nlohmann/json.hpp>
#include <set>
#include <string>
#include <utility>
#include <vector>

// Private numeric validation only. This owns no source, row or delivery
// authority and is never retained beyond one owned export selection.
namespace lattice::detail::recovery_upload_json {
struct bounds {
    size_t entries,wire,scalar,nodes,depth,deletes;
};

inline bool parse_envelope(const std::string& wire,size_t entries,const bounds& caps,
                           size_t prior_member_events,size_t& total_events,std::string& reason) {
    using json=nlohmann::json;
    struct limit {};
    size_t nodes=prior_member_events;std::vector<std::set<std::string>> keys;
    try {
        const auto parsed=json::parse(wire,[&](int depth,json::parse_event_t event,json& value){
            if(depth<0||static_cast<size_t>(depth)>caps.depth){reason="parser depth";throw limit{};}
            // Actual negotiated caps are clamped to 32768. The comparison
            // before increment also makes the private helper overflow-safe.
            if(nodes>=caps.nodes){reason="parser events";throw limit{};}++nodes;
            if(value.is_string()&&value.get_ref<const std::string&>().size()>caps.scalar){reason="decoded scalar bytes";throw limit{};}
            if(event==json::parse_event_t::object_start)keys.emplace_back();
            if(event==json::parse_event_t::key&&(keys.empty()||!keys.back().insert(value.get<std::string>()).second))
                throw db_error("negotiated export duplicate JSON key");
            if(event==json::parse_event_t::object_end)keys.pop_back();return true;
        });
        if(!parsed.is_object()||parsed.size()!=1||!parsed.contains("auditLog")||
           !parsed.at("auditLog").is_array()||parsed.at("auditLog").size()!=entries)
            throw db_error("negotiated export envelope differs");
    }catch(const limit&){return false;}
    total_events=nodes;return true;
}

inline bool fits(const std::string& wire,size_t entries,size_t deletes,
                 const bounds& caps,std::string& reason) {
    if(entries>caps.entries){reason="entry count";return false;}
    if(wire.size()>caps.wire){reason="wire bytes";return false;}
    if(deletes>caps.deletes){reason="delete count";return false;}
    size_t events=0;return parse_envelope(wire,entries,caps,0,events,reason);
}

class prefix {
    const bounds caps_;
    std::string encoded_="{\"auditLog\":[";
    size_t entries_=0,deletes_=0,member_events_=0;
public:
    explicit prefix(bounds caps):caps_(caps){}
    bool append(const std::string& member,bool deleted,std::string& reason) {
        const size_t suffix=entries_?3:2; // optional comma plus final ]}
        // Match the original adapter's wire-first refusal, without addition
        // overflow and without copying any accepted member's bytes.
        if(encoded_.size()>caps_.wire||suffix>caps_.wire-encoded_.size()||
           member.size()>caps_.wire-encoded_.size()-suffix){reason="wire bytes";return false;}
        if(entries_>=caps_.entries){reason="entry count";return false;}
        if(deleted&&deletes_>=caps_.deletes){reason="delete count";return false;}
        std::string single="{\"auditLog\":[";single+=member;single+="]}";
        size_t events=0;
        const auto parse_original_context=[&] {
            auto complete=encoded_;if(entries_)complete+=',';complete+=member;complete+="]}";
            return parse_envelope(complete,entries_+1,caps_,0,events,reason);
        };
        try {
            if(!parse_envelope(single,1,caps_,member_events_,events,reason))return false;
        }catch(const db_error&) {
            // An empty member alone is an envelope-count error, but after an
            // accepted member it is a trailing-comma syntax error. Preserve
            // the original parser's context and error precedence on failure.
            if(!entries_)throw;
            if(!parse_original_context())return false;
        }catch(const nlohmann::json::exception&) {
            if(!entries_)throw;
            if(!parse_original_context())return false;
        }
        // The mounted callback emits object start, auditLog key, array start,
        // array end, object end. Container construction skips value callbacks.
        // The wrapper preserves every member depth and duplicate-key scope.
        constexpr size_t envelope_events=5;
        if(events<envelope_events)throw db_error("negotiated export envelope event accounting differs");
        if(entries_)encoded_+=',';encoded_+=member;
        ++entries_;deletes_+=deleted;member_events_=events-envelope_events;return true;
    }
    const std::string& open_bytes()const noexcept{return encoded_;}
    size_t entries()const noexcept{return entries_;}
    size_t deletes()const noexcept{return deletes_;}
    size_t member_events()const noexcept{return member_events_;}
    std::string release_open()&&{return std::move(encoded_);}
};
}
