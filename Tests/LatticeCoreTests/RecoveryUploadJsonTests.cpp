#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/recovery_upload_json.hpp"
#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace {
namespace upload=lattice::detail::recovery_upload_json;
using json=nlohmann::json;
using lattice::db_error;

// Frozen b14 receiver_upload_view::fits body, deliberately not delegated to
// the implementation under test. Only the containing class name differs.
struct original_full_checker {
    size_t entries_,wire_,scalar_,nodes_,depth_,deletes_;
    explicit original_full_checker(upload::bounds b):entries_(b.entries),wire_(b.wire),scalar_(b.scalar),nodes_(b.nodes),depth_(b.depth),deletes_(b.deletes){}
    [[noreturn]] static void reject(const char* message){throw db_error(message);}
bool fits(const std::string& wire,size_t entries,size_t deletes,std::string& reason)const {
    if(entries>entries_){reason="entry count";return false;}
    if(wire.size()>wire_){reason="wire bytes";return false;}
    if(deletes>deletes_){reason="delete count";return false;}
    // Use the exact mounted parser event semantics on the complete envelope.
    // Only a bound refusal is a fitting-prefix decision. JSON/read/provenance
    // errors still propagate; they can never prove absence or a safe prefix.
    struct limit {};
    size_t nodes=0;std::vector<std::set<std::string>> keys;
    try {
        const auto parsed=json::parse(wire,[&](int depth,json::parse_event_t event,json& value){
            if(depth<0||static_cast<size_t>(depth)>depth_){reason="parser depth";throw limit{};}
            if(++nodes>nodes_){reason="parser events";throw limit{};}
            if(value.is_string()&&value.get_ref<const std::string&>().size()>scalar_){reason="decoded scalar bytes";throw limit{};}
            if(event==json::parse_event_t::object_start)keys.emplace_back();
            if(event==json::parse_event_t::key&&(keys.empty()||!keys.back().insert(value.get<std::string>()).second))reject("negotiated export duplicate JSON key");
            if(event==json::parse_event_t::object_end)keys.pop_back();return true;
        });
        if(!parsed.is_object()||parsed.size()!=1||!parsed.contains("auditLog")||!parsed.at("auditLog").is_array()||parsed.at("auditLog").size()!=entries)
            reject("negotiated export envelope differs");
    }catch(const limit&){return false;}
    return true;
}
};
upload::bounds generous(){return {1000,8*1024*1024,1024*1024,32768,16,1000};}
struct member {std::string bytes;bool deleted=false;};
struct measured {size_t events=0,depth=0,scalar=0;std::vector<json::parse_event_t> order;};
measured measure(const std::string& wire){
    measured result;
    (void)json::parse(wire,[&](int depth,json::parse_event_t event,json& value){
        ++result.events;result.depth=std::max(result.depth,static_cast<size_t>(depth));result.order.push_back(event);
        if(value.is_string())result.scalar=std::max(result.scalar,value.get_ref<const std::string&>().size());return true;
    });return result;
}
std::string envelope(const std::vector<member>& members){
    std::string result="{\"auditLog\":[";
    for(size_t i=0;i<members.size();++i){if(i)result+=',';result+=members[i].bytes;}return result+"]}";
}
// This independently retains the old adapter's wire-first check and complete
// growing-prefix parse. It is an oracle, not a performance implementation.
struct original_prefix {
    upload::bounds caps;std::string encoded="{\"auditLog\":[";size_t count=0,deletes=0;
    bool append(const std::string& value,bool deleted,std::string& reason){
        const size_t overhead=encoded.size()+(count?1:0)+2;
        if(overhead>caps.wire||value.size()>caps.wire-overhead){reason="wire bytes";return false;}
        auto candidate=encoded;if(count)candidate+=',';candidate+=value;candidate+="]}";
        const size_t next_deletes=deletes+deleted;
        if(!original_full_checker(caps).fits(candidate,count+1,next_deletes,reason))return false;
        encoded.assign(candidate.data(),candidate.size()-2);deletes=next_deletes;++count;return true;
    }
};
enum class result_kind {accepted,refused,database_error,json_error,other_error};
struct result {result_kind kind;std::string reason,detail;int json_id=0;};
template<class F> result capture(F&& operation){
    std::string reason="untouched";
    try{return {operation(reason)?result_kind::accepted:result_kind::refused,reason,{},0};}
    catch(const db_error& error){return {result_kind::database_error,reason,error.what(),0};}
    catch(const json::exception& error){return {result_kind::json_error,reason,{},error.id};}
    catch(const std::exception& error){return {result_kind::other_error,reason,error.what(),0};}
}
void same_result(const result& actual,const result& expected){
    EXPECT_EQ(actual.kind,expected.kind);EXPECT_EQ(actual.reason,expected.reason);
    EXPECT_EQ(actual.detail,expected.detail);EXPECT_EQ(actual.json_id,expected.json_id);
    EXPECT_NE(actual.kind,result_kind::other_error);
}
// Stop at the first refusal/exception, as the real ordered adapter does.
// Return its index to make first-refusal behavior part of each caller's oracle.
size_t compare_prefix(const std::vector<member>& members,upload::bounds caps){
    upload::prefix actual(caps);original_prefix original{caps};size_t stopped=members.size();
    EXPECT_EQ(actual.open_bytes(),original.encoded);EXPECT_EQ(actual.entries(),0u);EXPECT_EQ(actual.deletes(),0u);EXPECT_EQ(actual.member_events(),0u);
    for(size_t i=0;i<members.size();++i){
        SCOPED_TRACE(i);const auto bytes=actual.open_bytes();const auto entries=actual.entries(),deletes=actual.deletes(),events=actual.member_events();
        const auto expected=capture([&](auto& reason){return original.append(members[i].bytes,members[i].deleted,reason);});
        const auto observed=capture([&](auto& reason){return actual.append(members[i].bytes,members[i].deleted,reason);});
        same_result(observed,expected);EXPECT_EQ(actual.open_bytes(),original.encoded);EXPECT_EQ(actual.entries(),original.count);EXPECT_EQ(actual.deletes(),original.deletes);
        if(expected.kind!=result_kind::accepted){
            EXPECT_EQ(actual.open_bytes(),bytes);EXPECT_EQ(actual.entries(),entries);EXPECT_EQ(actual.deletes(),deletes);EXPECT_EQ(actual.member_events(),events);stopped=i;break;
        }
        const auto closed=actual.open_bytes()+"]}";const auto measured=measure(closed);
        EXPECT_GE(measured.events,5u);if(measured.events<5)return i;
        EXPECT_EQ(actual.member_events(),measured.events-5);
        same_result(capture([&](auto& reason){return upload::fits(closed,actual.entries(),actual.deletes(),caps,reason);}),
                    capture([&](auto& reason){return original_full_checker(caps).fits(closed,original.count,original.deletes,reason);}));
    }
    const auto open=actual.open_bytes();EXPECT_EQ(std::move(actual).release_open(),open);return stopped;
}

void compare_full(const std::string& wire,size_t entries,size_t deletes,upload::bounds caps){
    same_result(capture([&](auto& reason){return upload::fits(wire,entries,deletes,caps,reason);}),
                capture([&](auto& reason){return original_full_checker(caps).fits(wire,entries,deletes,reason);}));
}
std::vector<member> varied(){return {
    {R"({"operation":"INSERT","text":"snowman \u2603 / pair \ud83d\ude80","nested":{"same":1,"array":[true,null,{"same":"x"}]}})"},
    {R"({"operation":"DELETE","embedded\u0000key":"zero\u0000byte","controls":"\b\f\n\r\t\"\\"})",true},
    {R"([1,-0,1.25,1e2,{"same":2},[[],{}]])"},
    {"null"},{"true"},{"-9223372036854775808"},{"18446744073709551615"},{"\"UTF-8 é 🚀\""}
};}
}

TEST(RecoveryUploadJson, MountedParserHasExactlyFiveEnvelopeEvents) {
    const auto empty=measure(envelope({}));
    EXPECT_EQ(empty.events,5u);
    EXPECT_EQ(empty.order,(std::vector<json::parse_event_t>{json::parse_event_t::object_start,
        json::parse_event_t::key,json::parse_event_t::array_start,json::parse_event_t::array_end,
        json::parse_event_t::object_end}));
    const auto members=varied();size_t member_events=0;
    for(const auto& item:members){const auto one=measure(envelope({item}));ASSERT_GE(one.events,5u);member_events+=one.events-5;}
    EXPECT_EQ(measure(envelope(members)).events,member_events+5);
    EXPECT_EQ(compare_prefix(members,generous()),members.size());
}

TEST(RecoveryUploadJson, EveryEventBoundaryMatchesOriginalGrowingPrefix) {
    const auto members=varied();const auto total=measure(envelope(members)).events;
    for(size_t nodes=0;nodes<=total+1;++nodes){SCOPED_TRACE(nodes);auto caps=generous();caps.nodes=nodes;compare_prefix(members,caps);}
    // Refuse on either closing callback of the second otherwise valid member.
    const std::vector<member> objects{{"{}"},{"{}"}};
    auto caps=generous();caps.nodes=7;EXPECT_EQ(compare_prefix(objects,caps),1u);
    caps.nodes=8;EXPECT_EQ(compare_prefix(objects,caps),1u);
    caps.nodes=9;EXPECT_EQ(compare_prefix(objects,caps),2u);
}

TEST(RecoveryUploadJson, EveryWireAndEntryBoundaryPreservesFirstRefusal) {
    const auto members=varied();const auto bytes=envelope(members).size();
    for(size_t wire=0;wire<=bytes+1;++wire){SCOPED_TRACE(wire);auto caps=generous();caps.wire=wire;compare_prefix(members,caps);}
    for(size_t entries=0;entries<=members.size()+1;++entries){SCOPED_TRACE(entries);auto caps=generous();caps.entries=entries;
        EXPECT_EQ(compare_prefix(members,caps),std::min(entries,members.size()));}
}

TEST(RecoveryUploadJson, DepthAndDecodedScalarBoundariesIncludeEnvelopeAndEscapes) {
    const auto members=varied();const auto metrics=measure(envelope(members));
    for(size_t depth=0;depth<=metrics.depth+1;++depth){SCOPED_TRACE(depth);auto caps=generous();caps.depth=depth;compare_prefix(members,caps);}
    for(size_t scalar=0;scalar<=metrics.scalar+1;++scalar){SCOPED_TRACE(scalar);auto caps=generous();caps.scalar=scalar;compare_prefix(members,caps);}
    auto caps=generous();caps.scalar=7;EXPECT_EQ(compare_prefix({{"null"}},caps),0u);
    caps.scalar=8;EXPECT_EQ(compare_prefix({{"null"}},caps),1u); // Literal auditLog key is eight bytes.
}

TEST(RecoveryUploadJson, DeleteCountsAndCombinedLimitsPreserveOriginalOrdering) {
    const std::vector<member> members{{"{}",false},{"null",true},{"[]",false},{"true",true},{"0",true}};
    for(size_t deletes=0;deletes<=4;++deletes){auto caps=generous();caps.deletes=deletes;SCOPED_TRACE(deletes);compare_prefix(members,caps);}
    auto caps=generous();caps.wire=0;caps.entries=0;caps.deletes=0;caps.nodes=0;
    upload::prefix value(caps);std::string reason;
    EXPECT_FALSE(value.append("{bad",true,reason));EXPECT_EQ(reason,"wire bytes");
    caps.wire=1024;upload::prefix entry_limited(caps);
    EXPECT_FALSE(entry_limited.append("{bad",true,reason));EXPECT_EQ(reason,"entry count");
    caps.entries=1;upload::prefix delete_limited(caps);
    EXPECT_FALSE(delete_limited.append("{bad",true,reason));EXPECT_EQ(reason,"delete count");
    for(size_t nodes=0;nodes<20;++nodes)for(size_t depth=0;depth<4;++depth){
        caps=generous();caps.nodes=nodes;caps.depth=depth;caps.scalar=8;caps.entries=3;caps.deletes=1;caps.wire=40;
        SCOPED_TRACE(nodes);
        SCOPED_TRACE(depth);compare_prefix(members,caps);
    }
}

TEST(RecoveryUploadJson, FalseAppendDoesNotChangePrefixAndSmallerRetryRemainsValid) {
    auto caps=generous();caps.wire=32;upload::prefix actual(caps);original_prefix original{caps};
    const std::vector<member> attempts{{"{}"},{"\"this member cannot fit the selected prefix\""},{"null"}};
    for(size_t i=0;i<attempts.size();++i){const auto before=actual.open_bytes();const auto count=actual.entries(),events=actual.member_events();
        const auto expected=capture([&](auto& reason){return original.append(attempts[i].bytes,false,reason);});
        const auto observed=capture([&](auto& reason){return actual.append(attempts[i].bytes,false,reason);});same_result(observed,expected);
        EXPECT_EQ(observed.kind,i==1?result_kind::refused:result_kind::accepted);
        EXPECT_EQ(actual.open_bytes(),original.encoded);EXPECT_EQ(actual.entries(),original.count);
        if(i==1){EXPECT_EQ(actual.open_bytes(),before);EXPECT_EQ(actual.entries(),count);EXPECT_EQ(actual.member_events(),events);}
    }
    EXPECT_EQ(actual.entries(),2u);EXPECT_EQ(actual.open_bytes()+"]}",envelope({{"{}"},{"null"}}));
}

TEST(RecoveryUploadJson, DuplicateDecodedKeysRemainErrorsInEachMemberScope) {
    const std::vector<std::string> duplicates{
        R"({"key":1,"\u006bey":2})",R"({"zero\u0000key":1,"zero\u0000key":2})",
        R"({"outer":{"é":1,"\u00e9":2}})"
    };
    for(const auto& invalid:duplicates){
        EXPECT_EQ(compare_prefix({{invalid}},generous()),0u);
        EXPECT_EQ(compare_prefix({{R"({"key":0})"},{invalid}},generous()),1u);
        upload::prefix value(generous());std::string reason;EXPECT_THROW(value.append(invalid,false,reason),db_error);
        for(size_t nodes=0;nodes<24;++nodes){auto caps=generous();caps.nodes=nodes;compare_prefix({{"{}"},{invalid}},caps);}
    }
    EXPECT_EQ(compare_prefix({{R"({"same":1})"},{R"({"same":2})"}},generous()),2u);
}

TEST(RecoveryUploadJson, MalformedFragmentsPropagateWithoutBecomingSafePrefixes) {
    std::vector<std::string> invalid{"", "{", "[1,]", "true false", "null,null", "01", "NaN", "1e99999",
        R"("\ud800")",R"("\udc00")",R"(0],"unexpected":1})"};
    invalid.push_back(std::string("\"")+char(0xc0)+char(0xaf)+"\"");
    invalid.push_back(std::string("\"raw")+char(0)+"nul\"");
    for(const auto& fragment:invalid){
        EXPECT_EQ(compare_prefix({{fragment}},generous()),0u);
        EXPECT_EQ(compare_prefix({{"{}"},{fragment}},generous()),1u);
        for(size_t nodes=0;nodes<12;++nodes){auto caps=generous();caps.nodes=nodes;compare_prefix({{"{}"},{fragment}},caps);}
        auto caps=generous();caps.wire=0;compare_prefix({{fragment}},caps);
    }
}

TEST(RecoveryUploadJson, CompleteCheckerRetainsEnvelopeShapeAndCountPrecedence) {
    for(const auto& wire:std::vector<std::string>{"{}","[]","null",R"({"auditLog":null})",
        R"({"auditLog":[],"extra":0})",R"({"auditLog":[],"auditLog":[]})",R"({"auditLog":[null]})"}){
        compare_full(wire,0,0,generous());compare_full(wire,1,0,generous());
        auto caps=generous();caps.entries=0;caps.wire=0;caps.deletes=0;
        std::string reason;EXPECT_FALSE(upload::fits(wire,1,1,caps,reason));EXPECT_EQ(reason,"entry count");
        EXPECT_FALSE(upload::fits(wire,0,1,caps,reason));EXPECT_EQ(reason,"wire bytes");
        caps.wire=1024;EXPECT_FALSE(upload::fits(wire,0,1,caps,reason));EXPECT_EQ(reason,"delete count");
    }
    compare_full(envelope({}),0,0,generous());compare_full(envelope(varied()),varied().size(),1,generous());
}

TEST(RecoveryUploadJson, EmptyLaterMemberRetainsSyntaxErrorAndUnchangedPrefixForRetry) {
    for(const std::string fragment:{std::string{},std::string{" \t\r\n"}}) {
        auto caps=generous();upload::prefix actual(caps);original_prefix original{caps};
        std::string reason;
        ASSERT_TRUE(actual.append("{}",false,reason));ASSERT_TRUE(original.append("{}",false,reason));
        const auto before=actual.open_bytes();const auto events=actual.member_events();
        const auto expected=capture([&](auto& why){return original.append(fragment,false,why);});
        const auto observed=capture([&](auto& why){return actual.append(fragment,false,why);});
        same_result(observed,expected);EXPECT_EQ(observed.kind,result_kind::json_error);EXPECT_EQ(observed.json_id,101);
        EXPECT_EQ(actual.open_bytes(),before);EXPECT_EQ(actual.entries(),1u);EXPECT_EQ(actual.deletes(),0u);EXPECT_EQ(actual.member_events(),events);
        same_result(capture([&](auto& why){return actual.append("null",true,why);}),
                    capture([&](auto& why){return original.append("null",true,why);}));
        EXPECT_EQ(actual.open_bytes(),original.encoded);EXPECT_EQ(actual.entries(),2u);EXPECT_EQ(actual.deletes(),1u);
        for(size_t nodes=0;nodes<16;++nodes){caps=generous();caps.nodes=nodes;compare_prefix({{"{}"},{fragment}},caps);}
    }
}
