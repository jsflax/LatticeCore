#include <gtest/gtest.h>
#include <lattice/sync.hpp>
#include <nlohmann/json.hpp>
#include "../../Sources/LatticeCore/src/sync_canonical_range.hpp"
#include <algorithm>
#include <functional>
#include <limits>
#include <set>

namespace cr = lattice::detail::canonical_range;
namespace {
using json=nlohmann::json;
const std::string zero(64,'0');
std::string uuid(char last){return std::string("00000000-0000-4000-8000-00000000000")+last;}
cr::limits budget(){return {{16384,4096,2,8,16,65536,8,16,65536},16,4096,4096,8,16,8192,32768,60000,{4096,16,256,2048,4096}};}
cr::attempt logical(){return {uuid('1'),uuid('2'),"channel-A",5,uuid('5')};}
cr::source_binding source(){return {"authority-A",uuid('3'),uuid('4'),std::string(64,'a'),std::string(64,'b')};}
cr::content_item row(std::string id,std::string payload=R"({"name":{"kind":2,"value":"value"}})"){
    return {{"Person",std::move(id)},cr::present{std::move(payload)}};
}
struct Bundle {
    cr::limits b=budget();cr::attempt a=logical();cr::request r;cr::manifest m;
    std::vector<cr::content_item> rows{row("A"),{{"Person","B"},cr::tombstone{}}};
    std::vector<cr::receipt_item> receipts{{"op-A",cr::committed{"namespace-A","coverage-A",cr::decision::applied,7,cr::identity{"Person","A"}}}};
    Bundle(){r.source=source();r.selection=cr::mode::delta;r.base=10;r.expected={4,r.source,{cr::frontier_kind::position,10}};r.budget=b.maximum;r.receipts={{"op-A",std::string("namespace-A"),{{"Person","A"}}}};m.head=12;m.protection={"lease-A",30000};seal();}
    void seal(){
        r.request_digest=cr::request_sha256(a,r,b);m.request_digest=r.request_digest;m.source=r.source;m.selection=r.selection;m.base=r.base;
        m.counts={};const auto chunk=r.budget.items_per_page;
        m.counts.content_pages=rows.empty()?0:1+(rows.size()-1)/chunk;m.counts.identities=rows.size();
        for(const auto& x:rows){m.counts.content_bytes+=cr::content_record_bytes(x,b);if(std::holds_alternative<cr::present>(x.value))++m.counts.present;else ++m.counts.tombstones;}
        m.counts.receipt_pages=receipts.empty()?0:1+(receipts.size()-1)/chunk;m.counts.receipts=receipts.size();for(const auto& x:receipts)m.counts.receipt_bytes+=cr::receipt_record_bytes(x,b);
        std::set<std::pair<std::string,std::string>> refresh;for(const auto& q:r.receipts)for(const auto& i:q.targets)refresh.emplace(i.table,i.id);
        m.counts.rebase_identities=refresh.size();for(const auto& [table,id]:refresh)m.counts.rebase_bytes+=16+table.size()+id.size();
        m.content_digest=m.receipt_digest=m.rebase_digest=zero;m.rebase_digest=cr::rebase_sha256(a,r,b);
        m.content_digest=cr::content_sha256(m,rows,b);m.receipt_digest=cr::receipts_sha256(m,receipts,b);m.manifest_digest=cr::manifest_sha256(m,b);
    }
    cr::content_page page(size_t first=0,size_t count=0,uint64_t index=0) const {
        if(!count)count=rows.size()-first;cr::content_page p;p.manifest_digest=m.manifest_digest;p.index=index;p.count=count;
        p.items.assign(rows.begin()+first,rows.begin()+first+count);for(const auto& x:p.items)p.bytes+=cr::content_record_bytes(x,b);p.digest=cr::page_sha256(p,b);return p;
    }
    cr::receipt_page receipt_page() const {cr::receipt_page p;p.manifest_digest=m.manifest_digest;p.count=receipts.size();p.items=receipts;for(const auto& x:p.items)p.bytes+=cr::receipt_record_bytes(x,b);p.digest=cr::page_sha256(p,b);return p;}
    cr::sequence_state start() const{return cr::begin(a,r,m,b);}
    cr::sequence_state after_content() const{return cr::propose(start(),{a,1,page()},b);}
    cr::sequence_state finished() const {
        auto s=start();if(!rows.empty())s=cr::propose(s,{a,1,page()},b);if(!receipts.empty())s=cr::propose(s,{a,1,receipt_page()},b);return cr::propose(s,{a,1,cr::end{m.manifest_digest}},b);
    }
};
void exact_error(const std::function<void()>& f,const char* expected){
    try{f();FAIL()<<"expected protocol refusal";}catch(const cr::protocol_error& e){EXPECT_STREQ(e.what(),expected);}
}
json wire(const cr::frame& f,const cr::limits& b){return json::parse(cr::encode(f,b));}
size_t containers(const json& j){size_t child=0;if(j.is_structured())for(const auto& x:j)child=std::max(child,containers(x));return j.is_structured()?1+child:0;}
size_t nodes(const json& j){size_t n=1;if(j.is_object())n+=j.size();if(j.is_structured())for(const auto& x:j)n+=nodes(x);return n;}
}

TEST(SyncCanonicalRange, OwnedFramesRoundTripWithoutLegacyDispatch) {
    Bundle x;const std::vector<cr::message> messages{x.r,x.m,x.page(),x.receipt_page(),cr::end{x.m.manifest_digest}};
    for(const auto& body:messages){const cr::frame f{x.a,1,body};const auto encoded=cr::encode(f,x.b);EXPECT_EQ(cr::decode(encoded,x.b),f);EXPECT_FALSE(lattice::server_sent_event::from_json(encoded));EXPECT_EQ(json::parse(encoded).size(),1u);}
    auto bytes=cr::encode({x.a,1,x.page()},x.b);const auto owned=cr::decode(bytes,x.b);bytes.assign(bytes.size(),'x');EXPECT_EQ(std::get<cr::content_page>(owned.body),x.page());
}

TEST(SyncCanonicalRange, TaggedSequenceIsPureAndOnlyStructurallyComplete) {
    Bundle x;const auto initial=x.start();auto s=cr::propose(initial,{x.a,1,x.page()},x.b);
    EXPECT_EQ(initial.identities,0);EXPECT_EQ(initial.rebase_seen,std::vector<uint8_t>{0});EXPECT_EQ(s.rebase_seen,std::vector<uint8_t>{1});
    s=cr::propose(s,{x.a,1,x.receipt_page()},x.b);s=cr::propose(s,{x.a,1,cr::end{x.m.manifest_digest}},x.b);
    EXPECT_EQ(s.status,cr::phase::sequence_complete_unverified);EXPECT_EQ(s.present_count,1);EXPECT_EQ(s.tombstone_count,1);
    EXPECT_THROW(cr::propose(s,{x.a,1,cr::end{x.m.manifest_digest}},x.b),cr::protocol_error);EXPECT_EQ(initial,x.start());
}

TEST(SyncCanonicalRange, DomainSeparatedHashesMatchIndependentReferenceBytes) {
    // Derived by preparation/core-sync-canonical-range-framing-003/hash-reference.py
    // using Python struct/hashlib and explicit wire-spec bytes, not these helpers.
    Bundle x;
    EXPECT_EQ(x.r.request_digest,"9e29aec12981a5dac685cde9444bef0fa6d295102952234314189b5a59abd8fa");
    EXPECT_EQ(x.m.content_digest,"748c3b98eec56e4e9cc909654e334a37a6c56e0df20fc8a808ce9e2c1de99a03");
    EXPECT_EQ(x.m.receipt_digest,"56e76613b67f5cd51cb776605c5828c49d20dc740195aa1813b2905ce5d35417");
    EXPECT_EQ(x.m.rebase_digest,"e6be1b1d708dbe2548b691dadd6e052606661d05d8991e92699d242e7f38ea8e");
    EXPECT_EQ(x.m.manifest_digest,"822b5b63a82be515da3f398999adfe7c98fdda14d0c86a6280c60219eb4f28df");
    EXPECT_EQ(x.page().digest,"216547b843ddde13215bf84dee4a7849b704452dfeb36d412a4ede2a4da19306");
    EXPECT_EQ(x.receipt_page().digest,"3d631b749b0130df79fe5ffa301000b6e49f1ec206cc5651b389ab92eabb784a");
    EXPECT_EQ(x.m.counts.content_bytes,91);EXPECT_EQ(x.m.counts.receipt_bytes,113);EXPECT_EQ(x.m.counts.rebase_bytes,23);
    auto p=x.page();++p.index;EXPECT_NE(cr::page_sha256(p,x.b),x.page().digest);
    auto m=x.m;++m.head;EXPECT_NE(cr::content_sha256(m,x.rows,x.b),x.m.content_digest);
}

TEST(SyncCanonicalRange, FullAndEmptyRangesRetainExplicitExpectedState) {
    Bundle x;x.r.selection=cr::mode::full;x.r.base.reset();x.rows[1]=row("B");x.seal();
    EXPECT_EQ(x.r.expected.base.value,std::optional<uint64_t>{10});EXPECT_FALSE(x.m.base);EXPECT_EQ(x.finished().status,cr::phase::sequence_complete_unverified);
    x.rows.clear();x.receipts.clear();x.r.receipts.clear();x.seal();EXPECT_EQ(x.m.counts.identities,0);EXPECT_EQ(x.m.counts.content_pages,0);EXPECT_EQ(x.finished().status,cr::phase::sequence_complete_unverified);
    x.m.head=9;x.seal();EXPECT_THROW(x.start(),cr::protocol_error);
}

TEST(SyncCanonicalRange, RequestHashBindsLogicalRevisionAndBudgetButNotRoute) {
    Bundle x;const auto q=x.r.request_digest,m=x.m.manifest_digest;
    auto f=cr::frame{x.a,1,x.r};auto other=f;other.route_generation=99;EXPECT_NE(cr::encode(f,x.b),cr::encode(other,x.b));EXPECT_EQ(std::get<cr::request>(cr::decode(cr::encode(other,x.b),x.b).body).request_digest,q);
    auto a=x.a;++a.sequence;EXPECT_NE(cr::request_sha256(a,x.r,x.b),q);a=x.a;a.attempt_id=uuid('6');EXPECT_NE(cr::request_sha256(a,x.r,x.b),q);
    auto r=x.r;--r.expected.revision;EXPECT_NE(cr::request_sha256(x.a,r,x.b),q);r=x.r;--r.budget.content_bytes;EXPECT_NE(cr::request_sha256(x.a,r,x.b),q);
    // Pure framing intentionally does not authorize route 99. The controller must.
    EXPECT_NO_THROW(cr::propose(x.start(),{x.a,99,x.page()},x.b));EXPECT_EQ(x.m.manifest_digest,m);
    EXPECT_THROW(cr::propose(x.start(),{a,1,x.page()},x.b),cr::protocol_error);
}

TEST(SyncCanonicalRange, NullBeginningAndNumericZeroAreNotInterchangeable) {
    Bundle x;x.r.selection=cr::mode::full;x.r.base.reset();x.r.expected={0,std::nullopt,{cr::frontier_kind::uninitialized,std::nullopt}};x.seal();const auto uninitialized=x.r.request_digest;
    x.r.expected.binding=x.r.source;x.r.expected.base.kind=cr::frontier_kind::beginning_null;x.seal();const auto beginning=x.r.request_digest;
    x.r.expected.base={cr::frontier_kind::position,0};x.r.expected.revision=1;x.seal();EXPECT_NE(uninitialized,beginning);EXPECT_NE(beginning,x.r.request_digest);
    x.r.selection=cr::mode::delta;x.r.base=0;x.seal();EXPECT_NO_THROW(x.start());x.r.base.reset();EXPECT_THROW(cr::request_sha256(x.a,x.r,x.b),cr::protocol_error);
    x.r.base=0;x.r.expected.revision=static_cast<uint64_t>(std::numeric_limits<int64_t>::max());EXPECT_THROW(cr::request_sha256(x.a,x.r,x.b),cr::protocol_error);
}

TEST(SyncCanonicalRange, ExpectedFrontierRevisionPairMustMatchReceiverInitialization) {
    Bundle x;x.r.selection=cr::mode::full;x.r.base.reset();
    const std::vector<cr::expected_install> valid{
        {0,std::nullopt,{cr::frontier_kind::uninitialized,std::nullopt}},
        {0,x.r.source,{cr::frontier_kind::beginning_null,std::nullopt}},
        {1,x.r.source,{cr::frontier_kind::position,0}},
        {2,x.r.source,{cr::frontier_kind::position,0}}
    };
    for(const auto& expected:valid){
        x.r.expected=expected;x.seal();const cr::frame frame{x.a,1,x.r};
        EXPECT_EQ(cr::decode(cr::encode(frame,x.b),x.b),frame);
        EXPECT_EQ(cr::decode_state(cr::encode_state(x.start(),x.b),x.a,x.b),x.start());
    }
    const std::vector<cr::expected_install> invalid{
        {0,x.r.source,{cr::frontier_kind::position,0}},
        {0,x.r.source,{cr::frontier_kind::position,10}},
        {1,x.r.source,{cr::frontier_kind::beginning_null,std::nullopt}},
        {1,std::nullopt,{cr::frontier_kind::uninitialized,std::nullopt}}
    };
    for(const auto& expected:invalid){
        auto request=x.r;request.expected=expected;
        exact_error([&]{(void)cr::request_sha256(x.a,request,x.b);},"expected frontier contradicts installation revision");
    }
    auto encoded=wire({x.a,1,x.r},x.b);
    encoded["latticeCanonicalRange"]["body"]["expected_install"]["revision"]="0";
    exact_error([&]{(void)cr::decode(encoded.dump(),x.b);},"expected frontier contradicts installation revision");
    auto restart=json::parse(cr::encode_state(x.start(),x.b));
    restart["latticeCanonicalRangeState"]["request"]["expected_install"]["revision"]="0";
    exact_error([&]{(void)cr::decode_state(restart.dump(),x.a,x.b);},"expected frontier contradicts installation revision");
}

TEST(SyncCanonicalRange, SameHeadRefreshKeepsBeforeBaseReceiptAndChangesRevisionIdentity) {
    Bundle x;x.rows.resize(1);x.m.head=10;x.seal();const auto first=x.finished();EXPECT_EQ(first.offer.base,std::optional<uint64_t>{first.offer.head});
    EXPECT_LT(std::get<cr::committed>(x.receipts[0].value).position,*x.m.base);const auto q=x.r.request_digest;
    ++x.a.sequence;++x.r.expected.revision;x.seal();EXPECT_NE(q,x.r.request_digest);EXPECT_EQ(x.finished().offer.head,first.offer.head);
    x.rows.push_back(row("B"));x.seal();exact_error([&]{(void)x.after_content();},"same-head refresh contains identity outside request union");
}

TEST(SyncCanonicalRange, DurableAttemptSequenceExceedsRevisionAndAllowsFailedAttemptGaps) {
    Bundle x;const auto first=x.r.request_digest;
    x.a.sequence=9;x.seal();EXPECT_NE(x.r.request_digest,first);
    EXPECT_EQ(x.finished().status,cr::phase::sequence_complete_unverified);
    for(const auto sequence:{x.r.expected.revision,x.r.expected.revision-1}){
        auto attempt=x.a;attempt.sequence=sequence;
        exact_error([&]{(void)cr::request_sha256(attempt,x.r,x.b);},"logical attempt sequence must exceed expected revision");
        auto encoded=wire({x.a,1,x.r},x.b);
        encoded["latticeCanonicalRange"]["attempt"]["sequence"]=std::to_string(sequence);
        exact_error([&]{(void)cr::decode(encoded.dump(),x.b);},"logical attempt sequence must exceed expected revision");
        auto restart=json::parse(cr::encode_state(x.start(),x.b));
        restart["latticeCanonicalRangeState"]["attempt"]["sequence"]=std::to_string(sequence);
        exact_error([&]{(void)cr::decode_state(restart.dump(),attempt,x.b);},"logical attempt sequence must exceed expected revision");
    }
}

TEST(SyncCanonicalRange, MissingReceiptRefreshIdentityAndExtraFullTombstoneRefuse) {
    Bundle x;x.rows={row("B"),row("C")};x.seal();exact_error([&]{(void)x.after_content();},"missing or impossible rebase identity");
    x=Bundle{};x.r.selection=cr::mode::full;x.r.base.reset();x.seal();exact_error([&]{(void)x.after_content();},"full snapshot tombstone is not a requested refresh");
    x.r.receipts[0].targets.push_back({"Person","B"});x.seal();EXPECT_NO_THROW(x.finished());
}

TEST(SyncCanonicalRange, ReceiptRequestCoverageIsCompleteAndNamespaceBound) {
    Bundle x;const auto before=x.after_content();auto p=x.receipt_page();p.items[0].original_id="op-B";p.digest=cr::page_sha256(p,x.b);
    exact_error([&]{(void)cr::propose(before,{x.a,1,p},x.b);},"receipt does not cover exact requested ID");EXPECT_EQ(before.receipt_count,0);
    p=x.receipt_page();std::get<cr::committed>(p.items[0].value).namespace_id="namespace-B";p.digest=cr::page_sha256(p,x.b);
    exact_error([&]{(void)cr::propose(before,{x.a,1,p},x.b);},"receipt namespace is not negotiated request namespace");
    EXPECT_THROW(cr::propose(x.start(),{x.a,1,x.receipt_page()},x.b),cr::protocol_error);
    EXPECT_THROW(cr::propose(before,{x.a,1,cr::end{x.m.manifest_digest}},x.b),cr::protocol_error);
}

TEST(SyncCanonicalRange, OriginalReceiptTargetAndAcceptancePositionCannotBeReinterpreted) {
    Bundle x;const auto before=x.after_content();auto p=x.receipt_page();std::get<cr::committed>(p.items[0].value).accepted_target=cr::identity{"Person","B"};p.digest=cr::page_sha256(p,x.b);
    exact_error([&]{(void)cr::propose(before,{x.a,1,p},x.b);},"stored receipt target differs from request");
    p=x.receipt_page();std::get<cr::committed>(p.items[0].value).position=13;p.digest=cr::page_sha256(p,x.b);
    exact_error([&]{(void)cr::propose(before,{x.a,1,p},x.b);},"receipt accepted after captured head");
    std::get<cr::committed>(x.receipts[0].value).accepted_target.reset();x.seal();EXPECT_NO_THROW(x.finished());
}

TEST(SyncCanonicalRange, UnknownNeverBecomesCoveredNegativeByMissingFields) {
    Bundle x;x.r.receipts[0].namespace_id.reset();x.receipts[0].value=cr::unknown{cr::unknown_reason::legacy};x.seal();EXPECT_NO_THROW(x.finished());
    auto j=wire({x.a,1,x.receipt_page()},x.b);auto& item=j["latticeCanonicalRange"]["body"]["items"][0];item["status"]="not_committed";
    EXPECT_THROW(cr::decode(j.dump(),x.b),cr::protocol_error);
    x.receipts[0].value=cr::not_committed{"namespace-A","coverage-A"};x.seal();exact_error([&]{(void)x.finished();},"receipt namespace is not negotiated request namespace");
    x.r.receipts[0].namespace_id="namespace-A";x.seal();EXPECT_NO_THROW(x.finished());
}

TEST(SyncCanonicalRange, CrossPageIdentityAndOrderAreStrictWithoutMutatingInput) {
    Bundle x;x.r.budget.items_per_page=1;x.seal();const auto initial=x.start();auto first=x.page(0,1,0),second=x.page(1,1,1);
    auto s=cr::propose(initial,{x.a,1,first},x.b);EXPECT_THROW(cr::propose(s,{x.a,1,first},x.b),cr::protocol_error);
    auto duplicate=first;duplicate.index=1;duplicate.digest=cr::page_sha256(duplicate,x.b);EXPECT_THROW(cr::propose(s,{x.a,1,duplicate},x.b),cr::protocol_error);
    EXPECT_NO_THROW(cr::propose(s,{x.a,1,second},x.b));EXPECT_EQ(s.identities,1);EXPECT_EQ(initial.identities,0);
}

TEST(SyncCanonicalRange, SameCountRepartitionKeepsManifestButExactPageBytesDiffer) {
    Bundle x;x.rows={row("A"),row("B"),row("C")};x.seal();ASSERT_EQ(x.m.counts.content_pages,2);
    auto one=x.page(0,1,0),two=x.page(0,2,0);EXPECT_NE(one.digest,two.digest);const auto m=x.m.manifest_digest;
    auto s1=cr::propose(x.start(),{x.a,1,one},x.b);auto s2=cr::propose(x.start(),{x.a,1,two},x.b);
    s1=cr::propose(s1,{x.a,1,x.page(1,2,1)},x.b);s2=cr::propose(s2,{x.a,1,x.page(2,1,1)},x.b);EXPECT_EQ(s1,s2);EXPECT_EQ(x.m.manifest_digest,m);
    // A durable adapter must compare retained page bytes; pure sequence refuses an old index.
    EXPECT_THROW(cr::propose(s1,{x.a,1,two},x.b),cr::protocol_error);
    auto changed=x.m;changed.counts.content_pages=3;EXPECT_NE(cr::manifest_sha256(changed,x.b),m);
}

TEST(SyncCanonicalRange, StrictPayloadPreservesNulBlobAndSQLiteKinds) {
    Bundle x;x.rows[0]=row("A",R"({"blob":{"kind":6,"value":"00ff"},"n":{"kind":1,"value":-9},"real":{"kind":7,"value":1.25},"text":{"kind":2,"value":"a\u0000b"},"z":{"kind":4,"value":null}})");x.seal();
    const auto f=cr::decode(cr::encode({x.a,1,x.page()},x.b),x.b);EXPECT_EQ(std::get<cr::content_page>(f.body).items[0],x.rows[0]);
    const auto& payload=std::get<cr::present>(x.rows[0].value).payload;const auto values=lattice::detail::sync_recovery::decode_values(payload,x.b.values);
    EXPECT_EQ(std::get<std::string>(values.at("text")),std::string("a\0b",3));EXPECT_EQ(std::get<std::vector<uint8_t>>(values.at("blob")),(std::vector<uint8_t>{0,255}));EXPECT_NO_THROW(x.finished());
    for(const auto* invalid:{R"({"x":{"kind":2,"value":"a"},"x":{"kind":2,"value":"b"}})",R"({"x":{"kind":1,"value":9223372036854775808}})",R"({"x":{"kind":6,"value":"0F"}})",R"({"x":{"kind":7,"value":1e999}})",R"({"x":{"kind":99,"value":null}})"})EXPECT_THROW(cr::content_record_bytes(row("A",invalid),x.b),cr::protocol_error);
}

TEST(SyncCanonicalRange, ValidWireHasIndependentRawDepthNodeAndDecodedStringGuards) {
    Bundle x;x.rows[0]=row("A",std::string("{\"name\":{\"kind\":2,\"value\":\"")+std::string(128,'q')+"\"}}");x.seal();const auto encoded=cr::encode({x.a,1,x.page()},x.b);const auto parsed=json::parse(encoded);
    ASSERT_NO_THROW(cr::decode(encoded,x.b));auto b=x.b;b.maximum.frame_bytes=encoded.size()-1;b.maximum.payload_bytes=std::min<uint64_t>(b.maximum.payload_bytes,b.maximum.frame_bytes);b.string_bytes=std::min<uint64_t>(b.string_bytes,b.maximum.frame_bytes);
    exact_error([&]{(void)cr::decode(encoded,b);},"raw canonical frame exceeds budget");
    b=x.b;b.depth=containers(parsed)-1;exact_error([&]{(void)cr::decode(encoded,b);},"invalid or over-budget canonical JSON");
    b=x.b;b.nodes=nodes(parsed)-1;exact_error([&]{(void)cr::decode(encoded,b);},"invalid or over-budget canonical JSON");
    b=x.b;b.string_bytes=64;exact_error([&]{(void)cr::decode(encoded,b);},"invalid or over-budget canonical JSON");
    auto escaped=encoded;const auto start=escaped.find(std::string(128,'q'));ASSERT_NE(start,std::string::npos);escaped.replace(start,128,std::string(128,'q')); // positive literal control
    std::string escaped_run;for(int i=0;i<128;++i)escaped_run+="\\u0071";escaped.replace(start,128,escaped_run);
    ASSERT_EQ(cr::decode(escaped,x.b),cr::decode(encoded,x.b));exact_error([&]{(void)cr::decode(escaped,b);},"invalid or over-budget canonical JSON");
}

TEST(SyncCanonicalRange, DuplicateKeysUnknownFieldsVersionsAndLegacyRootsRefuse) {
    Bundle x;const auto encoded=cr::encode({x.a,1,x.m},x.b);auto duplicate=encoded;const auto pos=duplicate.find("\"version\":2");ASSERT_NE(pos,std::string::npos);duplicate.replace(pos,11,"\"version\":2,\"version\":2");
    exact_error([&]{(void)cr::decode(duplicate,x.b);},"invalid or over-budget canonical JSON");
    auto j=json::parse(encoded);j["latticeCanonicalRange"]["body"]["ack"]=true;EXPECT_THROW(cr::decode(j.dump(),x.b),cr::protocol_error);
    j=json::parse(encoded);j["latticeCanonicalRange"]["version"]=1;EXPECT_THROW(cr::decode(j.dump(),x.b),cr::protocol_error);
    j=json::parse(encoded);j["latticeRecovery"]=j["latticeCanonicalRange"];j.erase("latticeCanonicalRange");EXPECT_THROW(cr::decode(j.dump(),x.b),cr::protocol_error);
    j=json::parse(encoded);j["latticeCanonicalRange"]["body"]["head"]="012";EXPECT_THROW(cr::decode(j.dump(),x.b),cr::protocol_error);
    j=json::parse(encoded);j["latticeCanonicalRange"]["body"]["head"]=12;EXPECT_THROW(cr::decode(j.dump(),x.b),cr::protocol_error);
}

TEST(SyncCanonicalRange, IndependentResourceCapsRefuseWithoutPartialSequence) {
    Bundle x;ASSERT_NO_THROW(x.start());const auto initial=x.start();
    auto b=x.b;b.request_entries=0;EXPECT_THROW(cr::request_sha256(x.a,x.r,b),cr::protocol_error);
    b=x.b;b.request_targets=0;EXPECT_THROW(cr::request_sha256(x.a,x.r,b),cr::protocol_error);
    b=x.b;b.request_target_bytes=x.m.counts.rebase_bytes-1;EXPECT_THROW(cr::request_sha256(x.a,x.r,b),cr::protocol_error);
    b=x.b;b.maximum.content_identities=x.m.counts.identities-1;EXPECT_THROW(cr::manifest_sha256(x.m,b),cr::protocol_error);
    b=x.b;b.maximum.content_bytes=x.m.counts.content_bytes-1;EXPECT_THROW(cr::manifest_sha256(x.m,b),cr::protocol_error);
    b=x.b;b.maximum.receipts=0;EXPECT_THROW(cr::manifest_sha256(x.m,b),cr::protocol_error);
    b=x.b;b.maximum.receipt_bytes=x.m.counts.receipt_bytes-1;EXPECT_THROW(cr::manifest_sha256(x.m,b),cr::protocol_error);
    b=x.b;b.maximum.content_pages=0;EXPECT_THROW(cr::manifest_sha256(x.m,b),cr::protocol_error);
    b=x.b;b.maximum.receipt_pages=0;EXPECT_THROW(cr::manifest_sha256(x.m,b),cr::protocol_error);
    b=x.b;b.lease_ms=x.m.protection.duration_ms-1;EXPECT_THROW(cr::manifest_sha256(x.m,b),cr::protocol_error);
    b=x.b;b.maximum.items_per_page=1;EXPECT_THROW(cr::page_sha256(x.page(),b),cr::protocol_error);
    b=x.b;b.restart_bytes=cr::encode_state(initial,b).size()-1;EXPECT_THROW(cr::encode_state(initial,b),cr::protocol_error);EXPECT_EQ(initial,x.start());
}

TEST(SyncCanonicalRange, ManifestSourceRequestAndAdvertisedCapsRemainExactlyBound) {
    Bundle x;auto m=x.m;m.source.epoch=uuid('7');m.manifest_digest=cr::manifest_sha256(m,x.b);EXPECT_THROW(cr::begin(x.a,x.r,m,x.b),cr::protocol_error);
    m=x.m;m.request_digest=std::string(64,'c');m.manifest_digest=cr::manifest_sha256(m,x.b);EXPECT_THROW(cr::begin(x.a,x.r,m,x.b),cr::protocol_error);
    m=x.m;++m.counts.receipts;m.counts.receipt_bytes+=50;m.manifest_digest=cr::manifest_sha256(m,x.b);EXPECT_THROW(cr::begin(x.a,x.r,m,x.b),cr::protocol_error);
    auto r=x.r;r.budget.content_identities=1;r.request_digest=cr::request_sha256(x.a,r,x.b);m=x.m;m.request_digest=r.request_digest;m.rebase_digest=cr::rebase_sha256(x.a,r,x.b);m.manifest_digest=cr::manifest_sha256(m,x.b);EXPECT_THROW(cr::begin(x.a,r,m,x.b),cr::protocol_error);
    auto p=x.page();p.items[0]=row("A",R"({"name":{"kind":2,"value":"other"}})"); // equal record length, stale page digest
    EXPECT_THROW(cr::encode({x.a,1,p},x.b),cr::protocol_error);
    p.digest=cr::page_sha256(p,x.b);EXPECT_NO_THROW(cr::encode({x.a,1,p},x.b)); // full C verification remains required
}

TEST(SyncCanonicalRange, DuplicateRequestTargetsAliasesAndOverflowAreNotCoerced) {
    Bundle x;auto r=x.r;r.receipts.push_back(r.receipts[0]);EXPECT_THROW(cr::request_sha256(x.a,r,x.b),cr::protocol_error);
    r=x.r;r.receipts[0].targets.push_back(r.receipts[0].targets[0]);EXPECT_THROW(cr::request_sha256(x.a,r,x.b),cr::protocol_error);
    r=x.r;r.receipts[0].targets[0].id=std::string(257,'a');EXPECT_THROW(cr::request_sha256(x.a,r,x.b),cr::protocol_error);
    r=x.r;r.receipts[0].original_id="OP-A";EXPECT_NE(cr::request_sha256(x.a,r,x.b),x.r.request_digest); // byte preservation, not schema alias authority
    auto m=x.m;m.counts.present=std::numeric_limits<uint64_t>::max();EXPECT_THROW(cr::manifest_sha256(m,x.b),cr::protocol_error);
    m=x.m;m.counts.content_pages=0;EXPECT_THROW(cr::manifest_sha256(m,x.b),cr::protocol_error);
    auto p=x.page();std::get<cr::present>(p.items[0].value).payload=std::string("\xff",1);EXPECT_THROW(cr::page_sha256(p,x.b),cr::protocol_error);
}

TEST(SyncCanonicalRange, RestartRecordsRemainUnverifiedAndRejectContradictoryCoverage) {
    Bundle x;const auto first=x.after_content();const auto finished=x.finished();EXPECT_EQ(cr::decode_state(cr::encode_state(first,x.b),x.a,x.b),first);EXPECT_EQ(cr::decode_state(cr::encode_state(finished,x.b),x.a,x.b),finished);
    auto j=json::parse(cr::encode_state(x.start(),x.b));j["latticeCanonicalRangeState"]["phase"]="sequence_complete_unverified";EXPECT_THROW(cr::decode_state(j.dump(),x.a,x.b),cr::protocol_error);
    j=json::parse(cr::encode_state(first,x.b));j["latticeCanonicalRangeState"]["rebase_seen"]="0";EXPECT_THROW(cr::decode_state(j.dump(),x.a,x.b),cr::protocol_error);
    j=json::parse(cr::encode_state(first,x.b));j["latticeCanonicalRangeState"]["identities"]="02";EXPECT_THROW(cr::decode_state(j.dump(),x.a,x.b),cr::protocol_error);
    auto other=x.a;++other.sequence;EXPECT_THROW(cr::decode_state(cr::encode_state(first,x.b),other,x.b),cr::protocol_error);
}

TEST(SyncCanonicalRange, StructuralCompletionDoesNotClaimWholeHashOrAuthority) {
    Bundle x;x.m.content_digest=std::string(64,'e');x.m.manifest_digest=cr::manifest_sha256(x.m,x.b);
    EXPECT_NE(cr::content_sha256(x.m,x.rows,x.b),x.m.content_digest);const auto s=x.finished();EXPECT_EQ(s.status,cr::phase::sequence_complete_unverified);
    // A durable adapter MUST re-stream the whole C/E digests and validate source,
    // receipt coverage, scope/schema and current route before any installation.
}
