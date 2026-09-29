#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/canonical_range_package.hpp"
#include "../../Sources/LatticeCore/src/canonical_validated_sequence.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstdio>
#include <set>

namespace {
namespace cr=lattice::detail::canonical_range;
namespace sr=lattice::detail::sync_recovery;
using json=nlohmann::json;
std::string coverage_uuid(unsigned n){char value[37];std::snprintf(value,sizeof(value),"90000000-0000-4000-8000-%012u",n);return value;}
cr::package_limits coverage_large_profile(){
    // Exact bounded48MiBV1 codec/package values from the mounted source. This
    // pure DTO fixture issues no producer/source registration or authority.
    return {{{4194304,16384,64,512,16384,33554432,256,8192,8388608},
        16,262144,32768,8192,8192,2097152,4194304,3600000,{16384,64,256,4096,16384}},41943040,770};
}
struct CoverageRangeFixture {
    cr::package_limits policy=coverage_large_profile();
    cr::attempt attempt{coverage_uuid(1),coverage_uuid(2),"coverage-channel",2,coverage_uuid(3)};
    cr::request request;
    cr::lease lease{"pure-lease-spelling",300000};
    std::vector<cr::content_item> rows;
    std::vector<cr::receipt_item> receipts;
    CoverageRangeFixture(){
        policy.codec.maximum.items_per_page=1;request.budget=policy.codec.maximum;
        request.source={"coverage-source",coverage_uuid(4),coverage_uuid(5),std::string(64,'a'),std::string(64,'b')};
        request.registered_producer=lattice::detail::recovery_receipt_binding{{"registered-producer",coverage_uuid(6)},coverage_uuid(7),7,1};
        request.receipt_namespace="namespace-a";
        request.receipts={{coverage_uuid(30),request.receipt_namespace,{{"CoverageRangeRow","A"}},std::string(64,'c')},
            {coverage_uuid(31),request.receipt_namespace,{{"CoverageRangeRow","B"}},std::string(64,'d')}};
        rows={{{"CoverageRangeRow","A"},cr::present{sr::encode_values({{"value",std::string("one")}},policy.codec.values)}},
            {{"CoverageRangeRow","B"},cr::tombstone{}}};
        receipts={{coverage_uuid(30),cr::committed{"namespace-a","coverage-a",cr::decision::applied,3,cr::identity{"CoverageRangeRow","A"}},std::string(64,'c'),false},
            {coverage_uuid(31),cr::unknown{cr::unknown_reason::missing_coverage},std::string(64,'d'),false}};
        seal();
    }
    void seal(){request.request_digest=cr::request_sha256(attempt,request,policy.codec);}
    cr::encoded_package build(std::optional<uint64_t> revision=11)const{return cr::assemble_package(attempt,9,request,7,lease,rows,receipts,policy,revision);}
    cr::frame request_frame()const{return {attempt,9,request,3};}
    cr::sequence_state receipt_start(const cr::encoded_package& package)const{
        auto state=cr::begin(attempt,request,package.offer(),policy.codec);
        for(size_t i=1;i<=package.offer().counts.content_pages;++i)state=cr::propose(state,cr::decode(package.frames()[i],policy.codec),policy.codec);
        return state;
    }
    cr::frame first_receipt(const cr::encoded_package& package)const{return cr::decode(package.frames()[1+package.offer().counts.content_pages],policy.codec);}
    void verify(const cr::encoded_package& package)const{
        auto state=cr::begin(attempt,request,package.offer(),policy.codec);cr::validated_sequence cursor(attempt,request,package.offer(),policy.codec);
        std::vector<cr::content_item> decoded_rows;std::vector<cr::receipt_item> decoded_receipts;
        for(size_t i=0;i<package.frames().size();++i){const auto frame=cr::decode(package.frames()[i],policy.codec);
            EXPECT_EQ(frame.version,request.registered_producer?3u:2u);EXPECT_EQ(frame.logical,attempt);EXPECT_EQ(frame.route_generation,9u);
            if(i){const auto saved=cr::encode_state(state,policy.codec);EXPECT_EQ(cr::decode_state(saved,attempt,policy.codec),state);
                state=cr::propose(state,frame,policy.codec);cursor.advance(frame);EXPECT_EQ(cursor.snapshot(),state);}
            if(const auto* page=std::get_if<cr::content_page>(&frame.body))decoded_rows.insert(decoded_rows.end(),page->items.begin(),page->items.end());
            if(const auto* page=std::get_if<cr::receipt_page>(&frame.body))decoded_receipts.insert(decoded_receipts.end(),page->items.begin(),page->items.end());
        }
        EXPECT_EQ(decoded_rows,rows);EXPECT_EQ(decoded_receipts,receipts);EXPECT_EQ(state.status,cr::phase::sequence_complete_unverified);
        EXPECT_EQ(cr::decode_state(cr::encode_state(state,policy.codec),attempt,policy.codec),state);
        EXPECT_EQ(cr::content_sha256(package.offer(),decoded_rows,policy.codec),package.offer().content_digest);
        EXPECT_EQ(cr::receipts_sha256(package.offer(),decoded_receipts,policy.codec),package.offer().receipt_digest);
    }
};
}

TEST(CanonicalReceiptCoverageRange, RegisteredPositiveAndMissingRemainDistinctAcrossEveryPageRestart) {
    CoverageRangeFixture f;const auto package=f.build();f.verify(package);
    EXPECT_EQ(package.offer().registered_producer,f.request.registered_producer);EXPECT_EQ(package.offer().receipt_namespace,f.request.receipt_namespace);
    EXPECT_EQ(package.offer().coverage_revision,std::optional<uint64_t>{11});EXPECT_EQ(package.offer().counts.receipt_pages,2u);
    EXPECT_TRUE(std::holds_alternative<cr::committed>(f.receipts[0].value));EXPECT_TRUE(std::holds_alternative<cr::unknown>(f.receipts[1].value));
}
TEST(CanonicalReceiptCoverageRange, RegisteredMissingCannotBeEncodedOrPackagedAsNegative) {
    CoverageRangeFixture f;auto package=f.build();auto frame=f.first_receipt(package);auto& page=std::get<cr::receipt_page>(frame.body);
    page.items[0].value=cr::not_committed{"namespace-a","coverage-a"};
    EXPECT_THROW(cr::receipt_record_bytes(page.items[0],f.policy.codec),cr::protocol_error);
    EXPECT_THROW(cr::encode(frame,f.policy.codec),cr::protocol_error);
    f.receipts[1].value=cr::not_committed{"namespace-a","coverage-a"};
    EXPECT_THROW(f.build(),cr::protocol_error);
}
TEST(CanonicalReceiptCoverageRange, LegacyUnboundTagRequiresExactOriginalNamespacePositive) {
    CoverageRangeFixture f;f.receipts[0].legacy_unbound=true;const auto legacy=f.build();f.verify(legacy);
    auto ordinary=f.receipts;ordinary[0].legacy_unbound=false;
    EXPECT_NE(cr::receipts_sha256(legacy.offer(),ordinary,f.policy.codec),legacy.offer().receipt_digest);
    f.receipts[0].value=cr::unknown{cr::unknown_reason::legacy};
    EXPECT_THROW(f.build(),cr::protocol_error);
    f.receipts[0].value=cr::not_committed{"namespace-a","coverage-a"};
    EXPECT_THROW(f.build(),cr::protocol_error);
    f.receipts[0].value=cr::committed{"namespace-b","coverage-a",cr::decision::applied,3,cr::identity{"CoverageRangeRow","A"}};
    EXPECT_THROW(f.build(),cr::protocol_error);
    f.receipts[0].value=cr::committed{"namespace-a","coverage-a",cr::decision::applied,3,cr::identity{"CoverageRangeRow","A"}};
    f.receipts[0].operation_digest.reset();
    EXPECT_THROW(f.build(),cr::protocol_error);
}
TEST(CanonicalReceiptCoverageRange, RequestAndManifestDowngradeAndMixedMetadataRefuse) {
    CoverageRangeFixture f;const auto package=f.build();const auto original=json::parse(cr::encode(f.request_frame(),f.policy.codec));
    std::vector<json> rejected;
    auto j=original;j["latticeCanonicalRange"]["version"]=2;rejected.push_back(j);
    j=original;j["latticeCanonicalRange"]["body"].erase("receipt_namespace");rejected.push_back(j);
    j=original;j["latticeCanonicalRange"]["body"]["receipt_requests"][0].erase("operation_digest");rejected.push_back(j);
    j=original;j["latticeCanonicalRange"]["body"]["receipt_requests"][0]["namespace_id"]="namespace-a";rejected.push_back(j);
    j=json::parse(package.frames().front());j["latticeCanonicalRange"]["version"]=2;rejected.push_back(j);
    j=json::parse(package.frames().front());j["latticeCanonicalRange"]["body"].erase("coverage_revision");rejected.push_back(j);
    for(const auto& bad:rejected){SCOPED_TRACE(bad.dump());EXPECT_THROW(cr::decode(bad.dump(),f.policy.codec),cr::protocol_error);}
    EXPECT_EQ(std::get<cr::request>(cr::decode(original.dump(),f.policy.codec).body),f.request);
}
TEST(CanonicalReceiptCoverageRange, ReceiptPageVersionMustAgreeWithAllRegisteredMetadata) {
    CoverageRangeFixture f;const auto package=f.build();const auto original=f.first_receipt(package);const auto raw=cr::encode(original,f.policy.codec);
    auto j=json::parse(raw);j["latticeCanonicalRange"]["version"]=2;
    EXPECT_THROW(cr::decode(j.dump(),f.policy.codec),cr::protocol_error);
    auto wrong=original;wrong.version=2;
    EXPECT_THROW(cr::encode(wrong,f.policy.codec),cr::protocol_error);
    j=json::parse(raw);j["latticeCanonicalRange"]["body"]["items"][0].erase("legacy_unbound");
    EXPECT_THROW(cr::decode(j.dump(),f.policy.codec),cr::protocol_error);
    wrong=original;auto& page=std::get<cr::receipt_page>(wrong.body);page.items[0].operation_digest.reset();
    page.bytes=cr::receipt_record_bytes(page.items[0],f.policy.codec);page.digest=cr::page_sha256(page,f.policy.codec);
    EXPECT_THROW(cr::encode(wrong,f.policy.codec),cr::protocol_error);
    j=json::parse(cr::encode({f.attempt,9,page,2},f.policy.codec));j["latticeCanonicalRange"]["version"]=3;
    EXPECT_THROW(cr::decode(j.dump(),f.policy.codec),cr::protocol_error);
}
TEST(CanonicalReceiptCoverageRange, ExactProducerCohortAndNamespaceAreBoundBeyondSelfConsistentManifestHash) {
    CoverageRangeFixture f;const auto package=f.build();
    std::vector<cr::request> altered;
    auto q=f.request;q.registered_producer->producer.registration_id="different-registered-producer";altered.push_back(q);
    q=f.request;q.registered_producer->producer.incarnation=coverage_uuid(90);altered.push_back(q);
    q=f.request;q.registered_producer->cohort_id=coverage_uuid(91);altered.push_back(q);
    q=f.request;++q.registered_producer->cohort_revision;altered.push_back(q);
    q=f.request;q.receipt_namespace="namespace-b";for(auto& asked:q.receipts)asked.namespace_id=q.receipt_namespace;altered.push_back(q);
    for(auto changed:altered){const auto digest=cr::request_sha256(f.attempt,changed,f.policy.codec);EXPECT_NE(digest,f.request.request_digest);
        EXPECT_THROW(cr::encode({f.attempt,9,changed,3},f.policy.codec),cr::protocol_error);
        auto manifest=package.offer();manifest.registered_producer=changed.registered_producer;manifest.receipt_namespace=changed.receipt_namespace;
        manifest.manifest_digest=cr::manifest_sha256(manifest,f.policy.codec);
        EXPECT_THROW(cr::begin(f.attempt,f.request,manifest,f.policy.codec),cr::protocol_error);
    }
}
TEST(CanonicalReceiptCoverageRange, RehashedOperationDigestTamperCannotAdvanceExactReceiptRequest) {
    CoverageRangeFixture f;const auto package=f.build();auto state=f.receipt_start(package);const auto before=state;auto frame=f.first_receipt(package);
    auto& page=std::get<cr::receipt_page>(frame.body);page.items[0].operation_digest=std::string(64,'e');
    page.bytes=cr::receipt_record_bytes(page.items[0],f.policy.codec);page.digest=cr::page_sha256(page,f.policy.codec);
    EXPECT_NO_THROW(cr::decode(cr::encode(frame,f.policy.codec),f.policy.codec));
    EXPECT_THROW(cr::propose(state,frame,f.policy.codec),cr::protocol_error);EXPECT_EQ(state,before);
    cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
    for(size_t i=1;i<=package.offer().counts.content_pages;++i)cursor.advance(cr::decode(package.frames()[i],f.policy.codec));
    const auto saved=cursor.snapshot();EXPECT_THROW(cursor.advance(frame),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),saved);
    const auto correct=f.first_receipt(package);cursor.advance(correct);EXPECT_EQ(cursor.snapshot(),cr::propose(state,correct,f.policy.codec));
}
TEST(CanonicalReceiptCoverageRange, CoverageRevisionChangesManifestAndStreamAnchorsWithoutInventingNegatives) {
    CoverageRangeFixture f;const auto one=f.build(11),two=f.build(12);
    EXPECT_EQ(one.offer().request_digest,two.offer().request_digest);EXPECT_EQ(one.offer().rebase_digest,two.offer().rebase_digest);
    EXPECT_NE(one.offer().manifest_digest,two.offer().manifest_digest);EXPECT_NE(one.offer().content_digest,two.offer().content_digest);
    EXPECT_NE(one.offer().receipt_digest,two.offer().receipt_digest);f.verify(one);f.verify(two);
    EXPECT_THROW(f.build(std::nullopt),cr::protocol_error);
}
TEST(CanonicalReceiptCoverageRange, RestartAndTerminalRemainExactAcrossVersionAndBindingTamper) {
    CoverageRangeFixture f;const auto package=f.build();auto state=f.receipt_start(package);const auto saved=cr::encode_state(state,f.policy.codec);
    std::vector<json> rejected;const auto original=json::parse(saved);auto j=original;
    j["latticeCanonicalRangeState"]["version"]=2;rejected.push_back(j);
    j=original;j["latticeCanonicalRangeState"]["request"]["registered_producer"]["producer"]["incarnation"]=coverage_uuid(92);rejected.push_back(j);
    j=original;j["latticeCanonicalRangeState"]["manifest"]["coverage_revision"]="12";rejected.push_back(j);
    for(const auto& bad:rejected){EXPECT_THROW(cr::decode_state(bad.dump(),f.attempt,f.policy.codec),cr::protocol_error);}
    auto other=f.attempt;other.channel="other-channel";
    EXPECT_THROW(cr::decode_state(saved,other,f.policy.codec),cr::protocol_error);
    auto terminal=cr::decode(package.frames().back(),f.policy.codec);
    EXPECT_THROW(cr::propose(state,terminal,f.policy.codec),cr::protocol_error);
    for(size_t i=1+package.offer().counts.content_pages;i+1<package.frames().size();++i)state=cr::propose(state,cr::decode(package.frames()[i],f.policy.codec),f.policy.codec);
    const auto before=state;terminal.version=2;
    EXPECT_THROW(cr::propose(state,terminal,f.policy.codec),cr::protocol_error);EXPECT_EQ(state,before);
    terminal.version=3;state=cr::propose(state,terminal,f.policy.codec);EXPECT_EQ(state.status,cr::phase::sequence_complete_unverified);
    EXPECT_THROW(cr::propose(state,terminal,f.policy.codec),cr::protocol_error);
    EXPECT_EQ(cr::decode_state(cr::encode_state(state,f.policy.codec),f.attempt,f.policy.codec),state);
}
TEST(CanonicalReceiptCoverageRange, OriginalV2PositiveAndNegativeSpellingRemainsValidWithoutRegisteredMetadata) {
    CoverageRangeFixture f;f.request.registered_producer.reset();f.request.receipt_namespace.reset();
    for(auto& asked:f.request.receipts)asked.operation_digest.reset();for(auto& receipt:f.receipts)receipt.operation_digest.reset();
    f.receipts[1].value=cr::not_committed{"namespace-a","coverage-a"};f.seal();const auto legacy=f.build(std::nullopt);f.verify(legacy);
    EXPECT_FALSE(legacy.offer().registered_producer);EXPECT_FALSE(legacy.offer().coverage_revision);
    EXPECT_THROW(f.build(1),cr::protocol_error);
    const auto q=json::parse(cr::encode({f.attempt,9,f.request},f.policy.codec));EXPECT_EQ(q["latticeCanonicalRange"]["version"],2);
    EXPECT_FALSE(q["latticeCanonicalRange"]["body"].contains("registered_producer"));
}
TEST(CanonicalReceiptCoverageRange, Full8192CompactRequestsFitEachOfSixteenActualLargeProfileContributions) {
    CoverageRangeFixture f;f.policy=coverage_large_profile();f.request.budget=f.policy.codec.maximum;f.request.receipts.clear();f.receipts.clear();
    f.rows={{{"CoverageRangeRow",coverage_uuid(100)},cr::tombstone{}}};
    for(unsigned i=0;i<8192;++i){const auto id=coverage_uuid(1000+i);
        f.request.receipts.push_back({id,f.request.receipt_namespace,{f.rows[0].key},std::string(64,'c')});
        f.receipts.push_back({id,cr::unknown{cr::unknown_reason::missing_coverage},std::string(64,'c'),false});}
    std::set<std::string> digests;
    for(unsigned channel=0;channel<16;++channel){f.attempt.channel="coverage-channel-"+std::to_string(channel);f.attempt.channel_incarnation=coverage_uuid(200+channel);
        f.request.receipt_namespace="namespace-"+std::to_string(channel);for(auto& asked:f.request.receipts)asked.namespace_id=f.request.receipt_namespace;f.seal();
        const auto raw=cr::encode(f.request_frame(),f.policy.codec);EXPECT_LE(raw.size(),4194304u);EXPECT_TRUE(digests.insert(f.request.request_digest).second);
        const auto decoded=cr::decode(raw,f.policy.codec);EXPECT_EQ(std::get<cr::request>(decoded.body),f.request);
        const auto body=json::parse(raw).at("latticeCanonicalRange").at("body");EXPECT_EQ(body.at("receipt_requests").size(),8192u);
        for(const auto& asked:body.at("receipt_requests")){EXPECT_EQ(asked.size(),3u);EXPECT_FALSE(asked.contains("registered_producer"));EXPECT_FALSE(asked.contains("namespace_id"));}
        // Exact mounted control shape: Q is one bounded escaped string, not
        // 8192 entries expanded into the outer control parser's node budget.
        const auto control=json{{"kind","recoveryReady"},{"version",1},{"operation","prepare"},{"requestID",coverage_uuid(300+channel)},
            {"routeGeneration","9"},{"request",raw},{"durationMilliseconds",300000}}.dump();
        EXPECT_LE(control.size(),8388608u);size_t events=0,max_depth=0,max_scalar=0;
        const auto parsed=json::parse(control,[&](int depth,json::parse_event_t,json& value){++events;max_depth=std::max(max_depth,static_cast<size_t>(depth));
            if(value.is_string())max_scalar=std::max(max_scalar,value.get_ref<const std::string&>().size());return true;});
        EXPECT_LE(events,32768u);EXPECT_LE(max_depth,16u);EXPECT_LE(max_scalar,4194304u);EXPECT_EQ(parsed.at("request"),raw);
        const auto package=f.build();EXPECT_EQ(package.offer().counts.receipts,8192u);EXPECT_EQ(package.offer().counts.receipt_pages,128u);
        EXPECT_LE(package.frames().size(),770u);EXPECT_LE(package.retained_wire_bytes(),41943040u);
        cr::validated_sequence sequence(f.attempt,f.request,package.offer(),f.policy.codec);
        for(size_t page=1;page<package.frames().size();++page)sequence.advance(cr::decode(package.frames()[page],f.policy.codec));
        const auto terminal=sequence.snapshot();EXPECT_EQ(terminal.receipt_count,8192u);EXPECT_EQ(terminal.status,cr::phase::sequence_complete_unverified);
        const auto restart=cr::encode_state(terminal,f.policy.codec);EXPECT_LE(restart.size(),4194304u);
        EXPECT_EQ(cr::decode_state(restart,f.attempt,f.policy.codec),terminal);
    }
    EXPECT_EQ(digests.size(),16u);
}

TEST(CanonicalReceiptCoverageRange, CanonicalV3BytesPreserveExactRegisteredRequestAndEveryPrefix) {
    CoverageRangeFixture f;const auto package=f.build();
    const auto q=cr::decode_canonical(cr::encode(f.request_frame(),f.policy.codec),f.policy.codec);
    EXPECT_EQ(q.version,3u);EXPECT_EQ(std::get<cr::request>(q.body),f.request);
    cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
    auto reference=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
    for(size_t i=1;i<package.frames().size();++i) {
        reference=cr::propose(reference,cr::decode(package.frames()[i],f.policy.codec),f.policy.codec);
        const auto decoded=cursor.advance_canonical(package.frames()[i],9);
        EXPECT_EQ(decoded.version,3u);EXPECT_EQ(cr::encode(decoded,f.policy.codec),package.frames()[i]);
        EXPECT_EQ(cursor.snapshot(),reference);
    }
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}

TEST(CanonicalReceiptCoverageRange, CanonicalV3MetadataAndRehashedOperationTamperKeepPriorState) {
    CoverageRangeFixture f;const auto package=f.build();
    cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
    for(size_t i=1;i<=package.offer().counts.content_pages;++i)cursor.advance_canonical(package.frames()[i],9);
    const auto before=cursor.snapshot();const auto index=1+package.offer().counts.content_pages;
    const auto original=json::parse(package.frames()[index]);std::vector<std::string> rejected;
    auto bad=original;bad["latticeCanonicalRange"]["version"]=2;rejected.push_back(bad.dump());
    bad=original;bad["latticeCanonicalRange"]["body"]["items"][0].erase("operation_digest");rejected.push_back(bad.dump());
    bad=original;bad["latticeCanonicalRange"]["body"]["items"][0].erase("legacy_unbound");rejected.push_back(bad.dump());
    auto frame=f.first_receipt(package);auto& page=std::get<cr::receipt_page>(frame.body);
    page.items[0].operation_digest=std::string(64,'e');page.bytes=cr::receipt_record_bytes(page.items[0],f.policy.codec);
    page.digest=cr::page_sha256(page,f.policy.codec);const auto rehashed=cr::encode(frame,f.policy.codec);
    EXPECT_NO_THROW(cr::decode_canonical(rehashed,f.policy.codec));rejected.push_back(rehashed);
    for(size_t i=0;i<rejected.size();++i) {
        SCOPED_TRACE(i);EXPECT_THROW(cursor.advance_canonical(rejected[i],9),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),before);
    }
    for(size_t i=index;i<package.frames().size();++i)cursor.advance_canonical(package.frames()[i],9);
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}

TEST(CanonicalReceiptCoverageRange, CanonicalV3ReceiptKeepsActualBooleanTypeAndBothValues) {
    for(bool legacy_unbound:{false,true}) {
        CoverageRangeFixture f;f.receipts[0].legacy_unbound=legacy_unbound;const auto package=f.build();
        const auto index=1+package.offer().counts.content_pages;const auto& raw=package.frames()[index];
        const auto original=json::parse(raw);
        ASSERT_TRUE(original.at("latticeCanonicalRange").at("body").at("items")[0].at("legacy_unbound").is_boolean());
        const auto decoded=cr::decode_canonical(raw,f.policy.codec);
        EXPECT_EQ(decoded.version,3u);EXPECT_EQ(cr::encode(decoded,f.policy.codec),raw);
        EXPECT_EQ(std::get<cr::receipt_page>(decoded.body).items[0],f.receipts[0]);
        for(const auto& wrong_type:{json(legacy_unbound?1:0),json(legacy_unbound?"true":"false"),json(nullptr),json(1.0)}) {
            auto bad=original;bad["latticeCanonicalRange"]["body"]["items"][0]["legacy_unbound"]=wrong_type;
            EXPECT_THROW(cr::decode_canonical(bad.dump(),f.policy.codec),cr::protocol_error);
        }
    }
}

TEST(CanonicalReceiptCoverageRange, PrivateContentReuseKeepsBothEntriesAndActualReceiptBooleansExact) {
    for(bool legacy_unbound:{false,true})for(bool raw:{false,true}) {
        SCOPED_TRACE(legacy_unbound);SCOPED_TRACE(raw);
        CoverageRangeFixture f;f.receipts[0].legacy_unbound=legacy_unbound;const auto package=f.build();
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
        auto reference=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);uint64_t content_items=0;
        for(size_t index=1;index<package.frames().size();++index) {
            const auto frame=cr::decode(package.frames()[index],f.policy.codec);reference=cr::propose(reference,frame,f.policy.codec);
            const auto* page=std::get_if<cr::content_page>(&frame.body);const auto expected=page?page->items.size():0;
            cr::sequence_test_observation::counters count;
            struct scope {
                cr::sequence_test_observation::counters* prior=cr::sequence_test_observation::current;
                explicit scope(cr::sequence_test_observation::counters& value){cr::sequence_test_observation::current=&value;}
                ~scope(){cr::sequence_test_observation::current=prior;}
            };
            {scope observed(count);if(raw)(void)cursor.advance_canonical(package.frames()[index],9);else cursor.advance(frame);}
            EXPECT_EQ(count.content_shape_calls,expected);content_items+=count.content_shape_calls;
            EXPECT_EQ(cursor.snapshot(),reference);
            if(const auto* receipts=std::get_if<cr::receipt_page>(&frame.body))for(const auto& item:receipts->items) {
                if(item.original_id==f.receipts[0].original_id)EXPECT_EQ(item.legacy_unbound,legacy_unbound);
            }
        }
        EXPECT_EQ(content_items,f.rows.size());EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
        EXPECT_EQ(cr::content_sha256(package.offer(),f.rows,f.policy.codec),package.offer().content_digest);
        EXPECT_EQ(cr::receipts_sha256(package.offer(),f.receipts,f.policy.codec),package.offer().receipt_digest);
    }
}
