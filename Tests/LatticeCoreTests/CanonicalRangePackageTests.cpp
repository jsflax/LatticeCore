#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/canonical_range_package.hpp"
#include "../../Sources/LatticeCore/src/canonical_validated_sequence.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <type_traits>
#include <numeric>
#include <nlohmann/json.hpp>

struct PackageSourceRow { std::string body; };
LATTICE_SCHEMA(PackageSourceRow,body);

namespace {
namespace cr=lattice::detail::canonical_range;
namespace sr=lattice::detail::sync_recovery;
using namespace lattice::detail;
std::string package_uuid(char c){return std::string("00000000-0000-4000-8000-00000000000")+c;}
struct PackageFixture {
    cr::package_limits policy{{{16384,4096,2,32,128,262144,32,32,131072},
        16,4096,4096,32,128,32768,65536,60000,{4096,16,256,2048,4096}},524288,66};
    cr::attempt attempt{package_uuid('1'),package_uuid('2'),"package-channel",2,package_uuid('3')};
    cr::request request;
    cr::lease lease{"mechanical-lease-spelling",30000};
    std::vector<cr::content_item> rows;
    std::vector<cr::receipt_item> receipts;
    PackageFixture(){
        request.source={"mechanical-authority",package_uuid('4'),package_uuid('5'),std::string(64,'a'),std::string(64,'b')};
        request.selection=cr::mode::full;request.budget=policy.codec.maximum;
        rows={row("A","first"),row("B","second"),row("C","third")};seal_request();
    }
    cr::content_item row(std::string id,std::string value)const {
        return {{"PackageSourceRow",std::move(id)},cr::present{sr::encode_values({{"body",std::move(value)}},policy.codec.values)}};
    }
    void seal_request(){request.request_digest=cr::request_sha256(attempt,request,policy.codec);}
    void receipt(cr::receipt_item value,std::optional<std::string> ns=std::string("namespace"),
                 std::string target="A") {
        request.receipts={{value.original_id,std::move(ns),{{"PackageSourceRow",std::move(target)}}}};
        receipts={std::move(value)};seal_request();
    }
    cr::encoded_package build(uint64_t head=7)const {
        return cr::assemble_package(attempt,1,request,head,lease,rows,receipts,policy);
    }
    void verify(const cr::encoded_package& package)const {
        ASSERT_GE(package.frames().size(),2u);
        const auto first=cr::decode(package.frames().front(),policy.codec);
        ASSERT_TRUE(std::holds_alternative<cr::manifest>(first.body));
        EXPECT_EQ(std::get<cr::manifest>(first.body),package.offer());
        auto state=cr::begin(attempt,request,package.offer(),policy.codec);
        std::vector<cr::content_item> actual_rows;std::vector<cr::receipt_item> actual_receipts;
        uint64_t bytes=0;
        for(size_t i=0;i<package.frames().size();++i) {
            const auto& raw=package.frames()[i];bytes+=raw.size();EXPECT_LE(raw.size(),request.budget.frame_bytes);
            const auto frame=cr::decode(raw,policy.codec);EXPECT_EQ(frame.logical,attempt);EXPECT_EQ(frame.route_generation,1u);
            if(i)state=cr::propose(state,frame,policy.codec);
            if(const auto* page=std::get_if<cr::content_page>(&frame.body))actual_rows.insert(actual_rows.end(),page->items.begin(),page->items.end());
            if(const auto* page=std::get_if<cr::receipt_page>(&frame.body))actual_receipts.insert(actual_receipts.end(),page->items.begin(),page->items.end());
        }
        EXPECT_EQ(bytes,package.retained_wire_bytes());EXPECT_EQ(actual_rows,rows);EXPECT_EQ(actual_receipts,receipts);
        EXPECT_EQ(state.status,cr::phase::sequence_complete_unverified);
        EXPECT_EQ(cr::content_sha256(package.offer(),actual_rows,policy.codec),package.offer().content_digest);
        EXPECT_EQ(cr::receipts_sha256(package.offer(),actual_receipts,policy.codec),package.offer().receipt_digest);
    }
};
}

TEST(CanonicalRangePackage, ExactFramesRoundTripAndRetryAfterInputMutation) {
    PackageFixture f;auto package=f.build();f.verify(package);const auto original=package.frames();
    f.rows[0]=f.row("A","changed after capture");f.request.source.epoch=package_uuid('9');
    EXPECT_EQ(package.frames(),original);auto moved=std::move(package);EXPECT_EQ(moved.frames(),original);
    static_assert(!std::is_copy_constructible_v<cr::encoded_package>);
    static_assert(!std::is_copy_assignable_v<cr::encoded_package>);
}

TEST(CanonicalRangePackage, EmptySourceProducesManifestAndEndAtNumericZero) {
    PackageFixture f;f.rows.clear();auto p=f.build(0);f.verify(p);EXPECT_EQ(p.frames().size(),2u);
    EXPECT_EQ(p.offer().head,0u);EXPECT_EQ(p.offer().counts.identities,0u);EXPECT_EQ(p.offer().counts.content_pages,0u);
}

TEST(CanonicalRangePackage, EscapedPayloadBytesDrivePagePackingWithoutChangingValues) {
    PackageFixture f;f.policy.codec.maximum.frame_bytes=4096;f.policy.codec.maximum.payload_bytes=2048;
    f.policy.codec.string_bytes=2048;f.policy.codec.maximum.items_per_page=100;
    f.request.budget=f.policy.codec.maximum;f.rows.clear();
    // Scalar JSON itself is a quoted wire string. Its backslashes must be
    // counted again by the outer frame, including NUL/control escapes.
    std::string body;for(int i=0;i<120;++i){body+='"';body+='\\';body+='\0';}
    for(char c='A';c<'F';++c)f.rows.push_back(f.row(std::string(1,c),body));
    f.seal_request();auto p=f.build();f.verify(p);
    EXPECT_GT(p.offer().counts.content_pages,1u);EXPECT_EQ(p.offer().counts.identities,5u);
}

TEST(CanonicalRangePackage, NodeLimitSplitsPagesBeforeCountLimit) {
    PackageFixture f;f.policy.codec.maximum.items_per_page=100;f.policy.codec.nodes=512;
    f.policy.codec.maximum.content_identities=128;f.request.budget=f.policy.codec.maximum;f.rows.clear();
    for(int i=0;i<80;++i)f.rows.push_back(f.row("id-"+std::to_string(100+i),"value"));
    f.seal_request();auto p=f.build();f.verify(p);EXPECT_GT(p.offer().counts.content_pages,1u);
}

TEST(CanonicalRangePackage, ExactRetainedByteAndFrameBoundsIncludeManifestAndEnd) {
    PackageFixture f;const auto p=f.build();f.policy.retained_wire_bytes=p.retained_wire_bytes();f.policy.frames=p.frames().size();
    EXPECT_NO_THROW(f.build());--f.policy.retained_wire_bytes;EXPECT_THROW(f.build(),cr::protocol_error);
    f.policy.retained_wire_bytes=p.retained_wire_bytes();--f.policy.frames;EXPECT_THROW(f.build(),cr::protocol_error);
}

TEST(CanonicalRangePackage, ReceiptClassesRemainDistinctAndRequireExactRequestCoverage) {
    PackageFixture f;f.receipt({"original",cr::committed{"namespace","coverage",cr::decision::no_op,3,cr::identity{"PackageSourceRow","A"}}});
    f.verify(f.build());f.receipts[0].value=cr::not_committed{"namespace","coverage"};f.verify(f.build());
    f.receipts[0].value=cr::unknown{cr::unknown_reason::missing_coverage};f.verify(f.build());
    f.receipts.clear();EXPECT_THROW(f.build(),cr::protocol_error);
}

TEST(CanonicalRangePackage, LegacyProvenanceCannotBecomeCommittedOrNotCommitted) {
    PackageFixture f;f.receipt({"original",cr::unknown{cr::unknown_reason::legacy}},std::nullopt);f.verify(f.build());
    f.receipts[0].value=cr::not_committed{"namespace","coverage"};EXPECT_THROW(f.build(),cr::protocol_error);
    f.receipts[0].value=cr::committed{"namespace","coverage",cr::decision::applied,3,std::nullopt};
    EXPECT_THROW(f.build(),cr::protocol_error);
}

TEST(CanonicalRangePackage, LaterReceiptAndWrongOriginalNamespaceOrTargetRefuse) {
    PackageFixture f;f.receipt({"original",cr::committed{"namespace","coverage",cr::decision::applied,8,std::nullopt}});
    EXPECT_THROW(f.build(),cr::protocol_error);
    f.receipts[0].value=cr::committed{"different","coverage",cr::decision::applied,3,std::nullopt};EXPECT_THROW(f.build(),cr::protocol_error);
    f.receipts[0].value=cr::committed{"namespace","coverage",cr::decision::applied,3,cr::identity{"PackageSourceRow","B"}};
    EXPECT_THROW(f.build(),cr::protocol_error);
    f.receipts[0]={"other",cr::unknown{}};EXPECT_THROW(f.build(),cr::protocol_error);
}

TEST(CanonicalRangePackage, MissingRebaseTargetCannotProduceACompletePackage) {
    PackageFixture f;f.receipt({"original",cr::unknown{}},std::nullopt,"missing");EXPECT_THROW(f.build(),cr::protocol_error);
    f.rows.push_back({{"PackageSourceRow","missing"},cr::tombstone{}});f.verify(f.build());
}

TEST(CanonicalRangePackage, SameHeadDeltaCarriesReceiptRebaseAndExplicitTombstone) {
    PackageFixture f;f.request.selection=cr::mode::delta;f.request.base=7;
    f.request.expected={1,f.request.source,{cr::frontier_kind::position,7}};
    f.rows={{{"PackageSourceRow","A"},cr::tombstone{}}};
    f.receipt({"old-original",cr::committed{"namespace","coverage",cr::decision::applied,2,cr::identity{"PackageSourceRow","A"}}});
    const auto p=f.build();f.verify(p);EXPECT_EQ(p.offer().head,p.offer().base);EXPECT_EQ(p.offer().counts.tombstones,1u);
    EXPECT_THROW(f.build(6),cr::protocol_error);
}

TEST(CanonicalRangePackage, DuplicateUnorderedMalformedOrOversizedInputsRefuseWithoutMutation) {
    PackageFixture f;const auto original=f.rows;f.rows.push_back(f.rows.front());EXPECT_THROW(f.build(),cr::protocol_error);
    f.rows=original;std::swap(f.rows[0],f.rows[1]);EXPECT_THROW(f.build(),cr::protocol_error);
    f.rows=original;f.rows[0].value=cr::present{"not scalar JSON"};EXPECT_THROW(f.build(),cr::protocol_error);
    f.rows=original;f.request.budget.content_pages=1;f.seal_request();EXPECT_THROW(f.build(),cr::protocol_error);
    EXPECT_EQ(f.rows,original);
}

TEST(CanonicalRangePackage, FrozenRequestAndLeaseOrRouteSpellingCannotBeRewritten) {
    PackageFixture f;f.request.request_digest[0]=f.request.request_digest[0]=='0'?'1':'0';EXPECT_THROW(f.build(),cr::protocol_error);
    f.seal_request();f.lease.duration_ms=0;EXPECT_THROW(f.build(),cr::protocol_error);f.lease.duration_ms=30000;
    EXPECT_THROW(cr::assemble_package(f.attempt,0,f.request,7,f.lease,f.rows,f.receipts,f.policy),cr::protocol_error);
    f.policy.frames=1;EXPECT_THROW(f.build(),cr::protocol_error);f.policy.frames=66;
    f.policy.retained_wire_bytes=0;EXPECT_THROW(f.build(),cr::protocol_error);
}

TEST(CanonicalRangePackage, ActualCanonicalCaptureProducesCompleteFrozenSourceValues) {
    PackageFixture f;TempDB path{"canonical-range-package"};lattice::configuration config(path.str());config.audit_retention_seconds=0;
    auto owner=std::make_shared<lattice::lattice_db>(config);
    canonical_writer_profile profile{{f.request.source.source_id,f.request.source.epoch,f.request.source.scope_digest,f.request.source.schema_digest},
        {128,65536,128,65536,32,64,64},{"PackageSourceRow"},false};
    auto adapter=canonical_writer_adapter::attach(*owner,profile);auto row=owner->add(PackageSourceRow{"captured"});
    sr::canonical_capture_limits limit{{{65536,16,4096,8192,2,32,128,262144},8,16,16},profile.limits,32,128,2};
    const auto captured=adapter->capture_recovery_owned(owner,profile.binding,std::nullopt,{},limit);
    ASSERT_TRUE(captured.capture);f.rows.clear();
    for(const auto& item:captured.capture->rows) {
        ASSERT_TRUE(item.payload);f.rows.push_back({{item.key.table,item.key.global_id},cr::present{*item.payload}});
    }
    const auto package=f.build(captured.head);f.verify(package);const auto bytes=package.frames();
    row.body="after capture";adapter.reset();owner->close();owner.reset();EXPECT_EQ(package.frames(),bytes);
    ASSERT_EQ(f.rows.size(),1u);const auto values=sr::decode_values(std::get<cr::present>(f.rows[0].value).payload,f.policy.codec.values);
    EXPECT_EQ(std::get<std::string>(values.at("body")),"captured");
    static_assert(!canonical_writer_adapter::serving_capability);
}

TEST(CanonicalRangePackage, ActualImportedOriginalAndItsStoredReceiptSurviveCanonicalPackaging) {
    PackageFixture f;TempDB path{"canonical-range-package-imported"};lattice::configuration config(path.str());config.audit_retention_seconds=0;
    auto owner=std::make_shared<lattice::lattice_db>(config);
    canonical_writer_profile profile{{f.request.source.source_id,f.request.source.epoch,f.request.source.scope_digest,f.request.source.schema_digest},
        {128,65536,128,65536,32,64,64},{"PackageSourceRow"},true};
    auto adapter=canonical_writer_adapter::attach_upstream_for_qualification(owner,profile,{32,65536,262144});
    lattice::audit_log_entry entry;entry.global_id=package_uuid('6');entry.global_row_id=package_uuid('7');
    entry.table_name="PackageSourceRow";entry.operation="INSERT";entry.timestamp="1789819200.0";
    entry.changed_fields_names={"body"};entry.changed_fields={{"body",lattice::any_property(std::string("imported"))}};
    ASSERT_EQ(adapter->apply_upstream_owned(owner,{entry}),std::vector<std::string>{entry.global_id});
    const auto audit=owner->db().query("SELECT isFromRemote FROM AuditLog WHERE globalId=?",{entry.global_id});
    ASSERT_EQ(audit.size(),1u);EXPECT_EQ(std::get<int64_t>(audit[0].at("isFromRemote")),1);
    sr::canonical_capture_limits limit{{{65536,16,4096,8192,2,32,128,262144},8,16,16},profile.limits,32,128,2};
    const auto capture=adapter->capture_recovery_owned(owner,profile.binding,std::nullopt,
        {{entry.global_id,{{entry.table_name,entry.global_row_id}}}},limit);
    ASSERT_TRUE(capture.capture);ASSERT_EQ(capture.capture->rows.size(),1u);ASSERT_EQ(capture.capture->receipts.size(),1u);
    const auto& stored=capture.capture->receipts[0].stored;ASSERT_TRUE(stored);
    ASSERT_TRUE(stored->original.target);EXPECT_EQ(stored->original.outcome,canonical_receipt_outcome::applied);
    f.rows.clear();for(const auto& item:capture.capture->rows) {
        ASSERT_TRUE(item.payload);f.rows.push_back({{item.key.table,item.key.global_id},cr::present{*item.payload}});
    }
    // Mechanical namespace/coverage spellings only. This test proves actual
    // imported state/receipt encoding, not a production coverage issuer.
    f.receipt({stored->original.original_id,cr::committed{"namespace","coverage",cr::decision::applied,
        static_cast<uint64_t>(stored->position),cr::identity{stored->original.target->table,stored->original.target->global_id}}},
        std::string("namespace"),entry.global_row_id);
    const auto package=f.build(capture.head);f.verify(package);
    const auto values=sr::decode_values(std::get<cr::present>(f.rows[0].value).payload,f.policy.codec.values);
    EXPECT_EQ(std::get<std::string>(values.at("body")),"imported");
    EXPECT_EQ(package.offer().counts.receipts,1u);EXPECT_EQ(package.offer().counts.rebase_identities,1u);
    adapter.reset();owner->close();
}

namespace {
void sequence_reseal(cr::frame& frame,const cr::limits& limits) {
    if(auto* p=std::get_if<cr::content_page>(&frame.body)) {
        p->count=p->items.size();p->bytes=0;
        for(const auto& item:p->items)p->bytes+=cr::content_record_bytes(item,limits);
        p->digest=cr::page_sha256(*p,limits);
    }
    if(auto* p=std::get_if<cr::receipt_page>(&frame.body)) {
        p->count=p->items.size();p->bytes=0;
        for(const auto& item:p->items)p->bytes+=cr::receipt_record_bytes(item,limits);
        p->digest=cr::page_sha256(*p,limits);
    }
}
struct sequence_counter_scope {
    cr::sequence_test_observation::counters value;
    cr::sequence_test_observation::counters* previous=cr::sequence_test_observation::current;
    sequence_counter_scope(){cr::sequence_test_observation::current=&value;}
    ~sequence_counter_scope(){cr::sequence_test_observation::current=previous;}
};
}
TEST(CanonicalValidatedSequence, EveryPrefixMatchesStrictDTOAndExactWireAcrossReceiptKinds) {
    for(unsigned kind=0;kind<3;++kind) {
        PackageFixture f;
        cr::receipt_item receipt{"original",cr::unknown{}};
        if(kind==0)receipt.value=cr::committed{"namespace","coverage",cr::decision::no_op,3,cr::identity{"PackageSourceRow","A"}};
        if(kind==1)receipt.value=cr::not_committed{"namespace","coverage"};
        f.receipt(receipt);auto package=f.build();
        auto reference=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
        EXPECT_EQ(cursor.snapshot(),reference);
        for(size_t i=1;i<package.frames().size();++i) {
            const auto frame=cr::decode(package.frames()[i],f.policy.codec);
            EXPECT_EQ(cr::encode(frame,f.policy.codec),package.frames()[i]);
            reference=cr::propose(reference,frame,f.policy.codec);cursor.advance(frame);
            EXPECT_EQ(cursor.snapshot(),reference);
            EXPECT_EQ(cr::encode_state(cursor.snapshot(),f.policy.codec),cr::encode_state(reference,f.policy.codec));
        }
        EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
    }
}
TEST(CanonicalValidatedSequence, OwnsFrozenInputsAndMoveRetainsProgressWithoutImportBypass) {
    PackageFixture f;f.receipt({"original",cr::unknown{}});auto package=f.build();
    cr::validated_sequence original(f.attempt,f.request,package.offer(),f.policy.codec);
    original.advance(cr::decode(package.frames()[1],f.policy.codec));const auto before=original.snapshot();
    cr::validated_sequence cursor(std::move(original));EXPECT_EQ(cursor.snapshot(),before);
    const auto codec=f.policy.codec;f.request.receipts.clear();f.request.request_digest.assign(64,'0');f.policy.codec.maximum={};
    for(size_t i=2;i<package.frames().size();++i)cursor.advance(cr::decode(package.frames()[i],codec));
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
    EXPECT_THROW(original.advance(cr::decode(package.frames().back(),codec)),cr::protocol_error);
    static_assert(!std::is_copy_constructible_v<cr::validated_sequence>);
    static_assert(!std::is_constructible_v<cr::validated_sequence,cr::sequence_state,cr::limits>);
}
TEST(CanonicalValidatedSequence, CorruptPageAttemptOrderCountsAndDigestLeaveExactPriorState) {
    PackageFixture f;f.receipt({"original",cr::unknown{}});auto package=f.build();
    cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
    const auto before=cursor.snapshot();const auto valid=cr::decode(package.frames()[1],f.policy.codec);
    for(unsigned variant=0;variant<6;++variant) {
        auto bad=valid;auto& page=std::get<cr::content_page>(bad.body);
        if(variant==0)bad.logical.sequence++;
        if(variant==1){++page.index;sequence_reseal(bad,f.policy.codec);}
        if(variant==2)++page.count;
        if(variant==3)++page.bytes;
        if(variant==4)page.digest.assign(64,'0');
        if(variant==5){page.manifest_digest.assign(64,'0');sequence_reseal(bad,f.policy.codec);}
        EXPECT_THROW(cr::propose(before,bad,f.policy.codec),cr::protocol_error);
        EXPECT_THROW(cursor.advance(bad),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),before);
    }
    EXPECT_THROW(cursor.advance(cr::decode(package.frames().back(),f.policy.codec)),cr::protocol_error);
    EXPECT_EQ(cursor.snapshot(),before);
    for(size_t i=1;i<package.frames().size();++i)cursor.advance(cr::decode(package.frames()[i],f.policy.codec));
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}
TEST(CanonicalValidatedSequence, RebaseSkipAndReceiptBindingRefuseBeforeHashOrProgressPublication) {
    PackageFixture f;f.receipt({"original",cr::committed{"namespace","coverage",cr::decision::applied,3,cr::identity{"PackageSourceRow","A"}}});
    auto package=f.build();cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
    auto first=cr::decode(package.frames()[1],f.policy.codec);const auto initial=cursor.snapshot();
    auto& page=std::get<cr::content_page>(first.body);page.items.front().key.id="AA";sequence_reseal(first,f.policy.codec);
    EXPECT_THROW(cr::propose(initial,first,f.policy.codec),cr::protocol_error);
    EXPECT_THROW(cursor.advance(first),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),initial);
    size_t i=1;
    for(;i<package.frames().size();++i) {
        const auto frame=cr::decode(package.frames()[i],f.policy.codec);
        if(std::holds_alternative<cr::receipt_page>(frame.body))break;
        cursor.advance(frame);
    }
    ASSERT_LT(i,package.frames().size());const auto prior=cursor.snapshot();
    for(unsigned variant=0;variant<4;++variant) {
        auto bad=cr::decode(package.frames()[i],f.policy.codec);auto& item=std::get<cr::receipt_page>(bad.body).items[0];
        auto& receipt=std::get<cr::committed>(item.value);
        if(variant==0)item.original_id="different";
        if(variant==1)receipt.namespace_id="other";
        if(variant==2)receipt.accepted_target->id="B";
        if(variant==3)receipt.position=8;
        sequence_reseal(bad,f.policy.codec);
        EXPECT_THROW(cr::propose(prior,bad,f.policy.codec),cr::protocol_error);
        EXPECT_THROW(cursor.advance(bad),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),prior);
    }
    for(;i<package.frames().size();++i)cursor.advance(cr::decode(package.frames()[i],f.policy.codec));
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}
TEST(CanonicalValidatedSequence, RestartByteBoundaryMatchesStrictProposalsIncludingEscapedLastIdentity) {
    PackageFixture f;f.rows={f.row("A","first"),f.row("B\"\\","second"),f.row("C","third")};auto package=f.build();
    auto initial=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
    const auto first=cr::decode(package.frames()[1],f.policy.codec);
    const auto next=cr::propose(initial,first,f.policy.codec);
    const auto size=cr::encode_state(next,f.policy.codec).size();
    ASSERT_GT(size,cr::encode_state(initial,f.policy.codec).size());
    for(int extra=-1;extra<=1;++extra) {
        auto limits=f.policy.codec;limits.restart_bytes=size+extra;
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),limits);
        if(extra<0) {
            EXPECT_THROW(cr::propose(initial,first,limits),cr::protocol_error);
            EXPECT_THROW(cursor.advance(first),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),initial);
        }else {EXPECT_EQ(cr::propose(initial,first,limits),next);cursor.advance(first);EXPECT_EQ(cursor.snapshot(),next);}
    }
}
TEST(CanonicalValidatedSequence, TerminalWholeHashesRefuseEvenWhenAllPageHashesAndSequenceAreValid) {
    for(bool corrupt_receipts:{false,true}) {
        PackageFixture f;f.receipt({"original",cr::unknown{}});auto package=f.build();auto manifest=package.offer();
        (corrupt_receipts?manifest.receipt_digest:manifest.content_digest).assign(64,'0');
        manifest.manifest_digest=cr::manifest_sha256(manifest,f.policy.codec);
        cr::validated_sequence cursor(f.attempt,f.request,manifest,f.policy.codec);
        auto reference=cr::begin(f.attempt,f.request,manifest,f.policy.codec);
        for(size_t i=1;i+1<package.frames().size();++i) {
            auto frame=cr::decode(package.frames()[i],f.policy.codec);
            std::visit([&](auto& body){using T=std::decay_t<decltype(body)>;
                if constexpr(std::is_same_v<T,cr::content_page>||std::is_same_v<T,cr::receipt_page>)body.manifest_digest=manifest.manifest_digest;
            },frame.body);sequence_reseal(frame,f.policy.codec);
            cursor.advance(frame);reference=cr::propose(reference,frame,f.policy.codec);
        }
        const cr::frame terminal{f.attempt,1,cr::end{manifest.manifest_digest}};const auto prior=cursor.snapshot();
        // Public proposals deliberately remain sequence-only, as documented.
        EXPECT_EQ(cr::propose(reference,terminal,f.policy.codec).status,cr::phase::sequence_complete_unverified);
        EXPECT_THROW(cursor.advance(terminal),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),prior);
        EXPECT_THROW(cursor.advance(terminal),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),prior);
    }
}
TEST(CanonicalValidatedSequence, FullRequestWorkIsConstantAcrossTwoHundredFiftySixPages) {
    PackageFixture f;auto& b=f.policy.codec;
    b.maximum.frame_bytes=65536;b.maximum.items_per_page=1;b.maximum.content_pages=128;b.maximum.receipt_pages=128;b.maximum.receipts=128;
    b.request_entries=128;b.nodes=16384;f.policy.frames=258;f.request.budget=b.maximum;f.rows.clear();
    for(unsigned i=0;i<128;++i) {
        const auto id="id-"+std::to_string(1000+i);f.rows.push_back(f.row(id,"payload"));
        const auto original="original-"+std::to_string(1000+i);
        f.request.receipts.push_back({original,"namespace",{{"PackageSourceRow",id}}});f.receipts.push_back({original,cr::unknown{}});
    }
    f.seal_request();auto package=f.build();ASSERT_EQ(package.frames().size(),258u);
    sequence_counter_scope observed;cr::validated_sequence cursor(f.attempt,f.request,package.offer(),b);
    const auto construction=observed.value;EXPECT_EQ(construction.cursors,1u);
    EXPECT_GT(construction.request_validations,0u);EXPECT_GT(construction.rebase_builds,0u);EXPECT_GT(construction.restart_objects,0u);
    for(size_t i=1;i<package.frames().size();++i)cursor.advance(cr::decode(package.frames()[i],b));
    EXPECT_EQ(observed.value.request_validations,construction.request_validations);
    EXPECT_EQ(observed.value.rebase_builds,construction.rebase_builds);
    EXPECT_EQ(observed.value.restart_objects,construction.restart_objects);
    EXPECT_EQ(observed.value.transitions,257u);EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}
TEST(CanonicalValidatedSequence, RawDTOAndRestartStillRejectForgedImmutableProofAndBitmap) {
    PackageFixture f;f.receipt({"original",cr::unknown{}});auto package=f.build();
    const auto first=cr::decode(package.frames()[1],f.policy.codec);
    auto state=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
    auto forged=state;forged.frozen_request.receipts.clear();
    EXPECT_THROW(cr::propose(forged,first,f.policy.codec),cr::protocol_error);
    EXPECT_THROW(cr::encode_state(forged,f.policy.codec),cr::protocol_error);
    EXPECT_THROW(cr::validated_sequence(f.attempt,forged.frozen_request,package.offer(),f.policy.codec),cr::protocol_error);
    forged=state;forged.rebase_seen[0]=1;
    EXPECT_THROW(cr::propose(forged,first,f.policy.codec),cr::protocol_error);
    auto raw=cr::encode_state(state,f.policy.codec);const auto at=raw.find("\"rebase_seen\":\"0\"");ASSERT_NE(at,std::string::npos);
    raw.replace(at,std::string("\"rebase_seen\":\"0\"").size(),"\"rebase_seen\":\"1\"");
    EXPECT_THROW(cr::decode_state(raw,f.attempt,f.policy.codec),cr::protocol_error);
}

TEST(CanonicalValidatedSequence, TerminalRestartBudgetRefusalRetainsReceivingStateAndHashes) {
    PackageFixture f;auto package=f.build();auto reference=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
    for(size_t i=1;i+1<package.frames().size();++i)reference=cr::propose(reference,cr::decode(package.frames()[i],f.policy.codec),f.policy.codec);
    const auto terminal=cr::decode(package.frames().back(),f.policy.codec);
    const auto complete=cr::propose(reference,terminal,f.policy.codec);
    const auto exact=cr::encode_state(complete,f.policy.codec).size();
    ASSERT_GT(exact,cr::encode_state(reference,f.policy.codec).size());
    for(int delta=-1;delta<=0;++delta) {
        auto limits=f.policy.codec;limits.restart_bytes=exact+delta;
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),limits);
        for(size_t i=1;i+1<package.frames().size();++i)cursor.advance(cr::decode(package.frames()[i],limits));
        if(delta<0) {
            EXPECT_THROW(cr::propose(reference,terminal,limits),cr::protocol_error);
            EXPECT_THROW(cursor.advance(terminal),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),reference);
        }else {cursor.advance(terminal);EXPECT_EQ(cursor.snapshot(),complete);}
    }
}
TEST(CanonicalValidatedSequence, EmptyFullAndSameHeadRequestedTombstoneMatchPublicSequence) {
    for(bool same_head:{false,true}) {
        PackageFixture f;f.rows.clear();
        if(same_head) {
            f.receipt({"original",cr::unknown{}},std::nullopt,"absent");
            f.request.selection=cr::mode::delta;f.request.base=7;f.request.expected.revision=1;
            f.request.expected.binding=f.request.source;f.request.expected.base={cr::frontier_kind::position,7};
            f.rows={{{"PackageSourceRow","absent"},cr::tombstone{}}};f.seal_request();
        }
        auto package=f.build();auto reference=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
        for(size_t i=1;i<package.frames().size();++i) {
            const auto frame=cr::decode(package.frames()[i],f.policy.codec);
            reference=cr::propose(reference,frame,f.policy.codec);cursor.advance(frame);EXPECT_EQ(cursor.snapshot(),reference);
        }
        EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
    }
}

TEST(CanonicalValidatedSequence, ExactRestartNodeBudgetMatchesSAXWhenLastIdentityAppears) {
    PackageFixture f;auto package=f.build();auto initial=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
    const auto first=cr::decode(package.frames()[1],f.policy.codec);
    const auto next=cr::propose(initial,first,f.policy.codec);
    const auto nodes=[](const std::string& raw) {
        size_t count=0;
        (void)nlohmann::json::parse(raw,[&](int,nlohmann::json::parse_event_t event,nlohmann::json&){
            if(event!=nlohmann::json::parse_event_t::object_end&&event!=nlohmann::json::parse_event_t::array_end)++count;
            return true;
        });return count;
    };
    const auto exact=nodes(cr::encode_state(next,f.policy.codec));
    ASSERT_GT(exact,nodes(cr::encode_state(initial,f.policy.codec)));
    for(int delta=-1;delta<=0;++delta) {
        auto limits=f.policy.codec;limits.nodes=exact+delta;
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),limits);
        if(delta<0) {
            EXPECT_THROW(cr::propose(initial,first,limits),cr::protocol_error);
            EXPECT_THROW(cursor.advance(first),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),initial);
        }else {cursor.advance(first);EXPECT_EQ(cursor.snapshot(),next);}
    }
}

TEST(CanonicalValidatedSequence, CanonicalBytesMatchStrictDTOPrefixesForEveryV2ReceiptClass) {
    for(unsigned kind=0;kind<3;++kind) {
        PackageFixture f;cr::receipt_item receipt{"original",cr::unknown{}};
        if(kind==0)receipt.value=cr::committed{"namespace","coverage",cr::decision::no_op,3,cr::identity{"PackageSourceRow","A"}};
        if(kind==1)receipt.value=cr::not_committed{"namespace","coverage"};
        f.receipt(receipt);const auto package=f.build();
        const auto request_raw=cr::encode({f.attempt,1,f.request},f.policy.codec);
        auto request=cr::decode_canonical(request_raw,f.policy.codec);
        EXPECT_EQ(std::get<cr::request>(request.body),f.request);
        EXPECT_THROW(cr::decode_canonical(" "+request_raw,f.policy.codec),cr::protocol_error);
        // A returned DTO is mutable data, never a validation token.
        std::get<cr::request>(request.body).request_digest.assign(64,'0');
        EXPECT_THROW(cr::encode(request,f.policy.codec),cr::protocol_error);
        auto reference=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
        for(size_t i=1;i<package.frames().size();++i) {
            const auto strict=cr::decode(package.frames()[i],f.policy.codec);
            reference=cr::propose(reference,strict,f.policy.codec);
            const auto decoded=cursor.advance_canonical(package.frames()[i],1);
            EXPECT_EQ(cr::encode(decoded,f.policy.codec),package.frames()[i]);
            EXPECT_EQ(cursor.snapshot(),reference);
        }
        EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
    }
}

TEST(CanonicalValidatedSequence, CanonicalByteRefusalsKeepExactStateAndPermitTheOriginalRetry) {
    PackageFixture f;f.receipt({"original",cr::unknown{}});const auto package=f.build();
    cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
    const auto before=cursor.snapshot();const auto& raw=package.frames()[1];
    const auto original=nlohmann::json::parse(raw);
    std::vector<std::string> rejected{" "+raw,raw+"\n",package.frames().front(),package.frames().back(),
        cr::encode({f.attempt,1,f.request},f.policy.codec)};
    auto duplicate=raw;const auto route=duplicate.find("\"route_generation\":\"1\"");
    ASSERT_NE(route,std::string::npos);duplicate.insert(route,"\"route_generation\":\"1\",");rejected.push_back(duplicate);
    for(unsigned variant=0;variant<7;++variant) {
        auto bad=original;auto& envelope=bad["latticeCanonicalRange"];auto& body=envelope["body"];
        if(variant==0)envelope["attempt"]["sequence"]="3";
        if(variant==1)envelope["route_generation"]="2";
        if(variant==2)body["count"]="0";
        if(variant==3)body["bytes"]="0";
        if(variant==4)body["digest"]=std::string(64,'0');
        if(variant==5)body["unexpected"]=true;
        if(variant==6)body["index"]="1";
        rejected.push_back(bad.dump());
    }
    for(size_t i=0;i<rejected.size();++i) {
        SCOPED_TRACE(i);EXPECT_THROW(cursor.advance_canonical(rejected[i],1),cr::protocol_error);
        EXPECT_EQ(cursor.snapshot(),before);
    }
    EXPECT_THROW(cursor.advance_canonical(raw,2),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),before);
    for(size_t i=1;i<package.frames().size();++i)cursor.advance_canonical(package.frames()[i],1);
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
    const auto complete=cursor.snapshot();
    EXPECT_THROW(cursor.advance_canonical(package.frames().back(),1),cr::protocol_error);
    EXPECT_EQ(cursor.snapshot(),complete);
}

TEST(CanonicalValidatedSequence, CanonicalBytesEnforceFrozenRequestEscapedFrameLimit) {
    PackageFixture f;f.request.budget.frame_bytes=4096;f.seal_request();const auto package=f.build();
    auto oversized=cr::decode(package.frames()[1],f.policy.codec);
    std::get<cr::content_page>(oversized.body).items[0]=f.row("A",std::string(2000,'\\'));
    sequence_reseal(oversized,f.policy.codec);
    const auto raw=cr::encode(oversized,f.policy.codec);
    ASSERT_GT(raw.size(),f.request.budget.frame_bytes);ASSERT_LE(raw.size(),f.policy.codec.maximum.frame_bytes);
    // This is a well-formed canonical frame under the outer profile. The
    // cursor must still apply the narrower limits frozen in its request.
    EXPECT_NO_THROW(cr::decode_canonical(raw,f.policy.codec));
    cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
    const auto before=cursor.snapshot();EXPECT_THROW(cursor.advance_canonical(raw,1),cr::protocol_error);
    EXPECT_EQ(cursor.snapshot(),before);
    for(size_t i=1;i<package.frames().size();++i)cursor.advance_canonical(package.frames()[i],1);
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}

TEST(CanonicalValidatedSequence, CanonicalTerminalWholeHashRefusalDoesNotPublishProgress) {
    for(bool corrupt_receipts:{false,true}) {
        PackageFixture f;f.receipt({"original",cr::unknown{}});const auto package=f.build();auto manifest=package.offer();
        (corrupt_receipts?manifest.receipt_digest:manifest.content_digest).assign(64,'0');
        manifest.manifest_digest=cr::manifest_sha256(manifest,f.policy.codec);
        cr::validated_sequence cursor(f.attempt,f.request,manifest,f.policy.codec);
        auto reference=cr::begin(f.attempt,f.request,manifest,f.policy.codec);
        for(size_t i=1;i+1<package.frames().size();++i) {
            auto frame=cr::decode(package.frames()[i],f.policy.codec);
            std::visit([&](auto& body){using T=std::decay_t<decltype(body)>;
                if constexpr(std::is_same_v<T,cr::content_page>||std::is_same_v<T,cr::receipt_page>)body.manifest_digest=manifest.manifest_digest;
            },frame.body);sequence_reseal(frame,f.policy.codec);
            cursor.advance_canonical(cr::encode(frame,f.policy.codec),1);
            reference=cr::propose(reference,frame,f.policy.codec);EXPECT_EQ(cursor.snapshot(),reference);
        }
        const cr::frame terminal{f.attempt,1,cr::end{manifest.manifest_digest}};
        EXPECT_EQ(cr::propose(reference,terminal,f.policy.codec).status,cr::phase::sequence_complete_unverified);
        const auto before=cursor.snapshot();const auto raw=cr::encode(terminal,f.policy.codec);
        EXPECT_THROW(cursor.advance_canonical(raw,1),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),before);
        EXPECT_THROW(cursor.advance_canonical(raw,1),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),before);
    }
}

TEST(CanonicalValidatedSequence, CanonicalTerminalRestartBoundaryAndMovedFromCursorRemainStrict) {
    PackageFixture f;const auto package=f.build();auto reference=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
    for(size_t i=1;i+1<package.frames().size();++i)reference=cr::propose(reference,cr::decode(package.frames()[i],f.policy.codec),f.policy.codec);
    const auto complete=cr::propose(reference,cr::decode(package.frames().back(),f.policy.codec),f.policy.codec);
    const auto exact=cr::encode_state(complete,f.policy.codec).size();ASSERT_GT(exact,cr::encode_state(reference,f.policy.codec).size());
    for(int delta=-1;delta<=0;++delta) {
        auto limits=f.policy.codec;limits.restart_bytes=exact+delta;
        cr::validated_sequence original(f.attempt,f.request,package.offer(),limits);
        for(size_t i=1;i+1<package.frames().size();++i)original.advance_canonical(package.frames()[i],1);
        cr::validated_sequence cursor(std::move(original));EXPECT_EQ(cursor.snapshot(),reference);
        EXPECT_THROW(original.advance_canonical(package.frames().back(),1),cr::protocol_error);
        if(delta<0){EXPECT_THROW(cursor.advance_canonical(package.frames().back(),1),cr::protocol_error);EXPECT_EQ(cursor.snapshot(),reference);}
        else{cursor.advance_canonical(package.frames().back(),1);EXPECT_EQ(cursor.snapshot(),complete);}
    }
}

namespace {
// Count the parser's actual SAX events, including keys and container starts;
// a DOM element count would understate the initial wire parser's node budget.
struct CanonicalWireSAXCounts : nlohmann::json_sax<nlohmann::json> {
    size_t nodes=0,depth=0,max_depth=0,max_string=0;
    bool node(){++nodes;return true;}
    bool null() override{return node();}
    bool boolean(bool) override{return node();}
    bool number_integer(number_integer_t) override{return node();}
    bool number_unsigned(number_unsigned_t) override{return node();}
    bool number_float(number_float_t,const string_t&) override{return node();}
    bool string(string_t& value) override{max_string=std::max(max_string,value.size());return node();}
    bool key(string_t& value) override{return string(value);}
    bool binary(binary_t&) override{return false;}
    bool start(){++depth;max_depth=std::max(max_depth,depth);return node();}
    bool start_object(size_t) override{return start();}
    bool start_array(size_t) override{return start();}
    bool end_object() override{--depth;return true;}
    bool end_array() override{--depth;return true;}
    bool parse_error(size_t,const std::string&,const nlohmann::detail::exception&) override{return false;}
};
template<class F> void canonical_reparse_error(F&& operation,const char* expected) {
    try{operation();ADD_FAILURE()<<"canonical operation unexpectedly accepted";}
    catch(const cr::protocol_error& error){EXPECT_STREQ(error.what(),expected);}
}
}

TEST(CanonicalValidatedSequence, CanonicalBytesKeepExactInitialSAXBoundariesForEveryFrameKind) {
    PackageFixture f;f.rows[0]=f.row("A",std::string(160,'q'));
    f.receipt({"original",cr::committed{"namespace","coverage",cr::decision::applied,3,cr::identity{"PackageSourceRow","A"}}});
    const auto package=f.build();auto wires=package.frames();
    wires.push_back(cr::encode({f.attempt,1,f.request},f.policy.codec));
    bool tested_string_refusal=false;
    for(const auto& raw:wires) {
        CanonicalWireSAXCounts measured;ASSERT_TRUE(nlohmann::json::sax_parse(raw,&measured));
        ASSERT_EQ(measured.depth,0u);ASSERT_GT(measured.nodes,1u);ASSERT_GT(measured.max_depth,1u);
        ASSERT_GE(measured.max_string,64u);
        for(unsigned dimension=0;dimension<3;++dimension) {
            SCOPED_TRACE(dimension);auto exact=f.policy.codec;
            if(dimension==0)exact.nodes=measured.nodes;
            if(dimension==1)exact.depth=measured.max_depth;
            if(dimension==2)exact.string_bytes=measured.max_string;
            EXPECT_EQ(cr::encode(cr::decode_canonical(raw,exact),f.policy.codec),raw);
            // string_bytes below 64 is an invalid profile, not a SAX boundary.
            if(dimension==2&&measured.max_string==64)continue;
            auto below=exact;
            if(dimension==0)--below.nodes;
            if(dimension==1)--below.depth;
            if(dimension==2){--below.string_bytes;tested_string_refusal=true;}
            canonical_reparse_error([&]{(void)cr::decode_canonical(raw,below);},"invalid or over-budget canonical JSON");
        }
    }
    EXPECT_TRUE(tested_string_refusal);
}

TEST(CanonicalValidatedSequence, CanonicalBytesEnforceExactRawAndOwnRequestCapsWithLegalProfiles) {
    PackageFixture f;const auto package=f.build();const auto& terminal=package.frames().back();
    auto exact=f.policy.codec;exact.maximum.frame_bytes=terminal.size();
    exact.maximum.payload_bytes=1;exact.string_bytes=64;
    EXPECT_EQ(cr::encode(cr::decode_canonical(terminal,exact),exact),terminal);
    --exact.maximum.frame_bytes;
    canonical_reparse_error([&]{(void)cr::decode_canonical(terminal,exact);},"raw canonical frame exceeds budget");

    // Keep every advertised limit legal while finding the request's exact
    // encoded size, including its own decimal cap and recomputed digest.
    f.request.budget.payload_bytes=1;f.seal_request();
    auto raw=cr::encode({f.attempt,1,f.request},f.policy.codec);
    auto wire=nlohmann::json::parse(raw);
    for(unsigned iteration=0;iteration<8;++iteration) {
        f.request.budget.frame_bytes=raw.size();f.seal_request();
        wire["latticeCanonicalRange"]["body"]["limits"]["frame_bytes"]=std::to_string(f.request.budget.frame_bytes);
        wire["latticeCanonicalRange"]["body"]["request_digest"]=f.request.request_digest;
        raw=wire.dump();if(raw.size()==f.request.budget.frame_bytes)break;
    }
    ASSERT_EQ(raw.size(),f.request.budget.frame_bytes);
    ASSERT_LT(raw.size(),f.policy.codec.maximum.frame_bytes);
    ASSERT_LE(f.request.budget.payload_bytes,f.request.budget.frame_bytes);
    EXPECT_EQ(cr::encode({f.attempt,1,f.request},f.policy.codec),raw);
    EXPECT_EQ(std::get<cr::request>(cr::decode_canonical(raw,f.policy.codec).body),f.request);
    --f.request.budget.frame_bytes;f.seal_request();
    wire["latticeCanonicalRange"]["body"]["limits"]["frame_bytes"]=std::to_string(f.request.budget.frame_bytes);
    wire["latticeCanonicalRange"]["body"]["request_digest"]=f.request.request_digest;
    raw=wire.dump();ASSERT_GT(raw.size(),f.request.budget.frame_bytes);
    ASSERT_LE(raw.size(),f.policy.codec.maximum.frame_bytes);
    ASSERT_LE(f.request.budget.payload_bytes,f.request.budget.frame_bytes);
    canonical_reparse_error([&]{(void)cr::decode(raw,f.policy.codec);},"request raw frame exceeds advertised budget");
    canonical_reparse_error([&]{(void)cr::decode_canonical(raw,f.policy.codec);},"request raw frame exceeds advertised budget");
}

TEST(CanonicalValidatedSequence, CanonicalBytesRejectEquivalentSpellingEscapedDuplicateKeysAndWrongTypes) {
    PackageFixture f;f.attempt.channel="package/channel";f.seal_request();const auto package=f.build();
    const auto& raw=package.frames().back();const auto original=nlohmann::json::parse(raw);
    std::vector<std::string> equivalents{original.dump(2)};
    auto escaped=raw;auto at=escaped.find("package/channel");ASSERT_NE(at,std::string::npos);
    escaped.replace(at,std::string("package/channel").size(),"package\\/channel");equivalents.push_back(escaped);
    escaped=raw;at=escaped.find("\"version\"");ASSERT_NE(at,std::string::npos);
    escaped.replace(at,std::string("\"version\"").size(),"\"\\u0076ersion\"");equivalents.push_back(escaped);
    for(const auto& equivalent:equivalents) {
        ASSERT_NE(equivalent,raw);EXPECT_EQ(cr::encode(cr::decode(equivalent,f.policy.codec),f.policy.codec),raw);
        canonical_reparse_error([&]{(void)cr::decode_canonical(equivalent,f.policy.codec);},"canonical frame bytes differ from exact spelling");
    }
    auto duplicate=raw;at=duplicate.find("\"version\":2");ASSERT_NE(at,std::string::npos);
    duplicate.insert(at,"\"\\u0076ersion\":2,");
    canonical_reparse_error([&]{(void)cr::decode_canonical(duplicate,f.policy.codec);},"invalid or over-budget canonical JSON");
    std::vector<std::string> malformed{raw.substr(0,raw.size()-1),raw+"{}"};
    auto invalid_utf8=raw;at=invalid_utf8.find("package/channel");ASSERT_NE(at,std::string::npos);
    invalid_utf8[at]=static_cast<char>(0xff);malformed.push_back(invalid_utf8);
    for(const auto& value:{nlohmann::json(2.0),nlohmann::json(true),nlohmann::json("2"),nlohmann::json(nullptr)}) {
        auto bad=original;bad["latticeCanonicalRange"]["version"]=value;malformed.push_back(bad.dump());
    }
    auto bad=original;bad["latticeCanonicalRange"]["route_generation"]=1;malformed.push_back(bad.dump());
    for(const auto& invalid:malformed)EXPECT_THROW(cr::decode_canonical(invalid,f.policy.codec),cr::protocol_error);
}

TEST(CanonicalValidatedSequence, CanonicalBytesPreserveEscapedUnicodeAndRealInsideTypedPayloadString) {
    PackageFixture f;const std::string payload=R"({"real":{"kind":7,"value":1.25},"text":{"kind":2,"value":"quote\" slash\\ nul\u0000 caf\u00e9 \ud83d\ude80"}})";
    f.attempt.channel="package-\xc3\xa9-\xf0\x9f\x9a\x80";f.seal_request();
    f.rows={{{"PackageSourceRow","A"},cr::present{payload}}};const auto package=f.build();
    const auto decoded=cr::decode_canonical(package.frames()[1],f.policy.codec);
    const auto& item=std::get<cr::content_page>(decoded.body).items[0];
    EXPECT_EQ(std::get<cr::present>(item.value).payload,payload);
    EXPECT_EQ(cr::encode(decoded,f.policy.codec),package.frames()[1]);
    const auto values=sr::decode_values(std::get<cr::present>(item.value).payload,f.policy.codec.values);
    EXPECT_DOUBLE_EQ(std::get<double>(values.at("real")),1.25);
    const auto expected=std::string("quote\" slash\\ nul")+std::string(1,'\0')+" caf\xc3\xa9 \xf0\x9f\x9a\x80";
    EXPECT_EQ(std::get<std::string>(values.at("text")),expected);
    // These escapes belong to the opaque typed payload. Equivalent outer
    // Unicode spelling still fails exact canonical wire equality.
    const auto escaped=nlohmann::json::parse(package.frames()[1]).dump(-1,' ',true);
    ASSERT_NE(escaped,package.frames()[1]);
    EXPECT_EQ(cr::encode(cr::decode(escaped,f.policy.codec),f.policy.codec),package.frames()[1]);
    EXPECT_THROW(cr::decode_canonical(escaped,f.policy.codec),cr::protocol_error);
}

#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <limits>
namespace {
// Deliberately encodes the hash framing without validating a typed payload.
// These bounded test pages must reach the real parser with a freshly matching
// outer digest even when their inner value grammar is malformed. A valid page
// is compared to the production digest before this helper makes negatives.
void content_append_test_reseal(cr::content_page& page) {
    page.count=page.items.size();page.bytes=0;
    for(const auto& item:page.items) {
        page.bytes+=1+16+item.key.table.size()+item.key.id.size();
        if(const auto* value=std::get_if<cr::present>(&item.value))page.bytes+=8+value->payload.size();
    }
    std::string wire;
    const auto u=[&](uint64_t value){for(int shift=56;shift>=0;shift-=8)wire.push_back(static_cast<char>(value>>shift));};
    const auto s=[&](const std::string& value){u(value.size());wire+=value;};
    s("lattice.canonical-range.v2/content-page");
    const auto nib=[](char c){return c<='9'?c-'0':c-'a'+10;};
    for(size_t i=0;i<page.manifest_digest.size();i+=2)
        wire.push_back(static_cast<char>((nib(page.manifest_digest[i])<<4)|nib(page.manifest_digest[i+1])));
    u(page.index);u(page.count);u(page.bytes);
    for(const auto& item:page.items) {
        const auto* value=std::get_if<cr::present>(&item.value);wire.push_back(value?1:2);
        s(item.key.table);s(item.key.id);if(value)s(value->payload);
    }
    page.digest=picosha2::hash256_hex_string(wire);
}
std::string content_append_test_wire(const std::string& valid,const cr::content_page& page) {
    auto frame=nlohmann::json::parse(valid);auto& body=frame["latticeCanonicalRange"]["body"];
    body["manifest_digest"]=page.manifest_digest;body["index"]=std::to_string(page.index);
    body["count"]=std::to_string(page.count);body["bytes"]=std::to_string(page.bytes);body["digest"]=page.digest;
    body["items"]=nlohmann::json::array();
    for(const auto& item:page.items) {
        nlohmann::json row={{"table",item.key.table},{"id",item.key.id}};
        if(const auto* value=std::get_if<cr::present>(&item.value)){row["tag"]="present";row["payload"]=value->payload;}
        else row["tag"]="tombstone";
        body["items"].push_back(std::move(row));
    }
    return frame.dump();
}
void content_append_test_advance(cr::validated_sequence& cursor,bool raw,const std::string& wire,const cr::frame& frame) {
    if(raw)(void)cursor.advance_canonical(wire,1);else cursor.advance(frame);
}
void content_append_test_finish(cr::validated_sequence& cursor,bool raw,const cr::encoded_package& package,
    const cr::limits& limits,size_t first=1) {
    for(size_t i=first;i<package.frames().size();++i)
        content_append_test_advance(cursor,raw,package.frames()[i],cr::decode(package.frames()[i],limits));
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}
}

TEST(CanonicalValidatedContentAppend, BothEntriesValidateEachItemOnceAndMatchStrictTypedPrefixes) {
    PackageFixture f;
    const sr::row_values values{{"integer",int64_t(-17)},{"real",1.25},{"null",nullptr},
        {"blob",std::vector<uint8_t>{0,1,127,255}},
        {"text",std::string("quote\" slash\\ nul")+std::string(1,'\0')+" caf\xc3\xa9 \xf0\x9f\x9a\x80"}};
    f.rows={{{"PackageSourceRow","A"},cr::present{sr::encode_values(values,f.policy.codec.values)}},
        f.row("B","second"),{{"PackageSourceRow","C"},cr::tombstone{}}};
    f.receipt({"original",cr::unknown{}},std::string("namespace"),"C");const auto package=f.build();
    EXPECT_EQ(sr::decode_values(std::get<cr::present>(f.rows[0].value).payload,f.policy.codec.values),values);
    EXPECT_EQ(cr::content_sha256(package.offer(),f.rows,f.policy.codec),package.offer().content_digest);
    for(bool raw:{false,true}) {
        SCOPED_TRACE(raw);cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
        auto reference=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);uint64_t observed_items=0;
        for(size_t i=1;i<package.frames().size();++i) {
            const auto frame=cr::decode(package.frames()[i],f.policy.codec);
            reference=cr::propose(reference,frame,f.policy.codec);
            const auto* page=std::get_if<cr::content_page>(&frame.body);const auto expected=page?page->items.size():0;
            {sequence_counter_scope count;
                content_append_test_advance(cursor,raw,package.frames()[i],frame);
                EXPECT_EQ(count.value.content_shape_calls,expected);observed_items+=count.value.content_shape_calls;}
            EXPECT_EQ(cursor.snapshot(),reference);
            EXPECT_EQ(cr::encode_state(cursor.snapshot(),f.policy.codec),cr::encode_state(reference,f.policy.codec));
        }
        EXPECT_EQ(observed_items,f.rows.size());EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
    }
}

TEST(CanonicalValidatedContentAppend, PublicHasherRemainsStrictAndCounterSaturatesWithoutChangingHashes) {
    PackageFixture f;const auto package=f.build();
    cr::stream_hasher strict(package.offer(),cr::stream_kind::content,f.policy.codec);
    {sequence_counter_scope count;for(const auto& item:f.rows)strict.append(item);
        EXPECT_EQ(count.value.content_shape_calls,f.rows.size());}
    EXPECT_EQ(strict.finish(),package.offer().content_digest);
    auto bad=f.rows[0];bad.value=cr::present{R"({"body":{"kind":1,"value":true}})"};
    cr::stream_hasher rejected(package.offer(),cr::stream_kind::content,f.policy.codec);
    {sequence_counter_scope count;EXPECT_THROW(rejected.append(bad),cr::protocol_error);
        EXPECT_EQ(count.value.content_shape_calls,1u);}
    // A refused public hasher is discarded under its original contract.
    for(bool raw:{false,true}) {
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
        sequence_counter_scope count;count.value.content_shape_calls=std::numeric_limits<uint64_t>::max();
        content_append_test_finish(cursor,raw,package,f.policy.codec);
        EXPECT_EQ(count.value.content_shape_calls,std::numeric_limits<uint64_t>::max());
    }
    cr::validated_sequence unobserved(f.attempt,f.request,package.offer(),f.policy.codec);
    content_append_test_finish(unobserved,true,package,f.policy.codec);
}

TEST(CanonicalValidatedContentAppend, MalformedTypedPayloadWithMatchingOuterDigestRefusesBothEntriesAndPublicAppend) {
    PackageFixture f;const auto package=f.build();const auto valid=cr::decode(package.frames()[1],f.policy.codec);
    auto calibration=std::get<cr::content_page>(valid.body);const auto digest=calibration.digest;
    content_append_test_reseal(calibration);ASSERT_EQ(calibration.digest,digest);
    ASSERT_EQ(content_append_test_wire(package.frames()[1],calibration),package.frames()[1]);
    const std::vector<std::string> malformed{
        R"({"body":{"kind":1,"value":true}})",
        R"({"body":{"kind":2,"value":"a"},"body":{"kind":2,"value":"b"}})",
        R"({"body":{"kind":2,"kind":2,"value":"a"}})",
        R"({"body":{"kind":2,"value":17}})",R"({"body":{"kind":1,"value":1.0}})",
        R"({"body":{"kind":7,"value":1}})",R"({"body":{"kind":4,"value":"null"}})",
        R"({"body":{"kind":6,"value":"0"}})",R"({"body":{"kind":6,"value":"0g"}})",
        R"({"body":{"kind":6,"value":"AB"}})",R"({"body":{"kind":2,"value":"\ud800"}})",
        R"({"body":{"kind":3,"value":0}})",R"({"body":{"kind":2,"value":"a","extra":0}})"};
    for(size_t index=0;index<malformed.size();++index) {
        SCOPED_TRACE(index);auto bad=valid;auto& page=std::get<cr::content_page>(bad.body);
        page.items[0].value=cr::present{malformed[index]};content_append_test_reseal(page);
        const auto wire=content_append_test_wire(package.frames()[1],page);
        EXPECT_THROW(sr::decode_values(malformed[index],f.policy.codec.values),cr::protocol_error);
        EXPECT_THROW(cr::encode(bad,f.policy.codec),cr::protocol_error);
        cr::stream_hasher strict(package.offer(),cr::stream_kind::content,f.policy.codec);
        EXPECT_THROW(strict.append(page.items[0]),cr::protocol_error);
        for(bool raw:{false,true}) {
            SCOPED_TRACE(raw);cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
            const auto before=cursor.snapshot();
            {sequence_counter_scope count;EXPECT_THROW(content_append_test_advance(cursor,raw,wire,bad),cr::protocol_error);
                EXPECT_EQ(count.value.content_shape_calls,1u);EXPECT_EQ(count.value.transitions,0u);}
            EXPECT_EQ(cursor.snapshot(),before);EXPECT_THROW(cr::propose(before,bad,f.policy.codec),cr::protocol_error);
            content_append_test_finish(cursor,raw,package,f.policy.codec);
        }
    }
}

TEST(CanonicalValidatedContentAppend, FrozenRequestPayloadCapStillRejectsOuterProfileValidContentAtBothEntries) {
    PackageFixture f;f.rows={f.row("A","12345678")};
    const auto exact=std::get<cr::present>(f.rows[0].value).payload.size();
    f.request.budget.payload_bytes=exact;f.seal_request();const auto package=f.build();
    auto bad=cr::decode(package.frames()[1],f.policy.codec);auto& page=std::get<cr::content_page>(bad.body);
    page.items[0]=f.row("A","123456789");sequence_reseal(bad,f.policy.codec);
    const auto wire=cr::encode(bad,f.policy.codec);
    ASSERT_EQ(std::get<cr::present>(page.items[0].value).payload.size(),exact+1);
    EXPECT_NO_THROW(cr::decode_canonical(wire,f.policy.codec));
    for(bool raw:{false,true}) {
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);const auto before=cursor.snapshot();
        {sequence_counter_scope count;canonical_reparse_error([&]{content_append_test_advance(cursor,raw,wire,bad);},"canonical payload exceeds budget");
            EXPECT_EQ(count.value.content_shape_calls,1u);}
        EXPECT_EQ(cursor.snapshot(),before);content_append_test_finish(cursor,raw,package,f.policy.codec);
    }
}

TEST(CanonicalValidatedContentAppend, ExactTypedValueLimitsAndOneBeyondRemainEnforcedAtBothEntries) {
    PackageFixture f;f.rows={f.row("A","12345678")};const auto package=f.build();
    const auto valid=cr::decode(package.frames()[1],f.policy.codec);
    for(unsigned boundary=0;boundary<5;++boundary) {
        SCOPED_TRACE(boundary);auto limits=f.policy.codec;auto bad=valid;auto& page=std::get<cr::content_page>(bad.body);
        std::string payload=R"({"body":{"kind":2,"value":"123456789"}})";
        if(boundary==0)limits.values.value_bytes=8;
        if(boundary==1){limits.values.fields=1;payload=R"({"body":{"kind":2,"value":"12345678"},"other":{"kind":4,"value":null}})";}
        if(boundary==2)limits.values.decoded_bytes=4+1+8;
        if(boundary==3){limits.values.name_bytes=4;payload=R"({"bodyx":{"kind":2,"value":"12345678"}})";}
        if(boundary==4){limits.values.raw_bytes=std::get<cr::present>(f.rows[0].value).payload.size();
            limits.values.value_bytes=8;limits.values.decoded_bytes=4+1+8;}
        page.items[0].value=cr::present{payload};content_append_test_reseal(page);
        const auto wire=content_append_test_wire(package.frames()[1],page);
        EXPECT_NO_THROW(cr::decode_canonical(wire,f.policy.codec));
        EXPECT_THROW(sr::decode_values(payload,limits.values),cr::protocol_error);
        cr::stream_hasher strict(package.offer(),cr::stream_kind::content,limits);
        EXPECT_THROW(strict.append(page.items[0]),cr::protocol_error);
        for(bool raw:{false,true}) {
            cr::validated_sequence cursor(f.attempt,f.request,package.offer(),limits);const auto before=cursor.snapshot();
            {sequence_counter_scope count;EXPECT_THROW(content_append_test_advance(cursor,raw,wire,bad),cr::protocol_error);
                EXPECT_EQ(count.value.content_shape_calls,1u);}
            EXPECT_EQ(cursor.snapshot(),before);content_append_test_finish(cursor,raw,package,limits);
        }
    }
}

TEST(CanonicalValidatedContentAppend, LaterCrossPageRefusalKeepsPriorHashesForTheOriginalRetryAtBothEntries) {
    PackageFixture f;const auto package=f.build();ASSERT_EQ(package.offer().counts.content_pages,2u);
    const auto first=cr::decode(package.frames()[1],f.policy.codec),second=cr::decode(package.frames()[2],f.policy.codec);
    auto bad=second;auto& page=std::get<cr::content_page>(bad.body);page.items[0].key.id="B";sequence_reseal(bad,f.policy.codec);
    const auto wire=cr::encode(bad,f.policy.codec);
    for(bool raw:{false,true}) {
        cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
        content_append_test_advance(cursor,raw,package.frames()[1],first);const auto before=cursor.snapshot();
        const auto strict=cr::propose(before,second,f.policy.codec);
        {sequence_counter_scope count;EXPECT_THROW(content_append_test_advance(cursor,raw,wire,bad),cr::protocol_error);
            EXPECT_EQ(count.value.content_shape_calls,1u);EXPECT_EQ(count.value.transitions,0u);}
        EXPECT_EQ(cursor.snapshot(),before);EXPECT_THROW(cr::propose(before,bad,f.policy.codec),cr::protocol_error);
        content_append_test_advance(cursor,raw,package.frames()[2],second);EXPECT_EQ(cursor.snapshot(),strict);
        content_append_test_finish(cursor,raw,package,f.policy.codec,3);
    }
}

TEST(CanonicalValidatedContentAppend, WholeHashFailureAtEndNeverPublishesEitherEntryCandidate) {
    PackageFixture f;const auto package=f.build();auto wrong=package.offer();wrong.content_digest.assign(64,'0');
    wrong.manifest_digest=cr::manifest_sha256(wrong,f.policy.codec);
    for(bool raw:{false,true}) {
        cr::validated_sequence cursor(f.attempt,f.request,wrong,f.policy.codec);
        auto reference=cr::begin(f.attempt,f.request,wrong,f.policy.codec);
        for(size_t i=1;i+1<package.frames().size();++i) {
            auto frame=cr::decode(package.frames()[i],f.policy.codec);auto& page=std::get<cr::content_page>(frame.body);
            page.manifest_digest=wrong.manifest_digest;sequence_reseal(frame,f.policy.codec);
            const auto wire=cr::encode(frame,f.policy.codec);reference=cr::propose(reference,frame,f.policy.codec);
            {sequence_counter_scope count;content_append_test_advance(cursor,raw,wire,frame);
                EXPECT_EQ(count.value.content_shape_calls,page.items.size());}
            EXPECT_EQ(cursor.snapshot(),reference);
        }
        const cr::frame end{f.attempt,1,cr::end{wrong.manifest_digest}};const auto wire=cr::encode(end,f.policy.codec);
        EXPECT_EQ(cr::propose(reference,end,f.policy.codec).status,cr::phase::sequence_complete_unverified);
        for(unsigned retry=0;retry<2;++retry) {
            {sequence_counter_scope count;EXPECT_THROW(content_append_test_advance(cursor,raw,wire,end),cr::protocol_error);
                EXPECT_EQ(count.value.content_shape_calls,0u);EXPECT_EQ(count.value.transitions,0u);}
            EXPECT_EQ(cursor.snapshot(),reference);
        }
        cr::validated_sequence healthy(f.attempt,f.request,package.offer(),f.policy.codec);
        content_append_test_finish(healthy,raw,package,f.policy.codec);
    }
}

namespace {
struct CursorInitialOutcome {
    std::optional<cr::sequence_state> value;
    std::optional<std::string> error;
};
CursorInitialOutcome cursor_initial_outcome(bool cursor,const cr::attempt& a,const cr::request& r,
    const cr::manifest& m,const cr::limits& b) {
    try {
        if(cursor){cr::validated_sequence value(a,r,m,b);return {value.snapshot(),std::nullopt};}
        return {cr::begin(a,r,m,b),std::nullopt};
    } catch(const cr::protocol_error& error) {return {std::nullopt,std::string(error.what())};}
}
void cursor_initial_matches(const cr::attempt& a,const cr::request& r,const cr::manifest& m,
    const cr::limits& b,bool accepted) {
    const auto strict=cursor_initial_outcome(false,a,r,m,b),actual=cursor_initial_outcome(true,a,r,m,b);
    EXPECT_EQ(bool(strict.value),accepted);EXPECT_EQ(bool(actual.value),accepted);
    EXPECT_EQ(actual.error,strict.error);ASSERT_EQ(bool(actual.value),bool(strict.value));
    if(actual.value){EXPECT_EQ(*actual.value,*strict.value);EXPECT_EQ(cr::encode_state(*actual.value,b),cr::encode_state(*strict.value,b));}
}
void cursor_initial_reseal(const cr::attempt& a,cr::request& r,cr::manifest& m,const cr::limits& b) {
    r.request_digest=cr::request_sha256(a,r,b);m.request_digest=r.request_digest;
    m.rebase_digest=cr::rebase_sha256(a,r,b);m.manifest_digest=cr::manifest_sha256(m,b);
}
}

TEST(CanonicalCursorInitial, FreshFullDeltaEmptyAndRegisteredPrefixesMatchTheStrictOracle) {
    for(bool registered:{false,true})for(bool delta:{false,true})for(bool empty:{false,true}) {
        SCOPED_TRACE(registered);
        SCOPED_TRACE(delta);
        SCOPED_TRACE(empty);
        PackageFixture f;
        if(delta){f.request.selection=cr::mode::delta;f.request.base=7;f.request.expected={1,f.request.source,{cr::frontier_kind::position,7}};}
        if(empty)f.rows.clear();
        else {
            f.request.receipts={{"original-a","namespace",{{"PackageSourceRow","A"},{"PackageSourceRow","B"}}},
                {"original-b","namespace",{{"PackageSourceRow","B"},{"PackageSourceRow","C"}}}};
            f.receipts={{"original-a",cr::unknown{}},{"original-b",cr::unknown{}}};
        }
        if(registered){
            f.request.registered_producer=recovery_receipt_binding{{"cursor-producer",package_uuid('6')},package_uuid('7'),1,1};
            f.request.receipt_namespace="namespace";
            for(size_t i=0;i<f.request.receipts.size();++i){f.request.receipts[i].operation_digest=std::string(64,char('c'+i));f.receipts[i].operation_digest=f.request.receipts[i].operation_digest;}
        }
        f.seal_request();const auto package=cr::assemble_package(f.attempt,1,f.request,7,f.lease,f.rows,f.receipts,f.policy,
            registered?std::optional<uint64_t>{1}:std::nullopt);
        auto strict=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
        cr::validated_sequence original(f.attempt,f.request,package.offer(),f.policy.codec);
        EXPECT_EQ(original.snapshot(),strict);EXPECT_EQ(original.status(),cr::phase::receiving);
        cr::validated_sequence cursor(std::move(original));EXPECT_EQ(cursor.snapshot(),strict);
        EXPECT_THROW(original.advance(cr::decode(package.frames().back(),f.policy.codec)),cr::protocol_error);
        for(size_t i=1;i<package.frames().size();++i){
            const auto frame=cr::decode(package.frames()[i],f.policy.codec);strict=cr::propose(strict,frame,f.policy.codec);
            if(i%2)cursor.advance(frame);else (void)cursor.advance_canonical(package.frames()[i],1);
            EXPECT_EQ(cursor.snapshot(),strict);EXPECT_EQ(cr::encode_state(cursor.snapshot(),f.policy.codec),cr::encode_state(strict,f.policy.codec));
        }
        EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
    }
}

TEST(CanonicalCursorInitial, ActualInitialRestartRetainsEscapedByteNodeDepthAndScalarBoundaries) {
    PackageFixture f;f.attempt.channel=std::string(160,'x')+"\"\\caf\xc3\xa9";
    f.receipt({"original",cr::unknown{}},"namespace","B");f.seal_request();const auto package=f.build();
    const auto strict=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
    const auto restart=cr::encode_state(strict,f.policy.codec);
    std::vector<std::string> wires{cr::encode({f.attempt,1,f.request},f.policy.codec),
        cr::encode({f.attempt,1,package.offer()},f.policy.codec),restart};
    CanonicalWireSAXCounts bound;
    for(const auto& raw:wires){
        CanonicalWireSAXCounts actual;ASSERT_TRUE(nlohmann::json::sax_parse(raw,&actual));ASSERT_EQ(actual.depth,0u);
        bound.nodes=std::max(bound.nodes,actual.nodes);bound.max_depth=std::max(bound.max_depth,actual.max_depth);
        bound.max_string=std::max(bound.max_string,actual.max_string);
    }
    ASSERT_GT(bound.max_string,64u);ASSERT_GT(bound.nodes,1u);ASSERT_GT(bound.max_depth,1u);
    for(unsigned dimension=0;dimension<4;++dimension)for(int offset=-1;offset<=1;++offset){
        SCOPED_TRACE(dimension);
        SCOPED_TRACE(offset);
        auto limit=f.policy.codec;
        if(dimension==0)limit.restart_bytes=restart.size()+offset;
        if(dimension==1)limit.nodes=bound.nodes+offset;
        if(dimension==2)limit.depth=bound.max_depth+offset;
        if(dimension==3)limit.string_bytes=bound.max_string+offset;
        cursor_initial_matches(f.attempt,f.request,package.offer(),limit,offset>=0);
    }
}

TEST(CanonicalCursorInitial, RequestAndManifestOwnWireCapsKeepExactFirstRefusal) {
    for(bool request_larger:{false,true}) {
        SCOPED_TRACE(request_larger);
        PackageFixture f;f.rows.clear();f.request.budget.payload_bytes=1;
        if(request_larger){
            f.rows={{{"PackageSourceRow","A"},cr::tombstone{}}};
            for(unsigned i=0;i<8;++i){const auto id="original-"+std::to_string(i)+std::string(100,'q');
                f.request.receipts.push_back({id,"namespace",{{"PackageSourceRow","A"}}});f.receipts.push_back({id,cr::unknown{}});}
        }else f.lease.id=std::string(256,'\\');
        f.seal_request();auto manifest=f.build().offer();
        auto request_wire=nlohmann::json::parse(cr::encode({f.attempt,1,f.request},f.policy.codec));
        auto manifest_wire=nlohmann::json::parse(cr::encode({f.attempt,1,manifest},f.policy.codec));
        auto update_wire=[&]{
            auto& q=request_wire["latticeCanonicalRange"]["body"];q["limits"]["frame_bytes"]=std::to_string(f.request.budget.frame_bytes);q["request_digest"]=f.request.request_digest;
            auto& m=manifest_wire["latticeCanonicalRange"]["body"];m["request_digest"]=manifest.request_digest;m["rebase_digest"]=manifest.rebase_digest;m["manifest_digest"]=manifest.manifest_digest;
        };
        for(unsigned attempt=0;attempt<12;++attempt){
            const auto cap=std::max(request_wire.dump().size(),manifest_wire.dump().size());
            f.request.budget.frame_bytes=cap;cursor_initial_reseal(f.attempt,f.request,manifest,f.policy.codec);update_wire();
            if(cap==std::max(request_wire.dump().size(),manifest_wire.dump().size()))break;
        }
        const auto exact=f.request.budget.frame_bytes;
        ASSERT_EQ(exact,std::max(request_wire.dump().size(),manifest_wire.dump().size()));
        if(request_larger){ASSERT_GT(request_wire.dump().size(),manifest_wire.dump().size());}
        else {ASSERT_GT(manifest_wire.dump().size(),request_wire.dump().size());}
        EXPECT_EQ(cr::encode({f.attempt,1,f.request},f.policy.codec),request_wire.dump());
        EXPECT_EQ(cr::encode({f.attempt,1,manifest},f.policy.codec),manifest_wire.dump());
        for(int offset=-1;offset<=1;++offset){
            SCOPED_TRACE(offset);
            f.request.budget.frame_bytes=exact+offset;cursor_initial_reseal(f.attempt,f.request,manifest,f.policy.codec);update_wire();
            ASSERT_EQ(std::max(request_wire.dump().size(),manifest_wire.dump().size()),exact);
            cursor_initial_matches(f.attempt,f.request,manifest,f.policy.codec,offset>=0);
        }
    }
}

TEST(CanonicalCursorInitial, MalformedInputsRetainStrictErrorPrecedenceAndDoNotYieldState) {
    PackageFixture f;f.receipt({"original",cr::unknown{}});const auto package=f.build();
    for(unsigned fault=0;fault<24;++fault){
        SCOPED_TRACE(fault);
        auto a=f.attempt;auto r=f.request;auto m=package.offer();auto b=f.policy.codec;
        switch(fault){
        case 0:a.attempt_id="invalid";break;
        case 1:a.sequence=0;break;
        case 2:r.source.authority=std::string("bad")+char(0xff);break;
        case 3:r.request_digest.assign(64,'0');break;
        case 4:m.source.epoch=package_uuid('9');break;
        case 5:++m.counts.present;break;
        case 6:m.counts.content_pages=0;break;
        case 7:m.protection.duration_ms=0;break;
        case 8:b.depth=0;break;
        case 9:r.registered_producer=recovery_receipt_binding{{"producer",package_uuid('6')},package_uuid('7'),1,1};break;
        case 10:r.receipts.push_back(r.receipts[0]);break;
        case 11:r.receipts[0].targets.push_back(r.receipts[0].targets[0]);break;
        case 12:r.selection=cr::mode::delta;break;
        case 13:r.expected.revision=1;break;
        case 14:r.budget.frame_bytes=0;break;
        case 15:m.request_digest.assign(64,'0');m.manifest_digest=cr::manifest_sha256(m,b);break;
        case 16:m.rebase_digest.assign(64,'0');m.manifest_digest=cr::manifest_sha256(m,b);break;
        case 17:r.receipts[0].targets[0].id=std::string("bad")+char(0xff);break;
        case 18:a.sequence=std::numeric_limits<uint64_t>::max();break;
        case 19:b.nodes=0;break;
        case 20:b.string_bytes=63;break;
        case 21:m.coverage_revision=1;break;
        case 22:m.selection=static_cast<cr::mode>(99);break;
        case 23:r.source.authority=std::string("bad\0name",8);b.nodes=1;break;
        }
        cursor_initial_matches(a,r,m,b,false);
    }
    cursor_initial_matches(f.attempt,f.request,package.offer(),f.policy.codec,true);
}

TEST(CanonicalCursorInitial, RequestTargetAndBitmapAdmissionRemainStrictAtEveryBoundary) {
    PackageFixture f;f.request.receipts={{"original-a","namespace",{{"PackageSourceRow","A"},{"PackageSourceRow","B"}}},
        {"original-b","namespace",{{"PackageSourceRow","B"},{"PackageSourceRow","C"}}}};
    f.receipts={{"original-a",cr::unknown{}},{"original-b",cr::unknown{}}};f.seal_request();const auto package=f.build();
    uint64_t bytes=0;for(const auto& q:f.request.receipts)for(const auto& id:q.targets)bytes+=16+id.table.size()+id.id.size();
    for(unsigned dimension=0;dimension<3;++dimension)for(int offset=-1;offset<=1;++offset){
        SCOPED_TRACE(dimension);
        SCOPED_TRACE(offset);
        auto b=f.policy.codec;
        if(dimension==0)b.request_entries=2+offset;
        if(dimension==1)b.request_targets=4+offset; // Total requested targets, distinct union is three.
        if(dimension==2)b.request_target_bytes=bytes+offset;
        cursor_initial_matches(f.attempt,f.request,package.offer(),b,offset>=0);
    }
    auto strict=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);ASSERT_EQ(strict.rebase_seen.size(),3u);
    strict.rebase_seen[0]=1;EXPECT_THROW(cr::encode_state(strict,f.policy.codec),cr::protocol_error);
    const auto valid=cr::encode_state(cr::begin(f.attempt,f.request,package.offer(),f.policy.codec),f.policy.codec);
    auto wire=nlohmann::json::parse(valid);wire["latticeCanonicalRangeState"]["rebase_seen"]="100";
    EXPECT_THROW(cr::decode_state(wire.dump(),f.attempt,f.policy.codec),cr::protocol_error);
}

TEST(CanonicalCursorInitial, OwnedCopiesRetainZeroStateAfterCallerMutationAndMove) {
    PackageFixture f;f.receipt({"original",cr::unknown{}});const auto package=f.build();auto manifest=package.offer();
    const auto expected=cr::begin(f.attempt,f.request,manifest,f.policy.codec);auto policy=f.policy.codec;
    cr::validated_sequence source(f.attempt,f.request,manifest,policy);
    f.request.receipts.clear();f.attempt.attempt_id="corrupted after construction";manifest.manifest_digest="changed";policy.nodes=0;
    EXPECT_EQ(source.snapshot(),expected);cr::validated_sequence moved(std::move(source));EXPECT_EQ(moved.snapshot(),expected);
    for(size_t i=1;i<package.frames().size();++i)(void)moved.advance_canonical(package.frames()[i],1);
    EXPECT_EQ(moved.status(),cr::phase::sequence_complete_unverified);
}

TEST(CanonicalCursorInitial, ConstructionCountersRemoveOnlyDuplicateImmutableWork) {
    PackageFixture f;f.receipt({"original",cr::unknown{}});const auto package=f.build();
    sequence_counter_scope observed;cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);
    const auto constructed=observed.value;
    EXPECT_EQ(constructed.request_validations,2u);EXPECT_EQ(constructed.rebase_builds,3u);
    EXPECT_EQ(constructed.restart_objects,1u);EXPECT_EQ(constructed.cursors,1u);EXPECT_EQ(constructed.transitions,0u);
    for(size_t i=1;i<package.frames().size();++i)(void)cursor.advance_canonical(package.frames()[i],1);
    EXPECT_EQ(observed.value.request_validations,constructed.request_validations);
    EXPECT_EQ(observed.value.rebase_builds,constructed.rebase_builds);EXPECT_EQ(observed.value.restart_objects,constructed.restart_objects);
    EXPECT_EQ(observed.value.transitions,package.frames().size()-1);EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}

TEST(CanonicalCursorInitial, LargestLegalDecimalSpellingsPreserveInitialAndTerminalStates) {
    PackageFixture f;const auto maximum=static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    f.attempt.sequence=maximum;f.policy.codec.lease_ms=maximum;f.lease.duration_ms=maximum;
    f.seal_request();const auto package=f.build(maximum);
    auto strict=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);
    cr::validated_sequence cursor(f.attempt,f.request,package.offer(),f.policy.codec);EXPECT_EQ(cursor.snapshot(),strict);
    for(size_t i=1;i<package.frames().size();++i){
        const auto frame=cr::decode(package.frames()[i],f.policy.codec);strict=cr::propose(strict,frame,f.policy.codec);
        cursor.advance(frame);EXPECT_EQ(cursor.snapshot(),strict);
    }
    EXPECT_EQ(cursor.status(),cr::phase::sequence_complete_unverified);
}

TEST(CanonicalCursorInitial, InitialBitmapItselfStillConsumesTheDecodedScalarBudget) {
    PackageFixture f;f.policy.codec.maximum.items_per_page=128;f.request.budget=f.policy.codec.maximum;f.rows.clear();
    cr::receipt_request asked{"original","namespace",{}};
    for(unsigned i=100;i<165;++i){
        const cr::identity id{"PackageSourceRow","id-"+std::to_string(i)};
        asked.targets.push_back(id);f.rows.push_back({id,cr::tombstone{}});
    }
    f.request.receipts={asked};f.receipts={{"original",cr::unknown{}}};f.seal_request();const auto package=f.build();
    const auto strict=cr::begin(f.attempt,f.request,package.offer(),f.policy.codec);ASSERT_EQ(strict.rebase_seen.size(),65u);
    for(const auto& raw:{cr::encode({f.attempt,1,f.request},f.policy.codec),cr::encode({f.attempt,1,package.offer()},f.policy.codec)}){
        CanonicalWireSAXCounts actual;ASSERT_TRUE(nlohmann::json::sax_parse(raw,&actual));ASSERT_EQ(actual.max_string,64u);
    }
    CanonicalWireSAXCounts restart;ASSERT_TRUE(nlohmann::json::sax_parse(cr::encode_state(strict,f.policy.codec),&restart));ASSERT_EQ(restart.max_string,65u);
    for(int offset=-1;offset<=1;++offset){
        SCOPED_TRACE(offset);
        auto b=f.policy.codec;b.string_bytes=65+offset;
        cursor_initial_matches(f.attempt,f.request,package.offer(),b,offset>=0);
    }
}
