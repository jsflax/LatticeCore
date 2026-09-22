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
