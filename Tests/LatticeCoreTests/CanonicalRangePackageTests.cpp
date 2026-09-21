#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/canonical_range_package.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <type_traits>
#include <numeric>

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
