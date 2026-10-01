#include <gtest/gtest.h>
#include <lattice/sync.hpp>
#include <nlohmann/json.hpp>
#include "../../Sources/LatticeCore/src/sync_recovery_protocol.hpp"
#include <array>
#include <limits>

namespace sr = lattice::detail::sync_recovery;
namespace {
using json = nlohmann::json;

sr::limits budget() { return {8192, 12, 1024, 1024, 2, 8, 16, 65536}; }
sr::binding binding() {
    return {"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002",
            "channel-A", std::string(64, 'a'), std::string(64, 'b'),
            "00000000-0000-4000-8000-000000000003", "00000000-0000-4000-8000-000000000004"};
}
sr::page page(uint64_t index, std::string id) {
    sr::page result{binding(), index, 0, std::string(64, 'c'), {{"Person", std::move(id), R"({"name":"sample"})"}}};
    result.content_bytes = sr::canonical_row_bytes(result.rows.front()); return result;
}
sr::manifest offer() {
    const auto first = page(0, "row-A"), second = page(1, "row-B");
    return {binding(), std::nullopt, 42, 2, 2, first.content_bytes + second.content_bytes, std::string(64, 'd')};
}
sr::end terminal(const sr::manifest& m) {
    return {m.identity, m.frontier, m.page_count, m.row_count, m.content_bytes, m.content_digest};
}
json wire(const sr::message& message) { return json::parse(sr::encode(message, budget())); }
void rejected(const json& value) { EXPECT_THROW((void)sr::decode(value.dump(), budget()), sr::protocol_error); }
void rejected_by_parser(const std::string& bytes, const sr::limits& b) {
    try { (void)sr::decode(bytes, b); FAIL() << "parser limit accepted the frame"; }
    catch (const sr::protocol_error& e) { EXPECT_STREQ(e.what(), "invalid or over-budget JSON"); }
}
}

TEST(SyncRecoveryProtocol, RoundTripsOwnedMessagesWithoutLegacyDispatchKeys) {
    const auto m = offer();
    const std::array<sr::message, 3> cases{m, page(0, "row-A"), terminal(m)};
    for (const auto& value : cases) {
        const auto bytes = sr::encode(value, budget());
        EXPECT_EQ(sr::decode(bytes, budget()), value);
        const auto object = json::parse(bytes);
        EXPECT_EQ(object.size(), 1u);
        EXPECT_FALSE(object.contains("auditLog")); EXPECT_FALSE(object.contains("ack"));
        EXPECT_FALSE(object.contains("replayRequest"));
        EXPECT_FALSE(lattice::server_sent_event::from_json(bytes).has_value());
    }
    std::string input = sr::encode(page(0, "row-A"), budget());
    const auto parsed = std::get<sr::page>(sr::decode(input, budget()));
    input.assign(input.size(), 'x');
    EXPECT_EQ(parsed.rows.front().payload, R"({"name":"sample"})");
}

TEST(SyncRecoveryProtocol, RejectsLegacyMixedUnknownMissingAndWrongTypeFields) {
    for (const auto* legacy : {"ack", "auditLog", "replayRequest"}) {
        auto j = wire(offer()); j[legacy] = json::array(); rejected(j);
        rejected(json{{legacy, json::array()}});
    }
    auto missing = wire(offer()); missing["latticeRecovery"].erase("base"); rejected(missing);
    auto unknown = wire(offer()); unknown["latticeRecovery"]["trusted"] = true; rejected(unknown);
    auto future = wire(offer()); future["latticeRecovery"]["version"] = 2; rejected(future);
    auto floating = wire(offer()); floating["latticeRecovery"]["version"] = 1.0; rejected(floating);
    auto kind = wire(offer()); kind["latticeRecovery"]["kind"] = "installed"; rejected(kind);
    auto numeric = wire(offer()); numeric["latticeRecovery"]["frontier"] = 42; rejected(numeric);
    auto malformed = wire(page(0, "row-A")); malformed["latticeRecovery"]["items"][0]["payload"] = json::object(); rejected(malformed);
}

TEST(SyncRecoveryProtocol, RequiresEveryBindingComponentAndCanonicalDigests) {
    for (const auto* field : {"source", "epoch", "channel", "scope", "schema", "attempt", "recovery"}) {
        auto j = wire(offer()); j["latticeRecovery"]["binding"].erase(field); rejected(j);
        j = wire(offer()); j["latticeRecovery"]["binding"][field] = ""; rejected(j);
    }
    auto uppercase = wire(offer()); uppercase["latticeRecovery"]["binding"]["scope"] = std::string(64, 'A'); rejected(uppercase);
    auto embedded = wire(offer()); embedded["latticeRecovery"]["binding"]["channel"] = std::string("a\0b", 3); rejected(embedded);
    auto extended = wire(offer()); extended["latticeRecovery"]["binding"]["channel"] = std::string(257, 'x'); rejected(extended);
}

TEST(SyncRecoveryProtocol, CanonicalPositionsHaveNoFloatSignWhitespaceOrOverflow) {
    EXPECT_EQ(sr::parse_position("0"), 0u);
    EXPECT_EQ(sr::parse_position("9223372036854775807"), static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    for (const auto* text : {"", "00", "01", "+1", "-1", " 1", "1 ", "1.0", "1e0", "0x10",
                             "9223372036854775808", "18446744073709551615"})
        EXPECT_THROW((void)sr::parse_position(text), sr::protocol_error) << text;
    auto m = offer(); m.base_position = 0;
    EXPECT_NE(std::get<sr::manifest>(sr::decode(sr::encode(m, budget()), budget())).base_position,
              std::get<sr::manifest>(sr::decode(sr::encode(offer(), budget()), budget())).base_position);
    m.base_position = 43; EXPECT_THROW((void)sr::encode(m, budget()), sr::protocol_error);
}

TEST(SyncRecoveryProtocol, RawBytesAreRejectedBeforeMalformedInputParsing) {
    auto b = budget(); b.frame_bytes = 1024;
    try { (void)sr::decode(std::string(1025, '['), b); FAIL() << "oversize accepted"; }
    catch (const sr::protocol_error& e) { EXPECT_STREQ(e.what(), "raw frame exceeds budget"); }
    b.frame_bytes = 0; EXPECT_THROW((void)sr::decode("{}", b), sr::protocol_error);
    b = budget(); b.frame_bytes = 16 * 1024 * 1024 + 1;
    EXPECT_THROW((void)sr::decode("{}", b), sr::protocol_error);
}

TEST(SyncRecoveryProtocol, DecodedDepthNodesAndEscapedStringBytesAreBounded) {
    const auto valid_page = page(0, "row-A");
    const auto valid_bytes = sr::encode(valid_page, budget());
    EXPECT_EQ(std::get<sr::page>(sr::decode(valid_bytes, budget())), valid_page);
    // Root/body/items/row has depth four. All other schema and budget checks
    // still pass when only this parser depth allowance is reduced.
    auto b = budget(); b.depth = 3;
    rejected_by_parser(valid_bytes, b);
    b = budget(); b.nodes = 20;
    rejected_by_parser(valid_bytes, b);

    auto escaped_page = page(0, "row-A");
    escaped_page.rows.front().payload.assign(65, 'a');
    escaped_page.content_bytes = sr::canonical_row_bytes(escaped_page.rows.front());
    auto escaped = sr::encode(escaped_page, budget());
    const auto token = json(escaped_page.rows.front().payload).dump();
    const auto at = escaped.find(token); ASSERT_NE(at, std::string::npos);
    std::string replacement = "\"";
    for (int i = 0; i < 65; ++i) replacement += "\\u0061";
    replacement += "\"";
    escaped.replace(at, token.size(), replacement);
    EXPECT_EQ(std::get<sr::page>(sr::decode(escaped, budget())), escaped_page);
    b = budget(); b.string_bytes = 64;
    // The DTO also limits payload size: assert the parser-stage error so a
    // missing SAX string guard cannot be hidden by that later rejection.
    rejected_by_parser(escaped, b);
    auto too_many = page(0, "row-A");
    too_many.rows.push_back({"Person", "row-B", "{}"}); too_many.rows.push_back({"Person", "row-C", "{}"});
    EXPECT_THROW((void)sr::encode(too_many, budget()), sr::protocol_error);
}

TEST(SyncRecoveryProtocol, DuplicateJSONKeysNeverCollapseIntoAValidManifest) {
    const auto bytes = sr::encode(offer(), budget());
    auto duplicate = bytes;
    const auto start = duplicate.find("\"frontier\":"); ASSERT_NE(start, std::string::npos);
    duplicate.insert(start, "\"frontier\":\"1\",");
    EXPECT_THROW((void)sr::decode(duplicate, budget()), sr::protocol_error);
    auto nested = bytes; const auto key = nested.find("\"source\":"); ASSERT_NE(key, std::string::npos);
    nested.insert(key, "\"source\":\"00000000-0000-4000-8000-000000000999\",");
    EXPECT_THROW((void)sr::decode(nested, budget()), sr::protocol_error);
}

TEST(SyncRecoveryProtocol, EncoderRejectsInvalidUTF8AndEscapingCannotEvadeRawLimit) {
    auto p = page(0, "row-A");
    p.rows.front().payload = std::string(1, static_cast<char>(0xff));
    p.content_bytes = sr::canonical_row_bytes(p.rows.front());
    EXPECT_THROW((void)sr::encode(p, budget()), sr::protocol_error);
    p = page(0, "row-A"); p.rows.front().payload.assign(700, '\0');
    p.content_bytes = sr::canonical_row_bytes(p.rows.front());
    auto b = budget(); b.frame_bytes = 2048;
    // Decoded payload fits, but each NUL requires six JSON bytes.
    EXPECT_THROW((void)sr::encode(p, b), sr::protocol_error);
}

TEST(SyncRecoveryProtocol, ManifestAndPageBudgetsIncludeIdentityAndLengthBytes) {
    auto m = offer(); auto b = budget(); b.pages = 1;
    EXPECT_THROW((void)sr::begin(m, binding(), b), sr::protocol_error);
    b = budget(); b.total_rows = 1; EXPECT_THROW((void)sr::begin(m, binding(), b), sr::protocol_error);
    b = budget(); b.total_bytes = m.content_bytes - 1; EXPECT_THROW((void)sr::begin(m, binding(), b), sr::protocol_error);
    const sr::row r{"T", "I", "{}"}; EXPECT_EQ(sr::canonical_row_bytes(r), 28u);
    auto p = page(0, "row-A"); p.content_bytes = p.rows.front().payload.size();
    EXPECT_THROW((void)sr::encode(p, budget()), sr::protocol_error);
    m.page_count = 3; EXPECT_THROW((void)sr::begin(m, binding(), budget()), sr::protocol_error);
    m = offer(); m.row_count = 5; EXPECT_THROW((void)sr::begin(m, binding(), budget()), sr::protocol_error);
    m = offer(); m.content_bytes = 1; EXPECT_THROW((void)sr::begin(m, binding(), budget()), sr::protocol_error);
}

TEST(SyncRecoveryProtocol, EveryIdentityMismatchRefusesWithoutChangingTheInputState) {
    const auto initial = sr::begin(offer(), binding(), budget());
    using member = std::string sr::binding::*;
    const std::array<member, 7> fields{&sr::binding::source_id, &sr::binding::epoch, &sr::binding::channel,
        &sr::binding::scope_digest, &sr::binding::schema_digest, &sr::binding::attempt_id, &sr::binding::recovery_id};
    for (const auto field : fields) {
        auto p = page(0, "row-A"); auto& text = p.identity.*field;
        text.back() = text.back() == 'a' ? 'b' : 'a';
        EXPECT_THROW((void)sr::propose(initial, p, budget()), sr::protocol_error);
        auto m = offer(); m.identity = p.identity;
        EXPECT_THROW((void)sr::begin(m, binding(), budget()), sr::protocol_error);
        EXPECT_EQ(initial.next_page, 0u); EXPECT_EQ(initial.rows, 0u);
    }
}

TEST(SyncRecoveryProtocol, OutOfOrderAndDuplicatePagesDoNotAdvanceTheJournalProposal) {
    const auto initial = sr::begin(offer(), binding(), budget());
    EXPECT_THROW((void)sr::propose(initial, page(1, "row-B"), budget()), sr::protocol_error);
    const auto next = sr::propose(initial, page(0, "row-A"), budget());
    EXPECT_EQ(initial.next_page, 0u); EXPECT_EQ(next.next_page, 1u);
    EXPECT_THROW((void)sr::propose(next, page(0, "row-A"), budget()), sr::protocol_error);
    EXPECT_EQ(next.next_page, 1u);
    EXPECT_THROW((void)sr::propose(next, terminal(offer()), budget()), sr::protocol_error);
}

TEST(SyncRecoveryProtocol, DuplicateAndUnorderedRowsAreRejectedWithinAndAcrossPages) {
    auto duplicate = page(0, "row-A"); duplicate.rows.push_back(duplicate.rows.front()); duplicate.content_bytes *= 2;
    EXPECT_THROW((void)sr::encode(duplicate, budget()), sr::protocol_error);
    duplicate.rows.back().global_id = "row-0";
    EXPECT_THROW((void)sr::encode(duplicate, budget()), sr::protocol_error);
    const auto first = sr::propose(sr::begin(offer(), binding(), budget()), page(0, "row-A"), budget());
    EXPECT_THROW((void)sr::propose(first, page(1, "row-A"), budget()), sr::protocol_error);
    EXPECT_THROW((void)sr::propose(first, page(1, "row-0"), budget()), sr::protocol_error);
}

TEST(SyncRecoveryProtocol, TerminalChecksEveryManifestFieldAndNeverProducesInstalledProof) {
    const auto m = offer();
    const auto ready = sr::propose(sr::propose(sr::begin(m, binding(), budget()), page(0, "row-A"), budget()), page(1, "row-B"), budget());
    auto wrong = terminal(m); ++wrong.frontier; EXPECT_THROW((void)sr::propose(ready, wrong, budget()), sr::protocol_error);
    wrong = terminal(m); wrong.content_digest[0] = 'e'; EXPECT_THROW((void)sr::propose(ready, wrong, budget()), sr::protocol_error);
    wrong = terminal(m); --wrong.row_count; EXPECT_THROW((void)sr::propose(ready, wrong, budget()), sr::protocol_error);
    wrong = terminal(m); --wrong.page_count; EXPECT_THROW((void)sr::propose(ready, wrong, budget()), sr::protocol_error);
    wrong = terminal(m); --wrong.content_bytes; EXPECT_THROW((void)sr::propose(ready, wrong, budget()), sr::protocol_error);
    const auto complete = sr::propose(ready, terminal(m), budget());
    EXPECT_EQ(ready.status, sr::phase::receiving);
    EXPECT_EQ(complete.status, sr::phase::sequence_complete_unverified);
    EXPECT_THROW((void)sr::propose(complete, terminal(m), budget()), sr::protocol_error);
    EXPECT_THROW((void)sr::propose(complete, page(2, "row-C"), budget()), sr::protocol_error);
    // Digests are deliberately arbitrary, proving this state is NOT a digest,
    // payload, database commit, acceptance or installation verifier.
    EXPECT_EQ(complete.offer.content_digest, std::string(64, 'd'));
}

TEST(SyncRecoveryProtocol, EmptySnapshotRequiresExplicitTerminalAndKeepsNullBase) {
    auto m = offer(); m.page_count = 0; m.row_count = 0; m.content_bytes = 0;
    const auto initial = sr::begin(m, binding(), budget());
    EXPECT_EQ(initial.status, sr::phase::receiving); EXPECT_FALSE(initial.offer.base_position);
    EXPECT_THROW((void)sr::propose(initial, page(0, "row-A"), budget()), sr::protocol_error);
    const auto ended = sr::propose(initial, terminal(m), budget());
    EXPECT_EQ(ended.status, sr::phase::sequence_complete_unverified);
}

TEST(SyncRecoveryProtocol, RestartRestoresOnlyStructuralProgressAndRejectsForgedPhaseOrIdentity) {
    const auto first = sr::propose(sr::begin(offer(), binding(), budget()), page(0, "row-A"), budget());
    const auto bytes = sr::encode_staging(first, budget());
    EXPECT_EQ(sr::decode_staging(bytes, binding(), budget()), first);
    const auto next = sr::propose(sr::decode_staging(bytes, binding(), budget()), page(1, "row-B"), budget());
    EXPECT_EQ(next.next_page, 2u); EXPECT_EQ(next.status, sr::phase::receiving);
    EXPECT_THROW((void)sr::decode(bytes, budget()), sr::protocol_error);
    auto stale = binding(); stale.attempt_id.back() = '9';
    EXPECT_THROW((void)sr::decode_staging(bytes, stale, budget()), sr::protocol_error);
    auto bad = json::parse(bytes); bad["latticeRecoveryStaging"]["phase"] = "installed";
    EXPECT_THROW((void)sr::decode_staging(bad.dump(), binding(), budget()), sr::protocol_error);
    bad = json::parse(bytes); bad["latticeRecoveryStaging"]["next"] = "0";
    EXPECT_THROW((void)sr::decode_staging(bad.dump(), binding(), budget()), sr::protocol_error);
    bad = json::parse(bytes); bad["latticeRecoveryStaging"]["last"] = nullptr;
    EXPECT_THROW((void)sr::decode_staging(bad.dump(), binding(), budget()), sr::protocol_error);
    bad = json::parse(bytes); bad["latticeRecoveryStaging"]["phase"] = "sequence_complete_unverified";
    EXPECT_THROW((void)sr::decode_staging(bad.dump(), binding(), budget()), sr::protocol_error);
}
