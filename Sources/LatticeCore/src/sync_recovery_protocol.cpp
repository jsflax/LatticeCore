#include "sync_recovery_protocol.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <limits>
#include <set>
#include <type_traits>
#include <utility>

namespace lattice::detail::sync_recovery {
namespace {
using json = nlohmann::json;
constexpr uint64_t position_max = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());

void require(bool condition, const char* reason) {
    if (!condition) throw protocol_error(reason);
}

void validate_limits(const limits& b) {
    require(b.frame_bytes > 0 && b.frame_bytes <= 16 * 1024 * 1024, "invalid frame budget");
    require(b.depth > 0 && b.depth <= 64 && b.nodes > 0 && b.nodes <= 65536, "invalid parser budget");
    require(b.string_bytes > 0 && b.string_bytes <= b.frame_bytes, "invalid string budget");
    require(b.rows_per_page > 0 && b.rows_per_page <= 4096, "invalid page budget");
    require(b.pages > 0 && b.pages <= position_max && b.total_rows > 0 && b.total_rows <= position_max &&
            b.total_bytes > 0 && b.total_bytes <= position_max, "invalid snapshot budget");
}

// First pass bounds decoded strings/nodes/depth and rejects duplicate object
// keys BEFORE constructing a DOM. The lexer may allocate one token up to the
// already checked raw-frame cap. Both passes read the same owned bytes.
struct bounded_sax : nlohmann::json_sax<json> {
    const limits& budget;
    size_t nodes = 0;
    struct level { bool object; std::set<std::string> keys; };
    std::vector<level> stack;
    explicit bounded_sax(const limits& b) : budget(b) {}
    bool node() { return ++nodes <= budget.nodes; }
    bool null() override { return node(); }
    bool boolean(bool) override { return node(); }
    bool number_integer(number_integer_t) override { return node(); }
    bool number_unsigned(number_unsigned_t) override { return node(); }
    bool number_float(number_float_t, const string_t&) override { return false; }
    bool string(string_t& s) override { return s.size() <= budget.string_bytes && node(); }
    bool binary(binary_t&) override { return false; }
    bool start(bool object) {
        if (!node() || stack.size() >= budget.depth) return false;
        stack.push_back({object, {}}); return true;
    }
    bool start_object(std::size_t) override { return start(true); }
    bool start_array(std::size_t) override { return start(false); }
    bool key(string_t& s) override {
        return !stack.empty() && stack.back().object && s.size() <= budget.string_bytes &&
               node() && stack.back().keys.insert(s).second;
    }
    bool end_object() override { stack.pop_back(); return true; }
    bool end_array() override { stack.pop_back(); return true; }
    bool parse_error(std::size_t, const std::string&, const nlohmann::detail::exception&) override {
        return false;
    }
};

json parse(std::string_view frame, const limits& budget) {
    validate_limits(budget);
    require(!frame.empty() && frame.size() <= budget.frame_bytes, "raw frame exceeds budget");
    const std::string owned(frame);
    bounded_sax sax(budget);
    require(json::sax_parse(owned, &sax), "invalid or over-budget JSON");
    return json::parse(owned); // SAX checked the identical, immutable byte sequence.
}

void keys(const json& value, std::initializer_list<const char*> allowed) {
    require(value.is_object() && value.size() == allowed.size(), "missing or unknown protocol field");
    for (const auto* key : allowed) require(value.contains(key), "missing protocol field");
}

std::string text(const json& j) {
    require(j.is_string(), "protocol string required");
    return j.get<std::string>();
}

bool lower_hex(char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }
void digest(const std::string& value) {
    require(value.size() == 64 && std::all_of(value.begin(), value.end(), lower_hex), "invalid digest spelling");
}
void uuid(const std::string& value) {
    require(value.size() == 36, "invalid identity");
    for (size_t i = 0; i < value.size(); ++i) {
        const bool dash = i == 8 || i == 13 || i == 18 || i == 23;
        require(dash ? value[i] == '-' : lower_hex(value[i]), "invalid identity");
    }
}
void identifier(const std::string& value, const limits& b) {
    require(!value.empty() && value.size() <= std::min<size_t>(256, b.string_bytes), "invalid identifier length");
    require(std::none_of(value.begin(), value.end(), [](unsigned char c) { return c < 32 || c == 127; }),
            "control byte in identifier");
}
void validate_binding(const binding& v, const limits& b) {
    uuid(v.source_id); uuid(v.epoch); uuid(v.attempt_id); uuid(v.recovery_id);
    digest(v.scope_digest); digest(v.schema_digest); identifier(v.channel, b);
    require(b.string_bytes >= 64, "string budget excludes identity fields");
}
json binding_json(const binding& v) {
    return {{"source", v.source_id}, {"epoch", v.epoch}, {"channel", v.channel},
            {"scope", v.scope_digest}, {"schema", v.schema_digest},
            {"attempt", v.attempt_id}, {"recovery", v.recovery_id}};
}
binding read_binding(const json& j, const limits& b) {
    keys(j, {"source", "epoch", "channel", "scope", "schema", "attempt", "recovery"});
    binding result{text(j.at("source")), text(j.at("epoch")), text(j.at("channel")),
                   text(j.at("scope")), text(j.at("schema")), text(j.at("attempt")), text(j.at("recovery"))};
    validate_binding(result, b); return result;
}
uint64_t position(const json& j) { return parse_position(text(j)); }
void version(const json& j) {
    require(j.is_number_integer() && j == 1, "unsupported protocol version");
}
void counts(uint64_t pages, uint64_t rows, uint64_t bytes, const limits& b) {
    require(pages <= b.pages && rows <= b.total_rows && bytes <= b.total_bytes, "snapshot exceeds budget");
    require((pages == 0 && rows == 0 && bytes == 0) || (pages > 0 && rows >= pages && bytes > 0),
            "inconsistent snapshot counts");
    if (rows) {
        require((rows - 1) / b.rows_per_page < pages, "page count cannot hold rows");
        const uint64_t largest_row = 24 + 2 * std::min<size_t>(256, b.string_bytes) + b.string_bytes;
        require(rows <= bytes / 27 && (bytes - 1) / largest_row < rows &&
                (bytes - 1) / b.frame_bytes < pages, "impossible content byte count");
    }
}
void validate_manifest(const manifest& m, const limits& b) {
    validate_binding(m.identity, b); digest(m.content_digest);
    require(m.frontier <= position_max && (!m.base_position || *m.base_position <= m.frontier), "invalid frontier");
    counts(m.page_count, m.row_count, m.content_bytes, b);
}
json manifest_json(const manifest& m) {
    return {{"version", 1}, {"kind", "manifest"}, {"binding", binding_json(m.identity)},
            {"base", m.base_position ? json(std::to_string(*m.base_position)) : json(nullptr)},
            {"frontier", std::to_string(m.frontier)}, {"pages", std::to_string(m.page_count)},
            {"rows", std::to_string(m.row_count)}, {"bytes", std::to_string(m.content_bytes)},
            {"digest", m.content_digest}};
}
manifest read_manifest(const json& j, const limits& b) {
    keys(j, {"version", "kind", "binding", "base", "frontier", "pages", "rows", "bytes", "digest"});
    version(j.at("version")); require(j.at("kind") == "manifest", "manifest kind required");
    manifest m;
    m.identity = read_binding(j.at("binding"), b);
    if (!j.at("base").is_null()) m.base_position = position(j.at("base"));
    m.frontier = position(j.at("frontier")); m.page_count = position(j.at("pages"));
    m.row_count = position(j.at("rows")); m.content_bytes = position(j.at("bytes"));
    m.content_digest = text(j.at("digest")); validate_manifest(m, b); return m;
}
auto identity(const row& r) { return std::pair{r.table, r.global_id}; }
void validate_page(const page& p, const limits& b) {
    validate_binding(p.identity, b); digest(p.content_digest);
    require(p.index < b.pages && !p.rows.empty() && p.rows.size() <= b.rows_per_page, "invalid page size/index");
    uint64_t bytes = 0;
    std::optional<std::pair<std::string, std::string>> previous;
    for (const auto& r : p.rows) {
        identifier(r.table, b); identifier(r.global_id, b);
        require(!r.payload.empty() && r.payload.size() <= b.string_bytes, "invalid payload size");
        require(!previous || *previous < identity(r), "duplicate or unordered row identity");
        previous = identity(r);
        const auto add = canonical_row_bytes(r);
        require(add <= b.frame_bytes && bytes <= b.frame_bytes - add, "page content exceeds frame budget");
        bytes += add;
    }
    require(bytes == p.content_bytes && bytes <= b.total_bytes, "page byte count mismatch");
}
void validate_end(const end& e, const limits& b) {
    validate_binding(e.identity, b); digest(e.content_digest);
    require(e.frontier <= position_max, "invalid frontier"); counts(e.page_count, e.row_count, e.content_bytes, b);
}
void validate_state(const staging_state& s, const limits& b) {
    validate_manifest(s.offer, b);
    require(s.status == phase::receiving || s.status == phase::sequence_complete_unverified, "invalid staging phase");
    require(s.next_page <= s.offer.page_count && s.rows <= s.offer.row_count &&
            s.content_bytes <= s.offer.content_bytes, "staging totals exceed manifest");
    require((s.next_page == 0 && s.rows == 0 && s.content_bytes == 0 && !s.last_identity) ||
            (s.next_page > 0 && s.rows >= s.next_page && s.content_bytes > 0 && s.last_identity),
            "inconsistent staging state");
    if (s.rows) require((s.rows - 1) / b.rows_per_page < s.next_page, "staging row count exceeds pages");
    const auto remaining_pages = s.offer.page_count - s.next_page;
    const auto remaining_rows = s.offer.row_count - s.rows;
    require((remaining_pages == 0 && remaining_rows == 0) ||
            (remaining_pages > 0 && remaining_rows >= remaining_pages &&
             (remaining_rows - 1) / b.rows_per_page < remaining_pages), "impossible remaining page counts");
    require((remaining_pages == 0 && s.content_bytes == s.offer.content_bytes) ||
            (remaining_pages > 0 && s.content_bytes < s.offer.content_bytes), "impossible remaining byte count");
    if (s.last_identity) { identifier(s.last_identity->first, b); identifier(s.last_identity->second, b); }
    if (s.status == phase::sequence_complete_unverified)
        require(s.next_page == s.offer.page_count && s.rows == s.offer.row_count &&
                s.content_bytes == s.offer.content_bytes, "incomplete terminal staging state");
}
std::string dump(const json& value, const limits& b) {
    std::string bytes;
    try { bytes = value.dump(); }
    catch (const json::exception&) { throw protocol_error("invalid protocol encoding"); }
    // DTO validation bounds allocations before encoding; this final raw limit
    // also accounts for JSON escaping, which can expand decoded strings.
    (void)parse(bytes, b); return bytes;
}
} // namespace

uint64_t parse_position(std::string_view value) {
    require(!value.empty() && value.size() <= 19 && (value == "0" || value.front() != '0'), "noncanonical position");
    uint64_t result = 0;
    for (char c : value) {
        require(c >= '0' && c <= '9', "nondecimal position");
        const auto digit = static_cast<uint64_t>(c - '0');
        require(result <= (position_max - digit) / 10, "position overflow"); result = result * 10 + digit;
    }
    return result;
}
uint64_t canonical_row_bytes(const row& r) {
    uint64_t result = 24;
    for (const auto* s : {&r.table, &r.global_id, &r.payload}) {
        require(s->size() <= position_max - result, "row byte count overflow"); result += s->size();
    }
    return result;
}

message decode(std::string_view frame, const limits& b) {
    const auto root = parse(frame, b); keys(root, {"latticeRecovery"});
    const auto& j = root.at("latticeRecovery");
    require(j.is_object() && j.contains("kind") && j.at("kind").is_string(), "recovery kind required");
    const auto kind = text(j.at("kind"));
    if (kind == "manifest") return read_manifest(j, b);
    if (kind == "page") {
        keys(j, {"version", "kind", "binding", "index", "bytes", "digest", "items"}); version(j.at("version"));
        page p; p.identity = read_binding(j.at("binding"), b); p.index = position(j.at("index"));
        p.content_bytes = position(j.at("bytes")); p.content_digest = text(j.at("digest"));
        const auto& rows = j.at("items");
        require(rows.is_array() && !rows.empty() && rows.size() <= b.rows_per_page, "invalid item count");
        for (const auto& r : rows) {
            keys(r, {"table", "id", "payload"}); p.rows.push_back({text(r.at("table")), text(r.at("id")), text(r.at("payload"))});
        }
        validate_page(p, b); return p;
    }
    if (kind == "end") {
        keys(j, {"version", "kind", "binding", "frontier", "pages", "rows", "bytes", "digest"}); version(j.at("version"));
        end e{read_binding(j.at("binding"), b), position(j.at("frontier")), position(j.at("pages")),
              position(j.at("rows")), position(j.at("bytes")), text(j.at("digest"))};
        validate_end(e, b); return e;
    }
    throw protocol_error("unknown recovery kind");
}

std::string encode(const message& value, const limits& b) {
    validate_limits(b);
    json body = std::visit([&](const auto& v) -> json {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, manifest>) { validate_manifest(v, b); return manifest_json(v); }
        else if constexpr (std::is_same_v<T, page>) {
            validate_page(v, b); json rows = json::array();
            for (const auto& r : v.rows) rows.push_back({{"table", r.table}, {"id", r.global_id}, {"payload", r.payload}});
            return {{"version", 1}, {"kind", "page"}, {"binding", binding_json(v.identity)},
                    {"index", std::to_string(v.index)}, {"bytes", std::to_string(v.content_bytes)},
                    {"digest", v.content_digest}, {"items", std::move(rows)}};
        } else {
            validate_end(v, b);
            return {{"version", 1}, {"kind", "end"}, {"binding", binding_json(v.identity)},
                    {"frontier", std::to_string(v.frontier)}, {"pages", std::to_string(v.page_count)},
                    {"rows", std::to_string(v.row_count)}, {"bytes", std::to_string(v.content_bytes)}, {"digest", v.content_digest}};
        }
    }, value);
    return dump({{"latticeRecovery", std::move(body)}}, b);
}

staging_state begin(const manifest& offer, const binding& expected, const limits& b) {
    validate_limits(b); validate_manifest(offer, b); validate_binding(expected, b);
    require(offer.identity == expected, "manifest binding mismatch"); return {offer};
}
staging_state propose(const staging_state& current, const page& p, const limits& b) {
    validate_limits(b); validate_state(current, b); validate_page(p, b);
    require(current.status == phase::receiving, "staging sequence already ended");
    require(p.identity == current.offer.identity, "page binding mismatch");
    require(p.index == current.next_page && current.next_page < current.offer.page_count, "duplicate or out-of-order page");
    require(!current.last_identity || *current.last_identity < identity(p.rows.front()), "row identity repeated across pages");
    require(p.rows.size() <= current.offer.row_count - current.rows &&
            p.content_bytes <= current.offer.content_bytes - current.content_bytes, "page exceeds manifest totals");
    auto next = current; ++next.next_page; next.rows += p.rows.size(); next.content_bytes += p.content_bytes;
    next.last_identity = identity(p.rows.back()); validate_state(next, b); return next;
}
staging_state propose(const staging_state& current, const end& e, const limits& b) {
    validate_limits(b); validate_state(current, b); validate_end(e, b);
    require(current.status == phase::receiving, "staging sequence already ended");
    const auto& m = current.offer;
    require(e.identity == m.identity && e.frontier == m.frontier && e.page_count == m.page_count &&
            e.row_count == m.row_count && e.content_bytes == m.content_bytes && e.content_digest == m.content_digest,
            "terminal manifest mismatch");
    require(current.next_page == m.page_count && current.rows == m.row_count && current.content_bytes == m.content_bytes,
            "terminal record precedes all pages");
    auto next = current; next.status = phase::sequence_complete_unverified; return next;
}

std::string encode_staging(const staging_state& s, const limits& b) {
    validate_limits(b); validate_state(s, b);
    json last = nullptr;
    if (s.last_identity) last = {{"table", s.last_identity->first}, {"id", s.last_identity->second}};
    return dump({{"latticeRecoveryStaging", {{"version", 1}, {"manifest", manifest_json(s.offer)},
        {"phase", s.status == phase::receiving ? "receiving" : "sequence_complete_unverified"},
        {"next", std::to_string(s.next_page)}, {"rows", std::to_string(s.rows)},
        {"bytes", std::to_string(s.content_bytes)}, {"last", std::move(last)}}}}, b);
}
staging_state decode_staging(std::string_view bytes, const binding& expected, const limits& b) {
    const auto root = parse(bytes, b); keys(root, {"latticeRecoveryStaging"});
    const auto& j = root.at("latticeRecoveryStaging");
    keys(j, {"version", "manifest", "phase", "next", "rows", "bytes", "last"}); version(j.at("version"));
    staging_state s = begin(read_manifest(j.at("manifest"), b), expected, b);
    const auto state = text(j.at("phase"));
    require(state == "receiving" || state == "sequence_complete_unverified", "invalid staging phase");
    s.status = state == "receiving" ? phase::receiving : phase::sequence_complete_unverified;
    s.next_page = position(j.at("next")); s.rows = position(j.at("rows")); s.content_bytes = position(j.at("bytes"));
    if (!j.at("last").is_null()) {
        const auto& last = j.at("last"); keys(last, {"table", "id"});
        s.last_identity = std::make_pair(text(last.at("table")), text(last.at("id")));
    }
    validate_state(s, b); return s;
}
} // namespace lattice::detail::sync_recovery
