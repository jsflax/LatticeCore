#include "sync_recovery_values.hpp"
#include <nlohmann/json.hpp>
#include <cmath>
#include <limits>

namespace lattice::detail::sync_recovery {
namespace {
using json = nlohmann::json;
using blob = std::vector<uint8_t>;
void check(bool ok, const char* why) { if (!ok) throw protocol_error(why); }
void validate(const value_limits& b) {
    check(b.raw_bytes && b.raw_bytes <= 16 * 1024 * 1024 && b.fields && b.fields <= 4096 &&
          b.name_bytes && b.name_bytes <= 256 && b.value_bytes <= b.raw_bytes &&
          b.decoded_bytes <= b.raw_bytes, "invalid snapshot value limits");
}
bool name_ok(const std::string& s, const value_limits& b) {
    if (s.empty() || s.size() > b.name_bytes) return false;
    for (unsigned char c : s) if (c < 32 || c == 127) return false;
    return true;
}
size_t value_bytes(const column_value_t& v) {
    if (const auto* s = std::get_if<std::string>(&v)) return s->size();
    if (const auto* s = std::get_if<blob>(&v)) return s->size();
    return std::holds_alternative<std::nullptr_t>(v) ? 0 : 8;
}
bool charge(const std::string& name, const column_value_t& v, const value_limits& b, size_t& used) {
    const auto n = value_bytes(v);
    if (n > b.value_bytes || name.size() + 1 > b.decoded_bytes - used) return false;
    const auto overhead = name.size() + 1;
    if (n > b.decoded_bytes - used - overhead) return false;
    used += overhead + n; return true;
}
int hex(char c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1; }

// The grammar has exactly two object levels and scalar property values.
// Decode directly through SAX: no permissive DOM or duplicate-key collapse.
struct value_sax : nlohmann::json_sax<json> {
    const value_limits& budget;
    row_values result;
    int depth = 0;
    bool started = false, ended = false, field_expected = false;
    bool kind_seen = false, value_seen = false;
    size_t used = 0;
    std::string field, key_name;
    std::optional<int> kind;
    std::optional<column_value_t> scalar;
    explicit value_sax(const value_limits& b) : budget(b) {}
    bool value(column_value_t v) {
        if (depth != 2 || key_name != "value" || scalar) return false;
        scalar = std::move(v); return true;
    }
    bool null() override { return value(nullptr); }
    bool boolean(bool) override { return false; }
    bool number_integer(number_integer_t n) override {
        if (depth == 2 && key_name == "kind") {
            if (kind || !(n == 1 || n == 2 || n == 4 || n == 6 || n == 7)) return false;
            kind = static_cast<int>(n); return true;
        }
        return value(static_cast<int64_t>(n));
    }
    bool number_unsigned(number_unsigned_t n) override {
        if (n > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) return false;
        return number_integer(static_cast<int64_t>(n));
    }
    bool number_float(number_float_t n, const string_t&) override {
        return std::isfinite(n) && value(static_cast<double>(n));
    }
    bool string(string_t& s) override {
        // Hex BLOBs take two encoded bytes per decoded byte; final kind/type
        // validation applies the exact scalar budget before publishing it.
        if (s.size() > budget.raw_bytes || s.size() > 2 * budget.value_bytes) return false;
        return value(std::move(s));
    }
    bool binary(binary_t&) override { return false; }
    bool start_array(std::size_t) override { return false; }
    bool end_array() override { return false; }
    bool start_object(std::size_t) override {
        if (depth == 0 && !started) { started = true; depth = 1; return true; }
        if (depth == 1 && field_expected) {
            depth = 2; field_expected = false; kind_seen = value_seen = false;
            kind.reset(); scalar.reset(); key_name.clear(); return true;
        }
        return false;
    }
    bool key(string_t& s) override {
        if (depth == 1) {
            if (field_expected || result.size() >= budget.fields || !name_ok(s, budget) || result.contains(s)) return false;
            field = std::move(s); field_expected = true; return true;
        }
        if (depth != 2) return false;
        if (s == "kind" && !kind_seen) kind_seen = true;
        else if (s == "value" && !value_seen) value_seen = true;
        else return false;
        key_name = std::move(s); return true;
    }
    bool end_object() override {
        if (depth == 1 && !field_expected) { depth = 0; ended = true; return true; }
        if (depth != 2 || !kind_seen || !value_seen || !kind || !scalar) return false;
        if (*kind == 1 && !std::holds_alternative<int64_t>(*scalar)) return false;
        if (*kind == 2 && !std::holds_alternative<std::string>(*scalar)) return false;
        if (*kind == 4 && !std::holds_alternative<std::nullptr_t>(*scalar)) return false;
        if (*kind == 7 && !std::holds_alternative<double>(*scalar)) return false;
        if (*kind == 6) {
            const auto* text = std::get_if<std::string>(&*scalar);
            if (!text || text->size() % 2 || text->size() / 2 > budget.value_bytes) return false;
            // Refuse aggregate capacity before allocating the decoded BLOB.
            if (field.size() + 1 > budget.decoded_bytes - used ||
                text->size() / 2 > budget.decoded_bytes - used - field.size() - 1) return false;
            blob bytes; bytes.reserve(text->size() / 2);
            for (size_t i = 0; i < text->size(); i += 2) {
                const int a = hex((*text)[i]), b = hex((*text)[i + 1]);
                if (a < 0 || b < 0) return false;
                bytes.push_back(static_cast<uint8_t>((a << 4) | b));
            }
            scalar = std::move(bytes);
        }
        if (!charge(field, *scalar, budget, used)) return false;
        result.emplace(std::move(field), std::move(*scalar));
        depth = 1; return true;
    }
    bool parse_error(std::size_t, const std::string&, const nlohmann::detail::exception&) override { return false; }
};

void append(std::string& out, std::string_view part, const value_limits& b) {
    check(part.size() <= b.raw_bytes - out.size(), "snapshot payload exceeds raw budget");
    out.append(part);
}
// Bound JSON string escaping before the library allocates the encoded token.
std::string quote(const std::string& s, size_t remaining) {
    check(remaining >= 2, "snapshot string exceeds raw budget");
    size_t bytes = 2;
    for (unsigned char c : s) {
        const size_t add = c < 32 ? ((c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t') ? 2 : 6)
                                  : (c == '"' || c == '\\') ? 2 : 1;
        check(add <= remaining - bytes, "snapshot string exceeds raw budget"); bytes += add;
    }
    return json(s).dump(); // also rejects invalid UTF-8
}
} // namespace

row_values decode_values(std::string_view payload, const value_limits& budget) {
    validate(budget);
    check(!payload.empty() && payload.size() <= budget.raw_bytes, "snapshot payload exceeds raw budget");
    const std::string owned(payload);
    value_sax sax(budget);
    check(json::sax_parse(owned, &sax) && sax.ended && sax.depth == 0, "invalid or over-budget snapshot values");
    return std::move(sax.result);
}
std::string encode_values(const row_values& values, const value_limits& budget) {
    validate(budget);
    check(values.size() <= budget.fields, "snapshot field count exceeds budget");
    size_t used = 0;
    std::string out;
    try {
        append(out, "{", budget);
        bool first = true;
        for (const auto& [name, value] : values) {
            check(name_ok(name, budget) && charge(name, value, budget, used), "snapshot value exceeds decoded budget");
            if (!first) append(out, ",", budget);
            first = false;
            append(out, quote(name, budget.raw_bytes - out.size()), budget);
            int kind;
            std::string encoded;
            if (const auto* n = std::get_if<int64_t>(&value)) { kind = 1; encoded = json(*n).dump(); }
            else if (const auto* n = std::get_if<double>(&value)) {
                check(std::isfinite(*n), "nonfinite snapshot value"); kind = 7; encoded = json(*n).dump();
            } else if (const auto* s = std::get_if<std::string>(&value)) {
                kind = 2; encoded = quote(*s, budget.raw_bytes - out.size());
            } else if (const auto* data = std::get_if<blob>(&value)) {
                kind = 6;
                check(budget.raw_bytes - out.size() >= 2 && data->size() <= (budget.raw_bytes - out.size() - 2) / 2,
                      "snapshot BLOB exceeds raw budget");
                encoded = "\""; encoded.reserve(data->size() * 2 + 2);
                constexpr char digits[] = "0123456789abcdef";
                for (auto byte : *data) { encoded += digits[byte >> 4]; encoded += digits[byte & 15]; }
                encoded += '"';
            } else { kind = 4; encoded = "null"; }
            append(out, ":{\"kind\":" + std::to_string(kind) + ",\"value\":", budget);
            append(out, encoded, budget); append(out, "}", budget);
        }
        append(out, "}", budget);
        return out;
    } catch (const json::exception&) { throw protocol_error("invalid snapshot string encoding"); }
}
} // namespace lattice::detail::sync_recovery
