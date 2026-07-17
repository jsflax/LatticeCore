// CAPISurfaceTests — C-ABI surface freeze enforcement.
//
// The C ABI (Sources/LatticeCAPI/include/lattice.h) shipped two functions
// that were declared but never implemented, undetected for 3.5 months
// (docs/capi-gap-audit.md B-9 "phantom symbols") because nothing diffed the
// declared surface against the implemented one. These tests pin the surface
// three ways:
//
//   1. every function declared in lattice.h has a definition in
//      Sources/LatticeCAPI/src/lattice.cpp (no phantoms),
//   2. every extern "C" lattice_* definition is declared in the header
//      (no undocumented exports),
//   3. the checked-in Sources/LatticeCAPI/lattice_capi.symbols file matches
//      the declared surface exactly (any surface change must be explicit —
//      see docs/CAPI-STABILITY.md for the regeneration recipe).
//
// Plus the C11 pin: capi_header_compiles.c compiles lattice.h as pure C11 in
// this target; the probe call below fails if that TU is ever dropped.
//
// The parsers below work on the source checkout, resolved relative to this
// file (__FILE__), so the tests run identically under SwiftPM and CMake.

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "capi_header_check.h"  // LatticeCAPIHeaderCheck (the C11 pin target)

namespace {

std::filesystem::path repo_root() {
    // .../Tests/LatticeCoreTests/CAPISurfaceTests.cpp -> repo root
    return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path();
}

std::string read_file(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << "cannot open " << path;
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

// Remove //, /* */ comments and string/char literal contents so the
// name-extraction below never matches text inside them.
std::string strip_comments_and_literals(const std::string& src) {
    std::string out;
    out.reserve(src.size());
    enum class state { code, line_comment, block_comment, string_lit, char_lit };
    state st = state::code;
    for (size_t i = 0; i < src.size(); i++) {
        char c = src[i];
        char next = (i + 1 < src.size()) ? src[i + 1] : '\0';
        switch (st) {
            case state::code:
                if (c == '/' && next == '/') { st = state::line_comment; i++; }
                else if (c == '/' && next == '*') { st = state::block_comment; i++; }
                else if (c == '"') { st = state::string_lit; out += c; }
                else if (c == '\'') { st = state::char_lit; out += c; }
                else out += c;
                break;
            case state::line_comment:
                if (c == '\n') { st = state::code; out += c; }
                break;
            case state::block_comment:
                if (c == '*' && next == '/') { st = state::code; i++; }
                break;
            case state::string_lit:
                if (c == '\\') i++;                 // skip escaped char
                else if (c == '"') { st = state::code; out += c; }
                break;
            case state::char_lit:
                if (c == '\\') i++;
                else if (c == '\'') { st = state::code; out += c; }
                break;
        }
    }
    return out;
}

bool is_ident_char(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

// Function names declared in the public header: every identifier starting
// with "lattice_" that is directly followed by '(' — i.e. a function
// declarator. Function-POINTER typedef names ("(*lattice_invoke_fn)") are
// followed by ')' and never match; a fn-pointer typedef's RETURN type
// ("lattice_websocket_state_t (*lattice_ws_state_fn)(...)") is followed by
// "(*" and is excluded by the fn-ptr check below; type/param usages are
// never followed by '('; macro names are uppercase and fail the prefix
// check.
std::set<std::string> declared_functions(const std::string& header_src) {
    std::string text = strip_comments_and_literals(header_src);
    std::set<std::string> names;
    for (size_t i = 0; i < text.size();) {
        if (!is_ident_char(text[i]) || (i > 0 && is_ident_char(text[i - 1]))) { i++; continue; }
        size_t start = i;
        while (i < text.size() && is_ident_char(text[i])) i++;
        std::string ident = text.substr(start, i - start);
        size_t j = i;
        while (j < text.size() && std::isspace(static_cast<unsigned char>(text[j]))) j++;
        if (j < text.size() && text[j] == '(' && ident.rfind("lattice_", 0) == 0) {
            // "name (*" is a function-pointer declarator, not a function:
            // `ident` is the pointed-to function's return type. (No function
            // in this C header takes an un-typedef'd fn-ptr first parameter.)
            size_t k = j + 1;
            while (k < text.size() && std::isspace(static_cast<unsigned char>(text[k]))) k++;
            if (k < text.size() && text[k] == '*') { continue; }
            names.insert(ident);
        }
    }
    return names;
}

// Function names DEFINED in the implementation: for every `extern "C"`
// occurrence, the identifier immediately preceding the next '(' is the
// function being defined.
std::set<std::string> defined_functions(const std::string& impl_src) {
    std::string text = strip_comments_and_literals(impl_src);
    std::set<std::string> names;
    // NOTE: the stripper above empties string-literal CONTENTS but keeps the
    // quotes, so `extern "C"` appears as `extern ""` in the stripped text.
    const std::string anchor = "extern \"\"";
    for (size_t pos = text.find(anchor); pos != std::string::npos;
         pos = text.find(anchor, pos + anchor.size())) {
        size_t paren = text.find('(', pos);
        if (paren == std::string::npos) break;
        size_t end = paren;
        while (end > pos && std::isspace(static_cast<unsigned char>(text[end - 1]))) end--;
        size_t start = end;
        while (start > pos && is_ident_char(text[start - 1])) start--;
        std::string ident = text.substr(start, end - start);
        if (ident.rfind("lattice_", 0) == 0) {
            names.insert(ident);
        }
    }
    return names;
}

std::vector<std::string> symbols_file_entries(const std::string& src) {
    std::vector<std::string> entries;
    std::istringstream in(src);
    std::string line;
    while (std::getline(in, line)) {
        while (!line.empty() && (line.back() == '\r' || line.back() == ' ')) line.pop_back();
        if (line.empty() || line[0] == '#') continue;
        entries.push_back(line);
    }
    return entries;
}

std::string join(const std::set<std::string>& names) {
    std::string out;
    for (const auto& n : names) {
        out += "  ";
        out += n;
        out += "\n";
    }
    return out;
}

std::set<std::string> difference(const std::set<std::string>& a, const std::set<std::string>& b) {
    std::set<std::string> out;
    std::set_difference(a.begin(), a.end(), b.begin(), b.end(),
                        std::inserter(out, out.begin()));
    return out;
}

}  // namespace

TEST(CAPISurfaceTests, HeaderCompilesAndLinksAsC11) {
    // The real assertion is that capi_header_compiles.c BUILT (as pure C11).
    // Calling its probe proves the TU is still part of the test target.
    EXPECT_EQ(lattice_capi_header_compiles_as_c11(), 1);
}

TEST(CAPISurfaceTests, EveryDeclaredFunctionIsImplemented) {
    auto declared = declared_functions(
        read_file(repo_root() / "Sources/LatticeCAPI/include/lattice.h"));
    auto defined = defined_functions(
        read_file(repo_root() / "Sources/LatticeCAPI/src/lattice.cpp"));
    ASSERT_GE(declared.size(), 90u) << "header parser regressed (implausibly few declarations)";
    ASSERT_GE(defined.size(), 90u) << "impl parser regressed (implausibly few definitions)";

    auto phantoms = difference(declared, defined);
    EXPECT_TRUE(phantoms.empty())
        << "functions declared in lattice.h but NOT implemented in "
           "Sources/LatticeCAPI/src/lattice.cpp (phantom symbols - consumers "
           "linking these fail; see docs/capi-gap-audit.md B-9):\n"
        << join(phantoms);
}

TEST(CAPISurfaceTests, EveryImplementedFunctionIsDeclared) {
    auto declared = declared_functions(
        read_file(repo_root() / "Sources/LatticeCAPI/include/lattice.h"));
    auto defined = defined_functions(
        read_file(repo_root() / "Sources/LatticeCAPI/src/lattice.cpp"));

    auto undeclared = difference(defined, declared);
    EXPECT_TRUE(undeclared.empty())
        << "extern \"C\" lattice_* functions defined in lattice.cpp but not "
           "declared in the public header (undocumented ABI surface):\n"
        << join(undeclared);
}

TEST(CAPISurfaceTests, SymbolsFileMatchesDeclaredSurface) {
    auto declared = declared_functions(
        read_file(repo_root() / "Sources/LatticeCAPI/include/lattice.h"));
    auto entries = symbols_file_entries(
        read_file(repo_root() / "Sources/LatticeCAPI/lattice_capi.symbols"));

    // The checked-in file must be sorted and duplicate-free (so nm-based
    // tooling can diff it directly).
    auto sorted = entries;
    std::sort(sorted.begin(), sorted.end());
    EXPECT_EQ(entries, sorted) << "lattice_capi.symbols must be sorted";
    std::set<std::string> symbol_set(entries.begin(), entries.end());
    EXPECT_EQ(symbol_set.size(), entries.size())
        << "lattice_capi.symbols contains duplicates";

    auto missing_from_file = difference(declared, symbol_set);
    auto extra_in_file = difference(symbol_set, declared);
    EXPECT_TRUE(missing_from_file.empty() && extra_in_file.empty())
        << "Sources/LatticeCAPI/lattice_capi.symbols is out of sync with "
           "lattice.h. Any C-ABI surface change must update the symbols file "
           "deliberately (regeneration recipe in docs/CAPI-STABILITY.md).\n"
        << (missing_from_file.empty() ? ""
                : "declared in header, missing from symbols file:\n" + join(missing_from_file))
        << (extra_in_file.empty() ? ""
                : "listed in symbols file, not declared in header:\n" + join(extra_in_file));
}
