#pragma once
#include "../../Sources/LatticeSwiftCppBridge/include/error.hpp"
#include <cstdio>
#include <exception>
#include <string>

namespace lattice_test {
// A thin exception_ptr adapter; the one genuine Bridge formatter owns all
// nested-type/depth/node/byte policy. Thread-owned exception slots are read
// only after their actual thread joins.
inline std::string actual_exception_text(const std::exception_ptr& original) noexcept {
    std::string text;
    if (!original) return text;
    try { std::rethrow_exception(original); }
    catch (const std::exception& error) { lattice::assign_ordinary_bridge_exception(text, error); }
    catch (...) {
        try { text = "<non-standard exception>"; }
        catch (...) { /* Text unavailable; caller retains/reports actual original presence. */ }
    }
    return text;
}
}
