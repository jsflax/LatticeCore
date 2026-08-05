#ifndef error_hpp
#define error_hpp

#include <exception>
#include <string>
#include <type_traits>

namespace lattice {

struct cxx_error {
    cxx_error(const std::exception& e)
    : name(typeid(e).name())
    , msg(e.what())
    {
    }

    std::string name;
    std::string msg;
};

/// Thread-local record of the most recent failure in a sealed Swift-facing
/// bridge API. Swift cannot catch C++ exceptions, so the sealed surface
/// (bulk query APIs on swift_lattice, field access on dynamic_object_ref)
/// returns empty/0 on failure instead of throwing — this slot is how Swift
/// distinguishes "no data" from "the call failed" and bubbles a real error.
///
/// Contract: cleared at the entry of every sealed call, set by its catch —
/// so immediately after any sealed call it holds either "" (success) or
/// that call's failure message. Read it on the same thread, right after
/// the call; do not cache it.
inline std::string& last_bridge_error() {
    static thread_local std::string err;
    return err;
}

/// Run a Swift-facing bridge body under the sealed contract: clear the
/// error slot, catch any C++ exception, stash its message, and return a
/// default-constructed value ({} / 0 / "" / empty vector) in its place.
template <typename F>
decltype(auto) sealed(F&& f) {
    last_bridge_error().clear();
    using R = decltype(f());
    if constexpr (std::is_void_v<R>) {
        try { f(); }
        catch (const std::exception& e) { last_bridge_error() = e.what(); }
    } else {
        try { return f(); }
        catch (const std::exception& e) {
            last_bridge_error() = e.what();
            return R{};
        }
    }
}

}

#endif /* error_hpp */
