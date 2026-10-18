#ifndef error_hpp
#define error_hpp

#include <exception>
#include <cstddef>
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
/// Text compatibility surface only: an empty what() or a failed diagnostic
/// allocation cannot establish success. Use borrow_bridge_outcome immediately
/// after the call for authoritative status. The text borrow expires at the
/// next Bridge call on this thread; it owns neither an owner nor a payload.
inline std::string& last_bridge_error() {
    static thread_local std::string err;
    return err;
}

namespace detail {
struct bridge_failure_status {
    bool failed = false;
    bool diagnostic_available = true;
};
inline bridge_failure_status& bridge_failure_status_slot() noexcept {
    static thread_local bridge_failure_status status;
    return status;
}

// Failure is published BEFORE any diagnostic initialization/copy. The writer
// returns whether it produced a valid diagnostic, including a valid empty one.
// Kept separate so allocation/copy refusal can be tested at this real boundary.
template <typename Writer>
inline void record_bridge_failure(Writer&& writer) noexcept {
    auto& status = bridge_failure_status_slot();
    status.failed = true;
    status.diagnostic_available = false;
    try { status.diagnostic_available = writer(last_bridge_error()); }
    catch (...) {} // A secondary failure cannot erase authoritative failure.
}
}

inline void clear_bridge_outcome() noexcept {
    auto& status = detail::bridge_failure_status_slot();
    status = {};
    last_bridge_error().clear();
}
inline bool last_bridge_call_failed() noexcept {
    return detail::bridge_failure_status_slot().failed;
}

struct bridge_outcome_view {
    bool failed = false;
    bool diagnostic_available = true;
    const char* message_data = nullptr;
    std::size_t message_size = 0;
};

// No allocation, TLS clearing, owner lookup, SQL or callback. The caller must
// copy an available diagnostic before any subsequent Bridge call/reentry.
inline bridge_outcome_view borrow_bridge_outcome() noexcept {
    const auto status = detail::bridge_failure_status_slot();
    if (!status.failed || !status.diagnostic_available)
        return {status.failed, status.diagnostic_available, nullptr, 0};
    const auto& message = last_bridge_error();
    return {true, true, message.data(), message.size()};
}

inline void record_bridge_error(const char* message) noexcept {
    detail::record_bridge_failure([&](std::string& destination) {
        if (!message) return false;
        destination = message;
        return true;
    });
}

// The ordinary adapter reads retained first-error banks without changing them.
// It writes at most 2048 bytes; failure/validity never comes from this text.
void assign_ordinary_bridge_exception(std::string&, const std::exception&) noexcept;
bool assign_ordinary_bridge_exception_checked(std::string&, const std::exception&) noexcept;
// False leaves unrelated exceptions on the unchanged generic sealed path.
bool record_ordinary_bridge_exception(const std::exception&) noexcept;

/// Run a Swift-facing bridge body under the sealed contract: clear the
/// error slot, catch any C++ exception, stash its message, and return a
/// default-constructed value ({} / 0 / "" / empty vector) in its place.
template <typename F>
decltype(auto) sealed(F&& f) {
    clear_bridge_outcome();
    using R = decltype(f());
    if constexpr (std::is_void_v<R>) {
        try { f(); }
        catch (const std::exception& e) {
            if (!record_ordinary_bridge_exception(e)) record_bridge_error(e.what());
        }
        catch (...) { record_bridge_error("Unknown C++ bridge exception"); }
    } else {
        try { return f(); }
        catch (const std::exception& e) {
            if (!record_ordinary_bridge_exception(e)) record_bridge_error(e.what());
            return R{};
        }
        catch (...) {
            record_bridge_error("Unknown C++ bridge exception");
            return R{};
        }
    }
}

// Existing typed message API remains available. The bool, not message length,
// is authoritative even if an exception has empty what() or message copy fails.
template <typename F>
bool sealed_with_error(cxx_error& error, F&& body) noexcept {
    clear_bridge_outcome();
    try {
        error.name.clear();
        error.msg.clear();
        body();
        return !last_bridge_call_failed();
    } catch (const std::exception& value) {
        if (!record_ordinary_bridge_exception(value)) record_bridge_error(value.what());
        try { error = value; }
        catch (...) { try { error.msg = "?"; } catch (...) {} }
    } catch (...) {
        record_bridge_error("Unknown C++ bridge exception");
        try { error.msg = "Unknown C++ bridge exception"; }
        catch (...) { try { error.msg = "?"; } catch (...) {} }
    }
    return false;
}

}

#endif /* error_hpp */
