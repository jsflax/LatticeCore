#ifndef lattice_bridge_owner_identity_hpp
#define lattice_bridge_owner_identity_hpp

#ifdef __cplusplus
#include <bridging.hpp>
#include <cstdint>
#include <memory>
#include <new>
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

namespace lattice {
class swift_lattice;
class swift_lattice_ref;
class dynamic_object_ref;
struct link_list;

// A weak identity for one real Native wrapper incarnation. This conveys no
// access to the owner, SQLite, a path, a callback, or an admission grant.
// Copies, comparison and destruction check the creating process before
// touching even the weak control block inherited by a forked child.
class bridge_owner_identity final {
public:
    bridge_owner_identity() noexcept {}
    bridge_owner_identity(const bridge_owner_identity& other) noexcept {
        if (other.current()) {
            new (&owner_) std::weak_ptr<swift_lattice>(other.owner_);
            creator_ = other.creator_;
        }
    }
    bridge_owner_identity(bridge_owner_identity&& other) noexcept
        : bridge_owner_identity(static_cast<const bridge_owner_identity&>(other)) {}
    bridge_owner_identity& operator=(const bridge_owner_identity& other) noexcept {
        if (this != &other) {
            reset();
            if (other.current()) {
                new (&owner_) std::weak_ptr<swift_lattice>(other.owner_);
                creator_ = other.creator_;
            }
        }
        return *this;
    }
    bridge_owner_identity& operator=(bridge_owner_identity&& other) noexcept {
        return operator=(static_cast<const bridge_owner_identity&>(other));
    }
    ~bridge_owner_identity() { reset(); }

    bool valid() const noexcept SWIFT_NAME(isValid()) {
        return current() && !owner_.expired();
    }
    bool matches(const bridge_owner_identity& other) const noexcept SWIFT_NAME(matches(_:)) {
        return current() && other.current() && !owner_.expired() && !other.owner_.expired()
            && !owner_.owner_before(other.owner_) && !other.owner_.owner_before(owner_);
    }

private:
    friend class swift_lattice_ref;
    explicit bridge_owner_identity(const std::shared_ptr<swift_lattice>& owner) noexcept {
        if (owner) {
            new (&owner_) std::weak_ptr<swift_lattice>(owner);
            creator_ = process();
        }
    }
    static std::uint64_t process() noexcept {
#if defined(__APPLE__) || defined(__linux__)
        return static_cast<std::uint64_t>(::getpid());
#else
        return 1;
#endif
    }
    bool current() const noexcept { return creator_ != 0 && creator_ == process(); }
    void reset() noexcept {
        if (current()) owner_.~weak_ptr();
        creator_ = 0;
    }
    union { std::weak_ptr<swift_lattice> owner_; };
    std::uint64_t creator_ = 0;
};
}
#endif
#endif
