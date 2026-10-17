#pragma once
#include <cstdint>
#include <memory>
#include <new>
#include <unistd.h>
namespace lattice { class lattice_db; }
namespace lattice::detail {
// Opaque authority for one actual successful Core BEGIN. Empty construction,
// pure identity comparison and creator-fenced copies expose no SQLite, owner,
// path, numeric serial, setters, callback or current-transaction lookup.
class core_transaction_receipt final {
public:
    core_transaction_receipt() noexcept {}
    core_transaction_receipt(const core_transaction_receipt& other) noexcept {
        if (other.current_()) {
            new (&original_) std::shared_ptr<const void>(other.original_);
            creator_ = other.creator_; issued_ = other.issued_;
        }
    }
    core_transaction_receipt(core_transaction_receipt&& other) noexcept
        : core_transaction_receipt(static_cast<const core_transaction_receipt&>(other)) {}
    core_transaction_receipt& operator=(const core_transaction_receipt& other) noexcept {
        if (this != &other) {
            reset_();
            if (other.current_()) {
                new (&original_) std::shared_ptr<const void>(other.original_);
                creator_ = other.creator_; issued_ = other.issued_;
            }
        }
        return *this;
    }
    core_transaction_receipt& operator=(core_transaction_receipt&& other) noexcept {
        return operator=(static_cast<const core_transaction_receipt&>(other));
    }
    ~core_transaction_receipt() noexcept { reset_(); }
    bool valid() const noexcept { return current_() && issued_ && bool(original_); }
    bool same_original(const core_transaction_receipt& other) const noexcept {
        return valid() && other.valid() && original_.get() == other.original_.get() &&
            !original_.owner_before(other.original_) && !other.original_.owner_before(original_);
    }
private:
    friend class ::lattice::lattice_db;
    explicit core_transaction_receipt(std::shared_ptr<const void> original) noexcept {
        if (original) {
            new (&original_) std::shared_ptr<const void>(std::move(original));
            creator_ = process_();
        }
    }
    static uint64_t process_() noexcept { return static_cast<uint64_t>(::getpid()); }
    bool current_() const noexcept { return creator_ != 0 && creator_ == process_(); }
    void reset_() noexcept {
        if (current_()) original_.~shared_ptr();
        creator_ = 0; issued_ = false; // inherited payload is abandoned, never decremented
    }
    union { std::shared_ptr<const void> original_; };
    uint64_t creator_ = 0;
    bool issued_ = false; // only actual successful BEGIN publishes authority
};
}
