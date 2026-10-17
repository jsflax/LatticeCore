#pragma once
#include <lattice/core_transaction_receipt.hpp>
#include <owner_identity.hpp>
#include <error.hpp>
#include <array>
#include <algorithm>
#include <utility>

namespace lattice {
class swift_lattice_ref;

// Only the actual successful BEGIN can populate this opaque value. Copies
// preserve that original; neither an owner lookup nor a serial can mint one.
class swift_transaction_receipt {
    friend class swift_lattice_ref;
    detail::core_transaction_receipt core_;
    bridge_owner_identity owner_;
    swift_transaction_receipt(detail::core_transaction_receipt core,
                              bridge_owner_identity owner) noexcept
        : core_(std::move(core)), owner_(std::move(owner)) {}
public:
    swift_transaction_receipt() noexcept = default;
    bool valid() const noexcept SWIFT_NAME(isValid()) {
        return owner_.valid() && core_.valid();
    }
    bool same_original(const swift_transaction_receipt& other) const noexcept
        SWIFT_NAME(sameOriginal(_:)) {
        return owner_.matches(other.owner_) && core_.same_original(other.core_);
    }
};

// Own all status before returning to Swift. No error handler can replace this
// result by reentering another Bridge API and changing the TLS diagnostic.
class swift_transaction_result {
    friend class swift_lattice_ref;
    swift_transaction_receipt receipt_;
    bool failed_ = true, available_ = false, ended_ = false, truncated_ = false;
    std::array<char, 2048> message_{};
    std::size_t size_ = 0;
    void capture(bridge_outcome_view view) noexcept {
        failed_ = view.failed;
        available_ = view.diagnostic_available;
        if (!view.failed || !view.diagnostic_available) return;
        if (!view.message_data && view.message_size) { available_ = false; return; }
        size_ = std::min(view.message_size, message_.size());
        if (size_) std::copy_n(view.message_data, size_, message_.data());
        truncated_ = view.message_size > size_;
    }
public:
    swift_transaction_result() noexcept = default;
    swift_transaction_receipt receipt() const noexcept { return receipt_; }
    bool failed() const noexcept { return failed_; }
    bool ended() const noexcept { return ended_; }
    bool diagnostic_available() const noexcept SWIFT_NAME(diagnosticAvailable()) { return available_; }
    bool diagnostic_truncated() const noexcept SWIFT_NAME(diagnosticTruncated()) { return truncated_; }
    bridge_outcome_view borrow_outcome() const noexcept SWIFT_NAME(borrowOutcome()) {
        return {failed_, available_, message_.data(), size_};
    }
};
}
