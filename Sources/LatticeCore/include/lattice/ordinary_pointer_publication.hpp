#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <utility>

namespace lattice::detail {
// Private publication storage only. Actual owner/currentness and native
// enrollment checks remain at every caller; a pointer copy issues no grant.
template<class T> class ordinary_shared_publication final {
    std::shared_ptr<T> value_;
public:
    ordinary_shared_publication() noexcept = default;
    ordinary_shared_publication(const ordinary_shared_publication&) = delete;
    ordinary_shared_publication& operator=(const ordinary_shared_publication&) = delete;
    std::shared_ptr<T> load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
        return std::atomic_load_explicit(&value_,order);
    }
    void store(std::shared_ptr<T> incoming,std::memory_order order = std::memory_order_seq_cst) noexcept {
        // The actual shared_ptr free function swaps its old value out, then
        // unlocks before that displaced shared owner can be destroyed.
        std::atomic_store_explicit(&value_,std::move(incoming),order);
    }
    std::shared_ptr<T> exchange(std::shared_ptr<T> incoming,std::memory_order order = std::memory_order_seq_cst) noexcept {
        return std::atomic_exchange_explicit(&value_,std::move(incoming),order);
    }
};

// Use explicit weak synchronization where the selected library lacks usable
// atomic<weak_ptr<T>> support. Callers perform .lock() after snapshot returns.
template<class T> class ordinary_weak_publication final {
    mutable std::mutex mutex_;
    std::weak_ptr<T> value_;
public:
    ordinary_weak_publication() = default;
    ordinary_weak_publication(const ordinary_weak_publication&) = delete;
    ordinary_weak_publication& operator=(const ordinary_weak_publication&) = delete;
    std::weak_ptr<T> snapshot() const {
        std::weak_ptr<T> retained;
        {
            std::lock_guard<std::mutex> leaf(mutex_);
            retained=value_; // Empty output: no displaced weak owner dies under the leaf.
        }
        return retained;
    }
    void publish(std::weak_ptr<T> incoming) {
        {
            std::lock_guard<std::mutex> leaf(mutex_);
            value_.swap(incoming);
        }
        // The displaced weak reference/control block dies after unlock.
        // No strong receiver owner or native capability is stored here.
    }
};
} // namespace lattice::detail
