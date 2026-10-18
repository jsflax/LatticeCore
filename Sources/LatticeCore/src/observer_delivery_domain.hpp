#pragma once
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace lattice::detail {
struct observer_delivery_domain_test_access;
// Private, UNINTEGRATED admission primitive. A caller must acquire BEFORE any
// Native/store/registry leaf; no SQL, callback, scheduler or observer authority
// is created here. Units are declared count/byte liabilities, not sizeof(fn).
class observer_delivery_domain {
    struct state;
    struct client_state;
    struct lease_state;
public:
    using clock = std::chrono::steady_clock;
    struct units {
        uint64_t count = 0, bytes = 0;
        bool operator==(const units&) const = default;
    };
    struct limits {
        size_t clients = 0, waiters = 0;
        units guaranteed, burst;
    };
    enum class status {
        admitted, invalid, oversized, unavailable, deadline, cancelled,
        client_capacity, waiter_capacity, admission_capacity, closed, wrong_process,
        reentrant_would_block, exhausted, invariant_failure
    };
    struct snapshot {
        status code = status::invalid;
        units used, burst_used, capacity;
        size_t clients = 0, waiters = 0;
        uint64_t leases = 0;
    };
    class cancellation {
        const uint64_t creator_;
        const std::shared_ptr<state> domain_;
        std::atomic<bool> stopped_{false};
        friend class observer_delivery_domain;
    public:
        explicit cancellation(const observer_delivery_domain&);
        void request_stop() noexcept;
        bool stop_requested() const noexcept;
    };
    class delivery_scope;
    class lease {
        std::shared_ptr<lease_state> value_;
        explicit lease(std::shared_ptr<lease_state> value) : value_(std::move(value)) {}
        friend class observer_delivery_domain;
        friend class delivery_scope;
    public:
        lease() = default;
        // Copies share ONE actual allocation/charge. Only its last retirement
        // returns credit; callback entry/return alone is not an ending.
        bool valid() const noexcept;
        units charge() const noexcept;
        void reset() noexcept { value_.reset(); }
    };
    struct admission { status code = status::invalid; lease held; };
    class client {
        std::shared_ptr<client_state> value_;
        explicit client(std::shared_ptr<client_state> value) : value_(std::move(value)) {}
        friend class observer_delivery_domain;
    public:
        client() = default;
        admission try_acquire(units) const;
        // Optional cancellation cell must belong to this exact domain and
        // outlive the call. Wait has ONE absolute deadline, no polling thread.
        admission acquire(units, clock::time_point absolute_deadline,
                          const cancellation* = nullptr) const;
        void close() const noexcept;
        void reset() noexcept { value_.reset(); }
    };
    struct registration { status code = status::invalid; client value; };
    class delivery_scope {
        lease held_;
        const state* domain_ = nullptr;
        delivery_scope* previous_ = nullptr;
        uint64_t creator_ = 0;
        static thread_local delivery_scope* current_;
        static bool active_for(const state*) noexcept;
        friend class observer_delivery_domain;
    public:
        // Actual dispatch wiring must install this across user callbacks AND
        // their captured tail. It does not execute work or grant SQL access.
        explicit delivery_scope(const lease&);
        ~delivery_scope() noexcept;
        delivery_scope(const delivery_scope&) = delete;
        delivery_scope& operator=(const delivery_scope&) = delete;
    };

    observer_delivery_domain() = default;
    // Fixed storage ceiling: <=64 clients, <=256 queued waiters. Unused client
    // guarantees are never lent as burst, protecting a future quiet client.
    static observer_delivery_domain create(limits);
    registration register_client() const;
    snapshot inspect() const noexcept;
    void close() const noexcept;
private:
    std::shared_ptr<state> value_;
    explicit observer_delivery_domain(std::shared_ptr<state> value) : value_(std::move(value)) {}
    static admission acquire_(const std::shared_ptr<client_state>&, units,
        clock::time_point, const cancellation*, bool wait);
    friend struct observer_delivery_domain_test_access;
};
}
