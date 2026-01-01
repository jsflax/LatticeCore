#pragma once

#include <vector>
#include <string>
#include <functional>
#include <memory>
#include <optional>
#include <variant>
#include <exception>

// C++20 coroutine support detection
#if __cplusplus >= 202002L && __has_include(<coroutine>)
#define LATTICE_HAS_COROUTINES 1
#include <coroutine>
#else
#define LATTICE_HAS_COROUTINES 0
#endif

namespace lattice {

// Forward declaration for object_change (default arg defined in managed.hpp)
template<typename T, typename> struct managed;

// ============================================================================
// collection_change - Describes changes to a collection (results/list)
// Matches realm-cpp's collection_change / results_change pattern
// ============================================================================

struct collection_change {
    /// Indices of objects that were deleted from the collection
    std::vector<uint64_t> deletions;

    /// Indices of objects that were inserted into the collection
    std::vector<uint64_t> insertions;

    /// Indices of objects that were modified
    std::vector<uint64_t> modifications;

    /// True if the underlying collection root was deleted
    bool collection_root_was_deleted = false;

    /// Returns true if no changes occurred
    [[nodiscard]] bool empty() const noexcept {
        return deletions.empty() && insertions.empty() && modifications.empty() &&
               !collection_root_was_deleted;
    }
};

// ============================================================================
// property_change - Describes a change to a single property
// ============================================================================

template<typename T>
struct property_change {
    /// The name of the property that changed
    std::string name;

    // Note: old_value and new_value tracking can be added later if needed
    // For now, we just track which properties changed (like realm-cpp basic observation)
};

// ============================================================================
// object_change - Describes changes to a single object
// Matches realm-cpp's object_change pattern
// ============================================================================

template<typename T>
struct object_change {
    /// The managed object being observed (nullptr if deleted or error)
    /// T is the unmanaged type (e.g., Person), object is managed<Person>*
    const managed<T, void>* object = nullptr;

    /// True if the object was deleted
    bool is_deleted = false;

    /// Error if one occurred during observation
    std::exception_ptr error;

    /// Properties that changed
    std::vector<property_change<T>> property_changes;

    /// Returns true if this represents a deletion
    [[nodiscard]] bool deleted() const noexcept { return is_deleted; }

    /// Returns true if this represents an error
    [[nodiscard]] bool has_error() const noexcept { return error != nullptr; }
};

// ============================================================================
// notification_token - Retains observation until destroyed
// Matches realm-cpp's notification_token pattern (move-only)
// ============================================================================

class notification_token {
public:
    notification_token() = default;

    explicit notification_token(std::function<void()> unregister_fn)
        : unregister_(std::move(unregister_fn)) {}

    ~notification_token() {
        unregister();
    }

    // Move-only (like realm-cpp)
    notification_token(const notification_token&) = delete;
    notification_token& operator=(const notification_token&) = delete;

    notification_token(notification_token&& other) noexcept
        : unregister_(std::move(other.unregister_)) {
        other.unregister_ = nullptr;
    }

    notification_token& operator=(notification_token&& other) noexcept {
        if (this != &other) {
            unregister();
            unregister_ = std::move(other.unregister_);
            other.unregister_ = nullptr;
        }
        return *this;
    }

    /// Explicitly unregister the observation
    void unregister() {
        if (unregister_) {
            unregister_();
            unregister_ = nullptr;
        }
    }

    /// Alias for unregister() (backward compatibility)
    void invalidate() { unregister(); }

    /// Returns true if this token is valid (has an active observation)
    [[nodiscard]] bool is_valid() const noexcept {
        return unregister_ != nullptr;
    }

    explicit operator bool() const noexcept {
        return is_valid();
    }

private:
    std::function<void()> unregister_;
};

// ============================================================================
// C++20 Coroutine Support: change_stream
// Provides AsyncSequence-like API: for co_await (auto change : stream) { ... }
// ============================================================================

#if LATTICE_HAS_COROUTINES

/// Awaitable change event for coroutine-based observation
template<typename T>
struct change_awaiter {
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    struct promise_type {
        std::optional<T> current_value;
        std::exception_ptr exception;
        std::coroutine_handle<> waiting_coroutine;
        bool done = false;

        change_awaiter get_return_object() {
            return change_awaiter{handle_type::from_promise(*this)};
        }

        std::suspend_always initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept {
            done = true;
            if (waiting_coroutine) {
                waiting_coroutine.resume();
            }
            return {};
        }

        void return_void() {}

        void unhandled_exception() {
            exception = std::current_exception();
        }

        std::suspend_always yield_value(T value) {
            current_value = std::move(value);
            if (waiting_coroutine) {
                waiting_coroutine.resume();
            }
            return {};
        }
    };

    handle_type handle;

    change_awaiter(handle_type h) : handle(h) {}

    ~change_awaiter() {
        if (handle) handle.destroy();
    }

    // Move-only
    change_awaiter(const change_awaiter&) = delete;
    change_awaiter& operator=(const change_awaiter&) = delete;
    change_awaiter(change_awaiter&& other) noexcept : handle(other.handle) {
        other.handle = nullptr;
    }
    change_awaiter& operator=(change_awaiter&& other) noexcept {
        if (this != &other) {
            if (handle) handle.destroy();
            handle = other.handle;
            other.handle = nullptr;
        }
        return *this;
    }
};

/// Generator for collection changes that can be used with co_await
template<typename T>
class change_stream {
public:
    struct iterator;

    change_stream() = default;

    // The stream is created with a setup function that registers the observation
    // and provides a way to push changes
    using push_fn = std::function<void(collection_change)>;
    using setup_fn = std::function<notification_token(push_fn)>;

    explicit change_stream(setup_fn setup) : setup_(std::move(setup)) {}

    // Awaitable begin() for range-based for with co_await
    struct awaitable_iterator {
        change_stream* stream;
        std::optional<collection_change> current;
        bool done = false;

        bool operator!=(const awaitable_iterator& other) const {
            return !done || !other.done;
        }

        collection_change& operator*() {
            return *current;
        }

        // co_await ++it
        struct increment_awaiter {
            awaitable_iterator* it;

            bool await_ready() { return false; }

            void await_suspend(std::coroutine_handle<> h) {
                it->stream->waiting_ = h;
            }

            void await_resume() {
                if (it->stream->pending_changes_.empty()) {
                    it->done = true;
                } else {
                    it->current = std::move(it->stream->pending_changes_.front());
                    it->stream->pending_changes_.erase(it->stream->pending_changes_.begin());
                }
            }
        };

        increment_awaiter operator++() {
            return {this};
        }
    };

    // co_await begin()
    struct begin_awaiter {
        change_stream* stream;

        bool await_ready() { return !stream->pending_changes_.empty(); }

        void await_suspend(std::coroutine_handle<> h) {
            stream->waiting_ = h;
            // Start observation - capture stream pointer explicitly
            auto s = stream;
            stream->token_ = stream->setup_([s](collection_change change) {
                s->pending_changes_.push_back(std::move(change));
                if (s->waiting_) {
                    auto handle = s->waiting_;
                    s->waiting_ = nullptr;
                    handle.resume();
                }
            });
        }

        awaitable_iterator await_resume() {
            awaitable_iterator it{stream, std::nullopt, false};
            if (!stream->pending_changes_.empty()) {
                it.current = std::move(stream->pending_changes_.front());
                stream->pending_changes_.erase(stream->pending_changes_.begin());
            }
            return it;
        }
    };

    begin_awaiter begin() { return {this}; }

    awaitable_iterator end() { return {this, std::nullopt, true}; }

    /// Stop the observation
    void stop() {
        token_.unregister();
    }

private:
    setup_fn setup_;
    notification_token token_;
    std::vector<collection_change> pending_changes_;
    std::coroutine_handle<> waiting_;
};

#endif // LATTICE_HAS_COROUTINES

} // namespace lattice
