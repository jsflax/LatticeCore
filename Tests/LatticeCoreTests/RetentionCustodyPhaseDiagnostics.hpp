#pragma once

#include <cstdio>
#include <cstddef>
#include <exception>
#include <utility>

// Fixture diagnostics only: no SQLite calls, raw handles, extra descriptors,
// callbacks registered with an owner, sleeps, retries, or changed assertions.
namespace retention_fixture_diagnostics {
class phases {
    const char* test_;
    const char* mode_ = "none";
    unsigned records_ = 0;
    static constexpr unsigned max_records = 128;

    void record(const char* operation, const char* event, const char* detail = "") noexcept {
        if (records_ >= max_records) return;
        ++records_;
        char message[161]{};
        for (unsigned i = 0; i < sizeof(message) - 1 && detail[i]; ++i)
            message[i] = detail[i] == '\n' || detail[i] == '\r' ? ' ' : detail[i];
        char line[512]{};
        const int size = std::snprintf(line, sizeof(line),
            "[retention-phase] case=%.110s mode=%.24s record=%u operation=%.64s event=%.24s detail=%.160s\n",
            test_, mode_, records_, operation, event, message);
        if (size > 0) {
            const auto count = static_cast<std::size_t>(size) < sizeof(line) ? static_cast<std::size_t>(size) : sizeof(line) - 1;
            (void)std::fwrite(line, 1, count, stderr);
            (void)std::fflush(stderr);
        }
    }
    void error(const char* operation, const char* role, const std::exception_ptr& value) noexcept {
        if (!value) return;
        try { std::rethrow_exception(value); }
        catch (const std::exception& caught) { record(operation, role, caught.what()); }
        catch (...) { record(operation, role, "non-std exception"); }
    }
public:
    explicit phases(const char* test) noexcept : test_(test) {}
    void mode(const char* value) noexcept { mode_ = value; record("mode", "begin"); }

    template<class Function>
    decltype(auto) call(const char* operation, Function&& action) {
        record(operation, "begin");
        struct completion {
            phases& owner;
            const char* operation;
            int exceptions = std::uncaught_exceptions();
            ~completion() noexcept {
                if (std::uncaught_exceptions() == exceptions) owner.record(operation, "returned");
            }
        } completed{*this, operation};
        try { return std::forward<Function>(action)(); }
        catch (const std::exception& caught) { record(operation, "threw", caught.what()); throw; }
        catch (...) { record(operation, "threw", "non-std exception"); throw; }
    }

    template<class Result>
    void settlement(const char* operation, const Result& value) noexcept {
        char detail[128]{};
        std::snprintf(detail, sizeof(detail),
            "state=%d primary=%d cleanup=%d postcommit=%d notification=%d",
            static_cast<int>(value.state), bool(value.primary_error), bool(value.cleanup_error),
            bool(value.postcommit_error), bool(value.notification_error));
        record(operation, "settlement", detail);
        error(operation, "primary", value.primary_error);
        error(operation, "cleanup", value.cleanup_error);
        error(operation, "postcommit", value.postcommit_error);
        error(operation, "notification", value.notification_error);
    }

    // Declare this before the fixture's unchanged restoration guard. Its
    // destructor records only after that guard has completed, including unwind.
    class restoration_completion {
        phases& owner_;
        const char* operation_;
    public:
        restoration_completion(phases& owner, const char* operation) noexcept
            : owner_(owner), operation_(operation) {}
        restoration_completion(const restoration_completion&) = delete;
        ~restoration_completion() noexcept {
            owner_.record(operation_, "scope-exit-after-restore",
                std::uncaught_exceptions() ? "exception unwinding" : "normal scope exit");
        }
    };
    restoration_completion restored(const char* operation) noexcept {
        return restoration_completion(*this, operation);
    }
};
} // namespace retention_fixture_diagnostics
