#pragma once
#ifdef __cplusplus
#include "observation.hpp"
#include <cstdint>
#include <set>

namespace lattice::detail {
// Independent custody: tokens and queued work never retain a raw lattice_db.
// Cancellation fences admission; an already admitted callback may finish.
class managed_observation_state {
public:
    using callback = std::function<void(bool, const std::vector<std::string>&)>;
    managed_observation_state();
    ~managed_observation_state();
    managed_observation_state(const managed_observation_state&) = delete;
    managed_observation_state& operator=(const managed_observation_state&) = delete;
    notification_token observe(const std::string& table, int64_t row,
        const std::string& global_id, std::vector<std::string> fallback_fields, callback);
    void append(const std::string& table, const std::string& operation, int64_t row,
        const std::string& global_id, const std::string& fields_json,
        std::vector<std::function<void()>>& callbacks);
    void retire();
    // Private recovery interest seam. The hook is weak, immutable and invoked
    // off all slot/state locks. Its internal callback must not throw or perform
    // SQL; revision orders notifications that can arrive from different threads.
    using interest_callback = std::function<void(uint64_t, bool)>;
    void set_interest_observer(const std::shared_ptr<const interest_callback>&);
    std::set<std::string> observed_tables() const;
private:
    struct state;
    std::shared_ptr<state> state_;
};
}
#endif
