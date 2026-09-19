#pragma once
#ifdef __cplusplus
#include "observation.hpp"
#include <cstdint>

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
private:
    struct state;
    std::shared_ptr<state> state_;
};
}
#endif
