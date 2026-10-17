#pragma once
#include <cstdint>
#include <memory>
#include <utility>

namespace lattice::detail {
struct ordinary_history_refund_cell;
class ordinary_export_access;

// Private DATA only. No pointer/coordinate can construct an issued source,
// cut, page, capacity grant or completed cleanup. Empty/forked is unavailable.
enum class ordinary_history_refund_status : std::uint8_t {
    unavailable, held, quarantined, refunded, inherited
};
class ordinary_history_refund_observation final {
    std::shared_ptr<const ordinary_history_refund_cell> cell_;
    explicit ordinary_history_refund_observation(std::shared_ptr<const ordinary_history_refund_cell> cell)
        :cell_(std::move(cell)) {}
    friend class ordinary_export_access;
public:
    ordinary_history_refund_observation()=default;
    ordinary_history_refund_status status()const noexcept;
    bool same_reservation(const ordinary_history_refund_observation& other)const noexcept {
        return cell_&&cell_==other.cell_;
    }
};
}
