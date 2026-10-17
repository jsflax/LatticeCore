#pragma once
#include <chrono>

namespace lattice::detail {
// Closed hooks for the one actual export reservation pool. The common resource
// header defines its sole counter; these operations cannot enlarge its limit.
// Admission is serialized with irreversible service closure and precedes source
// effects. Only actual final reservation destruction releases capacity, after
// retained native output storage is gone; that release supplies the same hint.
void reserve_ordinary_export_capacity();
void release_ordinary_export_capacity()noexcept;
void prepare_ordinary_export_capacity_notifications();
void ordinary_export_capacity_released()noexcept;
// Close is irreversible for this process. It stops new admission, emits a
// terminal hint so waiting callbacks see semantic refusal, then drains finite
// snapshot ownership. Native drain joins the one worker. Browser drain returns
// false until its main-runtime turn and explicit queue destruction have settled.
void request_ordinary_export_capacity_notification_close()noexcept;
// Completion rendezvous for owned lifecycle/tests; never schedules a retry or
// polls capacity. The supplied deadline does not create or extend a retry timer.
bool await_ordinary_export_capacity_notification_idle(std::chrono::steady_clock::time_point)noexcept;
// Drain also requests irreversible close. In-flight service creation returns
// false until its creator has published/settled it; absence is never inferred
// while initialization remains in flight.
bool drain_ordinary_export_capacity_notifications()noexcept;
}
