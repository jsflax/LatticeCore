#pragma once
#include "sync_canonical_range.hpp"

namespace lattice::detail::canonical_range {
// Private raw-byte entry point. It validates the received bytes and exact
// canonical spelling under these limits; the returned mutable DTO is no proof
// token and cannot bypass validation in any later operation.
frame decode_canonical(std::string_view,const limits&);
// Private, source-owned session optimization. Construction validates and owns
// every immutable input; no caller can import a proof flag, digest or restart
// state. A completed cursor proves sequence/whole hashes only, never custody,
// publication, remote installation, a receipt's authority, or physical routing.
class validated_sequence {
    struct state;
    std::unique_ptr<state> state_;
    void advance_validated(const frame&);
public:
    validated_sequence(const attempt&,const request&,const manifest&,const limits&);
    ~validated_sequence();
    validated_sequence(validated_sequence&&) noexcept;
    validated_sequence& operator=(validated_sequence&&) noexcept;
    validated_sequence(const validated_sequence&)=delete;
    validated_sequence& operator=(const validated_sequence&)=delete;
    // Strong refusal guarantee, including terminal digest mismatch/allocation:
    // live progress and hashes are unchanged until the complete transition fits.
    void advance(const frame&);
    // Decode and canonical-check under this cursor's narrowed frozen-Q limits,
    // then advance atomically. The route argument is exact spelling only, not
    // authentication. No caller-supplied DTO or validation flag is accepted.
    frame advance_canonical(std::string_view,uint64_t expected_route);
    phase status() const noexcept;
    // Explicit diagnostic copy. Production assembly/audit never calls this.
    sequence_state snapshot() const;
};
namespace sequence_test_observation {
// Passive TLS counters only: no callback, clock, allocation, or input override.
struct counters { uint64_t request_validations=0,rebase_builds=0,restart_objects=0,cursors=0,transitions=0; };
extern thread_local counters* current;
}
}
