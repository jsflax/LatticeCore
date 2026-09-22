#pragma once
#include "sync_canonical_range.hpp"

namespace lattice::detail::canonical_range {
// Private, source-owned session optimization. Construction validates and owns
// every immutable input; no caller can import a proof flag, digest or restart
// state. A completed cursor proves sequence/whole hashes only, never custody,
// publication, remote installation, a receipt's authority, or physical routing.
class validated_sequence {
    struct state;
    std::unique_ptr<state> state_;
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
