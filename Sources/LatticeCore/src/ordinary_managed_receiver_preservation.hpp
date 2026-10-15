#pragma once
#include "ordinary_managed_history.hpp"
#include <lattice/ordinary_history.hpp>

namespace lattice::detail {
// Passive sealed archive coordinates only. They are not a source grant, a
// receiver receipt, a canonical conflict decision, or a final-install result.
struct ordinary_receiver_preservation_record final {
    std::string snapshot,generation,scope,schema,device,inode,catalog_digest,content,seal;
    uint64_t rows=0,records=0,bytes=0;
};
class ordinary_managed_receiver_preservation final {
    friend class ordinary_export_access;
    struct state;
    std::shared_ptr<const state> state_;
    explicit ordinary_managed_receiver_preservation(std::shared_ptr<const state> value):state_(std::move(value)){}
public:
    ~ordinary_managed_receiver_preservation();
    ordinary_managed_receiver_preservation(const ordinary_managed_receiver_preservation&)=delete;
    ordinary_managed_receiver_preservation& operator=(const ordinary_managed_receiver_preservation&)=delete;
};
// Test friendship has access only to passive storage capture/verification.
// There is no test constructor or conversion to the production restart cap.
struct ordinary_receiver_preservation_test_access;
}
