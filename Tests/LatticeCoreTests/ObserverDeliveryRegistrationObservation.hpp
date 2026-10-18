#pragma once
#include "../../Sources/LatticeCore/src/observer_delivery_registration_observation.hpp"
#include <lattice/db.hpp>
#include <cstdio>
#include <utility>
namespace lattice::detail::observer_delivery_registration_testing {
// Actual install/enroll are called OUTSIDE Native admission by these two
// fixture entries. Their exception has unwound every actual Native scope
// before this catch reports fixed copied DATA. No what()/user target runs.
class scope final {
    const uint64_t creator_=database_native_process();
    observer_delivery_registration_observation data_;
    observer_delivery_registration_observation* const previous_;
public:
    scope():previous_(observer_delivery_registration_observation::current) {
        data_.creator=creator_;observer_delivery_registration_observation::current=&data_;
    }
    void reset()noexcept {
        if(creator_==database_native_process()&&observer_delivery_registration_observation::current==&data_)
            observer_delivery_registration_observation::current=previous_;
    }
    ~scope()noexcept{reset();}
    void report()const noexcept {
        if(creator_!=database_native_process()||!data_.observed)return;
        std::fprintf(stderr,"delivery-registration-statements process=%llu count=%u more=%u\n",
            static_cast<unsigned long long>(creator_),unsigned(data_.count),unsigned(data_.more));
        for(size_t index=0;index<data_.count;++index) {
            const auto& entry=data_.entries[index];
            std::fprintf(stderr,"delivery-registration-statement index=%zu readonly=%u busy=%u sqlPresent=%u bytes=%u truncated=%u sqlHex=",
                index,unsigned(entry.readonly),unsigned(entry.busy),unsigned(entry.sql_present),
                unsigned(entry.bytes),unsigned(entry.truncated));
            for(size_t i=0;i<entry.bytes;++i)std::fprintf(stderr,"%02x",unsigned(static_cast<unsigned char>(entry.sql[i])));
            std::fputc('\n',stderr);
        }
        std::fflush(stderr);
    }
    scope(const scope&)=delete;
    scope& operator=(const scope&)=delete;
};
template<class Function> decltype(auto) observe(Function&& actual) {
    scope held;
    try{return std::forward<Function>(actual)();}
    catch(...) {held.reset();held.report();throw;}
}
}
