#pragma once
#include "observer_delivery_values.hpp"
#include <string>
#include <cstdint>
#include <vector>

namespace lattice {
struct managed_base;
template<typename T,typename>struct managed;
}
namespace lattice::detail {
// Closed library adapter, not an application collector or SQL callback.
class observer_delivery_assignment final {
    template<typename T>static void builtin(managed<T,void>&,std::size_t kind,
        uint64_t extent,observer_delivery_values::value_view);
public:
    // Throws for an inherited actual owner before copying an input. Legacy
    // and unmanaged fields return false and preserve their old entry behavior.
    static bool enabled(const managed_base&);
    static void scalar(managed<int64_t,void>&,int64_t);
    static void scalar(managed<int,void>&,int);
    static void scalar(managed<double,void>&,double);
    static void scalar(managed<float,void>&,float);
    static void scalar(managed<bool,void>&,bool);
    static void blob(managed<std::vector<uint8_t>,void>&,const std::vector<uint8_t>&);
    static void string(managed<std::string,void>&,const std::string&,
                       std::string* consumed_rvalue=nullptr);
};
}
