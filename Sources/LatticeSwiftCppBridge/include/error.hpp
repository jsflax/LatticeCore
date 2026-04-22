#ifndef error_hpp
#define error_hpp

#include <exception>

namespace lattice {

struct cxx_error {
    cxx_error(const std::exception& e)
    : name(typeid(e).name())
    , msg(e.what())
    {
    }
    
    std::string name;
    std::string msg;
};

}

#endif /* error_hpp */
