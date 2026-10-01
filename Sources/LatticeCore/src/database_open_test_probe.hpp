#pragma once
#include <cstring>

namespace lattice::detail::database_open_test_hooks {
// Private native-test selector. It grants no owner/custody authority and never
// replaces an open result. Production leaves current null. All pointed-to
// strings are owned by the scoped test until this once-only open returns.
struct selector {
    const char* path;
    int flags;
    const char* vfs;
    bool consumed=false;
};
inline thread_local selector* current=nullptr;
inline const char* consume(const char* path,int flags) noexcept {
    auto* selected=current;
    if(!selected||selected->consumed||!selected->path||!selected->vfs||
       flags!=selected->flags||std::strcmp(path,selected->path)!=0)return nullptr;
    selected->consumed=true;
    return selected->vfs;
}
}
