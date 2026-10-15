#pragma once
#include <string_view>

namespace lattice::detail {
// SQLite's built-in NOCASE folds only ASCII A-Z. Callers reject embedded NUL
// before admission; this comparator does not grant identity or rewrite payloads.
struct ordinary_v2_nocase_less {
    using is_transparent=void;
    bool operator()(std::string_view left,std::string_view right)const noexcept {
        const auto fold=[](unsigned char c)->unsigned char{return c>='A'&&c<='Z'?static_cast<unsigned char>(c+('a'-'A')):c;};
        const auto size=left.size()<right.size()?left.size():right.size();
        for(std::size_t n=0;n<size;++n){const auto a=fold(static_cast<unsigned char>(left[n])),b=fold(static_cast<unsigned char>(right[n]));if(a!=b)return a<b;}
        return left.size()<right.size();
    }
};
}
