#pragma once
#include <lattice/db.hpp>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <string_view>
#if defined(__APPLE__) && (!defined(_LIBCPP_AVAILABILITY_HAS_FROM_CHARS_FLOATING_POINT) || !_LIBCPP_AVAILABILITY_HAS_FROM_CHARS_FLOATING_POINT)
#include <bit>
#include <cerrno>
#include <cfenv>
#include <cstdlib>
#include <limits>
#include <xlocale.h>
#endif

namespace lattice::detail {
// Required known originals preserve their exact timestamp text separately.
// This is only deterministic finite projection into AuditLog's REAL column.
// No current clock, process locale, partial-prefix parse or implicit timezone.
inline double normalize_required_ordinary_timestamp(std::string_view text) {
    const auto fail=[]()->void {throw db_error("ordinary required timestamp is not a complete finite numeric or zoned ISO value");};
    if(text.empty()||text.size()>64){fail();return 0;}
    const auto digit=[](char c){return c>='0'&&c<='9';};
    if(text.size()<10||text[4]!='-'||text[7]!='-') {
        // Complete decimal, optionally signed negative and exponent-scaled.
        // Leading/trailing whitespace, '+' prefix, NaN and infinity refuse.
        size_t at=text.front()=='-'?1:0;const auto integral=at;
        while(at<text.size()&&digit(text[at]))++at;
        if(at==integral){fail();return 0;}
        if(at<text.size()&&text[at]=='.'){const auto first=++at;while(at<text.size()&&digit(text[at]))++at;if(at==first){fail();return 0;}}
        if(at<text.size()&&(text[at]=='e'||text[at]=='E')){++at;if(at<text.size()&&(text[at]=='+'||text[at]=='-'))++at;
            const auto first=at;while(at<text.size()&&digit(text[at]))++at;if(at==first){fail();return 0;}}
        if(at!=text.size()){fail();return 0;}
#if defined(__APPLE__) && (!defined(_LIBCPP_AVAILABILITY_HAS_FROM_CHARS_FLOATING_POINT) || !_LIBCPP_AVAILABILITY_HAS_FROM_CHARS_FLOATING_POINT)
        // libc++ floating from_chars is strictly unavailable to older Apple
        // deployment targets. The SDK's immutable C locale avoids ambient
        // locale reads, global mutation and per-row locale allocation.
        char buffer[65]{};bool mantissa=true,nonzero_mantissa=false;
        for(size_t n=0;n<text.size();++n){buffer[n]=text[n];
            if(text[n]=='e'||text[n]=='E')mantissa=false;
            if(mantissa&&text[n]>='1'&&text[n]<='9')nonzero_mantissa=true;}
        struct restore_errno {int value;~restore_errno(){errno=value;}} errno_owner{errno};
        std::fenv_t saved{};
        if(std::fegetenv(&saved)!=0){fail();return 0;}
        // feholdexcept alone retains nonstandard denormal/flush controls.
        // Convert and classify in the default, non-stop IEEE environment.
        if(std::fesetenv(FE_DFL_ENV)!=0||std::fesetround(FE_TONEAREST)!=0){
            (void)std::fesetenv(&saved);fail();return 0;}
        errno=0;char* end=nullptr;const double result=strtod_l(buffer,&end,_c_locale);
        const int conversion_errno=errno;
        // ERANGE may accompany a representable nonzero subnormal. Only a
        // nonzero decimal rounded to zero is underflow outside double's range;
        // an all-zero mantissa retains its sign even with a huge exponent.
        // Bit classification cannot move a floating comparison into the
        // restored caller's flush/trapping environment under optimization.
        static_assert(sizeof(double)==sizeof(uint64_t)&&std::numeric_limits<double>::is_iec559
                &&std::numeric_limits<double>::digits==53&&std::numeric_limits<double>::max_exponent==1024);
        const uint64_t magnitude=std::bit_cast<uint64_t>(result)&UINT64_C(0x7fffffffffffffff);
        const bool invalid=end!=buffer+text.size()||magnitude>=UINT64_C(0x7ff0000000000000)
                ||(conversion_errno!=0&&conversion_errno!=ERANGE)
                ||(magnitude==0&&nonzero_mantissa);
        const int restored=std::fesetenv(&saved);
        if(restored!=0||invalid){fail();return 0;}
        return result;
#else
        double result=0;const auto parsed=std::from_chars(text.data(),text.data()+text.size(),result,std::chars_format::general);
        if(parsed.ec!=std::errc{}||parsed.ptr!=text.data()+text.size()||!std::isfinite(result)){fail();return 0;}
        return result;
#endif
    }
    if(text.size()<20||text[10]!='T'||text[13]!=':'||text[16]!=':'){fail();return 0;}
    const auto number=[&](size_t at,size_t count){int value=0;for(size_t n=0;n<count;++n){if(!digit(text[at+n])){fail();return 0;}value=value*10+(text[at+n]-'0');}return value;};
    const int year=number(0,4),month=number(5,2),day=number(8,2),hour=number(11,2),minute=number(14,2),second=number(17,2);
    const bool leap=year%4==0&&(year%100!=0||year%400==0);constexpr int days[]{31,28,31,30,31,30,31,31,30,31,30,31};
    if(year<1||month<1||month>12||day<1||day>days[month-1]+(month==2&&leap)||hour>23||minute>59||second>59){fail();return 0;}
    size_t at=19;uint64_t fraction=0,scale=1;
    if(at<text.size()&&text[at]=='.'){const auto first=++at;while(at<text.size()&&digit(text[at])){
        if(at-first==9){fail();return 0;}fraction=fraction*10+static_cast<unsigned>(text[at]-'0');scale*=10;++at;}
        if(at==first){fail();return 0;}}
    int offset=0;
    if(at<text.size()&&text[at]=='Z'){++at;}
    else if(at<text.size()&&(text[at]=='+'||text[at]=='-')){
        const int sign=text[at]=='+'?1:-1;
        if(text.size()-at!=6||text[at+3]!=':'){fail();return 0;}
        const int hours=number(at+1,2),minutes=number(at+4,2);
        if(hours>23||minutes>59){fail();return 0;}offset=sign*(hours*3600+minutes*60);at+=6;
    } else {fail();return 0;}
    if(at!=text.size()){fail();return 0;}
    // Gregorian civil date to days since 1970-01-01, using integer arithmetic.
    const int y=year-(month<=2),era=y/400;const unsigned yoe=static_cast<unsigned>(y-era*400);
    const unsigned adjusted=static_cast<unsigned>(month+(month>2?-3:9));
    const unsigned doy=(153*adjusted+2)/5+static_cast<unsigned>(day)-1;
    const unsigned doe=yoe*365+yoe/4-yoe/100+doy;
    const int64_t day_number=int64_t(era)*146097+doe-719468;
    const int64_t seconds=day_number*86400+hour*3600+minute*60+second-offset;
    const double result=static_cast<double>(seconds)+static_cast<double>(fraction)/static_cast<double>(scale);
    if(!std::isfinite(result)){fail();return 0;}return result;
}
}
