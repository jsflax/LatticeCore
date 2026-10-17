// Hosted disposable-account test-environment observation only. No SDK/product
// descendant join, waitpid/reap, account deletion or authority from source pins.
#include <Security/Security.h>
#include <CoreFoundation/CoreFoundation.h>
#include <libproc.h>
#include <bsm/libbsm.h>
#include <mach/mach.h>
#include <mach/mach_time.h>
#include <sys/event.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <algorithm>
#include <climits>
#include <time.h>
#include <utility>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <set>
#include <stdexcept>
#include <limits>
#include <memory>

namespace {
static_assert(sizeof(audit_token_t)==32,"closed JSON transport must match the actual SDK audit token type");
struct refusal : std::runtime_error {
    const char* domain;const std::int64_t code;const int observed_errno;
    explicit refusal(const char* why,const char* source="source-precondition",std::int64_t result=0,int error=0):
        std::runtime_error(why),domain(source),code(result),observed_errno(error){}
};
void require(bool okay,const char* why){if(!okay)throw refusal(why);}
void posix_result(int result,const char* why){if(result<0)throw refusal(why,"posix-return",result,errno);}
void mach_result(kern_return_t result,const char* why){if(result!=KERN_SUCCESS)throw refusal(why,"mach-return",result);}
void security_result(OSStatus result,const char* why){if(result!=errSecSuccess)throw refusal(why,"security-return",result);}
template<typename T> struct cf {
    T value=nullptr;
    ~cf(){if(value)CFRelease(value);}
    cf()=default;cf(const cf&)=delete;cf& operator=(const cf&)=delete;
    cf(cf&& other)noexcept:value(std::exchange(other.value,nullptr)){}
};
std::string string(CFTypeRef value) {
    require(value&&CFGetTypeID(value)==CFStringGetTypeID(),"native_cf_string_type");
    char buffer[8192]{};
    require(CFStringGetCString(static_cast<CFStringRef>(value),buffer,sizeof buffer,kCFStringEncodingUTF8),"native_cf_string_bound");
    return buffer;
}
std::int64_t integer(CFTypeRef value) {
    require(value&&CFGetTypeID(value)==CFNumberGetTypeID(),"native_cf_integer_type");
    std::int64_t out=0;require(CFNumberGetValue(static_cast<CFNumberRef>(value),kCFNumberSInt64Type,&out),"native_cf_integer_value");
    return out;
}
std::string canonical(const std::string& path) {
    char out[PATH_MAX]{};require(!path.empty()&&path[0]=='/'&&::realpath(path.c_str(),out),"native_absolute_path");
    return out;
}
std::string hex(const void* bytes,std::size_t count) {
    const auto* data=static_cast<const unsigned char*>(bytes);std::string out;out.reserve(count*2);
    for(std::size_t i=0;i<count;++i){out+="0123456789abcdef"[data[i]>>4];out+="0123456789abcdef"[data[i]&15];}return out;
}
std::string quote(const std::string& text) {
    std::string out="\"";for(unsigned char c:text) {
        if(c=='"'||c=='\\'){out+='\\';out+=char(c);}
        else if(c<32){char escaped[7]{};std::snprintf(escaped,sizeof escaped,"\\u%04x",unsigned(c));out+=escaped;}
        else out+=char(c);
    }return out+'"';
}
std::uint64_t decimal(const char* text) {
    const auto size=std::strlen(text);require(size>0&&size<=20&&!(size>1&&text[0]=='0'),"native_decimal_grammar");
    std::uint64_t value=0;const auto read=std::from_chars(text,text+size,value);
    require(read.ec==std::errc{}&&read.ptr==text+size,"native_decimal_range");return value;
}
std::uint64_t now() {
    mach_timebase_info_data_t scale{};require(::mach_timebase_info(&scale)==KERN_SUCCESS&&scale.numer&&scale.denom,"native_mach_clock");
    const __uint128_t value=__uint128_t(::mach_absolute_time())*scale.numer/scale.denom;
    require(value<=std::numeric_limits<std::uint64_t>::max(),"native_mach_clock_overflow");return std::uint64_t(value);
}
struct file {
    int fd=-1;
    struct stat initial{};
    explicit file(const std::string& path) {
        fd=::open(path.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
        require(fd>=0,"native_protected_file_open");
        struct statfs volume{};
        if(::fstat(fd,&initial)||!S_ISREG(initial.st_mode)||initial.st_uid!=0||(initial.st_mode&0022)||
           ::fstatfs(fd,&volume)||!(volume.f_flags&MNT_RDONLY)) {
            const auto held=fd;fd=-1;(void)::close(held);
            throw refusal("native_file_not_protected_readonly_os");
        }
    }
    void finish() {
        struct stat last{};require(fd>=0&&!::fstat(fd,&last)&&initial.st_dev==last.st_dev&&initial.st_ino==last.st_ino&&
            initial.st_size==last.st_size&&initial.st_mode==last.st_mode&&initial.st_uid==last.st_uid&&
            initial.st_mtimespec.tv_sec==last.st_mtimespec.tv_sec&&initial.st_mtimespec.tv_nsec==last.st_mtimespec.tv_nsec,
            "native_protected_file_changed");
        const int held=fd;fd=-1;require(!::close(held),"native_file_close_unproved");
    }
    ~file(){if(fd>=0)::close(fd);}
    file(const file&)=delete;file& operator=(const file&)=delete;
};
cf<CFDictionaryRef> plist(const std::string& path) {
    file owned(path);require(owned.initial.st_size>0&&owned.initial.st_size<=1024*1024,"native_policy_plist_bound");
    std::vector<unsigned char> raw(static_cast<std::size_t>(owned.initial.st_size));std::size_t at=0;
    while(at<raw.size()){const auto n=::read(owned.fd,raw.data()+at,raw.size()-at);require(n>0,"native_policy_plist_read");at+=std::size_t(n);}
    cf<CFDataRef> data;data.value=CFDataCreate(nullptr,raw.data(),CFIndex(raw.size()));require(data.value,"native_policy_data");
    cf<CFPropertyListRef> decoded;decoded.value=CFPropertyListCreateWithData(nullptr,data.value,kCFPropertyListImmutable,nullptr,nullptr);
    require(decoded.value&&CFGetTypeID(decoded.value)==CFDictionaryGetTypeID(),"native_policy_dictionary");
    cf<CFDictionaryRef> result;result.value=static_cast<CFDictionaryRef>(CFRetain(decoded.value));owned.finish();return result;
}
struct policy { const char* descriptor;const char* label;const char* executable;bool application; };
constexpr policy policies[]={
    {"/System/Library/LaunchAgents/com.apple.distnoted.xpc.agent.plist","com.apple.distnoted.xpc.agent","/usr/sbin/distnoted",false},
    {"/System/Library/LaunchAgents/com.apple.lsd.plist","com.apple.lsd","/usr/libexec/lsd",false},
    {"/System/Library/LaunchAgents/com.apple.cfprefsd.xpc.agent.plist","com.apple.cfprefsd.xpc.agent","/usr/sbin/cfprefsd",false},
    {"/System/Library/LaunchAgents/com.apple.trustd.agent.plist","com.apple.trustd.agent","/usr/libexec/trustd",false},
    {"/System/Library/LaunchAgents/com.apple.secd.plist","com.apple.secd","/usr/libexec/secd",false},
    {"/System/Library/LaunchAgents/com.apple.containermanagerd.plist","com.apple.containermanagerd","/usr/libexec/containermanagerd",false},
    {"/System/Library/Frameworks/NetFS.framework/Versions/A/XPCServices/PlugInLibraryService.xpc/Contents/Info.plist",
     "com.apple.filesystems.netfs.PlugInLibraryService",
     "/System/Library/Frameworks/NetFS.framework/Versions/A/XPCServices/PlugInLibraryService.xpc/Contents/MacOS/PlugInLibraryService",true}
};
void policy_matches(const policy& selected) {
    auto root=plist(selected.descriptor);
    if(!selected.application) {
        require(string(CFDictionaryGetValue(root.value,CFSTR("Label")))==selected.label,"native_os_job_label");
        auto value=CFDictionaryGetValue(root.value,CFSTR("Program"));
        if(value)require(canonical(string(value))==canonical(selected.executable),"native_os_job_program");
        else {
            value=CFDictionaryGetValue(root.value,CFSTR("ProgramArguments"));
            require(value&&CFGetTypeID(value)==CFArrayGetTypeID()&&CFArrayGetCount(static_cast<CFArrayRef>(value))>0,"native_os_job_arguments");
            require(canonical(string(CFArrayGetValueAtIndex(static_cast<CFArrayRef>(value),0)))==canonical(selected.executable),"native_os_job_program");
        }
    }else {
        require(string(CFDictionaryGetValue(root.value,CFSTR("CFBundleIdentifier")))==selected.label&&
            string(CFDictionaryGetValue(root.value,CFSTR("CFBundleExecutable")))=="PlugInLibraryService","native_os_xpc_identity");
        auto service=CFDictionaryGetValue(root.value,CFSTR("XPCService"));
        require(service&&CFGetTypeID(service)==CFDictionaryGetTypeID()&&
            string(CFDictionaryGetValue(static_cast<CFDictionaryRef>(service),CFSTR("ServiceType")))=="Application","native_os_xpc_scope");
    }
}
struct process {
    const int pid;const uid_t uid;
    mach_port_t task=MACH_PORT_NULL;
    audit_token_t token{};
    proc_bsdinfo bsd{};
    const policy* selected=nullptr;
    std::string path,identifier,cdhash;
    std::int64_t status=0;
    cf<SecCodeRef> code;
    explicit process(int original,uid_t account):pid(original),uid(account) {
        try {
        mach_result(::task_name_for_pid(::mach_task_self(),pid,&task),"native_task_incarnation_unproved");
        require(task!=MACH_PORT_NULL,"native_task_incarnation_unproved");
        token=read_token();bsd=read_bsd();
        require(audit_token_to_pid(token)==pid&&audit_token_to_euid(token)==uid&&audit_token_to_ruid(token)==uid&&
            audit_token_to_pidversion(token)!=0,"native_audit_uid_or_incarnation");
        char buffer[PROC_PIDPATHINFO_MAXSIZE]{};
        require(::proc_pidpath_audittoken(&token,buffer,sizeof buffer)>0&&std::memchr(buffer,0,sizeof buffer),"native_audit_executable_path");
        path=canonical(buffer);
        for(const auto& item:policies)if(path==canonical(item.executable)){selected=&item;break;}
        require(selected,"native_os_executable_not_permitted");policy_matches(*selected);file image(path);
        cf<CFDataRef> audit;audit.value=CFDataCreate(nullptr,reinterpret_cast<const UInt8*>(&token),sizeof token);
        require(audit.value,"native_dynamic_code_audit_data");
        const void* keys[]={kSecGuestAttributeAudit};const void* values[]={audit.value};
        cf<CFDictionaryRef> attributes;attributes.value=CFDictionaryCreate(nullptr,keys,values,1,&kCFTypeDictionaryKeyCallBacks,&kCFTypeDictionaryValueCallBacks);
        require(attributes.value,"native_dynamic_code_attributes");
        security_result(SecCodeCopyGuestWithAttributes(nullptr,attributes.value,kSecCSDefaultFlags,&code.value),"native_dynamic_code_incarnation");
        require(code.value,"native_dynamic_code_incarnation");
        validate();
        cf<CFDictionaryRef> info;security_result(SecCodeCopySigningInformation(reinterpret_cast<SecStaticCodeRef>(code.value),
            kSecCSDynamicInformation,&info.value),"native_dynamic_signature_information");
        require(info.value,"native_dynamic_signature_information");
        identifier=string(CFDictionaryGetValue(info.value,kSecCodeInfoIdentifier));
        status=integer(CFDictionaryGetValue(info.value,kSecCodeInfoStatus));
        require((status&kSecCodeStatusValid)&&(status&kSecCodeStatusPlatform)&&!(status&kSecCodeStatusDebugged),"native_os_platform_status");
        auto digest=CFDictionaryGetValue(info.value,kSecCodeInfoUnique);
        require(digest&&CFGetTypeID(digest)==CFDataGetTypeID()&&CFDataGetLength(static_cast<CFDataRef>(digest))>=16&&
            CFDataGetLength(static_cast<CFDataRef>(digest))<=64,"native_cdhash_bound");
        cdhash=hex(CFDataGetBytePtr(static_cast<CFDataRef>(digest)),std::size_t(CFDataGetLength(static_cast<CFDataRef>(digest))));
        auto location=CFDictionaryGetValue(info.value,kSecCodeInfoMainExecutable);
        require(location&&CFGetTypeID(location)==CFURLGetTypeID(),"native_code_executable_url");
        cf<CFStringRef> executable;executable.value=CFURLCopyFileSystemPath(static_cast<CFURLRef>(location),kCFURLPOSIXPathStyle);
        require(canonical(string(executable.value))==path,"native_code_path_mismatch");
        current();image.finish();
        }catch(...) {
            const auto held=task;task=MACH_PORT_NULL;
            if(held!=MACH_PORT_NULL)(void)::mach_port_deallocate(::mach_task_self(),held);
            throw;
        }
    }
    audit_token_t read_token()const {
        audit_token_t value{};mach_msg_type_number_t count=TASK_AUDIT_TOKEN_COUNT;
        require(task!=MACH_PORT_NULL,"native_task_audit_token");
        mach_result(::task_info(task,TASK_AUDIT_TOKEN,reinterpret_cast<task_info_t>(&value),&count),"native_task_audit_token");
        require(count==TASK_AUDIT_TOKEN_COUNT,"native_task_audit_token");return value;
    }
    proc_bsdinfo read_bsd()const {
        proc_bsdinfo value{};require(::proc_pidinfo(pid,PROC_PIDTBSDINFO,0,&value,sizeof value)==sizeof value&&
            value.pbi_pid==unsigned(pid)&&value.pbi_uid==uid&&value.pbi_ruid==uid&&value.pbi_svuid==uid,"native_current_uid");return value;
    }
    void validate()const {
        cf<SecRequirementRef> requirement;
        security_result(SecRequirementCreateWithString(CFSTR("anchor apple"),kSecCSDefaultFlags,&requirement.value),"native_os_signature_requirement");
        require(requirement.value,"native_os_signature_requirement");
        security_result(SecCodeCheckValidity(code.value,kSecCSDefaultFlags,requirement.value),"native_os_dynamic_signature_validity");
    }
    void current()const {
        const auto again=read_token();const auto live=read_bsd();char executable[PROC_PIDPATHINFO_MAXSIZE]{};
        require(!std::memcmp(&again,&token,sizeof token)&&live.pbi_start_tvsec==bsd.pbi_start_tvsec&&
            live.pbi_start_tvusec==bsd.pbi_start_tvusec&&::proc_pidpath_audittoken(const_cast<audit_token_t*>(&token),executable,sizeof executable)>0&&
            std::memchr(executable,0,sizeof executable)&&canonical(executable)==path,"native_incarnation_or_image_changed");
        validate();
        // Status is a documented snapshot. Reobserve it at EACH currentness
        // boundary instead of granting a later job/signal from its old value.
        cf<CFDictionaryRef> info;
        security_result(SecCodeCopySigningInformation(code.value,kSecCSDynamicInformation,&info.value),"native_dynamic_signature_reobservation");
        require(info.value,"native_dynamic_signature_reobservation");
        const auto current_status=integer(CFDictionaryGetValue(info.value,kSecCodeInfoStatus));
        require((current_status&kSecCodeStatusValid)&&(current_status&kSecCodeStatusPlatform)&&
            !(current_status&kSecCodeStatusDebugged),"native_os_platform_status_changed");
        const auto digest=CFDictionaryGetValue(info.value,kSecCodeInfoUnique);
        require(digest&&CFGetTypeID(digest)==CFDataGetTypeID()&&CFDataGetLength(static_cast<CFDataRef>(digest))>=16&&
            CFDataGetLength(static_cast<CFDataRef>(digest))<=64,"native_current_cdhash_bound");
        require(hex(CFDataGetBytePtr(static_cast<CFDataRef>(digest)),std::size_t(CFDataGetLength(static_cast<CFDataRef>(digest))))==cdhash&&
            string(CFDictionaryGetValue(info.value,kSecCodeInfoIdentifier))==identifier,"native_current_signature_changed");
    }
    void finish(){const auto held=task;task=MACH_PORT_NULL;mach_result(::mach_port_deallocate(::mach_task_self(),held),"native_task_right_close");}
    ~process(){if(task!=MACH_PORT_NULL)::mach_port_deallocate(::mach_task_self(),task);}
};
std::string observed(process& value,bool ended,const std::vector<int>& signals) {
    std::string out="{\"pid\":"+std::to_string(value.pid)+",\"uid\":"+std::to_string(value.uid)+
        ",\"auditToken\":"+quote(hex(&value.token,sizeof value.token))+",\"pidVersion\":"+std::to_string(audit_token_to_pidversion(value.token))+
        ",\"startSeconds\":"+std::to_string(value.bsd.pbi_start_tvsec)+",\"startMicroseconds\":"+std::to_string(value.bsd.pbi_start_tvusec)+
        ",\"executable\":"+quote(value.path)+",\"identifier\":"+quote(value.identifier)+",\"cdhash\":"+quote(value.cdhash)+
        ",\"codeStatus\":"+std::to_string(value.status)+",\"descriptor\":"+quote(value.selected->descriptor)+
        ",\"jobLabel\":"+quote(value.selected->label)+",\"kind\":"+quote(value.selected->application?"application-xpc":"background-agent")+
        ",\"exitRegistered\":"+(ended?"true":"false")+",\"registeredExitObserved\":"+(ended?"true":"false")+",\"signals\":[";
    for(std::size_t i=0;i<signals.size();++i){if(i)out+=',';out+=std::to_string(signals[i]);}
    return out+"],\"firstFailure\":null}";
}
void end_application(process& value,std::uint64_t deadline,std::vector<int>& signals) {
    require(value.selected->application&&now()<deadline,"native_not_preproduct_application_scope");
    const int queue=::kqueue();posix_result(queue,"native_exit_queue");
    bool closed=false;
    try {
        posix_result(::fcntl(queue,F_SETFD,FD_CLOEXEC),"native_exit_queue_cloexec");
        struct kevent change{};EV_SET(&change,uintptr_t(value.pid),EVFILT_PROC,EV_ADD|EV_ENABLE|EV_ONESHOT,NOTE_EXIT|NOTE_EXEC,0,&value);
        const auto registration=::kevent(queue,&change,1,nullptr,0,nullptr);
        posix_result(registration,"native_exit_registration");require(registration==0,"native_exit_registration_count");
        value.current(); // The actual registered PID must STILL be this incarnation before any signal.
        bool exited=false;unsigned sent=0;auto term_end=deadline;
        while(now()<deadline&&!exited) {
            struct kevent event{};timespec zero{};const auto count=::kevent(queue,nullptr,0,&event,1,&zero);
            posix_result(count,"native_exit_observation");
            if(count) {
                require(count==1&&event.ident==uintptr_t(value.pid)&&event.udata==&value&&event.filter==EVFILT_PROC&&
                    !(event.flags&EV_ERROR)&&!(event.fflags&NOTE_EXEC)&&(event.fflags&NOTE_EXIT),"native_exit_event_identity");
                exited=true;break;
            }
            if(!sent|| (sent==1&&now()>=term_end)) {
                const int number=sent?SIGKILL:SIGTERM;require(now()<deadline,"native_original_deadline");
                value.current();
                require(now()<deadline,"native_original_deadline");
                // Kernel audit-token addressing closes recycled-PID races.
                // An error, including ESRCH, is never turned into exit proof.
                const auto result=::proc_signal_with_audittoken(&value.token,number);
                if(result!=0)throw refusal("native_audit_token_signal_failed","libproc-return",result,errno);
                signals.push_back(number);++sent;
                const auto at=now();term_end=at>deadline-std::min<std::uint64_t>(deadline,3000000000ull)?
                    deadline:std::min(deadline,at+3000000000ull);
            }
            timespec pause{0,10000000};while(::nanosleep(&pause,&pause)&&errno==EINTR){require(now()<deadline,"native_original_deadline");}
        }
        require(exited,"native_registered_exit_deadline");
        closed=true; // First close attempt cannot be retried after an ambiguous return.
        posix_result(::close(queue),"native_exit_queue_close");
    }catch(...) {if(!closed)::close(queue);throw;}
}
} // namespace
int main(int argc,char** argv) {
    std::vector<std::string> rows;
    try {
        require(argc>=5&&argc<=68&&::geteuid()==0,"native_observer_root_grammar");
        const auto gate=::getenv("ENGRAM_MANAGED_HOSTED_OS_CUSTODY");
        require(gate&&std::string(gate)=="1","native_observer_hosted_custody_gate");
        const std::string mode=argv[1];require(mode=="observe"||mode=="preproduct-census","native_observer_mode");
        const auto uid=decimal(argv[2]),deadline=decimal(argv[3]);require(uid>=60000&&uid<80000&&now()<deadline,"native_account_or_deadline");
        std::set<int> pids;
        std::vector<std::unique_ptr<process>> retained;
        retained.reserve(std::size_t(argc-4));
        for(int at=4;at<argc;++at) {
            const auto number=decimal(argv[at]);require(number>1&&number<=INT_MAX&&pids.insert(int(number)).second,"native_pid_grammar");
            retained.push_back(std::make_unique<process>(int(number),uid_t(uid)));
        }
        // Validate the WHOLE actual census before the first ending effect.
        // Each retained process is revalidated again before its own action.
        for(const auto& owned:retained) {
            auto& original=*owned;std::vector<int> signals;bool ended=false;
            if(mode=="preproduct-census"&&original.selected->application){end_application(original,deadline,signals);ended=true;}
            else original.current();
            const auto row=observed(original,ended,signals);original.finish();require(now()<deadline,"native_original_deadline");rows.push_back(row);
        }
        std::printf("{\"schemaVersion\":1,\"mode\":%s,\"qualified\":true,\"productDescendantJoin\":false,\"processes\":[",quote(mode).c_str());
        for(std::size_t i=0;i<rows.size();++i)std::printf("%s%s",i?",":"",rows[i].c_str());
        std::puts("],\"firstFailure\":null}");return 0;
    }catch(const std::exception& failure) {
        const auto* first=dynamic_cast<const refusal*>(&failure);
        std::printf("{\"schemaVersion\":1,\"qualified\":false,\"productDescendantJoin\":false,\"firstFailure\":%s,\"firstErrorDomain\":%s,\"firstErrorCode\":%lld,\"observedErrno\":%d,\"completedProcesses\":[",
            quote(failure.what()).c_str(),quote(first?first->domain:"cpp-exception").c_str(),
            static_cast<long long>(first?first->code:0),first?first->observed_errno:0);
        for(std::size_t i=0;i<rows.size();++i)std::printf("%s%s",i?",":"",rows[i].c_str());
        std::puts("]}");return 1;
    }
}
