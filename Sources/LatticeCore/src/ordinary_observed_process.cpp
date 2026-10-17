#include "ordinary_observed_process.hpp"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <thread>
#include <utility>

#if defined(__APPLE__)
#include <TargetConditionals.h>
#endif
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
#include <dlfcn.h>
#include <fcntl.h>
#include <libproc.h>
#include <mach/mach.h>
#include <signal.h>
#include <sys/event.h>
#include <sys/proc.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace lattice::detail::ordinary_launch {
namespace {
[[noreturn]] void refuse(error_code code) { throw error(code, "External process retirement unproved"); }
void bounded(deadline end) {
    if (std::chrono::steady_clock::now() >= end) refuse(error_code::timed_out);
}
}

struct observed_process::implementation {
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
    const pid_t owner = ::getpid();
    const std::thread::id thread = std::this_thread::get_id();
    pid_t target = -1;
    int queue = -1, executable = -1;
    mach_port_t task_name = MACH_PORT_NULL;
    audit_token_t token{};
    struct stat image{};
    bool disqualified = false, exited = false;
    using signal_function = int (*)(audit_token_t*, int);
    signal_function signal_exact = nullptr;
    ~implementation() {
        if (queue >= 0) ::close(queue);
        if (executable >= 0) ::close(executable);
        if (::getpid() == owner && task_name != MACH_PORT_NULL)
            ::mach_port_deallocate(mach_task_self(), task_name);
        // Destruction is observation cancellation, never a positive exit or
        // release of caller-owned cohort debt. No destructor signal/wait.
    }
    void check(deadline end) const {
        bounded(end);
        if (::getpid() != owner) refuse(error_code::inherited_use);
        if (std::this_thread::get_id() != thread) refuse(error_code::wrong_thread);
    }
    audit_token_t read_token_from(mach_port_t port, deadline end) const {
        check(end);
        audit_token_t actual{};
        mach_msg_type_number_t count = TASK_AUDIT_TOKEN_COUNT;
        if (::task_info(port, TASK_AUDIT_TOKEN, reinterpret_cast<task_info_t>(&actual), &count)
                != KERN_SUCCESS || count != TASK_AUDIT_TOKEN_COUNT)
            refuse(error_code::identity_unproved);
        int pid = -1;
        if (::pid_for_task(port, &pid) != KERN_SUCCESS || pid != target)
            refuse(error_code::identity_unproved);
        bounded(end);
        return actual;
    }
    audit_token_t read_token(deadline end) const { return read_token_from(task_name, end); }
    void same_token(deadline end) const {
        const auto current = read_token(end);
        if (std::memcmp(&current, &token, sizeof(token))) refuse(error_code::identity_unproved);
        // Re-resolve the numeric lookup used by libproc/kqueue as well as the
        // retained task port. A dead retained port cannot bless a reused PID.
        struct current_port {
            mach_port_t value = MACH_PORT_NULL;
            ~current_port() { if (value != MACH_PORT_NULL) ::mach_port_deallocate(mach_task_self(), value); }
        } fresh;
        if (::task_name_for_pid(mach_task_self(), target, &fresh.value) != KERN_SUCCESS ||
            fresh.value == MACH_PORT_NULL) refuse(error_code::identity_unproved);
        const auto rebound = read_token_from(fresh.value, end);
        if (std::memcmp(&rebound, &token, sizeof(token))) refuse(error_code::identity_unproved);
    }
    void same_image(deadline end) const {
        check(end);
        struct stat current{};
        if (::fstat(executable, &current) || !S_ISREG(current.st_mode) ||
            current.st_dev != image.st_dev || current.st_ino != image.st_ino ||
            current.st_uid != image.st_uid || current.st_mode != image.st_mode ||
            current.st_nlink != image.st_nlink || current.st_size != image.st_size ||
            current.st_mtimespec.tv_sec != image.st_mtimespec.tv_sec ||
            current.st_mtimespec.tv_nsec != image.st_mtimespec.tv_nsec ||
            current.st_ctimespec.tv_sec != image.st_ctimespec.tv_sec ||
            current.st_ctimespec.tv_nsec != image.st_ctimespec.tv_nsec)
            refuse(error_code::identity_unproved);
    }
    void mapped_executable(deadline end) const {
        same_token(end);
        proc_bsdinfo bsd{};
        if (::proc_pidinfo(target, PROC_PIDTBSDINFO, 0, &bsd, sizeof(bsd)) != sizeof(bsd) ||
            bsd.pbi_uid != ::geteuid() || bsd.pbi_ruid != ::getuid())
            refuse(error_code::identity_unproved);
        char loaded_path[PROC_PIDPATHINFO_MAXSIZE]{};
        auto token_copy = token;
        if (::proc_pidpath_audittoken(&token_copy, loaded_path, sizeof(loaded_path)) <= 0)
            refuse(error_code::identity_unproved);
        char retained_path[PATH_MAX]{};
        if (::fcntl(executable, F_GETPATH, retained_path) < 0 ||
            !std::memchr(loaded_path, 0, sizeof(loaded_path)) ||
            !std::memchr(retained_path, 0, sizeof(retained_path)) ||
            std::strcmp(loaded_path, retained_path)) refuse(error_code::identity_unproved);
        // Path comparison alone is insufficient: require an actual executable
        // mapping of the retained vnode. Every PID-keyed inspection is bracketed
        // by the held task's unchanged token; permission/errors refuse.
        std::uint64_t address = 0;
        bool found = false;
        for (std::size_t i = 0; i != 4096; ++i) {
            check(end);
            proc_regionwithpathinfo region{};
            if (::proc_pidinfo(target, PROC_PIDREGIONPATHINFO, address, &region, sizeof(region)) != sizeof(region))
                refuse(error_code::identity_unproved);
            const auto& mapped = region.prp_vip.vip_vi.vi_stat;
            if ((region.prp_prinfo.pri_protection & VM_PROT_EXECUTE) &&
                mapped.vst_ino == static_cast<std::uint64_t>(image.st_ino) &&
                mapped.vst_dev == static_cast<std::uint32_t>(image.st_dev)) {
                found = true;
                break;
            }
            const auto start = region.prp_prinfo.pri_address, size = region.prp_prinfo.pri_size;
            if (!size || start < address || size > std::numeric_limits<std::uint64_t>::max() - start)
                refuse(error_code::identity_unproved);
            address = start + size;
        }
        if (!found) refuse(error_code::identity_unproved);
        same_token(end);
        same_image(end);
    }
    observed_exit result(deadline end) const {
        check(end);
        if (!exited || disqualified) refuse(error_code::terminal_unproved);
        observed_exit value;
        value.pid = target;
        static_assert(sizeof(token) == 32);
        std::memcpy(value.audit_token.data(), &token, sizeof(token));
        value.executable_device = static_cast<std::uint64_t>(image.st_dev);
        value.executable_inode = static_cast<std::uint64_t>(image.st_ino);
        return value;
    }
#endif
};

observed_process::observed_process(std::unique_ptr<implementation> value) : impl_(std::move(value)) {}
observed_process::~observed_process() = default;
observed_process::observed_process(observed_process&&) noexcept = default;
observed_process& observed_process::operator=(observed_process&&) noexcept = default;

observed_process observed_process::attach(std::int64_t pid, int retained_executable, deadline end) {
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
    bounded(end);
    if (pid <= 1 || pid > std::numeric_limits<pid_t>::max() || pid == ::getpid() || retained_executable < 0)
        refuse(error_code::invalid_specification);
    auto value = std::make_unique<implementation>();
    value->target = static_cast<pid_t>(pid);
    value->executable = ::fcntl(retained_executable, F_DUPFD_CLOEXEC, 3);
    if (value->executable < 0 || ::fstat(value->executable, &value->image) ||
        !S_ISREG(value->image.st_mode) || (value->image.st_uid != ::geteuid() && value->image.st_uid != 0) ||
        (value->image.st_mode & 0022) || !(value->image.st_mode & 0111) || value->image.st_nlink != 1)
        refuse(error_code::identity_unproved);
    // Private libproc availability is runtime-qualified. Missing symbols have
    // no compatibility path that changes the signal target to a numeric PID.
    value->signal_exact = reinterpret_cast<implementation::signal_function>(
        ::dlsym(RTLD_DEFAULT, "proc_signal_with_audittoken"));
    if (!value->signal_exact) refuse(error_code::unavailable);
    if (::task_name_for_pid(mach_task_self(), value->target, &value->task_name) != KERN_SUCCESS ||
        value->task_name == MACH_PORT_NULL) refuse(error_code::identity_unproved);
    value->token = value->read_token(end);
    value->mapped_executable(end);
    if (observed_test_hooks::observe) observed_test_hooks::observe(observed_test_hooks::boundary::before_registration);
    value->check(end);
    value->queue = ::kqueue();
    if (value->queue < 0 || ::fcntl(value->queue, F_SETFD, FD_CLOEXEC)) refuse(error_code::io);
    // The dedicated supervisor excludes concurrent fork/exec during this small
    // Darwin kqueue+close-on-exec construction interval, as for owned_child.
    struct kevent change{}, receipt{};
    EV_SET(&change, static_cast<uintptr_t>(pid), EVFILT_PROC, EV_ADD | EV_ENABLE | EV_CLEAR | EV_RECEIPT,
           NOTE_EXIT | NOTE_EXEC | NOTE_FORK, 0, value.get());
    timespec immediate{};
    value->check(end);
    if (::kevent(value->queue, &change, 1, &receipt, 1, &immediate) != 1 ||
        receipt.ident != static_cast<uintptr_t>(pid) || receipt.filter != EVFILT_PROC ||
        !(receipt.flags & EV_ERROR) || receipt.data != 0)
        refuse(error_code::identity_unproved);
    if (observed_test_hooks::observe) observed_test_hooks::observe(observed_test_hooks::boundary::after_registration);
    value->same_token(end);
    value->mapped_executable(end);
    return observed_process(std::move(value));
#else
    (void)pid; (void)retained_executable; (void)end;
    refuse(error_code::unavailable);
#endif
}

observed_incarnation observed_process::incarnation(deadline end) const {
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
    if(!impl_)refuse(error_code::invalid_specification);
    impl_->check(end);if(impl_->exited||impl_->disqualified)refuse(error_code::terminal_unproved);
    impl_->same_token(end);impl_->same_image(end);
    observed_incarnation value;value.pid=impl_->target;
    std::memcpy(value.audit_token.data(),&impl_->token,sizeof(impl_->token));
    value.executable_device=static_cast<std::uint64_t>(impl_->image.st_dev);
    value.executable_inode=static_cast<std::uint64_t>(impl_->image.st_ino);
    return value;
#else
    (void)end;refuse(error_code::unavailable);
#endif
}

std::optional<observed_process_files> observed_process::inspect_files(std::int64_t pid,deadline end){
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
    bounded(end);
    if(pid<=1||pid>std::numeric_limits<pid_t>::max()||pid==::getpid())refuse(error_code::invalid_specification);
    // Include exited-list entries: arg=0 can return ESRCH while proc_exit is
    // still draining AIO/files. Only absence from this wider lookup or actual
    // SZOMB is terminal. A missing task port is never completion evidence.
    proc_bsdinfo bsd{};errno=0;
    const auto looked_up=::proc_pidinfo(static_cast<pid_t>(pid),PROC_PIDTBSDINFO,1,&bsd,sizeof(bsd));
    const int lookup_error=errno;bounded(end);
    if(looked_up==0&&lookup_error==ESRCH)return std::nullopt;
    if(looked_up!=sizeof(bsd)||bsd.pbi_uid!=::geteuid()||bsd.pbi_ruid!=::getuid())refuse(error_code::identity_unproved);
    // XNU publishes SZOMB only after _aio_exit, fdt_invalidate, task detach,
    // and NOTE_EXIT. The parent need not have reaped the zombie yet.
    if(bsd.pbi_status==SZOMB)return std::nullopt;
    implementation binding;binding.target=static_cast<pid_t>(pid);
    if(::task_name_for_pid(mach_task_self(),binding.target,&binding.task_name)!=KERN_SUCCESS||binding.task_name==MACH_PORT_NULL)
        refuse(error_code::identity_unproved);
    binding.token=binding.read_token(end);binding.same_token(end);
    proc_bsdinfo exact_bsd{};
    if(::proc_pidinfo(binding.target,PROC_PIDTBSDINFO,0,&exact_bsd,sizeof(exact_bsd))!=sizeof(exact_bsd)||
       exact_bsd.pbi_uid!=::geteuid()||exact_bsd.pbi_ruid!=::getuid())refuse(error_code::identity_unproved);
    binding.same_token(end);
    observed_process_files value;value.pid=pid;value.parent_pid=exact_bsd.pbi_ppid;
    std::memcpy(value.audit_token.data(),&binding.token,sizeof(binding.token));
    std::array<proc_fdinfo,4097> descriptors{};
    errno=0;const auto bytes=::proc_pidinfo(binding.target,PROC_PIDLISTFDS,0,descriptors.data(),sizeof(descriptors));
    const int descriptor_error=errno;
    if(bytes<0||(bytes==0&&descriptor_error!=0)||bytes%sizeof(proc_fdinfo)||std::size_t(bytes)>=sizeof(descriptors))refuse(error_code::identity_unproved);
    binding.same_token(end);
    for(std::size_t n=0;n<std::size_t(bytes)/sizeof(proc_fdinfo);++n){
        binding.same_token(end);const auto& descriptor=descriptors[n];
        if(descriptor.proc_fdtype!=PROX_FDTYPE_VNODE)continue;
        if(descriptor.proc_fd<0)refuse(error_code::identity_unproved);
        vnode_fdinfo file{};
        if(::proc_pidfdinfo(binding.target,static_cast<int>(descriptor.proc_fd),PROC_PIDFDVNODEINFO,&file,sizeof(file))!=sizeof(file))
            refuse(error_code::identity_unproved); // FD reuse/close is not silently empty.
        binding.same_token(end);
        value.files.push_back({std::uint32_t(file.pvi.vi_stat.vst_dev),file.pvi.vi_stat.vst_ino,
            static_cast<std::uint32_t>(descriptor.proc_fd),file.pfi.fi_openflags,bool(file.pfi.fi_status&PROC_FP_CLEXEC)});
    }
    binding.same_token(end);return value;
#else
    (void)pid;(void)end;refuse(error_code::unavailable);
#endif
}

std::optional<observed_process> observed_process::reobserve(const observed_incarnation& expected,int image,deadline end){
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
    bounded(end);struct stat actual{};
    if(image<0||::fstat(image,&actual)||!S_ISREG(actual.st_mode)||
       std::uint64_t(actual.st_dev)!=expected.executable_device||std::uint64_t(actual.st_ino)!=expected.executable_inode)
        refuse(error_code::identity_unproved);
    if(expected.pid==::getpid()){
        audit_token_t own{};mach_msg_type_number_t count=TASK_AUDIT_TOKEN_COUNT;
        if(::task_info(mach_task_self(),TASK_AUDIT_TOKEN,reinterpret_cast<task_info_t>(&own),&count)!=KERN_SUCCESS||count!=TASK_AUDIT_TOKEN_COUNT)
            refuse(error_code::identity_unproved);
        bounded(end);
        if(std::memcmp(&own,expected.audit_token.data(),sizeof(own)))return std::nullopt;
        refuse(error_code::identity_unproved); // Never target this controller.
    }
    const auto current=inspect_files(expected.pid,end);
    if(!current||current->audit_token!=expected.audit_token)return std::nullopt;
    auto retained=attach(expected.pid,image,end);
    if(retained.incarnation(end).audit_token!=expected.audit_token)refuse(error_code::identity_unproved);
    return std::optional<observed_process>(std::move(retained));
#else
    (void)expected;(void)image;(void)end;refuse(error_code::unavailable);
#endif
}

void observed_process::request_termination(deadline end) {
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
    if (!impl_) refuse(error_code::invalid_specification);
    impl_->check(end);
    if (impl_->exited || impl_->disqualified) refuse(error_code::terminal_unproved);
    impl_->same_image(end);
    impl_->same_token(end);
    auto token = impl_->token;
    if (impl_->signal_exact(&token, SIGTERM) != 0) refuse(error_code::terminal_unproved);
    // No post-signal PID lookup or success inference. Only the retained knote
    // can establish the actual selected incarnation's subsequent exit.
    impl_->check(end);
#else
    (void)end; refuse(error_code::unavailable);
#endif
}

observed_exit observed_process::wait_for_exit(deadline end) {
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
    if (!impl_) refuse(error_code::invalid_specification);
    impl_->check(end);
    if (impl_->disqualified) refuse(error_code::identity_unproved);
    while (!impl_->exited) {
        impl_->check(end);
        const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(end - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) refuse(error_code::timed_out);
        timespec timeout{static_cast<time_t>(remaining.count() / 1000000000),
                         static_cast<long>(remaining.count() % 1000000000)};
        struct kevent event{};
        const int count = ::kevent(impl_->queue, nullptr, 0, &event, 1, &timeout);
        const int saved_errno = errno;
        impl_->check(end); // A delayed wake never fabricates an on-time result.
        if (count < 0 && saved_errno == EINTR) continue;
        if (count < 0) refuse(error_code::io);
        if (!count) continue;
        if (event.ident != static_cast<uintptr_t>(impl_->target) || event.filter != EVFILT_PROC ||
            event.udata != impl_.get() || (event.flags & EV_ERROR)) refuse(error_code::identity_unproved);
        if (event.fflags & (NOTE_EXEC | NOTE_FORK)) {
            impl_->disqualified = true;
            refuse(error_code::identity_unproved);
        }
        if (!(event.fflags & NOTE_EXIT)) refuse(error_code::terminal_unproved);
        impl_->exited = true;
    }
    return impl_->result(end);
#else
    (void)end; refuse(error_code::unavailable);
#endif
}
} // namespace lattice::detail::ordinary_launch
