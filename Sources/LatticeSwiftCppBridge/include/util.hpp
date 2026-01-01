#ifndef util_hpp
#define util_hpp

template <typename Fn>
struct _defer {
    Fn fn;
    _defer(Fn fn) : fn(fn) {}
    ~_defer() { fn(); }
};

#define defer(fn, ...) _defer _scope_exit_##__LINE__(fn, __VA_ARGS__);


#endif
