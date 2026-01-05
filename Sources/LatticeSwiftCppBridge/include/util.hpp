#ifndef util_hpp
#define util_hpp

template <typename Fn>
struct _defer {
    Fn fn;
    _defer(Fn fn) : fn(fn) {}
    ~_defer() { fn(); }
};

#define CONCAT(a, b) CONCAT_INNER(a, b)
#define CONCAT_INNER(a, b) a ## b


#define defer(...) _defer CONCAT(scope_exit_, __LINE__)(__VA_ARGS__)


#endif
