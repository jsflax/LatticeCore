// Source-private accounting observations, never source/install authority.
// Every mutating turn remains inside the existing real native owned WRITE.
namespace {
enum class history_capacity_class : size_t { stage,page,receive,key,snapshot,record,accepted_cut,accepted_row };
uint64_t history_capacity_creator()noexcept;
struct history_capacity_witness {
    const uint64_t creator=history_capacity_creator();
    sqlite3_int64 changes=0;int64_t data_version=0,schema_version=0;uint64_t epoch=0;
    uint64_t device=0,inode=0;std::array<char,64> seal{};
};
// Explicit fixed retained payload + new retirement-cell header/epoch/PID fields. No dynamic
// vector/string backing store, allocator/control-block/RSS claim is inferred.
constexpr size_t history_capacity_retained_payload=sizeof(history_capacity_witness)+sizeof(std::shared_ptr<void>)+sizeof(std::atomic<uint64_t>)+sizeof(uint64_t);
uint64_t history_capacity_cost(history_capacity_class,const values&);
struct history_capacity_turn {
    database& writer;ordinary_history_limits limits;values prior,next;
    sqlite3_int64 changes=0;int64_t data_version=0,schema_version=0;uint64_t epoch=0;
    uint64_t mutations=0;std::string device,inode;bool finished=false;
    ordinary_launch::deadline end;
    explicit history_capacity_turn(database&,const ordinary_history_limits&,budget&,bool full=false,
        ordinary_launch::deadline=ordinary_launch::deadline::max());
    uint64_t bytes()const;
    void reserve(uint64_t)const;
    void insert(history_capacity_class,const values&);
    void replace(history_capacity_class,const values&,const values&);
    void finish(budget&,bool full=false);
};
uint64_t history_capacity_observe(database&,const ordinary_history_limits&,budget&,bool full=false,
    ordinary_launch::deadline=ordinary_launch::deadline::max());
}
