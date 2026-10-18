#include <lattice/store_physical_api.hpp>
#include "ordinary_cohort_attachment_qualification.hpp"
#if defined(LATTICE_MANAGED_ATTACHMENT_QUALIFICATION)
#include "ordinary_cohort_attachment.hpp"
#include "ordinary_export.hpp"
#include <lattice/lattice.hpp>
#include <filesystem>
#include <vector>

namespace lattice::detail {
struct ordinary_cohort_attachment_qualification_access {
    static bool enrolled(lattice_db& value){return static_cast<bool>(value.ordinary_managed_profile_.load(std::memory_order_acquire));}
    static std::shared_ptr<database> writer(lattice_db& value){
        std::lock_guard lock(value.connection_ownership_mutex_);return value.db_;
    }
    static std::shared_ptr<database> reader(lattice_db& value){
        std::lock_guard lock(value.connection_ownership_mutex_);return value.read_db_;
    }
    static std::shared_ptr<database> replace(lattice_db& value,std::shared_ptr<database> actual,bool writer){
        std::shared_ptr<database> previous;
        {std::lock_guard lock(value.connection_ownership_mutex_);
            auto& slot=writer?value.db_:value.read_db_;
            previous=std::move(slot);slot=std::move(actual);++value.connection_revision_;++value.topology_ownership_revision_;}
        return previous; // Captures remain owned outside the leaf.
    }
    static bool valid(lattice_db& value){std::lock_guard lock(value.attach_mutex_);return value.attachment_topology_valid_;}
    static bool unpublished(lattice_db& value,const std::string& alias){
        std::lock_guard lock(value.attach_mutex_);
        return !value.attached_route_tokens_.contains(alias)&&!value.attachment_topology_valid_&&
            value.attached_cohort_bindings_.contains(alias);
    }
    static int raw_attach(database& value){
        return ::lattice::detail::store_physical_exec(value.internal_handle(),"ATTACH DATABASE ':memory:' AS forbidden_raw",nullptr,nullptr,nullptr);
    }
    static bool attached(database& value,const std::string& alias){
        sqlite3_stmt* raw=nullptr;
        const auto rc=::lattice::detail::store_physical_prepare_v2(value.internal_handle(),"PRAGMA database_list",-1,&raw,nullptr);
        const std::unique_ptr<sqlite3_stmt,decltype(&::lattice::detail::store_physical_finalize)> statement(raw,&::lattice::detail::store_physical_finalize);
        if(rc!=SQLITE_OK)throw db_error("qualification database list unavailable");
        bool found=false;int step=SQLITE_OK;
        while((step=::lattice::detail::store_physical_step(raw))==SQLITE_ROW){
            const auto* name=sqlite3_column_text(raw,1);
            if(name&&alias==reinterpret_cast<const char*>(name))found=true;
        }
        if(step!=SQLITE_DONE)throw db_error("qualification database list incomplete");
        return found;
    }
};
namespace {
using access=ordinary_cohort_attachment_qualification_access;
void require(bool value){if(!value)throw db_error("managed attachment resource qualification failed");}
class hook_scope final {
    const std::shared_ptr<const ordinary_cohort_attachment_test_hooks::callback> prior_;
public:
    explicit hook_scope(ordinary_cohort_attachment_test_hooks::callback callback)
        :prior_(ordinary_cohort_attachment_test_hooks::after_handle){
        ordinary_cohort_attachment_test_hooks::after_handle=
            std::make_shared<const ordinary_cohort_attachment_test_hooks::callback>(std::move(callback));
    }
    ~hook_scope(){ordinary_cohort_attachment_test_hooks::after_handle=prior_;}
};
class actual_handle_substitution final {
    lattice_db& owner_;bool writer_;
    std::shared_ptr<database> original_;
public:
    actual_handle_substitution(lattice_db& owner,std::shared_ptr<database> actual,bool writer)
        :owner_(owner),writer_(writer),original_(access::replace(owner,std::move(actual),writer)){}
    ~actual_handle_substitution(){auto displaced=access::replace(owner_,std::move(original_),writer_);}
};
}
ordinary_cohort_attachment_qualification_result qualify_managed_cohort_attachment(lattice_db& parent,lattice_db& target){
    ordinary_cohort_attachment_qualification_result result;
    std::vector<std::unique_ptr<lattice_db>> children;children.reserve(8);
    bool closes=true;int stage=1;
    auto clone=[&](lattice_db& owner)->lattice_db&{
        ordinary_export_access::validate_managed_reference(owner);
        auto value=ordinary_export_access::child(owner,owner.config());
        // The production child path must have retained its actual historical
        // profile; normal-primary-only custody is not sufficient for this case.
        require(value->config().ordinary_context!=nullptr&&access::enrolled(*value));
        ordinary_export_access::validate_managed_reference(*value);
        children.push_back(std::move(value));return *children.back();
    };
    auto close=[&](lattice_db& owner){
        const auto value=owner.close_checked();
        closes=closes&&!value.error&&!value.cleanup_error&&value.cleanup_complete;
    };
    const auto alias=std::filesystem::path(target.config().path).filename().replace_extension().string();
    try{
        require(&parent!=&target&&parent.config().path!=target.config().path);
        auto& first=clone(parent);auto& first_target=clone(target);
        auto writer=access::writer(first),reader=access::reader(first);
        require(writer&&reader&&writer!=reader);
        result.issued=true;
        stage=2;std::size_t observed=0;bool refused=false;
        {
            hook_scope observe([&](lattice_db& recipient,lattice_db& source,std::size_t index){
                if(&recipient!=&first||&source!=&first_target)return;
                require(index==0);++observed;
                result.raw_denied=access::raw_attach(*writer)==SQLITE_AUTH;
                try{recipient.attach(source);}catch(...){result.reentrant_denied=true;}
                throw db_error("qualification after actual writer attachment");
            });
            try{first.attach(first_target);}catch(...){refused=true;}
        }
        require(refused&&observed==1&&result.raw_denied&&result.reentrant_denied);
        require(access::unpublished(first,alias)&&access::attached(*writer,alias)&&!access::attached(*reader,alias));
        first.detach(first_target);
        require(access::valid(first)&&!access::attached(*writer,alias)&&!access::attached(*reader,alias));
        result.throw_cleanup=true;

        stage=3;
        auto& wrong_writer=clone(parent);auto& writer_target=clone(target);
        auto original_writer=access::writer(wrong_writer),wrong_actual=access::writer(writer_target);
        require(original_writer&&wrong_actual&&original_writer!=wrong_actual);
        refused=false;
        {actual_handle_substitution substitute(wrong_writer,wrong_actual,true);
            try{wrong_writer.attach(writer_target);}catch(...){refused=true;}}
        require(refused&&!access::attached(*original_writer,alias)&&!access::attached(*wrong_actual,alias));
        result.writer_mismatch=true;

        stage=4;
        auto& wrong_reader=clone(parent);auto& reader_target=clone(target);
        writer=access::writer(wrong_reader);reader=access::reader(wrong_reader);
        auto wrong_reader_actual=access::reader(reader_target);
        require(writer&&reader&&wrong_reader_actual&&reader!=wrong_reader_actual);
        observed=0;refused=false;
        {actual_handle_substitution substitute(wrong_reader,wrong_reader_actual,false);
            hook_scope observe([&](lattice_db& recipient,lattice_db&,std::size_t index){
                if(&recipient==&wrong_reader){require(index==0);++observed;}});
            try{wrong_reader.attach(reader_target);}catch(...){refused=true;}}
        require(refused&&observed==1&&access::unpublished(wrong_reader,alias));
        require(access::attached(*writer,alias)&&!access::attached(*wrong_reader_actual,alias));
        wrong_reader.detach(reader_target);
        require(access::valid(wrong_reader)&&!access::attached(*writer,alias)&&!access::attached(*reader,alias));
        result.reader_mismatch=true;

        stage=5;
        auto& closed=clone(parent);auto& closing_target=clone(target);
        writer=access::writer(closed);reader=access::reader(closed);require(writer&&reader);
        observed=0;refused=false;
        {hook_scope observe([&](lattice_db& recipient,lattice_db& source,std::size_t index){
                if(&recipient==&closed){require(index==0);++observed;close(source);require(closes);}});
            try{closed.attach(closing_target);}catch(...){refused=true;}}
        require(refused&&observed==1&&closing_target.is_closed()&&access::unpublished(closed,alias));
        closed.detach(closing_target);
        require(access::valid(closed)&&!access::attached(*writer,alias)&&!access::attached(*reader,alias));
        result.closed_target=true;
    }catch(...){result.failed_stage=stage;}
    for(auto it=children.rbegin();it!=children.rend();++it)close(**it);
    result.cleanup_complete=closes;
    return result;
}
}
#endif
