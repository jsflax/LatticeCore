#include <lattice.hpp>
#include "../../LatticeCore/src/recovery_server_export.hpp"

namespace lattice {
namespace {
void export_failure(std::exception_ptr error)noexcept{
    if(!error)return;
    try{std::rethrow_exception(error);}
    catch(const std::exception& e){record_bridge_error(e.what());}
    catch(...){record_bridge_error("Unknown server export bridge exception");}
}
}
server_export_endpoint swift_lattice_ref::make_server_export_endpoint_for_qualification(
    void* context,int32_t(*enqueue)(void*,const uint8_t*,size_t,uint64_t),void(*destroy)(void*),const server_export_limits& limits)const noexcept {
    last_bridge_error().clear();
    // Core rejects a missing destroy callback before ownership transfer.
    // Otherwise it consumes context on every outcome, including allocation
    // failure. The caller must retain context when destroy is null.
    try{
        detail::recovery_export_limits native;
        // Validation belongs inside the consuming factory. Invalid signed
        // inputs map to an invalid native budget, never unsigned wrap/adoption.
        const bool valid=limits.entries>0&&limits.entries<=1000&&limits.field_bytes>0&&limits.field_bytes<=1048576&&
            limits.raw_bytes>=limits.field_bytes&&limits.raw_bytes<=4194304&&limits.wire_bytes>=16&&limits.wire_bytes<=8388608;
        native.entries=valid?static_cast<size_t>(limits.entries):0;
        native.field_bytes=valid?static_cast<size_t>(limits.field_bytes):0;
        native.raw_bytes=valid?static_cast<size_t>(limits.raw_bytes):0;
        native.wire_bytes=valid?static_cast<size_t>(limits.wire_bytes):0;
        auto endpoint=detail::recovery_server_export_endpoint::create_for_qualification(impl_,context,enqueue,destroy,native);
        server_export_endpoint result;
        result.stop_=std::make_shared<detail::recovery_server_export_stop>(endpoint.stop_token());
        result.value_=std::make_shared<detail::recovery_server_export_endpoint>(std::move(endpoint));return result;
    }catch(...){export_failure(std::current_exception());return {};}
}
bool server_export_endpoint::valid()const noexcept{return value_&&value_->valid();}
server_export_stop server_export_endpoint::stop_token()const noexcept{
    server_export_stop result;result.value_=stop_;return result;
}
server_export_page server_export_endpoint::prepare_history(int64_t after,int64_t count)const noexcept{
    last_bridge_error().clear();server_export_page result;
    try{
        if(!value_)return result;
        if(count<=0||count>1000){record_bridge_error("server export page count outside bound");result.failure_=9;return result;}
        // Allocate the wrapper before any claims. No result has to escape a
        // completed claim transaction merely to allocate its portable handle.
        result.value_=std::make_shared<detail::recovery_server_export_page>();
        result.completion_=std::make_shared<detail::recovery_server_export_completion>();
        *result.value_=value_->prepare_history(after,static_cast<size_t>(count));
        *result.completion_=result.value_->completion();export_failure(result.value_->failure());return result;
    }catch(...){export_failure(std::current_exception());result.failure_=6;return result;}
}
void server_export_endpoint::close_on_io()const noexcept{if(value_)value_->close_on_io();}
bool server_export_stop::valid()const noexcept{return value_&&value_->valid();}
void server_export_stop::request_stop()const noexcept{if(value_)value_->request_stop();}
bool server_export_stop::stopped()const noexcept{return !value_||value_->stopped();}
bool server_export_stop::resources_released()const noexcept{return !value_||value_->resources_released();}
bool server_export_completion::valid()const noexcept{return value_&&value_->valid();}
bool server_export_completion::record_result(bool success)const noexcept{return value_&&value_->record_result(success);}
bool server_export_completion::completed()const noexcept{return value_&&value_->completed();}
bool server_export_completion::permits_advance()const noexcept{return value_&&value_->permits_advance();}
int32_t server_export_page::status_code()const noexcept{return failure_?failure_:value_?static_cast<int32_t>(value_->status()):0;}
uint64_t server_export_page::serial()const noexcept{return value_?value_->serial():0;}
int64_t server_export_page::count()const noexcept{return value_?value_->count():0;}
bool server_export_page::has_last_audit_id()const noexcept{return value_&&value_->last_audit_id().has_value();}
int64_t server_export_page::last_audit_id()const noexcept{return value_?value_->last_audit_id().value_or(0):0;}
server_export_completion server_export_page::completion_token()const noexcept{
    server_export_completion result;result.value_=completion_;return result;
}
int32_t server_export_page::consume()const noexcept{
    if(failure_||!value_)return status_code();last_bridge_error().clear();const auto result=value_->consume();export_failure(value_->failure());return static_cast<int32_t>(result);
}
void server_export_page::close_on_io()const noexcept{if(value_)value_->close_on_io();}
} // namespace lattice
