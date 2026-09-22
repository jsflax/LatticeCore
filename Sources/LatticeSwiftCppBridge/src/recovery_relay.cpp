#include <lattice.hpp>
#include "../../LatticeCore/src/recovery_authenticated_session.hpp"
namespace lattice {
namespace {
void relay_failure()noexcept {
    try{throw;}catch(const std::exception& e){record_bridge_error(e.what());}
    catch(...){record_bridge_error("Unknown authenticated relay bridge exception");}
}
}
relay_recovery_setup swift_lattice_ref::open_relay_recovery_setup(const std::string& policy,const std::string& connection,
    void* context,int32_t(*current)(void*),void(*destroy)(void*))const noexcept {
    last_bridge_error().clear();
    try{relay_recovery_setup result;
        result.value_=detail::authenticated_relay_setup::open(impl_,policy,connection,context,current,destroy);return result;
    }catch(...){relay_failure();return {};}
}
bool relay_recovery_setup::valid()const noexcept{return bool(value_);}
std::string relay_recovery_setup::descriptor()const noexcept {
    last_bridge_error().clear();try{return value_?value_->descriptor():std::string{};}catch(...){relay_failure();return {};}
}
relay_recovery_stop relay_recovery_setup::stop_token()const noexcept {relay_recovery_stop r;if(value_)r.value_=value_->stop_token();return r;}
bool relay_recovery_setup::finish_authorization(const std::string& outcome)const noexcept {
    last_bridge_error().clear();try{return value_&&value_->finish_authorization(outcome);}catch(...){relay_failure();return false;}
}
relay_recovery_result relay_recovery_setup::receive(const std::string& frame)const noexcept {
    last_bridge_error().clear();relay_recovery_result result;
    try{if(!value_)return result;auto actual=value_->receive(frame);result.status_=actual.status;
        result.ids_=std::move(actual.applied);result.operation_=std::move(actual.operation);return result;
    }catch(...){relay_failure();result.status_=4;return result;}
}
void relay_recovery_setup::close_on_io()const noexcept{if(value_)value_->close();}
void relay_recovery_stop::stop()const noexcept{if(value_)value_->stop();}
bool relay_recovery_stop::live()const noexcept{return value_&&value_->live();}
bool relay_recovery_stop::drained()const noexcept{return !value_||value_->drained();}
int32_t relay_recovery_result::status_code()const noexcept{return status_;}
const std::vector<std::string>& relay_recovery_result::ids()const noexcept{return ids_;}
std::vector<std::string> relay_recovery_result::take_ids()noexcept {
    std::vector<std::string> result;result.swap(ids_);return result;
}
bool relay_recovery_result::publishable()const noexcept{return operation_&&operation_->publishable();}
}
