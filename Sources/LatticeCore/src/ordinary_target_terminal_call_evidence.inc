// Included after the actual descriptor phase and same first-close producers.
// A private terminal observer: no native/file effect, normal-work permission,
// failed-leave rewrite, bank recycling, input destruction or admission refund.
bool ordinary_target_staging_connection::terminal_descriptor_calls_known(
    std::uint64_t environment_generation)const noexcept {
    try {
        const auto native=state_;
        if(!native||!native->creator_current()||native->facade.lock().get()!=this||
           !native->shell||!native->io||!native->cleanup_attempted)return false;
        const auto& elected=native->terminal_abandonment;
        if(!elected.attempted||!elected.seal_attempted||!elected.seal_returned||!elected.sealed||
           elected.shell_original!=native->shell.get()||native->abandoned_main.selected)return false;
        if(elected.reason==state::terminal_reason::database_failure) {
            if(!elected.failure_original||elected.owner_original)return false;
        }else if(elected.reason==state::terminal_reason::receiving_owner_stop) {
            if(!elected.owner_original||elected.failure_original)return false;
        }else return false;
        const auto scope=native->active_scope.lock();
        if(!scope||scope.get()!=native->scope_identity||native->last_scope!=scope||!scope->cleanup||
           scope->left||!scope->creator_binding(*native->shell)||scope->generation!=native->scope_generation||
           !scope->close_observed||scope->close_rc!=SQLITE_OK||!scope->environment||!scope->ledger||
           native->settling_scope||native->settling_callback_phase)return false;
        // This is the separate real VM/transaction/UDF/decode/checked-close
        // witness. A returned callback does not supply that native ending.
        if(!native->shell->target_staging_native_cleanup_known_(*this)||
           !native->io->callbacks_joined(*this))return false;
        const auto prior=scope->cleanup_predecessor;
        if(!prior||prior==scope||prior->cleanup||prior->cleanup_predecessor||!prior->published||
           prior->original.lock()!=native||prior->shell!=native->shell.get()||
           prior->retirement!=scope->retirement||!prior->generation||prior->generation>=scope->generation||
           prior->ledger!=scope->ledger||scope->ledger->issuer!=prior.get())return false;
        if(!environment_generation||
           !(environment_generation==scope->generation||
             (environment_generation==prior->generation&&!prior->leave_joined&&
              prior->environment==scope->environment)))return false;
        const auto callbacks_known=[&](const auto& phase)noexcept {
            if(phase->active_callback_slot!=phase->callback_originals.size()||phase->active_callback_generation||
               phase->settling_callback_slot!=phase->callback_originals.size()||
               phase->settling_callback_generation||phase->settling_callback_high_generation)return false;
            for(const auto& retained:phase->callback_originals) {
                if(!retained)continue;
                const auto& original=*retained;
                if(!original.generation||original.generation>native->callback_generation||
                   !original.body_returned||!original.leave_attempted||!original.leave_returned)return false;
                if(original.previous_slot==phase->callback_originals.size()) {
                    if(original.previous_generation)return false;
                }else if(original.previous_slot>=phase->callback_originals.size()||
                         !original.previous_generation||original.previous_generation>=original.generation)return false;
                const auto first_known=[&](const auto& first)noexcept {
                    return !first||(first->attempted&&first->entered&&first->returned&&
                        first->callback_slot<phase->callback_originals.size()&&
                        first->callback_generation==original.generation&&
                        first->original&&first->original==original.first_resource);
                };
                if(!first_known(original.first_negative)||!first_known(original.first_short_read))return false;
                // Both a normal error return and a separately recorded caught
                // body exception are endings; neither changes leave_joined,
                // callback_credit_sealed or the original native error.
            }
            // Existing leave resets a successful slot only after its real body
            // and local destruction have returned. Failed/unknown slots stay
            // retained. Absence alone is never the active-stack/count proof.
            return true;
        };
        if(!callbacks_known(scope)||!callbacks_known(prior))return false;
        const auto bank=scope->ledger;
        if(bank->count>bank->calls.size())return false;
        for(std::size_t at=0;at<bank->count;++at) {
            const auto& call=bank->calls[at];
            if(!call.attempted||!call.original||call.resource_index>=native->resources.size()||
               state::resource_slot(call.original->cookie)!=call.resource_index)return false;
            // Reserved-but-never-entered work did not issue a native effect.
            // An entered effect must retain its actual first return.
            if((call.entered&&!call.returned)||(!call.entered&&call.returned))return false;
            if(call.callback_generation) {
                if(call.callback_generation>native->callback_generation||
                   call.callback_slot>=scope->callback_originals.size())return false;
            }else if(call.callback_slot!=scope->callback_originals.size())return false;
        }
        return true; // Call endings only; namespace/borrow/lock/payload/VFS/leave are separate.
    }catch(...){return false;}
}
