// Include in ordinary_export.cpp AFTER managed profile implementation.
std::shared_ptr<const ordinary_managed_profile> ordinary_export_access::managed_wss_profile(lattice_db& owner) {
    // This copies the exact private enrolled profile shared by application and
    // actual child(owner) setup. It grants no source/remote/membership authority.
    if(owner.is_closed()||!owner.config_.ordinary_context||!owner.config_.ordinary_context->is_managed_history_view())
        refuse("managed WSS needs actual managed query owner");
    const auto profile=owner.ordinary_managed_profile_.load(std::memory_order_acquire);
    if(!profile)refuse("managed WSS configured before actual enrollment");
    return profile;
}
