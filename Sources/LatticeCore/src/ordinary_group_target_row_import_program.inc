// Genuine SAME schema/request producer for exact fresh-target row statements.
// Header table ordinals/rows remain DATA; actual role admission is separate.
const std::shared_ptr<ordinary_group_target_schema_original::row_import_program>&
ordinary_group_target_schema_original::prepare_row_import(const ordinary_target_staging_connection& connection,ordinary_launch::deadline end)const {
    ordinary_export_access::prepare_receiver_group_target_row_import(*this,connection,end);return require_row_import(connection,end);
}
const std::shared_ptr<ordinary_group_target_schema_original::row_import_program>&
ordinary_group_target_schema_original::require_row_import(const ordinary_target_staging_connection& connection,ordinary_launch::deadline end)const {
    require_connection(connection,end);const auto out=state_->row_import;
    if(!state_->row_import_attempted||!out||out->original.lock().get()!=this||!out->generation_entered||!out->generation_returned||
       out->first_failure||out->projection!=state_->projection)
        throw std::runtime_error("GROUP exact row programme unavailable");
    return state_->row_import;
}
void ordinary_export_access::prepare_receiver_group_target_row_import(const ordinary_group_target_schema_original& original,
    const ordinary_target_staging_connection& connection,ordinary_launch::deadline end) {
    creator_check();original.require_connection(connection,end);const auto held=original.state_;const auto request=held->request.lock();
    require(request&&request->state_->target_schema.get()==&original,"GROUP row programme actual request retired");
    const auto receiving=std::atomic_load(&request->state_->group_receiver);
    require(receiving&&request->state_->staging_connection.get()==&connection,"GROUP row programme actual connection differs");
    connection.require_import_suspension_ending(*receiving,end); // Genuine prior Native ending, not copied metadata permission.
    const auto projection=original.require_projection(connection,end);
    require(projection->logical&&projection->validation_returned&&!projection->first_failure,"GROUP row actual validated logical projection missing");
    const auto& actual=*projection->logical;
    require(!held->row_import_attempted&&!held->row_import,"GROUP row programme already attempted");held->row_import_attempted=true;
    try {
        held->row_import=std::make_shared<ordinary_group_target_schema_original::row_import_program>(request->state_->target_schema,projection);
        const auto out=held->row_import;out->generation_entered=true;original.require_connection(connection,end);
        out->add(projection->plan->tables.size()*sizeof(ordinary_group_target_schema_original::row_import_program::table_program));
        out->tables.reserve(projection->plan->tables.size());
        if(out->tables.capacity()>projection->plan->tables.size())out->add((out->tables.capacity()-projection->plan->tables.size())*sizeof(ordinary_group_target_schema_original::row_import_program::table_program));
        std::size_t position=0;
        for(const auto& [name,logical]:projection->plan->tables) {
            require(position<actual.tables.size(),"GROUP row programme actual table ordinal absent");
            const auto& wire=actual.tables[position++];const auto& physical=projection->metadata->tables.at(name);
            require(wire.name==name&&wire.columns.size()==logical.columns.size()&&wire.types.size()==logical.columns.size(),
                "GROUP row programme actual logical corroboration differs");
            out->tables.emplace_back();auto& table=out->tables.back();table.name=name;out->add(table.name.capacity());table.local_id=logical.local_id;
            out->add(logical.columns.size()*(sizeof(std::string)+sizeof(column_type)+sizeof(bool)));
            table.columns.reserve(logical.columns.size());table.types.reserve(logical.columns.size());table.required.reserve(logical.columns.size());
            if(table.columns.capacity()>logical.columns.size())out->add((table.columns.capacity()-logical.columns.size())*sizeof(std::string));
            if(table.types.capacity()>logical.columns.size())out->add((table.types.capacity()-logical.columns.size())*sizeof(column_type));
            if(table.required.capacity()>logical.columns.size())out->add((table.required.capacity()-logical.columns.size())*sizeof(bool));
            for(std::size_t c=0;c<logical.columns.size();++c) {
                const auto& column=logical.columns[c];require(wire.columns[c]==column.name&&wire.types[c]==column.type,"GROUP row actual column differs");
                const auto actual=std::find_if(physical.columns.begin(),physical.columns.end(),[&](const auto& value){return value.name==column.name;});
                require(actual!=physical.columns.end(),"GROUP row actual physical column missing");
                table.columns.push_back(column.name);out->add(table.columns.back().capacity());table.types.push_back(column.type);table.required.push_back(actual->not_null!=0);
            }
            // SAME exact normal absent-row INSERT expression; no logical ID is
            // copied into the fresh target's local autoincrement identity.
            const auto fields=ordinary_row_recipe::fields(table.columns);
            out->add(fields.columns.capacity()+fields.arguments.capacity()+fields.assignments.capacity());
            table.insert_sql=ordinary_row_recipe::insert(name,fields);out->add(table.insert_sql.capacity());
            table.read_sql="SELECT "+fields.columns+" FROM main."+ordinary_row_recipe::quote(name)+" WHERE globalId=?";out->add(table.read_sql.capacity());
        }
        require(position==actual.tables.size(),"GROUP row programme gained actual logical table");
        out->generation_returned=true;connection.require_import_suspension_ending(*receiving,end);original.require_connection(connection,end);
    }catch(...) {if(held->row_import&&!held->row_import->first_failure)held->row_import->first_failure=std::current_exception();if(!held->first_failure)held->first_failure=std::current_exception();throw;}
}
