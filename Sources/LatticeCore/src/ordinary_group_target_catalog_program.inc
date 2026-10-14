// Included only inside the actual private receiver translation unit/namespace.
// The private program retains generated input, never a SQL/current/cut grant.
const std::shared_ptr<ordinary_group_target_schema_original::catalog_program>&
ordinary_group_target_schema_original::require_catalog(
    const ordinary_target_staging_connection& connection,ordinary_launch::deadline end)const {
    require_connection(connection,end);
    if(!state_->catalog_preparation_attempted||!state_->catalog||state_->catalog->original.lock().get()!=this||
       !state_->catalog->generation_returned||state_->catalog->first_failure||state_->catalog->operations.empty())
        throw std::runtime_error("GROUP actual complete catalog program unavailable");
    return state_->catalog;
}
void ordinary_export_access::prepare_receiver_group_target_catalog(
    const std::shared_ptr<ordinary_group_target_schema_original>& original,ordinary_launch::deadline end) {
    creator_check();require(original&&original->state_,"GROUP catalog original unavailable");
    auto held=original->state_;original->require_connection(*held->connection,end);
    require(!held->catalog_preparation_attempted&&!held->catalog,"GROUP catalog preparation already charged");
    held->catalog_preparation_attempted=true; // Never reset/retry a failed producing original.
    using program=ordinary_group_target_schema_original::catalog_program;
    held->catalog=std::make_shared<program>(original); // SAME request holds output before postchecks/generation errors.
    try {
        original->require_connection(*held->connection,end);
        const auto& catalog=*held->declarations;auto& out=*held->catalog;
        const auto emit=[&](std::string sql,std::vector<program::argument> values={}) {
            original->require_connection(*held->connection,end);
            // Same native writer must prove each actual generated object's
            // absence before CREATE...IF NOT EXISTS can silently adopt it.
            const auto first=sql.find_first_not_of(" \t\r\n");
            for(const auto* prefix:{"CREATE VIRTUAL TABLE ","CREATE TABLE ","CREATE INDEX ","CREATE UNIQUE INDEX ","CREATE TRIGGER "}) {
                if(first==std::string::npos||sql.compare(first,std::strlen(prefix),prefix)!=0)continue;
                auto name=sql.substr(first+std::strlen(prefix));
                if(name.starts_with("IF NOT EXISTS "))name.erase(0,std::strlen("IF NOT EXISTS "));
                name=name.substr(0,name.find_first_of(" (\t\r\n"));
                require(!name.empty(),"GROUP generated object name unavailable");
                out.append(program::purpose::require_absent_object,
                    "SELECT 1 FROM main.sqlite_schema WHERE name=? COLLATE NOCASE LIMIT 1",{std::move(name)});
                break;
            }
            out.append(program::purpose::generated,std::move(sql),std::move(values));
        };
        // New target-only conservative refusal, not a normal naming contract.
        // Existing generators interpolate bare identifiers/literal case names.
        const auto identifier=[](const std::string& name) {
            const auto first=[](unsigned char c){return c=='_'||(c>='A'&&c<='Z')||(c>='a'&&c<='z');};
            if(name.empty()||name.size()>1024||!first(static_cast<unsigned char>(name.front())))
                throw std::runtime_error("GROUP target generated identifier unavailable");
            for(unsigned char c:name)if(!first(c)&&!(c>='0'&&c<='9'))
                throw std::runtime_error("GROUP target identifier requires unsupported normal quoting");
        };
        std::set<std::string> object_names;
        const auto object=[&](const std::string& name) {
            identifier(name);std::string key=name;
            for(auto& c:key)if(c>='A'&&c<='Z')c=static_cast<char>(c-'A'+'a');
            if(!object_names.insert(key).second)throw std::runtime_error("GROUP generated schema object collision");
        };
        for(const auto* name:{"_lattice_meta","_SyncControl","AuditLog","idx_audit_log_pending_sync","idx_audit_log_change_lookup","sqlite_sequence",
             "_lattice_sync_set","_lattice_sync_state","idx_sync_state_pending","_lattice_replication_slots","_lattice_receive_guard_store","_lattice_receive_guard","_lattice_receive_guard_state"})object(name);
        std::vector<const model_schema*> order;order.reserve(catalog.models.size());
        for(const auto& [name,schema]:catalog.models) {
            identifier(name);require(schema.table_name==name,"GROUP catalog model identity differs");object(name);
            std::set<std::string> columns{"id","globalId"};
            for(const auto& prop:schema.properties) {
                identifier(prop.name);if(!columns.insert(prop.name).second)throw std::runtime_error("GROUP model property collision");
                if(!prop.target_table.empty())identifier(prop.target_table);
                if(prop.is_union) {
                    identifier(prop.union_desc.union_table_name);
                    for(const auto& c:prop.union_desc.cases) {identifier(c.case_name);for(const auto& v:c.values)if(!v.param_name.empty())identifier(v.param_name);}
                }
            }
            if(!catalog.swift_models.contains(name))order.push_back(&schema);
        }
        std::set<std::string> seen_swift;
        require(catalog.swift_order.size()==catalog.swift_models.size(),"GROUP original Swift schema order missing");
        for(const auto& name:catalog.swift_order) {
            require(catalog.swift_models.contains(name)&&seen_swift.insert(name).second,"GROUP original Swift schema order differs");
            const auto* schema=catalog.find(name);require(schema,"GROUP ordered declaration unavailable");order.push_back(schema);
        }
        emit(ordinary_schema_recipe::sync_set());emit(ordinary_schema_recipe::sync_state());
        emit(ordinary_schema_recipe::sync_state_pending());emit(ordinary_schema_recipe::replication_slots());
        emit(ordinary_schema_recipe::replication_upload_floor());
        emit(ordinary_schema_recipe::receive_store());emit(ordinary_schema_recipe::receive_channel());emit(ordinary_schema_recipe::receive_index());
        // Fresh exclusive target contains no old active pending Audit/history.
        emit("INSERT INTO main._lattice_receive_guard_store VALUES(1,1,?,0,0,0,0)",{std::int64_t{0}});
        const auto audit=[&](const std::string& table,const std::vector<std::pair<std::string,column_type>>& columns,const std::set<std::string>& flags={}) {
            std::vector<std::string> emitted;held->application->create_model_table_triggers(table,columns,flags,{},&emitted);
            object("AuditLog_Update_"+table);object("Audit"+table+"Insert");object("Audit"+table+"Delete");
            for(auto& sql:emitted)emit(std::move(sql));
        };
        // Exact normal Swift physical conversion; model tables precede union parent triggers.
        for(const auto* schema:order) {
            const auto physical=catalog.swift_models.contains(schema->table_name)?ordinary_schema_recipe::swift_physical_model(*schema):*schema;
            std::vector<std::pair<std::string,column_type>> columns;emit(ordinary_schema_recipe::model_table(physical,columns));
            std::set<std::string> flags;for(const auto& prop:physical.properties)
                if(prop.kind==property_kind::primitive&&!prop.is_geo_bounds&&prop.no_history)flags.insert(prop.name);
            audit(schema->table_name,columns,flags);
            emit("INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES(?, ?)",{"trigger_flags:"+schema->table_name,lattice_db::no_history_marker(physical)});
            for(const auto& prop:physical.properties)if(prop.is_geo_bounds) {
                if(prop.kind==property_kind::list) {
                    const auto list="_"+schema->table_name+"_"+prop.name,tree=list+"_rtree";
                    object(list);object(tree);object("idx_"+list+"_parent");
                    emit("INSERT OR IGNORE INTO _lattice_meta(key, value) VALUES(?, ?)",{"internal_table:"+list,schema->table_name});
                    emit(ordinary_schema_recipe::geo_list_table(list));emit(ordinary_schema_recipe::geo_list_parent_index(list));emit(ordinary_schema_recipe::geo_list_rtree_table(tree));
                    object(tree+"_insert");object(tree+"_update");object(tree+"_delete");
                    emit(ordinary_schema_recipe::geo_list_rtree_insert(list));emit(ordinary_schema_recipe::geo_list_rtree_update(list));emit(ordinary_schema_recipe::geo_list_rtree_delete(list));
                    object("Audit"+list+"Insert");object("Audit"+list+"Delete");for(auto& sql:ordinary_schema_recipe::geo_list_audit(list))emit(std::move(sql));
                } else {
                    const auto tree="_"+schema->table_name+"_"+prop.name+"_rtree";object(tree);
                    object(tree+"_insert");object(tree+"_update");object(tree+"_delete");
                    emit(ordinary_schema_recipe::rtree_table(tree));emit(ordinary_schema_recipe::rtree_insert(schema->table_name,prop.name));
                    emit(ordinary_schema_recipe::rtree_update(schema->table_name,prop.name));emit(ordinary_schema_recipe::rtree_delete(schema->table_name,prop.name));emit(ordinary_schema_recipe::rtree_population(schema->table_name,prop.name));
                }
            }
        }
        std::map<std::string,std::pair<std::string,std::string>> unions; // Generated SQL + actual normal multi-parent metadata.
        for(const auto* schema:order)for(const auto& prop:schema->properties)if(prop.is_union) {
            const auto& name=prop.union_desc.union_table_name;auto generated=ordinary_schema_recipe::union_table(name,prop.union_desc);
            const auto parent=schema->table_name+":"+prop.name;auto old=unions.find(name);
            if(old==unions.end()) {
                object(name);object("idx_"+name+"_case");
                emit("INSERT INTO _lattice_meta(key, value) VALUES(?, ?)",{"internal_table:"+name,parent});
                emit(generated.sql);emit("CREATE INDEX IF NOT EXISTS idx_"+name+"_case ON "+name+"(\"case\")");audit(name,generated.columns);
                unions.emplace(name,std::make_pair(std::move(generated.sql),parent));
            } else {
                require(old->second.first==generated.sql,"GROUP shared union declarations differ");
                if(old->second.second.find(parent)==std::string::npos) {
                    old->second.second+=";"+parent;
                    emit("UPDATE _lattice_meta SET value = ? WHERE key = ?",{old->second.second,"internal_table:"+name});
                }
            }
            object("_cascade_delete_"+schema->table_name+"_"+prop.name+"_union");
            emit(ordinary_schema_recipe::union_cascade(schema->table_name,prop.name,name));
        }
        for(const auto* schema:order) {
            const auto physical=catalog.swift_models.contains(schema->table_name)?ordinary_schema_recipe::swift_physical_model(*schema):*schema;
            for(const auto& prop:physical.properties) {
                if(prop.is_vector&&prop.type==column_type::blob) {
                    // Native result must genuinely establish fresh empty data;
                    // no copied count/dimension invents a vec0 sidecar.
                    out.append(program::purpose::require_empty_vector_model,"SELECT id FROM "+schema->table_name+" WHERE "+prop.name+" IS NOT NULL AND length("+prop.name+")>0 LIMIT 1");
                }
                if(prop.is_full_text&&prop.type==column_type::text) {
                    auto derived=fts5_porter_program(schema->table_name,prop.name);object(derived.table);
                    for(const auto* suffix:{"_data","_idx","_content","_docsize","_config"})object(derived.table+suffix);
                    emit(std::move(derived.create_table));for(auto& trigger:derived.triggers){object(trigger.name);emit(std::move(trigger.sql));}emit(std::move(derived.populate));
                }
            }
            for(const auto& prop:schema->properties) {
                std::string link;const bool polymorphic=prop.kind==property_kind::virtual_list||prop.kind==property_kind::virtual_link;
                if(polymorphic)link="_"+schema->table_name+"_"+prop.name;
                else if((prop.kind==property_kind::link||(prop.kind==property_kind::list&&!prop.is_geo_bounds))&&!prop.target_table.empty()) {
                    require(catalog.find(prop.target_table),"GROUP relationship target missing from full catalog");link="_"+schema->table_name+"_"+prop.target_table+"_"+prop.name;
                }
                if(link.empty())continue;object(link);
                emit("INSERT OR IGNORE INTO _lattice_meta(key, value) VALUES(?, ?)",{"internal_table:"+link,schema->table_name});
                emit(polymorphic?ordinary_schema_recipe::virtual_link_table(link):ordinary_schema_recipe::link_table(link));
                object("Audit"+link+"Insert");object("Audit"+link+"Delete");
                if(polymorphic)for(auto& sql:ordinary_schema_recipe::virtual_link_audit(link))emit(std::move(sql));
                else {std::vector<std::string> emitted;held->application->create_link_table_triggers(link,{},&emitted);for(auto& sql:emitted)emit(std::move(sql));}
                if(catalog.swift_models.contains(schema->table_name))emit("INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES(?, ?)",{"internal_table:"+link,schema->table_name+":"+prop.name});
            }
        }
        for(const auto* schema:order) {
            if(catalog.swift_models.contains(schema->table_name)) {
                const auto found=catalog.swift_constraints.find(schema->table_name);require(found!=catalog.swift_constraints.end(),"GROUP actual constraints missing");
                std::set<std::string> shadows;
                for(std::size_t i=0;i<found->second.size();++i) {
                    const auto& constraint=found->second[i];if(constraint.columns.empty())continue;std::vector<std::string> resolved;
                    for(const auto& col:constraint.columns) {
                        identifier(col);const property_descriptor* property=nullptr;for(const auto& prop:schema->properties)if(prop.name==col)property=&prop;
                        if(property&&property->kind==property_kind::link&&!property->target_table.empty()) {
                            const auto shadow=col+"__link_gid",link="_"+schema->table_name+"_"+property->target_table+"_"+col;
                            for(const auto& prop:schema->properties)require(prop.name!=shadow,"GROUP generated shadow collides with declared property");
                            if(shadows.insert(shadow).second) {
                                emit(ordinary_schema_recipe::shadow_column(schema->table_name,shadow));
                                const auto insert="LinkShadow_"+link+"_Insert_"+shadow,erased="LinkShadow_"+link+"_Delete_"+shadow;object(insert);object(erased);
                                emit(ordinary_schema_recipe::shadow_insert(schema->table_name,link,shadow,insert));emit(ordinary_schema_recipe::shadow_delete(schema->table_name,link,shadow,erased));
                                emit(ordinary_schema_recipe::shadow_backfill(schema->table_name,link,shadow));
                            }
                            resolved.push_back(shadow);
                        }else resolved.push_back(col);
                    }
                    object("unique_"+schema->table_name+"_"+std::to_string(i));emit(ordinary_schema_recipe::unique_index(schema->table_name,i,ordinary_schema_recipe::column_list(resolved)));
                }
            }else {
                std::size_t ordinal=0;for(const auto& prop:schema->properties)if(prop.is_unique) {
                    const auto i=ordinal++;if(prop.kind!=property_kind::primitive||prop.is_geo_bounds)continue;
                    object("unique_"+schema->table_name+"_"+std::to_string(i));emit(ordinary_schema_recipe::core_unique_index(schema->table_name,i,prop.name));
                }
            }
            const auto physical=catalog.swift_models.contains(schema->table_name)?ordinary_schema_recipe::swift_physical_model(*schema):*schema;
            for(const auto& prop:physical.properties)if(prop.is_indexed){object("idx_"+schema->table_name+"_"+prop.name);emit(ordinary_schema_recipe::nonunique_index(physical,prop));}
        }
        // Only after every generated model/all-kind/index operation. Native
        // channel binds the actual schema-cookie read, never a copied receipt.
        out.append(program::purpose::schema_cookie,"PRAGMA main.schema_version");
        const auto marker=[&](const std::string& key) {
            out.append(program::purpose::fingerprint_cookie,"INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES(?, ?)",{key,std::string{}});
            out.append(program::purpose::fingerprint_cookie,"DELETE FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%' AND key <> ? AND value <> ?",{key,std::string{}});
        };
        marker(catalog.swift_fingerprint);marker(held->application->compute_core_fingerprint_key());
        emit("UPDATE _lattice_meta SET value = '"+std::to_string(held->version)+"' WHERE key = 'schema_version'");
        require(!catalog.swift_snapshot.empty(),"GROUP actual normal Swift snapshot missing");
        emit("INSERT OR REPLACE INTO _lattice_meta(key, value) VALUES('lattice_swift_schema', ?)",{catalog.swift_snapshot});
        original->require_connection(*held->connection,end);out.generation_returned=true;
    }catch(...) {
        if(!held->catalog->first_failure)held->catalog->first_failure=std::current_exception();
        if(!held->first_failure)held->first_failure=std::current_exception();throw;
    }
}
void ordinary_export_access::install_receiver_group_target_catalog(
    const std::shared_ptr<ordinary_group_target_schema_original>& original,ordinary_launch::deadline end) {
    prepare_receiver_group_target_catalog(original,end);const auto held=original->state_;
    try {original->require_catalog(*held->connection,end);held->connection->install_catalog(original,end);
         original->require_connection(*held->connection,end);}
    catch(...) {if(!held->first_failure)held->first_failure=std::current_exception();throw;}
}
