#include "derived_profile.hpp"
#include <sqlite-vec.h>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <set>

namespace lattice::detail {
namespace {
void check(bool ok, const char* message) {
    if (!ok) throw derived_profile_error(message);
}
void limits_ok(const derived_limits& b) {
    check(b.properties && b.properties <= 64 && b.metadata_rows && b.metadata_rows <= 8192 &&
          b.metadata_bytes && b.metadata_bytes <= 8 * 1024 * 1024 &&
          b.sql_bytes && b.sql_bytes <= 256 * 1024 && b.sql_bytes <= b.metadata_bytes &&
          b.initial_rows && b.initial_rows <= 65536 &&
          b.value_bytes && b.value_bytes <= 16 * 1024 * 1024 &&
          b.total_value_bytes && b.total_value_bytes <= 128 * 1024 * 1024 &&
          b.value_bytes <= b.total_value_bytes, "derived limits refused");
}
std::string lower(std::string s) {
    for (auto& c : s) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return s;
}
void identifier(const std::string& s, bool model = false) {
    check(!s.empty() && s.size() <= 128, "derived identifier length refused");
    for (size_t i = 0; i < s.size(); ++i) {
        const auto c = static_cast<unsigned char>(s[i]);
        check((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
              (i && c >= '0' && c <= '9'), "derived identifier spelling refused");
    }
    const auto folded = lower(s);
    check(folded.compare(0, 7, "sqlite_") != 0, "derived SQLite identifier refused");
    if (model) check(s[0] != '_' && folded != "auditlog", "derived internal model refused");
}
std::string quote(const std::string& s) { return "\"" + s + "\""; }
const column_value_t& cell(const database::row_t& r, const std::string& key) {
    const auto i = r.find(key); check(i != r.end(), "derived result field missing"); return i->second;
}
int64_t integer(const database::row_t& r, const std::string& key) {
    const auto& v = cell(r, key); check(std::holds_alternative<int64_t>(v), "derived integer refused");
    return std::get<int64_t>(v);
}
std::string bytes(const database::row_t& r, const std::string& key) {
    const auto& v = cell(r, key);
    check(std::holds_alternative<std::vector<uint8_t>>(v), "derived metadata bytes refused");
    const auto& b = std::get<std::vector<uint8_t>>(v);
    return b.empty() ? std::string{} : std::string(reinterpret_cast<const char*>(b.data()), b.size());
}
bool null_cell(const database::row_t& r, const std::string& key) {
    return std::holds_alternative<std::nullptr_t>(cell(r, key));
}
size_t dynamic_bytes(const column_value_t& value) {
    if (const auto* s = std::get_if<std::string>(&value)) return s->size();
    if (const auto* b = std::get_if<std::vector<uint8_t>>(&value)) return b->size();
    return std::holds_alternative<std::nullptr_t>(value) ? 0 : 8;
}
struct projection {
    std::string expression;
    std::string alias;
    bool numeric = false;
    bool nullable = false;
    size_t max_bytes = 256;
};
struct metadata_reader {
    const derived_query& query;
    const derived_limits& limits;
    size_t rows = 0;
    size_t copied = 0;

    // Every generated metadata query returns at most one row. Its combined
    // projection predicate bounds the entire dynamic row before db::query
    // copies any TEXT/BLOB. It also guards INTEGER-declared corrupt columns.
    std::vector<database::row_t> one(const std::string& from,
        const std::vector<column_value_t>& params, const std::vector<projection>& fields) {
        check(!fields.empty() && fields.size() <= 16, "derived projection count refused");
        if (rows == limits.metadata_rows) {
            // A bounded existence probe distinguishes exact-cap completion
            // from another row without copying any of that row's metadata.
            const auto more = query("SELECT 1 AS present FROM " + from, params);
            check(more.empty(), "derived metadata row cap exceeded");
            return {};
        }
        const auto remaining = limits.metadata_bytes - copied;
        std::string predicate = "1", amount = "0";
        for (const auto& p : fields) {
            const auto e = "(" + p.expression + ")";
            const auto kind = p.numeric ? "integer" : "text";
            predicate += " AND (" + (p.nullable ? e + " IS NULL OR " : "") +
                "typeof(" + e + ")='" + kind + "')";
            if (p.numeric) amount += "+CASE WHEN " + e + " IS NULL THEN 0 ELSE 8 END";
            else {
                predicate += " AND (" + e + " IS NULL OR length(CAST(" + e + " AS BLOB))<=" +
                    std::to_string(p.max_bytes) + ")";
                amount += "+COALESCE(length(CAST(" + e + " AS BLOB)),0)";
            }
        }
        predicate += " AND (" + amount + ")<=" + std::to_string(remaining);
        std::string sql = "SELECT CASE WHEN " + predicate + " THEN 1 ELSE 0 END AS _ok";
        for (const auto& p : fields) sql += ",CASE WHEN " + predicate + " THEN " +
            (p.numeric ? p.expression : "CAST(" + p.expression + " AS BLOB)") + " END AS " + p.alias;
        sql += " FROM " + from;
        check(sql.size() <= 1024 * 1024, "derived generated query too large");
        auto result = query(sql, params);
        check(result.size() <= 1, "derived query row contract refused");
        if (result.empty()) return result;
        check(rows < limits.metadata_rows, "derived metadata row cap exceeded");
        check(integer(result.front(), "_ok") == 1, "derived metadata type or copy cap refused");
        size_t charged = 0;
        for (const auto& p : fields) {
            const auto n = dynamic_bytes(cell(result.front(), p.alias));
            check(n <= remaining - charged, "derived metadata charge overflow"); charged += n;
        }
        copied += charged; ++rows; return result;
    }
};
std::string stored_trigger(std::string sql) {
    const std::string prefix = "CREATE TRIGGER IF NOT EXISTS ";
    check(sql.compare(0, prefix.size(), prefix) == 0, "derived generator trigger prefix changed");
    sql.replace(0, prefix.size(), "CREATE TRIGGER "); return sql;
}

// Module shadow DDL is compared structurally: whitespace/case and identifier
// quoting are incidental, but tokens, AUTOINCREMENT and WITHOUT ROWID are not.
// No comments, expressions or alternate module programs are silently adopted.
std::vector<std::string> ddl_tokens(const std::string& sql) {
    std::vector<std::string> result;
    for (size_t i = 0; i < sql.size();) {
        const auto c = static_cast<unsigned char>(sql[i]);
        if (c == ' ' || c == '\n' || c == '\t' || c == '\r') { ++i; continue; }
        if (c == '\'' || c == '"' || c == '`') {
            const char delimiter = sql[i++]; std::string token; bool closed = false;
            while (i < sql.size()) {
                const char next = sql[i++];
                if (next == delimiter) {
                    if (i < sql.size() && sql[i] == delimiter) { token += delimiter; ++i; }
                    else { closed = true; break; }
                } else token += next;
            }
            check(closed, "derived shadow DDL quote refused"); result.push_back(lower(token));
        } else if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_') {
            const auto start = i++;
            while (i < sql.size()) {
                const auto x = static_cast<unsigned char>(sql[i]);
                if (!((x >= 'A' && x <= 'Z') || (x >= 'a' && x <= 'z') ||
                      (x >= '0' && x <= '9') || x == '_')) break;
                ++i;
            }
            result.push_back(lower(sql.substr(start, i - start)));
        } else {
            check(c == '(' || c == ')' || c == ',' || c == ';', "derived shadow DDL token refused");
            if (c == ';') {
                ++i;
                while (i < sql.size() && (sql[i] == ' ' || sql[i] == '\n' || sql[i] == '\t' || sql[i] == '\r')) ++i;
                check(i == sql.size(), "derived shadow trailing statement refused");
            } else { result.emplace_back(1, static_cast<char>(c)); ++i; }
        }
    }
    return result;
}
} // namespace

derived_descriptor describe_derived(const std::vector<model_schema>& models,
    const std::vector<derived_spec>& specs, const derived_limits& b) {
    limits_ok(b);
    check(!models.empty() && models.size() <= 64 && !specs.empty() && specs.size() <= b.properties,
          "derived descriptor count refused");
    std::map<std::pair<std::string, std::string>, const property_descriptor*> flagged;
    std::set<std::string> model_names;
    for (const auto& m : models) {
        identifier(m.table_name, true);
        check(model_names.insert(lower(m.table_name)).second && m.properties.size() <= 128,
              "derived model/schema count refused");
        std::set<std::string> columns;
        for (const auto& p : m.properties) {
            identifier(p.name);
            check(columns.insert(lower(p.name)).second && p.column_name.empty() && !p.is_geo_bounds &&
                  !p.is_union && p.kind != property_kind::union_type &&
                  p.kind != property_kind::virtual_link && p.kind != property_kind::virtual_list,
                  "derived unsupported property schema");
            check(!(p.is_vector && p.is_full_text), "derived mixed field flags refused");
            if (!p.is_vector && !p.is_full_text) continue;
            check(p.kind == property_kind::primitive && lower(p.name) != "id" && lower(p.name) != "globalid" &&
                  ((p.is_vector && p.type == column_type::blob) ||
                   (p.is_full_text && p.type == column_type::text)), "derived property type refused");
            check(flagged.size() < b.properties, "derived property cap exceeded");
            flagged.emplace(std::make_pair(m.table_name, p.name), &p);
        }
    }
    check(flagged.size() == specs.size(), "derived unlisted or extra specification");
    derived_descriptor result; std::set<std::pair<std::string, std::string>> seen;
    std::set<std::string> generated_names; size_t generated_bytes = 0;
    for (const auto& s : specs) {
        identifier(s.model, true); identifier(s.property);
        const auto key = std::make_pair(s.model, s.property); const auto found = flagged.find(key);
        check(found != flagged.end() && seen.insert(key).second, "derived specification binding refused");
        const auto& p = *found->second; derived_field field; field.spec = s;
        field.nullable = p.nullable; field.no_history = p.no_history;
        if (s.kind == derived_kind::fts5_porter_v1) {
            check(p.is_full_text && !s.dimensions, "derived FTS dimensions/type refused");
            const auto program = fts5_porter_program(s.model, s.property);
            field.table = program.table; field.create_sql = program.create_table;
            field.triggers.assign(program.triggers.begin(), program.triggers.end());
        } else {
            check(s.kind == derived_kind::vec0_flat_f32_v1 && p.is_vector &&
                  s.dimensions > 0 && s.dimensions <= 8192, "derived vector dimensions/type refused");
            const auto program = vec0_program(s.model, s.property); field.table = program.table;
            field.create_sql = vec0_create_table_program(program.table, static_cast<int>(s.dimensions), 0, 0);
            for (size_t i = 0; i < program.sql.size(); ++i)
                field.triggers.push_back({program.names[i], program.owner_table, program.sql[i]});
        }
        check(field.table.size() <= 256 && generated_names.insert(lower(field.table)).second,
              "derived generated name collision");
        auto charge_sql = [&](const std::string& sql) {
            check(sql.size() <= b.sql_bytes && sql.size() <= b.metadata_bytes - generated_bytes,
                  "derived generated SQL cap exceeded"); generated_bytes += sql.size();
        };
        charge_sql(field.create_sql);
        for (const auto& trigger : field.triggers) {
            check(trigger.name.size() <= 256 && generated_names.insert(lower(trigger.name)).second,
                  "derived trigger name collision/length");
            charge_sql(trigger.sql);
        }
        result.fields_.push_back(std::move(field));
    }
    return result;
}

void validate_derived_values(const derived_descriptor& expected, const std::string& model,
    const database::row_t& values, derived_value_usage& usage, const derived_limits& b) {
    limits_ok(b); check(!expected.fields().empty() && expected.fields().size() <= b.properties,
                        "derived empty/oversized descriptor");
    check(usage.rows < b.initial_rows && usage.bytes <= b.total_value_bytes, "derived row budget exceeded");
    auto next = usage; bool found = false;
    for (const auto& f : expected.fields()) {
        if (f.spec.model != model) continue;
        found = true; const auto& value = cell(values, f.spec.property);
        if (std::holds_alternative<std::nullptr_t>(value)) {
            check(f.nullable, "derived nonnullable value refused"); continue;
        }
        size_t n = 0;
        if (f.spec.kind == derived_kind::fts5_porter_v1) {
            check(std::holds_alternative<std::string>(value), "derived FTS value must be TEXT");
            n = std::get<std::string>(value).size();
        } else {
            check(std::holds_alternative<std::vector<uint8_t>>(value), "derived vector value must be BLOB");
            const auto& blob = std::get<std::vector<uint8_t>>(value); n = blob.size();
            check(n == 0 || n == 4 * f.spec.dimensions, "derived vector byte dimension refused");
            static_assert(sizeof(float) == 4 && std::numeric_limits<float>::is_iec559,
                          "flat float32 profile requires four-byte IEEE float");
            for (size_t i = 0; i < n; i += sizeof(float)) {
                float v; std::memcpy(&v, blob.data() + i, sizeof(float));
                check(std::isfinite(v), "derived nonfinite float32 refused");
            }
        }
        check(n <= b.value_bytes && n <= b.total_value_bytes - next.bytes,
              "derived value byte cap exceeded"); next.bytes += n;
    }
    check(found, "derived value model binding refused"); ++next.rows; usage = next;
}

namespace {
struct column_shape {
    std::string name, type;
    int64_t not_null = 0, pk = 0, hidden = 0;
};
struct table_shape {
    std::string name, declaration;
    std::vector<column_shape> columns;
    bool without_rowid = false;
    std::string index_origin;
    std::vector<std::string> index_keys;
    bool sqlite_shadow = true;
};
using inventory = std::map<std::string, derived_object_fact>;

inventory read_inventory(metadata_reader& read) {
    inventory result; bool first = true; int64_t last = 0;
    while (true) {
        const auto rows = read.one("main.sqlite_schema" + std::string(first ? "" : " WHERE rowid>?") +
            " ORDER BY rowid LIMIT 1", first ? std::vector<column_value_t>{} : std::vector<column_value_t>{last},
            {{"rowid","rid",true}, {"type","kind",false,false,16}, {"name","name"},
             {"tbl_name","owner"}, {"sql","sql",false,true,read.limits.sql_bytes}});
        if (rows.empty()) break;
        const auto& row = rows.front(); const auto id = integer(row, "rid");
        check(first || id > last, "derived inventory cursor refused"); first = false; last = id;
        derived_object_fact fact{bytes(row,"kind"),bytes(row,"name"),bytes(row,"owner"),
            null_cell(row,"sql") ? std::string{} : bytes(row,"sql")};
        check(!fact.name.empty() && fact.name.find('\0') == std::string::npos &&
              fact.owner.find('\0') == std::string::npos && fact.sql.find('\0') == std::string::npos,
              "derived metadata embedded NUL refused");
        const auto name = fact.name;
        check(result.emplace(name, std::move(fact)).second, "derived duplicate schema name");
    }
    return result;
}
const derived_object_fact& object(const inventory& all, const std::string& name,
    const std::string& kind, const std::string& owner) {
    const auto it = all.find(name);
    check(it != all.end() && it->second.type == kind && it->second.owner == owner,
          "derived required schema object missing or mismatched"); return it->second;
}
std::vector<column_shape> columns(metadata_reader& read, const std::string& table, bool allow_defaults = false) {
    std::vector<column_shape> out;
    for (size_t offset = 0;; ++offset) {
        const auto rows = read.one("pragma_table_xinfo(?, 'main') LIMIT 1 OFFSET " + std::to_string(offset), {table},
            {{"cid","cid",true}, {"name","name"}, {"type","kind",false,false,32},
             {"\"notnull\"","nn",true}, {"pk","pk",true}, {"hidden","hidden",true},
             {"dflt_value","def",false,true,read.limits.sql_bytes}});
        if (rows.empty()) break;
        const auto& row = rows.front();
        check(offset < 128 && integer(row,"cid") == static_cast<int64_t>(offset),
              "derived column count/order refused");
        check(allow_defaults || null_cell(row,"def"), "derived column default refused");
        out.push_back({bytes(row,"name"),lower(bytes(row,"kind")),integer(row,"nn"),
                       integer(row,"pk"),integer(row,"hidden")});
    }
    return out;
}
void match_columns(const std::vector<column_shape>& actual, const std::vector<column_shape>& expected) {
    check(actual.size() == expected.size(), "derived column layout count mismatch");
    for (size_t i = 0; i < actual.size(); ++i) {
        const auto& a = actual[i]; const auto& e = expected[i];
        check(a.name == e.name && a.type == lower(e.type) && a.not_null == e.not_null &&
              a.pk == e.pk && a.hidden == e.hidden, "derived column layout mismatch");
    }
}
void table_kind(metadata_reader& read, const std::string& table, const std::string& kind, bool wr) {
    const auto rows = read.one("pragma_table_list WHERE schema='main' AND name=? COLLATE BINARY LIMIT 1", {table},
        {{"type","kind",false,false,16}, {"wr","wr",true}, {"strict","strict",true}});
    check(rows.size() == 1 && bytes(rows[0],"kind") == kind && integer(rows[0],"wr") == (wr ? 1 : 0) &&
          integer(rows[0],"strict") == 0, "derived table kind/layout refused");
}
void indexes(metadata_reader& read, const inventory& all, const table_shape& shape,
             std::set<std::string>& allowed) {
    const auto list = read.one("pragma_index_list(?, 'main') LIMIT 1", {shape.name},
        {{"seq","seq",true}, {"name","name"}, {"\"unique\"","uniq",true},
         {"origin","origin",false,false,8}, {"partial","partial",true}});
    if (shape.index_keys.empty()) {
        check(list.empty(), "derived unexpected index"); return;
    }
    check(list.size() == 1 && integer(list[0],"seq") == 0 && integer(list[0],"uniq") == 1 &&
          integer(list[0],"partial") == 0 && bytes(list[0],"origin") == shape.index_origin,
          "derived index origin/shape refused");
    const auto name = bytes(list[0],"name");
    check(name == "sqlite_autoindex_" + shape.name + "_1", "derived automatic index name refused");
    const auto more = read.one("pragma_index_list(?, 'main') LIMIT 1 OFFSET 1", {shape.name}, {{"seq","seq",true}});
    check(more.empty(), "derived extra index refused");
    // WITHOUT ROWID primary keys have an index_list entry but no separate
    // sqlite_schema index object. Ordinary implicit indexes have NULL SQL.
    if (!shape.without_rowid) {
        const auto& fact = object(all,name,"index",shape.name);
        check(fact.sql.empty(), "derived automatic index SQL refused"); allowed.insert(name);
    } else check(all.find(name) == all.end(), "derived WITHOUT ROWID extra index object");
    std::vector<column_shape> order;
    for (const auto& key : shape.index_keys) {
        const auto i = std::find_if(shape.columns.begin(),shape.columns.end(),[&](const auto& c){return c.name == key;});
        check(i != shape.columns.end(), "derived expected index key missing"); order.push_back(*i);
    }
    if (shape.without_rowid) {
        for (const auto& c : shape.columns)
            if (std::find(shape.index_keys.begin(),shape.index_keys.end(),c.name) == shape.index_keys.end()) order.push_back(c);
    } else order.push_back({"",""}); // physical rowid auxiliary column
    for (size_t offset = 0; offset <= order.size(); ++offset) {
        const auto rows = read.one("pragma_index_xinfo(?, 'main') LIMIT 1 OFFSET " + std::to_string(offset), {name},
            {{"seqno","seq",true}, {"cid","cid",true}, {"name","name",false,true,256},
             {"\"desc\"","descending",true}, {"coll","coll",false,false,16}, {"\"key\"","key",true}});
        if (offset == order.size()) { check(rows.empty(), "derived extra index column"); break; }
        check(rows.size() == 1, "derived index column missing"); const auto& row = rows[0];
        const auto& expected = order[offset]; int64_t cid = -1;
        if (!expected.name.empty()) cid = static_cast<int64_t>(std::find_if(shape.columns.begin(),shape.columns.end(),
            [&](const auto& c){return c.name == expected.name;}) - shape.columns.begin());
        check(integer(row,"seq") == static_cast<int64_t>(offset) && integer(row,"cid") == cid &&
              integer(row,"descending") == 0 && lower(bytes(row,"coll")) == "binary" &&
              integer(row,"key") == (offset < shape.index_keys.size() ? 1 : 0) &&
              (expected.name.empty() ? null_cell(row,"name") : bytes(row,"name") == expected.name),
              "derived index columns refused");
    }
}
std::vector<table_shape> shadows(const derived_field& field) {
    const auto& t = field.table;
    if (field.spec.kind == derived_kind::fts5_porter_v1) return {
        {t+"_data", "(id INTEGER PRIMARY KEY,block BLOB)", {{"id","integer",0,1},{"block","blob"}}},
        {t+"_idx", "(segid,term,pgno,PRIMARY KEY(segid,term)) WITHOUT ROWID",
            {{"segid","",1,1},{"term","",1,2},{"pgno",""}},true,"pk",{"segid","term"}},
        {t+"_docsize", "(id INTEGER PRIMARY KEY,sz BLOB)", {{"id","integer",0,1},{"sz","blob"}}},
        {t+"_config", "(k PRIMARY KEY,v) WITHOUT ROWID", {{"k","",1,1},{"v",""}},true,"pk",{"k"}}
    };
    return {
        {t+"_info", "(key text primary key,value any)", {{"key","text",0,1},{"value","any"}},false,"pk",{"key"}},
        {t+"_chunks", "(chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,size INTEGER NOT NULL,validity BLOB NOT NULL,rowids BLOB NOT NULL)",
            {{"chunk_id","integer",0,1},{"size","integer",1},{"validity","blob",1},{"rowids","blob",1}}},
        {t+"_rowids", "(rowid INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,chunk_id INTEGER,chunk_offset INTEGER)",
            {{"rowid","integer",0,1},{"id","text",1},{"chunk_id","integer"},{"chunk_offset","integer"}},false,"u",{"id"}},
        {t+"_vector_chunks00", "(rowid PRIMARY KEY,vectors BLOB NOT NULL)",
            {{"rowid","",0,1},{"vectors","blob",1}},false,"pk",{"rowid"},false}
    };
}
void module_info(metadata_reader& read, const derived_field& f) {
    if (f.spec.kind == derived_kind::fts5_porter_v1) {
        const auto rows = read.one("main."+quote(f.table+"_config")+" LIMIT 1", {},
            {{"k","k",false,false,64},{"v","v",true}});
        check(rows.size() == 1 && bytes(rows[0],"k") == "version" && integer(rows[0],"v") == 4,
              "derived FTS format/config refused");
        check(read.one("main."+quote(f.table+"_config")+" LIMIT 1 OFFSET 1", {},
            {{"k","k",false,false,64}}).empty(), "derived extra FTS config refused"); return;
    }
    std::set<std::string> keys;
    const std::map<std::string,int64_t> integers{{"CREATE_VERSION_MAJOR",SQLITE_VEC_VERSION_MAJOR},
        {"CREATE_VERSION_MINOR",SQLITE_VEC_VERSION_MINOR},{"CREATE_VERSION_PATCH",SQLITE_VEC_VERSION_PATCH}};
    for (size_t offset = 0; offset <= 4; ++offset) {
        const auto rows = read.one("main."+quote(f.table+"_info")+" LIMIT 1 OFFSET "+std::to_string(offset), {},
            {{"key","k",false,false,64},{"typeof(value)","kind",false,false,16},
             {"CASE WHEN typeof(value)='text' THEN value END","text",false,true,64},
             {"CASE WHEN typeof(value)='integer' THEN value END","number",true,true}});
        if (offset == 4) { check(rows.empty(), "derived extra vector version metadata"); break; }
        check(rows.size() == 1, "derived vector version metadata missing"); const auto& r = rows[0];
        const auto key = bytes(r,"k"); check(keys.insert(key).second, "derived repeated vector version key");
        if (key == "CREATE_VERSION")
            check(bytes(r,"kind") == "text" && bytes(r,"text") == SQLITE_VEC_VERSION, "derived vector version refused");
        else {
            const auto i = integers.find(key);
            check(i != integers.end() && bytes(r,"kind") == "integer" && integer(r,"number") == i->second,
                  "derived vector version component refused");
        }
    }
}
int64_t cookie(const derived_query& query) {
    const auto r = query("PRAGMA main.schema_version", {});
    check(r.size() == 1, "derived retained schema view unavailable"); return integer(r[0],"schema_version");
}
} // namespace

derived_metadata validate_derived_schema(const derived_query& query,
    const derived_descriptor& expected, const derived_limits& b) {
    limits_ok(b); check(query && !expected.fields().empty() && expected.fields().size() <= b.properties,
                        "derived query/descriptor refused");
    derived_metadata out; out.schema_cookie = cookie(query); metadata_reader read{query,b};
    const auto all = read_inventory(read); std::set<std::string> checked_models;
    for (const auto& f : expected.fields()) {
        const auto& model = object(all,f.spec.model,"table",f.spec.model);
        check(!model.sql.empty(), "derived model declaration missing");
        table_kind(read,f.spec.model,"table",false);
        // Validate only this component's ordinary columns/identity. Complete
        // source schema and owner-trigger admission belong to later adapters.
        // Ordinary globalId's generated DEFAULT is outside this projection.
        const auto actual = read.one("pragma_table_xinfo(?, 'main') WHERE name=? COLLATE BINARY LIMIT 1",
            {f.spec.model,f.spec.property}, {{"type","kind",false,false,32},{"\"notnull\"","nn",true},
             {"pk","pk",true},{"hidden","hidden",true}});
        check(actual.size() == 1 && lower(bytes(actual[0],"kind")) ==
              (f.spec.kind == derived_kind::fts5_porter_v1 ? "text" : "blob") &&
              integer(actual[0],"nn") == (f.nullable ? 0 : 1) && integer(actual[0],"pk") == 0 &&
              integer(actual[0],"hidden") == 0, "derived ordinary field metadata refused");
        if (checked_models.insert(f.spec.model).second) {
            const auto model_columns = columns(read,f.spec.model,true);
            size_t primary = 0; bool id = false;
            for (const auto& c : model_columns) {
                if (c.pk) ++primary;
                if (c.name == "id") id = c.type == "integer" && c.pk == 1 && c.hidden == 0;
            }
            check(id && primary == 1, "derived model keyset identity refused");
            // An INTEGER PRIMARY KEY DESC can have a separate primary index
            // and is not a true rowid alias. Reject it rather than skip NULL
            // or repeated IDs during the explicit initial keyset scan.
            check(read.one("pragma_index_list(?, 'main') WHERE origin='pk' LIMIT 1", {f.spec.model},
                {{"seq","seq",true}}).empty(), "derived model id is not a rowid alias");
        }
        std::set<std::string> allowed{f.table};
        const auto& virtual_table = object(all,f.table,"table",f.table);
        check(virtual_table.sql == f.create_sql, "derived module/options/dimensions mismatch");
        out.objects.push_back(virtual_table);
        const bool vector = f.spec.kind == derived_kind::vec0_flat_f32_v1;
        table_kind(read,f.table,"virtual",vector);
        match_columns(columns(read,f.table), vector ? std::vector<column_shape>{
            {"global_id","",1,1},{"embedding",""},{f.table,"",0,0,1},{"distance","",0,0,1},{"k","",0,0,1}}
            : std::vector<column_shape>{{f.spec.property,""},{f.table,"",0,0,1},{"rank","",0,0,1}});
        for (const auto& trigger : f.triggers) {
            const auto& fact = object(all,trigger.name,"trigger",trigger.owner_table);
            check(fact.sql == stored_trigger(trigger.sql), "derived generated trigger mismatch");
            allowed.insert(trigger.name); out.objects.push_back(fact);
        }
        for (const auto& shape : shadows(f)) {
            const auto& fact = object(all,shape.name,"table",shape.name);
            check(ddl_tokens(fact.sql) == ddl_tokens("CREATE TABLE "+quote(shape.name)+shape.declaration),
                  "derived shadow table declaration mismatch");
            allowed.insert(shape.name); out.objects.push_back(fact);
            // This sqlite-vec revision does not list vector_chunks00 in
            // xShadowName; it is module-owned but SQLite reports "table".
            table_kind(read,shape.name,shape.sqlite_shadow ? "shadow" : "table",shape.without_rowid);
            match_columns(columns(read,shape.name),shape.columns); indexes(read,all,shape,allowed);
        }
        if (vector) (void)object(all,"sqlite_sequence","table","sqlite_sequence");
        const auto folded_table = lower(f.table);
        for (const auto& pair : all) {
            const auto& fact = pair.second;
            const auto name = lower(fact.name), owner = lower(fact.owner);
            // SQLite compares identifiers with ASCII case folding. Exact
            // required programs stay byte-bound, but alternate capitalization
            // cannot make an extra family object appear unrelated.
            const bool reserved = name.compare(0,folded_table.size()+1,folded_table+"_") == 0;
            const bool owned = owner == folded_table ||
                owner.compare(0,folded_table.size()+1,folded_table+"_") == 0;
            check(!(reserved || owned) || allowed.count(fact.name), "derived extra family object refused");
        }
        module_info(read,f);
    }
    check(cookie(query) == out.schema_cookie, "derived schema view changed during validation");
    out.inspected_rows = read.rows; out.copied_bytes = read.copied; return out;
}
derived_metadata validate_derived_schema(database& db, const derived_descriptor& expected, const derived_limits& b) {
    return validate_derived_schema([&](const std::string& sql,const std::vector<column_value_t>& p){return db.query(sql,p);},expected,b);
}

derived_value_usage validate_initial_derived_values(const derived_query& query,
    const derived_descriptor& expected, const derived_limits& b) {
    // Admission is explicitly requested here, never at ordinary reopen or on
    // a hot mutation. Metadata and every row share the caller's retained view.
    (void)validate_derived_schema(query,expected,b);
    derived_value_usage usage; std::set<std::string> models;
    for (const auto& f : expected.fields()) models.insert(f.spec.model);
    for (const auto& model : models) {
        bool first = true; int64_t last = 0;
        for (;;) {
            const auto ids = query("SELECT id FROM main." + quote(model) +
                (first ? "" : " WHERE id>?") + " ORDER BY id LIMIT 1",
                first ? std::vector<column_value_t>{} : std::vector<column_value_t>{last});
            check(ids.size() <= 1, "derived initial keyset query refused"); if (ids.empty()) break;
            check(usage.rows < b.initial_rows, "derived initial row cap exceeded");
            const auto id = integer(ids[0],"id"); check(first || id > last, "derived initial cursor refused");
            first = false; last = id; database::row_t values; size_t row_bytes = 0;
            for (const auto& f : expected.fields()) {
                if (f.spec.model != model) continue;
                const auto col = quote(f.spec.property);
                const auto remaining = b.total_value_bytes - usage.bytes - row_bytes;
                const auto cap = std::min(b.value_bytes,remaining);
                const bool vector = f.spec.kind == derived_kind::vec0_flat_f32_v1;
                std::string valid = "(typeof(" + col + ")='" + (vector ? "blob" : "text") +
                    "' AND length(CAST(" + col + " AS BLOB))<=" + std::to_string(cap);
                if (vector) valid += " AND length(" + col + ") IN (0," + std::to_string(4 * f.spec.dimensions) + ")";
                valid += ")"; if (f.nullable) valid += " OR " + col + " IS NULL";
                const auto rows = query("SELECT CASE WHEN " + valid + " THEN 1 ELSE 0 END AS ok,"
                    "typeof(" + col + ") AS kind,CASE WHEN " + valid + " THEN CAST(" + col +
                    " AS BLOB) END AS value FROM main." + quote(model) + " WHERE id=? LIMIT 1", {id});
                check(rows.size() == 1 && integer(rows[0],"ok") == 1, "derived initial value type/copy cap refused");
                const auto& kind_value = cell(rows[0],"kind");
                check(std::holds_alternative<std::string>(kind_value), "derived initial type result refused");
                const auto& kind = std::get<std::string>(kind_value);
                if (kind == "null") values.emplace(f.spec.property,nullptr);
                else {
                    const auto& v = cell(rows[0],"value");
                    check(std::holds_alternative<std::vector<uint8_t>>(v), "derived initial byte result refused");
                    const auto& blob = std::get<std::vector<uint8_t>>(v);
                    check(blob.size() <= remaining, "derived initial charge refused"); row_bytes += blob.size();
                    if (vector) values.emplace(f.spec.property,blob);
                    else values.emplace(f.spec.property,blob.empty() ? std::string{} :
                        std::string(reinterpret_cast<const char*>(blob.data()),blob.size()));
                }
            }
            validate_derived_values(expected,model,values,usage,b);
        }
    }
    return usage;
}
derived_value_usage validate_initial_derived_values(database& db, const derived_descriptor& expected,
    const derived_limits& b) {
    return validate_initial_derived_values([&](const std::string& sql,const std::vector<column_value_t>& p){return db.query(sql,p);},expected,b);
}

} // namespace lattice::detail
