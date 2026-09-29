#pragma once
#include <lattice/db.hpp>

namespace lattice::detail {
// Trusted test fault injection and managed-read observation only.
// Public handle() deliberately retires
// canonical admission; using it to build an unrelated fault would otherwise
// make the existing counter/receipt/rollback tests pass for the wrong reason.
// No definition of this access seam is linked into a production target.
struct canonical_writer_custody_test_access {
    static sqlite3* fault_handle(database& writer) {return writer.internal_handle();}
    static std::optional<column_value_t> query_managed_cell(database& writer,
            const std::string& sql,const std::string& column,primary_key_t row_id) {
        return writer.query_managed_cell(sql,column,row_id);
    }
};
}
