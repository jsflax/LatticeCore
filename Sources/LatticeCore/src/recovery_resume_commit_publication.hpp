#pragma once
namespace lattice {
class lattice_db;class database;
namespace detail {
// Called only by the common owning engine after successful own COMMIT and
// verified settlement, while its actual Native/store admission remains held.
// Production has no callback. An unarmed, unrelated or consumed producer frame
// does nothing; only an exact armed resume may publish its denial metadata.
void publish_controller_resume_after_owned_commit(lattice_db&,database&);
}
}
