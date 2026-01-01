#pragma once

// LatticeCpp - Cross-platform ORM with Sync
//
// Usage:
//   #include <LatticeCpp.hpp>
//
//   struct Trip {
//       std::string name;
//       int days;
//   };
//   LATTICE_SCHEMA(Trip, name, days);
//
//   int main() {
//       lattice::lattice_db db;  // in-memory, or db("path.db")
//
//       // Create - property writes go directly to database!
//       auto trip = db.create<Trip>();
//       trip.name = "Costa Rica";  // Persists immediately
//       trip.days = 10;            // No save() needed
//
//       // Query
//       for (auto& t : db.all<Trip>()) {
//           std::cout << t.name.detach() << std::endl;
//       }
//   }

#include "lattice/types.hpp"
#include "lattice/db.hpp"
#include "lattice/managed.hpp"
#include "lattice/schema.hpp"
#include "lattice/scheduler.hpp"
#include "lattice/network.hpp"
#include "lattice/sync.hpp"
#include "lattice/lattice.hpp"
