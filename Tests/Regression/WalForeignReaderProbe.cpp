#include <lattice/lattice.hpp>
#include <sqlite3.h>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

struct Probe { std::string body; };
LATTICE_SCHEMA(Probe, body);

static void require(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
static void ack() { std::string line; require(bool(std::getline(std::cin, line)) && line == "continue", "missing controller acknowledgement"); }
static void sql(sqlite3* db, const char* text) { char* error=nullptr; int rc=sqlite3_exec(db,text,nullptr,nullptr,&error); if(rc!=SQLITE_OK) { std::string message=error?error:"SQLite failure"; sqlite3_free(error); throw std::runtime_error(message); } }
int main(int argc, char** argv) {
 try {
  require(argc==3,"mode and explicit isolated path required");
  const std::string mode=argv[1], path=argv[2];
  if(mode=="reader") {
   sqlite3* db=nullptr;
   require(sqlite3_open_v2(path.c_str(),&db,SQLITE_OPEN_READONLY|SQLITE_OPEN_FULLMUTEX,nullptr)==SQLITE_OK,"reader open failed");
   sqlite3_busy_timeout(db,1000);
   sql(db,"BEGIN"); sql(db,"SELECT count(*) FROM Probe");
   std::cout << "{\"event\":\"holding\"}" << std::endl;
   ack(); sql(db,"COMMIT"); require(sqlite3_close(db)==SQLITE_OK,"reader close failed");
   std::cout << "{\"event\":\"readerClosed\"}" << std::endl; return 0;
  }
  require(mode=="writer","unknown mode");
  require(!std::filesystem::exists(path),"isolated database already exists");
  lattice::configuration config(path); config.busy_timeout_ms=5000;
  lattice::lattice_db db(config);
  const std::string payload(4096, 'x');
  db.add(Probe{payload});
  db.set_wal_keeper_eviction_threshold_bytes(64*1024);
  std::cout << "{\"event\":\"ready\",\"sqliteVersion\":\"" << sqlite3_libversion() << "\",\"sqliteSourceID\":\"" << sqlite3_sourceid() << "\",\"setlkTimeoutCompileOption\":" << sqlite3_compileoption_used("ENABLE_SETLK_TIMEOUT") << "}" << std::endl;
  ack();
  int rows=1;
  for(int i=0;i<128 && !db.wal_eviction_pending();++i) { db.add(Probe{payload}); ++rows; }
  require(db.wal_eviction_pending(),"threshold did not become pending");
  for(int i=0;i<8;++i) {
   db.add(Probe{payload}); ++rows;
   require(db.wal_eviction_pending(),"post-threshold commit did not reflag");
   auto start=std::chrono::steady_clock::now();
   auto gen=db.acquire_read_generation();
   auto elapsed=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-start).count();
   require(gen!=0,"no generation acquired");
   require(elapsed<100,"foreground acquisition waited like the original checkpoint");
   require(db.wal_eviction_pending(),"busy foreground checkpoint lost maintenance request");
   auto result=db.query_at_generation(gen,"SELECT COUNT(*) AS c FROM Probe");
   require(result.has_value() && result->size()==1,"generation count missing");
   auto count=std::get<int64_t>((*result)[0].at("c"));
   require(count==rows,"generation returned wrong row count");
   db.release_read_generation(gen);
   std::cout << "{\"event\":\"cycle\",\"index\":" << i << ",\"acquireMilliseconds\":" << elapsed << ",\"rows\":" << count << ",\"expectedRows\":" << rows << ",\"pendingAfterAcquire\":" << (db.wal_eviction_pending()?"true":"false") << "}" << std::endl;
  }
  auto maintenanceStart=std::chrono::steady_clock::now();
  db.run_read_pool_maintenance();
  auto maintenanceMS=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-maintenanceStart).count();
  require(db.wal_eviction_pending(),"busy maintenance lost pending request");
  require(maintenanceMS>=200 && maintenanceMS<1000,"maintenance did not retain its bounded wait");
  std::cout << "{\"event\":\"maintenanceWhileHeld\",\"milliseconds\":" << maintenanceMS << ",\"pending\":true}" << std::endl;
  std::cout << "{\"event\":\"releaseReader\"}" << std::endl;
  ack();
  // No new write: foreground/maintenance busy attempts must preserve this obligation.
  require(db.wal_eviction_pending(),"final maintenance request was lost");
  db.retire_all_read_generations(); db.run_read_pool_maintenance();
  auto wal=std::filesystem::file_size(path+"-wal");
  require(wal<=64*1024,"maintenance failed to rewind after foreign reader release");
  require(!db.wal_eviction_pending(),"successful truncation did not clear pending");
  auto gen=db.acquire_read_generation(); require(gen!=0,"final generation failed");
  auto result=db.query_at_generation(gen,"SELECT COUNT(*) AS c FROM Probe");
  require(result && result->size()==1 && std::get<int64_t>((*result)[0].at("c"))==rows,"final rows wrong");
  db.release_read_generation(gen);
  std::cout << "{\"event\":\"finished\",\"walBytesAfterMaintenance\":" << wal << ",\"rows\":" << rows << ",\"pending\":" << (db.wal_eviction_pending()?"true":"false") << "}" << std::endl;
  db.close(); return 0;
 } catch(const std::exception& error) { std::cerr << error.what() << std::endl; return 1; }
}
