#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE 1
#endif
#define _POSIX_C_SOURCE 200809L
#include <sqlite3.h>
#include <errno.h>
#include <fcntl.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t stopping=0;
static void stop(int ignored){(void)ignored;stopping=1;}
static int elapsed(struct timespec start){struct timespec now;if(clock_gettime(CLOCK_MONOTONIC,&now)!=0)return 1;return now.tv_sec-start.tv_sec>=240;}
int main(void){
    struct passwd value,*found=0;char buffer[16384],directory[4096],path[4096];
    if(getpwuid_r(geteuid(),&value,buffer,sizeof(buffer),&found)!=0||!found||!found->pw_dir||found->pw_dir[0]!='/')return 74;
    if(snprintf(directory,sizeof(directory),"%s/.claude",found->pw_dir)>=(int)sizeof(directory)||
       snprintf(path,sizeof(path),"%s/memory.sqlite",directory)>=(int)sizeof(path))return 74;
    umask(0077);if(mkdir(directory,0700)!=0&&errno!=EEXIST)return 74;
    struct stat parent;if(lstat(directory,&parent)!=0||!S_ISDIR(parent.st_mode)||parent.st_uid!=geteuid()||(parent.st_mode&0022))return 74;
    /* NEW empty MAIN only, before any SQLite user exists. No extra descriptor
       for an existing SQLite MAIN is ever opened/closed by this control. */
    int fresh=open(path,O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);if(fresh<0)return 74;if(close(fresh)!=0)return 74;
    struct sigaction action;memset(&action,0,sizeof(action));action.sa_handler=stop;sigemptyset(&action.sa_mask);
    if(sigaction(SIGTERM,&action,0)!=0||sigaction(SIGINT,&action,0)!=0)return 74;
    sqlite3* db=0;int opened=sqlite3_open_v2(path,&db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX,0);
    if(opened!=SQLITE_OK){if(db)sqlite3_close(db);return 74;}
    int rc=sqlite3_exec(db,"PRAGMA journal_mode=WAL;CREATE TABLE resource_bootstrap(globalId TEXT NOT NULL UNIQUE,value INTEGER NOT NULL);BEGIN;INSERT INTO resource_bootstrap VALUES('old',41);COMMIT;",0,0,0);
    if(rc!=SQLITE_OK){sqlite3_close(db);return 74;}
    struct timespec start;if(clock_gettime(CLOCK_MONOTONIC,&start)!=0){sqlite3_close(db);return 74;}
    puts("resource-prior-unenrolled-writer:committed41-and-live");if(fflush(stdout)!=0){sqlite3_close(db);return 74;}
    while(!stopping&&!elapsed(start)){const struct timespec pause={0,10000000};nanosleep(&pause,0);}
    const int closed=sqlite3_close(db);printf("resource-prior-unenrolled-writer:close=%d;signal=%d\n",closed,(int)stopping);fflush(stdout);
    return stopping&&closed==SQLITE_OK?0:74;
}
