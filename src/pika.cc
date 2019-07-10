// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <signal.h>
#include <glog/logging.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <fcntl.h> 
#include <unistd.h>
#include <iostream>
#include "slash/include/env.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/pika_version.h"
#include "include/pika_cmd_table_manager.h"

#ifdef TCMALLOC_EXTENSION
#include <gperftools/malloc_extension.h>
#endif

#include <thread>

PikaConf* g_pika_conf;
PikaServer* g_pika_server;
PikaReplicaManager* g_pika_rm;

PikaCmdTableManager* g_pika_cmd_table_manager;

static void version() {
    char version[32];
    snprintf(version, sizeof(version), "%d.%d.%d", PIKA_MAJOR,
        PIKA_MINOR, PIKA_PATCH);
    printf("-----------Pika server %s ----------\n", version);
}

static void PikaConfInit(const std::string& path) {
  printf("path : %s\n", path.c_str());
  g_pika_conf = new PikaConf(path);
  if (g_pika_conf->Load() != 0) {
    LOG(FATAL) << "pika load conf error";
  }
  version();
  printf("-----------Pika config list----------\n");
  g_pika_conf->DumpConf();
  printf("-----------Pika config end----------\n");
}

static void PikaGlogInit() {
  if (!slash::FileExists(g_pika_conf->log_path())) {
    slash::CreatePath(g_pika_conf->log_path()); 
  }

  FLAGS_alsologtostderr = true;
  FLAGS_log_dir = g_pika_conf->log_path();
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("pika");
}

#if 0
static void daemonize() {
  if (fork() != 0) exit(0); /* parent exits */
  setsid(); /* create a new session */
}
#endif
#if 0
static void close_std() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}
#endif
#if 0
static void create_pid_file(void) {
  /* Try to write the pid file in a best-effort way. */
  std::string path(g_pika_conf->pidfile());

  size_t pos = path.find_last_of('/');
  if (pos != std::string::npos) {
    // mkpath(path.substr(0, pos).c_str(), 0755);
    slash::CreateDir(path.substr(0, pos));
  } else {
    path = kPikaPidFile;
  }

  FILE *fp = fopen(path.c_str(), "w");
  if (fp) {
    fprintf(fp,"%d\n",(int)getpid());
    fclose(fp);
  }
}
#endif

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  g_pika_server->Exit();
}

static void PikaSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void usage()
{
    char version[32];
    snprintf(version, sizeof(version), "%d.%d.%d", PIKA_MAJOR,
        PIKA_MINOR, PIKA_PATCH);
    fprintf(stderr,
            "Pika module %s\n"
            "usage: pika [-hv] [-c conf/file]\n"
            "\t-h               -- show this help\n"
            "\t-c conf/file     -- config file \n"
            "  example: ./output/bin/pika -c ./conf/pika.conf\n",
            version
           );
}


std::map<std::string, std::shared_ptr<std::atomic<int>>>  partitionSeq ;
void SimulateWriteCmd(std::string partition, std::string cmd,  std::string keyprefix, std::string valprefix){

  std::string slaveof = g_pika_conf->slaveof(); 
  
  fprintf(stderr, " slaveof  %s\n",slaveof.data());
  std::shared_ptr<std::atomic<int>> i = partitionSeq[partition];
  
  *i  = 0;
  while( slaveof.empty()){

          int seq = i->load(std::memory_order_relaxed);
          (*i)++;
          std::this_thread::sleep_for (std::chrono::milliseconds(100));
          char key[100] = {0 };
          sprintf(key, "%s%d",keyprefix.data(),seq);
          char val[100] = {0 };
          sprintf(val, "%s%d",valprefix.data(),seq);
	  PikaCmdArgsType argv = {cmd,key,val};
	  Cmd* c_ptr = g_pika_cmd_table_manager->GetCmd(cmd);

	  // Initial
	  c_ptr->Initial(argv, partition);
	  if (!c_ptr->res().ok()) {
	    fprintf(stderr,c_ptr->res().message().data());
	    exit(-1);
	  }
	  c_ptr->Execute();
  }
}

int main(int argc, char *argv[]) {
  if (argc != 2 && argc != 3) {
    usage();
    exit(-1);
  }

  bool path_opt = false;
  char c;
  char path[1024];
  while (-1 != (c = getopt(argc, argv, "c:hv"))) {
    switch (c) {
      case 'c':
        snprintf(path, 1024, "%s", optarg);
        path_opt = true;
        break;
      case 'h':
        usage();
        return 0;
      case 'v':
        version();
        return 0;
      default:
        usage();
        return 0;
    }
  }

  if (path_opt == false) {
    fprintf (stderr, "Please specify the conf file path\n" );
    usage();
    exit(-1);
  }
#ifdef TCMALLOC_EXTENSION
  MallocExtension::instance()->Initialize();
#endif
  PikaConfInit(path);

  PikaGlogInit();
  PikaSignalSetup();

  partitionSeq["db0"] = std::make_shared<std::atomic<int>>(0);
  partitionSeq["db1"] = std::make_shared<std::atomic<int>>(1);
  LOG(INFO) << "Server at: " << path;
  g_pika_server = new PikaServer();
  g_pika_rm = new PikaReplicaManager();
  g_pika_cmd_table_manager = new PikaCmdTableManager();

  std::thread ttdb1(SimulateWriteCmd, "db0", "set", "setdb0","setdb0v");
  std::thread ttdb2(SimulateWriteCmd, "db1", "set", "setdb1","setdb1v");
  std::thread ttdb3(SimulateWriteCmd, "db0", "del", "deldb0","deldb0v");
  std::thread ttdb4(SimulateWriteCmd, "db1", "del", "deldb1","deldb1v");
  g_pika_rm->Start();
  g_pika_server->Start();
  

  // stop PikaReplicaManager first，avoid internal threads
  // may references to dead PikaServer
  g_pika_rm->Stop();

  delete g_pika_server;
  delete g_pika_rm;
  delete g_pika_cmd_table_manager;
  ::google::ShutdownGoogleLogging();
  delete g_pika_conf;
  
  return 0;
}
