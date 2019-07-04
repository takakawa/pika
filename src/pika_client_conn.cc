// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_client_conn.h"

#include <vector>
#include <algorithm>

#include <glog/logging.h>

#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_cmd_table_manager.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaCmdTableManager* g_pika_cmd_table_manager;


PikaClientConn::PikaClientConn(int fd, std::string ip_port,
                               pink::Thread* thread,
                               pink::PinkEpoll* pink_epoll,
                               const pink::HandleType& handle_type)
      : RedisConn(fd, ip_port, thread, pink_epoll, handle_type),
        server_thread_(reinterpret_cast<pink::ServerThread*>(thread)),
        current_table_(g_pika_conf->default_table()){
}

std::string PikaClientConn::DoCmd(const PikaCmdArgsType& argv,
                                  const std::string& opt) {
  // Get command info
  Cmd* c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
  if (!c_ptr) {
      return "-Err unknown or unsupported command \'" + opt + "\'\r\n";
  }


  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  // For now, only shutdown need check local
  if (c_ptr->is_local()) {
    if (ip_port().find("127.0.0.1") == std::string::npos
        && ip_port().find(g_pika_server->host()) == std::string::npos) {
      LOG(WARNING) << "\'shutdown\' should be localhost";
      return "-ERR \'shutdown\' should be localhost\r\n";
    }
  }


  // Initial
  c_ptr->Initial(argv, current_table_);
  if (!c_ptr->res().ok()) {
    return c_ptr->res().message();
  }

  g_pika_server->UpdateQueryNumAndExecCountTable(opt);
 

  if (!g_pika_server->IsCommandSupport(opt)) {
    return "-ERR This command only support in classic mode\r\n";
  }

  if (!g_pika_server->IsTableExist(current_table_)) {
    return "-ERR Table not found\r\n";
  }

  // TODO: Consider special commands, like flushall, flushdb?
  if (c_ptr->is_write()) {
    if (g_pika_server->IsTableBinlogIoError(current_table_)) {
      return "-ERR Writing binlog failed, maybe no space left on device\r\n";
    }
    if (g_pika_server->readonly()) {
      return "-ERR Server in read-only\r\n";
    }
  }

  // Process Command
  c_ptr->Execute();

  return c_ptr->res().message();
}

void PikaClientConn::AsynProcessRedisCmds(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response) {
  BgTaskArg* arg = new BgTaskArg();
  arg->redis_cmds = argvs;
  arg->response = response;
  arg->pcc = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
  g_pika_server->Schedule(&DoBackgroundTask, arg);
}

void PikaClientConn::BatchExecRedisCmd(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response) {
  bool success = true;
  for (const auto& argv : argvs) {
    if (DealMessage(argv, response) != 0) {
      success = false;
      break;
    }
  }
  if (!response->empty()) {
    set_is_reply(true);
    NotifyEpoll(success);
  }
}

int PikaClientConn::DealMessage(const PikaCmdArgsType& argv, std::string* response) {

  if (argv.empty()) return -2;
  std::string opt = argv[0];
  slash::StringToLower(opt);

  if (response->empty()) {
    // Avoid memory copy
    *response = std::move(DoCmd(argv, opt));
  } else {
    // Maybe pipeline
    response->append(DoCmd(argv, opt));
  }
  return 0;
}

void PikaClientConn::DoBackgroundTask(void* arg) {
  BgTaskArg* bg_arg = reinterpret_cast<BgTaskArg*>(arg);
  bg_arg->pcc->BatchExecRedisCmd(bg_arg->redis_cmds, bg_arg->response);
  delete bg_arg;
}


