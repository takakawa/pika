// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include "include/pika_command.h"

class PikaClientConn: public pink::RedisConn {
 public:
  struct BgTaskArg {
    std::shared_ptr<PikaClientConn> pcc;
    std::vector<pink::RedisCmdArgsType> redis_cmds;
    std::string* response;
  };

  PikaClientConn(int fd, std::string ip_port,
                 pink::Thread *server_thread,
                 pink::PinkEpoll* pink_epoll,
                 const pink::HandleType& handle_type);
  virtual ~PikaClientConn() {}

  void AsynProcessRedisCmds(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response) override;

  void BatchExecRedisCmd(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response);
  int DealMessage(const pink::RedisCmdArgsType& argv, std::string* response);
  static void DoBackgroundTask(void* arg);


 private:
  pink::ServerThread* const server_thread_;
  std::string current_table_;

  std::string DoCmd(const PikaCmdArgsType& argv, const std::string& opt);

};

struct ClientInfo {
  int fd;
  std::string ip_port;
  int64_t last_interaction;
  std::shared_ptr<PikaClientConn> conn;
};

#endif
