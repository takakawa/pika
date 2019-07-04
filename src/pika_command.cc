// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_command.h"

#include "include/pika_kv.h"
#include "include/pika_admin.h"
#include "include/pika_pubsub.h"
#include "include/pika_server.h"

extern PikaServer* g_pika_server;

void InitCmdTable(std::unordered_map<std::string, Cmd*> *cmd_table) {

  //Kv
  ////SetCmd
  Cmd* setptr = new SetCmd(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSet, setptr));
  ////DelCmd
  Cmd* delptr = new DelCmd(kCmdNameDel, -2, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDel, delptr));
}

Cmd* GetCmdFromTable(const std::string& opt, const CmdTable& cmd_table) {
  CmdTable::const_iterator it = cmd_table.find(opt);
  if (it != cmd_table.end()) {
    return it->second;
  }
  return NULL;
}

void DestoryCmdTable(CmdTable* cmd_table) {
  CmdTable::const_iterator it = cmd_table->begin();
  for (; it != cmd_table->end(); ++it) {
    delete it->second;
  }
}

void Cmd::Initial(const PikaCmdArgsType& argv,
                  const std::string& table_name) {
  argv_ = argv;
  table_name_ = table_name;
  res_.clear(); // Clear res content
  Clear();      // Clear cmd, Derived class can has own implement
  DoInitial();
};

std::string Cmd::current_key() const {
  return "";
}

void Cmd::Execute() {
  if (name_ == kCmdNameFlushdb) {
    ProcessFlushDBCmd();
  } else if (name_ == kCmdNameFlushall) {
    ProcessFlushAllCmd();
  } else if (name_ == kCmdNameInfo || name_ == kCmdNameConfig) {
    ProcessDoNotSpecifyPartitionCmd();
  } else if (is_single_partition() || g_pika_conf->classic_mode()) {
    ProcessSinglePartitionCmd();
  } else if (is_multi_partition()) {
    ProcessMultiPartitionCmd();
  } else {
    ProcessDoNotSpecifyPartitionCmd();
  }
}

void Cmd::ProcessFlushDBCmd() {
  std::shared_ptr<Table> table = g_pika_server->GetTable(table_name_);
  if (!table) {
    res_.SetRes(CmdRes::kInvalidTable);
  } else {
    if (table->IsKeyScaning()) {
      res_.SetRes(CmdRes::kErrOther, "The keyscan operation is executing, Try again later");
    } else {
      slash::RWLock l_prw(&table->partitions_rw_, true);
      for (const auto& partition_item : table->partitions_) {
        partition_item.second->DoCommand(this);
      }
      res_.SetRes(CmdRes::kOk);
    }
  }
}

void Cmd::ProcessFlushAllCmd() {
  slash::RWLock l_trw(&g_pika_server->tables_rw_, true);
  for (const auto& table_item : g_pika_server->tables_) {
    if (table_item.second->IsKeyScaning()) {
      res_.SetRes(CmdRes::kErrOther, "The keyscan operation is executing, Try again later");
      return;
    }
  }

  for (const auto& table_item : g_pika_server->tables_) {
    slash::RWLock l_prw(&table_item.second->partitions_rw_, true);
    for (const auto& partition_item : table_item.second->partitions_) {
      partition_item.second->DoCommand(this);
    }
  }
  res_.SetRes(CmdRes::kOk);
}

void Cmd::ProcessSinglePartitionCmd() {
  std::shared_ptr<Partition> partition;
  if (is_single_partition()) {
    partition = g_pika_server->GetTablePartitionByKey(table_name_, current_key());
  } else {
    partition = g_pika_server->GetTablePartitionById(table_name_, 0);
  }

  if (!partition) {
    res_.SetRes(CmdRes::kErrOther, "Partition not found");
    return;
  }
  partition->DoCommand(this);
}

void Cmd::ProcessMultiPartitionCmd() {
  LOG(INFO) << "Process Multi partition Cmd? -> " << name_;
}

void Cmd::ProcessDoNotSpecifyPartitionCmd() {
  Do();
}

bool Cmd::is_write() const {
  return ((flag_ & kCmdFlagsMaskRW) == kCmdFlagsWrite);
}
bool Cmd::is_local() const {
  return ((flag_ & kCmdFlagsMaskLocal) == kCmdFlagsLocal);
}
// Others need to be suspended when a suspend command run
bool Cmd::is_suspend() const {
  return ((flag_ & kCmdFlagsMaskSuspend) == kCmdFlagsSuspend);
}
// Must with admin auth
bool Cmd::is_admin_require() const {
  return ((flag_ & kCmdFlagsMaskAdminRequire) == kCmdFlagsAdminRequire);
}
bool Cmd::is_single_partition() const {
  return ((flag_ & kCmdFlagsMaskPartition) == kCmdFlagsSinglePartition);
}
bool Cmd::is_multi_partition() const {
  return ((flag_ & kCmdFlagsMaskPartition) == kCmdFlagsMultiPartition);
}

std::string Cmd::name() const {
  return name_;
}
CmdRes& Cmd::res() {
  return res_;
}

std::string Cmd::ToBinlog(uint32_t exec_time,
                          const std::string& server_id,
                          uint64_t logic_id,
                          uint32_t filenum,
                          uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, argv_.size(), "*");

  for (const auto& v : argv_) {
    RedisAppendLen(content, v.size(), "$");
    RedisAppendContent(content, v);
  }

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

bool Cmd::CheckArg(int num) const {
  if ((arity_ > 0 && num != arity_)
    || (arity_ < 0 && num < -arity_)) {
    return false;
  }
  return true;
}

void Cmd::LogCommand() const {
  std::string command;
  for (const auto& item : argv_) {
    command.append(" ");
    command.append(item);
  }
  LOG(INFO) << "command:" << command;
}
