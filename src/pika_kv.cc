// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_kv.h"
#include <thread>
#include "slash/include/slash_string.h"

#include "include/pika_binlog_transverter.h"

/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void SetCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  key_ = argv_[1];
  value_ = argv_[2];
  condition_ = SetCmd::kNONE;
#if 0
  sec_ = 0;
  size_t index = 3;
  while (index != argv_.size()) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "xx")) {
      condition_ = SetCmd::kXX;
    } else if (!strcasecmp(opt.data(), "nx")) {
      condition_ = SetCmd::kNX;
    } else if (!strcasecmp(opt.data(), "vx")) {
      condition_ = SetCmd::kVX;
      index++;
      if (index == argv_.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      } else {
        target_ = argv_[index];
      }
    } else if (!strcasecmp(opt.data(), "ex") || !strcasecmp(opt.data(), "px")) {
      condition_ = (condition_ == SetCmd::kNONE) ? SetCmd::kEXORPX : condition_;
      index++;
      if (index == argv_.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!slash::string2l(argv_[index].data(), argv_[index].size(), &sec_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      } else if (sec_ <= 0) {
        res_.SetRes(CmdRes::kErrOther, "invalid expire time in set");
        return;
      }

      if (!strcasecmp(opt.data(), "px")) {
        sec_ /= 1000;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
#endif
  return;
}

void SetCmd::Do(std::shared_ptr<Partition> partition) {
//   printf("[%]SetCmd write binlog..[%s]->[%s]\n",key_.data(),value_.data());
   std::cout << std::this_thread::get_id() << " | " << "SetCmd write binlog. " << key_ << " -> " << value_<< std::endl;
#if 0
  rocksdb::Status s;
  int32_t res = 1;
  switch (condition_) {
    case SetCmd::kXX:
      s = partition->db()->Setxx(key_, value_, &res, sec_);
      break;
    case SetCmd::kNX:
      s = partition->db()->Setnx(key_, value_, &res, sec_);
      break;
    case SetCmd::kVX:
      s = partition->db()->Setvx(key_, target_, value_, &success_, sec_);
      break;
    case SetCmd::kEXORPX:
      s = partition->db()->Setex(key_, value_, sec_);
      break;
    default:
      s = partition->db()->Set(key_, value_);
      break;
  }

  if (s.ok() || s.IsNotFound()) {
    if (condition_ == SetCmd::kVX) {
      res_.AppendInteger(success_);
    } else {
      if (res == 1) {
        res_.SetRes(CmdRes::kOk);
      } else {
        res_.AppendArrayLen(-1);;
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
#endif
}

std::string SetCmd::ToBinlog(
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) {
  if (condition_ == SetCmd::kEXORPX) {
    std::string content;
    content.reserve(RAW_ARGS_LEN);
    RedisAppendLen(content, 4, "*");

    // to pksetexat cmd
    std::string pksetexat_cmd("pksetexat");
    RedisAppendLen(content, pksetexat_cmd.size(), "$");
    RedisAppendContent(content, pksetexat_cmd);
    // key
    RedisAppendLen(content, key_.size(), "$");
    RedisAppendContent(content, key_);
    // time_stamp
    char buf[100];
    int32_t time_stamp = time(nullptr) + sec_;
    slash::ll2string(buf, 100, time_stamp);
    std::string at(buf);
    RedisAppendLen(content, at.size(), "$");
    RedisAppendContent(content, at);
    // value
    RedisAppendLen(content, value_.size(), "$");
    RedisAppendContent(content, value_);
    return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                               exec_time,
                                               std::stoi(server_id),
                                               logic_id,
                                               filenum,
                                               offset,
                                               content,
                                               {});
  } else {
    return Cmd::ToBinlog(exec_time, server_id, logic_id, filenum, offset);
  }
}

void DelCmd::DoInitial() {
#if 0
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDel);
    return;
  }
  std::vector<std::string>::iterator iter = argv_.begin();
  keys_.assign(++iter, argv_.end());
  return;
#endif
}

void DelCmd::Do(std::shared_ptr<Partition> partition) {
   printf("del cmd write binlog...\n");
#if 0
  std::map<blackwidow::DataType, blackwidow::Status> type_status;
  int64_t count = partition->db()->Del(keys_, &type_status);
  if (count >= 0) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, "delete error");
  }
  return;
#endif
}
