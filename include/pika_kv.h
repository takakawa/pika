// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_KV_H_
#define PIKA_KV_H_

#if 0
#include "blackwidow/blackwidow.h"
#endif

#include "include/pika_command.h"
#include "include/pika_partition.h"


/*
 * kv
 */
class SetCmd : public Cmd {
 public:
  enum SetCondition {kNONE, kNX, kXX, kVX, kEXORPX};
  SetCmd(const std::string& name , int arity, uint16_t flag)
      : Cmd(name, arity, flag), sec_(0), condition_(kNONE) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);

 private:
  std::string key_;
  std::string value_;
  std::string target_;
  int32_t success_;
  int64_t sec_;
  SetCmd::SetCondition condition_;
  virtual void DoInitial() override;
  virtual void Clear() override {
    sec_ = 0;
    success_ = 0;
    condition_ = kNONE;
  }
  virtual std::string ToBinlog(
      uint32_t exec_time,
      const std::string& server_id,
      uint64_t logic_id,
      uint32_t filenum,
      uint64_t offset) override;
};


class DelCmd : public Cmd {
 public:
  DelCmd(const std::string& name , int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::vector<std::string> keys_;
  virtual void DoInitial() override;
};


#endif
