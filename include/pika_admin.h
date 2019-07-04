// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ADMIN_H_
#define PIKA_ADMIN_H_

#include <sstream>
#include <sys/time.h>
#include <sys/resource.h>
#include <iomanip>

#if 0
#include "blackwidow/blackwidow.h"
#endif

#include "include/pika_command.h"

/*
 * Admin
 */
class SlaveofCmd : public Cmd {
 public:
  SlaveofCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), is_noone_(false) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);

 private:
  std::string master_ip_;
  int64_t master_port_;
  bool is_noone_;
  virtual void DoInitial() override;
  virtual void Clear() {
    is_noone_ = false;
    master_ip_.clear();
    master_port_ = 0;
  }
};

class DbSlaveofCmd : public Cmd {
 public:
  DbSlaveofCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);

 private:
  std::string db_name_;
  bool force_sync_;
  bool is_noone_;
  bool have_offset_;
  int64_t filenum_;
  int64_t offset_;
  virtual void DoInitial() override;
  virtual void Clear() {
    db_name_.clear();
    force_sync_ = false;
    is_noone_ = false;
    have_offset_ = false;
  }
};


#endif
