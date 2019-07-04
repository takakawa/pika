// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_admin.h"

#include <algorithm>
#include <sys/time.h>
#include <sys/utsname.h>

#include "slash/include/rsync.h"

#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_rm.h"
#include "include/pika_version.h"
#include "include/build_version.h"

#ifdef TCMALLOC_EXTENSION
#include <gperftools/malloc_extension.h>
#endif

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;
extern PikaReplicaManager *g_pika_rm;

/*
 * slaveof no one
 * slaveof ip port
 * slaveof ip port force
 */
void SlaveofCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    return;
  }

  if (argv_.size() == 3
    && !strcasecmp(argv_[1].data(), "no")
    && !strcasecmp(argv_[2].data(), "one")) {
    is_noone_ = true;
    return;
  }

  // self is master of A , want to slavof B
  if (g_pika_server->role() & PIKA_ROLE_MASTER) {
    res_.SetRes(CmdRes::kErrOther, "already master of others, invalid usage");
    return;
  }

  master_ip_ = argv_[1];
  std::string str_master_port = argv_[2];
  if (!slash::string2l(str_master_port.data(), str_master_port.size(), &master_port_) || master_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }

  if ((master_ip_ == "127.0.0.1" || master_ip_ == g_pika_server->host())
    && master_port_ == g_pika_server->port()) {
    res_.SetRes(CmdRes::kErrOther, "you fucked up");
    return;
  }

  if (argv_.size() == 4) {
    if (!strcasecmp(argv_[3].data(), "force")) {
      g_pika_server->SetForceFullSync(true);
    } else {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    }
  }
}

void SlaveofCmd::Do(std::shared_ptr<Partition> partition) {
  // Check if we are already connected to the specified master
  if ((master_ip_ == "127.0.0.1" || g_pika_server->master_ip() == master_ip_)
    && g_pika_server->master_port() == master_port_) {
    res_.SetRes(CmdRes::kOk);
    return;
  }

  g_pika_server->RemoveMaster();

  if (is_noone_) {
    res_.SetRes(CmdRes::kOk);
    return;
  }

  bool sm_ret = g_pika_server->SetMaster(master_ip_, master_port_);
  
  if (sm_ret) {
    res_.SetRes(CmdRes::kOk);
    g_pika_conf->SetSlaveof(master_ip_ + ":" + std::to_string(master_port_));
  } else {
    res_.SetRes(CmdRes::kErrOther, "Server is not in correct state for slaveof");
  }
}

/*
 * dbslaveof db[0 ~ 7]
 * dbslaveof db[0 ~ 7] force
 * dbslaveof db[0 ~ 7] no one
 * dbslaveof db[0 ~ 7] filenum offset
 */
void DbSlaveofCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameDbSlaveof);
    return;
  }
  if (!g_pika_conf->classic_mode()) {
    res_.SetRes(CmdRes::kErrOther, "DbSlaveof only support on classic mode");
    return;
  }
  if (g_pika_server->role() ^ PIKA_ROLE_SLAVE
    || !g_pika_server->MetaSyncDone()) {
    res_.SetRes(CmdRes::kErrOther, "Not currently a slave");
    return;
  }

  db_name_ = argv_[1];
  if (!g_pika_server->IsTableExist(db_name_)) {
    res_.SetRes(CmdRes::kErrOther, "Invaild db name");
    return;
  }

  if (argv_.size() == 3
    && !strcasecmp(argv_[2].data(), "force")) {
    force_sync_ = true;
    return;
  }

  if (argv_.size() == 4) {
    if (!strcasecmp(argv_[2].data(), "no")
      && !strcasecmp(argv_[3].data(), "one")) {
      is_noone_ = true;
      return;
    }

    if (!slash::string2l(argv_[2].data(), argv_[2].size(), &filenum_) || filenum_ < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    if (!slash::string2l(argv_[3].data(), argv_[3].size(), &offset_) || offset_ < 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    have_offset_ = true;
  }
}

void DbSlaveofCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Partition> db_partition = g_pika_server->GetPartitionByDbName(db_name_);

  if (!db_partition) {
    res_.SetRes(CmdRes::kErrOther, "Db not found");
    return;
  }

  std::shared_ptr<SyncSlavePartition> slave_partition =
    g_pika_rm->GetSyncSlavePartitionByName(RmNode(db_partition->GetTableName(), db_partition->GetPartitionId()));
  if (!slave_partition) {
    res_.SetRes(CmdRes::kErrOther, "Db not found");
    return;
  }

  Status s;
  if (is_noone_) {
    if (slave_partition->State() == ReplState::kConnected) {
      slave_partition->SetReplState(ReplState::kNoConnect);
      s = g_pika_server->SendRemoveSlaveNodeRequest(
              db_partition->GetTableName(), db_partition->GetPartitionId());
    }
  } else {
    if (slave_partition->State() == ReplState::kNoConnect
      || slave_partition->State() == ReplState::kError) {
      if (force_sync_) {
        slave_partition->SetReplState(ReplState::kTryDBSync);
      } else {
        if (have_offset_) {
          db_partition->logger()->SetProducerStatus(filenum_, offset_);
        }
        slave_partition->SetReplState(ReplState::kTryConnect);
      }
      g_pika_server->SetLoopPartitionStateMachine(true);
    }
  }

  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

