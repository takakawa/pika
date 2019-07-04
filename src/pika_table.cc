// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_table.h"

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

std::string TablePath(const std::string& path,
                      const std::string& table_name) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%s/", table_name.data());
  return path + buf;
}

Table::Table(const std::string& table_name,
             uint32_t partition_num,
             const std::string& db_path,
             const std::string& log_path,
             const std::string& trash_path) :
  table_name_(table_name),
  partition_num_(partition_num) {

  db_path_ = TablePath(db_path, table_name_);
  log_path_ = TablePath(log_path, "log_" + table_name_);
  trash_path_ = TablePath(trash_path, "trash_" + table_name_);

  slash::CreatePath(db_path_);
  slash::CreatePath(log_path_);

  pthread_rwlock_init(&partitions_rw_, NULL);

  for (uint32_t idx = 0; idx < partition_num_; ++idx) {
    partitions_.emplace(idx, std::shared_ptr<Partition>(
                new Partition(table_name_, idx, db_path_, log_path_, trash_path_)));
  }

}

Table::~Table() {
  pthread_rwlock_destroy(&partitions_rw_);
  partitions_.clear();
}

std::string Table::GetTableName() {
  return table_name_;
}

void Table::BgSaveTable() {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    item.second->BgSavePartition();
  }
}

#if 0
void Table::CompactTable(const blackwidow::DataType& type) {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    item.second->Compact(type);
  }
}
#endif

bool Table::FlushPartitionDB() {
  slash::MutexLock ml(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return false;
  }
  slash::RWLock rwl(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    item.second->FlushDB();
  }
  return true;
}

bool Table::FlushPartitionSubDB(const std::string& db_name) {
  slash::MutexLock ml(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return false;
  }
  slash::RWLock rwl(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    item.second->FlushSubDB(db_name);
  }
  return true;
}

bool Table::IsBinlogIoError() {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    if (item.second->IsBinlogIoError()) {
      return true;
    }
  }
  return false;
}

uint32_t Table::PartitionNum() {
  return partition_num_;
}


void Table::LeaveAllPartition() {
  slash::RWLock rwl(&partitions_rw_, true);
  for (const auto& item : partitions_) {
    item.second->Leave();
  }
  partitions_.clear();
}

std::shared_ptr<Partition> Table::GetPartitionById(uint32_t partition_id) {
  slash::RWLock rwl(&partitions_rw_, false);
  auto iter = partitions_.find(partition_id);
  return (iter == partitions_.end()) ? NULL : iter->second;
}

std::shared_ptr<Partition> Table::GetPartitionByKey(const std::string& key) {
  assert(partition_num_ != 0);
  slash::RWLock rwl(&partitions_rw_, false);
  auto iter = partitions_.find(std::hash<std::string>()(key) % partition_num_);
  return (iter == partitions_.end()) ? NULL : iter->second;
}
