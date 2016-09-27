// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/push-pull-sink.h"

#include "runtime/row-batch.h"

#include <boost/thread/mutex.hpp>
#include <memory>

using namespace std;
using boost::unique_lock;
using boost::mutex;

namespace impala {

const string PushPullSink::NAME = "PUSH_PULL_SINK";

PushPullSink::PushPullSink(const RowDescriptor& row_desc, const TDataSink& thrift_sink)
  : DataSink(row_desc) {
  cur_batch_ = nullptr;
}

Status PushPullSink::Prepare(RuntimeState* state, MemTracker* mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, mem_tracker));
  return Status::OK();
}

Status PushPullSink::Open(RuntimeState* state) {
  return Status::OK();
}

Status PushPullSink::Send(RuntimeState* state, RowBatch* batch) {
  unique_lock<mutex> l(cv_lock_);
  if (consumer_done_) return Status::OK();
  DCHECK(cur_batch_ == nullptr);
  cur_batch_ = batch;
  consumer_cv_.notify_all();

  while (!sender_ready_) {
    sender_cv_.wait(l);
  }

  return Status::OK();
}

void PushPullSink::Close(RuntimeState* state) {
  unique_lock<mutex> l(cv_lock_);
  sender_done_ = true;
  consumer_cv_.notify_all();
  // Wait for consumer to be done, in case sender tries to tear-down this sink while the
  // sender is still reading from it.
  while (!consumer_done_) sender_cv_.wait(l);
}

void PushPullSink::CloseConsumer() {
  unique_lock<mutex> l(cv_lock_);
  consumer_done_ = true;
  sender_cv_.notify_all();
}

Status PushPullSink::GetNext(RuntimeState* state, RowBatch** row_batch) {
  unique_lock<mutex> l(cv_lock_);
  sender_ready_ = true;
  // Trigger sender to send next row batch now.
  if (cur_batch_ == nullptr) {
    sender_cv_.notify_all();
    while (cur_batch_ == nullptr && !sender_done_) consumer_cv_.wait(l);
  }
  *row_batch = cur_batch_;
  cur_batch_ = nullptr;
  sender_ready_ = false;
  return Status::OK();
}
}
