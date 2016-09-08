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

#include <memory>

using namespace std;

namespace impala {

const string PushPullSink::NAME = "PUSH_PULL_SINK";

namespace {
constexpr int32_t QUEUE_DEPTH = 16;
}

PushPullSink::PushPullSink(const RowDescriptor& row_desc,
    const vector<TExpr>& output_exprs, const TDataSink& thrift_sink)
  : DataSink(row_desc), row_batch_queue_(QUEUE_DEPTH) {}

Status PushPullSink::Prepare(RuntimeState* state, MemTracker* mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, mem_tracker));
  return Status::OK();
}

Status PushPullSink::Open(RuntimeState* state) {
  return Status::OK();
}

Status PushPullSink::Send(RuntimeState* state, RowBatch* batch) {
  // Make a duplicate RowBatch, and then acquire the state of the argument.
  unique_ptr<RowBatch> my_batch = make_unique<RowBatch>(
      row_desc_, batch->InitialCapacity(), state->instance_mem_tracker());
  my_batch->AcquireState(batch);

  // Move batch into queue to avoid copies.
  row_batch_queue_.BlockingPut(move(my_batch));
  return Status::OK();
}

void PushPullSink::Close(RuntimeState* state) {
  // Needed to wake a consumer blocked in GetNext() when the queue is empty.
  row_batch_queue_.Shutdown();
  consumer_done_.Get();
}

void PushPullSink::CloseConsumer() {
  if (!consumer_done_.IsSet()) {
    row_batch_queue_.Shutdown();
    consumer_done_.Set(true);
  }
}

Status PushPullSink::GetNext(RuntimeState* state, RowBatch** row_batch) {
  // Release resources associated with last batch.
  if (cur_batch_.get()) cur_batch_->Reset();

  // TODO: Signal when this is the last batch, to avoid needing a further fetch to get
  // null.
  if (row_batch_queue_.BlockingGet(&cur_batch_)) {
    *row_batch = cur_batch_.get();
  } else {
    *row_batch = nullptr;
  }
  return Status::OK();
}
}
