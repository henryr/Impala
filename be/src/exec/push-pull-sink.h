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

#ifndef IMPALA_EXEC_PUSH_PULL_SINK_H
#define IMPALA_EXEC_PUSH_PULL_SINK_H

#include "exec/data-sink.h"

#include "util/blocking-queue.h"
#include "util/promise.h"

namespace impala {

/// Sink which queues incoming batches from fragment, allowing consumers to pull from the
/// queue at their own pace by calling GetNext().
///
/// All batches are owned by the sink, and previously retrieved batches may be overwritten
/// during subsequent GetNext() calls. Consumers must AcquireState() from those batches if
/// they need a longer lifetime.
///
/// Consumers must call CloseConsumer() when finished to allow the fragment to shut down.
///
/// The sink is thread safe up to a single producer and single consumer.
class PushPullSink : public DataSink {
 public:
  PushPullSink(const RowDescriptor& row_desc, const std::vector<TExpr>& output_exprs,
      const TDataSink& thrift_sink);

  virtual std::string GetName() { return NAME; }

  virtual Status Prepare(RuntimeState* state, MemTracker* tracker);

  virtual Status Open(RuntimeState* state);

  /// Enqueues a new batch. If the queue is full, blocks until GetNext() opens a spot in
  /// the queue, or CloseConsumer() shuts the queue down. The sink will acquire all the
  /// state of 'batch' before enqueuing it.
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  /// No-op: batches are not pushed out by this sink, so flushing has no meaning.
  virtual Status FlushFinal(RuntimeState* state) { return Status::OK(); }

  /// To be called by sender only. Blocks until CloseConsumer() is called.
  virtual void Close(RuntimeState* state);

  /// Returns a batch from the queue in 'row_batch'. The batch is still owned by the
  /// sink. Subsequent calls to GetNext() will overwrite that batch, so consumers must
  /// acquire the state of the batch before returning control to the sink.
  ///
  /// '*row_batch' is set to nullptr when there is no more input to consume.
  virtual Status GetNext(RuntimeState* state, RowBatch** row_batch);

  /// Signals to the producer that the sink will no longer be used. It's an error to call
  /// GetNext() after this returns. May be called more than once; only the first call has
  /// any effect.
  void CloseConsumer();

  static const std::string NAME;

 private:
  /// Queue of pending row batches. Queue entry should be movable to avoid excessive
  /// copying overhead; since RowBatch is not movable it's wrapped in a unique_ptr.
  BlockingQueue<std::unique_ptr<RowBatch>> row_batch_queue_;

  /// The last batch consumed is saved here. When another batch is consumed via GetNext(),
  /// all state associated with this batch is freed.
  std::unique_ptr<RowBatch> cur_batch_;

  /// Signals to producer that the consumer is done, and the sink may be torn down.
  Promise<bool> consumer_done_;
};
}

#endif
