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

#include <boost/thread/condition_variable.hpp>

namespace impala {

class RowBatch;

/// Sink which manages the handoff between an asynchronous plan fragment that produces
/// batches, and a consumer (e.g. the coordinator) which consumes those batches.
///
/// All batches are owned bythe producing plan, and previously retrieved batches may be
/// overwritten during subsequent GetNext() calls. Consumers must AcquireState() from
/// those batches if they need a longer lifetime.
///
/// Although batch production and consumption happens concurrently, senders will be
/// blocked until the previous batch has been consumed and GetNext() is called. This
/// ensures that the sender can safely overwrite the batch that was previously returned.
///
/// Consumers must call CloseConsumer() when finished to allow the fragment to shut down.
///
/// The sink is thread safe up to a single producer and single consumer.
class PushPullSink : public DataSink {
 public:
  PushPullSink(const RowDescriptor& row_desc, const TDataSink& thrift_sink);

  virtual std::string GetName() { return NAME; }

  virtual Status Prepare(RuntimeState* state, MemTracker* tracker);

  virtual Status Open(RuntimeState* state);

  /// Sends a new batch. Blocks until the consumer calls GetNext() to consume the batch or
  /// CloseConsumer() shuts the queue down.
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  /// No-op: batches are not pushed out by this sink, so flushing has no meaning.
  virtual Status FlushFinal(RuntimeState* state) { return Status::OK(); }

  /// To be called by sender only. Blocks until CloseConsumer() is called.
  virtual void Close(RuntimeState* state);

  /// Returns a batch from the queue in 'row_batch'. The batch is not owned by the
  /// caller. Subsequent calls to GetNext() will overwrite that batch, so consumers must
  /// acquire the state of the batch if needed before returning control to the sink.
  ///
  /// '*row_batch' is set to nullptr when there is no more input to consume.
  Status GetNext(RuntimeState* state, RowBatch** row_batch);

  /// Signals to the producer that the sink will no longer be used. It's an error to call
  /// GetNext() after this returns. May be called more than once; only the first call has
  /// any effect.
  void CloseConsumer();

  static const std::string NAME;

 private:
  boost::mutex cv_lock_;
  boost::condition_variable sender_cv_;
  boost::condition_variable consumer_cv_;

  /// Signals to producer that the consumer is done, and the sink may be torn down.
  bool consumer_done_ = false;

  /// Signals to consumer that the sender is done, and that there are no more row batches
  /// to consume.
  bool sender_done_ = false;

  bool sender_ready_ = false;

  RowBatch* cur_batch_;
};
}

#endif
