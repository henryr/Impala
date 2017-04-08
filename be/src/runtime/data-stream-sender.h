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


#ifndef IMPALA_RUNTIME_DATA_STREAM_SENDER_H
#define IMPALA_RUNTIME_DATA_STREAM_SENDER_H

#include <vector>
#include <string>

#include "common/global-types.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exec/data-sink.h"
#include "runtime/row-batch.h"
#include "util/runtime-profile.h"

namespace impala {

class Expr;
class ExprContext;
class RowBatch;
class RowDescriptor;
class MemTracker;
class TDataStreamSink;
class TNetworkAddress;
class TPlanFragmentDestination;

/// Single sender of an m:n data stream.
/// Row batch data is routed to destinations based on the provided
/// partitioning specification.
/// *Not* thread-safe.
//
/// TODO: capture stats that describe distribution of rows/data volume
/// across channels.
class DataStreamSender : public DataSink {
 public:
  /// Construct a sender according to the output specification (sink),
  /// sending to the given destinations. sender_id identifies this
  /// sender instance, and is unique within a fragment.
  /// Per_channel_buffer_size is the buffer size allocated to each channel
  /// and is specified in bytes.
  /// The RowDescriptor must live until Close() is called.
  /// NOTE: supported partition types are UNPARTITIONED (broadcast), HASH_PARTITIONED,
  /// and RANDOM.
  DataStreamSender(ObjectPool* pool, int sender_id,
    const RowDescriptor& row_desc, const TDataStreamSink& sink,
    const std::vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size);

  virtual std::string GetName();

  /// Must be called before other API calls, and before the codegen'd IR module is
  /// compiled (i.e. in an ExecNode's Prepare() function).
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker);

  /// Must be called before Send() or Close(), and after the codegen'd IR module is
  /// compiled (i.e. in an ExecNode's Open() function).
  virtual Status Open(RuntimeState* state);

  /// Flush all buffered data and close all existing channels to destination hosts.
  /// Further Send() calls are illegal after calling FlushFinal().
  /// It is legal to call FlushFinal() only 0 or 1 times.
  virtual Status FlushFinal(RuntimeState* state);

  /// Send data in 'batch' to destination nodes according to partitioning
  /// specification provided in c'tor.
  /// Blocks until all rows in batch are placed in their appropriate outgoing
  /// buffers (ie, blocks if there are still in-flight rpcs from the last
  /// Send() call).
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  /// Shutdown all existing channels to destination hosts. Further FlushFinal() calls are
  /// illegal after calling Close().
  virtual void Close(RuntimeState* state);

  /// Serializes the src batch into the dest thrift batch. Maintains metrics.
  /// num_receivers is the number of receivers this batch will be sent to. Only
  /// used to maintain metrics.
  Status SerializeBatch(RowBatch* src, OutboundProtoRowBatch* dest, int num_receivers = 1);

  /// Return total number of bytes sent in RowBatchPb.data(). If batches are
  /// broadcast to multiple receivers, they are counted once per receiver.
  int64_t GetNumDataBytesSent() const;

 private:
  class Channel;

  std::shared_ptr<OutboundProtoRowBatch> GetReferenceForBatch(OutboundProtoRowBatch* batch) {
    if (batch == proto_batch1_.get()) return proto_batch1_;
    DCHECK(batch == proto_batch2_.get());
    return proto_batch2_;
  }

  /// Sender instance id, unique within a fragment.
  int sender_id_;
  RuntimeState* state_;
  TPartitionType::type partition_type_; // The type of partitioning to perform.

  // index of current channel to send to if using RANDOM partitioning.
  int current_channel_idx_ = 0;

  /// If true, this sender has called FlushFinal() successfully.
  /// Not valid to call Send() anymore.
  bool flushed_ = false;

  /// If true, this sender has been closed. Not valid to call Send() anymore.
  bool closed_ = false;

  /// serialized batches for broadcasting; we need two so we can write
  /// one while the other one is still being sent
  std::shared_ptr<OutboundProtoRowBatch> proto_batch1_ =
      std::make_shared<OutboundProtoRowBatch>();
  std::shared_ptr<OutboundProtoRowBatch> proto_batch2_ =
      std::make_shared<OutboundProtoRowBatch>();
  OutboundProtoRowBatch* current_proto_batch_; // the next one to fill in Send()

  std::vector<ExprContext*> partition_expr_ctxs_;  // compute per-row partition values
  std::vector<std::shared_ptr<Channel>> channels_;

  // TODO: Add network throughput counters when KUDU-1858 (add instrumentation to
  // OutboundCall) is completed.

  RuntimeProfile::Counter* serialize_batch_timer_;
  RuntimeProfile::Counter* bytes_sent_counter_;
  RuntimeProfile::Counter* uncompressed_bytes_counter_;
  RuntimeProfile::Counter* total_sent_rows_counter_;

  /// Throughput per total time spent in sender
  RuntimeProfile::Counter* overall_throughput_;

  /// Identifier of the destination plan node.
  PlanNodeId dest_node_id_;

  /// Used for Kudu partitioning to round-robin rows that don't correspond to a partition
  /// or when errors are encountered.
  int next_unknown_partition_ = 0;
};

}

#endif
