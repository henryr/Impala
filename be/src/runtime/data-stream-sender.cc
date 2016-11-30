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

#include "runtime/data-stream-sender.h"

#include <iostream>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "exec/kudu-util.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"
#include "runtime/row-batch.h"
#include "runtime/raw-value.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/client-cache.h"
#include "runtime/mem-tracker.h"
#include "util/aligned-new.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/thread-pool.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-util.h"

#include "service/impala-server.h"
#include "rpc/rpc-mgr.h"
#include "service/prototest.pb.h"
#include "service/prototest.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/net_util.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

using boost::condition_variable;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using kudu::HostPort;
using kudu::Sockaddr;

using kudu::rpc_test::TransmitDataRequestPB;
using kudu::rpc_test::TransmitDataResponsePB;
using kudu::rpc_test::RowBatchPB;
using kudu::rpc_test::ImpalaKRPCServiceProxy;
using kudu::rpc::RpcController;

DECLARE_int32(be_port);

namespace impala {

// A channel sends data asynchronously via calls to TransmitData
// to a single destination ipaddress/node.
// It has a fixed-capacity buffer and allows the caller either to add rows to
// that buffer individually (AddRow()), or circumvent the buffer altogether and send
// TRowBatches directly (SendBatch()). Either way, there can only be one in-flight RPC
// at any one time (ie, sending will block if the most recent rpc hasn't finished,
// which allows the receiver node to throttle the sender by withholding acks).
// *Not* thread-safe.
class DataStreamSender::Channel : public CacheLineAligned {
 public:
  // Create channel to send data to particular ipaddress/port/query/node
  // combination. buffer_size is specified in bytes and a soft limit on
  // how much tuple data is getting accumulated before being sent; it only applies
  // when data is added via AddRow() and not sent directly via SendBatch().
  Channel(DataStreamSender* parent, const RowDescriptor& row_desc,
          const TNetworkAddress& destination, const TUniqueId& fragment_instance_id,
          PlanNodeId dest_node_id, int buffer_size)
    : parent_(parent),
      buffer_size_(buffer_size),
      row_desc_(row_desc),
      address_(MakeNetworkAddress(destination.hostname, destination.port)),
      fragment_instance_id_(fragment_instance_id),
      dest_node_id_(dest_node_id),
      num_data_bytes_sent_(0),
      rpc_thread_("DataStreamSender", "SenderThread", 1, 1,
          bind<void>(mem_fn(&Channel::TransmitData), this, _1, _2)),
      rpc_in_flight_(false) {
  }

  // Initialize channel.
  // Returns OK if successful, error indication otherwise.
  Status Init(RuntimeState* state);

  // Copies a single row into this channel's output buffer and flushes buffer
  // if it reaches capacity.
  // Returns error status if any of the preceding rpcs failed, OK otherwise.
  Status AddRow(TupleRow* row);

  // Asynchronously sends a row batch.
  // Returns the status of the most recently finished TransmitData
  // rpc (or OK if there wasn't one that hasn't been reported yet).
  Status SendBatch(TRowBatch* batch);

  // Return status of last TransmitData rpc (initiated by the most recent call
  // to either SendBatch() or SendCurrentBatch()).
  Status GetSendStatus();

  // Waits for the rpc thread pool to finish the current rpc.
  void WaitForRpc();

  // Drain and shutdown the rpc thread and free the row batch allocation.
  void Teardown(RuntimeState* state);

  // Flushes any buffered row batches and sends the EOS RPC to close the channel.
  Status FlushAndSendEos(RuntimeState* state);

  int64_t num_data_bytes_sent() const { return num_data_bytes_sent_; }
  TRowBatch* thrift_batch() { return &thrift_batch_; }

 private:
  DataStreamSender* parent_;
  int buffer_size_;

  const RowDescriptor& row_desc_;
  TNetworkAddress address_;
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  // the number of TRowBatch.data bytes sent successfully
  int64_t num_data_bytes_sent_;

  // we're accumulating rows into this batch
  scoped_ptr<RowBatch> batch_;
  TRowBatch thrift_batch_;

  // We want to reuse the rpc thread to prevent creating a thread per rowbatch.
  // TODO: currently we only have one batch in flight, but we should buffer more
  // batches. This is a bit tricky since the channels share the outgoing batch
  // pointer we need some mechanism to coordinate when the batch is all done.
  // TODO: if the order of row batches does not matter, we can consider increasing
  // the number of threads.
  ThreadPool<TRowBatch*> rpc_thread_; // sender thread.
  condition_variable rpc_done_cv_;   // signaled when rpc_in_flight_ is set to true.
  mutex rpc_thread_lock_; // Lock with rpc_done_cv_ protecting rpc_in_flight_
  bool rpc_in_flight_;  // true if the rpc_thread_ is busy sending.

  Status rpc_status_;  // status of most recently finished TransmitData rpc
  RuntimeState* runtime_state_;

  // Serialize batch_ into thrift_batch_ and send via SendBatch().
  // Returns SendBatch() status.
  Status SendCurrentBatch();

  // Synchronously call TransmitData() and update rpc_status_ based on return value (or
  // set to error if RPC failed).
  // Called from a thread from the rpc_thread_ pool.
  void TransmitData(int thread_id, const TRowBatch*);
  void TransmitDataHelper(const TRowBatch*);
};

Status DataStreamSender::Channel::Init(RuntimeState* state) {
  runtime_state_ = state;
  // TODO: figure out how to size batch_
  int capacity = max(1, buffer_size_ / max(row_desc_.GetRowSize(), 1));
  batch_.reset(new RowBatch(row_desc_, capacity, parent_->mem_tracker()));
  return Status::OK();
}

Status DataStreamSender::Channel::SendBatch(TRowBatch* batch) {
  VLOG_ROW << "Channel::SendBatch() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_ << " #rows=" << batch->num_rows;
  // return if the previous batch saw an error
  RETURN_IF_ERROR(GetSendStatus());
  {
    unique_lock<mutex> l(rpc_thread_lock_);
    rpc_in_flight_ = true;
  }
  if (!rpc_thread_.Offer(batch)) {
    unique_lock<mutex> l(rpc_thread_lock_);
    rpc_in_flight_ = false;
  }
  return Status::OK();
}

void DataStreamSender::Channel::TransmitData(int thread_id, const TRowBatch* batch) {
  DCHECK(rpc_in_flight_);
  TransmitDataHelper(batch);

  {
    unique_lock<mutex> l(rpc_thread_lock_);
    rpc_in_flight_ = false;
  }
  rpc_done_cv_.notify_one();
}

void DataStreamSender::Channel::TransmitDataHelper(const TRowBatch* batch) {
  DCHECK(batch != NULL);
  VLOG_ROW << "Channel::TransmitData() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows=" << batch->num_rows;

  TransmitDataRequestPB request;
  request.mutable_dest_fragment_instance_id()->set_lo(fragment_instance_id_.lo);
  request.mutable_dest_fragment_instance_id()->set_hi(fragment_instance_id_.hi);
  request.set_dest_node_id(dest_node_id_);
  RowBatchPB* row_batch_pb = request.mutable_row_batch();
  row_batch_pb->set_num_rows(batch->num_rows);
  for (int32_t tuple: batch->row_tuples) row_batch_pb->add_row_tuples(tuple);
  for (int32_t offset: batch->tuple_offsets) row_batch_pb->add_tuple_offsets(offset);
  row_batch_pb->set_tuple_data(batch->tuple_data);
  row_batch_pb->set_uncompressed_size(batch->uncompressed_size);
  row_batch_pb->set_compression_type(batch->compression_type);
  request.set_eos(false);
  request.set_sender_id(parent_->sender_id_);

  unique_ptr<ImpalaKRPCServiceProxy> proxy;
  ExecEnv::GetInstance()->rpc_mgr()->GetProxy(address_, &proxy);

  RpcController controller;
  TransmitDataResponsePB response;
  // TODO: Is this the right status? Or controller.status()?
  kudu::Status rpc_status = proxy->TransmitData(request, &response, &controller);

  COUNTER_ADD(parent_->profile_->total_time_counter(),
      parent_->thrift_transmit_timer_->LapTime());

  if (!rpc_status.ok()) {
    rpc_status_ = Status(Substitute("$0: $1", "RPC failure", rpc_status.ToString()));
  } else {
    num_data_bytes_sent_ += RowBatch::GetBatchSize(*batch);
    VLOG_ROW << "incremented #data_bytes_sent="
             << num_data_bytes_sent_;
  }
}

void DataStreamSender::Channel::WaitForRpc() {
  SCOPED_TIMER(parent_->state_->total_network_send_timer());
  unique_lock<mutex> l(rpc_thread_lock_);
  while (rpc_in_flight_) {
    rpc_done_cv_.wait(l);
  }
}

Status DataStreamSender::Channel::AddRow(TupleRow* row) {
  if (batch_->AtCapacity()) {
    // batch_ is full, let's send it; but first wait for an ongoing
    // transmission to finish before modifying thrift_batch_
    RETURN_IF_ERROR(SendCurrentBatch());
  }
  TupleRow* dest = batch_->GetRow(batch_->AddRow());
  const vector<TupleDescriptor*>& descs = row_desc_.tuple_descriptors();
  for (int i = 0; i < descs.size(); ++i) {
    if (UNLIKELY(row->GetTuple(i) == NULL)) {
      dest->SetTuple(i, NULL);
    } else {
      dest->SetTuple(i, row->GetTuple(i)->DeepCopy(*descs[i], batch_->tuple_data_pool()));
    }
  }
  batch_->CommitLastRow();
  return Status::OK();
}

Status DataStreamSender::Channel::SendCurrentBatch() {
  // make sure there's no in-flight TransmitData() call that might still want to
  // access thrift_batch_
  WaitForRpc();
  RETURN_IF_ERROR(parent_->SerializeBatch(batch_.get(), &thrift_batch_));
  batch_->Reset();
  RETURN_IF_ERROR(SendBatch(&thrift_batch_));
  return Status::OK();
}

Status DataStreamSender::Channel::GetSendStatus() {
  WaitForRpc();
  if (!rpc_status_.ok()) {
    LOG(ERROR) << "channel send status: " << rpc_status_.GetDetail();
  }
  return rpc_status_;
}

Status DataStreamSender::Channel::FlushAndSendEos(RuntimeState* state) {
  VLOG_RPC << "Channel::FlushAndSendEos() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows= " << batch_->num_rows();

  // We can return an error here and not go on to send the EOS RPC because the error that
  // we returned will be sent to the coordinator who will then cancel all the remote
  // fragments including the one that this sender is sending to.
  if (batch_->num_rows() > 0) {
    // flush
    RETURN_IF_ERROR(SendCurrentBatch());
  }

  RETURN_IF_ERROR(GetSendStatus());

  TransmitDataRequestPB request;
  request.mutable_dest_fragment_instance_id()->set_lo(fragment_instance_id_.lo);
  request.mutable_dest_fragment_instance_id()->set_hi(fragment_instance_id_.hi);
  request.set_dest_node_id(dest_node_id_);
  request.mutable_row_batch()->set_num_rows(0);
  request.mutable_row_batch()->set_tuple_data("");
  request.mutable_row_batch()->set_compression_type(0);
  request.mutable_row_batch()->set_uncompressed_size(0);
  request.set_eos(true);
  request.set_sender_id(parent_->sender_id_);

  unique_ptr<ImpalaKRPCServiceProxy> proxy;
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->GetProxy(address_,&proxy));
  RpcController controller;
  TransmitDataResponsePB response;

  KUDU_RETURN_IF_ERROR(proxy->TransmitData(request, &response, &controller), "RPC Error");
  return Status::OK();
}

void DataStreamSender::Channel::Teardown(RuntimeState* state) {
  // FlushAndSendEos() should have been called before calling Teardown(), which means that
  // all the data should already be drained. Calling DrainAndShutdown() only to shutdown.
  rpc_thread_.DrainAndShutdown();
  batch_.reset();
}

DataStreamSender::DataStreamSender(ObjectPool* pool, int sender_id,
    const RowDescriptor& row_desc, const TDataStreamSink& sink,
    const vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size)
  : DataSink(row_desc),
    sender_id_(sender_id),
    current_channel_idx_(0),
    flushed_(false),
    closed_(false),
    current_thrift_batch_(&thrift_batch1_),
    serialize_batch_timer_(NULL),
    thrift_transmit_timer_(NULL),
    bytes_sent_counter_(NULL),
    total_sent_rows_counter_(NULL),
    dest_node_id_(sink.dest_node_id) {
  DCHECK_GT(destinations.size(), 0);
  DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
      || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
      || sink.output_partition.type == TPartitionType::RANDOM);
  broadcast_ = sink.output_partition.type == TPartitionType::UNPARTITIONED;
  random_ = sink.output_partition.type == TPartitionType::RANDOM;
  // TODO: use something like google3's linked_ptr here (scoped_ptr isn't copyable)
  for (int i = 0; i < destinations.size(); ++i) {
    channels_.push_back(
        new Channel(this, row_desc, destinations[i].server,
                    destinations[i].fragment_instance_id,
                    sink.dest_node_id, per_channel_buffer_size));
  }

  if (broadcast_ || random_) {
    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    srand(reinterpret_cast<uint64_t>(this));
    random_shuffle(channels_.begin(), channels_.end());
  }

  if (sink.output_partition.type == TPartitionType::HASH_PARTITIONED) {
    // TODO: move this to Init()? would need to save 'sink' somewhere
    Status status =
        Expr::CreateExprTrees(pool, sink.output_partition.partition_exprs,
                              &partition_expr_ctxs_);
    DCHECK(status.ok());
  }
}

string DataStreamSender::GetName() {
  return Substitute("DataStreamSender (dst_id=$0)", dest_node_id_);
}

DataStreamSender::~DataStreamSender() {
  // TODO: check that sender was either already closed() or there was an error
  // on some channel
  for (int i = 0; i < channels_.size(); ++i) {
    delete channels_[i];
  }
}

Status DataStreamSender::Prepare(RuntimeState* state, MemTracker* mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, mem_tracker));
  state_ = state;
  SCOPED_TIMER(profile_->total_time_counter());

  RETURN_IF_ERROR(Expr::Prepare(partition_expr_ctxs_, state, row_desc_, mem_tracker));

  bytes_sent_counter_ =
      ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
  uncompressed_bytes_counter_ =
      ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
  serialize_batch_timer_ =
      ADD_TIMER(profile(), "SerializeBatchTime");
  thrift_transmit_timer_ = profile()->AddConcurrentTimerCounter("TransmitDataRPCTime",
      TUnit::TIME_NS);
  network_throughput_ =
      profile()->AddDerivedCounter("NetworkThroughput(*)", TUnit::BYTES_PER_SECOND,
          bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_sent_counter_,
                        thrift_transmit_timer_));
  overall_throughput_ =
      profile()->AddDerivedCounter("OverallThroughput", TUnit::BYTES_PER_SECOND,
           bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_sent_counter_,
                         profile()->total_time_counter()));

  total_sent_rows_counter_= ADD_COUNTER(profile(), "RowsReturned", TUnit::UNIT);
  for (int i = 0; i < channels_.size(); ++i) {
    RETURN_IF_ERROR(channels_[i]->Init(state));
  }
  return Status::OK();
}

Status DataStreamSender::Open(RuntimeState* state) {
  return Expr::Open(partition_expr_ctxs_, state);
}

Status DataStreamSender::Send(RuntimeState* state, RowBatch* batch) {
  DCHECK(!closed_);
  DCHECK(!flushed_);

  if (batch->num_rows() == 0) return Status::OK();
  if (broadcast_ || channels_.size() == 1) {
    // current_thrift_batch_ is *not* the one that was written by the last call
    // to Serialize()
    RETURN_IF_ERROR(SerializeBatch(batch, current_thrift_batch_, channels_.size()));
    // SendBatch() will block if there are still in-flight rpcs (and those will
    // reference the previously written thrift batch)
    for (int i = 0; i < channels_.size(); ++i) {
      RETURN_IF_ERROR(channels_[i]->SendBatch(current_thrift_batch_));
    }
    current_thrift_batch_ =
        (current_thrift_batch_ == &thrift_batch1_ ? &thrift_batch2_ : &thrift_batch1_);
  } else if (random_) {
    // Round-robin batches among channels. Wait for the current channel to finish its
    // rpc before overwriting its batch.
    Channel* current_channel = channels_[current_channel_idx_];
    current_channel->WaitForRpc();
    RETURN_IF_ERROR(SerializeBatch(batch, current_channel->thrift_batch()));
    RETURN_IF_ERROR(current_channel->SendBatch(current_channel->thrift_batch()));
    current_channel_idx_ = (current_channel_idx_ + 1) % channels_.size();
  } else {
    // hash-partition batch's rows across channels
    int num_channels = channels_.size();
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      uint32_t hash_val = HashUtil::FNV_SEED;
      for (int i = 0; i < partition_expr_ctxs_.size(); ++i) {
        ExprContext* ctx = partition_expr_ctxs_[i];
        void* partition_val = ctx->GetValue(row);
        // We can't use the crc hash function here because it does not result
        // in uncorrelated hashes with different seeds.  Instead we must use
        // fnv hash.
        // TODO: fix crc hash/GetHashValue()
        hash_val =
            RawValue::GetHashValueFnv(partition_val, ctx->root()->type(), hash_val);
      }
      ExprContext::FreeLocalAllocations(partition_expr_ctxs_);
      RETURN_IF_ERROR(channels_[hash_val % num_channels]->AddRow(row));
    }
  }
  COUNTER_ADD(total_sent_rows_counter_, batch->num_rows());
  RETURN_IF_ERROR(state->CheckQueryState());
  return Status::OK();
}

Status DataStreamSender::FlushFinal(RuntimeState* state) {
  DCHECK(!flushed_);
  DCHECK(!closed_);
  flushed_ = true;
  for (int i = 0; i < channels_.size(); ++i) {
    // If we hit an error here, we can return without closing the remaining channels as
    // the error is propagated back to the coordinator, which in turn cancels the query,
    // which will cause the remaining open channels to be closed.
    RETURN_IF_ERROR(channels_[i]->FlushAndSendEos(state));
  }
  return Status::OK();
}

void DataStreamSender::Close(RuntimeState* state) {
  if (closed_) return;
  for (int i = 0; i < channels_.size(); ++i) {
    channels_[i]->Teardown(state);
  }
  Expr::Close(partition_expr_ctxs_, state);
  DataSink::Close(state);
  closed_ = true;
}

Status DataStreamSender::SerializeBatch(RowBatch* src, TRowBatch* dest, int num_receivers) {
  VLOG_ROW << "serializing " << src->num_rows() << " rows";
  {
    SCOPED_TIMER(profile_->total_time_counter());
    SCOPED_TIMER(serialize_batch_timer_);
    RETURN_IF_ERROR(src->Serialize(dest));
    int bytes = RowBatch::GetBatchSize(*dest);
    int uncompressed_bytes = bytes - dest->tuple_data.size() + dest->uncompressed_size;
    // The size output_batch would be if we didn't compress tuple_data (will be equal to
    // actual batch size if tuple_data isn't compressed)

    COUNTER_ADD(bytes_sent_counter_, bytes * num_receivers);
    COUNTER_ADD(uncompressed_bytes_counter_, uncompressed_bytes * num_receivers);
  }
  return Status::OK();
}

int64_t DataStreamSender::GetNumDataBytesSent() const {
  // TODO: do we need synchronization here or are reads & writes to 8-byte ints
  // atomic?
  int64_t result = 0;
  for (int i = 0; i < channels_.size(); ++i) {
    result += channels_[i]->num_data_bytes_sent();
  }
  return result;
}

}
