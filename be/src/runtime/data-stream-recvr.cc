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

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <condition_variable>
#include <queue>

#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/sorted-run-merger.h"
#include "service/data_stream_service.pb.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

#include "kudu/rpc/rpc_context.h"

#include "common/names.h"

using std::condition_variable_any;
using std::queue;

using kudu::rpc::ErrorStatusPB;
using kudu::rpc::RpcContext;

namespace impala {

// Implements a FIFO queue of row batches from one or more senders. One queue is
// maintained per sender if is_merging_ is true for the enclosing receiver, otherwise rows
// from all senders are placed in the same queue.
//
// Batches are added by senders via AddBatch(), and removed by an enclosing
// DataStreamRecvr via GetBatch(). If a sender adds a batch that overflows the queue, the
// batch is discarded, and the sender is added to a queue of 'pending senders' that will
// be signalled to try again in FIFO order. That signal is usually sent when a space
// arrives in the queue, but may also be sent by the DataStreamMgr. See the DataStreamMgr
// class comment for details.
class DataStreamRecvr::SenderQueue {
 public:
  SenderQueue(DataStreamRecvr* parent_recvr, int num_senders);

  // Return the next batch from this sender queue. Sets the returned batch in cur_batch_.
  // A returned batch that is not filled to capacity does *not* indicate
  // end-of-stream.
  // The call blocks until another batch arrives or all senders close.
  // their channels. The returned batch is owned by the sender queue. The caller
  // must acquire data from the returned batch before the next call to GetBatch().
  Status GetBatch(RowBatch** next_batch);

  // Adds a row batch to this sender queue if this stream has not been cancelled. If this
  // batch will make the stream exceed its buffer limit, the payload is copied to a queue
  // to be responded to in the future, and the method returns immediately.
  void AddBatch(TransmitDataCtx&& payload);

  // Decrement the number of remaining senders for this queue and signal eos ("new data")
  // if the count drops to 0. The number of senders will be 1 for a merging
  // DataStreamRecvr.
  void DecrementSenders();

  // Set cancellation flag and signal cancellation to receiver and sender. Subsequent
  // incoming batches will be dropped.
  void Cancel();

  // Must be called once to cleanup any queued resources.
  void Close();

  // Returns the current batch from this queue being processed by a consumer.
  RowBatch* current_batch() const { return current_batch_.get(); }

  // Loops through all pending senders and responds to their RPC if more than
  // TRANSMIT_DATA_TIMEOUT_SECONDS / 2 has elapsed since they were added to the queue. The
  // response is always ERROR_SERVER_TOO_BUSY, signalling to the sender that they should
  // re-transmit. Called by DataStreamMgr periodically.
  void CheckPendingSenders();

 private:
  // Receiver of which this queue is a member.
  DataStreamRecvr* recvr_;

  // Enforces mutual exclusion between Close() and AddBatch(), so that if AddBatch() does
  // any work, Close() will not run until after it (and can perform any clean up).
  SpinLock closing_lock_;

  // protects all subsequent data.
  SpinLock lock_;

  // if true, the receiver fragment for this stream got cancelled
  bool is_cancelled_ = false;

  // number of senders which haven't closed the channel yet
  // (if it drops to 0, end-of-stream is true)
  int num_remaining_senders_;

  // signal arrival of new batch or the eos/cancelled condition
  condition_variable_any data_arrival_cv_;

  // queue of (batch length, batch) pairs.  The SenderQueue block owns memory to
  // these batches. They are handed off to the caller via GetBatch.
  typedef list<pair<int, RowBatch*>> RowBatchQueue;
  RowBatchQueue batch_queue_;

  // The batch that was most recently returned via GetBatch(), i.e. the current batch
  // from this queue being processed by a consumer. Is destroyed when the next batch
  // is retrieved.
  scoped_ptr<RowBatch> current_batch_;

  // Set to true when the first batch has been received
  bool received_first_batch_ = false;

  // Queue of pending senders - those that have a batch to deliver, but the queue was full
  // when they last tried to do so. The senders wait here until there is a space for their
  // batches, allowing the receiver-side to implement basic flow-control.
  queue<TransmitDataCtx> pending_senders_;
};

DataStreamRecvr::SenderQueue::SenderQueue(DataStreamRecvr* parent_recvr, int num_senders)
    : recvr_(parent_recvr), num_remaining_senders_(num_senders) { }

Status DataStreamRecvr::SenderQueue::GetBatch(RowBatch** next_batch) {
  SCOPED_TIMER(recvr_->queue_get_batch_time_);
  unique_lock<SpinLock> l(lock_);
  // wait until something shows up or we know we're done
  while (!is_cancelled_ && batch_queue_.empty() && num_remaining_senders_ > 0) {
    VLOG_ROW << "wait arrival fragment_instance_id=" << recvr_->fragment_instance_id()
             << " node=" << recvr_->dest_node_id();
    // Don't count time spent waiting on the sender as active time.
    CANCEL_SAFE_SCOPED_TIMER(recvr_->data_arrival_timer_, &is_cancelled_);
    CANCEL_SAFE_SCOPED_TIMER(recvr_->inactive_timer_, &is_cancelled_);
    CANCEL_SAFE_SCOPED_TIMER(
        received_first_batch_ ? nullptr : recvr_->first_batch_wait_total_timer_,
        &is_cancelled_);
    data_arrival_cv_.wait(l);
  }

  // cur_batch_ must be replaced with the returned batch.
  current_batch_.reset();
  *next_batch = nullptr;
  if (is_cancelled_) return Status::CANCELLED;

  if (batch_queue_.empty()) {
    DCHECK_EQ(num_remaining_senders_, 0);
    return Status::OK();
  }

  received_first_batch_ = true;

  DCHECK(!batch_queue_.empty());
  RowBatch* result = batch_queue_.front().second;
  recvr_->num_buffered_bytes_.Add(-batch_queue_.front().first);
  VLOG_ROW << "fetched #rows=" << result->num_rows();
  batch_queue_.pop_front();

  // We consumed a batch, so there's room in the queue. Ask the longest-blocked sender to
  // try again.
  if (!pending_senders_.empty()) {
    SCOPED_TIMER(recvr_->pending_response_timer_);
    recvr_->mgr_->EnqueueSenderReject(move(pending_senders_.front()));
    pending_senders_.pop();
  }

  current_batch_.reset(result);
  *next_batch = current_batch_.get();
  return Status::OK();
}

void DataStreamRecvr::SenderQueue::AddBatch(TransmitDataCtx&& payload) {
  lock_guard<SpinLock> closing_lock(closing_lock_);

  int batch_size = RowBatch::GetBatchSize(payload.proto_batch);

  {
    unique_lock<SpinLock> l(lock_);
    COUNTER_ADD(recvr_->bytes_received_counter_, batch_size);

    // num_remaining_senders_ could be 0 because an AddBatch() can arrive *after* a
    // EndDataStream() RPC for the same sender, due to asynchrony on the sender side (the
    // sender gets closed or cancelled, but doesn't wait for the oustanding TransmitData()
    // to complete before trying to close the channel).
    if (is_cancelled_ || num_remaining_senders_ == 0) {
      Status::OK().ToProto(payload.response->mutable_status());
      payload.context->RespondSuccess();
      return;
    }

    // If there's something in the queue and this batch will push us over the buffer limit
    // we need to wait until the queue gets drained. We store the rpc context so that we
    // can signal it at a later time to resend the batch that we couldn't process here.
    //
    // Note: It's important that we enqueue proto_batch regardless of buffer limit if
    // the queue is currently empty. In the case of a merging receiver, batches are
    // received from a specific queue based on data order, and the pipeline will stall
    // if the merger is waiting for data from an empty queue that cannot be filled because
    // the limit has been reached.
    if (!batch_queue_.empty() && recvr_->ExceedsLimit(batch_size)) {
      // We're not going to use the memory associated with this RPC.
      payload.context->DiscardTransfer();

      // Enqueue pending sender, return.
      pending_senders_.push(move(payload));
      COUNTER_ADD(recvr_->num_rejected_batches_, 1);
      return;
    }
    COUNTER_ADD(recvr_->num_accepted_batches_, 1);
    COUNTER_ADD(recvr_->bytes_accepted_counter_, batch_size);
    recvr_->num_buffered_bytes_.Add(batch_size);
  }

  RowBatch* batch = nullptr;
  {
    SCOPED_TIMER(recvr_->deserialize_row_batch_timer_);
    // Note: if this function makes a row batch, the batch *must* be added to
    // batch_queue_. Close() will handle deleting any unconsumed batches from
    // batch_queue_. Taking closing_lock_ at the beginning of this method ensures that
    // Close() cannot run until after the batch is added to the queue.
    // TODO: move this off this thread.
    batch = new RowBatch(recvr_->row_desc(), payload.proto_batch, recvr_->mem_tracker());
  }

  {
    lock_guard<SpinLock> l(lock_);
    VLOG_ROW << "added #rows=" << batch->num_rows() << " batch_size=" << batch_size;
    batch_queue_.push_back(make_pair(batch_size, batch));
    data_arrival_cv_.notify_one();
  }

  Status::OK().ToProto(payload.response->mutable_status());
  payload.context->RespondSuccess();

}

void DataStreamRecvr::SenderQueue::DecrementSenders() {
  lock_guard<SpinLock> l(lock_);
  DCHECK_GT(num_remaining_senders_, 0);
  num_remaining_senders_ = max(0, num_remaining_senders_ - 1);
  VLOG_FILE << "decremented senders: fragment_instance_id="
            << recvr_->fragment_instance_id()
            << " node_id=" << recvr_->dest_node_id()
            << " #senders=" << num_remaining_senders_;
  if (num_remaining_senders_ == 0) data_arrival_cv_.notify_one();
}

void DataStreamRecvr::SenderQueue::Cancel() {
  {
    lock_guard<SpinLock> l(lock_);
    if (is_cancelled_) return;
    is_cancelled_ = true;
    VLOG_QUERY << "cancelled stream: fragment_instance_id_="
               << recvr_->fragment_instance_id()
               << " node_id=" << recvr_->dest_node_id();
  }
  // Wake up all threads waiting to produce/consume batches.  They will all
  // notice that the stream is cancelled and handle it.
  data_arrival_cv_.notify_all();
  PeriodicCounterUpdater::StopTimeSeriesCounter(
      recvr_->bytes_received_time_series_counter_);
}

void DataStreamRecvr::SenderQueue::Close() {
  lock_guard<SpinLock> c(closing_lock_);
  lock_guard<SpinLock> l(lock_);
  // Note that the queue must be cancelled first before it can be closed or we may
  // risk running into a race which can leak row batches. Please see IMPALA-3034.
  DCHECK(is_cancelled_);

  // Delete any batches queued in batch_queue_
  for (const auto& queue_entry: batch_queue_) delete queue_entry.second;
  while (!pending_senders_.empty()) {
    TransmitDataCtx* payload = &pending_senders_.front();
    Status::OK().ToProto(payload->response->mutable_status());
    payload->context->RespondSuccess();
    pending_senders_.pop();
  }

  current_batch_.reset();
}

void DataStreamRecvr::SenderQueue::CheckPendingSenders() {
  int64_t now = MonotonicMillis();

  vector<TransmitDataCtx> to_notify;
  // Copy the senders to notify to avoid holding lock_ while we respond.
  {
    lock_guard<SpinLock> l(lock_);
    constexpr int32_t TIMEOUT = DataStreamMgr::TRANSMIT_DATA_TIMEOUT_SECONDS / 2;
    while (!pending_senders_.empty()
        && (now - pending_senders_.front().arrival_time_ms) > (TIMEOUT * 1000)) {
      to_notify.push_back(move(pending_senders_.front()));
      pending_senders_.pop();
    }
  }
  for (auto& ctx: to_notify) {
    ctx.context->RespondRpcFailure(ErrorStatusPB::ERROR_SERVER_TOO_BUSY,
        kudu::Status::ServiceUnavailable("Sender queue was full. Please re-send."));
  }
}

Status DataStreamRecvr::CreateMerger(const TupleRowComparator& less_than) {
  DCHECK(is_merging_);
  vector<SortedRunMerger::RunBatchSupplierFn> input_batch_suppliers;
  input_batch_suppliers.reserve(sender_queues_.size());

  // Create the merger that will a single stream of sorted rows.
  merger_.reset(new SortedRunMerger(less_than, &row_desc_, profile_, false));

  for (SenderQueue* queue: sender_queues_) {
    input_batch_suppliers.push_back(
        [queue](RowBatch** next_batch) -> Status {
          return queue->GetBatch(next_batch);
        });
  }

  RETURN_IF_ERROR(merger_->Prepare(input_batch_suppliers));
  return Status::OK();
}

void DataStreamRecvr::TransferAllResources(RowBatch* transfer_batch) {
  for (SenderQueue* sender_queue: sender_queues_) {
    if (sender_queue->current_batch() != nullptr) {
      sender_queue->current_batch()->TransferResourceOwnership(transfer_batch);
    }
  }
}

DataStreamRecvr::DataStreamRecvr(DataStreamMgr* stream_mgr, MemTracker* parent_tracker,
    const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int num_senders, bool is_merging, int total_buffer_limit,
    RuntimeProfile* profile)
  : mgr_(stream_mgr),
    fragment_instance_id_(fragment_instance_id),
    dest_node_id_(dest_node_id),
    total_buffer_limit_(total_buffer_limit),
    row_desc_(row_desc),
    is_merging_(is_merging),
    num_buffered_bytes_(0),
    profile_(profile) {
  mem_tracker_.reset(new MemTracker(-1, "DataStreamRecvr", parent_tracker));
  // Create one queue per sender if is_merging is true.
  int num_queues = is_merging ? num_senders : 1;
  sender_queues_.reserve(num_queues);
  int num_sender_per_queue = is_merging ? 1 : num_senders;
  for (int i = 0; i < num_queues; ++i) {
    SenderQueue* queue =
        sender_queue_pool_.Add(new SenderQueue(this, num_sender_per_queue));
    sender_queues_.push_back(queue);
  }

  // Maintain two child profiles - receiver side measurements (from the GetBatch() path),
  // and sender side measurements (from AddBatch()).
  RuntimeProfile* recvr_side_profile = profile_->CreateChild("RecvrSide");
  RuntimeProfile* sender_side_profile = profile_->CreateChild("SenderSide");

  // Initialize the counters
  bytes_received_counter_ =
      ADD_COUNTER(recvr_side_profile, "TotalBytesReceived", TUnit::BYTES);
  bytes_accepted_counter_ =
      ADD_COUNTER(recvr_side_profile, "AcceptedBytesReceived", TUnit::BYTES);
  bytes_received_time_series_counter_ = ADD_TIME_SERIES_COUNTER(
      recvr_side_profile, "BytesReceived", bytes_received_counter_);
  deserialize_row_batch_timer_ =
      ADD_TIMER(sender_side_profile, "DeserializeRowBatchTimer");
  inactive_timer_ = profile_->inactive_timer();
  queue_get_batch_time_ = ADD_TIMER(recvr_side_profile, "TotalGetBatchTime");
  data_arrival_timer_ =
      ADD_CHILD_TIMER(recvr_side_profile, "DataArrivalTimer", "TotalGetBatchTime");
  pending_response_timer_ =
      ADD_CHILD_TIMER(recvr_side_profile, "PendingResponseTime", "TotalGetBatchTime");
  first_batch_wait_total_timer_ =
      ADD_TIMER(recvr_side_profile, "FirstBatchArrivalWaitTime");
  num_rejected_batches_ =
      ADD_COUNTER(sender_side_profile, "NumBatchesRejected", TUnit::UNIT);
  num_accepted_batches_ =
      ADD_COUNTER(sender_side_profile, "NumBatchesAccepted", TUnit::UNIT);
}

Status DataStreamRecvr::GetNext(RowBatch* output_batch, bool* eos) {
  DCHECK(merger_.get() != nullptr);
  return merger_->GetNext(output_batch, eos);
}

void DataStreamRecvr::AddBatch(TransmitDataCtx&& payload) {
  int use_sender_id = is_merging_ ? payload.request->sender_id() : 0;
  // Add all batches to the same queue if is_merging_ is false.
  sender_queues_[use_sender_id]->AddBatch(move(payload));
}

void DataStreamRecvr::RemoveSender(int sender_id) {
  int use_sender_id = is_merging_ ? sender_id : 0;
  sender_queues_[use_sender_id]->DecrementSenders();
}

void DataStreamRecvr::CancelStream() {
  for (auto& queue: sender_queues_) queue->Cancel();
}

void DataStreamRecvr::Close() {
  // Remove this receiver from the DataStreamMgr that created it.
  // TODO: log error msg
  mgr_->DeregisterRecvr(fragment_instance_id(), dest_node_id());
  mgr_ = nullptr;
  for (auto& queue: sender_queues_) queue->Close();
  merger_.reset();
  mem_tracker_->UnregisterFromParent();
  mem_tracker_.reset();
}

DataStreamRecvr::~DataStreamRecvr() {
  DCHECK(mgr_ == nullptr) << "Must call Close()";
}

Status DataStreamRecvr::GetBatch(RowBatch** next_batch) {
  DCHECK(!is_merging_);
  DCHECK_EQ(sender_queues_.size(), 1);
  return sender_queues_[0]->GetBatch(next_batch);
}

void DataStreamRecvr::ReplyToPendingSenders() {
  for (SenderQueue* queue: sender_queues_) queue->CheckPendingSenders();
}

}
