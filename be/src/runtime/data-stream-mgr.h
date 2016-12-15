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


#ifndef IMPALA_RUNTIME_DATA_STREAM_MGR_H
#define IMPALA_RUNTIME_DATA_STREAM_MGR_H

#include <set>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common/status.h"
#include "runtime/descriptors.h"  // for PlanNodeId
#include "runtime/row-batch.h"
#include "util/metrics.h"
#include "util/promise.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace kudu { namespace rpc { class RpcContext; } }

namespace impala {

class DescriptorTbl;
class DataStreamRecvr;
class RuntimeState;
class TransmitDataRequestPb;
class TransmitDataResponsePb;

/// TRANSMIT DATA PROTOCOL
/// ----------------------
///
/// Impala daemons send tuple data between themselves using a transmission protocol that
/// is managed by DataStreamMgr and related classes. Batches of tuples are sent using the
/// TransmitData() RPC; since more data is usually transmitted than fits into a single
/// RPC, we refer to the ongoing transmission from a client to a server as a 'stream', and
/// the logical connection between them as a 'channel'. Clients and servers are referred
/// to as 'senders' and 'receivers'. The protocol proceeds in three phases.
///
/// Phase 1: Channel establishment
/// ------------------------------
///
/// The protocol proceeeds in three phases. In the first phase the sender initiates a
/// channel with the receiver by sending its first batch. Since the sender may start
/// sending before the receiver is ready, the data stream manager blocks the sender until
/// the receiver has finished initialization.
///
/// The sender does not distinguish this phase from the steady-state data transmission
/// phase, so may time-out etc. as described below.
///
/// Phase 2: Data transmission
/// --------------------------
///
/// After the first batch has been received, the sender continues to send batches, one at
/// a time (so only one RPC is pending completion at any one time). The rate of
/// transmission is controlled by the receiver: a sender will only schedule batch
/// transmission when the previous transmission completes successfully. When a batch is
/// received, a receiver will do one of three things: process it immediately, add it to a
/// fixed-size buffer for later processing, or discard the batch if the buffer is full. In
/// the first two cases, the sender is notified when the batch has been processed, or
/// enqueued, respectively. The sender will then send its next batch.
///
/// In the third case, the sender is added to a list of pending senders - those who have
/// batches to send, but there's no available space for their tuple data. This list of
/// pending senders is notified in turn whenever space becomes available in the batch
/// queue. An error code is returned which causes the sender to retry the previous batch.
///
/// Phase 3: End of stream
/// ----------------------
///
/// When the stream is terminated, the client will send an EndDataStream() RPC to the
/// server. During ordinary operation, this RPC will not be sent until after the final
/// TransmitData() RPC has completed and the stream's contents has been delivered. After
/// EndDataStream() is sent, no TransmitData() RPCs will successfully deliver their
/// payload.
///
/// Exceptional conditions: cancellation, timeouts, failure
/// -------------------------------------------------------
///
/// The protocol must deal with the following complications: asynchronous cancellation of
/// either the receiver or sender, timeouts during RPC transmission, and failure of either
/// the receiver or sender.
///
/// 1. Cancellation
///
/// If the receiver is cancelled (or closed for any other reason, like reaching a limit)
/// before the sender has completed the stream it will be torn down immediately. Any
/// incomplete senders may not be aware of this, and will continue to send batches. The
/// data stream manager on the receiver keeps a record of recently completed receivers so
/// that it may intercept the 'late' data transmissions and immediately reject them with
/// an error that signals the sender should terminate.
///
/// In exceptional circumstances, the data stream manager will garbage-collect the closed
/// receiver record before all senders have completed. In that case the sender will
/// time-out and cancel itself. However, it is usual that the coordinator will initiate
/// cancellation in this case and so the sender will be cancelled well before its timeout
/// period expires.
///
/// The sender RPCs are sent asynchronously to the main thread of fragment instance
/// execution. Senders do not block in TransmitData() RPCs, and may be cancelled at any
/// time. If an RPC is in-flight during cancellation, it will quietly drop its result when
/// it returns as the sender object may have been destroyed.
///
/// 2. Timeouts during RPC transmission
///
/// The sender has a default timeout of 2 minutes for TransmitData() calls. If the timeout
/// is reached, the RPC will be considered failed, and the channel and sender will be
/// cancelled. Therefore the receiver must respond to each RPC within two minutes for the
/// stream to continue correct operation.
///
/// If a batch is consumed immediately by the receiver, the RPC is responded to
/// immediately as well. If the batch is queued, the RPC is also responded to
/// immediately. Both of these cases are designed so that the sender can prepare a new
/// batch while the current one is processed. If the batch is rejected and the sender is
/// queued, the sender may time out if it sits in the pending queue for too long. To avoid
/// this risk, the receiver periodically responds to all senders in the pending queue, and
/// aims to do so halfway through (i.e. 60s) their timeout period. The idea is that, even
/// taking into account clock rate discrepancies and other delays, the error in
/// notification should not exceed 60s and so the sender will not time out.
///
/// 3. Node or instance failure
///
/// If the receiver node fails, RPCs will fail fast and the fragment instance will be
/// cancelled (if there is some kind of hang, the RPCs will time out after 2 minutes, but
/// again the coordinator is likely to initiate cancellation before that).
///
/// If a sender node fails, the receiver relies on the coordinator to detect the failure
/// and cancel all fragments.
///
/// Future improvements
/// -------------------
///
/// * Consider tracking, on the sender, whether a batch has been successfully sent or
///   not. That's enough state to realise that a recvr has failed (rather than not
///   prepared yet), and the data stream mgr can use that to fail an RPC fast, rather than
///   having the closed-stream list. The case where a receiver starts and fails
///   immediately would not be handled - but perhaps it's ok to rely on coordinator
///   cancellation in that case.

/// Context for a TransmitData() RPC. This structure is passed out of the RPC handler
/// itself, and queued by the data stream manager for processing and response.
struct TransmitDataCtx {
  /// Row batch attached to the request. The memory for the row batch is owned by the
  /// 'context' below.
  ProtoRowBatch proto_batch;

  /// Must be responded to once this RPC is finished with.
  kudu::rpc::RpcContext* context;

  /// Request data structure, memory owned by 'context'.
  const TransmitDataRequestPb* request;

  /// Response data structure, will be serialized back to client after 'context' is
  /// responded to.
  TransmitDataResponsePb* response;

  /// Time of construction, used to age unresponded-to RPCs out of the queue for a
  /// channel.
  int64_t arrival_time_ms;

  TransmitDataCtx(const ProtoRowBatch& batch, kudu::rpc::RpcContext* context, const
      TransmitDataRequestPb* request, TransmitDataResponsePb* response)
      : proto_batch(batch), context(context), request(request), response(response),
        arrival_time_ms(MonotonicMillis()) { }
};

/// Singleton class which manages all incoming data streams at a backend node. It
/// provides both producer and consumer functionality for each data stream.
///
/// - RPC service threads use this to add incoming data to streams in response to
///   TransmitData rpcs (AddData()) or to signal end-of-stream conditions (CloseSender()).
/// - Exchange nodes extract data from an incoming stream via a DataStreamRecvr, which is
///   created with CreateRecvr().
//
/// DataStreamMgr also allows asynchronous cancellation of streams via Cancel()
/// which unblocks all DataStreamRecvr::GetBatch() calls that are made on behalf
/// of the cancelled fragment id.
///
/// Exposes three metrics:
///  'senders-blocked-on-recvr-creation' - currently blocked senders.
///  'total-senders-blocked-on-recvr-creation' - total number of blocked senders over
///  time.
///  'total-senders-timedout-waiting-for-recvr-creation' - total number of senders that
///  timed-out while waiting for a receiver.
///
/// TODO: The recv buffers used in DataStreamRecvr should count against
/// per-query memory limits.
class DataStreamMgr {
 public:
  static constexpr int32_t TRANSMIT_DATA_TIMEOUT_SECONDS = 120;

  DataStreamMgr(MetricGroup* metrics);

  /// Create a receiver for a specific fragment_instance_id/node_id destination;
  /// If is_merging is true, the receiver maintains a separate queue of incoming row
  /// batches for each sender and merges the sorted streams from each sender into a
  /// single stream.
  /// Ownership of the receiver is shared between this DataStream mgr instance and the
  /// caller.
  std::shared_ptr<DataStreamRecvr> CreateRecvr(
      RuntimeState* state, const RowDescriptor& row_desc,
      const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
      int num_senders, int buffer_size, RuntimeProfile* profile,
      bool is_merging);

  /// Adds a row batch to the recvr identified by fragment_instance_id/dest_node_id
  /// if the recvr has not been cancelled. sender_id identifies the sender instance
  /// from which the data came.
  ///
  /// If the stream would exceed its buffering limit as a result of queuing this batch,
  /// the batch is discarded and the sender is queued for later notification that it
  /// should retry this transmission.
  ///
  /// TODO: enforce per-sender quotas (something like 200% of buffer_size/#senders),
  /// so that a single sender can't flood the buffer and stall everybody else.
  /// Returns OK if successful, error status otherwise.
  Status AddData(const TUniqueId& fragment_instance_id, const TransmitDataCtx& payload);

  /// Notifies the recvr associated with the fragment/node id that the specified
  /// sender has closed.
  /// Returns OK if successful, error status otherwise.
  Status CloseSender(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
      int sender_id);

  /// Closes all receivers registered for fragment_instance_id immediately.
  void Cancel(const TUniqueId& fragment_instance_id);

  /// Waits for pending sender thread to finish.
  ~DataStreamMgr();

 private:
  friend class DataStreamRecvr;

  /// Periodically calls DataStreamRecvr::ReplyToPendingSenders() to notify senders that
  /// have been blocked for a while to try again.
  boost::scoped_ptr<Thread> pending_sender_thread_;

  /// Used to notify pending_sender_thread_ that it should exit.
  Promise<bool> shutdown_promise_;

  /// Owned by the metric group passed into the constructor
  MetricGroup* metrics_;

  /// Current number of senders waiting for a receiver to register
  IntGauge* num_senders_waiting_;

  /// Total number of senders that have ever waited for a receiver to register
  IntCounter* total_senders_waited_;

  /// Total number of senders that timed-out waiting for a receiver to register
  IntCounter* num_senders_timedout_;

  /// protects all fields below
  boost::mutex lock_;

  /// map from hash value of fragment instance id/node id pair to stream receivers;
  /// Ownership of the stream revcr is shared between this instance and the caller of
  /// CreateRecvr().
  /// we don't want to create a map<pair<TUniqueId, PlanNodeId>, DataStreamRecvr*>,
  /// because that requires a bunch of copying of ids for lookup
  typedef boost::unordered_multimap<uint32_t, std::shared_ptr<DataStreamRecvr>> RecvrMap;
  RecvrMap receiver_map_;

  /// (Fragment instance id, Plan node id) pair that uniquely identifies a stream.
  typedef std::pair<impala::TUniqueId, PlanNodeId> RecvrId;

  /// Less-than ordering for RecvrIds.
  struct ComparisonOp {
    bool operator()(const RecvrId& a, const RecvrId& b) {
      if (a.first.hi < b.first.hi) {
        return true;
      } else if (a.first.hi > b.first.hi) {
        return false;
      } else if (a.first.lo < b.first.lo) {
        return true;
      } else if (a.first.lo > b.first.lo) {
        return false;
      }
      return a.second < b.second;
    }
  };

  /// Ordered set of receiver IDs so that we can easily find all receivers for a given
  /// fragment (by starting at (fragment instance id, 0) and iterating until the fragment
  /// instance id changes), which is required for cancellation of an entire fragment.
  ///
  /// There is one entry in fragment_recvr_set_ for every entry in receiver_map_.
  typedef std::set<RecvrId, ComparisonOp> FragmentRecvrSet;
  FragmentRecvrSet fragment_recvr_set_;

  /// Return the receiver for given fragment_instance_id/node_id, or NULL if not found. If
  /// 'acquire_lock' is false, assumes lock_ is already being held and won't try to
  /// acquire it.
  std::shared_ptr<DataStreamRecvr> FindRecvr(const TUniqueId& fragment_instance_id,
      PlanNodeId node_id, bool acquire_lock = true);

  /// Calls FindRecvr(), but if NULL is returned, wait for up to
  /// FLAGS_datastream_sender_timeout_ms for the receiver to be registered.  Senders may
  /// initialise and start sending row batches before a receiver is ready. To accommodate
  /// this, we allow senders to establish a rendezvous between them and the receiver. When
  /// the receiver arrives, it triggers the rendezvous, and all waiting senders can
  /// proceed. A sender that waits for too long (120s by default) will eventually time out
  /// and abort. The output parameter 'already_unregistered' distinguishes between the two
  /// cases in which this method returns NULL:
  ///
  /// 1. *already_unregistered == true: the receiver had previously arrived and was
  /// already closed
  ///
  /// 2. *already_unregistered == false: the receiver has yet to arrive when this method
  /// returns, and the timeout has expired
  std::shared_ptr<DataStreamRecvr> FindRecvrOrWait(
      const TUniqueId& fragment_instance_id, PlanNodeId node_id,
      bool* already_unregistered);

  /// Remove receiver block for fragment_instance_id/node_id from the map.
  Status DeregisterRecvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

  inline uint32_t GetHashValue(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

  /// Iterates over all receivers and calls
  /// DataStreamRecvr::ReplyToPendingSenders(). Called from pending_sender_thread_.
  void ReplyToPendingSenders();

  /// The coordination primitive used to signal the arrival of a waited-for receiver
  typedef Promise<std::shared_ptr<DataStreamRecvr>> RendezvousPromise;

  /// A reference-counted promise-wrapper used to coordinate between senders and
  /// receivers. The ref_count field tracks the number of senders waiting for the arrival
  /// of a particular receiver. When ref_count returns to 0, the last sender has ceased
  /// waiting (either because of a timeout, or because the receiver arrived), and the
  /// rendezvous can be torn down.
  ///
  /// Access is only thread-safe when lock_ is held.
  struct RefCountedPromise {
    uint32_t ref_count;

    // Without a conveniently copyable smart ptr, we keep a raw pointer to the promise and
    // are careful to delete it when ref_count becomes 0.
    RendezvousPromise* promise;

    void IncRefCount() { ++ref_count; }

    uint32_t DecRefCount() {
      if (--ref_count == 0) delete promise;
      return ref_count;
    }

    RefCountedPromise() : ref_count(0), promise(new RendezvousPromise()) { }
  };

  /// Map from stream (which identifies a receiver) to a (count, promise) pair that gives
  /// the number of senders waiting as well as a shared promise whose value is Set() with
  /// a pointer to the receiver when the receiver arrives. The count is used to detect
  /// when no receivers are waiting, to initiate clean-up after the fact.
  ///
  /// If pending_rendezvous_[X] exists, then receiver_map_[hash(X)] and
  /// fragment_recvr_set_[X] may exist (and vice versa), as entries are removed from
  /// pending_rendezvous_ some time after the rendezvous is triggered by the arrival of a
  /// matching receiver.
  typedef boost::unordered_map<RecvrId, RefCountedPromise> RendezvousMap;
  RendezvousMap pending_rendezvous_;

  /// Map from the time, in ms, that a stream should be evicted from closed_stream_cache
  /// to its RecvrId. Used to evict old streams from cache efficiently. multimap in case
  /// there are multiple streams with the same eviction time.
  typedef std::multimap<int64_t, RecvrId> ClosedStreamMap;
  ClosedStreamMap closed_stream_expirations_;

  /// Cache of recently closed RecvrIds. Used to allow straggling senders to fail fast by
  /// checking this cache, rather than waiting for the missed-receiver timeout to elapse
  /// in FindRecvrOrWait().
  boost::unordered_set<RecvrId> closed_stream_cache_;
};

}

#endif
