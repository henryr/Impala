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

#ifndef IMPALA_RPC_RPC_BUILDER_H
#define IMPALA_RPC_RPC_BUILDER_H

#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "rpc/common.pb.h"
#include "rpc/rpc-mgr.inline.h"
#include "rpc/thrift-util.h"
#include "util/time.h"

#include "common/status.h"

namespace impala {

/// Helper class to automate much of the boilerplate required to execute an RPC. Each
/// concrete type of this class can create RPCs for a particular proxy type P. Clients of
/// this class should create an Rpc object using Make(). The Rpc class functions as a
/// builder for a single remote method invocation. Clients can set timeouts and retry
/// parameters, and then execute a remote method using either Thrift or ProtoBuf arguments.
///
/// For example:
///
/// auto rpc = Rpc<MyServiceProxy>::Make(address, rpc_mgr)
///     .SetTimeout(timeout)
///     .SetRetryInterval(500);
/// RpcRequestPb request;
/// RpcResponsePb response;
/// RETURN_IF_ERROR(rpc.Execute(&MyServiceProxy::Rpc, request, &response));
///
/// For the async case:
///
/// unique_ptr<RpcRequestPB> request = make_unique<RpcRequestPB>();
/// unique_ptr<RpcResponsePB> response = make_unique<RpcResponsePB>();
///
/// // Release request and response into the completion handler, which is guaranteed to be
/// // called.
/// Rpc<MyServiceProxy>::Make(address, rpc_mgr)
///     .ExecuteAsync(&MyServiceProxy::Rpc, request.release(), response.release(),
///       [](const Status& status, RpcRequestPB* request, RpcResponsePB* response) {
///         if (!status.ok()) LOG(INFO) << "Error!";
///         delete request;
///         delete response;
///     });
///
/// All RPCs must have a timeout set. The default timeout is 5 minutes.

/// TODO: Move these into Rpc<>?
static constexpr int RPC_DEFAULT_MAX_ATTEMPTS = 3;
static constexpr int RPC_DEFAULT_RETRY_INTERVAL_MS = 100;
static constexpr int RPC_DEFAULT_TIMEOUT_S = 300;

template <typename P>
class Rpc {
 public:
  /// Factory method to create new Rpcs. 'remote' is the address of the machine on which
  /// the service to be called is running.
  static Rpc Make(const TNetworkAddress& remote, RpcMgr* mgr) {
    DCHECK(mgr != nullptr);
    DCHECK(mgr->is_inited()) << "Tried to build an RPC before RpcMgr::Init() is called";
    return Rpc(remote, mgr);
  }

  /// Sets the timeout for TCP writes and reads. If this timeout expires, Execute() will
  /// return an rpc-layer error.
  Rpc& SetTimeout(kudu::MonoDelta rpc_timeout) {
    rpc_timeout_ = rpc_timeout;
    return *this;
  }

  /// Sets the maximum number of attempts for retry when the remote service is too busy.
  Rpc& SetMaxAttempts(int32_t max_attempts) {
    DCHECK_LE(1, max_attempts);
    max_rpc_attempts_ = max_attempts;
    return *this;
  }

  /// Sets the maximum number of attempts for retry when the remote service is too busy.
  Rpc& SetRetryInterval(int32_t interval_ms) {
    DCHECK_LT(0, interval_ms);
    retry_interval_ms_ = interval_ms;
    return *this;
  }

  /// Handles asynchronous execution, and retry. The completion callback 'cb' is
  /// guaranteed to be called exactly once after the invocation and any retries have
  /// finished - whether the RPC was successful or not. The callback should have the
  /// following signature:
  ///
  /// CB(const Status& status, REQ* req, RESP* resp, RpcController* controller)
  ///
  /// where
  ///   'status' is OK if the RPC was successfully aattempted (i.e. there were no
  ///   problems acquiring the resources to send the RPC)
  ///   'req' and 'resp' are the protobuf messages passed into ExecuteAsync().
  ///   'controller' is the KRPC RpcController which contains the status of the RPC
  ///   attempt. If !status.ok(), may be nullptr.
  ///
  /// The RPC parameter objects 'req' and 'resp' are owned by the caller, and must not be
  /// destroyed before 'cb' is called. A convenient pattern is to allocate 'req' and
  /// 'resp' on the heap and delete them in 'cb'.
  template <typename F, typename REQ, typename RESP, typename CB>
  void ExecuteAsync(const F& func, REQ* req, RESP* resp, const CB& cb) const {
    Status status = CheckConfiguration();
    if (!status.ok()) {
      cb(status, req, resp, nullptr);
      return;
    }
    ExecuteAsyncHelper(func, req, resp, 0, cb);
  }

  /// Executes this RPC. If the remote service is too busy, execution is re-attempted up
  /// to a fixed number of times, after which an error is returned. Retries are attempted
  /// only if the remote server signals that it is too busy. Retries are spaced by the
  /// configured retry interval. All return values are the RPC-layer status; if OK() then
  /// the RPC was successfully executed. Otherwise, the remote service indicated an
  /// RPC-level failure. Application-level failures should be returned as Protobuf member
  /// fields.
  ///
  /// The actual method to invoke is passed in 'func' and is of type 'F', and is typically
  /// a member function pointer, e.g.:
  ///
  /// rpc.Execute(&MyServiceProxy::SomeRpc, request, &response);
  ///
  /// Therefore F is the type of a synchronous RPC method, with signature:
  ///
  /// func(const REQ& request, RESP* response, RpcController* controller).
  template <typename F, typename REQ, typename RESP>
  Status Execute(const F& func, const REQ& req, RESP* resp) {
    RETURN_IF_ERROR(CheckConfiguration());
    std::unique_ptr<P> proxy;
    RETURN_IF_ERROR(mgr_->GetProxy(remote_, &proxy));
    kudu::rpc::RpcController controller;
    for (int i = 0; i < max_rpc_attempts_; ++i) {
      controller.Reset();
      controller.set_timeout(rpc_timeout_);

      ((proxy.get())->*func)(req, resp, &controller);
      if (controller.status().ok()) return Status::OK();

      // Retry only if the remote is too busy. Otherwise we fail fast.
      if (!IsRetryableError(controller)) return FromKuduStatus(controller.status());

      SleepForMs(retry_interval_ms_);
    }

    return FromKuduStatus(controller.status());
  }

  /// Wrapper for Execute() that handles serialization from and to Thrift
  /// arguments. Provided for compatibility with RPCs that have not yet been translated to
  /// native Protobuf. Returns an error if serialization to or from protobuf fails,
  /// otherwise returns the same as Execute().
  template <typename F, typename TREQ, typename TRESP>
  Status ExecuteWithThriftArgs(const F& func, TREQ* req, TRESP* resp) {
    ThriftWrapperPb request_proto;
    RETURN_IF_ERROR(SerializeThriftToProtoWrapper(req, &request_proto));

    ThriftWrapperPb response_proto;
    RETURN_IF_ERROR(Execute(func, request_proto, &response_proto));

    RETURN_IF_ERROR(DeserializeThriftFromProtoWrapper(response_proto, resp));
    return Status::OK();
  }

  /// Wrapper for ExecuteAsync() that handles serialization from and to Thrift arguments.
  template <typename F, typename TREQ, typename TRESP, typename CB>
  void ExecuteWithThriftArgsAsync(
      const F& func, TREQ* thrift_req, TRESP* thrift_resp, const CB& cb) const {
    // There are two levels of RPC parameters at play here:
    //
    // thrift_req and thrift_resp are owned by the caller, and should be deleted by the
    // caller, typically during the user-supplied completion callback. They are 'wrapped'
    // by the *_proto structures.
    //
    // request_proto and response_proto are owned by this method, and are deleted by the
    // wrapper callback 'completion' below.
    std::unique_ptr<ThriftWrapperPb> request_proto = std::make_unique<ThriftWrapperPb>();
    Status status = SerializeThriftToProtoWrapper(thrift_req, request_proto.get());
    if (!status.ok()) {
      cb(status, thrift_req, thrift_resp, nullptr);
      return;
    }

    std::unique_ptr<ThriftWrapperPb> response_proto = std::make_unique<ThriftWrapperPb>();

    // Completion callback that handles deserializing the response_proto to Thrift, and
    // then calls the caller-supplied callback in all cases.
    auto completion = [thrift_req, thrift_resp, cb = std::move(cb)]
        (const Status& status, ThriftWrapperPb* request, ThriftWrapperPb* response,
            kudu::rpc::RpcController* controller) {

      // Most callers will delete thrift_req and thrift_resp inside the callback, so don't
      // refer to them after cb() is called.
      if (status.ok() && controller->status().ok()) {
        Status deser_status = DeserializeThriftFromProtoWrapper(*response, thrift_resp);
        cb(deser_status, thrift_req, thrift_resp, controller);
      } else {
        cb(status, thrift_req, thrift_resp, controller);
      }

      // Delete the request and response objects (which are request_proto and
      // response_proto).
      delete request;
      delete response;
    };

    // Call the standard asynchronous handler with the wrapper completion callback.
    ExecuteAsync(func, request_proto.release(), response_proto.release(), completion);
  }

 private:
  /// All RPCs must have a valid timeout. The default is five minutes, which should
  /// greatly exceed the maximum RPC processing time.
  kudu::MonoDelta rpc_timeout_ = kudu::MonoDelta::FromSeconds(RPC_DEFAULT_TIMEOUT_S);

  /// The maximum number of retries for this RPC before an error is returned to the
  /// caller.
  int32_t max_rpc_attempts_ = RPC_DEFAULT_MAX_ATTEMPTS;

  /// The time, in ms, between retry attempts.
  int32_t retry_interval_ms_ = RPC_DEFAULT_RETRY_INTERVAL_MS;

  /// The address of the remote machine to send the RPC to.
  const TNetworkAddress remote_;

  /// The RpcMgr handling this RPC.
  RpcMgr* mgr_ = nullptr;

  Rpc(const TNetworkAddress& remote, RpcMgr* mgr) : remote_(remote), mgr_(mgr) {}

  Status CheckConfiguration() const {
    if (max_rpc_attempts_ < 1) {
      return Status(
          strings::Substitute("Invalid number of retry attempts: $0", max_rpc_attempts_));
    }

    if (max_rpc_attempts_ > 1 && retry_interval_ms_ <= 0) {
      return Status(
          strings::Substitute("Invalid retry interval: $0", retry_interval_ms_));
    }

    return Status::OK();
  }

  /// Returns true if the controller is in an error state that corresponds to the remote
  /// server being too busy to handle this request. In that case, we may want to retry
  /// after waiting.
  static bool IsRetryableError(const kudu::rpc::RpcController& controller) {
    const kudu::rpc::ErrorStatusPB* err = controller.error_response();
    return controller.status().IsRemoteError() && err && err->has_code()
        && err->code() == kudu::rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY;
  }

  /// Helper method for ExecuteAsync(). Implements retry logic by scheduling a retry task
  /// on a reactor thread.
  template <typename F, typename REQ, typename RESP, typename CB>
  void ExecuteAsyncHelper(
      const F& func, REQ* req, RESP* resp, int num_attempts, const CB& cb) const {
    using RpcController = kudu::rpc::RpcController;
    std::unique_ptr<RpcController> controller = std::make_unique<RpcController>();
    controller->set_timeout(rpc_timeout_);
    RpcController* controller_ptr = controller.get();

    ++num_attempts;
    // Wraps the supplied completion callback, and implements retry logic for asynchronous
    // RPCs. May be executed after the enclosing ExecuteAsyncHelper() call has finished,
    // and indeed after the enclosing Rpc object has been destroyed. Therefore capture all
    // state necessary to retry the RPC by value.
    //
    // When the RPC has either successfully been executed, or the number of retries has
    // exceeded the limit, the completion callback 'cb' is called.
    //
    // Retries are scheduled on a reactor thread. Since this callback is also called from
    // a reactor thread, we cannot sleep here before rescheduling (like we do in the
    // synchronous case).
    //
    // To avoid forcing the caller to manage the lifetime of this Rpc object until the rpc
    // finally completes, we copy *this into the completion callback to ensure we can
    // access retry parameters.
    auto cb_wrapper = [rpc = *this, func, req, resp, cb = std::move(cb),
        controller_ptr = controller.release(), num_attempts]() {
      // Ensure that controller is always deleted on function exit.
      std::unique_ptr<kudu::rpc::RpcController> controller(controller_ptr);

      // If this RPC should not be retried, call the completion function.
      if (!IsRetryableError(*controller_ptr) || num_attempts >= rpc.max_rpc_attempts_) {
        cb(Status::OK(), req, resp, controller_ptr);
        return;
      }

      // Create a new task that retries the execution from a reactor thread.
      auto reactor_task = [rpc, func, req, resp, cb = std::move(cb), num_attempts] (
          const kudu::Status& status) {
        // Here 'status' refers to the success of scheduling on the reactor thread itself,
        // which typically does not fail.
        if (!status.ok()) {
          cb(FromKuduStatus(status), req, resp, nullptr);
          return;
        }
        rpc.ExecuteAsyncHelper(func, req, resp, num_attempts, cb);
      };
      rpc.mgr_->messenger()->ScheduleOnReactor(
          reactor_task, kudu::MonoDelta::FromMilliseconds(rpc.retry_interval_ms_));
    };

    std::unique_ptr<P> proxy;
    Status status = mgr_->GetProxy(remote_, &proxy);
    if (!status.ok()) {
      cb(status, req, resp, nullptr);
      return;
    }

    ((proxy.get())->*func)(*req, resp, controller_ptr, std::move(cb_wrapper));
  }
};
}

#endif
