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
#include "rpc/rpc-mgr.h"
#include "rpc/thrift-util.h"
#include "util/time.h"

#include "common/status.h"

namespace impala {

/// Helper class to automate much of the boilerplate required to execute an RPC. Each
/// instance of this class can create RPCs for a particular proxy type P. Clients of this
/// class should create an Rpc object using MakeRpc(). The Rpc class functions as a
/// builder for a single RPC instance. Clients can set timeouts and retry parameters, and
/// then execute an RPC using either Thrift or ProtoBuf arguments.
///
/// For example:
///
/// auto rpc =
///     RpcBuilder<MyServiceProxy>::MakeRpc(address, rpc_mgr).SetTimeout(timeout);
/// RpcRequestPB request;
/// RpcResponsePB response;
/// RETURN_IF_ERROR(rpc.Execute(&MyServiceProxy::Rpc, request, &response));
template <typename P>
class RpcBuilder {
 public:
  template <typename REQ, typename RESP>
  class Rpc {
   public:
    Rpc(const TNetworkAddress& remote, RpcMgr* mgr) : remote_(remote), mgr_(mgr) {}

    Rpc<REQ, RESP>& SetTimeout(kudu::MonoDelta rpc_timeout) {
      rpc_timeout_ = rpc_timeout;
      return *this;
    }

    Rpc<REQ, RESP>& SetMaxAttempts(int32_t max_attempts) {
      max_rpc_attempts_ = max_attempts;
      return *this;
    }

    template <typename F>
    Status Execute(const F& func, const REQ& req, RESP* resp) {
      std::unique_ptr<P> proxy;
      RETURN_IF_ERROR(mgr_->GetProxy(remote_, &proxy));
      Status status = Status::OK();
      for (int i = 0; i < max_rpc_attempts_; ++i) {
        kudu::rpc::RpcController controller;
        controller.set_timeout(rpc_timeout_);

        ((proxy.get())->*func)(req, resp, &controller);
        if (controller.status().ok()) return Status::OK();

        status = FromKuduStatus(controller.status());
        if (!controller.status().IsRemoteError()) break;

        const kudu::rpc::ErrorStatusPB* err = controller.error_response();
        if (!err || !err->has_code()
            || err->code() != kudu::rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY) {
          return FromKuduStatus(controller.status());
        }
        SleepForMs(retry_interval_ms_);
      }

      return status;
    }

    /// Wrapper for Execute() that handles serialization from and to Thrift
    /// arguments. Provided for compatibility with RPCs that have not yet been translated
    /// to native Protobuf.
    template <typename F, typename TREQ, typename TRESP>
    Status ExecuteWithThriftArgs(const F& func, TREQ* req, TRESP* resp) {
      REQ request_proto;
      RETURN_IF_ERROR(SerializeThriftToProtoWrapper(req, &request_proto));

      RESP response_proto;
      RETURN_IF_ERROR(Execute(func, request_proto, &response_proto));

      DeserializeThriftFromProtoWrapper(response_proto, resp);
      return Status::OK();
    }

   private:
    /// The 'uninitalized' state leads to an unbounded timeout.
    /// TODO(KRPC): Warn on uninitialized timeout, or set meaningful default.
    kudu::MonoDelta rpc_timeout_ = kudu::MonoDelta();
    int32_t max_rpc_attempts_ = 3;
    int32_t retry_interval_ms_ = 100;
    const TNetworkAddress remote_;
    RpcMgr* mgr_ = nullptr;
  };

  template <typename REQ, typename RESP>
  static Rpc<REQ, RESP> MakeRpc(const TNetworkAddress& remote, RpcMgr* mgr) {
    DCHECK(mgr != nullptr);
    DCHECK(mgr->is_inited()) << "Tried to build an RPC before RpcMgr::Init() is called";
    return Rpc<REQ, RESP>(remote, mgr);
  }
};
}

#endif
