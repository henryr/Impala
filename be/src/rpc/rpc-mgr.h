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

#ifndef IMPALA_RPC_RPC_MGR_H
#define IMPALA_RPC_RPC_MGR_H

#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"

#include "exec/kudu-util.h"

#include "common/status.h"

namespace kudu {
namespace rpc {
class ServiceIf;
class ResultTracker;
}
}

namespace impala {

/// Central manager for all RPC services and clients.
///
/// An RpcMgr managers 0 or more services: RPC interfaces that are a collection of
/// methods. A new service is registered by calling RegisterService(). All services are
/// served on the same port; the underlying RPC layer takes care of de-multiplexing RPC
/// calls to their respective endpoints.
///
/// A client for a remote service can be obtained by calling GetProxy(). Proxies are cheap
/// to create (relative to the cost of an RPC) and need not be cached.
///
/// Each service and proxy interacts with the IO system via a shared pool of 'reactor'
/// threads which respond to incoming and outgoing RPC events. Incoming events are passed
/// immediately to one of two thread pools: new connections are handled by an 'acceptor'
/// pool, and RPC request events are handled by a per-service 'service' pool. The size of
/// these pools may be configured.
///
/// If the rate of incoming RPC requests exceeds the rate at which those requests are
/// processed, some requests will be placed in a FIFO fixed-size queue. If the queue
/// becomes full, the RPC will fail at the caller.
class RpcMgr {
 public:
  /// Start the acceptor and reactor threads, making RPC services available.
  Status Start(int32_t port);

  /// Register a new service of type T (where T extends
  /// kudu::rpc::GeneratedServiceIf). Only one service of each type may be registered.
  template <typename T>
  Status RegisterService(T** service = nullptr) {
    T* service_ptr = new T(messenger_->metric_entity(), tracker_);
    if (service != nullptr) *service = service_ptr;
    return RegisterServiceImpl(service_ptr->service_name(), service_ptr);
  }

  /// Creates a new proxy for a remote service of type P at location 'address', and places
  /// it in 'proxy'. Returns an error if 'address' cannot be resolved to an IP address.
  template <typename P>
  Status GetProxy(const TNetworkAddress& address, std::unique_ptr<P>* proxy) {
    DCHECK(proxy != nullptr);
    // TODO(KRPC): Check if we have been started.
    std::vector<kudu::Sockaddr> addresses;
    KUDU_RETURN_IF_ERROR(
        kudu::HostPort(address.hostname, address.port).ResolveAddresses(&addresses),
        "Couldn't resolve addresses");
    proxy->reset(new P(messenger_, addresses[0]));
    return Status::OK();
  }

  /// Unregisters all previously registered services. The RPC layer continues to run.
  void UnregisterServices() {
    if (messenger_.get() == nullptr) return;
    messenger_->UnregisterAllServices();
  }

 private:
  Status RegisterServiceImpl(const std::string& name, kudu::rpc::ServiceIf* service);

  kudu::MetricRegistry registry_;

  std::shared_ptr<kudu::rpc::Messenger> messenger_;

  const scoped_refptr<kudu::rpc::ResultTracker> tracker_;
};
}

#endif
