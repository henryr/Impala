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
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/service_pool.h"
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
  RpcMgr();

  Status Init();
  bool is_inited() const { return messenger_.get() != nullptr; }

  /// Start the acceptor and reactor threads, making RPC services available.
  Status StartServices(int32_t port);

  /// Register a new service of type T (where T extends
  /// kudu::rpc::GeneratedServiceIf). Only one service of each type may be registered.
  ///
  /// 'num_service_threads' is the number of threads that should be started to execute RPC
  /// handlers for the new service.
  /// 'service_queue_depth' is the maximum number of requests that may be queued for this
  /// service before clients being to see rejection errors.
  template <typename T>
  Status RegisterService(
      int32_t num_service_threads, int32_t service_queue_depth, T** service = nullptr);

  /// Creates a new proxy for a remote service of type P at location 'address', and places
  /// it in 'proxy'. Returns an error if 'address' cannot be resolved to an IP address.
  ///
  /// A proxy is a client-side interface to a remote service. RPCs may be called on
  /// that proxy as though they were local methods.
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
  void UnregisterServices();

  ~RpcMgr() {
    DCHECK_EQ(service_pools_.size(), 0)
        << "Must call UnregisterServices() before destroying RpcMgr";
  }

 private:
  /// Details of each service that is registered with this RpcMgr.
  struct ServiceRegistration {
    /// Set during RegisterService(), unset after StartServices() completes successfully.
    gscoped_ptr<kudu::rpc::ServiceIf> service_if;

    /// The number of threads that should execute application logic for this service.
    const int32_t num_service_threads;

    /// The maximum size of the queue that new requests get placed in. If the queue
    /// becomes larger than this, requests are returned to the caller with
    /// ERROR_SERVER_TOO_BUSY.
    const int32_t service_queue_depth;

    /// The name of this service.
    const std::string service_name;

    ServiceRegistration(ServiceRegistration&& other)
      : service_if(std::move(other.service_if)),
        num_service_threads(other.num_service_threads),
        service_queue_depth(other.service_queue_depth),
        service_name(std::move(other.service_name)) {}

    /// Required for emplace_back().
    ServiceRegistration(kudu::rpc::ServiceIf* svc, int32_t num_svc_threads,
        int32_t svc_queue_depth, const std::string& name)
      : service_if(svc),
        num_service_threads(num_svc_threads),
        service_queue_depth(svc_queue_depth),
        service_name(name) {}
  };

  std::vector<ServiceRegistration> service_registrations_;

  /// Required Kudu boilerplate.
  /// TODO(KRPC): Integrate with Impala MetricGroup.
  kudu::MetricRegistry registry_;
  const scoped_refptr<kudu::rpc::ResultTracker> tracker_;
  const scoped_refptr<kudu::MetricEntity> entity_;

  /// Container for reactor threads which run event loops for RPC services, plus acceptor
  /// threads which manage connection setup.
  std::shared_ptr<kudu::rpc::Messenger> messenger_;

  /// List of ServicePools, one per registered service.
  std::vector<scoped_refptr<kudu::rpc::ServicePool>> service_pools_;
};

template <typename T>
Status RpcMgr::RegisterService(
    int32_t num_service_threads, int32_t service_queue_depth, T** service) {
  DCHECK(is_inited()) << "Must call Init() before RegisterService()";
  T* service_ptr = new T(messenger_->metric_entity(), tracker_);
  if (service != nullptr) *service = service_ptr;

  ServiceRegistration registration(
      service_ptr, num_service_threads, service_queue_depth, service_ptr->service_name());
  service_registrations_.push_back(std::move(registration));
  return Status::OK();
}
}

#endif
