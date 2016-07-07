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

#include "rpc/rpc-mgr.h"

#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/service_if.h"
#include "kudu/util/net/net_util.h"

#include "gutil/strings/substitute.h"

#include "common/names.h"

using namespace impala;
using std::move;

using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::AcceptorPool;
using kudu::rpc::ServicePool;
using kudu::Sockaddr;
using kudu::HostPort;
using kudu::MetricEntity;

DECLARE_string(hostname);

DEFINE_int32(num_acceptor_threads, 2, "Number of threads dedicated to accepting "
                                      "connection requests for RPC services");

RpcMgr::RpcMgr()
  : entity_(METRIC_ENTITY_server.Instantiate(&registry_, "impala-server")) {}

Status RpcMgr::Init() {
  MessengerBuilder bld("impala-server");
  bld.set_num_reactors(4).set_metric_entity(entity_);
  KUDU_RETURN_IF_ERROR(bld.Build(&messenger_), "Could not build messenger");
  return Status::OK();
}

Status RpcMgr::StartServices(int32_t port) {
  DCHECK(is_inited()) << "Must call Init() before StartServices()";
  for (ServiceRegistration& registration : service_registrations_) {
    scoped_refptr<ServicePool> service_pool =
        new ServicePool(move(registration.service_if), messenger_->metric_entity(),
            registration.service_queue_depth);
    KUDU_RETURN_IF_ERROR(
        messenger_->RegisterService(registration.service_name, service_pool),
        "Could not register service");
    KUDU_RETURN_IF_ERROR(service_pool->Init(registration.num_service_threads),
        "Service pool failed to start");
    service_pools_.push_back(service_pool);

    LOG(INFO) << Substitute("Service '$0' (nthreads: $1, queue depth: $2) is registered",
        registration.service_name, registration.num_service_threads,
        registration.service_queue_depth);
  }

  HostPort hostport(FLAGS_hostname, port);
  vector<Sockaddr> addresses;
  KUDU_RETURN_IF_ERROR(
      hostport.ResolveAddresses(&addresses), "Failed to resolve service address");
  DCHECK_GE(addresses.size(), 1);

  shared_ptr<AcceptorPool> acceptor_pool;
  KUDU_RETURN_IF_ERROR(messenger_->AddAcceptorPool(addresses[0], &acceptor_pool),
      "Failed to add acceptor pool");
  KUDU_RETURN_IF_ERROR(
      acceptor_pool->Start(FLAGS_num_acceptor_threads), "Acceptor pool failed to start");
  VLOG_QUERY << "Started " << FLAGS_num_acceptor_threads << " acceptor threads";
  LOG(INFO) << "All services listening on port: " << port;
  return Status::OK();
}

void RpcMgr::UnregisterServices() {
  if (messenger_.get() == nullptr) return;
  for (auto pool : service_pools_) pool->Shutdown();

  messenger_->UnregisterAllServices();
  messenger_->Shutdown();
  service_pools_.clear();
}
