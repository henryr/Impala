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

#include "rpc/rpc-mgr.inline.h"

#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/util/net/net_util.h"
#include "util/webserver.h"

#include "gutil/strings/substitute.h"
#include "rapidjson/document.h"

#include "common/names.h"

using namespace impala;
using namespace rapidjson;

using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::AcceptorPool;
using kudu::rpc::ServiceIf;
using kudu::rpc::ServicePool;
using kudu::Sockaddr;
using kudu::HostPort;
using kudu::MetricEntity;

DECLARE_string(hostname);

DEFINE_int32(num_acceptor_threads, 2, "Number of threads dedicated to accepting "
                                      "connection requests for RPC services");

DEFINE_int32(num_reactor_threads, 8, "Number of threads dedicated to managing "
                                     "network IO for RPC services");

DECLARE_int32(rpc_max_message_size);

Status RpcMgr::Init(int32_t num_reactor_threads) {
  FLAGS_rpc_max_message_size = numeric_limits<int32_t>::max();
  MessengerBuilder bld("impala-server");
  const scoped_refptr<MetricEntity> entity(
      METRIC_ENTITY_server.Instantiate(&registry_, "krpc-metrics"));

  bld.set_num_reactors(num_reactor_threads).set_metric_entity(entity);
  bld.set_connection_keepalive_time(kudu::MonoDelta::FromSeconds(60 * 10));
  KUDU_RETURN_IF_ERROR(bld.Build(&messenger_), "Could not build messenger");
  return Status::OK();
}

Status RpcMgr::RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
    unique_ptr<ServiceIf> service_ptr) {
  DCHECK(is_inited()) << "Must call Init() before RegisterService()";
  DCHECK(!services_started_) << "Must call RegisterService() before StartServices()";
  scoped_refptr<ImpalaServicePool> service_pool =
      new ImpalaServicePool(move(service_ptr), messenger_->metric_entity(), service_queue_depth);
  RETURN_IF_ERROR(service_pool->Init(num_service_threads));

  KUDU_RETURN_IF_ERROR(
      messenger_->RegisterService(service_pool->service_name(), service_pool),
      "Could not register service");
  service_pools_.push_back(service_pool);

  return Status::OK();
}

Status RpcMgr::StartServices(int32_t port, int32_t num_acceptor_threads) {
  DCHECK(is_inited()) << "Must call Init() before StartServices()";
  DCHECK(!services_started_) << "May not call StartServices() twice";
  HostPort hostport(FLAGS_hostname, port);
  vector<Sockaddr> addresses;
  KUDU_RETURN_IF_ERROR(
      hostport.ResolveAddresses(&addresses), "Failed to resolve service address");
  DCHECK_GE(addresses.size(), 1);

  shared_ptr<AcceptorPool> acceptor_pool;
  KUDU_RETURN_IF_ERROR(messenger_->AddAcceptorPool(addresses[0], &acceptor_pool),
      "Failed to add acceptor pool");
  KUDU_RETURN_IF_ERROR(
      acceptor_pool->Start(num_acceptor_threads), "Acceptor pool failed to start");
  VLOG_QUERY << "Started " << num_acceptor_threads << " acceptor threads";
  services_started_ = true;
  return Status::OK();
}

void RpcMgr::UnregisterServices() {
  if (messenger_.get() == nullptr) return;
  for (auto service_pool : service_pools_) service_pool->Shutdown();

  messenger_->UnregisterAllServices();
  messenger_->Shutdown();
}

void RpcMgr::ToJson(Document* doc) {
  using kudu::rpc::DumpRunningRpcsRequestPB;
  using kudu::rpc::DumpRunningRpcsResponsePB;

  DumpRunningRpcsResponsePB response;
  this->messenger_->DumpRunningRpcs(DumpRunningRpcsRequestPB(), &response);
  int32_t num_inbound_calls_in_flight = 0;
  int32_t num_outbound_calls_in_flight = 0;
  for (const auto& cnxn: response.inbound_connections()) {
    num_inbound_calls_in_flight += cnxn.calls_in_flight().size();
  }
  for (const auto& cnxn: response.outbound_connections()) {
    num_outbound_calls_in_flight += cnxn.calls_in_flight().size();
  }

  doc->AddMember("num_outbound_calls_in_flight", num_outbound_calls_in_flight,
      doc->GetAllocator());
  doc->AddMember("num_inbound_calls_in_flight", num_inbound_calls_in_flight,
      doc->GetAllocator());
  doc->AddMember("num_outbound_cnxns", response.outbound_connections().size(),
      doc->GetAllocator());
  doc->AddMember("num_inbound_cnxns", response.outbound_connections().size(),
      doc->GetAllocator());

  Value services(kArrayType);
  for (auto pool : service_pools_) {
    Value service(kObjectType);
    pool->ToJson(&service, doc);
    services.PushBack(service, doc->GetAllocator());
  }

  doc->AddMember("services", services, doc->GetAllocator());
};
