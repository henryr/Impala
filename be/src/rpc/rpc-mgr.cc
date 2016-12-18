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

#include "common/names.h"
#include "service/impala_internal_service.service.h"

#include "kudu/util/net/net_util.h"

#include "service/impala-internal-service.h"

using namespace impala;

using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::AcceptorPool;
using kudu::rpc::ServicePool;
using kudu::rpc::ServiceIf;
using kudu::Sockaddr;
using kudu::HostPort;
using kudu::MetricEntity;
using kudu::rpc::ResultTracker;
using kudu::rpc::ServiceIf;

DECLARE_string(hostname);

Status RpcMgr::RegisterServiceImpl(const string& name, ServiceIf* service) {
  gscoped_ptr<ServiceIf> service_ptr(service);
  scoped_refptr<ServicePool> service_pool =
      new ServicePool(std::move(service_ptr), messenger_->metric_entity(), 50);
  messenger_->RegisterService(name, service_pool);
  service_pool->Init(64);
  service_pools_.push_back(service_pool);
  LOG(INFO) << "Service '" << name << "' is listening";
  return Status::OK();
}

Status RpcMgr::Start(int32_t port) {
  scoped_refptr<MetricEntity> entity =
      METRIC_ENTITY_server.Instantiate(&registry_, "impala-server");
  MessengerBuilder bld("impala-server");
  bld.set_num_reactors(4).set_metric_entity(entity);
  bld.Build(&messenger_);

  shared_ptr<AcceptorPool> acceptor_pool;
  HostPort hostport(FLAGS_hostname, port);
  vector<Sockaddr> addresses;
  RETURN_IF_ERROR(FromKuduStatus(hostport.ResolveAddresses(&addresses)));
  DCHECK_GE(addresses.size(), 1);
  RETURN_IF_ERROR(
      FromKuduStatus(messenger_->AddAcceptorPool(addresses[0], &acceptor_pool)));
  RETURN_IF_ERROR(FromKuduStatus(acceptor_pool->Start(2)));
  return Status::OK();
}
