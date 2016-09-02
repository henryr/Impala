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

//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService and ImpalaInternalService.

#include <unistd.h>
#include <jni.h>

#include "common/logging.h"
#include "common/init.h"
#include "exec/hbase-table-scanner.h"
#include "exec/hbase-table-writer.h"
#include "exprs/hive-udf-call.h"
#include "runtime/hbase-table.h"
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "util/jni-util.h"
#include "util/network-util.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "rpc/rpc-trace.h"
#include "service/impala-server.h"
#include "service/fe-support.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "util/impalad-metrics.h"
#include "util/thread.h"

#include <memory>

#include "common/names.h"

using namespace impala;

DECLARE_string(classpath);
DECLARE_bool(use_statestore);
DECLARE_int32(beeswax_port);
DECLARE_int32(hs2_port);
DECLARE_int32(be_port);
DECLARE_string(principal);

#include "kudu/util/net/sockaddr.h"
#include "kudu/kudu_rpc/messenger.h"
#include "kudu/kudu_rpc/acceptor_pool.h"
#include "kudu/kudu_rpc/service_if.h"
#include "kudu/kudu_rpc/rpc_controller.h"
#include "kudu/kudu_rpc/service_pool.h"
#include "kudu/kudu_rpc/rpc_context.h"

#include "service/prototest.pb.h"
#include "service/prototest.service.h"
#include "service/prototest.proxy.h"

using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::AcceptorPool;
using kudu::Sockaddr;

using kudu::rpc::ServiceIf;

namespace kudu {
namespace rpc {

using namespace rpc_test;

class PingServiceImpl : public kudu::rpc_test::PingServiceIf {
 public:
  PingServiceImpl(const scoped_refptr<MetricEntity>& entity) : PingServiceIf(entity) { }

  virtual void Ping(const kudu::rpc_test::PingRequestPB* req,
      kudu::rpc_test::PingResponsePB* resp, kudu::rpc::RpcContext* context) {
    LOG(INFO) << "HNR:: PING RECEIVED";
    context->RespondSuccess();
  }
};

MetricRegistry registry;

std::shared_ptr<Messenger> StartMessenger(const string& name) {
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test-ping");

  MessengerBuilder bld(name);
  bld.set_num_reactors(4);
  bld.set_metric_entity(entity);
  std::shared_ptr<Messenger> messenger;
  bld.Build(&messenger);
  return messenger;
}

void StartAndPing() {

  std::shared_ptr<Messenger> msg = StartMessenger("hello-svc");
  std::shared_ptr<AcceptorPool> pool;
  msg->AddAcceptorPool(Sockaddr(), &pool);
  pool->Start(2);
  Sockaddr server_addr = pool->bind_address();

  gscoped_ptr<ServiceIf> service(new PingServiceImpl(msg->metric_entity()));
  scoped_refptr<ServicePool> service_pool = new ServicePool(std::move(service), msg->metric_entity(), 50);
  msg->RegisterService(PingServiceIf::static_service_name(), service_pool);
  service_pool->Init(4);

  RpcController controller;
  // shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  PingRequestPB request;
  PingResponsePB response;
  PingServiceProxy p(msg, server_addr);
  p.Ping(request, &response, &controller);

  msg->UnregisterAllServices();
}

}}


int ImpaladMain(int argc, char** argv) {
  InitCommonRuntime(argc, argv, true);

  LlvmCodeGen::InitializeLlvm();
  JniUtil::InitLibhdfs();
  ABORT_IF_ERROR(HBaseTableScanner::Init());
  ABORT_IF_ERROR(HBaseTable::InitJNI());
  ABORT_IF_ERROR(HBaseTableWriter::InitJNI());
  ABORT_IF_ERROR(HiveUdfCall::Init());
  InitFeSupport();

  kudu::rpc::StartAndPing();

  // start backend service for the coordinator on be_port
  ExecEnv exec_env;
  StartThreadInstrumentation(exec_env.metrics(), exec_env.webserver());
  InitRpcEventTracing(exec_env.webserver());

  ThriftServer* beeswax_server = NULL;
  ThriftServer* hs2_server = NULL;
  ThriftServer* be_server = NULL;
  ImpalaServer* server = NULL;
  ABORT_IF_ERROR(CreateImpalaServer(&exec_env, FLAGS_beeswax_port, FLAGS_hs2_port,
      FLAGS_be_port, &beeswax_server, &hs2_server, &be_server, &server));

  ABORT_IF_ERROR(be_server->Start());

  ABORT_IF_ERROR(beeswax_server->Start());
  ABORT_IF_ERROR(hs2_server->Start());
  Status status = exec_env.StartServices();
  if (!status.ok()) {
    LOG(ERROR) << "Impalad services did not start correctly, exiting.  Error: "
               << status.GetDetail();
    ShutdownLogging();
    exit(1);
  }
  ImpaladMetrics::IMPALA_SERVER_READY->set_value(true);
  LOG(INFO) << "Impala has started.";
  // this blocks until the beeswax and hs2 servers terminate
  beeswax_server->Join();
  hs2_server->Join();

  delete be_server;
  delete beeswax_server;
  delete hs2_server;

  return 0;
}
