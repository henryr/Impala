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
#include "common/init.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "rpc/rpc-builder.h"
#include "rpc/rpc_test.proxy.h"
#include "rpc/rpc_test.service.h"
#include "testutil/gtest-util.h"

#include <functional>

#include "common/names.h"

using namespace impala;

using impala::rpc_test::PingRequestPB;
using impala::rpc_test::PingResponsePB;
using impala::rpc_test::PingServiceIf;
using impala::rpc_test::PingServiceProxy;
using kudu::rpc::RpcController;
using kudu::rpc::RpcContext;
using kudu::rpc::ErrorStatusPB;

using namespace std;

namespace impala {

class PingServiceImpl : public PingServiceIf {
 public:
  PingServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker)
    : PingServiceIf(entity, tracker) {}

  virtual void Ping(
      const PingRequestPB* request, PingResponsePB* response, RpcContext* context) {
    response->set_int_response(42);
    cb_(context);
  }

  void set_response_cb(std::function<void(RpcContext*)> cb) { cb_ = cb; }

 private:
  std::function<void(RpcContext*)> cb_ = [](RpcContext* ctx) { ctx->RespondSuccess(); };
};

TEST(RpcMgr, ServiceSmokeTest) {
  RpcMgr mgr;
  mgr.Init();

  ASSERT_OK(mgr.RegisterService<PingServiceImpl>(10, 1024));
  ASSERT_OK(mgr.StartServices(30001));

  unique_ptr<PingServiceProxy> proxy;
  ASSERT_OK(
      mgr.GetProxy<PingServiceProxy>(MakeNetworkAddress("localhost", 30001), &proxy));

  PingRequestPB request;
  PingResponsePB response;
  RpcController controller;
  proxy->Ping(request, &response, &controller);
  ASSERT_EQ(response.int_response(), 42);
  mgr.UnregisterServices();
}

TEST(RpcMgr, RetryPolicyTest) {
  int retries = 0;
  auto cb = [&retries](RpcContext* context) {
    ++retries;
    context->RespondRpcFailure(
        ErrorStatusPB::ERROR_SERVER_TOO_BUSY, kudu::Status::ServiceUnavailable(""));
  };

  RpcMgr mgr;
  mgr.Init();

  PingServiceImpl* impl;
  ASSERT_OK(mgr.RegisterService<PingServiceImpl>(10, 1024, &impl));
  impl->set_response_cb(cb);
  mgr.StartServices(30001);

  auto rpc = RpcBuilder<PingServiceProxy>::Rpc<PingRequestPB, PingResponsePB>(
      MakeNetworkAddress("localhost", 30001), &mgr);

  PingRequestPB request;
  PingResponsePB response;
  rpc.Execute(&PingServiceProxy::Ping, request, &response);

  // Default
  ASSERT_EQ(retries, 3);

  retries = 0;
  rpc.SetMaxAttempts(10);
  rpc.Execute(&PingServiceProxy::Ping, request, &response);
  ASSERT_EQ(retries, 10);

  mgr.UnregisterServices();
}

TEST(RpcMgr, TimeoutTest) {
  auto cb = [](RpcContext* context) {
    SleepForMs(6000);
    context->RespondSuccess();
  };

  RpcMgr mgr;
  mgr.Init();

  PingServiceImpl* impl;
  ASSERT_OK(mgr.RegisterService<PingServiceImpl>(10, 1024, &impl));
  impl->set_response_cb(cb);
  mgr.StartServices(30001);

  auto rpc = RpcBuilder<PingServiceProxy>::Rpc<PingRequestPB, PingResponsePB>(
      MakeNetworkAddress("localhost", 30001), &mgr);

  PingRequestPB request;
  PingResponsePB response;

  int64_t now = MonotonicMillis();
  ASSERT_FALSE(rpc.SetTimeout(kudu::MonoDelta::FromSeconds(3))
                   .Execute(&PingServiceProxy::Ping, request, &response)
                   .ok());
  ASSERT_GE(MonotonicMillis() - now, 3000);

  rpc.SetTimeout(kudu::MonoDelta::FromSeconds(10));
  ASSERT_OK(rpc.Execute(&PingServiceProxy::Ping, request, &response));

  mgr.UnregisterServices();
}
}

IMPALA_TEST_MAIN();
