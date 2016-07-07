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

#include "common/init.h"
#include "statestore/statestore-subscriber.h"
#include "testutil/gtest-util.h"
#include "testutil/in-process-servers.h"
#include "util/metrics.h"
#include "util/webserver.h"

#include "common/names.h"

using namespace impala;

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_client_ca_certificate);

DECLARE_int32(webserver_port);
DECLARE_int32(state_store_port);

namespace impala {

class StatestoreTest : public testing::Test {
 protected:
  RpcMgr rpc_mgr_;
  unique_ptr<Statestore> statestore_;
  unique_ptr<MetricGroup> metrics_;
  unique_ptr<Webserver> webserver_;

  virtual void SetUp() {
    metrics_.reset(new MetricGroup("foo"));
    webserver_.reset(new Webserver());
    webserver_->Start();
    rpc_mgr_.Init();
    StartThreadInstrumentation(metrics_.get(), webserver_.get());
    statestore_.reset(new Statestore(metrics_.get()));
    statestore_->RegisterWebpages(webserver_.get());
    statestore_->Start();
  }

  virtual void TearDown() {
    statestore_->SetExitFlag();
    statestore_->MainLoop();
  }
};

TEST_F(StatestoreTest, SmokeTest) {
  StatestoreSubscriber sub_will_start("sub1",
      MakeNetworkAddress("localhost", FLAGS_state_store_port + 10),
      MakeNetworkAddress("localhost", FLAGS_state_store_port), &rpc_mgr_, metrics_.get());

  ASSERT_OK(sub_will_start.Start());
  ASSERT_OK(rpc_mgr_.StartServices(FLAGS_state_store_port + 10));

  int64_t now = MonotonicMillis();
  while (
      sub_will_start.num_heartbeats_received() < 3 && (MonotonicMillis() - now < 10000)) {
    SleepForMs(100);
  }

  ASSERT_GE(sub_will_start.num_heartbeats_received(), 3)
      << "Only received " << sub_will_start.num_heartbeats_received() << " heartbeats";

  sub_will_start.Shutdown();

  rpc_mgr_.UnregisterServices();

  // TODO(KRPC): SSL test
}
}

IMPALA_TEST_MAIN();
