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

#ifndef IMPALA_SERVICE_IMPALA_INTERNAL_SERVICE_H
#define IMPALA_SERVICE_IMPALA_INTERNAL_SERVICE_H

#include <boost/shared_ptr.hpp>

#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "service/impala-server.h"
#include "service/fragment-mgr.h"
#include "testutil/fault-injection-util.h"

#include "runtime/debug-rules.h"

namespace impala {

namespace debug {
static const std::string EXEC_PLAN_FRAGMENT = "execplanfragment";
static const std::string CANCEL_PLAN_FRAGMENT = "cancelplanfragment";
static const std::string REPORT_EXEC_STATUS = "reportexecstatus";
static const std::string TRANSMIT_DATA = "transmitdata";
static const std::string UPDATE_FILTER = "updatefilter";
static const std::string PUBLISH_FILTER = "publishfilter";
static const std::string INSTALL_DEBUG_ACTIONS = "installdebugactions";
}

/// Proxies Thrift RPC requests onto their implementing objects for the
/// ImpalaInternalService service.
class ImpalaInternalService : public ImpalaInternalServiceIf {
 public:
  ImpalaInternalService(const boost::shared_ptr<ImpalaServer>& impala_server,
      const boost::shared_ptr<FragmentMgr>& fragment_mgr)
      : impala_server_(impala_server), fragment_mgr_(fragment_mgr) { }

  virtual void ExecPlanFragment(TExecPlanFragmentResult& return_val,
      const TExecPlanFragmentParams& params) {
    RPC_TRACE_POINT(debug::EXEC_PLAN_FRAGMENT, "start_rpc", 0, return_val);
    fragment_mgr_->ExecPlanFragment(params).SetTStatus(&return_val);
    RPC_TRACE_POINT(debug::EXEC_PLAN_FRAGMENT, "end_rpc", 0, return_val);
  }

  virtual void CancelPlanFragment(TCancelPlanFragmentResult& return_val,
      const TCancelPlanFragmentParams& params) {
    RPC_TRACE_POINT(debug::CANCEL_PLAN_FRAGMENT, "start_rpc", 0, return_val);
    fragment_mgr_->CancelPlanFragment(return_val, params);
    RPC_TRACE_POINT(debug::CANCEL_PLAN_FRAGMENT, "end_rpc", 0, return_val);
  }

  virtual void ReportExecStatus(TReportExecStatusResult& return_val,
      const TReportExecStatusParams& params) {
    RPC_TRACE_POINT(debug::REPORT_EXEC_STATUS, "start_rpc", 0, return_val);
    impala_server_->ReportExecStatus(return_val, params);
    RPC_TRACE_POINT(debug::REPORT_EXEC_STATUS, "end_rpc", 0, return_val);
  }

  virtual void TransmitData(TTransmitDataResult& return_val,
      const TTransmitDataParams& params) {
    RPC_TRACE_POINT(debug::TRANSMIT_DATA, "start_rpc", 0, return_val);
    impala_server_->TransmitData(return_val, params);
    RPC_TRACE_POINT(debug::TRANSMIT_DATA, "end_rpc", 0, return_val);
  }

  virtual void UpdateFilter(TUpdateFilterResult& return_val,
      const TUpdateFilterParams& params) {
    RPC_TRACE_POINT(debug::UPDATE_FILTER, "start_rpc", 0, return_val);
    impala_server_->UpdateFilter(return_val, params);
    RPC_TRACE_POINT(debug::UPDATE_FILTER, "end_rpc", 0, return_val);
  }

  virtual void PublishFilter(TPublishFilterResult& return_val,
      const TPublishFilterParams& params) {
    RPC_TRACE_POINT(debug::PUBLISH_FILTER, "start_rpc", 0, return_val);
    fragment_mgr_->PublishFilter(return_val, params);
    RPC_TRACE_POINT(debug::PUBLISH_FILTER, "end_rpc", 0, return_val);
  }

  virtual void InstallDebugActions(TInstallDebugActionsResponse& return_val,
      const TInstallDebugActionsParams& params) {
    RPC_TRACE_POINT(debug::INSTALL_DEBUG_ACTIONS, "start_rpc", 0, return_val);
    impala_server_->InstallDebugRules(params.actions_json, params.distribute)
        .SetTStatus(&return_val);
    RPC_TRACE_POINT(debug::INSTALL_DEBUG_ACTIONS, "end_rpc", 0, return_val);
  }

 private:
  /// Manages fragment reporting and data transmission
  boost::shared_ptr<ImpalaServer> impala_server_;

  /// Manages fragment execution
  boost::shared_ptr<FragmentMgr> fragment_mgr_;
};

}

#endif
