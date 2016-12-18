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

#include "service/impala_internal_service.pb.h"
#include "service/impala_internal_service.service.h"

namespace impala {

/// Proxies RPC requests onto their implementing objects for the
/// ImpalaInternalService service.
class ImpalaInternalServiceImpl : public rpc::ImpalaInternalServiceIf {
 public:
  ImpalaInternalServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker)
    : ImpalaInternalServiceIf(entity, tracker) {}

  virtual void TransmitData(const rpc::TransmitDataRequestPB* request,
      rpc::TransmitDataResponsePB* response, kudu::rpc::RpcContext* context);

  virtual void PublishFilter(const rpc::PublishFilterRequestPB* request,
      rpc::PublishFilterResponsePB* response, kudu::rpc::RpcContext* context);

  virtual void UpdateFilter(const rpc::UpdateFilterRequestPB* request,
      rpc::UpdateFilterResponsePB* response, kudu::rpc::RpcContext* context);

  virtual void ExecPlanFragment(const rpc::ExecPlanFragmentRequestPB* request,
      rpc::ExecPlanFragmentResponsePB* response, kudu::rpc::RpcContext* context);

  virtual void ReportExecStatus(const rpc::ReportExecStatusRequestPB* request,
      rpc::ReportExecStatusResponsePB* response, kudu::rpc::RpcContext* context);

  virtual void CancelPlanFragment(const rpc::CancelPlanFragmentRequestPB* request,
      rpc::CancelPlanFragmentResponsePB* response, kudu::rpc::RpcContext* context);
};
}

#endif
