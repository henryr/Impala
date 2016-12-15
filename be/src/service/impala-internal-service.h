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

class RpcMgr;

/// Handles control messages for plan fragment instance execution by proxying them onto
/// ImpalaServer and QueryState objects.
class ExecControlService : public ExecControlServiceIf {
 public:
  ExecControlService(RpcMgr* rpc_mgr);

  virtual void ExecPlanFragment(const ThriftWrapperPb* request, ThriftWrapperPb* response,
      kudu::rpc::RpcContext* context);

  virtual void ReportExecStatus(const ThriftWrapperPb* request, ThriftWrapperPb* response,
      kudu::rpc::RpcContext* context);

  virtual void CancelPlanFragment(const ThriftWrapperPb* request,
      ThriftWrapperPb* response, kudu::rpc::RpcContext* context);
};

class DataStreamService : public DataStreamServiceIf {
 public:
  DataStreamService(RpcMgr* rpc_mgr);

  virtual void EndDataStream(const EndDataStreamRequestPb* request,
      EndDataStreamResponsePb* response, kudu::rpc::RpcContext* context);

  virtual void TransmitData(const TransmitDataRequestPb* request,
      TransmitDataResponsePb* response, kudu::rpc::RpcContext* context);

  virtual void PublishFilter(const PublishFilterRequestPb* request,
      PublishFilterResponsePb* response, kudu::rpc::RpcContext* context);

  virtual void UpdateFilter(const UpdateFilterRequestPb* request,
      UpdateFilterResponsePb* response, kudu::rpc::RpcContext* context);
};

}

#endif
