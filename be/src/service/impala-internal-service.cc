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

#include "service/impala-internal-service.h"

#include "gen-cpp/ImpalaInternalService_types.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/thrift-util.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/query-state.h"
#include "service/impala-server.h"
#include "service/impala_internal_service.pb.h"

using kudu::rpc::RpcContext;

namespace impala {

DataStreamService::DataStreamService(RpcMgr* mgr)
  : DataStreamServiceIf(mgr->metric_entity(), mgr->result_tracker()) {}

void DataStreamService::EndDataStream(const EndDataStreamRequestPb* request,
    EndDataStreamResponsePb* response, RpcContext* context) {
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());

  VLOG_ROW << "EndDataStream(): instance_id=" << PrintId(finst_id)
           << " node_id=" << request->dest_node_id()
           << " sender_id=" << request->sender_id();

  ExecEnv::GetInstance()->stream_mgr()->CloseSender(
      finst_id, request->dest_node_id(), request->sender_id());
  context->RespondSuccess();
}

void DataStreamService::TransmitData(const TransmitDataRequestPb* request,
    TransmitDataResponsePb* response, RpcContext* context) {
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());

  VLOG_ROW << "TransmitData(): instance_id=" << finst_id
           << " node_id=" << request->dest_node_id()
           << " #rows=" << request->row_batch().num_rows()
           << " sender_id=" << request->sender_id();
  Status status = ExecEnv::GetInstance()->stream_mgr()->AddData(
      finst_id, request->dest_node_id(), request->row_batch(), request->sender_id());
  status.ToProto(response->mutable_status());

  context->RespondSuccess();
}

void DataStreamService::PublishFilter(const PublishFilterRequestPb* request,
    PublishFilterResponsePb* response, RpcContext* context) {
  TUniqueId finst_id;
  finst_id.__set_lo(request->dst_instance_id().lo());
  finst_id.__set_hi(request->dst_instance_id().hi());

  QueryState::ScopedRef qs(GetQueryId(finst_id));
  if (qs.get() != nullptr) {
    FragmentInstanceState* fis = qs->GetFInstanceState(finst_id);
    if (fis != nullptr) {
      fis->PublishFilter(request->filter_id(), request->bloom_filter());
    }
  }

  context->RespondSuccess();
}

void DataStreamService::UpdateFilter(const UpdateFilterRequestPb* request,
    UpdateFilterResponsePb* response, RpcContext* context) {
  ExecEnv::GetInstance()->impala_server()->UpdateFilter(request, response);
  context->RespondSuccess();
}

ExecControlService::ExecControlService(RpcMgr* mgr)
    : ExecControlServiceIf(mgr->metric_entity(), mgr->result_tracker()) {}

void ExecControlService::ExecPlanFragment(
    const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
  TExecPlanFragmentParams thrift_request;
  Status status = DeserializeThriftFromProtoWrapper(*request, &thrift_request);
  TExecPlanFragmentResult return_val;
  if (status.ok()) {
    status = ExecEnv::GetInstance()->query_exec_mgr()->StartFInstance(thrift_request);
  }
  status.SetTStatus(&return_val);
  SerializeThriftToProtoWrapper(&return_val, response);
  context->RespondSuccess();
}

void ExecControlService::ReportExecStatus(
    const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
  TReportExecStatusParams thrift_request;
  Status status = DeserializeThriftFromProtoWrapper(*request, &thrift_request);

  if (status.ok()) {
    TReportExecStatusResult return_val;
    ExecEnv::GetInstance()->impala_server()->ReportExecStatus(return_val, thrift_request);
    SerializeThriftToProtoWrapper(&return_val, response);
  }
  context->RespondSuccess();
}

namespace {

Status GetUnknownIdError(const string& id_type, const TUniqueId& id) {
  return Status(ErrorMsg(
      TErrorCode::INTERNAL_ERROR, Substitute("Unknown $0 id: $1", id_type, PrintId(id))));
}

}

void ExecControlService::CancelPlanFragment(
    const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
  TCancelPlanFragmentParams thrift_request;
  Status status = DeserializeThriftFromProtoWrapper(*request, &thrift_request);

  TCancelPlanFragmentResult return_val;
  if (status.ok()) {
    QueryState::ScopedRef qs(GetQueryId(thrift_request.fragment_instance_id));
    if (qs.get() == nullptr) {
      status =
          GetUnknownIdError("query", GetQueryId(thrift_request.fragment_instance_id));
    } else {
      FragmentInstanceState* fis =
          qs->GetFInstanceState(thrift_request.fragment_instance_id);
      if (fis == nullptr) {
        status = GetUnknownIdError("instance", thrift_request.fragment_instance_id);
      } else {
        status = fis->Cancel();
      }
    }
  }

  status.SetTStatus(&return_val);
  SerializeThriftToProtoWrapper(&return_val, response);
  context->RespondSuccess();
}
}
