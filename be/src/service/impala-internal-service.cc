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
#include "runtime/query-exec-mgr.h"
#include "runtime/query-state.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "service/impala-server.h"
#include "service/impala_internal_service.pb.h"

using kudu::rpc::RpcContext;

using impala::rpc::ImpalaInternalServiceIf;
using impala::rpc::TransmitDataRequestPB;
using impala::rpc::TransmitDataResponsePB;
using impala::rpc::PublishFilterRequestPB;
using impala::rpc::PublishFilterResponsePB;

using boost::lexical_cast;

using namespace impala::rpc; // TODO - > decide if this a hack.

namespace impala {

void ImpalaInternalServiceImpl::TransmitData(const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, RpcContext* context) {
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());

  VLOG_ROW << "TransmitData(): instance_id=" << finst_id
           << " node_id=" << request->dest_node_id()
           << " #rows=" << request->row_batch().num_rows()
           << " sender_id=" << request->sender_id()
           << " eos=" << (request->eos() ? "true" : "false");
  if (request->row_batch().num_rows() > 0) {
    Status status = ExecEnv::GetInstance()->stream_mgr()->AddData(
        finst_id, request->dest_node_id(), request->row_batch(),
        request->sender_id());
    status.ToProto(response->mutable_status());
    if (!status.ok()) {
      // should we close the channel here as well?
      // TODO(KRPC) - context->RespondSuccess()
      return;
    }
  }

  if (request->eos()) {
    ExecEnv::GetInstance()->stream_mgr()->CloseSender(
        finst_id, request->dest_node_id(),
        request->sender_id()).ToProto(response->mutable_status());
  }
  context->RespondSuccess();
}

void ImpalaInternalServiceImpl::PublishFilter(const PublishFilterRequestPB* request,
    PublishFilterResponsePB* response, RpcContext* context) {
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

void ImpalaInternalServiceImpl::UpdateFilter(const UpdateFilterRequestPB* request,
    UpdateFilterResponsePB* response, RpcContext* context) {
  ExecEnv::GetInstance()->impala_server()->UpdateFilter(request, response);
  context->RespondSuccess();
}

void ImpalaInternalServiceImpl::ExecPlanFragment(const ExecPlanFragmentRequestPB* request,
    ExecPlanFragmentResponsePB* response, RpcContext* context) {
  TExecPlanFragmentParams thrift_request;
  uint32_t len = request->thrift_struct().size();
  Status status = DeserializeThriftMsg(
      reinterpret_cast<const uint8_t*>(request->thrift_struct().data()), &len, true,
      &thrift_request);
  TExecPlanFragmentResult return_val;
  ExecEnv::GetInstance()->query_exec_mgr()->StartFInstance(thrift_request).SetTStatus(&return_val);
  SerializeThriftToProtoWrapper(&return_val, true, response);
  context->RespondSuccess();
}

void ImpalaInternalServiceImpl::ReportExecStatus(const ReportExecStatusRequestPB* request,
    ReportExecStatusResponsePB* response, RpcContext* context) {
  TReportExecStatusParams thrift_request;
  uint32_t len = request->thrift_struct().size();
  Status status = DeserializeThriftMsg(
      reinterpret_cast<const uint8_t*>(request->thrift_struct().data()), &len, true,
      &thrift_request);
  TReportExecStatusResult return_val;
  ExecEnv::GetInstance()->impala_server()->ReportExecStatus(return_val, thrift_request);
  SerializeThriftToProtoWrapper(&return_val, true, response);
  context->RespondSuccess();
}

template <typename T> void SetUnknownIdError(
    const string& id_type, const TUniqueId& id, T* status_container) {
  Status status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
          Substitute("Unknown $0 id: $1", id_type, lexical_cast<string>(id))));
  status.SetTStatus(status_container);
}

void ImpalaInternalServiceImpl::CancelPlanFragment(
    const CancelPlanFragmentRequestPB* request, CancelPlanFragmentResponsePB* response,
    RpcContext* context) {
  TCancelPlanFragmentParams thrift_request;
  uint32_t len = request->thrift_struct().size();
  Status status = DeserializeThriftMsg(
      reinterpret_cast<const uint8_t*>(request->thrift_struct().data()), &len, true,
      &thrift_request);

  TCancelPlanFragmentResult return_val;
  QueryState::ScopedRef qs(GetQueryId(thrift_request.fragment_instance_id));
  if (qs.get() == nullptr) {
    SetUnknownIdError("query", GetQueryId(thrift_request.fragment_instance_id), &return_val);
    return;
  }
  FragmentInstanceState* fis = qs->GetFInstanceState(thrift_request.fragment_instance_id);
  if (fis == nullptr) {
    SetUnknownIdError("instance", thrift_request.fragment_instance_id, &return_val);
    return;
  }
  fis->Cancel().SetTStatus(&return_val);

  SerializeThriftToProtoWrapper(&return_val, true, response);
  context->RespondSuccess();

}

}
