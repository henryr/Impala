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

#include "common/status.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/trace.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/rpc.h"
#include "rpc/thrift-util.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "service/impala-server.h"
#include "service/data_stream_service.pb.h"
#include "util/bloom-filter.h"
#include "testutil/fault-injection-util.h"

#include "common/names.h"

using namespace impala;
using kudu::rpc::RpcContext;
using kudu::Trace;

namespace impala {

DataStreamService::DataStreamService(RpcMgr* mgr)
  : DataStreamServiceIf(mgr->metric_entity(), mgr->result_tracker()) {}

void DataStreamService::EndDataStream(const EndDataStreamRequestPb* request,
    EndDataStreamResponsePb* response, RpcContext* context) {
  FAULT_INJECTION_RPC_DELAY(RPC_ENDDATASTREAM);
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
  FAULT_INJECTION_RPC_DELAY(RPC_TRANSMITDATA);
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());

  VLOG_ROW << "TransmitData(): instance_id=" << finst_id
           << " node_id=" << request->dest_node_id()
           << " #rows=" << request->row_batch_header().num_rows()
           << " sender_id=" << request->sender_id();
  ProtoRowBatch batch;
  Status status = FromKuduStatus(context->GetInboundSidecar(
      request->row_batch_header().tuple_data_sidecar_idx(), &batch.tuple_data));
  if (status.ok()) {
    status = FromKuduStatus(context->GetInboundSidecar(
        request->row_batch_header().tuple_offsets_sidecar_idx(), &batch.incoming_tuple_offsets));
  }
  if (status.ok()) {
    batch.header.Swap((const_cast<TransmitDataRequestPb*>(request))->mutable_row_batch_header());
    TransmitDataCtx payload(batch, context, request, response);
    // AddData() is guaranteed to eventually respond to this RPC so we don't do it here.
    ExecEnv::GetInstance()->stream_mgr()->AddData(finst_id, move(payload));
  } else {
    // An application-level error occurred, so return 'success', but set the error status.
    status.ToProto(response->mutable_status());
    context->RespondSuccess();
  }
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
      ProtoBloomFilter proto_filter;
      proto_filter.header = request->bloom_filter();
      Status status = FromKuduStatus(context->GetInboundSidecar(
          proto_filter.header.directory_sidecar_idx(),
          &proto_filter.directory));
      if (status.ok()) fis->PublishFilter(request->filter_id(), proto_filter);
    }
  }

  context->RespondSuccess();
}

void DataStreamService::UpdateFilter(const UpdateFilterRequestPb* request,
    UpdateFilterResponsePb* response, RpcContext* context) {
  TUniqueId query_id;
  query_id.lo = request->query_id().lo();
  query_id.hi = request->query_id().hi();

  ProtoBloomFilter filter;
  filter.header = request->bloom_filter();
  Status status = Status::OK();
  if (!filter.header.always_true()) {
    status = FromKuduStatus(context->GetInboundSidecar(
        filter.header.directory_sidecar_idx(), &filter.directory));
  }
  if (status.ok()) {
    ExecEnv::GetInstance()->impala_server()->UpdateFilter(
        request->filter_id(), query_id, filter);
  }
  context->RespondSuccess();
}

ExecControlService::ExecControlService(RpcMgr* mgr)
  : ExecControlServiceIf(mgr->metric_entity(), mgr->result_tracker()) {}

void ExecControlService::ExecPlanFragment(
    const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
  FAULT_INJECTION_RPC_DELAY(RPC_EXECPLANFRAGMENT);
  TExecPlanFragmentParams thrift_request;
  Status status =
      DeserializeFromSidecar(context, request->sidecar_idx(), &thrift_request);
  TExecPlanFragmentResult return_val;
  if (status.ok()) {
    status = ExecEnv::GetInstance()->query_exec_mgr()->StartFInstance(thrift_request);
  }
  status.SetTStatus(&return_val);
  SerializeToSidecar(context, &return_val, response);
  context->RespondSuccess();
}

void ExecControlService::ReportExecStatus(
    const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
  FAULT_INJECTION_RPC_DELAY(RPC_REPORTEXECSTATUS);
  TReportExecStatusParams thrift_request;
  Status status =
      DeserializeFromSidecar(context, request->sidecar_idx(), &thrift_request);
  TRACE_TO(Trace::CurrentTrace(), "Deserialization complete");

  TReportExecStatusResult return_val;
  if (status.ok()) {
    ExecEnv::GetInstance()->impala_server()->ReportExecStatus(
        return_val, thrift_request);
  }

  TRACE_TO(Trace::CurrentTrace(), "Method complete");
  status.SetTStatus(&return_val);
  SerializeToSidecar(context, &return_val, response);
  TRACE_TO(Trace::CurrentTrace(), "Response serialization complete");
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
  FAULT_INJECTION_RPC_DELAY(RPC_CANCELPLANFRAGMENT);
  TCancelPlanFragmentParams thrift_request;
  Status status =
      DeserializeFromSidecar(context, request->sidecar_idx(), &thrift_request);

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
  SerializeToSidecar(context, &return_val, response);
  context->RespondSuccess();
}
}
