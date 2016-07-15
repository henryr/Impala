#include "service/impala-server-rpc.h"

#include "service/impala-server.h"
#include "common/names.h"
#include "service/prototest.service.h"

using namespace impala;

using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::AcceptorPool;
using kudu::rpc::ServicePool;
using kudu::rpc::ServiceIf;
using kudu::rpc::RpcContext;
using kudu::Sockaddr;
using kudu::MetricEntity;

using kudu::rpc_test::ImpalaKRPCServiceIf;
using kudu::rpc_test::TransmitDataRequestPB;
using kudu::rpc_test::TransmitDataResponsePB;

DECLARE_int32(be_port);

namespace {

impala::TUniqueId ToThriftID(const kudu::rpc_test::UniqueIdPB& pb) {
  impala::TUniqueId ret;
  ret.__set_lo(pb.lo());
  ret.__set_hi(pb.hi());
  return ret;
}

}

template <typename In, typename Out>
void ToTList(const google::protobuf::RepeatedField<In>& in, vector<Out>* out) {
  out->resize(in.size());
  for (int i = 0; i < in.size(); ++i) {
    (*out)[i] = in.Get(i);
  }
}

TRowBatch ToTRowBatch(const kudu::rpc_test::RowBatchPB& pb) {
  TRowBatch ret;
  ret.__set_num_rows(pb.num_rows());
  ToTList(pb.row_tuples(), &ret.row_tuples);
  ToTList(pb.tuple_offsets(), &ret.tuple_offsets);
  ret.__set_tuple_data(pb.tuple_data());
  ret.__set_uncompressed_size(pb.uncompressed_size());
  ret.__set_compression_type(static_cast<THdfsCompression::type>(pb.compression_type()));
  return ret;
}

class ImpalaKRPCServiceImpl : public ImpalaKRPCServiceIf {
 public:
  ImpalaKRPCServiceImpl(const scoped_refptr<MetricEntity>& entity,
      ImpalaServer* server) : ImpalaKRPCServiceIf(entity), server_(server) { }

  virtual void TransmitData(const TransmitDataRequestPB* request,
      TransmitDataResponsePB* response, RpcContext* context) {
    TTransmitDataParams tparams;
    tparams.__set_dest_fragment_instance_id(
        ToThriftID(request->dest_fragment_instance_id()));
    tparams.__set_sender_id(request->sender_id());
    tparams.__set_dest_node_id(request->dest_node_id());
    tparams.__set_row_batch(ToTRowBatch(request->row_batch()));
    tparams.__set_eos(request->eos());
    // LOG(INFO) << "HNR: PROTOBUF -> " << PrintThrift(tparams);
    TTransmitDataResult result;
    server_->TransmitData(result, tparams);

    context->RespondSuccess();
  }

 private:
  ImpalaServer* server_;
};

ImpalaServerRPC::ImpalaServerRPC(ImpalaServer* server) : server_(server) {

}

Status ImpalaServerRPC::Start() {
  scoped_refptr<MetricEntity> entity =
      METRIC_ENTITY_server.Instantiate(&registry_, "impala-server");
  MessengerBuilder bld("impala-server");
  bld.set_num_reactors(4)
     .set_metric_entity(entity);
  bld.Build(&messenger_);

  shared_ptr<AcceptorPool> acceptor_pool;
  Sockaddr address;
  address.ParseString("127.0.0.1", FLAGS_be_port + 100);
  messenger_->AddAcceptorPool(address, &acceptor_pool);
  acceptor_pool->Start(2);

  service_ = new ImpalaKRPCServiceImpl(messenger_->metric_entity(), server_);
  gscoped_ptr<ServiceIf> service(service_);
  scoped_refptr<ServicePool> service_pool =
      new ServicePool(std::move(service), messenger_->metric_entity(), 50);
  messenger_->RegisterService(ImpalaKRPCServiceIf::static_service_name(), service_pool);
  service_pool->Init(4);

  return Status::OK();
}
