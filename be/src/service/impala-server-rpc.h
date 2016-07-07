#ifndef IMPALA_SERVICE_IMPALA_SERVER_RPC_H
#define IMPALA_SERVICE_IMPALA_SERVER_RPC_H

#include "kudu/util/net/sockaddr.h"
#include "kudu/kudu_rpc/messenger.h"
#include "kudu/kudu_rpc/acceptor_pool.h"
#include "kudu/kudu_rpc/service_if.h"
#include "kudu/kudu_rpc/rpc_controller.h"
#include "kudu/kudu_rpc/service_pool.h"
#include "kudu/kudu_rpc/rpc_context.h"

#include "common/status.h"

class ImpalaKRPCServiceImpl;

namespace impala {

class ImpalaServer;

class ImpalaServerRPC {
 public:
  ImpalaServerRPC(ImpalaServer* server);
  Status Start();

  std::shared_ptr<kudu::rpc::Messenger> messenger() { return messenger_; }

 private:
  ImpalaServer* server_;

  ImpalaKRPCServiceImpl* service_;

  kudu::MetricRegistry registry_;

  std::shared_ptr<kudu::rpc::Messenger> messenger_;

};

}

#endif
