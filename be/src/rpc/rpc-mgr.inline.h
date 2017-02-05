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

#ifndef IMPALA_RPC_RPC_MGR_INLINE_H
#define IMPALA_RPC_RPC_MGR_INLINE_H

#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"

#include "exec/kudu-util.h"

#include "rpc/rpc-mgr.h"
#include "util/time.h"

#include <boost/thread/lock_guard.hpp>

namespace impala {

template <typename P>
Status RpcMgr::GetProxy(const TNetworkAddress& address, std::unique_ptr<P>* proxy) {
  DCHECK(proxy != nullptr);
  DCHECK(is_inited()) << "Must call Init() before GetProxy()";

  const int64_t TTL = 300;

  int64_t now = MonotonicSeconds();
  bool needs_lookup = false;
  kudu::Sockaddr kudu_addr;
  {
    boost::lock_guard<SpinLock> l(cached_addresses_lock_);
    auto cached = cached_addresses_.find(address.hostname);
    if (cached == cached_addresses_.end() || (now - cached->second.first > TTL)) {
      needs_lookup = true;
    } else {
      kudu_addr = cached->second.second;
    }
  }

  if (needs_lookup) {
    std::vector<kudu::Sockaddr> addresses;
    KUDU_RETURN_IF_ERROR(
        kudu::HostPort(address.hostname, address.port).ResolveAddresses(&addresses),
        "Couldn't resolve addresses");
    DCHECK_GT(addresses.size(), 0);
    kudu_addr = addresses[0];
    {
      boost::lock_guard<SpinLock> l(cached_addresses_lock_);
      cached_addresses_.insert(std::make_pair(address.hostname, std::make_pair(now, kudu_addr)));
    }
  }
  proxy->reset(new P(messenger_, kudu_addr));
  return Status::OK();
}
}

#endif
