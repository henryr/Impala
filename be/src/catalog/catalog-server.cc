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

#include "catalog/catalog-server.h"

#include <gutil/strings/substitute.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "catalog/catalog-util.h"
#include "rpc/rpc-mgr.inline.h"
#include "kudu/rpc/rpc_context.h"
#include "gen-cpp/CatalogInternalService_types.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/CatalogService_types.h"
#include "rpc/rpc-mgr.inline.h"
#include "statestore/statestore-subscriber.h"
#include "util/debug-util.h"

#include "catalog/catalog_service.pb.h"
#include "catalog/catalog_service.service.h"

#include "common/names.h"

using boost::bind;
using boost::mem_fn;
using namespace apache::thrift;
using namespace impala;
using namespace rapidjson;
using namespace strings;
using kudu::rpc::RpcContext;
using kudu::rpc::ServiceIf;

DEFINE_int32(catalog_service_port, 26000, "port where the CatalogService is running");
DECLARE_string(state_store_host);
DECLARE_int32(state_store_subscriber_port);
DECLARE_int32(state_store_port);
DECLARE_string(hostname);
DECLARE_bool(compact_catalog_topic);
DECLARE_int32(num_acceptor_threads);
DECLARE_int32(num_reactor_threads);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);

string CatalogServer::IMPALA_CATALOG_TOPIC = "catalog-update";

const string CATALOG_SERVER_TOPIC_PROCESSING_TIMES =
    "catalog-server.topic-processing-time-s";

const string CATALOG_WEB_PAGE = "/catalog";
const string CATALOG_TEMPLATE = "catalog.tmpl";
const string CATALOG_OBJECT_WEB_PAGE = "/catalog_object";
const string CATALOG_OBJECT_TEMPLATE = "catalog_object.tmpl";

class CatalogServiceImpl : public CatalogServiceIf {
 public:
  CatalogServiceImpl(RpcMgr* rpc_mgr, CatalogServer* catalog_server)
      : CatalogServiceIf(rpc_mgr->metric_entity(), rpc_mgr->result_tracker()),
        catalog_server_(catalog_server) {
  }

  template <typename THRIFTREQ, typename THRIFTRESP, typename RPC>
  void ExecRpc(
      const RPC& rpc, const ThriftWrapperPb* request, ThriftWrapperPb* response) {
    THRIFTREQ thrift_request;
    Status status = DeserializeThriftFromProtoWrapper(*request, &thrift_request);
    THRIFTRESP thrift_response;
    if (status.ok()) {
      status = rpc(thrift_request, &thrift_response);
      if (!status.ok()) LOG(ERROR) << status.GetDetail();
    }
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    thrift_response.__set_status(thrift_status);
    SerializeThriftToProtoWrapper(&thrift_response, response);
  }

  template <typename THRIFTREQ, typename THRIFTRESP, typename RPC>
  void ExecRpcNoStatus(
      const RPC& rpc, const ThriftWrapperPb* request, ThriftWrapperPb* response) {
    THRIFTREQ thrift_request;
    Status status = DeserializeThriftFromProtoWrapper(*request, &thrift_request);
    THRIFTRESP thrift_response;
    if (status.ok()) {
      status = rpc(thrift_request, &thrift_response);
      if (!status.ok()) LOG(ERROR) << status.GetDetail();
    }
    SerializeThriftToProtoWrapper(&thrift_response, response);
  }

  virtual void ExecDdl(
      const ThriftWrapperPb* request, ThriftWrapperPb* response, RpcContext* context) {
    TDdlExecRequest thrift_request;
    Status status = DeserializeThriftFromProtoWrapper(*request, &thrift_request);
    TDdlExecResponse thrift_response;
    if (status.ok()) {
      status = catalog_server_->catalog()->ExecDdl(thrift_request, &thrift_response);
      if (!status.ok()) LOG(ERROR) << status.GetDetail();
    }
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    thrift_response.result.__set_status(thrift_status);
    SerializeThriftToProtoWrapper(&thrift_response, response);
    context->RespondSuccess();
  }

  virtual void GetCatalogObject(const ThriftWrapperPb* request,
      ThriftWrapperPb* response, RpcContext* context) {
    auto rpc = [svr = catalog_server_](const TGetCatalogObjectRequest& request,
                   TGetCatalogObjectResponse* response)
                   ->Status {
      return svr->catalog()->GetCatalogObject(
          request.object_desc, &response->catalog_object);
    };

    ExecRpcNoStatus<
        TGetCatalogObjectRequest, TGetCatalogObjectResponse, decltype(rpc)>(
        rpc, request, response);
    context->RespondSuccess();
  }

  virtual void ResetMetadata(const ThriftWrapperPb* request,
      ThriftWrapperPb* response, RpcContext* context) {
    auto rpc = [svr = catalog_server_](
                   const TResetMetadataRequest& request, TResetMetadataResponse* response)
                   ->Status {
      return svr->catalog()->ResetMetadata(request, response);
    };

    ExecRpcNoStatus<
        TResetMetadataRequest, TResetMetadataResponse, decltype(rpc)>(
        rpc, request, response);
    context->RespondSuccess();
  }

  virtual void UpdateCatalog(const ThriftWrapperPb* request,
      ThriftWrapperPb* response, RpcContext* context) {
    auto rpc = [svr = catalog_server_](
                   const TUpdateCatalogRequest& request, TUpdateCatalogResponse* response)
                   ->Status {
      return svr->catalog()->UpdateCatalog(request, response);
    };

    ExecRpcNoStatus<
        TUpdateCatalogRequest, TUpdateCatalogResponse, decltype(rpc)>(
        rpc, request, response);
    context->RespondSuccess();
  }

  virtual void GetFunctions(const ThriftWrapperPb* request,
      ThriftWrapperPb* response, RpcContext* context) {
    auto rpc = [svr = catalog_server_](
                   const TGetFunctionsRequest& request, TGetFunctionsResponse* response)
                   ->Status {
      return svr->catalog()->GetFunctions(request, response);
    };

    ExecRpc< TGetFunctionsRequest,
        TGetFunctionsResponse, decltype(rpc)>(rpc, request, response);
    context->RespondSuccess();
  }

  virtual void PrioritizeLoad(const ThriftWrapperPb* request,
      ThriftWrapperPb* response, RpcContext* context) {
    auto rpc = [svr = catalog_server_](const TPrioritizeLoadRequest& request,
                   TPrioritizeLoadResponse* response)
                   ->Status {
      return svr->catalog()->PrioritizeLoad(request);
    };

    ExecRpc< TPrioritizeLoadRequest,
        TPrioritizeLoadResponse, decltype(rpc)>(rpc, request, response);
    context->RespondSuccess();
  }

  virtual void SentryAdminCheck(const ThriftWrapperPb* request,
      ThriftWrapperPb* response, RpcContext* context) {
    auto rpc = [svr = catalog_server_](const TSentryAdminCheckRequest& request,
                   TSentryAdminCheckResponse* response)
                   ->Status {
      return svr->catalog()->SentryAdminCheck(request);
    };

    ExecRpc<TSentryAdminCheckRequest, TSentryAdminCheckResponse, decltype(rpc)>(
        rpc, request, response);
    context->RespondSuccess();
  }

 private:
  CatalogServer* catalog_server_;
};

CatalogServer::CatalogServer(MetricGroup* metrics)
  : thrift_serializer_(FLAGS_compact_catalog_topic),
    metrics_(metrics),
    topic_updates_ready_(false),
    last_sent_catalog_version_(0L),
    catalog_objects_min_version_(0L),
    catalog_objects_max_version_(0L) {
  topic_processing_time_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      CATALOG_SERVER_TOPIC_PROCESSING_TIMES);
}

Status CatalogServer::Start() {
  TNetworkAddress subscriber_address =
      MakeNetworkAddress(FLAGS_hostname, FLAGS_catalog_service_port);
  TNetworkAddress statestore_address =
      MakeNetworkAddress(FLAGS_state_store_host, FLAGS_state_store_port);
  TNetworkAddress server_address = MakeNetworkAddress(FLAGS_hostname,
      FLAGS_catalog_service_port);

  if (FLAGS_ssl_server_certificate.empty()) {
    RETURN_IF_ERROR(rpc_mgr_.Init(FLAGS_num_reactor_threads));
  } else {
    RETURN_IF_ERROR(rpc_mgr_.InitWithSsl(FLAGS_num_acceptor_threads,
        FLAGS_ssl_server_certificate, FLAGS_ssl_private_key,
        FLAGS_ssl_client_ca_certificate));
  }

  // This will trigger a full Catalog metadata load.
  catalog_.reset(new Catalog());
  catalog_update_gathering_thread_.reset(new Thread("catalog-server",
      "catalog-update-gathering-thread",
      &CatalogServer::GatherCatalogUpdatesThread, this));

  statestore_subscriber_.reset(new StatestoreSubscriber(
      Substitute("catalog-server@$0", TNetworkAddressToString(server_address)),
      subscriber_address, statestore_address, &rpc_mgr_, metrics_));

  StatestoreSubscriber::UpdateCallback cb =
      bind<void>(mem_fn(&CatalogServer::UpdateCatalogTopicCallback), this, _1, _2);
  Status status = statestore_subscriber_->AddTopic(IMPALA_CATALOG_TOPIC, false, cb);
  if (!status.ok()) {
    status.AddDetail("CatalogService failed to start");
    return status;
  }
  RETURN_IF_ERROR(statestore_subscriber_->Start());
  unique_ptr<ServiceIf> impl(new CatalogServiceImpl(&rpc_mgr_, this));
  RETURN_IF_ERROR(rpc_mgr_.RegisterService(32, 1024, move(impl)));

  RETURN_IF_ERROR(rpc_mgr_.StartServices(
      FLAGS_catalog_service_port, FLAGS_num_acceptor_threads));

  // Notify the thread to start for the first time.
  {
    lock_guard<mutex> l(catalog_lock_);
    catalog_update_cv_.notify_one();
  }
  return Status::OK();
}

void CatalogServer::RegisterWebpages(Webserver* webserver) {
  Webserver::UrlCallback catalog_callback =
      bind<void>(mem_fn(&CatalogServer::CatalogUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback(CATALOG_WEB_PAGE, CATALOG_TEMPLATE,
      catalog_callback);

  Webserver::UrlCallback catalog_objects_callback =
      bind<void>(mem_fn(&CatalogServer::CatalogObjectsUrlCallback), this, _1, _2);
  webserver->RegisterUrlCallback(CATALOG_OBJECT_WEB_PAGE, CATALOG_OBJECT_TEMPLATE,
      catalog_objects_callback, false);
}

void CatalogServer::UpdateCatalogTopicCallback(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(CatalogServer::IMPALA_CATALOG_TOPIC);
  if (topic == incoming_topic_deltas.end()) return;

  try_mutex::scoped_try_lock l(catalog_lock_);
  // Return if unable to acquire the catalog_lock_ or if the topic update data is
  // not yet ready for processing. This indicates the catalog_update_gathering_thread_
  // is still building a topic update.
  if (!l || !topic_updates_ready_) return;

  const TTopicDelta& delta = topic->second;

  // If this is not a delta update, clear all catalog objects and request an update
  // from version 0 from the local catalog. There is an optimization that checks if
  // pending_topic_updates_ was just reloaded from version 0, if they have then skip this
  // step and use that data.
  if (delta.from_version == 0 && delta.to_version == 0 &&
      catalog_objects_min_version_ != 0) {
    catalog_topic_entry_keys_.clear();
    last_sent_catalog_version_ = 0L;
  } else {
    // Process the pending topic update.
    LOG_EVERY_N(INFO, 300) << "Catalog Version: " << catalog_objects_max_version_
                           << " Last Catalog Version: " << last_sent_catalog_version_;

    for (const TTopicItem& catalog_object: pending_topic_updates_) {
      if (subscriber_topic_updates->size() == 0) {
        subscriber_topic_updates->push_back(TTopicDelta());
        subscriber_topic_updates->back().topic_name = IMPALA_CATALOG_TOPIC;
      }
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_entries.push_back(catalog_object);
    }

    // Update the new catalog version and the set of known catalog objects.
    last_sent_catalog_version_ = catalog_objects_max_version_;
  }

  // Signal the catalog update gathering thread to start.
  topic_updates_ready_ = false;
  catalog_update_cv_.notify_one();
}

[[noreturn]] void CatalogServer::GatherCatalogUpdatesThread() {
  while (1) {
    unique_lock<mutex> unique_lock(catalog_lock_);
    // Protect against spurious wakups by checking the value of topic_updates_ready_.
    // It is only safe to continue on and update the shared pending_topic_updates_
    // when topic_updates_ready_ is false, otherwise we may be in the middle of
    // processing a heartbeat.
    while (topic_updates_ready_) {
      catalog_update_cv_.wait(unique_lock);
    }

    MonotonicStopWatch sw;
    sw.Start();

    // Clear any pending topic updates. They will have been processed by the heartbeat
    // thread by the time we make it here.
    pending_topic_updates_.clear();

    long current_catalog_version;
    Status status = catalog_->GetCatalogVersion(&current_catalog_version);
    if (!status.ok()) {
      LOG(ERROR) << status.GetDetail();
    } else if (current_catalog_version != last_sent_catalog_version_) {
      // If there has been a change since the last time the catalog was queried,
      // call into the Catalog to find out what has changed.
      TGetAllCatalogObjectsResponse catalog_objects;
      status = catalog_->GetAllCatalogObjects(last_sent_catalog_version_,
          &catalog_objects);
      if (!status.ok()) {
        LOG(ERROR) << status.GetDetail();
      } else {
        // Use the catalog objects to build a topic update list.
        BuildTopicUpdates(catalog_objects.objects);
        catalog_objects_min_version_ = last_sent_catalog_version_;
        catalog_objects_max_version_ = catalog_objects.max_catalog_version;
      }
    }

    topic_processing_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    topic_updates_ready_ = true;
  }
}

void CatalogServer::BuildTopicUpdates(const vector<TCatalogObject>& catalog_objects) {
  unordered_set<string> current_entry_keys;

  // Add any new/updated catalog objects to the topic.
  for (const TCatalogObject& catalog_object: catalog_objects) {
    const string& entry_key = TCatalogObjectToEntryKey(catalog_object);
    if (entry_key.empty()) {
      LOG_EVERY_N(WARNING, 60) << "Unable to build topic entry key for TCatalogObject: "
                               << ThriftDebugString(catalog_object);
    }

    current_entry_keys.insert(entry_key);
    // Remove this entry from catalog_topic_entry_keys_. At the end of this loop, we will
    // be left with the set of keys that were in the last update, but not in this
    // update, indicating which objects have been removed/dropped.
    catalog_topic_entry_keys_.erase(entry_key);

    // This isn't a new or an updated item, skip it.
    if (catalog_object.catalog_version <= last_sent_catalog_version_) continue;

    VLOG(1) << "Publishing update: " << entry_key << "@"
            << catalog_object.catalog_version;

    pending_topic_updates_.push_back(TTopicItem());
    TTopicItem& item = pending_topic_updates_.back();
    item.key = entry_key;
    Status status = thrift_serializer_.Serialize(&catalog_object, &item.value);
    if (!status.ok()) {
      LOG(ERROR) << "Error serializing topic value: " << status.GetDetail();
      pending_topic_updates_.pop_back();
    }
  }

  // Any remaining items in catalog_topic_entry_keys_ indicate the object was removed
  // since the last update.
  for (const string& key: catalog_topic_entry_keys_) {
    pending_topic_updates_.push_back(TTopicItem());
    TTopicItem& item = pending_topic_updates_.back();
    item.key = key;
    VLOG(1) << "Publishing deletion: " << key;
    // Don't set a value to mark this item as deleted.
  }
  catalog_topic_entry_keys_.swap(current_entry_keys);
}

void CatalogServer::CatalogUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  TGetDbsResult get_dbs_result;
  Status status = catalog_->GetDbs(NULL, &get_dbs_result);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
    return;
  }
  Value databases(kArrayType);
  for (const TDatabase& db: get_dbs_result.dbs) {
    Value database(kObjectType);
    Value str(db.db_name.c_str(), document->GetAllocator());
    database.AddMember("name", str, document->GetAllocator());

    TGetTablesResult get_table_results;
    Status status = catalog_->GetTableNames(db.db_name, NULL, &get_table_results);
    if (!status.ok()) {
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      database.AddMember("error", error, document->GetAllocator());
      continue;
    }

    Value table_array(kArrayType);
    for (const string& table: get_table_results.tables) {
      Value table_obj(kObjectType);
      Value fq_name(Substitute("$0.$1", db.db_name, table).c_str(),
          document->GetAllocator());
      table_obj.AddMember("fqtn", fq_name, document->GetAllocator());
      Value table_name(table.c_str(), document->GetAllocator());
      table_obj.AddMember("name", table_name, document->GetAllocator());
      table_array.PushBack(table_obj, document->GetAllocator());
    }
    database.AddMember("num_tables", table_array.Size(), document->GetAllocator());
    database.AddMember("tables", table_array, document->GetAllocator());
    databases.PushBack(database, document->GetAllocator());
  }
  document->AddMember("databases", databases, document->GetAllocator());
}


void CatalogServer::CatalogObjectsUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  Webserver::ArgumentMap::const_iterator object_type_arg = args.find("object_type");
  Webserver::ArgumentMap::const_iterator object_name_arg = args.find("object_name");
  if (object_type_arg != args.end() && object_name_arg != args.end()) {
    TCatalogObjectType::type object_type =
        TCatalogObjectTypeFromName(object_type_arg->second);

    // Get the object type and name from the topic entry key
    TCatalogObject request;
    TCatalogObjectFromObjectName(object_type, object_name_arg->second, &request);

    // Get the object and dump its contents.
    TCatalogObject result;
    Status status = catalog_->GetCatalogObject(request, &result);
    if (status.ok()) {
      Value debug_string(ThriftDebugString(result).c_str(), document->GetAllocator());
      document->AddMember("thrift_string", debug_string, document->GetAllocator());
    } else {
      Value error(status.GetDetail().c_str(), document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
    }
  } else {
    Value error("Please specify values for the object_type and object_name parameters.",
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

void CatalogServer::Join() {
  catalog_update_gathering_thread_->Join();
}
