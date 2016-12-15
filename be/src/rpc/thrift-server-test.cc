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

#include <string>

#include "testutil/gtest-util.h"
#include "rpc/thrift-client.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

using namespace impala;
using namespace beeswax;
using namespace strings;
using namespace apache::thrift;

DECLARE_string(ssl_client_ca_certificate);

DECLARE_int32(be_port);
DECLARE_int32(beeswax_port);

string IMPALA_HOME(getenv("IMPALA_HOME"));
const string& SERVER_CERT =
    Substitute("$0/be/src/testutil/server-cert.pem", IMPALA_HOME);
const string& PRIVATE_KEY =
    Substitute("$0/be/src/testutil/server-key.pem", IMPALA_HOME);
const string& BAD_SERVER_CERT =
    Substitute("$0/be/src/testutil/bad-cert.pem", IMPALA_HOME);
const string& BAD_PRIVATE_KEY =
    Substitute("$0/be/src/testutil/bad-key.pem", IMPALA_HOME);
const string& PASSWORD_PROTECTED_PRIVATE_KEY =
    Substitute("$0/be/src/testutil/server-key-password.pem", IMPALA_HOME);

/// Dummy server class (chosen because it has the smallest interface to implement) that
/// tests can use to start Thrift servers.
class DummyBeeswaxIf : public BeeswaxServiceIf {
 public:
  virtual void query(QueryHandle& _return, const Query& query){};
  virtual void executeAndWait(
      QueryHandle& _return, const Query& query, const LogContextId& clientCtx){};
  virtual void explain(QueryExplanation& _return, const Query& query){};
  virtual void fetch(Results& _return, const QueryHandle& query_id, const bool start_over,
      const int32_t fetch_size){};
  virtual beeswax::QueryState::type get_state(const QueryHandle& handle) {
    return beeswax::QueryState::CREATED;
  };
  virtual void get_results_metadata(
      ResultsMetadata& _return, const QueryHandle& handle){};
  virtual void echo(std::string& _return, const std::string& s){};
  virtual void dump_config(std::string& _return){};
  virtual void get_log(std::string& _return, const LogContextId& context){};
  virtual void get_default_configuration(
      std::vector<ConfigVariable>& _return, const bool include_hadoop){};
  virtual void close(const QueryHandle& handle){};
  virtual void clean(const LogContextId& log_context){};
};

boost::shared_ptr<TProcessor> MakeProcessor() {
  boost::shared_ptr<DummyBeeswaxIf> service(new DummyBeeswaxIf());
  return boost::shared_ptr<TProcessor>(new BeeswaxServiceProcessor(service));
}

int GetServerPort() {
  int port = FindUnusedEphemeralPort();
  EXPECT_FALSE(port == -1);
  return port;
}

TEST(ThriftServer, Connectivity) {
  int port = GetServerPort();
  ThriftClient<BeeswaxServiceClient> wrong_port_client(
      "localhost", port, "", NULL, false);
  ASSERT_FALSE(wrong_port_client.Open().ok());

  ThriftServer* server =
      new ThriftServer("DummyBeeswax", MakeProcessor(), port, NULL, NULL, 5);
  ASSERT_OK(server->Start());

  // Test that client recovers from failure to connect.
  ASSERT_OK(wrong_port_client.Open());
}

TEST(SslTest, Connectivity) {
  int port = GetServerPort();
  // Start a server using SSL and confirm that an SSL client can connect, while a non-SSL
  // client cannot.
  // Here and elsewhere - allocate ThriftServers on the heap to avoid race during
  // destruction. See IMPALA-2283.
  ThriftServer* server =
      new ThriftServer("DummyBeeswax", MakeProcessor(), port, NULL, NULL, 5);
  ASSERT_OK(server->EnableSsl(SERVER_CERT, PRIVATE_KEY, "echo password"));
  ASSERT_OK(server->Start());

  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  ThriftClient<BeeswaxServiceClient> ssl_client("localhost", port, "", NULL, true);
  ASSERT_OK(ssl_client.Open());
  QueryHandle resp;
  EXPECT_NO_THROW({ ssl_client.iface()->query(resp, Query()); });

  // Disable SSL for this client.
  ThriftClient<BeeswaxServiceClient> non_ssl_client("localhost", port, "", NULL, false);
  ASSERT_OK(non_ssl_client.Open());
  EXPECT_THROW(non_ssl_client.iface()->query(resp, Query()), TTransportException);
}

TEST(SslTest, BadCertificate) {
  FLAGS_ssl_client_ca_certificate = "unknown";
  int port = GetServerPort();
  ThriftClient<BeeswaxServiceClient> ssl_client("localhost", port, "", NULL, true);
  ASSERT_FALSE(ssl_client.Open().ok());

  ThriftServer* server =
      new ThriftServer("DummyBeeswaxg", MakeProcessor(), port, NULL, NULL, 5);
  ASSERT_OK(server->EnableSsl(SERVER_CERT, PRIVATE_KEY, "echo password"));
  ASSERT_OK(server->Start());

  // Check that client does not recover from failure to create socket.
  ASSERT_FALSE(ssl_client.Open().ok());
}

TEST(PasswordProtectedPemFile, CorrectOperation) {
  // Require the server to execute a shell command to read the password to the private key
  // file.
  int port = GetServerPort();
  ThriftServer* server =
      new ThriftServer("DummyBeeswax", MakeProcessor(), port, NULL, NULL, 5);
  ASSERT_OK(server->EnableSsl(
      SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY, "echo password"));
  ASSERT_OK(server->Start());
  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  ThriftClient<BeeswaxServiceClient> ssl_client("localhost", port, "", NULL, true);
  ASSERT_OK(ssl_client.Open());
  QueryHandle resp;
  EXPECT_NO_THROW({ ssl_client.iface()->query(resp, Query()); });
}

TEST(PasswordProtectedPemFile, BadPassword) {
  // Test failure when password to private key is wrong.
  ThriftServer server("DummyBeeswax", MakeProcessor(), GetServerPort(), NULL, NULL, 5);
  ASSERT_OK(server.EnableSsl(
      SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY, "echo wrongpassword"));
  EXPECT_FALSE(server.Start().ok());
}

TEST(PasswordProtectedPemFile, BadCommand) {
  // Test failure when password command is badly formed.
  ThriftServer server("DummyBeeswax", MakeProcessor(), GetServerPort(), NULL, NULL, 5);
  EXPECT_FALSE(server.EnableSsl(
      SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY, "cmd-no-exist").ok());
}

TEST(SslTest, ClientBeforeServer) {
  // Instantiate a thrift client before a thrift server and test if it works (IMPALA-2747)
  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  int port = GetServerPort();
  ThriftClient<BeeswaxServiceClient> ssl_client("localhost", port, "", NULL, true);
  ThriftServer* server =
      new ThriftServer("DummyBeeswax", MakeProcessor(), port, NULL, NULL, 5);
  ASSERT_OK(server->EnableSsl(SERVER_CERT, PRIVATE_KEY));
  ASSERT_OK(server->Start());

  ASSERT_OK(ssl_client.Open());
  QueryHandle resp;
  ssl_client.iface()->query(resp, Query());
}

/// Test disabled because requires a high ulimit -n on build machines. Since the test does
/// not always fail, we don't lose much coverage by disabling it until we fix the build
/// infra issue.
TEST(ConcurrencyTest, DISABLED_ManyConcurrentConnections) {
  // Test that a large number of concurrent connections will all succeed and not time out
  // waiting to be accepted. (IMPALA-4135)
  // Note that without the fix for IMPALA-4135, this test won't always fail, depending on
  // the hardware that it is run on.
  int port = GetServerPort();
  ThriftServer* server = new ThriftServer("DummyServer", MakeProcessor(), port);
  ASSERT_OK(server->Start());

  ThreadPool<int64_t> pool(
      "group", "test", 256, 10000, [port](int tid, const int64_t& item) {
        using Client = ThriftClient<BeeswaxServiceClient>;
        Client* client = new Client("127.0.0.1", port, "", NULL, false);
        Status status = client->Open();
        ASSERT_OK(status);
      });
  for (int i = 0; i < 1024 * 16; ++i) pool.Offer(i);
  pool.DrainAndShutdown();
}

TEST(NoPasswordPemFile, BadServerCertificate) {
  ThriftServer* server =
      new ThriftServer("DummyBeeswax", MakeProcessor(), FLAGS_be_port + 5, NULL, NULL, 5);
  EXPECT_OK(server->EnableSsl(BAD_SERVER_CERT, BAD_PRIVATE_KEY));
  EXPECT_OK(server->Start());
  FLAGS_ssl_client_ca_certificate = SERVER_CERT;
  ThriftClient<BeeswaxServiceClient> ssl_client(
      "localhost", FLAGS_be_port + 5, "", NULL, true);
  EXPECT_OK(ssl_client.Open());
  QueryHandle resp;
  EXPECT_THROW({ ssl_client.iface()->query(resp, Query()); }, TSSLException);
  // Close and reopen the socket
  ssl_client.Close();
  EXPECT_OK(ssl_client.Open());
  EXPECT_THROW({ ssl_client.iface()->query(resp, Query()); }, TSSLException);
}

IMPALA_TEST_MAIN();
