# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
import json
from impala._thrift_gen.beeswax.BeeswaxService import QueryState
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfBuildType
from tests.verifiers.metric_verifier import MetricVerifier
from tests.util.debug_rules import *

@SkipIfBuildType.not_dev_build
class Test1599(ImpalaTestSuite):
  """Tests for every Impala RPC timeout handling, query should not hang and
     resource should be all released."""
  TEST_QUERY = "select count(c2.string_col) from \
     functional.alltypestiny join functional.alltypessmall c2"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(Test1599, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(Test1599, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  @pytest.mark.execute_serially
  def test_scenario_2(self, vector):
    """Scenario 2: Check that a query fails if a fragment takes longer than
    --datastream_sender_timeout_ms to Prepare()"""

    # First rule: choose a single fragment instance to get delayed during fragment startup
    query_rules = DebugRuleSet().rule(
      DebugRule("query", "exchange_node", "prepare",
                id_selector=FixedRandSubset(1, CtxVar(key="id_set")),
                action=WaitFn(1000))
    )

    # Second rule: set datastream timeout much lower
    process_rules = DebugRuleSet().rule(
      DebugRule("rpc", "installdebugactions", "end_rpc",
                action=SetFlag("datastream_sender_timeout_ms", "100"))
    )
    self.client.set_debug_rules(process_rules.to_json())
    self.client.execute("SET QUERY_DEBUG_RULES='%s'" % (query_rules.to_json()))
    ex = self.execute_query_expect_failure(self.client, self.TEST_QUERY)
    assert "Sender timed out waiting for receiver fragment instance:" in str(ex)

  @pytest.mark.execute_serially
  def test_scenario_4(self, vector):
    """Scenario 4: Query fails if ExecRemoteFragment() RPC fails"""
    process_rules = DebugRuleSet().rule(
      DebugRule("rpc", "execplanfragment", "start_rpc", action=ReturnError())
    )
    self.client.set_debug_rules(process_rules.to_json())
    ex = self.execute_query_expect_failure(self.client, self.TEST_QUERY)
    assert "Error return by debug rule" in str(ex)

  @pytest.mark.execute_serially
  def test_scenario_5(self, vector):
    """Scenario 5: Query fails if cancelled before Prepare() has finished on some
    fragment"""
    query_rules = DebugRuleSet().rule(
      DebugRule("query", "exchange_node", "prepare",
                id_selector=FixedRandSubset(1, CtxVar(key="id_set")),
                action=WaitFn(2000))
    )
    self.client.execute("SET QUERY_DEBUG_RULES='%s'" % (query_rules.to_json()))
    handle = self.execute_query_async(self.TEST_QUERY, vector.get_value('exec_option'))
    self.client.cancel(handle)
    ex = self.expect_async_query_failure(self.client, handle)
    assert "Cancelled" in str(ex)

  @pytest.mark.execute_serially
  def test_scenario_8(self, vector):
    """Scenario 8: Query fails if Prepare() fails on any fragment"""
    query_rules = DebugRuleSet().rule(
      DebugRule("query", "exchange_node", "prepare",
                id_selector=FixedRandSubset(1, CtxVar(key="id_set")),
                action=ReturnError())
    )
    self.client.execute("SET QUERY_DEBUG_RULES='%s'" % (query_rules.to_json()))
    ex = self.execute_query_expect_failure(self.client, self.TEST_QUERY)
    assert "Error return by debug rule" in str(ex)

  def teardown_method(self, method):
    # Reinstate usual timeouts. How to do this better?
    process_rules = DebugRuleSet().rule(
      DebugRule("rpc", "installdebugactions", "end_rpc",
                action=SetFlag("datastream_sender_timeout_ms", "120000"))
    )
    self.client.set_debug_rules(process_rules.to_json())


  # def execute_query_then_cancel(self, query, vector, repeat = 1):
  #   for _ in range(repeat):
  #     handle = self.execute_query_async(query, vector.get_value('exec_option'))
  #     self.client.fetch(query, handle)
  #     try:
  #       self.client.cancel(handle)
  #     except ImpalaBeeswaxException:
  #       pass
  #     finally:
  #       self.client.close_query(handle)
  #   verifiers = [ MetricVerifier(i.service) for i in ImpalaCluster().impalads ]

  #   for v in verifiers:
  #     v.wait_for_metric("impala-server.num-fragments-in-flight", 0)
  #     v.verify_num_unused_buffers()

  # def execute_runtime_filter_query(self):
  #   query = "select STRAIGHT_JOIN * from functional_avro.alltypes a join \
  #           [SHUFFLE] functional_avro.alltypes b on a.month = b.id \
  #           and b.int_col = -3"
  #   self.client.execute("SET RUNTIME_FILTER_MODE=GLOBAL")
  #   self.client.execute("SET RUNTIME_FILTER_WAIT_TIME_MS=10000")
  #   self.client.execute("SET MAX_SCAN_RANGE_LENGTH=1024")
  #   self.execute_query_verify_metrics(query)

  # @pytest.mark.execute_serially
  # # @CustomClusterTestSuite.with_args("--backend_client_rpc_timeout_ms=1000"
  # #     " --fault_injection_rpc_delay_ms=3000 --fault_injection_rpc_type=1")
  # def test_execplanfragment_timeout(self, vector):
  #   debug_rules = ('{"PHASE": "START_RPC", "NAME": "execplanfragment", "SCOPE": "RPC", '
  #                  '"ACTION": [ '
  #                  '{"fn_name": "wait", "duration": 35000}]}')
  #   self.client.set_debug_rules(debug_rules)
  #   for i in range(3):
  #     ex = self.execute_query_expect_failure(self.client, self.TEST_QUERY)
  #     assert "RPC recv timed out" in str(ex)
  #   verifiers = [ MetricVerifier(i.service) for i in ImpalaCluster().impalads ]

  #   for v in verifiers:
  #     v.wait_for_metric("impala-server.num-fragments-in-flight", 0)
  #     v.verify_num_unused_buffers()
