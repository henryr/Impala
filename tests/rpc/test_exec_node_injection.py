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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_cluster import ImpalaCluster
from tests.common.skip import SkipIfBuildType
from tests.verifiers.metric_verifier import MetricVerifier
from tests.util.debug_rules import *

import itertools
import random

@SkipIfBuildType.not_dev_build
class TestExecNodesDelays(ImpalaTestSuite):
  TEST_QUERY = "select count(c2.string_col) from \
     functional.alltypestiny join functional.alltypessmall c2"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    # if cls.exploration_strategy() != 'exhaustive':
    #   pytest.skip('runs only in exhaustive')
    super(TestExecNodesDelays, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestExecNodesDelays, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  def random_rules(self):
    exec_nodes = ["aggregation_node", "hdfs_scan_node", "exchange_node", "hash_join_node"]
    phases = ["open", "prepare", "close", "get_next"]
    permutations = [x for x in itertools.product(exec_nodes, phases)]
    rules = DebugRuleSet()
    for name, phase in random.sample(permutations, 4):
      rules.rule(DebugRule(scope="query", name=name, phase=phase,
                           id_selector=RandSubset(1.0, set_arg=CtxVar(key="id_set")),
                           action=WaitFn(duration=Rand(max_val=4000))))
    return rules

  @pytest.mark.execute_serially
  def test_tpch_with_delays(self, vector):
    for _ in xrange(50):
      rules = self.random_rules()
      self.client.execute("SET QUERY_DEBUG_RULES='%s'" % rules.to_json())
      self.client.execute(self.TEST_QUERY)
