# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted tests for Impala joins
#
import logging
import os
import pytest
from copy import copy
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIf, SkipIfS3, SkipIfIsilon, SkipIfOldAggsJoins, SkipIfLocal

class TestRuntimeFilters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRuntimeFilters, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

    # if cls.exploration_strategy() != 'exhaustive':
    #   # Cut down on execution time when not running in exhaustive mode.
    #   cls.TestMatrix.add_constraint(lambda v: v.get_value('batch_size') != 1)

  def test_basic_filters(self, vector):
    self.run_test_case('QueryTest/runtime_filters', vector)
