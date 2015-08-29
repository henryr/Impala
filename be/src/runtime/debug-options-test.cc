#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <limits>
#include <gtest/gtest.h>
#include <boost/foreach.hpp>

#include "runtime/debug-options.h"

#include "common/init.h"
#include "common/names.h"
#include "gutil/strings/substitute.h"

using namespace boost;
using namespace strings;

namespace impala {

void ParseAndCheck(const string& txt, DebugOptions* opt, bool expect_failure=false) {
  Status s = opt->Parse(txt);
  if (!expect_failure) {
    ASSERT_TRUE(s.ok()) << s.msg().msg();
  } else {
    ASSERT_FALSE(s.ok()) << s.msg().msg();
  }
}

TEST(DebugOptionsParse, Backends) {
  typedef map<int, const char*> BackendChoiceFnMap;
  BOOST_FOREACH(const BackendChoiceFnMap::value_type& backend_choice_fn,
      _TBackendSelectorFn_VALUES_TO_NAMES) {
    DebugOptions opt;
    bool has_int_arg = (backend_choice_fn.first != TBackendSelectorFn::ALL &&
        backend_choice_fn.first != TBackendSelectorFn::NONE);
    const string& cmd_arg = has_int_arg ? "42" : "";
    const string& debug_cmd =
        Substitute("$0($1):NODE_ID(1):PREPARE:WAIT()", backend_choice_fn.second, cmd_arg);
    ParseAndCheck(debug_cmd, &opt);

    const TDebugCmd& cmd = opt.cmd();
    ASSERT_EQ(cmd.debug_phase, TExecNodePhase::PREPARE);
    ASSERT_EQ(cmd.action.cmd, TDebugActionCmd::WAIT);
    ASSERT_EQ(cmd.node_selector.choice_fn, TExecNodeSelectorFn::NODE_ID);
    ASSERT_EQ(opt.backend_selector().fn, backend_choice_fn.first);
    if (has_int_arg) ASSERT_EQ(opt.backend_selector().int_arg, 42);
  }
}

TEST(DebugOptionsParse, Nodes) {
  typedef map<int, const char*> NodeCmdMap;
  BOOST_FOREACH(const NodeCmdMap::value_type& node_cmd,
      _TExecNodeSelectorFn_VALUES_TO_NAMES) {
    DebugOptions opt;
    bool has_int_arg = node_cmd.first == TExecNodeSelectorFn::NODE_ID;
    const string& cmd_arg = has_int_arg ? "42" : "HDFS_SCAN*";
    const string& debug_cmd =
        Substitute("$0($1):PREPARE:WAIT()", node_cmd.second, cmd_arg);
    ParseAndCheck(debug_cmd, &opt);

    const TDebugCmd& cmd = opt.cmd();
    ASSERT_EQ(cmd.debug_phase, TExecNodePhase::PREPARE);
    ASSERT_EQ(cmd.action.cmd, TDebugActionCmd::WAIT);
    ASSERT_EQ(cmd.node_selector.choice_fn, node_cmd.first);
    if (has_int_arg) {
      ASSERT_EQ(cmd.node_selector.int_arg, 42);
    } else {
      if (node_cmd.first == TExecNodeSelectorFn::REGEX) {
        ASSERT_EQ(cmd.node_selector.str_arg, "HDFS_SCAN*");
      }
    }
    ASSERT_EQ(opt.backend_selector().fn, TBackendSelectorFn::ALL);
  }
}

TEST(DebugOptionsParse, Actions) {
  typedef map<int, const char*> ActionCmdMap;
  BOOST_FOREACH(const ActionCmdMap::value_type& action,
      _TDebugActionCmd_VALUES_TO_NAMES) {
    DebugOptions opt;
    bool has_arg =
        !(action.first == TDebugActionCmd::WAIT || action.first == TDebugActionCmd::FAIL);

    const string& cmd_arg = has_arg ? "42" : "";
    const string& debug_cmd =
        Substitute("FRAGMENT_PREPARE:$0($1)", action.second, cmd_arg);
    ParseAndCheck(debug_cmd, &opt);

    const TDebugCmd& cmd = opt.cmd();
    ASSERT_EQ(cmd.debug_phase, TExecNodePhase::FRAGMENT_PREPARE);
    ASSERT_EQ(cmd.action.cmd, action.first);
    if (has_arg) ASSERT_EQ(cmd.action.int_arg, 42);
    ASSERT_EQ(opt.backend_selector().fn, TBackendSelectorFn::ALL);
  }
}


TEST(DebugOptionsParse, Phases) {
  typedef map<int, const char*> PhaseMap;
  BOOST_FOREACH(const PhaseMap::value_type& phase, _TExecNodePhase_VALUES_TO_NAMES) {
    DebugOptions opt;
    ParseAndCheck(Substitute("$0:WAIT()", phase.second), &opt);

    const TDebugCmd& cmd = opt.cmd();
    ASSERT_EQ(cmd.debug_phase, phase.first);
    ASSERT_EQ(cmd.action.cmd, TDebugActionCmd::WAIT);
    ASSERT_EQ(opt.backend_selector().fn, TBackendSelectorFn::ALL);
  }
}

TEST(DebugOptionsParse, SpecificCases) {
  DebugOptions opt;
  ParseAndCheck("ALL():REGEX(HDFS_SCAN_NODE):PREPARE:WAIT()", &opt);
}

void SelectAndCheck(const string& action, const vector<TPlanFragment>& plan_fragments,
    const vector<FragmentExecParams>& params, int expected) {
  LOG(INFO) << "SelectAndCheck(): " << action;
  DebugOptions opt;
  ParseAndCheck(action, &opt);
  set<int32_t> backend_ids;
  opt.ComputeApplicableBackends(plan_fragments, params, &backend_ids);
  ASSERT_EQ(expected, backend_ids.size());
}

const int NUM_FRAGMENTS = 10;
const int NUM_INSTANCES_PER_FRAGMENT = 10;
const int TOTAL_INSTANCES = NUM_FRAGMENTS * NUM_INSTANCES_PER_FRAGMENT;

void CreateFragmentsAndParams(vector<TPlanFragment>* plan_fragments,
    vector<FragmentExecParams>* params) {
  for (int i = 0; i < NUM_FRAGMENTS; ++i) {
    TPlanFragment fragment;
    // For simplicity, make it look like there's no coordinator fragment
    fragment.partition.type = TPartitionType::RANDOM;
    for (int j = 0; j < 5; ++j) {
      TPlanNode plan_node;
      plan_node.node_id = (5 * i) + j;
      if (j == 1) {
        plan_node.node_type = (i % 2) == 0 ? TPlanNodeType::HDFS_SCAN_NODE :
            TPlanNodeType::HBASE_SCAN_NODE;
      } else {
        plan_node.node_type = (i % 2) == 0 ? TPlanNodeType::AGGREGATION_NODE :
            TPlanNodeType::SORT_NODE;
      }
      fragment.plan.nodes.push_back(plan_node);
    }
    plan_fragments->push_back(fragment);
    params->push_back(FragmentExecParams());
    params->back().hosts.resize(NUM_INSTANCES_PER_FRAGMENT);
  }
}

TEST(DebugOptionsSelect, BackendSelector) {
  vector<TPlanFragment> plan_fragments;
  vector<FragmentExecParams> params;
  CreateFragmentsAndParams(&plan_fragments, &params);

  SelectAndCheck("ALL():ALL():PREPARE:WAIT()", plan_fragments, params, TOTAL_INSTANCES);
  SelectAndCheck("NONE():ALL():PREPARE:WAIT()", plan_fragments, params, 0);
  SelectAndCheck("RAND(1):ALL():PREPARE:WAIT()", plan_fragments, params, 1);
  SelectAndCheck("RAND(10):ALL():PREPARE:WAIT()", plan_fragments, params, 10);
  SelectAndCheck("RAND(100000):ALL():PREPARE:WAIT()", plan_fragments, params,
      TOTAL_INSTANCES);
  SelectAndCheck("RAND(-1):ALL():PREPARE:WAIT()", plan_fragments, params, 0);

  // RAND() applies node selector to only pick from applicable backends
  SelectAndCheck("RAND(20):NODE_ID(0):PREPARE:WAIT()", plan_fragments, params, 10);
  SelectAndCheck("RAND(10):REGEX(NO_MATCH):PREPARE:WAIT()", plan_fragments, params, 0);
}

TEST(DebugOptionsSelect, NodeSelector) {
  vector<TPlanFragment> plan_fragments;
  vector<FragmentExecParams> params;
  CreateFragmentsAndParams(&plan_fragments, &params);

  SelectAndCheck("ALL():ALL():PREPARE:WAIT()", plan_fragments, params, TOTAL_INSTANCES);

  SelectAndCheck("ALL():NODE_ID(0):PREPARE:WAIT()", plan_fragments, params, 10);
  SelectAndCheck("ALL():NODE_ID(100):PREPARE:WAIT()", plan_fragments, params, 0);
  // Remove?
  SelectAndCheck("ALL():NODE_ID(-1):PREPARE:WAIT()", plan_fragments, params, 100);
  SelectAndCheck("ALL():NODE_ID(-2):PREPARE:WAIT()", plan_fragments, params, 0);

  SelectAndCheck("ALL():REGEX(HDFS_SCAN_NODE):PREPARE:WAIT()", plan_fragments, params,
      TOTAL_INSTANCES / 2);
  SelectAndCheck("ALL():REGEX(.*_SCAN_NODE):PREPARE:WAIT()", plan_fragments, params,
      TOTAL_INSTANCES);
  SelectAndCheck("ALL():REGEX(SORT_NODE):PREPARE:WAIT()", plan_fragments, params,
      TOTAL_INSTANCES / 2);
  SelectAndCheck("ALL():REGEX(AGGREGATION_NODE):PREPARE:WAIT()", plan_fragments, params,
      TOTAL_INSTANCES / 2);
  SelectAndCheck("ALL():REGEX(NO_MATCH):PREPARE:WAIT()", plan_fragments, params, 0);

  // Test implicit ALL() backend selector
  SelectAndCheck("NODE_ID(0):PREPARE:WAIT()", plan_fragments, params,
      NUM_INSTANCES_PER_FRAGMENT);
}


}
int main(int argc, char **argv) {
  using namespace impala;
  InitCommonRuntime(argc, argv, false, TestInfo::BE_TEST);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
