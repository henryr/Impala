// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/debug-options.h"

#include <map>
#include <boost/algorithm/string.hpp>

#include "util/debug-util.h"
#include "runtime/runtime-state.h"
#include "gutil/strings/substitute.h"

using namespace boost::algorithm;
using namespace strings;

#include "common/names.h"

namespace impala {

Status DebugOptions::Parse(const string& debug_str) {
  if (debug_str.empty()) {
    backend_selector_.__set_fn(TBackendSelectorFn::NONE);
    return Status::OK();
  }

  // Set defaults. No need to set default node selector - if it's not set, it's not used.
  backend_selector_.__set_fn(TBackendSelectorFn::ALL);

  vector<string> components;
  split(components, debug_str, is_any_of(":"), token_compress_on);
  // 2 components -> fragment-level cmd
  // 3 components -> all backends cmd
  // 4 components -> specific backends cmd
  if (components.size() < 2 || components.size() > 4) {
    return Status("Debug options should have 2-4 components (fields separated by ':').");
  }

  string* backend_str = NULL;
  string* node_str = NULL;
  string* phase_str = NULL;
  string* action_str = NULL;
  if (components.size() == 2) {
    phase_str = &components[0];
    action_str = &components[1];
  } else if (components.size() == 3) {
    node_str = &components[0];
    phase_str = &components[1];
    action_str = &components[2];
  } else {
    backend_str = &components[0];
    node_str = &components[1];
    phase_str = &components[2];
    action_str = &components[3];
  }

  if (backend_str != NULL) RETURN_IF_ERROR(ParseBackendSelector(*backend_str));
  if (phase_str != NULL) RETURN_IF_ERROR(ParsePhaseSelector(*phase_str));
  if (action_str != NULL) RETURN_IF_ERROR(ParseAction(*action_str));
  if (node_str != NULL) RETURN_IF_ERROR(ParseNodeSelector(*node_str));
  return Status::OK();
}

// Utility method to parse "FOO(BAR)" into *cmd == 'foo' and *arg == 'bar'. Returns true
// if parse was successful, false otherwise. Blank arguments are allowed.
bool GetCmdAndArg(const string& str, string* cmd, string* arg) {
  bool looking_for_open = true;
  int open_idx = 0;
  for (int i = 0; i < str.size(); ++i) {
    if (looking_for_open && str[i] == '(') {
      if (i == 0) return false;
      *cmd = str.substr(0, i);
      looking_for_open = false;
      open_idx = i;
    } else if (!looking_for_open && str[i] == ')') {
      *arg = str.substr(open_idx + 1, i - (open_idx + 1));
      return true;
    }
  }
  return false;
}

Status DebugOptions::ParseBackendSelector(const string& str) {
  // Backend selectors look like one of the following:
  //  ALL(), ID(x), RAND(n), NONE()
  string cmd;
  string arg;
  if (!GetCmdAndArg(str, &cmd, &arg)) {
    return Status(Substitute("Could not parse backend selector: $0", str));
  }
  TBackendSelectorFn::type fn;
  if (!GetTBackendSelectorFnByName(cmd, &fn)) {
    return Status(Substitute("Parse failed: $0", str));
  }
  backend_selector_.__set_fn(fn);
  switch (fn) {
    case TBackendSelectorFn::ALL: break;
    case TBackendSelectorFn::ID:
    case TBackendSelectorFn::RAND:
      backend_selector_.__set_int_arg(atoi(arg.c_str()));
      break;
    case TBackendSelectorFn::NONE:
      backend_selector_.__set_int_arg(-1);
      break;
    default:
      DCHECK(false) << "Unrecognised backend choice fn: " << str;
  }
  return Status::OK();
}

Status DebugOptions::ParsePhaseSelector(const string& phase) {
  TExecNodePhase::type val;
  if (!GetTExecNodePhaseByName(phase, &val)) {
    return Status(Substitute("Could not find exec-phase corresponding to: $0", phase));
  }
  cmd_.__set_debug_phase(val);
  return Status::OK();
}

Status DebugOptions::ParseAction(const string& str) {
  // Actions look like one of the following:
  //  WAIT(), FAIL(), DELAY(N), DELAY_RAND(N), DELAY_THEN_FAIL(N)
  string cmd;
  string arg;
  if (!GetCmdAndArg(str, &cmd, &arg)) return Status("Action parse failed");
  TDebugActionCmd::type fn;
  if (!GetTDebugActionCmdByName(cmd, &fn)) {
    return Status(Substitute("Action parse failed, no TDebugAction: $0", cmd));
  }
  cmd_.__set_action(TDebugAction());
  cmd_.action.__set_cmd(fn);
  switch (fn) {
    case TDebugActionCmd::WAIT:
    case TDebugActionCmd::FAIL:
      break;
    case TDebugActionCmd::DELAY:
    case TDebugActionCmd::DELAY_RAND:
    case TDebugActionCmd::DELAY_THEN_FAIL:
    case TDebugActionCmd::DELAY_RAND_THEN_FAIL:
      cmd_.action.__set_int_arg(atoi(arg.c_str()));
      break;
    default:
      DCHECK(false) << "Unrecognised backend choice fn";
  }
  return Status::OK();
}

Status DebugOptions::ParseNodeSelector(const string& str) {
  // NODE selectors look like one of the following:
  //  NODE_ID(N), REGEX(pattern)
  string cmd;
  string arg;
  if (!GetCmdAndArg(str, &cmd, &arg)) return Status("Node parse failed");
  TExecNodeSelectorFn::type fn;
  if (!GetTExecNodeSelectorFnByName(cmd, &fn)) {
    return Status(Substitute("Node parse failed, no TExecNodeSelectorFn: $0", cmd));
  }
  cmd_.__set_node_selector(TExecNodeSelector());
  cmd_.node_selector.__set_choice_fn(fn);
  switch (fn) {
    case TExecNodeSelectorFn::NODE_ID:
      cmd_.node_selector.__set_int_arg(atoi(arg.c_str()));
      break;
    case TExecNodeSelectorFn::REGEX: {
      // Check if the regex can be compiled
      node_regex_.reset(new re2::RE2(arg));
      if (!node_regex_->ok()) {
        return Status(Substitute("Could not compile REGEX($0): $1", arg,
            node_regex_->error()));
      }
      cmd_.node_selector.__set_str_arg(arg);
      break;
    }
    case TExecNodeSelectorFn::ALL:
      break;
    default:
      DCHECK(false) << "Unrecognised backend choice fn: " << str;
  }

  return Status::OK();
}

bool DebugOptions::FragmentMatches(const TPlanFragment& fragment) {
  if (cmd().node_selector.choice_fn == TExecNodeSelectorFn::ALL) return true;

  BOOST_FOREACH(const TPlanNode& plan_node, fragment.plan.nodes) {
    if (cmd().node_selector.choice_fn == TExecNodeSelectorFn::NODE_ID) {
      return cmd().node_selector.int_arg == -1 ||
          cmd().node_selector.int_arg == plan_node.node_id;
    } else {
      DCHECK(node_regex_->ok());
      if (node_regex_->FullMatch(PrintPlanNodeType(plan_node.node_type), *node_regex_)) {
        return true;
      }
    }
  }

  return false;
}

void DebugOptions::ComputeApplicableBackends(const vector<TPlanFragment>& fragments,
    const vector<FragmentExecParams>& params, set<int32_t>* backend_ids) {
  int32_t start_idx =
      fragments[0].partition.type == TPartitionType::UNPARTITIONED ? 1 : 0;

  switch (backend_selector_.fn) {
    case TBackendSelectorFn::NONE:
      return;
    case TBackendSelectorFn::ALL: {
      int32_t backend_num = 0;
      for (int i = start_idx; i < fragments.size(); ++i) {
        bool matches = FragmentMatches(fragments[i]);
        for (int j = 0; j < params[i].hosts.size(); ++j) {
          if (matches) backend_ids->insert(backend_num);
          ++backend_num;
        }
      }
      break;
    }
    case TBackendSelectorFn::ID: {
      backend_ids->insert(backend_selector_.int_arg);
      break;
    }
    case TBackendSelectorFn::RAND: {
      if (backend_selector_.int_arg < 0) return;
      int32_t backend_num = 0;
      vector<int32_t> applicable_backends;
      for (int i = start_idx; i < fragments.size(); ++i) {
        for (int j = 0; j < params[i].hosts.size(); ++j) {
          if (FragmentMatches(fragments[i])) {
            applicable_backends.push_back(backend_num);
          }
          ++backend_num;
        }
      }

      random_shuffle(applicable_backends.begin(), applicable_backends.end());
      // Pick at most N elements from the shuffled list
      int32_t sz = backend_selector_.int_arg < applicable_backends.size() ?
          backend_selector_.int_arg : applicable_backends.size();
      applicable_backends.resize(sz);
      backend_ids->insert(applicable_backends.begin(), applicable_backends.end());
      break;
    }
  }
}

Status DebugOptions::ExecDebugAction(const TDebugCmd& cmd, TExecNodePhase::type phase,
    RuntimeState* state) {
  if (phase != cmd.debug_phase) return Status::OK();
  switch (cmd.action.cmd) {
    case TDebugActionCmd::FAIL:
      return Status(TErrorCode::INTERNAL_ERROR, "Debug Action: FAIL");
    case TDebugActionCmd::WAIT:
      while (!state->is_cancelled()) SleepForMs(1000);
      return Status::CANCELLED;
    case TDebugActionCmd::DELAY:
      SleepForMs(cmd.action.int_arg);
      return Status::OK();
    case TDebugActionCmd::DELAY_RAND:
      SleepForMs(rand() % cmd.action.int_arg);
      return Status::OK();
    case TDebugActionCmd::DELAY_RAND_THEN_FAIL:
    case TDebugActionCmd::DELAY_THEN_FAIL: {
      int32_t sleep_time = cmd.action.int_arg;
      if (cmd.action.cmd == TDebugActionCmd::DELAY_RAND_THEN_FAIL) {
        sleep_time = rand() % sleep_time;
      }
      SleepForMs(sleep_time);
      return Status(TErrorCode::INTERNAL_ERROR,
          Substitute("Debug Action: $0", PrintTDebugActionCmd(cmd.action.cmd)));
    }
    default:
      DCHECK(false);
      return Status::OK();
  }
}

}
