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

#ifndef IMPALA_RUNTIME_DEBUG_OPTIONS_H
#define IMPALA_RUNTIME_DEBUG_OPTIONS_H

#include <boost/scoped_ptr.hpp>
#include <string>
#include <re2/re2.h>

#include "gen-cpp/PlanNodes_types.h"
#include "common/status.h"
#include "scheduling/query-schedule.h"

namespace impala {

class RuntimeState;

/// DebugOptions are used to manipulate the behaviour of a single query in order to expose
/// or debug problems that may be related to timing, or errors, or rarely-taken code
/// paths. DebugOptions are specified by the user as strings, parsed into a TDebugCmd by
/// this class, and then sent to all backends that they affect during query start-up.
///
/// A debug action has four parts:
///
/// * The 'backend selector' controls which fragment instances this action should apply to
/// * The 'node selector' controls which nodes in a particular fragment instance this
///   action should apply to.
/// * The 'execution phase' controls when an action should be applied - for example,
///   during Prepare(), or for every GetNext(). Phases may apply to fragments (i.e. the
///   fragment-wide prepare, close operations etc.) or to individual nodes.
/// * The 'action' describes what to do. Examples include delaying for some time, failing,
///   or waiting for cancellation.
///
/// DebugOptions are written in text like so:
///   <backend_sel_fn>(<arg1>):<node_sel_fn>(<arg2>):<exec_phase>:<action>(<arg3>)
///
/// The backend selector is optional (in which case it defaults to ALL()), and so is the
/// node selector (it defaults to ALL(), or is not needed if the exec. phase refers to a
/// fragment, not a node, phase).
class DebugOptions {
 public:
  /// Parses the debug option in `debug_str`, returning OK() if the parse was successful,
  /// and an error otherwise.
  Status Parse(const std::string& debug_str);

  /// Returns the TDebugCmd that was built during Parse().
  const TDebugCmd& cmd() const { return cmd_; }

  const TBackendSelector backend_selector() const { return backend_selector_; }

  /// Given a list of plan fragments, and their parameters, constructs a list of indexes
  /// of fragment instances that this debug options is applicable to; that is it evaluates
  /// the backend selector on a given plan. Backend IDs are computed in the same way as
  /// Coordinator::StartRemoteFragments(): the first non-coordinator fragment which has
  /// N_0 hosts has backend IDs 0..N_0-1, the second fragment which has N_1 hosts has
  /// backends IDs N_0..N_1-1 and so on.
  void ComputeApplicableBackends(const std::vector<TPlanFragment>& fragments,
      const std::vector<FragmentExecParams>& params, std::set<int32_t>* backend_ids);

  /// Given a debug command and a current execution phase, executes the command if the
  /// phase matches that in cmd. Returns OK() unless the action is WAIT, FAIL or
  /// DELAY_THEN_FAIL.
  static Status ExecDebugAction(const TDebugCmd& cmd, TExecNodePhase::type phase,
      RuntimeState* state);

 private:
  /// Set during Parse()
  TDebugCmd cmd_;

  /// Set during Parse()
  TBackendSelector backend_selector_;

  /// Only used to match REGEX() node selectors
  boost::scoped_ptr<re2::RE2> node_regex_;

  /// Helper functions for Parse().
  Status ParseBackendSelector(const std::string& str);
  Status ParsePhaseSelector(const std::string& str);
  Status ParseNodeSelector(const std::string& str);
  Status ParseAction(const std::string& str);

  // Returns true if the given fragment matches the current node selector.
  bool FragmentMatches(const TPlanFragment& fragment);
};

}

#endif
