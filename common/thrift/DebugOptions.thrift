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


namespace cpp impala
namespace java com.cloudera.impala.thrift

// A debug command is a way to tell Impala to take a certain action at some point during
// query execution. For example, we might tell Impala to pause for 10 seconds before
// continuing with a plan fragment, or to cancel a query once GetNext() is called.
//
// The effect is to inject (very) simple programs into Impala at any one of several
// predefined execution points.
//
// A debug command consists of two sections. The first describes *when* to run a debug
// command, and the second describes *what* to do.

enum TBackendSelectorFn {
  // Select all backends
  ALL,

  // Select only the instance with a given backend ID
  ID,

  // RAND(N) selects at most N
  RAND,
  NONE
}

struct TBackendSelector {
  1: optional TBackendSelectorFn fn,
  2: optional i32 int_arg
}

enum TExecNodeSelectorFn {
  NODE_ID,
  REGEX,
  ALL
}

struct TExecNodeSelector {
  1: optional TExecNodeSelectorFn choice_fn,
  2: optional i32 int_arg // For LITERAL
  3: optional string str_arg // For REGEX
}

// phases of an execution node
enum TExecNodePhase {
  PREPARE,
  OPEN,
  GETNEXT,
  CLOSE,
  INVALID,
  FRAGMENT_PREPARE,
  FRAGMENT_OPEN,
  FRAGMENT_CLOSE,
  FRAGMENT_GETNEXT
}

// what to do when hitting a debug point (TImpalaQueryOptions.DEBUG_ACTION)
enum TDebugActionCmd {
  WAIT,
  FAIL,
  // Delays a fixed amount
  DELAY,
  // Delays a random ms each time
  DELAY_RAND,
  // Delays exactly once, then fails
  DELAY_THEN_FAIL,
  // Delays for a random amount of time, then fails
  DELAY_RAND_THEN_FAIL
}

struct TDebugAction {
  1: optional TDebugActionCmd cmd
  2: optional i32 int_arg
}

struct TDebugCmd {
  // Note that backend spec is not included, as will be determined at coordinator
  1: optional TExecNodeSelector node_selector
  2: optional TExecNodePhase debug_phase
  3: optional TDebugAction action
}
