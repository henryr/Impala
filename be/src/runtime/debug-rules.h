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

#ifndef IMPALA_RUNTIME_DEBUG_RULES_H
#define IMPALA_RUNTIME_DEBUG_RULES_H

#include "common/status.h"
#include "rapidjson/document.h"

#include <vector>
#include <set>

#include <unordered_map>
#include "util/spinlock.h"

/// Debug rules allow programmers to inject behaviour into Impala at predefined points
/// without recompiling the Impala binary. For example, a debug rule can be used to delay
/// the execution of a particular RPC, to cause an HDFS scan node to fail during
/// preparation with probability 0.2 or to log a message every time a particular line of
/// code is executed.
///
/// Rules are evaluated at 'tracepoints', which are added by macros to code paths that
/// would benefit from behavioural injection. Tracepoints are only active in debug builds,
/// in release builds the debug rule framework is inactive.
///
/// Debug rules are composed of an sexpr-like tree of functions, expressed as JSON. As a
/// future improvement we might consider embedding Lua or a similar scripting language to
/// perform more general purpose computations.
///
////////////////////////
/// TRACEPOINTS
////////////////////////
/// Tracepoints are points in the code where a debug rule might be evaluated. Tracepoints
/// have a signature that uniquely identifies them. A signature is a triple:
///
/// signature -> (scope, entity, phase)
///
/// The scope is the general domain of the tracepoint (e.g. 'rpc', 'fragment' or
/// 'query'). The entity is the name of the thing which contains the tracepoint - perhaps
/// the RPC method name, or the name of an exec node. The phase is the point during
/// execution where the tracepoint exists - e.g. 'open', 'start_rpc' etc. The set of
/// phases changes with the entity. All three parameters are free-text fields and are not
/// structured: developers may use any scope, entitity and phase that is meaningful to
/// them.
///
/// For example -> ("query", "hdfs_scan_node", "open") is a tracepoint that we would
/// expect to find at the start of HdfsScanNode::Open().
///
/// Tracepoints are declared by a set of helper macros, defined below.
///
////////////////////////
/// RULES
////////////////////////
/// A debug rule consists of three parts.
///
/// 1. The 'tracepoint selector' determines which rules are applicable at a tracepoint. A
/// tracepoint is parameterised by its 'entity name', its 'scope' and its 'phase'. This is
/// the 'tracepoint signature'. A tracepoint selector is a function which, given a
/// tracepoint signature, returns a boolean value where 'true' means the rule applies to
/// that tracepoint. The default selector just matches tracepoints to rules based on their
/// signature.
///
/// 2. The 'instance selector' determines which set of 'instances' of tracepoints with
/// applicable rules, as determined by the tracepoint selector, should actually apply the
/// debug rule. Although a tracepoint with a single signature is typically only written
/// once in the code, the object that contains it may be instantiated many times.
///
/// For example, a tracepoint may apply to the Open() call in an HDFS scan node. One query
/// may have many fragment instances with HDFS scan nodes, and each of those counts as an
/// individual 'instance' of that tracepoint.
///
/// Each of these tracepoints has a unique 'instance id'. Evaluating the instance selector
/// requires knowledge of the complete set of possible instances, so that functions like
/// "a random subset consisting of at most 5 instances" may be computed.
///
/// We may wish to write a rule that applies only to a maximum of three fragment instances
/// with an hdfs_scan_node operator. The instance selector allows us to write the
/// equivalent of: RAND_SUBSET(instances where entity == "hdfs_scan_node", max_size=3)
///
/// 3. The 'action' is the action or list of actions a rule takes once its corresponding
/// tracepoint is actually executed. All actions ultimately return a Status, which, if not
/// OK(), is returned by the function the tracepoint is in. Several actions may be
/// executed in sequence at one tracepoint (but for now, only one rule). If any one action
/// returns an error Status, execution of that action sequence is aborted. This allows the
/// expression of such idioms as "with probability 0.5, execute the next action else
/// return an error".
///
/// Rules are serialized as JSON to be human-readable and (somewhat) writable, and to
/// interoperate easily with Python.
///
////////////////////////
/// RULE SETS
////////////////////////
///
/// A rule set is a container for a group of rules, typically bound to one or more
/// 'scopes'. For example, each query has its own rule set (so as not to interfere with
/// other queries), and there is also a process-wide rule set which has cross-system
/// rules, like setting a CLI flag or RPC tracepoints. Rules are registered with rule sets
/// via a set of client APIs.
///
////////////////////////
/// WORKING WITH DEBUG RULES
////////////////////////
///
/// 1. Adding new debug functions
///
/// All debug functions are subclasses of DebugFunction. DebugFunction implementations are
/// able to specify a simple schema in terms of the arguments (and their type) that they
/// expect. DebugFunctions must implement Get[T]() methods for all the types T that they
/// may return - their parent will select the correct method based on the expected
/// type. Unsupported types will cause a runtime error, as type-compatibility should be
/// checked at debug-rule construction time, not during evaluation.
///
/// A new debug function must then be registered with the function factory map in
/// DebugFunction::Create(). In the future, functions should be registered at run-time.
///
/// 2. Setting new debug rules
///
/// Two classes of rule set are currently supported. The first is the process rule
/// set. Each Impala server has an InstallDebugActions() RPC which accepts a
/// JSON-serialized set of rules and installs them across the cluster (by computing the
/// set of instance IDs and forwarding a custom list of rules to each individual Impala
/// server). These rules are permanent until another set is installed.
///
/// The second rule set class is the query-local rule set. Rules are specified via the
/// QUERY_DEBUG_RULES query option, and don't persist after the query has finished.
///
/// 3. Using debug rules in Python
///
/// Python tests may use the new set_debug_rules() method to specify process-wide debug
/// rules, and use execute("SET QUERY_DEBUG_RULES=...") as usual to set query debug rules.
///
/// For convenience, a Python rule builder exists (see tests/util/debug_rules.py). For
/// example, from test_1599.py:
///
///     # First rule: choose a single fragment instance to get delayed during fragment
///     # startup
///     query_rules = DebugRuleSet().rule(
///       DebugRule("query", "exchange_node", "prepare",
///                 id_selector=FixedRandSubset(1, CtxVar(key="id_set")),
///                 action=WaitFn(1000))
///     )
///
///     # Second rule: set datastream timeout much lower
///     process_rules = DebugRuleSet().rule(
///       DebugRule("rpc", "installdebugactions", "end_rpc",
///                 action=SetFlag("datastream_sender_timeout_ms", "100")))
///
///     # Install query and process-wide rules
///     self.client.set_debug_rules(process_rules.to_json())
///     self.client.execute("SET QUERY_DEBUG_RULES='%s'" % (query_rules.to_json()))


namespace impala {

namespace debug {

struct Scope {
  static const std::string RPC;
  static const std::string PROCESS;
  static const std::string FRAGMENT;
  static const std::string QUERY;
};

}

enum ArgType {
  INT64,
  DOUBLE,
  STRING,
  STATUS,
  ID_SET
};

typedef std::set<int64_t> DebugIdSet;

/// Environment container for DebugFunctions. Available during rule evaluation to access
/// runtime variables. Currently limited to trace point name data, but could be extended,
/// e.g., to provide current fragment IDs, or thread IDs, to rules.
///
/// EvalContext is used particularly to evaluate selector functions, which are typically
/// parameterised by entity name and instance ID.
class EvalContext {
 public:
  /// 'name' is the entity name for the current tracepoint. 'id' is the instance ID.
  EvalContext(const std::string& name, int64_t id) : name_(name), id_(id) { }
  std::string name() const { return name_; }
  int64_t id() const { return id_; }
  const DebugIdSet& id_set() const { return id_set_; }

  void SetIdSet(const DebugIdSet& id_set) { id_set_ = id_set; }

 private:
  std::string name_;
  int64_t id_;

  DebugIdSet id_set_;
};

/// Superclass for all debug functions that comprise the action part of a debug rule.
class DebugFunction {
 public:
  virtual void ToJson(rapidjson::Value* value, rapidjson::Document* document);

  /// Subclasses should perform type-checking per 'expected', set name_ and arg_schema_,
  /// then call InitFromSchema() here.
  virtual Status Init(rapidjson::Value* json, ArgType expected) = 0;

  /// Evaluation functions. Any unimplemented methods will cause runtime errors -
  /// subclasses should implement only those types that they are compatible with.
  virtual int64_t GetInt(const EvalContext& ctx) {
    DCHECK(false) << "GetInt()"; return 0;
  }
  virtual double GetDouble(const EvalContext& ctx) {
    DCHECK(false) << "GetDouble()"; return 0.0;
  }
  virtual std::string GetString(const EvalContext& ctx) {
    DCHECK(false) << "GetString()"; return "";
  }
  virtual Status GetStatus(const EvalContext& ctx) {
    DCHECK(false) << "GetStatus()"; return Status::OK();
  }
  virtual const DebugIdSet GetIdSet(const EvalContext& ctx) {
    DCHECK(false) << "GetIdSet()";
    // Can't return ref-to-temporary, so return a nonsensical heap ref instead.
    return DebugIdSet();
  }

  /// Static factory method
  static Status Create(ArgType expected, rapidjson::Value* json,
      std::unique_ptr<DebugFunction>* output);

  std::string name() const { return name_; }

 protected:
  /// List of arguments, indexed by corresponding position in arg_schema_.
  std::vector<std::unique_ptr<DebugFunction>> args_;
  std::vector<std::pair<std::string, ArgType>> arg_schema_;

  Status InitFromSchema(rapidjson::Value* json);
  std::string name_;
};

/// A single rule is evaluated at a tracepoint if they match on scope, phase and entity
/// name.
class DebugRule {
 public:
  DebugRule(const std::string& scope, const std::string& phase,
      std::vector<std::unique_ptr<DebugFunction>> actions, const std::string& name,
      std::unique_ptr<DebugFunction> selector,
      std::unique_ptr<DebugFunction> id_selector)
      : scope_(scope), phase_(phase), actions_(std::move(actions)), name_(name),
        selector_(std::move(selector)), id_selector_(std::move(id_selector)) { }

  DebugRule(DebugRule&& other) = default;

  const std::string& scope() const { return scope_; }
  const std::string& phase() const { return phase_; }
  const std::vector<std::unique_ptr<DebugFunction>>& actions() const { return actions_; }
  std::string entity_name() const { return name_; }

  Status Exec(const EvalContext& ctx);
  bool EvalSelector(const std::string& name, int64_t instance_id);

  /// Returns the set of instance ids in 'input' that this DebugRule should apply to.
  DebugIdSet ComputeIdSet(const DebugIdSet& input);

  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) const;

  static Status Create(rapidjson::Value* json, std::shared_ptr<DebugRule>* rule);

 private:
  const std::string scope_;
  const std::string phase_;
  const std::vector<std::unique_ptr<DebugFunction>> actions_;
  const std::string name_;
  const std::unique_ptr<DebugFunction> selector_;
  const std::unique_ptr<DebugFunction> id_selector_;
};

/// Container for all rules in one or more scopes.
class DebugRuleSet {
 public:
  static DebugRuleSet* GetInstance() {
    static DebugRuleSet singleton_;
    return &singleton_;
  }

  /// Given a scope, phase and entity name, determine if a debug rule exists and if so,
  /// executes it. Returns OK() unless the rule itself returns an error during execution.
  Status ExecDebugRule(const std::string& scope, const std::string& phase,
      const std::string& entity_name, int64_t id);

  /// If debug action should apply, call lambda and return its value.
  Status ExecInjectedDebugCode(const std::string& scope, const std::string& phase,
      const std::string& entity_name, int64_t id, const std::function<void()>& lambda);

  // Called from webpage, maybe argument is actual JSON object
  Status SetDebugRules(const std::string& action_json);

  /// Removes all rules
  void ClearRules();

  /// Adds a single rule to this provider. Returns an error if the json is missing
  /// required fields, or if the rule is otherwise incorrectly formed.
  Status AddOneRule(rapidjson::Value* json);

  /// Adds a set of rules to this provider. 'json' is expected to be an object which
  /// contains an array of rules:
  /// { "rules": [ <rule1>, <rule2>, ... ] }
  Status AddRules(rapidjson::Value* json);

  int num_rules() const { return rules_.size(); }

  void ToJson(rapidjson::Document* document, rapidjson::Value* val);

  /// Builds JSON document containing all rules applicable to 'id'. Only valid after
  /// ComputeIdSets() has been called. Returns the number of rules that match.
  int64_t GetJsonRulesForId(int64_t id, rapidjson::Document* document);

  /// Compute the set of applicable instance IDs for all rules, based on the supplied set
  /// of tracepoints. Since all tracepoints are presumed to contain all phases, only the
  /// entity name for each tracepoint is provided.
  Status ComputeIdSets(const std::vector<std::pair<int64_t, std::string>>& entities);

  /// Returns the list of applicable IDs for a rule with a particular signature.
  DebugIdSet GetIdSetForRule(const std::string& scope, const std::string& phase,
      const std::string& entity_name);

 private:
  /// Protects rules_
  SpinLock lock_;

  /// All rules.
  std::unordered_map<string, std::shared_ptr<DebugRule>> rules_;
  std::unordered_map<string, DebugIdSet> id_sets_;
};

#ifndef NDEBUG

#define PROCESS_TRACE_POINT(scope, entity, phase, id)            \
  RETURN_IF_ERROR(DebugRuleSet::GetInstance()->ExecDebugRule(scope, phase, entity, id))

#define PROCESS_TRACE_POINT_NO_RETURN(scope, entity, phase, id)                   \
  DebugRuleSet::GetInstance()->ExecDebugRule(scope, phase, entity, id)

#define RPC_TRACE_POINT(entity, phase, id, return_val)                             \
  { Status s = PROCESS_TRACE_POINT_NO_RETURN(debug::Scope::RPC, entity, phase, id); \
      if (!s.ok()) { \
      s.SetTStatus(&return_val); \
      return; \
      }\
  }

#define QUERY_TRACE_POINT(entity, phase, provider)        \
  RETURN_IF_ERROR(                                               \
       (provider)->ExecDebugRule("query", phase, entity, 0));

#define QUERY_TRACE_POINT_NO_RETURN(entity, phase, provider)              \
      (provider)->ExecDebugRule("query", phase, entity, 0);

#define FRAGMENT_TRACE_POINT(phase, provider) \
  RETURN_IF_ERROR(                                    \
      (provider)->ExecDebugRule("fragment", phase, "fragment", 0));

#else

// Disable tracepoints in release builds.
#define PROCESS_TRACE_POINT(scope, entity, phase, id)
#define PROCESS_TRACE_POINT_NO_RETURN(scope, entity, phase, id)
#define RPC_TRACE_POINT(entity, phase, id, return_val)
#define QUERY_TRACE_POINT(entity, phase, provider)
#define QUERY_TRACE_POINT_NO_RETURN(entity, phase, provider)
#define FRAGMENT_TRACE_POINT(phase, provider)
#endif

}

#endif
