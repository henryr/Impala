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

#include "runtime/debug-rules.h"

#include "runtime/client-cache.h"
#include "runtime/exec-env.h"
#include "util/json-util.h"
#include "util/time.h"
#include <mutex>

#include <boost/algorithm/string.hpp>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#include <gutil/strings/substitute.h>

using namespace std;
using namespace rapidjson;
using namespace strings;
using boost::algorithm::to_lower_copy;
using google::SetCommandLineOption;

namespace impala {
namespace debug {
const string Scope::RPC = "rpc";
}
}

namespace {

// JSON object keys
const char* PHASE_KEY = "phase";
const char* NAME_KEY = "name";
const char* ACTION_KEY = "action";
const char* SCOPE_KEY = "scope";
const char* RULES_KEY = "rules";
const char* FN_NAME_KEY = "fn_name";
const char* SELECTOR_KEY = "selector";
const char* ID_SELECTOR_KEY = "id_selector";

// These would be better written as static constexpr const members of their respective
// classes, but we hit an apparent compiler or linker bug which prevents them being
// referred to outside of their enclosing class.
constexpr const char* WAIT_NAME = "wait";
constexpr const char* CONT_NAME = "cont";
constexpr const char* RETURN_NAME = "return_error";
constexpr const char* LOG_NAME = "log";
constexpr const char* CTX_VAR_NAME = "ctx_var";
constexpr const char* RAND_NAME = "rand";
constexpr const char* NORM_RAND_NAME = "normrand";
constexpr const char* RAND_SUBSET_NAME = "rand_subset";
constexpr const char* FIXED_RAND_SUBSET_NAME = "fixed_rand_subset";
constexpr const char* SET_FLAG_NAME = "set_flag";
constexpr const char* SET_RPC_TIMEOUTS_NAME = "set_rpc_timeouts";

impala::Status CheckAndGetValue(const char* key, Value* json, Value** val) {
  if (!json->HasMember(key)) {
    return impala::Status(Substitute("Missing member: '$0'", key));
  }
  (*val) = &((*json)[key]);
  return impala::Status::OK();
}

}

namespace impala {

////////////////////////////////////////////////////////////////////////////
// Arguments
////////////////////////////////////////////////////////////////////////////

class IntArg : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    DCHECK(json->IsInt64());
    if (expected == STRING) return Status("Int literal used in string context");
    val_ = json->GetInt64();
    return Status::OK();
  }

  virtual void ToJson(Value* value, Document* document) {
    Value val(val_);
    *value = val;
  }

  virtual int64_t GetInt(const EvalContext& ctx) { return val_; }
  virtual double GetDouble(const EvalContext& ctx) { return val_; }
 private:
  int64_t val_;
};

class DoubleArg : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    DCHECK(json->IsDouble());
    if (expected == STRING) return Status("Double literal used in string context");
    val_ = json->GetDouble();
    return Status::OK();
  }

  virtual int64_t GetInt(const EvalContext& ctx) { return val_; }
  virtual double GetDouble(const EvalContext& ctx) { return val_; }

  virtual void ToJson(Value* value, Document* document) {
    Value temp(val_);
    *value = temp;
  }

 private:
  double val_;
};

class StringArg : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    DCHECK(json->IsString());
    if (expected != STRING) {
      return Status("String literal used in non-string context");
    }
    val_ = json->GetString();
    return Status::OK();
  }

  virtual string GetString(const EvalContext& ctx) { return val_; }

  virtual void ToJson(Value* value, Document* document) {
    Value temp(val_.c_str(), document->GetAllocator());
    *value = temp;
  }

 private:
  string val_;
};

class RandDebugFunction : public DebugFunction {
 public:
  virtual int64_t GetInt(const EvalContext& ctx) {
    return rand() % args_[0]->GetInt(ctx);
  }

  virtual double GetDouble(const EvalContext& ctx) { return GetInt(ctx); }

  virtual Status Init(Value* json, ArgType expected) {
    name_ = RAND_NAME;
    if (expected != INT64 && expected != DOUBLE) {
      return Status("RAND function used in non-numeric context");
    }
    arg_schema_ = {{"max", INT64}};
    RETURN_IF_ERROR(InitFromSchema(json));
    return Status::OK();
  }
};

class NormalizedRand : public DebugFunction {
 public:
  virtual int64_t GetInt(const EvalContext& ctx) {
    return GetDouble(ctx);
  }

  virtual double GetDouble(const EvalContext& ctx) {
    return rand() / static_cast<double>(RAND_MAX);
  }

  virtual Status Init(Value* json, ArgType expected) {
    name_ = NORM_RAND_NAME;
    if (expected != INT64 && expected != DOUBLE) {
      return Status("NormRand() function used in non-numeric context");
    }
    RETURN_IF_ERROR(InitFromSchema(json));
    return Status::OK();
  }
};

class ContextVarArg : public DebugFunction {
 public:
  virtual string GetString(const EvalContext& ctx) {
    string key = args_[0]->GetString(ctx);
    if (key == "name") return ctx.name();
    return "";
  }

  virtual int64_t GetInt(const EvalContext& ctx) {
    string key = args_[0]->GetString(ctx);
    if (key == "id") return ctx.id();
    return 0;
  }

  virtual Status Init(Value* json, ArgType expected) {
    name_ = CTX_VAR_NAME;
    arg_schema_ = {{"key", STRING}};
    RETURN_IF_ERROR(InitFromSchema(json));
    return Status::OK();
  }

  virtual const DebugIdSet GetIdSet(const EvalContext& ctx) {
    return ctx.id_set();
  }
};

class RandSubsetFunction : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    name_ = RAND_SUBSET_NAME;
    arg_schema_ = {{"proportion", DOUBLE}, {"set", ID_SET}};
    return InitFromSchema(json);
  }

  virtual const DebugIdSet GetIdSet(const EvalContext& ctx) {
    DebugIdSet input = args_[1]->GetIdSet(ctx);
    int target_size = input.size() * args_[0]->GetDouble(ctx);
    if (target_size > input.size()) target_size = input.size();
    if (target_size < 0) target_size = 0;
    vector<int64_t> ids(input.begin(), input.end());
    random_shuffle(ids.begin(), ids.end());
    return DebugIdSet(ids.begin(), ids.begin() + target_size);
  }
};

class FixedSizeRandSubset : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    name_ = FIXED_RAND_SUBSET_NAME;
    arg_schema_ = {{"size", INT64}, {"set", ID_SET}};
    return InitFromSchema(json);
  }

  virtual const DebugIdSet GetIdSet(const EvalContext& ctx) {
    DebugIdSet input = args_[1]->GetIdSet(ctx);
    int target_size = min(static_cast<int64_t>(input.size()), args_[0]->GetInt(ctx));
    if (target_size < 0) target_size = 0;
    vector<int64_t> ids(input.begin(), input.end());
    random_shuffle(ids.begin(), ids.end());
    return DebugIdSet(ids.begin(), ids.begin() + target_size);
  }
};

////////////////////////////////////////////////////////////////////////////
// Actions
////////////////////////////////////////////////////////////////////////////

Status DebugFunction::InitFromSchema(Value* json) {
  DCHECK(!name_.empty()) << "name_ must be set in InitFromSchema()";
  for (const auto& arg_def: arg_schema_) {
    Value* arg_json;
    RETURN_IF_ERROR(CheckAndGetValue(arg_def.first.c_str(), json, &arg_json));
    unique_ptr<DebugFunction> arg;
    RETURN_IF_ERROR(DebugFunction::Create(arg_def.second, arg_json, &arg));
    args_.push_back(move(arg));
  }
  return Status::OK();
}

void DebugFunction::ToJson(Value* value, Document* document) {
  value->AddMember(FN_NAME_KEY, name_.c_str(), document->GetAllocator());
  int idx = 0;
  for (const auto& arg: arg_schema_) {
    Value val(kObjectType);
    args_[idx]->ToJson(&val, document);
    ++idx;
    value->AddMember(arg.first.c_str(), val, document->GetAllocator());
  }
}

class WaitAction : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    if (expected != STATUS) return Status("Wrong expected type for action");
    name_ = WAIT_NAME;
    arg_schema_ = {{"duration", INT64}};
    return InitFromSchema(json);
  }

  virtual Status GetStatus(const EvalContext& ctx) {
    SleepForMs(args_[0]->GetInt(ctx));
    return Status::OK();
  }
};

class ReturnErrorAction : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    if (expected != STATUS) return Status("Wrong expected type for action");
    name_ = RETURN_NAME;
    return Status::OK();
  }

  virtual Status GetStatus(const EvalContext& ctx) {
    return Status("Error return by debug rule");
  }
};

class ContinueAction : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    if (expected != STATUS) return Status("Wrong expected type for action");
    name_ = CONT_NAME;
    arg_schema_ = {{"probability", DOUBLE}};
    return InitFromSchema(json);
  }

  virtual Status GetStatus(const EvalContext& ctx) {
    return ((rand() % 100) / 100.0) <= args_[0]->GetDouble(ctx) ?
        Status::OK() : Status("Debug action CONT");
  }
};

class LogAction : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    if (expected != STATUS) return Status("Wrong expected type for action");
    name_ = LOG_NAME;
    arg_schema_ = {{"msg", STRING}};
    return InitFromSchema(json);
  }

  virtual Status GetStatus(const EvalContext& ctx) {
    LOG(INFO) << "Log Debug Action: " << args_[0]->GetString(ctx);
    return Status::OK();
  }
};

class SetFlagAction : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    if (expected != STATUS) return Status("Wrong expected type for action");
    name_ = SET_FLAG_NAME;
    arg_schema_ = {{"flag", STRING}, {"value", STRING}};
    return InitFromSchema(json);
  }

  virtual Status GetStatus(const EvalContext& ctx) {
    const string& flag_name = args_[0]->GetString(ctx);
    const string& value = args_[1]->GetString(ctx);
    const string& result = SetCommandLineOption(flag_name.c_str(), value.c_str());
    if (result.empty()) {
      return Status(Substitute("Failed to set flag: $0 to $1", flag_name, value));
    }
    return Status::OK();
  }
};

//////
// TODO: Add a RegisterFunction() call with a singleton fn registry

class SetRpcTimeoutsAction : public DebugFunction {
 public:
  virtual Status Init(Value* json, ArgType expected) {
    if (expected != STATUS) return Status("Wrong expected type for action");
    name_ = SET_RPC_TIMEOUTS_NAME;
    arg_schema_ = {{"send", INT64}, {"recv", INT64}};
    return InitFromSchema(json);
  }

  virtual Status GetStatus(const EvalContext& ctx) {
    ExecEnv* exec_env = ExecEnv::GetInstance();
    if (exec_env == nullptr) return Status("No exec env");
    exec_env->impalad_client_cache()->set_send_timeout(args_[0]->GetInt(ctx));
    exec_env->impalad_client_cache()->set_recv_timeout(args_[1]->GetInt(ctx));
    exec_env->impalad_client_cache()->TestShutdown();
    return Status::OK();
  }
};

Status DebugFunction::Create(ArgType expected, Value* arg,
    unique_ptr<DebugFunction>* output) {

  using FunctorMap = unordered_map<string, function<DebugFunction*()>>;
  static const FunctorMap STRING_TO_FUNC = {
    {RAND_SUBSET_NAME, []() { return new RandSubsetFunction(); }},
    {FIXED_RAND_SUBSET_NAME, []() { return new FixedSizeRandSubset(); }},
    {RAND_NAME, []() { return new RandDebugFunction(); }},
    {NORM_RAND_NAME, []() { return new NormalizedRand(); }},
    {CTX_VAR_NAME, []() { return new ContextVarArg(); }},
    {WAIT_NAME, []() { return new WaitAction(); }},
    {RETURN_NAME, []() { return new ReturnErrorAction(); }},
    {LOG_NAME, []() { return new LogAction(); }},
    {CONT_NAME, []() { return new ContinueAction(); }},
    {SET_FLAG_NAME, []() { return new SetFlagAction(); }},
    {SET_RPC_TIMEOUTS_NAME, []() { return new SetRpcTimeoutsAction(); }},
  };

  switch (arg->GetType()) {
    case kNumberType:
      if (arg->IsDouble()) {
        output->reset(new DoubleArg());
      } else {
        output->reset(new IntArg());
      }
      break;
    case kStringType:
      output->reset(new StringArg());
      break;
    case kObjectType: {
      Value* name;
      RETURN_IF_ERROR(CheckAndGetValue(FN_NAME_KEY, arg, &name));
      if (!name->IsString()) {
        return Status(Substitute("'$0' field must be a string", FN_NAME_KEY));
      }

      FunctorMap::const_iterator it = STRING_TO_FUNC.find(name->GetString());
      if (it == STRING_TO_FUNC.end()) {
        return Status(Substitute("Unknown function: $0", name->GetString()));
      }
      output->reset(it->second());
      break;
    }
    default:
      return Status(Substitute("Unsupported argument type: $0", arg->GetType()));
  }
  return (*output)->Init(arg, expected);
}


void DebugRuleSet::ClearRules() {
  lock_guard<SpinLock> l(lock_);
  rules_.clear();
}

namespace {

static string MakeKey(const string& scope, const string& phase,
    const string& entity_name) {
  return to_lower_copy(
      Substitute("scope: $0 | entity: $1 | phase: $2", scope, entity_name, phase));
}

template <typename T>
Status LookupByString(const string& s, const unordered_map<string, T>& vals,
    Value* json, T* val) {
  const string& key = json->GetString();
  typename unordered_map<string, T>::const_iterator it = vals.find(key);
  if (it == vals.end()) {
    return Status("Could not find X");
  }
  *val = it->second;
  return Status::OK();
}

}

Status DebugRule::Create(Value* json, shared_ptr<DebugRule>* rule) {
  string jstr;
  RETURN_IF_ERROR(JsonToString(json, &jstr));
  LOG(INFO) << "Create() -> " <<  jstr;
  if (!json->IsObject()) return Status("JSON must be object");
  Value* phase_val;
  RETURN_IF_ERROR(CheckAndGetValue(PHASE_KEY, json, &phase_val));
  if (!phase_val->IsString()) {
    return Status(Substitute("'$0' field must be a string", PHASE_KEY));
  }
  string phase = phase_val->GetString();

  Value* entity_name_val;
  RETURN_IF_ERROR(CheckAndGetValue(NAME_KEY, json, &entity_name_val));
  if (!entity_name_val->IsString()) {
    return Status(Substitute("'$0' field must be a string", NAME_KEY));
  }
  string entity_name = entity_name_val->GetString();

  Value* actions_val;
  RETURN_IF_ERROR(CheckAndGetValue(ACTION_KEY, json, &actions_val));
  if (!actions_val->IsArray()) {
    return Status(Substitute("'$0' field must be an array", ACTION_KEY));
  }
  if (actions_val->Size() == 0) {
    return Status(Substitute("'$0' field must have at least one element", ACTION_KEY));
  }
  vector<unique_ptr<DebugFunction>> actions;
  for (int i = 0; i < actions_val->Size(); ++i) {
    unique_ptr<DebugFunction> act;
    RETURN_IF_ERROR(DebugFunction::Create(ArgType::STATUS, &(*actions_val)[i], &act));
    actions.push_back(move(act));
  }

  Value* scope_val;
  RETURN_IF_ERROR(CheckAndGetValue(SCOPE_KEY, json, &scope_val));
  if (!scope_val->IsString()) {
    RETURN_IF_ERROR(Status(Substitute("'$0' field must be a string", SCOPE_KEY)));
  }
  string scope = scope_val->GetString();

  unique_ptr<DebugFunction> selector;
  if (json->HasMember(SELECTOR_KEY)) {
    RETURN_IF_ERROR(
        DebugFunction::Create(ArgType::INT64, &(*json)[SELECTOR_KEY], &selector));
  }

  unique_ptr<DebugFunction> id_selector;
  if (json->HasMember(ID_SELECTOR_KEY)) {
    RETURN_IF_ERROR(
        DebugFunction::Create(ArgType::INT64, &(*json)[ID_SELECTOR_KEY], &id_selector));
  }

  rule->reset(new DebugRule(
      scope, phase, move(actions), entity_name, move(selector), move(id_selector)));

  return Status::OK();
}

Status DebugRuleSet::AddOneRule(Value* json) {
  shared_ptr<DebugRule> rule;
  RETURN_IF_ERROR(DebugRule::Create(json, &rule));
  lock_guard<SpinLock> l(lock_);
  rules_[MakeKey(rule->scope(), rule->phase(), rule->entity_name())] = rule;
  return Status::OK();
}

Status DebugRuleSet::AddRules(Value* json) {
  Value* rules_array;
  RETURN_IF_ERROR(CheckAndGetValue(RULES_KEY, json, &rules_array));
  if (!rules_array->IsArray()) return Status("'rules' must be an array");
  for (int i = 0; i < rules_array->Size(); ++i) {
    RETURN_IF_ERROR(AddOneRule(&(*rules_array)[i]));
  }
  return Status::OK();
}

Status DebugRuleSet::ExecDebugRule(const string& scope, const string& phase,
    const string& entity_name, int64_t id) {
  shared_ptr<DebugRule> rule;
  {
    lock_guard<SpinLock> l(lock_);
    unordered_map<string, shared_ptr<DebugRule>>::iterator it =
        rules_.find(MakeKey(scope, phase, entity_name));
    if (it == rules_.end()) return Status::OK();
    rule = it->second;
  }

  if (rule->EvalSelector(entity_name, id)) {
    LOG(INFO) << "DebugRuleSet -> Hit trace point " << MakeKey(scope, phase, entity_name);
    // Don't hold lock during GetStatus()
    return rule->Exec(EvalContext(entity_name, id));
  }

  return Status::OK();
}

void DebugRuleSet::ToJson(Document* document, Value* val) {
  Value rule_arr(kArrayType);
  lock_guard<SpinLock> l(lock_);

  for (const auto& rule: rules_) {
    Value rule_val(kObjectType);
    rule.second->ToJson(document, &rule_val);
    rule_arr.PushBack(rule_val, document->GetAllocator());
  }

  document->AddMember(RULES_KEY, rule_arr, document->GetAllocator());
}

int64_t DebugRuleSet::GetJsonRulesForId(int64_t id, Document* document) {
  Value rule_arr(kArrayType);
  lock_guard<SpinLock> l(lock_);

  for (const auto& rule: rules_) {
    auto entry = id_sets_.find(rule.first);
    LOG(INFO) << "**** FOUND? " << rule.first << " : " << (entry != id_sets_.end());
    LOG(INFO) << "**** HAVE ID: " << id << " -> " << (entry->second.find(id) != entry->second.end());
    if (entry != id_sets_.end() && entry->second.find(id) == entry->second.end()) {
      continue;
    }
    Value rule_val(kObjectType);
    rule.second->ToJson(document, &rule_val);
    rule_arr.PushBack(rule_val, document->GetAllocator());
  }

  int num_rules = rule_arr.Size();

  if (num_rules > 0) {
    document->AddMember(RULES_KEY, rule_arr, document->GetAllocator());
  }

  return num_rules;
}

void DebugRule::ToJson(Document* document, Value* val) const {
  Value container(kObjectType);
  Value phase_val(phase_.c_str(), document->GetAllocator());
  container.AddMember(PHASE_KEY, phase_val, document->GetAllocator());

  Value name_val(name_.c_str(), document->GetAllocator());
  container.AddMember(NAME_KEY, name_val, document->GetAllocator());

  Value scope_val(scope_.c_str(), document->GetAllocator());
  container.AddMember(SCOPE_KEY, scope_val, document->GetAllocator());

  Value actions_arr(kArrayType);
  for (auto& a: actions()) {
    Value act_val(kObjectType);
    a->ToJson(&act_val, document);
    actions_arr.PushBack(act_val, document->GetAllocator());
  }
  container.AddMember(ACTION_KEY, actions_arr, document->GetAllocator());

  *val = container;
}

Status DebugRule::Exec(const EvalContext& ctx) {
  for (auto& action: actions()) RETURN_IF_ERROR(action->GetStatus(ctx));
  return Status::OK();
}

bool DebugRule::EvalSelector(const string& name, int64_t instance_id) {
  if (selector_.get() == nullptr) return true;
  return selector_->GetInt(EvalContext(name, instance_id)) > 0;
}

DebugIdSet DebugRule::ComputeIdSet(const DebugIdSet& input) {
  if (id_selector_.get() == nullptr) return input;
  EvalContext ctx("", 0);
  ctx.SetIdSet(input);
  return id_selector_->GetIdSet(ctx);
}

Status DebugRuleSet::ComputeIdSets(const vector<pair<int64_t, string>>& entities) {
  lock_guard<SpinLock> l(lock_);
  for (auto& rule: rules_) {
    set<int64_t> candidate_ids;
    LOG(INFO) << "RULE NAME: " << rule.second->entity_name();
    for (const auto& entity: entities) {
      LOG(INFO) << "POSSIBLE ID: " << entity.second << Substitute("($0)", entity.first);
      if (entity.second == "*" ||
          rule.second->entity_name() == to_lower_copy(entity.second)) {
        candidate_ids.insert(entity.first);
      }
    }
    LOG(INFO) << "***** GOT CANDIDATE IDS: ";
    for (int64_t id: candidate_ids) LOG(INFO) << "ID: " << id;
    id_sets_[rule.first] = rule.second->ComputeIdSet(candidate_ids);
    LOG(INFO) << "Rule: " << rule.first << " has " << id_sets_[rule.first].size() << " IDs";
    for (auto id: id_sets_[rule.first]) LOG(INFO) << "ID: " << id;
  }
  return Status::OK();
}

DebugIdSet DebugRuleSet::GetIdSetForRule(const string& scope, const string& phase,
    const string& entity_name) {
  lock_guard<SpinLock> l(lock_);
  string key = MakeKey(scope, phase, entity_name);
  LOG(INFO) << "**** LOOKING FOR KEY: " << key;
  const auto it = id_sets_.find(key);
  if (it == id_sets_.end()) return DebugIdSet();
  LOG(INFO) << "**** FOUND KEY";
  return it->second;
}

}
