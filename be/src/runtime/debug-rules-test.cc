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

#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>

#include <algorithm>
#include "runtime/debug-rules.h"

#include "util/json-util.h"
#include "util/time.h"

using namespace rapidjson;
using namespace std;

DEFINE_string(debug_rule_testing_only, "off", "");

namespace impala {

namespace {

string replace_quotes(const string& json) {
  return boost::replace_all_copy(json, "'", "\"");
}

shared_ptr<DebugRule> ParseRule(const string& json_input, bool expect_success = true,
    const string& error = "") {
  string json = json_input;
  boost::algorithm::to_lower(json);
  Document d;
  d.Parse<0>(json.c_str());
  EXPECT_FALSE(d.HasParseError()) << "Parse error in JSON: " << json << endl << "Error: "
                                  << d.GetParseError();
  shared_ptr<DebugRule> rule;
  Status status = DebugRule::Create(&d, &rule);
  if (expect_success) {
    EXPECT_TRUE(status.ok()) << "JSON: " << json << endl
                             << "ERROR: " << status.GetDetail();
  } else {
    EXPECT_FALSE(status.ok());
    LOG(INFO) << "Error: " << status.GetDetail();
    if (!error.empty()) {
      EXPECT_TRUE(status.GetDetail().find(error) != string::npos)
          << "Could not find error: " << error;
    }
  }
  return rule;
}

shared_ptr<DebugRule> ParseOk(const string& json) {
  return ParseRule(replace_quotes(json), true);
}

void ParseFailure(const string& json, const string& error = "") {
  ParseRule(replace_quotes(json), false, error);
}

unique_ptr<DebugFunction> ParseArg(const string& json_input,
    ArgType expected, bool expect_success = true, const string& expected_error = "") {
  string json = replace_quotes(json_input);
  boost::algorithm::to_lower(json);
  Document d;
  d.Parse<0>(json.c_str());
  EXPECT_FALSE(d.HasParseError()) << "Parse error in JSON: " << json << endl << "Error: "
                                  << d.GetParseError();

  unique_ptr<DebugFunction> ret;
  Status status = DebugFunction::Create(expected, &d, &ret);
  if (expect_success) {
    EXPECT_TRUE(status.ok()) << "JSON: " << json << endl << "ERROR: " << status.GetDetail();
  } else {
    EXPECT_FALSE(status.ok());
    if (!expected_error.empty()) {
      EXPECT_TRUE(status.GetDetail().find(expected_error) != string::npos);
    }
  }
  return ret;
}

string RuleToString(DebugRule* rule) {
  Document d;
  rule->ToJson(&d, &d);
  string ret;
  EXPECT_TRUE(JsonToString(&d, &ret).ok());
  return ret;
}

}

TEST(DebugRule, RuleStructure) {
  // Tests to confirm that the parsing of rules works correctly.
  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'SCOPE': 'RPC', "
      " 'ACTION': {'fn_name': 'RETURN_ERROR'}}",
      "Missing member: 'name'");
  ParseFailure(
      "{'NAME': 'ExecRequest', "
      " 'SCOPE': 'RPC', "
      " 'ACTION': {'fn_name': 'RETURN_ERROR'}}",
      "Missing member: 'phase'");
  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'SCOPE': 'RPC', "
      " 'NAME': 'ExecRequest'}",
      "Missing member: 'action'");
  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest', "
      " 'ACTION': [{'fn_name': 'RETURN_ERROR'}]}",
      "Missing member: 'scope'");
  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': {'fn_name': 'RETURN_ERROR'}}",
      "'action' field must be an array");

  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': []}",
      "'action' field must have at least one element");

  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'RETURN_ERROR'}]}");
  EXPECT_EQ(rule->phase(), "start_response");
  EXPECT_EQ(rule->entity_name(), "execrequest");
  EXPECT_EQ(rule->scope(), debug::Scope::RPC);

  EXPECT_EQ(rule->actions().size(), 1);
  EXPECT_EQ(rule->actions()[0]->name(), "return_error");
}

TEST(DebugRule, MissingFunctions) {
  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'wait'}]}",
      "Missing member: 'duration'");
}

TEST(DebugRule, IncompatibleFunctionTypes) {
  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'wait', 'duration': 'string'}]}",
      "String literal used in non-string context");
}

TEST(DebugRule, TopLevelFnsMustReturnStatus) {
  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'rand', 'max': 100}]}",
      "RAND function used in non-numeric context");
}


TEST(DebugRule, FunctionArgs) {
  ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'wait', 'duration': "
      " {'fn_name': 'rand', 'max': 500}}]}");

  ParseFailure(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'wait', 'duration': "
      " {'fn_name': 'rand'}}]}",
      "Missing member: 'max'");
}

TEST(DebugRule, MultipleActions) {
  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'WAIT', 'duration': 10},"
      " {'fn_name': 'return_error'}]}");

  EXPECT_EQ(rule->actions().size(), 2);
  EXPECT_EQ(rule->actions()[0]->name(), "wait");
  EXPECT_EQ(rule->actions()[1]->name(), "return_error");

  // Rule should return !ok() only if second rule gets executed.
  EXPECT_TRUE(!rule->Exec(EvalContext("", 0)).ok());
}

TEST(DebugRule, MultipleActionsEarlyExit) {
  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'return_error'},"
      " {'fn_name': 'wait', 'duration': 10000}]}");

  EXPECT_EQ(rule->actions().size(), 2);
  EXPECT_EQ(rule->actions()[0]->name(), "return_error");
  EXPECT_EQ(rule->actions()[1]->name(), "wait");

  // TODO: Something less indirect when we write an action with more obvious side-effects.
  int64_t now = MonotonicMillis();
  EXPECT_TRUE(!rule->Exec(EvalContext("", 0)).ok());
  EXPECT_LT(MonotonicMillis(), now + 10000);
}

TEST(DebugRule, WaitAction) {
  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'WAIT', 'duration': 100}]}");

  EXPECT_EQ(rule->actions().size(), 1);
  DebugFunction* action = rule->actions()[0].get();
  EXPECT_EQ(action->name(), "wait");
  int64_t now = MonotonicMillis();
  EXPECT_TRUE(rule->Exec(EvalContext("", 0)).ok());
  EXPECT_GE(MonotonicMillis(), now + 100);
}

TEST(DebugRule, ReturnErrorAction) {
  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'return_error'}]}");

  EXPECT_EQ(rule->actions().size(), 1);
  DebugFunction* action = rule->actions()[0].get();
  EXPECT_EQ(action->name(), "return_error");
  EXPECT_FALSE(rule->Exec(EvalContext("", 0)).ok());
}

TEST(DebugRule, ContinueAction) {
  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 1.0}]}");

  EXPECT_EQ(rule->actions().size(), 1);
  DebugFunction* action = rule->actions()[0].get();
  EXPECT_EQ("cont", action->name());
  EXPECT_TRUE(rule->Exec(EvalContext("", 0)).ok());

  rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 0.0}]}");
  EXPECT_FALSE(rule->Exec(EvalContext("", 0)).ok());
}

TEST(DebugRule, SetFlagAction) {
  EXPECT_EQ(FLAGS_debug_rule_testing_only, "off");

  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'set_flag', 'flag': 'debug_rule_testing_only', 'value': 'on'}]}");
  EXPECT_EQ(rule->actions().size(), 1);
  DebugFunction* action = rule->actions()[0].get();
  EXPECT_EQ(action->name(), "set_flag");
  EXPECT_TRUE(rule->Exec(EvalContext("", 0)).ok());

  EXPECT_EQ(FLAGS_debug_rule_testing_only, "on");
}

TEST(DebugRule, ContextVars) {
  unique_ptr<DebugFunction> arg = ParseArg("{'fn_name': 'ctx_var', 'key': 'name'}",
      ArgType::STRING);
  EXPECT_EQ(arg->GetString(EvalContext("test_name", 10)), "test_name");

  arg = ParseArg("{'fn_name': 'ctx_var', 'key': 'id'}", ArgType::INT64);
  EXPECT_EQ(arg->GetInt(EvalContext("", 10)), 10);
}

TEST(DebugRule, Selectors) {
  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'selector': 1.0, "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 1.0}]}");
  EXPECT_TRUE(rule->EvalSelector("don't care", -1));

  rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'selector': 0.0, "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 1.0}]}");
  EXPECT_FALSE(rule->EvalSelector("don't care", -1));


  LOG(INFO) << "**** " << RuleToString(rule.get());
}

TEST(DebugRule, IdSelectors) {
  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'id_selector': {'fn_name': 'ctx_var', 'key': 'id_set'}, "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 1.0}]}");

  DebugIdSet id_set = {1, 2, 3, 4};
  EXPECT_EQ(rule->ComputeIdSet(id_set), id_set);

  rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'id_selector': {'fn_name': 'rand_subset', 'proportion': 0.5, "
      "                 'set': {'fn_name': 'ctx_var', 'key': 'id_set'}}, "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 1.0}]}");
  EXPECT_EQ(rule->ComputeIdSet(id_set).size(), 2);

  rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'id_selector': {'fn_name': 'rand_subset', 'proportion': 0.0, "
      "                 'set': {'fn_name': 'ctx_var', 'key': 'id_set'}}, "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 1.0}]}");
  EXPECT_EQ(rule->ComputeIdSet(id_set).size(), 0);
}

TEST(DebugRule, MissingIdSelector) {
  shared_ptr<DebugRule> rule = ParseOk(
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 1.0}]}");

  DebugIdSet id_set = {1, 2, 3, 4};
  EXPECT_EQ(rule->ComputeIdSet(id_set).size(), 4);
}

TEST(DebugRuleSet, ComputeIdSets) {
  Document document;
  string s("{'rules': ["
      "{'PHASE': 'START_RESPONSE', "
      " 'NAME': 'ExecRequest',"
      " 'SCOPE': 'RPC', "
      " 'id_selector': {'fn_name': 'ctx_var', 'key': 'id_set'}, "
      " 'ACTION': [{'fn_name': 'cont', 'probability': 1.0}]}]}");
  EXPECT_TRUE(ParseJsonFromString(
          replace_quotes(s),
          &document, true).ok());
  if (document.HasParseError()) {
    LOG(ERROR) << "****** " << document.GetParseError();
  }

  DebugRuleSet rule_set;
  EXPECT_TRUE(rule_set.AddRules(&document).ok());

  vector<pair<int64_t, string>> entities =
      {{1, "ExecRequest"}, {2, "CancelPlanFragment"}, {3, "*"}};
  rule_set.ComputeIdSets(entities);

  EXPECT_EQ(rule_set.GetIdSetForRule(debug::Scope::RPC, "start_response", "execrequest"),
      set<int64_t>({1, 3}));
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  //  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}
