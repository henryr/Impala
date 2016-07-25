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

#include <gutil/strings/substitute.h>

#include "util/json-util.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>


using namespace std;
using namespace rapidjson;
using namespace strings;

namespace impala {

Status ParseJsonFromString(const string& json, Document* document,
    bool force_lower_case) {
  if (force_lower_case) {
    document->Parse<0>(boost::algorithm::to_lower_copy(json).c_str());
  } else {
    document->Parse<0>(json.c_str());
  }
  if (!document->HasParseError()) return Status::OK();
  return Status(strings::Substitute(
          "JSON parsing failed. Error is: $0, document is: $1", document->GetParseError(),
          json));
}

Status JsonToString(Value* document, string* output) {
  if (!document->IsObject() && !document->IsArray()) {
    return Status("Can't print non-object or array types");
  }
  StringBuffer strbuf;
  PrettyWriter<StringBuffer> writer(strbuf);
  document->Accept(writer);
  *output = strbuf.GetString();
  return Status::OK();
}

}
