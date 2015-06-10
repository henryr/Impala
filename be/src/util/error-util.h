// Copyright 2012 Cloudera Inc.
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


#ifndef IMPALA_UTIL_ERROR_UTIL_H
#define IMPALA_UTIL_ERROR_UTIL_H


#include <string>
#include <vector>
#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>

#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/ErrorCodes_constants.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gutil/strings/substitute.h"

namespace impala {

/// Returns the error message for errno. We should not use strerror directly
/// as that is not thread safe.
/// Returns empty string if errno is 0.
std::string GetStrErrMsg();

/// Returns an error message warning that the given table names are missing relevant
/// table/and or column statistics.
std::string GetTablesMissingStatsWarning(
    const std::vector<TTableName>& tables_missing_stats);


/// Class that holds a formatted error message and potentially a set of detail
/// messages. Error messages are intended to be user facing. Error details can be attached
/// as strings to the message. These details should only be accessed internally.
class ErrorMsg {
 public:
  typedef strings::internal::SubstituteArg ArgType;

  /// Trivial constructor
  ErrorMsg() : error_(TErrorCode::OK) {}

  /// Below are a set of overloaded constructors taking all possible number of arguments
  /// that can be passed to Substitute. The reason is to try to avoid forcing the compiler
  /// putting all arguments for Substitute() on the stack whenver this is called and thus
  /// polute the instruction cache.
  ErrorMsg(TErrorCode::type error);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
      const ArgType& arg8);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
      const ArgType& arg8, const ArgType& arg9);

  ErrorMsg(TErrorCode::type error, const std::vector<string>& detail)
      : error_(error), details_(detail) {}

  /// Static initializer that is needed to avoid issues with static initialization order
  /// and the point in time when the string list generated via thrift becomes
  /// available. This method should not be used if no static initialization is needed as
  /// the cost of this method is proportional to the number of entries in the global error
  /// message list.
  /// WARNING: DO NOT CALL THIS METHOD IN A NON STATIC CONTEXT
  static ErrorMsg Init(TErrorCode::type error, const ArgType& arg0 = ArgType::NoArg,
      const ArgType& arg1 = ArgType::NoArg,
      const ArgType& arg2 = ArgType::NoArg,
      const ArgType& arg3 = ArgType::NoArg,
      const ArgType& arg4 = ArgType::NoArg,
      const ArgType& arg5 = ArgType::NoArg,
      const ArgType& arg6 = ArgType::NoArg,
      const ArgType& arg7 = ArgType::NoArg,
      const ArgType& arg8 = ArgType::NoArg,
      const ArgType& arg9 = ArgType::NoArg);

  TErrorCode::type error() const { return error_; }

  /// Add detail string message
  void AddDetail(const std::string& d) {
    details_.push_back(d);
  }

  /// Set a specific error code
  void SetError(TErrorCode::type e) {
    error_ = e;
  }

  /// Returns the formatted error string
  const std::string& msg() const {
    return message_;
  }

  const std::vector<std::string>& details() const {
    return details_;
  }

  /// Produce a string representation of the error message that includes the formatted
  /// message of the original error and the attached detail strings.
  std::string GetFullMessageDetails() const {
    std::stringstream ss;
    ss << message_ << "\n";
    for (size_t i=0, end=details_.size(); i < end; ++i) {
      ss << details_[i] << "\n";
    }
    return ss.str();
  }

private:
  TErrorCode::type error_;
  std::string message_;
  std::vector<std::string> details_;
};

/// Tracks log messages per error code
typedef std::map<TErrorCode::type, TErrorLogEntry> ErrorLogMap;

/// Merge error maps. Merging of error maps occurs, when the errors from multiple backends
/// are merged into a single error map.  General log messages are simply appended, specific
/// errors are deduplicated by either appending a new instance or incrementing the count of
/// an existing one.
void MergeErrorMaps(ErrorLogMap* left, const ErrorLogMap& right);

/// Append an error to the error map. Performs the aggregation as follows: GENERAL errors
/// are appended to the list of GENERAL errors, to keep one item each in the map, while for
/// all other error codes only the count is incremented and only the first message is kept
/// as a sample.
void AppendError(ErrorLogMap* map, const ErrorMsg& e);

/// Helper method to print the contents of an ErrorMap to a stream
void PrintErrorMap(std::ostream* stream, const ErrorLogMap& errors);

/// Returns the number of errors within this error maps. General errors are counted
/// individually, while specific errors are counted once per distinct occurrence.
size_t ErrorCount(const ErrorLogMap& errors);

/// Generates a string representation of the error map. Produces the same output as
/// PrintErrorMap, but returns a string instead of using a stream.
std::string PrintErrorMapToString(const ErrorLogMap& errors);

}

#endif
