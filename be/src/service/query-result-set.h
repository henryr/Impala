// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef IMPALA_SERVICE_QUERY_RESULT_SET_H
#define IMPALA_SERVICE_QUERY_RESULT_SET_H

#include "common/status.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Results_types.h"

#include <boost/scoped_ptr.hpp>
#include <vector>

namespace impala {

/// Stores client-ready query result rows returned by
/// QueryExecState::FetchRows(). Subclasses implement AddRows() / AddOneRow() to
/// specialise how Impala's row batches are converted to client-API result
/// representations.
class QueryResultSet {
 public:
  QueryResultSet() {}
  virtual ~QueryResultSet() {}

  /// Add a single row to this result set. The row is a vector of pointers to values,
  /// whose memory belongs to the caller. 'scales' contains the scales for decimal values
  /// (# of digits after decimal), with -1 indicating no scale specified or the
  /// corresponding value is not a decimal.
  virtual Status AddOneRow(
      const std::vector<void*>& row, const std::vector<int>& scales) = 0;

  /// Add the TResultRow to this result set. When a row comes from a DDL/metadata
  /// operation, the row in the form of TResultRow.
  virtual Status AddOneRow(const TResultRow& row) = 0;

  /// Copies rows in the range [start_idx, start_idx + num_rows) from the other result
  /// set into this result set. Returns the number of rows added to this result set.
  /// Returns 0 if the given range is out of bounds of the other result set.
  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) = 0;

  /// Returns the approximate size of this result set in bytes.
  int64_t ByteSize() { return ByteSize(0, size()); }

  /// Returns the approximate size of the given range of rows in bytes.
  virtual int64_t ByteSize(int start_idx, int num_rows) = 0;

  /// Returns the size of this result set in number of rows.
  virtual size_t size() = 0;
};

/// Ascii result set for Beeswax.
/// Beeswax returns rows in ascii, using "\t" as column delimiter.
class AsciiQueryResultSet : public QueryResultSet {
 public:
  // Rows are added into rowset.
  AsciiQueryResultSet(const TResultSetMetadata& metadata, vector<string>* rowset)
    : metadata_(metadata), result_set_(rowset), owned_result_set_(NULL) {}

  // Rows are added into a new rowset that is owned by this result set.
  AsciiQueryResultSet(const TResultSetMetadata& metadata)
    : metadata_(metadata),
      result_set_(new vector<string>()),
      owned_result_set_(result_set_) {}

  virtual ~AsciiQueryResultSet() {}

  // Convert expr values (col_values) to ASCII using "\t" as column delimiter and store
  // it in this result set.
  // TODO: Handle complex types.
  virtual Status AddOneRow(
      const std::vector<void*>& col_values, const std::vector<int>& scales);

  // Convert TResultRow to ASCII using "\t" as column delimiter and store it in this
  // result set.
  virtual Status AddOneRow(const TResultRow& row);

  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows);

  virtual int64_t ByteSize(int start_idx, int num_rows);

  virtual size_t size() { return result_set_->size(); }

 private:
  // Metadata of the result set
  const TResultSetMetadata& metadata_;

  // Points to the result set to be filled. The result set this points to may be owned by
  // this object, in which case owned_result_set_ is set.
  std::vector<string>* result_set_;

  // Set to result_set_ if result_set_ is owned.
  boost::scoped_ptr<std::vector<string>> owned_result_set_;
};

// Result set container for Hive protocol versions >= V6, where results are returned in
// column-orientation.
class HS2ColumnarResultSet : public QueryResultSet {
 public:
  HS2ColumnarResultSet(const TResultSetMetadata& metadata, TRowSet* rowset = NULL)
    : metadata_(metadata), result_set_(rowset), num_rows_(0) {
    if (rowset == NULL) {
      owned_result_set_.reset(new TRowSet());
      result_set_ = owned_result_set_.get();
    }
    InitColumns();
  }

  virtual ~HS2ColumnarResultSet() {}

  // Add a row of expr values
  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales);
  // Add a row from a TResultRow
  virtual Status AddOneRow(const TResultRow& row);
  // Copy all columns starting at 'start_idx' and proceeding for a maximum of 'num_rows'
  // from 'other' into this result set
  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows);

  virtual int64_t ByteSize(int start_idx, int num_rows);

  virtual size_t size() { return num_rows_; }

 private:
  // Metadata of the result set
  const TResultSetMetadata& metadata_;

  // Points to the TRowSet to be filled. The row set this points to may be owned by
  // this object, in which case owned_result_set_ is set.
  TRowSet* result_set_;

  // Set to result_set_ if result_set_ is owned.
  scoped_ptr<TRowSet> owned_result_set_;

  int64_t num_rows_;

  void InitColumns();
};

// TRow result set for HiveServer2
class HS2RowOrientedResultSet : public QueryResultSet {
 public:
  // Rows are added into rowset.
  HS2RowOrientedResultSet(const TResultSetMetadata& metadata, TRowSet* rowset = NULL)
    : metadata_(metadata), result_set_(rowset) {
    if (rowset == NULL) {
      owned_result_set_.reset(new TRowSet());
      result_set_ = owned_result_set_.get();
    }
  }

  virtual ~HS2RowOrientedResultSet() {}

  // Convert expr value to HS2 TRow and store it in TRowSet.
  virtual Status AddOneRow(const vector<void*>& col_values, const vector<int>& scales);
  // Convert TResultRow to HS2 TRow and store it in TRowSet.
  virtual Status AddOneRow(const TResultRow& row);
  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows);
  virtual int64_t ByteSize(int start_idx, int num_rows);
  virtual size_t size() { return result_set_->rows.size(); }

 private:
  // Metadata of the result set
  const TResultSetMetadata& metadata_;

  // Points to the TRowSet to be filled. The row set this points to may be owned by
  // this object, in which case owned_result_set_ is set.
  TRowSet* result_set_;

  // Set to result_set_ if result_set_ is owned.
  scoped_ptr<TRowSet> owned_result_set_;
};
}

#endif
