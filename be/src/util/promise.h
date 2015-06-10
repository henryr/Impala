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

#ifndef IMPALA_UTIL_PROMISE_H
#define IMPALA_UTIL_PROMISE_H

#include <condition_variable>
#include <chrono>

#include "common/logging.h"
#include "runtime/timestamp-value.h"
#include "util/time.h"

#include "common/names.h"

namespace impala {

/// A stripped-down replacement for boost::promise which, to the best of our knowledge,
/// actually works. A single producer provides a single value by calling Set(..), which
/// one or more consumers retrieve through calling Get(..).  Consumers must be consistent
/// in their use of Get(), i.e., for a particular promise all consumers should either
/// have a timeout or not.
template <typename T>
class Promise {
 public:
  Promise() : val_is_set_(false) { }

  /// Copies val into this promise, and notifies any consumers blocked in Get().
  /// It is invalid to call Set() twice.
  void Set(const T& val) {
    std::unique_lock<std::mutex> l(val_lock_);
    DCHECK(!val_is_set_) << "Called Set(..) twice on the same Promise";
    val_ = val;
    val_is_set_ = true;

    /// Note: this must be called with 'val_lock_' taken. There are places where
    /// we use this object with this pattern:
    /// {
    ///   Promise p;
    ///   ...
    ///   p.get();
    /// }
    /// < promise object gets destroyed >
    /// Calling notify_all() with the val_lock_ guarantees that the thread calling
    /// Set() is done and the promise is safe to delete.
    val_set_cond_.notify_all();
  }

  /// Blocks until a value is set, and then returns a reference to that value. Once Get()
  /// returns, the returned value will not change, since Set(..) may not be called twice.
  const T& Get() {
    std::unique_lock<std::mutex> l(val_lock_);
    while (!val_is_set_) {
      val_set_cond_.wait(l);
    }
    return val_;
  }

  /// Blocks until a value is set or the given timeout was reached.
  /// Returns a reference to that value which is invalid if the timeout was reached.
  /// Once Get() returns and *timed_out is false, the returned value will not
  /// change, since Set(..) may not be called twice.
  /// timeout_millis: The max wall-clock time in milliseconds to wait (must be > 0).
  /// timed_out: Indicates whether Get() returned due to timeout. Must be non-NULL.
  const T& Get(int64_t timeout_millis, bool* timed_out) {
    DCHECK_GT(timeout_millis, 0);
    int64_t timeout_micros = timeout_millis * 1000;
    DCHECK(timed_out != NULL);
    std::unique_lock<std::mutex> l(val_lock_);
    int64_t start;
    int64_t now;
    now = start = MonotonicMicros();
    while (!val_is_set_ && (now - start) < timeout_micros) {
      std::chrono::microseconds wait_time =
          std::chrono::microseconds(std::max(1L, timeout_micros - (now - start)));
      val_set_cond_.wait_for (l, wait_time);
      now = MonotonicMicros();
    }
    *timed_out = !val_is_set_;
    return val_;
  }

  /// Returns whether the value is set.
  bool IsSet() {
    std::lock_guard<std::mutex> l(val_lock_);
    return val_is_set_;
  }

 private:
  /// These variables deal with coordination between consumer and producer, and protect
  /// access to val_;
  std::condition_variable val_set_cond_;
  bool val_is_set_;
  std::mutex val_lock_;

  /// The actual value transferred from producer to consumer
  T val_;
};

}

#endif
