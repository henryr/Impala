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

package com.cloudera.impala.planner;

import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TPushPullSink;
import com.cloudera.impala.thrift.TExplainLevel;

/**
 * A sink which acts as a buffer for results produced by a fragment instance, so that a
 * consumer may pull them from the buffer asynchronously.
 */
public class PushPullSink extends DataSink {

  public String getExplainString(String prefix, String detailPrefix,
      TExplainLevel explainLevel) {
    return String.format("%sPUSH-PULL SINK\n", prefix);
  }

  protected TDataSink toThrift() {
    TDataSink result = new TDataSink(TDataSinkType.PUSH_PULL_SINK);
    result.setPush_pull_sink(new TPushPullSink());
    return result;
  }
}
