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

package org.apache.impala.planner;

import java.util.List;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TExplainLevel;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * A DataSink describes the destination of a plan fragment's output rows.
 * The destination could be another plan fragment on a remote machine,
 * or a table into which the rows are to be inserted
 * (i.e., the destination of the last fragment of an INSERT statement).
 */
public abstract class DataSink {

  // estimated per-host memory requirement for sink;
  // set in computeCosts(); invalid: -1
  protected long perHostMemCost_ = -1;

  // Fragment that this DataSink belongs to. Set by the PlanFragment enclosing this sink.
  protected PlanFragment fragment_;

  // Exprs evaluated by this sink before emitting its results. May be null.
  protected List<Expr> outputExprs_;

  /**
   * Return an explain string for the DataSink. Each line of the explain will be prefixed
   * by "prefix". The output string will be wrapped to 100 chars.
   *
   * TODO: Wrap every line in the explain plan. This is tricky to do without knowing the
   * prefix for every line. For now, wrap the data sinks because they can be especially
   * long.
   */
  public String getExplainString(String prefix, String detailPrefix,
      TExplainLevel explainLevel) {
    String explain = getExplainStringImpl(prefix, detailPrefix, explainLevel);
    return prefix + WordUtils.wrap(explain, 100, "\n" + detailPrefix, false);
  }

  protected abstract String getExplainStringImpl(String prefix, String detailPrefix,
      TExplainLevel explainLevel);

  protected abstract TDataSink toThrift();

  public void setFragment(PlanFragment fragment) { fragment_ = fragment; }
  public PlanFragment getFragment() { return fragment_; }
  public long getPerHostMemCost() { return perHostMemCost_; }

  public void setOutputExprs(List<Expr> outputExprs) {
    outputExprs_ = outputExprs;
  }
  public List<Expr> getOutputExprs() { return outputExprs_; }

  /**
   * Estimates the cost of executing this DataSink. Currently only sets perHostMemCost.
   */
  public void computeCosts() {
    perHostMemCost_ = 0;
  }

  /**
   * Helper method for adding output expressions to the explain string
   */
  protected String getOutputExprsString() {
    return String.format("OUTPUT-EXPRS=(%s)", Expr.toSql(outputExprs_));
  }
}
