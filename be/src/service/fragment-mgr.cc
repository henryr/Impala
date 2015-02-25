// Copyright 2014 Cloudera Inc.
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

#include "service/fragment-mgr.h"

#include <boost/lexical_cast.hpp>
#include <google/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "service/fragment-exec-state.h"
#include "runtime/exec-env.h"
#include "util/impalad-metrics.h"
#include "util/uid-util.h"

#include "common/names.h"

using namespace impala;
using namespace strings;

// TODO: this logging should go into a per query log.
DEFINE_int32(log_mem_usage_interval, 0, "If non-zero, impalad will output memory usage "
    "every log_mem_usage_interval'th fragment completion.");

Status FragmentMgr::ExecPlanFragment(const TExecPlanFragmentParams& exec_params) {
  VLOG_QUERY << "ExecPlanFragment() instance_id="
             << exec_params.fragment_instance_ctx.fragment_instance_id
             << " coord=" << exec_params.fragment_instance_ctx.query_ctx.coord_address
             << " backend#=" << exec_params.fragment_instance_ctx.backend_num;

  if (!exec_params.fragment.__isset.output_sink) {
    return Status("missing sink in plan fragment");
  }

  shared_ptr<FragmentExecState> exec_state(
      new FragmentExecState(exec_params.fragment_instance_ctx, ExecEnv::GetInstance()));

  // execute plan fragment in new thread
  // TODO: manage threads via global thread pool
  exec_state->set_exec_thread(new Thread("impala-server", "exec-plan-fragment",
      &FragmentMgr::FragmentThread, this, exec_params, exec_state));

  return Status::OK;
}

void FragmentMgr::FragmentThread(const TExecPlanFragmentParams& params,
    shared_ptr<FragmentExecState> exec_state) {
  // Call Prepare() now, before registering the exec state, to avoid calling
  // exec_state->Cancel().
  // We might get an async cancellation, and the executor requires that Cancel() not
  // be called before Prepare() returns.
  Status status = exec_state->Prepare(params);

  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    // register exec_state before starting exec thread
    fragment_exec_state_map_.insert(
        make_pair(params.fragment_instance_ctx.fragment_instance_id, exec_state));
  }

  if (!status.ok()) return;

  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->Increment(1L);
  exec_state->Exec();
  // we're done with this plan fragment

  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    FragmentExecStateMap::iterator i =
        fragment_exec_state_map_.find(exec_state->fragment_instance_id());
    if (i != fragment_exec_state_map_.end()) {
      fragment_exec_state_map_.erase(i);
    } else {
      LOG(ERROR) << "missing entry in fragment exec state map: instance_id="
                 << exec_state->fragment_instance_id();
    }
  }
#ifndef ADDRESS_SANITIZER
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_log_mem_usage_interval > 0) {
    uint64_t num_complete = ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->value();
    if (num_complete % FLAGS_log_mem_usage_interval == 0) {
      char buf[2048];
      // This outputs how much memory is currently being used by this impalad
      MallocExtension::instance()->GetStats(buf, 2048);
      LOG(INFO) << buf;
    }
  }
#endif
}

shared_ptr<FragmentMgr::FragmentExecState> FragmentMgr::GetFragmentExecState(
    const TUniqueId& fragment_instance_id) {
  lock_guard<mutex> l(fragment_exec_state_map_lock_);
  FragmentExecStateMap::iterator i = fragment_exec_state_map_.find(fragment_instance_id);
  if (i == fragment_exec_state_map_.end()) {
    return shared_ptr<FragmentExecState>();
  } else {
    return i->second;
  }
}

void FragmentMgr::CancelPlanFragment(TCancelPlanFragmentResult& return_val,
    const TCancelPlanFragmentParams& params) {
  VLOG_QUERY << "CancelPlanFragment(): instance_id=" << params.fragment_instance_id;
  shared_ptr<FragmentExecState> exec_state =
      GetFragmentExecState(params.fragment_instance_id);
  if (exec_state.get() == NULL) {
    Status status(ErrorMsg(TErrorCode::INTERNAL_ERROR, Substitute("Unknown fragment id: $0",
        lexical_cast<string>(params.fragment_instance_id))));
    status.SetTStatus(&return_val);
    return;
  }
  // we only initiate cancellation here, the map entry as well as the exec state
  // are removed when fragment execution terminates (which is at present still
  // running in exec_state->exec_thread_)
  exec_state->Cancel().SetTStatus(&return_val);
}
