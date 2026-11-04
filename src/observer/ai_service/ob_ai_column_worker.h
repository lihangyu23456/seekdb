/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_OBSERVER_AI_SERVICE_OB_AI_COLUMN_WORKER_H_
#define OCEANBASE_OBSERVER_AI_SERVICE_OB_AI_COLUMN_WORKER_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace observer
{

// Background timer task that drains the oceanbase.__all_ai_column_task queue.
// It groups eligible tasks by (table_id, request_id), evaluates the prompt of
// every AI column in the group against its dependency row, calls the configured
// AI model, and writes all results back (and removes the task rows) in a single
// transaction so group members can never be observed partially.
class ObAIColumnWorkerTask : public common::ObTimerTask
{
public:
  ObAIColumnWorkerTask();
  ~ObAIColumnWorkerTask();
  int init();
  void destroy();
  void pause();
  void resume();
  int schedule(common::ObTimer &timer);
  virtual void runTimerTask() override;
  void do_work();

  static const int64_t SCHEDULE_PERIOD_US = 500 * 1000L; // 0.5s
private:
  int process_pending_tasks();

  bool is_inited_;
  bool is_paused_;
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_AI_SERVICE_OB_AI_COLUMN_WORKER_H_