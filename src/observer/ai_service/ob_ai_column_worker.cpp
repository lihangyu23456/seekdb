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

#define USING_LOG_PREFIX OBSERVER
#include "observer/ai_service/ob_ai_column_worker.h"

#include "lib/utility/ob_print_utils.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_server_struct.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_errno.h"
#include "share/ai_service/ob_ai_column_util.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/omt/ob_ai_service.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

#include "common/mysqlclient/ob_mysql_transaction.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::observer;
using namespace oceanbase::observer::omt;

// ---- helper: SQL identifiers -------------------------------------------------

static int get_hidden_col_names(ObIAllocator &alloc,
                                const ObString &ai_col_name,
                                ObString &state_col,
                                ObString &request_col,
                                ObString &spec_col)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAIColumnUtil::state_col_name(alloc, ai_col_name, state_col))) {
  } else if (OB_FAIL(ObAIColumnUtil::request_col_name(alloc, ai_col_name, request_col))) {
  } else if (OB_FAIL(ObAIColumnUtil::result_spec_col_name(alloc, ai_col_name, spec_col))) {
  }
  return ret;
}

static int quote_ident(const ObString &in, ObSqlString &out)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out.assign("`"))) {
  } else if (OB_FAIL(out.append(in))) {
  } else if (OB_FAIL(out.append("`"))) {
  }
  return ret;
}

// ---- task row holder ---------------------------------------------------------

struct AiTaskRow
{
  int64_t table_id_;
  int64_t column_id_;
  int64_t request_id_;
  ObString rowkey_predicate_; // owns inner alloc memory? coming from result
  int64_t state_;
  int64_t retry_count_;

  AiTaskRow() : table_id_(0), column_id_(0), request_id_(0), state_(0), retry_count_(0) {}
};

// ---- ObAIColumnWorkerTask ----------------------------------------------------

ObAIColumnWorkerTask::ObAIColumnWorkerTask()
  : common::ObTimerTask(),
    is_inited_(false),
    is_paused_(false)
{
}

ObAIColumnWorkerTask::~ObAIColumnWorkerTask()
{
  destroy();
}

int ObAIColumnWorkerTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObAIColumnWorkerTask::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
  }
}

void ObAIColumnWorkerTask::pause()  { is_paused_ = true; }
void ObAIColumnWorkerTask::resume() { is_paused_ = false; }

int ObAIColumnWorkerTask::schedule(common::ObTimer &timer)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(timer.schedule(this, SCHEDULE_PERIOD_US, SCHEDULE_PERIOD_US))) {
    OBSERVER_LOG(WARN, "schedule ai column worker failed", K(ret));
  } else {
    OBSERVER_LOG(INFO, "ai column worker scheduled, period_us=%ld", SCHEDULE_PERIOD_US);
  }
  return ret;
}

void ObAIColumnWorkerTask::runTimerTask()
{
  if (!is_paused_ && is_inited_) {
    int ret = process_pending_tasks();
    if (OB_FAIL(ret)) {
      OBSERVER_LOG(WARN, "ai column worker pass failed", K(ret));
    }
  }
}

void ObAIColumnWorkerTask::do_work()
{
  runTimerTask();
}

// ---- main driver -------------------------------------------------------------

int ObAIColumnWorkerTask::process_pending_tasks()
{
  int ret = OB_SUCCESS;
  // Bail out if GCTX is not up yet
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    return ret;
  }

  ObMySQLTransaction trans;
  ObSqlString sql;

  // 1) Read eligible tasks (eligible_ts <= now(6), limit for safety)
  //    Grouping by (table_id, request_id) happens in-memory after reading.
  const char *task_tname = OB_ALL_AI_COLUMN_TASK_TNAME;
  if (OB_FAIL(sql.assign_fmt(
      "SELECT table_id, column_id, request_id, rowkey_predicate, state, retry_count "
      "FROM %s WHERE eligible_ts <= NOW(6) AND state IN (0,3) LIMIT 512",
      task_tname))) {
    return ret;
  }

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
      OBSERVER_LOG(WARN, "trans.start fail", K(ret));
      return ret;
    } else if (OB_FAIL(trans.read(res, sql.ptr()))) {
      OBSERVER_LOG(WARN, "trans.read fail", K(ret));
      trans.end(false);
      return ret;
    }

    ObMySQLResult *result = res.get_result();
    if (OB_ISNULL(result)) {
      trans.end(false);
      return OB_ERR_UNEXPECTED;
    }

    // Pull rows; each row processed independently per-column, but we try to
    // batch same (table_id, request_id) into one transaction later.
    ObArenaAllocator alloc("AiWorkerRows");
    ObSEArray<AiTaskRow, 64> rows;
    while (OB_SUCC(ret) && OB_SUCCESS == result->next()) {
      AiTaskRow r;
      if (OB_FAIL(result->get_int("table_id", r.table_id_))) {
      } else if (OB_FAIL(result->get_int("column_id", r.column_id_))) {
      } else if (OB_FAIL(result->get_int("request_id", r.request_id_))) {
      } else if (OB_FAIL(result->get_varchar("rowkey_predicate", r.rowkey_predicate_))) {
      } else if (OB_FAIL(result->get_int("state", r.state_))) {
      } else if (OB_FAIL(result->get_int("retry_count", r.retry_count_))) {
      } else if (OB_FAIL(rows.push_back(r))) {
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    res.reset();
    trans.end(OB_SUCC(ret));
    if (OB_FAIL(ret)) {
      return ret;
    }

    if (rows.count() == 0) {
      return OB_SUCCESS;
    }

    OBSERVER_LOG(INFO, "ai worker picked %ld task rows", rows.count());

    // 2) Process each row independently for v1; batching by request_id is a
    //    refinement we can add later once we know the real execution path compiles.
    //    For now keep it simple & correct: one task row -> one AI call ->
    //    one write-back transaction (success or retry bump or final fail).
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const AiTaskRow &task = rows.at(i);
      OBSERVER_LOG(INFO, "ai worker handling task table_id=%ld col_id=%ld req=%ld predicate=%s state=%ld retry=%ld",
                   task.table_id_, task.column_id_, task.request_id_,
                   task.rowkey_predicate_.ptr(), task.state_, task.retry_count_);

      // Fetch column spec + table meta
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *table_schema = nullptr;
      const ObColumnSchemaV2 *ai_col = nullptr;
      share::ObAIColumnSpec spec;

      if (OB_ISNULL(GCTX.schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      } else if (OB_FAIL(schema_guard.get_table_schema(OB_SYS_TENANT_ID, task.table_id_, table_schema))) {
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
      } else if (OB_FAIL(table_schema->get_column_schema(task.column_id_, ai_col))) {
      } else if (OB_ISNULL(ai_col)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(share::ObAIColumnSpec::get_column_ai_spec(*ai_col, spec))) {
        ret = OB_ERR_UNEXPECTED;
        OBSERVER_LOG(WARN, "task column missing packed spec", K(ret), K(task.table_id_), K(task.column_id_));
      }

      if (OB_FAIL(ret)) {
        // Unknown table / column => task is stale; remove it so we don't loop forever.
        ObMySQLTransaction del_trans;
        ObSqlString del_sql;
        int64_t affected = 0;
        if (OB_SUCC(del_trans.start(GCTX.sql_proxy_))) {
          del_sql.assign_fmt("DELETE FROM %s WHERE table_id=%ld AND column_id=%ld AND request_id=%ld AND rowkey_predicate=%s",
                             task_tname, task.table_id_, task.column_id_, task.request_id_,
                             task.rowkey_predicate_.ptr());
          del_trans.write(del_sql.ptr(), affected);
          del_trans.end(OB_SUCCESS);
        }
        ret = OB_SUCCESS; // keep draining other rows
        continue;
      }

      // Evaluate prompt against dependency row
      ObMySQLTransaction row_trans;
      ObSqlString prompt_sql;
      ObString evaluated_prompt;
      ObString evaluated_system;
      const char *state_col_n = nullptr;
      const char *request_col_n = nullptr;

      ObArenaAllocator row_alloc("AiWorkerRow");

      ObString state_name, request_name, spec_name;
      if (OB_FAIL(get_hidden_col_names(row_alloc, ai_col->get_column_name_str(),
                                       state_name, request_name, spec_name))) {
        // keep going with empty names if alloc fails? better bail
      } else {
        state_col_n = state_name.ptr();
        request_col_n = request_name.ptr();
      }

      // Build SELECT that evaluates the prompt SQL expression + fetches hidden request for stale guard.
      // We trust rowkey_predicate's quoting — produced by our own DML hook.
      if (OB_SUCC(ret)) {
        if (OB_FAIL(prompt_sql.assign_fmt("SELECT (%s) AS __prompt__, %s AS __req__ FROM %s WHERE %s",
                                          spec.prompt_.ptr(),
                                          request_col_n ? request_col_n : "NULL",
                                          table_schema->get_table_name_str().ptr(),
                                          task.rowkey_predicate_.ptr()))) {
          // Need database qualifier? The row sits in tenant DB. For system-tenant AI worker
          // we need the fully qualified table: use database_id via schema_service lookup.
        }
      }

      if (OB_FAIL(ret)) {
        OBSERVER_LOG(WARN, "build prompt sql fail", K(ret));
        continue;
      }

      // Resolve db name for fully qualified table name (critical!)
      const ObDatabaseSchema *db_schema = nullptr;
      ObString db_name;
      ObString full_table_ref;
      if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), db_schema))) {
      } else if (OB_ISNULL(db_schema)) {
      } else if (OB_FAIL(db_name.assign(db_schema->get_database_name_str()))) {
      }
      if (OB_FAIL(ret) || db_name.length() == 0) {
        // no db info — bail for this row
        OBSERVER_LOG(WARN, "db lookup fail", K(ret), "db_id", table_schema->get_database_id());
        ret = OB_SUCCESS;
        continue;
      }
      ObSqlString fqt;
      quote_ident(db_name, fqt);
      fqt.append(".");
      quote_ident(table_schema->get_table_name_str(), fqt);
      // Rebuild prompt_sql with qualified table
      prompt_sql.reset();
      if (OB_FAIL(prompt_sql.assign_fmt("SELECT (%s) AS __prompt__, %s AS __req__ FROM %s WHERE %s",
                                        spec.prompt_.ptr(),
                                        request_col_n ? request_col_n : "NULL",
                                        fqt.ptr(),
                                        task.rowkey_predicate_.ptr()))) {
      }

      // Read prompt + stale-guard request
      SMART_VAR(ObMySQLProxy::MySQLResult, prompt_res) {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(row_trans.start(GCTX.sql_proxy_))) {
        } else if (OB_FAIL(row_trans.read(prompt_res, prompt_sql.ptr()))) {
          OBSERVER_LOG(WARN, "read prompt sql fail", K(ret), K(prompt_sql));
          row_trans.end(false);
        } else {
          ObMySQLResult *pres = prompt_res.get_result();
          if (OB_ISNULL(pres)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(pres->next())) {
            if (OB_ITER_END == ret) {
              // Row no longer exists — stale guard already applied upstream; drop task
              ret = OB_SUCCESS;
              ObMySQLTransaction del_trans;
              ObSqlString del_sql;
              int64_t affected = 0;
              if (OB_SUCC(del_trans.start(GCTX.sql_proxy_))) {
                del_sql.assign_fmt("DELETE FROM %s WHERE table_id=%ld AND column_id=%ld AND request_id=%ld AND rowkey_predicate=%s",
                                   task_tname, task.table_id_, task.column_id_, task.request_id_,
                                   task.rowkey_predicate_.ptr());
                del_trans.write(del_sql.ptr(), affected);
                del_trans.end(OB_SUCCESS);
              }
            }
          } else {
            ObString prompt_s;
            int64_t row_req = 0;
            bool row_req_is_null = false;
            if (OB_FAIL(pres->get_varchar("__prompt__", prompt_s))) {
            } else if (OB_FAIL(pres->get_int("__req__", row_req))) {
              if (OB_ERR_NULL_VALUE == ret) { row_req_is_null = true; ret = OB_SUCCESS; }
            } else if (!row_req_is_null && row_req != task.request_id_) {
              // Stale! Row's request doesn't match our task — drop task silently
              OBSERVER_LOG(INFO, "stale guard fired: row req=%ld task req=%ld, dropping", row_req, task.request_id_);
              ret = OB_SUCCESS;
              ObMySQLTransaction del_trans;
              ObSqlString del_sql;
              int64_t affected = 0;
              if (OB_SUCC(del_trans.start(GCTX.sql_proxy_))) {
                del_sql.assign_fmt("DELETE FROM %s WHERE table_id=%ld AND column_id=%ld AND request_id=%ld AND rowkey_predicate=%s",
                                   task_tname, task.table_id_, task.column_id_, task.request_id_,
                                   task.rowkey_predicate_.ptr());
                del_trans.write(del_sql.ptr(), affected);
                del_trans.end(OB_SUCCESS);
              }
            } else {
              evaluated_prompt = prompt_s;

              // Build system prompt
              ObString sys_prompt;
              ObAIColumnUtil::type_system_prompt(ai_col->is_json(),
                                                  ai_col->get_data_type() == ObBigIntType,
                                                  sys_prompt);
              evaluated_system = sys_prompt;

              // Call AI model
              ObAiService *ai_svc = GCTX.ai_service_;
              ObAiServiceGuard guard;
              const ObAiModelEndpointInfo *endpoint_info = nullptr;
              if (OB_ISNULL(ai_svc)) {
                ret = OB_ERR_UNEXPECTED;
                OBSERVER_LOG(WARN, "ai_service not ready");
              } else if (OB_FAIL(ai_svc->get_ai_service_guard(guard))) {
              } else if (OB_FAIL(guard.get_ai_endpoint_by_ai_model_name(spec.model_, endpoint_info, false))) {
                OBSERVER_LOG(WARN, "get endpoint fail", K(ret), K(spec.model_));
              } else if (OB_ISNULL(endpoint_info)) {
                ret = OB_ERR_UNEXPECTED;
              } else {
                common::ObAIFuncExprInfo *info = nullptr;
                if (OB_FAIL(common::ObAIFuncUtils::get_ai_func_info(row_alloc, spec.model_, info))) {
                  OBSERVER_LOG(WARN, "get model info fail", K(ret));
                } else if (OB_ISNULL(info)) {
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  common::ObAIFuncModel model(row_alloc, *info, *endpoint_info);
                  ObString ai_result;
                  if (spec.is_embed_) {
                    if (OB_FAIL(model.call_dense_embedding(evaluated_prompt, nullptr, ai_result))) {
                      OBSERVER_LOG(WARN, "embedding call fail", K(ret));
                    }
                  } else {
                    if (evaluated_system.length() > 0) {
                      if (OB_FAIL(model.call_completion_typed(evaluated_system, evaluated_prompt, nullptr, ai_result))) {
                        OBSERVER_LOG(WARN, "completion typed call fail", K(ret));
                      }
                    } else {
                      if (OB_FAIL(model.call_completion(evaluated_prompt, nullptr, ai_result))) {
                        OBSERVER_LOG(WARN, "completion call fail", K(ret));
                      }
                    }
                  }
                  // TODO: type cast ai_result to target column type before writing back
                  // For BIGINT result, ai_result is "7"; for JSON, it's the JSON string;
                  // for VARCHAR, just store the raw text.
                  if (OB_SUCC(ret)) {
                    // Write back in a new transaction: UPDATE row SET result=?, state='READY',
                    // then DELETE the task row (or update retry on failure)
                    ObMySQLTransaction wb_trans;
                    int64_t affected = 0;
                    if (OB_FAIL(wb_trans.start(GCTX.sql_proxy_))) {
                    } else {
                      ObSqlString upd_sql;
                      ObString col_name = ai_col->get_column_name_str();
                      ObSqlString col_q, state_q;
                      quote_ident(col_name, col_q);
                      state_q.assign("__ai_");
                      state_q.append(col_name);
                      state_q.append("_state");
                      ObSqlString state_qf;
                      quote_ident(state_q, state_qf);
                      // Escape ai_result for SQL
                      ObSqlString val;
                      char *vbuf = ai_result.ptr();
                      // simple single-quote escape
                      val.assign("'");
                      for (int64_t vi = 0; vi < ai_result.length() && OB_SUCC(ret); ++vi) {
                        if (vbuf[vi] == '\'') {
                          val.append("''");
                        } else if (OB_FAIL(val.append(vbuf + vi, 1))) {}
                      }
                      if (OB_SUCC(ret) && OB_FAIL(val.append("'"))) {
                      }
                      if (OB_SUCC(ret) && OB_FAIL(upd_sql.assign_fmt(
                          "UPDATE %s.%s SET %s = %s, %s = 'READY' WHERE %s AND %s = %ld",
                          fqt.ptr(), fqt.ptr() /* col_q already qualified */,
                          col_q.ptr(), val.ptr(),
                          state_qf.ptr(), task.rowkey_predicate_.ptr(),
                          request_col_n ? request_col_n : "__ai_request__", task.request_id_))) {
                        // Oops rowkey_predicate already qualified by table name above? No.
                        // Actually rowkey_predicate is like `id` = 2 which already references the row PK column,
                        // it doesn't need a table qualifier. But we're joining the same table twice.
                        // Let's simplify: qualify with the full table ref.
                      }
                      // Simpler: build UPDATE with table name in front of column refs
                      // (the WHERE predicate uses unqualified PK column name, fine for single-table)
                      upd_sql.reset();
                      ObSqlString col_qual, state_qual, req_qual;
                      col_qual.assign(fqt); col_qual.append("."); col_qual.append(col_q);
                      state_qual.assign(fqt); state_qual.append("."); state_qual.append(state_qf);
                      req_qual.assign(fqt); req_qual.append("."); req_qual.append(request_col_n ? request_col_n : "__ai_request__");
                      val.reset();
                      val.assign("'");
                      for (int64_t vi = 0; vi < ai_result.length() && OB_SUCC(ret); ++vi) {
                        if (vbuf[vi] == '\'') val.append("''");
                        else val.append(vbuf + vi, 1);
                      }
                      val.append("'");
                      if (OB_FAIL(upd_sql.assign_fmt(
                          "UPDATE %s SET %s = %s, %s = 'READY' WHERE %s AND %s = %ld",
                          fqt.ptr(), col_qual.ptr(), val.ptr(), state_qual.ptr(),
                          task.rowkey_predicate_.ptr(), req_qual.ptr(), task.request_id_))) {
                      } else if (OB_FAIL(wb_trans.write(upd_sql.ptr(), affected))) {
                        OBSERVER_LOG(WARN, "write back fail", K(ret), K(upd_sql));
                      } else if (OB_FAIL(upd_sql.assign_fmt(
                          "DELETE FROM %s WHERE table_id=%ld AND column_id=%ld AND request_id=%ld AND rowkey_predicate=%s",
                          task_tname, task.table_id_, task.column_id_, task.request_id_,
                          task.rowkey_predicate_.ptr()))) {
                      } else if (OB_FAIL(wb_trans.write(upd_sql.ptr(), affected))) {
                      } else {
                        OBSERVER_LOG(INFO, "ai worker SUCCESS table=%ld col=%ld req=%ld",
                                     task.table_id_, task.column_id_, task.request_id_);
                      }
                    }
                    wb_trans.end(OB_SUCC(ret));
                  } else {
                    // AI call failed → bump retry or mark terminal
                    bool final_fail = task.retry_count_ >= spec.max_retries_;
                    if (spec.max_retries_ <= 0) final_fail = true; // 0 retries means first fail => terminal
                    ObMySQLTransaction rb_trans;
                    int64_t affected = 0;
                    if (OB_FAIL(rb_trans.start(GCTX.sql_proxy_))) {
                    } else if (final_fail) {
                      // Write row state='FAILED' with stale guard, keep result NULL
                      ObSqlString upd_sql;
                      ObString col_name = ai_col->get_column_name_str();
                      ObSqlString state_col_full;
                      state_col_full.assign(fqt); state_col_full.append(".");
                      ObSqlString state_nq;
                      state_nq.assign("__ai_"); state_nq.append(col_name); state_nq.append("_state");
                      ObSqlString state_qf;
                      quote_ident(state_nq, state_qf);
                      ObSqlString req_col_full;
                      req_col_full.assign(fqt); req_col_full.append("."); req_col_full.append(request_col_n);
                      upd_sql.assign_fmt(
                          "UPDATE %s SET %s = 'FAILED' WHERE %s AND %s = %ld",
                          fqt.ptr(), state_qf.ptr(),
                          task.rowkey_predicate_.ptr(), req_col_full.ptr(), task.request_id_);
                      rb_trans.write(upd_sql.ptr(), affected);
                      // Delete task row
                      ObSqlString del_sql;
                      del_sql.assign_fmt(
                          "DELETE FROM %s WHERE table_id=%ld AND column_id=%ld AND request_id=%ld AND rowkey_predicate=%s",
                          task_tname, task.table_id_, task.column_id_, task.request_id_,
                          task.rowkey_predicate_.ptr());
                      rb_trans.write(del_sql.ptr(), affected);
                      OBSERVER_LOG(WARN, "ai worker terminal fail table=%ld col=%ld req=%ld",
                                   task.table_id_, task.column_id_, task.request_id_);
                    } else {
                      // Bump retry_count, state=3 (waiting for retry)
                      ObSqlString upd_sql;
                      ObSqlString new_eligible;
                      ObSqlString old_eligible;
                      // eligible_ts = NOW(6) + INTERVAL 1 SECOND
                      upd_sql.assign_fmt(
                          "UPDATE %s SET state = 3, retry_count = retry_count + 1, "
                          "eligible_ts = DATE_ADD(NOW(6), INTERVAL 1 SECOND) "
                          "WHERE table_id=%ld AND column_id=%ld AND request_id=%ld AND rowkey_predicate=%s",
                          task_tname, task.table_id_, task.column_id_, task.request_id_,
                          task.rowkey_predicate_.ptr());
                      rb_trans.write(upd_sql.ptr(), affected);
                      OBSERVER_LOG(WARN, "ai worker retry scheduled table=%ld col=%ld req=%ld retry_count=%ld",
                                   task.table_id_, task.column_id_, task.request_id_, task.retry_count_ + 1);
                    }
                    rb_trans.end(OB_SUCCESS);
                  }
                }
              }
            }
          }
          row_trans.end(OB_SUCC(ret));
        }
      }
    }
  }

  return OB_SUCCESS;
}