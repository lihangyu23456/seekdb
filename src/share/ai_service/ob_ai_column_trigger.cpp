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

#define USING_LOG_PREFIX SHARE
#include "share/ai_service/ob_ai_column_trigger.h"

#include "common/ob_obj_cast.h"
#include "common/ob_object.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ai_service/ob_ai_column_util.h"
#include "share/ob_errno.h"
#include "share/schema/ob_table_schema.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace share
{

bool ObAIColumnTrigger::table_has_ai_column(const ObTableSchema &table)
{
  bool found = false;
  ObTableSchema::const_column_iterator it = table.column_begin();
  ObTableSchema::const_column_iterator end = table.column_end();
  for (; it != end && !found; ++it) {
    const ObColumnSchemaV2 *col = *it;
    if (OB_NOT_NULL(col) && ObAIColumnSpec::column_is_ai_result(*col)) {
      found = true;
    }
  }
  return found;
}

int ObAIColumnTrigger::qualified_table_ref(ObIAllocator &alloc,
                                            const ObString &db_name,
                                            const ObString &tb_name,
                                            ObSqlString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (OB_FAIL(out.assign("`"))) {
  } else if (OB_FAIL(out.append(db_name))) {
  } else if (OB_FAIL(out.append("`.`"))) {
  } else if (OB_FAIL(out.append(tb_name))) {
  } else if (OB_FAIL(out.append("`"))) {
  }
  return ret;
}

// Append a single-column predicate `col` = lit to sql string.
static int append_pred_col(ObSqlString &sql,
                            const ObString &col_name,
                            const ObObj &val,
                            bool is_first)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("`"))) {
  } else if (OB_FAIL(sql.append(col_name))) {
  } else if (OB_FAIL(sql.append("` = "))) {
  }
  if (OB_FAIL(ret)) return ret;
  if (val.is_null()) {
    ret = sql.append("NULL");
  } else if (val.is_varchar()) {
    // single-quote escape
    ret = sql.append("'");
    const char *p = val.get_varchar().ptr();
    int64_t n = val.get_varchar().length();
    for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
      if (p[i] == '\'') {
        ret = sql.append("''");
      } else {
        ret = sql.append(p + i, 1);
      }
    }
    if (OB_SUCC(ret)) ret = sql.append("'");
  } else if (val.is_integer_type()) {
    char buf[64];
    int64_t n = 0;
    if (val.is_tinyint() || val.is_smallint() || val.is_mediumint() ||
        val.is_int() || val.is_bigint()) {
      n = snprintf(buf, sizeof(buf), "%ld", val.get_int());
    } else if (val.is_utinyint() || val.is_usmallint() || val.is_umediumint() ||
               val.is_uint() || val.is_ubigint()) {
      n = snprintf(buf, sizeof(buf), "%lu", val.get_uint());
    }
    if (n < 0) ret = OB_ERR_UNEXPECTED;
    else ret = sql.append(buf, n);
  } else if (val.is_float() || val.is_double()) {
    char buf[64];
    int64_t n = snprintf(buf, sizeof(buf), "%g", val.get_double());
    if (n < 0) ret = OB_ERR_UNEXPECTED;
    else ret = sql.append(buf, n);
  } else {
    // Fallback: print as hex literal for unknown types (DATE/DATETIME etc.)
    char hex_buf[256];
    int64_t hex_len = 0;
    TO_STRING_KV("", val);
    // Too complicated — just print as `?` for now so we don't crash.
    OBS_LOG(WARN, "unhandled PK type in rowkey predicate, falling back to placeholder");
    ret = sql.append("NULL /*unhandled*/");
  }
  return ret;
}

int ObAIColumnTrigger::build_rowkey_predicate(ObIAllocator &alloc,
                                               const ObTableSchema &table,
                                               const ObNewRow &row,
                                               ObSqlString &out_pred)
{
  int ret = OB_SUCCESS;
  out_pred.reset();
  const ObRowkeyInfo &rk_info = table.get_rowkey_info();
  const common::ObIArray<ObRowkeyColumn> &rk_cols = rk_info.get_rowkey_columns();
  for (int64_t i = 0; OB_SUCC(ret) && i < rk_cols.count(); ++i) {
    const ObRowkeyColumn &rk = rk_cols.at(i);
    int64_t col_idx = rk.column_id_ - 1; // rowkey column_ids are 1-based
    const ObColumnSchemaV2 *col = nullptr;
    if (OB_FAIL(table.get_column_schema(rk.column_id_, col))) {
      // fallback to ordinal lookup
      col_idx = -1;
      ObTableSchema::const_column_iterator it = table.column_begin();
      ObTableSchema::const_column_iterator end = table.column_end();
      int64_t idx = 0;
      for (; it != end && OB_ISNULL(col); ++it, ++idx) {
        if (rk.column_id_ == (*it)->get_column_id()) {
          col = *it;
          col_idx = idx;
        }
      }
      if (OB_ISNULL(col) || col_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "rowkey column not found", K(ret), K(rk.column_id_));
      }
    }
    if (OB_FAIL(ret) || OB_ISNULL(col)) continue;
    if (i > 0) {
      ret = out_pred.append(" AND ");
    }
    if (OB_SUCC(ret)) {
      // row.cells_ ordinal index: for insert_rtdef, cells_ is indexed by the
      // resolved column ordinal; we get that via col_idx above.
      ObObj val = row.cells_.at(col_idx);
      ret = append_pred_col(out_pred, col->get_column_name_str(), val, i == 0);
    }
  }
  return ret;
}

// Helper: fetch all AI column specs + hidden companion column names for a table.
struct AiColInfo
{
  int64_t col_id_;
  ObString col_name_;
  ObAIColumnSpec spec_;
  ObString state_col_;    // __ai_<col>_state
  ObString request_col_;  // __ai_<col>_request
  ObString result_spec_col_;
};

static int collect_ai_columns(ObIAllocator &alloc,
                               const ObTableSchema &table,
                               ObSEArray<AiColInfo, 8> &out)
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_column_iterator it = table.column_begin();
  ObTableSchema::const_column_iterator end = table.column_end();
  for (; OB_SUCC(ret) && it != end; ++it) {
    const ObColumnSchemaV2 *col = *it;
    ObAIColumnSpec spec;
    if (OB_ISNULL(col)) {
      // skip
    } else if (OB_FAIL(ObAIColumnSpec::get_column_ai_spec(*col, spec))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // not AI column
      }
    } else {
      AiColInfo info;
      info.col_id_ = col->get_column_id();
      info.col_name_ = col->get_column_name_str();
      info.spec_ = spec;
      ObString st_nq, req_nq, sp_nq;
      if (OB_FAIL(ObAIColumnUtil::state_col_name(alloc, info.col_name_, st_nq))) {
      } else if (OB_FAIL(ObAIColumnUtil::request_col_name(alloc, info.col_name_, req_nq))) {
      } else if (OB_FAIL(ObAIColumnUtil::result_spec_col_name(alloc, info.col_name_, sp_nq))) {
      } else if (OB_FAIL(ob_write_string(alloc, st_nq, info.state_col_))) {
      } else if (OB_FAIL(ob_write_string(alloc, req_nq, info.request_col_))) {
      } else if (OB_FAIL(ob_write_string(alloc, sp_nq, info.result_spec_col_))) {
      } else if (OB_FAIL(out.push_back(info))) {
      }
    }
  }
  return ret;
}

static int exec_sql(common::ObISQLConnection &conn, const ObString &sql)
{
  int64_t affected = 0;
  return conn.write(sql.ptr(), affected);
}

// ---- INSERT trigger ---------------------------------------------------------

int ObAIColumnTrigger::trigger_insert(common::ObISQLConnection &inner_conn,
                                       const ObTableSchema &table,
                                       const ObIArray<common::ObNewRow> &inserted_rows,
                                       int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!table_has_ai_column(table)) return ret;

  ObArenaAllocator alloc("AiTrigIns");
  ObSEArray<AiColInfo, 8> ai_cols;
  ObString db_name;
  ObSqlString fqt;

  if (OB_FAIL(collect_ai_columns(alloc, table, ai_cols))) {
  } else if (OB_FAIL(db_name.assign(table.get_database_name_str()))) {
  } else if (OB_FAIL(qualified_table_ref(alloc, db_name, table.get_table_name_str(), fqt))) {
  }

  const int64_t table_id = table.get_table_id();
  const char *task_tname = "oceanbase.__all_ai_column_task";

  for (int64_t ri = 0; OB_SUCC(ret) && ri < inserted_rows.count(); ++ri) {
    const ObNewRow &row = inserted_rows.at(ri);
    ObSqlString rowkey_pred;
    ObSqlString set_req;

    // Generate rowkey predicate for this row
    if (OB_FAIL(build_rowkey_predicate(alloc, table, row, rowkey_pred))) {
      SHARE_LOG(WARN, "build rowkey_pred fail", K(ret));
      // keep going with other rows
      ret = OB_SUCCESS;
      continue;
    }

    // SET @ai_req := UUID_SHORT()
    if (OB_FAIL(set_req.assign("SET @ai_req := UUID_SHORT()"))) {
    } else if (OB_FAIL(exec_sql(inner_conn, set_req.string()))) {
      SHARE_LOG(WARN, "set @ai_req fail", K(ret));
      ret = OB_SUCCESS;
      continue;
    }

    // UPDATE t SET __ai_col_request = @ai_req, state default already PENDING where rowkey
    // Result column default NULL is fine.
    // Actually: state default is PENDING (set in build_ai_hidden_columns), result default NULL,
    // request default NULL. So we only need to set request = @ai_req for each AI column.
    for (int64_t ci = 0; OB_SUCC(ret) && ci < ai_cols.count(); ++ci) {
      const AiColInfo &ac = ai_cols.at(ci);
      ObSqlString upd;
      if (OB_FAIL(upd.assign_fmt(
          "UPDATE %s SET `%.*s` = @ai_req WHERE %s",
          fqt.ptr(),
          ac.request_col_.length(), ac.request_col_.ptr(),
          rowkey_pred.ptr()))) {
      } else if (OB_FAIL(exec_sql(inner_conn, upd.string()))) {
        SHARE_LOG(WARN, "update request fail", K(ret), K(upd));
      }

      // INSERT INTO __all_ai_column_task
      // (table_id, column_id, request_id, rowkey_predicate, eligible_ts, state, retry_count)
      // VALUES (...)
      ObSqlString ins;
      if (OB_FAIL(ins.assign_fmt(
          "INSERT INTO %s (table_id, column_id, request_id, rowkey_predicate, eligible_ts, state, retry_count) "
          "VALUES (%ld, %ld, @ai_req, '%.*s', NOW(6), 0, 0)",
          task_tname, table_id, ac.col_id_,
          rowkey_pred.length(), rowkey_pred.ptr()))) {
      } else if (OB_FAIL(exec_sql(inner_conn, ins.string()))) {
        SHARE_LOG(WARN, "insert task fail", K(ret), K(ins));
      }
    }
  }
  return OB_SUCCESS; // trigger failure must not roll back user DML
}

// ---- UPDATE trigger ---------------------------------------------------------

int ObAIColumnTrigger::trigger_update(common::ObISQLConnection &inner_conn,
                                       const ObTableSchema &table,
                                       const ObIArray<int64_t> &changed_col_idxs,
                                       const ObIArray<common::ObNewRow> &old_rows,
                                       const ObIArray<common::ObNewRow> &new_rows,
                                       int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!table_has_ai_column(table)) return ret;

  // First: figure out if any changed column is a PK column OR a dependency of
  // any AI column. Only if something matters do we invalidate + reschedule.
  ObArenaAllocator alloc("AiTrigUpd");
  ObSEArray<AiColInfo, 8> ai_cols;
  ObSEArray<int64_t, 8> pk_col_idxs; // ordinal positions of PK columns
  bool need_invalidate = false;

  if (OB_FAIL(collect_ai_columns(alloc, table, ai_cols))) {
    return OB_SUCCESS;
  }

  // Identify PK column ordinal indices
  const ObRowkeyInfo &rk_info = table.get_rowkey_info();
  const ObIArray<ObRowkeyColumn> &rk_cols = rk_info.get_rowkey_columns();
  ObTableSchema::const_column_iterator it = table.column_begin();
  ObTableSchema::const_column_iterator end = table.column_end();
  int64_t ord = 0;
  for (; OB_SUCC(ret) && it != end; ++it, ++ord) {
    const ObColumnSchemaV2 *col = *it;
    if (OB_ISNULL(col)) continue;
    for (int64_t k = 0; OB_SUCC(ret) && k < rk_cols.count(); ++k) {
      if (col->get_column_id() == rk_cols.at(k).column_id_) {
        if (OB_FAIL(pk_col_idxs.push_back(ord))) break;
      }
    }
  }
  if (OB_FAIL(ret)) return OB_SUCCESS;

  // Check: any changed column is PK?
  for (int64_t i = 0; i < changed_col_idxs.count() && !need_invalidate; ++i) {
    const int64_t idx = changed_col_idxs.at(i);
    for (int64_t p = 0; p < pk_col_idxs.count() && !need_invalidate; ++p) {
      if (idx == pk_col_idxs.at(p)) need_invalidate = true;
    }
  }

  // Check: any changed column is a dependency of an AI column?
  // ObAIColumnSpec.deps_ is a comma-separated list of dependency column names.
  // We match column name strings with changed column names.
  if (!need_invalidate) {
    for (int64_t ci = 0; ci < ai_cols.count() && !need_invalidate; ++ci) {
      const ObString &deps = ai_cols.at(ci).spec_.deps_;
      const char *cur = deps.ptr();
      const char *dep_end = deps.ptr() + deps.length();
      while (cur < dep_end && !need_invalidate) {
        const char *comma = static_cast<const char *>(memchr(cur, ',', dep_end - cur));
        int64_t dep_len = comma ? comma - cur : dep_end - cur;
        // trim leading/trailing spaces
        while (cur < dep_end && *cur == ' ') { ++cur; --dep_len; }
        while (dep_len > 0 && cur[dep_len - 1] == ' ') { --dep_len; }
        if (dep_len > 0) {
          ObString dep_name(cur, dep_len);
          // Compare with changed columns
          for (int64_t ch = 0; ch < changed_col_idxs.count() && !need_invalidate; ++ch) {
            int64_t idx = changed_col_idxs.at(ch);
            const ObColumnSchemaV2 *ch_col = nullptr;
            it = table.column_begin();
            ord = 0;
            for (; it != end && OB_ISNULL(ch_col); ++it, ++ord) {
              if (ord == idx) ch_col = *it;
            }
            if (OB_NOT_NULL(ch_col)) {
              const ObString &ch_name = ch_col->get_column_name_str();
              if (dep_name.length() == ch_name.length() &&
                  0 == memcmp(dep_name.ptr(), ch_name.ptr(), dep_name.length())) {
                need_invalidate = true;
              }
            }
          }
        }
        cur = comma ? comma + 1 : dep_end;
      }
    }
  }

  if (!need_invalidate) return OB_SUCCESS;

  // Now invalidate and reschedule for each changed row pair
  ObString db_name;
  ObSqlString fqt;
  if (OB_FAIL(db_name.assign(table.get_database_name_str()))) {
    return OB_SUCCESS;
  } else if (OB_FAIL(qualified_table_ref(alloc, db_name, table.get_table_name_str(), fqt))) {
    return OB_SUCCESS;
  }

  const int64_t table_id = table.get_table_id();
  const char *task_tname = "oceanbase.__all_ai_column_task";
  const int64_t N = old_rows.count() < new_rows.count() ? old_rows.count() : new_rows.count();

  for (int64_t i = 0; i < N && OB_SUCC(ret); ++i) {
    ObSqlString rowkey_pred;
    if (OB_FAIL(build_rowkey_predicate(alloc, table, new_rows.at(i), rowkey_pred))) {
      SHARE_LOG(WARN, "build new rowkey_pred fail", K(ret));
      ret = OB_SUCCESS;
      continue;
    }
    ObSqlString set_req;
    if (OB_FAIL(set_req.assign("SET @ai_req := UUID_SHORT()"))) {
    } else if (OB_FAIL(exec_sql(inner_conn, set_req.string()))) {
      ret = OB_SUCCESS;
      continue;
    }
    for (int64_t ci = 0; ci < ai_cols.count() && OB_SUCC(ret); ++ci) {
      const AiColInfo &ac = ai_cols.at(ci);
      // UPDATE t SET result = NULL, state = 'PENDING', request = @ai_req WHERE rowkey_pred
      ObSqlString upd;
      if (OB_FAIL(upd.assign_fmt(
          "UPDATE %s SET `%.*s` = NULL, `%.*s` = 'PENDING', `%.*s` = @ai_req WHERE %s",
          fqt.ptr(),
          ac.col_name_.length(), ac.col_name_.ptr(),
          ac.state_col_.length(), ac.state_col_.ptr(),
          ac.request_col_.length(), ac.request_col_.ptr(),
          rowkey_pred.ptr()))) {
      } else if (OB_FAIL(exec_sql(inner_conn, upd.string()))) {
        SHARE_LOG(WARN, "update invalidation fail", K(ret), K(upd));
      }
      // INSERT task
      ObSqlString ins;
      if (OB_FAIL(ins.assign_fmt(
          "INSERT INTO %s (table_id, column_id, request_id, rowkey_predicate, eligible_ts, state, retry_count) "
          "VALUES (%ld, %ld, @ai_req, '%.*s', NOW(6), 0, 0)",
          task_tname, table_id, ac.col_id_,
          rowkey_pred.length(), rowkey_pred.ptr()))) {
      } else if (OB_FAIL(exec_sql(inner_conn, ins.string()))) {
        SHARE_LOG(WARN, "insert task fail on update", K(ret), K(ins));
      }
    }
  }

  // Also invalidate rowkey_predicate that changed!
  // (DELETE old PK's task rows so stale requests don't keep running)
  for (int64_t i = 0; i < N && OB_SUCC(ret); ++i) {
    bool pk_changed = false;
    for (int64_t p = 0; p < pk_col_idxs.count() && !pk_changed; ++p) {
      int64_t idx = pk_col_idxs.at(p);
      if (old_rows.at(i).cells_.at(idx) != new_rows.at(i).cells_.at(idx)) {
        pk_changed = true;
      }
    }
    if (pk_changed) {
      ObSqlString old_rowkey_pred;
      ObSqlString del_task;
      if (OB_FAIL(build_rowkey_predicate(alloc, table, old_rows.at(i), old_rowkey_pred))) {
      } else if (OB_FAIL(del_task.assign_fmt(
          "DELETE FROM %s WHERE table_id=%ld AND rowkey_predicate='%.*s'",
          task_tname, table_id,
          old_rowkey_pred.length(), old_rowkey_pred.ptr()))) {
      } else {
        int64_t ar = 0;
        inner_conn.write(del_task.ptr(), ar); // ignore error, stale GC
      }
    }
  }

  return OB_SUCCESS;
}

// ---- DELETE trigger ---------------------------------------------------------

int ObAIColumnTrigger::trigger_delete(common::ObISQLConnection &inner_conn,
                                       const ObTableSchema &table,
                                       const ObIArray<common::ObNewRow> &deleted_rows,
                                       int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!table_has_ai_column(table)) return ret;
  if (deleted_rows.count() == 0) return ret;

  ObArenaAllocator alloc("AiTrigDel");
  const int64_t table_id = table.get_table_id();

  // Build IN list of rowkey_predicates
  ObSqlString in_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < deleted_rows.count(); ++i) {
    ObSqlString pred;
    if (OB_FAIL(build_rowkey_predicate(alloc, table, deleted_rows.at(i), pred))) {
      SHARE_LOG(WARN, "build rowkey_pred fail on delete", K(ret));
      ret = OB_SUCCESS;
      continue;
    }
    if (i > 0) {
      if (OB_FAIL(in_list.append(","))) break;
    }
    if (OB_FAIL(in_list.append("'"))) break;
    if (OB_FAIL(in_list.append(pred))) break;
    if (OB_FAIL(in_list.append("'"))) break;
  }

  if (OB_SUCC(ret) && in_list.length() > 0) {
    ObSqlString del_task;
    if (OB_FAIL(del_task.assign_fmt(
        "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id = %ld AND rowkey_predicate IN (%.*s)",
        table_id, in_list.length(), in_list.ptr()))) {
    } else {
      int64_t ar = 0;
      inner_conn.write(del_task.ptr(), ar); // ignore error
    }
  }
  return OB_SUCCESS;
}

} // namespace share
} // namespace oceanbase