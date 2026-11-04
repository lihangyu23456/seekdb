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

#ifndef OCEANBASE_SHARE_OB_AI_COLUMN_TRIGGER_H_
#define OCEANBASE_SHARE_OB_AI_COLUMN_TRIGGER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObString;
class ObISQLConnection;
class ObNewRow;
} // namespace common

namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
} // namespace schema

class ObAIColumnTrigger
{
public:
  // Check whether a table has any AI result column.
  static bool table_has_ai_column(const schema::ObTableSchema &table);

  // Generate `rowkey_predicate` for a given row. For INSERT/UPDATE:
  // read PK column values from `row`; for DELETE: use the WHERE clause text
  // from the DELETE spec. Format: `` `pk_col` = value `` (one per PK column,
  // AND-joined). Values are escaped/quoted per SQL literal rules.
  // Returns predicate in the form consumed by __all_ai_column_task.rowkey_predicate_.
  static int build_rowkey_predicate(common::ObIAllocator &alloc,
                                     const schema::ObTableSchema &table,
                                     const common::ObNewRow &row,
                                     common::ObSqlString &out_pred);

  // --- triggers run inside the USER transaction (via inner conn) -------------
  // All SQL below executes through an ObInnerSQLConnection that shares the
  // user transaction so ROLLBACK naturally undoes every triggered row.

  // INSERT: for each AI column in the table, generate a new request_id per row,
  // write it into the hidden request column, and insert a pending task row
  // into oceanbase.__all_ai_column_task for each (row, AI-col) pair.
  static int trigger_insert(common::ObISQLConnection &inner_conn,
                            const schema::ObTableSchema &table,
                            const common::ObIArray<common::ObNewRow> &inserted_rows,
                            int64_t tenant_id);

  // UPDATE: invalidates result + reschedules task when any AI dependency column
  // or the primary key changes. For unrelated column changes, does nothing.
  //   - changed_cols: bitmap of column ordinal positions modified by SET
  //   - old_rows: rows as they were before UPDATE (PK rowkey identity + deps)
  //   - new_rows: rows after UPDATE (PK rowkey possibly different)
  // For every (old_row, new_row) pair that touches deps or PK:
  //   new_row gets result=NULL, state='PENDING', request=new_request_id,
  //   a fresh task is inserted keyed on the new PK rowkey_predicate.
  static int trigger_update(common::ObISQLConnection &inner_conn,
                            const schema::ObTableSchema &table,
                            const common::ObIArray<int64_t> &changed_col_idxs,
                            const common::ObIArray<common::ObNewRow> &old_rows,
                            const common::ObIArray<common::ObNewRow> &new_rows,
                            int64_t tenant_id);

  // DELETE: drop pending task rows whose rowkey_predicate matches any deleted row.
  static int trigger_delete(common::ObISQLConnection &inner_conn,
                            const schema::ObTableSchema &table,
                            const common::ObIArray<common::ObNewRow> &deleted_rows,
                            int64_t tenant_id);

  // Build a fully-qualified table identifier: `db`.`tbl`.
  static int qualified_table_ref(common::ObIAllocator &alloc,
                                  const ObString &db_name,
                                  const ObString &tb_name,
                                  common::ObSqlString &out);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_AI_COLUMN_TRIGGER_H_