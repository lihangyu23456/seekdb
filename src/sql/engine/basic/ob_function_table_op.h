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

#ifndef OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_
#define OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "lib/charset/ob_charset.h"
#include "storage/ob_file_scan_utils.h"

namespace oceanbase
{
namespace sql
{

class ObExpr;
class ObFunctionTableSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(4);
public:
  ObFunctionTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), value_expr_(nullptr), column_exprs_(alloc), has_correlated_expr_(false),
      file_format_(-1), column_names_(alloc), alloc_(&alloc)
  {}
  ObExpr *value_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_;
  bool has_correlated_expr_;
  // the format of a file, -1 when the path is not a file
  int8_t file_format_;
  // the names of the columns, the rows of a json file are matched by them
  common::ObFixedArray<common::ObString, common::ObIAllocator> column_names_;
  // the memory of the plan, the names of the columns are copied into it when
  // the plan is put into the plan cache
  common::ObIAllocator *alloc_;
};

class ObFunctionTableOp : public ObOperator
{
public:
  ObFunctionTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    node_idx_(0),
    already_calc_(false),
    row_count_(0),
    col_count_(0),
    value_table_(NULL),
      file_reader_(),
      file_cells_(),
      file_path_(),
      file_eof_(false),
      file_glob_files_(),
      file_glob_index_(0),
      jsonl_file_(nullptr),
      jsonl_path_(),
      jsonl_fields_()
  {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  //virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
private:
  int inner_get_next_row_udf();
  int inner_get_next_row_sys_func();
  int inner_get_next_row_file();
  int inner_get_next_row_glob();
  int inner_get_next_row_jsonl();
  int open_file();
  int open_glob();
  int open_jsonl();
  int set_cell_datum(int64_t index, const storage::ObFileCell &cell);
  int get_current_result(common::ObObj &result);
  int64_t node_idx_;
  bool already_calc_;
  int64_t row_count_;
  int64_t col_count_;
  common::ObObj value_;
  pl::ObPLCollection *value_table_;
  int (ObFunctionTableOp::*next_row_func_)();
  // the reader of a file, the rows of the file are the rows of the table
  storage::ObCsvFileReader file_reader_;
  common::ObArray<storage::ObFileCell> file_cells_;
  common::ObString file_path_;
  bool file_eof_;
  // the files of a pattern, the rows of the table are the files
  common::ObArray<common::ObString> file_glob_files_;
  int64_t file_glob_index_;
  // the rows of a json lines file
  FILE *jsonl_file_;
  common::ObString jsonl_path_;
  common::ObArray<storage::ObFileJsonField> jsonl_fields_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_ */
