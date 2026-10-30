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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_obj_cast_runtime.h"
#include "sql/session/ob_basic_session_info.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_,
                    file_format_);

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (MY_SPEC.file_format_ >= 0) {
    // the rows of the table are the rows of a file
    if (static_cast<int8_t>(storage::ObFileScanUtils::ObFileFormat::GLOB) == MY_SPEC.file_format_) {
      next_row_func_ = &ObFunctionTableOp::inner_get_next_row_glob;
      if (OB_FAIL(open_glob())) {
      }
    } else {
      next_row_func_ = &ObFunctionTableOp::inner_get_next_row_file;
      if (OB_FAIL(open_file())) {
      }
    }
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    node_idx_ = 0;
    if (MY_SPEC.file_format_ >= 0) {
      // the file is read again from its beginning
      file_eof_ = false;
      file_reader_.close();
      if (OB_FAIL(open_file())) {
      }
    }
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  file_reader_.close();
  file_path_ = ObString();
  return ret;
}

void ObFunctionTableOp::destroy()
{
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::open_file()
{
  int ret = OB_SUCCESS;
  ObDatum *value = nullptr;
  file_path_ = ObString();
  if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
  } else if (value->is_null()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    file_path_ = value->get_string();
    // the path of a file is used by the functions of the system, so it needs
    // a zero at its end
    char *path_buf = nullptr;
    if (OB_ISNULL(path_buf = static_cast<char *>(get_exec_ctx().get_allocator().alloc(file_path_.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(path_buf, file_path_.ptr(), file_path_.length());
      path_buf[file_path_.length()] = '\0';
      file_path_ = common::ObString(file_path_.length(), path_buf);
    }
    if (OB_SUCC(ret) && OB_FAIL(file_reader_.open(file_path_))) {
      LOG_WARN("failed to open the file of the table", K(ret), K(file_path_));
    } else if (OB_SUCC(ret)) {
      // the header of the file holds the names of the columns, it is not a row
      common::ObArray<common::ObString> header_names;
      if (OB_FAIL(file_reader_.header(header_names))) {
        LOG_WARN("failed to read the header of the file", K(ret), K(file_path_));
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_file()
{
  int ret = OB_SUCCESS;
  bool is_eof = false;
  if (file_eof_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(file_reader_.read_line(file_cells_, is_eof))) {
    if (OB_ITER_END == ret) {
      file_eof_ = true;
    }
  } else {
    const int64_t col_count = MY_SPEC.column_exprs_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const storage::ObFileCell *cell = (i < file_cells_.count()) ? &file_cells_.at(i) : nullptr;
      if (OB_FAIL(set_cell_datum(i, (nullptr == cell) ? storage::ObFileCell() : *cell))) {
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::open_glob()
{
  int ret = OB_SUCCESS;
  ObDatum *value = nullptr;
  file_glob_files_.reset();
  file_glob_index_ = 0;
  if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
  } else if (value->is_null()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const common::ObString pattern = value->get_string();
    common::ObArenaAllocator allocator;
    if (OB_FAIL(storage::ObFileScanUtils::list_files(pattern, allocator, file_glob_files_))) {
      LOG_WARN("cannot list the files of the pattern", K(ret), K(pattern));
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_glob()
{
  int ret = OB_SUCCESS;
  if (file_glob_index_ >= file_glob_files_.count()) {
    ret = OB_ITER_END;
  } else {
    ObExpr *expr = MY_SPEC.column_exprs_.at(0);
    ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
    const common::ObString &path = file_glob_files_.at(file_glob_index_++);
    ObObj value;
    value.set_varchar(path);
    value.set_collation_type(expr->obj_meta_.get_collation_type());
    if (OB_FAIL(datum.from_obj(value, expr->obj_datum_map_))) {
    }
  }
  return ret;
}

int ObFunctionTableOp::set_cell_datum(int64_t index, const storage::ObFileCell &cell)
{
  int ret = OB_SUCCESS;
  ObExpr *expr = MY_SPEC.column_exprs_.at(index);
  ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
  if (cell.is_null_) {
    datum.set_null();
  } else {
    const ObObjType type = expr->datum_meta_.type_;
    common::ObString text = cell.string();
    common::ObArenaAllocator allocator;
    char *buf = nullptr;
    // The value of a boolean column may be written as true or false, the cast
    // of the database only knows the numbers.
    if (ObTinyIntType == type
        && (0 == text.case_compare("true") || 0 == text.case_compare("false"))) {
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(2)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        buf[0] = (0 == text.case_compare("true")) ? '1' : '0';
        buf[1] = '\0';
        text.assign_ptr(buf, 1);
      }
    }
    // The date and the time of a file may be written with a T between the date
    // and the time, the cast of the database does not know that separator.
    if (OB_SUCC(ret) && ObDateTimeType == type) {
      char *time_buf = nullptr;
      if (OB_ISNULL(time_buf = static_cast<char *>(allocator.alloc(text.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(time_buf, text.ptr(), text.length());
        time_buf[text.length()] = '\0';
        for (int64_t i = 0; i < text.length(); ++i) {
          if ('T' == time_buf[i]) {
            time_buf[i] = ' ';
          }
        }
        text.assign_ptr(time_buf, text.length());
      }
    }
    if (OB_SUCC(ret)) {
      ObObj src_obj;
      ObObj dst_obj;
      src_obj.set_varchar(text);
      src_obj.set_collation_type(expr->obj_meta_.get_collation_type());
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(GET_MY_SESSION(ctx_));
      ObCastCtx cast_ctx(&eval_ctx_.exec_ctx_.get_allocator(), &dtc_params, CM_NONE,
                         expr->obj_meta_.get_collation_type());
      ObSqlObjCastRuntime cast_runtime(&eval_ctx_.exec_ctx_);
      cast_ctx.runtime_ = &cast_runtime;
      if (OB_FAIL(ObObjCaster::to_type(type, expr->obj_meta_.get_collation_type(), cast_ctx,
                                      src_obj, dst_obj))) {
        char msg[256];
        snprintf(msg, sizeof(msg), "cannot read the value '%.*s' of the file",
                 static_cast<int>(text.length()), text.ptr());
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, msg);
      } else if (OB_FAIL(datum.from_obj(dst_obj, expr->obj_datum_map_))) {
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
