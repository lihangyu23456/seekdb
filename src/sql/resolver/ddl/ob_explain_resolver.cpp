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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;

// the name of the type of a column of the database
static const char *column_type_name(common::ObObjType type)
{
  const char *name = "VARCHAR";
  switch (type) {
    case common::ObIntType: {
      name = "BIGINT";
    } break;
    case common::ObTinyIntType: {
      name = "TINYINT";
    } break;
    case common::ObSmallIntType: {
      name = "SMALLINT";
    } break;
    case common::ObMediumIntType: {
      name = "INT";
    } break;
    case common::ObDoubleType: {
      name = "DOUBLE";
    } break;
    case common::ObFloatType: {
      name = "FLOAT";
    } break;
    case common::ObVarcharType: {
      name = "VARCHAR";
    } break;
    case common::ObCharType: {
      name = "CHAR";
    } break;
    case common::ObDateType: {
      name = "DATE";
    } break;
    case common::ObDateTimeType: {
      name = "DATETIME";
    } break;
    case common::ObTimeType: {
      name = "TIME";
    } break;
    case common::ObTimestampType: {
      name = "TIMESTAMP";
    } break;
    case common::ObNumberType: {
      name = "DECIMAL";
    } break;
    case common::ObNullType: {
      name = "NULL";
    } break;
    default: {
      name = "VARCHAR";
    } break;
  }
  return name;
}
int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_  && 5 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt *child_stmt = NULL;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *child_node,
                                   child_stmt))) {
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
          case T_EXPLAIN_DESC:
          {
            // a describe shows the columns of the statement
            explain_stmt->set_is_describe(true);
          } break;
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
        if (OB_SUCC(ret) && explain_stmt->is_describe()) {
          ret = collect_describe_columns(*explain_stmt, *static_cast<ObDMLStmt *>(child_stmt));
        }
      }
    }
    ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
    if (OB_SUCC(ret) && NULL != opt_node) {
      switch (opt_node->type_) {
      case T_PRETTY_COLOR:
        opt.with_color_ = true;
        // pass
      case T_PRETTY:
        opt.with_tree_line_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      explain_stmt->set_display_opt(opt);
    }
    ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
    if (OB_SUCC(ret) && NULL != into_table_node) {
      ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
      explain_stmt->set_into_table(into_table);
    }
    ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
    if (OB_SUCC(ret) && NULL != statement_id_node) {
      ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
      explain_stmt->set_statement_id(statement_id);
    }
  }

  return ret;
}

int ObExplainResolver::collect_describe_columns(ObExplainStmt &explain_stmt,
                                                ObDMLStmt &child_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  if (!child_stmt.is_select_stmt()
      || NULL == (select_stmt = static_cast<ObSelectStmt *>(&child_stmt))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "only the columns of a select statement can be described");
  } else {
    const int64_t size = select_stmt->get_select_item_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      const SelectItem &select_item = select_stmt->get_select_item(i);
      ObRawExpr *expr = select_item.expr_;
      ObExplainColumnMeta meta;
      if (NULL == expr) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      // the column of the table that the item refers to
      ObColumnRefRawExpr *column_expr = NULL;
      if (OB_SUCC(ObRawExprUtils::get_col_ref_expr_recursively(expr, column_expr))
          && NULL != column_expr) {
        ColumnItem *column_item = select_stmt->get_column_item_by_id(column_expr->get_table_id(),
                                                                      column_expr->get_column_id());
        if (NULL != column_item) {
          meta.name_ = column_item->column_name_;
        }
      }
      if (select_item.alias_name_.length() > 0) {
        meta.name_ = select_item.alias_name_;
      }
      // the type of a column of a file is the type that the file uses
      if (NULL != column_expr) {
        const TableItem *table_item = select_stmt->get_table_item_by_id(column_expr->get_table_id());
        if (NULL != table_item && table_item->is_file_table()) {
          const int64_t index = column_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
          if (index >= 0 && index < table_item->file_columns_.count()) {
            const TableItem::FileColumnMeta &file_column = table_item->file_columns_.at(index);
            meta.type_name_ = file_column.type_name_;
            meta.nullable_ = file_column.nullable_;
          }
        }
      }
      if (0 == meta.type_name_.length()) {
        // the type of the database
        meta.type_name_ = ObString::make_string(column_type_name(expr->get_data_type()));
        meta.nullable_ = !expr->is_not_null_for_read();
      }
      if (OB_FAIL(explain_stmt.get_describe_columns().push_back(meta))) {
        LOG_WARN("failed to add the column of the describe", K(ret));
      }
    }
  }
  return ret;
}
