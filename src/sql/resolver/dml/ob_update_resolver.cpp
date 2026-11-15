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
#include "sql/resolver/dml/ob_update_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
ObUpdateResolver::ObUpdateResolver(ObResolverParams &param)
  : ObDelUpdResolver(param)
{
  param.contain_dml_ = true;
}

ObUpdateResolver::~ObUpdateResolver()
{
}

int ObUpdateResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = NULL;
  ObSEArray<ObTableAssignment, 2> tables_assign;
  if (T_UPDATE != parse_tree.type_
      || 3 > parse_tree.num_child_
      || OB_ISNULL(parse_tree.children_)
      || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree for update", K(parse_tree.type_),
             K(parse_tree.num_child_), K(parse_tree.children_), K((session_info_)));
  } else if (OB_ISNULL(update_stmt = create_stmt<ObUpdateStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create update stmt failed");
  } else {
    stmt_ = update_stmt;
    update_stmt->set_ignore(false);
    if (NULL != parse_tree.children_[IGNORE]) {
      update_stmt->set_ignore(true);
      session_info_->set_ignore_stmt(true);
    }
  }

  // resolve outline data hints first
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_outline_data_hints())) {
    }
  }

  // resolve with clause before resolve table items
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_with_clause(parse_tree.children_[WITH_MYSQL]))) {
    }
  }

  // 1. resolve table items
  if (OB_SUCC(ret)) {
    ParseNode *table_node = parse_tree.children_[TABLE];
    if (OB_FAIL(resolve_table_list(*table_node))) {
    }
  }

  if (OB_SUCC(ret)) {
    current_scope_ = T_UPDATE_SCOPE;
    // resolve assignments
    ParseNode *assign_list = parse_tree.children_[UPDATE_LIST];
    if (OB_ISNULL(assign_list) || OB_UNLIKELY(T_ASSIGN_LIST != assign_list->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid assign list node", K(assign_list));
    } else if (OB_FAIL(resolve_assignments(*assign_list,
                                           tables_assign,
                                           current_scope_))) {
    }
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(it, update_stmt->get_table_items(), OB_SUCC(ret)) {
      if (NULL != *it && ((*it)->is_generated_table() || (*it)->is_temp_table())) {
        if (NULL != (*it)->view_base_item_ &&
            OB_FAIL(add_all_column_to_updatable_view(*update_stmt, *(*it)))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(view_pullup_special_column_exprs())) {
      } else if (OB_FAIL(view_pullup_part_exprs())) {
      } else { /*do nothing*/ }
    }
  }

  if (OB_SUCC(ret)) {
    // Parse cascading update columns
    if (OB_FAIL(resolve_additional_assignments(tables_assign,
                                               T_UPDATE_SCOPE))) {
    }
  }

  //add column for table scan, keep rowkey column in the head
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_assign.count(); i++) {
      if (OB_FAIL(invalidate_ai_columns(tables_assign.at(i)))) {
      } else if (OB_FAIL(generate_update_table_info(tables_assign.at(i)))) {
      } else { /*do nothing*/ }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_multi_update_table_conflict())) {
    } else if (update_stmt->is_ignore() && update_stmt->has_global_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ignore with global index");
    } else { /*do nothing*/ }
  }

  // 3. resolve other clauses
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_add_remove_const_expr_for_assignments())) {
    } else if (OB_FAIL(resolve_update_constraints())) {
    } else if (OB_FAIL(resolve_hints(parse_tree.children_[HINT]))) {
    } else if (OB_FAIL(resolve_where_clause(parse_tree.children_[WHERE]))) {
    } else if (params_.is_batch_stmt_ && OB_FAIL(generate_batched_stmt_info())) {
    } else if (OB_FAIL(resolve_order_clause(parse_tree.children_[ORDER_BY]))) {
    } else if (OB_FAIL(resolve_limit_clause(parse_tree.children_[LIMIT], true))) {
    } else if (!update_stmt->is_ignore() &&
               OB_FAIL(check_join_update_conflict())) {
    } else if (OB_FAIL(update_stmt->formalize_stmt(session_info_))) {
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_view_updatable())) {
    } else if (OB_FAIL(update_stmt->check_dml_need_filter_null())) {
    } else if (OB_FAIL(update_stmt->check_dml_source_from_join())) {
    } else if (OB_FAIL(check_safe_update_mode(update_stmt))) {
    } else { /*do nothing*/ }
  }
  return ret;
}


namespace {

// An ai column keeps three hidden companion columns: <col>__ai_state,
// <col>__ai_request and <col>__ai_result_spec. The model expression is stored in
// the comment of the result-spec column as
//   ai_expr=<text>\nsync_mode=<mode>\nmax_retries=<n>
// Updating a column the expression reads (or a rowkey column, the worker locates
// rows by primary key) invalidates the ai column: the cached result is cleared and
// the state is reset, so the asynchronous worker recomputes it.
static const char *AI_COLUMN_STATE_SUFFIX = "__ai_state";
static const char *AI_COLUMN_RESULT_SPEC_SUFFIX = "__ai_result_spec";
static const char *AI_COLUMN_EXPR_PREFIX = "ai_expr=";

static int64_t find_bytes(const char *buf, int64_t len, const char *pattern)
{
  int64_t pos = -1;
  const int64_t pattern_len = static_cast<int64_t>(strlen(pattern));
  for (int64_t i = 0; -1 == pos && i + pattern_len <= len; ++i) {
    if (0 == strncmp(buf + i, pattern, pattern_len)) {
      pos = i;
    }
  }
  return pos;
}

static bool is_ident_char(char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
         || '_' == c || '$' == c;
}

// whether the stored ai expression references the given column
static bool ai_expr_reads_column(const char *expr, int64_t len, const ObString &column_name)
{
  bool found = false;
  char ident[OB_MAX_COLUMN_NAME_LENGTH + 1];
  for (int64_t i = 0; !found && i < len; ) {
    const char c = expr[i];
    if ('\'' == c || '"' == c || '`' == c) {
      const char quote = c;
      ++i;
      while (i < len && expr[i] != quote) {
        if ('\\' == expr[i]) {
          ++i;
        }
        ++i;
      }
      ++i;
    } else if (is_ident_char(c) && (c < '0' || c > '9')) {
      int64_t ident_len = 0;
      while (i < len && is_ident_char(expr[i]) && ident_len < OB_MAX_COLUMN_NAME_LENGTH) {
        ident[ident_len++] = expr[i++];
      }
      while (i < len && is_ident_char(expr[i])) {
        ++i;
      }
      int64_t next = i;
      while (next < len && isspace(static_cast<unsigned char>(expr[next]))) {
        ++next;
      }
      const bool is_call = (next < len && '(' == expr[next]);
      if (!is_call && ident_len > 0
          && 0 == ObString(static_cast<int32_t>(ident_len), ident).case_compare(column_name)) {
        found = true;
      }
    } else {
      ++i;
    }
  }
  return found;
}

} // namespace

int ObUpdateResolver::invalidate_ai_columns(ObTableAssignment &table_assign)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *stmt = get_update_stmt();
  const TableItem *table_item = NULL;
  const ObTableSchema *table_schema = NULL;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolver is not ready", K(ret), K(stmt), K(schema_checker_), K(expr_factory));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_assign.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item not found", K(ret), K(table_assign.table_id_));
  } else if (!table_item->is_basic_table()) {
    // the ai columns of a view live on its base tables
  } else if (OB_FAIL(schema_checker_->get_table_schema(table_item->ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_item->ref_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_item->ref_id_));
  } else {
    for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
         OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
      const ObColumnSchemaV2 *state_column = *iter;
      const ObString state_name = (NULL == state_column)
                                       ? ObString() : state_column->get_column_name_str();
      const int64_t state_suffix_len = static_cast<int64_t>(strlen(AI_COLUMN_STATE_SUFFIX));
      if (NULL == state_column || !state_column->is_hidden()
          || state_name.length() <= state_suffix_len) {
        continue;
      } else if (0 != ObString(state_suffix_len,
                               state_name.ptr() + state_name.length() - state_suffix_len)
                          .case_compare(AI_COLUMN_STATE_SUFFIX)) {
        continue;
      }
      const ObString ai_name(state_name.length() - static_cast<int32_t>(state_suffix_len),
                             state_name.ptr());
      const ObColumnSchemaV2 *result_column = table_schema->get_column_schema(ai_name);
      char spec_name_buf[OB_MAX_COLUMN_NAME_LENGTH + 1];
      MEMSET(spec_name_buf, 0, sizeof(spec_name_buf));
      const int64_t spec_len = snprintf(spec_name_buf, sizeof(spec_name_buf), "%.*s%s",
                                        ai_name.length(), ai_name.ptr(),
                                        AI_COLUMN_RESULT_SPEC_SUFFIX);
      const ObColumnSchemaV2 *spec_column = NULL;
      if (OB_ISNULL(result_column)) {
        // not a regular ai column
      } else if (spec_len <= 0 || spec_len > OB_MAX_COLUMN_NAME_LENGTH) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ai column spec name is too long", K(ret), K(spec_len));
      } else if (OB_ISNULL(spec_column = table_schema->get_column_schema(
                               ObString(static_cast<int32_t>(spec_len), spec_name_buf)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ai column has no result spec column", K(ret), K(ai_name));
      } else {
        const ObString &comment = spec_column->get_comment_str();
        const int64_t expr_prefix =
            find_bytes(comment.ptr(), comment.length(), AI_COLUMN_EXPR_PREFIX);
        const char *expr_begin = NULL;
        int64_t expr_len = 0;
        if (expr_prefix < 0) {
          continue;
        } else {
          expr_begin = comment.ptr() + expr_prefix + strlen(AI_COLUMN_EXPR_PREFIX);
          int64_t expr_end = expr_prefix + static_cast<int64_t>(strlen(AI_COLUMN_EXPR_PREFIX));
          while (expr_end < comment.length() && '\n' != comment.ptr()[expr_end]) {
            ++expr_end;
          }
          expr_len = expr_end - (expr_prefix + strlen(AI_COLUMN_EXPR_PREFIX));
        }
        bool touched = false;
        bool user_sets_result = false;
        for (int64_t i = 0; OB_SUCC(ret) && !user_sets_result && i < table_assign.assignments_.count(); ++i) {
          const ObAssignment &assign = table_assign.assignments_.at(i);
          const ObColumnSchemaV2 *column = NULL;
          if (OB_ISNULL(assign.column_expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("assignment has no column", K(ret));
          } else if (OB_ISNULL(column = table_schema->get_column_schema(
                                   assign.column_expr_->get_column_id()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("assignment column not found", K(ret),
                     K(assign.column_expr_->get_column_id()));
          } else if (column->get_column_id() == result_column->get_column_id()) {
            user_sets_result = true;
          } else if (!touched
                     && (column->is_rowkey_column() || column->is_part_key_column()
                         || ai_expr_reads_column(expr_begin, expr_len,
                                                 column->get_column_name_str()))) {
            touched = true;
          }
        }
        if (OB_SUCC(ret) && touched && !user_sets_result) {
          ObSEArray<ObColumnRefRawExpr *, 2> result_exprs;
          ObSEArray<ObColumnRefRawExpr *, 2> state_exprs;
          ObRawExpr *null_expr = NULL;
          ObConstRawExpr *zero_expr = NULL;
          if (OB_FAIL(add_column_to_stmt(*table_item, *result_column, result_exprs))) {
            LOG_WARN("failed to add the ai result column to stmt", K(ret));
          } else if (OB_FAIL(add_column_to_stmt(*table_item, *state_column, state_exprs))) {
            LOG_WARN("failed to add the ai state column to stmt", K(ret));
          } else if (result_exprs.count() != 1 || state_exprs.count() != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected column count", K(ret), K(result_exprs.count()),
                     K(state_exprs.count()));
          } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*expr_factory, null_expr))) {
            LOG_WARN("failed to build the null expression", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                         *expr_factory, ObIntType, 0, zero_expr))) {
            LOG_WARN("failed to build the ai state expression", K(ret));
          } else {
            ObAssignment result_assign;
            ObAssignment state_assign;
            result_assign.column_expr_ = result_exprs.at(0);
            result_assign.expr_ = null_expr;
            result_assign.is_implicit_ = true;
            state_assign.column_expr_ = state_exprs.at(0);
            state_assign.expr_ = zero_expr;
            state_assign.is_implicit_ = true;
            if (OB_FAIL(table_assign.assignments_.push_back(result_assign))) {
              LOG_WARN("failed to invalidate the ai result column", K(ret));
            } else if (OB_FAIL(table_assign.assignments_.push_back(state_assign))) {
              LOG_WARN("failed to reset the ai state column", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::try_add_remove_const_expr_for_assignments()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_update_stmt()) ||OB_ISNULL(session_info_) ||
      OB_ISNULL(schema_checker_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObIArray<ObUpdateTableInfo*> &tables_info = stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); i++) {
      const ObTableSchema *table_schema = NULL;
      if (OB_ISNULL(tables_info.at(i))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(schema_checker_->get_table_schema(
                                                           tables_info.at(i)->ref_table_id_,
                                                           table_schema))) {
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (!table_schema->is_user_table()) {
        /*do nothing*/
      } else {
        const ObIArray<ObForeignKeyInfo> &fk_infos = table_schema->get_foreign_key_infos();
        ObIArray<ObAssignment> &assignments = tables_info.at(i)->assignments_;
        for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
          ObAssignment &assign = assignments.at(i);
          if (OB_ISNULL(assign.expr_) || OB_ISNULL(assign.column_expr_)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (assign.expr_->is_const_expr() &&
                     is_parent_col_self_ref_fk(assign.column_expr_->get_column_id(), fk_infos)) {
            ObRawExpr *new_expr = NULL;
            if (OB_FAIL(ObRawExprUtils::build_remove_const_expr(*params_.expr_factory_,
                                                                *session_info_,
                                                                 assign.expr_,
                                                                 new_expr))) {
            } else if (OB_ISNULL(new_expr)) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              assign.expr_ = new_expr;
            }
          }
        }
      }
    }
  }
  return ret;
}

// check if %parent_col_id is parent column of a self reference foreign key
bool ObUpdateResolver::is_parent_col_self_ref_fk(uint64_t parent_col_id,
                                                const ObIArray<ObForeignKeyInfo> &fk_infos)
{
  bool bret = false;
  for (int64_t i = 0; i < fk_infos.count(); i++) {
    const ObForeignKeyInfo &fk_info = fk_infos.at(i);
    if (fk_info.parent_table_id_ == fk_info.child_table_id_) {
      if (has_exist_in_array(fk_info.parent_column_ids_, parent_col_id, NULL)) {
        bret = true;
        break;
      }
    }
  }
  return bret;
}

int ObUpdateResolver::check_safe_update_mode(ObUpdateStmt *update_stmt)
{
  int ret = OB_SUCCESS;
  bool is_sql_safe_updates = false;
  if (OB_ISNULL(params_.session_info_) || OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(params_.session_info_->get_sql_safe_updates(is_sql_safe_updates))) {
  } else if (is_sql_safe_updates) {
    /*Update table values in mysql safe mode, needs to meet one of the following two conditions:
    * 1.contains limit;
    * 2.contains where condition, which can extract query range ==> Since extracting query range can only be done during the optimizer phase, therefore here we only
    *   check if there exists a where condition;
    */
    if (!update_stmt->has_limit() && update_stmt->get_condition_exprs().empty()) {
      ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
    }
  } else {/*do nothing*/}
  return ret;
}

int ObUpdateResolver::check_multi_update_table_conflict()
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt *update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (update_stmt->get_update_table_info().count() <= 1) {
    /*do nothing*/
  } else {
    const ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count() - 1; ++i) {
      if (OB_ISNULL(tables_info.at(i))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < tables_info.count(); ++j) {
          if (OB_ISNULL(tables_info.at(j))) {
            ret = OB_ERR_UNEXPECTED;
          } else if (tables_info.at(i)->table_id_ != tables_info.at(j)->table_id_ &&
                     tables_info.at(i)->ref_table_id_ == tables_info.at(j)->ref_table_id_) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple aliases to same table");
          }
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::check_join_update_conflict()
{
  int ret = OB_SUCCESS;
  bool is_join_update = false;
  ObUpdateStmt *update_stmt = NULL;
  if (OB_ISNULL(update_stmt = get_update_stmt())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(is_join_table_update(update_stmt, is_join_update))) {
  } else if (is_join_update && update_stmt->has_order_by()) {
    // Incorrect usage of UPDATE and ORDER BY
    ret = OB_ERR_UPDATE_ORDER_BY;
  } else if (is_join_update && update_stmt->has_limit()) {
    // Incorrect usage of UPDATE and LIMIT
    ret = OB_ERR_UPDATE_LIMIT;
  }
  return ret;
}

int ObUpdateResolver::resolve_table_list(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = get_update_stmt();
  TableItem *table_item = NULL;
  ObSelectStmt *ref_stmt = NULL;
  if (OB_UNLIKELY(T_TABLE_REFERENCES != parse_tree.type_)
      || OB_UNLIKELY(parse_tree.num_child_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid update stmt", K(update_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
    const ParseNode *table_node = parse_tree.children_[i];
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ObDMLResolver::resolve_table(*table_node, table_item))) {
    } else {/*do nothing*/}
    if (OB_SUCC(ret)) {
      if (OB_FAIL(column_namespace_checker_.add_reference_table(table_item))) {
      } else if (OB_FAIL(update_stmt->add_from_item(table_item->table_id_, table_item->is_joined_table()))) {
      } else if (OB_FAIL(check_need_fired_trigger(table_item))) {
      } else {
      /*
        In order to share the same logic with 'select' to generate access path costly, we
        add the table in the udpate stmt in the from_item list as well.
       */
      }
    }
  }
  if (OB_SUCC(ret) && 1 == update_stmt->get_from_item_size()) {
    const TableItem *table_item = update_stmt->get_table_item(update_stmt->get_from_item(0));
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (table_item->cte_type_ != TableItem::NOT_CTE) {
      ret = OB_ERR_NON_UPDATABLE_TABLE;
      const ObString &table_name = table_item->alias_name_.empty() ? table_item->table_name_ : table_item->alias_name_;
      ObString scope_name = "UPDATE";
      LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
                      table_name.length(), table_name.ptr(),
                      scope_name.length(), scope_name.ptr());
    }
  }
  return ret;
}

int ObUpdateResolver::generate_update_table_info(ObTableAssignment &table_assign)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObUpdateStmt *update_stmt = get_update_stmt();
  const ObTableSchema *table_schema = NULL;
  const TableItem *table_item = NULL;
  ObUpdateTableInfo *table_info = NULL;
  uint64_t index_tid[OB_MAX_AUX_TABLE_PER_MAIN_TABLE];
  int64_t gindex_cnt = OB_MAX_AUX_TABLE_PER_MAIN_TABLE;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(params_.session_info_) ||
      OB_ISNULL(allocator_) || OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(table_item = update_stmt->get_table_item_by_id(table_assign.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(schema_checker_->get_table_schema(
                                                       table_item->get_base_table_item().ref_id_,
                                                       table_schema))) {
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(schema_checker_->get_can_write_index_array(table_item->get_base_table_item().ref_id_,
                                                                index_tid, gindex_cnt, true))) {
  } else if (OB_FAIL(params_.session_info_->get_binlog_row_image(binlog_row_image))) {
  } else if (NULL == (ptr = allocator_->alloc(sizeof(ObUpdateTableInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    table_info = new(ptr) ObUpdateTableInfo();
    if (OB_FAIL(table_info->assignments_.assign(table_assign.assignments_))) {
    } else if (OB_FAIL(table_info->part_ids_.assign(table_item->get_base_table_item().part_ids_))) {
    } else {
      if (OB_FAIL(add_all_rowkey_columns_to_stmt(*table_item, table_info->column_exprs_))) {
      } else if (need_all_columns(*table_schema, binlog_row_image)) {
        if (OB_FAIL(add_all_columns_to_stmt(*table_item, table_info->column_exprs_))) {
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_assign.assignments_.count(); ++i) {
          ObAssignment &assign = table_assign.assignments_.at(i);
          if (OB_FAIL(add_var_to_array_no_dup(table_info->column_exprs_, assign.column_expr_))) {
          } else if (OB_FAIL(add_index_related_columns_to_stmt(*table_item,
                             assign.column_expr_->get_column_id(), table_info->column_exprs_))) {
          } else { /*do nothing*/ }
        }
      }
      if (OB_SUCC(ret)) {
        table_info->table_id_ = table_item->table_id_;
        table_info->loc_table_id_ = table_item->get_base_table_item().table_id_;
        table_info->ref_table_id_ = table_item->get_base_table_item().ref_id_;
        table_info->table_name_ = table_schema->get_table_name_str();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_stmt->get_update_table_info().push_back(table_info))) {
      } else if (gindex_cnt > 0) {
        update_stmt->set_has_global_index(true);
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObUpdateResolver::check_view_updatable()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = get_update_stmt();
  if (NULL == update_stmt) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); i++) {
      const TableItem *table_item = NULL;
      if (OB_ISNULL(tables_info.at(i)) ||
          OB_ISNULL(table_item = update_stmt->get_table_item_by_id(tables_info.at(i)->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (!table_item->is_generated_table() && !table_item->is_temp_table()) {
        continue;
      } else {
        // check select item subquery
        if (OB_SUCC(ret)) {
          bool has_subquery = false;
          bool has_dependent_subquery = false;
          bool ref_update_table = false;
          if (OB_FAIL(ObResolverUtils::uv_check_select_item_subquery(
              *table_item, has_subquery, has_dependent_subquery, ref_update_table))) {
          } else {
            ret = has_dependent_subquery ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret)) {
          bool ref_update_table = false;
          if (OB_FAIL(ObResolverUtils::uv_check_where_subquery(*table_item, ref_update_table))) {
          } else {
            ret = ref_update_table ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
          }
        }

        if (OB_SUCC(ret)) {
          bool has_non_inner_join = false;
          if (OB_FAIL(ObResolverUtils::uv_check_has_non_inner_join(*table_item, has_non_inner_join))) {
          } else {
            ret = has_non_inner_join ? OB_ERR_NON_UPDATABLE_TABLE : OB_SUCCESS;
          }
        }

        if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
          ObString upd_str = "UPDATE";
          LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
              table_item->get_table_name().length(), table_item->get_table_name().ptr(),
              upd_str.length(), upd_str.ptr());
        }
      }
    }
  }
  return ret;
}

int ObUpdateResolver::is_join_table_update(const ObDMLStmt *stmt, bool &is_multi_table)
{
  int ret = OB_SUCCESS;
  is_multi_table = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (stmt->get_from_item_size() > 1) {
    is_multi_table = true;
  } else {
    const TableItem *table_item = stmt->get_table_item(stmt->get_from_item(0));
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (table_item->is_joined_table()) {
      is_multi_table = true;
    } else if ((table_item->is_generated_table() || table_item->is_temp_table()) &&
               OB_FAIL(is_join_table_update(table_item->ref_query_, is_multi_table))) {
    }
  }
  return ret;
}

int ObUpdateResolver::generate_batched_stmt_info()
{
  int ret = OB_SUCCESS;
  //extract all predicate column from condition exprs
  //see the issue: 
  ObSEArray<ObRawExpr*, 4> predicate_columns;
  ObUpdateStmt *update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(update_stmt->get_condition_exprs(),
                                                          predicate_columns))) {
  } else {
    ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); ++i) {
      if (OB_ISNULL(tables_info.at(i))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObIArray<ObAssignment> &assignments = tables_info.at(i)->assignments_;
        for (int64_t j = 0; OB_SUCC(ret) && j < assignments.count(); ++j) {
          ObAssignment &assignment = assignments.at(j);
          ObRawExpr *column_expr = assignment.column_expr_;
          bool contain_case_when = false;
          if (has_exist_in_array(predicate_columns, column_expr)) {
            assignment.is_predicate_column_ = true;
          } else if (OB_FAIL(ObRawExprUtils::check_contain_case_when_exprs(assignment.expr_,
                                                                           contain_case_when))) {
          } else if (contain_case_when) {
            ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
          }
        }

      }
    }
  }
  return ret;
}

int ObUpdateResolver::resolve_update_constraints()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *update_stmt = get_update_stmt();
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObIArray<ObUpdateTableInfo*> &tables_info = update_stmt->get_update_table_info();
    // resolve view-check exprs
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_info.count(); i++) {
      TableItem *table_item = NULL;
      ObUpdateTableInfo* table_info = tables_info.at(i);
      if (OB_ISNULL(table_info) ||
          OB_ISNULL(table_item = update_stmt->get_table_item_by_id(table_info->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(resolve_view_check_exprs(table_item->table_id_, table_item, false, table_info->view_check_exprs_))) {
      } else if (OB_FAIL(resolve_check_constraints(table_item, table_info->check_constraint_exprs_))) {
      } else if (OB_FAIL(ObResolverUtils::prune_check_constraints(table_info->assignments_,
                                                                  table_info->check_constraint_exprs_))) {
      } else {
        // TODO @yibo remove view check exprs in log_del_upd
        for (uint64_t j = 0; OB_SUCC(ret) && j < table_info->view_check_exprs_.count(); ++j) {
          if (OB_FAIL(ObTableAssignment::expand_expr(*params_.expr_factory_,
                                                     table_info->assignments_,
                                                     table_info->view_check_exprs_.at(j)))) {
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
