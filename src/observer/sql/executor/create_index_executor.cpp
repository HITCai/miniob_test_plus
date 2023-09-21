/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/4/25.
//

#include "sql/executor/create_index_executor.h"
#include "sql/stmt/create_index_stmt.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "session/session.h"
#include "common/log/log.h"
#include "storage/table/table.h"

RC CreateIndexExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::CREATE_INDEX, 
         "create index executor can not run this command: %d", static_cast<int>(stmt->type()));

  CreateIndexStmt *create_index_stmt = static_cast<CreateIndexStmt *>(stmt);
  //将 SQL 语句对象转换为创建索引语句对象
  
  Trx *trx = session->current_trx();
  //从会话中获取当前事务（Trx）对象，以便执行索引创建操作
  Table *table = create_index_stmt->table();
  //调用表格对象的 create_index 方法，传递了事务对象、字段元信息和索引名称。这个方法用于执行创建索引的具体操作，并返回一个表示执行结果的返回码（RC）。
  //RC 是一个枚举类型，表示执行结果的状态，包括成功、失败、重复等。
  return table->create_index(trx, create_index_stmt->field_meta(), create_index_stmt->index_name().c_str());
}