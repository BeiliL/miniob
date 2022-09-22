/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/common/db.h"
#include "storage/common/table.h"
UpdateStmt::UpdateStmt(Table *table, const char *attribute_name , const Value value, int value_amount , FilterStmt *filter_stmt)
  : table_ (table), attribute_name_(attribute_name), value_(value), value_amount_(value_amount) , filter_stmt_(filter_stmt)
{}

RC UpdateStmt::create(Db *db, const Updates &update, Stmt *&stmt)
{
  // TODO
  if(nullptr == db){
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }
  Table *defaultTable = nullptr;
  const char * table_name = update.relation_name;

  if (nullptr == table_name)
  {
    LOG_WARN("invalid argument. relation name is null.");
    return RC::INVALID_ARGUMENT;
  }
  else
  {
    defaultTable = db->find_table(table_name);
    if (nullptr == defaultTable)
    {
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
  }

  const char * field_name = update.attribute_name;
  if (nullptr == field_name)
  {
    LOG_WARN("invalid argument. attribute name is null.");
    return RC::INVALID_ARGUMENT;
  }
  else
  {
    const FieldMeta * field =  defaultTable->table_meta().field(field_name);
    if (nullptr ==field)
    {
      LOG_WARN("no such field. field=%s.%s.%s", db->name(), defaultTable->name(), field_name);
      return RC::SCHEMA_FIELD_MISSING;
    }
    AttrType fieldType = field->type();
    AttrType valueType = update.value.type;
    if (fieldType != valueType)
    {
      LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d", 
               table_name, field->name(), fieldType, valueType);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }
  std::unordered_map<std::string , Table*> tables;
  tables.insert(std::make_pair(std::string(table_name) , defaultTable));
  
  FilterStmt *filterStmt = nullptr;
  RC rc = FilterStmt::create(db , defaultTable , &tables , update.conditions , update.condition_num , filterStmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  UpdateStmt *updatestmt = new UpdateStmt(defaultTable ,update.attribute_name ,update.value , 1 , filterStmt);
  stmt = updatestmt;
  return RC::SUCCESS;
}
