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
// Created by WangYunlai on 2021/6/10.
//

#pragma once

#include "sql/operator/operator.h"
// TODO fixme
class JoinOperator : public Operator
{
public:
  JoinOperator(Operator *left, Operator *right , bool same_operator)
    : left_(left),right_(right),same_operator_(same_operator)
  {}

  virtual ~JoinOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;
  JoinTuple* current_tuple() ;
private:
  Operator *left_ = nullptr;
  Operator *right_ = nullptr;
  JoinTuple join_tuple_;
  Tuple * temp_left = nullptr;
  bool round_done_ = true;
  bool same_operator_ = true;
};