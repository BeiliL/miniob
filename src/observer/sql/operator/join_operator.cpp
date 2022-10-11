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
// Created by BeiliL on 2022/09/29.
//
#include "common/log/log.h"
#include "sql/operator/join_operator.h"
#include "rc.h"
RC JoinOperator::open()
{
    
    if (left_==nullptr ||right_ ==nullptr)
    {
        LOG_ERROR("There is no two real operators");
        return RC::INTERNAL;
    }
    RC rc =RC::SUCCESS;
    rc=left_->open();
    if (RC::SUCCESS !=rc)
    {
        LOG_ERROR("left opreator open failed");
        return rc;
    }
    rc=right_->open();
    if (RC::SUCCESS !=rc)
    {
        LOG_ERROR("right opreator open failed");
        return rc;
    }
    round_done_=true;
    temp_left=nullptr;
    return rc;
}

RC JoinOperator::next()
{
    if(same_operator_)
    {
        LOG_INFO("Begin to join tablescanop and tablescanop ");
        RC rc = RC::SUCCESS;
        JoinTuple join_tuple;
        Tuple *left_tuple_ = temp_left;
        if(round_done_)//第一次从左边拿一个
        {
            while (RC::SUCCESS == (rc = left_->next()))
            {
                left_tuple_ = left_->current_tuple();
                if (nullptr == left_tuple_) {
                    rc = RC::INTERNAL;
                    LOG_WARN("failed to get tuple from left operator");
                    break;
                }
                break;
            }
            if(RC::RECORD_EOF ==rc)
            {
                left_->close();
                right_->close();
                return RC::RECORD_EOF;
            }  
        }
        round_done_ = false;
        Tuple *right_tuple_ = nullptr;
        //尝试从右边拿一个
        while (RC::SUCCESS ==(rc = right_->next()))
        {
            right_tuple_ = right_->current_tuple();
            if (nullptr == right_tuple_) {
                rc = RC::INTERNAL;
                LOG_WARN("failed to get tuple from right operator");
                break;
            }
            break;  
        }
        //拿不到，尝试去左边拿一个
        if(RC::RECORD_EOF == rc){
            while (RC::SUCCESS == (rc = left_->next()))
            {
                left_tuple_ = left_->current_tuple();
                if (nullptr == left_tuple_) {
                    rc = RC::INTERNAL;
                    LOG_WARN("failed to get tuple from left operator");
                    break;
                }
                break;
            }
            if(RC::RECORD_EOF ==rc)//左边也拿不到 换上一级
            {
                left_->close();
                right_->close();
                return RC::RECORD_EOF;
            }  
            //左边拿到了，拿右边的
            right_->open();
            while (RC::SUCCESS ==(rc = right_->next()))
            {
                right_tuple_ = right_->current_tuple();
                if (nullptr == right_tuple_) {
                    rc = RC::INTERNAL;
                    LOG_WARN("failed to get tuple from right operator");
                    break;
                }
                break;  
            }       
        } 
        RowTuple * left_tuple = dynamic_cast<RowTuple *> (left_tuple_);
        RowTuple * right_tuple = dynamic_cast<RowTuple *> (right_tuple_);
        if (left_tuple ==nullptr || right_tuple == nullptr)
        {
            return RC::NOTFOUND;
        }
        join_tuple.push_tuple(left_tuple);
        join_tuple.push_tuple(right_tuple);
        join_tuple_ =join_tuple;
        temp_left = left_tuple;
        return RC::SUCCESS;
    }else{
        LOG_INFO("Begin to join joinop and tablescanop ");
        RC rc = RC::SUCCESS;
        JoinTuple join_tuple;
        Tuple *left_tuple_ = temp_left;
        if(round_done_)
        {
            while (RC::SUCCESS == (rc = left_->next()))
            {
                left_tuple_ = left_->current_tuple();
                if (nullptr == left_tuple_) {
                    rc = RC::INTERNAL;
                    LOG_WARN("failed to get tuple from left operator");
                    break;
                }
                break;
            }
            if(RC::RECORD_EOF ==rc)
            {
                left_->close();
                right_->close();
                return RC::RECORD_EOF;
            }
            
        }
        round_done_ = false;
        Tuple *right_tuple_ = nullptr;
        while (RC::SUCCESS ==(rc = right_->next()))
        {
            right_tuple_ = right_->current_tuple();
            if (nullptr == right_tuple_) {
                rc = RC::INTERNAL;
                LOG_WARN("failed to get tuple from right operator");
                break;
            }
            break;  
        }
        if(RC::RECORD_EOF == rc){
            while (RC::SUCCESS == (rc = left_->next()))
            {
                left_tuple_ = left_->current_tuple();
                if (nullptr == left_tuple_) {
                    rc = RC::INTERNAL;
                    LOG_WARN("failed to get tuple from left operator");
                    break;
                }
                break;
            }
            if(RC::RECORD_EOF ==rc)
            {
                left_->close();
                right_->close();
                return RC::RECORD_EOF;
            }  
            right_->open();
            while (RC::SUCCESS ==(rc = right_->next()))
            {
                right_tuple_ = right_->current_tuple();
                if (nullptr == right_tuple_) {
                    rc = RC::INTERNAL;
                    LOG_WARN("failed to get tuple from right operator");
                    break;
                }
                break;  
            }
        } 
        RowTuple * left_tuple = dynamic_cast<RowTuple *> (left_tuple_);
        JoinTuple * right_tuple = dynamic_cast<JoinTuple *> (right_tuple_);
        if (left_tuple ==nullptr || right_tuple == nullptr)
        {
            return RC::NOTFOUND;
        }
        join_tuple.push_tuple(left_tuple);
        join_tuple.add_tuples(right_tuple->tuples());
        join_tuple_ = join_tuple;
        temp_left = left_tuple;
        return RC::SUCCESS;
    }
}
JoinTuple* JoinOperator::current_tuple()
{
    return &join_tuple_;
}

RC JoinOperator::close()
{
    RC rc=left_->close();
    if (rc != RC::SUCCESS)
        return rc;
    rc = right_->close();
    return rc;
}