/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;

/**
 * Special aggregate expression with expects only a single value for a group. If more than one value appears, an exception is
 * thrown.
 */
public class SingleValueAggregateExpression<T> extends AbstractSingleOperandAggregateExpression<T> {
    public SingleValueAggregateExpression() {
        // No-op.
    }

    public SingleValueAggregateExpression(boolean distinct, Expression operand) {
        super(distinct, operand);
    }

    @Override
    protected boolean isIgnoreNull() {
        return false;
    }

    @Override
    protected DataType resolveReturnType(DataType operandType) {
        return operandType;
    }

    @Override
    public AggregateCollector newCollector(QueryContext ctx) {
        return new SingleValueAggregateCollector(distinct);
    }
}
