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
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

/**
 * Aggregate accumulator which uses only a single input.
 */
public abstract class SingleAggregateExpression<T> extends AggregateExpression<T> {
    /** Operand type. */
    protected transient DataType operandType;

    /** Operand. */
    private Expression operand;

    public SingleAggregateExpression() {
        // No-op.
    }

    public SingleAggregateExpression(boolean distinct, Expression operand) {
        super(distinct);

        this.operand = operand;
    }

    @Override
    public void collect(QueryContext ctx, Row row, AggregateCollector collector) {
        Object operandValue = operand.eval(ctx, row);

        // Null operands are not processed.
        if (operandValue == null) {
            return;
        }

        // Resolve types.
        if (operandType == null) {
            operandType = operand.getType();

            resType = resolveReturnType(operandType);
        }

        collector.collect(operandValue);
    }

    /**
     * Resolve return type for the accumulator.
     *
     * @param operandType Operand type.
     * @return Return type.
     */
    protected abstract DataType resolveReturnType(DataType operandType);
}
