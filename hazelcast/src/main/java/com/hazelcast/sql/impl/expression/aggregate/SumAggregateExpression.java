/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Summing accumulator.
 */
public class SumAggregateExpression<T> extends AbstractSingleOperandAggregateExpression<T> {
    public SumAggregateExpression() {
        // No-op.
    }

    private SumAggregateExpression(Expression<?> operand, QueryDataType resType, boolean distinct) {
        super(operand, resType, distinct);
    }

    public static SumAggregateExpression<?> create(Expression<?> operand, boolean distinct) {
        QueryDataType resType = inferResultType(operand.getType());

        return new SumAggregateExpression<>(operand, resType, distinct);
    }

    @Override
    public AggregateCollector newCollector(QueryFragmentContext ctx) {
        return new SumAggregateCollector(resType, distinct);
    }

    @Override
    protected boolean isIgnoreNull() {
        return true;
    }

    private static QueryDataType inferResultType(QueryDataType operandType) {
        switch (operandType.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
                return QueryDataType.INT;

            case INT:
            case BIGINT:
                return QueryDataType.BIGINT;

            case DECIMAL:
                return QueryDataType.DECIMAL;

            case REAL:
            case DOUBLE:
                return QueryDataType.DOUBLE;

            default:
                throw QueryException.error("Unsupported operand type: " + operandType);
        }
    }
}
