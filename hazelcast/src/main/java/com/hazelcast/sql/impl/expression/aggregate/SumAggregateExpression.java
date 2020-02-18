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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryFragmentContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;

/**
 * Summing accumulator.
 */
public class SumAggregateExpression<T> extends AbstractSingleOperandAggregateExpression<T> {
    public SumAggregateExpression() {
        // No-op.
    }

    private SumAggregateExpression(Expression<?> operand, DataType resType, boolean distinct) {
        super(operand, resType, distinct);
    }

    public static SumAggregateExpression<?> create(Expression<?> operand, boolean distinct) {
        DataType resType = inferResultType(operand.getType());

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

    private static DataType inferResultType(DataType operandType) {
        switch (operandType.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
                return DataType.INT;

            case INT:
            case BIGINT:
                return DataType.BIGINT;

            case DECIMAL:
                return DataType.DECIMAL;

            case REAL:
            case DOUBLE:
                return DataType.DOUBLE;

            default:
                throw HazelcastSqlException.error("Unsupported operand type: " + operandType);
        }
    }
}
