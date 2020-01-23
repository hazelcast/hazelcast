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
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;

/**
 * Local average expression.
 */
public class AverageAggregateExpression<T> extends AbstractSingleOperandAggregateExpression<T> {
    public AverageAggregateExpression() {
        // No-op.
    }

    public AverageAggregateExpression(boolean distinct, Expression operand) {
        super(distinct, operand);
    }

    @Override
    public AggregateCollector newCollector(QueryContext ctx) {
        return new AverageAggregateCollector(distinct);
    }

    @Override
    protected boolean isIgnoreNull() {
        return true;
    }

    @Override
    protected DataType resolveReturnType(DataType operandType) {
        return returnType(operandType);
    }

    // TODO: Read ANSI standard on how to infer return types here.
    public static DataType returnType(DataType operandType) {
        switch (operandType.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case DOUBLE:
                return DataType.DOUBLE;

            default:
                throw new HazelcastSqlException(-1, "Unsupported operand type: " + operandType);
        }
    }
}
