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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.GenericType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;

/**
 * Summing accumulator.
 */
public class SumAggregateExpression<T> extends SingleAggregateExpression<T> {
    public SumAggregateExpression() {
        // No-op.
    }

    public SumAggregateExpression(boolean distinct, Expression operand) {
        super(distinct, operand);
    }

    @Override
    public AggregateCollector newCollector(QueryContext ctx) {
        return new Collector(distinct);
    }

    @Override
    protected DataType resolveReturnType(DataType operandType) {
        switch (operandType.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
                return DataType.INT;

            case BIGINT:
                return DataType.BIGINT;

            case DECIMAL:
                return DataType.DECIMAL;

            case REAL:
            case DOUBLE:
                return DataType.DOUBLE;

            default:
                throw new HazelcastSqlException(-1, "Unsupported operand type: " + operandType);
        }
    }

    /**
     * Summing collector.
     */
    private final class Collector extends AggregateCollector {
        /** Result. */
        private Object res;

        private Collector(boolean distinct) {
            super(distinct);
        }

        @Override
        protected void collect0(Object value) {
            if (res == null) {
                reset();
            }

            Converter converter = operandType.getConverter();

            switch (resType.getType()) {
                case INT:
                    res = (int) res + converter.asInt(value);

                    break;

                case BIGINT:
                    res = (long) res + converter.asBigInt(value);

                    break;

                case DECIMAL:
                    res = ((BigDecimal) res).add(converter.asDecimal(value));

                    break;

                default:
                    assert resType.getType() == GenericType.DOUBLE;

                    res = (double) res + converter.asDouble(value);
            }
        }

        @Override
        public Object reduce() {
            return res;
        }

        @Override
        public void reset() {
            switch (resType.getType()) {
                case INT:
                    res = 0;

                    break;

                case BIGINT:
                    res = 0;

                    break;

                case DECIMAL:
                    res = BigDecimal.ZERO;

                    break;

                default:
                    assert resType.getType() == GenericType.DOUBLE;

                    res = 0.0d;
            }
        }
    }
}
