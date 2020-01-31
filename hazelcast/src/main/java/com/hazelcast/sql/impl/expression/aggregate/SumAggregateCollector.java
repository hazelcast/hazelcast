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

import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.GenericType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;

/**
 * Summing collector.
 */
public final class SumAggregateCollector extends AggregateCollector {
    /** Result type. */
    private final DataType resType;

    /** Result. */
    private Object res;

    public SumAggregateCollector(DataType resType, boolean distinct) {
        super(distinct);

        this.resType = resType;
    }

    @Override
    protected void collect0(Object operandValue, DataType operandType) {
        if (res == null) {
            initialize();
        }

        Converter converter = operandType.getConverter();

        switch (resType.getType()) {
            case INT:
                res = (int) res + converter.asInt(operandValue);

                break;

            case BIGINT:
                res = (long) res + converter.asBigint(operandValue);

                break;

            case DECIMAL:
                res = ((BigDecimal) res).add(converter.asDecimal(operandValue));

                break;

            default:
                assert resType.getType() == GenericType.DOUBLE;

                res = (double) res + converter.asDouble(operandValue);
        }
    }

    @Override
    public Object reduce() {
        return res;
    }

    private void initialize() {
        switch (resType.getType()) {
            case INT:
                res = 0;

                break;

            case BIGINT:
                res = 0L;

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
