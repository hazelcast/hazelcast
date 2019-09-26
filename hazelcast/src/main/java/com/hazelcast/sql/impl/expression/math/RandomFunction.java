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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random function implementation.
 */
public class RandomFunction extends UniCallExpression<Double> {
    /** Seed value type. */
    private transient DataType seedType;

    public RandomFunction() {
        // No-op.
    }

    public RandomFunction(Expression seedExp) {
        super(seedExp);
    }

    @Override
    public Double eval(QueryContext ctx, Row row) {
        Random random = getRandom(ctx, row);

        return random.nextDouble();
    }

    /**
     * Get random generator.
     *
     * @param ctx Context.
     * @param row Row.
     * @return Random generator.
     */
    @SuppressWarnings("checkstyle:NestedIfDepth")
    private Random getRandom(QueryContext ctx, Row row) {
        Expression seedExp = operand;

        if (seedExp != null) {
            Object seedRes0 = seedExp.eval(ctx, row);

            if (seedRes0 != null) {
                if (seedType == null) {
                    DataType type = seedExp.getType();

                    if (!type.isCanConvertToNumeric()) {
                        throw new HazelcastSqlException(-1, "Seed is not numeric: " + seedExp);
                    }

                    seedType = type;
                }

                int seedRes = seedType.getConverter().asInt(seedRes0);

                return new Random(seedRes);
            }
        }

        return ThreadLocalRandom.current();
    }

    @Override
    public DataType getType() {
        return DataType.DOUBLE;
    }

    @Override
    public int operator() {
        return CallOperator.RAND;
    }
}
