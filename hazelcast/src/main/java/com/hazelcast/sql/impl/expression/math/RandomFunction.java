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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random function implementation.
 */
public class RandomFunction extends UniCallExpression<Double> {
    public RandomFunction() {
        // No-op.
    }

    public RandomFunction(Expression<?> seedExp) {
        super(seedExp);
    }

    public static RandomFunction create(Expression<?> seedExp) {
        if (seedExp != null && !seedExp.getType().isNumeric()) {
            throw HazelcastSqlException.error("Operand is not numeric: " + seedExp.getType());
        }

        return new RandomFunction(seedExp);
    }

    @Override
    public Double eval(Row row) {
        Long seed = operand != null ? operand.evalAsBigint(row) : null;

        Random random = seed != null ? new Random(seed) : ThreadLocalRandom.current();

        return random.nextDouble();
    }

    @Override
    public DataType getType() {
        return DataType.DOUBLE;
    }
}
