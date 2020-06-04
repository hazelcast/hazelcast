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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.util.Eval;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random function implementation.
 */
public class RandomFunction extends UniExpression<Double> {
    public RandomFunction() {
        // No-op.
    }

    public RandomFunction(Expression<?> seedExp) {
        super(seedExp);
    }

    public static RandomFunction create(Expression<?> seedExp) {
        if (seedExp != null && !MathFunctionUtils.canConvertToNumber(seedExp.getType())) {
            throw QueryException.error("Operand is not numeric: " + seedExp.getType());
        }

        return new RandomFunction(seedExp);
    }

    @Override
    public Double eval(Row row, ExpressionEvalContext context) {
        Long seed = operand != null ? Eval.asBigint(operand, row, context) : null;

        Random random = seed != null ? new Random(seed) : ThreadLocalRandom.current();

        return random.nextDouble();
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.DOUBLE;
    }
}
