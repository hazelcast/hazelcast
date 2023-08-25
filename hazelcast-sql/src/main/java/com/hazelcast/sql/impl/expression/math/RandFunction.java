/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random function implementation.
 */
public class RandFunction extends UniExpression<Double> {
    public RandFunction() {
        // No-op.
    }

    public RandFunction(Expression<?> seedExp) {
        super(seedExp);
    }

    public static RandFunction create(Expression<?> seedExp) {
        return new RandFunction(seedExp);
    }

    @Override
    public Double eval(Row row, ExpressionEvalContext context) {
        Long seed = operand != null ? MathFunctionUtils.asBigint(operand, row, context) : null;

        Random random = seed != null ? new Random(seed) : ThreadLocalRandom.current();

        return random.nextDouble();
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.DOUBLE;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_RAND;
    }
}
