/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
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
public class RandFunction extends UniExpression<Double> implements IdentifiedDataSerializable {
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
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_RAND;
    }
}
