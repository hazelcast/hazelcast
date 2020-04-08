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
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.util.Eval;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * POWER function.
 */
public class PowerFunction extends BiExpression<Double> {
    public PowerFunction() {
        // No-op.
    }

    private PowerFunction(Expression<?> first, Expression<?> second) {
        super(first, second);
    }

    public static PowerFunction create(Expression<?> first, Expression<?> second) {
        if (!MathFunctionUtils.canConvertToNumber(first.getType())) {
            throw QueryException.error("Operand 1 is not numeric: " + first.getType());
        }

        if (!MathFunctionUtils.canConvertToNumber(second.getType())) {
            throw QueryException.error("Operand 2 is not numeric: " + second.getType());
        }

        return new PowerFunction(first, second);
    }

    @Override
    public Double eval(Row row, ExpressionEvalContext context) {
        Double a = Eval.asDouble(operand1, row, context);
        Double b = Eval.asDouble(operand1, row, context);

        if (a == null || b == null) {
            return null;
        }

        return Math.pow(a, b);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.DOUBLE;
    }
}
