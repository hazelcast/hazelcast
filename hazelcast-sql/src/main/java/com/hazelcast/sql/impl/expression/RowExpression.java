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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * An expression that creates a RowValue from a list of expressions.
 * <p>
 * (1, 2) - a row with two values. Each value can be an expression.
 */
public class RowExpression extends VariExpressionWithType<RowValue> {

    public RowExpression() { }

    private RowExpression(Expression<?>[] operands) {
        super(operands, QueryDataType.ROW);
    }

    public static RowExpression create(Expression<?>[] operands) {
        return new RowExpression(operands);
    }

    @Override
    public RowValue eval(final Row row, final ExpressionEvalContext context) {
        final List<Object> values = new ArrayList<>();
        for (final Expression<?> operand : operands) {
            values.add(operand.eval(row, context));
        }

        return new RowValue(values);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.ROW;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_ROW;
    }
}
