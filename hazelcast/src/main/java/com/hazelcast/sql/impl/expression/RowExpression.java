/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * An expression that creates a RowValue from a list of expressions.
 * <p>
 * (1, 2) - a row with two values. Each value can be an expression.
 */
public class RowExpression extends VariExpressionWithType<RowValue> implements IdentifiedDataSerializable {

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
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_ROW;
    }
}
