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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class AsciiFunction extends UniExpression<Integer> {
    public AsciiFunction() {
        // No-op.
    }

    private AsciiFunction(Expression<?> operand) {
        super(operand);
    }

    public static AsciiFunction create(Expression<?> operand) {
        return new AsciiFunction(operand);
    }

    @Override
    public Integer eval(Row row, ExpressionEvalContext context) {
        String value = StringFunctionUtils.asVarchar(operand, row, context);

        return StringFunctionUtils.ascii(value);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.INT;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_ASCII;
    }
}
