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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class CharLengthFunction extends UniExpression<Integer> implements IdentifiedDataSerializable {
    public CharLengthFunction() {
        // No-op.
    }

    private CharLengthFunction(Expression<?> operand) {
        super(operand);
    }

    public static CharLengthFunction create(Expression<?> operand) {
        return new CharLengthFunction(operand);
    }

    @Override
    public Integer eval(Row row, ExpressionEvalContext context) {
        String value = StringFunctionUtils.asVarchar(operand, row, context);

        return StringFunctionUtils.charLength(value);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.INT;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_CHAR_LENGTH;
    }
}
