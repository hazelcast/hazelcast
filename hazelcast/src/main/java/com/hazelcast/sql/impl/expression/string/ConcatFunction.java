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
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import static com.hazelcast.sql.impl.expression.string.StringFunctionUtils.asVarchar;

/**
 * A function which accepts a string, and return another string.
 */
public class ConcatFunction extends BiExpression<String> implements IdentifiedDataSerializable {
    public ConcatFunction() {
        // No-op.
    }

    private ConcatFunction(Expression<?> first, Expression<?> second) {
        super(first, second);
    }

    public static ConcatFunction create(Expression<?> first, Expression<?> second) {
        return new ConcatFunction(first, second);
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        String first = asVarchar(operand1, row, context);
        String second = asVarchar(operand2, row, context);

        return StringFunctionUtils.concat(first, second);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_CONCAT;
    }
}
