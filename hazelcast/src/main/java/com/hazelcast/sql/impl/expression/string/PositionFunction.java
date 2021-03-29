/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

public class PositionFunction extends BiExpression<Integer> implements IdentifiedDataSerializable {
    public PositionFunction() { }

    private PositionFunction(Expression<?> text, Expression<?> search) {
        super(text, search);
    }

    public static PositionFunction create(Expression<?> search, Expression<?> text) {
        return new PositionFunction(text, search);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_POSITION;
    }

    @Override
    public Integer eval(Row row, ExpressionEvalContext context) {
        String text = StringFunctionUtils.asVarchar(operand1, row, context);
        if (text == null) {
            return null;
        }
        String search = StringFunctionUtils.asVarchar(operand2, row, context);
        if (search == null) {
            return null;
        }
        return StringFunctionUtils.search(text, search, 0);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.INT;
    }
}
