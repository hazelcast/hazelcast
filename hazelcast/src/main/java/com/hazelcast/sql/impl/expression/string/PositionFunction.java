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
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.expression.math.MathFunctionUtils;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class PositionFunction extends TriExpression<Integer> implements IdentifiedDataSerializable {
    // SQL index starts from 1
    static final int SQL_START_INDEX = 1;
    // SQL not found is 0
    static final int SQL_NOT_FOUND = 0;

    public PositionFunction() { }

    private PositionFunction(Expression<?> text, Expression<?> search, Expression<?> start) {
        super(text, search, start);
    }

    public static PositionFunction create(Expression<?> search, Expression<?> text, Expression<?> start) {
        return new PositionFunction(text, search, start);
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
        // If any argument is NULL, the function will return NULL.
        String text = StringFunctionUtils.asVarchar(operand1, row, context);
        if (text == null) {
            return null;
        }
        String search = StringFunctionUtils.asVarchar(operand2, row, context);
        if (search == null) {
            return null;
        }

        // In this case the `start` argument is not given, then search it from the beginning.
        if (operand3 == null) {
            return SQL_START_INDEX + StringFunctionUtils.search(text, search, 0);
        }

        Integer start = MathFunctionUtils.asInt(operand3, row, context);
        // In this case the `start` argument is given but it is NULL, then return NULL.
        if (start == null) {
            return null;
        }

        // Valid `start` argument is between [1,Text.length()].
        // Invalid argument will return 0 (NOT_FOUND).
        if (SQL_START_INDEX <= start && start <= text.length()) {
            // Convert SQL `start` index to Java index for passing as argument to function.
            // Convert back the result when it returns.
            return SQL_START_INDEX + StringFunctionUtils.search(text, search, start - SQL_START_INDEX);
        } else {
            return SQL_NOT_FOUND;
        }

    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.INT;
    }
}
