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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class NullIfExpression<T> extends BiExpression<T> implements IdentifiedDataSerializable {
    public NullIfExpression() {
    }

    public static NullIfExpression<?> create(Expression<?> left, Expression<?> right) {
        return new NullIfExpression(left, right);
    }

    private NullIfExpression(Expression<T> left, Expression<T> right) {
        super(left, right);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_NULLIF;
    }

    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        T leftResult = (T) operand1.eval(row, context);
        T rightResult = (T) operand2.eval(row, context);
        if (leftResult.equals(rightResult)) {
            return null;
        }
        return leftResult;
    }

    @Override
    public QueryDataType getType() {
        return operand1.getType();
    }
}
