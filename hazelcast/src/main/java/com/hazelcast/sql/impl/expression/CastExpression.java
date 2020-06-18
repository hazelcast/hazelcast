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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

/**
 * Expression which converts data from one type to another.
 */
public class CastExpression<T> extends UniExpressionWithType<T> {

    @SuppressWarnings("unused")
    public CastExpression() {
        // No-op.
    }

    private CastExpression(Expression<?> operand, QueryDataType resultType) {
        super(operand, resultType);
    }

    public static CastExpression<?> create(Expression<?> operand, QueryDataType resultType) {
        return new CastExpression<>(operand, resultType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object value = operand.eval(row, context);

        if (value == null) {
            return null;
        }

        Converter fromConverter = operand.getType().getConverter();
        Converter toConverter = resultType.getConverter();
        return (T) toConverter.convertToSelf(fromConverter, value);
    }

}
