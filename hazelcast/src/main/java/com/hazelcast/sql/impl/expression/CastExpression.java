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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.io.IOException;

/**
 * Expression which converts data from one type to another.
 */
public class CastExpression<T> extends UniExpression<T> {

    private QueryDataType type;

    @SuppressWarnings("unused")
    public CastExpression() {
        // No-op.
    }

    private CastExpression(Expression<?> operand, QueryDataType type) {
        super(operand);
        this.type = type;
    }

    public static CastExpression<?> create(Expression<?> operand, QueryDataType type) {
        boolean convertible = operand.getType().getConverter().canConvertTo(type.getTypeFamily());

        if (!convertible) {
            throw QueryException.error("Cannot convert " + operand.getType() + " to " + type);
        }

        return new CastExpression<>(operand, type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object value = operand.eval(row, context);

        if (value == null) {
            return null;
        }

        Converter valueConverter = operand.getType().getConverter();
        Converter typeConverter = type.getConverter();
        return (T) typeConverter.convertToSelf(valueConverter, value);
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    public static Expression<?> coerceExpression(Expression<?> from, QueryDataTypeFamily toTypeFamily) {
        QueryDataType fromType = from.getType();

        if (fromType.getTypeFamily() == toTypeFamily) {
            return from;
        } else {
            QueryDataType type = QueryDataTypeUtils.resolveTypeForTypeFamily(toTypeFamily);

            return CastExpression.create(from, type);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeObject(type);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        type = in.readObject();
    }

}
