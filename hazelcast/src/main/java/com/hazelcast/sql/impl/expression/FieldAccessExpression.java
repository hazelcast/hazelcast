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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

public class FieldAccessExpression<T> implements Expression<T>, IdentifiedDataSerializable {
    private QueryDataType type;
    private String name;
    private Expression<?> ref;

    public FieldAccessExpression() { }

    private FieldAccessExpression(
            final QueryDataType type,
            final String name,
            final Expression<?> ref
    ) {
        this.type = type;
        this.name = name;
        this.ref = ref;
    }

    public static FieldAccessExpression<?> create(
            final QueryDataType type,
            final String name,
            final Expression<?> ref
    ) {
        return new FieldAccessExpression<>(type, name, ref);
    }


    @Override
    public T eval(final Row row, final ExpressionEvalContext context) {
        Object res = ref.eval(row, context);
        if (isPrimitive(res.getClass())) {
            throw QueryException.error("Field Access expression can not be applied to primitive types");
        }

        try {
            return (T) type.convert(ReflectionUtils.getFieldValue(name, res));
        } catch (Exception e) {
            throw QueryException.error("Failed to extract field");
        }
    }

    private boolean isPrimitive(Class<?> clazz) {
        return clazz.getPackage().getName().startsWith("java.");
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_FIELD_ACCESS;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeObject(type);
        out.writeString(name);
        out.writeObject(ref);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        type = in.readObject();
        name = in.readString();
        ref = in.readObject();
    }

    @Override
    public QueryDataType getType() {
        return type;
    }
}
