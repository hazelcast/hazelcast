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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

public class FieldAccessExpression<T> implements Expression<T>, IdentifiedDataSerializable {
    private int index;
    private QueryDataType type;
    private List<String> path;

    public FieldAccessExpression() { }

    private FieldAccessExpression(final int index, final QueryDataType type, final List<String> path) {
        this.index = index;
        this.type = type;
        this.path = path;
    }

    public static FieldAccessExpression<?> create(final int index, final QueryDataType type, final List<String> path) {
        return new FieldAccessExpression<>(index, type, path);
    }


    @Override
    public T eval(final Row row, final ExpressionEvalContext context) {
        final Object res = row.get(index);
        if (isPrimitive(res.getClass())) {
            throw QueryException.error("Field Access expression can not be applied to primitive types");
        }

        try {
            return (T) extract(res);
        } catch (Exception e) {
            throw QueryException.error("Failed to extract field");
        }
    }

    private Object extract(Object res) throws Exception {
        Object value = res;
        for (String component : path) {
            final Field field = value.getClass().getDeclaredField(component);
            boolean accessible = field.isAccessible();
            field.setAccessible(true);

            value = field.get(value);

            field.setAccessible(accessible);
        }

        return value;
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
        out.writeInt(index);
        out.writeObject(type);
        out.writeObject(path);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        index = in.readInt();
        type = in.readObject();
        path = in.readObject();
    }

    @Override
    public QueryDataType getType() {
        return type;
    }
}
