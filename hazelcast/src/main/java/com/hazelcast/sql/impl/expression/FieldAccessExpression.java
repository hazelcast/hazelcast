/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.CompactGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecord;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.getters.CompactGetter;
import com.hazelcast.query.impl.getters.PortableGetter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * An expression backing the DOT operator for extracting field from a struct type.
 * <p>
 * {@code ref.field} - extracts `field` from `ref`.
 */
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
    public T eval(Row row, ExpressionEvalContext context) {
        return eval(row, context, false);
    }

    @Override
    public T eval(final Row row, final ExpressionEvalContext context, boolean useLazyDeserialization) {
        // Use lazy deserialization for nested queries. Only the last access should be eager.
        Object res = ref.eval(row, context, true);
        if (res == null) {
            return null;
        }

        if (isPrimitive(res.getClass())) {
            throw QueryException.error("Field Access expression can not be applied to primitive types");
        }

        try {
            if (res instanceof PortableGenericRecord) {
                return (T) type.convert(extractPortableField(
                        (PortableGenericRecord) res,
                        name,
                        context.getSerializationService(),
                        useLazyDeserialization
                ));
            } else if (res instanceof CompactGenericRecord) {
                return (T) type.convert(extractCompactField(
                        (CompactGenericRecord) res,
                        name,
                        context.getSerializationService(),
                        useLazyDeserialization
                ));
            } else {
                return (T) type.convert(ReflectionUtils.getFieldValue(name, res));
            }
        } catch (Exception e) {
            throw QueryException.error("Failed to extract field");
        }
    }

    private Object extractPortableField(PortableGenericRecord portable, String name, InternalSerializationService ss,
                                        boolean useLazyDeserialization) {
        final PortableGetter getter = new PortableGetter(ss);
        try {
            return getter.getValue(portable, name, useLazyDeserialization);
        } catch (Exception e) {
            return null;
        }
    }

    private Object extractCompactField(CompactGenericRecord compact, String name, InternalSerializationService ss,
                                      boolean useLazyDeserialization) {
        final CompactGetter getter = new CompactGetter(ss);
        try {
            return getter.getValue(compact, name, useLazyDeserialization);
        } catch (Exception e) {
            return null;
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
