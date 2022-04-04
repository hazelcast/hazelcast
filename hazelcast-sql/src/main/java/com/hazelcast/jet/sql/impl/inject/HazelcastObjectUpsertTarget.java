/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.type.TypeRegistry;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class HazelcastObjectUpsertTarget implements UpsertTarget {
    private Object object;

    public HazelcastObjectUpsertTarget() { }

    public HazelcastObjectUpsertTarget(final QueryDataType queryDataType) {
        // TODO precompute injectors and perform checks
    }

    @Override
    public UpsertInjector createInjector(@Nullable final String path, final QueryDataType queryDataType) {
        final Type type = TypeRegistry.INSTANCE.getTypeByName(queryDataType.getTypeName());
        return value -> {
            this.object = convertRowToTargetType(value, type);
        };
    }

    private Object convertRowToTargetType(final Object value, final Type type) {
        final Class<?> targetClass = ReflectionUtils.loadClass(type.getJavaClassName());
        if (value.getClass().isAssignableFrom(targetClass)) {
            object = value;
            return value;
        }

        if (!(value instanceof RowValue)) {
            throw QueryException.error("Can not assign value of class " + value.getClass().getName()
                    + " to HZ_OBJECT field.");
        }

        final RowValue rowValue = (RowValue) value;
        final Object result = ReflectionUtils.newInstance(
                Thread.currentThread().getContextClassLoader(),
                targetClass.getName()
        );

        for (int i = 0; i < type.getFields().size(); i++) {
            final Type.TypeField typeField = type.getFields().get(i);
            final Class<?> typeFieldClass = getTypeFieldClass(typeField);

            final Method setter = ReflectionUtils
                    .findPropertySetter(targetClass, typeField.getName(), typeFieldClass);
            final Object fieldValue = rowValue.getValues().get(i) instanceof RowValue
                    ? convertRowToTargetType(rowValue.getValues().get(i),
                    TypeRegistry.INSTANCE.getTypeByName(typeField.getQueryDataType().getTypeName()))
                    : rowValue.getValues().get(i);

            if (setter != null) {
                try {
                    setter.invoke(result, fieldValue);
                    continue;
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw QueryException.error("Can not use setter for field " + typeField.getName(), e);
                }
            }

            final Field field = ReflectionUtils.findPropertyField(targetClass, typeField.getName());
            if (field == null) {
                throw QueryException.error("Can not find field: " + typeField.getName());
            }

            try {
                field.set(result, fieldValue);
            } catch (IllegalAccessException e) {
                throw QueryException.error("Can not set value for field " + typeField.getName(), e);
            }
        }

        return result;
    }

    private Class<?> getTypeFieldClass(final Type.TypeField typeField) {
        final QueryDataType queryDataType = typeField.getQueryDataType();
        if (queryDataType.getTypeFamily().equals(QueryDataTypeFamily.HZ_OBJECT)) {
            final Type fieldType = TypeRegistry.INSTANCE.getTypeByName(queryDataType.getTypeName());
            if (fieldType == null) {
                throw QueryException.error("Type not found for field " + typeField.getName());
            }
            return ReflectionUtils.loadClass(fieldType.getJavaClassName());
        } else {
            return queryDataType.getConverter().getValueClass();
        }
    }

    @Override
    public void init() {

    }

    @Override
    public Object conclude() {
        return object;
    }
}
