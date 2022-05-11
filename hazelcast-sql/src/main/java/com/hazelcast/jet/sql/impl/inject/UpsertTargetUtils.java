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
import com.hazelcast.jet.sql.impl.type.converter.ToConverters;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class UpsertTargetUtils {

    private UpsertTargetUtils() { }

    public static Object convertRowToTargetType(final Object value, final QueryDataType type) {
        final Class<?> targetClass = ReflectionUtils.loadClass(type.getTypeClassName());
        if (value.getClass().isAssignableFrom(targetClass)) {
            return value;
        }

        if (!(value instanceof RowValue)) {
            throw QueryException.error("Can not assign value of class " + value.getClass().getName()
                    + " to OBJECT field.");
        }

        final RowValue rowValue = (RowValue) value;
        final Object result = ReflectionUtils.newInstance(
                Thread.currentThread().getContextClassLoader(),
                targetClass.getName()
        );

        for (int i = 0; i < type.getFields().size(); i++) {
            final QueryDataType.QueryDataTypeField typeField = type.getFields().get(i);
            final Class<?> typeFieldClass = getTypeFieldClass(typeField);
            final Object fieldValue = rowValue.getValues().get(i) instanceof RowValue
                    ? convertRowToTargetType(rowValue.getValues().get(i), typeField.getDataType())
                    : ToConverters.getToConverter(typeField.getDataType()).convert(rowValue.getValues().get(i));

            Method setter = ReflectionUtils
                    .findPropertySetter(targetClass, typeField.getName(), typeFieldClass);
            if (setter == null && isJavaType(typeFieldClass)) {
                setter = ReflectionUtils
                        .findPropertySetter(targetClass, typeField.getName(), toPrimitive(typeFieldClass));
            }

            if (setter != null) {
                if (fieldValue == null && setter.getParameterTypes()[0].isPrimitive()) {
                    throw QueryException.error("Cannot pass NULL to a method with a primitive argument: " + setter);
                }
                try {
                    setter.invoke(result, fieldValue);
                    continue;
                } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
                    throw QueryException.error("Can not use setter for field " + typeField.getName(), e);
                }
            }

            final Field field = ReflectionUtils.findPropertyField(targetClass, typeField.getName());
            if (field == null) {
                throw QueryException.error("Can not find field: " + typeField.getName());
            }

            try {
                if (fieldValue == null && field.getType().isPrimitive()) {
                    throw QueryException.error("Cannot set NULL to a primitive field: " + field);
                }
                field.set(result, fieldValue);
            } catch (IllegalAccessException e) {
                throw QueryException.error("Can not set value for field " + typeField.getName(), e);
            }
        }

        return result;
    }

    public static Class<?> getTypeFieldClass(final QueryDataType.QueryDataTypeField typeField) {
        final QueryDataType queryDataType = typeField.getDataType();
        if (queryDataType.isCustomType()) {
            return ReflectionUtils.loadClass(queryDataType.getTypeClassName());
        } else {
            return queryDataType.getConverter().getValueClass();
        }
    }

    private static boolean isJavaType(Class<?> clazz) {
        return clazz.isPrimitive() || clazz.getPackage().getName().startsWith("java");
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static Class<?> toPrimitive(Class<?> clazz) {
        if (clazz == Boolean.class) {
            return boolean.class;
        } else if (clazz == Character.class) {
            return char.class;
        } else if (clazz == Byte.class) {
            return byte.class;
        } else if (clazz == Short.class) {
            return short.class;
        } else if (clazz == Integer.class) {
            return int.class;
        } else if (clazz == Long.class) {
            return long.class;
        } else if (clazz == Float.class) {
            return float.class;
        } else if (clazz == Double.class) {
            return double.class;
        } else {
            return null;
        }
    }
}
