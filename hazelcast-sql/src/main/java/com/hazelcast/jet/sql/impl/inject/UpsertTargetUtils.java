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
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

public final class UpsertTargetUtils {

    private UpsertTargetUtils() { }

    public static Object convertRowToJavaType(final Object value, final QueryDataType type) {
        final Class<?> targetClass = ReflectionUtils.loadClass(type.getObjectTypeClassName());
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

        for (int i = 0; i < type.getObjectFields().size(); i++) {
            final QueryDataType.QueryDataTypeField typeField = type.getObjectFields().get(i);
            final boolean isRowValueField = rowValue.getValues().get(i) instanceof RowValue;
            final Object fieldValue = isRowValueField
                    ? convertRowToJavaType(rowValue.getValues().get(i), typeField.getDataType())
                    : rowValue.getValues().get(i);
            Method setter = ReflectionUtils.findPropertySetter(targetClass, typeField.getName());

            if (setter != null) {
                if (fieldValue == null && setter.getParameterTypes()[0].isPrimitive()) {
                    throw QueryException.error("Cannot pass NULL to a method with a primitive argument: " + setter);
                }
                try {
                    if (typeField.getDataType().getTypeFamily().equals(QueryDataTypeFamily.OBJECT)) {
                        setter.invoke(result, fieldValue);
                    } else {
                        setter.invoke(result, ToConverters
                                .getToConverter(QueryDataTypeUtils.resolveTypeForClass(setter.getParameterTypes()[0]))
                                .convert(fieldValue));
                    }
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
                if (typeField.getDataType().getTypeFamily().equals(QueryDataTypeFamily.OBJECT)) {
                    field.set(result, fieldValue);
                } else {
                    field.set(result, ToConverters
                            .getToConverter(QueryDataTypeUtils.resolveTypeForClass(field.getClass()))
                            .convert(fieldValue));
                }
            } catch (IllegalAccessException e) {
                throw QueryException.error("Can not set value for field " + typeField.getName(), e);
            }
        }

        return result;
    }

    public static GenericRecord convertRowToCompactType(RowValue rowValue, QueryDataType targetDataType) {
        final GenericRecordBuilder recordBuilder = GenericRecordBuilder.compact(targetDataType.getObjectTypeName());

        setFields(rowValue, targetDataType, recordBuilder);

        return recordBuilder.build();
    }

    private static void setFields(RowValue rowValue, QueryDataType targetDataType, GenericRecordBuilder recordBuilder) {
        for (int i = 0; i < targetDataType.getObjectFields().size(); i++) {
            final QueryDataType.QueryDataTypeField field = targetDataType.getObjectFields().get(i);
            final Object fieldValue = rowValue.getValues().get(i);
            switch (field.getDataType().getTypeFamily()) {
                case VARCHAR:
                    recordBuilder.setString(field.getName(), (String) fieldValue);
                    break;
                case BOOLEAN:
                    recordBuilder.setNullableBoolean(field.getName(), (Boolean) fieldValue);
                    break;
                case TINYINT:
                    recordBuilder.setNullableInt8(field.getName(), (Byte) fieldValue);
                    break;
                case SMALLINT:
                    recordBuilder.setNullableInt16(field.getName(), (Short) fieldValue);
                    break;
                case INTEGER:
                    recordBuilder.setNullableInt32(field.getName(), (Integer) fieldValue);
                    break;
                case BIGINT:
                    recordBuilder.setNullableInt64(field.getName(), (Long) fieldValue);
                    break;
                case DECIMAL:
                    recordBuilder.setDecimal(field.getName(), (BigDecimal) fieldValue);
                    break;
                case REAL:
                    recordBuilder.setNullableFloat32(field.getName(), (Float) fieldValue);
                    break;
                case DOUBLE:
                    recordBuilder.setNullableFloat64(field.getName(), (Double) fieldValue);
                    break;
                case TIME:
                    recordBuilder.setTime(field.getName(), (LocalTime) fieldValue);
                    break;
                case DATE:
                    recordBuilder.setDate(field.getName(), (LocalDate) fieldValue);
                    break;
                case TIMESTAMP:
                    recordBuilder.setTimestamp(field.getName(), (LocalDateTime) fieldValue);
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    recordBuilder.setTimestampWithTimezone(field.getName(), (OffsetDateTime) fieldValue);
                    break;
                case OBJECT:
                    final GenericRecordBuilder nestedRecordBuilder = GenericRecordBuilder
                            .compact(field.getDataType().getObjectTypeName());
                    setFields((RowValue) fieldValue, field.getDataType(), nestedRecordBuilder);
                    recordBuilder.setGenericRecord(field.getName(), nestedRecordBuilder.build());
                    break;
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_SECOND:
                case MAP:
                case JSON:
                case ROW:
                default:
                    throw QueryException.error("Unsupported upsert type: " + field.getDataType());
            }
        }
    }
}
