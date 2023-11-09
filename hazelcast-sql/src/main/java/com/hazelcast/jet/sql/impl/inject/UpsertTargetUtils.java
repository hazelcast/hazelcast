/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.internal.serialization.impl.portable.FieldDefinitionImpl;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.type.converter.ToConverter;
import com.hazelcast.jet.sql.impl.type.converter.ToConverters;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableId;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataType.QueryDataTypeField;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

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
        final Class<?> targetClass = ReflectionUtils.loadClass(type.getObjectTypeMetadata());
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
            final QueryDataTypeField typeField = type.getObjectFields().get(i);
            final boolean isRowValueField = rowValue.getValues().get(i) instanceof RowValue;
            final Object fieldValue = isRowValueField
                    ? convertRowToJavaType(rowValue.getValues().get(i), typeField.getType())
                    : rowValue.getValues().get(i);
            Method setter = ReflectionUtils.findPropertySetter(targetClass, typeField.getName());

            ToConverter toConverter = ToConverters.getToConverter(typeField.getType());
            if (setter != null) {
                if (fieldValue == null && setter.getParameterTypes()[0].isPrimitive()) {
                    throw QueryException.error("Cannot pass NULL to a method with a primitive argument: " + setter);
                }
                try {
                    if (typeField.getType().getTypeFamily().equals(QueryDataTypeFamily.OBJECT)) {
                        setter.invoke(result, fieldValue);
                    } else {
                        setter.invoke(result, toConverter.convert(fieldValue));
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
                if (typeField.getType().getTypeFamily().equals(QueryDataTypeFamily.OBJECT)) {
                    field.set(result, fieldValue);
                } else {
                    field.set(result, toConverter.convert(fieldValue));
                }
            } catch (IllegalAccessException e) {
                throw QueryException.error("Can not set value for field " + typeField.getName(), e);
            }
        }

        return result;
    }

    public static GenericRecord convertRowToCompactType(RowValue rowValue, QueryDataType targetType) {
        final GenericRecordBuilder recordBuilder = GenericRecordBuilder.compact(targetType.getObjectTypeMetadata());

        setCompactFields(rowValue, targetType, recordBuilder);

        return recordBuilder.build();
    }

    public static GenericRecord convertRowToPortableType(RowValue rowValue, QueryDataType queryDataType) {
        final ClassDefinition classDefinition = toPortableClassDefinition(queryDataType);
        final GenericRecordBuilder recordBuilder = GenericRecordBuilder.portable(classDefinition);

        setPortableFields(recordBuilder, rowValue, classDefinition, queryDataType);

        return recordBuilder.build();
    }

    private static void setPortableFields(
            GenericRecordBuilder builder,
            RowValue rowValue,
            ClassDefinition classDefinition,
            QueryDataType queryDataType
    ) {
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            final Object value = rowValue.getValues().get(i);
            final FieldDefinition field = classDefinition.getField(i);
            final String name = field.getName();

            switch (field.getType()) {
                case UTF:
                    builder.setString(name, value == null ? null : (String) QueryDataType.VARCHAR.convert(value));
                    break;
                case BOOLEAN:
                    builder.setBoolean(name, value != null && (boolean) value);
                    break;
                case BYTE:
                    builder.setInt8(name, value == null ? (byte) 0 : (byte) value);
                    break;
                case SHORT:
                    builder.setInt16(name, value == null ? (short) 0 : (short) value);
                    break;
                case CHAR:
                    builder.setChar(name, value == null ? (char) 0 : (char) value);
                    break;
                case INT:
                    builder.setInt32(name, value == null ? 0 : (int) value);
                    break;
                case LONG:
                    builder.setInt64(name, value == null ? 0L : (long) value);
                    break;
                case FLOAT:
                    builder.setFloat32(name, value == null ? 0F : (float) value);
                    break;
                case DOUBLE:
                    builder.setFloat64(name, value == null ? 0D : (double) value);
                    break;
                case DECIMAL:
                    builder.setDecimal(name, value == null ? null : (BigDecimal) value);
                    break;
                case TIME:
                    builder.setTime(name, value == null ? null : (LocalTime) value);
                    break;
                case DATE:
                    builder.setDate(name, value == null ? null : (LocalDate) value);
                    break;
                case TIMESTAMP:
                    builder.setTimestamp(name, value == null ? null : (LocalDateTime) value);
                    break;
                case TIMESTAMP_WITH_TIMEZONE:
                    builder.setTimestampWithTimezone(name, value == null ? null : (OffsetDateTime) value);
                    break;
                case PORTABLE:
                    if (value instanceof RowValue) {
                        final QueryDataType fieldQDT = queryDataType.getObjectFields().get(i).getType();
                        final ClassDefinition fieldClassDefinition = toPortableClassDefinition(fieldQDT);
                        final GenericRecordBuilder fieldBuilder = GenericRecordBuilder.portable(fieldClassDefinition);

                        setPortableFields(fieldBuilder, (RowValue) value, fieldClassDefinition, fieldQDT);
                        builder.setGenericRecord(name, fieldBuilder.build());
                    } else if (value instanceof GenericRecord) {
                        builder.setGenericRecord(name, (GenericRecord) value);
                    } else {
                        throw QueryException.error("Can not set non-GenericRecord or RowValue to field " + name);
                    }
                    break;
                default:
                    throw QueryException.error("Unsupported Portable Nested Fields upsert target type: "
                            + field.getType());
            }
        }
    }

    public static ClassDefinition toPortableClassDefinition(final QueryDataType queryDataType) {
        final PortableId portableId = new PortableId(queryDataType.getObjectTypeMetadata());
        final ClassDefinitionBuilder builder = new ClassDefinitionBuilder(portableId);

        for (int i = 0; i < queryDataType.getObjectFields().size(); i++) {
            final QueryDataTypeField field = queryDataType.getObjectFields().get(i);
            final String name = field.getName();
            final QueryDataType fieldType = field.getType();

            switch (fieldType.getTypeFamily()) {
                case BOOLEAN:
                    builder.addBooleanField(name);
                    break;
                case TINYINT:
                    builder.addByteField(name);
                    break;
                case SMALLINT:
                    builder.addShortField(name);
                    break;
                case INTEGER:
                    builder.addIntField(name);
                    break;
                case BIGINT:
                    builder.addLongField(name);
                    break;
                case REAL:
                    builder.addFloatField(name);
                    break;
                case DOUBLE:
                    builder.addDoubleField(name);
                    break;
                case DECIMAL:
                    builder.addDecimalField(name);
                    break;
                case VARCHAR:
                    builder.addStringField(name);
                    break;
                case TIME:
                    builder.addTimeField(name);
                    break;
                case DATE:
                    builder.addDateField(name);
                    break;
                case TIMESTAMP:
                    builder.addTimestampField(name);
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    builder.addTimestampWithTimezoneField(name);
                    break;
                case OBJECT:
                    if (fieldType.isCustomType()) {
                        final PortableId fieldId = new PortableId(fieldType.getObjectTypeMetadata());
                        builder.addField(new FieldDefinitionImpl(i, name, FieldType.PORTABLE, fieldId));
                    }
                    break;
                default:
                    throw QueryException.error("Unsupported Nested Fields Portable data type: " + fieldType);
            }
        }

        return builder.build();
    }

    private static void setCompactFields(
            final RowValue rowValue,
            final QueryDataType targetType,
            final GenericRecordBuilder recordBuilder
    ) {
        for (int i = 0; i < targetType.getObjectFields().size(); i++) {
            final QueryDataTypeField field = targetType.getObjectFields().get(i);
            final Object fieldValue = rowValue.getValues().get(i);
            switch (field.getType().getTypeFamily()) {
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
                    if (fieldValue == null) {
                        recordBuilder.setGenericRecord(field.getName(), null);
                    } else {
                        final GenericRecordBuilder nestedRecordBuilder = GenericRecordBuilder
                                .compact(field.getType().getObjectTypeMetadata());
                        setCompactFields((RowValue) fieldValue, field.getType(), nestedRecordBuilder);
                        recordBuilder.setGenericRecord(field.getName(), nestedRecordBuilder.build());
                    }
                    break;
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_SECOND:
                case MAP:
                case JSON:
                case ROW:
                default:
                    throw QueryException.error("Unsupported upsert type: " + field.getType());
            }
        }
    }
}
