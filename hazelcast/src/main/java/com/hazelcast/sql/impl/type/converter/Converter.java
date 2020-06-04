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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Interface to convert an item from one type to another.
 * <p>
 * Converters assume that the passed values are not null, caller of conversion methods must ensure that.
 * We do this because most SQL expressions have special treatment for null values, and in general null check
 * is already performed by the time the converter is called.
 */
@SuppressWarnings("checkstyle:MethodCount")
public abstract class Converter {
    protected static final int ID_LATE = 0;
    protected static final int ID_BOOLEAN = 1;
    protected static final int ID_BYTE = 2;
    protected static final int ID_SHORT = 3;
    protected static final int ID_INTEGER = 4;
    protected static final int ID_LONG = 5;
    protected static final int ID_BIG_INTEGER = 6;
    protected static final int ID_BIG_DECIMAL = 7;
    protected static final int ID_FLOAT = 8;
    protected static final int ID_DOUBLE = 9;
    protected static final int ID_CHARACTER = 10;
    protected static final int ID_STRING = 11;
    protected static final int ID_DATE = 12;
    protected static final int ID_CALENDAR = 13;
    protected static final int ID_LOCAL_DATE = 14;
    protected static final int ID_LOCAL_TIME = 15;
    protected static final int ID_LOCAL_DATE_TIME = 16;
    protected static final int ID_INSTANT = 17;
    protected static final int ID_OFFSET_DATE_TIME = 18;
    protected static final int ID_ZONED_DATE_TIME = 19;
    protected static final int ID_OBJECT = 20;

    private final int id;
    private final QueryDataTypeFamily typeFamily;

    private final boolean convertToBoolean;
    private final boolean convertToTinyint;
    private final boolean convertToSmallint;
    private final boolean convertToInt;
    private final boolean convertToBigint;
    private final boolean convertToDecimal;
    private final boolean convertToReal;
    private final boolean convertToDouble;
    private final boolean convertToVarchar;
    private final boolean convertToDate;
    private final boolean convertToTime;
    private final boolean convertToTimestamp;
    private final boolean convertToTimestampWithTimezone;
    private final boolean convertToObject;

    protected Converter(int id, QueryDataTypeFamily typeFamily) {
        this.id = id;
        this.typeFamily = typeFamily;

        try {
            Class<? extends Converter> clazz = getClass();

            convertToBoolean = canConvert(clazz.getMethod("asBoolean", Object.class));
            convertToTinyint = canConvert(clazz.getMethod("asTinyint", Object.class));
            convertToSmallint = canConvert(clazz.getMethod("asSmallint", Object.class));
            convertToInt = canConvert(clazz.getMethod("asInt", Object.class));
            convertToBigint = canConvert(clazz.getMethod("asBigint", Object.class));
            convertToDecimal = canConvert(clazz.getMethod("asDecimal", Object.class));
            convertToReal = canConvert(clazz.getMethod("asReal", Object.class));
            convertToDouble = canConvert(clazz.getMethod("asDouble", Object.class));
            convertToVarchar = canConvert(clazz.getMethod("asVarchar", Object.class));
            convertToDate = canConvert(clazz.getMethod("asDate", Object.class));
            convertToTime = canConvert(clazz.getMethod("asTime", Object.class));
            convertToTimestamp = canConvert(clazz.getMethod("asTimestamp", Object.class));
            convertToTimestampWithTimezone = canConvert(clazz.getMethod("asTimestampWithTimezone", Object.class));
            convertToObject = canConvert(clazz.getMethod("asObject", Object.class));
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to initialize converter: " + getClass().getName(), e);
        }
    }

    public final int getId() {
        return id;
    }

    public final QueryDataTypeFamily getTypeFamily() {
        return typeFamily;
    }

    public abstract Class<?> getValueClass();

    @NotConvertible
    public boolean asBoolean(Object val) {
        throw cannotConvert(QueryDataTypeFamily.BOOLEAN);
    }

    @NotConvertible
    public byte asTinyint(Object val) {
        throw cannotConvert(QueryDataTypeFamily.TINYINT);
    }

    @NotConvertible
    public short asSmallint(Object val) {
        throw cannotConvert(QueryDataTypeFamily.SMALLINT);
    }

    @NotConvertible
    public int asInt(Object val) {
        throw cannotConvert(QueryDataTypeFamily.INT);
    }

    @NotConvertible
    public long asBigint(Object val) {
        throw cannotConvert(QueryDataTypeFamily.BIGINT);
    }

    @NotConvertible
    public BigDecimal asDecimal(Object val) {
        throw cannotConvert(QueryDataTypeFamily.DECIMAL);
    }

    @NotConvertible
    public float asReal(Object val) {
        throw cannotConvert(QueryDataTypeFamily.REAL);
    }

    @NotConvertible
    public double asDouble(Object val) {
        throw cannotConvert(QueryDataTypeFamily.DOUBLE);
    }

    @NotConvertible
    public String asVarchar(Object val) {
        throw cannotConvert(QueryDataTypeFamily.VARCHAR);
    }

    @NotConvertible
    public LocalDate asDate(Object val) {
        throw cannotConvert(QueryDataTypeFamily.DATE);
    }

    @NotConvertible
    public LocalTime asTime(Object val) {
        throw cannotConvert(QueryDataTypeFamily.TIME);
    }

    @NotConvertible
    public LocalDateTime asTimestamp(Object val) {
        throw cannotConvert(QueryDataTypeFamily.TIMESTAMP);
    }

    @NotConvertible
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        throw cannotConvert(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE);
    }

    public Object asObject(Object val) {
        return val;
    }

    public final boolean canConvertToBoolean() {
        return convertToBoolean;
    }

    public final boolean canConvertToTinyint() {
        return convertToTinyint;
    }

    public final boolean canConvertToSmallint() {
        return convertToSmallint;
    }

    public final boolean canConvertToInt() {
        return convertToInt;
    }

    public final boolean canConvertToBigint() {
        return convertToBigint;
    }

    public final boolean canConvertToDecimal() {
        return convertToDecimal;
    }

    public final boolean canConvertToReal() {
        return convertToReal;
    }

    public final boolean canConvertToDouble() {
        return convertToDouble;
    }

    public final boolean canConvertToVarchar() {
        return convertToVarchar;
    }

    public final boolean canConvertToDate() {
        return convertToDate;
    }

    public final boolean canConvertToTime() {
        return convertToTime;
    }

    public final boolean canConvertToTimestamp() {
        return convertToTimestamp;
    }

    public final boolean canConvertToTimestampWithTimezone() {
        return convertToTimestampWithTimezone;
    }

    public final boolean canConvertToObject() {
        return convertToObject;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    public final boolean canConvertTo(QueryDataTypeFamily typeFamily) {
        switch (typeFamily) {
            case BOOLEAN:
                return canConvertToBoolean();

            case TINYINT:
                return canConvertToTinyint();

            case SMALLINT:
                return canConvertToSmallint();

            case INT:
                return canConvertToInt();

            case BIGINT:
                return canConvertToBigint();

            case DECIMAL:
                return canConvertToDecimal();

            case REAL:
                return canConvertToReal();

            case DOUBLE:
                return canConvertToDouble();

            case VARCHAR:
                return canConvertToVarchar();

            case DATE:
                return canConvertToDate();

            case TIME:
                return canConvertToTime();

            case TIMESTAMP:
                return canConvertToTimestamp();

            case TIMESTAMP_WITH_TIME_ZONE:
                return canConvertToTimestampWithTimezone();

            case OBJECT:
                return canConvertToObject();

            default:
                return getTypeFamily() == typeFamily;
        }
    }

    public abstract Object convertToSelf(Converter converter, Object val);

    protected final QueryException cannotConvert(QueryDataTypeFamily target) {
        return cannotConvert(target, null);
    }

    protected final QueryException cannotConvert(QueryDataTypeFamily target, Object val) {
        return cannotConvert(typeFamily, target, val);
    }

    protected final QueryException cannotConvert(QueryDataTypeFamily source, QueryDataTypeFamily target, Object val) {
        String message = "Cannot convert " + source + " to " + target;

        if (val != null) {
            message += ": " + val;
        }

        return QueryException.error(SqlErrorCode.DATA_EXCEPTION, message);
    }

    private static boolean canConvert(Method method) {
        return method.getAnnotation(NotConvertible.class) == null;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
