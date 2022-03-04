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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.io.Serializable;
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
 * <p>
 * Java serialization is needed for Jet.
 */
@SuppressWarnings("checkstyle:MethodCount")
public abstract class Converter implements Serializable {
    protected static final int ID_BOOLEAN = 0;
    protected static final int ID_BYTE = 1;
    protected static final int ID_SHORT = 2;
    protected static final int ID_INTEGER = 3;
    protected static final int ID_LONG = 4;
    protected static final int ID_BIG_INTEGER = 5;
    protected static final int ID_BIG_DECIMAL = 6;
    protected static final int ID_FLOAT = 7;
    protected static final int ID_DOUBLE = 8;
    protected static final int ID_CHARACTER = 9;
    protected static final int ID_STRING = 10;
    protected static final int ID_DATE = 11;
    protected static final int ID_CALENDAR = 12;
    protected static final int ID_LOCAL_DATE = 13;
    protected static final int ID_LOCAL_TIME = 14;
    protected static final int ID_LOCAL_DATE_TIME = 15;
    protected static final int ID_INSTANT = 16;
    protected static final int ID_OFFSET_DATE_TIME = 17;
    protected static final int ID_ZONED_DATE_TIME = 18;
    protected static final int ID_OBJECT = 19;
    protected static final int ID_NULL = 20;
    protected static final int ID_INTERVAL_YEAR_MONTH = 21;
    protected static final int ID_INTERVAL_DAY_SECOND = 22;
    protected static final int ID_MAP = 23;
    protected static final int ID_JSON = 24;

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
    private final boolean convertToJson;

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
            convertToJson = canConvert(clazz.getMethod("asJson", Object.class));
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

    /**
     * @return Class of the value that is handled by this converter.
     */
    public abstract Class<?> getValueClass();

    /**
     * @return Class the value should be converted to as a result of {@link #convertToSelf(Converter, Object)} call.
     */
    public Class<?> getNormalizedValueClass() {
        return getValueClass();
    }

    @NotConvertible
    public boolean asBoolean(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.BOOLEAN);
    }

    @NotConvertible
    public byte asTinyint(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.TINYINT);
    }

    @NotConvertible
    public short asSmallint(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.SMALLINT);
    }

    @NotConvertible
    public int asInt(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.INTEGER);
    }

    @NotConvertible
    public long asBigint(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.BIGINT);
    }

    @NotConvertible
    public BigDecimal asDecimal(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.DECIMAL);
    }

    @NotConvertible
    public float asReal(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.REAL);
    }

    @NotConvertible
    public double asDouble(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.DOUBLE);
    }

    @NotConvertible
    public String asVarchar(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.VARCHAR);
    }

    @NotConvertible
    public LocalDate asDate(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.DATE);
    }

    @NotConvertible
    public LocalTime asTime(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.TIME);
    }

    @NotConvertible
    public LocalDateTime asTimestamp(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.TIMESTAMP);
    }

    @NotConvertible
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE);
    }

    @NotConvertible
    public HazelcastJsonValue asJson(Object val) {
        throw cannotConvertError(QueryDataTypeFamily.JSON);
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

    public final boolean canConvertToJson() {
        return convertToJson;
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

            case INTEGER:
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

            case JSON:
                return canConvertToJson();

            default:
                return getTypeFamily() == typeFamily;
        }
    }

    public abstract Object convertToSelf(Converter converter, Object val);

    protected final QueryException cannotConvertError(QueryDataTypeFamily target) {
        String message = "Cannot convert " + typeFamily + " to " + target;

        return QueryException.error(SqlErrorCode.DATA_EXCEPTION, message);
    }

    protected final QueryException numericOverflowError(QueryDataTypeFamily target) {
        String message = "Numeric overflow while converting " + typeFamily + " to " + target;

        return QueryException.error(SqlErrorCode.DATA_EXCEPTION, message);
    }

    protected final QueryException infiniteValueError(QueryDataTypeFamily target) {
        String message = "Cannot convert infinite " + typeFamily + " to " + target;

        return QueryException.error(SqlErrorCode.DATA_EXCEPTION, message);
    }

    protected final QueryException nanValueError(QueryDataTypeFamily target) {
        String message = "Cannot convert NaN to " + target;

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
