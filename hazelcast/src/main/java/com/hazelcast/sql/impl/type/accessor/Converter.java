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

package com.hazelcast.sql.impl.type.accessor;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.type.GenericType;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Interface to convert an item from one type to another.
 */
@SuppressWarnings("checkstyle:MethodCount")
public abstract class Converter {
    private final boolean convertToBit;
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

    protected Converter() {
        try {
            Class<? extends Converter> clazz = getClass();

            convertToBit = canConvert(clazz.getMethod("asBit", Object.class));
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

    private static boolean canConvert(Method method) {
        return method.getAnnotation(NotConvertible.class) == null;
    }

    /**
     * @return Class of the input.
     */
    public abstract Class<?> getValueClass();

    /**
     * @return Matching generic type.
     */
    public abstract GenericType getGenericType();

    @NotConvertible
    public boolean asBit(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public byte asTinyint(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public short asSmallint(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public int asInt(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public long asBigint(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public BigDecimal asDecimal(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public float asReal(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public double asDouble(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public String asVarchar(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public LocalDate asDate(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public LocalTime asTime(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public LocalDateTime asTimestamp(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        throw cannotConvertImplicit(val);
    }

    @NotConvertible
    public Object asObject(Object val) {
        throw cannotConvertImplicit(val);
    }

    public final boolean canConvertToBit() {
        return convertToBit;
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

    /**
     * @return {@code True} if this converter represents a numeric type.
     */
    public final boolean isNumeric() {
        return canConvertToDecimal();
    }

    /**
     * @return {@code True} if this converter represents a temporal type.
     */
    public final boolean isTemporal() {
        return getGenericType().isTemporal();
    }

    public abstract Object convertToSelf(Converter converter, Object val);

    protected final HazelcastSqlException cannotConvertImplicit(Object val) {
        throw HazelcastSqlException.error("Cannot implicitly convert a value to " + getGenericType() + ": " + val);
    }

    protected static Converter getConverter(Object val) {
        assert val != null;

        return Converters.getConverter(val.getClass());
    }
}
