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

package com.hazelcast.jet.sql.impl.support.expressions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class ExpressionTypes {

    public static final ExpressionType<?> STRING = new ExpressionType.StringType();
    public static final ExpressionType<?> CHARACTER = new ExpressionType.CharacterType();
    public static final ExpressionType<?> BOOLEAN = new ExpressionType.BooleanType();
    public static final ExpressionType<?> BYTE = new ExpressionType.ByteType();
    public static final ExpressionType<?> SHORT = new ExpressionType.ShortType();
    public static final ExpressionType<?> INTEGER = new ExpressionType.IntegerType();
    public static final ExpressionType<?> LONG = new ExpressionType.LongType();
    public static final ExpressionType<?> BIG_DECIMAL = new ExpressionType.BigDecimalType();
    public static final ExpressionType<?> BIG_INTEGER = new ExpressionType.BigIntegerType();
    public static final ExpressionType<?> FLOAT = new ExpressionType.FloatType();
    public static final ExpressionType<?> DOUBLE = new ExpressionType.DoubleType();
    public static final ExpressionType<?> LOCAL_DATE = new ExpressionType.LocalDateType();
    public static final ExpressionType<?> LOCAL_TIME = new ExpressionType.LocalTimeType();
    public static final ExpressionType<?> LOCAL_DATE_TIME = new ExpressionType.LocalDateTimeType();
    public static final ExpressionType<?> OFFSET_DATE_TIME = new ExpressionType.OffsetDateTimeType();
    public static final ExpressionType<?> OBJECT = new ExpressionType.ObjectType();

    private static final Object MUX = new Object();

    private static volatile List<ExpressionType<?>> all;

    public static ExpressionType<?> resolve(Object value) {
        if (value instanceof String) {
            return STRING;
        } else if (value instanceof Character) {
            return CHARACTER;
        } else if (value instanceof Boolean) {
            return BOOLEAN;
        } else if (value instanceof Byte) {
            return BYTE;
        } else if (value instanceof Short) {
            return SHORT;
        } else if (value instanceof Integer) {
            return INTEGER;
        } else if (value instanceof Long) {
            return LONG;
        } else if (value instanceof BigInteger) {
            return BIG_INTEGER;
        } else if (value instanceof BigDecimal) {
            return BIG_DECIMAL;
        } else if (value instanceof Float) {
            return FLOAT;
        } else if (value instanceof Double) {
            return DOUBLE;
        } else if (value instanceof LocalDate) {
            return LOCAL_DATE;
        } else if (value instanceof LocalTime) {
            return LOCAL_TIME;
        } else if (value instanceof LocalDateTime) {
            return LOCAL_DATE_TIME;
        } else if (value instanceof OffsetDateTime) {
            return OFFSET_DATE_TIME;
        } else {
            return OBJECT;
        }
    }

    public static ExpressionType<?>[] numeric() {
        return new ExpressionType[]{
                BYTE,
                SHORT,
                INTEGER,
                LONG,
                BIG_INTEGER,
                BIG_DECIMAL,
                FLOAT,
                DOUBLE
        };
    }

    public static ExpressionType<?>[] allExcept(ExpressionType<?>... excludeTypes) {
        if (excludeTypes == null || excludeTypes.length == 0) {
            return all();
        }

        List<ExpressionType<?>> res = new ArrayList<>(Arrays.asList(all()));

        for (ExpressionType<?> excludedType : excludeTypes) {
            res.remove(excludedType);
        }

        return res.toArray(new ExpressionType[0]);
    }

    public static ExpressionType<?>[] all() {
        List<ExpressionType<?>> res = all;

        if (res == null) {
            synchronized (MUX) {
                res = all;

                if (res == null) {
                    res = all0();

                    all = res;
                }
            }
        }

        return res.toArray(new ExpressionType[0]);
    }

    private static List<ExpressionType<?>> all0() {
        try {
            List<ExpressionType<?>> types = new ArrayList<>();

            for (Field field : ExpressionTypes.class.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(ExpressionType.class)) {
                    ExpressionType<?> type = (ExpressionType<?>) field.get(null);

                    types.add(type);
                }
            }

            return Collections.unmodifiableList(types);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to initialize " + ExpressionTypes.class + ".all()", e);
        }
    }

    private ExpressionTypes() {
        // No-op.
    }
}
