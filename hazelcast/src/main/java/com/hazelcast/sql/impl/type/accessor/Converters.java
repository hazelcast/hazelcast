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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for converters.
 */
@SuppressWarnings("checkstyle:ExecutableStatementCount")
public final class Converters {
    /** Map from input class to converter. */
    private static final Map<Class<?>, Converter> CLASS_TO_CONVERTER;

    static {
        List<Converter> converters = new ArrayList<>();

        // Boolean converter.
        converters.add(BooleanConverter.INSTANCE);

        // Converters for exact numeric types.
        converters.add(ByteConverter.INSTANCE);
        converters.add(ShortConverter.INSTANCE);
        converters.add(IntegerConverter.INSTANCE);
        converters.add(LongConverter.INSTANCE);
        converters.add(BigIntegerConverter.INSTANCE);
        converters.add(BigDecimalConverter.INSTANCE);

        // Converters for inexact numeric types.
        converters.add(FloatConverter.INSTANCE);
        converters.add(DoubleConverter.INSTANCE);

        // String converter.
        converters.add(StringConverter.INSTANCE);

        // Converters for temporal data types.
        converters.add(DateConverter.INSTANCE);
        converters.add(CalendarConverter.INSTANCE);

        converters.add(LocalDateConverter.INSTANCE);
        converters.add(LocalTimeConverter.INSTANCE);
        converters.add(LocalDateTimeConverter.INSTANCE);
        converters.add(OffsetDateTimeConverter.INSTANCE);

        converters.add(SqlYearMonthIntervalConverter.INSTANCE);
        converters.add(SqlDaySecondIntervalConverter.INSTANCE);

        // Object.
        converters.add(ObjectConverter.INSTANCE);

        CLASS_TO_CONVERTER = new HashMap<>();

        for (Converter converter : converters) {
            Class<?> valueClass = converter.getValueClass();

            if (valueClass != null) {
                Converter prevConverter = CLASS_TO_CONVERTER.put(valueClass, converter);

                if (prevConverter != null) {
                    throw new HazelcastException("Duplicate converter for class {class=" + valueClass
                        + ", oldConverter=" + prevConverter.getValueClass().getName()
                        + ", newConverter=" + valueClass.getName()
                        + '}');
                }

                Class<?> primitiveValueClass = getPrimitiveClass(valueClass);

                if (primitiveValueClass != null) {
                    CLASS_TO_CONVERTER.put(primitiveValueClass, converter);
                }
            }
        }
    }

    private Converters() {
        // No-op.
    }

    /**
     * Get converter for the given class.
     *
     * @param clazz Class.
     * @return Converter or exception if no matching converters found.
     */
    public static Converter getConverter(Class<?> clazz) {
        Converter res = CLASS_TO_CONVERTER.get(clazz);

        if (res == null) {
            res = getConverterInexact(clazz);
        }

        if (res == null) {
            throw HazelcastSqlException.error("Class is not supported by Hazelcast SQL: " + clazz.getName());
        }

        return res;
    }

    /**
     * Try to get inexact converter in case there is a type hierarchy.
     *
     * @param clazz Class.
     * @return Converter or {@code null}.
     */
    private static Converter getConverterInexact(Class<?> clazz) {
        if (Calendar.class.isAssignableFrom(clazz)) {
            return CalendarConverter.INSTANCE;
        }

        return ObjectConverter.INSTANCE;
    }

    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:ReturnCount"})
    private static Class<?> getPrimitiveClass(Class<?> targetClass) {
        if (targetClass == Boolean.class) {
            return boolean.class;
        }

        if (targetClass == Byte.class) {
            return byte.class;
        }

        if (targetClass == Short.class) {
            return short.class;
        }

        if (targetClass == Character.class) {
            return char.class;
        }

        if (targetClass == Integer.class) {
            return int.class;
        }

        if (targetClass == Long.class) {
            return long.class;
        }

        if (targetClass == Float.class) {
            return float.class;
        }

        if (targetClass == Double.class) {
            return double.class;
        }

        return null;
    }
}
