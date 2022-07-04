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
import com.hazelcast.sql.impl.QueryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Utility methods for converters.
 */
@SuppressWarnings("checkstyle:ExecutableStatementCount")
public final class Converters {
    /** Synthetic maximum number of converters to prevent an accidental bug which will generate too big array. */
    private static final int MAX_CONVERTER_COUNT = 100;

    private static final Converter[] CONVERTERS;
    private static final Map<Class<?>, Converter> CLASS_TO_CONVERTER;

    static {
        List<Converter> converters = prepareConverters();

        CONVERTERS = createConvertersArray(converters);
        CLASS_TO_CONVERTER = createConvertersMap(converters);
    }

    private Converters() {
        // No-op.
    }

    public static Converter getConverter(int converterId) {
        if (converterId < CONVERTERS.length) {
            return CONVERTERS[converterId];
        }

        throw QueryException.error("Converter with ID " + converterId + " doesn't exist");
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
            if (Calendar.class.isAssignableFrom(clazz)) {
                res = CalendarConverter.INSTANCE;
            } else {
                res = ObjectConverter.INSTANCE;
            }
        }

        return res;
    }

    /**
     * For testing only.
     */
    public static List<Converter> getConverters() {
        return Arrays.asList(CONVERTERS);
    }

    /**
     * @return List of all supported converters.
     */
    private static List<Converter> prepareConverters() {
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

        // String converters.
        converters.add(CharacterConverter.INSTANCE);
        converters.add(StringConverter.INSTANCE);

        // Converters for temporal data types.
        converters.add(DateConverter.INSTANCE);
        converters.add(CalendarConverter.INSTANCE);

        converters.add(LocalDateConverter.INSTANCE);
        converters.add(LocalTimeConverter.INSTANCE);
        converters.add(LocalDateTimeConverter.INSTANCE);
        converters.add(InstantConverter.INSTANCE);
        converters.add(OffsetDateTimeConverter.INSTANCE);
        converters.add(ZonedDateTimeConverter.INSTANCE);

        // Object converter.
        converters.add(ObjectConverter.INSTANCE);

        // Interval converters.
        converters.add(IntervalConverter.YEAR_MONTH);
        converters.add(IntervalConverter.DAY_SECOND);

        // MAP converter.
        converters.add(MapConverter.INSTANCE);

        // NULL converter.
        converters.add(NullConverter.INSTANCE);

        // JSON converter
        converters.add(JsonConverter.INSTANCE);

        return converters;
    }

    private static Map<Class<?>, Converter> createConvertersMap(List<Converter> converters) {
        Map<Class<?>, Converter> res = new HashMap<>(converters.size());

        for (Converter converter : converters) {
            Class<?> valueClass = converter.getValueClass();

            if (valueClass != null) {
                Converter prevConverter = res.put(valueClass, converter);

                if (prevConverter != null) {
                    throw new HazelcastException("Duplicate converter for class [class=" + valueClass
                         + ", converter1=" + prevConverter.getValueClass().getName()
                         + ", converter2=" + valueClass.getName()
                         + ']');
                }

                Class<?> primitiveValueClass = getPrimitiveClass(valueClass);

                if (primitiveValueClass != null) {
                    res.put(primitiveValueClass, converter);
                }
            }
        }

        return res;
    }

    private static Converter[] createConvertersArray(List<Converter> converters) {
        TreeMap<Integer, Converter> map = new TreeMap<>();

        for (Converter converter : converters) {
            Converter oldConverter = map.put(converter.getId(), converter);

            if (oldConverter != null) {
                throw new HazelcastException("Two converters have the same ID [id=" + converter.getId()
                    + ", converter1=" + oldConverter.getClass().getSimpleName()
                    + ", converter2=" + converter.getClass().getSimpleName() + ']');
            }
        }

        int maxId = map.lastKey();

        if (maxId > MAX_CONVERTER_COUNT) {
            throw new HazelcastException("Converter ID cannot be greater than " + MAX_CONVERTER_COUNT + ": "
                + map.lastEntry().getValue().getClass().getSimpleName());
        }

        Converter[] res = new Converter[maxId + 1];

        for (int i = 0; i <= maxId; i++) {
            Converter converter = map.get(i);

            if (converter == null) {
                throw new HazelcastException("Converter with ID " + i + " is not defined");
            }

            res[i] = converter;
        }

        return res;
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

        if (targetClass == Void.class) {
            return void.class;
        }

        return null;
    }
}
