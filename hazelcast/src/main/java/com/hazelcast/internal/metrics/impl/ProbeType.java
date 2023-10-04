/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.impl;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.util.Collections.unmodifiableMap;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

enum ProbeType {
    TYPE_LONG_PRIMITIVE(long.class, byte.class, short.class, int.class, long.class),
    TYPE_LONG_NUMBER(long.class, Byte.class, Integer.class, Short.class, Long.class, AtomicInteger.class, AtomicLong.class,
            LongAdder.class, LongAccumulator.class),
    TYPE_DOUBLE_PRIMITIVE(double.class, double.class, float.class),
    TYPE_DOUBLE_NUMBER(double.class, Double.class, Float.class),
    TYPE_COLLECTION(long.class, Collection.class),
    TYPE_MAP(long.class, Map.class),
    TYPE_COUNTER(long.class, Counter.class),
    TYPE_SEMAPHORE(long.class, Semaphore.class);

    private static final Map<Class<?>, ProbeType> TYPES;

    /** The type the {@link Probe} would return */
    private final Class<?> mapsTo;
    /** The type(s) the {@link Probe} could be attached to */
    private final Class<?>[] types;

    ProbeType(final Class<?> mapsTo, Class<?>... types) {
        this.mapsTo = mapsTo;
        this.types = types;
    }

    static {
        final Map<Class<?>, ProbeType> types = createHashMap(20);

        for (final ProbeType probeUtils : values()) {
            for (final Class<?> type : probeUtils.types) {
                types.put(type, probeUtils);
            }
        }

        TYPES = unmodifiableMap(types);
    }

    Class<?> getMapsTo() {
        return mapsTo;
    }

    /** @return if {@link #types} are all {@link Class#isPrimitive()} */
    boolean isPrimitive() {
        return Arrays.stream(types).allMatch(Class::isPrimitive);
    }

    /**
     * @param classType the class object type.
     * @return the accessible object probe type.
     */
    static ProbeType getType(Class<?> classType) {
        final ProbeType type = TYPES.get(classType);
        if (type != null) {
            return type;
        }

        final Collection<Class<?>> flattenedClasses = new LinkedHashSet<>();

        ProbeUtils.flatten(classType, flattenedClasses);

        return flattenedClasses.stream().map(TYPES::get).filter(Objects::nonNull).findFirst().orElse(null);
    }
}
