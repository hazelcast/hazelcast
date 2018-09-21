/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.probing;

import static com.hazelcast.internal.probing.ProbeSource.TAG_INSTANCE;
import static com.hazelcast.internal.probing.ProbeSource.TAG_TYPE;
import static java.lang.Math.round;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.internal.probing.ProbingCycle.Tags;
import com.hazelcast.internal.util.counters.Counter;

/**
 * Provides utilities around probing.
 *
 * This includes general conversion functionality like {@link #toLong(boolean)}
 * as well as common metrics on core types of objects that cannot be annotated.
 */
public final class ProbeUtils {

    /**
     * To get 4 fractional digits precision for doubles these are multiplied by 10k.
     */
    private static final double DOUBLE_TO_LONG_FACTOR = 10000d;

    private ProbeUtils() {
        // utility
    }

    /**
     * @param value any double value
     * @return the long value representing the double as expected by a
     *         {@link ProbeRenderer} that only works in longs
     */
    public static long toLong(double value) {
        return round(value * DOUBLE_TO_LONG_FACTOR);
    }

    /**
     * Undoes the scaling done by {@link #toLong(double)}.
     *
     * @param value a value originally given as {@code double} that has been
     *        converted to {@code long} using {@link #toLong(double)}
     * @return the original double value
     */
    public static double doubleValue(long value) {
        return value / DOUBLE_TO_LONG_FACTOR;
    }

    /**
     * @param value any boolean value
     * @return the long representing the boolean as expected by a
     *         {@link ProbeRenderer} that only works in longs
     */
    public static long toLong(boolean value) {
        return value ? 1 : 0;
    }

    /**
     * Converts instances to their long representation.
     *
     * Supported are: Primitive wrapper types for {@link Number}s, atomic
     * {@link Number}s, {@link Counter}s, {@link Boolean} and {@link AtomicBoolean}
     * In case of {@link Collection} and {@link Map} their size is returned.
     *
     * @param value a value of a set of supported types
     * @return the long representing the passed object value
     */
    @SuppressWarnings({ "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:returncount" })
    public static long toLong(Object value) {
        if (value == null) {
            return -1L;
        }
        Class<?> type = value.getClass();
        if (value instanceof Number) {
            if (type == Float.class || type == Double.class) {
                return ProbeUtils.toLong(((Number) value).doubleValue());
            }
            return ((Number) value).longValue();
        }
        if (type == Boolean.class) {
            return ProbeUtils.toLong(((Boolean) value).booleanValue());
        }
        if (type == AtomicBoolean.class) {
            return ProbeUtils.toLong(((AtomicBoolean) value).get());
        }
        if (value instanceof Collection) {
            return ((Collection<?>) value).size();
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).size();
        }
        if (value instanceof Counter) {
            return ((Counter) value).get();
        }
        if (value instanceof Semaphore) {
            return ((Semaphore) value).availablePermits();
        }
        throw new UnsupportedOperationException(
                "It is not known how to convert a value of type "
                        + value.getClass().getSimpleName() + " to primitive long.");
    }

    /**
     * Check if a type can be probed.
     *
     * @param type any type, not null
     * @return true, if {@link #toLong(Object)} knows how to convert it to a long,
     *         else false
     */
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public static boolean isSupportedProbeType(Class<?> type) {
        if (type.isPrimitive()) {
            return type != void.class;
        }
        return     type == Boolean.class
                || type == AtomicBoolean.class
                || Number.class.isAssignableFrom(type)
                || Collection.class.isAssignableFrom(type)
                || Map.class.isAssignableFrom(type)
                || Counter.class.isAssignableFrom(type)
                || Semaphore.class.isAssignableFrom(type);
    }

    public static long updateInterval(int value, TimeUnit unit) {
        return unit.toMillis(value);
    }

    public static void probeAllThreads(ProbingCycle cycle, String type, Thread[] threads) {
        if (threads.length == 0) {
            // avoid unnecessary context manipulation
            return;
        }
        Tags tags = cycle.openContext().tag(TAG_TYPE, type);
        for (int i = 0; i < threads.length; i++) {
            tags.tag(TAG_INSTANCE, threads[i].getName());
            cycle.probe(threads[i]);
        }
    }

    public static <T> void probeAllInstances(ProbingCycle cycle, String type, Map<String, T> entries) {
        if (entries.isEmpty()) {
            // avoid unnecessary context manipulation
            return;
        }
        Tags tags = cycle.openContext().tag(TAG_TYPE, type);
        for (Entry<String, T> e : entries.entrySet()) {
            tags.tag(TAG_INSTANCE, e.getKey());
            cycle.probe(e.getValue());
        }
    }

}
