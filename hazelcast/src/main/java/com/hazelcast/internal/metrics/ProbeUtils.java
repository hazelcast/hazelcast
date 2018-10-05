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

package com.hazelcast.internal.metrics;

import static com.hazelcast.internal.metrics.MetricsSource.TAG_INSTANCE;
import static com.hazelcast.internal.metrics.MetricsSource.TAG_TYPE;
import static com.hazelcast.util.StringUtil.getterIntoProperty;
import static java.lang.Math.round;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.internal.metrics.CollectionCycle.Tags;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Provides utilities around probing.
 *
 * This includes general conversion functionality like {@link #toLong(boolean)}
 * as well as common metrics on core types of objects that cannot be annotated.
 */
public final class ProbeUtils {

    private static final ILogger LOGGER = Logger.getLogger(ProbeUtils.class);

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
     *         {@link MetricsCollector} that only works in longs
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
     *         {@link MetricsCollector} that only works in longs
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
     * @see #isSupportedProbeType(Class)
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
                || Semaphore.class.isAssignableFrom(type)
                || MetricsSource.class.isAssignableFrom(type)
                || type.isAnnotationPresent(Probe.class);
    }

    public static long updateInterval(int value, TimeUnit unit) {
        return unit.toMillis(value);
    }

    public static String probeName(Probe probe, Method probed) {
        String name = probe == null ? "" : probe.name();
        return name.isEmpty() ? getterIntoProperty(probed.getName()) : name;
    }

    public static String probeName(Probe probe, Field probed) {
        String name = probe == null ? "" : probe.name();
        return name.isEmpty() ? probed.getName() : name.equals(Probe.BLANK_NAME) ? "" : name;
    }

    public static List<Method> findProbedMethods(Class<?> type) {
        List<Method> probedMethods = new ArrayList<Method>();
        collectProbeMethods(type, probedMethods);
        return probedMethods;
    }

    public static List<Field> findProbedFields(Class<?> type) {
        List<Field> probedFields = new ArrayList<Field>();
        collectProbeFields(type, probedFields);
        return probedFields;
    }

    private static void collectProbeMethods(Class<?> type, List<Method> probes) {
        for (Method m : type.getDeclaredMethods()) {
            if (m.isAnnotationPresent(Probe.class) && isSuitableProbeMethod(m, probes)) {
                if (isSupportedProbeType(m.getReturnType())) {
                    m.setAccessible(true);
                    probes.add(m);
                } else {
                    LOGGER.warning("@Probe annotated method " + m.getName() + " in "
                            + m.getDeclaringClass().getSimpleName()
                            + " has an unsupported return type.");
                }
            }
        }
        if (type.getSuperclass() != null) {
            collectProbeMethods(type.getSuperclass(), probes);
        }
        for (Class<?> t : type.getInterfaces()) {
            collectProbeMethods(t, probes);
        }
    }

    public static boolean isSuitableProbeMethod(Method m, List<Method> probes) {
        if (m.getParameterTypes().length > 0) {
            LOGGER.warning("Probe method must not have parameters: " + m.toGenericString());
            return false;
        }
        return !m.getDeclaringClass().isInterface() || !hasMethod(m, probes);
    }

    private static boolean hasMethod(Method probe, List<Method> probes) {
        for (Method p : probes) {
            if (p.getName().equals(probe.getName())) {
                return true;
            }
        }
        return false;
    }

    private static void collectProbeFields(Class<?> type, List<Field> probes) {
        for (Field f : type.getDeclaredFields()) {
            if (f.isAnnotationPresent(Probe.class)) {
                if (isSupportedProbeType(f.getType())) {
                    f.setAccessible(true);
                    probes.add(f);
                } else {
                    LOGGER.warning("@Probe annotated field " + f.getName() + " in "
                            + f.getDeclaringClass().getSimpleName()
                            + " has an unsupported type!");
                }
            }
        }
        if (type.getSuperclass() != null) {
            collectProbeFields(type.getSuperclass(), probes);
        }
    }


    public static void probeAllThreads(CollectionCycle cycle, String type, Thread[] threads) {
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

    public static <T> void probeAllInstances(CollectionCycle cycle, String type, Map<String, T> entries) {
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
