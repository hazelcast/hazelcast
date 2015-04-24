/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.util.counters.Counter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility functions for probes.
 */
final class ProbeUtils {

    static final int TYPE_PRIMITIVE_LONG = 1;
    static final int TYPE_LONG_NUMBER = 2;

    static final int TYPE_DOUBLE_PRIMITIVE = 3;
    static final int TYPE_DOUBLE_NUMBER = 4;

    static final int TYPE_COLLECTION = 5;
    static final int TYPE_MAP = 6;
    static final int TYPE_COUNTER = 7;
    static final int TYPE_SEMAPHORE = 8;

    private static final Map<Class<?>, Integer> TYPES = new HashMap<Class<?>, Integer>();

    static {
        TYPES.put(byte.class, TYPE_PRIMITIVE_LONG);
        TYPES.put(short.class, TYPE_PRIMITIVE_LONG);
        TYPES.put(int.class, TYPE_PRIMITIVE_LONG);
        TYPES.put(long.class, TYPE_PRIMITIVE_LONG);

        TYPES.put(Byte.class, TYPE_LONG_NUMBER);
        TYPES.put(Short.class, TYPE_LONG_NUMBER);
        TYPES.put(Integer.class, TYPE_LONG_NUMBER);
        TYPES.put(Long.class, TYPE_LONG_NUMBER);
        TYPES.put(AtomicInteger.class, TYPE_LONG_NUMBER);
        TYPES.put(AtomicLong.class, TYPE_LONG_NUMBER);

        TYPES.put(double.class, TYPE_DOUBLE_PRIMITIVE);
        TYPES.put(float.class, TYPE_DOUBLE_PRIMITIVE);

        TYPES.put(Double.class, TYPE_DOUBLE_NUMBER);
        TYPES.put(Float.class, TYPE_DOUBLE_NUMBER);

        TYPES.put(Collection.class, TYPE_COLLECTION);
        TYPES.put(Map.class, TYPE_MAP);
        TYPES.put(Counter.class, TYPE_COUNTER);

        TYPES.put(Semaphore.class, TYPE_SEMAPHORE);
    }

    private ProbeUtils() {
    }

    static boolean isDouble(int type) {
        return type == TYPE_DOUBLE_PRIMITIVE || type == TYPE_DOUBLE_NUMBER;
    }

    /**
     * Gets the accessible object probe type for this class object type.
     * accessible object probe    class object
     * TYPE_PRIMITIVE_LONG = 1    byte, short, int, long
     * TYPE_LONG_NUMBER = 2       Byte, Short, Integer, Long, AtomicInteger, AtomicLong
     * TYPE_DOUBLE_PRIMITIVE = 3  double, float
     * TYPE_DOUBLE_NUMBER = 4     Double, Float
     * TYPE_COLLECTION = 5        Collection
     * TYPE_MAP = 6               Map
     * TYPE_COUNTER = 7           Counter
     *
     * @param classType the class object type.
     * @return the accessible object probe type.
     */
    static int getType(Class classType) {
        Integer type = TYPES.get(classType);
        if (type != null) {
            return type;
        }

        List<Class<?>> flattenedClasses = new ArrayList<Class<?>>();

        flatten(classType, flattenedClasses);

        for (Class<?> clazz : flattenedClasses) {
            type = TYPES.get(clazz);
            if (type != null) {
                return type;
            }
        }

        return -1;
    }

    static void flatten(Class clazz, List<Class<?>> result) {
        if (!result.contains(clazz)) {
            result.add(clazz);
        }

        if (clazz.getSuperclass() != null) {
            flatten(clazz.getSuperclass(), result);
        }

        for (Class interfaze : clazz.getInterfaces()) {
            if (!result.contains(interfaze)) {
                result.add(interfaze);
            }

            flatten(interfaze, result);
        }
    }
}
