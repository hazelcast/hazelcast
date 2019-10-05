/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.collection.Int2ObjectHashMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Enum representing the target platforms of the metrics collection.
 */
public enum MetricTarget {

    // Note: ordinals of this enum are used for 32-bit bitset (#bitset);
    // thus if it ever gets bigger than 32 values, convert it to 64-bit bitset
    MANAGEMENT_CENTER,
    JMX,
    DIAGNOSTICS;

    private static final Int2ObjectHashMap<Set<MetricTarget>> BITSET_TO_SET_CACHE = new Int2ObjectHashMap<>();

    static {
        putInSetCache(MANAGEMENT_CENTER);
        putInSetCache(JMX);
        putInSetCache(DIAGNOSTICS);
        putInSetCache(MANAGEMENT_CENTER, JMX);
        putInSetCache(MANAGEMENT_CENTER, DIAGNOSTICS);
        putInSetCache(JMX, DIAGNOSTICS);
        putInSetCache(MANAGEMENT_CENTER, JMX, DIAGNOSTICS);
    }

    /**
     * Returns set based on the given array of {@link MetricTarget}.
     * Set objects are returned from a preliminary warmed up cache, so this method has no memory overhead.
     *
     * @param targets   input array
     * @return          set containing all items from the input array
     */
    public static Set<MetricTarget> asSet(MetricTarget[] targets) {
        return BITSET_TO_SET_CACHE.get(bitset(targets));
    }

    private static int bitset(MetricTarget[] targets) {
        int bitset = 0;
        for (MetricTarget target : targets) {
            bitset |= 1 << target.ordinal();
        }
        return bitset;
    }

    private static void putInSetCache(MetricTarget... targets) {
        EnumSet<MetricTarget> targetsSet = EnumSet.noneOf(MetricTarget.class);
        Collections.addAll(targetsSet, targets);
        BITSET_TO_SET_CACHE.put(bitset(targets), targetsSet);
    }

}
