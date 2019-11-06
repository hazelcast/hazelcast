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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Enum representing the target platforms of the metrics collection.
 */
public enum MetricTarget {

    // Note: ordinals of this enum are used for 32-bit bitset (see #bitset);
    // thus if it ever gets bigger than 32 values, start using 64-bit bitset
    MANAGEMENT_CENTER,
    JMX,
    DIAGNOSTICS;

    private static final int MASK_ALL_TARGETS = bitset(values());

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
    public static Set<MetricTarget> asSet(MetricTarget... targets) {
        if (targets.length == 0) {
            return emptySet();
        }
        return BITSET_TO_SET_CACHE.get(bitset(targets));
    }

    /**
     * Returns set based on the given bitset representing a set of {@link MetricTarget}.
     * Set objects are returned from a preliminary warmed up cache, so this method has no memory overhead.
     *
     * @param bitset input array
     * @return set containing all items from the input array
     */
    public static Set<MetricTarget> asSet(int bitset) {
        return BITSET_TO_SET_CACHE.get(bitset & MASK_ALL_TARGETS);
    }

    /**
     * Returns set based on the given {@link Collection} of {@link MetricTarget}s
     * and one other to include in the returned set.
     * Set objects are returned from a preliminary warmed up cache, so this method has no memory overhead.
     *
     * @param targets        targets to be present in the returned set
     * @param includedTarget the target to be present in the returned set
     * @return set containing all items from the input array
     */
    public static Set<MetricTarget> asSetWith(Collection<MetricTarget> targets, MetricTarget includedTarget) {
        int bitset = bitset(targets) | targetMask(includedTarget);
        return BITSET_TO_SET_CACHE.get(bitset);
    }

    /**
     * Returns set based on the given {@link Collection} of {@link MetricTarget}s
     * and one other to include in the returned set.
     * Set objects are returned from a preliminary warmed up cache, so this method has no memory overhead.
     *
     * @param targets        targets to be present in the returned set
     * @param excludedTarget the target to not be in the returned set
     * @return set containing all items from the input array
     */
    public static Set<MetricTarget> asSetWithout(Collection<MetricTarget> targets, MetricTarget excludedTarget) {
        int bitset = bitset(targets) ^ targetMask(excludedTarget);
        return BITSET_TO_SET_CACHE.get(bitset);
    }

    private static int bitset(MetricTarget[] targets) {
        int bitset = 0;
        for (MetricTarget target : targets) {
            bitset |= targetMask(target);
        }
        return bitset;
    }

    public static int bitset(Iterable<MetricTarget> targets) {
        int bitset = 0;
        for (MetricTarget target : targets) {
            bitset |= targetMask(target);
        }
        return bitset;
    }

    private static int targetMask(MetricTarget target) {
        return 1 << target.ordinal();
    }

    private static void putInSetCache(MetricTarget... targets) {
        EnumSet<MetricTarget> targetsSet = EnumSet.noneOf(MetricTarget.class);
        Collections.addAll(targetsSet, targets);
        BITSET_TO_SET_CACHE.put(0, emptySet());
        BITSET_TO_SET_CACHE.put(bitset(targets), targetsSet);
    }

}
