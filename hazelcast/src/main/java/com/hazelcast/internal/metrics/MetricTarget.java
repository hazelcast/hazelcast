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

package com.hazelcast.internal.metrics;

import com.hazelcast.internal.util.collection.Int2ObjectHashMap;

import java.util.Arrays;
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
    DIAGNOSTICS,
    JET_JOB;

    public static final Collection<MetricTarget> ALL_TARGETS = EnumSet.copyOf(Arrays.asList(values()));
    public static final Collection<MetricTarget> NONE_OF = EnumSet.noneOf(MetricTarget.class);
    public static final Collection<MetricTarget> ALL_TARGETS_BUT_DIAGNOSTICS;

    static final Int2ObjectHashMap<Set<MetricTarget>> BITSET_TO_SET_CACHE = new Int2ObjectHashMap<>();

    private static final int MASK_ALL_TARGETS = bitset(values());

    static {
        // building BITSET_TO_SET_CACHE using a recursive algorithm for generating all combinations
        for (int i = 0; i <= values().length; i++) {
            generateCombinations(new int[i], 0, values().length - 1, 0);
        }

        ALL_TARGETS_BUT_DIAGNOSTICS = asSetWithout(ALL_TARGETS, DIAGNOSTICS);
    }

    private static void generateCombinations(int[] ordinals, int start, int end, int index) {
        if (index == ordinals.length) {
            MetricTarget[] allTargets = values();
            MetricTarget[] combination = new MetricTarget[ordinals.length];
            for (int i = 0; i < ordinals.length; i++) {
                combination[i] = allTargets[ordinals[i]];
            }
            putInSetCache(combination);
        } else if (start <= end) {
            ordinals[index] = start;
            generateCombinations(ordinals, start + 1, end, index + 1);
            generateCombinations(ordinals, start + 1, end, index);
        }
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
     * @see #bitset(Iterable)
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

    /**
     * Returns the union of two MetricTarget collections as a Set.
     * Set objects are returned from a preliminary warmed up cache, so this method has no memory overhead.
     *
     * @param targets1 The first MetricTarget collection
     * @param targets2 The second MetricTarget collection
     * @return the union of the two MetricTarget collections
     */
    public static Set<MetricTarget> union(Collection<MetricTarget> targets1, Collection<MetricTarget> targets2) {
        return asSet(bitset(targets1) | bitset(targets2));
    }

    /**
     * Creates a bitset from the given metric targets. The bitset is
     * used internally for caching target sets.
     *
     * @param targets The targets to create the bitset for
     * @return the bitset
     * @see #asSet(int)
     */
    public static int bitset(Iterable<MetricTarget> targets) {
        int bitset = 0;
        for (MetricTarget target : targets) {
            bitset |= targetMask(target);
        }
        return bitset;
    }

    private static int bitset(MetricTarget[] targets) {
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
        BITSET_TO_SET_CACHE.put(bitset(targets), targetsSet);
    }

}
