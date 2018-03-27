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

package com.hazelcast.util;

import java.util.BitSet;

/**
 * Convenient method for BitSet manipulations
 */
public final class BitSetUtils {

    private BitSetUtils() {
    }

    /**
     * Returns true if at least one bit on a given position is set.
     *
     * @param bitSet  the {@link BitSet} to modify
     * @param indexes the index positions to check
     * @return {@code true} if {@link BitSet} contains at least one bit at any position from index which is set,
     * otherwise returns {@code false}
     */
    public static boolean hasAtLeastOneBitSet(BitSet bitSet, Iterable<Integer> indexes) {
        for (Integer index : indexes) {
            if (bitSet.get(index)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if all bits at given {@code indexes} are set.
     *
     * @param bitSet  the {@link BitSet} to modify
     * @param indexes the index positions to check
     * @return {@code true} if {@link BitSet} all bits at given {@code indexes} are set.
     * otherwise returns {@code false}
     */
    public static boolean hasAllBitsSet(BitSet bitSet, Iterable<Integer> indexes) {
        for (Integer index : indexes) {
            if (!bitSet.get(index)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Set all bits on a given positions.
     *
     * @param bitSet  the {@link BitSet} to modify
     * @param indexes the index positions to set
     */
    public static void setBits(BitSet bitSet, Iterable<Integer> indexes) {
        for (Integer index : indexes) {
            bitSet.set(index);
        }
    }

    /**
     * Unsets all bits on a given positions.
     *
     * @param bitSet  the {@link BitSet} to modify
     * @param indexes the index positions to set
     */
    public static void unsetBits(BitSet bitSet, Iterable<Integer> indexes) {
        for (Integer index : indexes) {
            bitSet.set(index, false);
        }
    }
}
