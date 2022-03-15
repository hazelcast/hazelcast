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

package com.hazelcast.internal.util.hashslot.impl;

import com.hazelcast.internal.util.QuickMath;

/**
 * Utility functions related to data structure capacity calculation.
 */
public final class CapacityUtil {

    /** Maximum length of a Java array that is a power of two. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int MAX_INT_CAPACITY = 1 << 30;

    /** Maximum length of an off-heap array that is a power of two. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final long MAX_LONG_CAPACITY = 1L << 62;

    /** Minimum capacity for a hash container. */
    public static final int MIN_CAPACITY = 4;

    /** Default capacity for a hash container. */
    public static final int DEFAULT_CAPACITY = 16;

    /** Default load factor. */
    public static final float DEFAULT_LOAD_FACTOR = 0.6f;


    private CapacityUtil() { }

    /** Round the capacity to the next allowed value. */
    public static long roundCapacity(long requestedCapacity) {
        if (requestedCapacity > MAX_LONG_CAPACITY) {
            throw new IllegalArgumentException(requestedCapacity + " is greater than max allowed capacity["
                    + MAX_LONG_CAPACITY + "].");
        }

        return Math.max(MIN_CAPACITY, QuickMath.nextPowerOfTwo(requestedCapacity));
    }

    /** Round the capacity to the next allowed value. */
    public static int roundCapacity(int requestedCapacity) {
        if (requestedCapacity > MAX_INT_CAPACITY) {
            throw new IllegalArgumentException(requestedCapacity + " is greater than max allowed capacity["
                + MAX_INT_CAPACITY + "].");
        }

        return Math.max(MIN_CAPACITY, QuickMath.nextPowerOfTwo(requestedCapacity));
    }

    /** Returns the next possible capacity, counting from the current buffers' size. */
    public static int nextCapacity(int current) {
        assert current > 0 && Long.bitCount(current) == 1 : "Capacity must be a power of two.";

        if (current < MIN_CAPACITY / 2) {
            current = MIN_CAPACITY / 2;
        }

        current <<= 1;
        if (current < 0) {
            throw new RuntimeException("Maximum capacity exceeded.");
        }
        return current;
    }

    /** Returns the next possible capacity, counting from the current buffers' size. */
    public static long nextCapacity(long current) {
        assert current > 0 && Long.bitCount(current) == 1 : "Capacity must be a power of two, but was " + current;

        if (current < MIN_CAPACITY / 2) {
            current = MIN_CAPACITY / 2;
        }

        current <<= 1;
        if (current < 0) {
            throw new RuntimeException("Maximum capacity exceeded.");
        }
        return current;
    }
}
