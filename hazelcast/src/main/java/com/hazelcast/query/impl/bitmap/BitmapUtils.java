/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.bitmap;

/**
 * Provides utilities shared among bitmap implementation classes.
 */
final class BitmapUtils {

    private static final int INT_ARRAY_MIN_CAPACITY = 2;
    private static final int SHORT_ARRAY_MIN_CAPACITY = 4;

    /**
     * Regulates the growth/shrink rate of the arrays. We are growing/shrinking
     * by 1/4 of the current array size. The rate may seem as too low, but we
     * are paying almost nothing for more frequent resizing while arrays are
     * small, and we don't waste as much memory for large arrays as with doubling.
     */
    private static final int CAPACITY_SHIFT = 2;

    private BitmapUtils() {
    }

    /** @see Short#toUnsignedInt(short) */
    public static int toUnsignedInt(short value) {
        return Short.toUnsignedInt(value);
    }

    /** @see Short#toUnsignedLong(short) */
    public static long toUnsignedLong(short value) {
        return Short.toUnsignedLong(value);
    }

    /** @see Integer#toUnsignedLong(int) */
    public static long toUnsignedLong(int value) {
        return Integer.toUnsignedLong(value);
    }

    /**
     * Does binary search in a way similar to Arrays.binarySearch, but treats
     * values as unsigned.
     */
    public static int unsignedBinarySearch(short[] array, int size, int unsignedValue) {
        return unsignedBinarySearch(array, 0, size, unsignedValue);
    }

    /**
     * Does binary search in a way similar to Arrays.binarySearch, but treats
     * values as unsigned.
     */
    public static int unsignedBinarySearch(short[] array, int from, int size, int unsignedValue) {
        int right = size - 1;
        int lastValue = toUnsignedInt(array[right]);
        if (unsignedValue > lastValue) {
            // fast exit if we are beyond the last value
            return -(size + 1);
        } else if (lastValue == right) {
            // The array is populated with contiguous values starting from 0 up
            // to size - 1. The value we are searching for is less than or equal
            // to size - 1 at this point, so we may just return the value itself
            // as its index.
            return unsignedValue;
        }

        int left = from;

        while (left <= right) {
            int middle = (left + right) >>> 1;
            int candidate = toUnsignedInt(array[middle]);

            if (candidate < unsignedValue) {
                left = middle + 1;
            } else if (candidate > unsignedValue) {
                right = middle - 1;
            } else {
                return middle;
            }
        }
        return -(left + 1);
    }

    /**
     * Does binary search in a way similar to Arrays.binarySearch, but treats
     * values as unsigned.
     */
    public static int unsignedBinarySearch(int[] array, int size, long unsignedValue) {
        return unsignedBinarySearch(array, 0, size, unsignedValue);
    }

    /**
     * Does binary search in a way similar to Arrays.binarySearch, but treats
     * values as unsigned.
     */
    public static int unsignedBinarySearch(int[] array, int from, int size, long unsignedValue) {
        int right = size - 1;
        long lastValue = toUnsignedLong(array[right]);
        if (unsignedValue > lastValue) {
            // fast exit if we are beyond the last value
            return -(size + 1);
        } else if (lastValue == right) {
            // The array is populated with contiguous values starting from 0 up
            // to size - 1. The value we are searching for is less than or equal
            // to size - 1 at this point, so we may just return the value itself
            // as its index.
            return (int) unsignedValue;
        }

        int left = from;

        while (left <= right) {
            int middle = (left + right) >>> 1;
            long candidate = toUnsignedLong(array[middle]);

            if (candidate < unsignedValue) {
                left = middle + 1;
            } else if (candidate > unsignedValue) {
                right = middle - 1;
            } else {
                return middle;
            }
        }
        return -(left + 1);
    }

    /**
     * Computes capacity delta for sorted int arrays.
     */
    public static int capacityDeltaInt(int capacity) {
        return Math.max(INT_ARRAY_MIN_CAPACITY, capacity >>> CAPACITY_SHIFT);
    }

    /**
     * Computes capacity delta for sorted short arrays.
     */
    public static int capacityDeltaShort(int capacity) {
        return Math.max(SHORT_ARRAY_MIN_CAPACITY, capacity >>> CAPACITY_SHIFT);
    }

    /**
     * Computes capacity delta for dense directly indexable int arrays.
     */
    public static int denseCapacityDeltaInt(int size, int capacity) {
        assert capacity > 0;
        assert size <= capacity;
        if (size == 0) {
            return 0;
        }

        int delta = capacityDeltaInt(capacity);
        int wasted = capacity - size;
        return Math.max(0, delta - wasted);
    }

    /**
     * Computes capacity delta for dense directly indexable short arrays.
     */
    public static int denseCapacityDeltaShort(int size, int capacity) {
        assert capacity > 0;
        assert size <= capacity;
        if (size == 0) {
            return 0;
        }

        int delta = capacityDeltaShort(capacity);
        int wasted = capacity - size;
        return Math.max(0, delta - wasted);
    }

}
