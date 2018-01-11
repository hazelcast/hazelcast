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

package com.hazelcast.util.collection;

import java.util.Arrays;

/**
 * Convenient method for array manipulations.
 *
 */
public final class ArrayUtils {

    private ArrayUtils() {
    }

    /**
     * Create copy of the src array.
     *
     * @param src
     * @param <T>
     * @return copy of the original array
     */
    public static <T> T[] createCopy(T[] src) {
        return Arrays.copyOf(src, src.length);
    }

    /**
     * Copy src array into destination and skip null values.
     * Warning: It does not do any validation. It expect the dst[] is
     * created with right capacity.
     *
     * You can calculate required capacity as <code>src.length - getNoOfNullItems(src)</code>
     *
     * @param src source array
     * @param dst destination. It has to have the right capacity
     * @param <T>
     */
    public static <T> void copyWithoutNulls(T[] src, T[] dst) {
        int skipped = 0;
        for (int i = 0; i < src.length; i++) {
            T object = src[i];
            if (object == null) {
                skipped++;
            } else {
                dst[i - skipped] = object;
            }
        }
    }

    public static <T> boolean contains(T[] array, T item) {
        for (T o : array) {
            if (o == null) {
                if (item == null) {
                    return true;
                }
            } else {
                if (o.equals(item)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static <T> T getItemAtPositionOrNull(T[] array, int position) {
        if (array.length > position) {
            return array[position];
        }
        return null;
    }

    /**
     * Copies in order {@code sourceFirst} and {@code sourceSecond} into {@code dest}.
     * @param sourceFirst
     * @param sourceSecond
     * @param dest
     * @param <T>
     */
    public static <T> void concat(T[] sourceFirst, T[] sourceSecond, T[] dest) {
        System.arraycopy(sourceFirst, 0, dest, 0, sourceFirst.length);
        System.arraycopy(sourceSecond, 0, dest, sourceFirst.length, sourceSecond.length);
    }

    /**
     * Bounds check when copying to/from a buffer
     *
     * @param capacity capacity of the buffer
     * @param index    index of copying will start from/to
     * @param length   length of the buffer that will be read/writen
     */
    public static void boundsCheck(int capacity, int index, int length) {
        if (capacity < 0 || index < 0 || length < 0 || (index > (capacity - length))) {
            throw new IndexOutOfBoundsException(String.format("index=%d, length=%d, capacity=%d", index, length, capacity));
        }
    }
}
