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

package com.hazelcast.internal.util.sort;

/** <p>
 * Base class for QuickSort implementations. Works with an abstract notion of a mutable array of
 * slots addressed by their index. Makes no assumptions on the size or structure of the slots, or on
 * the way they are compared.
 * </p><p>
 * <strong>Not thread-safe</strong> because the contract for implementing subclasses is stateful:
 * {@link #loadPivot(long)} must update the internal state which is later accessed by
 * {@link #isGreaterThanPivot(long)} and {@link #isLessThanPivot(long)}.
 * </p>
 */
public abstract class QuickSorter {

    /**
     * Sort the part of the array that starts at the supplied index and has the supplied length.
     *
     * @param startIndex first index of the region to sort
     * @param length length of the region to sort
     */
    public final void sort(long startIndex, long length) {
        quickSort(startIndex, length - 1);
    }

    /**
     * Loads the data from the given index as needed to satisfy later calls to {@link #isLessThanPivot(long)}
     * and {@link #isGreaterThanPivot(long)}.
     * @param index the index from which to load the data.
     */
    protected abstract void loadPivot(long index);

    /**
     * @param index the supplied index.
     * @return {@code true} if the slot at the supplied index is "less than" the slot remembered by the
     * previous call to {@link #loadPivot(long)}; {@code false} otherwise.
     */
    protected abstract boolean isLessThanPivot(long index);

    /**
     * @param index the supplied index.
     * @return {@code true} if the slot at the supplied index is "greater than" the slot remembered by the
     * previous call to {@link #loadPivot(long)}; {@code false} otherwise.
     */
    protected abstract boolean isGreaterThanPivot(long index);

    /**
     * Swaps the contents of the slots at the supplied indices.
     * @param index1 the index from which to swap contents.
     * @param index2 the other index from which to swap contents.
     */
    protected abstract void swap(long index1, long index2);


    private void quickSort(long lo, long hi) {
        if (lo >= hi) {
            return;
        }
        long p = partition(lo, hi);
        quickSort(lo, p);
        quickSort(p + 1, hi);
    }

    private long partition(long lo, long hi) {
        loadPivot((lo + hi) >>> 1);
        long i = lo - 1;
        long j = hi + 1;
        while (true) {
            do {
                i++;
            } while (isLessThanPivot(i));
            do {
                j--;
            } while (isGreaterThanPivot(j));
            if (i >= j) {
                return j;
            }
            swap(i, j);
        }
    }
}
