/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.binarystorage.accumulator;

import com.hazelcast.internal.memory.MemoryAccessor;

/**
 * Similar to the notion of accumulator used in {@code java.util.stream}, but applied to binary storage.
 * Given a pointer to the current accumulated value and to the next value to accumulate, applies a function
 * to them and destructively stores the result as the new accumulated value.
 */
public interface Accumulator {

    /**
     * Says whether this accumulator is "associative": whether it can encounter values in any order,
     * always producing the same end result.
     */
    boolean isAssociative();

    /** Resolves the given address into the address of the payload, accounting for any headers. */
    long toDataAddress(long address);

    /**
     * Accepts the next value to accumulate and replaces the accumulated value with the new result.
     *
     * @param accMemoryAccessor to access the accumulated value
     * @param newMemoryAccessor to access the new value
     * @param accAddress address of the accumulated value
     * @param accSize size of the accumulated value
     * @param newAddress address of the new value
     * @param newSize size of the new value
     * @param useBigEndian whether the values are stored in big-endian encoding
     */
    void accept(MemoryAccessor accMemoryAccessor, MemoryAccessor newMemoryAccessor,
                long accAddress, long accSize, long newAddress, long newSize, boolean useBigEndian);

    /**
     * Same as {@link #accept(MemoryAccessor, MemoryAccessor, long, long, long, long, boolean)}, but
     * with one memory accessor used for both values.
     */
    default void accept(MemoryAccessor memoryAccessor,
                        long oldAddress, long oldSize, long newAddress, long newSize, boolean useBigEndian) {
        accept(memoryAccessor, memoryAccessor, oldAddress, oldSize, newAddress, newSize, useBigEndian);
    }
}
