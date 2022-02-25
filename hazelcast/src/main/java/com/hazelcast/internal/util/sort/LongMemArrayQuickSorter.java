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

import com.hazelcast.internal.memory.MemoryAccessor;

import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * {@link QuickSorter} implementation for a memory block which stores an array of {@code int}s.
 * Memory is accessed using the provided {@link MemoryAccessor}.
 */
public class LongMemArrayQuickSorter extends MemArrayQuickSorter {

    private long pivot;

    public LongMemArrayQuickSorter(MemoryAccessor mem, long baseAddress) {
        super(mem, baseAddress);
    }

    @Override
    protected void loadPivot(long index) {
        pivot = longAtIndex(index);
    }

    @Override
    protected boolean isLessThanPivot(long index) {
        return longAtIndex(index) < pivot;
    }

    @Override
    protected boolean isGreaterThanPivot(long index) {
        return longAtIndex(index) > pivot;
    }

    @Override
    protected void swap(long index1, long index2) {
        final long addrOfIndex1 = addrOfIndex(index1);
        final long addrOfIndex2 = addrOfIndex(index2);
        final long tmp = longAtAddress(addrOfIndex1);
        mem.putLong(addrOfIndex1, longAtAddress(addrOfIndex2));
        mem.putLong(addrOfIndex2, tmp);
    }

    private long addrOfIndex(long index) {
        return baseAddress + LONG_SIZE_IN_BYTES * index;
    }

    private long longAtIndex(long index) {
        return longAtAddress(addrOfIndex(index));
    }

    private long longAtAddress(long address) {
        return mem.getLong(address);
    }

}
