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

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;

/**
 * {@link QuickSorter} implementation for a memory block which stores an array of {@code int}s.
 * Memory is accessed using the provided {@link MemoryAccessor}.
 */
public class IntMemArrayQuickSorter extends MemArrayQuickSorter {

    private int pivot;

    public IntMemArrayQuickSorter(MemoryAccessor mem, long baseAddress) {
        super(mem, baseAddress);
    }

    @Override
    protected void loadPivot(long index) {
        pivot = intAtIndex(index);
    }

    @Override
    protected boolean isLessThanPivot(long index) {
        return intAtIndex(index) < pivot;
    }

    @Override
    protected boolean isGreaterThanPivot(long index) {
        return intAtIndex(index) > pivot;
    }

    @Override
    protected void swap(long index1, long index2) {
        final long addrOfIndex1 = addrOfIndex(index1);
        final long addrOfIndex2 = addrOfIndex(index2);
        final int tmp = intAtAddress(addrOfIndex1);
        mem.putInt(addrOfIndex1, intAtAddress(addrOfIndex2));
        mem.putInt(addrOfIndex2, tmp);
    }

    private long addrOfIndex(long index) {
        return baseAddress + INT_SIZE_IN_BYTES * index;
    }

    private int intAtIndex(long index) {
        return intAtAddress(addrOfIndex(index));
    }

    private int intAtAddress(long address) {
        return mem.getInt(address);
    }
}
