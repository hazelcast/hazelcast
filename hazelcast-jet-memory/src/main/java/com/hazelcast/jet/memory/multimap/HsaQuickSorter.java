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

package com.hazelcast.jet.memory.multimap;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.util.sort.QuickSorter;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;

/**
 * Quick sorter implementation for JET openAddressing storage
 */
public class HsaQuickSorter extends QuickSorter {
    private final int slotSize;
    private final TupleMultimapHsa multimap;
    private final byte[] temporaryBuffer;
    private Comparator comparator;

    private long pivotKeySize;
    private long pivotKeyAddress;
    private MemoryAccessor memoryAccessor;

    public HsaQuickSorter(TupleMultimapHsa multimap) {
        this.multimap = multimap;
        this.slotSize = JetHashSlotArray.KEY_SIZE;
        this.temporaryBuffer = new byte[JetHashSlotArray.KEY_SIZE];
    }

    public void setMemoryAccessor(MemoryAccessor memoryAccessor) {
        this.memoryAccessor = memoryAccessor;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    @Override
    protected void loadPivot(long index) {
        long slotAddress = loadByIndex(index);
        long recordAddress = multimap.addrOfFirstTupleAt(slotAddress);
        pivotKeyAddress = multimap.addrOfKeyBlockAt(recordAddress);
        pivotKeySize = multimap.sizeOfKeyBlockAt(recordAddress);
    }

    @Override
    protected boolean isLessThanPivot(long index) {
        return compareWithPivot(index) < 0;
    }

    @Override
    protected boolean isGreaterThanPivot(long index) {
        return compareWithPivot(index) > 0;
    }

    @Override
    protected void swap(long index1, long index2) {
        long leftSlotAddress = loadByIndex(index1);
        long rightSlotAddress = loadByIndex(index2);
        memoryAccessor.copyToByteArray(rightSlotAddress, temporaryBuffer, 0, temporaryBuffer.length);
        memoryAccessor.copyMemory(leftSlotAddress, rightSlotAddress, slotSize);
        memoryAccessor.copyFromByteArray(temporaryBuffer, 0, leftSlotAddress, temporaryBuffer.length);
    }

    private int compareWithPivot(long index) {
        long keySlot = loadByIndex(index);
        long recordAddress = multimap.addrOfFirstTupleAt(keySlot);
        long keyAddress = multimap.addrOfKeyBlockAt(recordAddress);
        long keySize = multimap.sizeOfKeyBlockAt(recordAddress);
        return comparator.compare(memoryAccessor, memoryAccessor, keyAddress, keySize, pivotKeyAddress, pivotKeySize);
    }

    private long loadByIndex(long index) {
        return multimap.getHashSlotArray().address() + slotSize * index;
    }
}
