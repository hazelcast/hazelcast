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
import com.hazelcast.jet.memory.util.JetIoUtil;

import static com.hazelcast.jet.memory.multimap.TupleMultimapHsa.KEY_SIZE;

/**
 * Quick sorter implementation for JET openAddressing storage
 */
public class HsaQuickSorter extends QuickSorter {
    private final TupleMultimapHsa multimap;
    private Comparator comparator;

    private long pivotKeyAddress;
    private long pivotKeySize;
    private MemoryAccessor mem;

    public HsaQuickSorter(TupleMultimapHsa multimap) {
        this.multimap = multimap;
    }

    public void setMemoryAccessor(MemoryAccessor memoryAccessor) {
        this.mem = memoryAccessor;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    @Override
    protected void loadPivot(long index) {
        long slotAddress = addrOfSlotAt(index);
        long tupleAddress = multimap.addrOfFirstTupleAt(slotAddress);
        pivotKeyAddress = JetIoUtil.addressOfKeyBlockAt(tupleAddress);
        pivotKeySize = multimap.sizeOfKeyBlockAt(tupleAddress);
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
        final long addrOfSlot1 = addrOfSlotAt(index1);
        final long addrOfSlot2 = addrOfSlotAt(index2);
        final long tmp = mem.getLong(addrOfSlot1);
        mem.putLong(addrOfSlot1, mem.getLong(addrOfSlot2));
        mem.putLong(addrOfSlot2, tmp);
    }

    private int compareWithPivot(long index) {
        long keySlot = addrOfSlotAt(index);
        long tupleAddress = multimap.addrOfFirstTupleAt(keySlot);
        long keyAddress = JetIoUtil.addressOfKeyBlockAt(tupleAddress);
        long keySize = multimap.sizeOfKeyBlockAt(tupleAddress);
        return comparator.compare(mem, mem, keyAddress, keySize, pivotKeyAddress, pivotKeySize);
    }

    private long addrOfSlotAt(long index) {
        return multimap.getHashSlotArray().address() + KEY_SIZE * index;
    }
}
