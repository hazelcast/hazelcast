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

package com.hazelcast.jet.memory.impl.binarystorage.sorted;

import com.hazelcast.jet.memory.impl.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout;
import com.hazelcast.jet.memory.api.binarystorage.sorted.JetQuickSorter;

/**
 * Quick sorter implementation for JET openAddressing storage
 */
public class OpenAddressingQuickSorter extends JetQuickSorter {
    private long pivotKeySize;
    private long pivotKeyAddress;

    private final int slotSize;
    private final OALayout layout;
    private final byte[] temporaryBuffer;
    private final BinaryComparator defaultComparator;
    private final ObjectHolder<BinaryComparator> comparatorHolder;

    public OpenAddressingQuickSorter(OALayout layout,
                                     BinaryComparator defaultComparator,
                                     ObjectHolder<BinaryComparator> comparatorHolder
    ) {
        this.layout = layout;
        this.comparatorHolder = comparatorHolder;
        this.defaultComparator = defaultComparator;
        this.slotSize = layout.getSlotSizeInBytes();
        this.temporaryBuffer = new byte[layout.getSlotSizeInBytes()];
    }

    private BinaryComparator getComparator() {
        return comparatorHolder.getObject(defaultComparator);
    }

    @Override
    protected void loadPivot(long index) {
        long slotAddress = loadByIndex(index);
        long recordAddress = layout.getHeaderRecordAddress(slotAddress);
        pivotKeyAddress = layout.getKeyAddress(recordAddress);
        pivotKeySize = layout.getKeyWrittenBytes(recordAddress);
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
        memoryAccessor.copyMemory(
                leftSlotAddress,
                rightSlotAddress,
                slotSize
        );
        memoryAccessor.copyFromByteArray(temporaryBuffer, 0, leftSlotAddress, temporaryBuffer.length);
    }

    private int compareWithPivot(long index) {
        long keySlot = loadByIndex(index);
        long recordAddress = layout.getHeaderRecordAddress(keySlot);
        long keyAddress = layout.getKeyAddress(recordAddress);
        long keySize = layout.getKeyWrittenBytes(recordAddress);

        return getComparator().compare(
                memoryAccessor,
                memoryAccessor,
                keyAddress,
                keySize,
                pivotKeyAddress,
                pivotKeySize
        );
    }

    private long loadByIndex(long index) {
        return layout.getHashSlotAllocator().address() + slotSize * index;
    }
}
