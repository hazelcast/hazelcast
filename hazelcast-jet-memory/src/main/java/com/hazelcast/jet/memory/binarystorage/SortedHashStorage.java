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

package com.hazelcast.jet.memory.binarystorage;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.util.hashslot.HashSlotArray;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.multimap.HsaQuickSorter;
import com.hazelcast.jet.memory.multimap.TupleMultimapHsa;

import java.util.function.LongConsumer;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.jet.memory.binarystorage.SortOrder.ASC;
import static com.hazelcast.jet.memory.multimap.TupleMultimapHsa.DEFAULT_INITIAL_CAPACITY;
import static com.hazelcast.jet.memory.multimap.TupleMultimapHsa.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.jet.memory.multimap.TupleMultimapHsa.KEY_SIZE;

/**
 * Hashtable-based binary key-value storage which can be iterated over in a given sort order.
 */
public class SortedHashStorage extends HashStorage implements SortedStorage {
    private final HashSlotArray8byteKey hsa;

    private final HsaQuickSorter quickSorter;

    private final SortedSlotCursor sortedSlotCursor;

    private final Comparator comparator;

    private final Comparator reverseComparator;

    private long sortedCount = -1L;

    private SortOrder currentSortOrder;


    public SortedHashStorage(MemoryBlock memoryBlock, Comparator comparator, LongConsumer hsaResizeListener) {
        this(memoryBlock, comparator, hsaResizeListener, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public SortedHashStorage(
            MemoryBlock memoryBlock, Comparator comparator, LongConsumer hsaResizeListener,
            int initialCapacity, float loadFactor
    ) {
        super(memoryBlock, comparator.getHasher(), hsaResizeListener, initialCapacity, loadFactor);
        this.hsa = getMultimap().getHashSlotArray();
        this.quickSorter = new HsaQuickSorter(getMultimap());
        if (memoryBlock != null) {
            this.quickSorter.setMemoryAccessor(memoryBlock.getAccessor());
        }
        this.sortedSlotCursor = new SortedSlotCursor();
        this.comparator = comparator;
        this.reverseComparator = new ReverseComparator(comparator);
        getMultimap().setSlotAddedListener(() -> currentSortOrder = null);
    }

    @Override
    public void ensureSorted(SortOrder preferredOrder) {
        if (currentSortOrder != null) {
            return;
        }
        currentSortOrder = preferredOrder;
        sortedCount = super.count();
        getMultimap().compact();
        quickSorter.setComparator(preferredOrder == ASC ? comparator : reverseComparator);
        quickSorter.sort(0, sortedCount);
    }

    @Override
    public long addrOfFirstSlot(SortOrder order) {
        assert currentSortOrder != null : "Attempt to call first() on yet-unsorted storage";
        if (sortedCount == 0) {
            return NULL_ADDRESS;
        }
        return order == currentSortOrder
                ? hsa.address()
                : hsa.address() + ((sortedCount - 1) * KEY_SIZE);
    }

    @Override
    public long addrOfNextSlot(long slotAddress, SortOrder order) {
        assert currentSortOrder != null : "Attempt to call next() on yet-unsorted storage";
        final int slotSize = KEY_SIZE;
        return nullIfInvalid(slotAddress + (order == currentSortOrder ? slotSize : -slotSize), getMultimap());
    }

    @Override
    public SlotAddressCursor slotCursor(SortOrder order) {
        assert currentSortOrder != null : "Attempt to call slotCursor(SortOrder) on yet-unsorted storage";
        sortedSlotCursor.setOrder(order);
        return sortedSlotCursor;
    }

    @Override
    public long count() {
        return currentSortOrder == null ? super.count() : sortedCount;
    }

    @Override
    public void setMemoryBlock(MemoryBlock memoryBlock) {
        currentSortOrder = null;
        quickSorter.setMemoryAccessor(memoryBlock.getAccessor());
        super.setMemoryBlock(memoryBlock);
    }

    @Override
    public void gotoAddress(long address) {
        currentSortOrder = null;
        super.gotoAddress(address);
    }

    @Override
    public void setAlreadySorted(SortOrder order) {
        currentSortOrder = order;
        sortedCount = getMultimap().slotCount();
        sortedSlotCursor.setOrder(order);
    }

    private static long nullIfInvalid(long address, TupleMultimapHsa layout) {
        final HashSlotArray hsa = layout.getHashSlotArray();
        final long allocAddress = hsa.address();
        final int slotLength = KEY_SIZE;
        return address >= allocAddress && address <= allocAddress + (hsa.size() - 1) * slotLength
                ? address : NULL_ADDRESS;
    }


    private class SortedSlotCursor implements SlotAddressCursor {
        private long slotAddress;
        private SortOrder order;

        void setOrder(SortOrder order) {
            this.order = order;
            reset();
        }

        @Override
        public boolean advance() {
            assert order != null : "Cursor is invalid because the underlying storage hasn't been sorted";
            slotAddress = (slotAddress == NULL_ADDRESS) ? addrOfFirstSlot(order) : addrOfNextSlot(slotAddress, order);
            return slotAddress != NULL_ADDRESS;
        }

        @Override
        public long slotAddress() {
            return slotAddress;
        }

        @Override
        public void reset() {
            slotAddress = NULL_ADDRESS;
        }
    }

    private static class ReverseComparator implements Comparator {

        private final Comparator original;

        ReverseComparator(Comparator original) {
            this.original = original;
        }

        @Override
        public int compare(long leftAddress, long leftSize, long rightAddress, long rightSize) {
            return -original.compare(leftAddress, leftSize, rightAddress, rightSize);
        }

        @Override
        public int compare(MemoryAccessor leftAccessor, MemoryAccessor rightAccessor, long leftAddress, long leftSize,
                           long rightAddress, long rightSize
        ) {
            return -original.compare(leftAccessor, rightAccessor, leftAddress, leftSize, rightAddress, rightSize);
        }

        @Override
        public Hasher getHasher() {
            return original.getHasher();
        }

        @Override
        public Hasher getPartitionHasher() {
            return original.getPartitionHasher();
        }
    }
}
