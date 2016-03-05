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

import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.impl.binarystorage.ObjectHolder;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.api.binarystorage.sorted.JetQuickSorter;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaResizeListener;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.HsaSlotCreationListener;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotSortedIterator;
import com.hazelcast.jet.memory.api.binarystorage.sorted.BinaryKeyValueSortedStorage;
import com.hazelcast.jet.memory.impl.binarystorage.BinaryKeyValueOpenAddressingStorage;
import com.hazelcast.jet.memory.impl.binarystorage.iterator.BinarySlotSortedIteratorImpl;

import static com.hazelcast.jet.memory.impl.util.MemoryUtil.nvl;

import static com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout.DEFAULT_INITIAL_CAPACITY;

public class BinaryKeyValueOpenAddressingSortedStorage<T>
        extends BinaryKeyValueOpenAddressingStorage<T>
        implements BinaryKeyValueSortedStorage<T> {
    private long sortedCount = -1L;

    private final JetQuickSorter quickSorter;

    private OrderingDirection lastDirection;

    private final HashSlotArray8byteKey hsa;

    private final BinarySlotSortedIterator binaryKeySortedIterator;

    private final ObjectHolder<BinaryComparator> comparatorHolder;

    public BinaryKeyValueOpenAddressingSortedStorage(
            MemoryBlock memoryBlock,
            BinaryComparator binaryComparator,
            HsaResizeListener hsaResizeListener) {
        this(
                memoryBlock,
                binaryComparator,
                hsaResizeListener,
                DEFAULT_INITIAL_CAPACITY,
                DEFAULT_LOAD_FACTOR
        );
    }

    public BinaryKeyValueOpenAddressingSortedStorage(
            MemoryBlock memoryBlock,
            BinaryComparator binaryComparator,
            HsaResizeListener hsaResizeListener,
            int initialCapacity,
            float loadFactor) {
        super(
                memoryBlock,
                binaryComparator.getHasher(),
                hsaResizeListener,
                initialCapacity,
                loadFactor
        );

        this.hsa = getLayOut().getHashSlotAllocator();
        this.comparatorHolder = new ObjectHolder<BinaryComparator>();
        this.quickSorter = new OpenAddressingQuickSorter(
                getLayOut(),
                binaryComparator,
                comparatorHolder
        );

        this.quickSorter.setMemoryAccessor(memoryBlock);
        this.binaryKeySortedIterator = new BinarySlotSortedIteratorImpl(this);

        getLayOut().setListener(new HsaSlotCreationListener() {
            @Override
            public void onSlotCreated() {
                lastDirection = null;
            }
        });
    }

    private void checkSorted(OrderingDirection direction) {
        if (lastDirection == null) {
            lastDirection = direction;
            sortedCount = super.count();
            getLayOut().compact();
            quickSorter.sort(0, sortedCount);
        }
    }

    @Override
    public long first(OrderingDirection direction) {
        checkSorted(direction);

        if (sortedCount == 0) {
            return MemoryUtil.NULL_VALUE;
        }

        return
                lastDirection == direction
                        ?
                        hsa.address()
                        :
                        hsa.address() + ((sortedCount - 1) * getLayOut().getSlotSizeInBytes());
    }

    @Override
    public long getNext(long slotAddress, OrderingDirection direction) {
        checkSorted(direction);

        return lastDirection == direction
                ?
                nvl(slotAddress + getLayOut().getSlotSizeInBytes(), getLayOut())
                :
                nvl(slotAddress - getLayOut().getSlotSizeInBytes(), getLayOut());
    }

    @Override
    public BinarySlotSortedIterator slotIterator(OrderingDirection direction) {
        checkSorted(direction);
        binaryKeySortedIterator.setDirection(direction);
        return binaryKeySortedIterator;
    }

    @Override
    public BinarySlotSortedIterator slotIterator() {
        assert lastDirection != null;
        binaryKeySortedIterator.reset();
        return binaryKeySortedIterator;
    }

    @Override
    public long count() {
        if (lastDirection == null) {
            return super.count();
        } else {
            return sortedCount;
        }
    }

    @Override
    public void setMemoryBlock(MemoryBlock memoryBlock) {
        resetSorting();
        quickSorter.setMemoryAccessor(memoryBlock.getAccessor());
        super.setMemoryBlock(memoryBlock);
    }

    @Override
    public void setComparator(BinaryComparator comparator) {
        comparatorHolder.setObject(comparator);
    }

    @Override
    public void gotoAddress(long address) {
        resetSorting();
        super.gotoAddress(address);
    }

    private void resetSorting() {
        lastDirection = null;
    }

    @Override
    public void setSorted(OrderingDirection direction) {
        lastDirection = direction;
        sortedCount = getLayOut().slotsCount();
        binaryKeySortedIterator.setDirection(direction);
    }
}
