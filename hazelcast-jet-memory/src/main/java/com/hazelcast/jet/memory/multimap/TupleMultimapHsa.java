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
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.jet.memory.binarystorage.FetchMode;
import com.hazelcast.jet.memory.binarystorage.Hasher;
import com.hazelcast.jet.memory.binarystorage.ObjectHolder;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.util.JetIoUtil;
import com.hazelcast.jet.memory.util.Util;
import com.hazelcast.nio.Bits;

import java.util.function.LongConsumer;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.jet.memory.binarystorage.FetchMode.CREATE_OR_APPEND;
import static com.hazelcast.jet.memory.binarystorage.FetchMode.JUST_GET;
import static com.hazelcast.jet.memory.multimap.JetHashSlotArray.KEY_SIZE;
import static com.hazelcast.jet.memory.multimap.JetHashSlotArray.toSlotAddr;
import static com.hazelcast.jet.memory.util.JetIoUtil.getByte;
import static com.hazelcast.jet.memory.util.JetIoUtil.getLong;
import static com.hazelcast.jet.memory.util.JetIoUtil.putByte;
import static com.hazelcast.jet.memory.util.JetIoUtil.putLong;
import static com.hazelcast.jet.memory.util.JetIoUtil.sizeOfTupleAt;

/**
 * A multimap in native memory whose value is a linked list of tuples. Based on the {@code HashSlotArray}
 * abstraction.
 */
public class TupleMultimapHsa {
    public static final int DEFAULT_INITIAL_CAPACITY = 2048;
    public static final float DEFAULT_LOAD_FACTOR = 0.5f;

    public static final int FOOTER_SIZE_BYTES = 35;
    public static final int FOOTER_DELTA = FOOTER_SIZE_BYTES - Bits.LONG_SIZE_IN_BYTES - Bits.SHORT_SIZE_IN_BYTES;
    public static final int NEXT_TUPLE_OFFSET = 0;
    public static final int PREV_TUPLE_OFFSET = 8;
    public static final int TUPLE_COUNT_OFFSET = 16;
    public static final int HASH_CODE_OFFSET = 24;
    public static final int MARKER_OFFSET = 32;

    private final Hasher defaultHasher;
    private final ObjectHolder<Hasher> hasherHolder;
    private final ObjectHolder<MemoryAccessor> memoryAccessorHolder;
    private final long slotSize;
    private final CompactionState sourceContext;
    private final CompactionState targetContext;
    private JetHashSlotArray hsa;
    private MemoryAccessor accessor;
    private Runnable layoutListener;

    public TupleMultimapHsa(MemoryBlock memoryBlock, Hasher defaultHasher, LongConsumer hsaResizeListener,
                            int initialCapacity, float loadFactor
    ) {
        assert Util.isPositivePowerOfTwo(initialCapacity);
        this.defaultHasher = defaultHasher;
        this.hasherHolder = new ObjectHolder<>();
        this.memoryAccessorHolder = new ObjectHolder<>();
        this.sourceContext = new CompactionState();
        this.targetContext = new CompactionState();
        this.hsa = new JetHashSlotArray(
                NULL_ADDRESS, defaultHasher, hasherHolder, memoryAccessorHolder,
                hsaResizeListener, HASH_CODE_OFFSET, initialCapacity, loadFactor);
        if (memoryBlock != null) {
            this.accessor = memoryBlock.getAccessor();
            this.hsa.setMemoryManager(memoryBlock.getAuxMemoryManager());
            this.hsa.gotoNew();
        }
        this.slotSize = KEY_SIZE;
    }

    public void setMemoryManager(MemoryManager memMgr) {
        accessor = memMgr.getAccessor();
        hsa.setMemoryManager(memMgr);
    }

    public long slotHashCode(long slotAddress) {
        long tupleAddress = addrOfFirstTupleAt(slotAddress);
        long tupleSize = JetIoUtil.sizeOfTupleAt(tupleAddress, accessor);
        return getLong(tupleAddress, tupleSize + HASH_CODE_OFFSET, accessor);
    }

    public void setSlotHashCode(long slotAddress, long hashCode) {
        long tupleAddress = addrOfFirstTupleAt(slotAddress);
        long tupleSize = JetIoUtil.sizeOfTupleAt(tupleAddress, accessor);
        JetIoUtil.putLong(tupleAddress, tupleSize + HASH_CODE_OFFSET, hashCode, accessor);
    }

    public long addrOfKeyBlockAt(long tupleAddress) {
        return JetIoUtil.addressOfKeyBlockAt(tupleAddress);
    }

    public long sizeOfKeyBlockAt(long tupleAddress) {
        return JetIoUtil.sizeOfKeyBlockAt(tupleAddress, accessor);
    }

    public byte slotMarker(long slotAddress) {
        long recordAddress = addrOfFirstTupleAt(slotAddress);
        long recordSize = JetIoUtil.sizeOfTupleAt(recordAddress, accessor);
        return getByte(recordAddress, recordSize + MARKER_OFFSET, accessor);
    }

    public void markSlot(long slotAddress, byte marker) {
        long recordAddress = addrOfFirstTupleAt(slotAddress);
        long recordSize = JetIoUtil.sizeOfTupleAt(recordAddress, accessor);
        putByte(recordAddress, recordSize + MARKER_OFFSET, marker, accessor);
    }

    public long tupleCountAt(long slotAddress) {
        long addrOfFirstTuple = addrOfFirstTupleAt(slotAddress);
        long sizeOfFirstTuple = JetIoUtil.sizeOfTupleAt(addrOfFirstTuple, accessor);
        return getLong(addrOfFirstTuple, sizeOfFirstTuple + TUPLE_COUNT_OFFSET, accessor);
    }

    public long addrOfFirstTupleAt(long slotAddress) {
        return accessor.getLong(slotAddress);
    }

    /**
     * @return abs(return value) is the address of the slot where the tuple was inserted. It is positive
     * if a new slot was assigned, negative otherwise.
     */
    public long fetchSlot(long tupleAddress, final MemoryAccessor ma, Hasher hasher, FetchMode fetchMode) {
        hasherHolder.set(hasher);
        memoryAccessorHolder.set(ma);
        try {
            if (fetchMode == JUST_GET) {
                final long rawHsaAddr = hsa.get(tupleAddress);
                return rawHsaAddr == NULL_ADDRESS ? NULL_ADDRESS : toSlotAddr(rawHsaAddr);
            }
            long hsaResult = hsa.ensure(tupleAddress);
            assert hsaResult != 0;
            final long slotAddress = hsaResult > 0 ? toSlotAddr(hsaResult) : -toSlotAddr(-hsaResult);
            if (slotAddress > 0) {
                onSlotCreated(slotAddress, tupleAddress);
                if (layoutListener != null) {
                    layoutListener.run();
                }
            } else if (fetchMode == CREATE_OR_APPEND) {
                onSlotUpdated(-slotAddress, tupleAddress);
            }
            return slotAddress;
        } finally {
            memoryAccessorHolder.set(accessor);
            hasherHolder.set(defaultHasher);
        }
    }
    public HashSlotArray8byteKey getHashSlotArray() {
        return hsa;
    }

    public long slotCount() {
        return hsa.size();
    }

    public void gotoAddress(long baseAddress) {
        hsa.gotoAddress(baseAddress);
    }

    public void setSlotCreationListener(Runnable listener) {
        this.layoutListener = listener;
    }

    public void compact() {
        sourceContext.reset();
        targetContext.reset();
        compactSlotArray();
    }

    public long addrOfNextTuple(long tupleAddress) {
        return getLong(tupleAddress, sizeOfTupleAt(tupleAddress, accessor) + NEXT_TUPLE_OFFSET, accessor);
    }

    private void onSlotCreated(long slotAddress, long recordAddress) {
        long recordSize = sizeOfTupleAt(recordAddress, accessor);
        putLong(recordAddress, recordSize + PREV_TUPLE_OFFSET, recordAddress, accessor);
        putLong(recordAddress, recordSize + NEXT_TUPLE_OFFSET, MemoryAllocator.NULL_ADDRESS, accessor);
        putLong(recordAddress, recordSize + HASH_CODE_OFFSET, hsa.getLastHashCode(), accessor);
        putLong(recordAddress, recordSize + TUPLE_COUNT_OFFSET, 1, accessor);
        markSlot(slotAddress, Util.B_ZERO);
    }

    private void onSlotUpdated(long slotAddress, long tupleAddress) {
        long addrOfFirstTuple = addrOfFirstTupleAt(slotAddress);
        long sizeOfFirstTuple = sizeOfTupleAt(addrOfFirstTuple, accessor);
        long addrOfPrevTuple = getLong(addrOfFirstTuple, sizeOfFirstTuple + PREV_TUPLE_OFFSET, accessor);
        long tupleCount = getLong(addrOfFirstTuple, sizeOfFirstTuple + TUPLE_COUNT_OFFSET, accessor);
        long tupleSize = sizeOfTupleAt(tupleAddress, accessor);
        putLong(addrOfFirstTuple, sizeOfFirstTuple + PREV_TUPLE_OFFSET, tupleAddress, accessor);
        putLong(addrOfPrevTuple, sizeOfTupleAt(addrOfPrevTuple, accessor) + NEXT_TUPLE_OFFSET,
                tupleAddress, accessor);
        putLong(addrOfFirstTuple, sizeOfFirstTuple + TUPLE_COUNT_OFFSET, tupleCount + 1, accessor);
        putLong(tupleAddress, tupleSize + NEXT_TUPLE_OFFSET, MemoryAllocator.NULL_ADDRESS, accessor);
    }


    private void compactSlotArray() {
        long baseAddress = hsa.address();
        for (long slotNumber = 0; slotNumber < hsa.capacity(); slotNumber++) {
            boolean isAssigned = hsa.isSlotAssigned(baseAddress, slotNumber);
            CompactionState contextToUpdate = isAssigned ? sourceContext : targetContext;
            boolean needToUpdateContext = !isAssigned || (targetContext.slotsCount > 0);
            boolean needToCopy = !isAssigned && targetContext.slotsCount > 0L && sourceContext.slotsCount > 0L;
            boolean isLastSlot = isLastSlot(slotNumber);
            needToCopy = needToCopy || isLastSlot;
            if (isLastSlot) {
                updateContext(baseAddress, slotNumber, contextToUpdate);
                needToUpdateContext = false;
            }
            if (needToCopy) {
                copySlots();
            }
            if (needToUpdateContext) {
                updateContext(baseAddress, slotNumber, contextToUpdate);
            }
        }
    }

    private void updateContext(long baseAddress, long slotNumber, CompactionState compactionState) {
        if (compactionState.chunkAddress == NULL_ADDRESS) {
            compactionState.chunkAddress = hsa.slotBaseAddress(baseAddress, slotNumber);
            compactionState.slotNumber = slotNumber;
        }
        compactionState.slotsCount++;
    }

    private void copySlots() {
        long slotsToCopy = sourceContext.slotsCount;
        accessor.copyMemory(sourceContext.chunkAddress, targetContext.chunkAddress, slotsToCopy * slotSize);
        long newTargetSlotNumber = targetContext.slotNumber + sourceContext.slotsCount;
        targetContext.chunkAddress = hsa.slotBaseAddress(hsa.address(), newTargetSlotNumber);
        targetContext.slotNumber = newTargetSlotNumber;
        targetContext.slotsCount = Math.abs(sourceContext.slotsCount - targetContext.slotsCount);
        sourceContext.reset();
    }

    private boolean isLastSlot(long slotNumber) {
        return slotNumber == hsa.capacity() - 1;
    }

    private static class CompactionState {
        long slotsCount;
        long slotNumber;
        long chunkAddress;

        void reset() {
            slotsCount = 0L;
            slotNumber = 0L;
            chunkAddress = NULL_ADDRESS;
        }
    }
}
