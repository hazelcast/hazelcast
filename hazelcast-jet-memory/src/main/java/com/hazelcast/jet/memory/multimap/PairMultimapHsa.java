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
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.jet.memory.binarystorage.FetchMode;
import com.hazelcast.jet.memory.binarystorage.Hasher;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.util.JetIoUtil;
import com.hazelcast.jet.memory.util.Util;

import java.util.function.LongConsumer;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.jet.memory.binarystorage.FetchMode.CREATE_OR_APPEND;
import static com.hazelcast.jet.memory.binarystorage.FetchMode.JUST_GET;
import static com.hazelcast.jet.memory.util.JetIoUtil.getByte;
import static com.hazelcast.jet.memory.util.JetIoUtil.getLong;
import static com.hazelcast.jet.memory.util.JetIoUtil.putByte;
import static com.hazelcast.jet.memory.util.JetIoUtil.putLong;
import static com.hazelcast.jet.memory.util.JetIoUtil.sizeOfPairAt;
import static com.hazelcast.jet.memory.util.Util.BYTE_0;
import static com.hazelcast.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * A multimap in native memory whose value is a linked list of pairs. Based on the {@code HashSlotArray}
 * structure.
 */
public class PairMultimapHsa {
    public static final int KEY_SIZE = 8;
    public static final int DEFAULT_INITIAL_CAPACITY = 2048;
    public static final float DEFAULT_LOAD_FACTOR = 0.5f;

    /** Size of the first pair's footer */
    public static final int FIRST_FOOTER_SIZE_BYTES =
            // next pair           last pair           pair count          hash code            marker
            LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES;
    /** Size of all other pairs' footer */
    //                                                   next pair
    public static final int OTHER_FOOTER_SIZE_BYTES = LONG_SIZE_IN_BYTES;
    /** Address of the next pair is stored at this offset within the footer. Size: 8 bytes. */
    public static final int NEXT_TUPLE_OFFSET = 0;
    /** Address of the previous pair is stored at this offset within the footer. Size: 8 bytes.  */
    public static final int LAST_TUPLE_OFFSET = 8;
    /** Pair count is stored at this offset within the footer. Size: 8 bytes.  */
    public static final int TUPLE_COUNT_OFFSET = 16;
    /** Pair's hash code is stored at this offset within the footer. Only the first pair has this field.
     *  Size: 8 bytes. */
    public static final int HASH_CODE_OFFSET = 24;
    /** Pair marker is stored at this offset within the footer. Only the first pair has this field.
     *  Size: 1 byte. */
    public static final int MARKER_OFFSET = 32;

    private final Hasher defaultHasher;
    private final CompactionState sourceCompactionState;
    private final CompactionState targetCompactionState;
    private final JetHashSlotArray hsa;
    private MemoryAccessor mem;
    private Runnable slotAddedListener;

    public PairMultimapHsa(MemoryBlock memoryBlock, Hasher defaultHasher, LongConsumer hsaResizeListener,
                           int initialCapacity, float loadFactor
    ) {
        assert Util.isPositivePowerOfTwo(initialCapacity);
        this.defaultHasher = defaultHasher;
        this.sourceCompactionState = new CompactionState();
        this.targetCompactionState = new CompactionState();
        this.hsa = new JetHashSlotArray(hsaResizeListener, initialCapacity, loadFactor);
        if (memoryBlock != null) {
            this.mem = memoryBlock.getAccessor();
            this.hsa.setMemoryManager(memoryBlock.getAuxMemoryManager());
            this.hsa.gotoNew();
        }
    }

    public static long toSlotAddr(long rawHsaAddr) {
        return rawHsaAddr - KEY_SIZE;
    }

    public void setMemoryManager(MemoryManager memMgr) {
        mem = memMgr.getAccessor();
        hsa.setMemoryManager(memMgr);
    }

    public long slotHashCode(long slotAddress) {
        long pairAddress = addrOfFirstPairAt(slotAddress);
        long pairSize = JetIoUtil.sizeOfPairAt(pairAddress, mem);
        return getLong(pairAddress, pairSize + HASH_CODE_OFFSET, mem);
    }

    public void setSlotHashCode(long slotAddress, long hashCode) {
        long pairAddress = addrOfFirstPairAt(slotAddress);
        long pairSize = JetIoUtil.sizeOfPairAt(pairAddress, mem);
        JetIoUtil.putLong(pairAddress, pairSize + HASH_CODE_OFFSET, hashCode, mem);
    }

    public long sizeOfKeyBlockAt(long pairAddress) {
        return JetIoUtil.sizeOfKeyBlockAt(pairAddress, mem);
    }

    public byte slotMarker(long slotAddress) {
        long recordAddress = addrOfFirstPairAt(slotAddress);
        long recordSize = JetIoUtil.sizeOfPairAt(recordAddress, mem);
        return getByte(recordAddress, recordSize + MARKER_OFFSET, mem);
    }

    public void markSlot(long slotAddress, byte marker) {
        long recordAddress = addrOfFirstPairAt(slotAddress);
        long recordSize = JetIoUtil.sizeOfPairAt(recordAddress, mem);
        putByte(recordAddress, recordSize + MARKER_OFFSET, marker, mem);
    }

    public long pairCountAt(long slotAddress) {
        long addrOfFirstPair = addrOfFirstPairAt(slotAddress);
        long sizeOfFirstPair = JetIoUtil.sizeOfPairAt(addrOfFirstPair, mem);
        return getLong(addrOfFirstPair, sizeOfFirstPair + TUPLE_COUNT_OFFSET, mem);
    }

    public long addrOfFirstPairAt(long slotAddress) {
        return mem.getLong(slotAddress);
    }

    /**
     * @return abs(return value) is the address of the slot where the pair was inserted. It is positive
     * if a new slot was assigned, negative otherwise.
     */
    public long fetchSlot(long pairAddress, MemoryAccessor mem, Hasher hasher, FetchMode fetchMode) {
        hsa.setHasher(hasher != null ? hasher : defaultHasher);
        hsa.setLocalMemoryAccessor(mem);
        try {
            if (fetchMode == JUST_GET) {
                final long rawHsaAddr = hsa.get(pairAddress);
                return rawHsaAddr == NULL_ADDRESS ? NULL_ADDRESS : toSlotAddr(rawHsaAddr);
            }
            final long hsaResult = hsa.ensure(pairAddress);
            assert hsaResult != 0;
            final long slotAddress = hsaResult > 0 ? toSlotAddr(hsaResult) : -toSlotAddr(-hsaResult);
            if (slotAddress > 0) {
                initializeFirstFooter(pairAddress);
                if (slotAddedListener != null) {
                    slotAddedListener.run();
                }
            } else if (fetchMode == CREATE_OR_APPEND) {
                updateFooters(-slotAddress, pairAddress);
            }
            return slotAddress;
        } finally {
            hsa.setHasher(defaultHasher);
            hsa.setLocalMemoryAccessor(null);
        }
    }
    public JetHashSlotArray getHashSlotArray() {
        return hsa;
    }

    public long slotCount() {
        return hsa.size();
    }

    public void gotoAddress(long baseAddress) {
        hsa.gotoAddress(baseAddress);
    }

    public void setSlotAddedListener(Runnable listener) {
        this.slotAddedListener = listener;
    }

    public void compact() {
        sourceCompactionState.reset();
        targetCompactionState.reset();
        compactSlotArray();
    }

    public long addrOfNextPair(long pairAddress) {
        return getLong(pairAddress, sizeOfPairAt(pairAddress, mem) + NEXT_TUPLE_OFFSET, mem);
    }

    private void initializeFirstFooter(long pairAddress) {
        long pairSize = sizeOfPairAt(pairAddress, mem);
        putLong(pairAddress, pairSize + NEXT_TUPLE_OFFSET, NULL_ADDRESS, mem);
        putLong(pairAddress, pairSize + LAST_TUPLE_OFFSET, pairAddress, mem);
        putLong(pairAddress, pairSize + TUPLE_COUNT_OFFSET, 1, mem);
        putLong(pairAddress, pairSize + HASH_CODE_OFFSET, hsa.getLastHashCode(), mem);
        putByte(pairAddress, pairSize + MARKER_OFFSET, BYTE_0, mem);
    }

    private void updateFooters(long slotAddress, long pairAddress) {
        long addrOfFirstPair = addrOfFirstPairAt(slotAddress);
        long sizeOfFirstPair = sizeOfPairAt(addrOfFirstPair, mem);
        long addrOfLastPair = getLong(addrOfFirstPair, sizeOfFirstPair + LAST_TUPLE_OFFSET, mem);
        long pairCount = getLong(addrOfFirstPair, sizeOfFirstPair + TUPLE_COUNT_OFFSET, mem);
        long pairSize = sizeOfPairAt(pairAddress, mem);
        putLong(addrOfFirstPair, sizeOfFirstPair + LAST_TUPLE_OFFSET, pairAddress, mem);
        putLong(addrOfFirstPair, sizeOfFirstPair + TUPLE_COUNT_OFFSET, pairCount + 1, mem);
        putLong(addrOfLastPair, sizeOfPairAt(addrOfLastPair, mem) + NEXT_TUPLE_OFFSET, pairAddress, mem);
        putLong(pairAddress, pairSize + NEXT_TUPLE_OFFSET, NULL_ADDRESS, mem);
    }


    private void compactSlotArray() {
        long baseAddress = hsa.address();
        for (long slotIndex = 0; slotIndex < hsa.capacity(); slotIndex++) {
            boolean isAssigned = hsa.isSlotAssigned(baseAddress, slotIndex);
            CompactionState stateToUpdate = isAssigned ? sourceCompactionState : targetCompactionState;
            boolean needToUpdateState = !isAssigned || targetCompactionState.slotCount > 0;
            boolean isLastSlot = slotIndex == hsa.capacity() - 1;
            boolean needToCopy = isLastSlot
                    || !isAssigned && targetCompactionState.slotCount > 0L && sourceCompactionState.slotCount > 0L;
            if (isLastSlot) {
                updateState(baseAddress, slotIndex, stateToUpdate);
                needToUpdateState = false;
            }
            if (needToCopy) {
                copySlots();
            }
            if (needToUpdateState) {
                updateState(baseAddress, slotIndex, stateToUpdate);
            }
        }
    }

    private void updateState(long baseAddress, long slotIndex, CompactionState cState) {
        if (cState.slotAddress == NULL_ADDRESS) {
            cState.slotAddress = hsa.slotBaseAddress(baseAddress, slotIndex);
            cState.slotIndex = slotIndex;
        }
        cState.slotCount++;
    }

    private void copySlots() {
        long slotsToCopy = sourceCompactionState.slotCount;
        mem.copyMemory(sourceCompactionState.slotAddress, targetCompactionState.slotAddress, slotsToCopy * KEY_SIZE);
        long newTargetSlotNumber = targetCompactionState.slotIndex + sourceCompactionState.slotCount;
        targetCompactionState.slotAddress = hsa.slotBaseAddress(hsa.address(), newTargetSlotNumber);
        targetCompactionState.slotIndex = newTargetSlotNumber;
        targetCompactionState.slotCount = Math.abs(sourceCompactionState.slotCount - targetCompactionState.slotCount);
        sourceCompactionState.reset();
    }

    private static class CompactionState {
        long slotCount;
        long slotIndex;
        long slotAddress;

        void reset() {
            slotCount = 0L;
            slotIndex = 0L;
            slotAddress = NULL_ADDRESS;
        }
    }
}
