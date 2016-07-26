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
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.memory.serialization.MemoryDataOutput;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursorImpl;
import com.hazelcast.jet.memory.binarystorage.cursor.PairAddressCursor;
import com.hazelcast.jet.memory.binarystorage.cursor.PairAddressCursorImpl;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.multimap.PairMultimapHsa;

import java.util.function.LongConsumer;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.jet.memory.binarystorage.FetchMode.CREATE_IF_ABSENT;
import static com.hazelcast.jet.memory.binarystorage.FetchMode.CREATE_OR_APPEND;
import static com.hazelcast.jet.memory.binarystorage.FetchMode.JUST_GET;
import static com.hazelcast.jet.memory.multimap.PairMultimapHsa.DEFAULT_INITIAL_CAPACITY;
import static com.hazelcast.jet.memory.multimap.PairMultimapHsa.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.jet.memory.multimap.PairMultimapHsa.FIRST_FOOTER_SIZE_BYTES;
import static com.hazelcast.jet.memory.multimap.PairMultimapHsa.OTHER_FOOTER_SIZE_BYTES;
import static com.hazelcast.jet.memory.util.JetIoUtil.writePair;

/**
 * Hashtable-based binary key-value storage.
 */
@SuppressWarnings({
        "checkstyle:methodcount"
})
public class HashStorage implements Storage {
    private final PairMultimapHsa multimap;
    private MemoryBlock memoryBlock;
    private final SlotAddressCursor slotCursor;
    private final PairAddressCursor pairCursor;

    public HashStorage(MemoryBlock memoryBlock, Hasher hasher, LongConsumer hsaResizeListener) {
        this(memoryBlock, hasher, hsaResizeListener, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public HashStorage(MemoryBlock memoryBlock, Hasher hasher, LongConsumer hsaResizeListener,
                       int initialCapacity, float loadFactor
    ) {
        this.multimap = new PairMultimapHsa(memoryBlock, hasher, hsaResizeListener, initialCapacity, loadFactor);
        this.memoryBlock = memoryBlock != null ? memoryBlock : null;
        this.slotCursor = new SlotAddressCursorImpl(getMultimap());
        this.pairCursor = new PairAddressCursorImpl(getMultimap());
    }


    @Override
    public long addrOfNextPair(long pairAddress) {
        return multimap.addrOfNextPair(pairAddress);
    }

    @Override
    public long addrOfFirstPair(long slotAddress) {
        return multimap.addrOfFirstPairAt(slotAddress);
    }

    @Override
    public long getOrCreateSlotWithSameKey(long pairAddress, Comparator comparator) {
        return multimap.fetchSlot(pairAddress, memoryBlock.getAccessor(), getHasher(comparator), CREATE_IF_ABSENT);
    }

    @Override
    public long addrOfSlotWithSameKey(long pairAddress, MemoryAccessor pairAccessor) {
        return multimap.fetchSlot(pairAddress, pairAccessor, null, JUST_GET);
    }

    @Override
    public long addrOfSlotWithSameKey(long recordAddress, Comparator comparator, MemoryAccessor pairAccessor) {
        return multimap.fetchSlot(recordAddress, pairAccessor, getHasher(comparator), JUST_GET);
    }

    @Override
    public long pairCountAt(long recordAddress) {
        return multimap.pairCountAt(recordAddress);
    }

    @Override
    public void markSlot(long slotAddress, byte marker) {
        multimap.markSlot(slotAddress, marker);
    }

    @Override
    public byte getSlotMarker(long slotAddress) {
        return multimap.slotMarker(slotAddress);
    }

    @Override
    public void setSlotHashCode(long slotAddress, long hashCode) {
        multimap.setSlotHashCode(slotAddress, hashCode);
    }

    @Override
    public long getSlotHashCode(long slotAddress) {
        return multimap.slotHashCode(slotAddress);
    }

    @Override
    public long count() {
        return multimap.slotCount();
    }

    @Override
    public long gotoNew() {
        return ((HashSlotArray) multimap.getHashSlotArray()).gotoNew();
    }

    @Override
    public void gotoAddress(long baseAddress) {
        multimap.gotoAddress(baseAddress);
    }

    @Override
    public void insertPair(Pair pair, SerializationOptimizer optimizer, MemoryDataOutput output) {
        writePair(pair, output, memoryBlock);
        output.skip(PairMultimapHsa.FIRST_FOOTER_SIZE_BYTES);
        final long slotAddr = multimap.fetchSlot(output.baseAddress(), memoryBlock.getAccessor(), null, CREATE_OR_APPEND);
        adjustAllocatedSizeAsNeeded(slotAddr);
    }

    @Override
    public long insertPair(long pairAddress, Comparator comparator) {
        long slotAddr = multimap.fetchSlot(pairAddress, memoryBlock.getAccessor(), getHasher(comparator), CREATE_OR_APPEND);
        adjustAllocatedSizeAsNeeded(slotAddr);
        return slotAddr;
    }

    @Override
    public PairAddressCursor pairCursor(long slotAddress) {
        pairCursor.reset(slotAddress, 0);
        return pairCursor;
    }

    @Override
    public SlotAddressCursor slotCursor() {
        slotCursor.reset();
        return slotCursor;
    }

    @Override
    public void setMemoryBlock(MemoryBlock memoryBlock) {
        this.memoryBlock = memoryBlock;
        this.multimap.setMemoryManager(memoryBlock.getAuxMemoryManager());
    }

    @Override
    public MemoryBlock getMemoryBlock() {
        return memoryBlock;
    }

    public PairMultimapHsa getMultimap() {
        return multimap;
    }

    protected static Hasher getHasher(Comparator comparator) {
        return comparator == null ? null : comparator.getHasher();
    }

    private void adjustAllocatedSizeAsNeeded(long slotAddr) {
        // negative slotAddr => slot already existed => currently inserted pair is not the first one in the slot
        // => reduce allocated size by the difference between the size of first pair's footer and other pair's footer
        if (slotAddr < 0) {
            memoryBlock.getAllocator().free(NULL_ADDRESS, FIRST_FOOTER_SIZE_BYTES - OTHER_FOOTER_SIZE_BYTES);
        }
    }
}
