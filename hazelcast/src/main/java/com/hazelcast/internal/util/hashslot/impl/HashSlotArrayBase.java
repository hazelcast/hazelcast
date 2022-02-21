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

package com.hazelcast.internal.util.hashslot.impl;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray;
import com.hazelcast.internal.util.hashslot.HashSlotCursor12byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor16byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.internal.util.hashslot.SlotAssignmentResult;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.util.HashUtil.fastLongMix;

/**
 * Implementation of {@link HashSlotArray}, common to all its subtype implementations.
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class HashSlotArrayBase implements HashSlotArray {

    public static final int HEADER_SIZE = 0x18;
    public static final int CAPACITY_OFFSET = -0x8;
    public static final int SIZE_OFFSET = -0x10;
    public static final int EXPAND_THRESHOLD_OFFSET = -0x18;

    protected static final int VALUE_SIZE_GRANULARITY = 8;
    protected static final int KEY_1_OFFSET = 0;
    protected static final int KEY_2_OFFSET = 8;

    /**
     * Sentinel value that marks a slot as "unassigned".
     */
    protected final long unassignedSentinel;

    /**
     * Offset (from the slot's base address) where the unassigned sentinel value is to be found.
     */
    protected final long offsetOfUnassignedSentinel;

    /**
     * Total length of an array slot in bytes.
     */
    protected final int slotLength;

    /**
     * Allows access to memory allocated from {@link #malloc}
     */
    private MemoryAccessor mem;

    /**
     * Memory allocator
     */
    private MemoryAllocator malloc;

    // For temporary storage during resizing. Should allocate from a different memory pool
    // than the main allocator. Can be null; in that case the main malloc will be used.
    private MemoryAllocator auxMalloc;

    /**
     * Base address of the memory region containing the hash slots. Preceded by a header that stores metadata
     * ({@code capacity}, {@code size}, and {@code expandAt}).
     */
    private long baseAddress = HEADER_SIZE;

    /**
     * The initial capacity of a newly created array.
     */
    private final int initialCapacity;

    /**
     * Offset of the value block from the slot's base address.
     */
    private final int valueOffset;

    /**
     * Length of the value block in bytes.
     */
    private final int valueLength;

    /**
     * The maximum load factor ({@code size} / {@code capacity}) for this hash slot array.
     * The array will be expanded as needed to enforce this limit.
     */
    private final float loadFactor;

    /**
     * Flyweight return object for the {@link #ensure0(long, long)} that contains
     * information about the slot assignment - the slot address and if the key
     * was already present in the map or if the slot is newly assigned.
     */
    private final SlotAssignmentResultImpl slotAssignmentResult = new SlotAssignmentResultImpl();

    /**
     * Constructs a new {@code HashSlotArrayImpl} with the given initial capacity and the load factor.
     * {@code valueLength} must be a factor of 8. <strong>Does not allocate any memory</strong>, therefore
     * the instance is unusable until one of the {@code goto...} methods is called.
     *
     * @param unassignedSentinel the value to be used to mark an unassigned slot
     * @param offsetOfUnassignedSentinel offset (from each slot's base address) where the unassigned sentinel is kept
     * @param mm the memory manager
     * @param auxMalloc memory allocator to use for temporary storage during resizing. Its memory must be accessible
     *                  by the supplied memory manager's accessor.
     * @param keyLength length of key in bytes
     * @param valueLength length of value in bytes
     * @param initialCapacity Initial capacity of map (will be rounded to closest power of 2, if not already)
     */
    protected HashSlotArrayBase(long unassignedSentinel, long offsetOfUnassignedSentinel,
                                MemoryManager mm, MemoryAllocator auxMalloc,
                                int keyLength, int valueLength, int initialCapacity, float loadFactor
    ) {
        this.unassignedSentinel = unassignedSentinel;
        this.offsetOfUnassignedSentinel = offsetOfUnassignedSentinel;
        if (mm != null) {
            this.malloc = mm.getAllocator();
            this.mem = mm.getAccessor();
        }
        this.auxMalloc = auxMalloc;
        this.valueOffset = keyLength;
        this.valueLength = valueLength;
        this.slotLength = keyLength + valueLength;
        this.initialCapacity = initialCapacity;
        this.loadFactor = loadFactor;
    }


    // These public final methods implement the general HashSlotArray interface

    @Override
    public final long address() {
        return baseAddress;
    }

    @Override
    public final void gotoAddress(long address) {
        baseAddress = address;
    }

    @Override
    public final long gotoNew() {
        allocateInitial();
        return address();
    }

    @Override
    public final long size() {
        assertValid();
        return mem.getLong(baseAddress + SIZE_OFFSET);
    }

    @Override public final long capacity() {
        assertValid();
        return mem.getLong(baseAddress + CAPACITY_OFFSET);
    }

    @Override public final long expansionThreshold() {
        assertValid();
        return mem.getLong(baseAddress + EXPAND_THRESHOLD_OFFSET);
    }

    @Override
    public final void clear() {
        assertValid();
        markAllUnassigned();
        setSize(0);
    }

    @Override
    public final boolean trimToSize() {
        final long minCapacity = minCapacityForSize(size(), loadFactor);
        if (capacity() <= minCapacity) {
            return false;
        }
        resizeTo(minCapacity);
        assert expansionThreshold() >= size() : String.format(
                "trimToSize() shrunk the capacity to %,d and expandAt to %,d, which is less than the current size %,d",
                capacity(), expansionThreshold(), size());
        return true;
    }

    @Override
    public final void dispose() {
        if (baseAddress <= HEADER_SIZE) {
            return;
        }
        malloc.free(baseAddress - HEADER_SIZE, HEADER_SIZE + capacity() * slotLength);
        baseAddress = -1L;
    }


    // Additional non-interface public method. Has a specific use case in Hot Restart.

    /**
     * Migrates the backing memory region to a new allocator, freeing the current region. Memory allocated by the
     * new allocator must be accessible by the same accessor as the current one.
     */
    public final void migrateTo(MemoryAllocator newMalloc) {
        baseAddress = move(baseAddress, capacity(), malloc, newMalloc);
        malloc = newMalloc;
        auxMalloc = null;
    }


    // These protected final methods will be called from the subclasses

    protected final SlotAssignmentResult ensure0(long key1, long key2) {
        assertValid();
        final long size = size();
        if (size == expansionThreshold()) {
            resizeTo(CapacityUtil.nextCapacity(capacity()));
        }
        long slot = keyHash(key1, key2) & mask();
        while (isSlotAssigned(slot)) {
            if (equal(key1OfSlot(slot), key2OfSlot(slot), key1, key2)) {
                slotAssignmentResult.setAddress(valueAddrOfSlot(slot));
                slotAssignmentResult.setNew(false);
                return slotAssignmentResult;
            }
            slot = (slot + 1) & mask();
        }
        setSize(size + 1);
        putKey(baseAddress, slot, key1, key2);
        slotAssignmentResult.setAddress(valueAddrOfSlot(slot));
        slotAssignmentResult.setNew(true);
        return slotAssignmentResult;
    }

    protected final long get0(long key1, long key2) {
        assertValid();
        long slot = keyHash(key1, key2) & mask();
        final long wrappedAround = slot;
        while (isSlotAssigned(slot)) {
            if (equal(key1OfSlot(slot), key2OfSlot(slot), key1, key2)) {
                return valueAddrOfSlot(slot);
            }
            slot = (slot + 1) & mask();
            if (slot == wrappedAround) {
                break;
            }
        }
        return NULL_ADDRESS;
    }

    protected final boolean remove0(long key1, long key2) {
        assertValid();
        long slot = keyHash(key1, key2) & mask();
        final long wrappedAround = slot;
        while (isSlotAssigned(slot)) {
            if (equal(key1OfSlot(slot), key2OfSlot(slot), key1, key2)) {
                setSize(size() - 1);
                shiftConflictingKeys(slot);
                return true;
            }
            slot = (slot + 1) & mask();
            if (slot == wrappedAround) {
                break;
            }
        }
        return false;
    }

    /**
     * Shift all the slot-conflicting keys allocated to (and including) <code>slot</code>.
     */
    @SuppressWarnings("checkstyle:innerassignment")
    protected final void shiftConflictingKeys(long slotCurr) {
        long slotPrev;
        long slotOther;
        final long mask = mask();
        while (true) {
            slotCurr = ((slotPrev = slotCurr) + 1) & mask;
            while (isSlotAssigned(slotCurr)) {
                slotOther = slotHash(baseAddress, slotCurr) & mask;
                // slotPrev <= slotCurr means we're at or to the right of the original slot.
                // slotPrev > slotCurr means we're to the left of the original slot because we've wrapped around.
                if (slotPrev <= slotCurr) {
                    if (slotPrev >= slotOther || slotOther > slotCurr) {
                        break;
                    }
                } else if (slotPrev >= slotOther && slotOther > slotCurr) {
                    break;
                }
                slotCurr = (slotCurr + 1) & mask;
            }
            if (!isSlotAssigned(slotCurr)) {
                break;
            }
            // Shift key/value pair.
            putKey(baseAddress, slotPrev, key1OfSlot(slotCurr), key2OfSlot(slotCurr));
            mem.copyMemory(valueAddrOfSlot(slotCurr), valueAddrOfSlot(slotPrev), valueLength);
        }
        markUnassigned(baseAddress, slotPrev);
    }

    protected final void allocateArrayAndAdjustFields(long size, long newCapacity) {
        baseAddress = malloc.allocate(HEADER_SIZE + newCapacity * slotLength) + HEADER_SIZE;
        setSize(size);
        setCapacity(newCapacity);
        setExpansionThreshold(maxSizeForCapacity(newCapacity, loadFactor));
        markAllUnassigned();
    }

    protected final void rehash(long oldCapacity, long oldAddress) {
        final long mask = mask();
        for (long slot = oldCapacity; --slot >= 0;) {
            if (!isAssigned(oldAddress, slot)) {
                continue;
            }
            long newSlot = slotHash(oldAddress, slot) & mask;
            while (isSlotAssigned(newSlot)) {
                newSlot = (newSlot + 1) & mask;
            }
            putKey(baseAddress, newSlot, key1OfSlot(oldAddress, slot), key2OfSlot(oldAddress, slot));
            final long valueAddrOfOldSlot = slotBase(oldAddress, slot) + valueOffset;
            mem.copyMemory(valueAddrOfOldSlot, valueAddrOfSlot(newSlot), valueLength);
        }
    }

    protected final void setMemMgr(MemoryManager memoryManager) {
        mem = memoryManager.getAccessor();
        malloc = memoryManager.getAllocator();
    }

    protected final void assertValid() {
        assert baseAddress - HEADER_SIZE != NULL_ADDRESS
                : "This instance doesn't point to a valid hashtable. Base address = " + baseAddress;
    }

    protected final MemoryAllocator malloc() {
        return malloc;
    }

    protected final MemoryAccessor mem() {
        return mem;
    }

    protected final long slotBase(long baseAddr, long slot) {
        return baseAddr + slotLength * slot;
    }


    // These protected methods will be overridden in some subclasses

    /**
     * Allocates a new slot array with the requested size and moves all the
     * assigned slots from the current array into the new one.
     */
    protected void resizeTo(long newCapacity) {
        final long oldCapacity = capacity();
        final long oldAllocatedSize = HEADER_SIZE + oldCapacity * slotLength;
        final MemoryAllocator oldMalloc;
        final long oldAddress;
        if (auxMalloc != null) {
            final long size = size();
            oldAddress = move(baseAddress, oldCapacity, malloc, auxMalloc);
            oldMalloc = auxMalloc;
            auxAllocateAndAdjustFields(oldAddress, size, oldCapacity, newCapacity);
        } else {
            oldMalloc = malloc;
            oldAddress = baseAddress;
            allocateArrayAndAdjustFields(size(), newCapacity);
        }
        rehash(oldCapacity, oldAddress);
        oldMalloc.free(oldAddress - HEADER_SIZE, oldAllocatedSize);
    }

    protected long key1OfSlot(long baseAddress, long slot) {
        return mem.getLong(slotBase(baseAddress, slot) + KEY_1_OFFSET);
    }

    protected long key2OfSlot(long baseAddress, long slot) {
        return mem.getLong(slotBase(baseAddress, slot) + KEY_2_OFFSET);
    }

    protected boolean isAssigned(long baseAddress, long slot) {
        return mem.getLong(slotBase(baseAddress, slot) + offsetOfUnassignedSentinel) != unassignedSentinel;
    }

    protected void markUnassigned(long baseAddress, long slot) {
        mem.putLong(slotBase(baseAddress, slot) + offsetOfUnassignedSentinel, unassignedSentinel);
    }

    protected void putKey(long baseAddress, long slot, long key1, long key2) {
        final long slotBase = slotBase(baseAddress, slot);
        mem.putLong(slotBase + KEY_1_OFFSET, key1);
        mem.putLong(slotBase + KEY_2_OFFSET, key2);
    }

    protected long keyHash(long key1, long key2) {
        return fastLongMix(fastLongMix(key1) + key2);
    }

    protected long slotHash(long baseAddress, long slot) {
        return keyHash(key1OfSlot(baseAddress, slot), key2OfSlot(baseAddress, slot));
    }

    protected boolean equal(long key1a, long key2a, long key1b, long key2b) {
        return key1a == key1b && key2a == key2b;
    }


    // These are private instance methods

    private void setCapacity(long capacity) {
        assertValid();
        mem.putLong(baseAddress + CAPACITY_OFFSET, capacity);
    }

    private void setExpansionThreshold(long thresh) {
        assertValid();
        mem.putLong(baseAddress + EXPAND_THRESHOLD_OFFSET, thresh);
    }

    /** Bit mask used to compute the slot index. */
    private long mask() {
        return capacity() - 1;
    }

    private void setSize(long newSize) {
        mem.putLong(baseAddress + SIZE_OFFSET, newSize);
    }

    private void allocateInitial() {
        allocateArrayAndAdjustFields(0, CapacityUtil.roundCapacity((int) (initialCapacity / loadFactor)));
    }

    private long key1OfSlot(long slot) {
        return key1OfSlot(baseAddress, slot);
    }

    private long key2OfSlot(long slot) {
        return key2OfSlot(baseAddress, slot);
    }

    private long valueAddrOfSlot(long slot) {
        return slotBase(baseAddress, slot) + valueOffset;
    }

    private boolean isSlotAssigned(long slot) {
        return isAssigned(baseAddress, slot);
    }

    private void auxAllocateAndAdjustFields(long auxAddress, long size, long oldCapacity, long newCapacity) {
        try {
            allocateArrayAndAdjustFields(size, newCapacity);
        } catch (Error e) {
            try {
                // Try to restore state prior to allocation failure
                baseAddress = move(auxAddress, oldCapacity, auxMalloc, malloc);
            } catch (Error e1) {
                baseAddress = NULL_ADDRESS;
            }
            throw e;
        }
    }

    /** Copies a block from one allocator to another, then frees the source block. */
    private long move(long fromBaseAddress, long capacity, MemoryAllocator fromMalloc, MemoryAllocator toMalloc) {
        final long allocatedSize = HEADER_SIZE + capacity * slotLength;
        final long toBaseAddress = toMalloc.allocate(allocatedSize) + HEADER_SIZE;
        mem.copyMemory(fromBaseAddress - HEADER_SIZE, toBaseAddress - HEADER_SIZE, allocatedSize);
        fromMalloc.free(fromBaseAddress - HEADER_SIZE, allocatedSize);
        return toBaseAddress;
    }

    private void markAllUnassigned() {
        final long capacity = capacity();
        for (long i = 0; i < capacity; i++) {
            markUnassigned(baseAddress, i);
        }
    }

    private static long maxSizeForCapacity(long capacity, float loadFactor) {
        return Math.max(2, (long) Math.ceil(capacity * loadFactor)) - 1;
    }

    private static long minCapacityForSize(long size, float loadFactor) {
        return CapacityUtil.roundCapacity((long) Math.ceil(size / loadFactor));
    }


    protected class Cursor implements HashSlotCursor8byteKey {

        long currentSlot;

        public Cursor() {
            reset();
        }

        @Override
        public final void reset() {
            currentSlot = -1;
        }

        @Override public final boolean advance() {
            assertValid();
            assert currentSlot != Long.MIN_VALUE : "Cursor has advanced past the last slot";
            if (tryAdvance()) {
                return true;
            }
            currentSlot = Long.MIN_VALUE;
            return false;
        }

        @Override public final long key() {
            return key1();
        }

        public final long key1() {
            assertCursorValid();
            return key1OfSlot(currentSlot);
        }

        @Override public final long valueAddress() {
            assertCursorValid();
            return valueAddrOfSlot(currentSlot);
        }

        final void assertCursorValid() {
            assertValid();
            assert currentSlot >= 0 : "Cursor is invalid";
        }

        private boolean tryAdvance() {
            final long capacity = capacity();
            for (long slot = currentSlot + 1; slot < capacity; slot++) {
                if (isSlotAssigned(slot)) {
                    currentSlot = slot;
                    return true;
                }
            }
            return false;
        }
    }

    protected final class CursorIntKey2 extends Cursor implements HashSlotCursor12byteKey {
        @Override public int key2() {
            assertCursorValid();
            return (int) key2OfSlot(currentSlot);
        }
    }

    protected final class CursorLongKey2 extends Cursor implements HashSlotCursor16byteKey {
        @Override public long key2() {
            assertCursorValid();
            return key2OfSlot(currentSlot);
        }
    }
}
