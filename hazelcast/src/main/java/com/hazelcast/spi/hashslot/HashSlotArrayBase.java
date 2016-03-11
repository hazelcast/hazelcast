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

package com.hazelcast.spi.hashslot;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryManager;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.spi.hashslot.CapacityUtil.nextCapacity;
import static com.hazelcast.spi.hashslot.CapacityUtil.roundCapacity;
import static com.hazelcast.util.HashUtil.fastLongMix;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;

/**
 * Common implementation base for {@link HashSlotArray} and {@link HashSlotArrayTwinKey}.
 */
@SuppressWarnings("checkstyle:methodcount")
abstract class HashSlotArrayBase {

    protected static final int KEY_1_OFFSET = 0;
    private static final int KEY_2_OFFSET = 8;
    private static final int VALUE_LENGTH_GRANULARITY = 8;
    private static final int HEADER_SIZE = 0x18;
    private static final int CAPACITY_OFFSET = -0x8;
    private static final int SIZE_OFFSET = -0x10;
    private static final int EXPAND_AT_OFFSET = -0x18;

    /**
     * Sentinel value that marks a slot as "unassigned".
     */
    protected final long unassignedSentinel;

    /**
     * Offset (from the slot's base address) where the unassigned sentinel value is to be found.
     */
    protected final long offsetOfUnassignedSentinel;

    /**
     * Allows access to memory allocated from {@link #malloc}
     */
    protected final MemoryAccessor mem;

    /**
     * Memory allocator
     */
    private MemoryAllocator malloc;

    // For temporary storage during resizing. Should allocate from a different memory pool
    // than the main allocator. Can be null; in that case the main malloc will be used.
    private MemoryAllocator auxMalloc;

    /**
     * Base address of the memory region containing the hash slots. Preceded by a header that stores metadata
     * ({@code capacity}, {@code mask}, {@code size}, and {@code expandAt}).
     */
    private long baseAddress;

    /**
     * The initial capacity of a newly created array.
     */
    private final int initialCapacity;

    /**
     * Total length of an array slot in bytes.
     */
    private final int slotLength;

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
        assert modPowerOfTwo(valueLength, VALUE_LENGTH_GRANULARITY) == 0
                : "Value length must be a positive multiple of 8, but was " + valueLength;
        this.unassignedSentinel = unassignedSentinel;
        this.offsetOfUnassignedSentinel = offsetOfUnassignedSentinel;
        this.malloc = mm.getAllocator();
        this.mem = mm.getAccessor();
        this.auxMalloc = auxMalloc;
        this.valueOffset = keyLength;
        this.valueLength = valueLength;
        this.slotLength = keyLength + valueLength;
        this.initialCapacity = initialCapacity;
        this.loadFactor = loadFactor;
    }


    // These public final methods will automatically fit as interface implementation in subclasses

    public final long address() {
        return baseAddress - HEADER_SIZE;
    }

    public final void gotoAddress(long address) {
        baseAddress = address + HEADER_SIZE;
    }

    public final long gotoNew() {
        allocateInitial();
        return address();
    }

    public final long size() {
        ensureLive();
        return mem.getLong(baseAddress + SIZE_OFFSET);
    }

    public final void clear() {
        ensureLive();
        markAllUnassigned();
        setSize(0);
    }

    public final boolean trimToSize() {
        final long minCapacity = minCapacityForSize(size(), loadFactor);
        if (capacity() <= minCapacity) {
            return false;
        }
        resizeTo(minCapacity);
        assert expandAt() >= size() : String.format(
                "trimToSize() shrunk the capacity to %,d and expandAt to %,d, which is less than the current size %,d",
                capacity(), expandAt(), size());
        return true;
    }

    /**
     * Migrates the backing memory region to a new allocator, freeing the current region. Memory allocated by the
     * new allocator must be accessible by the same accessor as the current one.
     */
    public final void migrateTo(MemoryAllocator newMalloc) {
        baseAddress = move(baseAddress, capacity(), malloc, newMalloc);
        malloc = newMalloc;
        auxMalloc = null;
    }

    public final void dispose() {
        if (baseAddress <= HEADER_SIZE) {
            return;
        }
        malloc.free(baseAddress - HEADER_SIZE, HEADER_SIZE + capacity() * slotLength);
        baseAddress = -1L;
    }


    // These protected final methods will be called from the subclasses

    protected final long slotBase(long baseAddr, long slot) {
        return baseAddr + slotLength * slot;
    }

    protected final long ensure0(long key1, long key2) {
        ensureLive();
        final long size = size();
        if (size == expandAt()) {
            resizeTo(nextCapacity(capacity()));
        }
        long slot = hash(key1, key2) & mask();
        while (isAssigned(slot)) {
            if (equal(key1OfSlot(slot), key2OfSlot(slot), key1, key2)) {
                return -valueAddrOfSlot(slot);
            }
            slot = (slot + 1) & mask();
        }
        setSize(size + 1);
        putKey(slot, key1, key2);
        return valueAddrOfSlot(slot);
    }

    protected final long get0(long key1, long key2) {
        ensureLive();
        long slot = hash(key1, key2) & mask();
        final long wrappedAround = slot;
        while (isAssigned(slot)) {
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
        ensureLive();
        long slot = hash(key1, key2) & mask();
        final long wrappedAround = slot;
        while (isAssigned(slot)) {
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
            while (isAssigned(slotCurr)) {
                slotOther = hash(key1OfSlot(slotCurr), key2OfSlot(slotCurr)) & mask;
                if (slotPrev <= slotCurr) {
                    // we're to the right of the original slot.
                    if (slotPrev >= slotOther || slotOther > slotCurr) {
                        break;
                    }
                } else {
                    // we've wrapped around.
                    if (slotPrev >= slotOther && slotOther > slotCurr) {
                        break;
                    }
                }
                slotCurr = (slotCurr + 1) & mask;
            }
            if (!isAssigned(slotCurr)) {
                break;
            }
            // Shift key/value pair.
            putKey(slotPrev, key1OfSlot(slotCurr), key2OfSlot(slotCurr));
            mem.copyMemory(valueAddrOfSlot(slotCurr), valueAddrOfSlot(slotPrev), valueLength);
        }
        final long slotBase = slotBase(slotPrev);
        mem.setMemory(slotBase, slotLength, (byte) 0);
        mem.putLong(slotBase + offsetOfUnassignedSentinel, unassignedSentinel);
    }

    protected final void ensureLive() {
        assert baseAddress >= HEADER_SIZE : "This instance doesn't point to a valid hashtable";
    }

    protected final long slotBase(long slot) {
        return slotBase(baseAddress, slot);
    }


    // These protected methods will be overridden in some subclasses

    protected long key2OfSlot(long slot) {
        return mem.getLong(slotBase(baseAddress, slot) + KEY_2_OFFSET);
    }

    protected long key2OfSlot(long baseAddress, long slot) {
        return mem.getLong(slotBase(baseAddress, slot) + KEY_2_OFFSET);
    }

    protected long hash(long key1, long key2) {
        return fastLongMix(fastLongMix(key1) + key2);
    }

    protected boolean equal(long key1a, long key2a, long key1b, long key2b) {
        return key1a == key1b && key2a == key2b;
    }

    protected void putKey(long slot, long key1, long key2) {
        final long slotBase = slotBase(baseAddress, slot);
        mem.putLong(slotBase + KEY_1_OFFSET, key1);
        mem.putLong(slotBase + KEY_2_OFFSET, key2);
    }


    // These are private instance methods

    /** Capacity (in terms of slots) of the currently allocated memory region. */
    private long capacity() {
        ensureLive();
        return mem.getLong(baseAddress + CAPACITY_OFFSET);
    }

    private void setCapacity(long capacity) {
        ensureLive();
        mem.putLong(baseAddress + CAPACITY_OFFSET, capacity);
    }

    /** Resize the array when {@code size} hits this value. */
    private long expandAt() {
        ensureLive();
        return mem.getLong(baseAddress + EXPAND_AT_OFFSET);
    }

    private void setExpandAt(long expandAt) {
        ensureLive();
        mem.putLong(baseAddress + EXPAND_AT_OFFSET, expandAt);
    }

    /** Bit mask used to compute the slot index. */
    private long mask() {
        return capacity() - 1;
    }

    private void setSize(long newSize) {
        mem.putLong(baseAddress + SIZE_OFFSET, newSize);
    }

    private void allocateInitial() {
        allocateArrayAndAdjustFields(0, roundCapacity((int) (initialCapacity / loadFactor)));
    }

    private boolean isAssigned(long baseAddr, long slot) {
        return mem.getLong(slotBase(baseAddr, slot) + offsetOfUnassignedSentinel) != unassignedSentinel;
    }

    private boolean isAssigned(long slot) {
        return isAssigned(baseAddress, slot);
    }

    private long key1OfSlot(long slot) {
        return mem.getLong(slotBase(baseAddress, slot) + KEY_1_OFFSET);
    }

    private long valueAddrOfSlot(long slot) {
        return slotBase(baseAddress, slot) + valueOffset;
    }

    private long key1OfSlot(long baseAddress, long slot) {
        return mem.getLong(slotBase(baseAddress, slot) + KEY_1_OFFSET);
    }

    private void allocateArrayAndAdjustFields(long size, long newCapacity) {
        baseAddress = malloc.allocate(HEADER_SIZE + newCapacity * slotLength) + HEADER_SIZE;
        setSize(size);
        setCapacity(newCapacity);
        setExpandAt(maxSizeForCapacity(newCapacity, loadFactor));
        markAllUnassigned();
    }

    private void auxAllocateAndAdjustFields(long auxAddress, long oldCapacity, long newCapacity) {
        try {
            allocateArrayAndAdjustFields(size(), newCapacity);
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
        final long addrOfFirstSentinel = baseAddress + offsetOfUnassignedSentinel;
        final int stride = slotLength;
        for (long i = 0; i < capacity; i++) {
            mem.putLong(addrOfFirstSentinel + stride * i, unassignedSentinel);
        }
    }

    /**
     * Allocate a new slot array with the requested size and move all the
     * assigned slots from the current array into the new one.
     */
    private void resizeTo(long newCapacity) {
        final long oldCapacity = capacity();
        final long oldAllocatedSize = HEADER_SIZE + oldCapacity * slotLength;
        final MemoryAllocator oldMalloc;
        final long oldAddress;
        if (auxMalloc != null) {
            oldAddress = move(baseAddress, oldCapacity, malloc, auxMalloc);
            oldMalloc = auxMalloc;
            auxAllocateAndAdjustFields(oldAddress, oldCapacity, newCapacity);
        } else {
            oldMalloc = malloc;
            oldAddress = baseAddress;
            allocateArrayAndAdjustFields(size(), newCapacity);
        }
        final long mask = mask();
        for (long slot = oldCapacity; --slot >= 0;) {
            if (isAssigned(oldAddress, slot)) {
                long key1 = key1OfSlot(oldAddress, slot);
                long key2 = key2OfSlot(oldAddress, slot);
                long valueAddress = slotBase(oldAddress, slot) + valueOffset;
                long newSlot = hash(key1, key2) & mask;
                while (isAssigned(newSlot)) {
                    newSlot = (newSlot + 1) & mask;
                }
                putKey(newSlot, key1, key2);
                mem.copyMemory(valueAddress, valueAddrOfSlot(newSlot), valueLength);
            }
        }
        oldMalloc.free(oldAddress - HEADER_SIZE, oldAllocatedSize);
    }

    private static long maxSizeForCapacity(long capacity, float loadFactor) {
        return Math.max(2, (long) Math.ceil(capacity * loadFactor)) - 1;
    }

    private static long minCapacityForSize(long size, float loadFactor) {
        return roundCapacity((long) Math.ceil(size / loadFactor));
    }


    protected final class Cursor implements HashSlotCursor, HashSlotCursorTwinKey {

        private long currentSlot;

        protected Cursor() {
            reset();
        }

        @Override
        public void reset() {
            currentSlot = -1;
        }

        @Override public boolean advance() {
            ensureLive();
            assert currentSlot != Long.MIN_VALUE : "Cursor has advanced past the last slot";
            if (tryAdvance()) {
                return true;
            }
            currentSlot = Long.MIN_VALUE;
            return false;
        }

        @Override public long key() {
            return key1();
        }

        @Override public long key1() {
            ensureValid();
            return key1OfSlot(currentSlot);
        }

        @Override public long key2() {
            ensureValid();
            return key2OfSlot(currentSlot);
        }

        @Override public long valueAddress() {
            ensureValid();
            return valueAddrOfSlot(currentSlot);
        }

        private void ensureValid() {
            ensureLive();
            assert currentSlot >= 0 : "Cursor is invalid";
        }

        private boolean tryAdvance() {
            final long capacity = capacity();
            for (long slot = currentSlot + 1; slot < capacity; slot++) {
                if (isAssigned(slot)) {
                    currentSlot = slot;
                    return true;
                }
            }
            return false;
        }
    }
}
