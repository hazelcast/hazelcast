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

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray16byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor16byteKey;
import com.hazelcast.internal.util.hashslot.SlotAssignmentResult;

import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.internal.util.QuickMath.modPowerOfTwo;

/**
 * Implementation of {@link HashSlotArray16byteKey}.
 * <p>
 * This class uses the first 8 bytes of the value block for the unassigned sentinel.
 * <strong>It is the responsibility of the caller to ensure that the unassigned sentinel
 * is overwritten with a non-sentinel value as soon as a new slot is assigned (after calling
 * {@link #ensure(long, long)} and getting a positive return value).</strong>
 * For the same reason this class must not be instantiated with zero value length. Use
 * {@link HashSlotArray16byteKeyNoValue} as a zero-length key implementation.
 */
public class HashSlotArray16byteKeyImpl extends HashSlotArrayBase implements HashSlotArray16byteKey {

    private static final int KEY_SIZE = 16;

    public HashSlotArray16byteKeyImpl(long nullSentinel, MemoryManager memMgr, MemoryAllocator auxMalloc, int valueLength,
                                      int initialCapacity, float loadFactor) {
        this(nullSentinel, KEY_SIZE, memMgr, auxMalloc, valueLength, initialCapacity, loadFactor);
        assert valueLengthValid(valueLength) : "Invalid value length: " + valueLength;
    }

    public HashSlotArray16byteKeyImpl(long nullSentinel, MemoryManager memMgr, int valueLength,
                                      int initialCapacity, float loadFactor) {
        this(nullSentinel, memMgr, null, valueLength, initialCapacity, loadFactor);
    }

    public HashSlotArray16byteKeyImpl(long nullSentinel, MemoryManager mm, int valueLength) {
        this(nullSentinel, mm, null, valueLength, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    protected HashSlotArray16byteKeyImpl(
            long nullSentinel, long offsetOfNullSentinel, MemoryManager mm, MemoryAllocator auxMalloc,
            int valueLength, int initialCapacity, float loadFactor
    ) {
        super(nullSentinel, offsetOfNullSentinel, mm, auxMalloc, KEY_SIZE, valueLength,
                initialCapacity, loadFactor);
        assert modPowerOfTwo(valueLength, VALUE_SIZE_GRANULARITY) == 0
                : "Value length must be a positive multiple of 8, but was " + valueLength;
    }


    /**
     * {@inheritDoc}
     * <p>
     * Whenever this method returns a newly assigned slot, the caller must ensure
     * that the null-sentinel value at the returned address is overwritten with
     * a non-sentinel value.
     *
     * @see SlotAssignmentResult#isNew()
     */
    @Override public SlotAssignmentResult ensure(long key1, long key2) {
        return super.ensure0(key1, key2);
    }

    @Override public long get(long key1, long key2) {
        return super.get0(key1, key2);
    }

    @Override public boolean remove(long key1, long key2) {
        return super.remove0(key1, key2);
    }

    @Override public HashSlotCursor16byteKey cursor() {
        return new CursorLongKey2();
    }

    protected boolean valueLengthValid(int valueLength) {
        return valueLength > 0;
    }

    public static long addrOfKey1At(long slotBase) {
        return slotBase + KEY_1_OFFSET;
    }

    public static long addrOfKey2At(long slotBase) {
        return slotBase + KEY_2_OFFSET;
    }

    public static long addrOfValueAt(long slotBase) {
        return slotBase + KEY_SIZE;
    }

    public static long valueAddr2slotBase(long valueAddr) {
        return valueAddr - KEY_SIZE;
    }
}
