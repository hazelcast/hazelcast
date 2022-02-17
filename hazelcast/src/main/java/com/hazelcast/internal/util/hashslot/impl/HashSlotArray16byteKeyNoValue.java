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

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.SlotAssignmentResult;

import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;

/**
 * Twin-key hash slot array with zero-width value. Suitable for a twin-long set implementation.
 * Stores the null-sentinel value into the {@code key1} field, therefore the chosen null-sentinel value
 * is not valid as the value of {@code key1}.
 */
public class HashSlotArray16byteKeyNoValue extends HashSlotArray16byteKeyImpl {

    /**
     * @param nullKey1 the null-sentinel value checked against the {@code key1} field.
     */
    public HashSlotArray16byteKeyNoValue(long nullKey1, MemoryManager mm, int initialCapacity, float loadFactor) {
        super(nullKey1, 0L, mm, null, 0, initialCapacity, loadFactor);
    }

    /**
     * @param nullKey1 the null-sentinel value checked against the {@code key1} field.
     */
    public HashSlotArray16byteKeyNoValue(long nullKey1, MemoryManager mm) {
        this(nullKey1, mm, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    // Value length is always zero (not under user's control)
    @Override protected boolean valueLengthValid(int valueLength) {
        return true;
    }

    @Override public SlotAssignmentResult ensure(long key1, long key2) {
        assert key1 != unassignedSentinel : "ensure() called with key1 == nullKey1 (" + unassignedSentinel + ')';
        return super.ensure0(key1, key2);
    }

    @Override public long get(long key1, long key2) {
        assert key1 != unassignedSentinel : "get() called with key1 == nullKey1 (" + unassignedSentinel + ')';
        return super.get0(key1, key2);
    }

    @Override public boolean remove(long key1, long key2) {
        assert key1 != unassignedSentinel : "remove() called with key1 == nullKey1 (" + unassignedSentinel + ')';
        return super.remove0(key1, key2);
    }
}
