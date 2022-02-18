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

import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;

/**
 * Specialization of {@link HashSlotArray8byteKeyImpl} to the case of zero-length value. Suitable for a {@code long} set
 * implementation. Unassigned sentinel is kept at the start of the slot, i.e., in the key part.
 * Therefore the sentinel value cannot be used as a key.
 */
public class HashSlotArray8byteKeyNoValue extends HashSlotArray8byteKeyImpl {

    public HashSlotArray8byteKeyNoValue(long unassignedSentinel, MemoryManager mm,
                                        int initialCapacity, float loadFactor) {
        super(unassignedSentinel, 0L, mm, 0, initialCapacity, loadFactor);
    }

    public HashSlotArray8byteKeyNoValue(long unassignedSentinel, MemoryManager mm) {
        super(unassignedSentinel, 0L, mm, 0, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }
}
