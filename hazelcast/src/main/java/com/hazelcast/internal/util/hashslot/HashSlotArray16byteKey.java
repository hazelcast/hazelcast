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

package com.hazelcast.internal.util.hashslot;

import com.hazelcast.internal.memory.MemoryAllocator;

/**
 * Specialization of {@link HashSlotArray} to the case where the key consists of two {@code long} values
 * and the value part is a block whose size is a multiple of 8 (including zero).
 */
public interface HashSlotArray16byteKey extends HashSlotArray {

    /**
     * Ensures that there is a mapping from {@code (key1, key2)} to a slot in the array.
     * The {@code abs} of the returned integer is the address of the slot's value block.
     * The returned integer is positive if a new slot had to be assigned and negative
     * if the slot was already assigned.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return address of value block
     */
    long ensure(long key1, long key2);

    /**
     * Returns the address of the value block mapped by {@code (key1, key2)}.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return address of the value block or
     * {@link MemoryAllocator#NULL_ADDRESS MemoryAllocator.NULL_ADDRESS}
     * if no mapping for {@code (key1, key2)} exists.
     */
    long get(long key1, long key2);

    /**
     * Removes the mapping for {@code (key1, key2)}, if any.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return true if there was a mapping, false otherwise
     */
    boolean remove(long key1, long key2);

    /**
     * Returns a cursor over all assigned slots in this array.
     */
    HashSlotCursor16byteKey cursor();
}
