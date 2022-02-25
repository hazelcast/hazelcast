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

package com.hazelcast.internal.util.hashslot;

import com.hazelcast.internal.memory.MemoryAllocator;

/**
 * Specialization of {@link HashSlotArray} to the case where the key consists of a {@code long}
 * and an {@code int} value and the value part is a block whose size is 4 + multiple of 8 (including zero).
 */
public interface HashSlotArray12byteKey extends HashSlotArray {

    /**
     * Ensures that there is a mapping from {@code (key1, key2)} to a slot in the array.
     * The returned object contains the slot value block address and whether a new
     * slot had to be assigned. The hash slot array implementation keeps a reference
     * to the returned object and will always return the same instance, albeit with
     * updated fields on each new invocation.
     * This means the returned object is valid until the next invocation of this
     * method.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return the value block assignment result
     */
    SlotAssignmentResult ensure(long key1, int key2);

    /**
     * Returns the address of the value block mapped by {@code (key1, key2)}.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return address of the value block or
     * {@link MemoryAllocator#NULL_ADDRESS MemoryAllocator.NULL_ADDRESS}
     * if no mapping for {@code (key1, key2)} exists.
     */
    long get(long key1, int key2);

    /**
     * Removes the mapping for {@code (key1, key2)}, if any.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return true if there was a mapping, false otherwise
     */
    boolean remove(long key1, int key2);

    /**
     * Returns a cursor over all assigned slots in this array.
     */
    HashSlotCursor12byteKey cursor();
}
