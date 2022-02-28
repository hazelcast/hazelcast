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
 * Specialization of {@link HashSlotArray} to the case where the key is a single {@code long} value
 * and the value part is a block whose size is a multiple of 8 (including zero).
 */
public interface HashSlotArray8byteKey extends HashSlotArray {

    /**
     * Ensures that there is a mapping from the given key to a slot in the array.
     * The returned object contains the slot value block address and whether a new
     * slot had to be assigned. The hash slot array implementation keeps a reference
     * to the returned object and will always return the same instance, albeit with
     * updated fields on each new invocation.
     * This means the returned object is valid until the next invocation of this
     * method.
     *
     * @param key the key
     * @return the value block assignment result
     */
    SlotAssignmentResult ensure(long key);

    /**
     * Returns the address of the value block mapped by the given key.
     *
     * @param key the key
     * @return address of the value block or
     * {@link MemoryAllocator#NULL_ADDRESS MemoryAllocator.NULL_ADDRESS}
     * if no mapping for {@code key} exists.
     */
    long get(long key);

    /**
     * Removes the mapping for the given key.
     *
     * @param key the key
     * @return true if a mapping existed and was removed, false otherwise
     */
    boolean remove(long key);

    /**
     * Returns a cursor over all assigned slots in this array.
     */
    HashSlotCursor8byteKey cursor();
}
