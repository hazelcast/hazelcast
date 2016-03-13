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

import com.hazelcast.nio.Disposable;

/** <p>
 * Manages the backbone array of an off-heap open-addressed hashtable. The backbone consists
 * of <i>slots</i>, where each slot has a key part and an optional value part. The key part
 * consists of two {@code long} values and the value part is a block whose size is a multiple of 8.
 * </p><p>
 * The update operations on this class only ensure that a slot for a given key exists/doesn't exist
 * and it is up to the caller to manage the contents of the value part. The caller will be provided
 * with the raw address of the value, suitable for accessing with {@code Unsafe} memory operations.
 * <b>The returned address is valid only up to the next map update operation</b>.
 * </p><p>
 * This class makes no assumptions about the key's semantics; it just uses them as the literal
 * 16 bytes of data. However, a major use case is where the key represents a "safe pointer" to
 * a native memory block. It consists of a pair <code>(nativeAddress, sequenceId)</code>, where
 * <code>nativeAddress</code> is the address of a native memory block containing key data and
 * <code>sequenceId</code> is a number unique to the particular allocation event of that memory block.
 * This avoids the "ABA problem": the block at <code>nativeAddress</code> could be deallocated, then
 * reallocated for another purpose. Then <code>nativeAddress</code> would represent a valid pointer
 * pointing to invalid data. The <code>(nativeAddress, sequenceId)</code> pair is expected to be
 * acquired from a native data structure which complies with this concept and stores the relevant
 * key data.
 * </p><p>
 * Since this is a <i>Flyweight</i>-style object, the same instance can be used to manage many
 * hashtables, one at a time. The {@link #gotoAddress(long)} method resets the instance to work
 * with the hashtable at the provided address and {@link #gotoNew()} allocates a new hashtable.
 * It is the caller's duty to ensure against memory leaks by keeping track of all existing hashtables'
 * base addresses. Since an update operation may trigger a new allocation, freeing the old block,
 * it is the caller's duty to save the new address before moving on to another base address.
 * </p>
 */
public interface HashSlotArrayTwinKey extends Disposable {

    /**
     * @return current base address of this flyweight
     */
    long address();

    /**
     * Position this flyweight to the supplied base address.
     */
    void gotoAddress(long address);

    /**
     * Allocate a new array and position this flyweight to its base address.
     * @return the base address of the new array.
     */
    long gotoNew();

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
     * @return address of the value block or {@link com.hazelcast.memory.MemoryAllocator#NULL_ADDRESS}
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
     * Clears the map.
     */
    void clear();

    /** Compact the slot array if allowed by the current size.
     * @return true if the array was compacted; false if no action was taken. */
    boolean trimToSize();

    /**
     * Returns the number of keys in this hash slot array.
     */
    long size();

    /**
     * Returns a cursor over all assigned slots in this array.
     */
    HashSlotCursorTwinKey cursor();
}
