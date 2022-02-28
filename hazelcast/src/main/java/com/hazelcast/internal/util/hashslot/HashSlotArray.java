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

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyNoValue;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray8byteKeyNoValue;
import com.hazelcast.internal.nio.Disposable;

/** <p>
 * A <i>Flyweight</i> object that manages the backbone array of an off-heap open-addressed hashtable.
 * The backbone consists of <i>slots</i>, where each slot has a key part and an optional value part.
 * The size of the key part varies by subtype ({@link HashSlotArray8byteKey}, {@link HashSlotArray16byteKey}).
 * The size of the value part can be specified on instantiation and there are subtypes specialized for
 * zero-size values ({@link HashSlotArray8byteKeyNoValue}, {@link HashSlotArray16byteKeyNoValue}).
 * At this level of abstraction the slot-accessing methods are not defined because they are specific
 * to a given key size. These methods are defined in the key size-specific subtypes. The notes presented
 * here, however, apply to all cases.
 * </p><p>
 * The update operations only ensure that a slot for a given key exists/doesn't exist and it is
 * up to the caller to manage the contents of the value part. The caller will be provided
 * with the raw address of the value, suitable for accessing with the associated
 * {@link MemoryAccessor MemoryAccessor}.
 * <b>The returned address is valid only up to the next map update operation</b>.
 * </p><p>
 * Since this is a <i>Flyweight</i>-style object, the same instance can be used to manage many
 * hashtables, one at a time. The {@link #gotoAddress(long)} method resets the instance to work
 * with the hashtable at the provided address and {@link #gotoNew()} allocates a new hashtable.
 * It is the caller's duty to ensure against memory leaks by keeping track of all existing hashtables'
 * base addresses. Since an update operation may trigger a new allocation, freeing the old block,
 * it is the caller's duty to save the new address before moving on to another base address.
 * </p><h3>
 *     Memory layout
 * </h3> <p>
 * The base address, returned by {@link #address()}, is the addres of the first slot in the array.
 * It is preceded by the header of size {@value com.hazelcast.internal.util.hashslot.impl.HashSlotArrayBase#HEADER_SIZE}
 * which holds the metadata
 * that pertains to the structure as a whole ({@code capacity}, {@code size}, and {@code expandAt}).
 * A slot consists of the key part and the value part, in that order, and the size of a slot is exactly the sum of the
 * sizes of the key and value parts. The size of the memory block that backs the entire structure is equal to
 * {@value com.hazelcast.internal.util.hashslot.impl.HashSlotArrayBase#HEADER_SIZE} + ({@link #capacity()} * slot size), and
 * its base address is {@link #address()} - {@value com.hazelcast.internal.util.hashslot.impl.HashSlotArrayBase#HEADER_SIZE}.
 * </p><p>
 * A special value must be reserved to mark an <em>unassigned</em> slot. The offset of the marker
 * in the slot can be customized, as well as the choice of the special value.
 * </p>
 */
public interface HashSlotArray extends Disposable {

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
     * @return the number of assigned slots in the current hash slot array
     */
    long size();

    /**
     * @return the total number of slots in the current hash slot array
     */
    long capacity();

    /** Array expansion and rehashing will be triggered when {@code size} reaches this value. */
    long expansionThreshold();

    /**
     * After this method returns, all slots in the current hash slot array are unassigned.
     */
    void clear();

    /** Compact the array by reducing its capacity, if allowed by the current size and load factor.
     * @return true if the array was compacted; false if no action was taken. */
    boolean trimToSize();

}
