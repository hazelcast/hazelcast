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

package com.hazelcast.jet.memory.binarystorage;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.memory.serialization.MemoryDataOutput;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;
import com.hazelcast.jet.memory.binarystorage.cursor.TupleAddressCursor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;


/**
 * Flyweight over binary key-multivalue storage. Under each key there is a
 * <em>slot</em> which points to a chain (linked list) of <em>tuples</em>.
 */
public interface Storage {

    /**
     * Allocates a new storage structure and sets it as the current structure.
     * @return base address of the newly allocated structure
     */
    long gotoNew();

    /**
     * Sets the given address as the base address of the current storage structure.
     */
    void gotoAddress(long baseAddress);

    /**
     * Sets the memory block to work with.
     */
    void setMemoryBlock(MemoryBlock memoryBlock);

    /**
     * @return the current memory block
     */
    MemoryBlock getMemoryBlock();

    /**
     * @return the number of distinct keys in this storage
     */
    long count();

    /**
     * Returns the length of the pair chain for the slot at the given address.
     */
    long tupleCountAt(long slotAddress);

    /**
     * Given the address of a slot, returns the address of the first pair (head of the chain).
     */
    long addrOfFirstTuple(long slotAddress);

    /**
     * Given the address of a pair, returns the address of the next pair in the
     * chain, or {@value com.hazelcast.internal.memory.MemoryAllocator#NULL_ADDRESS} if the
     * next pair doesn't exist.
     */
    long addrOfNextTuple(long tupleAddress);

    /**
     * Uses the pair's key to look up a slot with the same key.
     *
     * @param tupleAddress address of the pair
     * @param tupleAccessor used to access the pair
     * @return address of the slot, if found;
     *         {@value com.hazelcast.internal.memory.MemoryAllocator#NULL_ADDRESS} otherwise
     */
    long addrOfSlotWithSameKey(long tupleAddress, MemoryAccessor tupleAccessor);

    /**
     * Uses the pair's key to look up a slot with the same key.
     *
     * @param tupleAddress address of the pair
     * @param comparator to calculate hashcode; if null, the default one is used
     * @param tupleAccessor used to access the pair
     * @return address of the slot, if found;
     *         {@value com.hazelcast.internal.memory.MemoryAllocator#NULL_ADDRESS} otherwise
     */
    long addrOfSlotWithSameKey(long tupleAddress, Comparator comparator, MemoryAccessor tupleAccessor);

    /**
     * Uses the pair's key to look up a slot with the same key, assigning a new one if not found.
     *
     * @param tupleAddress address of the pair
     * @param comparator to calculate hashcode; if null, the default one is used
     * @return abs(return value) is the address of the slot where the pair was inserted. It is positive
     * if a new slot was assigned, negative otherwise.
     */
    long getOrCreateSlotWithSameKey(long tupleAddress, Comparator comparator);

    /**
     * Adds the given pair to storage for the 0-source.
     *
     * @return abs(return value) is the address of the slot where the pair was inserted. It is positive
     * if a new slot was assigned, negative otherwise.
     */
    long insertTuple(long recordAddress, Comparator comparator);

    /**
     * Serializes and adds the given pair to the storage for the source with number 0.
     */
    void insertTuple(Pair pair, SerializationOptimizer optimizer, MemoryDataOutput output);

    /**
     * Marks key with specified slot address with value marker;
     *
     * @param slotAddress - address of the slot;
     * @param marker      - marker value;
     */
    void markSlot(long slotAddress, byte marker);

    /**
     * Return marker's value for the specified keyEntry;
     *
     * @param slotAddress - address of the keyEntry;
     * @return marker's value;
     */
    byte getSlotMarker(long slotAddress);

    /**
     * Sets the hashCode field of the given slot.
     *
     * @param slotAddress address of the slot
     * @param hashCode    hashcode to set
     */
    void setSlotHashCode(long slotAddress, long hashCode);

    /**
     * Returns the value of the the hashCode field in the given slot.
     *
     * @param slotAddress address of the slot
     */
    long getSlotHashCode(long slotAddress);

    SlotAddressCursor slotCursor();

    TupleAddressCursor tupleCursor(long slotAddress);
}
