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
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.io.tuple.Tuple2;
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
     * Returns the length of the tuple chain for the slot at the given address.
     */
    long tupleCountAt(long slotAddress);

    /**
     * Given the address of a slot, returns the address of the first tuple (head of the chain).
     */
    long addrOfFirstTuple(long slotAddress);

    /**
     * Given the address of a tuple, returns the address of the next tuple in the
     * chain, or {@value com.hazelcast.internal.memory.MemoryAllocator#NULL_ADDRESS} if the
     * next tuple doesn't exist.
     */
    long addrOfNextTuple(long tupleAddress);

    /**
     * Uses the tuple's key to look up a slot with the same key.
     *
     * @param tupleAddress address of the tuple
     * @param tupleAccessor used to access the tuple
     * @return address of the slot, if found;
     *         {@value com.hazelcast.internal.memory.MemoryAllocator#NULL_ADDRESS} otherwise
     */
    long addrOfSlotWithSameKey(long tupleAddress, MemoryAccessor tupleAccessor);

    /**
     * Uses the tuple's key to look up a slot with the same key.
     *
     * @param tupleAddress address of the tuple
     * @param comparator to calculate hashcode; if null, the default one is used
     * @param tupleAccessor used to access the tuple
     * @return address of the slot, if found;
     *         {@value com.hazelcast.internal.memory.MemoryAllocator#NULL_ADDRESS} otherwise
     */
    long addrOfSlotWithSameKey(long tupleAddress, Comparator comparator, MemoryAccessor tupleAccessor);

    /**
     * Uses the tuple's key to look up a slot with the same key, assigning a new one if not found.
     *
     * @param tupleAddress address of the tuple
     * @param comparator to calculate hashcode; if null, the default one is used
     * @return abs(return value) is the address of the slot where the tuple was inserted. It is positive
     * if a new slot was assigned, negative otherwise.
     */
    long getOrCreateSlotWithSameKey(long tupleAddress, Comparator comparator);

    /**
     * Adds the given tuple to storage for the 0-source.
     *
     * @return abs(return value) is the address of the slot where the tuple was inserted. It is positive
     * if a new slot was assigned, negative otherwise.
     */
    long insertTuple(long recordAddress, Comparator comparator);

    /**
     * Serializes and adds the given tuple to the storage for the source with number 0.
     */
    void insertTuple(Tuple2 tuple, IOContext ioContext, JetDataOutput output);

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
