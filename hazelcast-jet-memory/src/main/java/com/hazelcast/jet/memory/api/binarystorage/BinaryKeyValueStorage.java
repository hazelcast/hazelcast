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

package com.hazelcast.jet.memory.api.binarystorage;

import com.hazelcast.jet.io.IOContext;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.memory.spi.operations.ElementsReader;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotIterator;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinaryRecordIterator;


/**
 * Represents abstract API for the binary key-value storage
 */
public interface BinaryKeyValueStorage<T> {
    /**
     * @param recordAddress - record entry address;
     * @return address of the next value address;
     * 0 - if there is no next address
     */
    long getNextRecordAddress(long recordAddress);

    /**
     * @param slotAddress - address of the slot;
     * @return - address of the first record;
     */
    long getHeaderRecordAddress(long slotAddress);

    /**
     * @param recordAddress - record entry address;
     * @return address of the value's content;
     */
    long getValueAddress(long recordAddress);

    /**
     * @param recordAddress - value entry address;
     * @return actually written bytes to the value's buffer;
     */
    long getValueWrittenBytes(long recordAddress);

    /**
     * @param slotAddress - address of the corresponding keyEntry;
     * @return iterator over records of the corresponding key;
     */
    BinaryRecordIterator recordIterator(long slotAddress);

    /**
     * @return iterator over slots;
     */
    BinarySlotIterator slotIterator();

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p>
     * In case if not-found - if createIfNotExists==true - create new slot;
     * if createIfNotExists==false - don't create new slot - return 0L;
     *
     * @param recordAddress     - address of the record;
     * @param sourceId          - id of the source;
     * @param createIfNotExists - flag which determines if we want to create entry if it doesn't exist;
     * @return address of the slot;
     */
    long getSlotAddress(long recordAddress,
                        short sourceId,
                        boolean createIfNotExists);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p>
     * In case if not-found - if createIfNotExists==true - create new slot;
     * if createIfNotExists==false - don't create new slot - return 0L;
     *
     * @param recordAddress     - address of the record;
     * @param sourceId          - id of the source;
     * @param comparator        - comparator to be used, if null default passed in constructor will be used;
     * @param createIfNotExists - flag which determines if we want to create entry if it doesn't exist;
     * @return address of the slot;
     */
    long getSlotAddress(long recordAddress,
                        short sourceId,
                        BinaryComparator comparator,
                        boolean createIfNotExists);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p>
     * In case if not-found - if createIfNotExists==true - create new slot;
     * if createIfNotExists==false - don't create new slot - return 0L;
     *
     * @param recordAddress - address of the record;
     * @param comparator    - comparator to be used, if null default passed in constructor will be used;
     * @return address of the slot;
     */
    long createOrGetSlot(long recordAddress,
                         short sourceId,
                         BinaryComparator comparator);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p>
     * In case if not-found - if createIfNotExists==true - create new slot;
     * if createIfNotExists==false - don't create new slot - return 0L;
     *
     * @param recordAddress - address of the record - for the source with number 0;
     * @param comparator    - comparator to be used, if null default passed in constructor will be used;
     * @return address of the slot;
     */
    long createOrGetSlot(long recordAddress, BinaryComparator comparator);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p>
     *
     * @param recordAddress        - address of the record;
     * @param recordMemoryAccessor - to be used to access key;
     * @return address of the slot  if corresponding record has been found,
     * {@link com.hazelcast.internal.memory.MemoryAllocator#NULL_ADDRESS} otherwise
     */
    long lookUpSlot(long recordAddress,
                    MemoryAccessor recordMemoryAccessor);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     *
     * @param recordAddress        - address of the record;
     * @param comparator           - comparator to be used for lookup;
     * @param recordMemoryAccessor - to be used to access key;
     * @return address of the slot  if corresponding record has been found,
     * {@link com.hazelcast.internal.memory.MemoryAllocator#NULL_ADDRESS} otherwise
     */
    long lookUpSlot(
            long recordAddress,
            BinaryComparator comparator,
            MemoryAccessor recordMemoryAccessor
    );

    /**
     * @param recordAddress - address of the record;
     * @return address of the key buffer in corresponding slot;
     */
    long getKeyAddress(long recordAddress);

    /**
     * Return size of the key in byte stored by corresponding pointer;
     *
     * @param recordAddress - address of the record;
     * @return amount of written bytes of the key;
     */
    long getKeyWrittenBytes(long recordAddress);

    /**
     * Return number of records written under slot;
     *
     * @param slotAddress - address of the slot;
     * @return - number of values written under slot;
     */
    long getRecordsCount(long slotAddress);

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
     * Set hashCode for the corresponding slot;
     *
     * @param slotAddress - address of the slot entry;
     * @param hashCode    - value of the hashCode;
     */
    void setKeyHashCode(long slotAddress, long hashCode);

    /**
     * Return value of the hashCode for the corresponding slot;
     *
     * @param slotAddress - address of the slot entry;
     * @return - value of the hashCode;
     */
    long getSlotHashCode(long slotAddress);

    /**
     * @return amount of elements in the tree;
     */
    long count();

    /**
     * Validate if storage is in consistent state;
     *
     * @return true - in case of storage is in consistent state;
     * false - in case of storage is not in consistent state;
     */
    boolean validate();

    /**
     * Check the state of the storage after last operation with slot;
     * <p>
     * <p>
     * <pre>
     *     - put
     *     - getSlotAddress
     * </pre>
     *
     * @return true - if last invocation inserted new slot,
     * false if key existed and hasn't been inserted
     */
    boolean wasLastSlotCreated();

    /**
     * Allocates new storage and switch current object to the new pointer;
     */
    long gotoNew();

    /**
     * Switch current storage to the corresponding address;
     */
    void gotoAddress(long baseAddress);

    /**
     * @return root address for the storage
     */
    long rootAddress();

    /**
     * Serialize and add record to the storage;
     */
    void addRecord(T record,
                   short sourceId,
                   ElementsReader<T> keyReader,
                   ElementsReader<T> valueReader,
                   IOContext ioContext,
                   JetDataOutput output,
                   BinaryComparator binaryComparator);

    /**
     * Serialize and add record to the storage;
     */
    void addRecord(T record,
                   short sourceId,
                   ElementsReader<T> keyReader,
                   ElementsReader<T> valueReader,
                   IOContext ioContext,
                   JetDataOutput output);

    /**
     * Serialize and add record to the storage for the source with number 0;
     */
    void addRecord(T tuple,
                   ElementsReader<T> keyReader,
                   ElementsReader<T> valueReader,
                   IOContext ioContext,
                   JetDataOutput output
    );

    /**
     * Add record to storage;
     *
     * @param recordAddress    - record;
     * @param binaryComparator - comparator;
     * @return - address of the last slot;
     */
    long addRecord(long recordAddress,
                   short sourceId,
                   BinaryComparator binaryComparator
    );

    /**
     * Add record to storage for the 0-source;
     *
     * @param recordAddress    - record;
     * @param binaryComparator - comparator;
     * @return - address of the last slot;
     */
    long addRecord(long recordAddress,
                   BinaryComparator binaryComparator);

    /**
     * Set memory block to work with;
     *
     * @param memoryBlock - specified memoryBlock;
     */
    void setMemoryBlock(MemoryBlock memoryBlock);

    /**
     * @return memoryBlock to work with storage;
     */
    MemoryBlock getMemoryBlock();
}
