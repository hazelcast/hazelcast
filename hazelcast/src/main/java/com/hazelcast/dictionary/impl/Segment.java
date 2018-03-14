/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary.impl;

import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;


/**
 * http://www.docjar.com/docs/api/sun/misc/Unsafe.html
 *
 * http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/687fd7c7986d/src/share/classes/sun/misc/Unsafe.java
 *
 * The Segment is created eagerly as soon as the partition for the dictionary
 * is created, but the memory is allocated lazily.
 */
public class Segment {

    private final static Unsafe unsafe = UnsafeUtil.UNSAFE;
    private static final int OFFSET_TABLE_SLOT_BYTES = INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    private final AtomicReference<SegmentTask> ref = new AtomicReference<>();
    private final SerializationService serializationService;
    private final DictionaryConfig config;
    private final EntryModel model;
    private final EntryEncoder encoder;

    // the number of bytes of memory in this segment.
    private int segmentLength;
    // the address of the first byte of memory where key/values are stored.
    private long segmentAddress = 0;
    // the offset of the first free byes to store data (key/values)
    private int dataFreeOffset;
    // the bytes available for writing key/values
    private int dataAvailable;

    // contains the number of entries in this segment.
    // is volatile so it can be read by different threads concurrently
    // will never be modified concurrently
    private volatile int count;

    private int keyTableOffset;
    private int keyTableLength;
    private int keyTableSlots;

    public Segment(SerializationService serializationService,
                   EntryModel model,
                   EntryEncoder encoder,
                   DictionaryConfig config) {
        this.serializationService = serializationService;
        this.config = config;
        this.model = model;
        this.encoder = encoder;
        this.segmentLength = config.getInitialSegmentSize();
    }

    private void ensureAllocated() {
        if (segmentAddress == 0) {
            alloc();
        }
    }

    private void alloc() {
        this.segmentAddress = unsafe.allocateMemory(segmentLength);
        // we assume keytable is 1/8 of the segment size for now
        this.keyTableLength = segmentLength / 2;
        this.keyTableOffset = segmentLength - keyTableLength;
        this.dataAvailable = segmentLength - keyTableLength;
        this.dataFreeOffset = 0;

        this.keyTableSlots = keyTableLength / OFFSET_TABLE_SLOT_BYTES;

        long address = segmentAddress + keyTableOffset;
        for (int k = 0; k < keyTableSlots; k++) {
            unsafe.putInt(address, 0);
            address += OFFSET_TABLE_SLOT_BYTES;
        }
    }

    private void expandData() {
        if (segmentLength == config.getMaxSegmentSize()) {
            throw new IllegalStateException(
                    "Can't grow segment beyond configured maxSegmentSize of " + config.getMaxSegmentSize());
        }

        long newSegmentLength = Math.min(config.getMaxSegmentSize(), segmentLength * 2L);
        System.out.println("expanding from:" + segmentLength + " to:" + newSegmentLength);


        if (newSegmentLength > Integer.MAX_VALUE) {
            throw new IllegalStateException("Can't grow beyond 2GB");
        }

        long newSegmentAddress = unsafe.allocateMemory(newSegmentLength);
        // copy the data
        unsafe.copyMemory(segmentAddress, newSegmentAddress, dataFreeOffset);

        // copy the keytable
        unsafe.copyMemory(segmentAddress + keyTableOffset, newSegmentAddress + (newSegmentLength - keyTableLength), keyTableLength);

        unsafe.freeMemory(segmentAddress);

        int dataConsumed = segmentLength - dataAvailable;

        this.dataAvailable = (int) (newSegmentLength - keyTableLength - dataConsumed);

        this.segmentLength = (int) newSegmentLength;
        this.segmentAddress = newSegmentAddress;
    }

    // todo: count could be volatile size it can be accessed by any thread.
    public int count() {
        return count;
    }

    public void put(Data keyData, int partitionHash, Data valueData) {
        ensureAllocated();

        // creating these objects can cause performance problems. E.g. when the value is a large
        // byte array. So we should not be forcing to pull these objects to memory.
        Object key = serializationService.toObject(keyData);
        Object value = serializationService.toObject(valueData);

        int offset = offsetSearch(key, partitionHash);

        for (; ; ) {
            if (offset == -1) {
                // System.out.println("offset not found");
                int bytesWritten = encoder.writeEntry(key, value, segmentAddress + dataFreeOffset, dataAvailable);
                if (bytesWritten == -1) {
                    expandData();
                    continue;
                }
                count++;
                offsetInsert(keyData, partitionHash, dataFreeOffset);
                // System.out.println("Inserted offset:" + dataFreeOffset);

                //  System.out.println("bytes written:" + bytesWritten);
                dataAvailable -= bytesWritten;
                dataFreeOffset += bytesWritten;
                // System.out.println("address after value insert:" + dataFreeOffset);
                // System.out.println("count:" + count);
                // no item exists, so we need to allocate new

                break;
            } else {
                // System.out.println("put existing record found, overwriting value, found offset:" + offset);
                encoder.writeValue(value, segmentAddress + offset);
                break;
            }
        }

        //System.out.println("added");
    }

    public Object get(Data keyData, int partitionHash) {
        if (segmentAddress == 0) {
            // no memory has been allocated, so no items are stored.
            return null;
        }

        Object key = serializationService.toObject(keyData);
        int offset = offsetSearch(key, partitionHash);
        return offset == -1 ? null : encoder.readValue(segmentAddress + offset + model.keyLength());
        //todo: inclusion of  keyLength here sucks
    }

    /**
     * Gets the offset of entry.
     *
     * @param key           the key of the entry
     * @param partitionHash the hashcode of the entry (comes from Data).
     * @return the offset or -1 if the key isn't found in the segment.
     */
    private int offsetSearch(Object key, int partitionHash) {
        int i = 0;
        int hash = correctPartitionHash(partitionHash);
        for (; ; ) {
            int slot = slot(hash, i);
            int foundHash = unsafe.getInt(segmentAddress + keyTableOffset + OFFSET_TABLE_SLOT_BYTES * slot);
            ///System.out.println("hash in slot:" + foundHash);
            if (foundHash == 0) {
                return -1;
            } else if (foundHash == hash) {
                // System.out.println("reading offset");
                return unsafe.getInt(segmentAddress + keyTableOffset + OFFSET_TABLE_SLOT_BYTES * slot + INT_SIZE_IN_BYTES);
            }
            i++;
        }
    }

    private void offsetInsert(Object key, int partitionHash, int offset) {
        int i = 0;
        int hash = correctPartitionHash(partitionHash);
        // System.out.println("writing hash:" + hash);
        // System.out.println("writing offset:" + offset);
        for (; ; ) {
            int slot = slot(hash, i);
            int foundHash = unsafe.getInt(segmentAddress + keyTableOffset + OFFSET_TABLE_SLOT_BYTES * slot);

            if (foundHash == 0) {
                // empty slot
                unsafe.putInt(segmentAddress + keyTableOffset + OFFSET_TABLE_SLOT_BYTES * slot, hash);
                unsafe.putInt(segmentAddress + keyTableOffset + OFFSET_TABLE_SLOT_BYTES * slot + INT_SIZE_IN_BYTES, offset);
                return;
            }
            i++;
        }
    }

    private int correctPartitionHash(int partitionHash) {
        if (partitionHash > 0) {
            return partitionHash;
        } else if (partitionHash < 0) {
            return partitionHash == Integer.MIN_VALUE ? partitionHash - 1 : partitionHash;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public int slot(int partitionHash, int i) {
        return (Math.abs(partitionHash) + i) % keyTableSlots;
    }

    /**
     * Executes the task on this segment.
     *
     * The task is executed immediately if the segment is available, or parked for later
     * execution when the semgnet is in use.
     *
     * @param task
     * @return true if the task got executed, false if the task is appended for later execution.
     */
    public boolean execute(SegmentTask task) {
        return false;
    }

    public void clear() {
        count = 0;
        dataFreeOffset = 0;
        dataAvailable = segmentLength;
    }
}
