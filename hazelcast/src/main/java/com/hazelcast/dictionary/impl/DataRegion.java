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
import com.hazelcast.dictionary.impl.type.EntryType;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

/**
 * A DataRegion is the region of memory in {@link Segment} where the
 * actual key/values are stored.
 *
 * Currently a DataRegion contains a single block of memory that gets
 * increased or decreased in size by copying the data into a larger or
 * smaller block of memory.
 *
 * In the future the DataRegion could become smarter and instead of
 * having a single block of memory, a set of blocks is used. Blocks
 * can be added and removed. The advantage would be that a memory a
 * allocation that doesn't fit into the existing claimed memory, doesn't
 * lead to a full copy of the whole data region.
 */
public class DataRegion {
    private static final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final DictionaryConfig config;
    private final EntryEncoder encoder;
    private final EntryType entryType;

    // the number of bytes of memory in this segment.
    private volatile int length;
    // the address of the first byte of memory where key/values are stored.
    private long address;
    // the heapOffset of the first free byes to store data (key/values)
    private int freeOffset;
    // the bytes available for writing key/values
    private volatile int available;
    // contains the number of entries in this segment.
    // is volatile so it can be read by different threads concurrently
    // will never be modified concurrently
    private volatile int count;

    public DataRegion(DictionaryConfig config, EntryEncoder encoder, EntryType entryType) {
        this.config = config;
        this.entryType = entryType;
        this.length = config.getInitialSegmentSize();
        this.encoder = encoder;
    }

    public long address(){
        return address;
    }

    public void init() {
        this.address = unsafe.allocateMemory(length);
        this.available = length;
        this.freeOffset = 0;
    }

    public void clear() {
        this.freeOffset = 0;
        this.available = length;
    }

    public int insert(Object key, Object value) {
        for (; ; ) {
            int offset = freeOffset;
            int bytesWritten = encoder.writeEntry(key, value, address + offset, available);
            if (bytesWritten == -1) {
                expand();
                continue;
            }

            count++;

            //  System.out.println("bytes written:" + bytesWritten);
            available -= bytesWritten;
            this.freeOffset += bytesWritten;
            // System.out.println("address after value insert:" + dataFreeOffset);
            // System.out.println("count:" + count);
            // no item exists, so we need to allocate new
            return offset;
        }
    }

    public Object readValue(int offset) {
        return encoder.readValue(address + offset);
    }

    private void expand() {
        if (length == config.getMaxSegmentSize()) {
            throw new IllegalStateException(
                    "Can't grow segment beyond configured maxSegmentSize of " + config.getMaxSegmentSize());
        }

        long newSegmentLength = Math.min(config.getMaxSegmentSize(), length * 2L);

        System.out.println("expanding from:" + length + " to:" + newSegmentLength);

        if (newSegmentLength > Integer.MAX_VALUE) {
            throw new IllegalStateException("Can't grow beyond 2GB");
        }

        long newSegmentAddress = unsafe.allocateMemory(newSegmentLength);
        // copy the data
        unsafe.copyMemory(address, newSegmentAddress, freeOffset);

        unsafe.freeMemory(address);

        int dataConsumed = length - available;

        this.available = (int) (newSegmentLength - dataConsumed);
        this.length = (int) newSegmentLength;
        this.address = newSegmentAddress;
    }

    public void overwrite(Object value, long offset) {
        // System.out.println("put existing record found, overwriting value, found heapOffset:" + heapOffset);
        encoder.writeValue(value, address + offset);
    }

    public int count() {
        return count;
    }

    public long allocated() {
        return length;
    }

    public long consumed() {
        return length - available;
    }

    public void remove(int offset) {
        count--;
    }
}
