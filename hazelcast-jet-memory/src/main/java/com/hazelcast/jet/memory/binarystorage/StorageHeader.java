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

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.nio.Bits;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Each Memory block has a header:
 * <pre>
 * ----------------------------------------------------------------------
 * |            Key-value storage base address (8 bytes)                |
 * ----------------------------------------------------------------------
 * </pre>
 */
public class StorageHeader {
    private static final long DEFAULT_HEADER_OFFSET = 1L;
    private MemoryBlock memoryBlock;
    private long headerAddress;

    /**
     * Return address of the storage by the corresponding slot's address;
     *
     * @return address of the storage;
     */
    public long baseAddress() {
        checkNotNull(memoryBlock);
        return memoryBlock.getAccessor().getLong(headerAddress);
    }

    /**
     * Set corresponding memory manager for the storage;
     *
     * @param memoryBlock - corresponding memory manager;
     */
    public void setMemoryBlock(MemoryBlock memoryBlock) {
        checkNotNull(memoryBlock);
        this.memoryBlock = memoryBlock;
        this.headerAddress = memoryBlock.toAddress(DEFAULT_HEADER_OFFSET);
    }

    /**
     * Allocated header for the corresponding
     * memoryAccessor by corresponding address;
     */
    public void allocatedHeader() {
        checkNotNull(memoryBlock);
        long headerPointer = memoryBlock.getAllocator().allocate(getSize());
        assert headerPointer == headerAddress;
    }

    /**
     * @param baseAddress - address of the slot;
     */
    public void setBaseStorageAddress(long baseAddress) {
        checkNotNull(memoryBlock);
        memoryBlock.getAccessor().putLong(headerAddress, baseAddress);
    }

    /**
     * @return total size acquired by header;
     */
    public int getSize() {
        return Bits.LONG_SIZE_IN_BYTES;
    }

    /**
     * Reset header for current memory block
     */
    public void resetHeader() {
        setBaseStorageAddress(MemoryAllocator.NULL_ADDRESS);
    }
}
