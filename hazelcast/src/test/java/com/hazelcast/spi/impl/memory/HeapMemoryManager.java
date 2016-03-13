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

package com.hazelcast.spi.impl.memory;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.spi.memory.MemoryAccessor;
import com.hazelcast.util.collection.Long2LongHashMap;
import junit.framework.AssertionFailedError;

import java.util.Arrays;

import static com.hazelcast.spi.impl.memory.EndiannessUtil.BYTE_ARRAY_ACCESS;

/**
 * Manages memory in Java byte arrays. For testing only.
 */
public class HeapMemoryManager implements MemoryManager {

    private static final int HEAP_BOTTOM = 8;

    private final Allocator malloc = new Allocator();

    private final Accessor mem = new Accessor();

    private final Long2LongHashMap allocatedAddrs = new Long2LongHashMap(1024, 0.7, -1);

    private byte[] storage;

    private int heapTop = HEAP_BOTTOM;

    private int usedMemory;

    public HeapMemoryManager(int size) {
        this.storage = new byte[size];
    }

    // Suports the testing of HashSlotArray#migrateTo()
    public HeapMemoryManager(HeapMemoryManager that) {
        this.storage = that.storage;
        this.heapTop = storage.length / 2;
    }

    @Override
    public MemoryAllocator getAllocator() {
        return malloc;
    }

    @Override
    public MemoryAccessor getAccessor() {
        return mem;
    }

    public long getUsedMemory() {
        return usedMemory;
    }

    @Override
    public void dispose() {
        storage = null;
    }

    class Allocator implements MemoryAllocator {

        @Override
        public long allocate(long size) {
            final long bytesFree = storage.length - toStorageIndex(heapTop);
            if (size > bytesFree) {
                throw new OutOfMemoryError(String.format("Asked to allocate %d, free bytes left %d", size, bytesFree));
            }
            final long addr = heapTop;
            heapTop += size;
            usedMemory += size;
            allocatedAddrs.put(addr, size);
            return addr;
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void free(long address, long size) {
            final long allocatedSize = allocatedAddrs.remove(address);
            if (allocatedSize == -1) {
                throw new AssertionFailedError(String.format("Address %d was not allocated", address));
            }
            if (allocatedSize != size) {
                throw new AssertionFailedError(String.format(
                        "Allocated size at %d was %d, but tried to free %d bytes", address, allocatedSize, size));
            }
            usedMemory -= size;
        }

        @Override
        public void dispose() {
            storage = null;
        }
    }

    private static long toStorageIndex(long addr) {
        return addr - HEAP_BOTTOM;
    }

    class Accessor implements MemoryAccessor {

        @Override
        public int getInt(long address) {
            return EndiannessUtil.readIntL(BYTE_ARRAY_ACCESS, storage, toStorageIndex(address));
        }

        @Override
        public void putInt(long address, int x) {
            EndiannessUtil.writeIntL(BYTE_ARRAY_ACCESS, storage, toStorageIndex(address), x);
        }

        @Override
        public long getLong(long address) {
            return EndiannessUtil.readLongL(BYTE_ARRAY_ACCESS, storage, toStorageIndex(address));
        }

        @Override
        public void putLong(long address, long x) {
            EndiannessUtil.writeLongL(BYTE_ARRAY_ACCESS, storage, toStorageIndex(address), x);
        }

        @Override
        public void copyMemory(long srcAddress, long destAddress, long lengthBytes) {
            System.arraycopy(storage, (int) toStorageIndex(srcAddress),
                    storage, (int) toStorageIndex(destAddress), (int) lengthBytes);
        }

        @Override
        public void copyFromByteArray(byte[] source, int offset, long destAddress, int length) {
            System.arraycopy(source, offset, storage, (int) toStorageIndex(destAddress), length);
        }

        @Override
        public void copyToByteArray(long srcAddress, byte[] destination, int offset, int length) {
            System.arraycopy(storage, (int) toStorageIndex(srcAddress), destination, offset, length);
        }

        @Override
        public void setMemory(long address, long lengthBytes, byte value) {
            final int fromIndex = (int) toStorageIndex(address);
            Arrays.fill(storage, fromIndex, fromIndex + (int) lengthBytes, value);
        }

        @Override public boolean getBoolean(long address) {
            throw new UnsupportedOperationException();
        }
        @Override public void putBoolean(long address, boolean x) {
            throw new UnsupportedOperationException();
        }
        @Override public byte getByte(long address) {
            throw new UnsupportedOperationException();
        }
        @Override public void putByte(long address, byte x) {
            throw new UnsupportedOperationException();
        }
        @Override public char getChar(long address) {
            throw new UnsupportedOperationException();
        }
        @Override public void putChar(long address, char x) {
            throw new UnsupportedOperationException();
        }
        @Override public short getShort(long address) {
            throw new UnsupportedOperationException();
        }
        @Override public void putShort(long address, short x) {
            throw new UnsupportedOperationException();
        }
        @Override public float getFloat(long address) {
            throw new UnsupportedOperationException();
        }
        @Override public void putFloat(long address, float x) {
            throw new UnsupportedOperationException();
        }
        @Override public double getDouble(long address) {
            throw new UnsupportedOperationException();
        }
        @Override public void putDouble(long address, double x) {
            throw new UnsupportedOperationException();
        }
    }
}
