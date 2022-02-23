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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.collection.Long2LongHashMap;
import junit.framework.AssertionFailedError;

import java.util.Arrays;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.BYTE_ARRAY_ACCESS;

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

    private int lastAllocatedAddress;

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
            assertFitsInt(size);
            assureEnoughMemoryLeft(size);
            final long addr = heapTop;
            lastAllocatedAddress = heapTop;
            heapTop += size;
            usedMemory += size;
            allocatedAddrs.put(addr, size);
            return addr;
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            assertFitsInt(address);
            assertFitsInt(currentSize);
            assertFitsInt(newSize);
            validateBlock(address, currentSize);
            if (address == lastAllocatedAddress) {
                final long sizeDelta = newSize - currentSize;
                assureEnoughMemoryLeft(sizeDelta);
                allocatedAddrs.put(address, newSize);
                heapTop += sizeDelta;
                usedMemory += sizeDelta;
                return address;
            } else {
                final long newAddress = allocate(newSize);
                System.arraycopy(storage, (int) address, storage, (int) newAddress,
                        (int) Math.min(currentSize, newSize));
                free0(address, currentSize);
                return newAddress;
            }
        }

        @Override
        public void free(long address, long size) {
            assertFitsInt(address);
            assertFitsInt(size);
            validateBlock(address, size);
            free0(address, size);
        }

        @Override
        public void dispose() {
            storage = null;
        }

        private void assureEnoughMemoryLeft(long size) {
            final long bytesFree = storage.length - toStorageIndex(heapTop);
            if (size > bytesFree) {
                throw new OutOfMemoryError(String.format("Asked to allocate %d, free bytes left %d", size, bytesFree));
            }
        }

        private void validateBlock(long address, long size) {
            final long allocatedSize = allocatedAddrs.get(address);
            if (allocatedSize == -1) {
                throw new AssertionFailedError(String.format("Address %d was not allocated", address));
            }
            if (allocatedSize != size) {
                throw new AssertionFailedError(String.format(
                        "Allocated size at %d was %d, but tried to free %d bytes", address, allocatedSize, size));
            }
        }

        private void free0(long address, long size) {
            allocatedAddrs.remove(address);
            usedMemory -= size;
        }

    }

    static void assertFitsInt(long arg) {
        assert arg > 0 && arg <= Integer.MAX_VALUE : "argument outside of legal range: " + arg;
    }

    static long toStorageIndex(long addr) {
        return addr - HEAP_BOTTOM;
    }

    class Accessor implements MemoryAccessor {

        @Override
        public byte getByte(long address) {
            assertAllocated(address);
            return storage[(int) toStorageIndex(address)];
        }

        @Override
        public void putByte(long address, byte x) {
            assertAllocated(address);
            storage[(int) toStorageIndex(address)] = x;
        }

        private void assertAllocated(long address) {
            assert address >= HEAP_BOTTOM && address < heapTop : String.format(
                    "Attempted to access unallocated address %,d. Heap top is %,d", address, heapTop);
        }

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

        @Override
        public boolean isBigEndian() {
            return false;
        }

        @Override
        public boolean getBoolean(long address) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBoolean(long address, boolean x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public char getChar(long address) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putChar(long address, char x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getShort(long address) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putShort(long address, short x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(long address) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putFloat(long address, float x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(long address) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDouble(long address, double x) {
            throw new UnsupportedOperationException();
        }
    }
}
