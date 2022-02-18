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

package com.hazelcast.internal.util.collection;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;

import static java.lang.System.arraycopy;

/**
 * Memory manager backed by {@code long[][]}. Supports the minimum function needed for {@link LongSetHsa}:
 * <ul>
 *     <li>A maximum of two blocks can be allocated at any time.</li>
 *     <li>All addresses and sizes must be 8 byte-aligned.</li>
 *     <li>Memory accessor supports only {@code getLong()}, {@code putLong()}, and {@code copyMemory()}.</li>
 * </ul>
 */
public class HsaHeapMemoryManager implements MemoryManager {

    private static final int BLOCK_INDEX_BIT = 62;
    private static final int ALIGNMENT_BITS = 7;
    private static final int ADDR_TO_ARRAY_INDEX_SHIFT = 3;
    private static final int LOWEST_ADDRESS = 8;

    private final long[][] blocks = new long[2][];
    private final Allocator malloc = new Allocator();
    private final Accessor mem = new Accessor();

    @Override
    public MemoryAllocator getAllocator() {
        return malloc;
    }

    @Override
    public MemoryAccessor getAccessor() {
        return mem;
    }

    @Override
    public void dispose() {
        malloc.dispose();
    }

    public long getUsedMemory() {
        long used = 0;
        for (long[] block : blocks) {
            used += (block != null ? block.length : 0);
        }
        return used;
    }

    final long[] addrToBlock(long address) {
        final long[] block = blocks[addrToBlockIndex(address)];
        assert block != null : "Attempt to access non-allocated address " + address;
        return block;
    }

    static boolean isAligned(long address) {
        return (address & ALIGNMENT_BITS) == 0;
    }

    static int addrToBlockIndex(long address) {
        assert address >= LOWEST_ADDRESS && isAligned(address) : "Invalid address " + address;
        return (int) (address - LOWEST_ADDRESS >> BLOCK_INDEX_BIT);
    }

    static int addrToArrayIndex(long address) {
        return (int) (address - LOWEST_ADDRESS >> ADDR_TO_ARRAY_INDEX_SHIFT);
    }


    private final class Allocator implements MemoryAllocator {

        @Override
        public long allocate(long size) {
            assert size > 0 && size <= Integer.MAX_VALUE && isAligned(size) : "HsaHeapAllocator.allocate(" + size + ")";
            final int emptyBlockIndex = findEmptyBlockIndex();
            blocks[emptyBlockIndex] = new long[(int) size];
            return ((long) emptyBlockIndex << BLOCK_INDEX_BIT) + LOWEST_ADDRESS;
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            throw new UnsupportedOperationException("HsaHeapAllocator.reallocate()");
        }

        @Override
        public void free(long address, long size) {
            final int blockIndex = addrToBlockIndex(address);
            final long[] block = blocks[blockIndex];
            assert addrToArrayIndex(address) == 0 && block != null && block.length == size
                    : String.format("Misplaced HsaHeapAllocator.free(%x, %,d)", address, size);
            blocks[blockIndex] = null;
        }

        @Override
        public void dispose() {
            blocks[0] = null;
            blocks[1] = null;
        }

        private int findEmptyBlockIndex() {
            final int emptySlot = blocks[0] == null ? 0 : blocks[1] == null ? 1 : -1;
            assert emptySlot >= 0 : "Attempted to allocate a third block from HsaHeapAllocator";
            return emptySlot;
        }
    }

    private final class Accessor implements MemoryAccessor {

        @Override
        public boolean isBigEndian() {
            return false;
        }

        @Override
        public long getLong(long address) {
            return addrToBlock(address)[addrToArrayIndex(address)];
        }

        @Override
        public void putLong(long address, long x) {
            addrToBlock(address)[addrToArrayIndex(address)] = x;
        }

        @Override
        public void copyMemory(long srcAddress, long destAddress, long lengthBytes) {
            assert isAligned(srcAddress | destAddress | lengthBytes) : String.format(
                    "Unaligned copyMemory(%x, %x, %x)", srcAddress, destAddress, lengthBytes);
            final long[] srcArray = addrToBlock(srcAddress);
            final long[] destArray = addrToBlock(destAddress);
            final int srcIndexBase = addrToArrayIndex(srcAddress);
            final int destIndexBase = addrToArrayIndex(destAddress);
            arraycopy(srcArray, srcIndexBase, destArray, destIndexBase, (int) (lengthBytes >> ADDR_TO_ARRAY_INDEX_SHIFT));
        }


        // Begin unsupported MemoryAccessor API

        @Override
        public void copyFromByteArray(byte[] source, int offset, long destAddress, int length) {
            throw new UnsupportedOperationException("HsaHeapMemoryManager.Accessor.copyFromByteArray");
        }

        @Override
        public void copyToByteArray(long srcAddress, byte[] destination, int offset, int length) {
            throw new UnsupportedOperationException("HsaHeapMemoryManager.Accessor.copyToByteArray");
        }

        @Override
        public void setMemory(long address, long lengthBytes, byte value) {
            throw new UnsupportedOperationException("HsaHeapMemoryManager.Accessor.setMemory");
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
        public byte getByte(long address) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putByte(long address, byte x) {
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
        public int getInt(long address) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInt(long address, int x) {
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
