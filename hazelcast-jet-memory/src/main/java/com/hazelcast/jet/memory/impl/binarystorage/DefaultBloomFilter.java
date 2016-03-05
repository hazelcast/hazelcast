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

package com.hazelcast.jet.memory.impl.binarystorage;

import com.hazelcast.nio.Bits;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.api.binarystorage.BloomFilter;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Represents abstract bloomFilter functionality;
 */
public class DefaultBloomFilter implements BloomFilter {
    private static final int BITS_IN_BYTES_DEGREE = 3;

    private final long bloomFilterMask;
    private final int bloomFilterSize;
    private MemoryBlock memoryBlock;
    private final long bloomFilterOffset;
    private long bloomFilterAddress;

    public DefaultBloomFilter(int bloomFilterSize,
                              long bloomFilterOffset) {
        assert Util.isPositivePowerOfTwo(bloomFilterSize);

        this.bloomFilterSize = bloomFilterSize;
        this.bloomFilterOffset = bloomFilterOffset;
        this.bloomFilterMask = (bloomFilterSize << BITS_IN_BYTES_DEGREE) - 1;
    }

    @Override
    public void mark(long hashCode) {
        checkNotNull(memoryBlock);

        long bitAbsoluteOffset = getIndexOfBit(Math.abs(hashCode));
        long byteOffset = getByteOffset(bitAbsoluteOffset);
        int bitOffset = getBitOffset(bitAbsoluteOffset);
        byte value = memoryBlock.getAccessor().getByte(bloomFilterAddress + byteOffset);
        value = Bits.setBit(value, bitOffset);
        memoryBlock.getAccessor().putByte(bloomFilterAddress + byteOffset, value);
    }

    @Override
    public boolean isSet(long hashCode) {
        checkNotNull(memoryBlock);

        long bitAbsoluteOffset = getIndexOfBit(Math.abs(hashCode));
        long byteOffset = getByteOffset(bitAbsoluteOffset);
        int bitOffset = getBitOffset(bitAbsoluteOffset);
        int value = memoryBlock.getAccessor().getByte(bloomFilterAddress + byteOffset);
        return Bits.isBitSet(value, bitOffset);
    }

    @Override
    public void allocateBloomFilter() {
        checkNotNull(memoryBlock);

        long address = memoryBlock.getAllocator().allocate(
                bloomFilterSize
        );

        assert bloomFilterAddress == address;
        memoryBlock.getAccessor().setMemory(address, bloomFilterSize, Util.ZERO);
    }

    @Override
    public void setMemoryBlock(MemoryBlock memoryBlock) {
        this.memoryBlock = memoryBlock;
        this.bloomFilterAddress = memoryBlock.toAddress(bloomFilterOffset);
    }

    @Override
    public void reset() {
        memoryBlock.getAccessor().setMemory(bloomFilterAddress, bloomFilterSize, Util.ZERO);
    }

    private long getIndexOfBit(long hashCode) {
        return hashCode & bloomFilterMask;
    }

    private long getByteOffset(long bitAbsoluteOffset) {
        return bitAbsoluteOffset >> BITS_IN_BYTES_DEGREE;
    }

    private int getBitOffset(long bitAbsoluteOffset) {
        return (int) (bitAbsoluteOffset & BITS_IN_BYTES_DEGREE);
    }
}
