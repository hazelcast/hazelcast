package com.hazelcast.jet.memory.impl.binarystorage.comparator;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryHasher;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;

public class NumericComparator implements BinaryComparator, BinaryHasher {

    private final MemoryAccessor mem;

    public NumericComparator(MemoryAccessor mem) {
        this.mem = mem;
    }

    @Override
    public int compare(long leftAddress, long leftSize, long rightAddress, long rightSize) {
        return compare(this.mem, this.mem, leftAddress, rightSize, rightAddress, rightSize);
    }

    @Override
    public int compare(MemoryAccessor leftAccessor,
                       MemoryAccessor rightAccessor,
                       long leftAddress, long leftSize,
                       long rightAddress, long rightSize) {
        long minLength = leftSize <= rightSize ? leftSize : (int) rightSize;

        for (long i = 0; i < minLength; i++) {
            byte rightByte = leftAccessor.getByte(rightAddress + i);
            byte leftByte = rightAccessor.getByte(leftAddress + i);

            byte leftBit = (byte) (leftByte & (byte) 0x80);
            byte rightBit = (byte) (rightByte & (byte) 0x80);

            if ((leftBit != 0) && (rightBit == 0)) {
                return 1;
            } else if ((leftBit == 0) && (rightBit != 0)) {
                return -1;
            }

            byte unsignedLeftBit = (byte) (leftByte & (byte) 0x7F);
            byte unsignedRightBit = (byte) (rightByte & (byte) 0x7F);

            if (unsignedLeftBit > unsignedRightBit) {
                return 1;
            } else if (leftByte < rightByte) {
                return -1;
            }
        }

        if (rightSize == leftSize) {
            return 0;
        }

        return (leftSize > rightSize) ? 1 : -1;
    }

    @Override
    public BinaryHasher getHasher() {
        return this;
    }

    @Override
    public BinaryHasher getPartitionHasher() {
        return this;
    }

    @Override
    public boolean equal(long leftAddress, long leftSize,
                         long rightAddress, long rightSize) {
        return compare(leftAddress, leftSize, rightAddress, rightSize) == 0;
    }

    @Override
    public boolean equal(MemoryAccessor leftMemoryAccessor,
                         MemoryAccessor rightMemoryAccessor,
                         long leftAddress,
                         long leftSize,
                         long rightAddress,
                         long rightSize) {
        return compare(leftMemoryAccessor, rightMemoryAccessor, leftAddress, leftSize, rightAddress, rightSize) == 0;
    }

    @Override
    public long hash(long address, long length) {
        return hash(address, length, mem);
    }

    @Override
    public long hash(long address,
                     long length,
                     MemoryAccessor memoryAccessor) {
        int h = 0;

        if (length > 0) {
            for (int i = 0; i < length; i++) {
                h = 31 * h + memoryAccessor.getByte(address + i);
            }
        }

        return h;

    }
}
