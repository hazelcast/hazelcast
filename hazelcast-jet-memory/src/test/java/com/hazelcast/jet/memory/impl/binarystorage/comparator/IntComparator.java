package com.hazelcast.jet.memory.impl.binarystorage.comparator;


import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.impl.util.HashUtil;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryHasher;

public class IntComparator implements BinaryComparator, BinaryHasher {
    private MemoryAccessor mem;
    private BinaryHasher partitionHasher = new PartitionHasher();

    public IntComparator(MemoryAccessor mem) {
        this.mem = mem;
    }

    public IntComparator() {

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
        return partitionHasher;
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
        return HashUtil.MurmurHash3_x64_64_direct(
                memoryAccessor,
                address,
                3,
                (int) (length - 3),
                0x01000193,
                true
        );
    }

    private class PartitionHasher implements BinaryHasher {
        private static final long FNV1_64_INIT = 0xcbf29ce484222325L;
        private static final long FNV1_PRIME_64 = 1099511628211L;

        @Override
        public boolean equal(long leftAddress, long leftSize, long rightAddress, long rightSize) {
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
            long hash = FNV1_64_INIT;
            for (int i = 0; i < length; i++) {
                hash ^= (memoryAccessor.getByte(address + i) & 0xff);
                hash *= FNV1_PRIME_64;
            }
            return hash;
        }
    }
}

