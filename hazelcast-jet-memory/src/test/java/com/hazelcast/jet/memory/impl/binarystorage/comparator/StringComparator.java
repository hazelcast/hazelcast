package com.hazelcast.jet.memory.impl.binarystorage.comparator;

import com.hazelcast.jet.memory.impl.util.HashUtil;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryHasher;
import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;

public class StringComparator implements BinaryComparator, BinaryHasher {
    private MemoryAccessor mem;
    private BinaryHasher partitionHasher = new PartitionHasher();

    public StringComparator() {

    }

    public StringComparator(MemoryAccessor mem) {
        this.mem = mem;
    }

    @Override
    public int compare(long leftAddress, long leftSize, long rightAddress, long rightSize) {
        return compare(mem, mem, leftAddress, leftSize, rightAddress, rightSize);
    }

    @Override
    public int compare(MemoryAccessor leftAccessor,
                       MemoryAccessor rightAccessor,
                       long leftAddress,
                       long leftSize,
                       long rightAddress,
                       long rightSize) {
        int leftLength = EndiannessUtil.readInt(EndiannessUtil.CUSTOM_ACCESS, leftAccessor, leftAddress + 5, true);
        int rightLength = EndiannessUtil.readInt(EndiannessUtil.CUSTOM_ACCESS, rightAccessor, rightAddress + 5, true);

        long minLength = leftLength <= rightLength ? leftLength : rightLength;

        long leftStart = 9 + leftAddress;
        long rightStart = 9 + rightAddress;

        for (long i = 0; i < minLength; i++) {
            int leftChar = leftAccessor.getByte(leftStart + i) & 0xff;
            int rightChar = rightAccessor.getByte(rightStart + i) & 0xff;

            if (leftChar > rightChar) {
                return 1;
            } else if (leftChar < rightChar) {
                return -1;
            }
        }

        if (leftLength == rightLength) {
            return 0;
        }

        return (leftLength > rightLength) ? 1 : -1;
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
    public long hash(long address, long length, MemoryAccessor memoryAccessor) {
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

