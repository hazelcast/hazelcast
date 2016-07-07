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

package com.hazelcast.jet.memory.binarystorage.comparator;


import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.binarystorage.Hasher;

import static com.hazelcast.util.HashUtil.MurmurHash3_x64_64_direct;

/**
 * Compares two blobs lexicographically, by finding the first bit position where they differ and returning the
 * result of comparing the bits at that position. If the blobs are of unequal lengths, but match at all corresponding
 * bits, then the longer blob is deemed "greater".
 */
public class LexicographicBitwiseComparator implements Comparator, Hasher {
    private MemoryAccessor mem;
    private final Hasher partitionHasher = new PartitionHasher();

    public LexicographicBitwiseComparator(MemoryAccessor mem) {
        this.mem = mem;
    }

    public LexicographicBitwiseComparator() {
    }


    @Override
    public int compare(long leftAddress, long leftSize, long rightAddress, long rightSize) {
        return compare(this.mem, this.mem, leftAddress, rightSize, rightAddress, rightSize);
    }

    @Override
    public int compare(
            MemoryAccessor leftAccessor, MemoryAccessor rightAccessor,
            long leftAddress, long leftSize, long rightAddress, long rightSize
    ) {
        return lexicographicBlobCompare(leftAccessor, rightAccessor, leftAddress, leftSize, rightAddress, rightSize);
    }

    @Override
    public Hasher getHasher() {
        return this;
    }

    @Override
    public Hasher getPartitionHasher() {
        return partitionHasher;
    }

    @Override
    public boolean equal(MemoryAccessor leftMemoryAccessor, MemoryAccessor rightMemoryAccessor,
                         long leftAddress, long leftSize, long rightAddress, long rightSize
    ) {
        return compare(leftMemoryAccessor, rightMemoryAccessor, leftAddress, leftSize, rightAddress, rightSize) == 0;
    }

    @Override
    public long hash(MemoryAccessor memoryAccessor, long address, long length) {
        return MurmurHash3_x64_64_direct(memoryAccessor, address, 3, (int) (length - 3));
    }

    static int lexicographicBlobCompare(MemoryAccessor leftAccessor, MemoryAccessor rightAccessor,
                                        long leftAddress, long leftSize, long rightAddress, long rightSize
    ) {
        long shorterLength = Math.min(leftSize, rightSize);
        for (long i = 0; i < shorterLength; i++) {
            final int leftByte = Byte.toUnsignedInt(leftAccessor.getByte(leftAddress + i));
            final int rightByte = Byte.toUnsignedInt(rightAccessor.getByte(rightAddress + i));
            if (leftByte == rightByte) {
                continue;
            }
            return leftByte > rightByte ? 1 : -1;
        }
        return Long.compare(leftSize, rightSize);
    }

    private class PartitionHasher implements Hasher {
        private static final long FNV1_64_INIT = 0xcbf29ce484222325L;
        private static final long FNV1_PRIME_64 = 1099511628211L;

        @Override
        public boolean equal(MemoryAccessor leftMemoryAccessor, MemoryAccessor rightMemoryAccessor,
                             long leftAddress, long leftSize, long rightAddress, long rightSize
        ) {
            return compare(leftMemoryAccessor, rightMemoryAccessor, leftAddress, leftSize, rightAddress, rightSize) == 0;
        }

        @Override
        public long hash(MemoryAccessor memoryAccessor, long address, long length) {
            long hash = FNV1_64_INIT;
            for (int i = 0; i < length; i++) {
                hash ^= Byte.toUnsignedInt(memoryAccessor.getByte(address + i));
                hash *= FNV1_PRIME_64;
            }
            return hash;
        }
    }
}

