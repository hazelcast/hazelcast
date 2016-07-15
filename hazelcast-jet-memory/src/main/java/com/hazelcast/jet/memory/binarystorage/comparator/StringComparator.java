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

import static com.hazelcast.jet.memory.binarystorage.comparator.LexicographicBitwiseComparator.lexicographicBlobCompare;
import static com.hazelcast.util.HashUtil.MurmurHash3_x64_64_direct;

/**
 * Compares blobs byte for byte. A blob is assumed to have a header which tells its size.
 */
public class StringComparator implements Comparator, Hasher {
    public static final int PAYLOAD_OFFSET = 5;
    private MemoryAccessor mem;
    private Hasher partitionHasher = new PartitionHasher();

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
    public int compare(MemoryAccessor leftAccessor, MemoryAccessor rightAccessor,
                       long leftAddress, long leftSize, long rightAddress, long rightSize
    ) {
        return lexicographicBlobCompare(leftAccessor, rightAccessor,
                leftAddress + PAYLOAD_OFFSET, leftSize - PAYLOAD_OFFSET,
                rightAddress + PAYLOAD_OFFSET, rightSize - PAYLOAD_OFFSET);
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
        return MurmurHash3_x64_64_direct(memoryAccessor, address, PAYLOAD_OFFSET, (int) (length - PAYLOAD_OFFSET));
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

