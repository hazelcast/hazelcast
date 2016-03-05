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

package com.hazelcast.jet.memory.spi.binarystorage;

import com.hazelcast.internal.memory.MemoryAccessor;

/**
 * Comparator of  binary data.
 */
public interface BinaryComparator {
    /**
     * Compares two blobs specified by addresses and sizes;
     *
     * @param leftAddress  address of the first blob;
     * @param leftSize     size of the first blob;
     * @param rightAddress address of the second blob;
     * @param rightSize    size of the second blob;
     * @return 1 if left blob greater than right blob;
     * -1 if right blob greater than left blob;
     * 0 if blobs are equal;
     */
    int compare(long leftAddress, long leftSize,
                long rightAddress, long rightSize);

    /**
     * Compares two blobs specified by addresses and sizes;
     *
     * @param leftAccessor  memory accessor to read data for the left blob;
     * @param rightAccessor memory accessor to read data for the right blob;
     * @param leftAddress   address of the first blob;
     * @param leftSize      size of the first blob;
     * @param rightAddress  address of the second blob;
     * @param rightSize     size of the second blob;
     * @return 1 if left blob is greater than right blob;
     * -1 if right blob greater than left blob;
     * 0 if blobs are equal;
     */
    int compare(MemoryAccessor leftAccessor,
                MemoryAccessor rightAccessor,
                long leftAddress, long leftSize,
                long rightAddress, long rightSize);

    /**
     * @return Hasher corresponding to current
     * comparator which will be used for insertion into OA hashTable
     */
    BinaryHasher getHasher();

    /**
     * @return Hasher corresponding to current
     * comparator which will be used for partition balancing
     */
    BinaryHasher getPartitionHasher();
}
