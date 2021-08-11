/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.util.HashUtil;

/**
 * PartitionStamp is a utility class to generate stamp for the partition table.
 */
public final class PartitionStamp {

    private static ThreadLocal<byte[]> BYTE_ARRAY = new ThreadLocal<>();

    private PartitionStamp() {
    }

    /**
     * Calculates 64-bit stamp value for the given partitions.
     * Stamp is calculated by hashing the individual partition versions
     * using MurmurHash3.
     *
     * @param partitions partition table
     * @return stamp value
     */
    public static long calculateStamp(InternalPartition[] partitions) {
        byte[] bb = BYTE_ARRAY.get();
        if (bb == null) {
            bb = new byte[Integer.BYTES * partitions.length];
            BYTE_ARRAY.set(bb);
        }

        for (InternalPartition partition : partitions) {
            Bits.writeIntB(bb, partition.getPartitionId() * Integer.BYTES, partition.version());
        }
        return HashUtil.MurmurHash3_x64_64(bb, 0, bb.length);
    }
}
