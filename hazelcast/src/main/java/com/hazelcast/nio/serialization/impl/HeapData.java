/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization.impl;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.HashUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

/**
 * A {@link Data} implementation where the content lives on the heap.
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public final class HeapData implements Data {
  // type and partition_hash are always written with BIG_ENDIAN byte-order
    public static final int TYPE_OFFSET = 0;
    // will use a byte to store partition_hash bit
    public static final int PARTITION_HASH_BIT_OFFSET = 4;
    public static final int DATA_OFFSET = 5;

    // array (12: array header, 4: length)
    private static final int ARRAY_HEADER_SIZE_IN_BYTES = 16;

    private byte[] data;

    public HeapData() {
    }

    public HeapData(byte[] data) {
        if (data != null && data.length > 0 && data.length < DATA_OFFSET) {
            throw new IllegalArgumentException("Data should be either empty or should contain more than "
                    + HeapData.DATA_OFFSET + " bytes! -> " + Arrays.toString(data));
        }
        this.data = data;
    }

    @Override
    public int dataSize() {
        return Math.max(totalSize() - DATA_OFFSET, 0);
    }

    @Override
    public int totalSize() {
        return data != null ? data.length : 0;
    }

    @Override
    public int getPartitionHash() {
        if (hasPartitionHash()) {
            return Bits.readIntB(data, data.length - Bits.INT_SIZE_IN_BYTES);
        }
        return hashCode();
    }

    @Override
    public boolean hasPartitionHash() {
        return totalSize() != 0 && data[PARTITION_HASH_BIT_OFFSET] != 0;
    }

    @Override
    public byte[] toByteArray() {
        return data;
    }

    @Override
    public int getType() {
        if (totalSize() == 0) {
            return SerializationConstants.CONSTANT_TYPE_NULL;
        }
        return Bits.readIntB(data, TYPE_OFFSET);
    }

    @Override
    public int getHeapCost() {
        // reference (assuming compressed oops)
        int objectRef = Bits.INT_SIZE_IN_BYTES;
        return objectRef + (data != null ? ARRAY_HEADER_SIZE_IN_BYTES + data.length : 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (!(o instanceof Data)) {
            return false;
        }

        Data data = (Data) o;
        if (getType() != data.getType()) {
            return false;
        }

        final int dataSize = dataSize();
        if (dataSize != data.dataSize()) {
            return false;
        }

        return dataSize == 0 || equals(this.data, data.toByteArray());
    }

    // Same as Arrays.equals(byte[] a, byte[] a2) but loop order is reversed.
    private static boolean equals(byte[] data1, byte[] data2) {
        if (data1 == data2) {
            return true;
        }
        if (data1 == null || data2 == null) {
            return false;
        }
        final int length = data1.length;
        if (data2.length != length) {
            return false;
        }
        for (int i = length - 1; i >= DATA_OFFSET; i--) {
            if (data1[i] != data2[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return HashUtil.MurmurHash3_x86_32(data, DATA_OFFSET, dataSize());
    }

    @Override
    public long hash64() {
        return HashUtil.MurmurHash3_x64_64(data, DATA_OFFSET, dataSize());
    }

    @Override
    public boolean isPortable() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE == getType();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DefaultData{");
        sb.append("type=").append(getType());
        sb.append(", hashCode=").append(hashCode());
        sb.append(", partitionHash=").append(getPartitionHash());
        sb.append(", totalSize=").append(totalSize());
        sb.append(", dataSize=").append(dataSize());
        sb.append(", heapCost=").append(getHeapCost());
        sb.append('}');
        return sb.toString();
    }
}
