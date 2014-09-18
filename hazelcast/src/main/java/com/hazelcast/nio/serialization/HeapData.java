/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.util.HashUtil;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("EI_EXPOSE_REP")
public final class HeapData implements MutableData {

    private int type = SerializationConstants.CONSTANT_TYPE_NULL;
    private byte[] header;
    private byte[] data;
    private int partitionHash;

    public HeapData() {
    }

    public HeapData(int type, byte[] data) {
        this.data = data;
        this.type = type;
    }

    public HeapData(int type, byte[] data, int partitionHash) {
        this.data = data;
        this.partitionHash = partitionHash;
        this.type = type;
    }

    public HeapData(int type, byte[] data, int partitionHash, byte[] header) {
        this.type = type;
        this.data = data;
        this.partitionHash = partitionHash;
        this.header = header;
    }

    @Override
    public int dataSize() {
        return data != null ? data.length : 0;
    }

    @Override
    public int getPartitionHash() {
        return partitionHash != 0 ? partitionHash : hashCode();
    }

    @Override
    public boolean hasPartitionHash() {
        return partitionHash != 0;
    }

    @Override
    public byte[] getHeader() {
        return header;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public void setData(byte[] array) {
        this.data = array;
    }

    @Override
    public void setPartitionHash(int partitionHash) {
        this.partitionHash = partitionHash;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public void setType(int type) {
        this.type = type;
    }

    @Override
    public void setHeader(byte[] header) {
        this.header = header;
    }

    @Override
    public int getHeapCost() {
        final int integerSizeInBytes = 4;
        final int arrayHeaderSizeInBytes = 16;

        int total = 0;
        // type
        total += integerSizeInBytes;

        if (header != null) {
            // metadata array ref (12: array header, 4: length)
            total += arrayHeaderSizeInBytes;
            total += header.length;
        } else {
            total += integerSizeInBytes;
        }

        if (data != null) {
            // buffer array ref (12: array header, 4: length)
            total += arrayHeaderSizeInBytes;
            // data itself
            total += data.length;
        } else {
            total += integerSizeInBytes;
        }

        // partition-hash
        total += integerSizeInBytes;
        return total;
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

        return dataSize == 0 || equals(this.data, data.getData());
    }

    // Same as Arrays.equals(byte[] a, byte[] a2) but loop order is reversed.
    private static boolean equals(final byte[] data1, final byte[] data2) {
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
        for (int i = length - 1; i >= 0; i--) {
            if (data1[i] != data2[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return HashUtil.MurmurHash3_x86_32(data, 0, dataSize());
    }

    @Override
    public long hash64() {
        return HashUtil.MurmurHash3_x64_64(data, 0, dataSize());
    }

    @Override
    public boolean isPortable() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE == type;
    }

    //    @Override
//    public int getPortableFactoryId(ByteOrder byteOrder) {
//        return Bits.readInt(data, 0, byteOrder == ByteOrder.BIG_ENDIAN);
//    }
//
//    @Override
//    public int getPortableClassId(ByteOrder byteOrder) {
//        return Bits.readInt(data, 4, byteOrder == ByteOrder.BIG_ENDIAN);
//    }
//
//    @Override
//    public int getPortableVersion(ByteOrder byteOrder) {
//        return Bits.readInt(data, 8, byteOrder == ByteOrder.BIG_ENDIAN);
//    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HeapData{");
        sb.append("type=").append(getType());
        sb.append(", hashCode=").append(hashCode());
        sb.append(", partitionHash=").append(getPartitionHash());
        sb.append(", dataSize=").append(dataSize());
        sb.append(", heapCost=").append(getHeapCost());
        sb.append('}');
        return sb.toString();
    }
}
