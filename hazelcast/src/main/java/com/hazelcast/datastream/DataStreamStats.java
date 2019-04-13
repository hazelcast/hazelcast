/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Contains various statistics about the {@link DataStream}.
 */
public class DataStreamStats implements DataSerializable {
    private long consumedBytes;
    private long allocatedBytes;
    private int segmentsInUse;
    private long count;

    private DataStreamStats(){}

    public DataStreamStats(long consumedBytes, long allocatedBytes, int segmentsInUse, long count) {
        this.consumedBytes = consumedBytes;
        this.allocatedBytes = allocatedBytes;
        this.segmentsInUse = segmentsInUse;
        this.count = count;
    }

    public int segmentsInUse() {
        return segmentsInUse;
    }

    public long consumedBytes() {
        return consumedBytes;
    }

    public long allocatedBytes() {
        return allocatedBytes;
    }

    public long count() {
        return count;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(consumedBytes);
        out.writeLong(allocatedBytes);
        out.writeInt(segmentsInUse);
        out.writeLong(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.consumedBytes = in.readLong();
        this.allocatedBytes = in.readLong();
        this.segmentsInUse = in.readInt();
        this.count = in.readLong();
    }

    @Override
    public String toString() {
        return "MemoryInfo{"
                + "count=" + count
                + ", consumedBytes=" + consumedBytes
                + ", consumedBytesPerEntry=" + (consumedBytes / count)
                + ", allocatedBytes=" + allocatedBytes
                + ", storageEfficiency=" + ((100d * consumedBytes) / allocatedBytes) + "%"
                + ", segmentsInUse=" + segmentsInUse
                + '}';
    }
}
