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
public class DataStreamInfo implements DataSerializable {
    private long bytesConsumed;
    private long bytesAllocated;
    private long regionsInUse;
    private long count;

    private DataStreamInfo() {
    }

    public DataStreamInfo(long bytesConsumed, long bytesAllocated, long regionsInUse, long count) {
        this.bytesConsumed = bytesConsumed;
        this.bytesAllocated = bytesAllocated;
        this.regionsInUse = regionsInUse;
        this.count = count;
    }

    public long regionsInUse() {
        return regionsInUse;
    }

    public long bytesConsumed() {
        return bytesConsumed;
    }

    public long bytesAllocated() {
        return bytesAllocated;
    }

    public long count() {
        return count;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(bytesConsumed);
        out.writeLong(bytesAllocated);
        out.writeLong(regionsInUse);
        out.writeLong(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.bytesConsumed = in.readLong();
        this.bytesAllocated = in.readLong();
        this.regionsInUse = in.readLong();
        this.count = in.readLong();
    }

    @Override
    public String toString() {
        return "MemoryInfo{"
                + "count=" + count
                + ", bytesConsumed=" + bytesConsumed
                + ", bytesConsumedPerEntry=" + (bytesConsumed / count)
                + ", bytesAllocated=" + bytesAllocated
                + ", storageEfficiency=" + ((100d * bytesConsumed) / bytesAllocated) + "%"
                + ", regionsInUse=" + regionsInUse
                + '}';
    }
}
