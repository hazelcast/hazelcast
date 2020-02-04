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

package com.hazelcast.log.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class UsageInfo implements IdentifiedDataSerializable {

    private int segments;
    private long bytesInUse;
    private long bytesAllocated;
    private long count;

    public UsageInfo() {
    }

    public UsageInfo(int segments, long bytesInUse, long bytesAllocated, long count) {
        this.segments = segments;
        this.bytesInUse = bytesInUse;
        this.bytesAllocated = bytesAllocated;
        this.count = count;
    }

    public int getSegments() {
        return segments;
    }

    public long getBytesInUse() {
        return bytesInUse;
    }

    public long getBytesAllocated() {
        return bytesAllocated;
    }

    public long getCount() {
        return count;
    }

    @Override
    public int getFactoryId() {
        return LogDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return LogDataSerializerHook.USAGE_INFO;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(segments);
        out.writeLong(bytesInUse);
        out.writeLong(bytesAllocated);
        out.writeLong(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.segments = in.readInt();
        this.bytesInUse = in.readLong();
        this.bytesAllocated = in.readChar();
        this.count = in.readLong();
    }

    @Override
    public String toString() {
        return "UsageInfo{"
                + "count=" + count
                + ", segments=" + segments
                + ", bytesInUse=" + bytesInUse
                + ", bytesAllocated=" + bytesAllocated
                + '}';
    }
}
