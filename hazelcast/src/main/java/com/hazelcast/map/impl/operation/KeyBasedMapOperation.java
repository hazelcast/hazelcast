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

package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V3_11;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_MAX_IDLE;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

public abstract class KeyBasedMapOperation extends MapOperation
        implements PartitionAwareOperation, Versioned {

    protected Data dataKey;
    protected long threadId;
    protected Data dataValue;
    protected long ttl = DEFAULT_TTL;
    protected long maxIdle = DEFAULT_MAX_IDLE;

    public KeyBasedMapOperation() {
    }

    public KeyBasedMapOperation(String name, Data dataKey) {
        super(name);
        this.dataKey = dataKey;
    }

    protected KeyBasedMapOperation(String name, Data dataKey, Data dataValue) {
        super(name);
        this.dataKey = dataKey;
        this.dataValue = dataValue;
    }

    protected KeyBasedMapOperation(String name, Data dataKey, long ttl, long maxIdle) {
        super(name);
        this.dataKey = dataKey;
        this.ttl = ttl;
        this.maxIdle = maxIdle;
    }

    protected KeyBasedMapOperation(String name, Data dataKey, Data dataValue, long ttl, long maxIdle) {
        super(name);
        this.dataKey = dataKey;
        this.dataValue = dataValue;
        this.ttl = ttl;
        this.maxIdle = maxIdle;
    }

    public final Data getKey() {
        return dataKey;
    }

    @Override
    public final long getThreadId() {
        return threadId;
    }

    @Override
    public final void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public final Data getValue() {
        return dataValue;
    }

    public final long getTtl() {
        return ttl;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(dataKey);
        out.writeLong(threadId);
        out.writeData(dataValue);
        out.writeLong(ttl);
        //RU_COMPAT_3_10
        if (out.getVersion().isGreaterOrEqual(V3_11)) {
            out.writeLong(maxIdle);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataKey = in.readData();
        threadId = in.readLong();
        dataValue = in.readData();
        ttl = in.readLong();
        //RU_COMPAT_3_10
        if (in.getVersion().isGreaterOrEqual(V3_11)) {
            maxIdle = in.readLong();
        }
    }
}
