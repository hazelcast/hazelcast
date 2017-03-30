/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

public abstract class MutatingKeyBasedMapOperation extends MapOperation
        implements PartitionAwareOperation, MutatingOperation {

    protected Data dataKey;
    protected long threadId;
    protected Data dataValue;
    protected long ttl = DEFAULT_TTL;

    public MutatingKeyBasedMapOperation() {
    }

    public MutatingKeyBasedMapOperation(String name, Data dataKey) {
        super(name);
        this.dataKey = dataKey;
    }

    protected MutatingKeyBasedMapOperation(String name, Data dataKey, Data dataValue) {
        super(name);
        this.dataKey = dataKey;
        this.dataValue = dataValue;
    }

    protected MutatingKeyBasedMapOperation(String name, Data dataKey, long ttl) {
        super(name);
        this.dataKey = dataKey;
        this.ttl = ttl;
    }

    protected MutatingKeyBasedMapOperation(String name, Data dataKey, Data dataValue, long ttl) {
        super(name);
        this.dataKey = dataKey;
        this.dataValue = dataValue;
        this.ttl = ttl;
    }

    public final Data getKey() {
        return dataKey;
    }

    /**
     * noOp in two cases:
     * - setValue not called on entry
     * - or entry does not exist and no add operation is done.
     */
    protected boolean noOp(Map.Entry entry, Object oldValue) {
        final LazyMapEntry mapEntrySimple = (LazyMapEntry) entry;
        return !mapEntrySimple.isModified() || (oldValue == null && entry.getValue() == null);
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected LocalMapStatsImpl getLocalMapStats() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        return localMapStatsProvider.getLocalMapStatsImpl(name);
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
        out.writeUTF(name);
        out.writeData(dataKey);
        out.writeLong(threadId);
        out.writeData(dataValue);
        out.writeLong(ttl);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        dataKey = in.readData();
        threadId = in.readLong();
        dataValue = in.readData();
        ttl = in.readLong();
    }
}
