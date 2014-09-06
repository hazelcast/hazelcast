/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.operation;

import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapService;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import java.io.IOException;

public abstract class KeyBasedMapOperation extends Operation implements PartitionAwareOperation {

    protected String name;
    protected Data dataKey;
    protected long threadId;
    protected Data dataValue;
    protected long ttl = -1;

    protected transient MapService mapService;
    protected transient MapContainer mapContainer;
    protected transient PartitionContainer partitionContainer;
    protected transient RecordStore recordStore;


    public KeyBasedMapOperation() {
    }

    public KeyBasedMapOperation(String name, Data dataKey) {
        super();
        this.dataKey = dataKey;
        this.name = name;
    }

    protected KeyBasedMapOperation(String name, Data dataKey, Data dataValue) {
        this.name = name;
        this.dataKey = dataKey;
        this.dataValue = dataValue;
    }

    protected KeyBasedMapOperation(String name, Data dataKey, long ttl) {
        this.name = name;
        this.dataKey = dataKey;
        this.ttl = ttl;
    }

    protected KeyBasedMapOperation(String name, Data dataKey, Data dataValue, long ttl) {
        this.name = name;
        this.dataKey = dataKey;
        this.dataValue = dataValue;
        this.ttl = ttl;
    }

    public final String getName() {
        return name;
    }

    public final Data getKey() {
        return dataKey;
    }

    public final long getThreadId() {
        return threadId;
    }

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
    public final void beforeRun() throws Exception {
        mapService = getService();
        mapContainer = mapService.getMapServiceContext().getMapContainer(name);
        partitionContainer = mapService.getMapServiceContext().getPartitionContainer(getPartitionId());
        recordStore = partitionContainer.getRecordStore(name);
        innerBeforeRun();
    }

    public void innerBeforeRun() {
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    protected final void invalidateNearCaches() {
        if (mapContainer.isNearCacheEnabled()
                && mapContainer.getMapConfig().getNearCacheConfig().isInvalidateOnChange()) {
            mapService.getMapServiceContext().getNearCacheProvider().invalidateAllNearCaches(name, dataKey);
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        dataKey.writeData(out);
        out.writeLong(threadId);
        IOUtil.writeNullableData(out, dataValue);
        out.writeLong(ttl);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        dataKey = new Data();
        dataKey.readData(in);
        threadId = in.readLong();
        dataValue = IOUtil.readNullableData(in);
        ttl = in.readLong();
    }
}
