/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.Iterator;
import java.util.Map;

public class MapChunkContext {

    private final String mapName;
    private ServiceNamespace serviceNamespace;
    private Iterator<Map.Entry<Data, Record>> iterator;

    private final int partitionId;
    private final long maxChunkSize;
    private final MutableInteger currentChunkSize;
    private final MapServiceContext mapServiceContext;

    public MapChunkContext(MapServiceContext mapServiceContext,
                           int partitionId, ServiceNamespace namespaces,
                           long maxChunkSize, MutableInteger currentChunkSize) {
        this.mapServiceContext = mapServiceContext;
        this.partitionId = partitionId;
        this.maxChunkSize = maxChunkSize;
        this.serviceNamespace = namespaces;
        this.currentChunkSize = currentChunkSize;
        this.mapName = ((ObjectNamespace) serviceNamespace).getObjectName();
        this.iterator = getRecordStore(mapName).iterator();
    }

    // TODO do we need to create a new record-store if there is no?
    private RecordStore getRecordStore(String mapName) {
        return mapServiceContext.getRecordStore(partitionId, mapName, true);
    }

    public boolean hasMoreChunks() {
        return iterator != null && iterator.hasNext();
    }

    public ServiceNamespace getServiceNamespace() {
        return serviceNamespace;
    }

    public Iterator<Map.Entry<Data, Record>> getIterator() {
        return iterator;
    }

    public MutableInteger getCurrentChunkSize() {
        return currentChunkSize;
    }

    public boolean hasReachedMaxSize() {
        boolean reached = maxChunkSize <= currentChunkSize.value;
        if (reached) {
            System.err.println("currentChunkSize.value: " + currentChunkSize.value);
        }
        return reached;
    }

    public void resetCurrentChunkSize() {
        currentChunkSize.value = 0;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getMapName() {
        return mapName;
    }
}
