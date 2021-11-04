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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;

import java.util.Iterator;
import java.util.Map;

public class MapChunkContext {

    private final String mapName;
    private final SerializationService ss;
    private final ExpirySystem expirySystem;
    private final int partitionId;
    private final MapServiceContext mapServiceContext;

    private ServiceNamespace serviceNamespace;
    private Iterator<Map.Entry<Data, Record>> iterator;

    public MapChunkContext(MapServiceContext mapServiceContext,
                           int partitionId, ServiceNamespace namespaces) {
        this.mapServiceContext = mapServiceContext;
        this.partitionId = partitionId;
        this.serviceNamespace = namespaces;
        this.mapName = ((ObjectNamespace) serviceNamespace).getObjectName();
        RecordStore recordStore = getRecordStore(mapName);
        this.iterator = recordStore.iterator();
        this.expirySystem = recordStore.getExpirySystem();
        this.ss = mapServiceContext.getNodeEngine().getSerializationService();
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

    public int getPartitionId() {
        return partitionId;
    }

    public String getMapName() {
        return mapName;
    }

    public SerializationService getSerializationService() {
        return ss;
    }

    public ExpiryMetadata getExpiryMetadata(Data dataKey) {
        return expirySystem.getExpiryMetadata(dataKey);
    }
}
