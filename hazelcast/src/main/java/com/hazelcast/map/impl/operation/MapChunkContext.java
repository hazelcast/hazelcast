/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.monitor.impl.LocalRecordStoreStatsImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.NotifiableIterator;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.MapIndexInfo;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Once instance created per record-store during migration.
 */
public class MapChunkContext {

    private final int partitionId;
    private final String mapName;
    private final SerializationService ss;
    private final ExpirySystem expirySystem;
    private final MapServiceContext mapServiceContext;
    private final RecordStore recordStore;
    private final LocalMapStatsImpl mapStats;

    private ServiceNamespace serviceNamespace;
    private volatile Iterator<Map.Entry<Data, Record>> iterator;

    public MapChunkContext(MapServiceContext mapServiceContext,
                           int partitionId, ServiceNamespace namespaces) {
        this.mapServiceContext = mapServiceContext;
        this.partitionId = partitionId;
        this.serviceNamespace = namespaces;
        this.mapName = ((ObjectNamespace) serviceNamespace).getObjectName();
        this.recordStore = getRecordStore(mapName);
        this.expirySystem = recordStore.getExpirySystem();
        this.ss = mapServiceContext.getNodeEngine().getSerializationService();
        this.mapStats = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(mapName);
    }

    // overridden in EE
    protected Iterator<Map.Entry<Data, Record>> createIterator() {
        return recordStore.iterator();
    }

    public final ILogger getLogger(String className) {
        return mapServiceContext.getNodeEngine().getLogger(className);
    }

    private RecordStore getRecordStore(String mapName) {
        return mapServiceContext.getRecordStore(partitionId, mapName, true);
    }

    public final boolean hasMoreChunks() {
        beforeOperation();
        try {
            return getIterator().hasNext();
        } finally {
            afterOperation();
        }
    }

    public final ServiceNamespace getServiceNamespace() {
        return serviceNamespace;
    }

    public final Iterator<Map.Entry<Data, Record>> getIterator() {
        if (iterator == null) {
            iterator = createIterator();
        }
        return iterator;
    }

    public final void setIterator(Iterator<Map.Entry<Data, Record>> iterator) {
        this.iterator = iterator;
    }

    public final RecordStore getRecordStore() {
        return recordStore;
    }

    public final int getPartitionId() {
        return partitionId;
    }

    public final String getMapName() {
        return mapName;
    }

    public final SerializationService getSerializationService() {
        return ss;
    }

    public final ExpiryMetadata getExpiryMetadata(Data dataKey) {
        return expirySystem.getExpiryMetadata(dataKey);
    }

    public final LocalMapStatsImpl getMapStats() {
        return mapStats;
    }

    public final MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    public final boolean isRecordStoreLoaded() {
        return recordStore.isLoaded();
    }

    public final LocalRecordStoreStatsImpl getStats() {
        return recordStore.getStats();
    }

    public final MapIndexInfo createMapIndexInfo() {
        MapContainer mapContainer = recordStore.getMapContainer();
        Set<IndexConfig> indexConfigs = new HashSet<>();
        if (mapContainer.isGlobalIndexEnabled()) {
            // global-index
            final Indexes indexes = mapContainer.getIndexes();
            for (Index index : indexes.getIndexes()) {
                indexConfigs.add(index.getConfig());
            }
            indexConfigs.addAll(indexes.getIndexDefinitions());
        } else {
            // partitioned-index
            final Indexes indexes = mapContainer.getIndexes(partitionId);
            if (indexes != null && indexes.haveAtLeastOneIndexOrDefinition()) {
                for (Index index : indexes.getIndexes()) {
                    indexConfigs.add(index.getConfig());
                }
                indexConfigs.addAll(indexes.getIndexDefinitions());
            }
        }
        return new MapIndexInfo(mapName)
                .addIndexCofigs(indexConfigs);
    }

    public final void beforeOperation() {
        recordStore.beforeOperation();
        if (getIterator() instanceof NotifiableIterator) {
            ((NotifiableIterator) getIterator()).onBeforeIteration();
        }
    }

    public final void afterOperation() {
        recordStore.afterOperation();
    }
}
