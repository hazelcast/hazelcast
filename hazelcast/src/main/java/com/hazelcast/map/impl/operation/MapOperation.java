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

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.wan.impl.CallerProvenance;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.util.List;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.util.CollectionUtil.isEmpty;

public abstract class MapOperation extends AbstractNamedOperation implements IdentifiedDataSerializable, ServiceNamespaceAware {

    protected transient MapService mapService;
    protected transient MapContainer mapContainer;
    protected transient MapServiceContext mapServiceContext;
    protected transient MapEventPublisher mapEventPublisher;
    protected transient RecordStore recordStore;

    protected transient boolean createRecordStoreOnDemand = true;

    /**
     * Used by wan-replication-service to disable wan-replication event publishing
     * otherwise in active-active scenarios infinite loop of event forwarding can be seen.
     */
    protected boolean disableWanReplicationEvent;

    public MapOperation() {
    }

    public MapOperation(String name) {
        this.name = name;
    }

    // for testing only
    public void setMapService(MapService mapService) {
        this.mapService = mapService;
    }

    // for testing only
    public void setMapContainer(MapContainer mapContainer) {
        this.mapContainer = mapContainer;
    }

    protected final CallerProvenance getCallerProvenance() {
        return disableWanReplicationEvent ? CallerProvenance.WAN : CallerProvenance.NOT_WAN;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();

        mapService = getService();
        mapServiceContext = mapService.getMapServiceContext();
        mapEventPublisher = mapServiceContext.getMapEventPublisher();

        innerBeforeRun();
    }

    public void innerBeforeRun() throws Exception {
        recordStore = getRecordStoreOrNull();
        if (recordStore == null) {
            mapContainer = mapServiceContext.getMapContainer(name);
        } else {
            mapContainer = recordStore.getMapContainer();
        }
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public boolean isPostProcessing(RecordStore recordStore) {
        MapDataStore mapDataStore = recordStore.getMapDataStore();
        return mapDataStore.isPostProcessingMapStore() || mapServiceContext.hasInterceptor(name);
    }

    public void setThreadId(long threadId) {
        throw new UnsupportedOperationException();
    }

    public long getThreadId() {
        throw new UnsupportedOperationException();
    }

    protected final void invalidateNearCache(List<Data> keys) {
        if (!mapContainer.hasInvalidationListener() || isEmpty(keys)) {
            return;
        }

        Invalidator invalidator = getNearCacheInvalidator();

        for (Data key : keys) {
            invalidator.invalidateKey(key, name, getCallerUuid());
        }
    }

    // TODO: improve here it's possible that client cannot manage to attach listener
    public final void invalidateNearCache(Data key) {
        if (!mapContainer.hasInvalidationListener() || key == null) {
            return;
        }

        Invalidator invalidator = getNearCacheInvalidator();
        invalidator.invalidateKey(key, name, getCallerUuid());
    }

    /**
     * This method helps to add clearing Near Cache event only from one-partition which matches partitionId of the map name.
     */
    protected final void invalidateAllKeysInNearCaches() {
        if (mapContainer.hasInvalidationListener()) {

            int partitionId = getPartitionId();
            Invalidator invalidator = getNearCacheInvalidator();

            if (partitionId == getNodeEngine().getPartitionService().getPartitionId(name)) {
                invalidator.invalidateAllKeys(name, getCallerUuid());
            }

            invalidator.resetPartitionMetaData(name, getPartitionId());
        }
    }

    private Invalidator getNearCacheInvalidator() {
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        return mapNearCacheManager.getInvalidator();
    }

    protected void evict(Data excludedKey) {
        assert recordStore != null : "Record-store cannot be null";

        recordStore.evictEntries(excludedKey);
    }

    private RecordStore getRecordStoreOrNull() {
        int partitionId = getPartitionId();
        if (partitionId == -1) {
            return null;
        }
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        if (createRecordStoreOnDemand) {
            return partitionContainer.getRecordStore(name);
        } else {
            return partitionContainer.getExistingRecordStore(name);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        MapContainer container = mapContainer;
        if (container == null) {
            MapService service = getService();
            container = service.getMapServiceContext().getMapContainer(name);
        }
        return container.getObjectNamespace();
    }

    /**
     * @return {@code true} if this operation can generate WAN event, otherwise return {@code false}
     * to indicate WAN event generation is not allowed for this operation
     */
    protected final boolean canThisOpGenerateWANEvent() {
        return !disableWanReplicationEvent;
    }

    protected final void publishWanUpdate(Data dataKey, Object value) {
        publishWanUpdateInternal(dataKey, value, false);
    }

    private void publishWanUpdateInternal(Data dataKey, Object value, boolean hasLoadProvenance) {
        if (!canPublishWANEvent()) {
            return;
        }

        Record record = recordStore.getRecord(dataKey);
        if (record == null) {
            return;
        }

        Data dataValue = toHeapData(mapServiceContext.toData(value));
        EntryView entryView = createSimpleEntryView(toHeapData(dataKey), dataValue, record);

        mapEventPublisher.publishWanUpdate(name, entryView, hasLoadProvenance);
    }

    protected final void publishLoadAsWanUpdate(Data dataKey, Object value) {
        publishWanUpdateInternal(dataKey, value, true);
    }

    protected final void publishWanRemove(Data dataKey) {
        if (!canPublishWANEvent()) {
            return;
        }

        mapEventPublisher.publishWanRemove(name, toHeapData(dataKey));
    }

    private boolean canPublishWANEvent() {
        return mapContainer.isWanReplicationEnabled() && canThisOpGenerateWANEvent();
    }
}
