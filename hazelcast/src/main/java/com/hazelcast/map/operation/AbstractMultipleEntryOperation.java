/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.LocalMapStatsProvider;
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapEntrySimple;
import com.hazelcast.map.MapEventPublisher;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.NearCacheProvider;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.EventService;
import com.hazelcast.util.Clock;

import java.util.AbstractMap;
import java.util.Map;

import static com.hazelcast.map.EntryViews.createSimpleEntryView;

abstract class AbstractMultipleEntryOperation extends AbstractMapOperation {

    protected MapEntrySet responses;
    protected EntryProcessor entryProcessor;
    protected EntryBackupProcessor backupProcessor;
    protected transient RecordStore recordStore;


    protected AbstractMultipleEntryOperation() {
    }

    protected AbstractMultipleEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    protected AbstractMultipleEntryOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name);
        this.backupProcessor = backupProcessor;
    }

    @Override
    public void innerBeforeRun() {
        super.innerBeforeRun();
        this.recordStore = getRecordStore();
    }

    protected RecordStore getRecordStore() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getRecordStore(getPartitionId(), name);
    }


    protected Map.Entry createMapEntry(Object key, Object value) {
        return new MapEntrySimple(key, value);
    }

    protected boolean hasRegisteredListenerForThisMap() {
        final String serviceName = mapService.getMapServiceContext().serviceName();
        final EventService eventService = getNodeEngine().getEventService();
        return eventService.hasEventRegistration(serviceName, name);
    }

    /**
     * Nullify old value if in memory format is object and operation is not removal
     * since old and new value in fired event {@link com.hazelcast.core.EntryEvent}
     * may be same due to the object in memory format.
     */
    protected Object nullifyOldValueIfNecessary(Object oldValue, EntryEventType eventType) {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final InMemoryFormat format = mapConfig.getInMemoryFormat();
        if (format == InMemoryFormat.OBJECT && eventType != EntryEventType.REMOVED) {
            return null;
        } else {
            return oldValue;
        }
    }

    protected MapEventPublisher getMapEventPublisher() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapEventPublisher();
    }

    protected LocalMapStatsImpl getLocalMapStats() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        return localMapStatsProvider.getLocalMapStatsImpl(name);
    }

    private EntryEventType pickEventTypeOrNull(Map.Entry entry, Object oldValue) {
        final Object value = entry.getValue();
        if (value == null) {
            return EntryEventType.REMOVED;
        } else {
            if (oldValue == null) {
                return EntryEventType.ADDED;
            }
            final MapEntrySimple mapEntrySimple = (MapEntrySimple) entry;
            if (mapEntrySimple.isModified()) {
                return EntryEventType.UPDATED;
            }
        }
        // return null for read only operations.
        return null;
    }

    /**
     * Entry has not exist and no add operation has been done.
     */
    protected boolean noOp(Map.Entry entry, Object oldValue) {
        final MapEntrySimple mapEntrySimple = (MapEntrySimple) entry;
        return !mapEntrySimple.isModified() || (oldValue == null && entry.getValue() == null);
    }

    protected boolean entryRemoved(Map.Entry entry, Data key, Object oldValue, long now) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.remove(key);
            getLocalMapStats().incrementRemoves(getLatencyFrom(now));
            doPostOps(key, oldValue, entry);
            return true;
        }
        return false;
    }

    protected boolean entryAddedOrUpdated(Map.Entry entry, Data key, Object oldValue, long now) {
        final Object value = entry.getValue();
        if (value != null) {
            put(key, value);
            getLocalMapStats().incrementPuts(getLatencyFrom(now));
            doPostOps(key, oldValue, entry);
            return true;
        }
        return false;
    }

    protected void doPostOps(Data key, Object oldValue, Map.Entry entry) {
        final EntryEventType eventType = pickEventTypeOrNull(entry, oldValue);
        if (eventType == null) {
            return;
        }

        Object newValue = entry.getValue();
        invalidateNearCaches(key);
        // assign it again since we don't want to serialize newValue every time.
        newValue = publishEntryEvent(key, newValue, oldValue, eventType);
        publishWanReplicationEvent(key, newValue, eventType);
    }

    protected boolean entryRemovedBackup(Map.Entry entry, Data key) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.removeBackup(key);
            return true;
        }
        return false;
    }

    protected boolean entryAddedOrUpdatedBackup(Map.Entry entry, Data key) {
        final Object value = entry.getValue();
        if (value != null) {
            recordStore.putBackup(key, value);
            return true;
        }
        return false;
    }

    protected void put(Data key, Object value) {
        recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(key, value));
    }


    protected Object toObject(Object data) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toObject(data);
    }

    protected Data toData(Object obj) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toData(obj);
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected void invalidateNearCaches(Data key) {
        final String mapName = name;
        final MapServiceContext mapServiceContext = getMapServiceContext();
        final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        if (nearCacheProvider.isNearCacheAndInvalidationEnabled(mapName)) {
            nearCacheProvider.invalidateAllNearCaches(mapName, key);
        }
    }

    protected Object publishEntryEvent(Data key, Object value, Object oldValue, EntryEventType eventType) {
        if (hasRegisteredListenerForThisMap()) {
            oldValue = nullifyOldValueIfNecessary(oldValue, eventType);
            final MapEventPublisher mapEventPublisher = getMapEventPublisher();
            value = toData(value);
            mapEventPublisher.
                    publishEvent(getCallerAddress(), name, eventType, key, toData(oldValue), (Data) value);
        }
        return value;
    }

    protected void publishWanReplicationEvent(Data key, Object value, EntryEventType eventType) {
        final MapContainer mapContainer = this.mapContainer;
        if (mapContainer.getWanReplicationPublisher() == null
                && mapContainer.getWanMergePolicy() == null) {
            return;
        }
        final MapEventPublisher mapEventPublisher = getMapEventPublisher();
        if (EntryEventType.REMOVED.equals(eventType)) {
            mapEventPublisher.publishWanReplicationRemove(name, key, getNow());
        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                final Data dataValueAsData = toData(value);
                final EntryView entryView = createSimpleEntryView(key, dataValueAsData, record);
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
            }
        }
    }


    protected MapServiceContext getMapServiceContext() {
        final MapService mapService = getService();
        return mapService.getMapServiceContext();
    }


    protected long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }

    protected void addToResponses(Data key, Data response) {
        if (response == null) {
            return;
        }
        if (responses == null) {
            responses = new MapEntrySet();
        }
        responses.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, response));
    }

    protected Data process(Map.Entry entry) {
        final Object result = entryProcessor.process(entry);
        return toData(result);
    }

    protected void processBackup(Map.Entry entry) {
        backupProcessor.processBackup(entry);
    }

    protected Object getValueFor(Data dataKey, long now) {
        final Map.Entry<Data, Object> mapEntry = recordStore.getMapEntry(dataKey, now);
        return mapEntry.getValue();
    }

    protected boolean keyNotOwnedByThisPartition(Data key) {
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        return partitionService.getPartitionId(key) != getPartitionId();
    }

    protected void evict(boolean backup) {
        final long now = Clock.currentTimeMillis();
        recordStore.evictEntries(now, backup);
    }

}
