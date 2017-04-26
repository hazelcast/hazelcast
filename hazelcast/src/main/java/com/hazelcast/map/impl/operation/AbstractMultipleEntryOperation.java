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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.LockAwareLazyMapEntry;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

abstract class AbstractMultipleEntryOperation extends MapOperation implements MutatingOperation, PartitionAwareOperation {

    protected MapEntries responses;
    protected EntryProcessor entryProcessor;
    protected EntryBackupProcessor backupProcessor;
    protected List<WanEventWrapper> wanEventList = new ArrayList<WanEventWrapper>();

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

    protected Map.Entry createMapEntry(Data key, Object value) {
        InternalSerializationService serializationService
                = ((InternalSerializationService) getNodeEngine().getSerializationService());
        boolean locked = recordStore.isLocked(key);
        return new LockAwareLazyMapEntry(key, value, serializationService, mapContainer.getExtractors(), locked);
    }

    protected boolean hasRegisteredListenerForThisMap() {
        final EventService eventService = getNodeEngine().getEventService();
        return eventService.hasEventRegistration(SERVICE_NAME, name);
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

    protected LocalMapStatsImpl getLocalMapStats() {
        LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
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
            final LazyMapEntry mapEntrySimple = (LazyMapEntry) entry;
            if (mapEntrySimple.isModified()) {
                return EntryEventType.UPDATED;
            }
        }
        // return null for read only operations.
        return null;
    }

    /**
     * Entry has not exist and no add operation has been done.
     * UPDATES gets count
     */
    protected boolean noOp(Map.Entry entry, Object oldValue, long now) {
        if (noOp(entry, oldValue)) {
            getLocalMapStats().incrementGets(getLatencyFrom(now));
            return true;
        }
        return false;
    }

    /**
     * Entry has not exist and no add operation has been done.
     * DOES NOT update gets count
     */
    protected boolean noOp(Map.Entry entry, Object oldValue) {
        final LazyMapEntry mapEntrySimple = (LazyMapEntry) entry;
        if (!mapEntrySimple.isModified() || (oldValue == null && entry.getValue() == null)) {
            return true;
        }
        return false;
    }

    protected boolean entryRemoved(Map.Entry entry, Data key, Object oldValue, long now) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.delete(key);
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
        invalidateNearCache(key);
        mapServiceContext.interceptAfterPut(name, newValue);
        if (isPostProcessing(recordStore)) {
            Record record = recordStore.getRecord(key);
            newValue = record == null ? null : record.getValue();
        }
        if (mapContainer.isWanReplicationEnabled()) {
            newValue = toData(newValue);
            publishWanReplicationEvent(key, (Data) newValue, eventType);
        }
        publishEntryEvent(key, newValue, oldValue, eventType);
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
        recordStore.set(key, value, DEFAULT_TTL);
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

    protected void publishEntryEvent(Data key, Object value, Object oldValue, EntryEventType eventType) {
        if (hasRegisteredListenerForThisMap()) {
            oldValue = nullifyOldValueIfNecessary(oldValue, eventType);
            mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, key, oldValue, value);
        }
    }

    protected void publishWanReplicationEvent(Data key, Data value, EntryEventType eventType) {
        if (EntryEventType.REMOVED == eventType) {
            mapEventPublisher.publishWanReplicationRemove(name, key, getNow());
            wanEventList.add(new WanEventWrapper(key, null, EntryEventType.REMOVED));
        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                final Data dataValueAsData = toData(value);
                final EntryView entryView = createSimpleEntryView(key, dataValueAsData, record);
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
                wanEventList.add(new WanEventWrapper(key, value, EntryEventType.UPDATED));
            }
        }
    }

    protected long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }

    protected Data process(Map.Entry entry) {
        final Object result = entryProcessor.process(entry);
        return toData(result);
    }

    protected void processBackup(Map.Entry entry) {
        backupProcessor.processBackup(entry);
    }

    protected boolean isKeyProcessable(Data key) {
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        return partitionService.getPartitionId(key) == getPartitionId();
    }

    // This method is overridden.
    protected boolean isEntryProcessable(Map.Entry entry) {
        return true;
    }

    public void setWanEventList(List<WanEventWrapper> wanEventList) {
        this.wanEventList = wanEventList;
    }

    protected static class WanEventWrapper {

        Data key;
        Data value;
        EntryEventType eventType;

        public WanEventWrapper(Data key, Data value, EntryEventType eventType) {
            this.key = key;
            this.value = value;
            this.eventType = eventType;
        }

        public Data getKey() {
            return key;
        }

        public void setKey(Data key) {
            this.key = key;
        }

        public Data getValue() {
            return value;
        }

        public void setValue(Data value) {
            this.value = value;
        }

        public EntryEventType getEventType() {
            return eventType;
        }

        public void setEventType(EntryEventType eventType) {
            this.eventType = eventType;
        }

    }
}
