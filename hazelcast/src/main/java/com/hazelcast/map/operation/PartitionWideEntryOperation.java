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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.EntryViews;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapEntrySimple;
import com.hazelcast.map.MapEventPublisher;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.NearCacheProvider;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;

/**
 * GOTCHA : This operation does not load missing keys from mapstore for now.
 */
public class PartitionWideEntryOperation extends AbstractMapOperation
        implements BackupAwareOperation, PartitionAwareOperation {

    private static final EntryEventType NO_NEED_TO_FIRE_EVENT = null;
    EntryProcessor entryProcessor;
    MapEntrySet response;

    public PartitionWideEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    public PartitionWideEntryOperation() {
    }

    public void innerBeforeRun() {
        final ManagedContext managedContext = getNodeEngine().getSerializationService().getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    public void run() {
        final MapServiceContext mapServiceContext = getMapServiceContext();
        response = new MapEntrySet();
        MapEntrySimple entry;
        final RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), name);
        final LocalMapStatsImpl mapStats
                = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(name);
        final Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            final long start = mapServiceContext.getNow();
            final Data key = record.getKey();
            final Object valueBeforeProcess = record.getValue();
            final Object valueBeforeProcessObject = mapServiceContext.toObject(valueBeforeProcess);
            Object objectKey = mapServiceContext.toObject(key);
            if (getPredicate() != null) {
                final SerializationService ss = getNodeEngine().getSerializationService();
                QueryEntry queryEntry = new QueryEntry(ss, key, objectKey, valueBeforeProcessObject);
                if (!getPredicate().apply(queryEntry)) {
                    continue;
                }
            }
            entry = new MapEntrySimple(objectKey, valueBeforeProcessObject);
            final Object result = entryProcessor.process(entry);
            final Object valueAfterProcess = entry.getValue();
            Data dataValue = null;
            if (result != null) {
                dataValue = mapServiceContext.toData(result);
                response.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, dataValue));
            }

            EntryEventType eventType;
            if (valueAfterProcess == null) {
                recordStore.remove(key);
                mapStats.incrementRemoves(getLatencyFrom(start));
                eventType = EntryEventType.REMOVED;
            } else {
                if (valueBeforeProcessObject == null) {
                    mapStats.incrementPuts(getLatencyFrom(start));
                    eventType = EntryEventType.ADDED;
                } else if (!entry.isModified()) {
                    // take this case as a read so no need to fire an event.
                    mapStats.incrementGets(getLatencyFrom(start));
                    eventType = NO_NEED_TO_FIRE_EVENT;
                } else {
                    mapStats.incrementPuts(getLatencyFrom(start));
                    eventType = EntryEventType.UPDATED;
                }
                // todo if this is a read only operation, record access operations should be done.
                if (eventType != NO_NEED_TO_FIRE_EVENT) {
                    recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(key, valueAfterProcess));
                }
            }
            fireEvent(key, valueBeforeProcess, valueAfterProcess, eventType);
            invalidateNearCache(key);
            publishWanReplicationEvent(key, dataValue, recordStore, eventType);
        }
    }

    private void fireEvent(Data dataKey, Object valueBeforeProcess,
                           Object valueAfterProcess, EntryEventType eventType) {
        final String mapName = name;
        final MapServiceContext mapServiceContext = getMapServiceContext();
        if (mapServiceContext.hasRegisteredListener(mapName) || eventType == NO_NEED_TO_FIRE_EVENT) {
            return;
        }
        final Data oldValue = mapServiceContext.toData(valueBeforeProcess);
        final Data value = mapServiceContext.toData(valueAfterProcess);
        final MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        mapEventPublisher.publishEvent(getCallerAddress(), mapName, eventType, dataKey, oldValue, value);

    }

    private void invalidateNearCache(Data key) {
        final String mapName = name;
        final MapServiceContext mapServiceContext = getMapServiceContext();
        final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        if (nearCacheProvider.isNearCacheAndInvalidationEnabled(mapName)) {
            nearCacheProvider.invalidateAllNearCaches(mapName, key);
        }
    }

    private void publishWanReplicationEvent(Data key, Data dataValue,
                                            RecordStore recordStore, EntryEventType eventType) {
        final String mapName = name;
        final MapServiceContext mapServiceContext = getMapServiceContext();
        final MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
            if (EntryEventType.REMOVED.equals(eventType)) {
                mapEventPublisher.publishWanReplicationRemove(mapName, key, Clock.currentTimeMillis());
            } else {
                Record record = recordStore.getRecord(key);
                if (record != null) {
                    final EntryView entryView = EntryViews.createSimpleEntryView(key, dataValue, record);
                    mapEventPublisher.publishWanReplicationUpdate(mapName, entryView);
                }
            }
        }
    }

    private MapServiceContext getMapServiceContext() {
        final MapService mapService = getService();
        return mapService.getMapServiceContext();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }

    @Override
    public String toString() {
        return "PartitionWideEntryOperation{}";
    }

    public boolean shouldBackup() {
        return entryProcessor.getBackupProcessor() != null;
    }

    public int getSyncBackupCount() {
        return 0;
    }

    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new PartitionWideEntryBackupOperation(name, backupProcessor) : null;
    }

    private long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }

}
