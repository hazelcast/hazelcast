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
import java.util.Map;

/**
 * GOTCHA : This operation does not load missing keys from mapstore for now.
 */
public class PartitionWideEntryOperation extends AbstractMapOperation
        implements BackupAwareOperation, PartitionAwareOperation {

    private static final EntryEventType __NO_NEED_TO_FIRE_EVENT = null;
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
        response = new MapEntrySet();
        MapEntrySimple entry;
        final RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        final LocalMapStatsImpl mapStats = mapService.getLocalMapStatsImpl(name);
        final Map<Data, Record> records = recordStore.getReadonlyRecordMap();
        for (final Map.Entry<Data, Record> recordEntry : records.entrySet()) {
            final long start = System.currentTimeMillis();
            final Data dataKey = recordEntry.getKey();
            final Record record = recordEntry.getValue();
            final Object valueBeforeProcess = record.getValue();
            final Object valueBeforeProcessObject = mapService.toObject(valueBeforeProcess);
            Object objectKey = mapService.toObject(record.getKey());
            if (getPredicate() != null) {
                final SerializationService ss = getNodeEngine().getSerializationService();
                QueryEntry queryEntry = new QueryEntry(ss, dataKey, objectKey, valueBeforeProcessObject);
                if (!getPredicate().apply(queryEntry)) {
                    continue;
                }
            }
            entry = new MapEntrySimple(objectKey, valueBeforeProcessObject);
            final Object result = entryProcessor.process(entry);
            final Object valueAfterProcess = entry.getValue();
            Data dataValue = null;
            if (result != null) {
                dataValue = mapService.toData(result);
                response.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(dataKey, dataValue));
            }

            EntryEventType eventType;
            if (valueAfterProcess == null) {
                recordStore.remove(dataKey);
                mapStats.incrementRemoves(getLatencyFrom(start));
                eventType = EntryEventType.REMOVED;
            } else {
                if (valueBeforeProcessObject == null) {
                    mapStats.incrementPuts(getLatencyFrom(start));
                    eventType = EntryEventType.ADDED;
                }
                // take this case as a read so no need to fire an event.
                else if (!entry.isModified()) {
                    mapStats.incrementGets(getLatencyFrom(start));
                    eventType = __NO_NEED_TO_FIRE_EVENT;
                } else {
                    mapStats.incrementPuts(getLatencyFrom(start));
                    eventType = EntryEventType.UPDATED;
                }
                // todo if this is a read only operation, record access operations should be done.
                if (eventType != __NO_NEED_TO_FIRE_EVENT) {
                    recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, valueAfterProcess));
                }
            }
            if (eventType != __NO_NEED_TO_FIRE_EVENT) {
                final Data oldValue = mapService.toData(valueBeforeProcess);
                final Data value = mapService.toData(valueAfterProcess);
                mapService.publishEvent(getCallerAddress(), name, eventType, dataKey, oldValue, value);
                if (mapService.isNearCacheAndInvalidationEnabled(name)) {
                    mapService.invalidateAllNearCaches(name, dataKey);
                }
                if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
                    if (EntryEventType.REMOVED.equals(eventType)) {
                        mapService.publishWanReplicationRemove(name, dataKey, Clock.currentTimeMillis());
                    } else {
                        Record r = recordStore.getRecord(dataKey);
                        final EntryView entryView = EntryViews.createSimpleEntryView(dataKey, dataValue, r);
                        mapService.publishWanReplicationUpdate(name, entryView);
                    }
                }
            }

        }
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
