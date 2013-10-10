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
import com.hazelcast.map.*;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

/**
 * GOTCHA : This operation does not use mapstore data for now. Only uses data already in memory.
 */
public class PartitionWideEntryOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    private static final EntryEventType __NO_NEED_TO_FIRE_EVENT = null;
    EntryProcessor entryProcessor;
    MapEntrySet response;

    public PartitionWideEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    public PartitionWideEntryOperation() {
    }

    public void run() {
        response = new MapEntrySet();
        Map.Entry entry;
        final RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        final Map<Data, Record> records = recordStore.getRecords();
        for (final Map.Entry<Data, Record> recordEntry : records.entrySet()) {
            final Data dataKey = recordEntry.getKey();
            final Record record = recordEntry.getValue();
            final Object valueBeforeProcess = mapService.toObject(record.getValue());
            entry = new AbstractMap.SimpleEntry(mapService.toObject(record.getKey()), valueBeforeProcess);
            final Object result = entryProcessor.process(entry);
            final Object valueAfterProcess = entry.getValue();
            Data dataValue = null;
            if (result != null) {
                dataValue = mapService.toData(result);
                response.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(dataKey, dataValue));

                EntryEventType eventType = null;
                if (valueAfterProcess == null) {
                    recordStore.remove(dataKey);
                    eventType = EntryEventType.REMOVED;
                } else {
                    if (valueBeforeProcess == null) {
                        eventType = EntryEventType.ADDED;
                    }
                    // take this case as a read so no need to fire an event.
                    // so do not fire any event if putting the same value again.
                    else if (mapService.compare(recordStore.getMapContainer().getName(), valueBeforeProcess, valueAfterProcess)) {
                        eventType = __NO_NEED_TO_FIRE_EVENT;
                    } else {
                        eventType = EntryEventType.UPDATED;
                    }
                    recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, valueAfterProcess));
                }

                if (eventType != __NO_NEED_TO_FIRE_EVENT) {
                    mapService.publishEvent(getCallerAddress(), name, eventType, dataKey, mapService.toData(record.getValue()), dataValue);
                    if (mapContainer.isNearCacheEnabled()
                            && mapContainer.getMapConfig().getNearCacheConfig().isInvalidateOnChange()) {
                        mapService.invalidateAllNearCaches(name, dataKey);
                    }
                    if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
                        if (EntryEventType.REMOVED.equals(eventType)) {
                            mapService.publishWanReplicationRemove(name, dataKey, Clock.currentTimeMillis());
                        } else {
                            SimpleEntryView entryView = new SimpleEntryView(dataKey, mapService.toData(dataValue), recordStore.getRecords().get(dataKey));
                            mapService.publishWanReplicationUpdate(name, entryView);
                        }
                    }
                }
            }

        }
    }

    public void afterRun() throws Exception {
        super.afterRun();

    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
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
}
