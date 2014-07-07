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
import com.hazelcast.map.MapEntrySimple;
import com.hazelcast.map.MapEventPublisher;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.record.Record;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.AbstractMap;

/**
 * GOTCHA : This operation loads missing keys from mapstore, in contrast with PartitionWideEntryOperation.
 */
public class EntryOperation extends LockAwareOperation implements BackupAwareOperation {

    private static final EntryEventType NO_NEED_TO_FIRE_EVENT = null;

    protected Object oldValue;
    private EntryProcessor entryProcessor;
    private EntryEventType eventType;
    private Object response;


    public EntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    public EntryOperation() {
    }

    public void innerBeforeRun() {
        final ManagedContext managedContext = getNodeEngine().getSerializationService().getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    public void run() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final long start = System.currentTimeMillis();
        oldValue = recordStore.getMapEntry(dataKey).getValue();
        final LocalMapStatsImpl mapStats
                = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(name);
        final Object valueBeforeProcess = mapServiceContext.toObject(oldValue);
        final MapEntrySimple entry = new MapEntrySimple(mapServiceContext.toObject(dataKey), valueBeforeProcess);
        response = mapServiceContext.toData(entryProcessor.process(entry));
        final Object valueAfterProcess = entry.getValue();
        // no matching data by key.
        if (oldValue == null && valueAfterProcess == null) {
            eventType = NO_NEED_TO_FIRE_EVENT;
        } else if (valueAfterProcess == null) {
            recordStore.remove(dataKey);
            mapStats.incrementRemoves(getLatencyFrom(start));
            eventType = EntryEventType.REMOVED;
        } else {
            if (oldValue == null) {
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
            if (eventType != NO_NEED_TO_FIRE_EVENT) {
                recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, entry.getValue()));
                dataValue = mapServiceContext.toData(entry.getValue());
            }
        }
    }


    public void afterRun() throws Exception {
        super.afterRun();
        if (eventType == NO_NEED_TO_FIRE_EVENT) {
            return;
        }
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        final Data oldValueAsData = mapServiceContext.toData(oldValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, oldValueAsData, dataValue);
        invalidateNearCaches();
        if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
            if (EntryEventType.REMOVED.equals(eventType)) {
                mapEventPublisher.publishWanReplicationRemove(name, dataKey, Clock.currentTimeMillis());
            } else {
                Record record = recordStore.getRecord(dataKey);
                final Data dataValueAsData = mapServiceContext.toData(dataValue);
                final EntryView entryView = EntryViews.createSimpleEntryView(dataKey, dataValueAsData, record);
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
            }
        }

    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
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
    public Object getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "EntryOperation{}";
    }

    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new EntryBackupOperation(name, dataKey, backupProcessor) : null;
    }

    public boolean shouldBackup() {
        return entryProcessor.getBackupProcessor() != null;
    }

    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    private long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }
}
