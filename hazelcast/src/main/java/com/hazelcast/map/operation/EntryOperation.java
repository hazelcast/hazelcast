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
import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapEntrySimple;
import com.hazelcast.map.SimpleEntryView;
import com.hazelcast.map.record.Record;
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

    private static final EntryEventType __NO_NEED_TO_FIRE_EVENT = null;
    private EntryProcessor entryProcessor;
    private transient EntryEventType eventType;
    private transient Object response;
    protected transient Object oldValue;


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
        oldValue = recordStore.getMapEntry(dataKey).getValue();
        final Object valueBeforeProcess = mapService.toObject(oldValue);
        final MapEntrySimple entry = new MapEntrySimple(mapService.toObject(dataKey), valueBeforeProcess);
        response = mapService.toData(entryProcessor.process(entry));
        final Object valueAfterProcess = entry.getValue();
        // no matching data by key.
        if (oldValue == null && valueAfterProcess == null) {
            eventType = __NO_NEED_TO_FIRE_EVENT;
        } else if (valueAfterProcess == null) {
            recordStore.remove(dataKey);
            eventType = EntryEventType.REMOVED;
        } else {
            if (oldValue == null) {
                eventType = EntryEventType.ADDED;
            }
            // take this case as a read so no need to fire an event.
            else if (!entry.isModified()) {
                eventType = __NO_NEED_TO_FIRE_EVENT;
            } else {
                eventType = EntryEventType.UPDATED;
            }
            if (eventType != __NO_NEED_TO_FIRE_EVENT) {
                dataValue = mapService.toData(entry.getValue());
                recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, dataValue));
            }
        }
    }


    public void afterRun() throws Exception {
        super.afterRun();
        if (eventType == __NO_NEED_TO_FIRE_EVENT) {
            return;
        }
        mapService.publishEvent(getCallerAddress(), name, eventType, dataKey, mapService.toData(oldValue), dataValue);
        invalidateNearCaches();
        if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
            if (EntryEventType.REMOVED.equals(eventType)) {
                mapService.publishWanReplicationRemove(name, dataKey, Clock.currentTimeMillis());
            } else {
                Record record = recordStore.getRecord(dataKey);
                SimpleEntryView entryView = new SimpleEntryView(dataKey, mapService.toData(dataValue), record.getStatistics(), record.getVersion());
                mapService.publishWanReplicationUpdate(name, entryView);
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

}
