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
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.SimpleEntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class PutAllOperation extends AbstractMapOperation implements PartitionAwareOperation, BackupAwareOperation {

    private MapEntrySet entrySet;
    private boolean initialLoad = false;
    private transient  MapEntrySet backupEntrySet;

    public PutAllOperation(String name, MapEntrySet entrySet) {
        super(name);
        this.entrySet = entrySet;
    }

    public PutAllOperation(String name, MapEntrySet entrySet, boolean initialLoad) {
        super(name);
        this.entrySet = entrySet;
        this.initialLoad = initialLoad;
    }

    public PutAllOperation() {
    }

    public void run() {
        backupEntrySet = new MapEntrySet();
        int partitionId = getPartitionId();
        RecordStore recordStore = mapService.getRecordStore(partitionId, name);
        Set<Map.Entry<Data, Data>> entries = entrySet.getEntrySet();

        PartitionService partitionService = getNodeEngine().getPartitionService();
        for (Map.Entry<Data, Data> entry : entries) {
            Data dataKey = entry.getKey();
            Data dataValue = entry.getValue();
            if (partitionId == partitionService.getPartitionId(dataKey)) {
                Data dataOldValue = null;
                if(initialLoad) {
                    recordStore.putFromLoad(dataKey, dataValue, -1);
                }
                else {
                    dataOldValue = mapService.toData(recordStore.put(dataKey, dataValue, -1));
                }
                mapService.interceptAfterPut(name, dataValue);
                EntryEventType eventType = dataOldValue == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
                mapService.publishEvent(getCallerAddress(), name, eventType, dataKey, dataOldValue, dataValue);
                invalidateNearCaches(dataKey);
                if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
                    SimpleEntryView entryView = new SimpleEntryView(dataKey, mapService.toData(dataValue), recordStore.getRecords().get(dataKey));
                    mapService.publishWanReplicationUpdate(name, entryView);
                }
                backupEntrySet.add(entry);
            }
        }
    }

    // todo optimize below, invalidate method should get the set of keys
    protected final void invalidateNearCaches(Data key) {
        if (mapContainer.isNearCacheEnabled()
                && mapContainer.getMapConfig().getNearCacheConfig().isInvalidateOnChange()) {
            mapService.invalidateAllNearCaches(name, key);
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public String toString() {
        return "PutAllOperation{" +
                '}';
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entrySet);
        out.writeBoolean(initialLoad);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entrySet = in.readObject();
        initialLoad = in.readBoolean();
    }

    @Override
    public boolean shouldBackup() {
        return !backupEntrySet.getEntrySet().isEmpty();
    }

    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(name, backupEntrySet);
    }
}
