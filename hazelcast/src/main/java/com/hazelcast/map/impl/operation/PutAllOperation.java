/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.record.Records.buildRecordInfo;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

public class PutAllOperation extends MapOperation implements PartitionAwareOperation,
        BackupAwareOperation, MutatingOperation {

    private MapEntries mapEntries;
    private boolean initialLoad;
    private List<Map.Entry<Data, Data>> backupEntries;
    private List<RecordInfo> backupRecordInfos;
    private transient RecordStore recordStore;
    private List<Data> invalidationKeys;

    public PutAllOperation() {
    }

    public PutAllOperation(String name, MapEntries mapEntries, boolean initialLoad) {
        super(name);
        this.mapEntries = mapEntries;
        this.initialLoad = initialLoad;
    }

    @Override
    public void run() {
        backupRecordInfos = new ArrayList<RecordInfo>(mapEntries.size());
        backupEntries = new ArrayList<Map.Entry<Data, Data>>(mapEntries.size());
        recordStore = mapServiceContext.getRecordStore(getPartitionId(), name);

        for (Map.Entry<Data, Data> entry : mapEntries) {
            put(entry);
        }
    }

    private boolean put(Map.Entry<Data, Data> entry) {
        Data dataKey = entry.getKey();
        Data dataValue = entry.getValue();

        Object oldValue = null;
        if (initialLoad) {
            recordStore.putFromLoad(dataKey, dataValue, -1);
        } else {
            oldValue = recordStore.put(dataKey, dataValue, DEFAULT_TTL);
        }
        mapServiceContext.interceptAfterPut(name, dataValue);
        EntryEventType eventType = oldValue == null ? ADDED : UPDATED;
        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        dataValue = getValueOrPostProcessedValue(dataKey, dataValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, oldValue, dataValue);

        Record record = recordStore.getRecord(dataKey);

        if (shouldWanReplicate()) {
            EntryView entryView = createSimpleEntryView(dataKey, dataValue, record);
            mapEventPublisher.publishWanReplicationUpdate(name, entryView);
        }
        backupEntries.add(entry);
        RecordInfo replicationInfo = buildRecordInfo(record);
        backupRecordInfos.add(replicationInfo);
        evict();

        addInvalidation(dataKey);

        return true;
    }

    private void addInvalidation(Data dataKey) {
        if (mapContainer.isNearCacheEnabled()) {
            if (invalidationKeys == null) {
                invalidationKeys = new ArrayList<Data>(mapEntries.size());
            }
            invalidationKeys.add(dataKey);
        }
    }

    @Override
    public void afterRun() throws Exception {
        invalidateNearCache(invalidationKeys);

        super.afterRun();
    }

    private Data getValueOrPostProcessedValue(Data dataKey, Data dataValue) {
        if (!recordStore.getMapDataStore().isPostProcessingMapStore()) {
            return dataValue;
        }
        Record record = recordStore.getRecord(dataKey);
        return mapServiceContext.toData(record.getValue());
    }

    private boolean shouldWanReplicate() {
        return mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null;
    }

    protected void evict() {
        final long now = Clock.currentTimeMillis();
        recordStore.evictEntries(now);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return !backupEntries.isEmpty();
    }

    @Override
    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(name, backupEntries, backupRecordInfos);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapEntries);
        out.writeBoolean(initialLoad);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = in.readObject();
        initialLoad = in.readBoolean();
    }
}
