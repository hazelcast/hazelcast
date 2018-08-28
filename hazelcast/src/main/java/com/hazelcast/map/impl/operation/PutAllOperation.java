/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.record.Records.buildRecordInfo;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_MAX_IDLE;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

/**
 * Inserts the {@link MapEntries} for a single partition to the local {@link com.hazelcast.map.impl.recordstore.RecordStore}.
 * <p>
 * Used to reduce the number of remote invocations of an {@link com.hazelcast.core.IMap#putAll(Map)} call.
 */
public class PutAllOperation extends MapOperation
        implements PartitionAwareOperation, BackupAwareOperation, MutatingOperation {

    private MapEntries mapEntries;

    private boolean hasMapListener;
    private boolean hasWanReplication;
    private boolean hasBackups;
    private boolean hasInvalidation;

    private List<RecordInfo> backupRecordInfos;
    private List<Data> invalidationKeys;

    public PutAllOperation() {
    }

    public PutAllOperation(String name, MapEntries mapEntries) {
        super(name);
        this.mapEntries = mapEntries;
    }

    @Override
    public void run() {
        hasMapListener = mapEventPublisher.hasEventListener(name);
        hasWanReplication = mapContainer.isWanReplicationEnabled();
        hasBackups = hasBackups();
        hasInvalidation = mapContainer.hasInvalidationListener();

        if (hasBackups) {
            backupRecordInfos = new ArrayList<RecordInfo>(mapEntries.size());
        }
        if (hasInvalidation) {
            invalidationKeys = new ArrayList<Data>(mapEntries.size());
        }

        for (int i = 0; i < mapEntries.size(); i++) {
            put(mapEntries.getKey(i), mapEntries.getValue(i));
        }
    }

    private boolean hasBackups() {
        return (mapContainer.getTotalBackupCount() > 0);
    }

    private void put(Data dataKey, Data dataValue) {
        Object oldValue = putToRecordStore(dataKey, dataValue);
        dataValue = getValueOrPostProcessedValue(dataKey, dataValue);
        mapServiceContext.interceptAfterPut(name, dataValue);

        if (hasMapListener) {
            EntryEventType eventType = (oldValue == null ? ADDED : UPDATED);
            mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, oldValue, dataValue);
        }

        if (hasWanReplication) {
            publishWanUpdate(dataKey, dataValue);
        }
        if (hasBackups) {
            Record record = recordStore.getRecord(dataKey);
            RecordInfo replicationInfo = buildRecordInfo(record);
            backupRecordInfos.add(replicationInfo);
        }

        evict(dataKey);
        if (hasInvalidation) {
            invalidationKeys.add(dataKey);
        }
    }

    /**
     * The method recordStore.put() tries to fetch the old value from the MapStore,
     * which can lead to a serious performance degradation if loading from MapStore is expensive.
     * We prevent this by calling recordStore.set() if no map listeners are registered.
     */
    private Object putToRecordStore(Data dataKey, Data dataValue) {
        if (hasMapListener) {
            return recordStore.put(dataKey, dataValue, DEFAULT_TTL, DEFAULT_MAX_IDLE);
        }
        recordStore.set(dataKey, dataValue, DEFAULT_TTL, DEFAULT_MAX_IDLE);
        return null;
    }

    @Override
    public void afterRun() throws Exception {
        invalidateNearCache(invalidationKeys);

        super.afterRun();
    }

    private Data getValueOrPostProcessedValue(Data dataKey, Data dataValue) {
        if (!isPostProcessing(recordStore)) {
            return dataValue;
        }
        Record record = recordStore.getRecord(dataKey);
        return mapServiceContext.toData(record.getValue());
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return (hasBackups && !mapEntries.isEmpty());
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
        return new PutAllBackupOperation(name, mapEntries, backupRecordInfos, false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        mapEntries.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = new MapEntries();
        mapEntries.readData(in);
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PUT_ALL;
    }
}
