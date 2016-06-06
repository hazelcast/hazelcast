/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

public abstract class BasePutOperation extends LockAwareOperation implements BackupAwareOperation {

    protected transient Data dataOldValue;
    protected transient EntryEventType eventType;
    protected transient boolean putTransient;

    public BasePutOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value, -1);
    }

    public BasePutOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    public BasePutOperation() {
    }

    @Override
    public void afterRun() {
        mapServiceContext.interceptAfterPut(name, dataValue);
        Object value = isPostProcessing(recordStore) ? recordStore.getRecord(dataKey).getValue() : dataValue;

        mapEventPublisher.publishEvent(getCallerAddress(), name, getEventType(), dataKey, dataOldValue, value);
        invalidateNearCache(dataKey);
        publishWANReplicationEvent(mapEventPublisher, value);
        evict(dataKey);
    }

    private void publishWANReplicationEvent(MapEventPublisher mapEventPublisher, Object value) {
        if (!mapContainer.isWanReplicationEnabled()) {
            return;
        }

        Record record = recordStore.getRecord(dataKey);
        if (record == null) {
            return;
        }
        final Data valueConvertedData = mapServiceContext.toData(value);
        final EntryView entryView = EntryViews.createSimpleEntryView(dataKey, valueConvertedData, record);
        mapEventPublisher.publishWanReplicationUpdate(name, entryView);
    }

    private EntryEventType getEventType() {
        if (eventType == null) {
            eventType = dataOldValue == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
        }
        return eventType;
    }

    @Override
    public boolean shouldBackup() {
        Record record = recordStore.getRecord(dataKey);
        return record != null;
    }

    @Override
    public Operation getBackupOperation() {
        final Record record = recordStore.getRecord(dataKey);
        final RecordInfo replicationInfo = buildRecordInfo(record);
        if (isPostProcessing(recordStore)) {
            dataValue = mapServiceContext.toData(record.getValue());
        }
        return new PutBackupOperation(name, dataKey, dataValue, replicationInfo, putTransient);
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
    public void onWaitExpire() {
        sendResponse(null);
    }
}
