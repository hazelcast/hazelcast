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
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import static com.hazelcast.map.impl.record.Records.buildRecordInfo;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_MAX_IDLE;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

public abstract class BasePutOperation extends LockAwareOperation implements BackupAwareOperation {

    protected transient Data dataOldValue;
    protected transient Data dataMergingValue;
    protected transient EntryEventType eventType;
    protected transient boolean putTransient;

    public BasePutOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value, DEFAULT_TTL, DEFAULT_MAX_IDLE);
    }

    public BasePutOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        super(name, dataKey, value, ttl, maxIdle);
    }

    public BasePutOperation() {
    }

    @Override
    public void afterRun() {
        mapServiceContext.interceptAfterPut(name, dataValue);
        Record record = recordStore.getRecord(dataKey);
        Object value = isPostProcessing(recordStore) ? record.getValue() : dataValue;
        mapEventPublisher.publishEvent(getCallerAddress(), name, getEventType(),
                dataKey, dataOldValue, value, dataMergingValue);
        invalidateNearCache(dataKey);
        publishWanUpdate(dataKey, value);
        evict(dataKey);
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
        Record record = recordStore.getRecord(dataKey);
        RecordInfo replicationInfo = buildRecordInfo(record);
        if (isPostProcessing(recordStore)) {
            dataValue = mapServiceContext.toData(record.getValue());
        }
        return new PutBackupOperation(name, dataKey, dataValue, replicationInfo, shouldUnlockKeyOnBackup(),
                putTransient, !canThisOpGenerateWANEvent());
    }

    protected boolean shouldUnlockKeyOnBackup() {
        return false;
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
