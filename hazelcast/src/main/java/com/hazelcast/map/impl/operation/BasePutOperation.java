/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

public abstract class BasePutOperation
        extends LockAwareOperation implements BackupAwareOperation {

    protected transient Object oldValue;
    protected transient EntryEventType eventType;

    public BasePutOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    public BasePutOperation() {
    }

    @Override
    protected void afterRunInternal() {
        Object value = isPostProcessing(recordStore)
                ? recordStore.getRecord(dataKey).getValue() : dataValue;
        mapServiceContext.interceptAfterPut(mapContainer.getInterceptorRegistry(), dataValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name, getEventType(),
                dataKey, oldValue, value);
        invalidateNearCache(dataKey);
        publishWanUpdate(dataKey, value);
        evict(dataKey);
    }

    private EntryEventType getEventType() {
        if (eventType == null) {
            eventType = oldValue == null
                    ? EntryEventType.ADDED : EntryEventType.UPDATED;
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
        dataValue = getValueOrPostProcessedValue(record, dataValue);
        return newBackupOperation(dataKey, record, dataValue);
    }

    protected PutBackupOperation newBackupOperation(Data dataKey, Record record, Data dataValue) {
        return new PutBackupOperation(name, dataKey, record, dataValue);
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
