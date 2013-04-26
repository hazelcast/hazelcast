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

package com.hazelcast.map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;

public abstract class BasePutOperation extends LockAwareOperation implements BackupAwareOperation {

    protected transient Data dataOldValue;

    public BasePutOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value, -1);
    }

    public BasePutOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    public BasePutOperation() {
    }

    public void afterRun() {
        mapService.interceptAfterPut(name, dataValue);
        int eventType = dataOldValue == null ? EntryEvent.TYPE_ADDED : EntryEvent.TYPE_UPDATED;
        mapService.publishEvent(getCallerAddress(), name, eventType, dataKey, dataOldValue, dataValue);
        invalidateNearCaches();
        if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
            SimpleEntryView entryView = new SimpleEntryView(dataKey, mapService.toData(dataValue), recordStore.getRecords().get(dataKey));
            mapService.publishWanReplicationUpdate(name, entryView);
        }
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new PutBackupOperation(name, dataKey, dataValue, ttl);
    }

    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    public void onWaitExpire() {
        final ResponseHandler responseHandler = getResponseHandler();
        responseHandler.sendResponse(null);
    }

    @Override
    public String toString() {
        return "BasePutOperation{" + name + "}";
    }
}
