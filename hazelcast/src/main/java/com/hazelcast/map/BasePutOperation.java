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
import com.hazelcast.util.Clock;

public abstract class BasePutOperation extends LockAwareOperation implements BackupAwareOperation {

    private transient PartitionContainer pc;
    protected transient Data dataOldValue;
    protected transient RecordStore recordStore;
    protected transient MapService mapService;
    protected transient MapContainer mapContainer;
    private transient long startTime;

    public BasePutOperation(String name, Data dataKey, Data value, String txnId) {
        super(name, dataKey, value, -1);
        setTxnId(txnId);
    }

    public BasePutOperation(String name, Data dataKey, Data value, String txnId, long ttl) {
        super(name, dataKey, value, ttl);
        setTxnId(txnId);
    }

    public BasePutOperation() {
    }

    protected final boolean prepareTransaction() {
        if (txnId != null) {
            pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, dataValue, false, false));
            return true;
        }
        return false;
    }

    public void beforeRun() {
        mapService = getService();
        pc = mapService.getPartitionContainer(getPartitionId());
        recordStore = pc.getRecordStore(name);
        mapContainer = mapService.getMapContainer(name);
    }

    public void run() {
        startTime = Clock.currentTimeMillis();
    }

    public void afterRun() {
        mapService.interceptAfterProcess(name, MapOperationType.PUT, dataKey, dataValue, dataOldValue);
        int eventType = dataOldValue == null ? EntryEvent.TYPE_ADDED : EntryEvent.TYPE_UPDATED;
        mapService.publishEvent(getCallerAddress(), name, eventType, dataKey, dataOldValue, dataValue);
        invalidateNearCaches();
        mapContainer.getMapOperationCounter().incrementPuts(Clock.currentTimeMillis() - startTime);
    }

    public final Operation getBackupOperation() {
        return new PutBackupOperation(name, dataKey, dataValue, ttl);
    }

    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }

    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public String toString() {
        return "BasePutOperation{" + name + "}";
    }
}
