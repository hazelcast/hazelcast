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
import com.hazelcast.util.Clock;

public abstract class BaseRemoveOperation extends LockAwareOperation implements BackupAwareOperation {

    private transient PartitionContainer pc;
    protected transient Data dataOldValue;
    protected transient RecordStore recordStore;
    protected transient MapService mapService;
    private transient long startTime;

    public BaseRemoveOperation(String name, Data dataKey, String txnId) {
        super(name, dataKey);
        setTxnId(txnId);
    }

    public BaseRemoveOperation() {
    }

    protected final boolean prepareTransaction() {
        if (txnId != null) {
            pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, null, false, true));
            ResponseHandler responseHandler = getResponseHandler();
            responseHandler.sendResponse(null);
            return true;
        }
        return false;
    }

    public void beforeRun() {
        mapService = getService();
        pc = mapService.getPartitionContainer(getPartitionId());
        recordStore = pc.getRecordStore(name);
    }

    public void run() {
        startTime = Clock.currentTimeMillis();
    }

    public void afterRun() {
        mapService.interceptAfterProcess(name, MapOperationType.REMOVE, dataKey, dataValue, dataOldValue);
        int eventType = EntryEvent.TYPE_REMOVED;
        mapService.publishEvent(getCallerAddress(), name, eventType, dataKey, dataOldValue, null);
        invalidateNearCaches();
        mapService.getMapContainer(name).getMapOperationCounter().incrementRemoves(Clock.currentTimeMillis() - startTime);
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey);
    }

    public int getAsyncBackupCount() {
        return mapService.getMapContainer(name).getAsyncBackupCount();
    }

    public int getSyncBackupCount() {
        return mapService.getMapContainer(name).getBackupCount();
    }

    public boolean shouldBackup() {
        return true;
    }

    @Override
    public String toString() {
        return "BaseRemoveOperation{" + name + "}";
    }
}
