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

public abstract class BasePutOperation extends LockAwareOperation implements BackupAwareOperation {

    private transient PartitionContainer pc;
    protected transient Data dataOldValue;
    protected transient RecordStore recordStore;
    protected transient MapService mapService;

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

    protected boolean prepareTransaction() {
        if (txnId != null) {
            pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, dataValue, false, false));
            return true;
        }
        return false;
    }

    public final void beforeRun() {
        mapService = getService();
        pc = mapService.getPartitionContainer(getPartitionId());
        recordStore = pc.getRecordStore(name);
    }

    public final void afterRun() {
        mapService.interceptAfterProcess(name, MapOperationType.PUT, dataKey, dataValue, dataOldValue);
        int eventType = dataOldValue == null ? EntryEvent.TYPE_ADDED : EntryEvent.TYPE_UPDATED;
        mapService.publishEvent(getCaller(), name, eventType, dataKey, dataOldValue, dataValue);
        invalidateNearCaches();
    }

    public final Operation getBackupOperation() {
        return new PutBackupOperation(name, dataKey, dataValue, ttl);
    }

    public final int getAsyncBackupCount() {
        return mapService.getMapContainer(name).getAsyncBackupCount();
    }

    public final int getSyncBackupCount() {
        return mapService.getMapContainer(name).getBackupCount();
    }

    @Override
    public String toString() {
        return "BasePutOperation{" + name + "}";
    }
}
