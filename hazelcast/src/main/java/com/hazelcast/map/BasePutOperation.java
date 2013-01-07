/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.impl.Record;
import com.hazelcast.map.GenericBackupOperation.BackupOpType;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.*;

import static com.hazelcast.nio.IOUtil.toObject;

public abstract class BasePutOperation extends LockAwareOperation implements BackupAwareOperation {

    Record record;

    Data dataOldValue;
    PartitionContainer pc;
    ResponseHandler responseHandler;
    RecordStore recordStore;
    MapService mapService;
    NodeEngine nodeEngine;

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
        System.out.println("Put txn " + txnId);
        if (txnId != null) {
            pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, dataValue, false, false));
            return true;
        }
        return false;
    }

    protected void init() {
        responseHandler = getResponseHandler();
        mapService = getService();
        nodeEngine = getNodeEngine();
        pc = mapService.getPartitionContainer(getPartitionId());
        recordStore = pc.getRecordStore(name);
    }

    public void beforeRun() {
        init();
    }

    public void afterRun() {
        int eventType = dataOldValue == null ? EntryEvent.TYPE_ADDED : EntryEvent.TYPE_UPDATED;
        mapService.publishEvent(getCaller(), name, eventType, dataKey, dataOldValue, dataValue);
    }


    public Operation getBackupOperation() {
        return new PutBackupOperation(name, dataKey, dataValue, ttl);
    }

    public int getAsyncBackupCount() {
        return mapService.getMapInfo(name).getAsyncBackupCount();
    }

    public int getSyncBackupCount() {
        return mapService.getMapInfo(name).getBackupCount();
    }

    @Override
    public String toString() {
        return "BasePutOperation{" + name + "}";
    }
}
