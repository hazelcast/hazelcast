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
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;

import static com.hazelcast.nio.IOUtil.toObject;

public abstract class BaseRemoveOperation extends LockAwareOperation implements BackupAwareOperation {
    Object key;
    Record record;

    Data dataOldValue;
    PartitionContainer pc;
    ResponseHandler responseHandler;
    RecordStore recordStore;
    MapService mapService;
    NodeEngine nodeEngine;


    public BaseRemoveOperation(String name, Data dataKey, String txnId) {
        super(name, dataKey);
        setTxnId(txnId);
    }

    public BaseRemoveOperation() {
    }

    protected boolean prepareTransaction() {
        if (txnId != null) {
            pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, null, false, true));
            responseHandler.sendResponse(null);
            return true;
        }
        return false;
    }

    protected void init() {
        responseHandler = getResponseHandler();
        mapService = (MapService) getService();
        nodeEngine = (NodeEngine) getNodeEngine();
        pc = mapService.getPartitionContainer(getPartitionId());
        recordStore = pc.getRecordStore(name);
    }

    public void beforeRun() {
        init();
    }

    public abstract void doOp();

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey);
    }

    public int getAsyncBackupCount() {
        return mapService.getMapInfo(name).getAsyncBackupCount();
    }

    public int getSyncBackupCount() {
        return mapService.getMapInfo(name).getBackupCount();
    }

    public boolean shouldBackup() {
        return true;
    }

    public void afterRun() {
        int eventType = EntryEvent.TYPE_REMOVED;
        mapService.publishEvent(getCaller(), name, eventType, dataKey, dataOldValue, null);
    }

    @Override
    public String toString() {
        return "BaseRemoveOperation{" + name + "}";
    }
}
