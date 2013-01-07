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
import com.hazelcast.map.GenericBackupOperation.BackupOpType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;

public class EvictOperation extends LockAwareOperation implements BackupAwareOperation {
    Object key;

    PartitionContainer pc;
    ResponseHandler responseHandler;
    RecordStore recordStore;
    MapService mapService;
    NodeEngine nodeEngine;
    boolean evicted = false;


    public EvictOperation(String name, Data dataKey, String txnId) {
        super(name, dataKey);
        setTxnId(txnId);
    }

    public EvictOperation() {
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

    public void doOp() {
        if (prepareTransaction()) {
            return;
        }
        evicted = recordStore.evict(dataKey);
    }

    @Override
    public Object getResponse() {
        return evicted;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    public Operation getBackupOperation() {
        final GenericBackupOperation op = new GenericBackupOperation(name, dataKey, dataValue, ttl);
        op.setBackupOpType(BackupOpType.REMOVE);
        return op;
    }

    public int getAsyncBackupCount() {
        return mapService.getMapInfo(name).getAsyncBackupCount();
    }

    public int getSyncBackupCount() {
        return mapService.getMapInfo(name).getBackupCount();
    }

    public boolean shouldBackup() {
        return evicted;
    }

    public void remove() {
        recordStore.getRecords().remove(dataKey);
    }


    public void afterRun() {
        int eventType = EntryEvent.TYPE_EVICTED;
        mapService.publishEvent(getCaller(), name, eventType, dataKey, null, dataValue);
    }

    @Override
    public String toString() {
        return "EvictOperation{" + name + "}";
    }
}
