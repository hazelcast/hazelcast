/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.map.response.UpdateResponse;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.NodeService;
import com.hazelcast.spi.ResponseHandler;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public abstract class BasePutOperation extends LockAwareOperation {
    Object key;
    Record record;
    int backupCount;
    long version;

    Data oldValueData;
    PartitionContainer pc;
    ResponseHandler responseHandler;
    MapPartition mapPartition;
    MapService mapService;
    NodeService nodeService;

    // put flags: put(), set() and other put related operation implemntations will differ according to these flags
    boolean LOAD_OLD = true;
    boolean STORE = true;
    boolean RETURN_RESPONSE = true;
    boolean RETURN_OLD_VALUE = true;
    boolean SEND_BACKUPS = true;
    boolean TRANSACTION_ENABLED = true;

    public BasePutOperation(String name, Data dataKey, Data value, String txnId, long ttl) {
        super(name, dataKey, value, ttl);
        setTxnId(txnId);
        initFlags();
    }

    public BasePutOperation() {
        initFlags();
    }

    abstract void initFlags();

    protected boolean prepareTransaction() {
        if (TRANSACTION_ENABLED) {
            if (txnId != null) {
                pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, dataValue, false, false));
                if (RETURN_RESPONSE)
                    responseHandler.sendResponse(null);
                return true;
            }
        }
        return false;
    }

    protected void init() {
        responseHandler = getResponseHandler();
        mapService = (MapService) getService();
        nodeService = (NodeService) getNodeService();
        pc = mapService.getPartitionContainer(getPartitionId());
        mapPartition = pc.getMapPartition(name);
    }

    protected void load() {
        if (LOAD_OLD) {
            if (mapPartition.loader != null) {
                key = toObject(dataKey);
                Object oldValue = mapPartition.loader.load(key);
                oldValueData = toData(oldValue);
            }
        }
    }


    protected void store() {
        if (STORE) {
            if (mapPartition.store != null && mapPartition.writeDelayMillis == 0) {
                if (key == null) {
                    key = toObject(dataKey);
                }
                mapPartition.store.store(key, record.getValue());
            }
        }
    }

    protected void sendBackups() {
        int mapBackupCount = mapPartition.getBackupCount();
        backupCount = Math.min(getClusterSize() - 1, mapBackupCount);
        if (SEND_BACKUPS) {
            version = pc.incrementAndGetVersion();
            if (backupCount > 0) {
                GenericBackupOperation op = new GenericBackupOperation(name, dataKey, dataValue, ttl, version);
                op.setBackupOpType(GenericBackupOperation.BackupOpType.PUT);
                op.setFirstCallerId(backupCallId, getCaller());
                nodeService.sendBackups(MapService.MAP_SERVICE_NAME, op, getPartitionId(), mapBackupCount);
            }
        }
    }

    protected void prepareRecord() {
        record = mapPartition.records.get(dataKey);
        if (record == null) {
            load();
            record = new DefaultRecord(getPartitionId(), dataKey, dataValue, -1, -1, mapService.nextId());
            mapPartition.records.put(dataKey, record);
        } else {
            oldValueData = record.getValueData();
            record.setValueData(dataValue);
        }
        record.setActive();
        record.setDirty(true);
    }

    protected void sendResponse() {
        if (RETURN_RESPONSE){
            if (RETURN_OLD_VALUE){
                responseHandler.sendResponse(new UpdateResponse(oldValueData, version, backupCount));
            }
            else {
                responseHandler.sendResponse(new UpdateResponse(null, version, backupCount));
            }
        }
    }


    // run operation is seperated into methods so each method can be overridden to differentiate put implementation
    public void doRun() {
        init();
        if (prepareTransaction()) {
            return;
        }
        prepareRecord();
        store();
        sendBackups();
        sendResponse();
    }

    protected int getClusterSize() {
        return getNodeService().getCluster().getMembers().size();
    }

    @Override
    public String toString() {
        return "BasePutOperation{" + name + "}";
    }
}
