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

package com.hazelcast.impl.map;

import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.spi.ResponseHandler;
import com.hazelcast.nio.Data;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class PutOperation extends LockAwareOperation {
    Object value;

    public PutOperation(String name, Data dataKey, Object value, String txnId, long ttl) {
        super(name, dataKey, toData(value), ttl);
        setTxnId(txnId);
        this.value = value;
    }

    public PutOperation() {
    }

    public void doRun() {
        ResponseHandler responseHandler = getResponseHandler();
        MapService mapService = (MapService) getService();
        int partitionId = getPartitionId();
        PartitionContainer pc = mapService.getPartitionContainer(partitionId);
        MapPartition mapPartition = pc.getMapPartition(name);
        if (dataValue == null) {
            dataValue = toData(value);
        }
        if (txnId != null) {
            pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, dataValue, false, false));
            responseHandler.sendResponse(null);
            return;
        }
        Record record = mapPartition.records.get(dataKey);
        Object key = null;
        Object oldValue = null;
        Data oldValueData = null;
        if (record == null) {
            if (mapPartition.loader != null) {
                key = toObject(dataKey);
                oldValue = mapPartition.loader.load(key);
                oldValueData = toData(oldValue);
            }
            record = new DefaultRecord(null, partitionId, dataKey, dataValue, -1, -1, mapService.nextId());
            mapPartition.records.put(dataKey, record);
        } else {
            oldValueData = record.getValueData();
            record.setValueData(dataValue);
        }
        record.setActive();
        record.setDirty(true);
        if (mapPartition.store != null && mapPartition.writeDelayMillis == 0) {
            if (key == null) {
                key = toObject(dataKey);
            }
            mapPartition.store.store(key, record.getValue());
        }
        int mapBackupCount = mapPartition.getBackupCount();
        int backupCount = Math.min(getClusterSize() - 1, mapBackupCount);
        long version = pc.incrementAndGetVersion();
        if (backupCount > 0) {
            GenericBackupOperation op = new GenericBackupOperation(name, dataKey, dataValue, ttl, version);
            op.setBackupOpType(GenericBackupOperation.BackupOpType.PUT);
            op.setFirstCallerId(backupCallId, getCaller());
            getNodeService().sendBackups(MapService.MAP_SERVICE_NAME, op, partitionId, mapBackupCount);
        }
        responseHandler.sendResponse(new UpdateResponse(oldValueData, version, backupCount));
    }

    private int getClusterSize() {
        return getNodeService().getCluster().getMembers().size();
    }

    @Override
    public String toString() {
        return "PutOperation{" + name + "}";
    }
}
