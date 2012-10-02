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
import com.hazelcast.nio.Data;
import com.hazelcast.spi.ResponseHandler;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class RemoveOperation extends LockAwareOperation {

    public RemoveOperation(String name, Data dataKey, String txnId) {
        super(name, dataKey);
        setTxnId(txnId);
    }

    public RemoveOperation() {
    }

    public void doRun() {
        ResponseHandler responseHandler = getResponseHandler();
        MapService mapService = (MapService) getService();
        int partitionId = getPartitionId();
        PartitionContainer pc = mapService.getPartitionContainer(partitionId);
        MapPartition mapPartition = pc.getMapPartition(name);

        if (txnId != null) {
            pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, null, false, true));
            responseHandler.sendResponse(null);
            return;
        }
        Record record = mapPartition.records.get(dataKey);
        Object key = toObject(dataKey);
        Object oldValue = null;
        Data oldValueData = null;
        if (record == null) {
            if (mapPartition.loader != null) {
                oldValue = mapPartition.loader.load(key);
                oldValueData = toData(oldValue);
            }
        } else {
            oldValueData = record.getValueData();
        }
        mapPartition.records.remove(dataKey);

        if (mapPartition.store != null && mapPartition.writeDelayMillis == 0) {
            mapPartition.store.delete(key);
        }
        int mapBackupCount = mapPartition.getBackupCount();
        int backupCount = Math.min(getClusterSize() - 1, mapBackupCount);
        long version = pc.incrementAndGetVersion();
        if (backupCount > 0) {
            GenericBackupOperation op = new GenericBackupOperation(name, dataKey, dataValue, ttl, version);
            op.setBackupOpType(GenericBackupOperation.BackupOpType.REMOVE);
            op.setFirstCallerId(backupCallId, getCaller());
            getNodeService().sendBackups(MapService.MAP_SERVICE_NAME, op, partitionId, mapBackupCount);
        }

        getResponseHandler().sendResponse(record == null ? null : new UpdateResponse(oldValueData, version, backupCount));
    }

    private int getClusterSize() {
        return getNodeService().getCluster().getMembers().size();
    }

    @Override
    public String toString() {
        return "RemoveOperation{" + name + "}";
    }
}
