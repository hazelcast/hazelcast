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
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.impl.spi.NodeService;
import com.hazelcast.impl.spi.ResponseHandler;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class PutOperation extends BackupAwareOperation {
    Object value;

    public PutOperation(String name, Data dataKey, Object value, String txnId, long ttl) {
        super(name, dataKey, toData(value), ttl);
        setTxnId(txnId);
        this.value = value;
    }

    public PutOperation() {
    }

    public void run() {
        if (dataValue == null) {
            dataValue = toData(value);
        }
        ResponseHandler responseHandler = getResponseHandler();
        MapService mapService = (MapService) getService();
        int partitionId = getPartitionId();
        PartitionContainer pc = mapService.getPartitionContainer(partitionId);
        if (txnId != null) {
            pc.addTransactionLogItem(txnId, new TransactionLogItem(name, dataKey, dataValue, false, false));
            responseHandler.sendResponse(null);
            return;
        }
        MapPartition mapPartition = pc.getMapPartition(name);
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
//        boolean callerBackup = takeBackup();
//        Operation preResponseBackupOp = null;
//        if (callerBackup) {
//            preResponseBackupOp = new PutBackupOperation(name, dataKey, dataValue, ttl, false);
//        }
//        responseHandler.sendResponse(new Response(preResponseBackupOp, oldValueData, false));
        int mapBackupCount = 1;
        int backupCount = Math.min(getNodeService().getClusterImpl().getSize() - 1, mapBackupCount);
        if (backupCount > 0) {
            GenericBackupOperation op = new GenericBackupOperation(name, dataKey, dataValue, ttl);
            op.setBackupOpType(GenericBackupOperation.BackupOpType.PUT);
            op.setFirstCallerId(backupCallId, getCaller());
//            System.out.println("PUT FIRST caller " + getCaller());
            try {
                getNodeService().sendBackups(MapService.MAP_SERVICE_NAME, op, partitionId, mapBackupCount);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        responseHandler.sendResponse(oldValueData);
//        System.out.println(getNodeService().getThisAddress() + "  PUT is complete " + backupCallId);
    }

    private boolean takeBackup() {
        boolean callerBackup = false;
        NodeService nodeService = getNodeService();
        MapService mapService = (MapService) getService();
        MapPartition mapPartition = mapService.getMapPartition(getPartitionId(), name);
        int mapBackupCount = 1;
        int backupCount = Math.min(nodeService.getClusterImpl().getSize() - 1, mapBackupCount);
        if (backupCount > 0) {
            List<Future> backupOps = new ArrayList<Future>(backupCount);
            PartitionInfo partitionInfo = mapPartition.partitionInfo;
            for (int i = 0; i < backupCount; i++) {
                int replicaIndex = i + 1;
                Address replicaTarget = partitionInfo.getReplicaAddress(replicaIndex);
                if (replicaTarget != null) {
                    if (replicaTarget.equals(nodeService.getThisAddress())) {
//                            Normally shouldn't happen!!
//                            PutBackupOperation pbo = new PutBackupOperation(name, dataKey, dataValue, ttl);
//                            pbo.call();
                    } else {
                        if (replicaTarget.equals(getCaller())) {
                            callerBackup = true;
//                                PutBackupOperation pbo = new PutBackupOperation(name, dataKey, dataValue, ttl);
//                                backupOps.add(service.backup(pbo, replicaTarget));
                        } else {
                            PutBackupOperation pbo = new PutBackupOperation(name, dataKey, dataValue, ttl);
                            try {
                                backupOps.add(nodeService.createSingleInvocation(MapService.MAP_SERVICE_NAME, pbo,
                                        partitionInfo.getPartitionId()).setReplicaIndex(replicaIndex).build().invoke());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
            for (Future backupOp : backupOps) {
                try {
                    backupOp.get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                }
            }
        }
        return callerBackup;
    }

    @Override
    public String toString() {
        return "PutOperation{}";
    }
}
