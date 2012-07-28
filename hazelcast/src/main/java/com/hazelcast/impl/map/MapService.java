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

import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.impl.spi.*;
import com.hazelcast.nio.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MapService implements ServiceLifecycle, TransactionalService {
    public final static String MAP_SERVICE_NAME = "hz:mapService";

    private final AtomicLong counter = new AtomicLong();
    private final PartitionContainer[] partitionContainers;
    private final NodeService nodeService;

    public MapService(final NodeService nodeService, PartitionInfo[] partitions) {
        this.nodeService = nodeService;
        int partitionCount = nodeService.getPartitionCount();
        partitionContainers = new PartitionContainer[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer(nodeService.getNode(), this, partitions[i]);
        }
    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public MapPartition getMapPartition(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getMapPartition(mapName);
    }

    public long nextId() {
        return counter.incrementAndGet();
    }

    public ServiceMigrationOperation getMigrationTask(final int partitionId, final int replicaIndex, boolean diffOnly) {
        if (partitionId < 0 || partitionId >= nodeService.getPartitionCount()) {
            return null;
        }
        final PartitionContainer container = partitionContainers[partitionId];
        return new MapMigrationOperation(container, partitionId, replicaIndex, diffOnly);
    }

    public void prepare(String txnId, int partitionId) {
        System.out.println(nodeService.getThisAddress() + " MapService prepare " + txnId);
        PartitionContainer pc = partitionContainers[partitionId];
        TransactionLog txnLog = pc.getTransactionLog(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        takeBackups(new MapTxnBackupPrepareOperation(txnLog), partitionId, maxBackupCount);
    }

    public void commit(String txnId, int partitionId) {
        System.out.println(nodeService.getThisAddress() + " MapService commit " + txnId);
        getPartitionContainer(partitionId).commit(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        takeBackups(new MapTxnBackupCommitOperation(txnId), partitionId, maxBackupCount);
    }

    public void rollback(String txnId, int partitionId) {
        System.out.println(nodeService.getThisAddress() + " MapService commit " + txnId);
        getPartitionContainer(partitionId).rollback(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        takeBackups(new MapTxnBackupRollbackOperation(txnId), partitionId, maxBackupCount);
    }

    private void takeBackups(Operation op, int partitionId, int backupCount) {
        if (backupCount > 0) {
            List<Future> backupOps = new ArrayList<Future>(backupCount);
            PartitionInfo partitionInfo = nodeService.getPartitionInfo(partitionId);
            for (int i = 0; i < backupCount; i++) {
                int replicaIndex = i + 1;
                Address replicaTarget = partitionInfo.getReplicaAddress(replicaIndex);
                if (replicaTarget != null) {
                    if (replicaTarget.equals(nodeService.getThisAddress())) {
                        // Normally shouldn't happen!!
                    } else {
                        try {
                            backupOps.add(nodeService.createSingleInvocation(MapService.MAP_SERVICE_NAME, op, partitionId)
                                    .setReplicaIndex(replicaIndex).build().invoke());
                        } catch (Exception e) {
                            e.printStackTrace();
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
    }

    public NodeService getNodeService() {
        return nodeService;
    }
}
