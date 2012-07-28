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
import com.hazelcast.impl.spi.NodeService;
import com.hazelcast.impl.spi.TransactionException;
import com.hazelcast.impl.spi.TransactionalService;

import java.util.concurrent.atomic.AtomicLong;

public class MapService implements TransactionalService {
    public final static String MAP_SERVICE_NAME = "mapService";

    private final AtomicLong counter = new AtomicLong();
    private final PartitionContainer[] partitionContainers;
    private final NodeService nodeService;

    public MapService(NodeService nodeService, PartitionInfo[] partitions) {
        this.nodeService = nodeService;
        int partitionCount = nodeService.getNode().groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
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

    public void prepare(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeService.getThisAddress() + " MapService prepare " + txnId);
        PartitionContainer pc = partitionContainers[partitionId];
        TransactionLog txnLog = pc.getTransactionLog(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeService.takeBackups(new MapTxnBackupPrepareOperation(txnLog), partitionId, maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    public void commit(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeService.getThisAddress() + " MapService commit " + txnId);
        getPartitionContainer(partitionId).commit(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeService.takeBackups(new MapTxnBackupCommitOperation(txnId), partitionId, maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    public void rollback(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeService.getThisAddress() + " MapService commit " + txnId);
        getPartitionContainer(partitionId).rollback(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeService.takeBackups(new MapTxnBackupRollbackOperation(txnId), partitionId, maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    public NodeService getNodeService() {
        return nodeService;
    }
}
