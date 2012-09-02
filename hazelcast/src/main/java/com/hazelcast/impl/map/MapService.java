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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MapService implements ServiceLifecycle, TransactionalService {
    public final static String MAP_SERVICE_NAME = "hz:mapService";

    private final AtomicLong counter = new AtomicLong(new Random().nextLong());
    private final PartitionContainer[] partitionContainers;
    private final NodeService nodeService;
    private final ConcurrentMap<Long, BlockingQueue<Boolean>> backupCalls = new ConcurrentHashMap<Long, BlockingQueue<Boolean>>(1000);

    public MapService(final NodeService nodeService, PartitionInfo[] partitions) {
        this.nodeService = nodeService;
        int partitionCount = nodeService.getPartitionCount();
        partitionContainers = new PartitionContainer[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer(nodeService.getConfig(), this, partitions[i]);
        }
        nodeService.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                List<Integer> ownedPartitions = new ArrayList<Integer>();
                for (int i = 0; i < partitionContainers.length; i++) {
                    if (nodeService.getThisAddress().equals(nodeService.getPartitionInfo(i).getOwner())) {
                        ownedPartitions.add(i);
                    }
                }
                final CountDownLatch latch = new CountDownLatch(ownedPartitions.size());
                for (Integer partitionId : ownedPartitions) {
                    Operation op = new AbstractOperation() {
                        public void run() {
                            try {
                                getPartitionContainer(getPartitionId()).invalidateExpiredScheduledOps();
                            } finally {
                                latch.countDown();
                            }
                        }
                    };
                    op.setPartitionId(partitionId).setValidateTarget(false);
                    nodeService.runLocally(op);
                    try {
                        latch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
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
        ServiceMigrationOperation op = new MapMigrationOperation(container, partitionId, replicaIndex, diffOnly);
        op.setServiceName(MAP_SERVICE_NAME);
        return op;
    }

    public long createNewBackupCallQueue() {
        long backupCallId = nextId();
        backupCalls.put(backupCallId, new LinkedBlockingQueue<Boolean>());
        return backupCallId;
    }

    public BlockingQueue getBackupCallQueue(long backupCallId) {
        return backupCalls.get(backupCallId);
    }

    public void removeBackupCallQueue(long backupCallId) {
        backupCalls.remove(backupCallId);
    }

    public void prepare(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeService.getThisAddress() + " MapService prepare " + txnId);
        PartitionContainer pc = partitionContainers[partitionId];
        TransactionLog txnLog = pc.getTransactionLog(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeService.takeBackups(MAP_SERVICE_NAME, new MapTxnBackupPrepareOperation(txnLog), partitionId,
                    maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    public void commit(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeService.getThisAddress() + " MapService commit " + txnId);
        getPartitionContainer(partitionId).commit(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeService.takeBackups(MAP_SERVICE_NAME, new MapTxnBackupCommitOperation(txnId), partitionId,
                    maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    public void rollback(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeService.getThisAddress() + " MapService commit " + txnId);
        getPartitionContainer(partitionId).rollback(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeService.takeBackups(MAP_SERVICE_NAME, new MapTxnBackupRollbackOperation(txnId), partitionId,
                    maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    public NodeService getNodeService() {
        return nodeService;
    }
}
