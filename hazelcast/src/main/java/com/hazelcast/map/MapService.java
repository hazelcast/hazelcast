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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapServiceConfig;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.map.proxy.MapProxy;
import com.hazelcast.map.proxy.ObjectMapProxy;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.MigrationServiceEvent.MigrationEndpoint;
import com.hazelcast.spi.MigrationServiceEvent.MigrationType;
import com.hazelcast.spi.exception.TransactionException;
import com.hazelcast.spi.impl.AbstractOperation;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class MapService implements ManagedService, MigrationAwareService, MembershipAwareService,
        TransactionalService, RemoteService {

    public final static String MAP_SERVICE_NAME = MapServiceConfig.SERVICE_NAME;

    private final ILogger logger;
    private final AtomicLong counter = new AtomicLong(new Random().nextLong());
    private final PartitionContainer[] partitionContainers;
    private final NodeService nodeService;
    private final ConcurrentMap<Long, BlockingQueue<Boolean>> backupCalls = new ConcurrentHashMap<Long, BlockingQueue<Boolean>>(1000);
    private final ConcurrentMap<String, MapProxy> proxies = new ConcurrentHashMap<String, MapProxy>();

    public MapService(final NodeService nodeService) {
        this.nodeService = nodeService;
        this.logger = nodeService.getLogger(MapService.class.getName());
        partitionContainers = new PartitionContainer[nodeService.getPartitionCount()];
    }

    public void init(NodeService nodeService, Properties properties) {
        int partitionCount = nodeService.getPartitionCount();
        final Config config = nodeService.getConfig();
        for (int i = 0; i < partitionCount; i++) {
            PartitionInfo partition = nodeService.getPartitionInfo(i);
            partitionContainers[i] = new PartitionContainer(config, this, partition);
        }

        nodeService.scheduleWithFixedDelay(new CleanupTask(), 1, 1, TimeUnit.SECONDS);
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

    public void beforeMigration(MigrationServiceEvent event) {
        // TODO: what if partition has transactions?
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        if (event.getPartitionId() < 0 || event.getPartitionId() >= nodeService.getPartitionCount()) {
            return null;
        }
        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        return new MapMigrationOperation(container, event.getPartitionId(), event.getReplicaIndex(), false);
    }

    public void commitMigration(MigrationServiceEvent event) {
        logger.log(Level.FINEST, "Committing " + event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE
                && event.getMigrationType() == MigrationType.MOVE) {
            clearPartitionData(event.getPartitionId());
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        logger.log(Level.FINEST, "Rolling back " + event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionData(event.getPartitionId());
        }
    }

    private void clearPartitionData(final int partitionId) {
        logger.log(Level.FINEST, "Clearing partition data -> " + partitionId);
        final PartitionContainer container = partitionContainers[partitionId];
        for (MapPartition mapPartition : container.maps.values()) {
            mapPartition.clear();
        }
        container.maps.clear();
        container.transactions.clear(); // TODO: not sure?
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

    public String getName() {
        return MAP_SERVICE_NAME;
    }

    public MapProxy createProxy(Object... params) {
        final String name = String.valueOf(params[0]);
        if (params.length > 1 && Boolean.TRUE.equals(params[1])) {
            return new DataMapProxy(name, this, nodeService);
        }
        final MapProxy proxy = new ObjectMapProxy(name, this, nodeService);
        final MapProxy currentProxy = proxies.putIfAbsent(name, proxy);
        return currentProxy != null ? currentProxy : proxy;
    }

    public Collection<ServiceProxy> getProxies() {
        return new HashSet<ServiceProxy>(proxies.values());
    }

    public void memberAdded(final MemberImpl member) {

    }

    public void memberRemoved(final MemberImpl member) {
        // submit operations to partition threads to;
        // * release locks
        // * rollback transaction
        // * do not know ?
    }

    public void destroy() {

    }

    private class CleanupTask implements Runnable {
        public void run() {
            final List<Integer> ownedPartitions = new ArrayList<Integer>();
            for (int i = 0; i < partitionContainers.length; i++) {
                final PartitionInfo partitionInfo = nodeService.getPartitionInfo(i);
                if (partitionInfo != null && nodeService.getThisAddress().equals(partitionInfo.getOwner())) {
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
            }
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }
}
