/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequest;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static java.util.Collections.newSetFromMap;

/**
 *
 * Maintains the version values for the partition replicas and manages the replica-related operations for partitions
 *
 */
public class PartitionReplicaManager implements PartitionReplicaVersionManager {

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final InternalPartitionServiceImpl partitionService;
    private final PartitionStateManager partitionStateManager;

    private final PartitionReplicaVersions[] replicaVersions;
    /** Replica sync requests that have been sent to the target and awaiting response */
    private final Set<ReplicaFragmentSyncInfo> replicaSyncRequests;
    private final EntryTaskScheduler<ReplicaFragmentSyncInfo, Void> replicaSyncTimeoutScheduler;
    @Probe
    private final Semaphore replicaSyncProcessLock;
    @Probe
    private final MwCounter replicaSyncRequestsCounter = newMwCounter();

    private final long partitionMigrationTimeout;
    private final int maxParallelReplications;

    PartitionReplicaManager(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(getClass());
        this.partitionService = partitionService;

        int partitionCount = partitionService.getPartitionCount();
        partitionStateManager = partitionService.getPartitionStateManager();

        HazelcastProperties properties = node.getProperties();
        partitionMigrationTimeout = properties.getMillis(GroupProperty.PARTITION_MIGRATION_TIMEOUT);
        maxParallelReplications = properties.getInteger(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS);
        replicaSyncProcessLock = new Semaphore(maxParallelReplications);

        replicaVersions = new PartitionReplicaVersions[partitionCount];
        for (int i = 0; i < replicaVersions.length; i++) {
            replicaVersions[i] = new PartitionReplicaVersions(i);
        }

        ExecutionService executionService = nodeEngine.getExecutionService();
        TaskScheduler globalScheduler = executionService.getGlobalTaskScheduler();

        // The reason behind this scheduler to have POSTPONE type is as follows:
        // When a node shifts up in the replica table upon a node failure, it sends a sync request to the partition owner and
        // registers it to the replicaSyncRequests. If another node fails before the already-running sync process completes,
        // the new sync request is simply scheduled to a further time. Again, before the already-running sync process completes,
        // if another node fails for the third time, the already-scheduled sync request should be overwritten with the new one.
        // This is because this node is shifted up to a higher level when the third node failure occurs and its respective sync
        // request will inherently include the backup data that is requested by the previously scheduled sync request.
        replicaSyncTimeoutScheduler = EntryTaskSchedulerFactory.newScheduler(globalScheduler,
                new ReplicaSyncTimeoutProcessor(), ScheduleType.POSTPONE);

        replicaSyncRequests = newSetFromMap(new ConcurrentHashMap<ReplicaFragmentSyncInfo, Boolean>(partitionCount));
    }

    /**
     * This method is called on a backup node (replica). Given all conditions are satisfied, this method initiates a replica sync
     * operation and registers it to replicaSyncRequest. The operation is scheduled for a future execution if :
     * <ul>
     * <li>the {@code delayMillis} is greater than 0</li>
     * <li>if a migration is not allowed (during repartitioning or a node joining the cluster)</li>
     * <li>the partition is currently migrating</li>
     * <li>another sync request has already been sent</li>
     * <li>the maximum number of parallel synchronizations has already been reached</li>
     * </ul>
     *
     * @param partitionId  the partition which is being synchronized
     * @param namespaces namespaces of partition replica fragments
     * @param replicaIndex the index of the replica which is being synchronized
     * @throws IllegalArgumentException if the replica index is not between 0 and {@link InternalPartition#MAX_REPLICA_COUNT}
     */
    public void triggerPartitionReplicaSync(int partitionId, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        assert replicaIndex >= 0 && replicaIndex < InternalPartition.MAX_REPLICA_COUNT
                : "Invalid replica index! partitionId=" + partitionId + ", replicaIndex=" + replicaIndex;

        Address target = checkAndGetPrimaryReplicaOwner(partitionId, replicaIndex);
        if (target == null) {
            return;
        }

        if (!partitionService.isMigrationAllowed()) {
            logger.finest("Cannot send sync replica request for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                    + ", namespaces=" + namespaces + ". Sync is not allowed.");
            return;
        }

        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        if (partition.isMigrating()) {
            logger.finest("Cannot send sync replica request for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                    + ", namespaces=" + namespaces + ". Partition is already migrating.");
            return;
        }

        sendSyncReplicaRequest(partitionId, namespaces, replicaIndex, target);
    }

    /** Checks preconditions for replica sync - if we don't know the owner yet, if this node is the owner or not a replica */
    Address checkAndGetPrimaryReplicaOwner(int partitionId, int replicaIndex) {
        final InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        final Address target = partition.getOwnerOrNull();
        if (target == null) {
            logger.info("Sync replica target is null, no need to sync -> partitionId=" + partitionId + ", replicaIndex="
                    + replicaIndex);
            return null;
        }

        Address thisAddress = nodeEngine.getThisAddress();
        if (target.equals(thisAddress)) {
            if (logger.isFinestEnabled()) {
                logger.finest("This node is now owner of partition, cannot sync replica -> partitionId=" + partitionId
                        + ", replicaIndex=" + replicaIndex + ", partition-info="
                        + partitionStateManager.getPartitionImpl(partitionId));
            }
            return null;
        }

        if (!partition.isOwnerOrBackup(thisAddress)) {
            if (logger.isFinestEnabled()) {
                logger.finest("This node is not backup replica of partitionId=" + partitionId
                        + ", replicaIndex=" + replicaIndex + " anymore.");
            }
            return null;
        }
        return target;
    }

    /**
     * Send the sync request to {@code target} if the max number of parallel sync requests has not been made and the target
     * was not removed while the cluster was not active. Also cancel any currently scheduled sync requests for the given
     * partition and schedule a new sync request that is to be run in the case of timeout
     */
    private void sendSyncReplicaRequest(int partitionId, Collection<ServiceNamespace> syncNamespaces,
            int replicaIndex, Address target) {
        if (node.clusterService.isMemberRemovedInNotJoinableState(target)) {
            return;
        }

        if (!tryToAcquireReplicaSyncPermit()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Cannot send sync replica request for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                        + ", namespaces=" + syncNamespaces + ". No permits available!");
            }
            return;
        }

        Collection<ServiceNamespace> namespaces = registerSyncInfoFor(partitionId, syncNamespaces, replicaIndex, target);
        if (namespaces.isEmpty()) {
            releaseReplicaSyncPermit();
            return;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync replica request for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                    + ", namespaces=" + namespaces);

        }
        replicaSyncRequestsCounter.inc();

        PartitionReplicaSyncRequest syncRequest = new PartitionReplicaSyncRequest(partitionId, namespaces, replicaIndex);
        nodeEngine.getOperationService().send(syncRequest, target);
    }

    private Collection<ServiceNamespace> registerSyncInfoFor(int partitionId,
            Collection<ServiceNamespace> requestedNamespaces, int replicaIndex, Address target) {

        // namespaces arg may not support removal
        Collection<ServiceNamespace> namespaces = new ArrayList<ServiceNamespace>(requestedNamespaces);
        Iterator<ServiceNamespace> iter = namespaces.iterator();
        while (iter.hasNext()) {
            ServiceNamespace namespace = iter.next();
            ReplicaFragmentSyncInfo syncInfo = new ReplicaFragmentSyncInfo(partitionId, namespace, replicaIndex, target);
            if (!replicaSyncRequests.add(syncInfo)) {
                logger.finest("Cannot send sync replica request for " + syncInfo + ". Sync is already in progress!");
                iter.remove();
                continue;
            }

            replicaSyncTimeoutScheduler.schedule(partitionMigrationTimeout, syncInfo, null);
        }
        return namespaces;
    }

    @Override
    public ServiceNamespace getServiceNamespace(Operation operation) {
        if (operation instanceof ServiceNamespaceAware) {
            return ((ServiceNamespaceAware) operation).getServiceNamespace();
        }
        return NonFragmentedServiceNamespace.INSTANCE;
    }

    @Override
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    public long[] incrementPartitionReplicaVersions(int partitionId, ServiceNamespace namespace, int backupCount) {
        PartitionReplicaVersions replicaVersion = replicaVersions[partitionId];
        return replicaVersion.incrementAndGet(namespace, backupCount);
    }

    @Override
    public void updatePartitionReplicaVersions(int partitionId, ServiceNamespace namespace,
                                               long[] versions, int replicaIndex) {
        PartitionReplicaVersions partitionVersion = replicaVersions[partitionId];
        if (!partitionVersion.update(namespace, versions, replicaIndex)) {
            // this partition backup is behind the owner or dirty.
            triggerPartitionReplicaSync(partitionId, Collections.singleton(namespace), replicaIndex);
        }
    }

    @Override
    public boolean isPartitionReplicaVersionStale(int partitionId, ServiceNamespace namespace,
                                                  long[] versions, int replicaIndex) {
        return replicaVersions[partitionId].isStale(namespace, versions, replicaIndex);
    }

    // called in operation threads
    public boolean isPartitionReplicaVersionDirty(int partitionId, ServiceNamespace namespace) {
        return replicaVersions[partitionId].isDirty(namespace);
    }

    @Override
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    public long[] getPartitionReplicaVersions(int partitionId, ServiceNamespace namespace) {
        return replicaVersions[partitionId].get(namespace);
    }

    // called in operation threads
    public void setPartitionReplicaVersions(int partitionId, ServiceNamespace namespace,
                                            long[] versions, int replicaOffset) {
        replicaVersions[partitionId].set(namespace, versions, replicaOffset);
    }

    // called in operation threads
    public void clearPartitionReplicaVersions(int partitionId, ServiceNamespace namespace) {
        replicaVersions[partitionId].clear(namespace);
    }

    /**
     * Set the new replica versions for the partition with the {@code partitionId} and reset any ongoing replica
     * synchronization request for this partition and replica index.
     *
     * @param partitionId the partition ID
     * @param replicaIndex the index of the replica
     * @param versions the new replica versions for the partition
     */
    // called in operation threads
    public void finalizeReplicaSync(int partitionId, int replicaIndex, ServiceNamespace namespace, long[] versions) {
        PartitionReplicaVersions replicaVersion = replicaVersions[partitionId];
        replicaVersion.clear(namespace);
        replicaVersion.set(namespace, versions, replicaIndex);
        clearReplicaSyncRequest(partitionId, namespace, replicaIndex);
    }

    /**
     * Resets the state of the replica synchronization request for the given partition and replica. This will cancel the
     * scheduled synchronization, clear the ongoing sync flag and release a synchronization permit.
     *
     * @param partitionId  the partition being synchronized
     * @param namespace namespace
     * @param replicaIndex the index of the replica being synchronized
     */
    // called in operation threads
    public void clearReplicaSyncRequest(int partitionId, ServiceNamespace namespace, int replicaIndex) {
        ReplicaFragmentSyncInfo syncInfo = new ReplicaFragmentSyncInfo(partitionId, namespace, replicaIndex, null);
        if (!replicaSyncRequests.remove(syncInfo)) {
            return;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Clearing sync replica request for partitionId=" + partitionId + ", replicaIndex="
                    + replicaIndex + ", namespace=" + namespace);
        }
        releaseReplicaSyncPermit();
        replicaSyncTimeoutScheduler.cancelIfExists(syncInfo, null);
    }

    void cancelReplicaSyncRequestsTo(Address deadAddress) {
        Iterator<ReplicaFragmentSyncInfo> iter = replicaSyncRequests.iterator();
        while (iter.hasNext()) {
            ReplicaFragmentSyncInfo syncInfo = iter.next();
            if (deadAddress.equals(syncInfo.target)) {
                iter.remove();
                replicaSyncTimeoutScheduler.cancel(syncInfo);
                releaseReplicaSyncPermit();
            }
        }
    }

    void cancelReplicaSync(int partitionId) {
        Iterator<ReplicaFragmentSyncInfo> iter = replicaSyncRequests.iterator();
        while (iter.hasNext()) {
            ReplicaFragmentSyncInfo syncInfo = iter.next();
            if (syncInfo.partitionId == partitionId) {
                iter.remove();
                replicaSyncTimeoutScheduler.cancel(syncInfo);
                releaseReplicaSyncPermit();
            }
        }
    }

    public boolean tryToAcquireReplicaSyncPermit() {
        return replicaSyncProcessLock.tryAcquire();
    }

    public void releaseReplicaSyncPermit() {
        replicaSyncProcessLock.release();
    }

    /**
     * @return copy of ongoing replica-sync operations
     */
    List<ReplicaFragmentSyncInfo> getOngoingReplicaSyncRequests() {
        return new ArrayList<ReplicaFragmentSyncInfo>(replicaSyncRequests);
    }

    /**
     * @return copy of scheduled replica-sync requests
     */
    List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> getScheduledReplicaSyncRequests() {
        final List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>>
                entries = new ArrayList<ScheduledEntry<ReplicaFragmentSyncInfo, Void>>();
        for (ReplicaFragmentSyncInfo syncInfo : replicaSyncRequests) {
            ScheduledEntry<ReplicaFragmentSyncInfo, Void> entry = replicaSyncTimeoutScheduler.get(syncInfo);
            if (entry != null) {
                entries.add(entry);
            }
        }
        return entries;
    }

    void reset() {
        replicaSyncRequests.clear();
        replicaSyncTimeoutScheduler.cancelAll();
        // this is not sync with possibly running sync process
        // permit count can exceed allowed parallelization count.
        replicaSyncProcessLock.drainPermits();
        replicaSyncProcessLock.release(maxParallelReplications);
    }

    void scheduleReplicaVersionSync(ExecutionService executionService) {
        long definedBackupSyncCheckInterval = node.getProperties().getSeconds(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL);
        long backupSyncCheckInterval = definedBackupSyncCheckInterval > 0 ? definedBackupSyncCheckInterval : 1;

        executionService.scheduleWithRepetition(new AntiEntropyTask(),
                backupSyncCheckInterval, backupSyncCheckInterval, TimeUnit.SECONDS);
    }

    @Override
    public Collection<ServiceNamespace> getNamespaces(int partitionId) {
        return replicaVersions[partitionId].getNamespaces();
    }

    public void retainNamespaces(int partitionId, Set<ServiceNamespace> namespaces) {
        PartitionReplicaVersions versions = replicaVersions[partitionId];
        versions.retainNamespaces(namespaces);
    }

    private class ReplicaSyncTimeoutProcessor implements ScheduledEntryProcessor<ReplicaFragmentSyncInfo, Void> {

        @Override
        public void process(EntryTaskScheduler<ReplicaFragmentSyncInfo, Void> scheduler,
                Collection<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> entries) {

            for (ScheduledEntry<ReplicaFragmentSyncInfo, Void> entry : entries) {
                ReplicaFragmentSyncInfo syncInfo = entry.getKey();
                if (replicaSyncRequests.remove(syncInfo)) {
                    releaseReplicaSyncPermit();
                }
            }
        }
    }

    private class AntiEntropyTask implements Runnable {
        @Override
        public void run() {
            if (!node.isRunning() || !node.getNodeExtension().isStartCompleted()
                    || !partitionService.isMigrationAllowed()) {
                return;
            }
            nodeEngine.getOperationService().executeOnPartitions(new PartitionAntiEntropyTaskFactory(), getLocalPartitions());
        }

        private BitSet getLocalPartitions() {
            BitSet localPartitions = new BitSet(partitionService.getPartitionCount());

            for (InternalPartition partition : partitionService.getInternalPartitions()) {
                if (partition.isLocal()) {
                    localPartitions.set(partition.getPartitionId());
                }
            }
            return localPartitions;
        }
    }

    private class PartitionAntiEntropyTaskFactory implements PartitionTaskFactory<Runnable> {
        @Override
        public Runnable create(int partitionId) {
            return new PartitionPrimaryReplicaAntiEntropyTask(nodeEngine, partitionId);
        }
    }
}
