/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.operation.ReplicaSyncRequest;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.partition.InternalPartitionService.DEFAULT_REPLICA_SYNC_DELAY;
import static com.hazelcast.internal.partition.InternalPartitionService.REPLICA_SYNC_RETRY_DELAY;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;

/**
 *
 * Maintains the version values for the partition replicas and manages the replica-related operations for partitions
 *
 */
public class PartitionReplicaManager {

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final InternalPartitionServiceImpl partitionService;
    private final PartitionStateManager partitionStateManager;

    private final PartitionReplicaVersions[] replicaVersions;
    /** Replica sync requests that have been sent to the target and awaiting response */
    private final AtomicReferenceArray<ReplicaSyncInfo> replicaSyncRequests;
    private final EntryTaskScheduler<Integer, ReplicaSyncInfo> replicaSyncScheduler;
    @Probe
    private final Semaphore replicaSyncProcessLock;
    @Probe
    private final MwCounter replicaSyncRequestsCounter = newMwCounter();

    private final long partitionMigrationTimeout;
    private final int partitionCount;
    private final int maxParallelReplications;

    PartitionReplicaManager(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(getClass());
        this.partitionService = partitionService;

        partitionCount = partitionService.getPartitionCount();
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
        replicaSyncScheduler = EntryTaskSchedulerFactory.newScheduler(globalScheduler,
                new ReplicaSyncEntryProcessor(), ScheduleType.POSTPONE);

        replicaSyncRequests = new AtomicReferenceArray<ReplicaSyncInfo>(partitionCount);
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
     * @param replicaIndex the index of the replica which is being synchronized
     * @param delayMillis  the delay before the sync operation is sent
     * @throws IllegalArgumentException if the replica index is not between 0 and {@link InternalPartition#MAX_REPLICA_COUNT}
     */
    public void triggerPartitionReplicaSync(int partitionId, int replicaIndex, long delayMillis) {
        if (replicaIndex < 0 || replicaIndex > InternalPartition.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Invalid replica index! replicaIndex=" + replicaIndex
                    + " for partitionId=" + partitionId);
        }

        if (!checkSyncPartitionTarget(partitionId, replicaIndex)) {
            return;
        }

        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        Address target = partition.getOwnerOrNull();
        ReplicaSyncInfo syncInfo = new ReplicaSyncInfo(partitionId, replicaIndex, target);

        if (delayMillis > 0) {
            schedulePartitionReplicaSync(syncInfo, target, delayMillis, "EXPLICIT DELAY");
            return;
        }

        // mdogan:
        // merged two conditions into single `if-return` block to
        // conform checkstyle return-count rule.
        if (!partitionService.isReplicaSyncAllowed() || partition.isMigrating()) {

            schedulePartitionReplicaSync(syncInfo, target, REPLICA_SYNC_RETRY_DELAY,
                    "MIGRATION IS DISABLED OR PARTITION IS MIGRATING");
            return;
        }

        if (replicaSyncRequests.compareAndSet(partitionId, null, syncInfo)) {
            if (fireSyncReplicaRequest(syncInfo, target)) {
                return;
            }

            replicaSyncRequests.compareAndSet(partitionId, syncInfo, null);
            schedulePartitionReplicaSync(syncInfo, target, REPLICA_SYNC_RETRY_DELAY, "NO PERMIT AVAILABLE");
            return;
        }

        long scheduleDelay = getReplicaSyncScheduleDelay(partitionId);
        schedulePartitionReplicaSync(syncInfo, target, scheduleDelay, "ANOTHER SYNC IN PROGRESS");
    }

    /** Checks preconditions for replica sync - if we don't know the owner yet, if this node is the owner or not a replica */
    boolean checkSyncPartitionTarget(int partitionId, int replicaIndex) {
        final InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        final Address target = partition.getOwnerOrNull();
        if (target == null) {
            logger.info("Sync replica target is null, no need to sync -> partitionId=" + partitionId + ", replicaIndex="
                    + replicaIndex);
            return false;
        }

        Address thisAddress = nodeEngine.getThisAddress();
        if (target.equals(thisAddress)) {
            if (logger.isFinestEnabled()) {
                logger.finest("This node is now owner of partition, cannot sync replica -> partitionId=" + partitionId
                        + ", replicaIndex=" + replicaIndex + ", partition-info="
                        + partitionStateManager.getPartitionImpl(partitionId));
            }
            return false;
        }

        if (!partition.isOwnerOrBackup(thisAddress)) {
            if (logger.isFinestEnabled()) {
                logger.finest("This node is not backup replica of partitionId=" + partitionId
                        + ", replicaIndex=" + replicaIndex + " anymore.");
            }
            return false;
        }
        return true;
    }

    /** Returns the delay in ms for the sync operation. Clears the sync request if a different replica is being synchronized */
    private long getReplicaSyncScheduleDelay(int partitionId) {
        long scheduleDelay = DEFAULT_REPLICA_SYNC_DELAY;
        Address thisAddress = node.getThisAddress();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        ReplicaSyncInfo currentSyncInfo = replicaSyncRequests.get(partitionId);
        if (currentSyncInfo != null
                && !thisAddress.equals(partition.getReplicaAddress(currentSyncInfo.replicaIndex))) {
            clearReplicaSyncRequest(partitionId, currentSyncInfo.replicaIndex);
            scheduleDelay = REPLICA_SYNC_RETRY_DELAY;
        }
        return scheduleDelay;
    }

    /**
     * Send the sync request to {@code target} if the max number of parallel sync requests has not been made and the target
     * was not removed while the cluster was not active. Also cancel any currently scheduled sync requests for the given
     * partition and schedule a new sync request that is to be run in the case of timeout
     */
    private boolean fireSyncReplicaRequest(ReplicaSyncInfo syncInfo, Address target) {
        if (node.clusterService.isMemberRemovedInNotJoinableState(target)) {
            return false;
        }

        if (tryToAcquireReplicaSyncPermit()) {
            int partitionId = syncInfo.partitionId;
            int replicaIndex = syncInfo.replicaIndex;
            replicaSyncScheduler.cancel(partitionId);

            if (logger.isFinestEnabled()) {
                logger.finest("Sending sync replica request to -> " + target + "; for partitionId=" + partitionId
                        + ", replicaIndex=" + replicaIndex);
            }
            replicaSyncRequestsCounter.inc();
            replicaSyncScheduler.schedule(partitionMigrationTimeout, partitionId, syncInfo);
            ReplicaSyncRequest syncRequest = new ReplicaSyncRequest(partitionId, replicaIndex);
            nodeEngine.getOperationService().send(syncRequest, target);
            return true;
        }
        return false;
    }

    /** Schedule replica sync request {@code syncInfo} for {@code target} in {@code delayMillis} because of {@code reason} */
    private void schedulePartitionReplicaSync(ReplicaSyncInfo syncInfo, Address target, long delayMillis, String reason) {
        int partitionId = syncInfo.partitionId;
        int replicaIndex = syncInfo.replicaIndex;

        if (logger.isFinestEnabled()) {
            logger.finest(
                    "Scheduling [" + delayMillis + "ms] sync replica request to -> " + target + "; for partitionId=" + partitionId
                            + ", replicaIndex=" + replicaIndex + ". Reason: [" + reason + "]");
        }
        replicaSyncScheduler.schedule(delayMillis, partitionId, syncInfo);
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    long[] incrementPartitionReplicaVersions(int partitionId, int backupCount) {
        PartitionReplicaVersions replicaVersion = replicaVersions[partitionId];
        return replicaVersion.incrementAndGet(backupCount);
    }

    // called in operation threads
    void updatePartitionReplicaVersions(int partitionId, long[] versions, int replicaIndex) {
        PartitionReplicaVersions partitionVersion = replicaVersions[partitionId];
        if (!partitionVersion.update(versions, replicaIndex)) {
            // this partition backup is behind the owner or dirty.
            triggerPartitionReplicaSync(partitionId, replicaIndex, 0L);
        }
    }

    // called in operation threads
    public boolean isPartitionReplicaVersionStale(int partitionId, long[] versions, int replicaIndex) {
        PartitionReplicaVersions partitionVersion = replicaVersions[partitionId];
        return partitionVersion.isStale(versions, replicaIndex);
    }

    // called in operation threads
    public boolean isPartitionReplicaVersionDirty(int partitionId) {
        PartitionReplicaVersions partitionVersion = replicaVersions[partitionId];
        return partitionVersion.isDirty();
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    public long[] getPartitionReplicaVersions(int partitionId) {
        return replicaVersions[partitionId].get();
    }

    // called in operation threads
    public void setPartitionReplicaVersions(int partitionId, long[] versions, int replicaOffset) {
        replicaVersions[partitionId].set(versions, replicaOffset);
    }

    // called in operation threads
    public void clearPartitionReplicaVersions(int partitionId) {
        replicaVersions[partitionId].clear();
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
    public void finalizeReplicaSync(int partitionId, int replicaIndex, long[] versions) {
        PartitionReplicaVersions replicaVersion = replicaVersions[partitionId];
        replicaVersion.clear();
        replicaVersion.set(versions, replicaIndex);
        clearReplicaSyncRequest(partitionId, replicaIndex);
    }

    /**
     * Resets the state of the replica synchronization request for the given partition and replica. This will cancel the
     * scheduled synchronization, clear the ongoing sync flag and release a synchronization permit.
     *
     * @param partitionId  the partition being synchronized
     * @param replicaIndex the index of the replica being synchronized
     */
    // called in operation threads
    public void clearReplicaSyncRequest(int partitionId, int replicaIndex) {
        ReplicaSyncInfo syncInfo = new ReplicaSyncInfo(partitionId, replicaIndex, null);
        ReplicaSyncInfo currentSyncInfo = replicaSyncRequests.get(partitionId);

        replicaSyncScheduler.cancelIfExists(partitionId, syncInfo);

        if (syncInfo.equals(currentSyncInfo)
                && replicaSyncRequests.compareAndSet(partitionId, currentSyncInfo, null)) {

            releaseReplicaSyncPermit();
        } else if (currentSyncInfo != null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Not able to cancel sync! " + syncInfo + " VS Current " + currentSyncInfo);
            }
        }
    }

    void cancelReplicaSyncRequestsTo(Address deadAddress) {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            ReplicaSyncInfo syncInfo = replicaSyncRequests.get(partitionId);
            if (syncInfo != null && deadAddress.equals(syncInfo.target)) {
                cancelReplicaSync(partitionId);
            }
        }
    }

    public void cancelReplicaSync(int partitionId) {
        ReplicaSyncInfo syncInfo = replicaSyncRequests.get(partitionId);
        if (syncInfo != null && replicaSyncRequests.compareAndSet(partitionId, syncInfo, null)) {
            replicaSyncScheduler.cancel(partitionId);
            releaseReplicaSyncPermit();
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
    List<ReplicaSyncInfo> getOngoingReplicaSyncRequests() {
        final int length = replicaSyncRequests.length();
        final List<ReplicaSyncInfo> replicaSyncRequestsList = new ArrayList<ReplicaSyncInfo>(length);
        for (int i = 0; i < length; i++) {
            final ReplicaSyncInfo replicaSyncInfo = replicaSyncRequests.get(i);
            if (replicaSyncInfo != null) {
                replicaSyncRequestsList.add(replicaSyncInfo);
            }
        }

        return replicaSyncRequestsList;
    }

    /**
     * @return copy of scheduled replica-sync requests
     */
    List<ScheduledEntry<Integer, ReplicaSyncInfo>> getScheduledReplicaSyncRequests() {
        final List<ScheduledEntry<Integer, ReplicaSyncInfo>> entries = new ArrayList<ScheduledEntry<Integer, ReplicaSyncInfo>>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final ScheduledEntry<Integer, ReplicaSyncInfo> entry = replicaSyncScheduler.get(partitionId);
            if (entry != null) {
                entries.add(entry);
            }
        }

        return entries;
    }

    void reset() {
        for (int k = 0; k < replicaSyncRequests.length(); k++) {
            replicaSyncRequests.set(k, null);
        }
        replicaSyncScheduler.cancelAll();
        // this is not sync with possibly running sync process
        // permit count can exceed allowed parallelization count.
        replicaSyncProcessLock.drainPermits();
        replicaSyncProcessLock.release(maxParallelReplications);
    }

    void scheduleReplicaVersionSync(ExecutionService executionService) {
        long definedBackupSyncCheckInterval = node.getProperties().getSeconds(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL);
        long backupSyncCheckInterval = definedBackupSyncCheckInterval > 0 ? definedBackupSyncCheckInterval : 1;

        executionService.scheduleWithRepetition(new SyncReplicaVersionTask(),
                backupSyncCheckInterval, backupSyncCheckInterval, TimeUnit.SECONDS);
    }

    private class ReplicaSyncEntryProcessor implements ScheduledEntryProcessor<Integer, ReplicaSyncInfo> {

        @Override
        public void process(EntryTaskScheduler<Integer, ReplicaSyncInfo> scheduler,
                Collection<ScheduledEntry<Integer, ReplicaSyncInfo>> entries) {

            for (ScheduledEntry<Integer, ReplicaSyncInfo> entry : entries) {
                ReplicaSyncInfo syncInfo = entry.getValue();
                int partitionId = syncInfo.partitionId;
                if (replicaSyncRequests.compareAndSet(partitionId, syncInfo, null)) {
                    releaseReplicaSyncPermit();
                }

                InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
                int currentReplicaIndex = partition.getReplicaIndex(node.getThisAddress());
                if (currentReplicaIndex > 0) {
                    triggerPartitionReplicaSync(partitionId, currentReplicaIndex, 0L);
                }
            }
        }
    }

    private class SyncReplicaVersionTask implements Runnable {
        @Override
        public void run() {
            if (!node.nodeEngine.isRunning() || !partitionService.isReplicaSyncAllowed()) {
                return;
            }

            for (InternalPartition partition : partitionStateManager.getPartitions()) {
                if (!partition.isLocal()) {
                    continue;
                }

                for (int index = 1; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                    if (partition.getReplicaAddress(index) != null) {
                        CheckReplicaVersionTask task = new CheckReplicaVersionTask(nodeEngine, partitionService,
                                partition.getPartitionId(), index, null);
                        nodeEngine.getOperationService().execute(task);
                    }
                }
            }
        }
    }
}
