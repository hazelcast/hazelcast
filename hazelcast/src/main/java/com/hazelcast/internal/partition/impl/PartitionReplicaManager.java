/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequest;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequestOffloadable;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.scheduler.EntryTaskScheduler;
import com.hazelcast.internal.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.internal.util.scheduler.ScheduleType;
import com.hazelcast.internal.util.scheduler.ScheduledEntry;
import com.hazelcast.internal.util.scheduler.ScheduledEntryProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_REPLICA_MANAGER_REPLICA_SYNC_SEMAPHORE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_REPLICA_MANAGER_SYNC_REQUEST_COUNTER;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static java.lang.String.format;

/**
 * Maintains the version values for the partition replicas and manages the replica-related operations for partitions
 */
public class PartitionReplicaManager implements PartitionReplicaVersionManager {

    /**
     * Marker that indicates a particular backup replica index requires a sync with the primary replica.
     */
    public static final long REQUIRES_SYNC = -1;

    /**
     * Allow running partition replica sync on generic operation threads? Default is true.
     * System property supplied as a workaround in case of unexpected issues.
     *
     * @since 5.0
     */
    private static final String PARTITION_REPLICA_ALLOW_OFFLOAD = "hazelcast.partition.replica.offload";
    private static final boolean ALLOW_OFFLOAD =
            Boolean.parseBoolean(System.getProperty(PARTITION_REPLICA_ALLOW_OFFLOAD, "true"));

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final InternalPartitionServiceImpl partitionService;
    private final PartitionStateManager partitionStateManager;

    private final PartitionReplicaVersions[] replicaVersions;
    /**
     * Replica sync requests that have been sent to the target and awaiting response
     */
    private final Set<ReplicaFragmentSyncInfo> replicaSyncRequests;
    private final EntryTaskScheduler<ReplicaFragmentSyncInfo, Void> replicaSyncTimeoutScheduler;
    @Probe(name = PARTITIONS_METRIC_PARTITION_REPLICA_MANAGER_REPLICA_SYNC_SEMAPHORE)
    private final Semaphore replicaSyncSemaphore;
    @Probe(name = PARTITIONS_METRIC_PARTITION_REPLICA_MANAGER_SYNC_REQUEST_COUNTER)
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
        partitionMigrationTimeout = properties.getMillis(ClusterProperty.PARTITION_MIGRATION_TIMEOUT);
        maxParallelReplications = properties.getInteger(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS);
        replicaSyncSemaphore = new Semaphore(maxParallelReplications);

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

        replicaSyncRequests = ConcurrentHashMap.newKeySet(partitionCount);
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
     * @param namespaces   namespaces of partition replica fragments
     * @param replicaIndex the index of the replica which is being synchronized
     * @throws IllegalArgumentException if the replica index is not between 0 and {@link InternalPartition#MAX_REPLICA_COUNT}
     */
    public void triggerPartitionReplicaSync(int partitionId, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        assert replicaIndex >= 0 && replicaIndex < InternalPartition.MAX_REPLICA_COUNT
                : "Invalid replica index! partitionId=" + partitionId + ", replicaIndex=" + replicaIndex;

        PartitionReplica target = checkAndGetPrimaryReplicaOwner(partitionId, replicaIndex);
        if (target == null) {
            return;
        }

        if (!partitionService.areMigrationTasksAllowed()) {
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

    /**
     * Checks preconditions for replica sync - if we don't know the owner yet, if this node is the owner or not a replica
     */
    PartitionReplica checkAndGetPrimaryReplicaOwner(int partitionId, int replicaIndex) {
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        PartitionReplica owner = partition.getOwnerReplicaOrNull();
        if (owner == null) {
            logger.info("Sync replica target is null, no need to sync -> partitionId=" + partitionId + ", replicaIndex="
                    + replicaIndex);
            return null;
        }

        PartitionReplica localReplica = PartitionReplica.from(nodeEngine.getLocalMember());
        if (owner.equals(localReplica)) {
            if (logger.isFinestEnabled()) {
                logger.finest("This node is now owner of partition, cannot sync replica -> partitionId=" + partitionId
                        + ", replicaIndex=" + replicaIndex + ", partition-info="
                        + partitionStateManager.getPartitionImpl(partitionId));
            }
            return null;
        }

        if (!partition.isOwnerOrBackup(localReplica)) {
            if (logger.isFinestEnabled()) {
                logger.finest("This node is not backup replica of partitionId=%s, replicaIndex=%s anymore.", partitionId,
                        replicaIndex);
            }
            return null;
        }
        return owner;
    }

    /**
     * Send the sync request to {@code target} if the max number of parallel sync requests has not been made and the target
     * was not removed while the cluster was not active. Also cancel any currently scheduled sync requests for the given
     * partition and schedule a new sync request that is to be run in the case of timeout
     */
    private void sendSyncReplicaRequest(int partitionId, Collection<ServiceNamespace> requestedNamespaces,
                                        int replicaIndex, PartitionReplica target) {
        if (node.clusterService.isMissingMember(target.address(), target.uuid())) {
            return;
        }

        int permits = tryAcquireReplicaSyncPermits(requestedNamespaces.size());
        if (permits == 0) {
            if (logger.isFinestEnabled()) {
                logger.finest("Cannot send sync replica request for partitionId=" + partitionId
                        + ", replicaIndex=" + replicaIndex + ", namespaces=" + requestedNamespaces
                        + ". No permits available!");
            }
            return;
        }

        // Select only permitted number of namespaces
        Collection<ServiceNamespace> namespaces =
                registerSyncInfoForNamespaces(partitionId, requestedNamespaces, replicaIndex, target, permits);

        // release unused permits
        if (namespaces.size() != permits) {
            releaseReplicaSyncPermits(permits - namespaces.size());
        }

        if (namespaces.isEmpty()) {
            return;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync replica request for partitionId="
                    + partitionId + ", replicaIndex=" + replicaIndex
                    + ", namespaces=" + namespaces);
        }
        replicaSyncRequestsCounter.inc();

        Operation syncRequest = ALLOW_OFFLOAD
                ? new PartitionReplicaSyncRequestOffloadable(namespaces, partitionId, replicaIndex)
                : new PartitionReplicaSyncRequest(namespaces, partitionId, replicaIndex);

        nodeEngine.getOperationService().send(syncRequest, target.address());
    }

    private Collection<ServiceNamespace> registerSyncInfoForNamespaces(int partitionId,
                                                                       Collection<ServiceNamespace> requestedNamespaces,
                                                                       int replicaIndex, PartitionReplica target, int permits) {

        List<ServiceNamespace> namespaces = new ArrayList<>(permits);
        for (ServiceNamespace namespace : requestedNamespaces) {
            if (namespaces.size() == permits) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Cannot send sync replica request for " + partitionId + ", replicaIndex=" + replicaIndex
                            + ", namespace=" + namespace + ". No permits available!");
                    continue;
                }
                break;
            } else if (registerSyncInfoFor(partitionId, namespace, replicaIndex, target)) {
                namespaces.add(namespace);
            }
        }
        return namespaces;
    }

    private boolean registerSyncInfoFor(int partitionId, ServiceNamespace namespace, int replicaIndex, PartitionReplica target) {
        ReplicaFragmentSyncInfo syncInfo = new ReplicaFragmentSyncInfo(partitionId, namespace, replicaIndex, target);
        if (!replicaSyncRequests.add(syncInfo)) {
            if (logger.isFinestEnabled()) {
                logger.finest("Cannot send sync replica request for %s. Sync is already in progress!", syncInfo);
            }
            return false;
        }
        replicaSyncTimeoutScheduler.schedule(partitionMigrationTimeout, syncInfo, null);
        return true;
    }

    @Override
    public ServiceNamespace getServiceNamespace(Operation operation) {
        if (operation instanceof ServiceNamespaceAware aware) {
            return aware.getServiceNamespace();
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
    public void markPartitionReplicaAsSyncRequired(int partitionId, ServiceNamespace namespace, int replicaIndex) {
        replicaVersions[partitionId].markAsSyncRequired(namespace, replicaIndex);
    }

    @Override
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    public long[] getPartitionReplicaVersions(int partitionId, ServiceNamespace namespace) {
        return replicaVersions[partitionId].get(namespace);
    }

    @Override
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    // Mutates the replica version array, so that any replica indexes which require sync are reset to 0
    public long[] getPartitionReplicaVersionsForSync(int partitionId, ServiceNamespace namespace) {
        long[] replicas = replicaVersions[partitionId].get(namespace);
        for (int i = 0; i < replicas.length; i++) {
            if (replicas[i] == REQUIRES_SYNC) {
                replicas[i] = 0;
            }
        }
        return replicas;
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
     * @param partitionId  the partition ID
     * @param replicaIndex the index of the replica
     * @param versions     the new replica versions for the partition
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
        releaseReplicaSyncPermits(1);
        replicaSyncTimeoutScheduler.cancelIfExists(syncInfo, null);
    }

    void cancelReplicaSyncRequestsTo(Member member) {
        Iterator<ReplicaFragmentSyncInfo> iter = replicaSyncRequests.iterator();
        while (iter.hasNext()) {
            ReplicaFragmentSyncInfo syncInfo = iter.next();
            if (syncInfo.target != null && syncInfo.target.isIdentical(member)) {
                iter.remove();
                replicaSyncTimeoutScheduler.cancel(syncInfo);
                releaseReplicaSyncPermits(1);
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
                releaseReplicaSyncPermits(1);
            }
        }
    }

    /**
     * Tries to acquire requested permits. Less than requested permits may be acquired,
     * if insufficient permits are available. Number of actually acquired permits will be
     * returned to the caller. Acquired permits will be in the range of {@code [0, requestedPermits]}.
     *
     * @param requestedPermits number of requested permits
     * @return number of actually acquired permits.
     */
    public int tryAcquireReplicaSyncPermits(int requestedPermits) {
        assert requestedPermits > 0 : "Invalid permits: " + requestedPermits;

        int permits = requestedPermits;
        while (permits > 0 && !replicaSyncSemaphore.tryAcquire(permits)) {
            permits--;
        }

        if (permits > 0 && logger.isFinestEnabled()) {
            logger.finest("Acquired " + permits + " replica sync permits, requested permits was " + requestedPermits
                    + ". Remaining permits: " + replicaSyncSemaphore.availablePermits());
        }
        return permits;
    }

    /**
     * Releases the previously acquired permits.
     *
     * @param permitsToRelease number of permits
     */
    public void releaseReplicaSyncPermits(int permitsToRelease) {
        assert permitsToRelease > 0 : "Invalid permits: " + permitsToRelease;

        int availablePermits = availableReplicaSyncPermits();
        int acquiredPermits = maxParallelReplications - availablePermits;

        if (logger.isWarningEnabled() && acquiredPermits < permitsToRelease) {
            logger.warning(format("Found more replica sync permits than configured max number!"
                            + " (permitsToRelease: %d, acquired: %d, available: %d, max: %d)",
                    permitsToRelease, acquiredPermits, availablePermits, maxParallelReplications));
        }

        int permits = Math.min(acquiredPermits, permitsToRelease);
        if (permits <= 0) {
            return;
        }

        replicaSyncSemaphore.release(permits);

        if (logger.isFinestEnabled()) {
            int currentAvailable = availableReplicaSyncPermits();
            logger.finest("Released %d replica sync permits. (acquired: %d, available: %d, max: %d)",
                    permits, maxParallelReplications - currentAvailable,
                    currentAvailable, maxParallelReplications);
        }
    }

    /**
     * Returns the number of available permits.
     */
    public int availableReplicaSyncPermits() {
        return replicaSyncSemaphore.availablePermits();
    }

    /**
     * @return copy of ongoing replica-sync operations
     */
    List<ReplicaFragmentSyncInfo> getOngoingReplicaSyncRequests() {
        return new ArrayList<>(replicaSyncRequests);
    }

    /**
     * @return copy of scheduled replica-sync requests
     */
    List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> getScheduledReplicaSyncRequests() {
        final List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> entries = new ArrayList<>();
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
        replicaSyncSemaphore.drainPermits();
        replicaSyncSemaphore.release(maxParallelReplications);

        if (logger.isFinestEnabled()) {
            logger.finest("Reset replica sync permits to %d", maxParallelReplications);
        }
    }

    void scheduleReplicaVersionSync(ExecutionService executionService) {
        long definedBackupSyncCheckInterval = node.getProperties().getSeconds(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL);
        long backupSyncCheckInterval = definedBackupSyncCheckInterval > 0 ? definedBackupSyncCheckInterval : 1;

        executionService.schedule(new AntiEntropyTask(backupSyncCheckInterval), backupSyncCheckInterval, TimeUnit.SECONDS);
    }

    @Override
    public Collection<ServiceNamespace> getNamespaces(int partitionId) {
        return replicaVersions[partitionId].getNamespaces();
    }

    public void retainNamespaces(int partitionId, Collection<ServiceNamespace> namespaces) {
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
                    releaseReplicaSyncPermits(1);
                }
            }
        }
    }

    private class AntiEntropyTask implements Runnable {

        private static final int UNSET = -1;

        private final long firstRunNanos;
        private final long backupSyncCheckIntervalSeconds;
        private final AtomicInteger taskCounter = new AtomicInteger();

        private int taskCountPerSecond;
        private Queue<Integer> localPartitionIds;

        AntiEntropyTask(long backupSyncCheckIntervalSeconds) {
            this(backupSyncCheckIntervalSeconds, null, UNSET, UNSET);
        }

        AntiEntropyTask(long backupSyncCheckIntervalSeconds, Queue<Integer> localPartitionIds,
                        long firstRunNanos, int taskCountPerSecond) {
            this.backupSyncCheckIntervalSeconds = backupSyncCheckIntervalSeconds;
            this.localPartitionIds = localPartitionIds;
            this.firstRunNanos = firstRunNanos;
            this.taskCountPerSecond = taskCountPerSecond;
        }

        @Override
        public void run() {
            long initialStartTimeInNanos = firstRunNanos == UNSET ? System.nanoTime() : firstRunNanos;

            if (!node.isRunning() || !node.getNodeExtension().isStartCompleted()
                    || !partitionService.areMigrationTasksAllowed()) {
                scheduleNextRun(initialStartTimeInNanos, null, UNSET);
                return;
            }

            initLocalPartitionsIfNull();

            BitSet partitionsBitSet = convertLocalPartitionsToBitSet();

            if (partitionsBitSet.cardinality() != 0) {
                runAntiEntropyTask(partitionsBitSet, () -> {
                    if (taskCounter.incrementAndGet() == partitionsBitSet.cardinality()) {
                        scheduleNextRun(initialStartTimeInNanos, localPartitionIds, taskCountPerSecond);
                    }
                });
            } else {
                scheduleNextRun(initialStartTimeInNanos, localPartitionIds, taskCountPerSecond);
            }

        }

        private void runAntiEntropyTask(BitSet partitionsBitSet, Runnable afterRun) {
            nodeEngine.getOperationService().executeOnPartitions(partitionId
                            -> new PartitionPrimaryReplicaAntiEntropyTask(nodeEngine, partitionId, afterRun),
                    partitionsBitSet);
        }

        private void scheduleNextRun(long initialStartTimeInNanos,
                                     Queue<Integer> localPartitionIds, int numInOneGo) {

            ExecutionService executionService = nodeEngine.getExecutionService();

            if (CollectionUtil.isEmpty(localPartitionIds)) {
                // we have finished checking of all local partitions, time to decide when to commence next run.
                long elapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - initialStartTimeInNanos);
                long delayBeforeNextRunSeconds = Math.max(1, backupSyncCheckIntervalSeconds - elapsedSeconds);
                executionService.schedule(new AntiEntropyTask(backupSyncCheckIntervalSeconds),
                        delayBeforeNextRunSeconds, TimeUnit.SECONDS);
            } else {
                // we still have local partitions to check
                executionService.schedule(new AntiEntropyTask(backupSyncCheckIntervalSeconds,
                        localPartitionIds, initialStartTimeInNanos, numInOneGo), 1, TimeUnit.SECONDS);
            }
        }

        private void initLocalPartitionsIfNull() {
            if (localPartitionIds != null) {
                return;
            }

            localPartitionIds = getLocalPartitionIds();
            taskCountPerSecond = Math.max(1, (int) Math.ceil(1D * localPartitionIds.size() / backupSyncCheckIntervalSeconds));
        }

        @Nonnull
        private BitSet convertLocalPartitionsToBitSet() {
            BitSet bitSet = new BitSet(partitionService.getPartitionCount());

            do {
                Integer partitionId = localPartitionIds.poll();
                if (partitionId == null) {
                    break;
                }

                bitSet.set(partitionId);

            } while (bitSet.cardinality() < taskCountPerSecond);

            return bitSet;
        }

        private Queue<Integer> getLocalPartitionIds() {
            Queue<Integer> localPartitions = new ArrayDeque<>(partitionService.getPartitionCount());

            for (InternalPartition partition : partitionService.getInternalPartitions()) {
                if (partition.isLocal()) {
                    localPartitions.offer(partition.getPartitionId());
                }
            }
            return localPartitions;
        }

    }
}
