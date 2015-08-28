/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.impl;

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationEvent.MigrationStatus;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.partition.PartitionEvent;
import com.hazelcast.partition.PartitionEventListener;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionRuntimeState;
import com.hazelcast.partition.PartitionServiceProxy;
import com.hazelcast.partition.membergroup.MemberGroup;
import com.hazelcast.partition.membergroup.MemberGroupFactory;
import com.hazelcast.partition.membergroup.MemberGroupFactoryFactory;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ExponentialBackoffSleeper;

import com.hazelcast.util.FutureUtil.ExceptionHandler;
import com.hazelcast.util.HashUtil;
import com.hazelcast.util.scheduler.CoalescingDelayedTrigger;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.partition.impl.InternalPartitionServiceState.MIGRATION_LOCAL;
import static com.hazelcast.partition.impl.InternalPartitionServiceState.MIGRATION_ON_MASTER;
import static com.hazelcast.partition.impl.InternalPartitionServiceState.REPLICA_NOT_SYNC;
import static com.hazelcast.partition.impl.InternalPartitionServiceState.SAFE;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;


/**
 * The {@link InternalPartitionService} implementation.
 */
public class InternalPartitionServiceImpl implements InternalPartitionService, ManagedService,
        EventPublishingService<PartitionEvent, PartitionEventListener<PartitionEvent>>, PartitionAwareService {

    private static final String EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT = "Partition state sync invocation timed out";

    private static final int DEFAULT_PAUSE_MILLIS = 1000;
    private static final int PARTITION_OWNERSHIP_WAIT_MILLIS = 10;
    private static final int REPLICA_SYNC_CHECK_TIMEOUT_SECONDS = 10;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final int partitionCount;
    private final InternalPartitionImpl[] partitions;
    private final PartitionReplicaVersions[] replicaVersions;
    private final AtomicReferenceArray<ReplicaSyncInfo> replicaSyncRequests;
    private final EntryTaskScheduler<Integer, ReplicaSyncInfo> replicaSyncScheduler;
    @Probe
    private final Semaphore replicaSyncProcessLock;
    private final MigrationThread migrationThread;
    private final long partitionMigrationInterval;
    private final long partitionMigrationTimeout;
    private final long backupSyncCheckInterval;
    private final int maxParallelReplications;
    private final PartitionStateGenerator partitionStateGenerator;
    private final MemberGroupFactory memberGroupFactory;
    private final PartitionServiceProxy proxy;
    private final Lock lock = new ReentrantLock();

    @Probe
    private final AtomicInteger stateVersion = new AtomicInteger();
    @Probe(name = "migrationQueueSize")
    private final BlockingQueue<Runnable> migrationQueue = new LinkedBlockingQueue<Runnable>();
    private final AtomicBoolean migrationActive = new AtomicBoolean(true);
    @Probe(name = "lastRepartitionTime")
    private final AtomicLong lastRepartitionTime = new AtomicLong();
    private final CoalescingDelayedTrigger delayedResumeMigrationTrigger;

    private final ExceptionHandler partitionStateSyncTimeoutHandler;

    @Probe
    // can be read and written concurrently...
    private volatile int memberGroupsSize;

    // updates will be done under lock, but reads will be multithreaded.
    private volatile boolean initialized;

    @Probe(name = "activeMigrationCount")
    // updates will be done under lock, but reads will be multithreaded.
    private final ConcurrentMap<Integer, MigrationInfo> activeMigrations
            = new ConcurrentHashMap<Integer, MigrationInfo>(3, 0.75f, 1);

    // both reads and updates will be done under lock!
    private final LinkedList<MigrationInfo> completedMigrations = new LinkedList<MigrationInfo>();

    @Probe
    private final AtomicLong completedMigrationCounter = new AtomicLong();

    public InternalPartitionServiceImpl(Node node) {
        this.partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(InternalPartitionService.class);
        partitionStateSyncTimeoutHandler =
                logAllExceptions(logger, EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT, Level.FINEST);
        this.partitions = new InternalPartitionImpl[partitionCount];
        PartitionListener partitionListener = new LocalPartitionListener(this, node.getThisAddress());
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new InternalPartitionImpl(i, partitionListener, node.getThisAddress());
        }
        replicaVersions = new PartitionReplicaVersions[partitionCount];
        for (int i = 0; i < replicaVersions.length; i++) {
            replicaVersions[i] = new PartitionReplicaVersions(i);
        }

        memberGroupFactory = MemberGroupFactoryFactory.newMemberGroupFactory(node.getConfig().getPartitionGroupConfig());
        partitionStateGenerator = new PartitionStateGeneratorImpl();

        long interval = node.groupProperties.PARTITION_MIGRATION_INTERVAL.getLong();
        partitionMigrationInterval = interval > 0 ? TimeUnit.SECONDS.toMillis(interval) : 0;

        partitionMigrationTimeout = TimeUnit.SECONDS.toMillis(
                node.groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong());

        migrationThread = new MigrationThread(node);
        proxy = new PartitionServiceProxy(this);

        ExecutionService executionService = nodeEngine.getExecutionService();
        ScheduledExecutorService scheduledExecutor = executionService.getDefaultScheduledExecutor();

        // The reason behind this scheduler to have POSTPONE type is as follows:
        // When a node shifts up in the replica table upon a node failure, it sends a sync request to the partition owner and
        // registers it to the replicaSyncRequests. If another node fails before the already-running sync process completes,
        // the new sync request is simply scheduled to a further time. Again, before the already-running sync process completes,
        // if another node fails for the third time, the already-scheduled sync request should be overwritten with the new one.
        // This is because this node is shifted up to a higher level when the third node failure occurs and its respective sync
        // request will inherently include the backup data that is requested by the previously scheduled sync request.
        replicaSyncScheduler = EntryTaskSchedulerFactory.newScheduler(scheduledExecutor,
                new ReplicaSyncEntryProcessor(this), ScheduleType.POSTPONE);

        replicaSyncRequests = new AtomicReferenceArray<ReplicaSyncInfo>(partitionCount);

        long maxMigrationDelayMs = calculateMaxMigrationDelayOnMemberRemoved();
        long minMigrationDelayMs = calculateMigrationDelayOnMemberRemoved(maxMigrationDelayMs);
        this.delayedResumeMigrationTrigger = new CoalescingDelayedTrigger(
                executionService, minMigrationDelayMs, maxMigrationDelayMs, new Runnable() {
            @Override
            public void run() {
                resumeMigration();
            }
        });

        long definedBackupSyncCheckInterval = node.groupProperties.PARTITION_BACKUP_SYNC_INTERVAL.getInteger();
        backupSyncCheckInterval = definedBackupSyncCheckInterval > 0 ? definedBackupSyncCheckInterval : 1;
        maxParallelReplications = node.groupProperties.PARTITION_MAX_PARALLEL_REPLICATIONS.getInteger();
        replicaSyncProcessLock = new Semaphore(maxParallelReplications);
        nodeEngine.getMetricsRegistry().scanAndRegister(this, "partitions");
    }

    private long calculateMaxMigrationDelayOnMemberRemoved() {
        //hard limit for migration pause is half of the call timeout. otherwise we might experience timeouts
        return node.groupProperties.OPERATION_CALL_TIMEOUT_MILLIS.getLong() / 2;
    }

    private long calculateMigrationDelayOnMemberRemoved(long maxDelayMs) {
        long migrationDelayMs = node.groupProperties.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS.getLong() * 1000;

        long connectionErrorDetectionIntervalMs = node.groupProperties.CONNECTION_MONITOR_INTERVAL.getLong()
                * node.groupProperties.CONNECTION_MONITOR_MAX_FAULTS.getInteger() * 5;
        migrationDelayMs = Math.max(migrationDelayMs, connectionErrorDetectionIntervalMs);

        long heartbeatIntervalMs = node.groupProperties.HEARTBEAT_INTERVAL_SECONDS.getLong() * 1000;
        migrationDelayMs = Math.max(migrationDelayMs, heartbeatIntervalMs * 3);

        migrationDelayMs = Math.min(migrationDelayMs, maxDelayMs);
        return migrationDelayMs;
    }

    @Probe(name = "migrationActive")
    private int migrationActiveProbe() {
        return migrationActive.get() ? 1 : 0;
    }

    @Probe
    private int localPartitionCount() {
        int count = 0;
        for (InternalPartition partition : partitions) {
            if (partition.isLocal()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        migrationThread.start();

        int partitionTableSendInterval = node.groupProperties.PARTITION_TABLE_SEND_INTERVAL.getInteger();
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleAtFixedRate(new SendClusterStateTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);

        executionService.scheduleWithFixedDelay(new SyncReplicaVersionTask(),
                backupSyncCheckInterval, backupSyncCheckInterval, TimeUnit.SECONDS);
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        if (!initialized) {
            firstArrangement();
        }
        if (partitions[partitionId].getOwnerOrNull() == null && !node.isMaster() && node.joined()) {
            notifyMasterToAssignPartitions();
        }
        return partitions[partitionId].getOwnerOrNull();
    }

    @Override
    public Address getPartitionOwnerOrWait(int partition) {
        Address owner = getPartitionOwner(partition);
        while (owner == null) {
            try {
                Thread.sleep(PARTITION_OWNERSHIP_WAIT_MILLIS);
            } catch (InterruptedException e) {
                ExceptionUtil.rethrow(e);
            }
            owner = getPartitionOwner(partition);
        }
        return owner;
    }

    private void notifyMasterToAssignPartitions() {
        if (initialized) {
            return;
        }
        if (lock.tryLock()) {
            try {
                if (!initialized && !node.isMaster() && node.getMasterAddress() != null && node.joined()) {
                    Future f = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, new AssignPartitions(),
                            node.getMasterAddress()).setTryCount(1).invoke();
                    f.get(1, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                logger.finest(e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void firstArrangement() {
        if (!node.isMaster() || !node.isActive()) {
            notifyMasterToAssignPartitions();
            return;
        }
        if (!initialized) {
            lock.lock();
            try {
                if (initialized) {
                    return;
                }
                PartitionStateGenerator psg = partitionStateGenerator;
                final Set<Member> members = node.getClusterService().getMembers();
                Collection<MemberGroup> memberGroups = memberGroupFactory.createMemberGroups(members);
                if (memberGroups.isEmpty()) {
                    logger.warning("No member group is available to assign partition ownership...");
                    return;
                }

                logger.info("Initializing cluster partition table first arrangement...");
                Address[][] newState = psg.initialize(memberGroups, partitionCount);
                if (newState.length != partitionCount) {
                    throw new HazelcastException("Invalid partition count! "
                            + "Expected: " + partitionCount + ", Actual: " + newState.length);
                }

                for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                    InternalPartitionImpl partition = partitions[partitionId];
                    Address[] replicas = newState[partitionId];
                    partition.setReplicaAddresses(replicas);
                }
                initialized = true;
                publishPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
        }
    }

    private void updateMemberGroupsSize() {
        Set<Member> members = node.getClusterService().getMembers();
        final Collection<MemberGroup> groups = memberGroupFactory.createMemberGroups(members);
        int size = 0;
        for (MemberGroup group : groups) {
            if (group.size() > 0) {
                size++;
            }
        }
        memberGroupsSize = size;
    }

    @Override
    public int getMemberGroupsSize() {
        int size = memberGroupsSize;
        // size = 0 means service is not initialized yet.
        // return 1 instead since there should be at least one member group
        return size > 0 ? size : 1;
    }

    @Probe(name = "maxBackupCount")
    @Override
    public int getMaxBackupCount() {
        return Math.min(getMemberGroupsSize() - 1, InternalPartition.MAX_BACKUP_COUNT);
    }

    @Override
    public void memberAdded(MemberImpl member) {
        if (!member.localMember()) {
            updateMemberGroupsSize();
        }
        if (node.isMaster() && node.isActive()) {
            lock.lock();
            try {
                migrationQueue.clear();
                if (initialized) {
                    migrationQueue.add(new RepartitioningTask());

                    // send initial partition table to newly joined node.
                    Collection<MemberImpl> members = node.clusterService.getMemberImpls();
                    PartitionStateOperation op = new PartitionStateOperation(createPartitionState(members));
                    nodeEngine.getOperationService().send(op, member.getAddress());
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void memberRemoved(final MemberImpl member) {
        updateMemberGroupsSize();
        final Address deadAddress = member.getAddress();
        final Address thisAddress = node.getThisAddress();
        if (deadAddress == null || deadAddress.equals(thisAddress)) {
            return;
        }
        lock.lock();
        try {
            migrationQueue.clear();
            if (!activeMigrations.isEmpty()) {
                if (node.isMaster()) {
                    rollbackActiveMigrationsFromPreviousMaster(node.getLocalMember().getUuid());
                }
                for (MigrationInfo migrationInfo : activeMigrations.values()) {
                    if (deadAddress.equals(migrationInfo.getSource()) || deadAddress.equals(migrationInfo.getDestination())) {
                        migrationInfo.invalidate();
                    }
                }
            }
            // Pause migration and let all other members notice the dead member
            // and fix their own partitions.
            // Otherwise new master may take action fast and send new partition state
            // before other members realize the dead one.
            pauseMigration();
            cancelReplicaSyncRequestsTo(deadAddress);
            removeDeadAddress(deadAddress, thisAddress);

            if (node.isMaster() && initialized) {
                migrationQueue.add(new RepartitioningTask());
            }

            resumeMigrationEventually();
        } finally {
            lock.unlock();
        }
    }

    private void cancelReplicaSyncRequestsTo(Address deadAddress) {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            ReplicaSyncInfo syncInfo = replicaSyncRequests.get(partitionId);
            if (syncInfo != null && deadAddress.equals(syncInfo.target)) {
                cancelReplicaSync(partitionId);
            }
        }
    }

    void cancelReplicaSync(int partitionId) {
        ReplicaSyncInfo syncInfo = replicaSyncRequests.get(partitionId);
        if (syncInfo != null && replicaSyncRequests.compareAndSet(partitionId, syncInfo, null)) {
            replicaSyncScheduler.cancel(partitionId);
            releaseReplicaSyncPermit();
        }
    }

    private void resumeMigrationEventually() {
        delayedResumeMigrationTrigger.executeWithDelay();
    }

    private void removeDeadAddress(Address deadAddress, Address thisAddress) {
        for (InternalPartitionImpl partition : partitions) {
            if (deadAddress.equals(partition.getOwnerOrNull()) && thisAddress.equals(partition.getReplicaAddress(1))) {
                partition.setMigrating(true);
            }
            // shift partition table up.
            partition.onDeadAddress(deadAddress);
            // safety check!
            if (partition.onDeadAddress(deadAddress)) {
                throw new IllegalStateException("Duplicate address found in partition replicas!");
            }

        }
    }

    private void rollbackActiveMigrationsFromPreviousMaster(final String currentMasterUuid) {
        lock.lock();
        try {
            if (!activeMigrations.isEmpty()) {
                for (MigrationInfo migrationInfo : activeMigrations.values()) {
                    if (!currentMasterUuid.equals(migrationInfo.getMasterUuid())) {
                        // Still there is possibility of the other endpoint commits the migration
                        // but this node roll-backs!
                        logger.info("Rolling-back migration initiated by the old master -> " + migrationInfo);
                        finalizeActiveMigration(migrationInfo);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private PartitionRuntimeState createPartitionState(Collection<MemberImpl> members) {
        lock.lock();
        try {
            List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(members.size());
            for (MemberImpl member : members) {
                MemberInfo memberInfo = new MemberInfo(member.getAddress(), member.getUuid(), member.getAttributes());
                memberInfos.add(memberInfo);
            }
            ArrayList<MigrationInfo> migrationInfos = new ArrayList<MigrationInfo>(completedMigrations);
            ILogger logger = node.getLogger(PartitionRuntimeState.class);
            return new PartitionRuntimeState(logger, memberInfos, partitions, migrationInfos, stateVersion.get());
        } finally {
            lock.unlock();
        }
    }

    private void publishPartitionRuntimeState() {
        if (!initialized) {
            // do not send partition state until initialized!
            return;
        }

        if (!node.isMaster() || !node.isActive() || !node.joined()) {
            return;
        }

        if (!isMigrationActive()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }

        lock.lock();
        try {
            Collection<MemberImpl> members = node.clusterService.getMemberImpls();
            PartitionRuntimeState partitionState = createPartitionState(members);
            PartitionStateOperation op = new PartitionStateOperation(partitionState);

            OperationService operationService = nodeEngine.getOperationService();
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    try {
                        operationService.send(op, member.getAddress());
                    } catch (Exception e) {
                        logger.finest(e);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void syncPartitionRuntimeState() {
        syncPartitionRuntimeState(node.clusterService.getMemberImpls());
    }

    private void syncPartitionRuntimeState(Collection<MemberImpl> members) {
        if (!initialized) {
            // do not send partition state until initialized!
            return;
        }

        if (!node.isMaster() || !node.isActive() || !node.joined()) {
            return;
        }

        lock.lock();
        try {
            PartitionRuntimeState partitionState = createPartitionState(members);
            OperationService operationService = nodeEngine.getOperationService();

            List<Future> calls = firePartitionStateOperation(members, partitionState, operationService);
            waitWithDeadline(calls, 3, TimeUnit.SECONDS, partitionStateSyncTimeoutHandler);
        } finally {
            lock.unlock();
        }
    }

    private List<Future> firePartitionStateOperation(Collection<MemberImpl> members,
                                                     PartitionRuntimeState partitionState,
                                                     OperationService operationService) {
        List<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                try {
                    Address address = member.getAddress();
                    PartitionStateOperation operation = new PartitionStateOperation(partitionState, true);
                    Future<Object> f = operationService.invokeOnTarget(SERVICE_NAME, operation, address);
                    calls.add(f);
                } catch (Exception e) {
                    logger.finest(e);
                }
            }
        }
        return calls;
    }

    void processPartitionRuntimeState(PartitionRuntimeState partitionState) {
        lock.lock();
        try {
            if (!node.isActive() || !node.joined()) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Node should be active(" + node.isActive() + ") and joined(" + node.joined()
                            + ") to be able to process partition table!");
                }
                return;
            }
            final Address sender = partitionState.getEndpoint();
            final Address master = node.getMasterAddress();
            if (node.isMaster()) {
                logger.warning("This is the master node and received a PartitionRuntimeState from "
                        + sender + ". Ignoring incoming state! ");
                return;
            } else {
                if (sender == null || !sender.equals(master)) {
                    if (node.clusterService.getMember(sender) == null) {
                        logger.severe("Received a ClusterRuntimeState from an unknown member!"
                                + " => Sender: " + sender + ", Master: " + master + "! ");
                        return;
                    } else {
                        logger.warning("Received a ClusterRuntimeState, but its sender doesn't seem to be master!"
                                + " => Sender: " + sender + ", Master: " + master + "! "
                                + "(Ignore if master node has changed recently.)");
                        return;
                    }
                }
            }

            stateVersion.set(partitionState.getVersion());
            initialized = true;

            PartitionInfo[] state = partitionState.getPartitions();
            filterAndLogUnknownAddressesInPartitionTable(sender, state);
            finalizeOrRollbackMigration(partitionState, state);
        } finally {
            lock.unlock();
        }
    }

    private void finalizeOrRollbackMigration(PartitionRuntimeState partitionState, PartitionInfo[] state) {
        Collection<MigrationInfo> completedMigrations = partitionState.getCompletedMigrations();
        for (MigrationInfo completedMigration : completedMigrations) {
            addCompletedMigration(completedMigration);
            int partitionId = completedMigration.getPartitionId();
            PartitionInfo partitionInfo = state[partitionId];
            // mdogan:
            // Each partition should be updated right after migration is finalized
            // at the moment, it doesn't cause any harm to existing services,
            // because we have a `migrating` flag in partition which is cleared during migration finalization.
            // But from API point of view, we should provide explicit guarantees.
            // For the time being, leaving this stuff as is to not to change behaviour.
            updatePartition(partitionInfo);
            finalizeActiveMigration(completedMigration);
        }
        if (!activeMigrations.isEmpty()) {
            final MemberImpl masterMember = getMasterMember();
            rollbackActiveMigrationsFromPreviousMaster(masterMember.getUuid());
        }

        updateAllPartitions(state);
    }

    private void updatePartition(PartitionInfo partitionInfo) {
        InternalPartitionImpl partition = partitions[partitionInfo.getPartitionId()];
        Address[] replicas = partitionInfo.getReplicaAddresses();
        partition.setReplicaAddresses(replicas);
    }

    private void updateAllPartitions(PartitionInfo[] state) {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            updatePartition(state[partitionId]);
        }
    }

    private void filterAndLogUnknownAddressesInPartitionTable(Address sender, PartitionInfo[] state) {
        final Set<Address> unknownAddresses = new HashSet<Address>();
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            PartitionInfo partitionInfo = state[partitionId];
            searchUnknownAddressesInPartitionTable(sender, unknownAddresses, partitionId, partitionInfo);
        }
        logUnknownAddressesInPartitionTable(sender, unknownAddresses);
    }

    private void logUnknownAddressesInPartitionTable(Address sender, Set<Address> unknownAddresses) {
        if (!unknownAddresses.isEmpty() && logger.isLoggable(Level.WARNING)) {
            StringBuilder s = new StringBuilder("Following unknown addresses are found in partition table")
                    .append(" sent from master[").append(sender).append("].")
                    .append(" (Probably they have recently joined or left the cluster.)")
                    .append(" {");
            for (Address address : unknownAddresses) {
                s.append("\n\t").append(address);
            }
            s.append("\n}");
            logger.warning(s.toString());
        }
    }

    private void searchUnknownAddressesInPartitionTable(Address sender, Set<Address> unknownAddresses, int partitionId,
                                                        PartitionInfo partitionInfo) {
        for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
            Address address = partitionInfo.getReplicaAddress(index);
            if (address != null && getMember(address) == null) {
                if (logger.isFinestEnabled()) {
                    logger.finest(
                            "Unknown " + address + " found in partition table sent from master "
                                    + sender + ". It has probably already left the cluster. partitionId="
                                    + partitionId);
                }
                unknownAddresses.add(address);
            }
        }
    }

    private void finalizeActiveMigration(final MigrationInfo migrationInfo) {
        if (activeMigrations.containsKey(migrationInfo.getPartitionId())) {
            lock.lock();
            try {
                if (activeMigrations.containsValue(migrationInfo)) {
                    if (migrationInfo.startProcessing()) {
                        processMigrationInfo(migrationInfo);
                    } else {
                        logger.info("Scheduling finalization of " + migrationInfo
                                + ", because migration process is currently running.");
                        nodeEngine.getExecutionService().schedule(new Runnable() {
                            @Override
                            public void run() {
                                finalizeActiveMigration(migrationInfo);
                            }
                        }, 3, TimeUnit.SECONDS);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void processMigrationInfo(MigrationInfo migrationInfo) {
        try {
            Address thisAddress = node.getThisAddress();
            boolean source = thisAddress.equals(migrationInfo.getSource());
            boolean destination = thisAddress.equals(migrationInfo.getDestination());
            if (source || destination) {
                int partitionId = migrationInfo.getPartitionId();
                InternalPartitionImpl migratingPartition = getPartitionImpl(partitionId);
                Address ownerAddress = migratingPartition.getOwnerOrNull();
                boolean success = migrationInfo.getDestination().equals(ownerAddress);
                MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                FinalizeMigrationOperation op = new FinalizeMigrationOperation(endpoint, success);
                op.setPartitionId(partitionId)
                        .setNodeEngine(nodeEngine)
                        .setValidateTarget(false)
                        .setService(this);
                nodeEngine.getOperationService().executeOperation(op);
            }
        } catch (Exception e) {
            logger.warning(e);
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    void addActiveMigration(MigrationInfo migrationInfo) {
        lock.lock();
        try {
            int partitionId = migrationInfo.getPartitionId();
            partitions[partitionId].setMigrating(true);
            MigrationInfo currentMigrationInfo = activeMigrations.putIfAbsent(partitionId, migrationInfo);
            if (currentMigrationInfo != null) {
                boolean oldMaster = false;
                MigrationInfo oldMigration;
                MigrationInfo newMigration;
                MemberImpl masterMember = getMasterMember();
                String master = masterMember.getUuid();
                if (!master.equals(currentMigrationInfo.getMasterUuid())) {
                    // master changed
                    oldMigration = currentMigrationInfo;
                    newMigration = migrationInfo;
                    oldMaster = true;
                } else if (!master.equals(migrationInfo.getMasterUuid())) {
                    // master changed
                    oldMigration = migrationInfo;
                    newMigration = currentMigrationInfo;
                    oldMaster = true;
                } else if (!currentMigrationInfo.isProcessing() && migrationInfo.isProcessing()) {
                    // new migration arrived before partition state!
                    oldMigration = currentMigrationInfo;
                    newMigration = migrationInfo;
                } else {
                    String message = "Something is seriously wrong! There are two migration requests for the "
                            + "same partition! First -> " + currentMigrationInfo + ", Second -> " + migrationInfo;
                    IllegalStateException error = new IllegalStateException(message);
                    logger.severe(message, error);
                    throw error;
                }

                if (oldMaster) {
                    logger.info("Finalizing migration instantiated by the old master -> " + oldMigration);
                } else {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Finalizing previous migration -> " + oldMigration);
                    }
                }
                finalizeActiveMigration(oldMigration);
                activeMigrations.put(partitionId, newMigration);
            }
        } finally {
            lock.unlock();
        }
    }

    private MemberImpl getMasterMember() {
        return node.clusterService.getMember(node.getMasterAddress());
    }

    MigrationInfo getActiveMigration(int partitionId) {
        return activeMigrations.get(partitionId);
    }

    MigrationInfo removeActiveMigration(int partitionId) {
        partitions[partitionId].setMigrating(false);
        return activeMigrations.remove(partitionId);
    }

    @Override
    public Collection<MigrationInfo> getActiveMigrations() {
        return Collections.unmodifiableCollection(activeMigrations.values());
    }

    private void addCompletedMigration(MigrationInfo migrationInfo) {
        completedMigrationCounter.incrementAndGet();

        lock.lock();
        try {
            if (completedMigrations.size() > 25) {
                completedMigrations.removeFirst();
            }
            completedMigrations.add(migrationInfo);
        } finally {
            lock.unlock();
        }
    }

    private void evictCompletedMigrations() {
        lock.lock();
        try {
            if (!completedMigrations.isEmpty()) {
                completedMigrations.removeFirst();
            }
        } finally {
            lock.unlock();
        }
    }

    // This method is called in backup node. Given all other conditions are satisfied,
    // this method initiates a replica sync operation and registers it to replicaSyncRequest.
    // If another sync request is already registered, it schedules the new replica sync request to a further time.
    void triggerPartitionReplicaSync(int partitionId, int replicaIndex, long delayMillis) {
        if (replicaIndex < 0 || replicaIndex > InternalPartition.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Invalid replica index! replicaIndex=" + replicaIndex
                    + " for partitionId=" + partitionId);
        }

        if (!checkSyncPartitionTarget(partitionId, replicaIndex)) {
            return;
        }

        InternalPartitionImpl partition = getPartitionImpl(partitionId);
        Address target = partition.getOwnerOrNull();
        ReplicaSyncInfo syncInfo = new ReplicaSyncInfo(partitionId, replicaIndex, target);

        if (delayMillis > 0) {
            schedulePartitionReplicaSync(syncInfo, target, delayMillis);
            return;
        }

        // mdogan:
        // merged two conditions into single `if-return` block to
        // conform checkstyle return-count rule.
        if (!isMigrationActive() || partition.isMigrating()) {
            schedulePartitionReplicaSync(syncInfo, target, REPLICA_SYNC_RETRY_DELAY);
            return;
        }

        if (replicaSyncRequests.compareAndSet(partitionId, null, syncInfo)) {
            if (fireSyncReplicaRequest(syncInfo, target)) {
                return;
            }

            replicaSyncRequests.compareAndSet(partitionId, syncInfo, null);
            schedulePartitionReplicaSync(syncInfo, target, REPLICA_SYNC_RETRY_DELAY);
            return;
        }

        long scheduleDelay = getReplicaSyncScheduleDelay(partitionId);
        schedulePartitionReplicaSync(syncInfo, target, scheduleDelay);
    }

    private long getReplicaSyncScheduleDelay(int partitionId) {
        long scheduleDelay = DEFAULT_REPLICA_SYNC_DELAY;
        Address thisAddress = node.getThisAddress();
        InternalPartitionImpl partition = getPartitionImpl(partitionId);
        ReplicaSyncInfo currentSyncInfo = replicaSyncRequests.get(partitionId);
        if (currentSyncInfo != null
                && !thisAddress.equals(partition.getReplicaAddress(currentSyncInfo.replicaIndex))) {
            clearReplicaSyncRequest(partitionId, currentSyncInfo.replicaIndex);
            scheduleDelay = REPLICA_SYNC_RETRY_DELAY;
        }
        return scheduleDelay;
    }

    private boolean fireSyncReplicaRequest(ReplicaSyncInfo syncInfo, Address target) {
        if (tryToAcquireReplicaSyncPermit()) {
            int partitionId = syncInfo.partitionId;
            int replicaIndex = syncInfo.replicaIndex;
            replicaSyncScheduler.cancel(partitionId);

            if (logger.isFinestEnabled()) {
                logger.finest("Sending sync replica request to -> " + target + "; for partitionId=" + partitionId
                        + ", replicaIndex=" + replicaIndex);
            }
            replicaSyncScheduler.schedule(partitionMigrationTimeout, partitionId, syncInfo);
            ReplicaSyncRequest syncRequest = new ReplicaSyncRequest(partitionId, replicaIndex);
            nodeEngine.getOperationService().send(syncRequest, target);
            return true;
        }
        return false;
    }

    private void schedulePartitionReplicaSync(ReplicaSyncInfo syncInfo, Address target, long delayMillis) {
        int partitionId = syncInfo.partitionId;
        int replicaIndex = syncInfo.replicaIndex;

        if (logger.isFinestEnabled()) {
            logger.finest("Scheduling [" + delayMillis + "ms] sync replica request to -> " + target
                    + "; for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex);
        }
        replicaSyncScheduler.schedule(delayMillis, partitionId, syncInfo);
    }

    private boolean checkSyncPartitionTarget(int partitionId, int replicaIndex) {
        final InternalPartitionImpl partition = getPartitionImpl(partitionId);
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
                        + getPartitionImpl(partitionId));
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

    @Override
    public InternalPartition[] getPartitions() {
        //a defensive copy is made to prevent breaking with the old approach, but imho not needed
        InternalPartition[] result = new InternalPartition[partitions.length];
        System.arraycopy(partitions, 0, result, 0, partitions.length);
        return result;
    }

    @Override
    public MemberImpl getMember(Address address) {
        return node.clusterService.getMember(address);
    }

    InternalPartitionImpl getPartitionImpl(int partitionId) {
        return partitions[partitionId];
    }

    @Override
    public InternalPartitionImpl getPartition(int partitionId) {
        return getPartition(partitionId, true);
    }

    @Override
    public InternalPartitionImpl getPartition(int partitionId, boolean triggerOwnerAssignment) {
        InternalPartitionImpl p = getPartitionImpl(partitionId);
        if (triggerOwnerAssignment && p.getOwnerOrNull() == null) {
            // probably ownerships are not set yet.
            // force it.
            getPartitionOwner(partitionId);
        }
        return p;
    }

    @Override
    public boolean prepareToSafeShutdown(long timeout, TimeUnit unit) {
        long timeoutInMillis = unit.toMillis(timeout);
        ExponentialBackoffSleeper sleeper = ExponentialBackoffSleeper.builder()
                .factor(1.3)
                .initialMs(25)
                .maxMs(DEFAULT_PAUSE_MILLIS)
                .timeoutMs(timeoutInMillis)
                .build();
        while (!sleeper.isTimedOut()) {
            while (!sleeper.isTimedOut() && shouldWaitMigrationOrBackups(Level.INFO)) {
                sleeper.sleepQuietly();
            }
            if (sleeper.isTimedOut()) {
                break;
            }

            if (node.isMaster()) {
                syncPartitionRuntimeState();
            } else {
                if (!waitForOngoingMigrations(sleeper)) {
                    break;
                }
            }

            if (checkReplicaSyncState()) {
                logger.finest("Replica sync state before shutdown is OK");
                return true;
            } else {
                if (sleeper.isTimedOut()) {
                    break;
                }
                logger.info("Some backup replicas are inconsistent with primary, waiting for synchronization. Timeout: "
                        + timeoutInMillis + "ms");
                sleeper.sleepQuietly();
            }
        }
        return false;
    }

    private boolean waitForOngoingMigrations(ExponentialBackoffSleeper sleeper) {
        while (!sleeper.isTimedOut() && hasOnGoingMigrationMaster(Level.WARNING)) {
            // ignore elapsed time during master inv.
            logger.info("Waiting for the master node to complete remaining migrations!");
            sleeper.sleepQuietly();
        }
        return !sleeper.isTimedOut();
    }

    @Override
    public boolean isMemberStateSafe() {
        return getMemberState() == SAFE;
    }

    public InternalPartitionServiceState getMemberState() {
        if (hasOnGoingMigrationLocal()) {
            return MIGRATION_LOCAL;
        }

        if (!node.isMaster()) {
            if (hasOnGoingMigrationMaster(Level.OFF)) {
                return MIGRATION_ON_MASTER;
            }
        }

        return isReplicaInSyncState() ? SAFE : REPLICA_NOT_SYNC;
    }

    @Override
    public boolean hasOnGoingMigration() {
        return hasOnGoingMigrationLocal() || (!node.isMaster() && hasOnGoingMigrationMaster(Level.FINEST));
    }

    private boolean hasOnGoingMigrationMaster(Level level) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            return true;
        }
        Operation operation = new HasOngoingMigration();
        OperationService operationService = nodeEngine.getOperationService();
        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation,
                masterAddress);
        Future future = invocationBuilder.setTryCount(100).setTryPauseMillis(100).invoke();
        try {
            return (Boolean) future.get(1, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            Logger.getLogger(InternalPartitionServiceImpl.class).finest("Future wait interrupted", ie);
        } catch (Exception e) {
            logger.log(level, "Could not get a response from master about migrations! -> " + e.toString());
        }
        return false;
    }

    @Override
    public boolean hasOnGoingMigrationLocal() {
        return !activeMigrations.isEmpty() || !migrationQueue.isEmpty()
                || !isMigrationActive()
                || migrationThread.isMigrating()
                || shouldWaitMigrationOrBackups(Level.OFF);
    }

    private boolean isReplicaInSyncState() {
        if (!initialized || getMemberGroupsSize() < 2) {
            return true;
        }
        final int replicaIndex = 1;
        final List<Future> futures = new ArrayList<Future>();
        final Address thisAddress = node.getThisAddress();
        for (InternalPartitionImpl partition : partitions) {
            final Address owner = partition.getOwnerOrNull();
            if (thisAddress.equals(owner)) {
                if (partition.getReplicaAddress(replicaIndex) != null) {
                    final int partitionId = partition.getPartitionId();
                    final long replicaVersion = getCurrentReplicaVersion(replicaIndex, partitionId);
                    final Operation operation = createReplicaSyncStateOperation(replicaVersion, partitionId);
                    final Future future = invoke(operation, replicaIndex, partitionId);
                    futures.add(future);
                }
            }
        }
        if (futures.isEmpty()) {
            return true;
        }
        for (Future future : futures) {
            boolean isSync = getFutureResult(future, REPLICA_SYNC_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!isSync) {
                return false;
            }
        }
        return true;
    }

    private long getCurrentReplicaVersion(int replicaIndex, int partitionId) {
        final long[] versions = getPartitionReplicaVersions(partitionId);
        return versions[replicaIndex - 1];
    }

    private boolean getFutureResult(Future future, long seconds, TimeUnit unit) {
        boolean sync;
        try {
            sync = (Boolean) future.get(seconds, unit);
        } catch (Throwable t) {
            sync = false;
            logger.warning("Exception while getting future", t);
        }
        return sync;
    }

    private Future invoke(Operation operation, int replicaIndex, int partitionId) {
        final OperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(InternalPartitionService.SERVICE_NAME, operation, partitionId)
                .setTryCount(3)
                .setTryPauseMillis(250)
                .setReplicaIndex(replicaIndex)
                .invoke();
    }

    private Operation createReplicaSyncStateOperation(long replicaVersion, int partitionId) {
        final Operation op = new IsReplicaVersionSync(replicaVersion);
        op.setService(this);
        op.setNodeEngine(nodeEngine);
        op.setOperationResponseHandler(createErrorLoggingResponseHandler(node.getLogger(IsReplicaVersionSync.class)));
        op.setPartitionId(partitionId);

        return op;
    }

    private boolean checkReplicaSyncState() {
        if (!initialized) {
            return true;
        }

        if (getMemberGroupsSize() < 2) {
            return true;
        }

        final Address thisAddress = node.getThisAddress();
        final Semaphore s = new Semaphore(0);
        final AtomicBoolean ok = new AtomicBoolean(true);
        final ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                if (Boolean.FALSE.equals(response)) {
                    ok.compareAndSet(true, false);
                }
                s.release();
            }

            @Override
            public void onFailure(Throwable t) {
                ok.compareAndSet(true, false);
            }
        };
        int ownedCount = submitSyncReplicaOperations(thisAddress, s, ok, callback);
        try {
            if (ok.get()) {
                int permits = ownedCount * getMaxBackupCount();
                return s.tryAcquire(permits, REPLICA_SYNC_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS) && ok.get();
            } else {
                return false;
            }
        } catch (InterruptedException ignored) {
            return false;
        }
    }

    private int submitSyncReplicaOperations(Address thisAddress, Semaphore s, AtomicBoolean ok,
                                            ExecutionCallback callback) {

        int ownedCount = 0;
        ILogger responseLogger = node.getLogger(SyncReplicaVersion.class);
        OperationResponseHandler responseHandler =
                createErrorLoggingResponseHandler(responseLogger);

        for (InternalPartitionImpl partition : partitions) {
            Address owner = partition.getOwnerOrNull();
            if (thisAddress.equals(owner)) {
                for (int i = 1; i <= getMaxBackupCount(); i++) {
                    if (partition.getReplicaAddress(i) != null) {
                        SyncReplicaVersion op = new SyncReplicaVersion(i, callback);
                        op.setService(this);
                        op.setNodeEngine(nodeEngine);
                        op.setOperationResponseHandler(responseHandler);
                        op.setPartitionId(partition.getPartitionId());
                        nodeEngine.getOperationService().executeOperation(op);
                    } else {
                        ok.set(false);
                        s.release();
                    }
                }
                ownedCount++;
            } else if (owner == null) {
                ok.set(false);
            }
        }
        return ownedCount;
    }

    private boolean shouldWaitMigrationOrBackups(Level level) {
        if (!preCheckShouldWaitMigrationOrBackups()) {
            return false;
        }

        if (checkForActiveMigrations(level)) {
            return true;
        }

        for (InternalPartitionImpl partition : partitions) {
            if (partition.getReplicaAddress(1) == null) {
                if (logger.isLoggable(level)) {
                    logger.log(level, "Should take backup of partitionId=" + partition.getPartitionId());
                }
                return true;
            }
        }
        int replicaSyncProcesses = maxParallelReplications - replicaSyncProcessLock.availablePermits();
        if (replicaSyncProcesses > 0) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Processing replica sync requests: " + replicaSyncProcesses);
            }
            return true;
        }
        return false;
    }

    private boolean preCheckShouldWaitMigrationOrBackups() {
        if (!initialized) {
            return false;
        }

        if (getMemberGroupsSize() < 2) {
            return false;
        }

        return true;
    }

    private boolean checkForActiveMigrations(Level level) {
        final int activeSize = activeMigrations.size();
        if (activeSize != 0) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Waiting for active migration tasks: " + activeSize);
            }
            return true;
        }

        int queueSize = migrationQueue.size();
        if (queueSize != 0) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Waiting for cluster migration tasks: " + queueSize);
            }
            return true;
        }
        return false;
    }

    @Override
    public final int getPartitionId(Data key) {
        return HashUtil.hashToIndex(key.getPartitionHash(), partitionCount);
    }

    @Override
    public final int getPartitionId(Object key) {
        return getPartitionId(nodeEngine.toData(key));
    }

    @Override
    public final int getPartitionCount() {
        return partitionCount;
    }

    public long getPartitionMigrationTimeout() {
        return partitionMigrationTimeout;
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    @Override
    public long[] incrementPartitionReplicaVersions(int partitionId, int backupCount) {
        PartitionReplicaVersions replicaVersion = replicaVersions[partitionId];
        return replicaVersion.incrementAndGet(backupCount);
    }

    // called in operation threads
    @Override
    public void updatePartitionReplicaVersions(int partitionId, long[] versions, int replicaIndex) {
        PartitionReplicaVersions partitionVersion = replicaVersions[partitionId];
        if (!partitionVersion.update(versions, replicaIndex)) {
            // this partition backup is behind the owner.
            triggerPartitionReplicaSync(partitionId, replicaIndex, 0L);
        }
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    @Override
    public long[] getPartitionReplicaVersions(int partitionId) {
        return replicaVersions[partitionId].get();
    }

    // called in operation threads
    @Override
    public void setPartitionReplicaVersions(int partitionId, long[] versions, int replicaOffset) {
        replicaVersions[partitionId].set(versions, replicaOffset);
    }

    @Override
    public void clearPartitionReplicaVersions(int partitionId) {
        replicaVersions[partitionId].clear();
    }

    // called in operation threads
    void finalizeReplicaSync(int partitionId, int replicaIndex, long[] versions) {
        PartitionReplicaVersions replicaVersion = replicaVersions[partitionId];
        replicaVersion.clear();
        replicaVersion.set(versions, replicaIndex);
        clearReplicaSyncRequest(partitionId, replicaIndex);
    }

    // called in operation threads
    void clearReplicaSyncRequest(int partitionId, int replicaIndex) {
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

    boolean tryToAcquireReplicaSyncPermit() {
        return replicaSyncProcessLock.tryAcquire();
    }

    void releaseReplicaSyncPermit() {
        replicaSyncProcessLock.release();
    }

    @Override
    public Map<Address, List<Integer>> getMemberPartitionsMap() {
        final int members = node.getClusterService().getSize();
        Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(members);
        for (int i = 0; i < partitionCount; i++) {
            Address owner;
            while ((owner = getPartitionOwner(i)) == null) {
                try {
                    Thread.sleep(PARTITION_OWNERSHIP_WAIT_MILLIS);
                } catch (InterruptedException e) {
                    throw new HazelcastException(e);
                }
            }
            List<Integer> ownedPartitions = memberPartitions.get(owner);
            if (ownedPartitions == null) {
                ownedPartitions = new ArrayList<Integer>();
                memberPartitions.put(owner, ownedPartitions);
            }
            ownedPartitions.add(i);
        }
        return memberPartitions;
    }

    @Override
    public List<Integer> getMemberPartitions(Address target) {
        List<Integer> ownedPartitions = new LinkedList<Integer>();
        for (int i = 0; i < partitionCount; i++) {
            final Address owner = getPartitionOwner(i);
            if (target.equals(owner)) {
                ownedPartitions.add(i);
            }
        }
        return ownedPartitions;
    }

    @Override
    public void reset() {
        migrationQueue.clear();
        for (int k = 0; k < replicaSyncRequests.length(); k++) {
            replicaSyncRequests.set(k, null);
        }
        replicaSyncScheduler.cancelAll();
        // this is not sync with possibly running sync process
        // permit count can exceed allowed parallelization count.
        replicaSyncProcessLock.drainPermits();
        replicaSyncProcessLock.release(maxParallelReplications);

        lock.lock();
        try {
            initialized = false;
            for (InternalPartitionImpl partition : partitions) {
                partition.reset();
            }
            activeMigrations.clear();
            completedMigrations.clear();
            stateVersion.set(0);
        } finally {
            lock.unlock();
        }
    }

    public void pauseMigration() {
        migrationActive.set(false);
    }

    public void resumeMigration() {
        migrationActive.set(true);
    }

    public boolean isMigrationActive() {
        return migrationActive.get();
    }

    @Override
    public void shutdown(boolean terminate) {
        logger.finest("Shutting down the partition service");
        migrationThread.stopNow();
        reset();
    }

    public long getMigrationQueueSize() {
        return migrationQueue.size();
    }

    public PartitionServiceProxy getPartitionServiceProxy() {
        return proxy;
    }

    private void sendMigrationEvent(final MigrationInfo migrationInfo, final MigrationStatus status) {
        MemberImpl current = getMember(migrationInfo.getSource());
        MemberImpl newOwner = getMember(migrationInfo.getDestination());
        MigrationEvent event = new MigrationEvent(migrationInfo.getPartitionId(), current, newOwner, status);
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, MIGRATION_EVENT_TOPIC);
        eventService.publishEvent(SERVICE_NAME, registrations, event, event.getPartitionId());
    }

    @Override
    public String addMigrationListener(MigrationListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final MigrationListenerAdapter adapter = new MigrationListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(SERVICE_NAME, MIGRATION_EVENT_TOPIC, adapter);
        return registration.getId();
    }

    @Override
    public boolean removeMigrationListener(String registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, MIGRATION_EVENT_TOPIC, registrationId);
    }

    @Override
    public String addPartitionLostListener(PartitionLostListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final PartitionLostListenerAdapter adapter = new PartitionLostListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, adapter);
        return registration.getId();
    }

    @Override
    public boolean removePartitionLostListener(String registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, registrationId);
    }

    @Override
    public void dispatchEvent(PartitionEvent partitionEvent, PartitionEventListener partitionEventListener) {
        partitionEventListener.onEvent(partitionEvent);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionManager[" + stateVersion + "] {\n");
        sb.append("\n");
        sb.append("migrationQ: ").append(migrationQueue.size());
        sb.append("\n}");
        return sb.toString();
    }

    public Node getNode() {
        return node;
    }

    @Override
    public boolean isPartitionOwner(int partitionId) {
        InternalPartitionImpl partition = getPartition(partitionId);
        return node.getThisAddress().equals(partition.getOwnerOrNull());
    }

    @Override
    public int getPartitionStateVersion() {
        return stateVersion.get();
    }

    @Override
    public void onPartitionLost(InternalPartitionLostEvent event) {
        final PartitionLostEvent partitionLostEvent = new PartitionLostEvent(event.getPartitionId(), event.getLostReplicaIndex(),
                event.getEventSource());
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> registrations = eventService
                .getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC);
        eventService.publishEvent(SERVICE_NAME, registrations, partitionLostEvent, event.getPartitionId());
    }

    /**
     * @return copy of ongoing replica-sync operations
     */
    public List<ReplicaSyncInfo> getOngoingReplicaSyncRequests() {
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
    public List<ScheduledEntry<Integer, ReplicaSyncInfo>> getScheduledReplicaSyncRequests() {
        final List<ScheduledEntry<Integer, ReplicaSyncInfo>> entries = new ArrayList<ScheduledEntry<Integer, ReplicaSyncInfo>>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final ScheduledEntry<Integer, ReplicaSyncInfo> entry = replicaSyncScheduler.get(partitionId);
            if (entry != null) {
                entries.add(entry);
            }
        }

        return entries;
    }

    private class SendClusterStateTask implements Runnable {
        @Override
        public void run() {
            if (node.isMaster() && node.isActive()) {
                if (!migrationQueue.isEmpty() && isMigrationActive()) {
                    logger.info("Remaining migration tasks in queue => " + migrationQueue.size());
                }
                publishPartitionRuntimeState();
            }
        }
    }

    private class SyncReplicaVersionTask implements Runnable {
        @Override
        public void run() {
            if (node.isActive() && migrationActive.get()) {
                for (InternalPartitionImpl partition : partitions) {
                    if (partition.isLocal()) {
                        for (int index = 1; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                            if (partition.getReplicaAddress(index) != null) {
                                SyncReplicaVersion op = new SyncReplicaVersion(index, null);
                                op.setService(InternalPartitionServiceImpl.this);
                                op.setNodeEngine(nodeEngine);
                                op.setOperationResponseHandler(
                                        createErrorLoggingResponseHandler(node.getLogger(SyncReplicaVersion.class)));
                                op.setPartitionId(partition.getPartitionId());
                                nodeEngine.getOperationService().executeOperation(op);
                            }
                        }
                    }
                }
            }
        }
    }

    private class RepartitioningTask implements Runnable {
        @Override
        public void run() {
            if (node.isMaster() && node.isActive()) {
                lock.lock();
                try {
                    if (!initialized) {
                        return;
                    }
                    if (!isMigrationAllowed()) {
                        return;
                    }

                    migrationQueue.clear();
                    PartitionStateGenerator psg = partitionStateGenerator;
                    Collection<MemberImpl> members = node.getClusterService().getMemberImpls();
                    Collection<MemberGroup> memberGroups = memberGroupFactory.createMemberGroups(members);
                    Address[][] newState = psg.reArrange(memberGroups, partitions);

                    if (!isMigrationAllowed()) {
                        return;
                    }

                    int migrationCount = 0;
                    int lostCount = 0;
                    lastRepartitionTime.set(Clock.currentTimeMillis());
                    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                        Address[] replicas = newState[partitionId];
                        InternalPartitionImpl currentPartition = partitions[partitionId];
                        Address currentOwner = currentPartition.getOwnerOrNull();
                        Address newOwner = replicas[0];

                        if (currentOwner == null) {
                            // assign new owner for lost partition
                            lostCount++;
                            assignNewPartitionOwner(partitionId, replicas, currentPartition, newOwner);
                        } else if (newOwner != null && !currentOwner.equals(newOwner)) {
                            if (logger.isFinestEnabled()) {
                                logger.finest("PartitionToMigrate partitionId=" + partitionId
                                        + " replicas=" + Arrays.toString(replicas) + " currentOwner="
                                        + currentOwner + " newOwner=" + newOwner);
                            }

                            migrationCount++;
                            migratePartitionToNewOwner(partitionId, replicas, currentOwner, newOwner);
                        } else {
                            currentPartition.setReplicaAddresses(replicas);
                        }
                    }
                    syncPartitionRuntimeState(members);
                    logMigrationStatistics(migrationCount, lostCount);
                } finally {
                    lock.unlock();
                }
            }
        }

        private void logMigrationStatistics(int migrationCount, int lostCount) {
            if (lostCount > 0) {
                logger.warning("Assigning new owners for " + lostCount + " LOST partitions!");
            }

            if (migrationCount > 0) {
                logger.info("Re-partitioning cluster data... Migration queue size: " + migrationCount);
            } else {
                logger.info("Partition balance is ok, no need to re-partition cluster data... ");
            }
        }

        private void migratePartitionToNewOwner(int partitionId, Address[] replicas, Address currentOwner, Address newOwner) {
            MigrationInfo info = new MigrationInfo(partitionId, currentOwner, newOwner);
            MigrateTask migrateTask = new MigrateTask(info, replicas);
            boolean offered = migrationQueue.offer(migrateTask);
            if (!offered) {
                logger.severe("Failed to offer: " + migrateTask);
            }
        }

        private void assignNewPartitionOwner(int partitionId, Address[] replicas, InternalPartitionImpl currentPartition,
                                             Address newOwner) {
            currentPartition.setReplicaAddresses(replicas);
            MigrationInfo migrationInfo = new MigrationInfo(partitionId, null, newOwner);
            sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
        }

        private boolean isMigrationAllowed() {
            if (isMigrationActive()) {
                return true;
            }
            migrationQueue.add(this);
            return false;
        }
    }

    private class MigrateTask implements Runnable {
        final MigrationInfo migrationInfo;
        final Address[] addresses;

        public MigrateTask(MigrationInfo migrationInfo, Address[] addresses) {
            this.migrationInfo = migrationInfo;
            this.addresses = addresses;
            final MemberImpl masterMember = getMasterMember();
            if (masterMember != null) {
                migrationInfo.setMasterUuid(masterMember.getUuid());
                migrationInfo.setMaster(masterMember.getAddress());
            }
        }

        @Override
        public void run() {
            if (!node.isActive() || !node.isMaster()) {
                return;
            }
            final MigrationRequestOperation migrationRequestOp = new MigrationRequestOperation(migrationInfo);
            try {
                MigrationInfo info = migrationInfo;
                InternalPartitionImpl partition = partitions[info.getPartitionId()];
                Address owner = partition.getOwnerOrNull();
                if (owner == null) {
                    logger.severe("ERROR: partition owner is not set! -> partitionId=" + info.getPartitionId()
                            + " , " + partition + " -VS- " + info);
                    return;
                }
                if (!owner.equals(info.getSource())) {
                    logger.severe("ERROR: partition owner is not the source of migration! -> partitionId="
                            + info.getPartitionId() + " , " + partition + " -VS- " + info + " found owner=" + owner);
                    return;
                }
                sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
                Boolean result;
                MemberImpl fromMember = getMember(migrationInfo.getSource());
                if (logger.isFinestEnabled()) {
                    logger.finest("Starting Migration: " + migrationInfo);
                }
                if (fromMember == null) {
                    // Partition is lost! Assign new owner and exit.
                    logger.warning("Partition is lost! Assign new owner and exit... partitionId=" + info.getPartitionId());
                    result = Boolean.TRUE;
                } else {
                    result = executeMigrateOperation(migrationRequestOp, fromMember);
                }
                processMigrationResult(result);
            } catch (Throwable t) {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] while executing " + migrationRequestOp);
                logger.finest(t);
                migrationOperationFailed();
            }
        }

        private void processMigrationResult(Boolean result) {
            if (Boolean.TRUE.equals(result)) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Finished Migration: " + migrationInfo);
                }
                migrationOperationSucceeded();
            } else {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Migration failed: " + migrationInfo);
                migrationOperationFailed();
            }
        }

        private Boolean executeMigrateOperation(MigrationRequestOperation migrationRequestOp, MemberImpl fromMember) {
            Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, migrationRequestOp,
                    migrationInfo.getSource())
                    .setCallTimeout(partitionMigrationTimeout)
                    .setTryPauseMillis(DEFAULT_PAUSE_MILLIS).invoke();

            try {
                Object response = future.get();
                return (Boolean) nodeEngine.toObject(response);
            } catch (Throwable e) {
                final Level level = node.isActive() && migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Failed migration from " + fromMember + " for " + migrationRequestOp.getMigrationInfo(), e);
            }
            return Boolean.FALSE;
        }

        private void migrationOperationFailed() {
            lock.lock();
            try {
                addCompletedMigration(migrationInfo);
                finalizeActiveMigration(migrationInfo);
                publishPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
            sendMigrationEvent(migrationInfo, MigrationStatus.FAILED);

            // migration failed, re-execute RepartitioningTask when all other migration tasks are done
            migrationQueue.add(new RepartitioningTask());
        }

        private void migrationOperationSucceeded() {
            lock.lock();
            try {
                final int partitionId = migrationInfo.getPartitionId();
                InternalPartitionImpl partition = partitions[partitionId];
                partition.setReplicaAddresses(addresses);
                addCompletedMigration(migrationInfo);
                finalizeActiveMigration(migrationInfo);
                syncPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + "migrationInfo=" + migrationInfo + '}';
        }
    }

    private class MigrationThread extends Thread implements Runnable {
        private final long sleepTime = Math.max(250L, partitionMigrationInterval);
        private volatile boolean migrating;

        MigrationThread(Node node) {
            super(node.getHazelcastThreadGroup().getInternalThreadGroup(),
                    node.getHazelcastThreadGroup().getThreadNamePrefix("migration"));
        }

        @Override
        public void run() {
            try {
                while (!isInterrupted()) {
                    doRun();
                }
            } catch (InterruptedException e) {
                if (logger.isFinestEnabled()) {
                    logger.finest("MigrationThread is interrupted: " + e.getMessage());
                }
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } finally {
                migrationQueue.clear();
            }
        }

        private void doRun() throws InterruptedException {
            for (; ; ) {
                if (!isMigrationActive()) {
                    break;
                }
                Runnable r = migrationQueue.poll(1, TimeUnit.SECONDS);
                if (r == null) {
                    break;
                }

                processTask(r);
                if (partitionMigrationInterval > 0) {
                    Thread.sleep(partitionMigrationInterval);
                }
            }
            boolean hasNoTasks = migrationQueue.isEmpty();
            if (hasNoTasks) {
                if (migrating) {
                    migrating = false;
                    logger.info("All migration tasks have been completed, queues are empty.");
                }
                evictCompletedMigrations();
                Thread.sleep(sleepTime);
            } else if (!isMigrationActive()) {
                Thread.sleep(sleepTime);
            }
        }

        boolean processTask(Runnable r) {
            if (r == null || isInterrupted()) {
                return false;
            }
            migrating = (r instanceof MigrateTask);
            try {
                r.run();
            } catch (Throwable t) {
                logger.warning(t);
            }
            return true;
        }

        void stopNow() {
            migrationQueue.clear();
            interrupt();
        }

        boolean isMigrating() {
            return migrating;
        }
    }

    private static final class LocalPartitionListener implements PartitionListener {
        final Address thisAddress;
        final InternalPartitionServiceImpl partitionService;

        private LocalPartitionListener(InternalPartitionServiceImpl partitionService, Address thisAddress) {
            this.thisAddress = thisAddress;
            this.partitionService = partitionService;
        }

        @Override
        public void replicaChanged(PartitionReplicaChangeEvent event) {
            final int partitionId = event.getPartitionId();
            final int replicaIndex = event.getReplicaIndex();
            final Address newAddress = event.getNewAddress();
            final Address oldAddress = event.getOldAddress();
            final PartitionReplicaChangeReason reason = event.getReason();

            final boolean initialAssignment = event.getOldAddress() == null;

            if (replicaIndex > 0) {
                // backup replica owner changed!
                if (thisAddress.equals(oldAddress)) {
                    clearPartition(partitionId, replicaIndex);
                } else if (thisAddress.equals(newAddress)) {
                    synchronizePartition(partitionId, replicaIndex, reason, initialAssignment);
                }
            } else {
                if (!initialAssignment && thisAddress.equals(newAddress)) {
                    // it is possible that I might become owner while waiting for sync request from the previous owner.
                    // I should check whether if have failed to get backups from the owner and lost the partition for
                    // some backups.
                    promoteFromBackups(partitionId, reason, oldAddress);
                }
                partitionService.cancelReplicaSync(partitionId);
            }

            Node node = partitionService.node;
            if (replicaIndex == 0 && newAddress == null && node.isActive() && node.joined()) {
                logOwnerOfPartitionIsRemoved(event);
            }
            if (node.isMaster()) {
                partitionService.stateVersion.incrementAndGet();
            }
        }

        private void clearPartition(final int partitionId, final int oldReplicaIndex) {
            NodeEngine nodeEngine = partitionService.nodeEngine;
            ClearReplicaOperation op = new ClearReplicaOperation(oldReplicaIndex);
            op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setService(partitionService);
            nodeEngine.getOperationService().executeOperation(op);
        }

        private void synchronizePartition(int partitionId, int replicaIndex,
                                          PartitionReplicaChangeReason reason, boolean initialAssignment) {
            // if not initialized yet, no need to sync, since this is the initial partition assignment
            if (partitionService.initialized) {
                long delayMillis = 0L;
                if (replicaIndex > 1) {
                    // immediately trigger replica synchronization for the first backups
                    // postpone replica synchronization for greater backups to a later time
                    // high priority is 1st backups
                    delayMillis = (long) (REPLICA_SYNC_RETRY_DELAY + (Math.random() * DEFAULT_REPLICA_SYNC_DELAY));
                }

                resetReplicaVersion(partitionId, replicaIndex, reason, initialAssignment);
                partitionService.triggerPartitionReplicaSync(partitionId, replicaIndex, delayMillis);
            }
        }

        private void resetReplicaVersion(int partitionId, int replicaIndex,
                                         PartitionReplicaChangeReason reason, boolean initialAssignment) {
            NodeEngine nodeEngine = partitionService.nodeEngine;
            ResetReplicaVersionOperation op = new ResetReplicaVersionOperation(reason, initialAssignment);
            op.setPartitionId(partitionId).setReplicaIndex(replicaIndex)
                    .setNodeEngine(nodeEngine).setService(partitionService);
            nodeEngine.getOperationService().executeOperation(op);
        }

        private void promoteFromBackups(int partitionId, PartitionReplicaChangeReason reason, Address oldAddress) {
            NodeEngine nodeEngine = partitionService.nodeEngine;
            PromoteFromBackupOperation op = new PromoteFromBackupOperation(reason, oldAddress);
            op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setService(partitionService);
            nodeEngine.getOperationService().executeOperation(op);
        }

        private void logOwnerOfPartitionIsRemoved(PartitionReplicaChangeEvent event) {
            String warning = "Owner of partition is being removed! "
                    + "Possible data loss for partitionId=" + event.getPartitionId() + " , " + event;
            partitionService.logger.warning(warning);
        }
    }

    private static class ReplicaSyncEntryProcessor implements ScheduledEntryProcessor<Integer, ReplicaSyncInfo> {

        final InternalPartitionServiceImpl partitionService;

        ReplicaSyncEntryProcessor(InternalPartitionServiceImpl partitionService) {
            this.partitionService = partitionService;
        }

        @Override
        public void process(EntryTaskScheduler<Integer, ReplicaSyncInfo> scheduler,
                            Collection<ScheduledEntry<Integer, ReplicaSyncInfo>> entries) {

            for (ScheduledEntry<Integer, ReplicaSyncInfo> entry : entries) {
                ReplicaSyncInfo syncInfo = entry.getValue();
                int partitionId = syncInfo.partitionId;
                if (partitionService.replicaSyncRequests.compareAndSet(partitionId, syncInfo, null)) {
                    partitionService.releaseReplicaSyncPermit();
                }

                InternalPartitionImpl partition = partitionService.getPartitionImpl(partitionId);
                int currentReplicaIndex = partition.getReplicaIndex(partitionService.node.getThisAddress());
                if (currentReplicaIndex > 0) {
                    partitionService.triggerPartitionReplicaSync(partitionId, currentReplicaIndex, 0L);
                }
            }
        }
    }
}
