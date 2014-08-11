/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.SystemLogService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.partition.PartitionRuntimeState;
import com.hazelcast.partition.PartitionServiceProxy;
import com.hazelcast.partition.membergroup.MemberGroup;
import com.hazelcast.partition.membergroup.MemberGroupFactory;
import com.hazelcast.partition.membergroup.MemberGroupFactoryFactory;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.Clock;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.core.MigrationEvent.MigrationStatus;
import static com.hazelcast.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;

public class InternalPartitionServiceImpl implements InternalPartitionService, ManagedService,
        EventPublishingService<MigrationEvent, MigrationListener> {

    private static final String EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT = "Partition state sync invocation timed out";
    private static final ExceptionHandler PARTITION_STATE_SYNC_TIMEOUT_HANDLER =
            logAllExceptions(EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT, Level.INFO);

    private static final int DEFAULT_PAUSE_MILLIS = 1000;
    private static final int DEFAULT_SLEEP_MILLIS = 10;
    private static final float DEFAULT_MIGRATION_TIMEOUT_MULTIPLICATOR = 1.5f;
    private static final long MAX_ACTIVATION_DELAY = 1000L;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final int partitionCount;
    private final InternalPartitionImpl[] partitions;
    private final PartitionReplicaVersions[] replicaVersions;
    private final AtomicReferenceArray<ReplicaSyncInfo> replicaSyncRequests;
    private final EntryTaskScheduler<Integer, ReplicaSyncInfo> replicaSyncScheduler;
    private final AtomicInteger replicaSyncProcessCount = new AtomicInteger();
    private final MigrationThread migrationThread;
    private final long partitionMigrationInterval;
    private final long partitionMigrationTimeout;
    private final PartitionStateGenerator partitionStateGenerator;
    private final MemberGroupFactory memberGroupFactory;
    private final PartitionServiceProxy proxy;
    private final Lock lock = new ReentrantLock();
    private final AtomicInteger stateVersion = new AtomicInteger();
    private final BlockingQueue<Runnable> migrationQueue = new LinkedBlockingQueue<Runnable>();
    private final AtomicBoolean migrationActive = new AtomicBoolean(true);
    private final AtomicLong lastRepartitionTime = new AtomicLong();
    private final SystemLogService systemLogService;

    // can be read and written concurrently...
    private volatile int memberGroupsSize;

    // updates will be done under lock, but reads will be multithreaded.
    private volatile boolean initialized;

    // updates will be done under lock, but reads will be multithreaded.
    private final ConcurrentMap<Integer, MigrationInfo> activeMigrations
            = new ConcurrentHashMap<Integer, MigrationInfo>(3, 0.75f, 1);

    // both reads and updates will be done under lock!
    private final LinkedList<MigrationInfo> completedMigrations = new LinkedList<MigrationInfo>();

    public InternalPartitionServiceImpl(Node node) {
        this.partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(InternalPartitionService.class);
        this.systemLogService = node.getSystemLogService();
        this.partitions = new InternalPartitionImpl[partitionCount];
        PartitionListener partitionListener = new LocalPartitionListener(this, node.getThisAddress());
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new InternalPartitionImpl(i, partitionListener);
        }
        replicaVersions = new PartitionReplicaVersions[partitionCount];
        for (int i = 0; i < replicaVersions.length; i++) {
            replicaVersions[i] = new PartitionReplicaVersions(i);
        }

        memberGroupFactory = MemberGroupFactoryFactory.newMemberGroupFactory(node.getConfig().getPartitionGroupConfig());
        partitionStateGenerator = new PartitionStateGeneratorImpl();

        partitionMigrationInterval = node.groupProperties.PARTITION_MIGRATION_INTERVAL.getLong() * DEFAULT_PAUSE_MILLIS;
        // partitionMigrationTimeout is 1.5 times of real timeout
        long defaultMigrationTimeout = node.groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong();
        partitionMigrationTimeout = (long) (defaultMigrationTimeout * DEFAULT_MIGRATION_TIMEOUT_MULTIPLICATOR);

        migrationThread = new MigrationThread(node);
        proxy = new PartitionServiceProxy(this);

        replicaSyncRequests = new AtomicReferenceArray<ReplicaSyncInfo>(new ReplicaSyncInfo[partitionCount]);
        ScheduledExecutorService scheduledExecutor = nodeEngine.getExecutionService().getDefaultScheduledExecutor();
        replicaSyncScheduler = EntryTaskSchedulerFactory.newScheduler(scheduledExecutor,
                new ReplicaSyncEntryProcessor(this), ScheduleType.SCHEDULE_IF_NEW);
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


        int backupSyncCheckInterval = node.groupProperties.PARTITION_BACKUP_SYNC_INTERVAL.getInteger();
        if (backupSyncCheckInterval <= 0) {
            backupSyncCheckInterval = 1;
        }
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
    public Address getPartitionOwnerOrWait(int partition) throws InterruptedException {
        Address owner = getPartitionOwner(partition);
        while (owner == null) {
            Thread.sleep(100);
            owner = getPartitionOwner(partition);
        }
        return owner;
    }

    private void notifyMasterToAssignPartitions() {
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
                    partition.setPartitionInfo(replicas);
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
                    Collection<MemberImpl> members = node.clusterService.getMemberList();
                    PartitionStateOperation op = new PartitionStateOperation(createPartitionState(members));
                    nodeEngine.getOperationService().send(op, member.getAddress());
                }
            } finally {
                lock.unlock();
            }
        }
    }

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
            promoteFromBackups(deadAddress, thisAddress);

            if (node.isMaster() && initialized) {
                migrationQueue.add(new RepartitioningTask());
            }

            // Add a delay before activating migration, to give other nodes time to notice the dead one.
            long migrationActivationDelay = node.groupProperties.CONNECTION_MONITOR_INTERVAL.getLong()
                    * node.groupProperties.CONNECTION_MONITOR_MAX_FAULTS.getInteger() * 5;

            long callTimeout = node.groupProperties.OPERATION_CALL_TIMEOUT_MILLIS.getLong();
            // delay should be smaller than call timeout, otherwise operations may fail because of invalid partition table
            migrationActivationDelay = Math.min(migrationActivationDelay, callTimeout / 2);
            migrationActivationDelay = Math.max(migrationActivationDelay, MAX_ACTIVATION_DELAY);

            nodeEngine.getExecutionService().schedule(new Runnable() {
                @Override
                public void run() {
                    resumeMigration();
                }
            }, migrationActivationDelay, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    private void promoteFromBackups(Address deadAddress, Address thisAddress) {
        for (InternalPartitionImpl partition : partitions) {
            boolean promote = false;
            if (deadAddress.equals(partition.getOwnerOrNull()) && thisAddress.equals(partition.getReplicaAddress(1))) {
                promote = true;
                partition.setMigrating(true);
            }
            // shift partition table up.
            partition.onDeadAddress(deadAddress);
            // safety check!
            if (partition.onDeadAddress(deadAddress)) {
                throw new IllegalStateException("Duplicate address found in partition replicas!");
            }

            if (promote) {
                final Operation op = new PromoteFromBackupOperation();
                op.setPartitionId(partition.getPartitionId())
                        .setNodeEngine(nodeEngine)
                        .setValidateTarget(false)
                        .setService(this);
                nodeEngine.getOperationService().executeOperation(op);
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
            final long clusterTime = node.getClusterService().getClusterTime();
            ILogger logger = node.getLogger(PartitionRuntimeState.class);
            return new PartitionRuntimeState(
                    logger, memberInfos, partitions, migrationInfos, clusterTime, stateVersion.get());
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

        if (!migrationActive.get()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }

        lock.lock();
        try {
            Collection<MemberImpl> members = node.clusterService.getMemberList();
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
        syncPartitionRuntimeState(node.clusterService.getMemberList());
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

            try {
                waitWithDeadline(calls, 3, TimeUnit.SECONDS, PARTITION_STATE_SYNC_TIMEOUT_HANDLER);
            } catch (TimeoutException e) {
                logger.finest(e);
            }
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
            finalizeActiveMigration(completedMigration);
        }
        if (!activeMigrations.isEmpty()) {
            final MemberImpl masterMember = getMasterMember();
            rollbackActiveMigrationsFromPreviousMaster(masterMember.getUuid());
        }

        allocateReplicas(state);
    }

    private void allocateReplicas(PartitionInfo[] state) {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartitionImpl partition = partitions[partitionId];
            Address[] replicas = state[partitionId].getReplicaAddresses();
            partition.setPartitionInfo(replicas);
        }
    }

    private void filterAndLogUnknownAddressesInPartitionTable(Address sender, PartitionInfo[] state) {
        final Set<Address> unknownAddresses = new HashSet<Address>();
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            PartitionInfo partitionInfo = state[partitionId];
            InternalPartitionImpl currentPartition = partitions[partitionId];
            searchUnknownAddressesInPartitionTable(sender, unknownAddresses, partitionId, partitionInfo);
            // backup replicas will be assigned after active migrations are finalized.
            currentPartition.setOwner(partitionInfo.getReplicaAddress(0));
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
                                    + sender + ". It has probably already left the cluster. Partition: "
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
                //todo: 2 different branches with the same content.
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
                    String message = "Something is seriously wrong! There are two migration client for the "
                            + "same partition! First-> " + currentMigrationInfo + ", Second -> " + migrationInfo;
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

    public Collection<MigrationInfo> getActiveMigrations() {
        return Collections.unmodifiableCollection(activeMigrations.values());
    }

    private void addCompletedMigration(MigrationInfo migrationInfo) {
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

    private void clearPartitionReplica(final int partitionId, final int replicaIndex) {
        ClearReplicaOperation op = new ClearReplicaOperation();
        op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setService(this);
        nodeEngine.getOperationService().executeOperation(op);
    }


    void triggerPartitionReplicaSync(int partitionId, int replicaIndex) {
        syncPartitionReplica(partitionId, replicaIndex, 0L, false);
    }

    void forcePartitionReplicaSync(int partitionId, int replicaIndex) {
        syncPartitionReplica(partitionId, replicaIndex, 0L, true);
    }

    void schedulePartitionReplicaSync(int partitionId, int replicaIndex, long delayMillis) {
        syncPartitionReplica(partitionId, replicaIndex, delayMillis, true);
    }

    private void syncPartitionReplica(int partitionId, int replicaIndex, long delayMillis, boolean force) {
        if (replicaIndex < 0 || replicaIndex > InternalPartition.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Invalid replica index: " + replicaIndex);
        }
        final InternalPartitionImpl partitionImpl = getPartition(partitionId);
        final Address target = partitionImpl.getOwnerOrNull();
        if (target != null) {
            if (checkSyncPartitionTarget(partitionId, replicaIndex, force, partitionImpl, target)) {
                return;
            }

            final ReplicaSyncRequest syncRequest = new ReplicaSyncRequest();
            syncRequest.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            final ReplicaSyncInfo currentSyncInfo = replicaSyncRequests.get(partitionId);
            final ReplicaSyncInfo syncInfo = new ReplicaSyncInfo(partitionId, replicaIndex, target);
            boolean sendRequest = false;
            if (currentSyncInfo == null) {
                sendRequest = replicaSyncRequests.compareAndSet(partitionId, null, syncInfo);
            } else if (currentSyncInfo.requestTime < (Clock.currentTimeMillis() - 10000)
                    || nodeEngine.getClusterService().getMember(currentSyncInfo.target) == null) {
                sendRequest = replicaSyncRequests.compareAndSet(partitionId, currentSyncInfo, syncInfo);
            } else if (force) {
                replicaSyncRequests.set(partitionId, syncInfo);
                sendRequest = true;
            }

            if (sendRequest) {
                fireSyncReplicaRequest(partitionId, replicaIndex, delayMillis, target, syncRequest, syncInfo);
            }
        } else {
            logger.warning("Sync replica target is null, no need to sync -> partition: " + partitionId
                    + ", replica: " + replicaIndex);
        }
    }

    private void fireSyncReplicaRequest(int partitionId, int replicaIndex, long delayMillis, Address target,
                                        ReplicaSyncRequest syncRequest, ReplicaSyncInfo syncInfo) {
        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync replica request to -> " + target + "; for partition: " + partitionId
                    + ", replica: " + replicaIndex);
        }

        replicaSyncScheduler.cancel(partitionId);
        if (delayMillis <= 0) {
            replicaSyncScheduler.schedule(DEFAULT_REPLICA_SYNC_DELAY, partitionId, syncInfo);
            nodeEngine.getOperationService().send(syncRequest, target);
        } else {
            replicaSyncScheduler.schedule(delayMillis, partitionId, syncInfo);
        }
    }

    private boolean checkSyncPartitionTarget(int partitionId, int replicaIndex, boolean force,
                                             InternalPartitionImpl partitionImpl, Address target) {

        if (target.equals(nodeEngine.getThisAddress())) {
            if (force) {
                Address thisAddress = node.nodeEngine.getThisAddress();
                throw new IllegalStateException("Replica target cannot be this node -> thisNode: " + thisAddress
                        + " partitionId: " + partitionId + ", replicaIndex: " + replicaIndex
                        + ", partition-info: " + partitionImpl);
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("This node is now owner of partition, cannot sync replica -> partitionId: " + partitionId
                            + ", replicaIndex: " + replicaIndex + ", partition-info: " + partitionImpl);
                }
                return true;
            }
        }
        return false;
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

    private InternalPartitionImpl getPartitionImpl(int partitionId) {
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
        int sleep = DEFAULT_PAUSE_MILLIS;
        while (timeoutInMillis > 0) {
            while (timeoutInMillis > 0 && shouldWaitMigrationOrBackups(Level.INFO)) {
                timeoutInMillis = sleepWithBusyWait(timeoutInMillis, sleep);
            }
            if (timeoutInMillis <= 0) {
                break;
            }

            if (node.isMaster()) {
                syncPartitionRuntimeState();
            } else {
                timeoutInMillis = waitForOngoingMigrations(timeoutInMillis, sleep);
                if (timeoutInMillis <= 0) {
                    break;
                }
            }

            long start = Clock.currentTimeMillis();
            boolean ok = checkReplicaSyncState();
            timeoutInMillis -= (Clock.currentTimeMillis() - start);
            if (ok) {
                logger.finest("Replica sync state before shutdown is OK");
                return true;
            } else {
                if (timeoutInMillis <= 0) {
                    break;
                }
                logger.info("Some backup replicas are inconsistent with primary, waiting for synchronization. Timeout: "
                        + timeoutInMillis + "ms");
                timeoutInMillis = sleepWithBusyWait(timeoutInMillis, sleep);
            }
        }
        return false;
    }

    private long waitForOngoingMigrations(long timeoutInMillis, int sleep) {
        long timeout = timeoutInMillis;
        while (timeout > 0 && hasOnGoingMigrationMaster(Level.WARNING)) {
            // ignore elapsed time during master inv.
            logger.info("Waiting for the master node to complete remaining migrations!");
            timeout = sleepWithBusyWait(timeout, sleep);
        }
        return timeout;
    }

    private long sleepWithBusyWait(long timeoutInMillis, int sleep) {
        try {
            //noinspection BusyWait
            Thread.sleep(sleep);
        } catch (InterruptedException ie) {
            Logger.getLogger(InternalPartitionServiceImpl.class).finest("Busy wait interrupted", ie);
        }
        return timeoutInMillis - sleep;
    }

    @Override
    public boolean isMemberStateSafe() {
        if (hasOnGoingMigrationLocal()) {
            return false;
        }
        if (!node.isMaster()) {
            if (hasOnGoingMigrationMaster(Level.OFF)) {
                return false;
            }
        }
        return isReplicaInSyncState();
    }

    @Override
    public boolean hasOnGoingMigration() {
        return hasOnGoingMigrationLocal() || (!node.isMaster() && hasOnGoingMigrationMaster(Level.FINEST));
    }

    private boolean hasOnGoingMigrationMaster(Level level) {
        Operation operation = new HasOngoingMigration();
        Address masterAddress = node.getMasterAddress();
        OperationService operationService = nodeEngine.getOperationService();
        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, masterAddress);
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

    boolean hasOnGoingMigrationLocal() {
        return !activeMigrations.isEmpty() || !migrationQueue.isEmpty()
                || !migrationActive.get()
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
        for (int i = 0; i < futures.size(); i++) {
            final Future future = futures.get(i);
            final boolean sync = getFutureResult(future, 10, TimeUnit.SECONDS);
            if (!sync) {
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
        op.setResponseHandler(ResponseHandlerFactory
                .createErrorLoggingResponseHandler(node.getLogger(IsReplicaVersionSync.class)));
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
        final Callback<Object> callback = new Callback<Object>() {
            @Override
            public void notify(Object object) {
                if (Boolean.FALSE.equals(object)) {
                    ok.compareAndSet(true, false);
                } else if (object instanceof Throwable) {
                    ok.compareAndSet(true, false);
                }
                s.release();
            }
        };
        int notOwnedCount = syncReplicaVersion(thisAddress, s, ok, callback);
        s.release(notOwnedCount);
        try {
            if (ok.get()) {
                return s.tryAcquire(partitionCount, DEFAULT_SLEEP_MILLIS, TimeUnit.SECONDS) && ok.get();
            } else {
                return false;
            }
        } catch (InterruptedException ignored) {
            return false;
        }
    }

    private int syncReplicaVersion(Address thisAddress, Semaphore s, AtomicBoolean ok, Callback<Object> callback) {
        int notOwnedCount = 0;
        ILogger responseLogger = node.getLogger(SyncReplicaVersion.class);
        for (InternalPartitionImpl partition : partitions) {
            Address owner = partition.getOwnerOrNull();
            if (thisAddress.equals(owner)) {
                if (partition.getReplicaAddress(1) != null) {
                    SyncReplicaVersion op = new SyncReplicaVersion(1, callback);
                    op.setService(this);
                    op.setNodeEngine(nodeEngine);
                    op.setResponseHandler(ResponseHandlerFactory.createErrorLoggingResponseHandler(responseLogger));
                    op.setPartitionId(partition.getPartitionId());
                    nodeEngine.getOperationService().executeOperation(op);
                } else {
                    ok.set(false);
                    s.release();
                }
            } else {
                if (owner == null) {
                    ok.set(false);
                }
                notOwnedCount++;
            }
        }
        return notOwnedCount;
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
                    logger.log(level, "Should take backup of partition: " + partition.getPartitionId());
                }
                return true;
            } else {
                int replicaSyncProcesses = replicaSyncProcessCount.get();
                if (replicaSyncProcesses > 0) {
                    if (logger.isLoggable(level)) {
                        logger.log(level, "Processing replica sync client: " + replicaSyncProcesses);
                    }
                    return true;
                }
            }
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
        int hash = key.getPartitionHash();
        if (hash == Integer.MIN_VALUE) {
            return 0;
        } else {
            return Math.abs(hash) % partitionCount;
        }
    }

    @Override
    public final int getPartitionId(Object key) {
        return getPartitionId(nodeEngine.toData(key));
    }

    @Override
    public final int getPartitionCount() {
        return partitionCount;
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
            triggerPartitionReplicaSync(partitionId, replicaIndex);
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
    public void setPartitionReplicaVersions(int partitionId, long[] versions) {
        replicaVersions[partitionId].reset(versions);
    }

    @Override
    public void clearPartitionReplicaVersions(int partitionId) {
        replicaVersions[partitionId].clear();
    }

    // called in operation threads
    void finalizeReplicaSync(int partitionId, long[] versions) {
        setPartitionReplicaVersions(partitionId, versions);
        replicaSyncRequests.set(partitionId, null);
        replicaSyncScheduler.cancel(partitionId);
    }

    boolean incrementReplicaSyncProcessCount() {
        int c = replicaSyncProcessCount.get();
        if (c >= MAX_PARALLEL_REPLICATIONS) {
            return false;
        }
        c = replicaSyncProcessCount.incrementAndGet();
        if (c >= MAX_PARALLEL_REPLICATIONS) {
            replicaSyncProcessCount.decrementAndGet();
            return false;
        }
        return true;
    }

    void decrementReplicaSyncProcessCount() {
        replicaSyncProcessCount.decrementAndGet();
    }

    @Override
    public Map<Address, List<Integer>> getMemberPartitionsMap() {
        final int members = node.getClusterService().getSize();
        Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(members);
        for (int i = 0; i < partitionCount; i++) {
            Address owner;
            while ((owner = getPartitionOwner(i)) == null) {
                try {
                    Thread.sleep(DEFAULT_SLEEP_MILLIS);
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
        lock.lock();
        try {
            initialized = false;
            for (InternalPartitionImpl partition : partitions) {
                for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                    partition.setReplicaAddress(i, null);
                    partition.setMigrating(false);
                }
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
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        eventService.publishEvent(SERVICE_NAME, registrations, event, event.getPartitionId());
    }

    @Override
    public String addMigrationListener(MigrationListener listener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(SERVICE_NAME, SERVICE_NAME, listener);
        return registration.getId();
    }

    @Override
    public boolean removeMigrationListener(String registrationId) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
    }

    @Override
    public void dispatchEvent(MigrationEvent migrationEvent, MigrationListener migrationListener) {
        final MigrationStatus status = migrationEvent.getStatus();
        switch (status) {
            case STARTED:
                migrationListener.migrationStarted(migrationEvent);
                break;
            case COMPLETED:
                migrationListener.migrationCompleted(migrationEvent);
                break;
            case FAILED:
                migrationListener.migrationFailed(migrationEvent);
                break;
            default:
                throw new IllegalArgumentException("Not a known MigrationStatus: " + status);
        }
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

    private class SendClusterStateTask implements Runnable {
        @Override
        public void run() {
            if (node.isMaster() && node.isActive()) {
                if (!migrationQueue.isEmpty() && migrationActive.get()) {
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
                final Address thisAddress = node.getThisAddress();
                for (final InternalPartitionImpl partition : partitions) {
                    if (thisAddress.equals(partition.getOwnerOrNull())) {
                        for (int index = 1; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                            if (partition.getReplicaAddress(index) != null) {
                                SyncReplicaVersion op = new SyncReplicaVersion(index, null);
                                op.setService(InternalPartitionServiceImpl.this);
                                op.setNodeEngine(nodeEngine);
                                op.setResponseHandler(ResponseHandlerFactory
                                        .createErrorLoggingResponseHandler(node.getLogger(SyncReplicaVersion.class)));
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
                    Collection<MemberImpl> members = node.getClusterService().getMemberList();
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
                            migrationCount++;
                            migratePartitionToNewOwner(partitionId, replicas, currentOwner, newOwner);
                        } else {
                            currentPartition.setPartitionInfo(replicas);
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
            MigrateTask migrateTask = new MigrateTask(info, new BackupMigrationTask(partitionId, replicas));
            boolean offered = migrationQueue.offer(migrateTask);
            if (!offered) {
                logger.severe("Failed to offer: " + migrateTask);
            }
        }

        private void assignNewPartitionOwner(int partitionId, Address[] replicas, InternalPartitionImpl currentPartition,
                                             Address newOwner) {
            currentPartition.setPartitionInfo(replicas);
            MigrationInfo migrationInfo = new MigrationInfo(partitionId, null, newOwner);
            sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
        }

        private boolean isMigrationAllowed() {
            if (migrationActive.get()) {
                return true;
            }
            migrationQueue.add(this);
            return false;
        }
    }

    private class BackupMigrationTask implements Runnable {
        final int partitionId;
        final Address[] replicas;

        BackupMigrationTask(int partitionId, Address[] replicas) {
            this.partitionId = partitionId;
            this.replicas = replicas;
        }

        @Override
        public void run() {
            lock.lock();
            try {
                InternalPartitionImpl currentPartition = partitions[partitionId];
                for (int index = 1; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                    currentPartition.setReplicaAddress(index, replicas[index]);
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("BackupMigrationTask{");
            sb.append("partitionId=").append(partitionId);
            sb.append("replicas=").append(Arrays.toString(replicas));
            sb.append('}');
            return sb.toString();
        }
    }

    private class MigrateTask implements Runnable {
        final MigrationInfo migrationInfo;
        final BackupMigrationTask backupTask;

        MigrateTask(MigrationInfo migrationInfo, BackupMigrationTask backupTask) {
            this.migrationInfo = migrationInfo;
            this.backupTask = backupTask;
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
                    logger.severe("ERROR: partition owner is not set! -> "
                            + partition + " -VS- " + info);
                    return;
                }
                if (!owner.equals(info.getSource())) {
                    logger.severe("ERROR: partition owner is not the source of migration! -> "
                            + partition + " -VS- " + info + " found owner:" + owner);
                    return;
                }
                sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
                Boolean result = Boolean.FALSE;
                MemberImpl fromMember = getMember(migrationInfo.getSource());
                if (logger.isFinestEnabled()) {
                    logger.finest("Started Migration : " + migrationInfo);
                }
                systemLogService.logPartition("Started Migration : " + migrationInfo);
                if (fromMember == null) {
                    // Partition is lost! Assign new owner and exit.
                    logger.warning("Partition is lost! Assign new owner and exit...");
                    result = Boolean.TRUE;
                } else {
                    result = executeMigrateOperation(migrationRequestOp, fromMember);
                }
                processMigrationResult(result);
            } catch (Throwable t) {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] while executing " + migrationRequestOp);
                logger.finest(t);
                migrationTaskFailed();
            }
        }

        private void processMigrationResult(Boolean result) {
            if (Boolean.TRUE.equals(result)) {
                String message = "Finished Migration: " + migrationInfo;
                if (logger.isFinestEnabled()) {
                    logger.finest(message);
                }
                systemLogService.logPartition(message);
                processMigrationResult();
            } else {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Migration failed: " + migrationInfo);
                migrationTaskFailed();
            }
        }

        private Boolean executeMigrateOperation(MigrationRequestOperation migrationRequestOp, MemberImpl fromMember) {
            Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                    migrationRequestOp, migrationInfo.getSource()).setTryPauseMillis(DEFAULT_PAUSE_MILLIS).invoke();

            try {
                Object response = future.get(partitionMigrationTimeout, TimeUnit.SECONDS);
                return (Boolean) nodeEngine.toObject(response);
            } catch (Throwable e) {
                final Level level = node.isActive() && migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Failed migration from " + fromMember, e);
            }
            return Boolean.FALSE;
        }

        private void migrationTaskFailed() {
            systemLogService.logPartition("Migration failed: " + migrationInfo);
            lock.lock();
            try {
                addCompletedMigration(migrationInfo);
                finalizeActiveMigration(migrationInfo);
                syncPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
            sendMigrationEvent(migrationInfo, MigrationStatus.FAILED);

            // migration failed, clear current pending migration tasks and re-execute RepartitioningTask
            migrationQueue.clear();
            migrationQueue.add(new RepartitioningTask());
        }

        private void processMigrationResult() {
            lock.lock();
            try {
                final int partitionId = migrationInfo.getPartitionId();
                Address newOwner = migrationInfo.getDestination();
                InternalPartitionImpl partition = partitions[partitionId];
                partition.setOwner(newOwner);
                addCompletedMigration(migrationInfo);
                finalizeActiveMigration(migrationInfo);
                if (backupTask != null) {
                    backupTask.run();
                }
                syncPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("MigrateTask{");
            sb.append("migrationInfo=").append(migrationInfo);
            sb.append('}');
            return sb.toString();
        }
    }

    private class MigrationThread extends Thread implements Runnable {
        private final long sleepTime = Math.max(250L, partitionMigrationInterval);
        private volatile boolean migrating;

        MigrationThread(Node node) {
            super(node.threadGroup, node.getThreadNamePrefix("migration"));
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
            } finally {
                migrationQueue.clear();
            }
        }

        private void doRun() throws InterruptedException {
            for (;;) {
                if (!migrationActive.get()) {
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
            } else if (!migrationActive.get()) {
                Thread.sleep(sleepTime);
            }
        }

        boolean processTask(Runnable r) {
            if (r == null || isInterrupted()) {
                return false;
            }
            try {
                migrating = (r instanceof MigrateTask);
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
        private InternalPartitionServiceImpl partitionService;

        private LocalPartitionListener(InternalPartitionServiceImpl partitionService, Address thisAddress) {
            this.thisAddress = thisAddress;
            this.partitionService = partitionService;
        }

        @Override
        public void replicaChanged(PartitionReplicaChangeEvent event) {
            int replicaIndex = event.getReplicaIndex();
            Address newAddress = event.getNewAddress();
            if (replicaIndex > 0) {
                // backup replica owner changed!
                int partitionId = event.getPartitionId();
                if (thisAddress.equals(event.getOldAddress())) {
                    InternalPartitionImpl partition = partitionService.partitions[partitionId];
                    if (!partition.isOwnerOrBackup(thisAddress)) {
                        partitionService.clearPartitionReplica(partitionId, replicaIndex);
                    }
                } else if (thisAddress.equals(newAddress)) {
                    partitionService.forcePartitionReplicaSync(partitionId, replicaIndex);
                }
            }
            Node node = partitionService.node;
            if (replicaIndex == 0 && newAddress == null && node.isActive() && node.joined()) {
                logOwnerOfPartitionIsRemoved(event);
            }
            if (partitionService.node.isMaster()) {
                partitionService.stateVersion.incrementAndGet();
            }
        }

        private void logOwnerOfPartitionIsRemoved(PartitionReplicaChangeEvent event) {
            String warning = "Owner of partition is being removed! "
                    + "Possible data loss for partition[" + event.getPartitionId() + "]. " + event;
            partitionService.logger.warning(warning);
            partitionService.systemLogService.logWarningPartition(warning);
        }
    }

    private static class ReplicaSyncEntryProcessor implements ScheduledEntryProcessor<Integer, ReplicaSyncInfo> {

        private InternalPartitionServiceImpl partitionService;

        public ReplicaSyncEntryProcessor(InternalPartitionServiceImpl partitionService) {
            this.partitionService = partitionService;
        }

        @Override
        public void process(EntryTaskScheduler<Integer, ReplicaSyncInfo> scheduler,
                            Collection<ScheduledEntry<Integer, ReplicaSyncInfo>> entries) {
            for (ScheduledEntry<Integer, ReplicaSyncInfo> entry : entries) {
                ReplicaSyncInfo syncInfo = entry.getValue();
                if (partitionService.replicaSyncRequests.compareAndSet(entry.getKey(), syncInfo, null)) {
                    logRendingSyncReplicaRequest(syncInfo);
                    partitionService.triggerPartitionReplicaSync(syncInfo.partitionId, syncInfo.replicaIndex);
                }
            }
        }

        private void logRendingSyncReplicaRequest(ReplicaSyncInfo syncInfo) {
            ILogger logger = partitionService.logger;
            if (logger.isFinestEnabled()) {
                logger.finest("Re-sending sync replica request for partition: " + syncInfo.partitionId + ", replica: "
                        + syncInfo.replicaIndex);
            }
        }
    }
}
