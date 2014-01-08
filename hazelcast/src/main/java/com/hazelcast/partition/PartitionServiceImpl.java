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

package com.hazelcast.partition;

import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.SystemLogService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.Clock;
import com.hazelcast.util.scheduler.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.core.MigrationEvent.MigrationStatus;
import static java.lang.System.arraycopy;

public class PartitionServiceImpl implements PartitionService, ManagedService,
        EventPublishingService<MigrationEvent, MigrationListener> {

    public static final String SERVICE_NAME = "hz:core:partitionService";

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
    private volatile boolean initialized = false;

    // updates will be done under lock, but reads will be multithreaded.
    private final ConcurrentMap<Integer, MigrationInfo> activeMigrations = new ConcurrentHashMap<Integer, MigrationInfo>(3, 0.75f, 1);

    // both reads and updates will be done under lock!
    private final LinkedList<MigrationInfo> completedMigrations = new LinkedList<MigrationInfo>();

    public PartitionServiceImpl(final Node node) {
        this.partitionCount = node.groupProperties.PARTITION_COUNT.getInteger();
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(PartitionService.class);
        this.systemLogService = node.getSystemLogService();
        this.partitions = new InternalPartitionImpl[partitionCount];
        final PartitionListener partitionListener = new LocalPartitionListener(node.getThisAddress());
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new InternalPartitionImpl(i, partitionListener);
        }
        replicaVersions = new PartitionReplicaVersions[partitionCount];
        for (int i = 0; i < replicaVersions.length; i++) {
            replicaVersions[i] = new PartitionReplicaVersions(i);
        }

        memberGroupFactory = newMemberGroupFactory(node.getConfig().getPartitionGroupConfig());
        partitionStateGenerator = new PartitionStateGeneratorImpl();

        partitionMigrationInterval = node.groupProperties.PARTITION_MIGRATION_INTERVAL.getLong() * 1000;
        // partitionMigrationTimeout is 1.5 times of real timeout
        partitionMigrationTimeout = (long) (node.groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong() * 1.5f);

        migrationThread = new MigrationThread(node);
        proxy = new PartitionServiceProxy(this);

        replicaSyncRequests = new AtomicReferenceArray<ReplicaSyncInfo>(new ReplicaSyncInfo[partitionCount]);
        replicaSyncScheduler = EntryTaskSchedulerFactory.newScheduler(nodeEngine.getExecutionService().getScheduledExecutor(),
                new ReplicaSyncEntryProcessor(), ScheduleType.SCHEDULE_IF_NEW);

        nodeEngine.getExecutionService().scheduleWithFixedDelay(new SyncReplicaVersionTask(), 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void init(final NodeEngine nodeEngine, Properties properties) {
        migrationThread.start();

        int partitionTableSendInterval = node.groupProperties.PARTITION_TABLE_SEND_INTERVAL.getInteger();
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        nodeEngine.getExecutionService().scheduleAtFixedRate(new SendClusterStateTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        if (!initialized) {
            firstArrangement();
        }
        if (partitions[partitionId].getOwner() == null && !node.isMaster() && node.joined()) {
            notifyMasterToAssignPartitions();
        }
        return partitions[partitionId].getOwner();
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
                logger.info("Initializing cluster partition table first arrangement...");
                final Set<Member> members = node.getClusterService().getMembers();
                Address[][] newState = psg.initialize(memberGroupFactory.createMemberGroups(members), partitionCount);

                //todo: why is there a null check?
                if (newState != null) {
                    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                        InternalPartitionImpl partition = partitions[partitionId];
                        Address[] replicas = newState[partitionId];
                        partition.setPartitionInfo(replicas);
                    }
                    initialized = true;
                    sendPartitionRuntimeState(true);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void updateMemberGroupsSize() {
        final Collection<MemberGroup> groups = memberGroupFactory.createMemberGroups(node.getClusterService().getMembers());
        memberGroupsSize = groups.size();
    }

    @Override
    public int getMemberGroupsSize() {
        final int size = memberGroupsSize;
        // size = 0 means service is not initialized yet.
        // return 1 instead since there should be at least one member group
        return size > 0 ? size : 1;
    }

    public void memberAdded(final MemberImpl member) {
        if (!member.localMember()) {
            updateMemberGroupsSize();
        }
        if (node.isMaster() && node.isActive()) {
            lock.lock();
            try {
                migrationQueue.clear();
                migrationQueue.offer(new RepartitioningTask());

                if (initialized) {
                    // send initial partition table to newly joined node.
                    PartitionStateOperation op = new PartitionStateOperation(node.clusterService.getMemberList(), getPartitions(),
                            new ArrayList<MigrationInfo>(completedMigrations),
                            node.getClusterService().getClusterTime(), stateVersion.get());
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
            for (final InternalPartitionImpl partition : partitions) {
                boolean promote = false;
                if (deadAddress.equals(partition.getOwner()) && thisAddress.equals(partition.getReplicaAddress(1))) {
                    promote = true;
                }
                // shift partition table up.
                while (partition.onDeadAddress(deadAddress));

                if (promote) {
                    final Operation op = new PromoteFromBackupOperation();
                    op.setPartitionId(partition.getPartitionId()).setNodeEngine(nodeEngine).setValidateTarget(false).setService(this);
                    nodeEngine.getOperationService().executeOperation(op);
                }
            }

            if (node.isMaster()) {
                migrationQueue.offer(new RepartitioningTask());
            }

            // Add a delay before activating migration, to give other nodes time to notice the dead one.
            long migrationActivationDelay = node.groupProperties.CONNECTION_MONITOR_INTERVAL.getLong()
                    * node.groupProperties.CONNECTION_MONITOR_MAX_FAULTS.getInteger() * 5;

            long callTimeout = node.groupProperties.OPERATION_CALL_TIMEOUT_MILLIS.getLong();
            // delay should be smaller than call timeout, otherwise operations may fail because of invalid partition table
            migrationActivationDelay = Math.min(migrationActivationDelay, callTimeout / 2);
            migrationActivationDelay = Math.max(migrationActivationDelay, 1000L);

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

    private void sendPartitionRuntimeState(boolean required) {
        if (!initialized) {
            // do not send partition state until initialized!
            return;
        }
        if (!node.isMaster() || !node.isActive() || !node.joined()) {
            return;
        }
        if (!required && !migrationActive.get()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }
        final Collection<MemberImpl> members = node.clusterService.getMemberList();
        lock.lock();
        try {
            final long clusterTime = node.getClusterService().getClusterTime();
            PartitionStateOperation op = new PartitionStateOperation(members, getPartitions(),
                    new ArrayList<MigrationInfo>(completedMigrations), clusterTime, stateVersion.get());

            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    try {
                        nodeEngine.getOperationService().send(op, member.getAddress());
                    } catch (Exception e) {
                        logger.finest(e);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    void processPartitionRuntimeState(PartitionRuntimeState partitionState) {
        lock.lock();
        try {
            if (!node.isActive() || !node.joined()) {
                if(logger.isFinestEnabled()){
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
                        logger.severe( "Received a ClusterRuntimeState from an unknown member!" +
                                " => Sender: " + sender + ", Master: " + master + "! ");
                        return;
                    } else {
                        logger.warning("Received a ClusterRuntimeState, but its sender doesn't seem to be master!" +
                                " => Sender: " + sender + ", Master: " + master + "! " +
                                "(Ignore if master node has changed recently.)");
                    }
                }
            }

            final Set<Address> unknownAddresses = new HashSet<Address>();
            PartitionInfo[] state = partitionState.getPartitions();
            for(int partitionId=0;partitionId<state.length;partitionId++){
                PartitionInfo  partitionInfo = state[partitionId];
                InternalPartitionImpl currentPartition = partitions[partitionId];
                for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                    Address address = partitionInfo.getReplicaAddress(index);
                    if (address != null && getMember(address) == null) {
                        if (logger.isFinestEnabled()) {
                            logger.finest(
                                    "Unknown " + address + " found in partition table sent from master "
                                            + sender + ". It has probably already left the cluster. Partition: " + partitionId);
                        }
                        unknownAddresses.add(address);
                    }
                }
                // backup replicas will be assigned after active migrations are finalized.
                currentPartition.setOwner(partitionInfo.getReplicaAddress(0));
            }
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

            Collection<MigrationInfo> completedMigrations = partitionState.getCompletedMigrations();
            for (MigrationInfo completedMigration : completedMigrations) {
                addCompletedMigration(completedMigration);
                finalizeActiveMigration(completedMigration);
            }
            if (!activeMigrations.isEmpty()) {
                final MemberImpl masterMember = getMasterMember();
                rollbackActiveMigrationsFromPreviousMaster(masterMember.getUuid());
            }

            for(int partitionId=0;partitionId<partitionCount;partitionId++){
                InternalPartitionImpl partition = partitions[partitionId];
                Address[] replicas = state[partitionId].getReplicaAddresses();
                partition.setPartitionInfo(replicas);
            }

            stateVersion.set(partitionState.getVersion());
            initialized = true;
        } finally {
            lock.unlock();
        }
    }

    private void finalizeActiveMigration(final MigrationInfo migrationInfo) {
        if (activeMigrations.containsKey(migrationInfo.getPartitionId())) {
            lock.lock();
            try {
                if (activeMigrations.containsValue(migrationInfo)) {
                    if (migrationInfo.startProcessing()) {
                        try {
                            final Address thisAddress = node.getThisAddress();
                            final boolean source = thisAddress.equals(migrationInfo.getSource());
                            final boolean destination = thisAddress.equals(migrationInfo.getDestination());
                            if (source || destination) {
                                final int partitionId = migrationInfo.getPartitionId();
                                final InternalPartitionImpl migratingPartition = getPartitionImpl(partitionId);
                                final Address ownerAddress = migratingPartition.getOwner();
                                final boolean success = migrationInfo.getDestination().equals(ownerAddress);
                                final MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                                final FinalizeMigrationOperation op = new FinalizeMigrationOperation(endpoint, success);
                                op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setValidateTarget(false).setService(this);
                                nodeEngine.getOperationService().executeOperation(op);
                            }
                        } catch (Exception e) {
                            logger.warning(e);
                        } finally {
                            migrationInfo.doneProcessing();
                        }
                    } else {
                        logger.info("Scheduling finalization of " + migrationInfo + ", because migration process is currently running.");
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

    void addActiveMigration(MigrationInfo migrationInfo) {
        lock.lock();
        try {
            final int partitionId = migrationInfo.getPartitionId();
            partitions[partitionId].isMigrating=true;
            final MigrationInfo currentMigrationInfo = activeMigrations.putIfAbsent(partitionId, migrationInfo);
            if (currentMigrationInfo != null) {
                boolean oldMaster = false;
                MigrationInfo oldMigration;
                MigrationInfo newMigration;
                final MemberImpl masterMember = getMasterMember();
                final String master = masterMember.getUuid();
                if (!master.equals(currentMigrationInfo.getMasterUuid())) {  // master changed
                    oldMigration = currentMigrationInfo;
                    newMigration = migrationInfo;
                    oldMaster = true;
                } else if (!master.equals(migrationInfo.getMasterUuid())) {  // master changed
                    oldMigration = migrationInfo;
                    newMigration = currentMigrationInfo;
                    oldMaster = true;
                } else if (!currentMigrationInfo.isProcessing() && migrationInfo.isProcessing()) {
                    // new migration arrived before partition state!
                    oldMigration = currentMigrationInfo;
                    newMigration = migrationInfo;
                } else {
                    final String message = "Something is seriously wrong! There are two migration requests for the same partition!" +
                            " First-> " + currentMigrationInfo + ", Second -> " + migrationInfo;
                    final IllegalStateException error = new IllegalStateException(message);
                    logger.severe(message, error);
                    throw error;
                }

                if (oldMaster) {
                    logger.info("Finalizing migration instantiated by the old master -> " + oldMigration);
                } else {
                    if (logger.isFinestEnabled()) {
                        logger.finest( "Finalizing previous migration -> " + oldMigration);
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
        partitions[partitionId].isMigrating=false;
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
        nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
            @Override
            public void run() {
                final Collection<MigrationAwareService> services = nodeEngine.getServices(MigrationAwareService.class);
                for (MigrationAwareService service : services) {
                    service.clearPartitionReplica(partitionId);
                }
            }
        });
    }

    @PrivateApi
    void syncPartitionReplica(int partitionId, int replicaIndex, boolean force) {
        if (replicaIndex < 0 || replicaIndex > InternalPartition.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Invalid replica index: " + replicaIndex);
        }
        final InternalPartitionImpl partitionImpl = getPartition(partitionId);
        final Address target = partitionImpl.getOwner();
        if (target != null) {
            final ReplicaSyncRequest syncRequest = new ReplicaSyncRequest();
            syncRequest.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            final ReplicaSyncInfo currentSyncInfo = replicaSyncRequests.get(partitionId);
            final ReplicaSyncInfo syncInfo = new ReplicaSyncInfo(partitionId, replicaIndex, target);
            boolean sendRequest = false;
            if (currentSyncInfo == null) {
                sendRequest = replicaSyncRequests.compareAndSet(partitionId, null,syncInfo);
            } else if (currentSyncInfo.requestTime < (Clock.currentTimeMillis() - 10000)
                    || nodeEngine.getClusterService().getMember(currentSyncInfo.target) == null) {
                sendRequest = replicaSyncRequests.compareAndSet(partitionId, currentSyncInfo, syncInfo);
            } else if (force) {
                replicaSyncRequests.set(partitionId, syncInfo);
                sendRequest = true;
            }
            if (target.equals(nodeEngine.getThisAddress())) {
                throw new IllegalStateException("Replica target cannot be this node -> partitionId: " + partitionId
                        + ", replicaIndex: " + replicaIndex + ", partition-info: " + partitionImpl);
            }
            if (sendRequest) {
//                final Level level = force ? Level.FINEST : Level.INFO;
                final Level level = Level.FINEST;
                if (logger.isLoggable(level)) {
                    logger.log(level, "Sending sync replica request to -> " + target
                            + "; for partition: " + partitionId + ", replica: " + replicaIndex);
                }
                replicaSyncScheduler.schedule(15000, partitionId, syncInfo);
                nodeEngine.getOperationService().send(syncRequest, target);
            }
        } else {
            logger.warning("Sync replica target is null, no need to sync -> partition: " + partitionId
                    + ", replica: " + replicaIndex);
        }
    }

    @PrivateApi
    public InternalPartition[] getPartitions() {
        //a defensive copy is made to prevent breaking with the old approach, but imho not needed
        InternalPartition[] result = new InternalPartition[partitions.length];
        System.arraycopy(partitions, 0, result, 0, partitions.length);
        return result;
    }

    MemberImpl getMember(Address address) {
        return node.clusterService.getMember(address);
    }

    private InternalPartitionImpl getPartitionImpl(int partitionId) {
        return partitions[partitionId];
    }

    @Override
    public InternalPartitionImpl getPartition(int partitionId) {
        InternalPartitionImpl p = getPartitionImpl(partitionId);
        if (p.getOwner() == null) {
            // probably ownerships are not set yet.
            // force it.
            getPartitionOwner(partitionId);
        }
        return p;
    }

    @PrivateApi
    public boolean prepareToSafeShutdown(final long timeout, TimeUnit unit) {
        long timeoutInMillis = unit.toMillis(timeout);
        int sleep = 500;
        while (timeoutInMillis > 0) {
            while (timeoutInMillis > 0 && shouldWaitMigrationOrBackups(Level.INFO)) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(sleep);
                } catch (InterruptedException ignored) {
                }
                timeoutInMillis -= sleep;
            }
            if (timeoutInMillis < 0) {
                return false;
            }

            if (!node.isMaster()) {
                while (timeoutInMillis > 0 && hasOnGoingMigrationMaster(Level.WARNING)) { // ignore elapsed time during master inv.
                    logger.info("Waiting for the master node to complete remaining migrations!");
                    try {
                        //noinspection BusyWait
                        Thread.sleep(sleep);
                    } catch (InterruptedException ignored) {
                    }
                    timeoutInMillis -= sleep;
                }
                if (timeoutInMillis < 0) {
                    return false;
                }
            }

            long start = Clock.currentTimeMillis();
            boolean ok = checkReplicaSyncState();
            timeoutInMillis -= (Clock.currentTimeMillis() - start);
            if (ok) {
                logger.finest( "Replica sync state before shutdown is OK");
                return true;
            } else {
                if (timeoutInMillis < 0) {
                    return false;
                }
                logger.info("Backup replica versions inconsistent, waiting for synchronization..");
                try {
                    //noinspection BusyWait
                    Thread.sleep(sleep);
                } catch (InterruptedException ignored) {
                }
                timeoutInMillis -= sleep;
            }
        }
        return false;
    }

    @Override
    public boolean hasOnGoingMigration() {
        return hasOnGoingMigrationLocal() || (!node.isMaster() && hasOnGoingMigrationMaster(Level.FINEST));
    }

    private boolean hasOnGoingMigrationMaster(Level level) {
        Operation op = new HasOngoingMigration();
        Future f = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, op, node.getMasterAddress())
                .setTryCount(100).setTryPauseMillis(100).invoke();
        try {
            return (Boolean) f.get(1, TimeUnit.MINUTES);
        } catch (InterruptedException ignored) {
        } catch (Exception e) {
            logger.log(level, "Could not get a response from master about migrations! -> " + e.toString());
        }
        return false;
    }
    boolean hasOnGoingMigrationLocal() {
        return !activeMigrations.isEmpty() || !migrationQueue.isEmpty() || shouldWaitMigrationOrBackups(Level.OFF);
    }

    private boolean checkReplicaSyncState() {
        final Address thisAddress = node.getThisAddress();
        final Semaphore s = new Semaphore(0);
        final AtomicBoolean ok = new AtomicBoolean(true);
        int empty = 0;
        for (final InternalPartitionImpl partition : partitions) {
            if (thisAddress.equals(partition.getOwner()) && partition.getReplicaAddress(1) != null) {
                Callback<Object> callback = new Callback<Object>() {
                    @Override
                    public void sendResponse(Object object) {
                        if (Boolean.FALSE.equals(object)) {
                            ok.compareAndSet(true, false);
                        }
                        s.release();
                    }
                };
                final SyncReplicaVersion op = new SyncReplicaVersion(1, callback);
                op.setService(this);
                op.setNodeEngine(nodeEngine);
                op.setResponseHandler(ResponseHandlerFactory
                        .createErrorLoggingResponseHandler(node.getLogger(SyncReplicaVersion.class)));
                op.setPartitionId(partition.getPartitionId());
                nodeEngine.getOperationService().executeOperation(op);
            } else {
                empty++;
            }
        }
        s.release(empty);
        try {
            return s.tryAcquire(partitionCount, 10, TimeUnit.SECONDS) && ok.get();
        } catch (InterruptedException ignored) {
            return false;
        }
    }

    private boolean shouldWaitMigrationOrBackups(Level level) {
        if (initialized) {
            MemberGroupFactory mgf = memberGroupFactory;
            final Collection<MemberGroup> memberGroups = mgf.createMemberGroups(node.getClusterService().getMembers());
            if (memberGroups.size() < 2) return false;

            int groups = 0;
            for (MemberGroup memberGroup : memberGroups) {
                if (memberGroup.size() > 0) {
                    groups++;
                }
            }
            if (groups < 2) return false;

            final int activeSize = activeMigrations.size();
            if (activeSize != 0) {
                if (logger.isLoggable(level)) {
                    logger.log(level, "Waiting for active migration tasks: " + activeSize);
                }
                return true;
            }

            final int queueSize = migrationQueue.size();
            if (queueSize == 0) {
                for (InternalPartitionImpl partition : partitions) {
                    if (partition.getReplicaAddress(1) == null) {
                        if (logger.isLoggable(level)) {
                            logger.log(level, "Should take backup of partition: " + partition.getPartitionId());
                        }
                        return true;
                    } else {
                        final int replicaSyncProcesses = replicaSyncProcessCount.get();
                        if (replicaSyncProcesses > 0) {
                            if (logger.isLoggable(level)) {
                                logger.log(level, "Processing replica sync requests: " + replicaSyncProcesses);
                            }
                            return true;
                        }
                    }
                }
            } else {
                if (logger.isLoggable(level)) {
                    logger.log(level, "Waiting for cluster migration tasks: " + queueSize);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public final int getPartitionId(Data key) {
        int hash = key.getPartitionHash();
        return (hash != Integer.MIN_VALUE) ? Math.abs(hash) % partitionCount : 0;
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
    @PrivateApi
    public long[] incrementPartitionReplicaVersions(int partitionId, int backupCount) {
        return replicaVersions[partitionId].incrementAndGet(backupCount);
    }

    // called in operation threads
    @PrivateApi
    public void updatePartitionReplicaVersions(int partitionId, long versions[], int replicaIndex) {
        final PartitionReplicaVersions partitionVersion = replicaVersions[partitionId];
        if (!partitionVersion.update(versions, replicaIndex)) {
            syncPartitionReplica(partitionId, replicaIndex, false);
        }
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    long[] getPartitionReplicaVersions(int partitionId) {
        return replicaVersions[partitionId].get();
    }

    // called in operation threads
    void setPartitionReplicaVersions(int partitionId, long[] versions) {
        replicaVersions[partitionId].reset(versions);
    }

    // called in operation threads
    void finalizeReplicaSync(int partitionId, long[] versions) {
        setPartitionReplicaVersions(partitionId, versions);
        replicaSyncRequests.set(partitionId, null);
        replicaSyncScheduler.cancel(partitionId);
    }

    void incrementReplicaSyncProcessCount() {
        replicaSyncProcessCount.incrementAndGet();
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
                    Thread.sleep(10);
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
                    partition.isMigrating=false;
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
        logger.finest( "Shutting down the partition service");
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
        final MemberImpl current = getMember(migrationInfo.getSource());
        final MemberImpl newOwner = getMember(migrationInfo.getDestination());
        final MigrationEvent event = new MigrationEvent(migrationInfo.getPartitionId(), current, newOwner, status);
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        eventService.publishEvent(SERVICE_NAME, registrations, event, event.getPartitionId());
    }

    @Override
    public String addMigrationListener(MigrationListener migrationListener) {
        final EventRegistration registration = nodeEngine.getEventService().registerListener(SERVICE_NAME, SERVICE_NAME, migrationListener);
        return registration.getId();
    }

    @Override
    public boolean removeMigrationListener(final String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
    }

    @Override
    public void dispatchEvent(MigrationEvent migrationEvent, MigrationListener migrationListener) {
        switch (migrationEvent.getStatus()) {
            case STARTED:
                migrationListener.migrationStarted(migrationEvent);
                break;
            case COMPLETED:
                migrationListener.migrationCompleted(migrationEvent);
                break;
            case FAILED:
                migrationListener.migrationFailed(migrationEvent);
                break;
        }
    }

    private static MemberGroupFactory newMemberGroupFactory(PartitionGroupConfig partitionGroupConfig) {
        PartitionGroupConfig.MemberGroupType memberGroupType;

        if (partitionGroupConfig == null || !partitionGroupConfig.isEnabled()) {
            memberGroupType = PartitionGroupConfig.MemberGroupType.PER_MEMBER;
        }else{
            memberGroupType = partitionGroupConfig.getGroupType();
        }

        switch (memberGroupType) {
            case HOST_AWARE:
                return new HostAwareMemberGroupFactory();
            case CUSTOM:
                return new ConfigMemberGroupFactory(partitionGroupConfig.getMemberGroupConfigs());
            case PER_MEMBER:
                return new SingleMemberGroupFactory();
            default:
                throw new RuntimeException("Unknown MemberGroupType:"+memberGroupType);
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

    public static class AssignPartitions extends AbstractOperation {
        @Override
        public void run() {
            final PartitionServiceImpl service = getService();
            service.firstArrangement();
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }
    }

    private class SendClusterStateTask implements Runnable {
        @Override
        public void run() {
            if (node.isMaster() && node.isActive()) {
                if (!migrationQueue.isEmpty() && migrationActive.get()) {
                    logger.info("Remaining migration tasks in queue => " + migrationQueue.size());
                }
                sendPartitionRuntimeState(false);
            }
        }
    }

    private class SyncReplicaVersionTask implements Runnable {
        @Override
        public void run() {
            if (node.isActive() && migrationActive.get()) {
                final Address thisAddress = node.getThisAddress();
                for (final InternalPartitionImpl partition : partitions) {
                    if (thisAddress.equals(partition.getOwner()) && partition.getReplicaAddress(1) != null) {
                        final SyncReplicaVersion op = new SyncReplicaVersion(1, null);
                        op.setService(PartitionServiceImpl.this);
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

    private class RepartitioningTask implements Runnable {
        @Override
        public void run() {
            if (node.isMaster() && node.isActive()) {
                lock.lock();
                try {
                    if (!initialized) {
                        return;
                    }
                    migrationQueue.clear();
                    final PartitionStateGenerator psg = partitionStateGenerator;
                    final Set<Member> members = node.getClusterService().getMembers();
                    final Address[][] newState = psg.reArrange(memberGroupFactory.createMemberGroups(members), partitions);
                    int migrationCount = 0;
                    int lostCount = 0;
                    lastRepartitionTime.set(Clock.currentTimeMillis());
                    for (int partitionId=0;partitionId<partitionCount;partitionId++) {
                        Address[] replicas = newState[partitionId];
                        InternalPartitionImpl currentPartition = partitions[partitionId];
                        Address currentOwner = currentPartition.getOwner();
                        Address newOwner = replicas[0];

                        if (currentOwner == null) {  // assign new owner for lost partition
                            lostCount++;
                            currentPartition.setPartitionInfo(replicas);
                            MigrationInfo migrationInfo = new MigrationInfo(partitionId, null, newOwner);
                            sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
                            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);

                        } else if (newOwner != null && !currentOwner.equals(newOwner)) {
                            migrationCount++;
                            MigrationInfo info = new MigrationInfo(partitionId, currentOwner, newOwner);
                            final Migrator migrator = new Migrator(info, new BackupMigrationTask(partitionId, replicas));
                            migrationQueue.offer(migrator);
                        } else {
                            currentPartition.setPartitionInfo(replicas);
                        }
                    }
                    sendPartitionRuntimeState(false);

                    if (lostCount > 0) {
                        logger.warning("Assigning new owners for " + lostCount + " LOST partitions!");
                    }

                    if (migrationCount > 0) {
                        logger.info("Re-partitioning cluster data... Migration queue size: " + migrationCount);
                    } else {
                        logger.info("Partition balance is ok, no need to re-partition cluster data... ");
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private class BackupMigrationTask implements Runnable {
        final Address[] replicas;
        final int partitionid;

        BackupMigrationTask(int partitionId, Address[] replicas) {
            this.partitionid = partitionId;
            this.replicas = replicas;
        }

        @Override
        public void run() {
            lock.lock();
            try {
                final InternalPartitionImpl currentPartition = partitions[partitionid];
                for (int index = 1; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                    currentPartition.setReplicaAddress(index, replicas[index]);
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("BackupMigrationTask{");
            sb.append("partitonId=").append(partitionid);
            sb.append("replicas=").append(Arrays.toString(replicas));
            sb.append('}');
            return sb.toString();
        }
    }

    private class Migrator implements Runnable {
        final MigrationInfo migrationInfo;
        final BackupMigrationTask backupTask;

        Migrator(MigrationInfo migrationInfo, BackupMigrationTask backupTask) {
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
                if (!partition.getOwner().equals(info.getSource())) {
                    logger.severe("ERROR: partition owner is not the source of migration! -> "
                            +  partition + " -VS- " + info);
                }
                sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
                Boolean result = Boolean.FALSE;
                MemberImpl fromMember = getMember(migrationInfo.getSource());
                if (logger.isFinestEnabled()) {
                    logger.finest( "Started Migration : " + migrationInfo);
                }
                systemLogService.logPartition("Started Migration : " + migrationInfo);
                if (fromMember != null) {
                    Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                            migrationRequestOp, migrationInfo.getSource()).setTryPauseMillis(1000).invoke();

                    try {
                        result = (Boolean) nodeEngine.toObject(future.get(partitionMigrationTimeout, TimeUnit.SECONDS));
                    } catch (Throwable e) {
                        final Level level = node.isActive() && migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                        logger.log(level, "Failed migrating from " + fromMember, e);
                    }
                } else {
                    // Partition is lost! Assign new owner and exit.
                    logger.warning( "Partition is lost! Assign new owner and exit...");
                    result = Boolean.TRUE;
                }
                if (Boolean.TRUE.equals(result)) {
                    String message =  "Finished Migration: " + migrationInfo;
                    logger.finest(message);
                    systemLogService.logPartition(message);
                    processMigrationResult();
                } else {
                    final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                    logger.log(level, "Migration failed: " + migrationInfo);
                    migrationTaskFailed();
                }
            } catch (Throwable t) {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] while executing " + migrationRequestOp);
                logger.finest(t);
                migrationTaskFailed();
            }
        }

        private void migrationTaskFailed() {
            systemLogService.logPartition("Migration failed: " + migrationInfo);
            lock.lock();
            try {
                addCompletedMigration(migrationInfo);
                finalizeActiveMigration(migrationInfo);
                sendPartitionRuntimeState(true);
            } finally {
                lock.unlock();
            }
            sendMigrationEvent(migrationInfo, MigrationStatus.FAILED);
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
                sendPartitionRuntimeState(true);
            } finally {
                lock.unlock();
            }
            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Migrator{");
            sb.append("migrationInfo=").append(migrationInfo);
            sb.append('}');
            return sb.toString();
        }
    }

    private class MigrationThread implements Runnable {
        private final Thread thread;
        private final long sleepTime = Math.max(250L, partitionMigrationInterval);
        private boolean migrating = false;

        MigrationThread(Node node) {
            thread = new Thread(node.threadGroup, this, node.getThreadNamePrefix("migration"));
        }

        @Override
        public void run() {
            try {
                while (!thread.isInterrupted()) {
                    Runnable r;
                    while (migrationActive.get() && (r = migrationQueue.poll(1, TimeUnit.SECONDS)) != null) {
                        safeRun(r);
                        if (partitionMigrationInterval > 0) {
                            Thread.sleep(partitionMigrationInterval);
                        }
                    }
                    final boolean hasNoTasks = migrationQueue.isEmpty();
                    if (hasNoTasks) {
                        if (migrating) {
                            migrating = false;
                            logger.info("All migration tasks has been completed, queues are empty.");
                        }
                        evictCompletedMigrations();
                        Thread.sleep(sleepTime);
                    } else if (!migrationActive.get()) {
                        Thread.sleep(sleepTime);
                    }
                }
            } catch (InterruptedException e) {
                if(logger.isFinestEnabled()){
                    logger.finest( "MigrationThread is interrupted: " + e.getMessage());
                }
            } finally {
                migrationQueue.clear();
            }
        }

        boolean safeRun(final Runnable r) {
            if (r == null || thread.isInterrupted()) return false;
            try {
                migrating = (r instanceof Migrator);
                r.run();
            } catch (Throwable t) {
                logger.warning( t);
            }
            return true;
        }

        void start() {
            thread.start();
        }

        void stopNow() {
            migrationQueue.clear();
            thread.interrupt();
        }
    }

    private static class ReplicaSyncInfo {
        final int partitionId;
        final int replicaIndex;
        final long requestTime = Clock.currentTimeMillis();
        final Address target;

        private ReplicaSyncInfo(int partitionId, int replicaIndex, Address target) {
            this.partitionId = partitionId;
            this.replicaIndex = replicaIndex;
            this.target = target;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReplicaSyncInfo that = (ReplicaSyncInfo) o;

            if (partitionId != that.partitionId) return false;
            if (replicaIndex != that.replicaIndex) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = partitionId;
            result = 31 * result + replicaIndex;
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ReplicaSyncInfo{");
            sb.append("partitionId=").append(partitionId);
            sb.append(", replicaIndex=").append(replicaIndex);
            sb.append(", requestTime=").append(requestTime);
            sb.append(", target=").append(target);
            sb.append('}');
            return sb.toString();
        }
    }

    private static final AtomicReferenceFieldUpdater<InternalPartitionImpl, Address[]> addressesUpdater
            = AtomicReferenceFieldUpdater.newUpdater(InternalPartitionImpl.class, Address[].class, "addresses");

    private final class InternalPartitionImpl implements InternalPartition {

        private final int partitionId;

        //The content of this array will never be updated, so it can be safely read using a volatile read.
        //Writing to 'addresses' is done using the 'addressUpdater' AtomicReferenceFieldUpdater which involves a
        //cas to prevent lost updates.
        //The old approach relied on a AtomicReferenceArray, but this performed a lot slower that the current approach.
        //Number of reads will outweigh the number of writes to the field.
        volatile Address[] addresses = new Address[MAX_REPLICA_COUNT];
        private final PartitionListener partitionListener;
        private volatile boolean isMigrating = false;

        InternalPartitionImpl(int partitionId, PartitionListener partitionListener) {
            this.partitionId = partitionId;
            this.partitionListener = partitionListener;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public boolean isMigrating() {
            return isMigrating;
        }

        @Override
        public Address getOwner() {
            return addresses[0];
        }

        void setOwner(Address ownerAddress) {
            setReplicaAddress(0, ownerAddress);
        }

        @Override
        public Address getReplicaAddress(int replicaIndex) {
            return addresses[replicaIndex];
        }

        void setReplicaAddress(int replicaIndex, Address newAddress) {
            boolean changed = false;
            Address oldAddress;
            for (; ; ) {
                Address[] oldAddresses = addresses;
                oldAddress = oldAddresses[replicaIndex];
                if (partitionListener != null) {
                    if (oldAddress == null) {
                        changed = (newAddress != null);
                    } else {
                        changed = !oldAddress.equals(newAddress);
                    }
                }

                Address[] newAddresses = new Address[MAX_REPLICA_COUNT];
                arraycopy(oldAddresses, 0, newAddresses, 0, MAX_REPLICA_COUNT);
                newAddresses[replicaIndex] = newAddress;
                if (addressesUpdater.compareAndSet(this, oldAddresses, newAddresses)) {
                    break;
                }
            }

            if (changed) {
                partitionListener.replicaChanged(new PartitionReplicaChangeEvent(partitionId, replicaIndex, oldAddress, newAddress));
            }
        }

        boolean onDeadAddress(Address deadAddress) {
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                if (deadAddress.equals(addresses[i])) {
                    for (int a = i; a + 1 < MAX_REPLICA_COUNT; a++) {
                        setReplicaAddress(a, addresses[a + 1]);
                    }
                    setReplicaAddress(MAX_REPLICA_COUNT - 1, null);
                    return true;
                }
            }
            return false;
        }

        void setPartitionInfo(Address[] replicas) {
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                setReplicaAddress(i, replicas[i]);
            }
        }

        @Override
        public boolean isOwnerOrBackup(Address address) {
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                if (address.equals(getReplicaAddress(i))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Partition [").append(partitionId).append("]{\n");
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                Address address = addresses[i];
                if (address != null) {
                    sb.append('\t');
                    sb.append(i).append(":").append(address);
                    sb.append("\n");
                }
            }
            sb.append("}");
            return sb.toString();
        }
    }

    private class LocalPartitionListener implements PartitionListener {
        final Address thisAddress;
        private LocalPartitionListener(Address thisAddress) {
            this.thisAddress = thisAddress;
        }

        @Override
        public void replicaChanged(PartitionReplicaChangeEvent event) {
            if (event.getReplicaIndex() > 0) {
                // backup replica owner changed!
                if (thisAddress.equals(event.getOldAddress())) {
                    final InternalPartitionImpl partition = partitions[event.getPartitionId()];
                    if (!partition.isOwnerOrBackup(thisAddress)) {
                        clearPartitionReplica(event.getPartitionId(), event.getReplicaIndex());
                    }
                } else if (thisAddress.equals(event.getNewAddress())) {
                    syncPartitionReplica(event.getPartitionId(), event.getReplicaIndex(), true);
                }
            }
            if (event.getReplicaIndex() == 0 && event.getNewAddress() == null && node.isActive() && node.joined()) {
                final String warning = "Owner of partition is being removed! " +
                        "Possible data loss for partition[" + event.getPartitionId() + "]. " + event;
                logger.warning(warning);
                systemLogService.logWarningPartition(warning);
            }
            if (node.isMaster()) {
                stateVersion.incrementAndGet();
            }
        }
    }

    private class ReplicaSyncEntryProcessor implements ScheduledEntryProcessor<Integer, ReplicaSyncInfo> {
        @Override
        public void process(EntryTaskScheduler<Integer, ReplicaSyncInfo> scheduler, Collection<ScheduledEntry<Integer, ReplicaSyncInfo>> entries) {
            for (ScheduledEntry<Integer, ReplicaSyncInfo> entry : entries) {
                final ReplicaSyncInfo syncInfo = entry.getValue();
                if (replicaSyncRequests.compareAndSet(entry.getKey(), syncInfo, null)) {
                    logger.info("Re-sending sync replica request for partition: " + syncInfo.partitionId + ", replica: " + syncInfo.replicaIndex);
                    syncPartitionReplica(syncInfo.partitionId, syncInfo.replicaIndex, false);
                }
            }
        }
    }

    private static class PartitionReplicaVersions {
        final int partitionId;
        final long versions[] = new long[InternalPartitionImpl.MAX_BACKUP_COUNT]; // read and updated only by operation/partition threads

        private PartitionReplicaVersions(int partitionId) {
            this.partitionId = partitionId;
        }

        long[] incrementAndGet(int backupCount) {
            for (int i = 0; i < backupCount; i++) {
                versions[i]++;
            }
            return versions;
        }

        long[] get() {
            return versions;
        }

        boolean update(final long[] newVersions, int currentReplica) {
            final int index = currentReplica - 1;
            final long current = versions[index];
            final long next = newVersions[index];
            final boolean updated = (current == next - 1);
            if (updated) {
                arraycopy(newVersions, 0, versions, 0, newVersions.length);
            }
            return updated;
        }

        void reset(long[] newVersions) {
            arraycopy(newVersions, 0, versions, 0, newVersions.length);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("PartitionReplicaVersions");
            sb.append("{partitionId=").append(partitionId);
            sb.append(", versions=").append(Arrays.toString(versions));
            sb.append('}');
            return sb.toString();
        }
    }
}