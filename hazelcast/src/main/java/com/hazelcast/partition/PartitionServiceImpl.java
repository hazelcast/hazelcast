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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.SystemLogService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

public class PartitionServiceImpl implements PartitionService, ManagedService,
        EventPublishingService<MigrationEvent, MigrationListener> {

    public static final String SERVICE_NAME = "hz:core:partitionService";

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final int partitionCount;
    private final PartitionInfo[] partitions;
    private final PartitionReplicaVersions[] replicaVersions;
    private final ConcurrentMap<Integer, ReplicaSyncInfo> replicaSyncRequests;
    private final AtomicInteger replicaSyncProcessCount = new AtomicInteger();
    private final MigrationThread migrationThread;
    private final int partitionMigrationInterval;
    private final long partitionMigrationTimeout;
    private final PartitionStateGenerator partitionStateGenerator;
    private final MemberGroupFactory memberGroupFactory;
    private final PartitionServiceProxy proxy;
    private final Lock lock = new ReentrantLock();
    private final AtomicInteger stateVersion = new AtomicInteger();
    private final BlockingQueue<Runnable> migrationQueue = new LinkedBlockingQueue<Runnable>();
    private final AtomicBoolean sendingDiffs = new AtomicBoolean(false);
    private final AtomicBoolean migrationActive = new AtomicBoolean(true);
    private final AtomicLong lastRepartitionTime = new AtomicLong();
    private final SystemLogService systemLogService;

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
        this.partitions = new PartitionInfo[partitionCount];
        final PartitionListener partitionListener = new LocalPartitionListener(node.getThisAddress());
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new PartitionInfo(i, partitionListener);
        }
        replicaVersions = new PartitionReplicaVersions[partitionCount];
        for (int i = 0; i < replicaVersions.length; i++) {
            replicaVersions[i] = new PartitionReplicaVersions(i);
        }

        memberGroupFactory = PartitionStateGeneratorFactory.newMemberGroupFactory(node.getConfig().getPartitionGroupConfig());
        partitionStateGenerator = PartitionStateGeneratorFactory.newCustomPartitionStateGenerator(memberGroupFactory);

        partitionMigrationInterval = node.groupProperties.PARTITION_MIGRATION_INTERVAL.getInteger() * 1000;
        // partitionMigrationTimeout is 1.5 times of real timeout
        partitionMigrationTimeout = (long) (node.groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong() * 1.5f);

        migrationThread = new MigrationThread(node);
        proxy = new PartitionServiceProxy(this);

        replicaSyncRequests = new ConcurrentHashMap<Integer, ReplicaSyncInfo>(partitionCount);

        nodeEngine.getExecutionService().scheduleWithFixedDelay(new SyncReplicaVersionTask(), 30, 30, TimeUnit.SECONDS);
    }

    private class LocalPartitionListener implements PartitionListener {
        final Address thisAddress;
        private LocalPartitionListener(Address thisAddress) {
            this.thisAddress = thisAddress;
        }

        public void replicaChanged(PartitionReplicaChangeEvent event) {
            if (event.getReplicaIndex() > 0) {
                // backup replica owner changed!
                if (thisAddress.equals(event.getOldAddress())) {
                    final PartitionInfo partition = partitions[event.getPartitionId()];
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
                logger.log(Level.WARNING, warning);
                systemLogService.logPartition(warning);
            }
            if (node.isMaster()) {
                stateVersion.incrementAndGet();
            }
        }
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public void init(final NodeEngine nodeEngine, Properties properties) {
        migrationThread.start();

        int partitionTableSendInterval = node.groupProperties.PARTITION_TABLE_SEND_INTERVAL.getInteger();
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        nodeEngine.getExecutionService().scheduleAtFixedRate(new SendClusterStateTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);
    }

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
                            node.getMasterAddress()).setTryCount(1).build().invoke();
                    f.get(1, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
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
                logger.log(Level.INFO, "Initializing cluster partition table first arrangement...");
                PartitionInfo[] newState = psg.initialize(node.getClusterService().getMembers(), partitionCount);
                if (newState != null) {
                    for (PartitionInfo partition : newState) {
                        partitions[partition.getPartitionId()].setPartitionInfo(partition);
                    }
                    initialized = true;
                    sendPartitionRuntimeState();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void memberAdded(final MemberImpl member) {
        if (node.isMaster() && node.isActive()) {
            if (sendingDiffs.get()) {
                logger.log(Level.INFO, "MigrationThread is already sending diffs for dead member, " +
                        "no need to initiate task!");
            } else {
                // to avoid repartitioning during a migration process.
                clearMigrationQueue();
                migrationQueue.offer(new PrepareRepartitioningTask());
            }
        }
    }

    public void memberRemoved(final MemberImpl member) {
        final Address deadAddress = member.getAddress();
        final Address thisAddress = node.getThisAddress();
        if (deadAddress == null || deadAddress.equals(thisAddress)) {
            return;
        }
        clearMigrationQueue();
        lock.lock();
        try {
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
            // inactivate migration and sending of PartitionRuntimeState (@see #sendPartitionRuntimeState)
            // let all members notice the dead and fix their own records and indexes.
            // otherwise new master may take action fast and send new partition state
            // before other members realize the dead one and fix their records.
            migrationActive.set(false);
            for (PartitionInfo partition : partitions) {
                // shift partition table up.
                while (partition.onDeadAddress(deadAddress));
            }
            migrationQueue.offer(new PrepareRepartitioningTask());

            // activate migration back after connectionDropTime x 10 milliseconds,
            // thinking optimistically that all nodes notice the dead one in this period.
            final long waitBeforeMigrationActivate = node.groupProperties.CONNECTION_MONITOR_INTERVAL.getLong()
                    * node.groupProperties.CONNECTION_MONITOR_MAX_FAULTS.getInteger() * 10;
            nodeEngine.getExecutionService().schedule(new Runnable() {
                public void run() {
                    migrationActive.set(true);
                }
            }, waitBeforeMigrationActivate, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    private void rollbackActiveMigrationsFromPreviousMaster(final String masterUuid) {
        lock.lock();
        try {
            if (!activeMigrations.isEmpty()) {
                for (MigrationInfo migrationInfo : activeMigrations.values()) {
                    if (!masterUuid.equals(migrationInfo.getMasterUuid())) {
                        // Still there is possibility of the other endpoint commits the migration
                        // but this node roll-backs!
                        logger.log(Level.INFO, "Rolling-back migration instantiated by the old master -> " + migrationInfo);
                        finalizeActiveMigration(migrationInfo);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void sendPartitionRuntimeState() {
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
        final Collection<MemberImpl> members = node.clusterService.getMemberList();
        lock.lock();
        try {
            final long clusterTime = node.getClusterService().getClusterTime();
            PartitionStateOperation op = new PartitionStateOperation(members, partitions,
                    new ArrayList<MigrationInfo>(completedMigrations), clusterTime, stateVersion.get());

            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    try {
                        nodeEngine.getOperationService().send(op, member.getAddress());
                    } catch (Exception e) {
                        logger.log(Level.FINEST, e.getMessage(), e);
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
                return;
            }
            final Address sender = partitionState.getEndpoint();
            final Address master = node.getMasterAddress();
            if (node.isMaster()) {
                logger.log(Level.WARNING, "This is the master node and received a PartitionRuntimeState from "
                        + sender + ". Ignoring incoming state! ");
                return;
            } else {
                if (sender == null || !sender.equals(master)) {
                    if (node.clusterService.getMember(sender) == null) {
                        logger.log(Level.SEVERE, "Received a ClusterRuntimeState from an unknown member!" +
                                " => Sender: " + sender + ", Master: " + master + "! ");
                        return;
                    } else {
                        logger.log(Level.WARNING, "Received a ClusterRuntimeState, but its sender doesn't seem master!" +
                                " => Sender: " + sender + ", Master: " + master + "! " +
                                "(Ignore if master node has changed recently.)");
                    }
                }
            }

            final Set<Address> unknownAddresses = new HashSet<Address>();
            PartitionInfo[] newPartitions = partitionState.getPartitions();
            for (PartitionInfo newPartition : newPartitions) {
                PartitionInfo currentPartition = partitions[newPartition.getPartitionId()];
                for (int index = 0; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                    Address address = newPartition.getReplicaAddress(index);
                    if (address != null && getMember(address) == null) {
                        if (logger.isLoggable(Level.FINEST)) {
                            logger.log(Level.FINEST,
                                    "Unknown " + address + " is found in partition table sent from master "
                                            + sender + ". Probably it's already left the cluster. Partition: " + newPartition);
                        }
                        unknownAddresses.add(address);
                    }
                }
                // backup replicas will be assigned after active migrations are finalized.
                currentPartition.setOwner(newPartition.getOwner());
            }
            if (!unknownAddresses.isEmpty()) {
                StringBuilder s = new StringBuilder("Following unknown addresses are found in partition table")
                        .append(" sent from master[").append(sender).append("].")
                        .append(" (Probably they have already left the cluster.)")
                        .append(" {");
                for (Address address : unknownAddresses) {
                    s.append("\n\t").append(address);
                }
                s.append("\n}");
                logger.log(Level.WARNING, s.toString());
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

            for (PartitionInfo newPartition : newPartitions) {
                PartitionInfo currentPartition = partitions[newPartition.getPartitionId()];
                currentPartition.setPartitionInfo(newPartition);
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
                                final PartitionInfo migratingPartition = getPartition(partitionId);
                                final Address ownerAddress = migratingPartition.getOwner();
                                final boolean success = migrationInfo.getDestination().equals(ownerAddress);
                                final MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;

                                final FinalizeMigrationOperation op = new FinalizeMigrationOperation(endpoint, success);
                                op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setValidateTarget(false).setService(this);
                                nodeEngine.getOperationService().executeOperation(op);
                            }
                        } catch (Exception e) {
                            logger.log(Level.WARNING, e.getMessage(), e);
                        } finally {
                            migrationInfo.doneProcessing();
                        }
                    } else {
                        logger.log(Level.INFO, "Scheduling finalization of " + migrationInfo + ", because migration process is currently running.");
                        nodeEngine.getExecutionService().schedule(new Runnable() {
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

    public boolean isPartitionMigrating(int partitionId) {
        return activeMigrations.containsKey(partitionId);
    }

    void addActiveMigration(MigrationInfo migrationInfo) {
        lock.lock();
        try {
            final int partitionId = migrationInfo.getPartitionId();
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
                    logger.log(Level.SEVERE, message, error);
                    throw error;
                }

                if (oldMaster) {
                    logger.log(Level.INFO, "Finalizing migration instantiated by the old master -> " + oldMigration);
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
        return activeMigrations.remove(partitionId);
    }

    private void addCompletedMigration(MigrationInfo migrationInfo) {
        lock.lock();
        try {
            if (completedMigrations.size() > 10) {
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
        if (replicaIndex < 0 || replicaIndex > PartitionInfo.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Invalid replica index: " + replicaIndex);
        }
        final PartitionInfo partitionInfo = getPartitionInfo(partitionId);
        final Address target = partitionInfo.getOwner();
        if (target != null) {
            final ReplicaSyncRequest syncRequest = new ReplicaSyncRequest();
            syncRequest.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            final ReplicaSyncInfo currentSyncInfo = replicaSyncRequests.get(partitionId);
            final ReplicaSyncInfo syncInfo = new ReplicaSyncInfo(partitionId, target);
            boolean sendRequest = false;
            if (currentSyncInfo == null) {
                sendRequest = replicaSyncRequests.putIfAbsent(partitionId, syncInfo) == null;
            } else if (currentSyncInfo.requestTime < (Clock.currentTimeMillis() - 10000)
                    || nodeEngine.getClusterService().getMember(currentSyncInfo.target) == null) {
                sendRequest = replicaSyncRequests.replace(partitionId, currentSyncInfo, syncInfo);
            } else if (force) {
                replicaSyncRequests.put(partitionId, syncInfo);
                sendRequest = true;
            }
            if (target.equals(nodeEngine.getThisAddress())) {
                throw new IllegalStateException("Replica target cannot be this node -> partitionId: " + partitionId
                        + ", replicaIndex: " + replicaIndex + ", partition-info: " + partitionInfo);
            }
            if (sendRequest) {
                final Level level = force ? Level.FINEST : Level.INFO;
                if (logger.isLoggable(level)) {
                    logger.log(level, "Sending sync replica request to -> " + target
                            + "; for partition: " + partitionId + ", replica: " + replicaIndex);
                }
                nodeEngine.getOperationService().send(syncRequest, target);
            }
        } else {
            logger.log(Level.WARNING, "Sync replica target is null, no need to sync -> partition: " + partitionId
                        + ", replica: " + replicaIndex);
        }
    }

    @PrivateApi
    public void sendReplicaVersionCheckTasks() {
        final Address thisAddress = node.getThisAddress();
        for (PartitionInfo partition : partitions) {
            if (thisAddress.equals(partition.getOwner()) && partition.getReplicaAddress(1) != null) {
                SyncReplicaVersion op = new SyncReplicaVersion(1);
                op.setService(this);
                op.setPartitionId(partition.getPartitionId());
                nodeEngine.getOperationService().executeOperation(op);
            }
        }
    }


    public PartitionInfo[] getPartitions() {
        return partitions;
    }

    MemberImpl getMember(Address address) {
        return node.clusterService.getMember(address);
    }

    public int getStateVersion() {
        return stateVersion.get();
    }

    private PartitionInfo getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public PartitionInfo getPartitionInfo(int partitionId) {
        PartitionInfo p = getPartition(partitionId);
        if (p.getOwner() == null) {
            // probably ownerships are not set yet.
            // force it.
            getPartitionOwner(partitionId);
        }
        return p;
    }

    public boolean hasOnGoingMigration() {
        return !migrationQueue.isEmpty() || !activeMigrations.isEmpty() || hasActiveBackupTask();
    }

    private boolean hasActiveBackupTask() {
        if (!initialized) return false;

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
            logger.log(Level.WARNING, "Waiting for active migration tasks: " + activeSize);
            return true;
        }

        final int queueSize = migrationQueue.size();
        if (queueSize == 0) {
            for (PartitionInfo partition : partitions) {
                if (partition.getReplicaAddress(1) == null) {
                    logger.log(Level.WARNING, "Should take backup of partition: " + partition.getPartitionId());
                    return true;
                } else {
                    final int replicaSyncProcesses = replicaSyncProcessCount.get();
                    if (replicaSyncProcesses > 0) {
                        logger.log(Level.WARNING, "Processing replica sync requests: " + replicaSyncProcesses);
                        return true;
                    }
                }
            }
        } else {
            logger.log(Level.WARNING, "Waiting for active migration tasks: " + queueSize);
            return true;
        }
        return false;
    }

    public final int getPartitionId(Data key) {
        int hash = key.getPartitionHash();
        return (hash != Integer.MIN_VALUE) ? Math.abs(hash) % partitionCount : 0;
    }

    public final int getPartitionId(Object key) {
        return getPartitionId(nodeEngine.toData(key));
    }

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
        replicaSyncRequests.remove(partitionId);
    }

    void incrementReplicaSyncProcessCount() {
        replicaSyncProcessCount.incrementAndGet();
    }

    void decrementReplicaSyncProcessCount() {
        replicaSyncProcessCount.decrementAndGet();
    }

    private class PartitionReplicaVersions {
        final int partitionId;
        final long versions[] = new long[PartitionInfo.MAX_BACKUP_COUNT]; // read and updated only by operation/partition threads

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
                System.arraycopy(newVersions, 0, versions, 0, newVersions.length);
            }
            return updated;
        }

        void reset(long[] newVersions) {
            System.arraycopy(newVersions, 0, versions, 0, newVersions.length);
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

    public static class AssignPartitions extends AbstractOperation {
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
        public void run() {
            if (node.isMaster() && node.isActive()) {
                if (!migrationQueue.isEmpty() && migrationActive.get()) {
                    logger.log(Level.INFO, "Remaining migration tasks in queue => " + migrationQueue.size());
                }
                sendPartitionRuntimeState();
            }
        }
    }

    private class SyncReplicaVersionTask implements Runnable {

        public void run() {
            if (node.isActive() && migrationActive.get()) {
                sendReplicaVersionCheckTasks();
            }
        }
    }

    private class PrepareRepartitioningTask implements Runnable {
        private PrepareRepartitioningTask() {
        }

        public final void run() {
            if (node.isMaster() && node.isActive() && initialized) {
                PartitionStateGenerator psg = partitionStateGenerator;
                List<MigrationInfo> migrationQ = new ArrayList<MigrationInfo>(partitionCount);
                PartitionInfo[] newState = psg.reArrange(partitions, node.getClusterService().getMembers(), partitionCount, migrationQ);
                Set<Integer> migratingPartitions = new HashSet<Integer>(migrationQ.size());
                Map<Integer, BackupMigrationTask> backupTasks = new HashMap<Integer, BackupMigrationTask>();
                for (MigrationInfo info : migrationQ) {
                    migratingPartitions.add(info.getPartitionId());
                }
                int lostCount = 0;
                lock.lock();
                try {
                    for (final PartitionInfo newPartition : newState) {
                        final int partitionId = newPartition.getPartitionId();
                        final PartitionInfo currentPartition = partitions[partitionId];
                        if (currentPartition.getOwner() == null) {  // assign new owner for lost partition
                            lostCount++;
                            final Address owner = newPartition.getOwner();
                            currentPartition.setOwner(owner);
                            MigrationInfo migrationInfo = new MigrationInfo(partitionId, null, owner);
                            sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
                            sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
                        }
                        if (migratingPartitions.contains(partitionId)) {
                            backupTasks.put(partitionId, new BackupMigrationTask(partitionId, newPartition));
                        } else {
                            for (int index = 1; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                                currentPartition.setReplicaAddress(index, newPartition.getReplicaAddress(index));
                            }
                        }
                    }
                    sendPartitionRuntimeState();
                } finally {
                    lock.unlock();
                    for (int i = 0; i < partitionCount; i++) {
                        newState[i] = null; // help GC
                    }
                }

                if (lostCount > 0) {
                    logger.log(Level.WARNING, "Assigning new owners for " + lostCount + " LOST partitions!");
                }

                lastRepartitionTime.set(Clock.currentTimeMillis());
                if (!migrationQ.isEmpty()) {
                    logger.log(Level.INFO, "Re-partitioning cluster data... Migration queue size: " + migrationQ.size());
                    for (MigrationInfo migrationInfo : migrationQ) {
                        final Migrator migrator = new Migrator(migrationInfo, backupTasks.get(migrationInfo.getPartitionId()));
                        migrationQueue.offer(migrator);
                    }
                    migrationQ.clear();
                } else {
                    logger.log(Level.INFO, "Partition balance is ok, no need to re-partition cluster data... ");
                }
            }
        }
    }

    private class BackupMigrationTask implements Runnable {
        final int partitionId;
        final PartitionInfo newPartition;

        BackupMigrationTask(int partitionId, PartitionInfo newPartition) {
            this.partitionId = partitionId;
            this.newPartition = newPartition;
        }

        public void run() {
            lock.lock();
            try {
                logger.log(Level.FINEST, "Executing backup migration tasks for partition: " + partitionId);
                final PartitionInfo currentPartition = partitions[partitionId];
                for (int index = 1; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                    currentPartition.setReplicaAddress(index, newPartition.getReplicaAddress(index));
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private class Migrator implements Runnable {
        final MigrationRequestOperation migrationRequestOp;
        final MigrationInfo migrationInfo;
        final BackupMigrationTask backupTask;

        Migrator(MigrationInfo migrationInfo, BackupMigrationTask backupTask) {
            this.migrationInfo = migrationInfo;
            this.backupTask = backupTask;
            final MemberImpl masterMember = getMasterMember();
            migrationInfo.setMasterUuid(masterMember.getUuid());
            migrationInfo.setMaster(masterMember.getAddress());
            this.migrationRequestOp = new MigrationRequestOperation(migrationInfo);
        }

        public void run() {
            try {
                if (!node.isActive() || !node.isMaster()) {
                    return;
                }
                sendMigrationEvent(migrationInfo, MigrationStatus.STARTED);
                Boolean result = Boolean.FALSE;
                MemberImpl fromMember = getMember(migrationInfo.getSource());
                logger.log(Level.FINEST, "Started Migration : " + migrationRequestOp);
                systemLogService.logPartition("Started Migration : " + migrationRequestOp);
                if (fromMember != null) {
                    Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                            migrationRequestOp, migrationInfo.getSource()).setTryPauseMillis(1000).build();

                    Future future = inv.invoke();
                    try {
                        result = (Boolean) nodeEngine.toObject(future.get(partitionMigrationTimeout, TimeUnit.SECONDS));
                    } catch (Throwable e) {
                        final Level level = node.isActive() && migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                        logger.log(level, "Failed migrating from " + fromMember, e);
                    }
                } else {
                    // Partition is lost! Assign new owner and exit.
                    result = Boolean.TRUE;
                }
                logger.log(Level.FINEST, "Finished Migration : " + migrationRequestOp);
                systemLogService.logPartition("Finished Migration : " + migrationRequestOp);
                if (Boolean.TRUE.equals(result)) {
                    processMigrationResult();
                } else {
                    // remove active partition migration
                    final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                    logger.log(level, "Migration task has failed => " + migrationRequestOp);
                    migrationTaskFailed();
                }
            } catch (Throwable t) {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] " +
                        "while executing " + migrationRequestOp);
                logger.log(Level.FINEST, t.getMessage(), t);
                migrationTaskFailed();
            }
        }

        private void migrationTaskFailed() {
            systemLogService.logPartition("Migration task has failed => " + migrationRequestOp);
            finalizeMigration();
            sendMigrationEvent(migrationInfo, MigrationStatus.FAILED);
        }

        private void processMigrationResult() {
            final int partitionId = migrationRequestOp.getPartitionId();
            final PartitionInfo partition = partitions[partitionId];
            lock.lock();
            try {
                Address newOwner = migrationInfo.getDestination();
                MemberImpl ownerMember = node.clusterService.getMember(newOwner);
                if (ownerMember == null) return;
                partition.setOwner(newOwner);
                finalizeMigration();
                sendMigrationEvent(migrationInfo, MigrationStatus.COMPLETED);
            } finally {
                lock.unlock();
            }
        }

        private void finalizeMigration() {
            lock.lock();
            try {
                addCompletedMigration(migrationInfo);
                finalizeActiveMigration(migrationInfo);
                if (backupTask != null) {
                    backupTask.run();
                }
                sendPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
        }
    }

    private class MigrationThread implements Runnable {
        private final Thread thread;
        private boolean migrating = false;

        MigrationThread(Node node) {
            thread = new Thread(node.threadGroup, this, node.getThreadNamePrefix("migration"));
        }

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
                    if (hasNoTasks && migrating) {
                        migrating = false;
                        logger.log(Level.INFO, "All migration tasks has been completed, queues are empty.");
                    }
                    if (!migrationActive.get() || hasNoTasks) {
                        evictCompletedMigrations();
                        Thread.sleep(250);
                    }
                }
            } catch (InterruptedException e) {
                logger.log(Level.FINEST, "MigrationThread is interrupted: " + e.getMessage());
            } finally {
                clearMigrationQueue();
            }
        }

        boolean safeRun(final Runnable r) {
            if (r == null || thread.isInterrupted()) return false;
            try {
                migrating = (r instanceof Migrator);
                r.run();
            } catch (Throwable t) {
                logger.log(Level.WARNING, t.getMessage(), t);
            }
            return true;
        }

        void start() {
            thread.start();
        }

        void stopNow() {
            clearMigrationQueue();
            thread.interrupt();
        }
    }

    private class ReplicaSyncInfo {
        final int partitionId;
        final long requestTime = Clock.currentTimeMillis();
        final Address target;

        private ReplicaSyncInfo(int partitionId, Address target) {
            this.partitionId = partitionId;
            this.target = target;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReplicaSyncInfo that = (ReplicaSyncInfo) o;

            if (partitionId != that.partitionId) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ReplicaSyncInfo{");
            sb.append("partitionId=").append(partitionId);
            sb.append(", target=").append(target);
            sb.append(", requestTime=").append(requestTime);
            sb.append('}');
            return sb.toString();
        }
    }

    public void reset() {
        clearMigrationQueue();
        lock.lock();
        try {
            initialized = false;
            for (PartitionInfo partition : partitions) {
                for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                    partition.setReplicaAddress(i, null);
                }
            }
            activeMigrations.clear();
            completedMigrations.clear();
            stateVersion.set(0);
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        logger.log(Level.FINEST, "Shutting down the partition service");
        migrationThread.stopNow();
        reset();
    }

    private void clearMigrationQueue() {
        migrationQueue.clear();
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
        eventService.publishEvent(SERVICE_NAME, registrations, event);
    }

    public String addMigrationListener(MigrationListener migrationListener) {
        final EventRegistration registration = nodeEngine.getEventService().registerListener(SERVICE_NAME, SERVICE_NAME, migrationListener);
        return registration.getId();
    }

    public boolean removeMigrationListener(final String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
    }

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionManager[" + stateVersion + "] {\n");
        sb.append("\n");
        sb.append("migrationQ: ").append(migrationQueue.size());
        sb.append("\n}");
        return sb.toString();
    }
}
