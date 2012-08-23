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

package com.hazelcast.impl.partition;

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.base.SystemLogService;
import com.hazelcast.impl.spi.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.util.Clock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

public class PartitionManager {
    public static final String PARTITION_SERVICE_NAME = "hz:PartitionService";

    private static final long MIGRATING_PARTITION_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(300); // 5 MINUTES
    private static final long REPARTITIONING_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(300); // 5 MINUTES
    private static final int REPARTITIONING_TASK_COUNT_THRESHOLD = 20;
    private static final int REPARTITIONING_TASK_REPLICA_THRESHOLD = 2;

    final Node node;
    final NodeService nodeService;
    private final ILogger logger;
    private final int partitionCount;
    private final PartitionInfo[] partitions;
    private final Lock lock = new ReentrantLock();
    private volatile MigratingPartition migratingPartition; // updates will be done under lock, but reads will be multithreaded.
    private volatile boolean initialized = false; // updates will be done under lock, but reads will be multithreaded.
    private final AtomicInteger version = new AtomicInteger();
    private final List<PartitionListener> lsPartitionListeners = new CopyOnWriteArrayList<PartitionListener>();
    private final MigrationService migrationService;
    private final int partitionMigrationInterval;
    private final long partitionMigrationTimeout;
    private final int immediateBackupInterval;
    private final BlockingQueue<Runnable> immediateTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final Queue<Runnable> scheduledTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final AtomicBoolean sendingDiffs = new AtomicBoolean(false);
    private final AtomicBoolean migrationActive = new AtomicBoolean(true); // for testing purposes only
    private final AtomicLong lastRepartitionTime = new AtomicLong();
    private final SystemLogService systemLogService;

    public final PartitionServiceImpl partitionServiceImpl;

    public PartitionManager(final Node node) {
        this.partitionCount = node.groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
        this.node = node;
        this.nodeService = node.nodeService;
        this.logger = this.node.getLogger(PartitionManager.class.getName());
        this.partitions = new PartitionInfo[partitionCount];
        systemLogService = node.getSystemLogService();
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new PartitionInfo(i, new PartitionListener() {
                public void replicaChanged(PartitionReplicaChangeEvent event) {
                    for (PartitionListener partitionListener : lsPartitionListeners) {
                        partitionListener.replicaChanged(event);
                    }
                    if (event.getReplicaIndex() == 0 && event.getNewAddress() == null
                            && node.isActive() && node.joined()) {
                        final String warning = "Owner of partition is being removed! " +
                                "Possible data loss for partition[" + event.getPartitionId() + "]. "
                                + event;
                        logger.log(Level.WARNING, warning);
                        systemLogService.logPartition(warning);
                    }
                    if (node.isMaster()) {
                        version.incrementAndGet();
                    }
                }
            });
        }
        partitionMigrationInterval = node.groupProperties.PARTITION_MIGRATION_INTERVAL.getInteger() * 1000;
        // partitionMigrationTimeout is 1.5 times of real timeout
        partitionMigrationTimeout = (long) (node.groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong() * 1.5f);
        immediateBackupInterval = node.groupProperties.IMMEDIATE_BACKUP_INTERVAL.getInteger() * 1000;
        migrationService = new MigrationService(node);
        migrationService.start();
        int partitionTableSendInterval = node.groupProperties.PARTITION_TABLE_SEND_INTERVAL.getInteger();
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        nodeService.getScheduledExecutorService().scheduleAtFixedRate(new SendClusterStateTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);
        nodeService.getScheduledExecutorService().scheduleAtFixedRate(new CheckMigratingPartitionTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);
        nodeService.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (node.isMaster() && node.isActive()
                        && initialized && shouldCheckRepartitioning()) {
                    logger.log(Level.FINEST, "Checking partition table for repartitioning...");
                    immediateTasksQueue.add(new CheckRepartitioningTask());
                }
            }
        }, 180, 180, TimeUnit.SECONDS);
        partitionServiceImpl = new PartitionServiceImpl(this);
        nodeService.registerService(PARTITION_SERVICE_NAME, this);
    }

    public Address getOwner(int partitionId) {
//        node.checkServiceThread();
        if (!initialized) {
            firstArrangement();
        }
        Address owner = partitions[partitionId].getOwner();
        if (owner == null && !node.isMaster()) {
//            node.clusterManager.sendProcessableTo(new AssignPartitions(), node.getMasterAddress());
            nodeService.createSingleInvocation(PARTITION_SERVICE_NAME, new AssignPartitions(), -1)
                    .setTarget(node.getMasterAddress()).setTryCount(1).build().invoke();
            return partitions[partitionId].getOwner();
        }
        return owner;
    }

    public void firstArrangement() {
//        node.checkServiceThread();
        if (!node.isMaster() || !node.isActive()) return;
        if (!hasStorageMember()) return;
        if (!initialized) {
            lock.lock();
            try {
                if (initialized) return;
                PartitionStateGenerator psg = getPartitionStateGenerator();
                logger.log(Level.INFO, "Initializing cluster partition table first arrangement...");
                PartitionInfo[] newState = psg.initialize(node.getClusterImpl().getMembers(), partitionCount);
                if (newState != null) {
                    for (PartitionInfo partitionInfo : newState) {
                        partitions[partitionInfo.getPartitionId()].setPartitionInfo(partitionInfo);
                    }
                }
                initialized = true;
                sendPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
        }
    }

    private void sendPartitionRuntimeState() {
        if (!node.isMaster() || !node.isActive() || !node.joined()) {
            return;
        }
        if (!migrationActive.get()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }
        if (!initialized) {
            // do not send partition state until initialized!
            return;
        }
        lock.lock();
        try {
            final long clusterTime = node.getClusterImpl().getClusterTime();
            final Collection<MemberImpl> members = node.clusterImpl.getMemberList();
            final List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(members.size());
            for (MemberImpl member : members) {
                memberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType(), member.getUuid()));
            }
            PartitionStateOperation operation = new PartitionStateOperation(memberInfos, partitions, clusterTime,
                    version.get());
            //        node.clusterManager.sendProcessableToAll(operation, false);
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    nodeService.createSingleInvocation(PARTITION_SERVICE_NAME, operation, -1)
                            .setTarget(member.getAddress()).setTryCount(1).build().invoke();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public Collection<ServiceMigrationOperation> collectMigrationTasks(final int partitionId, final int replicaIndex,
                                                                       final Address newAddress, boolean diffOnly) {
        final Address thisAddress = node.getThisAddress();
        addActiveMigration(partitionId, replicaIndex, thisAddress, newAddress);
        final Collection<MigrationAware> services = node.nodeService.getServices(MigrationAware.class);
        final Collection<ServiceMigrationOperation> tasks = new LinkedList<ServiceMigrationOperation>();
        for (MigrationAware service : services) {
            ServiceMigrationOperation op = service.getMigrationTask(partitionId, replicaIndex, diffOnly);
            if (op != null) {
                tasks.add(op);
            }
        }
        return tasks;
    }

    public boolean runMigrationTasks(Collection<ServiceMigrationOperation> tasks, final int partitionId, final int replicaIndex,
                                     final Address from) {
        addActiveMigration(partitionId, replicaIndex, from, node.getThisAddress());
        boolean error = false;
        for (ServiceMigrationOperation task : tasks) {
            try {
                Invocation inv = node.nodeService.createSingleInvocation(task.getServiceName(), task, partitionId)
                        .setTryCount(1).setTarget(node.getThisAddress()).build();
                inv.invoke().get();
            } catch (Throwable e) {
                error = true;
                logger.log(Level.SEVERE, e.getMessage(), e);
                break;
            }
        }
        for (ServiceMigrationOperation task : tasks) {
            if (!error) {
                task.onSuccess();
            } else {
                task.onError();
            }
        }
        return !error;
    }

    private void addActiveMigration(final MigratingPartition migrationRequestTask) {
        addActiveMigration(migrationRequestTask.getPartitionId(), migrationRequestTask.getReplicaIndex(),
                migrationRequestTask.getFromAddress(), migrationRequestTask.getToAddress());
    }

    private void addActiveMigration(final int partitionId, final int replicaIndex,
                                    final Address currentAddress, final Address newAddress) {
//        node.checkServiceThread();
        lock.lock();
        try {
            final MigratingPartition currentMigratingPartition = migratingPartition;
            final MigratingPartition newMigratingPartition = new MigratingPartition(partitionId,
                    replicaIndex, currentAddress, newAddress);
            if (!newMigratingPartition.equals(currentMigratingPartition)) {
                if (currentMigratingPartition != null) {
                    logger.log(Level.FINEST, "Replacing current " + currentMigratingPartition
                            + " with " + newMigratingPartition);
                }
                migratingPartition = newMigratingPartition;
            }
        } finally {
            lock.unlock();
        }
    }

    private void compareAndSetActiveMigratingPartition(final MigratingPartition expectedMigratingPartition,
                                                       final MigratingPartition newMigratingPartition) {
//        node.checkServiceThread();
        lock.lock();
        try {
            if (expectedMigratingPartition == null) {
                if (migratingPartition == null) {
                    migratingPartition = newMigratingPartition;
                }
            } else if (expectedMigratingPartition.equals(migratingPartition)) {
                migratingPartition = newMigratingPartition;
            }
        } finally {
            lock.unlock();
        }
    }

    public void syncForDead(final MemberImpl deadMember) {
        final Address deadAddress = deadMember.getAddress();
        final Address thisAddress = node.getThisAddress();
        if (deadAddress == null || deadAddress.equals(thisAddress)) {
            return;
        }
        if (!hasStorageMember()) {
            reset();
        }
        if (!deadMember.isLiteMember()) {
            clearTaskQueues();
            lock.lock();
            try {
                // inactivate migration and sending of PartitionRuntimeState (@see #sendPartitionRuntimeState)
                // let all members notice the dead and fix their own records and indexes.
                // otherwise new master may take action fast and send new partition state
                // before other members realize the dead one and fix their records.
                final boolean migrationStatus = migrationActive.getAndSet(false);
                partitionServiceImpl.reset();
                checkMigratingPartitionForDead(deadAddress);
                // list of partitions those have dead member in their replicas
                // !! this should be calculated before dead member is removed from partition table !!
                int[] indexesOfDead = new int[partitions.length];
                for (PartitionInfo partition : partitions) {
                    indexesOfDead[partition.getPartitionId()] = partition.getReplicaIndexOf(deadAddress);
                }
                // shift partition table up.
                for (PartitionInfo partition : partitions) {
                    // safe removal of dead address from partition table.
                    // there might be duplicate dead address in partition table
                    // during migration tasks' execution (when there are multiple backups and
                    // copy backup tasks; see MigrationRequestOperation selfCopyReplica.)
                    // or because of a bug.
                    while (partition.onDeadAddress(deadAddress)) ;
                }
                //        fixCMapsForDead(deadAddress, indexesOfDead);
                fixReplicasAndPartitionsForDead(deadMember, indexesOfDead);
                // activate migration back after connectionDropTime x 10 milliseconds,
                // thinking optimistically that all nodes notice the dead one in this period.
                final long waitBeforeMigrationActivate = node.groupProperties.CONNECTION_MONITOR_INTERVAL.getLong()
                        * node.groupProperties.CONNECTION_MONITOR_MAX_FAULTS
                        .getInteger() * 10;
                nodeService.getScheduledExecutorService().schedule(new Runnable() {
                    public void run() {
                        migrationActive.compareAndSet(false, migrationStatus);
                    }
                }, waitBeforeMigrationActivate, TimeUnit.MILLISECONDS);
            } finally {
                lock.unlock();
            }
        }
    }

    private void fixReplicasAndPartitionsForDead(final MemberImpl deadMember, final int[] indexesOfDead) {
        if (!deadMember.isLiteMember() && node.isMaster() && node.isActive()) {
            lock.lock();
            try {
                sendingDiffs.set(true);
                logger.log(Level.INFO, "Starting to send partition replica diffs..." + sendingDiffs.get());
                int diffCount = 0;
                final int maxBackupCount = getMaxBackupCount();
                for (int partitionId = 0; partitionId < indexesOfDead.length; partitionId++) {
                    int indexOfDead = indexesOfDead[partitionId];
                    if (indexOfDead != -1) {
                        PartitionInfo partition = partitions[partitionId];
                        Address owner = partition.getOwner();
                        if (owner == null) {
                            logger.log(Level.FINEST, "Owner of one of the replicas of Partition[" +
                                    partitionId + "] is dead, but partition owner " +
                                    "could not be found either!");
                            logger.log(Level.FINEST, partition.toString());
                            continue;
                        }
                        // send replica diffs to new replica owners after partition table shift.
                        for (int replicaIndex = indexOfDead; replicaIndex < maxBackupCount; replicaIndex++) {
                            Address target = partition.getReplicaAddress(replicaIndex);
                            if (target != null && !target.equals(owner)) {
                                if (getMember(target) != null) {
                                    MigrationRequestOperation mrt = new MigrationRequestOperation(partitionId, owner,
                                            target, replicaIndex, false, true);
                                    immediateTasksQueue.offer(new Migrator(mrt));
                                    diffCount++;
                                } else {
                                    logger.log(Level.WARNING, "Target member of replica diff task couldn't found! "
                                            + "Replica: " + replicaIndex + ", Dead: " + deadMember +
                                            "\n" + partition);
                                }
                            }
                        }
                        // if index of dead member is equal to or less than maxBackupCount
                        // clear indexes of equal to and greater than maxBackupCount of partition.
                        if (indexOfDead <= maxBackupCount) {
                            for (int index = maxBackupCount; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                                partition.setReplicaAddress(index, null);
                            }
                        }
                    }
                }
                sendPartitionRuntimeState();
                final int totalDiffCount = diffCount;
                immediateTasksQueue.offer(new Runnable() {
                    public void run() {
                        logger.log(Level.INFO,
                                "Total " + totalDiffCount + " partition replica diffs have been processed.");
                        sendingDiffs.set(false);
                    }
                });
                immediateTasksQueue.offer(new PrepareRepartitioningTask());
            } finally {
                lock.unlock();
            }
        }
    }

    private void checkMigratingPartitionForDead(final Address deadAddress) {
        lock.lock();
        try {
            if (migratingPartition != null) {
                if (deadAddress.equals(migratingPartition.getFromAddress())
                        || deadAddress.equals(migratingPartition.getToAddress())) {
                    migratingPartition = null;
                }
            }
        } finally {
            lock.unlock();
        }
    }
//    private void fixCMapsForDead(final Address deadAddress, final int[] indexesOfDead) {
//        Address thisAddress = node.getThisAddress();
//        for (CMap cmap : node.concurrentMapManager.maps.values()) {
//            cmap.onDisconnect(deadAddress);
//            Object[] records = cmap.mapRecords.values().toArray();
//            for (Object recordObject : records) {
//                if (recordObject != null) {
//                    Record record = (Record) recordObject;
//                    cmap.onDisconnect(record, deadAddress);
//                    final int partitionId = record.getBlockId();
//                    // owner of the partition is dead
//                    // and record is active
//                    // and new owner of partition is this member.
//                    if (indexesOfDead[partitionId] == 0
//                            && record.isActive()
//                            && thisAddress.equals(partitions[partitionId].getOwner())) {
//                        cmap.markAsDirty(record, true);
//                        // update the indexes
//                        cmap.updateIndexes(record);
//                    }
//                }
//            }
//        }
//    }

    private int getMaxBackupCount() {
//        final Collection<CMap> cmaps = node.concurrentMapManager.maps.values();
//        if (!cmaps.isEmpty()) {
//            int maxBackupCount = 0;
//            for (final CMap cmap : cmaps) {
//                maxBackupCount = Math.max(maxBackupCount, cmap.getTotalBackupCount());
//            }
//            return maxBackupCount;
//        }
        return 1; // if there is no map, avoid extra processing.
    }

    public void syncForAdd() {
        if (node.isMaster() && node.isActive()) {
            if (sendingDiffs.get()) {
                logger.log(Level.INFO, "MigrationService is already sending diffs for dead member, " +
                        "no need to initiate task!");
            } else {
                // to avoid repartitioning during a migration process.
                clearTaskQueues();
                immediateTasksQueue.offer(new PrepareRepartitioningTask());
            }
        }
    }

    public void processPartitionRuntimeState(PartitionRuntimeState runtimeState) {
//        node.checkServiceThread();
        lock.lock();
        try {
            final Address sender = runtimeState.getEndpoint();
            if (node.isMaster()) {
                logger.log(Level.WARNING, "This is the master node and received a PartitionRuntimeState from "
                        + sender + ". Ignoring incoming state! ");
                return;
            } else {
                final Address master = node.getMasterAddress();
                if (sender == null || master == null || !master.equals(sender)) {
                    logger.log(Level.WARNING, "Received a PartitionRuntimeState, but its sender doesn't seem master!" +
                            " => Sender: " + sender + ", Master: " + master + "! " +
                            "(Ignore if master node has changed recently.)");
                }
            }
            PartitionInfo[] newPartitions = runtimeState.getPartitions();
            for (PartitionInfo newPartition : newPartitions) {
                PartitionInfo currentPartition = partitions[newPartition.getPartitionId()];
                for (int index = 0; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                    Address address = newPartition.getReplicaAddress(index);
                    if (address != null && node.clusterImpl.getMember(address) == null) {
                        logger.log(Level.WARNING,
                                "Unknown " + address + " is found in received partition table from master "
                                        + sender + ". Probably it is dead. Partition: " + newPartition);
                    }
                }
                currentPartition.setPartitionInfo(newPartition);
                checkMigratingPartitionFor(currentPartition);
            }
            initialized = true;
            version.set(runtimeState.getVersion());
        } finally {
            lock.unlock();
        }
    }

    private void checkMigratingPartitionFor(PartitionInfo partition) {
//        node.checkServiceThread();
        lock.lock();
        try {
            final MigratingPartition mPartition = migratingPartition;
            if (mPartition != null && partition.getPartitionId() == mPartition.getPartitionId()) {
                final Address targetAddress = mPartition.getToAddress();
                if (targetAddress != null
                        && targetAddress.equals(partition.getReplicaAddress(mPartition.getReplicaIndex()))) {
                    migratingPartition = null;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private PartitionStateGenerator getPartitionStateGenerator() {
        return PartitionStateGeneratorFactory.newConfigPartitionStateGenerator(
                node.getConfig().getPartitionGroupConfig());
    }

    private boolean hasStorageMember() {
        for (Member member : node.getClusterImpl().getMembers()) {
            if (!member.isLiteMember()) {
                return true;
            }
        }
        return false;
    }

    public MigratingPartition getMigratingPartition() {
        return migratingPartition;
    }

    public void addPartitionListener(PartitionListener partitionListener) {
        lsPartitionListeners.add(partitionListener);
    }

    public PartitionInfo[] getPartitions() {
        return partitions;
    }

    public MemberImpl getMember(Address address) {
        return node.clusterImpl.getMember(address);
    }

    public int getVersion() {
        return version.get();
    }

    public boolean shouldPurge(int partitionId, int maxBackupCount) {
        if (isPartitionMigrating(partitionId)) return false;
        Address thisAddress = node.getThisAddress();
        PartitionInfo partitionInfo = getPartition(partitionId);
        return !partitionInfo.isOwnerOrBackup(thisAddress, maxBackupCount);
    }

    /**
     * @param partitionId
     * @return true if any replica of partition is migrating, false otherwise
     */
    public boolean isPartitionMigrating(int partitionId) {
        // volatile read
        final MigratingPartition currentMigratingPartition = migratingPartition;
        return currentMigratingPartition != null
                && currentMigratingPartition.getPartitionId() == partitionId;
    }

    /**
     * @param partitionId
     * @return true if owned replica (0) of partition is migrating, false otherwise
     */
    public boolean isOwnedPartitionMigrating(int partitionId) {
        return isPartitionMigrating(partitionId, 0);
    }

    /**
     * @param partitionId
     * @param replicaIndex
     * @return true if replicaIndex of partition is migrating, false otherwise
     */
    public boolean isPartitionMigrating(int partitionId, int replicaIndex) {
        // volatile read
        final MigratingPartition currentMigratingPartition = migratingPartition;
        return currentMigratingPartition != null
                && currentMigratingPartition.getPartitionId() == partitionId
                && currentMigratingPartition.getReplicaIndex() == replicaIndex;
    }

    public PartitionInfo getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public boolean hasActiveBackupTask() {
        if (!initialized) return false;
        if (node.isLiteMember()) return false;
        int maxBackupCount = getMaxBackupCount();
        if (maxBackupCount == 0) return false;
        MemberGroupFactory mgf = PartitionStateGeneratorFactory.newMemberGroupFactory(
                node.config.getPartitionGroupConfig());
        if (mgf.createMemberGroups(node.getClusterImpl().getMembers()).size() < 2) return false;
        boolean needBackup = false;
        if (immediateTasksQueue.isEmpty()) {
            for (PartitionInfo partition : partitions) {
                if (partition.getReplicaAddress(1) == null) {
                    needBackup = true;
                    logger.log(Level.WARNING, node.getThisAddress()
                            + " still has no replica for partitionId:" + partition.getPartitionId());
                    break;
                }
            }
        }
        return needBackup || !immediateTasksQueue.isEmpty();
    }

    public void fireMigrationEvent(final MigrationStatus status, int partitionId, Address from, Address to) {
        final MemberImpl current = node.clusterImpl.getMember(from);
        final MemberImpl newOwner = node.clusterImpl.getMember(to);
        final MigrationEvent migrationEvent = new MigrationEvent(node, partitionId, current, newOwner);
        systemLogService.logPartition("MigrationEvent [" + status + "] " + migrationEvent);
        partitionServiceImpl.doFireMigrationEvent(status, migrationEvent);
    }

    private boolean shouldCheckRepartitioning() {
        return immediateTasksQueue.isEmpty() && scheduledTasksQueue.isEmpty()
                && lastRepartitionTime.get() < (Clock.currentTimeMillis() - REPARTITIONING_CHECK_INTERVAL)
                && migratingPartition == null;
    }

    public final int getPartitionId(Data key) {
        int hash = key.getPartitionHash();
        return (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % partitionCount;
    }

    public Address getPartitionOwner(int partitionId) {
        return getOwner(partitionId);
    }

    public Address getKeyOwner(Data key) {
        int partitionId = getPartitionId(key);
        return getPartitionOwner(partitionId);
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public static class AssignPartitions extends AbstractOperation implements NonBlockingOperation, NoReply {
        public void run() {
            getNodeService().getNode().partitionManager.firstArrangement();
        }
    }

    private class SendClusterStateTask implements Runnable {
        public void run() {
            if (node.isMaster() && node.isActive()) {
                if (!scheduledTasksQueue.isEmpty() || !immediateTasksQueue.isEmpty()) {
                    logger.log(Level.INFO, "Remaining migration tasks in queue => Immediate-Tasks: " + immediateTasksQueue.size()
                            + ", Scheduled-Tasks: " + scheduledTasksQueue.size());
                    System.err.println("====> " + immediateTasksQueue);
                }
                sendPartitionRuntimeState();
            }
        }
    }

    public static class RemotelyCheckMigratingPartition extends AbstractOperation implements NonBlockingOperation {
        MigratingPartition migratingPartition;

        public RemotelyCheckMigratingPartition() {
        }

        public RemotelyCheckMigratingPartition(final MigratingPartition migratingPartition) {
            this.migratingPartition = migratingPartition;
        }

        public void run() {
            boolean result = false;
            if (migratingPartition != null) {
                Node node = getNodeService().getNode();
                final MigratingPartition masterMigratingPartition = node.partitionManager.migratingPartition;
                result = migratingPartition.equals(masterMigratingPartition);
            }
            getResponseHandler().sendResponse(result);
        }

        public void readInternal(final DataInput in) throws IOException {
            if (in.readBoolean()) {
                migratingPartition = new MigratingPartition();
                migratingPartition.readData(in);
            }
        }

        public void writeInternal(final DataOutput out) throws IOException {
            boolean b = migratingPartition != null;
            out.writeBoolean(b);
            if (b) {
                migratingPartition.writeData(out);
            }
        }
    }

    private class CheckMigratingPartitionTask implements Runnable {
        public void run() {
            if (!node.isMaster()) {
                final MigratingPartition currentMigratingPartition = migratingPartition;
                if (currentMigratingPartition != null
                        && (Clock.currentTimeMillis() - currentMigratingPartition.getCreationTime())
                        > MIGRATING_PARTITION_CHECK_INTERVAL) {
//                    try {
//                        Invocation inv = nodeService.createSingleInvocation(PARTITION_SERVICE_NAME,
//                                new RemotelyCheckMigratingPartition(currentMigratingPartition), -1)
//                                .setTarget(node.getMasterAddress()).setTryCount(1).build();
//
//                        Boolean valid = (Boolean) IOUtil.toObject(inv.invoke().get());
//                        if (valid) {
//                            logger.log(Level.FINEST, "Master has confirmed current " + currentMigratingPartition);
//                        } else {
//                            logger.log(Level.INFO, currentMigratingPartition +
//                                    " could not be validated with master! " +
//                                    "Removing current MigratingPartition...");
//                            node.clusterManager.enqueueAndReturn(new Processable() {
//                                public void process() {
//                                    migratingPartition = null;
//                                }
//                            });
//                        }
//                    } catch (Throwable t) {
//                        logger.log(Level.WARNING, t.getMessage(), t);
//                    }
                }
            }
        }
    }

    private class PrepareRepartitioningTask implements Runnable {
        final List<MigrationRequestOperation> lostQ = new ArrayList<MigrationRequestOperation>();
        final List<MigrationRequestOperation> scheduledQ = new ArrayList<MigrationRequestOperation>(partitionCount);
        final List<MigrationRequestOperation> immediateQ = new ArrayList<MigrationRequestOperation>(partitionCount * 2);

        private PrepareRepartitioningTask() {
        }

        public final void run() {
            if (node.isMaster() && node.isActive() && initialized) {
                doRun();
            }
        }

        void doRun() {
            prepareMigrationTasks();
            logger.log(Level.INFO, "Re-partitioning cluster data... Immediate-Tasks: "
                    + immediateQ.size() + ", Scheduled-Tasks: " + scheduledQ.size());
            fillMigrationQueues();
        }

        void prepareMigrationTasks() {
            PartitionStateGenerator psg = getPartitionStateGenerator();
            psg.reArrange(partitions, node.getClusterImpl().getMembers(),
                    partitionCount, lostQ, immediateQ, scheduledQ);
        }

        void fillMigrationQueues() {
            lastRepartitionTime.set(Clock.currentTimeMillis());
            if (!lostQ.isEmpty()) {
//                node.clusterManager.enqueueAndReturn(new LostPartitionsAssignmentProcess(lostQ));
                immediateTasksQueue.offer(new LostPartitionsAssignmentProcess(lostQ));
                logger.log(Level.WARNING, "Assigning new owners for " + lostQ.size() +
                        " LOST partitions!");
            }
            for (MigrationRequestOperation migrationRequestTask : immediateQ) {
                immediateTasksQueue.offer(new Migrator(migrationRequestTask));
            }
            immediateQ.clear();
            for (MigrationRequestOperation migrationRequestTask : scheduledQ) {
                scheduledTasksQueue.offer(new Migrator(migrationRequestTask));
            }
            scheduledQ.clear();
        }
    }

    private class LostPartitionsAssignmentProcess implements Runnable {
        final List<MigrationRequestOperation> lostQ;

        private LostPartitionsAssignmentProcess(final List<MigrationRequestOperation> lostQ) {
            this.lostQ = lostQ;
        }

        public void run() {
            if (!node.isMaster() || !node.isActive()) return;
            lock.lock();
            try {
                for (MigrationRequestOperation migrationRequestTask : lostQ) {
                    int partitionId = migrationRequestTask.getPartitionId();
                    int replicaIndex = migrationRequestTask.getReplicaIndex();
                    if (replicaIndex != 0 || partitionId >= partitionCount) {
                        logger.log(Level.WARNING, "Wrong task for lost partitions assignment process" +
                                " => " + migrationRequestTask);
                        continue;
                    }
                    PartitionInfo partition = partitions[partitionId];
                    Address newOwner = migrationRequestTask.getToAddress();
                    MemberImpl ownerMember = node.clusterImpl.getMember(newOwner);
                    if (ownerMember != null) {
                        partition.setReplicaAddress(replicaIndex, newOwner);
//                        node.concurrentMapManager.sendMigrationEvent(false, migrationRequestTask);
                    }
                }
                sendPartitionRuntimeState();
            } finally {
                lock.unlock();
            }
        }
    }

    private class CheckRepartitioningTask extends PrepareRepartitioningTask implements Runnable {
        void doRun() {
            if (shouldCheckRepartitioning()) {
                final int v = version.get();
                prepareMigrationTasks();
                int totalTasks = 0;
                for (MigrationRequestOperation task : immediateQ) {
                    if (task.getReplicaIndex() <= REPARTITIONING_TASK_REPLICA_THRESHOLD) {
                        totalTasks++;
                    }
                }
                for (MigrationRequestOperation task : scheduledQ) {
                    if (task.getReplicaIndex() <= REPARTITIONING_TASK_REPLICA_THRESHOLD) {
                        totalTasks++;
                    }
                }
                if (!lostQ.isEmpty() || totalTasks > REPARTITIONING_TASK_COUNT_THRESHOLD) {
                    logger.log(Level.WARNING, "Something weird! Migration task queues are empty," +
                            " last repartitioning executed on " + lastRepartitionTime.get() +
                            " but repartitioning check resulted " + totalTasks + " tasks" +
                            " and " + lostQ.size() + " lost partitions!");
                    if (version.get() == v && shouldCheckRepartitioning()) {
                        fillMigrationQueues();
                    }
                }
            }
        }
    }

    private class Migrator implements Runnable {
        final MigrationRequestOperation migrationRequestOp;

        Migrator(MigrationRequestOperation migrationRequestOperation) {
            this.migrationRequestOp = migrationRequestOperation;
        }

        public void run() {
            try {
                if (!node.isActive() || !node.isMaster()) {
                    return;
                }
//                if (migrationRequestOp.isMigration() && migrationRequestOp.getReplicaIndex() == 0) {
//                    node.clusterManager.enqueueAndWait(new Processable() {
//                        public void process() {
//                            node.concurrentMapManager.sendMigrationEvent(true, migrationRequestOp);
//                        }
//                    }, 100);
//                }
                if (migrationRequestOp.getToAddress() == null) {
                    // A member is dead, this replica should not have an owner!
                    logger.log(Level.FINEST, "Fixing partition, " + migrationRequestOp.getReplicaIndex()
                            + ". replica of partition[" + migrationRequestOp.getPartitionId() + "] should be removed.");
                    removeReplicaOwner();
                } else {
                    MemberImpl fromMember = null;
                    Boolean result = Boolean.FALSE;
//                    if (migrationRequestOp.isMigration()) {
//                        fromMember = getMember(migrationRequestOp.getFromAddress());
//                    } else {
                    // ignore fromAddress of task and get actual owner from partition table
                    final int partitionId = migrationRequestOp.getPartitionId();
                    fromMember = getMember(partitions[partitionId].getOwner());
//                    }
                    logger.log(Level.FINEST, "Started Migration : " + migrationRequestOp);
                    systemLogService.logPartition("Started Migration : " + migrationRequestOp);
                    if (fromMember != null) {
                        migrationRequestOp.setFromAddress(fromMember.getAddress());
                        Invocation inv = node.nodeService.createSingleInvocation(PARTITION_SERVICE_NAME,
                                migrationRequestOp, migrationRequestOp.getPartitionId())
                                .setTryCount(3).setTryPauseMillis(1000).setTarget(migrationRequestOp.getFromAddress())
                                .setReplicaIndex(migrationRequestOp.getReplicaIndex()).build();
//                        DistributedTask task = new DistributedTask(migrationRequestOp, fromMember);
//                        node.clusterManager.enqueueAndWait(new Processable() {
//                            public void process() {
                        addActiveMigration(migrationRequestOp.createMigratingPartition());
//                            }
//                        });
                        Future future = inv.invoke();
//                        Future future = node.factory
//                                .getExecutorService(MIGRATION_EXECUTOR_NAME).submit(task);
                        try {
                            result = (Boolean) IOUtil.toObject(future.get(partitionMigrationTimeout, TimeUnit.SECONDS));
                        } catch (Throwable e) {
                            logger.log(Level.WARNING, "Failed migrating from " + fromMember, e);
                        }
                    } else {
                        // Partition is lost! Assign new owner and exit.
                        result = Boolean.TRUE;
                    }
                    logger.log(Level.FINEST, "Finished Migration : " + migrationRequestOp);
                    systemLogService.logPartition("Finished Migration : " + migrationRequestOp);
                    if (Boolean.TRUE.equals(result)) {
//                        node.clusterManager.enqueueAndWait(new ProcessMigrationResult(migrationRequestOp), 10000);
                        processMigrationResult();
                    } else {
                        // remove active partition migration
                        logger.log(Level.WARNING, "Migration task has failed => " + migrationRequestOp);
                        systemLogService.logPartition("Migration task has failed => " + migrationRequestOp);
//                        node.clusterManager.enqueueAndWait(new Processable() {
//                            public void process() {
                        compareAndSetActiveMigratingPartition(migrationRequestOp.createMigratingPartition(), null);
//                            }
//                        });
                    }
                }
            } catch (Throwable t) {
                logger.log(Level.WARNING, "Error [" + t.getClass() + ": " + t.getMessage() + "] " +
                        "while executing " + migrationRequestOp);
                logger.log(Level.FINEST, t.getMessage(), t);
                systemLogService.logPartition("Failed! " + migrationRequestOp);
            }
        }

        private void processMigrationResult() {
            int partitionId = migrationRequestOp.getPartitionId();
            int replicaIndex = migrationRequestOp.getReplicaIndex();
            PartitionInfo partition = partitions[partitionId];
            if (PartitionInfo.MAX_REPLICA_COUNT < replicaIndex) {
                String msg = "Migrated [" + partitionId + ":" + replicaIndex
                        + "] but cannot assign. Length:" + PartitionInfo.MAX_REPLICA_COUNT;
                logger.log(Level.WARNING, msg);
            } else {
                lock.lock();
                try {
                    Address newOwner = migrationRequestOp.getToAddress();
                    MemberImpl ownerMember = node.clusterImpl.getMember(newOwner);
                    if (ownerMember == null) return;
                    partition.setReplicaAddress(replicaIndex, newOwner);
                    // if this partition should be copied back,
                    // just set partition's replica address
                    // before data is cleaned up.
                    if (migrationRequestOp.getSelfCopyReplicaIndex() > -1) {
                        partition.setReplicaAddress(migrationRequestOp.getSelfCopyReplicaIndex(),
                                migrationRequestOp.getFromAddress());
                    }
                    sendPartitionRuntimeState();
                    compareAndSetActiveMigratingPartition(migrationRequestOp.createMigratingPartition(), null);
                    //                if (replicaIndex == 0) {
                    //                    node.concurrentMapManager.sendMigrationEvent(false, migrationRequestTask);
                    //                }
                } finally {
                    lock.unlock();
                }
            }
        }

        private void removeReplicaOwner() {
            lock.lock();
            try {
                int partitionId = migrationRequestOp.getPartitionId();
                int replicaIndex = migrationRequestOp.getReplicaIndex();
                PartitionInfo partition = partitions[partitionId];
                partition.setReplicaAddress(replicaIndex, null);
                migratingPartition = null;
            } finally {
                lock.unlock();
            }
        }
    }
//    private class ProcessMigrationResult implements Processable {
//        final MigrationRequestOperation migrationRequestTask;
//
//        private ProcessMigrationResult(final MigrationRequestOperation migrationRequestTask) {
//            this.migrationRequestTask = migrationRequestTask;
//        }
//
//        public void process() {
//            int partitionId = migrationRequestTask.getPartitionId();
//            int replicaIndex = migrationRequestTask.getReplicaIndex();
//            PartitionInfo partition = partitions[partitionId];
//            if (PartitionInfo.MAX_REPLICA_COUNT < replicaIndex) {
//                String msg = "Migrated [" + partitionId + ":" + replicaIndex
//                        + "] but cannot assign. Length:" + PartitionInfo.MAX_REPLICA_COUNT;
//                logger.log(Level.WARNING, msg);
//            } else {
//                Address newOwner = migrationRequestTask.getToAddress();
//                MemberImpl ownerMember = node.clusterManager.getMember(newOwner);
//                if (ownerMember == null) return;
//                partition.setReplicaAddress(replicaIndex, newOwner);
//                // if this partition should be copied back,
//                // just set partition's replica address
//                // before data is cleaned up.
//                if (migrationRequestTask.getSelfCopyReplicaIndex() > -1) {
//                    partition.setReplicaAddress(migrationRequestTask.getSelfCopyReplicaIndex(),
//                            migrationRequestTask.getFromAddress());
//                }
//                sendPartitionRuntimeState();
//                compareAndSetActiveMigratingPartition(migrationRequestTask, null);
////                if (replicaIndex == 0) {
////                    node.concurrentMapManager.sendMigrationEvent(false, migrationRequestTask);
////                }
//            }
//        }
//    }

    private class MigrationService implements Runnable {
        private final Thread thread;
        private boolean running = true;

        MigrationService(Node node) {
            thread = new Thread(node.threadGroup, this, node.getThreadNamePrefix("MigrationThread"));
        }

        public void run() {
            ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
            try {
                while (running) {
                    Runnable r = null;
                    while (isActive() && (r = immediateTasksQueue.poll()) != null) {
                        safeRunImmediate(r);
                    }
                    if (!running) {
                        break;
                    }
                    // wait for partitionMigrationInterval before executing scheduled tasks
                    // and poll immediate tasks occasionally during wait time.
                    long totalWait = 0L;
                    while (isActive() && (r != null || totalWait < partitionMigrationInterval)) {
                        long start = Clock.currentTimeMillis();
                        r = immediateTasksQueue.poll(1, TimeUnit.SECONDS);
                        safeRunImmediate(r);
                        totalWait += (Clock.currentTimeMillis() - start);
                    }
                    if (isActive()) {
                        r = scheduledTasksQueue.poll();
                        safeRun(r);
                    }
                    if (!migrationActive.get() || hasNoTasks()) {
                        Thread.sleep(250);
                        continue;
                    }
                }
            } catch (InterruptedException e) {
                logger.log(Level.FINEST, "MigrationService is interrupted: " + e.getMessage(), e);
                running = false;
            } finally {
                clearTaskQueues();
            }
        }

        private boolean hasNoTasks() {
            return (immediateTasksQueue.isEmpty() && scheduledTasksQueue.isEmpty());
        }

        private boolean isActive() {
            return running && !thread.isInterrupted() && migrationActive.get();
        }

        private boolean safeRun(final Runnable r) {
            if (r == null || !running) return false;
            try {
                r.run();
            } catch (Throwable t) {
                logger.log(Level.WARNING, t.getMessage(), t);
            }
            return true;
        }

        private void safeRunImmediate(final Runnable r) throws InterruptedException {
            if (safeRun(r) && immediateBackupInterval > 0) {
                Thread.sleep(immediateBackupInterval);
            }
        }

        private void start() {
            thread.start();
        }

        private void stop() {
            clearTaskQueues();
            try {
                final CountDownLatch stopLatch = new CountDownLatch(1);
                immediateTasksQueue.offer(new Runnable() {
                    public void run() {
                        running = false;
                        stopLatch.countDown();
                    }
                });
                stopLatch.await(1, TimeUnit.SECONDS);
            } catch (Throwable ignore) {
            }
        }

        private void stopNow() {
            clearTaskQueues();
//            stop();
            immediateTasksQueue.offer(new Runnable() {
                public void run() {
                    running = false;
                }
            });
            thread.interrupt();
        }

        private void checkAccess() {
            if (Thread.currentThread() != thread) {
                String msg = "Only migration thread can access this method. " + Thread.currentThread();
                logger.log(Level.SEVERE, msg);
                throw new Error(msg);
            }
        }
    }

    public void reset() {
        clearTaskQueues();
        lock.lock();
        try {
            initialized = false;
            migratingPartition = null;
            for (PartitionInfo partition : partitions) {
                for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                    partition.setReplicaAddress(i, null);
                }
            }
            version.set(0);
        } finally {
            lock.unlock();
        }
    }

    public void onRestart() {
        reset();
    }

    public void shutdown() {
        logger.log(Level.FINEST, "Shutting down the partition manager");
        migrationService.stopNow();
        reset();
    }

    private void clearTaskQueues() {
        immediateTasksQueue.clear();
        scheduledTasksQueue.clear();
    }
//    private void checkThreadAccess() {
//        migrationService.checkAccess();
//    }
//
//    private void runImmediatelyAndWait(final Runnable r) {
//        try {
//            final CountDownLatch l = new CountDownLatch(1);
//            immediateTasksQueue.add(new Runnable() {
//                public void run() {
//                    r.run();
//                    l.countDown();
//                }
//            });
//            l.await();
//        } catch (InterruptedException ignored) {
//        }
//    }
    // for testing purposes only
//    public boolean activateMigration() {
//        return migrationActive.compareAndSet(false, true);
//    }
    // for testing purposes only
//    public boolean inactivateMigration() {
//        migrationActive.compareAndSet(true, false);
//        while (migratingPartition != null) {
//            try {
//                Thread.sleep(250);
//            } catch (InterruptedException e) {
//                return true;
//            }
//        }
//        return true;
//    }

    // for testing purposes only
    void forcePartitionOwnerMigration(int partitionId, int replicaIndex, Address from, Address to) {
        MigrationRequestOperation mrt = new MigrationRequestOperation(partitionId, from, to, replicaIndex, true);
        immediateTasksQueue.offer(new Migrator(mrt));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionManager[" + version + "] {\n");
        sb.append("migratingPartition: " + migratingPartition);
        sb.append("\n");
        sb.append("immediateQ:" + immediateTasksQueue.size());
        sb.append(", scheduledQ:" + scheduledTasksQueue.size());
        sb.append("\n}");
        return sb.toString();
    }
}
