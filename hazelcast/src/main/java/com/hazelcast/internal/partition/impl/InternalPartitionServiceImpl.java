/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.PartitionInfo;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.PartitionServiceProxy;
import com.hazelcast.internal.partition.impl.MigrationManager.MigrateTaskReason;
import com.hazelcast.internal.partition.operation.AssignPartitions;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.internal.partition.operation.HasOngoingMigration;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.PartitionEvent;
import com.hazelcast.partition.PartitionEventListener;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil.ExceptionHandler;
import com.hazelcast.util.HashUtil;
import com.hazelcast.util.scheduler.ScheduledEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * The {@link InternalPartitionService} implementation.
 */
public class InternalPartitionServiceImpl implements InternalPartitionService, ManagedService,
        EventPublishingService<PartitionEvent, PartitionEventListener<PartitionEvent>>, PartitionAwareService {

    private static final int PARTITION_OWNERSHIP_WAIT_MILLIS = 10;
    private static final String EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT = "Partition state sync invocation timed out";

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final int partitionCount;

    private final long partitionMigrationTimeout;

    private final PartitionServiceProxy proxy;
    private final Lock lock = new ReentrantLock();
    private final InternalPartitionListener partitionListener;

    private final PartitionStateManager partitionStateManager;
    private final MigrationManager migrationManager;
    private final PartitionReplicaManager replicaManager;
    private final PartitionReplicaChecker partitionReplicaChecker;
    private final PartitionEventManager partitionEventManager;

    private final ExceptionHandler partitionStateSyncTimeoutHandler;

    // used to limit partition assignment requests sent to master
    private final AtomicBoolean triggerMasterFlag = new AtomicBoolean(false);

    private volatile Address lastMaster;

    public InternalPartitionServiceImpl(Node node) {
        HazelcastProperties properties = node.getProperties();
        this.partitionCount = properties.getInteger(GroupProperty.PARTITION_COUNT);
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(InternalPartitionService.class);

        partitionListener = new InternalPartitionListener(node, this);

        partitionStateManager = new PartitionStateManager(node, this, partitionListener);
        migrationManager = new MigrationManager(node, this, lock);
        replicaManager = new PartitionReplicaManager(node, this);

        partitionReplicaChecker = new PartitionReplicaChecker(node, this);
        partitionEventManager = new PartitionEventManager(node);

        partitionStateSyncTimeoutHandler =
                logAllExceptions(logger, EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT, Level.FINEST);

        partitionMigrationTimeout = properties.getMillis(GroupProperty.PARTITION_MIGRATION_TIMEOUT);

        proxy = new PartitionServiceProxy(nodeEngine, this);

        nodeEngine.getMetricsRegistry().scanAndRegister(this, "partitions");
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        int partitionTableSendInterval = node.getProperties().getSeconds(GroupProperty.PARTITION_TABLE_SEND_INTERVAL);
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new PublishPartitionRuntimeStateTask(node, this),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);

        migrationManager.start();
        replicaManager.scheduleReplicaVersionSync(executionService);
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        if (!partitionStateManager.isInitialized()) {
            firstArrangement();
        }
        final InternalPartition partition = partitionStateManager.getPartitionImpl(partitionId);
        if (partition.getOwnerOrNull() == null && !node.isMaster()) {
            if (!isClusterFormedByOnlyLiteMembers()) {
                triggerMasterToAssignPartitions();
            }
        }
        return partition.getOwnerOrNull();
    }

    @Override
    public Address getPartitionOwnerOrWait(int partitionId) {
        Address owner;
        while ((owner = getPartitionOwner(partitionId)) == null) {
            if (!nodeEngine.isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }

            ClusterState clusterState = node.getClusterService().getClusterState();
            if (clusterState != ClusterState.ACTIVE) {
                throw new IllegalStateException("Partitions can't be assigned since cluster-state: " + clusterState);
            }
            if (isClusterFormedByOnlyLiteMembers()) {
                throw new NoDataMemberInClusterException(
                        "Partitions can't be assigned since all nodes in the cluster are lite members");
            }

            try {
                Thread.sleep(PARTITION_OWNERSHIP_WAIT_MILLIS);
            } catch (InterruptedException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return owner;
    }

    @Override
    public void firstArrangement() {
        if (partitionStateManager.isInitialized()) {
            return;
        }

        if (!node.isMaster()) {
            triggerMasterToAssignPartitions();
            return;
        }

        lock.lock();
        try {
            if (partitionStateManager.isInitialized()) {
                return;
            }
            if (!partitionStateManager.initializePartitionAssignments()) {
                return;
            }
            publishPartitionRuntimeState();
        } finally {
            lock.unlock();
        }
    }

    private void triggerMasterToAssignPartitions() {
        if (partitionStateManager.isInitialized()) {
            return;
        }

        if (!node.joined()) {
            return;
        }

        ClusterState clusterState = node.getClusterService().getClusterState();
        if (clusterState != ClusterState.ACTIVE) {
            logger.warning("Partitions can't be assigned since cluster-state= " + clusterState);
            return;
        }

        if (!triggerMasterFlag.compareAndSet(false, true)) {
            return;
        }

        try {
            final Address masterAddress = node.getMasterAddress();
            if (masterAddress != null && !masterAddress.equals(node.getThisAddress())) {
                Future f = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, new AssignPartitions(),
                        masterAddress).setTryCount(1).invoke();
                f.get(1, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            logger.finest(e);
        } finally {
            triggerMasterFlag.set(false);
        }
    }

    boolean isClusterFormedByOnlyLiteMembers() {
        final ClusterServiceImpl clusterService = node.getClusterService();
        return clusterService.getMembers(DATA_MEMBER_SELECTOR).isEmpty();
    }

    public void setInitialState(Address[][] newState, int partitionStateVersion) {
        lock.lock();
        try {
            partitionStateManager.setInitialState(newState, partitionStateVersion);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getMemberGroupsSize() {
        return partitionStateManager.getMemberGroupsSize();
    }

    @Probe(name = "maxBackupCount")
    @Override
    public int getMaxAllowedBackupCount() {
        return max(min(getMemberGroupsSize() - 1, InternalPartition.MAX_BACKUP_COUNT), 0);
    }

    @Override
    public void memberAdded(MemberImpl member) {
        if (!member.localMember()) {
            partitionStateManager.updateMemberGroupsSize();
        }
        lastMaster = node.getMasterAddress();

        if (node.isMaster()) {
            lock.lock();
            try {
                if (partitionStateManager.isInitialized()) {
                    final ClusterState clusterState = nodeEngine.getClusterService().getClusterState();
                    if (clusterState == ClusterState.ACTIVE) {
                        partitionStateManager.incrementVersion();
                        migrationManager.triggerRepartitioning();
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void memberRemoved(final MemberImpl member) {
        logger.info("Removing " + member);
        partitionStateManager.updateMemberGroupsSize();

        final Address deadAddress = member.getAddress();
        final Address thisAddress = node.getThisAddress();

        lock.lock();
        try {
            final int partitionStateVersionBeforeMemberRemove = partitionStateManager.getVersion();
            // TODO: why not increment only on master and publish it?
            // TODO BASRI i updated here to increment only on master
            if (node.isMaster() && partitionStateManager.isInitialized()
                    && node.getClusterService().getClusterState() == ClusterState.ACTIVE) {
                partitionStateManager.incrementVersion();
            }

            migrationManager.onMemberRemove(member);

            boolean isThisNodeNewMaster = node.isMaster() && !thisAddress.equals(lastMaster);
            if (isThisNodeNewMaster) {
                migrationManager.schedule(new FetchMostRecentPartitionTableTask(partitionStateVersionBeforeMemberRemove));
            }
            lastMaster = node.getMasterAddress();

            // TODO: here or before all other actions?
            // Pause migration and let all other members notice the dead member
            // Otherwise new master may take action fast and send new partition state
            // before other members realize the dead one.
            migrationManager.pauseMigration();

            // TODO: this looks fine, a local optimization
            replicaManager.cancelReplicaSyncRequestsTo(deadAddress);

            if (node.isMaster()) {
                migrationManager.schedule(new RepairPartitionTableTask(deadAddress));
            }

            // TODO: when master node changes, migration should be resumed after most recent ptable is chosen.
            if (!isThisNodeNewMaster) {
                migrationManager.resumeMigrationEventually();
            }
        } finally {
            lock.unlock();
        }
    }

    public void cancelReplicaSyncRequestsTo(Address deadAddress) {
        lock.lock();
        try {
            replicaManager.cancelReplicaSyncRequestsTo(deadAddress);
        } finally {
            lock.unlock();
        }
    }

    // TODO: when master node changes, we should not send ptable to the new members joining to the cluster.
    public PartitionRuntimeState createPartitionState() {
        return createPartitionState(getCurrentMembersAndMembersRemovedWhileNotClusterNotActive());
    }

    private PartitionRuntimeState createPartitionState(Collection<MemberImpl> members) {
        if (!partitionStateManager.isInitialized()) {
            return null;
        }

        lock.lock();
        try {
            List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(members.size());
            for (MemberImpl member : members) {
                MemberInfo memberInfo = new MemberInfo(member.getAddress(), member.getUuid(), member.getAttributes());
                memberInfos.add(memberInfo);
            }
            List<MigrationInfo> completedMigrations = migrationManager.getCompletedMigrations();
            ILogger logger = node.getLogger(PartitionRuntimeState.class);

            InternalPartition[] partitions = partitionStateManager.getPartitions();

            PartitionRuntimeState state =
                    new PartitionRuntimeState(logger, memberInfos, partitions, completedMigrations, getPartitionStateVersion());
            state.setActiveMigration(migrationManager.getActiveMigration());
            return state;
        } finally {
            lock.unlock();
        }
    }

    PartitionRuntimeState createMigrationCommitPartitionState(MigrationInfo migrationInfo) {
        if (!partitionStateManager.isInitialized()) {
            return null;
        }
        Collection<MemberImpl> members = node.clusterService.getMemberImpls();
        lock.lock();
        try {
            List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(members.size());
            for (MemberImpl member : members) {
                MemberInfo memberInfo = new MemberInfo(member.getAddress(), member.getUuid(), member.getAttributes());
                memberInfos.add(memberInfo);
            }
            List<MigrationInfo> completedMigrations = migrationManager.getCompletedMigrations();
            ILogger logger = node.getLogger(PartitionRuntimeState.class);

            InternalPartition[] partitions = partitionStateManager.getPartitionsCopy();

            int partitionId = migrationInfo.getPartitionId();
            InternalPartitionImpl partition = (InternalPartitionImpl) partitions[partitionId];
            partition.setReplicaAddress(migrationInfo.getReplicaIndex(), migrationInfo.getDestination());

            return new PartitionRuntimeState(logger, memberInfos, partitions, completedMigrations, getPartitionStateVersion() + 1);
        } finally {
            lock.unlock();
        }
    }

    void publishPartitionRuntimeState() {
        if (!partitionStateManager.isInitialized()) {
            // do not send partition state until initialized!
            return;
        }

        if (!node.isMaster()) {
            return;
        }

        if (!isReplicaSyncAllowed()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }

        lock.lock();
        try {
            Collection<MemberImpl> members = getCurrentMembersAndMembersRemovedWhileNotClusterNotActive();
            PartitionRuntimeState partitionState = createPartitionState(members);
            PartitionStateOperation op = new PartitionStateOperation(partitionState);

            OperationService operationService = nodeEngine.getOperationService();
            final ClusterServiceImpl clusterService = node.clusterService;
            for (MemberImpl member : members) {
                if (!(member.localMember() || clusterService.isMemberRemovedWhileClusterIsNotActive(member.getAddress()))) {
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

    void syncPartitionRuntimeState() {
        syncPartitionRuntimeState(node.clusterService.getMemberImpls());
    }

    void syncPartitionRuntimeState(Collection<MemberImpl> members) {
        if (!partitionStateManager.isInitialized()) {
            // do not send partition state until initialized!
            return;
        }

        if (!node.isMaster()) {
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
        final ClusterServiceImpl clusterService = node.clusterService;
        List<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (!(member.localMember() || clusterService.isMemberRemovedWhileClusterIsNotActive(member.getAddress()))) {
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

    public void processPartitionRuntimeState(PartitionRuntimeState partitionState) {
        lock.lock();
        try {
            final Address sender = partitionState.getEndpoint();
            if (!node.getNodeExtension().isStartCompleted()) {
                logger.warning("Ignoring received partition table, startup is not completed yet. Sender: " + sender);
                return;
            }

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

            applyNewState(partitionState, sender);
        } finally {
            lock.unlock();
        }
    }

    private void applyNewState(PartitionRuntimeState partitionState, Address sender) {
        partitionStateManager.setVersion(partitionState.getVersion());
        partitionStateManager.setInitialized();

        filterAndLogUnknownAddressesInPartitionTable(sender, partitionState.getPartitions());
        finalizeOrRollbackMigration(partitionState);
    }

    private void finalizeOrRollbackMigration(PartitionRuntimeState partitionState) {
        final PartitionInfo[] partitions = partitionState.getPartitions();
        Collection<MigrationInfo> completedMigrations = partitionState.getCompletedMigrations();
        for (MigrationInfo completedMigration : completedMigrations) {

            assert completedMigration.getStatus() == MigrationStatus.SUCCESS
                    || completedMigration.getStatus() == MigrationStatus.FAILED
                    : "Invalid migration: " + completedMigration;

            if (migrationManager.addCompletedMigration(completedMigration)) {
                int partitionId = completedMigration.getPartitionId();
                PartitionInfo partitionInfo = partitions[partitionId];
                // mdogan:
                // Each partition should be updated right after migration is finalized
                // at the moment, it doesn't cause any harm to existing services,
                // because we have a `migrating` flag in partition which is cleared during migration finalization.
                // But from API point of view, we should provide explicit guarantees.
                // For the time being, leaving this stuff as is to not to change behaviour.

                // TODO BASRI IS THIS STILL NECESSARY? CAN WE MOVE UPDATEALLPARTITIONS(partitions) BEFORE FOR LOOP AND REMOVE NEXT LINE?
                partitionStateManager.updateReplicaAddresses(partitionInfo.getPartitionId(), partitionInfo.getReplicaAddresses());
                migrationManager.scheduleActiveMigrationFinalization(completedMigration);
            }
        }

        updateAllPartitions(partitions);
        migrationManager.retainCompletedMigrations(completedMigrations);
    }

    private void updateAllPartitions(PartitionInfo[] state) {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final PartitionInfo partitionInfo = state[partitionId];
            partitionStateManager.updateReplicaAddresses(partitionInfo.getPartitionId(), partitionInfo.getReplicaAddresses());
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
        final ClusterServiceImpl clusterService = node.clusterService;
        final ClusterState clusterState = clusterService.getClusterState();
        for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
            Address address = partitionInfo.getReplicaAddress(index);
            if (address != null && node.clusterService.getMember(address) == null) {
                if (clusterState == ClusterState.ACTIVE || !clusterService.isMemberRemovedWhileClusterIsNotActive(address)) {
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
    }

    @Override
    public IPartition[] getPartitions() {
        IPartition[] result = new IPartition[partitionCount];
        System.arraycopy(partitionStateManager.getPartitions(), 0, result, 0, partitionCount);
        return result;
    }

    @Override
    public InternalPartition[] getInternalPartitions() {
        return partitionStateManager.getPartitions();
    }

    @Override
    public InternalPartition getPartition(int partitionId) {
        return getPartition(partitionId, true);
    }

    @Override
    public InternalPartition getPartition(int partitionId, boolean triggerOwnerAssignment) {
        InternalPartitionImpl p = partitionStateManager.getPartitionImpl(partitionId);
        if (triggerOwnerAssignment && p.getOwnerOrNull() == null) {
            // probably ownerships are not set yet.
            // force it.
            getPartitionOwner(partitionId);
        }
        return p;
    }

    @Override
    public boolean prepareToSafeShutdown(long timeout, TimeUnit unit) {
        return partitionReplicaChecker.prepareToSafeShutdown(timeout, unit);
    }

    List<MemberImpl> getCurrentMembersAndMembersRemovedWhileNotClusterNotActive() {
        final List<MemberImpl> members = new ArrayList<MemberImpl>();
        members.addAll(node.clusterService.getMemberImpls());
        members.addAll(node.clusterService.getMembersRemovedWhileClusterIsNotActive());
        return members;
    }

    @Override
    public boolean isMemberStateSafe() {
        return partitionReplicaChecker.getMemberState() == InternalPartitionServiceState.SAFE;
    }

    @Override
    public boolean hasOnGoingMigration() {
        return hasOnGoingMigrationLocal() || (!node.isMaster() && hasOnGoingMigrationMaster(Level.FINEST));
    }

    private boolean hasOnGoingMigrationMaster(Level level) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            return node.joined();
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
        return migrationManager.hasOnGoingMigration();
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
        return replicaManager.incrementPartitionReplicaVersions(partitionId, backupCount);
    }

    // called in operation threads
    @Override
    public void updatePartitionReplicaVersions(int partitionId, long[] versions, int replicaIndex) {
        replicaManager.updatePartitionReplicaVersions(partitionId, versions, replicaIndex);
    }

    @Override
    public boolean isPartitionReplicaVersionStale(int partitionId, long[] versions, int replicaIndex) {
        PartitionReplicaVersions partitionVersion = replicaVersions[partitionId];
        return partitionVersion.isStale(versions, replicaIndex);
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    @Override
    public long[] getPartitionReplicaVersions(int partitionId) {
        return replicaManager.getPartitionReplicaVersions(partitionId);
    }

    // called in operation threads
    @Override
    public void setPartitionReplicaVersions(int partitionId, long[] versions, int replicaOffset) {
        replicaManager.setPartitionReplicaVersions(partitionId, versions, replicaOffset);
    }

    @Override
    public void clearPartitionReplicaVersions(int partitionId) {
        replicaManager.clearPartitionReplicaVersions(partitionId);
    }

    @Override
    public Map<Address, List<Integer>> getMemberPartitionsMap() {
        final Collection<Member> dataMembers = node.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        final int dataMembersSize = dataMembers.size();
        Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(dataMembersSize);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final Address owner = getPartitionOwnerOrWait(partitionId);

            List<Integer> ownedPartitions = memberPartitions.get(owner);
            if (ownedPartitions == null) {
                ownedPartitions = new ArrayList<Integer>();
                memberPartitions.put(owner, ownedPartitions);
            }
            ownedPartitions.add(partitionId);
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
        lock.lock();
        try {
            replicaManager.reset();
            partitionStateManager.reset();
            migrationManager.reset();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void pauseMigration() {
        migrationManager.pauseMigration();
    }

    @Override
    public void resumeMigration() {
        migrationManager.resumeMigration();
    }

    public int getMigrationPauseCount() {
        return migrationManager.getMigrationPauseCount();
    }

    public boolean isReplicaSyncAllowed() {
        return migrationManager.isMigrationAllowed();
    }

    public boolean isMigrationAllowed() {
        if (migrationManager.isMigrationAllowed()) {
            ClusterState clusterState = node.getClusterService().getClusterState();
            return clusterState == ClusterState.ACTIVE;
        }

        return false;
    }

    @Override
    public void shutdown(boolean terminate) {
        logger.finest("Shutting down the partition service");
        migrationManager.stop();
        reset();
    }

    @Override
    @Probe(name = "migrationQueueSize")
    public long getMigrationQueueSize() {
        return migrationManager.getMigrationQueueSize();
    }

    public PartitionServiceProxy getPartitionServiceProxy() {
        return proxy;
    }

    @Override
    public String addMigrationListener(MigrationListener listener) {
        return partitionEventManager.addMigrationListener(listener);
    }

    @Override
    public boolean removeMigrationListener(String registrationId) {
        return partitionEventManager.removeMigrationListener(registrationId);
    }

    @Override
    public String addPartitionLostListener(PartitionLostListener listener) {
        return partitionEventManager.addPartitionLostListener(listener);
    }

    @Override
    public String addLocalPartitionLostListener(PartitionLostListener listener) {
        return partitionEventManager.addLocalPartitionLostListener(listener);
    }

    @Override
    public boolean removePartitionLostListener(String registrationId) {
        return partitionEventManager.removePartitionLostListener(registrationId);
    }

    @Override
    public void dispatchEvent(PartitionEvent partitionEvent, PartitionEventListener partitionEventListener) {
        partitionEventListener.onEvent(partitionEvent);
    }

    public void addPartitionListener(PartitionListener listener) {
        lock.lock();
        try {
            partitionListener.addChildListener(listener);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isPartitionOwner(int partitionId) {
        InternalPartition partition = partitionStateManager.getPartitionImpl(partitionId);
        return partition.isLocal();
    }

    @Override
    public int getPartitionStateVersion() {
        return partitionStateManager.getVersion();
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent event) {
        partitionEventManager.onPartitionLost(event);
    }

    public void setInternalMigrationListener(InternalMigrationListener listener) {
        migrationManager.setInternalMigrationListener(listener);
    }

    public InternalMigrationListener getInternalMigrationListener() {
        return migrationManager.getInternalMigrationListener();
    }

    public void resetInternalMigrationListener() {
        migrationManager.resetInternalMigrationListener();
    }

    /**
     * @return copy of ongoing replica-sync operations
     */
    public List<ReplicaSyncInfo> getOngoingReplicaSyncRequests() {
        return replicaManager.getOngoingReplicaSyncRequests();
    }

    /**
     * @return copy of scheduled replica-sync requests
     */
    public List<ScheduledEntry<Integer, ReplicaSyncInfo>> getScheduledReplicaSyncRequests() {
        return replicaManager.getScheduledReplicaSyncRequests();
    }

    public PartitionStateManager getPartitionStateManager() {
        return partitionStateManager;
    }

    public MigrationManager getMigrationManager() {
        return migrationManager;
    }

    public PartitionReplicaManager getReplicaManager() {
        return replicaManager;
    }

    public PartitionReplicaChecker getPartitionReplicaChecker() {
        return partitionReplicaChecker;
    }

    public PartitionEventManager getPartitionEventManager() {
        return partitionEventManager;
    }

    private class RepairPartitionTableTask implements MigrationRunnable {
        private final Address deadAddress;

        RepairPartitionTableTask(Address deadAddress) {this.deadAddress = deadAddress;}

        @Override
        public void run() {
            if (!partitionStateManager.isInitialized()) {
                return;
            }

            Collection<MigrationInfo> migrationInfos = partitionStateManager.removeDeadAddress(deadAddress);
            for (MigrationInfo migrationInfo : migrationInfos) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Scheduling repair migration: " + migrationInfo + " removed address: " + deadAddress);
                }
                migrationManager.scheduleMigration(migrationInfo, MigrateTaskReason.REPAIR_PARTITION_TABLE);
            }

            migrationManager.triggerRepartitioning();
            syncPartitionRuntimeState();
        }

        @Override
        public void invalidate(Address address) {

        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public boolean isPauseable() {
            return true;
        }
    }

    private class FetchMostRecentPartitionTableTask implements MigrationRunnable {

        private final Address thisAddress = node.getThisAddress();

        private final int version;

        public FetchMostRecentPartitionTableTask(int version) {
            this.version = version;
        }

        public void run() {
            Collection<MemberImpl> members = node.clusterService.getMemberImpls();
            Collection<Future<PartitionRuntimeState>> futures = new ArrayList<Future<PartitionRuntimeState>>(
                    members.size());

            for (MemberImpl m : members) {
                if (m.localMember()) {
                    continue;
                }
                Future<PartitionRuntimeState> future = nodeEngine.getOperationService()
                        .createInvocationBuilder(SERVICE_NAME, new FetchPartitionStateOperation(),
                                m.getAddress()).setTryCount(Integer.MAX_VALUE)
                        .setCallTimeout(Long.MAX_VALUE).invoke();
                futures.add(future);
            }

            int version = this.version;
            logger.info("Fetching most recent partition table! my version: " + version);
            PartitionRuntimeState newState = null;

            Collection<MigrationInfo> allCompletedMigrations = new HashSet<MigrationInfo>();
            Collection<MigrationInfo> allActiveMigrations = new HashSet<MigrationInfo>();

            for (Future<PartitionRuntimeState> future : futures) {
                try {
                    PartitionRuntimeState state = future.get();
                    if (version < state.getVersion()) {
                        newState = state;
                        version = state.getVersion();
                    }
                    allCompletedMigrations.addAll(state.getCompletedMigrations());

                    if (state.getActiveMigration() != null) {
                        allActiveMigrations.add(state.getActiveMigration());
                    }
                } catch (TargetNotMemberException e) {
                    // ignore
                } catch (MemberLeftException e) {
                    // ignore
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

            logger.info("Most recent partition table version: " + version);

            lock.lock();
            try {
                allCompletedMigrations.addAll(migrationManager.getCompletedMigrations());
                if (migrationManager.getActiveMigration() != null) {
                    allActiveMigrations.add(migrationManager.getActiveMigration());
                }

                for (MigrationInfo activeMigration : allActiveMigrations) {
                    activeMigration.setStatus(MigrationStatus.FAILED);
                    if (allCompletedMigrations.add(activeMigration)) {
                        logger.info("Marked active migration " + activeMigration + " as " + MigrationStatus.FAILED);
                    }
                }

                // TODO: if we get the same migration-info once as completed and once as active
                // then we will throw the active one and mark the migration as completed.
                if (newState != null) {
                    newState.setCompletedMigrations(allCompletedMigrations);
                    logger.info("Applying the most recent of partition state...");
                    applyNewState(newState, thisAddress);
                } else {
                    migrationManager.setCompletedMigrations(allCompletedMigrations);
                    for (MigrationInfo migrationInfo : allCompletedMigrations) {
                        migrationManager.scheduleActiveMigrationFinalization(migrationInfo);
                    }
                }
            } finally {
                lock.unlock();
            }

            syncPartitionRuntimeState();
            migrationManager.resumeMigrationEventually();
        }

        @Override
        public void invalidate(Address address) {

        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public boolean isPauseable() {
            return false;
        }
    }

    @Override
    public String toString() {
        return "PartitionManager[" + getPartitionStateVersion() + "] {\n\n"
                + "migrationQ: " + getMigrationQueueSize() + "\n}";
    }
}
