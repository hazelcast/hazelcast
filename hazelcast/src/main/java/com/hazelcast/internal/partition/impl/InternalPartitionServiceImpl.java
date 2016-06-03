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
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.PartitionServiceProxy;
import com.hazelcast.internal.partition.operation.AssignPartitions;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;
import com.hazelcast.internal.partition.operation.ShutdownRequestOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.PartitionEvent;
import com.hazelcast.partition.PartitionEventListener;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.EmptyStatement;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * The {@link InternalPartitionService} implementation.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class InternalPartitionServiceImpl implements InternalPartitionService, ManagedService,
        EventPublishingService<PartitionEvent, PartitionEventListener<PartitionEvent>>, PartitionAwareService {

    private static final int PARTITION_OWNERSHIP_WAIT_MILLIS = 10;
    private static final String EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT = "Partition state sync invocation timed out";
    private static final int PTABLE_SYNC_TIMEOUT_SECONDS = 10;
    private static final int SAFE_SHUTDOWN_MAX_AWAIT_STEP_MILLIS = 1000;

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
    private final PartitionReplicaStateChecker partitionReplicaStateChecker;
    private final PartitionEventManager partitionEventManager;

    private final ExceptionHandler partitionStateSyncTimeoutHandler;

    // used to limit partition assignment requests sent to master
    private final AtomicBoolean triggerMasterFlag = new AtomicBoolean(false);

    private final AtomicReference<CountDownLatch> shutdownLatchRef = new AtomicReference<CountDownLatch>();

    private volatile Address lastMaster;

    private volatile boolean shouldFetchPartitionTables;

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

        partitionReplicaStateChecker = new PartitionReplicaStateChecker(node, this);
        partitionEventManager = new PartitionEventManager(node);

        partitionStateSyncTimeoutHandler =
                logAllExceptions(logger, EXCEPTION_MSG_PARTITION_STATE_SYNC_TIMEOUT, Level.FINEST);

        partitionMigrationTimeout = properties.getMillis(GroupProperty.PARTITION_MIGRATION_TIMEOUT);

        proxy = new PartitionServiceProxy(nodeEngine, this);

        MetricsRegistry metricsRegistry = nodeEngine.getMetricsRegistry();
        metricsRegistry.scanAndRegister(this, "partitions");
        metricsRegistry.scanAndRegister(partitionStateManager, "partitions");
        metricsRegistry.scanAndRegister(migrationManager, "partitions");
        metricsRegistry.scanAndRegister(replicaManager, "partitions");
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
            Set<Address> excludedAddresses = migrationManager.getShutdownRequestedAddresses();
            if (!partitionStateManager.initializePartitionAssignments(excludedAddresses)) {
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

    private boolean isClusterFormedByOnlyLiteMembers() {
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
    public boolean isMemberAllowedToJoin(Address address) {
        lock.lock();
        try {
            ClusterState clusterState = node.getClusterService().getClusterState();
            if (clusterState == ClusterState.FROZEN || clusterState == ClusterState.PASSIVE) {
                return true;
            }

            if (partitionStateManager.isPresentInPartitionTable(address)) {
                return false;
            }

            final MigrationRunnable activeTask = migrationManager.getActiveTask();
            if (activeTask instanceof MigrationManager.MigrateTask) {
                final MigrationManager.MigrateTask migrateTask = (MigrationManager.MigrateTask) activeTask;
                final MigrationInfo migrationInfo = migrateTask.migrationInfo;
                if (address.equals(migrationInfo.getSource()) || address.equals(migrationInfo.getDestination())) {
                    return false;
                }
            }

            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void memberAdded(MemberImpl member) {
        logger.fine("Adding " + member);
        lock.lock();
        try {
            if (!member.localMember()) {
                partitionStateManager.updateMemberGroupsSize();
            }
            lastMaster = node.getMasterAddress();

            if (node.isMaster()) {
                if (partitionStateManager.isInitialized()) {
                    final ClusterState clusterState = nodeEngine.getClusterService().getClusterState();
                    if (clusterState == ClusterState.ACTIVE) {
                        migrationManager.triggerControlTask();
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void memberRemoved(final MemberImpl member) {
        logger.fine("Removing " + member);
        final Address deadAddress = member.getAddress();
        final Address thisAddress = node.getThisAddress();

        lock.lock();
        try {
            partitionStateManager.updateMemberGroupsSize();
            migrationManager.onMemberRemove(member);

            boolean isThisNodeNewMaster = node.isMaster() && !thisAddress.equals(lastMaster);
            if (isThisNodeNewMaster) {
                assert !shouldFetchPartitionTables : "SOMETHING IS WRONG! Removed member: " + member;
                shouldFetchPartitionTables = true;
            }

            lastMaster = node.getMasterAddress();

            migrationManager.pauseMigration();

            replicaManager.cancelReplicaSyncRequestsTo(deadAddress);

            if (node.isMaster()) {
                migrationManager.triggerControlTask();
            }

            migrationManager.resumeMigration();
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

    @Override
    public PartitionRuntimeState createPartitionState() {
        if (!isFetchMostRecentPartitionTableTaskRequired()) {
            return createPartitionStateInternal();
        }
        return null;
    }

    public PartitionRuntimeState createPartitionStateInternal() {
        if (!partitionStateManager.isInitialized()) {
            return null;
        }

        lock.lock();
        try {
            if (!partitionStateManager.isInitialized()) {
                return null;
            }

            List<MigrationInfo> completedMigrations = migrationManager.getCompletedMigrationsCopy();
            InternalPartition[] partitions = partitionStateManager.getPartitions();

            PartitionRuntimeState state = new PartitionRuntimeState(partitions, completedMigrations, getPartitionStateVersion());
            state.setActiveMigration(migrationManager.getActiveMigration());
            return state;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Creates a transient PartitionRuntimeState to commit given migration.
     * Result migration is applied to partition table and migration is added to completed-migrations set.
     * Version of created partition table is incremented by 1.
     */
    PartitionRuntimeState createMigrationCommitPartitionState(MigrationInfo migrationInfo) {
        lock.lock();
        try {
            if (!partitionStateManager.isInitialized()) {
                return null;
            }

            List<MigrationInfo> completedMigrations = migrationManager.getCompletedMigrationsCopy();
            InternalPartition[] partitions = partitionStateManager.getPartitionsCopy();

            int partitionId = migrationInfo.getPartitionId();
            InternalPartitionImpl partition = (InternalPartitionImpl) partitions[partitionId];
            migrationManager.applyMigration(partition, migrationInfo);

            migrationInfo.setStatus(MigrationStatus.SUCCESS);
            completedMigrations.add(migrationInfo);

            int committedVersion = getPartitionStateVersion() + 1;
            return new PartitionRuntimeState(partitions, completedMigrations, committedVersion);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Creates a transient PartitionRuntimeState to commit promotions.
     * Results of promotions are applied to partition table.
     * Version of created partition table is incremented by number of promotions.
     */
    PartitionRuntimeState createPromotionCommitPartitionState(Collection<MigrationInfo> migrationInfos) {
        lock.lock();
        try {
            if (!partitionStateManager.isInitialized()) {
                return null;
            }

            List<MigrationInfo> completedMigrations = migrationManager.getCompletedMigrationsCopy();
            InternalPartition[] partitions = partitionStateManager.getPartitionsCopy();

            for (MigrationInfo migrationInfo : migrationInfos) {
                int partitionId = migrationInfo.getPartitionId();
                InternalPartitionImpl partition = (InternalPartitionImpl) partitions[partitionId];
                migrationManager.applyMigration(partition, migrationInfo);
                migrationInfo.setStatus(MigrationStatus.SUCCESS);
            }

            int committedVersion = getPartitionStateVersion() + migrationInfos.size();
            return new PartitionRuntimeState(partitions, completedMigrations, committedVersion);
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
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

        PartitionRuntimeState partitionState = createPartitionStateInternal();
        if (partitionState == null) {
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Publishing partition state, version: " + partitionState.getVersion());
        }

        PartitionStateOperation op = new PartitionStateOperation(partitionState);
        OperationService operationService = nodeEngine.getOperationService();
        Collection<MemberImpl> members = node.clusterService.getMemberImpls();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                try {
                    operationService.send(op, member.getAddress());
                } catch (Exception e) {
                    logger.finest(e);
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    boolean syncPartitionRuntimeState() {
        if (!partitionStateManager.isInitialized()) {
            // do not send partition state until initialized!
            return false;
        }

        if (!node.isMaster()) {
            return false;
        }

        PartitionRuntimeState partitionState = createPartitionStateInternal();
        if (partitionState == null) {
            return false;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Sync'ing partition state, version: " + partitionState.getVersion());
        }

        OperationService operationService = nodeEngine.getOperationService();

        Collection<MemberImpl> members = node.clusterService.getMemberImpls();
        List<Future<Boolean>> calls = firePartitionStateOperation(members, partitionState, operationService);
        Collection<Boolean> results = returnWithDeadline(calls, PTABLE_SYNC_TIMEOUT_SECONDS,
                TimeUnit.SECONDS, partitionStateSyncTimeoutHandler);

        if (calls.size() != results.size()) {
            return false;
        }

        for (Boolean result : results) {
            if (!result) {
                if (logger.isFineEnabled()) {
                    logger.fine("Partition state, version: " + partitionState.getVersion()
                            + " sync failed to one of the members!");
                }
                return false;
            }
        }
        return true;
    }

    private List<Future<Boolean>> firePartitionStateOperation(Collection<MemberImpl> members,
                                                     PartitionRuntimeState partitionState,
                                                     OperationService operationService) {
        final ClusterServiceImpl clusterService = node.clusterService;
        List<Future<Boolean>> calls = new ArrayList<Future<Boolean>>(members.size());
        for (MemberImpl member : members) {
            if (!(member.localMember() || clusterService.isMemberRemovedWhileClusterIsNotActive(member.getAddress()))) {
                try {
                    Address address = member.getAddress();
                    PartitionStateOperation operation = new PartitionStateOperation(partitionState, true);
                    Future<Boolean> f = operationService.invokeOnTarget(SERVICE_NAME, operation, address);
                    calls.add(f);
                } catch (Exception e) {
                    logger.finest(e);
                }
            }
        }
        return calls;
    }

    public boolean processPartitionRuntimeState(final PartitionRuntimeState partitionState) {
        final Address sender = partitionState.getEndpoint();
        if (!node.getNodeExtension().isStartCompleted()) {
            logger.warning("Ignoring received partition table, startup is not completed yet. Sender: " + sender);
            return false;
        }

        final Address master = node.getMasterAddress();
        if (node.isMaster() && !node.getThisAddress().equals(sender)) {
            logger.warning("This is the master node and received a PartitionRuntimeState from "
                    + sender + ". Ignoring incoming state! ");
            return false;
        } else {
            if (sender == null || !sender.equals(master)) {
                if (node.clusterService.getMember(sender) == null) {
                    logger.severe("Received a ClusterRuntimeState from an unknown member!"
                            + " => Sender: " + sender + ", Master: " + master + "! ");
                    return false;
                } else {
                    logger.warning("Received a ClusterRuntimeState, but its sender doesn't seem to be master!"
                            + " => Sender: " + sender + ", Master: " + master + "! "
                            + "(Ignore if master node has changed recently.)");
                    return false;
                }
            }
        }

        return applyNewState(partitionState, sender);
    }

    private boolean applyNewState(PartitionRuntimeState partitionState, Address sender) {
        lock.lock();
        try {
            final int newVersion = partitionState.getVersion();
            final int currentVersion = partitionStateManager.getVersion();

            if (newVersion < currentVersion) {
                logger.warning("Master version should be greater than ours! Local version: " + currentVersion
                        + ", Master version: " + newVersion + " Master: " + nodeEngine.getMasterAddress());
                return false;
            } else if (newVersion == currentVersion) {
                if (logger.isFineEnabled()) {
                    logger.fine("Master version should be greater than ours! Local version: " + currentVersion
                            + ", Master version: " + newVersion + " Master: " + nodeEngine.getMasterAddress());
                }

                return true;
            }

            partitionStateManager.setVersion(newVersion);
            partitionStateManager.setInitialized();

            filterAndLogUnknownAddressesInPartitionTable(sender, partitionState.getPartitionTable());
            finalizeOrRollbackMigration(partitionState);
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void finalizeOrRollbackMigration(PartitionRuntimeState partitionState) {
        final Address[][] partitionTable = partitionState.getPartitionTable();
        Collection<MigrationInfo> completedMigrations = partitionState.getCompletedMigrations();
        for (MigrationInfo completedMigration : completedMigrations) {

            assert completedMigration.getStatus() == MigrationStatus.SUCCESS
                    || completedMigration.getStatus() == MigrationStatus.FAILED
                    : "Invalid migration: " + completedMigration;

            if (migrationManager.addCompletedMigration(completedMigration)) {
                int partitionId = completedMigration.getPartitionId();
                Address[] replicas = partitionTable[partitionId];
                // mdogan:
                // Each partition should be updated right after migration is finalized
                // at the moment, it doesn't cause any harm to existing services,
                // because we have a `migrating` flag in partition which is cleared during migration finalization.
                // But from API point of view, we should provide explicit guarantees.
                // For the time being, leaving this stuff as is to not to change behaviour.

                partitionStateManager.updateReplicaAddresses(partitionId, replicas);
                migrationManager.scheduleActiveMigrationFinalization(completedMigration);
            }
        }

        updateAllPartitions(partitionTable);
        migrationManager.retainCompletedMigrations(completedMigrations);
    }

    private void updateAllPartitions(Address[][] partitionTable) {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Address[] replicas = partitionTable[partitionId];
            partitionStateManager.updateReplicaAddresses(partitionId, replicas);
        }
    }

    private void filterAndLogUnknownAddressesInPartitionTable(Address sender, Address[][] partitionTable) {
        final Set<Address> unknownAddresses = new HashSet<Address>();
        for (int partitionId = 0; partitionId < partitionTable.length; partitionId++) {
            Address[] replicas = partitionTable[partitionId];
            searchUnknownAddressesInPartitionTable(sender, unknownAddresses, partitionId, replicas);
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
                                                        Address[] addresses) {
        final ClusterServiceImpl clusterService = node.clusterService;
        final ClusterState clusterState = clusterService.getClusterState();
        for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
            Address address = addresses[index];
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
        if (!node.joined()) {
            return true;
        }

        if (node.isLiteMember()) {
            return true;
        }

        CountDownLatch latch = getShutdownLatch();
        InternalOperationService operationService = nodeEngine.getOperationService();

        long timeoutMillis = unit.toMillis(timeout);
        long awaitStep = Math.min(SAFE_SHUTDOWN_MAX_AWAIT_STEP_MILLIS, timeoutMillis);
        try {
            do {
                Address masterAddress = nodeEngine.getMasterAddress();
                if (masterAddress == null) {
                    logger.warning("Safe shutdown failed, master member is not known!");
                    return false;
                }

                if (node.getThisAddress().equals(masterAddress)) {
                    onShutdownRequest(node.getThisAddress());
                } else {
                    operationService.send(new ShutdownRequestOperation(), masterAddress);
                }
                if (latch.await(awaitStep, TimeUnit.MILLISECONDS)) {
                    return true;
                }
                timeoutMillis -= awaitStep;
            } while (timeoutMillis > 0);
        } catch (InterruptedException e) {
            logger.info("Safe shutdown is interrupted!");
        }
        return false;
    }

    private CountDownLatch getShutdownLatch() {
        CountDownLatch latch = shutdownLatchRef.get();
        if (latch == null) {
            latch = new CountDownLatch(1);
            if (!shutdownLatchRef.compareAndSet(null, latch)) {
                latch = shutdownLatchRef.get();
            }
        }
        return latch;
    }

    public void onShutdownRequest(Address address) {
        if (lock.tryLock()) {
            try {
                migrationManager.onShutdownRequest(address);
            } finally {
                lock.unlock();
            }
        }
    }

    public void onShutdownResponse() {
        CountDownLatch latch = shutdownLatchRef.get();
        assert latch != null;
        latch.countDown();
    }

    @Override
    public boolean isMemberStateSafe() {
        return partitionReplicaStateChecker.getPartitionServiceState() == PartitionServiceState.SAFE;
    }

    @Override
    public boolean hasOnGoingMigration() {
        return hasOnGoingMigrationLocal()
                || (!node.isMaster() && partitionReplicaStateChecker.hasOnGoingMigrationMaster(Level.FINEST));
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
        return replicaManager.isPartitionReplicaVersionStale(partitionId, versions, replicaIndex);
    }

    // called in operation threads
    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    @Override
    public long[] getPartitionReplicaVersions(int partitionId) {
        return replicaManager.getPartitionReplicaVersions(partitionId);
    }

    @Override
    public Map<Address, List<Integer>> getMemberPartitionsMap() {
        Collection<Member> dataMembers = node.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        int dataMembersSize = dataMembers.size();
        int partitionsPerMember = (dataMembersSize > 0 ? (int) ceil((float) partitionCount / dataMembersSize) : 0);

        Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(dataMembersSize);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Address owner = getPartitionOwnerOrWait(partitionId);

            List<Integer> ownedPartitions = memberPartitions.get(owner);
            if (ownedPartitions == null) {
                ownedPartitions = new ArrayList<Integer>(partitionsPerMember);
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
            shouldFetchPartitionTables = false;
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

    public boolean isReplicaSyncAllowed() {
        return migrationManager.isMigrationAllowed();
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

    public PartitionReplicaStateChecker getPartitionReplicaStateChecker() {
        return partitionReplicaStateChecker;
    }

    public PartitionEventManager getPartitionEventManager() {
        return partitionEventManager;
    }

    boolean isFetchMostRecentPartitionTableTaskRequired() {
        return shouldFetchPartitionTables;
    }

    boolean scheduleFetchMostRecentPartitionTableTaskIfRequired() {
       lock.lock();
       try {
           if (shouldFetchPartitionTables) {
               migrationManager.schedule(new FetchMostRecentPartitionTableTask());
               return true;
           }

           return false;
       } finally {
           lock.unlock();
       }
    }

    private class FetchMostRecentPartitionTableTask implements MigrationRunnable {

        private final Address thisAddress = node.getThisAddress();

        private int maxVersion;

        private PartitionRuntimeState newState;

        public void run() {
            maxVersion = partitionStateManager.getVersion();

            Collection<Future<PartitionRuntimeState>> futures = invokeFetchPartitionStateOps();

            logger.info("Fetching most recent partition table! my version: " + maxVersion);

            Collection<MigrationInfo> allCompletedMigrations = new HashSet<MigrationInfo>();
            Collection<MigrationInfo> allActiveMigrations = new HashSet<MigrationInfo>();

            processResults(futures, allCompletedMigrations, allActiveMigrations);

            logger.info("Most recent partition table version: " + maxVersion);

            processNewState(allCompletedMigrations, allActiveMigrations);
            syncPartitionRuntimeState();
        }

        private Collection<Future<PartitionRuntimeState>> invokeFetchPartitionStateOps() {
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
            return futures;
        }

        private void processResults(Collection<Future<PartitionRuntimeState>> futures,
                Collection<MigrationInfo> allCompletedMigrations, Collection<MigrationInfo> allActiveMigrations) {
            for (Future<PartitionRuntimeState> future : futures) {
                try {
                    PartitionRuntimeState state = future.get();
                    if (state == null) {
                        // state can be null, if not initialized
                        continue;
                    }

                    if (maxVersion < state.getVersion()) {
                        newState = state;
                        maxVersion = state.getVersion();
                    }
                    allCompletedMigrations.addAll(state.getCompletedMigrations());

                    if (state.getActiveMigration() != null) {
                        allActiveMigrations.add(state.getActiveMigration());
                    }
                } catch (TargetNotMemberException e) {
                    EmptyStatement.ignore(e);
                } catch (MemberLeftException e) {
                    EmptyStatement.ignore(e);
                } catch (InterruptedException e) {
                    logger.fine("FetchMostRecentPartitionTableTask is interrupted.");
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (!(cause instanceof TargetNotMemberException) && !(cause instanceof MemberLeftException))  {
                        logger.warning("Failed to fetch partition table!", e);
                    }
                }
            }
        }

        private void processNewState(Collection<MigrationInfo> allCompletedMigrations,
                Collection<MigrationInfo> allActiveMigrations) {

            lock.lock();
            try {
                processMigrations(allCompletedMigrations, allActiveMigrations);

                if (newState != null) {
                    newState.setCompletedMigrations(allCompletedMigrations);
                    maxVersion = Math.max(maxVersion, getPartitionStateVersion()) + 1;
                    newState.setVersion(maxVersion);
                    logger.info("Applying the most recent of partition state...");
                    applyNewState(newState, thisAddress);
                } else if (partitionStateManager.isInitialized()) {
                    partitionStateManager.incrementVersion();
                    for (MigrationInfo migrationInfo : allCompletedMigrations) {
                        if (migrationManager.addCompletedMigration(migrationInfo)) {
                            if (logger.isFinestEnabled()) {
                                logger.finest("Scheduling migration finalization after finding most recent partition table: "
                                        + migrationInfo);
                            }
                            migrationManager.scheduleActiveMigrationFinalization(migrationInfo);
                        }
                    }
                }

                shouldFetchPartitionTables = false;
            } finally {
                lock.unlock();
            }
        }

        private void processMigrations(Collection<MigrationInfo> allCompletedMigrations,
                                       Collection<MigrationInfo> allActiveMigrations) {
            allCompletedMigrations.addAll(migrationManager.getCompletedMigrationsCopy());
            if (migrationManager.getActiveMigration() != null) {
                allActiveMigrations.add(migrationManager.getActiveMigration());
            }

            for (MigrationInfo activeMigration : allActiveMigrations) {
                activeMigration.setStatus(MigrationStatus.FAILED);
                if (allCompletedMigrations.add(activeMigration)) {
                    logger.info("Marked active migration " + activeMigration + " as " + MigrationStatus.FAILED);
                }
            }
        }

    }

    @Override
    public String toString() {
        return "InternalPartitionService {"
                + "version: " + getPartitionStateVersion() + ", migrationQ: " + getMigrationQueueSize() + "}";
    }

}
