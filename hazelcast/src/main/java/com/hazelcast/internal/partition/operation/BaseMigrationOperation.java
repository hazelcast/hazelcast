/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionStateVersionMismatchException;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.internal.partition.impl.MigrationInterceptor.MigrationParticipant;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeList;

abstract class BaseMigrationOperation extends AbstractPartitionOperation
        implements MigrationCycleOperation, PartitionAwareOperation {

    /**
     * Volatile fields can be accessed from another thread, as replication
     * operation preparation may occur on another thread.
     * see {@link MigrationRequestOperation#trySendNewFragment()}
     */
    protected volatile MigrationInfo migrationInfo;
    protected volatile boolean success;
    protected List<MigrationInfo> completedMigrations;
    protected int partitionStateVersion;

    private transient boolean nodeStartCompleted;

    BaseMigrationOperation() {
    }

    BaseMigrationOperation(MigrationInfo migrationInfo, List<MigrationInfo> completedMigrations, int partitionStateVersion) {
        this.migrationInfo = migrationInfo;
        this.completedMigrations = completedMigrations;
        this.partitionStateVersion = partitionStateVersion;
        setPartitionId(migrationInfo.getPartitionId());
    }

    /**
     * {@inheritDoc}
     * Validates the migration operation: the UUIDs of the node should match the one in the {@link #migrationInfo},
     * the cluster should be in {@link ClusterState#ACTIVE} and the node started and the partition state version from
     * the sender should match the local partition state version.
     *
     */
    @Override
    public void beforeRun() {
        try {
            onMigrationStart();
            verifyNodeStarted();
            verifyMaster();
            verifyMigrationParticipant();
            verifyClusterState();
            applyCompletedMigrations();
            verifyPartitionVersion();
        } catch (Exception e) {
            onMigrationComplete();
            throw e;
        }
    }

    /** Verifies that the node startup is completed. */
    private void verifyNodeStarted() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        nodeStartCompleted = nodeEngine.getNode().getNodeExtension().isStartCompleted();
        if (!nodeStartCompleted) {
            throw new IllegalStateException("Migration operation is received before startup is completed. "
                    + "Sender: " + getCallerAddress());
        }
    }

    private void applyCompletedMigrations() {
        if (completedMigrations.isEmpty()) {
            return;
        }
        InternalPartitionServiceImpl partitionService = getService();
        if (!partitionService.applyCompletedMigrations(completedMigrations, migrationInfo.getMaster())) {
            throw new PartitionStateVersionMismatchException("Failed to apply completed migrations! Migration: " + migrationInfo);
        }
        if (partitionService.getMigrationManager().isFinalizingMigrationRegistered(migrationInfo.getPartitionId())) {
            // There's a pending migration finalization operation in the queue.
            // This happens when this node was the source of a backup replica migration
            // and now it is destination of another replica migration on the same partition.
            // Sources of backup migrations are not part of migration transaction
            // and they learn the migration only while applying completed migrations.
            throw new RetryableHazelcastException("There is a scheduled FinalizeMigrationOperation for the same partition => "
                    + migrationInfo);
        }
    }

    /** Verifies that the sent partition version matches the local version or this node is master. */
    private void verifyPartitionVersion() {
        InternalPartitionService partitionService = getService();
        int localVersion = partitionService.getPartition(getPartitionId()).version();
        int expectedVersion = migrationInfo.getInitialPartitionVersion();
        if (expectedVersion != localVersion) {
            if (getNodeEngine().getThisAddress().equals(migrationInfo.getMaster())) {
                return;
            }

            // this is expected when cluster member list changes during migration
            throw new PartitionStateVersionMismatchException(expectedVersion, localVersion);
        }
    }

    /** Verifies that the local master is equal to the migration master. */
    final void verifyMaster() {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl service = getService();
        Address masterAddress = nodeEngine.getMasterAddress();

        if (!migrationInfo.getMaster().equals(masterAddress)) {
            if (!nodeEngine.isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }
            throw new IllegalStateException("Migration initiator is not master node! => " + toString());
        }

        if (!service.isMemberMaster(migrationInfo.getMaster())) {
            throw new RetryableHazelcastException("Migration initiator is not the master node known by migration system!");
        }

        if (getMigrationParticipantType() == MigrationParticipant.SOURCE && !service.isMemberMaster(getCallerAddress())) {
            throw new IllegalStateException("Caller is not master node! => " + toString());
        }
    }

    /**
     * Checks if the local member matches the migration source or destination if this node is the migration source or
     * destination.
     */
    private void verifyMigrationParticipant() {
        Member localMember = getNodeEngine().getLocalMember();
        if (getMigrationParticipantType() == MigrationParticipant.SOURCE) {
            if (migrationInfo.getSourceCurrentReplicaIndex() == 0
                    && !migrationInfo.getSource().isIdentical(localMember)) {
                throw new IllegalStateException(localMember
                        + " is the migration source but has a different identity! Migration: " + migrationInfo);
            }
            verifyPartitionOwner();
            verifyExistingDestination();
        } else if (getMigrationParticipantType() == MigrationParticipant.DESTINATION) {
            if (!migrationInfo.getDestination().isIdentical(localMember)) {
                throw new IllegalStateException(localMember
                        + " is the migration destination but has a different identity! Migration: " + migrationInfo);
            }
        }
    }

    /** Verifies that this node is the owner of the partition. */
    private void verifyPartitionOwner() {
        InternalPartition partition = getPartition();
        PartitionReplica owner = partition.getOwnerReplicaOrNull();
        if (owner == null) {
            throw new RetryableHazelcastException("Cannot migrate at the moment! Owner of the partition is null => "
                    + migrationInfo);
        }

        if (!owner.isIdentical(getNodeEngine().getLocalMember())) {
            throw new RetryableHazelcastException("Owner of partition is not this node! => " + toString());
        }
    }

    /** Verifies that the destination is a cluster member. */
    final void verifyExistingDestination() {
        PartitionReplica destination = migrationInfo.getDestination();
        Member target = getNodeEngine().getClusterService().getMember(destination.address(), destination.uuid());
        if (target == null) {
            throw new TargetNotMemberException("Destination of migration could not be found! => " + toString());
        }
    }


    /** Verifies that the cluster is active. */
    private void verifyClusterState() {
        NodeEngine nodeEngine = getNodeEngine();
        ClusterState clusterState = nodeEngine.getClusterService().getClusterState();
        if (!clusterState.isMigrationAllowed()) {
            throw new IllegalStateException("Cluster state does not allow migrations! " + clusterState);
        }
    }

    /** Sets the active migration and the partition migration flag. */
    void setActiveMigration() {
        InternalPartitionServiceImpl partitionService = getService();
        MigrationManager migrationManager = partitionService.getMigrationManager();
        MigrationInfo currentActiveMigration = migrationManager.addActiveMigration(migrationInfo);
        if (currentActiveMigration != null) {
            if (migrationInfo.equals(currentActiveMigration)) {
                migrationInfo = currentActiveMigration;
                return;
            }

            throw new RetryableHazelcastException("Cannot set active migration to " + migrationInfo
                    + ". Current active migration is " + currentActiveMigration);
        }
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        if (!partitionStateManager.trySetMigratingFlag(migrationInfo.getPartitionId())) {
            throw new RetryableHazelcastException("Cannot set migrating flag, "
                    + "probably previous migration's finalization is not completed yet.");
        }
    }

    void onMigrationStart() {
        InternalPartitionServiceImpl partitionService = getService();
        MigrationInterceptor migrationInterceptor = partitionService.getMigrationInterceptor();
        migrationInterceptor.onMigrationStart(getMigrationParticipantType(), migrationInfo);
    }

    void onMigrationComplete() {
        InternalPartitionServiceImpl partitionService = getService();
        MigrationInterceptor migrationInterceptor = partitionService.getMigrationInterceptor();
        migrationInterceptor.onMigrationComplete(getMigrationParticipantType(), migrationInfo, success);
    }

    /** Notifies all {@link MigrationAwareService}s that the migration is starting. */
    void executeBeforeMigrations() throws Exception {
        PartitionMigrationEvent event = getMigrationEvent();

        Throwable t = null;
        for (MigrationAwareService service : getMigrationAwareServices()) {
            // we need to make sure all beforeMigration() methods are executed
            try {
                service.beforeMigration(event);
            } catch (Throwable e) {
                getLogger().warning("Error while executing beforeMigration()", e);
                t = e;
            }
        }
        if (t != null) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    /** Returns the event which will be forwarded to {@link MigrationAwareService}s. */
    protected abstract PartitionMigrationEvent getMigrationEvent();

    protected abstract MigrationParticipant getMigrationParticipantType();

    InternalPartition getPartition() {
        InternalPartitionServiceImpl partitionService = getService();
        return partitionService.getPartitionStateManager()
                .getPartitionImpl(migrationInfo.getPartitionId());
    }

    public MigrationInfo getMigrationInfo() {
        return migrationInfo;
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public void logError(Throwable e) {
        ILogger logger = getLogger();
        if (e instanceof PartitionStateVersionMismatchException) {
            if (logger.isFineEnabled()) {
                logger.fine(e.getMessage(), e);
            } else {
                logger.info(e.getMessage());
            }
            return;
        }
        if (!nodeStartCompleted && e instanceof IllegalStateException) {
            logger.warning(e.getMessage());
            if (logger.isFineEnabled()) {
                logger.fine(e);
            }
            return;
        }
        super.logError(e);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(migrationInfo);
        writeList(completedMigrations, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        migrationInfo = in.readObject();
        completedMigrations = readList(in);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", migration=").append(migrationInfo);
    }
}
