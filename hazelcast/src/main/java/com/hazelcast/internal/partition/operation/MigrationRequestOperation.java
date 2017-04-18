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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import static com.hazelcast.internal.cluster.Versions.V3_9;

/**
 * Sent from the master node to the partition owner. It will perform the migration by preparing the migration operations and
 * sending them to the destination. A response with a value equal to {@link Boolean#TRUE} indicates a successful migration.
 *
 * @see com.hazelcast.internal.partition.impl.MigrationManager.MigrateTask
 */
public final class MigrationRequestOperation extends BaseMigrationOperation {

    private boolean returnResponse = true;

    public MigrationRequestOperation() {
    }

    public MigrationRequestOperation(MigrationInfo migrationInfo, int partitionStateVersion) {
        super(migrationInfo, partitionStateVersion);
    }

    @Override
    public void run() {
        NodeEngine nodeEngine = getNodeEngine();
        verifyGoodMaster(nodeEngine);

        Address source = migrationInfo.getSource();
        Address destination = migrationInfo.getDestination();
        verifyExistingTarget(nodeEngine, destination);

        if (destination.equals(source)) {
            getLogger().warning("Source and destination addresses are the same! => " + toString());
            setFailed();
            return;
        }

        InternalPartition partition = getPartition();
        verifySource(nodeEngine.getThisAddress(), partition);

        setActiveMigration();

        if (!migrationInfo.startProcessing()) {
            getLogger().warning("Migration is cancelled -> " + migrationInfo);
            setFailed();
            return;
        }

        try {
            executeBeforeMigrations();
            Collection<Operation> tasks = prepareMigrationOperations();
            InternalPartitionServiceImpl partitionService = getService();
            long[] replicaVersions = partitionService.getPartitionReplicaVersions(migrationInfo.getPartitionId());
            invokeMigrationOperation(destination, replicaVersions, tasks);
            returnResponse = false;
        } catch (Throwable e) {
            logThrowable(e);
            setFailed();
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    private void setFailed() {
        success = false;
        onMigrationComplete(false);
    }

    @Override
    protected MigrationParticipant getMigrationParticipantType() {
        return MigrationParticipant.SOURCE;
    }

    private void logThrowable(Throwable t) {
        Throwable throwableToLog = t;
        if (throwableToLog instanceof ExecutionException) {
            throwableToLog = throwableToLog.getCause() != null ? throwableToLog.getCause() : throwableToLog;
        }
        Level level = getLogLevel(throwableToLog);
        getLogger().log(level, throwableToLog.getMessage(), throwableToLog);
    }

    private Level getLogLevel(Throwable e) {
        return (e instanceof MemberLeftException || e instanceof InterruptedException)
                || !getNodeEngine().isRunning() ? Level.INFO : Level.WARNING;
    }

    /** Verifies that this node is the owner of the partition. */
    private void verifySource(Address thisAddress, InternalPartition partition) {
        Address owner = partition.getOwnerOrNull();
        if (owner == null) {
            throw new RetryableHazelcastException("Cannot migrate at the moment! Owner of the partition is null => "
                    + migrationInfo);
        }

        if (!thisAddress.equals(owner)) {
            throw new RetryableHazelcastException("Owner of partition is not this node! => " + toString());
        }
    }

    /**
     * Invokes the {@link MigrationOperation} on the migration destination and sets this object as the callback.
     *
     * @see #handleMigrationResultFromTarget(Object)
     */
    private void invokeMigrationOperation(Address destination, long[] replicaVersions, Collection<Operation> tasks)
            throws IOException {

        MigrationOperation operation = new MigrationOperation(migrationInfo, replicaVersions, tasks, partitionStateVersion);

        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();

        nodeEngine.getOperationService()
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, operation, destination)
                .setExecutionCallback(new MigrationCallback(migrationInfo, this))
                .setResultDeserialized(true)
                .setCallTimeout(partitionService.getPartitionMigrationTimeout())
                .setTryCount(InternalPartitionService.MIGRATION_RETRY_COUNT)
                .setTryPauseMillis(InternalPartitionService.MIGRATION_RETRY_PAUSE)
                .setReplicaIndex(getReplicaIndex())
                .invoke();
    }

    /** Verifies that the local master is equal to the migration master. */
    private void verifyGoodMaster(NodeEngine nodeEngine) {
        Address masterAddress = nodeEngine.getMasterAddress();
        boolean shouldFail = nodeEngine.getClusterService().getClusterVersion().isGreaterOrEqual(V3_9);

        if (!migrationInfo.getMaster().equals(masterAddress)) {
            if (shouldFail) {
                throw new IllegalStateException("Migration initiator is not master node! => " + toString());
            } else {
                throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
            }
        }

        if (!masterAddress.equals(getCallerAddress())) {
            if (shouldFail) {
                throw new IllegalStateException("Caller is not master node! => " + toString());
            } else {
                throw new RetryableHazelcastException("Caller is not master node! => " + toString());
            }
        }
    }

    /** Verifies that the destination is a cluster member. */
    private void verifyExistingTarget(NodeEngine nodeEngine, Address destination) {
        Member target = nodeEngine.getClusterService().getMember(destination);
        if (target == null) {
            throw new TargetNotMemberException("Destination of migration could not be found! => " + toString());
        }
    }

    @Override
    protected PartitionMigrationEvent getMigrationEvent() {
        return new PartitionMigrationEvent(MigrationEndpoint.SOURCE,
                migrationInfo.getPartitionId(), migrationInfo.getSourceCurrentReplicaIndex(),
                migrationInfo.getSourceNewReplicaIndex());
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public boolean returnsResponse() {
        return returnResponse;
    }

    /**
     * Processes the migration result sent from the migration destination and sends the response to the caller of this operation.
     * A response equal to {@link Boolean#TRUE} indicates successful migration.
     */
    private void handleMigrationResultFromTarget(Object result) {
        migrationInfo.doneProcessing();
        onMigrationComplete(Boolean.TRUE.equals(result));
        sendResponse(result);
    }

    @Override
    void executeBeforeMigrations() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        boolean ownerMigration = nodeEngine.getThisAddress().equals(migrationInfo.getSource());
        if (!ownerMigration) {
            return;
        }

        super.executeBeforeMigrations();
    }

    /** Collects the migration operations from all {@link MigrationAwareService}s. */
    private Collection<Operation> prepareMigrationOperations() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();

        PartitionReplicationEvent replicationEvent = new PartitionReplicationEvent(migrationInfo.getPartitionId(),
                migrationInfo.getDestinationNewReplicaIndex());

        Collection<Operation> tasks = new LinkedList<Operation>();
        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(MigrationAwareService.class)) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();

            Operation op = service.prepareReplicationOperation(replicationEvent);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                tasks.add(op);
            }
        }
        return tasks;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.MIGRATION_REQUEST;
    }

    private static final class MigrationCallback extends SimpleExecutionCallback<Object> {

        final MigrationInfo migrationInfo;
        final MigrationRequestOperation op;

        private MigrationCallback(MigrationInfo migrationInfo, MigrationRequestOperation op) {
            this.migrationInfo = migrationInfo;
            this.op = op;
        }

        @Override
        public void notify(Object result) {
            op.handleMigrationResultFromTarget(result);
        }
    }
}
