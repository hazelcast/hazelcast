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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.partition.MigrationEndpoint;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.logging.Level;

import static com.hazelcast.internal.cluster.Versions.V3_9;

/**
 * Sent by the partition owner to the migration destination to start the migration process on the destination.
 * Contains the operations which will be executed on the destination node to migrate the data and the replica versions to be set.
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public final class MigrationOperation extends BaseMigrationOperation {

    private static final OperationResponseHandler ERROR_RESPONSE_HANDLER = new OperationResponseHandler() {
        @Override
        public void sendResponse(Operation op, Object obj) {
            throw new HazelcastException("Migration operations can not send response!");
        }
    };

    private long[] replicaVersions;
    private Collection<Operation> tasks;

    private Throwable failureReason;

    public MigrationOperation() {
    }

    public MigrationOperation(MigrationInfo migrationInfo, long[] replicaVersions, Collection<Operation> tasks,
            int partitionStateVersion) {
        super(migrationInfo, partitionStateVersion);
        this.replicaVersions = replicaVersions;
        this.tasks = tasks;
    }

    @Override
    protected MigrationParticipant getMigrationParticipantType() {
        return MigrationParticipant.DESTINATION;
    }

    /**
     * {@inheritDoc}
     * Sets the active migration and the migration flag for the partition, notifies {@link MigrationAwareService}s that
     * the migration is starting and runs the sent replication operations.
     * If the migration was successful, set the replica versions. If it failed, notify the sent migration tasks.
     */
    @Override
    public void run() throws Exception {
        checkMigrationInitiatorIsMaster();
        setActiveMigration();

        try {
            doRun();
        } catch (Throwable t) {
            logMigrationFailure(t);
            success = false;
            failureReason = t;
        } finally {
            onMigrationComplete();
            if (!success) {
                onExecutionFailure(failureReason);
            }
        }
    }

    /** Notifies services that migration started, invokes all sent migration tasks and updates the replica versions. */
    private void doRun() throws Exception {
        if (migrationInfo.startProcessing()) {
            try {
                executeBeforeMigrations();

                for (Operation op : tasks) {
                    runMigrationOperation(op);
                }
                success = true;
            } catch (Throwable e) {
                success = false;
                failureReason = e;
                getLogger().severe("Error while executing replication operations " + migrationInfo, e);
            } finally {
                afterMigrate();
            }
        } else {
            success = false;
            logMigrationCancelled();
        }
    }

    private void checkMigrationInitiatorIsMaster() {
        NodeEngine nodeEngine = getNodeEngine();
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!masterAddress.equals(migrationInfo.getMaster())) {
            ClusterService clusterService = nodeEngine.getClusterService();
            if (clusterService.getClusterVersion().isGreaterOrEqual(V3_9)) {
                throw new IllegalStateException("Migration initiator is not master node! => " + toString());
            } else {
                throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
            }
        }
    }

    private void logMigrationCancelled() {
        getLogger().warning("Migration is cancelled -> " + migrationInfo);
    }

    /** Sets the partition replica versions from the destination replica index onwards, if the migration was successful. */
    private void afterMigrate() {
        if (success) {
            InternalPartitionServiceImpl partitionService = getService();
            PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
            int destinationNewReplicaIndex = migrationInfo.getDestinationNewReplicaIndex();
            int replicaOffset = destinationNewReplicaIndex <= 1 ? 1 : destinationNewReplicaIndex;
            replicaManager.setPartitionReplicaVersions(migrationInfo.getPartitionId(), replicaVersions, replicaOffset);
            if (getLogger().isFinestEnabled()) {
                getLogger().finest("ReplicaVersions are set after migration. partitionId="
                        + migrationInfo.getPartitionId() + " replicaVersions=" + Arrays.toString(replicaVersions));
            }
        } else if (getLogger().isFinestEnabled()) {
            getLogger().finest("ReplicaVersions are not set since migration failed. partitionId="
                    + migrationInfo.getPartitionId());
        }

        migrationInfo.doneProcessing();
    }

    private void logMigrationFailure(Throwable e) {
        Level level = Level.WARNING;
        if (e instanceof IllegalStateException) {
            level = Level.FINEST;
        }
        ILogger logger = getLogger();
        if (logger.isLoggable(level)) {
            logger.log(level, e.getMessage(), e);
        }
    }

    @Override
    protected PartitionMigrationEvent getMigrationEvent() {
        return new PartitionMigrationEvent(MigrationEndpoint.DESTINATION,
                migrationInfo.getPartitionId(), migrationInfo.getDestinationCurrentReplicaIndex(),
                migrationInfo.getDestinationNewReplicaIndex());
    }

    private void runMigrationOperation(Operation op) throws Exception {
        prepareOperation(op);
        op.beforeRun();
        op.run();
        op.afterRun();
    }

    private void prepareOperation(Operation op) {
        op.setNodeEngine(getNodeEngine())
                .setPartitionId(getPartitionId())
                .setReplicaIndex(getReplicaIndex());
        op.setOperationResponseHandler(ERROR_RESPONSE_HANDLER);
        OperationAccessor.setCallerAddress(op, migrationInfo.getSource());
    }

    /**
     * {@inheritDoc}
     * Notifies all sent migration tasks that the migration failed.
     */
    @Override
    public void onExecutionFailure(Throwable e) {
        if (tasks != null) {
            for (Operation op : tasks) {
                prepareOperation(op);
                onOperationFailure(op, e);
            }
        }
    }

    private void onOperationFailure(Operation op, Throwable e) {
        try {
            op.onExecutionFailure(e);
        } catch (Throwable t) {
            getLogger().warning("While calling operation.onFailure(). op: " + op, t);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLongArray(replicaVersions);
        int size = tasks != null ? tasks.size() : 0;
        out.writeInt(size);
        if (size > 0) {
            for (Operation task : tasks) {
                out.writeObject(task);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        replicaVersions = in.readLongArray();
        int size = in.readInt();
        if (size > 0) {
            tasks = new ArrayList<Operation>(size);
            for (int i = 0; i < size; i++) {
                Operation op = in.readObject();
                tasks.add(op);
            }
        } else {
            tasks = Collections.emptyList();
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        int numberOfTasks = tasks == null ? 0 : tasks.size();
        sb.append(", migration=").append(migrationInfo);
        sb.append(", replicaVersions=").append(Arrays.toString(replicaVersions));
        sb.append(", numberOfTasks=").append(numberOfTasks);
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.MIGRATION;
    }
}
