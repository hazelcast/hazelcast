/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.ReplicaFragmentMigrationState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.TargetAware;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Migration operation used by Hazelcast version 3.9
 *
 * It runs on the migration destination and applies the received fragments.
 * Sent by the partition owner to the migration destination to start the migration process on the destination.
 * Contains the operations which will be executed on the destination node to migrate the data and the replica versions to be set.
 */
public class MigrationOperation extends BaseMigrationDestinationOperation implements TargetAware {

    private ReplicaFragmentMigrationState fragmentMigrationState;

    private boolean firstFragment;

    private boolean lastFragment;

    public MigrationOperation() {
    }

    public MigrationOperation(MigrationInfo migrationInfo, int partitionStateVersion,
                       ReplicaFragmentMigrationState fragmentMigrationState, boolean firstFragment, boolean lastFragment) {
        super(migrationInfo, partitionStateVersion);
        this.fragmentMigrationState = fragmentMigrationState;
        this.firstFragment = firstFragment;
        this.lastFragment = lastFragment;
    }

    /**
     * {@inheritDoc}
     * Sets the active migration and the migration flag for the partition, notifies {@link MigrationAwareService}s that
     * the migration is starting and runs the sent replication operations.
     * If the migration was successful, set the replica versions. If it failed, notify the sent migration tasks.
     */
    @Override
    public void run() throws Exception {
        verifyMasterOnMigrationDestination();
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
                if (firstFragment) {
                    executeBeforeMigrations();
                }

                for (Operation migrationOperation : fragmentMigrationState.getMigrationOperations()) {
                    runMigrationOperation(migrationOperation);
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

    private void afterMigrate() {
        ILogger logger = getLogger();
        if (success) {
            InternalPartitionServiceImpl partitionService = getService();
            PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
            int destinationNewReplicaIndex = migrationInfo.getDestinationNewReplicaIndex();
            int replicaOffset = destinationNewReplicaIndex <= 1 ? 1 : destinationNewReplicaIndex;

            Map<ServiceNamespace, long[]> namespaceVersions = fragmentMigrationState.getNamespaceVersionMap();
            for (Entry<ServiceNamespace, long[]> e  : namespaceVersions.entrySet()) {
                ServiceNamespace namespace = e.getKey();
                long[] replicaVersions = e.getValue();
                replicaManager.setPartitionReplicaVersions(migrationInfo.getPartitionId(), namespace,
                                                           replicaVersions, replicaOffset);
                if (logger.isFinestEnabled()) {
                    logger.finest("ReplicaVersions are set after migration. partitionId="
                            + migrationInfo.getPartitionId() + " namespace: " + namespace
                            + " replicaVersions=" + Arrays.toString(replicaVersions));
                }
            }

        } else if (logger.isFinestEnabled()) {
            logger.finest("ReplicaVersions are not set since migration failed. partitionId="
                    + migrationInfo.getPartitionId());
        }

        migrationInfo.doneProcessing();
    }

    /**
     * {@inheritDoc}
     * Notifies all sent migration tasks that the migration failed.
     */
    @Override
    public void onExecutionFailure(Throwable e) {
        if (fragmentMigrationState == null) {
            return;
        }

        Collection<Operation> tasks = fragmentMigrationState.getMigrationOperations();
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
    public int getId() {
        return PartitionDataSerializerHook.MIGRATION;
    }

    @Override
    void onMigrationStart() {
        if (firstFragment) {
            super.onMigrationStart();
        }
    }

    @Override
    void onMigrationComplete(boolean result) {
        if (lastFragment) {
            super.onMigrationComplete(result);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        fragmentMigrationState.writeData(out);
        out.writeBoolean(firstFragment);
        out.writeBoolean(lastFragment);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        fragmentMigrationState = new ReplicaFragmentMigrationState();
        fragmentMigrationState.readData(in);
        firstFragment = in.readBoolean();
        lastFragment = in.readBoolean();
    }

    @Override
    public void setTarget(Address address) {
        fragmentMigrationState.setTarget(address);
    }
}
