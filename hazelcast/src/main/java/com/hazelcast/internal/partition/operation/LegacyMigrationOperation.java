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

import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Migration operation used before Hazelcast version 3.9
 * It runs on the migration destination and applies the received partition data in one shot.
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public final class LegacyMigrationOperation extends BaseMigrationDestinationOperation {

    private long[] replicaVersions;
    private Collection<Operation> tasks;

    public LegacyMigrationOperation() {
    }

    public LegacyMigrationOperation(MigrationInfo migrationInfo, long[] replicaVersions, Collection<Operation> tasks,
                                    int partitionStateVersion) {
        super(migrationInfo, partitionStateVersion);
        this.replicaVersions = replicaVersions;
        this.tasks = tasks;
    }

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

    private void afterMigrate() {
        if (success) {
            InternalPartitionServiceImpl partitionService = getService();
            PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
            int destinationNewReplicaIndex = migrationInfo.getDestinationNewReplicaIndex();
            int replicaOffset = destinationNewReplicaIndex <= 1 ? 1 : destinationNewReplicaIndex;
            replicaManager.setPartitionReplicaVersions(migrationInfo.getPartitionId(),
                    NonFragmentedServiceNamespace.INSTANCE, replicaVersions, replicaOffset);
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
        return PartitionDataSerializerHook.LEGACY_MIGRATION;
    }
}
