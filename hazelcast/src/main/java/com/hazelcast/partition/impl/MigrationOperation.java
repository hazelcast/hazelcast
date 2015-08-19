/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;

@SuppressFBWarnings("EI_EXPOSE_REP")
public final class MigrationOperation extends BaseMigrationOperation {

    private static final OperationResponseHandler ERROR_RESPONSE_HANDLER = new OperationResponseHandler() {
        @Override
        public void sendResponse(Operation op, Object obj) {
            throw new HazelcastException("Migration operations can not send response!");
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    };

    private long[] replicaVersions;
    private Collection<Operation> tasks;

    private Throwable failureReason;

    public MigrationOperation() {
    }

    public MigrationOperation(MigrationInfo migrationInfo, long[] replicaVersions, Collection<Operation> tasks) {
        super(migrationInfo);
        this.replicaVersions = replicaVersions;
        this.tasks = tasks;
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public void run() throws Exception {
        assertMigrationInitiatorIsMaster();

        try {
            doRun();
        } catch (Throwable t) {
            logMigrationFailure(t);
            failureReason = t;
        } finally {
            if (!success) {
                onExecutionFailure(failureReason);
            }
        }
    }

    private void doRun() throws Exception {
        if (startMigration()) {
            try {
                migrate();
            } catch (Throwable e) {
                success = false;
                failureReason = e;
                getLogger().severe("Error while processing " + migrationInfo, e);
            } finally {
                afterMigrate();
            }
        } else {
            success = false;
            logMigrationCancelled();
        }
    }

    private void assertMigrationInitiatorIsMaster() {
        Address masterAddress = getNodeEngine().getMasterAddress();
        if (!masterAddress.equals(migrationInfo.getMaster())) {
            throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
        }
    }

    private boolean startMigration() {
        return migrationInfo.startProcessing();
    }

    private void logMigrationCancelled() {
        getLogger().warning("Migration is cancelled -> " + migrationInfo);
    }

    private void afterMigrate() {
        if (success) {
            InternalPartitionService partitionService = getService();
            partitionService.setPartitionReplicaVersions(migrationInfo.getPartitionId(), replicaVersions, 1);
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

    private void migrate() throws Exception {
        addActiveMigration();

        for (Operation op : tasks) {
            prepareOperation(op);
            try {
                runMigrationTask(op);
            } catch (Throwable e) {
                getLogger().severe("An exception occurred while executing migration operation " + op, e);
                success = false;
                failureReason = e;
                return;
            }
        }
        success = true;
    }

    private void addActiveMigration() {
        InternalPartitionServiceImpl partitionService = getService();
        partitionService.addActiveMigration(migrationInfo);
    }

    private void runMigrationTask(Operation op) throws Exception {
        MigrationAwareService service = op.getService();
        PartitionMigrationEvent event =
                new PartitionMigrationEvent(MigrationEndpoint.DESTINATION, migrationInfo.getPartitionId());
        service.beforeMigration(event);
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
        }
    }

    @Override
    public String toString() {
        final int numberOfTasks = tasks != null ? tasks.size() : 0;
        return getClass().getSimpleName() + "{partitionId=" + getPartitionId() + ", migration=" + migrationInfo
                + ", replicaVersions=" + Arrays.toString(replicaVersions) + ", numberOfTasks=" + numberOfTasks + '}';
    }
}
