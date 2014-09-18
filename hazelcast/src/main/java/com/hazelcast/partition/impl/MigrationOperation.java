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

package com.hazelcast.partition.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("EI_EXPOSE_REP")
public final class MigrationOperation extends BaseMigrationOperation {

    private static final ResponseHandler ERROR_RESPONSE_HANDLER = new ResponseHandler() {
        @Override
        public void sendResponse(Object obj) {
            throw new HazelcastException("Migration operations can not send response!");
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    };

    private long[] replicaVersions;
    private Collection<Operation> tasks;

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
        }
    }

    private void doRun() throws Exception {
        if (startMigration()) {
            try {
                migrate();
            } catch (Throwable e) {
                success = false;
                getLogger().severe("Error while processing " + migrationInfo, e);
            } finally {
                afterMigrate();
            }
        } else {
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
            InternalPartitionServiceImpl partitionService = getService();
            partitionService.setPartitionReplicaVersions(migrationInfo.getPartitionId(), replicaVersions);
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
            try {
                runMigrationTask(op);
            } catch (Throwable e) {
                getLogger().severe("An exception occurred while executing migration operation " + op, e);
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
        op.setNodeEngine(getNodeEngine())
                .setPartitionId(getPartitionId())
                .setReplicaIndex(getReplicaIndex());
        op.setResponseHandler(ERROR_RESPONSE_HANDLER);
        OperationAccessor.setCallerAddress(op, migrationInfo.getSource());
        MigrationAwareService service = op.getService();
        PartitionMigrationEvent event =
                new PartitionMigrationEvent(MigrationEndpoint.DESTINATION, migrationInfo.getPartitionId());
        service.beforeMigration(event);
        op.beforeRun();
        op.run();
        op.afterRun();
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
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{partitionId=").append(getPartitionId());
        sb.append(", migration=").append(migrationInfo);
        sb.append(", version=").append(Arrays.toString(replicaVersions));
        sb.append(", tasks=").append(tasks != null ? tasks.size() : 0);
        sb.append('}');
        return sb.toString();
    }
}
