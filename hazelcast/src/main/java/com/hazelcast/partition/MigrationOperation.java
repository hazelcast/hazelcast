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

package com.hazelcast.partition;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;

public final class MigrationOperation extends BaseMigrationOperation {

    private static final ResponseHandler ERROR_RESPONSE_HANDLER = new ErrorResponseHandler();

    private long[] replicaVersions;
    private transient Collection<Operation> tasks;
    private byte[] taskData;
    private int taskCount;
    private boolean compressed;

    public MigrationOperation() {
    }

    public MigrationOperation(MigrationInfo migrationInfo, long[] replicaVersions, byte[] taskData, int taskCount, boolean compressed) {
        super(migrationInfo);
        this.replicaVersions = replicaVersions;
        this.taskCount = taskCount;
        this.taskData = taskData;
        this.compressed = compressed;
    }

    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        if (!nodeEngine.getMasterAddress().equals(migrationInfo.getMaster())) {
            throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
        }
        SerializationService serializationService = nodeEngine.getSerializationService();
        BufferObjectDataInput in = null;
        if (migrationInfo.startProcessing()) {
            try {
                final byte[] data;
                if (compressed) {
                    data = IOUtil.decompress(taskData);
                } else {
                    data = taskData;
                }
                in = serializationService.createObjectDataInput(data);
                int size = in.readInt();
                tasks = new ArrayList<Operation>(size);
                for (int i = 0; i < size; i++) {
                    Operation task = (Operation) serializationService.readObject(in);
                    tasks.add(task);
                }
                if (taskCount != tasks.size()) {
                    getLogger().severe("Migration task count mismatch! => " +
                            "expected-count: " + size + ", actual-count: " + tasks.size() +
                            "\nfrom: " + migrationInfo.getSource() + ", partition: " + getPartitionId()
                            + ", replica: " + getReplicaIndex());
                }
                success = runMigrationTasks();
                if (success) {
                    final PartitionServiceImpl partitionService = getService();
                    partitionService.setPartitionReplicaVersions(migrationInfo.getPartitionId(), replicaVersions);
                }
            } catch (Throwable e) {
                Level level = Level.WARNING;
                if (e instanceof IllegalStateException) {
                    level = Level.FINEST;
                }
                getLogger().log(level, e.getMessage(), e);
                success = false;
            } finally {
                migrationInfo.doneProcessing();
                IOUtil.closeResource(in);
            }
        } else {
            getLogger().warning( "Migration is cancelled -> " + migrationInfo);
            success = false;
        }
    }

    public Object getResponse() {
        return success;
    }

    private boolean runMigrationTasks() {
        boolean error = false;
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final PartitionServiceImpl partitionService = getService();
        partitionService.addActiveMigration(migrationInfo);

        for (Operation op : tasks) {
            try {
                op.setNodeEngine(nodeEngine)
                        .setPartitionId(getPartitionId()).setReplicaIndex(getReplicaIndex());
                op.setResponseHandler(ERROR_RESPONSE_HANDLER);
                OperationAccessor.setCallerAddress(op, migrationInfo.getSource());
                MigrationAwareService service = op.getService();
                service.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.DESTINATION, migrationInfo.getPartitionId()));
                op.beforeRun();
                op.run();
                op.afterRun();
            } catch (Throwable e) {
                error = true;
                getLogger().severe("While executing " + op, e);
                break;
            }
        }
        return !error;
    }

    private static class ErrorResponseHandler implements ResponseHandler {
        public void sendResponse(final Object obj) {
            throw new HazelcastException("Migration operations can not send response!");
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(compressed);
        out.writeInt(taskCount);
        out.writeInt(taskData.length);
        out.write(taskData);
        out.writeLongArray(replicaVersions);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        compressed = in.readBoolean();
        taskCount = in.readInt();
        int size = in.readInt();
        taskData = new byte[size];
        in.readFully(taskData);
        replicaVersions = in.readLongArray();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{partitionId=").append(getPartitionId());
        sb.append(", migration=").append(migrationInfo);
        sb.append(", taskCount=").append(taskCount);
        sb.append(", compressed=").append(compressed);
        sb.append('}');
        return sb.toString();
    }
}
