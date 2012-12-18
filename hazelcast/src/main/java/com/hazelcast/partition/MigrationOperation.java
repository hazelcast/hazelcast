/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.MigrationServiceEvent;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class MigrationOperation extends BaseMigrationOperation {

    private static final ResponseHandler ERROR_RESPONSE_HANDLER = new ErrorResponseHandler();

    private transient Collection<Operation> tasks;
    private byte[] bytesRecordSet;
    private int taskCount;

    public MigrationOperation() {
    }

    public MigrationOperation(MigrationInfo migrationInfo, Collection<Operation> tasks) throws IOException {
        super(migrationInfo);
        this.tasks = tasks;
        this.taskCount = tasks.size();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8192 * taskCount);
        DataOutputStream out = null;
        try {
            out = new DataOutputStream(new DeflaterOutputStream(bos));
            out.writeInt(taskCount);
            for (Operation task : tasks) {
                IOUtil.writeObject(out, task);
            }
        } finally {
            IOUtil.closeResource(out);
        }
        bytesRecordSet = bos.toByteArray();
    }

    public void run() {
        DataInputStream in = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytesRecordSet);
            in = new DataInputStream(new InflaterInputStream(bais));
            int size = in.readInt();
            tasks = new ArrayList<Operation>(size);
            for (int i = 0; i < size; i++) {
                Operation task = IOUtil.readObject(in);
                tasks.add(task);
            }
            if (taskCount != tasks.size()) {
                getLogger().log(Level.SEVERE, "Migration task count mismatch! => " +
                        "expected-count: " + size + ", actual-count: " + tasks.size() +
                        "\nfrom: " + migrationInfo.getFromAddress() + ", partition: " + getPartitionId()
                        + ", replica: " + getReplicaIndex());
            }
            success = runMigrationTasks();
        } catch (Throwable e) {
            Level level = Level.WARNING;
            if (e instanceof IllegalStateException) {
                level = Level.FINEST;
            }
            getLogger().log(level, e.getMessage(), e);
            success = false;
        } finally {
            IOUtil.closeResource(in);
        }
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    private boolean runMigrationTasks() {
        boolean error = false;
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final PartitionService partitionService = getService();
        partitionService.addActiveMigration(migrationInfo);

        for (Operation op : tasks) {
            try {
                op.setNodeEngine(nodeEngine).setCaller(migrationInfo.getFromAddress())
                        .setPartitionId(getPartitionId()).setReplicaIndex(getReplicaIndex());
                op.setResponseHandler(ERROR_RESPONSE_HANDLER);
                MigrationAwareService service = op.getService();
                service.beforeMigration(new MigrationServiceEvent(MigrationEndpoint.DESTINATION, migrationInfo));
                op.beforeRun();
                op.run();
                op.afterRun();
            } catch (Throwable e) {
                error = true;
                getLogger().log(Level.SEVERE, "While executing " + op, e);
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

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(taskCount);
        out.writeInt(bytesRecordSet.length);
        out.write(bytesRecordSet);
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        taskCount = in.readInt();
        int size = in.readInt();
        bytesRecordSet = new byte[size];
        in.readFully(bytesRecordSet);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{partitionId=").append(getPartitionId());
        sb.append(", replicaIndex=").append(getReplicaIndex());
        sb.append(", migration=").append(migrationInfo);
        sb.append(", taskCount=").append(taskCount);
        sb.append('}');
        return sb.toString();
    }
}
