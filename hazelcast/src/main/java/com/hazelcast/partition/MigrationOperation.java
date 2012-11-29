/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.spi.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.impl.AbstractOperation;
import com.hazelcast.spi.impl.NodeServiceImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class MigrationOperation extends AbstractOperation {
    private MigrationType migrationType;
    private int copyBackReplicaIndex = -1;
    private Collection<Operation> tasks;
    private byte[] bytesRecordSet;
    private Address from;
    private int taskCount;

    public MigrationOperation() {
    }

    public MigrationOperation(int partitionId, int replicaIndex, int copyBackReplicaIndex,
                              MigrationType migrationType, Collection<Operation> tasks, Address from) throws IOException {
        super();
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.copyBackReplicaIndex = copyBackReplicaIndex;
        this.migrationType = migrationType;
        this.from = from;
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
                        "\nfrom: " + from + ", partition: " + getPartitionId()
                        + ", replica: " + getReplicaIndex());
            }
            final boolean result = runMigrationTasks();
            getResponseHandler().sendResponse(result);
        } catch (Throwable e) {
            Level level = Level.WARNING;
            if (e instanceof IllegalStateException) {
                level = Level.FINEST;
            }
            getLogger().log(level, e.getMessage(), e);
            getResponseHandler().sendResponse(Boolean.FALSE);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private boolean runMigrationTasks() {
        boolean error = false;
        final NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        final PartitionService partitionService = getService();
        partitionService.addActiveMigration(new MigrationInfo(getPartitionId(), getReplicaIndex(), copyBackReplicaIndex,
                migrationType, from, nodeService.getThisAddress()));

        for (Operation op : tasks) {
            try {
                nodeService.setOperationContext(op, op.getServiceName(), from, -1).setPartitionId(getPartitionId());
                ResponseHandlerFactory.setNoReplyResponseHandler(nodeService, op);
                MigrationAwareService service = op.getService();
                service.beforeMigration(new MigrationServiceEvent(MigrationEndpoint.DESTINATION, getPartitionId(),
                        getReplicaIndex(), migrationType, copyBackReplicaIndex));
                op.run();
            } catch (Throwable e) {
                error = true;
                getLogger().log(Level.SEVERE, e.getMessage(), e);
                break;
            }
        }
        return !error;
    }

    private ILogger getLogger() {
        return getNodeService().getLogger(MigrationOperation.class.getName());
    }

    public void writeInternal(DataOutput out) throws IOException {
        MigrationType.writeTo(migrationType, out);
        out.writeInt(copyBackReplicaIndex);
        out.writeInt(taskCount);
        from.writeData(out);
        out.writeInt(bytesRecordSet.length);
        out.write(bytesRecordSet);
    }

    public void readInternal(DataInput in) throws IOException {
        migrationType = MigrationType.readFrom(in);
        copyBackReplicaIndex = in.readInt();
        taskCount = in.readInt();
        from = new Address();
        from.readData(in);
        int size = in.readInt();
        bytesRecordSet = new byte[size];
        in.readFully(bytesRecordSet);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MigrationOperation");
        sb.append("{partitionId=").append(getPartitionId());
        sb.append(", replicaIndex=").append(getReplicaIndex());
        sb.append(", migrationType=").append(migrationType);
        sb.append(", copyBackReplicaIndex=").append(copyBackReplicaIndex);
        sb.append(", from=").append(from);
        sb.append(", taskCount=").append(taskCount);
        sb.append('}');
        return sb.toString();
    }
}
