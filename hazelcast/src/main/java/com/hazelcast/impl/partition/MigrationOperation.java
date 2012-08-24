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

package com.hazelcast.impl.partition;

import com.hazelcast.impl.spi.AbstractOperation;
import com.hazelcast.impl.spi.NonBlockingOperation;
import com.hazelcast.impl.spi.ServiceMigrationOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class MigrationOperation extends AbstractOperation implements NonBlockingOperation {
    private int partitionId;
    private int replicaIndex;
    private Collection<ServiceMigrationOperation> tasks;
    private byte[] bytesRecordSet;
    private Address from;
    private int taskCount;

    public MigrationOperation() {
    }

    public MigrationOperation(int partitionId, Collection<ServiceMigrationOperation> tasks,
                              int replicaIndex, Address from) throws IOException {
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.from = from;
        this.tasks = tasks;
        this.taskCount = tasks.size();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8192 * taskCount);
        DataOutputStream out = null;
        try {
            out = new DataOutputStream(new DeflaterOutputStream(bos));
            out.writeInt(taskCount);
            for (ServiceMigrationOperation task : tasks) {
                IOUtil.writeObject(out, task);
            }
        } finally {
            IOUtil.closeResource(out);
        }
        bytesRecordSet = bos.toByteArray();
    }

    public void run() {
        PartitionManager pm = (PartitionManager) getService();
        DataInputStream in = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytesRecordSet);
            in = new DataInputStream(new InflaterInputStream(bais));
            int size = in.readInt();
            tasks = new ArrayList<ServiceMigrationOperation>(size);
            for (int i = 0; i < size; i++) {
                ServiceMigrationOperation task = IOUtil.readObject(in);
                tasks.add(task);
            }

            if (taskCount != tasks.size()) {
                getLogger().log(Level.SEVERE, "Migration task count mismatch! => " +
                        "expected-count: " + size + ", actual-count: " + tasks.size() +
                        "\nfrom: " + from + ", partition: " + partitionId
                        + ", replica: " + replicaIndex);
            }

            final boolean result = pm.runMigrationTasks(tasks, partitionId, replicaIndex, from);
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

    private ILogger getLogger() {
        return getNodeService().getLogger(MigrationOperation.class.getName());
    }

    public void writeInternal(DataOutput out) throws IOException {
        try {
            out.writeInt(partitionId);
            out.writeInt(replicaIndex);
            out.writeInt(taskCount);
            from.writeData(out);
            out.writeInt(bytesRecordSet.length);
            out.write(bytesRecordSet);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void readInternal(DataInput in) throws IOException {
        partitionId = in.readInt();
        replicaIndex = in.readInt();
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
        sb.append("{partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", from=").append(from);
        sb.append(", taskCount=").append(taskCount);
        sb.append('}');
        return sb.toString();
    }
}
