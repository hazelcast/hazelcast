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

import com.hazelcast.impl.PartitionManager;
import com.hazelcast.impl.spi.AbstractOperation;
import com.hazelcast.impl.spi.ServiceMigrationOperation;
import com.hazelcast.impl.spi.NonBlockingOperation;
import com.hazelcast.impl.spi.OperationContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ClassLoaderUtil;

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
//    private transient HazelcastInstance hazelcast;

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
                out.writeUTF(task.getClass().getName());
                task.writeData(out);
            }
        } finally {
            IOUtil.closeResource(out);
        }
        bytesRecordSet = bos.toByteArray();
    }

//    public MigrationOperation(int partitionId, CostAwareRecordList costAwareRecordList,
//                              int replicaIndex, Address from) throws IOException {
//        this.partitionId = partitionId;
//        this.replicaIndex = replicaIndex;
//        this.from = from;
//        this.recordCount = costAwareRecordList.getRecords().size();
//        ByteArrayOutputStream bos = new ByteArrayOutputStream((int) (costAwareRecordList.getCost() / 100));
//        DataOutputStream dos = null;
//        try {
//            dos = new DataOutputStream(new DeflaterOutputStream(bos));
//            List<Record> lsRecordsToMigrate = costAwareRecordList.getRecords();
//            dos.writeInt(lsRecordsToMigrate.size());
//            for (Record record : lsRecordsToMigrate) {
//                new DataRecordEntry(record, true).writeData(dos);
//            }
//        } finally {
//            IOUtil.closeResource(dos);
//        }
//        bytesRecordSet = bos.toByteArray();
//    }

    public void run() {
        OperationContext context = getOperationContext();
        PartitionManager pm = (PartitionManager) context.getService();
//        Node node = ((FactoryImpl) hazelcast).node;
//        PartitionManager pm = node.concurrentMapManager.getPartitionManager();
        System.err.println("RUNNING ... TASK... " + this);
        DataInputStream in = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytesRecordSet);
            in = new DataInputStream(new InflaterInputStream(bais));
            int size = in.readInt();
            tasks = new ArrayList<ServiceMigrationOperation>(size);
            for (int i = 0; i < size; i++) {
                ServiceMigrationOperation task = (ServiceMigrationOperation) ClassLoaderUtil.newInstance(in.readUTF());
                task.readData(in);
                tasks.add(task);
            }
//            RecordSet recordSet = new RecordSet();
//            for (int i = 0; i < size; i++) {
//                DataRecordEntry r = new DataRecordEntry();
//                r.readData(dis);
//                recordSet.addDataRecordEntry(r);
//            }
            if (taskCount != tasks.size()) {
                getLogger().log(Level.SEVERE, "Migration task count mismatch! => " +
                                              "expected-count: " + size + ", actual-count: " + tasks.size() +
                                              "\nfrom: " + from + ", partition: " + partitionId
                                              + ", replica: " + replicaIndex);
            }
//            pm.doMigrate(partitionId, replicaIndex, recordSet, from);
            final boolean result = pm.runMigrationTasks(tasks, partitionId, replicaIndex, from);
            context.getResponseHandler().sendResponse(result);
        } catch (Throwable e) {
            Level level = Level.WARNING;
            if (e instanceof IllegalStateException) {
                level = Level.FINEST;
            }
            getLogger().log(level, e.getMessage(), e);
            context.getResponseHandler().sendResponse(Boolean.FALSE);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private ILogger getLogger() {
//        return ((FactoryImpl) hazelcast).node.getLogger(MigrationOperation.class.getName());
        return getOperationContext().getNodeService().getNode().getLogger(MigrationOperation.class.getName());
    }

    public void writeData(DataOutput out) throws IOException {
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

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        taskCount = in.readInt();
        from = new Address();
        from.readData(in);
        int size = in.readInt();
        bytesRecordSet = new byte[size];
        in.readFully(bytesRecordSet);
    }

//    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
//        this.hazelcast = hazelcastInstance;
//    }
}
