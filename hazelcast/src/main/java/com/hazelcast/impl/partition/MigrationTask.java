/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.base.DataRecordEntry;
import com.hazelcast.impl.base.RecordSet;
import com.hazelcast.impl.concurrentmap.CostAwareRecordList;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;

import java.io.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class MigrationTask implements Callable<Boolean>, DataSerializable, HazelcastInstanceAware {
    private int partitionId;
    private int replicaIndex;
    private byte[] bytesRecordSet;
    private transient HazelcastInstance hazelcast;

    public MigrationTask() {
    }

    public MigrationTask(int partitionId, CostAwareRecordList costAwareRecordList, int replicaIndex) throws IOException {
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;

        ByteArrayOutputStream bos = new ByteArrayOutputStream((int) (costAwareRecordList.getCost() / 100));
        DataOutputStream dos = null;
        try {
            dos = new DataOutputStream(new DeflaterOutputStream(bos));
            List<Record> lsRecordsToMigrate = costAwareRecordList.getRecords();
            dos.writeInt(lsRecordsToMigrate.size());
            for (Record record : lsRecordsToMigrate) {
                new DataRecordEntry(record).writeData(dos);
            }
        } finally {
            IOUtil.closeResource(dos);
        }
        bytesRecordSet = bos.toByteArray();
    }

    public Boolean call() throws Exception {
        Node node = ((FactoryImpl) hazelcast).node;
        DataInputStream dis = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytesRecordSet);
            dis = new DataInputStream(new InflaterInputStream(bais));
            int size = dis.readInt();
            RecordSet recordSet = new RecordSet();
            for (int i = 0; i < size; i++) {
                DataRecordEntry r = new DataRecordEntry();
                r.readData(dis);
                recordSet.addDataRecordEntry(r);
            }
            node.concurrentMapManager.getPartitionManager().doMigrate(partitionId, replicaIndex, recordSet);
        } catch (IOException e) {
            node.getLogger(MigrationTask.class.getName()).log(Level.WARNING, e.getMessage(), e);
            return Boolean.FALSE;
        } finally {
            IOUtil.closeResource(dis);
        }
        return Boolean.TRUE;
    }

    public void writeData(DataOutput out) throws IOException {
        try {
            out.writeInt(partitionId);
            out.writeInt(replicaIndex);
            out.writeInt(bytesRecordSet.length);
            out.write(bytesRecordSet);
//            System.out.println("write size " + bytesRecordSet.length);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        int size = in.readInt();
        bytesRecordSet = new byte[size];
        in.readFully(bytesRecordSet);
//        System.out.println("read " + bytesRecordSet.length);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }
}
