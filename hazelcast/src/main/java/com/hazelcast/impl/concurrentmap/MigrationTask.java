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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.base.RecordSet;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

import static com.hazelcast.nio.IOUtil.*;

public class MigrationTask implements Callable<Boolean>, DataSerializable, HazelcastInstanceAware {
    private int partitionId;
    private int replicaIndex;
    private Data dataRecordSet;
    private HazelcastInstance hazelcast;

    public MigrationTask() {
    }

    public MigrationTask(int partitionId, Data dataRecordSet, int replicaIndex) {
        this.partitionId = partitionId;
        this.dataRecordSet = dataRecordSet;
        this.replicaIndex = replicaIndex;
    }

    public Boolean call() throws Exception {
        Node node = ((FactoryImpl) hazelcast).node;
        RecordSet recordSet = (RecordSet) toObject(dataRecordSet);
        node.concurrentMapManager.getPartitionManager().doMigrate(partitionId, replicaIndex, recordSet);
        return Boolean.TRUE;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        byte[] compressed = compress(dataRecordSet.buffer);
        out.writeInt(compressed.length);
        out.write(compressed);
    }

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        int size = in.readInt();
        byte[] compressed = new byte[size];
        in.readFully(compressed);
        dataRecordSet = new Data(decompress(compressed));
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }
}
