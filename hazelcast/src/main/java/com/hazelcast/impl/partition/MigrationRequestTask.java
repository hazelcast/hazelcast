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

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.PartitionManager;
import com.hazelcast.impl.concurrentmap.CostAwareRecordList;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MigrationRequestTask implements Callable<Boolean>, DataSerializable, HazelcastInstanceAware {
    private int partitionId;
    private Address from;
    private Address to;
    private int replicaIndex;
    private boolean migration; //
    private HazelcastInstance hazelcast;

    public MigrationRequestTask() {
    }

    public MigrationRequestTask(int partitionId, Address from, Address to, int replicaIndex, boolean migration) {
        this.partitionId = partitionId;
        this.from = from;
        this.to = to;
        this.replicaIndex = replicaIndex;
        this.migration = migration;
    }

    public Address getFromAddress() {
        return from;
    }

    public Address getToAddress() {
        return to;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public boolean isMigration() {
        return migration;
    }

    public Boolean call() throws Exception {
        if (to.equals(from)) return Boolean.TRUE;
        Node node = ((FactoryImpl) hazelcast).node;
        PartitionManager pm = node.concurrentMapManager.getPartitionManager();
        try {
            Member target = pm.getMember(to);
            if (target == null) return Boolean.FALSE;
            CostAwareRecordList costAwareRecordList = pm.getActivePartitionRecords(partitionId, replicaIndex, to);
            DistributedTask task = new DistributedTask(new MigrationTask(partitionId, costAwareRecordList, replicaIndex), target);
            Future future = node.factory.getExecutorService().submit(task);
            return (Boolean) future.get(400, TimeUnit.SECONDS);
        } catch (IllegalStateException e) {
            // thrown if MigrationTask is being submitted during shutdown
            node.getLogger(MigrationRequestTask.class.getName()).log(Level.FINEST, e.getMessage(), e);
        } catch (Throwable e) {
            node.getLogger(MigrationRequestTask.class.getName()).log(Level.WARNING, e.getMessage(), e);
        }
        return Boolean.FALSE;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        out.writeBoolean(migration);
        boolean hasFrom = from != null;
        out.writeBoolean(hasFrom);
        if (hasFrom) {
            from.writeData(out);
        }
        to.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        migration = in.readBoolean();
        boolean hasFrom = in.readBoolean();
        if (hasFrom) {
            from = new Address();
            from.readData(in);
        }
        to = new Address();
        to.readData(in);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }

    @Override
    public String toString() {
        return "MigrationRequestTask{" +
                "partitionId=" + partitionId +
                ", from=" + from +
                ", to=" + to +
                ", replicaIndex=" + replicaIndex +
                ", migration=" + migration +
                '}';
    }
}
