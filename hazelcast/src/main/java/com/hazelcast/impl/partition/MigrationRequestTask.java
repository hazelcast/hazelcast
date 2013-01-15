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

package com.hazelcast.impl.partition;

import com.hazelcast.core.*;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.PartitionManager;
import com.hazelcast.impl.concurrentmap.CostAwareRecordList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MigrationRequestTask extends MigratingPartition implements Callable<Boolean>, DataSerializable, HazelcastInstanceAware {
    private boolean migration; // migration or copy
    private boolean diffOnly;
    private int selfCopyReplicaIndex = -1;
    private transient HazelcastInstance hazelcast;

    public MigrationRequestTask() {
    }

    public MigrationRequestTask(int partitionId, Address from, Address to, int replicaIndex, boolean migration) {
        this(partitionId, from, to, replicaIndex, migration, false);
    }

    public MigrationRequestTask(int partitionId, Address from, Address to, int replicaIndex,
                                boolean migration, boolean diffOnly) {
        super(partitionId, replicaIndex, from, to);
        this.migration = migration;
        this.diffOnly = diffOnly;
    }

    public boolean isMigration() {
        return migration;
    }

    public int getSelfCopyReplicaIndex() {
        return selfCopyReplicaIndex;
    }

    public void setSelfCopyReplicaIndex(final int selfCopyReplicaIndex) {
        this.selfCopyReplicaIndex = selfCopyReplicaIndex;
    }

    public void setFromAddress(final Address from) {
        this.from = from;
    }

    public Boolean call() throws Exception {
        if (to.equals(from)) {
            getLogger().log(Level.FINEST, "To and from addresses are same! => " + toString());
            return Boolean.TRUE;
        }
        if (from == null) {
            getLogger().log(Level.FINEST, "From address is null => " + toString());
        }
        final Node node = ((FactoryImpl) hazelcast).node;
        PartitionManager pm = node.concurrentMapManager.getPartitionManager();
        try {
            Member target = pm.getMember(to);
            if (target == null) {
                getLogger().log(Level.WARNING, "Target member of task could not be found! => " + toString());
                return Boolean.FALSE;
            }
            final CostAwareRecordList costAwareRecordList = pm.getActivePartitionRecords(partitionId, replicaIndex, to, diffOnly);
            DistributedTask task = new DistributedTask(new MigrationTask(partitionId, costAwareRecordList,
                    replicaIndex, from), target);

            Future future = node.factory.getExecutorService(PartitionManager.MIGRATION_EXECUTOR_NAME).submit(task);
            final long timeout = node.groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong();
            return (Boolean) future.get(timeout, TimeUnit.SECONDS);
        } catch (Throwable e) {
            Level level = Level.WARNING;
            if (e instanceof ExecutionException) {
                e = e.getCause();
            }
            if (e instanceof MemberLeftException || e instanceof IllegalStateException) {
                level = Level.FINEST;
            }
            getLogger().log(level, e.getMessage(), e);
        }
        return Boolean.FALSE;
    }

    private ILogger getLogger() {
        return ((FactoryImpl) hazelcast).node.getLogger(MigrationRequestTask.class.getName());
    }

    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(migration);
        out.writeBoolean(diffOnly);
        out.writeInt(selfCopyReplicaIndex);
    }

    public void readData(DataInput in) throws IOException {
        super.readData(in);
        migration = in.readBoolean();
        diffOnly = in.readBoolean();
        selfCopyReplicaIndex = in.readInt();
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MigrationRequestTask");
        sb.append("{partitionId=").append(partitionId);
        sb.append(", from=").append(from);
        sb.append(", to=").append(to);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", migration=").append(migration);
        sb.append(", diffOnly=").append(diffOnly);
        sb.append(", selfCopyReplicaIndex=").append(selfCopyReplicaIndex);
        sb.append('}');
        return sb.toString();
    }
}
