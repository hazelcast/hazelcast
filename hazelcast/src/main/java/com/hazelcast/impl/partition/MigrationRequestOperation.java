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

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.impl.PartitionManager;
import com.hazelcast.impl.spi.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MigrationRequestOperation extends MigratingPartition implements Operation {
    private boolean migration; // migration or copy
    private boolean diffOnly;
    private int selfCopyReplicaIndex = -1;
    private OperationContext context = new OperationContext();
//    private transient HazelcastInstance hazelcast;

    public MigrationRequestOperation() {
    }

    public MigrationRequestOperation(int partitionId, Address from, Address to, int replicaIndex, boolean migration) {
        this(partitionId, from, to, replicaIndex, migration, false);
    }

    public MigrationRequestOperation(int partitionId, Address from, Address to, int replicaIndex,
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

    public void run() {
        OperationContext context = getOperationContext();
        final ResponseHandler responseHandler = context.getResponseHandler();
        if (to.equals(from)) {
            getLogger().log(Level.FINEST, "To and from addresses are same! => " + toString());
            responseHandler.sendResponse(Boolean.FALSE);
            return;
        }
        if (from == null) {
            getLogger().log(Level.FINEST, "From address is null => " + toString());
        }

        PartitionManager pm = (PartitionManager) context.getService();
//        final Node node = ((FactoryImpl) hazelcast).node;
//        PartitionManager pm = node.concurrentMapManager.getPartitionManager();
        System.err.println("RUNNING ... REQUEST TASK... " + this);
        try {
            Member target = pm.getMember(to);
            if (target == null) {
                getLogger().log(Level.WARNING, "Target member of task could not be found! => " + toString());
                responseHandler.sendResponse(Boolean.FALSE);
                return;
            }
//            final CostAwareRecordList costAwareRecordList = pm.getActivePartitionRecords(partitionId, replicaIndex, to, diffOnly);
//            DistributedTask task = new DistributedTask(new MigrationOperation(partitionId, costAwareRecordList,
//                    replicaIndex, from), target);
//
            final Collection<ServiceMigrationOperation> tasks = pm.collectMigrationTasks(partitionId, replicaIndex, to, diffOnly);
            Invocation inv = context.getNodeService().createSingleInvocation(PartitionManager.PARTITION_SERVICE_NAME,
                    new MigrationOperation(partitionId, tasks, replicaIndex, from), -1)
                    .setTryCount(3).setTryPauseMillis(1000).setReplicaIndex(replicaIndex).setTarget(to).build();

//            Future future = node.factory.getExecutorService(PartitionManager.MIGRATION_EXECUTOR_NAME).submit(task);
            Future future = inv.invoke();
            final long timeout = context.getNodeService().getNode().groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong();
            Boolean result = (Boolean) IOUtil.toObject(future.get(timeout, TimeUnit.SECONDS));
            responseHandler.sendResponse(result);
        } catch (Throwable e) {
            Level level = Level.WARNING;
            if (e instanceof ExecutionException) {
                e = e.getCause();
            }
            if (e instanceof MemberLeftException || e instanceof IllegalStateException) {
                level = Level.FINEST;
            }
            getLogger().log(level, e.getMessage(), e);
            responseHandler.sendResponse(Boolean.FALSE);
        }
    }

    private ILogger getLogger() {
//        return ((FactoryImpl) hazelcast).node.getLogger(MigrationRequestOperation.class.getName());
        return getOperationContext().getNodeService().getNode().getLogger(MigrationRequestOperation.class.getName());
    }

    public OperationContext getOperationContext() {
        return context;
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

//    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
//        this.hazelcast = hazelcastInstance;
//    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MigrationRequestOperation");
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
