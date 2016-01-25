/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import com.hazelcast.spi.exception.WrongTargetException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class ReplicaSyncResponse extends Operation
        implements PartitionAwareOperation, BackupOperation, UrgentSystemOperation, AllowedDuringPassiveState {

    private List<Operation> tasks;
    private long[] replicaVersions;

    public ReplicaSyncResponse() {
    }

    public ReplicaSyncResponse(List<Operation> data, long[] replicaVersions) {
        this.tasks = data;
        this.replicaVersions = replicaVersions;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        InternalPartitionImpl partition = partitionService.getPartitionImpl(partitionId);
        Address thisAddress = nodeEngine.getThisAddress();
        int currentReplicaIndex = partition.getReplicaIndex(thisAddress);
        try {
            if (replicaIndex == currentReplicaIndex) {
                executeTasks();
            } else {
                nodeNotOwnsBackup(partition);
            }
            if (tasks != null) {
                tasks.clear();
            }
        } finally {
            postProcessReplicaSync(partitionService, currentReplicaIndex);
        }
    }

    private void postProcessReplicaSync(InternalPartitionServiceImpl partitionService, int currentReplicaIndex) {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        if (replicaIndex == currentReplicaIndex) {
            partitionService.finalizeReplicaSync(partitionId, replicaIndex, replicaVersions);
        } else {
            partitionService.clearReplicaSyncRequest(partitionId, replicaIndex);
            if (currentReplicaIndex < 0) {
                partitionService.clearPartitionReplicaVersions(partitionId);
            } else if (currentReplicaIndex > 0) {
                partitionService.triggerPartitionReplicaSync(partitionId, currentReplicaIndex, 0);
            }
        }
    }

    private void nodeNotOwnsBackup(InternalPartitionImpl partition) {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        Address thisAddress = getNodeEngine().getThisAddress();
        int currentReplicaIndex = partition.getReplicaIndex(thisAddress);

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest(
                    "This node is not backup replica of partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                            + " anymore. current replicaIndex=" + currentReplicaIndex);
        }

        if (tasks != null) {
            Throwable throwable = new WrongTargetException(thisAddress, partition.getReplicaAddress(replicaIndex),
                    partitionId, replicaIndex, getClass().getName());
            for (Operation op : tasks) {
                prepareOperation(op);
                onOperationFailure(op, throwable);
            }
        }
    }

    private void executeTasks() {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        if (tasks != null && !tasks.isEmpty()) {
            logApplyReplicaSync(partitionId, replicaIndex);
            for (Operation op : tasks) {
                prepareOperation(op);
                try {
                    op.beforeRun();
                    op.run();
                    op.afterRun();
                } catch (Throwable e) {
                    onOperationFailure(op, e);
                    logException(op, e);
                }
            }
        } else {
            logEmptyTaskList(partitionId, replicaIndex);
        }
    }

    private void prepareOperation(Operation op) {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        NodeEngine nodeEngine = getNodeEngine();

        ILogger opLogger = nodeEngine.getLogger(op.getClass());
        OperationResponseHandler responseHandler = createErrorLoggingResponseHandler(opLogger);

        op.setNodeEngine(nodeEngine).setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex).setOperationResponseHandler(responseHandler);
    }

    private void logEmptyTaskList(int partitionId, int replicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("No data available for replica sync, partitionId=" + partitionId + ", replicaIndex=" + replicaIndex);
        }
    }

    private void logException(Operation op, Throwable e) {
        ILogger logger = getLogger();
        NodeEngine nodeEngine = getNodeEngine();
        Level level = nodeEngine.isRunning() ? Level.WARNING : Level.FINEST;
        if (logger.isLoggable(level)) {
            logger.log(level, "While executing " + op, e);
        }
    }

    private void logApplyReplicaSync(int partitionId, int replicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Applying replica sync for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex);
        }
    }


    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (tasks != null) {
            for (Operation op : tasks) {
                prepareOperation(op);
                onOperationFailure(op, e);
            }
        }
    }

    private void onOperationFailure(Operation op, Throwable e) {
        try {
            op.onExecutionFailure(e);
        } catch (Throwable t) {
            getLogger().warning("While calling operation.onFailure(). op: " + op, t);
        }
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLongArray(replicaVersions);
        int size = tasks != null ? tasks.size() : 0;
        out.writeInt(size);
        if (size > 0) {
            for (Operation task : tasks) {
                out.writeObject(task);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        replicaVersions = in.readLongArray();
        int size = in.readInt();
        if (size > 0) {
            tasks = new ArrayList<Operation>(size);
            for (int i = 0; i < size; i++) {
                Operation op = in.readObject();
                tasks.add(op);
            }
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", replicaVersions=").append(Arrays.toString(replicaVersions));
    }
}
