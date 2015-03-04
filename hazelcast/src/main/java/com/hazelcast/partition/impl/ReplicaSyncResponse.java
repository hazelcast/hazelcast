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

package com.hazelcast.partition.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("EI_EXPOSE_REP")
public class ReplicaSyncResponse extends Operation
        implements PartitionAwareOperation, BackupOperation, UrgentSystemOperation {

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
                logNodeNotOwnsBackup(partitionId, replicaIndex, currentReplicaIndex);
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
            partitionService.clearReplicaSync(partitionId, replicaIndex);
            if (currentReplicaIndex < 0) {
                partitionService.clearPartitionReplicaVersions(partitionId);
            } else if (currentReplicaIndex > 0) {
                partitionService.triggerPartitionReplicaSync(partitionId, currentReplicaIndex, 0);
            }
        }
    }

    private void logNodeNotOwnsBackup(int partitionId, int replicaIndex, int currentReplicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("This node is not backup replica of partition: " + partitionId + ", replica: " + replicaIndex
                    + " anymore. Current replica index: " + currentReplicaIndex);
        }
    }

    private void executeTasks() {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        if (tasks != null && tasks.size() > 0) {
            NodeEngine nodeEngine = getNodeEngine();
            logApplyReplicaSync(partitionId, replicaIndex);
            for (Operation op : tasks) {
                try {
                    ErrorLoggingResponseHandler responseHandler
                            = new ErrorLoggingResponseHandler(nodeEngine.getLogger(op.getClass()));
                    op.setNodeEngine(nodeEngine)
                            .setPartitionId(partitionId)
                            .setReplicaIndex(replicaIndex)
                            .setResponseHandler(responseHandler);
                    op.beforeRun();
                    op.run();
                    op.afterRun();
                } catch (Throwable e) {
                    logException(op, e);
                }
            }
        } else {
            logEmptyTaskList(partitionId, replicaIndex);
        }
    }

    private void logEmptyTaskList(int partitionId, int replicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("No data available for replica sync, partition: " + partitionId + ", replica: " + replicaIndex);
        }
    }

    private void logException(Operation op, Throwable e) {
        ILogger logger = getLogger();
        NodeEngine nodeEngine = getNodeEngine();
        Level level = nodeEngine.isActive() ? Level.WARNING : Level.FINEST;
        if (logger.isLoggable(level)) {
            logger.log(level, "While executing " + op, e);
        }
    }

    private void logApplyReplicaSync(int partitionId, int replicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Applying replica sync for partition: " + partitionId + ", replica: " + replicaIndex);
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
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReplicaSyncResponse");
        sb.append("{partition=").append(getPartitionId());
        sb.append(", replica=").append(getReplicaIndex());
        sb.append(", version=").append(Arrays.toString(replicaVersions));
        sb.append('}');
        return sb.toString();
    }

    private static final class ErrorLoggingResponseHandler implements ResponseHandler {
        private final ILogger logger;

        private ErrorLoggingResponseHandler(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void sendResponse(final Object obj) {
            if (obj instanceof Throwable) {
                Throwable t = (Throwable) obj;
                logger.severe(t);
            }
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }
}
