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
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceInfo;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.nio.IOUtil.closeResource;

public final class ReplicaSyncRequest extends Operation implements PartitionAwareOperation, MigrationCycleOperation {

    private static final int DEFAULT_DATA_OUTPUT_BUFFER_SIZE = 1024 * 32;

    public ReplicaSyncRequest() {
    }

    @Override
    public void beforeRun() throws Exception {
        int syncReplicaIndex = getReplicaIndex();
        if (syncReplicaIndex < 1 || syncReplicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index should be in range [1-"
                    + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        if (!preCheckReplicaSync(nodeEngine, partitionId, replicaIndex)) {
            sendEmptyResponse();
            return;
        }

        try {
            List<Operation> tasks = createReplicationOperations();
            if (tasks.isEmpty()) {
                logNoReplicaDataFound(partitionId, replicaIndex);
                sendEmptyResponse();
            } else {
                byte[] data = createReplicationData(tasks);
                sendResponse(data);
            }
        } finally {
            partitionService.decrementReplicaSyncProcessCount();
        }
    }

    private boolean preCheckReplicaSync(NodeEngineImpl nodeEngine, int partitionId, int replicaIndex) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        InternalPartitionImpl partition = partitionService.getPartition(partitionId);
        Address owner = partition.getOwnerOrNull();
        long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);
        long currentVersion = replicaVersions[replicaIndex - 1];

        ILogger logger = getLogger();
        if (!nodeEngine.getThisAddress().equals(owner)) {
            if (logger.isFinestEnabled()) {
                logger.finest("Wrong target! " + toString() + " cannot be processed! Target should be: " + owner);
            }
            return false;
        }

        if (currentVersion == 0) {
            return false;
        }

        if (!partitionService.incrementReplicaSyncProcessCount()) {
            if (logger.isFinestEnabled()) {
                logger.finest(
                        "Max parallel replication process limit exceeded! Could not run replica sync -> " + toString());
            }
            return false;
        }
        return true;
    }

    private void sendRetryResponse() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        ReplicaSyncRetryResponse response = new ReplicaSyncRetryResponse();
        response.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        Address target = getCallerAddress();
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(response, target);
    }

    private byte[] createReplicationData(List<Operation> tasks) throws IOException {
        byte[] data;
        NodeEngine nodeEngine = getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();
        BufferObjectDataOutput out = serializationService.createObjectDataOutput(DEFAULT_DATA_OUTPUT_BUFFER_SIZE);
        try {
            out.writeInt(tasks.size());
            for (Operation task : tasks) {
                serializationService.writeObject(out, task);
            }
            data = out.toByteArray();
        } finally {
            closeResource(out);
        }
        return data;
    }

    private List<Operation> createReplicationOperations() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);
        PartitionReplicationEvent event = new PartitionReplicationEvent(getPartitionId(), getReplicaIndex());
        List<Operation> tasks = new LinkedList<Operation>();
        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
            Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                tasks.add(op);
            }
        }
        return tasks;
    }

    private void sendEmptyResponse() throws IOException {
        sendResponse(null);
    }

    private void sendResponse(byte[] data) throws IOException {
        NodeEngine nodeEngine = getNodeEngine();

        ReplicaSyncResponse syncResponse = createResponse(data);
        Address target = getCallerAddress();
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync response to -> " + target + " for partition: " + getPartitionId() + ", replica: "
                    + getReplicaIndex());
        }
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(syncResponse, target);
    }

    private ReplicaSyncResponse createResponse(byte[] data) throws IOException {
        int partitionId = getPartitionId();
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);

        byte[] sendableData = data;
        boolean compress = nodeEngine.getGroupProperties().PARTITION_MIGRATION_ZIP_ENABLED.getBoolean();
        if (sendableData != null && sendableData.length > 0 && compress) {
            sendableData = IOUtil.compress(sendableData);
        }

        ReplicaSyncResponse syncResponse = new ReplicaSyncResponse(sendableData, replicaVersions, compress);
        syncResponse.setPartitionId(partitionId).setReplicaIndex(getReplicaIndex());
        return syncResponse;
    }

    private void logNoReplicaDataFound(int partitionId, int replicaIndex) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        ILogger logger = nodeEngine.getLogger(getClass());

        if (logger.isFinestEnabled()) {
            logger.finest("No replica data is found for partition: " + partitionId + ", replica: " + replicaIndex);
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
        return Boolean.TRUE;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReplicaSyncRequest");
        sb.append("{partition=").append(getPartitionId());
        sb.append(", replica=").append(getReplicaIndex());
        sb.append('}');
        return sb.toString();
    }
}
