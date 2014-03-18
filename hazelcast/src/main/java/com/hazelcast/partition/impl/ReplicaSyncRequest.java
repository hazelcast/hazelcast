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
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.MigrationAwareService;
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

public final class ReplicaSyncRequest extends Operation
        implements PartitionAwareOperation, MigrationCycleOperation {

    public ReplicaSyncRequest() {
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        ILogger logger = nodeEngine.getLogger(getClass());

        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        if (!partitionService.incrementReplicaSyncProcessCount()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Max parallel replication process limit exceeded! " +
                        "Could not run replica sync -> " + toString());
            }
            ReplicaSyncRetryResponse response = new ReplicaSyncRetryResponse();
            response.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            Address target = getCallerAddress();
            OperationService operationService = nodeEngine.getOperationService();
            operationService.send(response, target);
            return;
        }

        try {
            Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);
            PartitionReplicationEvent event = new PartitionReplicationEvent(partitionId, replicaIndex);
            List<Operation> tasks = new LinkedList<Operation>();
            for (ServiceInfo serviceInfo : services) {
                MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
                Operation op = service.prepareReplicationOperation(event);
                if (op != null) {
                    op.setServiceName(serviceInfo.getName());
                    tasks.add(op);
                }
            }
            byte[] data = null;
            boolean compress = nodeEngine.getGroupProperties().PARTITION_MIGRATION_ZIP_ENABLED.getBoolean();
            if (tasks.isEmpty()) {
                logNoReplicaDataFound(partitionId, replicaIndex);
            } else {
                SerializationService serializationService = nodeEngine.getSerializationService();
                BufferObjectDataOutput out = serializationService.createObjectDataOutput(1024 * 32);
                try {
                    out.writeInt(tasks.size());
                    for (Operation task : tasks) {
                        serializationService.writeObject(out, task);
                    }
                    data = out.toByteArray();
                    if (compress) {
                        data = IOUtil.compress(data);
                    }
                } finally {
                    closeResource(out);
                }
            }

            long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);
            ReplicaSyncResponse syncResponse = new ReplicaSyncResponse(data, replicaVersions, compress);
            syncResponse.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            Address target = getCallerAddress();
            logSendSyncResponse(partitionId, replicaIndex, target);
            OperationService operationService = nodeEngine.getOperationService();
            operationService.send(syncResponse, target);
        } finally {
            partitionService.decrementReplicaSyncProcessCount();
        }
    }

    private void logSendSyncResponse(int partitionId, int replicaIndex, Address target) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        ILogger logger = nodeEngine.getLogger(getClass());

        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync response to -> " + target + "; for partition: " + partitionId
                    + ", replica: " + replicaIndex);
        }
    }

    private void logNoReplicaDataFound(int partitionId, int replicaIndex) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        ILogger logger = nodeEngine.getLogger(getClass());

        if (logger.isFinestEnabled()) {
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            logger.finest("No replica data is found for partition: " + partitionId
                    + ", replica: " + replicaIndex + "\n" + partitionService.getPartition(partitionId));
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
