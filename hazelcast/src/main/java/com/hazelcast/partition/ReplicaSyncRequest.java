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

package com.hazelcast.partition;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

/**
 * @author mdogan 4/11/13
 */
public final class ReplicaSyncRequest extends Operation implements PartitionAwareOperation, MigrationCycleOperation {

    public ReplicaSyncRequest() {
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final PartitionServiceImpl partitionService = (PartitionServiceImpl) nodeEngine.getPartitionService();
        partitionService.incrementReplicaSyncProcessCount();

        final ILogger logger = nodeEngine.getLogger(getClass());
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();

        try {
            final Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);
            final PartitionReplicationEvent event = new PartitionReplicationEvent(partitionId, replicaIndex);
            final List<Operation> tasks = new LinkedList<Operation>();
            for (ServiceInfo serviceInfo : services) {
                MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
                final Operation op = service.prepareReplicationOperation(event);
                if (op != null) {
                    op.setServiceName(serviceInfo.getName());
                    tasks.add(op);
                }
            }
            byte[] data = null;
            if (!tasks.isEmpty()) {
                final SerializationService serializationService = nodeEngine.getSerializationService();
                final BufferObjectDataOutput out = serializationService.createObjectDataOutput(1024 * 32);
                try {
                    out.writeInt(tasks.size());
                    for (Operation task : tasks) {
                        serializationService.writeObject(out, task);
                    }
                    data = IOUtil.compress(out.toByteArray());
                } finally {
                    IOUtil.closeResource(out);
                }
            } else {
                final Level level = Level.FINEST;
                if (logger.isLoggable(level)) {
                    logger.log(level, "No replica data is found for partition: " + partitionId + ", replica: " + replicaIndex + "\n" + partitionService.getPartition(partitionId));
                }
            }

            final long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);
            ReplicaSyncResponse syncResponse = new ReplicaSyncResponse(data, replicaVersions);
            syncResponse.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            final Address target = getCallerAddress();
            if (logger.isFinestEnabled()) {
                logger.finest( "Sending sync response to -> " + target + "; for partition: " + partitionId + ", replica: " + replicaIndex);
            }
            final OperationService operationService = nodeEngine.getOperationService();
            operationService.send(syncResponse, target);
        } finally {
            partitionService.decrementReplicaSyncProcessCount();
        }
    }

    public void afterRun() throws Exception {
    }

    public boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return Boolean.TRUE;
    }

    public boolean validatesTarget() {
        return false;
    }

    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ReplicaSyncRequest");
        sb.append("{partition=").append(getPartitionId());
        sb.append(", replica=").append(getReplicaIndex());
        sb.append('}');
        return sb.toString();
    }
}
