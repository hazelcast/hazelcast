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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

/**
 * @mdogan 4/11/13
 */
public class ReplicaSyncRequest extends Operation {

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final PartitionServiceImpl partitionService = (PartitionServiceImpl) nodeEngine.getPartitionService();
        final int partitionId = getPartitionId();
        final PartitionInfo partitionInfo = partitionService.getPartitionInfo(partitionId);
        final Address owner = partitionInfo.getOwner();
        if (!nodeEngine.getThisAddress().equals(owner)) {
            final ILogger logger = nodeEngine.getLogger(getClass());
            logger.log(Level.FINEST, "Ignoring sync request, since this node is not owner of partition[" + partitionId + "].");
            return;
        }
        final Collection<MigrationAwareService> services = nodeEngine.getServices(MigrationAwareService.class);
        final PartitionReplicationEvent event = new PartitionReplicationEvent(partitionId, getReplicaIndex());
        final List<Operation> tasks = new LinkedList<Operation>();
        for (MigrationAwareService service : services) {
            final Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                tasks.add(op);
            }
        }
        if (!tasks.isEmpty()) {
            if (!nodeEngine.getThisAddress().equals(owner)) {
                final ILogger logger = nodeEngine.getLogger(getClass());
                logger.log(Level.FINEST, "Ignoring sync request, since this node is not owner of partition[" + partitionId + "].");
                return;
            }
            final SerializationService serializationService = nodeEngine.getSerializationService();
            final ObjectDataOutput out = serializationService.createObjectDataOutput(1024 * 32);
            try {
                out.writeInt(tasks.size());
                for (Operation task : tasks) {
                    serializationService.writeObject(out, task);
                }
                byte[] data = IOUtil.compress(out.toByteArray());
                final OperationService operationService = nodeEngine.getOperationService();
                ReplicaSyncResponse syncResponse = new ReplicaSyncResponse(data, partitionService.getPartitionVersion(partitionId));
                syncResponse.setPartitionId(partitionId).setReplicaIndex(getReplicaIndex());
                operationService.send(syncResponse, getCallerAddress());
            } finally {
                IOUtil.closeResource(out);
            }
        }
    }

    public void afterRun() throws Exception {
    }

    public boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return null;
    }

    public boolean validatesTarget() {
        return false;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
    }
}
