/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.vector.impl.VectorCollectionService;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.impl.storage.ReplicationStateHolder;
import com.hazelcast.vector.impl.storage.VectorCollectionStorage;

import java.io.IOException;
import java.util.Map;

/**
 * Replicates data of several Vector Collections in a single partition.
 * Implements fragmented (not chunked) replication for vector collections.
 */
public class ReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private ReplicationStateHolder replicationStateHolder;

    public ReplicationOperation() {
    }

    public ReplicationOperation(NodeEngine nodeEngine,
                                Map<String, VectorCollectionStorage> storageMap,
                                int partitionId, int replicaIndex) {
        this.replicationStateHolder = new ReplicationStateHolder(storageMap);
        this.setNodeEngine(nodeEngine);
        this.setPartitionId(partitionId);
        this.setReplicaIndex(replicaIndex);
    }

    @Override
    public void run() {
        VectorCollectionService service = getService();
        replicationStateHolder.apply(service, getPartitionId());
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(replicationStateHolder);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        replicationStateHolder = in.readObject();
    }

    @Override
    public String getServiceName() {
        return VectorCollectionService.SERVICE_NAME;
    }

    // visible for testing
    public ReplicationStateHolder getReplicationStateHolder() {
        return replicationStateHolder;
    }
}
