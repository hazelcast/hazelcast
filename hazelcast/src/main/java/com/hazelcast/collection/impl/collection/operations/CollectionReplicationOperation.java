/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.collection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionService;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;

public abstract class CollectionReplicationOperation extends Operation implements IdentifiedDataSerializable {

    protected Map<String, CollectionContainer> migrationData;

    public CollectionReplicationOperation() {
    }

    public CollectionReplicationOperation(Map<String, CollectionContainer> migrationData, int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        CollectionService service = getService();
        for (Map.Entry<String, CollectionContainer> entry : migrationData.entrySet()) {
            String name = entry.getKey();
            CollectionContainer container = entry.getValue();
            container.init(getNodeEngine());
            service.addContainer(name, container);
        }
    }

    @Override
    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, CollectionContainer> entry : migrationData.entrySet()) {
            out.writeString(entry.getKey());
            CollectionContainer container = entry.getValue();
            out.writeObject(container);
        }
    }
}
