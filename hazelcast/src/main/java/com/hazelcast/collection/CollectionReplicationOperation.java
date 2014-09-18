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

package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import java.io.IOException;
import java.util.Map;

public abstract class CollectionReplicationOperation extends AbstractOperation implements IdentifiedDataSerializable {

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
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, CollectionContainer> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            CollectionContainer container = entry.getValue();
            container.writeData(out);
        }
    }

    @Override
    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

}
