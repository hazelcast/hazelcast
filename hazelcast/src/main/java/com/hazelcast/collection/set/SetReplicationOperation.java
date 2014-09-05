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

package com.hazelcast.collection.set;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionReplicationOperation;
import com.hazelcast.nio.ObjectDataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SetReplicationOperation extends CollectionReplicationOperation {

    public SetReplicationOperation() {
    }

    public SetReplicationOperation(Map<String, CollectionContainer> migrationData, int partitionId, int replicaIndex) {
        super(migrationData, partitionId, replicaIndex);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = new HashMap<String, CollectionContainer>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            SetContainer container = new SetContainer();
            container.readData(in);
            migrationData.put(name, container);
        }
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.SET_REPLICATION;
    }
}
