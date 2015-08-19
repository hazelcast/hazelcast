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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntrySet;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class PutAllOperation extends AbstractOperation {
    private String name;
    private ReplicatedMapEntrySet entrySet;

    public PutAllOperation() {
    }

    public PutAllOperation(String name, ReplicatedMapEntrySet entrySet) {
        this.name = name;
        this.entrySet = entrySet;
    }

    @Override
    public void run() throws Exception {
        int partitionId = getPartitionId();
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, partitionId);
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        Set<Map.Entry<Data, Data>> entries = entrySet.getEntrySet();
        for (Map.Entry<Data, Data> entry : entries) {
            Data key = entry.getKey();
            Data value = entry.getValue();
            if (partitionId != partitionService.getPartitionId(key)) {
                continue;
            }
            store.put(key, value);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(entrySet);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        entrySet = in.readObject();
    }
}
