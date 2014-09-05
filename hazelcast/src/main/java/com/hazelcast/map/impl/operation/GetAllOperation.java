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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GetAllOperation extends AbstractMapOperation implements PartitionAwareOperation {

    Set<Data> keys = new HashSet<Data>();
    MapEntrySet entrySet;

    public GetAllOperation(String name, Set<Data> keys) {
        super(name);
        this.keys = keys;
    }

    public GetAllOperation() {
    }

    public void run() {
        int partitionId = getPartitionId();
        RecordStore recordStore = mapService.getMapServiceContext().getRecordStore(partitionId, name);
        Set<Data> partitionKeySet = new HashSet<Data>();
        for (Data key : keys) {
            if (partitionId == getNodeEngine().getPartitionService().getPartitionId(key)) {
                partitionKeySet.add(key);
            }
        }
        entrySet = recordStore.getAll(partitionKeySet);
    }

    @Override
    public Object getResponse() {
        return entrySet;
    }

    @Override
    public String toString() {
        return "GetAllOperation{"
                + '}';

    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                key.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > -1) {
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                keys.add(data);
            }
        }
    }
}
