/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.DataCollection;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiMapGetAllOperationFactory extends PartitionAwareOperationFactory {
    protected String name;
    protected DataCollection[] setEntries;

    protected Map<Integer, Collection<Data>> partitionsToEntriesMap = new HashMap<>();

    public MultiMapGetAllOperationFactory() {
    }

    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public MultiMapGetAllOperationFactory(String name, Collection<Integer> partitions, Collection<List<Data>> setEntries) {
        this.name = name;
        //NB: not used in this operation
        //super.partitions = partitions;
        this.setEntries = new DataCollection[setEntries.size()];

        int i = 0;
        for (List setEntry : setEntries) {
            this.setEntries[i++] = new DataCollection(setEntry);
        }

        //build partition map
        i = 0;
        for (int partition : partitions) {
            partitionsToEntriesMap.put(partition, this.setEntries[i++].getCollection());
        }
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        if (partitionsToEntriesMap.containsKey(partitionId)) {
            return new GetAllOperation(name, partitionsToEntriesMap.get(partitionId));
        } else {
            throw new IllegalArgumentException(
                    "Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
        }

    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeIntArray(partitions);
        for (DataCollection entry : setEntries) {
            out.writeObject(entry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitions = in.readIntArray();
        setEntries = new DataCollection[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            setEntries[i] = in.readObject();
        }

        //rebuild partition map
        int i = 0;
        for (int partition : partitions) {
            partitionsToEntriesMap.put(partition, setEntries[i++].getCollection());
        }
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.GET_ALL_PARTITION_AWARE_FACTORY;
    }
}
