/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.util.CollectionUtil.toIntArray;

public class QueryPartitionOperationFactory extends PartitionAwareOperationFactory {

    private Query query;

    public QueryPartitionOperationFactory() {
    }

    public QueryPartitionOperationFactory(Query query, Collection<Integer> partitions) {
        this.partitions = toIntArray(partitions);
        this.query = query;
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        QueryPartitionOperation op = new QueryPartitionOperation(query);
        op.setPartitionId(partitionId);
        return op;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_PARTITION_OP_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeIntArray(partitions);
        out.writeObject(query);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.partitions = in.readIntArray();
        this.query = in.readObject();
    }
}
