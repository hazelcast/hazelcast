/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryRunner;
import com.hazelcast.map.impl.query.ResultSegment;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;

/**
 * Fetches by query a batch of {@code fetchSize} items from a single partition ID for a map. The query is run by the query
 * engine which means it supports projections and filtering. The {@code lastTableIndex} denotes the position from which
 * to resume the query on the partition.
 * This is an operation for maps configured with {@link InMemoryFormat#BINARY} or {{@link InMemoryFormat#OBJECT} format.
 *
 * @see com.hazelcast.map.impl.proxy.MapProxyImpl#iterator(int, int, com.hazelcast.projection.Projection,
 * com.hazelcast.query.Predicate)
 * @since 3.9
 */
public class MapFetchWithQueryOperation extends MapOperation implements ReadonlyOperation {

    private Query query;
    private int fetchSize;
    private int lastTableIndex;
    private transient ResultSegment response;

    public MapFetchWithQueryOperation() {
    }

    public MapFetchWithQueryOperation(String name, int lastTableIndex, int fetchSize, Query query) {
        super(name);
        this.lastTableIndex = lastTableIndex;
        this.fetchSize = fetchSize;
        this.query = query;
    }

    @Override
    protected void runInternal() {
        final QueryRunner runner = mapServiceContext.getMapQueryRunner(query.getMapName());
        response = runner.runPartitionScanQueryOnPartitionChunk(query, getPartitionId(), lastTableIndex, fetchSize);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        fetchSize = in.readInt();
        lastTableIndex = in.readInt();
        query = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(fetchSize);
        out.writeInt(lastTableIndex);
        out.writeObject(query);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.FETCH_WITH_QUERY;
    }
}
