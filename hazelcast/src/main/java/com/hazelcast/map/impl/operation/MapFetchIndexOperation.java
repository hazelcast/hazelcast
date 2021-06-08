/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


import static com.hazelcast.map.impl.MapDataSerializerHook.MAP_FETCH_INDEX;

/**
 *  Operation for fetching map entries by index. It will only return the
 *  entries belong the partitions defined in {@code partitionIdSet}. At
 *  the end of the operation, if the operation detects some of the partitions
 *  are migrated, it will drop the entries which belong the migrated partition.
 *  The migrated partitions ids will be returned as a part of the result
 *  to signal the caller.
 */
public class MapFetchIndexOperation extends MapOperation implements ReadonlyOperation {

    private String indexName;
    private IndexFilter indexFilter;
    private boolean descending;
    private PartitionIdSet partitionIdSet;

    private transient ExpressionEvalContext expressionEvalContext;
    private transient Object response;

    public MapFetchIndexOperation() { }

    public MapFetchIndexOperation(
            String name,
            String indexName,
            IndexFilter indexFilter,
            boolean descending,
            PartitionIdSet partitionIdSet
    ) {
        super(name);
        this.indexName = indexName;
        this.indexFilter = indexFilter;
        this.descending = descending;
        this.partitionIdSet = partitionIdSet;
    }

    @Override
    protected void runInternal() {
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Address currentAddress = getNodeEngine().getLocalMember().getAddress();

        InternalIndex index = indexes.getIndex(indexName);
        if (index == null) {
            throw new QueryException("Index name does not exist");
        }

        Iterator<QueryableEntry> entriesIterator = indexFilter.getEntries(index, descending, expressionEvalContext);

        List<QueryableEntry> entries = new ArrayList<>();

        // Materialize the values
        while (entriesIterator.hasNext()) {
            QueryableEntry entry = entriesIterator.next();
            int partitionId = partitionService.getPartitionId(entry.getKeyData());
            if (partitionIdSet.contains(partitionId)) {
                entries.add(entry);
            }

        }

        // After materialization, we will check whether any partitions are migrated.
        // If it is case, we need to prune migrated data.
        List<Integer> currentPartitions = partitionService.getMemberPartitions(currentAddress);
        PartitionIdSet currentPartitionIdSet = new PartitionIdSet(partitionCount, currentPartitions);

        Set<Integer> migratedPartitionIds =
                partitionIdSet
                        .stream()
                        .filter(id -> !currentPartitionIdSet.contains(id))
                        .collect(Collectors.toSet());

        if (!migratedPartitionIds.isEmpty()) {
            entries = entries.stream()
                    .filter(entry -> {
                        int partitionId = partitionService.getPartitionId(entry.getKeyData());
                        return migratedPartitionIds.contains(partitionId);
                    })
                    .collect(Collectors.toList());
        }

        response = new Object[] {entries, migratedPartitionIds};
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        indexName = in.readString();
        indexFilter = in.readObject();
        partitionIdSet = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(indexName);
        out.writeObject(indexFilter);
        out.writeObject(partitionIdSet);
    }

    @Override
    public int getClassId() {
        return MAP_FETCH_INDEX;
    }
}
