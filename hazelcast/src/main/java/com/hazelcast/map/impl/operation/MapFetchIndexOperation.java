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
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.IndexValueBatch;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;

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
    private PartitionIdSet partitionIdSet;
    private IndexIterationPointer[] pointers;
    private int fetchLimit;

    private transient MapFetchIndexOperationResult response;

    public MapFetchIndexOperation() { }

    public MapFetchIndexOperation(
            String name,
            String indexName,
            IndexFilter indexFilter,
            IndexIterationPointer[] pointers,
            PartitionIdSet partitionIdSet,
            int fetchLimit
    ) {
        super(name);
        this.indexName = indexName;
        this.indexFilter = indexFilter;
        this.partitionIdSet = partitionIdSet;
        this.pointers = pointers;
        this.fetchLimit = fetchLimit;
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

        Object[] result = null;
        switch (index.getConfig().getType()) {
            case HASH:
                result = runInternalHash(index, pointers, fetchLimit);
            case SORTED:
                result = runInternalSorted(index, pointers, fetchLimit);
            case BITMAP:
                throw new UnsupportedOperationException("BITMAP scan is not implemented");
        }

        List<QueryableEntry> entries = (List<QueryableEntry>) result[0];
        IndexIterationPointer[] newPointers = (IndexIterationPointer[]) result[1];

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

        response = new MapFetchIndexOperationResult(entries, migratedPartitionIds, newPointers);
    }

    private Object[] runInternalSorted(InternalIndex index, IndexIterationPointer[] pointers, int fetchLimit) {
        assert pointers.length == 1;
        IndexIterationPointer pointer = pointers[0];

        Iterator<IndexValueBatch> entryIterator = index.getSqlRecordIteratorBatch(
                pointer.getFrom(),
                pointer.isFromInclusive(),
                pointer.getTo(),
                pointer.isToInclusive(),
                pointer.isDescending()
        );

        List<QueryableEntry> entries = new ArrayList<>();
        int totalFetched = 0;
        Comparable lastValueRead = null;

        while (entryIterator.hasNext()) {
            IndexValueBatch indexValueBatch = entryIterator.next();
            lastValueRead = indexValueBatch.getValue();
            entries.addAll(indexValueBatch.getEntries());
            totalFetched += indexValueBatch.getEntries().size();

            if (totalFetched >= fetchLimit) {
                break;
            }
        }

        IndexIterationPointer newPointer;
        if (totalFetched < fetchLimit) {
            newPointer = null;
        } else {
            newPointer = new IndexIterationPointer(
                    pointer.isDescending() ? pointer.getFrom() : lastValueRead,
                    pointer.isDescending() ? pointer.isFromInclusive() : false,
                    pointer.isDescending() ? lastValueRead : pointer.getTo(),
                    pointer.isToInclusive() ? false : pointer.isToInclusive(),
                    pointer.isDescending()
            );
        }

        return new Object[] { entries, new IndexIterationPointer[] { newPointer } };
    }

    private Object[] runInternalHash(InternalIndex index, IndexIterationPointer[] pointers, int fetchLimit) {
        IndexIterationPointer[] newPointers = new IndexIterationPointer[pointers.length];
        List<QueryableEntry> entries = new ArrayList<>();
        boolean done = false;
        for (int i = 0; i < pointers.length; i++) {
            if (done) {
                newPointers[i] = pointers[i];
                continue;
            }

            IndexIterationPointer pointer = pointers[i];
            entries.addAll(index.getRecords(pointer.getFrom()));
            newPointers[i] = null;

            if (entries.size() >= fetchLimit) {
                done = true;
            }
        }

        return new Object[] { entries, newPointers };
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

    public static class MapFetchIndexOperationResult {
        private List<QueryableEntry> result;
        private Set<Integer> migratedPartitionIds;
        private IndexIterationPointer[] continuationPointers;

        public MapFetchIndexOperationResult(
                List<QueryableEntry> result,
                Set<Integer> migratedPartitionIds,
                IndexIterationPointer[] pointers
        ){
            this.result = result;
            this.migratedPartitionIds = migratedPartitionIds;
            this.continuationPointers = pointers;
        }

        public List<QueryableEntry> getResult() {
            return result;
        }

        public Set<Integer> getMigratedPartitionIds() {
            return migratedPartitionIds;
        }

        public IndexIterationPointer[] getContinuationPointers() {
            return continuationPointers;
        }
    }
}
