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

        MapFetchIndexOperationResultInternal result;
        switch (index.getConfig().getType()) {
            case HASH:
                result = runInternalHash(index, pointers, fetchLimit);
                break;
            case SORTED:
                result = runInternalSorted(index, pointers, fetchLimit);
                break;
            case BITMAP:
                throw new UnsupportedOperationException("BITMAP scan is not implemented");
            default:
                throw new UnsupportedOperationException("Unknown index type:" + index.getConfig().getType().name());
        }

        List<QueryableEntry> entries = result.getResult();
        IndexIterationPointer[] newPointers = result.getPointers();

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

    private MapFetchIndexOperationResultInternal runInternalSorted(
            InternalIndex index,
            IndexIterationPointer[] pointers,
            int fetchLimit
    ) {
        List<QueryableEntry> entries = new ArrayList<>();
        int totalFetched = 0;
        Comparable lastValueRead = null;
        boolean done = false;

        IndexIterationPointer[] newPointers = new IndexIterationPointer[pointers.length];

        for (int i = 0; i < pointers.length; i++) {
            if (done) {
                newPointers[i] = pointers[i];
                continue;
            }

            IndexIterationPointer pointer = pointers[i];

            Iterator<IndexValueBatch> entryIterator = index.getSqlRecordIteratorBatch(
                    pointer.getFrom(),
                    pointer.isFromInclusive(),
                    pointer.getTo(),
                    pointer.isToInclusive(),
                    pointer.isDescending()
            );

            while (entryIterator.hasNext()) {
                IndexValueBatch indexValueBatch = entryIterator.next();
                lastValueRead = indexValueBatch.getValue();
                entries.addAll(indexValueBatch.getEntries());
                totalFetched += indexValueBatch.getEntries().size();

                if (totalFetched >= fetchLimit) {
                    done = true;
                    break;
                }
            }

            newPointers[i] = (totalFetched < fetchLimit)
                    ? null
                    : new IndexIterationPointer(
                    pointer.isDescending() ? pointer.getFrom() : lastValueRead,
                    pointer.isDescending() ? pointer.isFromInclusive() : false,
                    pointer.isDescending() ? lastValueRead : pointer.getTo(),
                    pointer.isToInclusive() ? false : pointer.isToInclusive(),
                    pointer.isDescending());

        }

        return new MapFetchIndexOperationResultInternal(entries, newPointers);
    }

    private MapFetchIndexOperationResultInternal runInternalHash(
            InternalIndex index,
            IndexIterationPointer[] pointers,
            int fetchLimit
    ) {
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

        return new MapFetchIndexOperationResultInternal(entries, newPointers);
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
        private final Set<Integer> migratedPartitionIds;
        private final MapFetchIndexOperationResultInternal inner;

        public MapFetchIndexOperationResult(
                List<QueryableEntry> result,
                Set<Integer> migratedPartitionIds,
                IndexIterationPointer[] pointers
        ) {
            this.migratedPartitionIds = migratedPartitionIds;
            this.inner = new MapFetchIndexOperationResultInternal(result, pointers);
        }

        public List<QueryableEntry> getResult() {
            return inner.getResult();
        }

        public Set<Integer> getMigratedPartitionIds() {
            return migratedPartitionIds;
        }

        public IndexIterationPointer[] getContinuationPointers() {
            return inner.getPointers();
        }
    }

    private static final class MapFetchIndexOperationResultInternal {
        private final List<QueryableEntry> result;
        private final IndexIterationPointer[] pointers;

        private MapFetchIndexOperationResultInternal(
                List<QueryableEntry> result,
                IndexIterationPointer[] pointers
        ) {
            this.result = result;
            this.pointers = pointers;
        }

        private List<QueryableEntry> getResult() {
            return result;
        }

        private IndexIterationPointer[] getPointers() {
            return pointers;
        }
    }
}
