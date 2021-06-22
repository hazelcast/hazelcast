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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker.PartitionStamp;
import com.hazelcast.query.impl.IndexValueBatch;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.properties.ClusterProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.map.impl.MapDataSerializerHook.MAP_FETCH_INDEX;

/**
 * Operation for fetching map entries from an index. It will only return
 * the entries belonging to the partitions defined in {@code
 * partitionIdSet} and skip over the remaining partitions. If any
 * concurrent migration is detected while the operation executes or if any
 * of the requested partitions isn't indexed locally, it throws {@link
 * MissingPartitionException}.
 */
public class MapFetchIndexOperation extends MapOperation implements ReadonlyOperation {

    private String indexName;
    private PartitionIdSet partitionIdSet;
    private IndexIterationPointer[] pointers;
    private int sizeHint;

    private transient MapFetchIndexOperationResult response;

    public MapFetchIndexOperation() { }

    public MapFetchIndexOperation(
            String mapName,
            String indexName,
            IndexIterationPointer[] pointers,
            PartitionIdSet partitionIdSet,
            int sizeHint
    ) {
        super(mapName);
        this.indexName = indexName;
        this.partitionIdSet = partitionIdSet;
        this.pointers = pointers;
        this.sizeHint = sizeHint;
    }

    @Override
    protected void runInternal() {
        Indexes indexes = mapContainer.getIndexes();
        if (indexes == null) {
            throw new QueryException("Cannot use the index \"" + indexName
                    + "\" of the IMap \"" + name + "\" because it is not global "
                    + "(make sure the property \"" + ClusterProperty.GLOBAL_HD_INDEX_ENABLED
                    + "\" is set to \"true\")");
        }

        InternalIndex index = indexes.getIndex(indexName);
        if (index == null) {
            throw new QueryException("Index \"" + indexName + "\" does not exist");
        }

        PartitionStamp indexStamp = index.getPartitionStamp();
        if (indexStamp == null) {
            throw new MissingPartitionException("index is being rebuilt");
        }
        if (indexStamp.partitions.equals(partitionIdSet)) {
            // We clear the requested partitionIdSet, which means that we won't filter out any partitions.
            // This is an optimization for the case when there was no concurrent migration.
            partitionIdSet = null;
        } else {
            if (!indexStamp.partitions.containsAll(partitionIdSet)) {
                throw new MissingPartitionException("some requested partitions are not indexed");
            }
        }

        switch (index.getConfig().getType()) {
            case HASH:
                response = runInternalHash(index);
                break;
            case SORTED:
                response = runInternalSorted(index);
                break;
            case BITMAP:
                throw new UnsupportedOperationException("BITMAP index scan is not implemented");
            default:
                throw new UnsupportedOperationException(
                        "Unknown index type: \"" + index.getConfig().getType().name() + "\"");
        }

        if (!index.validatePartitionStamp(indexStamp.stamp)) {
            throw new MissingPartitionException("partition timestamp has changed");
        }
    }

    private MapFetchIndexOperationResult runInternalSorted(InternalIndex index) {
        List<QueryableEntry<?, ?>> entries = new ArrayList<>(sizeHint + 128);
        Comparable<?> lastValueRead = null;
        boolean sizeHintReached = false;
        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();

        IndexIterationPointer[] newPointers = new IndexIterationPointer[pointers.length];

        for (int i = 0; i < pointers.length; i++) {

            if (pointers[i].isDone() || sizeHintReached) {
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
                @SuppressWarnings({"unchecked", "rawtypes"})
                List<QueryableEntry<?, ?>> keyEntries = (List) indexValueBatch.getEntries();
                if (partitionIdSet == null) {
                    entries.addAll(keyEntries);
                } else {
                    for (QueryableEntry<?, ?> entry : keyEntries) {
                        int partitionId = HashUtil.hashToIndex(entry.getKeyData().getPartitionHash(), partitionCount);
                        if (partitionIdSet.contains(partitionId)) {
                            entries.add(entry);
                        }
                    }
                }

                if (entries.size() >= sizeHint) {
                    sizeHintReached = true;
                    break;
                }
            }

            if (!entryIterator.hasNext()) {
                newPointers[i] = IndexIterationPointer.createFinishedIterator();
            } else {
                newPointers[i] = new IndexIterationPointer(
                        pointer.isDescending() ? pointer.getFrom() : lastValueRead,
                        pointer.isDescending() ? pointer.isFromInclusive() : false,
                        pointer.isDescending() ? lastValueRead : pointer.getTo(),
                        pointer.isDescending() ? false : pointer.isToInclusive(),
                        pointer.isDescending());
            }
        }

        return new MapFetchIndexOperationResult(entries, newPointers);
    }

    private MapFetchIndexOperationResult runInternalHash(InternalIndex index) {
        IndexIterationPointer[] newPointers = new IndexIterationPointer[pointers.length];
        List<QueryableEntry<?, ?>> entries = new ArrayList<>();
        boolean sizeHintReached = false;
        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();

        for (int i = 0; i < pointers.length; i++) {
            if (sizeHintReached || pointers[i].isDone()) {
                newPointers[i] = pointers[i];
                continue;
            }

            IndexIterationPointer pointer = pointers[i];

            // For hash lookups, pointer begin and end points must be the same
            assert ((Comparable) pointer.getFrom()).compareTo(pointer.getTo()) == 0
                    : "Unordered index iteration pointer must have same from and to values";

            @SuppressWarnings({"rawtypes", "unchecked"})
            Set<QueryableEntry<?, ?>> keyEntries = (Set) index.getRecords(pointer.getFrom());
            if (partitionIdSet == null) {
                entries.addAll(keyEntries);
            } else {
                for (QueryableEntry<?, ?> entry : keyEntries) {
                    int partitionId = HashUtil.hashToIndex(entry.getKeyData().getPartitionHash(), partitionCount);
                    if (partitionIdSet.contains(partitionId)) {
                        entries.add(entry);
                    }
                }
            }

            if (entries.size() >= sizeHint) {
                sizeHintReached = true;
            }
        }

        return new MapFetchIndexOperationResult(entries, newPointers);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        indexName = in.readString();
        partitionIdSet = in.readObject();
        pointers = in.readObject();
        sizeHint = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(indexName);
        out.writeObject(partitionIdSet);
        out.writeObject(pointers);
        out.writeInt(sizeHint);
    }

    @Override
    public int getClassId() {
        return MAP_FETCH_INDEX;
    }

    public static final class MapFetchIndexOperationResult {
        private final List<QueryableEntry<?, ?>> entries;
        private final IndexIterationPointer[] pointers;

        public MapFetchIndexOperationResult(
                List<QueryableEntry<?, ?>> entries,
                IndexIterationPointer[] pointers
        ) {
            this.entries = entries;
            this.pointers = pointers;
        }

        public List<QueryableEntry<?, ?>> getEntries() {
            return entries;
        }

        public IndexIterationPointer[] getPointers() {
            return pointers;
        }
    }

    public static final class MissingPartitionException extends HazelcastException {
        public MissingPartitionException(String message) {
            super(message);
        }
    }
}
