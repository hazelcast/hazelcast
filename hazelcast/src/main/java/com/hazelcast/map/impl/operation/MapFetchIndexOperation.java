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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.Comparison;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker.PartitionStamp;
import com.hazelcast.query.impl.IndexKeyEntries;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.OrderedIndexStore.DataComparator;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.properties.ClusterProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.map.impl.MapDataSerializerHook.MAP_FETCH_INDEX_OPERATION;

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
    private int sizeLimit;

    private transient MapFetchIndexOperationResult response;

    public MapFetchIndexOperation() { }

    public MapFetchIndexOperation(
            String mapName,
            String indexName,
            IndexIterationPointer[] pointers,
            PartitionIdSet partitionIdSet,
            int sizeLimit
    ) {
        super(mapName);
        this.indexName = indexName;
        this.partitionIdSet = partitionIdSet;
        this.pointers = pointers;
        this.sizeLimit = sizeLimit;
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

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity", "checkstyle:MethodLength"})
    private MapFetchIndexOperationResult runInternalSorted(InternalIndex index) {
        List<QueryableEntry<?, ?>> entries = new ArrayList<>(sizeLimit + 1);
        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
        DataComparator comparator = new DataComparator();

        for (int i = 0; i < pointers.length; i++) {
            IndexIterationPointer pointer = pointers[i];
            Data lastEntryKeyData = pointer.getLastEntryKeyData();

            Iterator<IndexKeyEntries> entryIterator = getEntryIterator(index, pointer);

            while (entryIterator.hasNext()) {
                IndexKeyEntries indexKeyEntries = entryIterator.next();
                @SuppressWarnings({"rawtypes"})
                Iterator<QueryableEntry> keyEntries = indexKeyEntries.getEntries();

                // Skip until the entry last read
                if (lastEntryKeyData != null) {
                    while (keyEntries.hasNext()) {
                        QueryableEntry<?, ?> entry = keyEntries.next();

                        int comparison = comparator.compare(entry.getKeyData(), lastEntryKeyData);
                        if (isDescendingEntryKey(pointer)) {
                            comparison = -comparison;
                        }

                        if (comparison == 0) {
                            break;
                        } else if (comparison > 0) {
                            entries.add(entry);
                            break;
                        }
                    }
                }

                // Read and add until size hint is reached or iterator ends
                while (keyEntries.hasNext()) {
                    if (entries.size() >= sizeLimit) {
                        break;
                    }
                    QueryableEntry<?, ?> entry = keyEntries.next();
                    if (partitionIdSet == null) {
                        entries.add(entry);
                        lastEntryKeyData = entry.getKeyData();
                    } else {
                        int partitionId = HashUtil.hashToIndex(entry.getKeyData().getPartitionHash(), partitionCount);
                        if (partitionIdSet.contains(partitionId)) {
                            lastEntryKeyData = entry.getKeyData();
                            entries.add(entry);
                        }
                    }
                }

                if (!keyEntries.hasNext()) {
                    lastEntryKeyData = null;
                }

                if (entries.size() >= sizeLimit) {
                    IndexIterationPointer[] newPointers;
                    if (entryIterator.hasNext() || lastEntryKeyData != null) {
                        Comparable<?> currentIndexKey = indexKeyEntries.getIndexKey();
                        newPointers = new IndexIterationPointer[pointers.length - i];
                        if (lastEntryKeyData != null) {
                            newPointers[0] = IndexIterationPointer.create(
                                    pointer.isDescending() ? pointer.getFrom() : currentIndexKey,
                                    pointer.isDescending() ? pointer.isFromInclusive() : true,
                                    pointer.isDescending() ? currentIndexKey : pointer.getTo(),
                                    pointer.isDescending() ? true : pointer.isToInclusive(),
                                    pointer.isDescending(),
                                    lastEntryKeyData
                            );
                        } else {
                            newPointers[0] = IndexIterationPointer.create(
                                    pointer.isDescending() ? pointer.getFrom() : currentIndexKey,
                                    pointer.isDescending() ? pointer.isFromInclusive() : false,
                                    pointer.isDescending() ? currentIndexKey : pointer.getTo(),
                                    pointer.isDescending() ? false : pointer.isToInclusive(),
                                    pointer.isDescending(),
                                    null
                            );
                        }


                        System.arraycopy(pointers, i + 1, newPointers, 1, newPointers.length - 1);
                    } else {
                        newPointers = new IndexIterationPointer[pointers.length - i - 1];
                        System.arraycopy(pointers, i + 1, newPointers, 0, newPointers.length);
                    }
                    return new MapFetchIndexOperationResult(entries, newPointers);
                }
            }
        }

        return new MapFetchIndexOperationResult(entries, new IndexIterationPointer[0]);
    }

    private static boolean isDescendingEntryKey(IndexIterationPointer pointer) {
        if (((Comparable) pointer.getFrom()).compareTo((Comparable) pointer.getTo()) == 0) {
            assert pointer.isFromInclusive() && pointer.isToInclusive()
                    : "Point lookup limits should be all inclusive";
            return false;
        } else {
            return pointer.isDescending();
        }
    }

    private static Iterator<IndexKeyEntries> getEntryIterator(InternalIndex index, IndexIterationPointer pointer) {
        Iterator<IndexKeyEntries> entryIterator;

        if (pointer.getFrom() != null) {
            if (pointer.getTo() != null) {
                if (((Comparable) pointer.getFrom()).compareTo(pointer.getTo()) == 0) {
                    assert pointer.isFromInclusive() && pointer.isToInclusive()
                            : "If range scan is a point lookup then limits should be all inclusive";
                    entryIterator = index.getSqlRecordIteratorBatch(pointer.getFrom());
                } else {
                    entryIterator = index.getSqlRecordIteratorBatch(
                            pointer.getFrom(),
                            pointer.isFromInclusive(),
                            pointer.getTo(),
                            pointer.isToInclusive(),
                            pointer.isDescending()
                    );
                }

            } else {
                entryIterator = index.getSqlRecordIteratorBatch(
                        pointer.isFromInclusive() ? Comparison.GREATER_OR_EQUAL : Comparison.GREATER,
                        pointer.getFrom(),
                        pointer.isDescending()
                );
            }
        } else {
            if (pointer.getTo() != null) {
                entryIterator = index.getSqlRecordIteratorBatch(
                        pointer.isToInclusive() ? Comparison.LESS_OR_EQUAL : Comparison.LESS,
                        pointer.getTo(),
                        pointer.isDescending()
                );
            } else {
                entryIterator = index.getSqlRecordIteratorBatch(pointer.isDescending());
            }
        }
        return entryIterator;
    }

    private MapFetchIndexOperationResult runInternalHash(InternalIndex index) {
        List<QueryableEntry<?, ?>> entries = new ArrayList<>();
        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();

        int pointerIndex;
        for (pointerIndex = 0; pointerIndex < pointers.length && entries.size() < sizeLimit; pointerIndex++) {
            IndexIterationPointer pointer = pointers[pointerIndex];

            // For hash lookups, pointer begin and end points must be the same
            assert pointer.getFrom() == pointer.getTo() : "Unordered index iteration pointer must have same from and to values";

            @SuppressWarnings({"rawtypes", "unchecked"})
            Collection<QueryableEntry<?, ?>> keyEntries = (Collection) index.getRecords(pointer.getFrom());
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
        }

        IndexIterationPointer[] newPointers = new IndexIterationPointer[pointers.length - pointerIndex];
        System.arraycopy(pointers, pointerIndex, newPointers, 0, newPointers.length);
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
        sizeLimit = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(indexName);
        out.writeObject(partitionIdSet);
        out.writeObject(pointers);
        out.writeInt(sizeLimit);
    }

    @Override
    public int getClassId() {
        return MAP_FETCH_INDEX_OPERATION;
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

        /**
         * Returns the new pointers to continue the iteration from. If it's an
         * empty array, iteration is done.
         */
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
