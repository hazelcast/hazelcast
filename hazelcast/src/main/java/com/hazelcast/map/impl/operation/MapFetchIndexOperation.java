/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.Comparison;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker.PartitionStamp;
import com.hazelcast.query.impl.IndexKeyEntries;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.OrderedIndexStore;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
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

    public MapFetchIndexOperation() {
    }

    public MapFetchIndexOperation(
            String mapName,
            String indexName,
            IndexIterationPointer[] pointers,
            PartitionIdSet partitionIdSet,
            int sizeLimit
    ) {
        super(mapName);
        checkPositive(sizeLimit, "" + sizeLimit);
        this.indexName = indexName;
        this.partitionIdSet = partitionIdSet;
        this.pointers = pointers;
        this.sizeLimit = sizeLimit;
    }

    @Override
    protected void runInternal() {
        Indexes indexes = mapContainer.getIndexes();
        if (indexes == null) {
            throw QueryException.error(SqlErrorCode.INDEX_INVALID, "Cannot use the index \"" + indexName
                    + "\" of the IMap \"" + name + "\" because it is not global "
                    + "(make sure the property \"" + ClusterProperty.GLOBAL_HD_INDEX_ENABLED
                    + "\" is set to \"true\")");
        }

        InternalIndex index = indexes.getIndex(indexName);
        if (index == null) {
            throw QueryException.error(SqlErrorCode.INDEX_INVALID, "Index \"" + indexName + "\" does not exist");
        }

        PartitionStamp indexStamp = index.getPartitionStamp();
        if (indexStamp == null) {
            throw new RetryableHazelcastException("Index is being rebuilt");
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
        List<QueryableEntry<?, ?>> entries = new ArrayList<>(sizeLimit);
        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();

        for (int i = 0; i < pointers.length; i++) {
            IndexIterationPointer pointer = pointers[i];
            Data lastEntryKeyData = pointer.getLastEntryKeyData();

            Comparator<Data> comparator = OrderedIndexStore.DATA_COMPARATOR;
            if (isDescendingEntryKey(pointer)) {
                comparator = comparator.reversed();
            }

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
                        if (comparison >= 0) {
                            if (comparison > 0 && isInPartitionSet(entry, partitionIdSet, partitionCount)) {
                                entries.add(entry);
                                lastEntryKeyData = entry.getKeyData();
                            }
                            break;
                        }
                    }
                }

                // Read and add until size limit is reached or iterator ends
                while (keyEntries.hasNext() && entries.size() < sizeLimit) {
                    QueryableEntry<?, ?> entry = keyEntries.next();
                    if (isInPartitionSet(entry, partitionIdSet, partitionCount)) {
                        entries.add(entry);
                        lastEntryKeyData = entry.getKeyData();
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
                                    !pointer.isDescending() || pointer.isFromInclusive(),
                                    pointer.isDescending() ? currentIndexKey : pointer.getTo(),
                                    pointer.isDescending() || pointer.isToInclusive(),
                                    pointer.isDescending(),
                                    lastEntryKeyData
                            );
                        } else {
                            newPointers[0] = IndexIterationPointer.create(
                                    pointer.isDescending() ? pointer.getFrom() : currentIndexKey,
                                    pointer.isDescending() && pointer.isFromInclusive(),
                                    pointer.isDescending() ? currentIndexKey : pointer.getTo(),
                                    !pointer.isDescending() && pointer.isToInclusive(),
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
        if (pointer.getFrom() != null && pointer.getTo() != null
                && ((Comparable) pointer.getFrom()).compareTo(pointer.getTo()) == 0) {
            assert pointer.isFromInclusive() && pointer.isToInclusive()
                    : "Point lookup limits must be all inclusive";
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
        List<QueryableEntry<?, ?>> entries = new ArrayList<>(sizeLimit);
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
                    if (isInPartitionSet(entry, partitionIdSet, partitionCount)) {
                        entries.add(entry);
                    }
                }
            }
        }

        IndexIterationPointer[] newPointers = new IndexIterationPointer[pointers.length - pointerIndex];
        System.arraycopy(pointers, pointerIndex, newPointers, 0, newPointers.length);
        return new MapFetchIndexOperationResult(entries, newPointers);
    }

    private static boolean isInPartitionSet(
            QueryableEntry entry,
            PartitionIdSet partitionIdSet,
            int partitionCount
    ) {
        if (partitionIdSet == null) {
            return true;
        }
        int partitionId = HashUtil.hashToIndex(entry.getKeyData().getPartitionHash(), partitionCount);
        return partitionIdSet.contains(partitionId);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    // Index scan via operation is thread-safe, no need to run from partition thread.
    @Override
    protected void assertNativeMapOnPartitionThread() {
    }

    @Override
    public void logError(Throwable e) {
        if (!(e instanceof MissingPartitionException)) {
            super.logError(e);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        indexName = in.readString();
        partitionIdSet = in.readObject();
        int pointersLength = in.readInt();
        pointers = new IndexIterationPointer[pointersLength];
        for (int i = 0; i < pointersLength; i++) {
            pointers[i] = in.readObject();
        }
        sizeLimit = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(indexName);
        out.writeObject(partitionIdSet);
        out.writeInt(pointers.length);
        for (IndexIterationPointer pointer : pointers) {
            out.writeObject(pointer);
        }
        out.writeInt(sizeLimit);
    }

    @Override
    public int getClassId() {
        return MAP_FETCH_INDEX_OPERATION;
    }

    public static final class MapFetchIndexOperationResult implements IdentifiedDataSerializable {
        private List<QueryableEntry<?, ?>> entries;
        private IndexIterationPointer[] pointers;

        public MapFetchIndexOperationResult() {
        }

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

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            SerializationUtil.writeList(entries, out);
            out.writeInt(pointers.length);
            for (IndexIterationPointer pointer : pointers) {
                out.writeObject(pointer);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            entries = SerializationUtil.readList(in);
            int len = in.readInt();
            pointers = new IndexIterationPointer[len];
            for (int i = 0; i < len; ++i) {
                pointers[i] = in.readObject();
            }
        }

        @Override
        public int getFactoryId() {
            return MapDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return MapDataSerializerHook.MAP_FETCH_INDEX_OPERATION_RESULT;
        }
    }

    public static final class MissingPartitionException extends HazelcastException {
        public MissingPartitionException(String message) {
            super(message);
        }

        public MissingPartitionException(String message, Throwable t) {
            super(message, t);
        }
    }

}
