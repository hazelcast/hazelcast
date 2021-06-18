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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.IndexValueBatch;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


import static com.hazelcast.map.impl.MapDataSerializerHook.MAP_FETCH_INDEX;

/**
 *  Operation for fetching map entries by index. It will only return the
 *  entries belong the partitions defined in {@code partitionIdSet}. At the
 *  beginning of the operations, if it detects some of {@code partitionIdSet}
 *  does not belong to the member, it will throw exception. At the end of the
 *  operation, if the operation detects some of the partitions are migrated,
 *  the exception will be thrown.
 */
public class MapFetchIndexOperation extends MapOperation implements ReadonlyOperation {

    private String indexName;
    private PartitionIdSet partitionIdSet;
    private IndexIterationPointer[] pointers;
    private int sizeHint;

    private transient MapFetchIndexOperationResult response;
    private transient IPartitionService partitionService;

    public MapFetchIndexOperation() { }

    public MapFetchIndexOperation(
            String name,
            String indexName,
            IndexIterationPointer[] pointers,
            PartitionIdSet partitionIdSet,
            int sizeHint
    ) {
        super(name);
        this.indexName = indexName;
        this.partitionIdSet = partitionIdSet;
        this.pointers = pointers;
        this.sizeHint = sizeHint;
    }

    @Override
    protected void runInternal() {
        Indexes indexes = mapContainer.getIndexes();
        if (indexes == null) {
            throw new IndexReadingException("Cannot use the index \"" + indexName
                    + "\" of the IMap \"" + name + "\" because it is not global "
                    + "(make sure the property \"" + ClusterProperty.GLOBAL_HD_INDEX_ENABLED
                    + "\" is set to \"true\")");
        }

        InternalIndex index = indexes.getIndex(indexName);
        if (index == null) {
            throw new IndexReadingException("Index name \"" + indexName + "\" does not exist");
        }
        if (!allIndexed(index, partitionIdSet)) {
            throw new IndexReadingException("Some of the partitions are not indexed in \"" + indexName + "\"");
        }

        int startMigrationTimestamp = getMigrationTimestamp();

        partitionService = getNodeEngine().getPartitionService();
        Address currentAddress = getNodeEngine().getLocalMember().getAddress();

        Set<Integer> ownedPartitions = new HashSet<>(partitionService.getMemberPartitions(currentAddress));
        // Some of partitions given as argument are not owned by the member, therefore throw exception
        if (partitionIdSet.stream().anyMatch(id -> !ownedPartitions.contains(id))) {
            throw new MissingPartitionException();
        }

        MapFetchIndexOperationResult result;
        switch (index.getConfig().getType()) {
            case HASH:
                result = runInternalHash(index, pointers, partitionIdSet, sizeHint);
                break;
            case SORTED:
                result = runInternalSorted(index, pointers, partitionIdSet, sizeHint);
                break;
            case BITMAP:
                throw new UnsupportedOperationException("BITMAP index scan is not implemented");
            default:
                throw new UnsupportedOperationException(
                        "Unknown index type: \"" + index.getConfig().getType().name() + "\"");
        }

        int endMigrationTimestamp = getMigrationTimestamp();

        // In case of migration, pessimistically throw exception
        if (endMigrationTimestamp != startMigrationTimestamp) {
            throw new MigrationDetectedException();
        }

        response = result;
    }

    private MapFetchIndexOperationResult runInternalSorted(
            InternalIndex index,
            IndexIterationPointer[] pointers,
            PartitionIdSet partitionIdSet,
            int sizeHint
    ) {
        List<QueryableEntry> entries = new ArrayList<>();
        Comparable lastValueRead = null;
        boolean sizeHintReached = false;

        IndexIterationPointer[] newPointers = new IndexIterationPointer[pointers.length];

        for (int i = 0; i < pointers.length; i++) {

            if (pointers[i].isDone()) {
                newPointers[i] = pointers[i];
                continue;
            }

            if (sizeHintReached) {
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

                List<QueryableEntry> filteredEntries = getOwnedEntries(indexValueBatch.getEntries(), partitionIdSet);
                entries.addAll(filteredEntries);

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

    private MapFetchIndexOperationResult runInternalHash(
            InternalIndex index,
            IndexIterationPointer[] pointers,
            PartitionIdSet partitionIdSet,
            int sizeHint
    ) {
        IndexIterationPointer[] newPointers = new IndexIterationPointer[pointers.length];
        List<QueryableEntry> entries = new ArrayList<>();
        boolean sizeHintReached = false;

        for (int i = 0; i < pointers.length; i++) {
            if (sizeHintReached || pointers[i].isDone()) {
                newPointers[i] = pointers[i];
                continue;
            }

            IndexIterationPointer pointer = pointers[i];

            // For hash lookups, pointer begin and end points will be the same
            assert checkPointerForLookup(pointer)
                    : "Unordered index iteration pointer must have same from and to values";

            List<QueryableEntry> filteredEntries = getOwnedEntries(index.getRecords(pointer.getFrom()), partitionIdSet);
            entries.addAll(filteredEntries);
            newPointers[i] = IndexIterationPointer.createFinishedIterator();

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

    private boolean allIndexed(InternalIndex index, PartitionIdSet partitionIdSet) {
        return partitionIdSet.stream().allMatch(index::hasPartitionIndexed);
    }

    protected int getMigrationTimestamp() {
        return mapService.getMigrationStamp();
    }

    /**
     *  Return only the {@code entries} from the partitions declared in {@code partitionIdSet}
     *
     * @param entries
     * @param partitionIdSet
     * @return list of entries belonging the given partitions
     */
    @Nonnull
    private List<QueryableEntry> getOwnedEntries(
            @Nonnull Collection<QueryableEntry> entries,
            @Nonnull PartitionIdSet partitionIdSet
    ) {
        return entries.stream()
                .filter(entry -> {
                    int partitionId = partitionService.getPartitionId(entry.getKeyData());
                    return partitionIdSet.contains(partitionId);
                })
                .collect(Collectors.toList());
    }

    private static boolean checkPointerForLookup(IndexIterationPointer pointer) {
        return ((Comparable) pointer.getFrom()).compareTo(pointer.getTo()) == 0;
    }

    public static class MapFetchIndexOperationResult {
        private final List<QueryableEntry> entries;
        private final IndexIterationPointer[] pointers;

        public MapFetchIndexOperationResult(
                List<QueryableEntry> entries,
                IndexIterationPointer[] pointers
        ) {
            this.entries = entries;
            this.pointers = pointers;
        }

        public List<QueryableEntry> getEntries() {
            return entries;
        }

        public IndexIterationPointer[] getPointers() {
            return pointers;
        }
    }

    public static final class MigrationDetectedException extends HazelcastException {
        public MigrationDetectedException() {
            super("Migration detected, operation is cancelled");
        }
    }

    public static final class MissingPartitionException extends HazelcastException {
        public MissingPartitionException() {
            super("Some partitions are missing");
        }
    }

    public static final class IndexReadingException extends HazelcastException {
        public IndexReadingException(String message) {
            super(message);
        }
    }
}
