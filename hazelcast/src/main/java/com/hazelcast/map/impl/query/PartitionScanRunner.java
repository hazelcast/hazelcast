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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntriesSegment;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.SortingUtil.compareAnchor;
import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.record.Records.getValueOrCachedValue;

/**
 * Responsible for running a full-partition scan for a single partition in the calling thread.
 */
public class PartitionScanRunner {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final InternalSerializationService ss;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    protected final ClusterService clusterService;

    public PartitionScanRunner(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
    }

    @SuppressWarnings("unchecked")
    public void run(String mapName, Predicate predicate, int partitionId, Result result) {
        PagingPredicateImpl pagingPredicate = predicate instanceof PagingPredicateImpl
                ? (PagingPredicateImpl) predicate : null;

        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore<Record> recordStore = partitionContainer.getRecordStore(mapName);
        MapContainer mapContainer = recordStore.getMapContainer();
        boolean nativeMemory = recordStore.getInMemoryFormat() == InMemoryFormat.NATIVE;
        boolean useCachedValues = isUseCachedDeserializedValuesEnabled(mapContainer, partitionId);
        Extractors extractors = mapServiceContext.getExtractors(mapName);
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry =
                pagingPredicate == null ? null : pagingPredicate.getNearestAnchorEntry();

        recordStore.forEachAfterLoad(new BiConsumer<Data, Record>() {
            LazyMapEntry queryEntry = new LazyMapEntry();

            @Override
            public void accept(Data key, Record record) {
                Object value = useCachedValues ? getValueOrCachedValue(record, ss) : record.getValue();
                // TODO how can a value be null?
                if (value == null) {
                    return;
                }

                queryEntry.init(ss, key, value, extractors);
                queryEntry.setRecord(record);
                queryEntry.setMetadata(recordStore.getOrCreateMetadataStore().get(key));

                if (predicate.apply(queryEntry)
                        && compareAnchor(pagingPredicate, queryEntry, nearestAnchorEntry)) {

                    // always copy key&value to heap if map is backed by native memory
                    value = nativeMemory ? toHeapData((Data) value) : value;
                    result.add(queryEntry.init(ss, toHeapData(key), value, extractors));

                    // We can't reuse the existing entry after it was added to the
                    // result. Allocate the new one.
                    queryEntry = new LazyMapEntry();
                }
            }
        }, false);
        result.orderAndLimit(pagingPredicate, nearestAnchorEntry);
    }

    /**
     * Executes the predicate on a partition chunk. The offset in the partition
     * is defined by the {@code pointers} and the soft limit is defined by the
     * {@code fetchSize}. The method returns the matched entries and updated
     * pointers from which new entries can be fetched which allows for efficient
     * iteration of query results.
     * <p>
     * <b>NOTE</b>
     * The iteration may be done when the map is being mutated or when there are
     * membership changes. The iterator does not reflect the state when it has
     * been constructed - it may return some entries that were added after the
     * iteration has started and may not return some entries that were removed
     * after iteration has started.
     * The iterator will not, however, skip an entry if it has not been changed
     * and will not return an entry twice.
     *
     * @param mapName     the map name
     * @param predicate   the predicate which the entries must match
     * @param partitionId the partition which is queried
     * @param pointers    the pointers defining the state of iteration
     * @param fetchSize   the soft limit for the number of entries to fetch
     * @return entries matching the predicate and a table index from which new
     * entries can be fetched
     */
    public QueryableEntriesSegment run(String mapName, Predicate predicate, int partitionId,
                                       IterationPointer[] pointers, int fetchSize) {
        List<QueryableEntry> resultList = new LinkedList<>();
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore recordStore = partitionContainer.getRecordStore(mapName);
        Extractors extractors = mapServiceContext.getExtractors(mapName);

        while (resultList.size() < fetchSize && pointers[pointers.length - 1].getIndex() >= 0) {
            MapEntriesWithCursor cursor = recordStore.fetchEntries(pointers, fetchSize - resultList.size());
            pointers = cursor.getIterationPointers();
            Collection<? extends Entry<Data, Data>> entries = cursor.getBatch();
            if (entries.isEmpty()) {
                break;
            }
            for (Entry<Data, Data> entry : entries) {
                QueryableEntry queryEntry = new LazyMapEntry(entry.getKey(), entry.getValue(), ss, extractors);
                if (predicate.apply(queryEntry)) {
                    resultList.add(queryEntry);
                }
            }
        }
        return new QueryableEntriesSegment(resultList, pointers);
    }

    protected boolean isUseCachedDeserializedValuesEnabled(MapContainer mapContainer, int partitionId) {
        return mapContainer.isUseCachedDeserializedValuesEnabled(partitionId);
    }
}
