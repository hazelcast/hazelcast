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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.map.IMap;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.StoreAdapter;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.RecordStoreAdapter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Metadata;
import com.hazelcast.query.impl.QueryableEntriesSegment;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.Clock;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.util.SortingUtil.compareAnchor;

/**
 * Responsible for running a full-partition scan for a single partition in the calling thread.
 */
public class PartitionScanRunner {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final InternalSerializationService serializationService;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    protected final ClusterService clusterService;

    public PartitionScanRunner(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
    }

    @SuppressWarnings("unchecked")
    public void run(String mapName, Predicate predicate, int partitionId, Result result) {
        PagingPredicateImpl pagingPredicate = predicate instanceof PagingPredicateImpl ? (PagingPredicateImpl) predicate : null;

        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        RecordStore recordStore = partitionContainer.getRecordStore(mapName);
        Iterator<Record> iterator = recordStore.loadAwareIterator(getNow(), false);
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry =
                pagingPredicate == null ? null : pagingPredicate.getNearestAnchorEntry();
        boolean useCachedValues = isUseCachedDeserializedValuesEnabled(mapContainer, partitionId);
        Extractors extractors = mapServiceContext.getExtractors(mapName);
        LazyMapEntry queryEntry = new LazyMapEntry();
        StoreAdapter storeAdapter = new RecordStoreAdapter(recordStore);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = (Data) toData(record.getKey());
            Metadata metadata = getMetadataFromRecord(recordStore, record);
            Object value = toData(
                    useCachedValues ? Records.getValueOrCachedValue(record, serializationService) : record.getValue());
            if (value == null) {
                continue;
            }

            queryEntry.init(serializationService, key, value, extractors);
            queryEntry.setMetadata(metadata);
            queryEntry.setRecord(record);
            queryEntry.setStoreAdapter(storeAdapter);
            boolean valid = predicate.apply(queryEntry);
            if (valid && compareAnchor(pagingPredicate, queryEntry, nearestAnchorEntry)) {
                result.add(queryEntry);

                // We can't reuse the existing entry after it was added to the
                // result. Allocate the new one.
                queryEntry = new LazyMapEntry();
            }
        }
        result.orderAndLimit(pagingPredicate, nearestAnchorEntry);
    }

    // overridden in ee
    protected Metadata getMetadataFromRecord(RecordStore recordStore, Record record) {
        return record.getMetadata();
    }

    /**
     * Executes the predicate on a partition chunk. The offset in the partition is defined by the {@code tableIndex}
     * and the soft limit is defined by the {@code fetchSize}. The method returns the matched entries and an
     * index from which new entries can be fetched which allows for efficient iteration of query results.
     * <p>
     * <b>NOTE</b>
     * Iterating the map should be done only when the {@link IMap} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
     *
     * @param mapName     the map name
     * @param predicate   the predicate which the entries must match
     * @param partitionId the partition which is queried
     * @param tableIndex  the index from which entries are queried
     * @param fetchSize   the soft limit for the number of entries to fetch
     * @return entries matching the predicate and a table index from which new entries can be fetched
     */
    public QueryableEntriesSegment run(String mapName, Predicate predicate, int partitionId, int tableIndex, int fetchSize) {
        int lastIndex = tableIndex;
        final List<QueryableEntry> resultList = new LinkedList<>();
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        final RecordStore recordStore = partitionContainer.getRecordStore(mapName);
        final Extractors extractors = mapServiceContext.getExtractors(mapName);

        while (resultList.size() < fetchSize && lastIndex >= 0) {
            final MapEntriesWithCursor cursor = recordStore.fetchEntries(lastIndex, fetchSize - resultList.size());
            lastIndex = cursor.getNextTableIndexToReadFrom();
            final Collection<? extends Entry<Data, Data>> entries = cursor.getBatch();
            if (entries.isEmpty()) {
                break;
            }
            for (Entry<Data, Data> entry : entries) {
                QueryableEntry queryEntry = new LazyMapEntry(entry.getKey(), entry.getValue(), serializationService, extractors);
                if (predicate.apply(queryEntry)) {
                    resultList.add(queryEntry);
                }
            }
        }
        return new QueryableEntriesSegment(resultList, lastIndex);
    }

    protected boolean isUseCachedDeserializedValuesEnabled(MapContainer mapContainer, int partitionId) {
        CacheDeserializedValues cacheDeserializedValues = mapContainer.getMapConfig().getCacheDeserializedValues();
        switch (cacheDeserializedValues) {
            case NEVER:
                return false;
            case ALWAYS:
                return true;
            default:
                //if index exists then cached value is already set -> let's use it
                return mapContainer.getIndexes(partitionId).haveAtLeastOneIndex();
        }
    }

    protected <T> Object toData(T input) {
        return input;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }
}
