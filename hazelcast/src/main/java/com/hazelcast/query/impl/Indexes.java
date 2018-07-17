/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.DefaultIndexProvider;
import com.hazelcast.map.impl.query.IndexProvider;
import com.hazelcast.monitor.impl.GlobalIndexesStats;
import com.hazelcast.monitor.impl.InternalIndexesStats;
import com.hazelcast.monitor.impl.PartitionIndexesStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.NodeEngine;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Contains all indexes for a data-structure, e.g. an IMap.
 */
@SuppressWarnings("checkstyle:finalclass")
public class Indexes {
    private static final InternalIndex[] EMPTY_INDEX = {};

    private final InternalSerializationService serializationService;
    private final IndexProvider indexProvider;
    private final Extractors extractors;
    private final boolean global;
    private final IndexCopyBehavior copyBehavior;
    private final boolean queryableEntriesAreCached;
    private final QueryContextProvider queryContextProvider;
    private final InternalIndexesStats stats;

    private final ConcurrentMap<String, InternalIndex> mapIndexes = new ConcurrentHashMap<String, InternalIndex>(3);
    private final AtomicReference<InternalIndex[]> indexes = new AtomicReference<InternalIndex[]>(EMPTY_INDEX);

    private volatile boolean hasIndex;

    private Indexes(MapContainer mapContainer, boolean global) {
        MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        MapConfig mapConfig = mapContainer.getMapConfig();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();

        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.indexProvider = mapServiceContext.getIndexProvider(mapConfig);
        this.extractors = mapContainer.getExtractors();
        this.global = global;
        this.copyBehavior = mapServiceContext.getIndexCopyBehavior();
        this.queryableEntriesAreCached = mapConfig.getCacheDeserializedValues() != CacheDeserializedValues.NEVER;
        this.queryContextProvider = createQueryContextProvider(mapConfig, global);
        this.stats = createStats(mapConfig, global);
    }

    private Indexes(InternalSerializationService serializationService, IndexCopyBehavior copyBehavior) {
        this.serializationService = serializationService;
        this.indexProvider = new DefaultIndexProvider();
        this.extractors = Extractors.empty();
        this.global = true;
        this.copyBehavior = copyBehavior;
        this.queryableEntriesAreCached = false;
        this.queryContextProvider = new GlobalQueryContextProvider();
        this.stats = InternalIndexesStats.EMPTY;
    }

    /**
     * Creates a new indexes instance for use as a global indexes for the given
     * map container.
     *
     * @param mapContainer the container of the map.
     * @return the constructed indexes.
     */
    public static Indexes createGlobalIndexes(MapContainer mapContainer) {
        return new Indexes(mapContainer, true);
    }

    /**
     * Creates a new indexes instance for use as a partition indexes for the
     * given map container.
     *
     * @param mapContainer the container of the map.
     * @return the constructed indexes.
     */
    public static Indexes createPartitionIndexes(MapContainer mapContainer) {
        return new Indexes(mapContainer, false);
    }

    /**
     * Creates a new stand-alone (not attached to any map container) indexes
     * instance for the given serialization service and with the given index
     * copy behaviour.
     *
     * @param serializationService the serialization service to be used by the
     *                             new indexes.
     * @param copyBehavior         the desired copy behaviour of the new indexes.
     * @return the constructed indexes.
     */
    public static Indexes createStandaloneIndexes(InternalSerializationService serializationService,
                                                  IndexCopyBehavior copyBehavior) {
        return new Indexes(serializationService, copyBehavior);
    }

    /**
     * Obtains the existing index or creates a new one (if an index doesn't exist
     * yet) for the given attribute in this indexes instance.
     *
     * @param attribute the attribute to index.
     * @param ordered   {@code true} if the new index should be ordered, {@code
     *                  false} otherwise.
     * @return the existing or created index.
     */
    public synchronized InternalIndex addOrGetIndex(String attribute, boolean ordered) {
        InternalIndex index = mapIndexes.get(attribute);
        if (index != null) {
            return index;
        }

        index = indexProvider.createIndex(attribute, ordered, extractors, serializationService, copyBehavior,
                stats.createIndexStats(ordered, queryableEntriesAreCached));
        mapIndexes.put(attribute, index);
        indexes.set(mapIndexes.values().toArray(EMPTY_INDEX));
        hasIndex = true;
        return index;
    }

    /**
     * Returns all the indexes known to this indexes instance.
     */
    public InternalIndex[] getIndexes() {
        return indexes.get();
    }

    /**
     * Destroys and then removes all the indexes from this indexes instance.
     */
    public void destroyIndexes() {
        for (InternalIndex index : getIndexes()) {
            index.destroy();
        }

        indexes.set(EMPTY_INDEX);
        mapIndexes.clear();
        hasIndex = false;
    }

    /**
     * Clears contents of indexes managed by this instance.
     */
    public void clearAll() {
        for (InternalIndex index : getIndexes()) {
            index.clear();
        }
    }

    /**
     * Returns {@code true} if this indexes instance contains at least one index,
     * {@code false} otherwise.
     */
    public boolean hasIndex() {
        return hasIndex;
    }

    /**
     * Inserts a new queryable entry into this indexes instance or updates the
     * existing one.
     *
     * @param queryableEntry  the queryable entry to insert or update.
     * @param oldValue        the old entry value to update, {@code null} if
     *                        inserting the new entry.
     * @param operationSource the operation source.
     */
    public void saveEntryIndex(QueryableEntry queryableEntry, Object oldValue, Index.OperationSource operationSource) {
        InternalIndex[] indexes = getIndexes();
        for (InternalIndex index : indexes) {
            index.saveEntryIndex(queryableEntry, oldValue, operationSource);
        }
    }

    /**
     * Removes the entry from this indexes instance identified by the given key
     * and value.
     *
     * @param key             the key if the entry to remove.
     * @param value           the value of the entry to remove.
     * @param operationSource the operation source.
     */
    public void removeEntryIndex(Data key, Object value, Index.OperationSource operationSource) {
        InternalIndex[] indexes = getIndexes();
        for (InternalIndex index : indexes) {
            index.removeEntryIndex(key, value, operationSource);
        }
    }

    /**
     * @return true if the index is global-per map, meaning there is just a single instance of this object per map.
     * Global indexes are used in on-heap maps, since they give a significant performance boost.
     * The opposite of global indexes are partitioned-indexes which are stored locally per partition.
     * In case of a partitioned-index, each query has to query the index in each partition separately, which all-together
     * may be around 3 times slower than querying a single global index.
     */
    public boolean isGlobal() {
        return global;
    }

    /**
     * Get index for a given attribute. If the index does not exist then returns null.
     *
     * @param attribute the attribute name to get the index of.
     * @return Index for attribute or null if the index does not exist.
     */
    public InternalIndex getIndex(String attribute) {
        return mapIndexes.get(attribute);
    }

    /**
     * Performs a query on this indexes instance using the given predicate.
     *
     * @param predicate the predicate to evaluate.
     * @return the produced result set or {@code null} if the query can't be
     * performed using the indexes known to this indexes instance.
     */
    @SuppressWarnings("unchecked")
    public Set<QueryableEntry> query(Predicate predicate) {
        stats.incrementQueryCount();

        if (!hasIndex || !(predicate instanceof IndexAwarePredicate)) {
            return null;
        }

        IndexAwarePredicate indexAwarePredicate = (IndexAwarePredicate) predicate;
        QueryContext queryContext = queryContextProvider.obtainContextFor(this);
        if (!indexAwarePredicate.isIndexed(queryContext)) {
            return null;
        }

        Set<QueryableEntry> result = indexAwarePredicate.filter(queryContext);
        if (result != null) {
            stats.incrementIndexedQueryCount();
            queryContext.applyPerQueryStats();
        }

        return result;
    }

    /**
     * Returns the indexes stats of this indexes instance.
     */
    public InternalIndexesStats getIndexesStats() {
        return stats;
    }

    private QueryContextProvider createQueryContextProvider(MapConfig mapConfig, boolean global) {
        if (mapConfig.isStatisticsEnabled()) {
            return global ? new GlobalQueryContextProviderWithStats() : new PartitionQueryContextProviderWithStats(this);
        } else {
            return global ? new GlobalQueryContextProvider() : new PartitionQueryContextProvider(this);
        }
    }

    private InternalIndexesStats createStats(MapConfig mapConfig, boolean global) {
        if (mapConfig.isStatisticsEnabled()) {
            return global ? new GlobalIndexesStats() : new PartitionIndexesStats();
        } else {
            return InternalIndexesStats.EMPTY;
        }
    }

}
