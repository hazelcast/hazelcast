/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.monitor.impl.GlobalIndexesStats;
import com.hazelcast.internal.monitor.impl.IndexesStats;
import com.hazelcast.internal.monitor.impl.PartitionIndexesStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.StoreAdapter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.IndexAwarePredicate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains all indexes for a data-structure, e.g. an IMap.
 */
@SuppressWarnings("checkstyle:finalclass")
public class Indexes {

    /**
     * The partitions count check detects a race condition when a
     * query is executed on the index which is under (re)construction.
     * The negative value means the check should be skipped.
     */
    public static final int SKIP_PARTITIONS_COUNT_CHECK = -1;
    private static final InternalIndex[] EMPTY_INDEXES = {};

    private final boolean global;
    private final boolean usesCachedQueryableEntries;
    private final IndexesStats stats;
    private final Extractors extractors;
    private final IndexProvider indexProvider;
    private final IndexCopyBehavior indexCopyBehavior;
    private final QueryContextProvider queryContextProvider;
    private final InternalSerializationService serializationService;

    private final Map<String, InternalIndex> indexesByName = new ConcurrentHashMap<>(3);
    private final AttributeIndexRegistry attributeIndexRegistry = new AttributeIndexRegistry();
    private final AttributeIndexRegistry evaluateOnlyAttributeIndexRegistry = new AttributeIndexRegistry();
    private final ConverterCache converterCache = new ConverterCache(this);
    private final Map<String, IndexConfig> definitions = new ConcurrentHashMap<>();

    private volatile InternalIndex[] indexes = EMPTY_INDEXES;
    private volatile InternalIndex[] compositeIndexes = EMPTY_INDEXES;

    private Indexes(InternalSerializationService serializationService, IndexCopyBehavior indexCopyBehavior, Extractors extractors,
                    IndexProvider indexProvider, boolean usesCachedQueryableEntries, boolean statisticsEnabled, boolean global) {
        this.global = global;
        this.indexCopyBehavior = indexCopyBehavior;
        this.serializationService = serializationService;
        this.usesCachedQueryableEntries = usesCachedQueryableEntries;
        this.stats = createStats(global, statisticsEnabled);
        this.extractors = extractors == null ? Extractors.newBuilder(serializationService).build() : extractors;
        this.indexProvider = indexProvider == null ? new DefaultIndexProvider() : indexProvider;
        this.queryContextProvider = createQueryContextProvider(this, global, statisticsEnabled);
    }

    /**
     * Marks the given partition as indexed by the given indexes.
     *
     * @param partitionId the ID of the partition to mark as indexed.
     * @param indexes     the indexes by which the given partition is indexed.
     */
    public static void markPartitionAsIndexed(int partitionId, InternalIndex[] indexes) {
        for (InternalIndex index : indexes) {
            index.markPartitionAsIndexed(partitionId);
        }
    }

    /**
     * Marks the given partition as unindexed by the given indexes.
     *
     * @param partitionId the ID of the partition to mark as unindexed.
     * @param indexes     the indexes by which the given partition is unindexed.
     */
    public static void markPartitionAsUnindexed(int partitionId, InternalIndex[] indexes) {
        for (InternalIndex index : indexes) {
            index.markPartitionAsUnindexed(partitionId);
        }
    }

    /**
     * @param ss                the serializationService
     * @param indexCopyBehavior the indexCopyBehavior
     * @return new builder instance which will be used to create Indexes object.
     * @see IndexCopyBehavior
     */
    public static Builder newBuilder(SerializationService ss, IndexCopyBehavior indexCopyBehavior) {
        return new Builder(ss, indexCopyBehavior);
    }

    public synchronized InternalIndex addOrGetIndex(IndexConfig indexConfig, StoreAdapter partitionStoreAdapter) {
        String name = indexConfig.getName();

        assert name != null;
        assert !name.isEmpty();

        InternalIndex index = indexesByName.get(name);
        if (index != null) {
            return index;
        }

        index = indexProvider.createIndex(
                indexConfig,
                extractors,
                serializationService,
                indexCopyBehavior,
                stats.createPerIndexStats(indexConfig.getType() == IndexType.SORTED, usesCachedQueryableEntries),
                partitionStoreAdapter
        );

        indexesByName.put(name, index);
        if (index.isEvaluateOnly()) {
            evaluateOnlyAttributeIndexRegistry.register(index);
        } else {
            attributeIndexRegistry.register(index);
        }
        converterCache.invalidate(index);

        indexes = indexesByName.values().toArray(EMPTY_INDEXES);
        if (index.getComponents().length > 1) {
            InternalIndex[] oldCompositeIndexes = compositeIndexes;
            InternalIndex[] newCompositeIndexes = Arrays.copyOf(oldCompositeIndexes, oldCompositeIndexes.length + 1);
            newCompositeIndexes[oldCompositeIndexes.length] = index;
            compositeIndexes = newCompositeIndexes;
        }
        return index;
    }

    /**
     * Records the given index definition in this indexes without creating an
     * index.
     *
     * @param config Index configuration.
     */
    public void recordIndexDefinition(IndexConfig config) {
        String name = config.getName();

        assert name != null && !name.isEmpty();

        if (definitions.containsKey(name) || indexesByName.containsKey(name)) {
            return;
        }

        definitions.put(name, config);
    }

    /**
     * Creates indexes according to the index definitions stored inside this
     * indexes.
     */
    public void createIndexesFromRecordedDefinitions(StoreAdapter partitionStoreAdapter) {
        definitions.forEach((name, indexConfig) -> {
            addOrGetIndex(indexConfig, partitionStoreAdapter);
            definitions.compute(name, (k, v) -> {
                return indexConfig == v ? null : v;
            });
        });
    }

    /**
     * Returns all the indexes known to this indexes instance.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public InternalIndex[] getIndexes() {
        return indexes;
    }

    /**
     * Returns all the composite indexes known to this indexes instance.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public InternalIndex[] getCompositeIndexes() {
        return compositeIndexes;
    }

    public Collection<IndexConfig> getIndexDefinitions() {
        return definitions.values();
    }

    /**
     * Destroys and then removes all the indexes from this indexes instance.
     */
    public void destroyIndexes() {
        InternalIndex[] indexesSnapshot = getIndexes();

        indexes = EMPTY_INDEXES;
        compositeIndexes = EMPTY_INDEXES;
        indexesByName.clear();
        attributeIndexRegistry.clear();
        evaluateOnlyAttributeIndexRegistry.clear();
        converterCache.clear();

        for (InternalIndex index : indexesSnapshot) {
            index.destroy();
        }
    }

    /**
     * Clears contents of indexes managed by this instance.
     */
    public void clearAll() {
        InternalIndex[] indexesSnapshot = getIndexes();

        for (InternalIndex index : indexesSnapshot) {
            index.clear();
        }
    }

    /**
     * Returns {@code true} if this indexes instance contains at least one index,
     * {@code false} otherwise.
     */
    public boolean haveAtLeastOneIndex() {
        return indexes != EMPTY_INDEXES;
    }

    /**
     * Returns {@code true} if the indexes instance contains either at least one index or its definition,
     * {@code false} otherwise.
     *
     * @return
     */
    public boolean haveAtLeastOneIndexOrDefinition() {
        boolean haveAtLeastOneIndexOrDefinition = haveAtLeastOneIndex() || !definitions.isEmpty();
        // for local indexes assert that indexes and definitions are exclusive
        assert isGlobal() || !haveAtLeastOneIndexOrDefinition || !haveAtLeastOneIndex() || definitions.isEmpty();
        return haveAtLeastOneIndexOrDefinition;
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
    public void putEntry(QueryableEntry queryableEntry, Object oldValue, Index.OperationSource operationSource) {
        InternalIndex[] indexes = getIndexes();
        for (InternalIndex index : indexes) {
            index.putEntry(queryableEntry, oldValue, operationSource);
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
    public void removeEntry(Data key, Object value, Index.OperationSource operationSource) {
        InternalIndex[] indexes = getIndexes();
        for (InternalIndex index : indexes) {
            index.removeEntry(key, value, operationSource);
        }
    }

    /**
     * @return true if the index is global-per map, meaning there is just a
     * single instance of this object per map. Global indexes are used in
     * on-heap maps, since they give a significant performance boost. The
     * opposite of global indexes are partitioned-indexes which are stored
     * locally per partition.
     * <p>
     * In case of a partitioned-index, each query has to query the index in each
     * partition separately, which all-together may be around 3 times slower
     * than querying a single global index.
     */
    public boolean isGlobal() {
        return global;
    }

    /**
     * @return the index with the given name or {@code null} if such index does
     * not exist. It's a caller's responsibility to canonicalize the passed
     * index name as specified by {@link Index#getName()}.
     */
    public InternalIndex getIndex(String name) {
        return indexesByName.get(name);
    }

    /**
     * Performs a query on this indexes instance using the given predicate.
     *
     * @param predicate           the predicate to evaluate.
     * @param ownedPartitionCount a count of owned partitions a query runs on.
     *                            Negative value indicates that the value is not defined.
     * @return the produced result set or {@code null} if the query can't be
     * performed using the indexes known to this indexes instance.
     */
    @SuppressWarnings("unchecked")
    public Set<QueryableEntry> query(Predicate predicate, int ownedPartitionCount) {
        stats.incrementQueryCount();

        if (!haveAtLeastOneIndex() || !(predicate instanceof IndexAwarePredicate)) {
            return null;
        }

        IndexAwarePredicate indexAwarePredicate = (IndexAwarePredicate) predicate;
        QueryContext queryContext = queryContextProvider.obtainContextFor(this, ownedPartitionCount);
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
     * Matches an index for the given pattern and match hint.
     *
     * @param pattern             the pattern to match an index for. May be either an
     *                            attribute name or an exact index name.
     * @param matchHint           the match hint.
     * @param ownedPartitionCount a count of owned partitions a query runs on.
     *                            Negative value indicates that the value is not defined.
     * @return the matched index or {@code null} if nothing matched.
     * @see QueryContext.IndexMatchHint
     * @see QueryContext#matchIndex
     */
    public InternalIndex matchIndex(String pattern, QueryContext.IndexMatchHint matchHint, int ownedPartitionCount) {
        InternalIndex index;
        if (matchHint == QueryContext.IndexMatchHint.EXACT_NAME) {
            index = indexesByName.get(pattern);
        } else {
            index = attributeIndexRegistry.match(pattern, matchHint);
        }

        if (index == null || !index.allPartitionsIndexed(ownedPartitionCount)) {
            return null;
        }

        return index;
    }

    /**
     * Matches an index for the given pattern and match hint that can evaluate
     * the given predicate class.
     *
     * @param pattern             the pattern to match an index for. May be either an
     *                            attribute name or an exact index name.
     * @param predicateClass      the predicate class the matched index must be
     *                            able to evaluate.
     * @param matchHint           the match hint.
     * @param ownedPartitionCount a count of owned partitions a query runs on.
     *                            Negative value indicates that the value is not defined.
     * @return the matched index or {@code null} if nothing matched.
     * @see QueryContext.IndexMatchHint
     * @see Index#evaluate
     */
    public InternalIndex matchIndex(String pattern, Class<? extends Predicate> predicateClass,
                                    QueryContext.IndexMatchHint matchHint, int ownedPartitionCount) {
        InternalIndex index;
        if (matchHint == QueryContext.IndexMatchHint.EXACT_NAME) {
            index = indexesByName.get(pattern);
        } else {
            index = evaluateOnlyAttributeIndexRegistry.match(pattern, matchHint);
            if (index == null) {
                index = attributeIndexRegistry.match(pattern, matchHint);
            }
        }

        if (index == null) {
            return null;
        }

        if (!index.canEvaluate(predicateClass)) {
            return null;
        }

        if (!index.allPartitionsIndexed(ownedPartitionCount)) {
            return null;
        }

        return index;
    }

    /**
     * @return a converter instance for the given attribute or {@code null} if
     * a converter is not available. The later may happen if the attribute is
     * unknown to this indexes instance or there are no <em>populated</em>
     * indexes involving the given attribute.
     */
    public TypeConverter getConverter(String attribute) {
        return converterCache.get(attribute);
    }

    /**
     * Returns the indexes stats of this indexes instance.
     */
    public IndexesStats getIndexesStats() {
        return stats;
    }

    private static QueryContextProvider createQueryContextProvider(Indexes indexes, boolean global, boolean statisticsEnabled) {
        if (statisticsEnabled) {
            return global ? new GlobalQueryContextProviderWithStats() : new PartitionQueryContextProviderWithStats(indexes);
        } else {
            return global ? new GlobalQueryContextProvider() : new PartitionQueryContextProvider(indexes);
        }
    }

    private static IndexesStats createStats(boolean global, boolean statisticsEnabled) {
        if (statisticsEnabled) {
            return global ? new GlobalIndexesStats() : new PartitionIndexesStats();
        } else {
            return IndexesStats.EMPTY;
        }
    }

    /**
     * Builder which is used to create a new Indexes object.
     */
    public static final class Builder {

        private final IndexCopyBehavior indexCopyBehavior;
        private final InternalSerializationService serializationService;

        private boolean global = true;
        private boolean statsEnabled;
        private boolean usesCachedQueryableEntries;
        private Extractors extractors;
        private IndexProvider indexProvider;

        Builder(SerializationService ss, IndexCopyBehavior indexCopyBehavior) {
            this.serializationService = checkNotNull((InternalSerializationService) ss, "serializationService cannot be null");
            this.indexCopyBehavior = checkNotNull(indexCopyBehavior, "indexCopyBehavior cannot be null");
        }

        /**
         * @param global set {@code true} to create global indexes, otherwise set
         *               {@code false} to have partitioned indexes.
         *               Default value is true.
         * @return this builder instance
         */
        public Builder global(boolean global) {
            this.global = global;
            return this;
        }

        /**
         * @param indexProvider the index provider
         * @return this builder instance
         */
        public Builder indexProvider(IndexProvider indexProvider) {
            this.indexProvider = indexProvider;
            return this;
        }

        /**
         * @param extractors the extractors
         * @return this builder instance
         */
        public Builder extractors(Extractors extractors) {
            this.extractors = extractors;
            return this;
        }

        /**
         * @param usesCachedQueryableEntries set {@code true} if cached entries are
         *                                   used for queryable entries, otherwise set {@code false}
         * @return this builder instance
         */
        public Builder usesCachedQueryableEntries(boolean usesCachedQueryableEntries) {
            this.usesCachedQueryableEntries = usesCachedQueryableEntries;
            return this;
        }

        /**
         * @param statsEnabled set {@code true} if stats will be collected for the
         *                     indexes, otherwise set {@code false}
         * @return this builder instance
         */
        public Builder statsEnabled(boolean statsEnabled) {
            this.statsEnabled = statsEnabled;
            return this;
        }

        /**
         * @return a new instance of Indexes
         */
        public Indexes build() {
            return new Indexes(serializationService, indexCopyBehavior, extractors, indexProvider, usesCachedQueryableEntries,
                    statsEnabled, global);
        }

    }

}
