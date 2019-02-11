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

package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.monitor.impl.GlobalIndexesStats;
import com.hazelcast.monitor.impl.IndexesStats;
import com.hazelcast.monitor.impl.PartitionIndexesStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.PredicateUtils;
import com.hazelcast.spi.serialization.SerializationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains all indexes for a data-structure, e.g. an IMap.
 */
@SuppressWarnings("checkstyle:finalclass")
public class Indexes {

    private static final InternalIndex[] EMPTY_INDEXES = {};

    private final boolean global;
    private final boolean usesCachedQueryableEntries;
    private final IndexesStats stats;
    private final Extractors extractors;
    private final IndexProvider indexProvider;
    private final IndexCopyBehavior indexCopyBehavior;
    private final QueryContextProvider queryContextProvider;
    private final InternalSerializationService serializationService;

    private final Map<String, InternalIndex> indexesByName = new ConcurrentHashMap<String, InternalIndex>(3);
    private final AttributeIndexRegistry attributeIndexRegistry = new AttributeIndexRegistry();
    private final ConverterCache converterCache = new ConverterCache(this);

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

    /**
     * Obtains the existing index or creates a new one (if an index doesn't exist
     * yet) for the given name in this indexes instance.
     *
     * @param name    the name of the index; the passed value might not
     *                represent a canonical index name (as specified by
     *                {@link Index#getName()), in this case the method
     *                canonicalizes it.
     * @param ordered {@code true} if the new index should be ordered, {@code
     *                false} otherwise.
     * @return the existing or created index.
     */
    public synchronized InternalIndex addOrGetIndex(String name, boolean ordered) {
        InternalIndex index = indexesByName.get(name);
        if (index != null) {
            return index;
        }

        String[] components = PredicateUtils.parseOutCompositeIndexComponents(name);
        if (components == null) {
            name = PredicateUtils.canonicalizeAttribute(name);
        } else {
            name = PredicateUtils.constructCanonicalCompositeIndexName(components);
        }

        index = indexesByName.get(name);
        if (index != null) {
            return index;
        }

        index = indexProvider.createIndex(name, components, ordered, extractors, serializationService, indexCopyBehavior,
                stats.createPerIndexStats(ordered, usesCachedQueryableEntries));

        indexesByName.put(name, index);
        attributeIndexRegistry.register(index);
        converterCache.invalidate(index);

        indexes = indexesByName.values().toArray(EMPTY_INDEXES);
        if (components != null) {
            InternalIndex[] oldCompositeIndexes = compositeIndexes;
            InternalIndex[] newCompositeIndexes = Arrays.copyOf(oldCompositeIndexes, oldCompositeIndexes.length + 1);
            newCompositeIndexes[oldCompositeIndexes.length] = index;
            compositeIndexes = newCompositeIndexes;
        }
        return index;
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

    /**
     * Destroys and then removes all the indexes from this indexes instance.
     */
    public void destroyIndexes() {
        InternalIndex[] indexesSnapshot = getIndexes();

        indexes = EMPTY_INDEXES;
        compositeIndexes = EMPTY_INDEXES;
        indexesByName.clear();
        attributeIndexRegistry.clear();
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
     * @param predicate the predicate to evaluate.
     * @return the produced result set or {@code null} if the query can't be
     * performed using the indexes known to this indexes instance.
     */
    @SuppressWarnings("unchecked")
    public Set<QueryableEntry> query(Predicate predicate) {
        stats.incrementQueryCount();

        if (!haveAtLeastOneIndex() || !(predicate instanceof IndexAwarePredicate)) {
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
     * Matches an index for the given pattern and match hint.
     *
     * @param pattern   the pattern to match an index for. May be either an
     *                  attribute name or an exact index name.
     * @param matchHint the match hint.
     * @return the matched index or {@code null} if nothing matched.
     * @see QueryContext.IndexMatchHint
     * @see Indexes#matchIndex
     */
    public InternalIndex matchIndex(String pattern, QueryContext.IndexMatchHint matchHint) {
        if (matchHint == QueryContext.IndexMatchHint.EXACT_NAME) {
            return indexesByName.get(pattern);
        } else {
            return attributeIndexRegistry.match(pattern, matchHint);
        }
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
