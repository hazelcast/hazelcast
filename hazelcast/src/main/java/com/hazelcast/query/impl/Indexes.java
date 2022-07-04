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

package com.hazelcast.query.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.monitor.impl.GlobalIndexesStats;
import com.hazelcast.internal.monitor.impl.HDGlobalIndexesStats;
import com.hazelcast.internal.monitor.impl.IndexesStats;
import com.hazelcast.internal.monitor.impl.PartitionIndexesStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterableUtil;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.IndexAwarePredicate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains all indexes for a data-structure, e.g. an IMap.
 */
@SuppressWarnings({"checkstyle:finalclass", "rawtypes"})
public class Indexes {

    /**
     * The partitions count check detects a race condition when a
     * query is executed on the index which is under (re)construction.
     * The negative value means the check should be skipped.
     */
    public static final int SKIP_PARTITIONS_COUNT_CHECK = -1;
    private static final InternalIndex[] EMPTY_INDEXES = {};

    private static final ThreadLocal<CachedQueryEntry[]> CACHED_ENTRIES =
            ThreadLocal.withInitial(() -> new CachedQueryEntry[]{new CachedQueryEntry(), new CachedQueryEntry()});

    private final boolean global;
    private final boolean usesCachedQueryableEntries;
    private final IndexesStats stats;
    private final Extractors extractors;
    private final IndexProvider indexProvider;
    private final IndexCopyBehavior indexCopyBehavior;
    private final Supplier<java.util.function.Predicate<QueryableEntry>> resultFilterFactory;
    private final QueryContextProvider queryContextProvider;
    private final InternalSerializationService ss;

    private final Map<String, InternalIndex> indexesByName = new ConcurrentHashMap<>(3);
    private final AttributeIndexRegistry attributeIndexRegistry = new AttributeIndexRegistry();
    private final AttributeIndexRegistry evaluateOnlyAttributeIndexRegistry = new AttributeIndexRegistry();
    private final ConverterCache converterCache = new ConverterCache(this);
    private final Map<String, IndexConfig> definitions = new ConcurrentHashMap<>();

    private final int partitionCount;

    private volatile InternalIndex[] indexes = EMPTY_INDEXES;
    private volatile InternalIndex[] compositeIndexes = EMPTY_INDEXES;

    private Indexes(InternalSerializationService ss,
                    IndexCopyBehavior indexCopyBehavior,
                    Extractors extractors,
                    IndexProvider indexProvider,
                    boolean usesCachedQueryableEntries,
                    boolean statisticsEnabled,
                    boolean global,
                    InMemoryFormat inMemoryFormat,
                    int partitionCount,
                    Supplier<java.util.function.Predicate<QueryableEntry>> resultFilterFactory) {
        this.global = global;
        this.indexCopyBehavior = indexCopyBehavior;
        this.ss = ss;
        this.usesCachedQueryableEntries = usesCachedQueryableEntries;
        this.stats = createStats(global, inMemoryFormat, statisticsEnabled);
        this.extractors = extractors == null ? Extractors.newBuilder(ss).build() : extractors;
        this.indexProvider = indexProvider == null ? new DefaultIndexProvider() : indexProvider;
        this.queryContextProvider = createQueryContextProvider(this, global, statisticsEnabled);
        this.partitionCount = partitionCount;
        this.resultFilterFactory = resultFilterFactory;
    }

    public static void beginPartitionUpdate(InternalIndex[] indexes) {
        for (InternalIndex index : indexes) {
            index.beginPartitionUpdate();
        }
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
    public static Builder newBuilder(SerializationService ss, IndexCopyBehavior indexCopyBehavior,
                                     InMemoryFormat inMemoryFormat) {
        return new Builder(ss, indexCopyBehavior, inMemoryFormat);
    }

    public synchronized InternalIndex addOrGetIndex(IndexConfig indexConfig) {
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
                ss,
                indexCopyBehavior,
                stats.createPerIndexStats(indexConfig.getType() == IndexType.SORTED, usesCachedQueryableEntries),
                partitionCount);

        indexesByName.put(name, index);
        if (index.isEvaluateOnly()) {
            evaluateOnlyAttributeIndexRegistry.register(index);
        } else {
            attributeIndexRegistry.register(index);
        }
        converterCache.invalidate(index);

        InternalIndex[] internalIndexes = indexesByName.values().toArray(EMPTY_INDEXES);
        /**
         * Sort indexes by creation timestamp. Some high-level
         * sub-systems like HD SQL optimizer may need the oldest index that has the most
         * chances to be completely constructed and being usable for queries.
         */
        Arrays.sort(internalIndexes, (o1, o2) -> {
            final long ts1 = o1.getPerIndexStats().getCreationTime();
            final long ts2 = o2.getPerIndexStats().getCreationTime();
            return Long.compare(ts1, ts2);
        });

        indexes = internalIndexes;
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
    public void createIndexesFromRecordedDefinitions() {
        definitions.forEach((name, indexConfig) -> {
            addOrGetIndex(indexConfig);
            definitions.compute(name, (k, v) -> indexConfig == v ? null : v);
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
     */
    public boolean haveAtLeastOneIndexOrDefinition() {
        boolean haveAtLeastOneIndexOrDefinition = haveAtLeastOneIndex() || !definitions.isEmpty();
        // for local indexes assert that indexes and definitions are exclusive
        assert isGlobal() || !haveAtLeastOneIndexOrDefinition || !haveAtLeastOneIndex() || definitions.isEmpty();
        return haveAtLeastOneIndexOrDefinition;
    }

    /**
     * Inserts a new entry into this indexes instance or updates an existing one.
     * <p>
     * Consider using {@link #putEntry(CachedQueryEntry, CachedQueryEntry,
     * QueryableEntry, Index.OperationSource)} for repetitive insertions/updates.
     *
     * @param entryToStore    the queryable entry to insert or update.
     * @param oldValue        the old entry value to update, {@code null} if
     *                        inserting a new entry.
     * @param operationSource the operation source.
     */
    public void putEntry(QueryableEntry entryToStore, Object oldValue, Index.OperationSource operationSource) {
        if (entryToStore instanceof CachedQueryEntry && oldValue == null) {
            putEntry((CachedQueryEntry) entryToStore, null, entryToStore, operationSource);
            return;
        }

        CachedQueryEntry[] cachedEntries = CACHED_ENTRIES.get();

        CachedQueryEntry newEntry;
        if (entryToStore instanceof CachedQueryEntry) {
            newEntry = (CachedQueryEntry) entryToStore;
        } else {
            newEntry = cachedEntries[0];
            newEntry.init(ss, entryToStore.getKeyData(), entryToStore.getTargetObject(false), extractors);
        }

        CachedQueryEntry oldEntry;
        if (oldValue == null) {
            oldEntry = null;
        } else {
            oldEntry = cachedEntries[1];
            oldEntry.init(ss, entryToStore.getKeyData(), oldValue, extractors);
        }

        putEntry(newEntry, oldEntry, entryToStore, operationSource);
    }

    /**
     * Inserts a new entry into this indexes instance or updates an existing one.
     *
     * @param newEntry        the new entry from which new attribute values
     *                        should be read.
     * @param oldEntry        the previous old entry from which old attribute
     *                        values should be read; or {@code null} if there is
     *                        no old entry.
     * @param entryToStore    the entry that should be stored in this indexes
     *                        instance; it might differ from the passed {@code
     *                        newEntry}: for instance, {@code entryToStore} might
     *                        be optimized specifically for storage, while {@code
     *                        newEntry} and {@code oldEntry} are always optimized
     *                        for attribute values extraction.
     * @param operationSource the operation source.
     */
    public void putEntry(CachedQueryEntry newEntry, CachedQueryEntry oldEntry, QueryableEntry entryToStore,
                         Index.OperationSource operationSource) {
        InternalIndex[] indexes = getIndexes();
        Throwable exception = null;
        for (InternalIndex index : indexes) {
            try {
                index.putEntry(newEntry, oldEntry, entryToStore, operationSource);
            } catch (Throwable t) {
                if (exception == null) {
                    exception = t;
                }
            }
        }

        if (exception != null) {
            rethrow(exception);
        }
    }

    /**
     * Removes an entry from this indexes instance identified by the given key
     * and value.
     * <p>
     * Consider using {@link #removeEntry(CachedQueryEntry, Index.OperationSource)}
     * for repetitive removals.
     *
     * @param key             the key of the entry to remove.
     * @param value           the value of the entry to remove.
     * @param operationSource the operation source.
     */
    public void removeEntry(Data key, Object value, Index.OperationSource operationSource) {
        CachedQueryEntry entry = CACHED_ENTRIES.get()[0];
        entry.init(ss, key, value, extractors);

        removeEntry(entry, operationSource);
    }

    /**
     * Removes the given entry from this indexes instance.
     *
     * @param entry           the entry to remove.
     * @param operationSource the operation source.
     */
    public void removeEntry(CachedQueryEntry entry, Index.OperationSource operationSource) {
        InternalIndex[] indexes = getIndexes();
        for (InternalIndex index : indexes) {
            index.removeEntry(entry, operationSource);
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
     * @return the produced iterable result object or {@code null} if the query can't be
     * performed using the indexes known to this indexes instance.
     */
    @SuppressWarnings("unchecked")
    public Iterable<QueryableEntry> query(Predicate predicate, int ownedPartitionCount) {
        stats.incrementQueryCount();

        if (!canQueryOverIndex(predicate)) {
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

        if (result != null && resultFilterFactory != null) {
            return IterableUtil.filter(result, resultFilterFactory.get());
        } else {
            return result;
        }
    }

    public boolean canQueryOverIndex(Predicate predicate) {
        return haveAtLeastOneIndex() && predicate instanceof IndexAwarePredicate;
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

    private static IndexesStats createStats(boolean global, InMemoryFormat inMemoryFormat, boolean statisticsEnabled) {
        if (statisticsEnabled) {
            if (global) {
                return inMemoryFormat.equals(NATIVE) ? new HDGlobalIndexesStats() : new GlobalIndexesStats();
            } else {
                return new PartitionIndexesStats();
            }
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
        private int partitionCount;
        private Extractors extractors;
        private IndexProvider indexProvider;
        private InMemoryFormat inMemoryFormat;
        private Supplier<java.util.function.Predicate<QueryableEntry>> resultFilterFactory;

        Builder(SerializationService ss, IndexCopyBehavior indexCopyBehavior, InMemoryFormat inMemoryFormat) {
            this.serializationService = checkNotNull((InternalSerializationService) ss, "serializationService cannot be null");
            this.indexCopyBehavior = checkNotNull(indexCopyBehavior, "indexCopyBehavior cannot be null");
            this.inMemoryFormat = inMemoryFormat;
        }

        /**
         * Set the factory to create filters to filter query result.
         * If filter returns {@code false}, entry is filtered
         * out from query result, otherwise it is included.
         *
         * @param filterFactory factory to create filters.
         * @return this builder instance
         */
        public Builder resultFilterFactory(Supplier<java.util.function.Predicate<QueryableEntry>> filterFactory) {
            this.resultFilterFactory = filterFactory;
            return this;
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

        public Builder partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        /**
         * @return a new instance of Indexes
         */
        public Indexes build() {
            return new Indexes(serializationService, indexCopyBehavior, extractors,
                    indexProvider, usesCachedQueryableEntries, statsEnabled, global,
                    inMemoryFormat, partitionCount, resultFilterFactory);
        }
    }
}
