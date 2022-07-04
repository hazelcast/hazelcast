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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.TruePredicate;
import com.hazelcast.spi.impl.UnmodifiableLazySet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.query.impl.IndexCopyBehavior.COPY_ON_READ;
import static com.hazelcast.query.impl.Indexes.SKIP_PARTITIONS_COUNT_CHECK;
import static java.util.Objects.requireNonNull;

/**
 * Contains helper methods for {@link
 * InternalQueryCache} main implementation.
 *
 * @param <K> the key type for this {@link InternalQueryCache}
 * @param <V> the value type for this {@link InternalQueryCache}
 */
abstract class AbstractInternalQueryCache<K, V> implements InternalQueryCache<K, V> {

    protected final boolean includeValue;
    protected final String mapName;
    protected final String cacheId;
    protected final String cacheName;
    protected final IMap delegate;
    protected final Indexes indexes;
    protected final QueryCacheContext context;
    protected final QueryCacheConfig queryCacheConfig;
    protected final QueryCacheRecordStore recordStore;
    protected final PartitioningStrategy partitioningStrategy;
    protected final InternalSerializationService ss;
    protected final Extractors extractors;
    /**
     * ID of registered listener on publisher side.
     */
    protected volatile UUID publisherListenerId;

    AbstractInternalQueryCache(String cacheId, String cacheName, QueryCacheConfig queryCacheConfig,
                               IMap delegate, QueryCacheContext context) {
        this.cacheId = cacheId;
        this.cacheName = cacheName;
        this.queryCacheConfig = queryCacheConfig;
        this.mapName = delegate.getName();
        this.delegate = delegate;
        this.context = context;
        this.ss = context.getSerializationService();
        // We are not using injected index provider since we're not supporting off-heap indexes in CQC due
        // to threading incompatibility. If we injected the IndexProvider from the MapServiceContext
        // the EE side would create HD indexes which is undesired.
        this.indexes = Indexes.newBuilder(ss, COPY_ON_READ, queryCacheConfig.getInMemoryFormat())
                .partitionCount(context.getPartitionCount())
                .build();

        this.includeValue = isIncludeValue();
        this.partitioningStrategy = getPartitioningStrategy();
        this.extractors = Extractors.newBuilder(ss).build();
        this.recordStore = new DefaultQueryCacheRecordStore(ss, indexes,
                queryCacheConfig, getEvictionListener(), extractors);

        assert indexes.isGlobal();

        for (IndexConfig indexConfig : queryCacheConfig.getIndexConfigs()) {
            IndexConfig indexConfig0 = getNormalizedIndexConfig(indexConfig);

            indexes.addOrGetIndex(indexConfig0);
        }
    }

    public QueryCacheContext getContext() {
        return context;
    }

    @Override
    public UUID getPublisherListenerId() {
        return publisherListenerId;
    }

    @Override
    public void setPublisherListenerId(UUID publisherListenerId) {
        this.publisherListenerId = requireNonNull(publisherListenerId, "publisherListenerId cannot be null");
    }

    @Override
    public String getCacheId() {
        return cacheId;
    }

    protected Predicate getPredicate() {
        return queryCacheConfig.getPredicateConfig().getImplementation();
    }

    @Override
    public boolean reachedMaxCapacity() {
        EvictionConfig evictionConfig = queryCacheConfig.getEvictionConfig();
        MaxSizePolicy maximumSizePolicy = evictionConfig.getMaxSizePolicy();
        return maximumSizePolicy == MaxSizePolicy.ENTRY_COUNT
                && size() == evictionConfig.getSize();
    }

    private EvictionListener getEvictionListener() {
        return (EvictionListener<Object, QueryCacheRecord>) (queryCacheKey, record, wasExpired)
                -> EventPublisherHelper.publishEntryEvent(context, mapName, cacheId,
                queryCacheKey, null, record, EVICTED, extractors);
    }

    PartitioningStrategy getPartitioningStrategy() {
        if (delegate instanceof MapProxyImpl) {
            return ((MapProxyImpl) delegate).getPartitionStrategy();
        }
        return null;
    }

    protected Set scanAndGetResult(Predicate predicate, ResultType resultType) {
        ResulCollector resulCollector = new ResulCollector(resultType);

        // 1. Optimization when predicate is true-predicate,
        // in this case no need to scan indexes.
        if (predicate == TruePredicate.INSTANCE) {
            doFullScan(predicate, resulCollector);
            return toImmutableLazySet(resulCollector.getResult());
        }

        // 2. Try to query over indexes
        if (tryQueryOverIndexes(predicate, resulCollector)) {
            return toImmutableLazySet(resulCollector.getResult());
        }

        // 3. If query can't be performed over indexes, scan full data set.
        doFullScan(predicate, resulCollector);
        return toImmutableLazySet(resulCollector.getResult());
    }

    private Set toImmutableLazySet(List resultSet) {
        return new UnmodifiableLazySet(resultSet, ss);
    }

    /**
     * Type of element that we expect to
     * see in the resulting collection.
     */
    enum ResultType {
        KEY, VALUE, ENTRY
    }

    private final class ResulCollector implements BiConsumer {

        private final ResultType resultType;
        private final List result = new ArrayList();

        private ResulCollector(ResultType resultType) {
            this.resultType = resultType;
        }

        @Override
        public void accept(Object key, Object value) {
            switch (resultType) {
                case KEY:
                    result.add(key);
                    break;
                case VALUE:
                    result.add(value);
                    break;
                case ENTRY:
                    result.add(new CachedQueryEntry<>(ss, key, value, extractors));
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected type: " + resultType);
            }
        }

        public List getResult() {
            return result;
        }
    }

    private boolean tryQueryOverIndexes(Predicate predicate, BiConsumer biConsumer) {
        Iterable<QueryableEntry> query = indexes.query(predicate, SKIP_PARTITIONS_COUNT_CHECK);
        if (query == null) {
            return false;
        }

        for (QueryableEntry entry : query) {
            biConsumer.accept(entry.getKeyData(), entry.getValueData());
        }
        return true;
    }

    private void doFullScan(Predicate predicate, BiConsumer biConsumer) {
        if (predicate == TruePredicate.INSTANCE) {
            dumpAll(biConsumer);
            return;
        }

        scanWithPredicate(predicate, biConsumer);
    }

    /**
     * Add all key-value pairs to result.
     */
    private void dumpAll(BiConsumer consumer) {
        Set<Map.Entry<Object, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Object, QueryCacheRecord> entry : entries) {
            consumer.accept(entry.getKey(), entry.getValue().getRawValue());
        }
    }

    /**
     * Scan all key-value pairs and add matching ones with predicate to result.
     */
    private void scanWithPredicate(Predicate predicate, BiConsumer consumer) {
        // needed for optimization where key and value are not an instance of Data type
        final boolean areKeyValueObjectType = !queryCacheConfig.isSerializeKeys()
                && InMemoryFormat.OBJECT == queryCacheConfig.getInMemoryFormat();

        CachedQueryEntry queryEntry = new CachedQueryEntry(ss, extractors);
        Set<Map.Entry<Object, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Object, QueryCacheRecord> entry : entries) {
            Object queryCacheKey = entry.getKey();
            Object rawValue = entry.getValue().getRawValue();

            if (areKeyValueObjectType) {
                queryEntry.initWithObjectKeyValue(queryCacheKey, rawValue);
            } else {
                queryEntry.init(queryCacheKey, rawValue);
            }

            if (!predicate.apply(queryEntry)) {
                continue;
            }

            consumer.accept(queryCacheKey, queryEntry.getByPrioritizingObjectValue());
        }
    }

    private boolean isIncludeValue() {
        return queryCacheConfig.isIncludeValue();
    }

    protected QueryCacheEventService getEventService() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        return subscriberContext.getEventService();
    }

    protected <T> T toObject(Object valueInRecord) {
        return ss.toObject(valueInRecord);
    }

    protected Data toData(Object key) {
        return ss.toData(key, partitioningStrategy);
    }

    @Override
    public Extractors getExtractors() {
        return extractors;
    }

    @Override
    public void clear() {
        indexes.destroyIndexes();
        recordStore.clear();
    }

    protected IndexConfig getNormalizedIndexConfig(IndexConfig originalConfig) {
        String name = delegate.getName() + "_" + cacheName;

        return IndexUtils.validateAndNormalize(name, originalConfig);
    }
}
