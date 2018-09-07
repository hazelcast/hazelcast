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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.query.impl.IndexCopyBehavior.COPY_ON_READ;

/**
 * Contains helper methods for {@link InternalQueryCache} main implementation.
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
    protected final InternalSerializationService serializationService;
    protected final PartitioningStrategy partitioningStrategy;
    /**
     * ID of registered listener on publisher side.
     */
    protected String publisherListenerId;

    public AbstractInternalQueryCache(String cacheId, String cacheName, QueryCacheConfig queryCacheConfig,
                                      IMap delegate, QueryCacheContext context) {
        this.cacheId = cacheId;
        this.cacheName = cacheName;
        this.queryCacheConfig = queryCacheConfig;
        this.mapName = delegate.getName();
        this.delegate = delegate;
        this.context = context;
        this.serializationService = context.getSerializationService();
        // We are not using injected index provider since we're not supporting off-heap indexes in CQC due
        // to threading incompatibility. If we injected the IndexProvider from the MapServiceContext
        // the EE side would create HD indexes which is undesired.
        this.indexes = Indexes.newBuilder(serializationService, COPY_ON_READ).build();
        this.includeValue = isIncludeValue();
        this.partitioningStrategy = getPartitioningStrategy();
        this.recordStore = new DefaultQueryCacheRecordStore(serializationService, indexes,
                queryCacheConfig, getEvictionListener());

        for (MapIndexConfig indexConfig : queryCacheConfig.getIndexConfigs()) {
            indexes.addOrGetIndex(indexConfig.getAttribute(), indexConfig.isOrdered());
        }
    }

    public QueryCacheContext getContext() {
        return context;
    }

    @Override
    public void setPublisherListenerId(String publisherListenerId) {
        this.publisherListenerId = publisherListenerId;
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
        EvictionConfig.MaxSizePolicy maximumSizePolicy = evictionConfig.getMaximumSizePolicy();
        return maximumSizePolicy == EvictionConfig.MaxSizePolicy.ENTRY_COUNT
                && size() == evictionConfig.getSize();
    }

    private EvictionListener getEvictionListener() {
        return new EvictionListener<Data, QueryCacheRecord>() {
            @Override
            public void onEvict(Data dataKey, QueryCacheRecord record, boolean wasExpired) {
                EventPublisherHelper.publishEntryEvent(context, mapName, cacheId, dataKey, null, record, EVICTED);
            }
        };
    }

    PartitioningStrategy getPartitioningStrategy() {
        if (delegate instanceof MapProxyImpl) {
            return ((MapProxyImpl) delegate).getPartitionStrategy();
        }
        return null;
    }

    protected void doFullKeyScan(Predicate predicate, Set<K> resultingSet) {
        InternalSerializationService serializationService = this.serializationService;

        CachedQueryEntry queryEntry = new CachedQueryEntry();
        Set<Map.Entry<Data, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Data, QueryCacheRecord> entry : entries) {
            Data keyData = entry.getKey();
            QueryCacheRecord record = entry.getValue();
            Object value = record.getValue();

            queryEntry.init(serializationService, keyData, value, Extractors.empty());

            boolean valid = predicate.apply(queryEntry);
            if (valid) {
                resultingSet.add((K) queryEntry.getKey());
            }
        }
    }

    protected void doFullEntryScan(Predicate predicate, Set<Map.Entry<K, V>> resultingSet) {
        InternalSerializationService serializationService = this.serializationService;

        CachedQueryEntry queryEntry = new CachedQueryEntry();
        Set<Map.Entry<Data, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Data, QueryCacheRecord> entry : entries) {
            Data keyData = entry.getKey();
            QueryCacheRecord record = entry.getValue();
            Object value = record.getValue();
            queryEntry.init(serializationService, keyData, value, Extractors.empty());

            boolean valid = predicate.apply(queryEntry);
            if (valid) {
                Object keyObject = queryEntry.getKey();
                Object valueObject = queryEntry.getValue();
                Map.Entry simpleEntry = new AbstractMap.SimpleEntry(keyObject, valueObject);
                resultingSet.add(simpleEntry);
            }
        }
    }

    protected void doFullValueScan(Predicate predicate, Set<V> resultingSet) {
        InternalSerializationService serializationService = this.serializationService;

        CachedQueryEntry queryEntry = new CachedQueryEntry();
        Set<Map.Entry<Data, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Data, QueryCacheRecord> entry : entries) {
            Data keyData = entry.getKey();
            QueryCacheRecord record = entry.getValue();
            Object value = record.getValue();

            queryEntry.init(serializationService, keyData, value, Extractors.empty());

            boolean valid = predicate.apply(queryEntry);
            if (valid) {
                Object valueObject = queryEntry.getValue();
                resultingSet.add((V) valueObject);
            }
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
        return serializationService.toObject(valueInRecord);
    }

    protected Data toData(Object key) {
        return serializationService.toData(key, partitioningStrategy);
    }

    @Override
    public void clear() {
        recordStore.clear();
        indexes.destroyIndexes();
    }
}
