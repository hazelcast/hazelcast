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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.NodeInvokerWrapper;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.spi.impl.eventservice.EventFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.map.impl.querycache.subscriber.AbstractQueryCacheEndToEndConstructor.OPERATION_WAIT_TIMEOUT_MINUTES;
import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.publishEntryEvent;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest.newQueryCacheRequest;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static com.hazelcast.query.impl.Indexes.SKIP_PARTITIONS_COUNT_CHECK;


/**
 * Default implementation of {@link com.hazelcast.map.QueryCache QueryCache} interface which holds actual data.
 *
 * @param <K> the key type for this {@link InternalQueryCache}
 * @param <V> the value type for this {@link InternalQueryCache}
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
class DefaultQueryCache<K, V> extends AbstractInternalQueryCache<K, V> {

    DefaultQueryCache(String cacheId, String cacheName, QueryCacheConfig queryCacheConfig,
                      IMap delegate, QueryCacheContext context) {
        super(cacheId, cacheName, queryCacheConfig, delegate, context);
    }

    @Override
    public void set(K key, V value, EntryEventType eventType) {
        setInternal(key, value, eventType, true);
    }

    @Override
    public void prepopulate(K key, V value) {
        setInternal(key, value, EntryEventType.ADDED, false);
    }

    /**
     * @param doEvictionCheck when doing pre-population of query cache, set
     *                        this to false since we quit population if we reach max capacity {@link
     *                        #reachedMaxCapacity()}, eviction is not needed.
     */
    private void setInternal(K key, V value, EntryEventType eventType, boolean doEvictionCheck) {
        Data keyData = toData(key);
        Data valueData = toData(value);

        QueryCacheRecord oldRecord = doEvictionCheck
                ? recordStore.add(keyData, valueData) : recordStore.addWithoutEvictionCheck(keyData, valueData);

        if (eventType != null) {
            publishEntryEvent(context, mapName, cacheId,
                    keyData, valueData, oldRecord, eventType, extractors);
        }
    }

    @Override
    public void delete(Object key, EntryEventType eventType) {
        checkNotNull(key, "key cannot be null");

        Data keyData = toData(key);

        QueryCacheRecord oldRecord = recordStore.remove(keyData);
        if (oldRecord == null) {
            return;
        }
        if (eventType != null) {
            publishEntryEvent(context, mapName, cacheId, keyData,
                    null, oldRecord, eventType, extractors);
        }
    }

    @Override
    public boolean tryRecover() {
        SubscriberAccumulator subscriberAccumulator = getOrNullSubscriberAccumulator();
        if (subscriberAccumulator == null) {
            return false;
        }

        ConcurrentMap<Integer, Long> brokenSequences = subscriberAccumulator.getBrokenSequences();
        if (brokenSequences.isEmpty()) {
            return true;
        }

        return isTryRecoverSucceeded(brokenSequences);
    }

    /**
     * This tries to reset cursor position of the accumulator to the supplied sequence,
     * if that sequence is still there, it will be succeeded, otherwise query cache content stays inconsistent.
     */
    private boolean isTryRecoverSucceeded(ConcurrentMap<Integer, Long> brokenSequences) {
        int numberOfBrokenSequences = brokenSequences.size();
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        SubscriberContext subscriberContext = context.getSubscriberContext();
        SubscriberContextSupport subscriberContextSupport = subscriberContext.getSubscriberContextSupport();

        List<Future<Object>> futures = new ArrayList<>(numberOfBrokenSequences);
        for (Map.Entry<Integer, Long> entry : brokenSequences.entrySet()) {
            Integer partitionId = entry.getKey();
            Long sequence = entry.getValue();
            Object recoveryOperation
                    = subscriberContextSupport.createRecoveryOperation(mapName, cacheId, sequence, partitionId);
            Future<Object> future
                    = (Future<Object>)
                    invokerWrapper.invokeOnPartitionOwner(recoveryOperation, partitionId);
            futures.add(future);
        }

        Collection<Object> results = FutureUtil.returnWithDeadline(futures, 1, MINUTES);
        int successCount = 0;
        for (Object object : results) {
            Boolean resolvedResponse = subscriberContextSupport.resolveResponseForRecoveryOperation(object);
            if (TRUE.equals(resolvedResponse)) {
                successCount++;
            }
        }
        return successCount == numberOfBrokenSequences;
    }

    @Override
    public void destroy() {
        removeAccumulatorInfo();
        removeSubscriberRegistry();
        removeInternalQueryCache();

        ContextMutexFactory.Mutex mutex = context.getLifecycleMutexFactory().mutexFor(mapName);
        try {
            synchronized (mutex) {
                destroyRemoteResources();
                removeAllUserDefinedListeners();
            }
        } finally {
            closeResource(mutex);
        }
    }

    private void destroyRemoteResources() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        SubscriberContextSupport subscriberContextSupport = subscriberContext.getSubscriberContextSupport();

        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        if (invokerWrapper instanceof NodeInvokerWrapper) {
            subscriberContext.getEventService().removePublisherListener(mapName, cacheId, publisherListenerId);

            Collection<Member> memberList = context.getMemberList();
            List<Future> futures = new ArrayList<>(memberList.size());

            for (Member member : memberList) {
                Object removePublisher = subscriberContextSupport.createDestroyQueryCacheOperation(mapName, cacheId);
                Future future = invokerWrapper.invokeOnTarget(removePublisher, member);
                futures.add(future);
            }
            waitWithDeadline(futures, OPERATION_WAIT_TIMEOUT_MINUTES, MINUTES);
        } else {
            try {
                subscriberContext.getEventService().removePublisherListener(mapName, cacheId, publisherListenerId);
            } finally {
                Object removePublisher = subscriberContextSupport.createDestroyQueryCacheOperation(mapName, cacheId);
                invokerWrapper.invoke(removePublisher, false);
            }
        }
    }

    /**
     * Removes listeners which are registered by users to query-cache via {@link com.hazelcast.map.QueryCache#addEntryListener}
     * These listeners are called user defined listeners to distinguish them from listeners which are required for internal
     * implementation of query-cache. User defined listeners are local to query-caches.
     */
    private void removeAllUserDefinedListeners() {
        context.getQueryCacheEventService().removeAllListeners(mapName, cacheId);
    }

    private boolean removeSubscriberRegistry() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();
        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrNull(mapName);
        if (subscriberRegistry == null) {
            return true;
        }

        subscriberRegistry.remove(cacheId);
        return false;
    }

    private void removeAccumulatorInfo() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        AccumulatorInfoSupplier accumulatorInfoSupplier = subscriberContext.getAccumulatorInfoSupplier();
        accumulatorInfoSupplier.remove(mapName, cacheId);
    }

    private boolean removeInternalQueryCache() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        QueryCacheEndToEndProvider cacheProvider = subscriberContext.getEndToEndQueryCacheProvider();
        cacheProvider.removeSingleQueryCache(mapName, cacheName);
        clear();
        return subscriberContext.getQueryCacheFactory().remove(this);
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, "key cannot be null");

        Data keyData = toData(key);
        return recordStore.containsKey(keyData);
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, "value cannot be null");

        return recordStore.containsValue(value);
    }

    @Override
    public V get(Object key) {
        checkNotNull(key, "key cannot be null");

        Data keyData = toData(key);
        QueryCacheRecord record = recordStore.get(keyData);
        // this is not a known key for the scope of this query-cache.
        if (record == null) {
            return null;
        }

        if (includeValue) {
            Object valueInRecord = record.getValue();
            return toObject(valueInRecord);
        } else {
            // if value caching is not enabled, we fetch the value from underlying IMap
            // for every request
            return (V) getDelegate().get(keyData);
        }
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        checkNotNull(keys, "keys cannot be null");
        checkNoNullInside(keys, "supplied key-set cannot contain null key");

        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }

        if (!includeValue) {
            return getDelegate().getAll(keys);
        }

        Map<K, V> map = MapUtil.createHashMap(keys.size());
        for (K key : keys) {
            Data keyData = toData(key);
            QueryCacheRecord record = recordStore.get(keyData);
            if (record != null) {
                V value = toObject(record.getValue());
                map.put(key, value);
            }
        }
        return map;
    }

    @Override
    public Set<K> keySet() {
        return keySet(Predicates.alwaysTrue());
    }

    @Override
    public Collection<V> values() {
        return values(Predicates.alwaysTrue());
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return entrySet(Predicates.alwaysTrue());
    }

    @Override
    public Set<K> keySet(Predicate predicate) {
        checkNotNull(predicate, "Predicate cannot be null!");

        Set<K> resultingSet = new HashSet<>();

        Set<QueryableEntry> query = indexes.query(predicate, SKIP_PARTITIONS_COUNT_CHECK);
        if (query != null) {
            for (QueryableEntry entry : query) {
                K key = toObject(entry.getKeyData());
                resultingSet.add(key);
            }
        } else {
            doFullKeyScan(predicate, resultingSet);
        }

        return resultingSet;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(Predicate predicate) {
        checkNotNull(predicate, "Predicate cannot be null!");

        Set<Map.Entry<K, V>> resultingSet = new HashSet<>();

        Set<QueryableEntry> query = indexes.query(predicate, SKIP_PARTITIONS_COUNT_CHECK);
        if (query != null) {
            for (QueryableEntry entry : query) {
                Map.Entry<K, V> copyEntry = new CachedQueryEntry<>(serializationService, entry.getKeyData(),
                        entry.getValueData(), null);
                resultingSet.add(copyEntry);
            }
        } else {
            doFullEntryScan(predicate, resultingSet);
        }
        return resultingSet;
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        checkNotNull(predicate, "Predicate cannot be null!");

        if (!includeValue) {
            return Collections.emptySet();
        }

        List<Data> resultingList = new ArrayList<>();

        Set<QueryableEntry> query = indexes.query(predicate, SKIP_PARTITIONS_COUNT_CHECK);
        if (query != null) {
            for (QueryableEntry entry : query) {
                resultingList.add(entry.getValueData());
            }
        } else {
            doFullValueScan(predicate, resultingList);
        }
        return new UnmodifiableLazyList(resultingList, serializationService);
    }

    @Override
    public boolean isEmpty() {
        return recordStore.isEmpty();
    }

    @Override
    public int size() {
        return recordStore.size();
    }

    @Override
    public UUID addEntryListener(MapListener listener, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public UUID addEntryListener(MapListener listener, K key, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");

        return addEntryListenerInternal(listener, key, includeValue);
    }

    private UUID addEntryListenerInternal(MapListener listener, K key, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");

        Data keyData = toData(key);
        EventFilter filter = new EntryEventFilter(includeValue, keyData);
        QueryCacheEventService eventService = getEventService();
        String mapName = delegate.getName();
        return eventService.addListener(mapName, cacheId, listener, filter);
    }

    @Override
    public UUID addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");
        checkNotNull(predicate, "predicate cannot be null");

        QueryCacheEventService eventService = getEventService();
        EventFilter filter = new QueryEventFilter(includeValue, null, predicate);
        String mapName = delegate.getName();
        return eventService.addListener(mapName, cacheId, listener, filter);
    }

    @Override
    public UUID addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotNull(key, "key cannot be null");

        QueryCacheEventService eventService = getEventService();
        EventFilter filter = new QueryEventFilter(includeValue, toData(key), predicate);
        String mapName = delegate.getName();
        return eventService.addListener(mapName, cacheId, listener, filter);
    }

    @Override
    public boolean removeEntryListener(UUID id) {
        checkNotNull(id, "listener ID cannot be null");

        QueryCacheEventService eventService = getEventService();
        return eventService.removeListener(mapName, cacheId, id);
    }

    @Override
    public void addIndex(IndexConfig config) {
        checkNotNull(config, "Index config cannot be null.");

        assert indexes.isGlobal();

        IndexConfig config0 = getNormalizedIndexConfig(config);

        indexes.addOrGetIndex(config0, null);

        InternalSerializationService serializationService = context.getSerializationService();

        Set<Map.Entry<Data, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Data, QueryCacheRecord> entry : entries) {
            Data keyData = entry.getKey();
            QueryCacheRecord record = entry.getValue();
            Object value = record.getValue();
            QueryEntry queryable = new QueryEntry(serializationService, keyData, value, extractors);
            indexes.putEntry(queryable, null, Index.OperationSource.USER);
        }
    }

    @Override
    public String getName() {
        return cacheName;
    }

    @Override
    public IMap getDelegate() {
        return delegate;
    }

    @Override
    public void recreate() {
        ContextMutexFactory.Mutex mutex = context.getLifecycleMutexFactory().mutexFor(mapName);
        try {
            // done other mutex to prevent races with `destroy`
            synchronized (mutex) {
                SubscriberContext subscriberContext = context.getSubscriberContext();

                // 0. Check subscriber still exists
                SubscriberAccumulator subscriberAccumulator = getOrNullSubscriberAccumulator();
                if (subscriberAccumulator == null) {
                    return;
                }

                // 1. Reset client-side subscriber resources
                subscriberAccumulator.reset();

                // 2. Reset/recreate server-side publisher resources
                QueryCacheRequest request = newQueryCacheRequest()
                        .withCacheName(cacheName)
                        .forMap(delegate)
                        .urgent(true)
                        .withContext(context);

                QueryCacheEndToEndProvider queryCacheEndToEndProvider
                        = subscriberContext.getEndToEndQueryCacheProvider();
                queryCacheEndToEndProvider.tryCreateQueryCache(mapName, cacheName,
                        subscriberContext.newEndToEndConstructor(request));
            }
        } finally {
            closeResource(mutex);
        }
    }

    private SubscriberAccumulator getOrNullSubscriberAccumulator() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();

        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrNull(mapName);
        if (subscriberRegistry == null) {
            return null;
        }

        Accumulator accumulator = subscriberRegistry.getOrNull(cacheId);
        if (accumulator == null) {
            return null;
        }

        return (SubscriberAccumulator) accumulator;
    }

    @Override
    public int removeEntriesOf(int partitionId) {
        int removedEntryCount = 0;

        Set<Data> keys = recordStore.keySet();
        for (Data keyData : keys) {
            if (context.getPartitionId(keyData) == partitionId) {
                if (recordStore.remove(keyData) != null) {
                    removedEntryCount++;
                }
            }
        }

        return removedEntryCount;
    }

    @Override
    public String toString() {
        return "DefaultQueryCache{"
                + "mapName='" + mapName + '\''
                + ", cacheId='" + cacheId + '\''
                + ", cacheName='" + cacheName + '\''
                + '}';
    }
}
