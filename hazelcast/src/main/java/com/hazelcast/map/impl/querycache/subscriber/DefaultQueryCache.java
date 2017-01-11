/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.NodeInvokerWrapper;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.MapUtil;
import com.hazelcast.util.SetUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Default implementation of {@link com.hazelcast.map.QueryCache QueryCache} interface which holds actual data.
 *
 * @param <K> the key type for this {@link InternalQueryCache}
 * @param <V> the value type for this {@link InternalQueryCache}
 */
@SuppressWarnings("checkstyle:methodcount")
class DefaultQueryCache<K, V> extends AbstractInternalQueryCache<K, V> {

    public DefaultQueryCache(String cacheName, String userGivenCacheName, IMap delegate, QueryCacheContext context) {
        super(cacheName, userGivenCacheName, delegate, context);
    }

    @Override
    public void setInternal(K key, V value, boolean callDelegate, EntryEventType eventType) {
        Data keyData = toData(key);
        Data valueData = toData(value);

        if (callDelegate) {
            getDelegate().set(keyData, valueData);
        }
        QueryCacheRecord oldRecord = recordStore.add(keyData, valueData);

        if (eventType != null) {
            EventPublisherHelper.publishEntryEvent(context, mapName, cacheName, keyData, valueData, oldRecord, eventType);
        }
    }

    @Override
    public void deleteInternal(Object key, boolean callDelegate, EntryEventType eventType) {
        checkNotNull(key, "key cannot be null");

        Data keyData = toData(key);

        if (callDelegate) {
            getDelegate().delete(keyData);
        }
        QueryCacheRecord oldRecord = recordStore.remove(keyData);
        if (oldRecord == null) {
            return;
        }
        if (eventType != null) {
            EventPublisherHelper.publishEntryEvent(context, mapName, cacheName, keyData, null, oldRecord, eventType);
        }
    }

    @Override
    public void clearInternal(EntryEventType eventType) {
        int removedCount = recordStore.clear();
        if (removedCount < 1) {
            return;
        }

        if (eventType != null) {
            EventPublisherHelper.publishCacheWideEvent(context, mapName, cacheName,
                    removedCount, eventType);
        }
    }

    @Override
    public boolean tryRecover() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();

        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrNull(mapName);
        if (subscriberRegistry == null) {
            return true;
        }

        Accumulator accumulator = subscriberRegistry.getOrNull(cacheName);
        if (accumulator == null) {
            return true;
        }

        SubscriberAccumulator subscriberAccumulator = (SubscriberAccumulator) accumulator;
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

        List<Future<Object>> futures = new ArrayList<Future<Object>>(numberOfBrokenSequences);
        for (Map.Entry<Integer, Long> entry : brokenSequences.entrySet()) {
            Integer partitionId = entry.getKey();
            Long sequence = entry.getValue();
            Object recoveryOperation
                    = subscriberContextSupport.createRecoveryOperation(mapName, cacheName, sequence, partitionId);
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
        boolean destroyed = destroyLocalResources();
        if (!destroyed) {
            return;
        }

        destroyRemoteResources();
    }

    private void destroyRemoteResources() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        SubscriberContextSupport subscriberContextSupport = subscriberContext.getSubscriberContextSupport();

        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        if (invokerWrapper instanceof NodeInvokerWrapper) {
            Collection<Member> memberList = context.getMemberList();
            for (Member member : memberList) {
                Address address = member.getAddress();
                Object removePublisher = subscriberContextSupport.createDestroyQueryCacheOperation(mapName, userGivenCacheName);
                invokerWrapper.invokeOnTarget(removePublisher, address);
            }
        } else {
            Object removePublisher = subscriberContextSupport.createDestroyQueryCacheOperation(mapName, userGivenCacheName);
            invokerWrapper.invoke(removePublisher);
        }
    }

    private boolean destroyLocalResources() {
        removeConfig();
        removeAccumulatorInfo();
        removeSubscriberRegistry();
        return removeInternalQueryCache();
    }

    private boolean removeSubscriberRegistry() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();
        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrNull(mapName);
        if (subscriberRegistry == null) {
            return true;
        }

        subscriberRegistry.remove(cacheName);
        return false;
    }

    private void removeAccumulatorInfo() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        AccumulatorInfoSupplier accumulatorInfoSupplier = subscriberContext.getAccumulatorInfoSupplier();
        accumulatorInfoSupplier.remove(mapName, cacheName);
    }

    private void removeConfig() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        QueryCacheConfigurator queryCacheConfigurator = subscriberContext.geQueryCacheConfigurator();
        queryCacheConfigurator.removeConfiguration(mapName, userGivenCacheName);
    }

    private boolean removeInternalQueryCache() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        QueryCacheEndToEndProvider cacheProvider = subscriberContext.getEndToEndQueryCacheProvider();
        InternalQueryCache internalQueryCache = cacheProvider.remove(mapName, userGivenCacheName);
        boolean exists = internalQueryCache != null;
        if (exists) {
            internalQueryCache.clear();
        }
        return exists;
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
            Object valueInRecord = record.getValue();
            V value = toObject(valueInRecord);
            map.put(key, value);
        }
        return map;
    }

    @Override
    public Set<K> keySet() {
        return keySet(TruePredicate.INSTANCE);
    }

    @Override
    public Collection<V> values() {
        return values(TruePredicate.INSTANCE);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return entrySet(TruePredicate.INSTANCE);
    }

    @Override
    public Set<K> keySet(Predicate predicate) {
        checkNotNull(predicate, "Predicate cannot be null!");

        final Set<K> resultingSet;

        Set<QueryableEntry> query = indexes.query(predicate);
        if (query != null) {
            resultingSet = SetUtil.createHashSet(query.size());
            for (QueryableEntry entry : query) {
                K key = (K) entry.getKey();
                resultingSet.add(key);
            }
        } else {
            resultingSet = new HashSet<K>();
            doFullKeyScan(predicate, resultingSet);
        }

        return resultingSet;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(Predicate predicate) {
        checkNotNull(predicate, "Predicate cannot be null!");

        final Set<Map.Entry<K, V>> resultingSet;

        Set<QueryableEntry> query = indexes.query(predicate);
        if (query != null) {
            if (query.isEmpty()) {
                return Collections.emptySet();
            }
            resultingSet = SetUtil.createHashSet(query.size());
            for (QueryableEntry entry : query) {
                resultingSet.add(entry);
            }
        } else {
            resultingSet = new HashSet<Map.Entry<K, V>>();
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

        final Set<V> resultingSet;

        Set<QueryableEntry> query = indexes.query(predicate);
        if (query != null) {
            resultingSet = SetUtil.createHashSet(query.size());
            for (QueryableEntry entry : query) {
                resultingSet.add((V) entry.getValue());
            }
        } else {
            resultingSet = new HashSet<V>();
            doFullValueScan(predicate, resultingSet);
        }
        return resultingSet;
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
    public String addEntryListener(MapListener listener, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, K key, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");

        return addEntryListenerInternal(listener, key, includeValue);
    }

    private String addEntryListenerInternal(MapListener listener, K key, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");

        Data keyData = toData(key);
        EventFilter filter = new EntryEventFilter(includeValue, keyData);
        QueryCacheEventService eventService = getEventService();
        String mapName = delegate.getName();
        return eventService.addListener(mapName, cacheName, listener, filter);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");
        checkNotNull(predicate, "predicate cannot be null");

        QueryCacheEventService eventService = getEventService();
        EventFilter filter = new QueryEventFilter(includeValue, null, predicate);
        String mapName = delegate.getName();
        return eventService.addListener(mapName, cacheName, listener, filter);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, "listener cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotNull(key, "key cannot be null");

        QueryCacheEventService eventService = getEventService();
        EventFilter filter = new QueryEventFilter(includeValue, toData(key), predicate);
        String mapName = delegate.getName();
        return eventService.addListener(mapName, cacheName, listener, filter);
    }

    @Override
    public boolean removeEntryListener(String id) {
        QueryCacheEventService eventService = getEventService();
        return eventService.removeListener(mapName, cacheName, id);
    }

    @Override
    public void addIndex(String attribute, boolean ordered) {
        getIndexes().addOrGetIndex(attribute, ordered);

        InternalSerializationService serializationService = context.getSerializationService();

        Set<Map.Entry<Data, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Data, QueryCacheRecord> entry : entries) {
            Data keyData = entry.getKey();
            QueryCacheRecord record = entry.getValue();
            Object value = record.getValue();
            QueryEntry queryable = new QueryEntry(serializationService, keyData, value, Extractors.empty());
            indexes.saveEntryIndex(queryable, null);
        }
    }

    @Override
    public String getName() {
        return userGivenCacheName;
    }


    @Override
    public IMap getDelegate() {
        return delegate;
    }

    @Override
    public Indexes getIndexes() {
        return indexes;
    }

    @Override
    public String toString() {
        return recordStore.toString();
    }
}
