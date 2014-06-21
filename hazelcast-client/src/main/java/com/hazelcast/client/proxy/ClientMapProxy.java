/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.nearcache.ClientNearCacheType;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapKeySet;
import com.hazelcast.map.MapValueCollection;
import com.hazelcast.map.SimpleEntryView;
import com.hazelcast.map.client.MapAddEntryListenerRequest;
import com.hazelcast.map.client.MapAddIndexRequest;
import com.hazelcast.map.client.MapAddInterceptorRequest;
import com.hazelcast.map.client.MapClearRequest;
import com.hazelcast.map.client.MapContainsKeyRequest;
import com.hazelcast.map.client.MapContainsValueRequest;
import com.hazelcast.map.client.MapDeleteRequest;
import com.hazelcast.map.client.MapEntrySetRequest;
import com.hazelcast.map.client.MapEvictAllRequest;
import com.hazelcast.map.client.MapEvictRequest;
import com.hazelcast.map.client.MapExecuteOnAllKeysRequest;
import com.hazelcast.map.client.MapExecuteOnKeyRequest;
import com.hazelcast.map.client.MapExecuteOnKeysRequest;
import com.hazelcast.map.client.MapExecuteWithPredicateRequest;
import com.hazelcast.map.client.MapFlushRequest;
import com.hazelcast.map.client.MapGetAllRequest;
import com.hazelcast.map.client.MapGetEntryViewRequest;
import com.hazelcast.map.client.MapGetRequest;
import com.hazelcast.map.client.MapIsLockedRequest;
import com.hazelcast.map.client.MapKeySetRequest;
import com.hazelcast.map.client.MapLoadAllKeysRequest;
import com.hazelcast.map.client.MapLoadGivenKeysRequest;
import com.hazelcast.map.client.MapLockRequest;
import com.hazelcast.map.client.MapPutAllRequest;
import com.hazelcast.map.client.MapPutIfAbsentRequest;
import com.hazelcast.map.client.MapPutRequest;
import com.hazelcast.map.client.MapPutTransientRequest;
import com.hazelcast.map.client.MapQueryRequest;
import com.hazelcast.map.client.MapRemoveEntryListenerRequest;
import com.hazelcast.map.client.MapRemoveIfSameRequest;
import com.hazelcast.map.client.MapRemoveInterceptorRequest;
import com.hazelcast.map.client.MapRemoveRequest;
import com.hazelcast.map.client.MapReplaceIfSameRequest;
import com.hazelcast.map.client.MapReplaceRequest;
import com.hazelcast.map.client.MapSetRequest;
import com.hazelcast.map.client.MapSizeRequest;
import com.hazelcast.map.client.MapTryPutRequest;
import com.hazelcast.map.client.MapTryRemoveRequest;
import com.hazelcast.map.client.MapUnlockRequest;
import com.hazelcast.map.client.MapValuesRequest;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.MappingJob;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.ReducingSubmittableJob;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PagingPredicateAccessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.AbstractPortableEventData;
import com.hazelcast.spi.impl.EventData;
import com.hazelcast.spi.impl.PortableEntryEventData;
import com.hazelcast.spi.impl.PortableMapEventData;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;
import com.hazelcast.util.SortedQueryResultSet;
import com.hazelcast.util.SortingUtil;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.ValidationUtil;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mdogan 5/17/13
 */
public final class ClientMapProxy<K, V> extends ClientProxy implements IMap<K, V> {

    private final String name;
    private volatile ClientNearCache<Data> nearCache;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();

    public ClientMapProxy(String instanceName, String serviceName, String name) {
        super(instanceName, serviceName, name);
        this.name = name;
    }

    @Override
    public boolean containsKey(Object key) {
        initNearCache();
        final Data keyData = toData(key);
        if (nearCache != null) {
            Object cached = nearCache.get(keyData);
            if (cached != null) {
                if (cached.equals(ClientNearCache.NULL_OBJECT)) {
                    return false;
                }
                return true;
            }
        }
        MapContainsKeyRequest request = new MapContainsKeyRequest(name, keyData);
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public boolean containsValue(Object value) {
        Data valueData = toData(value);
        MapContainsValueRequest request = new MapContainsValueRequest(name, valueData);
        Boolean result = invoke(request);
        return result;
    }

    @Override
    public V get(Object key) {
        initNearCache();

        final Data keyData = toData(key);
        if (nearCache != null) {
            Object cached = nearCache.get(keyData);
            if (cached != null) {
                if (cached.equals(ClientNearCache.NULL_OBJECT)) {
                    return null;
                }
                return (V) cached;
            }
        }
        MapGetRequest request = new MapGetRequest(name, keyData);
        final V result = invoke(request, keyData);
        if (nearCache != null) {
            nearCache.put(keyData, result);
        }
        return result;
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, null);
    }

    @Override
    public V remove(Object key) {
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        MapRemoveRequest request = new MapRemoveRequest(name, keyData, ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public boolean remove(Object key, Object value) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapRemoveIfSameRequest request = new MapRemoveIfSameRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public void delete(Object key) {
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        MapDeleteRequest request = new MapDeleteRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void flush() {
        MapFlushRequest request = new MapFlushRequest(name);
        invoke(request);
    }

    @Override
    public Future<V> getAsync(final K key) {
        initNearCache();
        final Data keyData = toData(key);
        if (nearCache != null) {
            Object cached = nearCache.get(keyData);
            if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
                return new CompletedFuture(getContext().getSerializationService(),
                        cached, getContext().getExecutionService().getAsyncExecutor());
            }
        }

        final MapGetRequest request = new MapGetRequest(name, keyData);
        try {
            final ICompletableFuture future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            final DelegatingFuture<V> delegatingFuture = new DelegatingFuture<V>(future, getContext().getSerializationService());
            delegatingFuture.andThen(new ExecutionCallback<V>() {
                @Override
                public void onResponse(V response) {
                    if (nearCache != null) {
                        nearCache.put(keyData, response);
                    }
                }

                @Override
                public void onFailure(Throwable t) {

                }
            });
            return delegatingFuture;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Future<V> putAsync(final K key, final V value) {
        return putAsync(key, value, -1, null);
    }

    @Override
    public Future<V> putAsync(final K key, final V value, final long ttl, final TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapPutRequest request = new MapPutRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        try {
            final ICompletableFuture future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture<V>(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Future<V> removeAsync(final K key) {
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        MapRemoveRequest request = new MapRemoveRequest(name, keyData, ThreadUtil.getThreadId());
        try {
            final ICompletableFuture future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture<V>(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        MapTryRemoveRequest request = new MapTryRemoveRequest(name, keyData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapTryPutRequest request = new MapTryPutRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapPutRequest request = new MapPutRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapPutTransientRequest request = new MapPutTransientRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, -1, null);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapPutIfAbsentRequest request = new MapPutIfAbsentRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        final Data keyData = toData(key);
        final Data oldValueData = toData(oldValue);
        final Data newValueData = toData(newValue);
        invalidateNearCache(keyData);
        MapReplaceIfSameRequest request = new MapReplaceIfSameRequest(name, keyData, oldValueData, newValueData,
                ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public V replace(K key, V value) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapReplaceRequest request = new MapReplaceRequest(name, keyData, valueData,
                ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapSetRequest request = new MapSetRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request, keyData);
    }

    @Override
    public void lock(K key) {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData,
                ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit), -1);
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(K key) {
        final Data keyData = toData(key);
        MapIsLockedRequest request = new MapIsLockedRequest(name, keyData);
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public boolean tryLock(K key) {
        try {
            return tryLock(key, 0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData,
                ThreadUtil.getThreadId(), Long.MAX_VALUE, getTimeInMillis(time, timeunit));
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public void unlock(K key) {
        final Data keyData = toData(key);
        MapUnlockRequest request = new MapUnlockRequest(name, keyData, ThreadUtil.getThreadId(), false);
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(K key) {
        final Data keyData = toData(key);
        MapUnlockRequest request = new MapUnlockRequest(name, keyData, ThreadUtil.getThreadId(), true);
        invoke(request, keyData);
    }

    @Override
    public String addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String addLocalEntryListener(EntryListener<K, V> listener,
                                        Predicate<K, V> predicate, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String addLocalEntryListener(EntryListener<K, V> listener,
                                        Predicate<K, V> predicate, K key, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    public String addInterceptor(MapInterceptor interceptor) {
        MapAddInterceptorRequest request = new MapAddInterceptorRequest(name, interceptor);
        return invoke(request);
    }

    @Override
    public void removeInterceptor(String id) {
        MapRemoveInterceptorRequest request = new MapRemoveInterceptorRequest(name, id);
        invoke(request);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, includeValue);
        EventHandler<EventData> handler = createHandler(listener, includeValue);
        return listen(request, handler);
    }

    @Override
    public boolean removeEntryListener(String id) {
        final MapRemoveEntryListenerRequest request = new MapRemoveEntryListenerRequest(name, id);
        return stopListening(request, id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        final Data keyData = toData(key);
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, keyData, includeValue);
        EventHandler<EventData> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        final Data keyData = toData(key);
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, keyData, includeValue, predicate);
        EventHandler<EventData> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, boolean includeValue) {
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, null, includeValue, predicate);
        EventHandler<EventData> handler = createHandler(listener, includeValue);
        return listen(request, null, handler);
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        final Data keyData = toData(key);
        MapGetEntryViewRequest request = new MapGetEntryViewRequest(name, keyData);
        SimpleEntryView entryView = invoke(request, keyData);
        if (entryView == null) {
            return null;
        }
        final Data value = (Data) entryView.getValue();
        entryView.setKey(key);
        entryView.setValue(toObject(value));
        //TODO putCache
        return entryView;
    }

    @Override
    public boolean evict(K key) {
        final Data keyData = toData(key);
        MapEvictRequest request = new MapEvictRequest(name, keyData, ThreadUtil.getThreadId());
        Boolean result = invoke(request);
        return result;
    }

    @Override
    public void evictAll() {
        clearNearCache();
        MapEvictAllRequest request = new MapEvictAllRequest(name);
        invoke(request);
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        if (replaceExistingValues) {
            clearNearCache();
        }
        final MapLoadAllKeysRequest request = new MapLoadAllKeysRequest(name, replaceExistingValues);
        invoke(request);
    }

    @Override
    public void loadAll(Set<K> keys, boolean replaceExistingValues) {
        if (keys == null) {
            throw new NullPointerException("Parameter keys should not be null.");
        }
        if (keys.isEmpty()) {
            return;
        }
        final Collection<Data> dataKeys = convertKeysToData(keys);
        if (replaceExistingValues) {
            invalidateNearCache(dataKeys);
        }
        final MapLoadGivenKeysRequest request = new MapLoadGivenKeysRequest(name, dataKeys, replaceExistingValues);
        invoke(request);
    }

    // todo duplicate code.
    private <K> Collection<Data> convertKeysToData(Set<K> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Data> dataKeys = new ArrayList<Data>(keys.size());
        for (K key : keys) {
            if (key == null) {
                throw new NullPointerException("Null key is not allowed");
            }
            final Data dataKey = toData(key);
            dataKeys.add(dataKey);
        }
        return dataKeys;
    }

    @Override
    public Set<K> keySet() {
        MapKeySetRequest request = new MapKeySetRequest(name);
        MapKeySet mapKeySet = invoke(request);
        Set<Data> keySetData = mapKeySet.getKeySet();
        Set<K> keySet = new HashSet<K>(keySetData.size());
        for (Data data : keySetData) {
            final K key = toObject(data);
            keySet.add(key);
        }
        return keySet;
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        initNearCache();
        Set<Data> keySet = new HashSet(keys.size());
        Map<K, V> result = new HashMap<K, V>();
        for (Object key : keys) {
            keySet.add(toData(key));
        }
        if (nearCache != null) {
            final Iterator<Data> iterator = keySet.iterator();
            while (iterator.hasNext()) {
                Data key = iterator.next();
                Object cached = nearCache.get(key);
                if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
                    result.put((K) toObject(key), (V) cached);
                    iterator.remove();
                }
            }
        }
        if (keys.isEmpty()) {
            return result;
        }
        MapGetAllRequest request = new MapGetAllRequest(name, keySet);
        MapEntrySet mapEntrySet = invoke(request);
        Set<Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
        for (Entry<Data, Data> dataEntry : entrySet) {
            final V value = (V) toObject(dataEntry.getValue());
            final K key = (K) toObject(dataEntry.getKey());
            result.put(key, value);
            if (nearCache != null) {
                nearCache.put(dataEntry.getKey(), value);
            }
        }
        return result;
    }

    @Override
    public Collection<V> values() {
        MapValuesRequest request = new MapValuesRequest(name);
        MapValueCollection mapValueCollection = invoke(request);
        Collection<Data> collectionData = mapValueCollection.getValues();
        Collection<V> collection = new ArrayList<V>(collectionData.size());
        for (Data data : collectionData) {
            final V value = toObject(data);
            collection.add(value);
        }
        return collection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        MapEntrySetRequest request = new MapEntrySetRequest(name);
        MapEntrySet result = invoke(request);
        Set<Entry<K, V>> entrySet = new HashSet<Entry<K, V>>();
        Set<Entry<Data, Data>> entries = result.getEntrySet();
        for (Entry<Data, Data> dataEntry : entries) {
            Data keyData = dataEntry.getKey();
            Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            V value = toObject(valueData);
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return entrySet;
    }

    @Override
    public Set<K> keySet(Predicate predicate) {
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(IterationType.KEY);

            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                keySet(pagingPredicate);
                pagingPredicate.nextPage();
            }
        }
        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.KEY);
        QueryResultSet result = invoke(request);
        if (pagingPredicate == null) {
            final HashSet<K> keySet = new HashSet<K>();
            for (Object o : result) {
                final K key = toObject(o);
                keySet.add(key);
            }
            return keySet;
        }


        final Comparator<Entry> comparator = SortingUtil.newComparator(pagingPredicate.getComparator(), IterationType.KEY);
        final SortedQueryResultSet sortedResult = new SortedQueryResultSet(comparator, IterationType.KEY,
                pagingPredicate.getPageSize());


        final Iterator<Entry> iterator = result.rawIterator();
        while (iterator.hasNext()) {
            final Entry entry = iterator.next();
            final K key = toObject(entry.getKey());
            final V value = toObject(entry.getValue());
            sortedResult.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
        }

        PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, sortedResult.last());

        return (Set<K>) sortedResult;
    }

    @Override
    public Set<Entry<K, V>> entrySet(Predicate predicate) {
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(IterationType.ENTRY);

            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                entrySet(pagingPredicate);
                pagingPredicate.nextPage();
            }
        }

        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.ENTRY);
        QueryResultSet result = invoke(request);
        Set entrySet;
        if (pagingPredicate == null) {
            entrySet = new HashSet<Entry<K, V>>(result.size());
        } else {
            entrySet = new SortedQueryResultSet(pagingPredicate.getComparator(), IterationType.ENTRY,
                    pagingPredicate.getPageSize());
        }
        for (Object data : result) {
            AbstractMap.SimpleImmutableEntry<Data, Data> dataEntry = (AbstractMap.SimpleImmutableEntry<Data, Data>) data;
            K key = toObject(dataEntry.getKey());
            V value = toObject(dataEntry.getValue());
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        if (pagingPredicate != null) {
            PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) entrySet).last());
        }
        return entrySet;
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(IterationType.VALUE);

            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                values(pagingPredicate);
                pagingPredicate.nextPage();
            }
        }
        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.VALUE);
        QueryResultSet result = invoke(request);

        if (pagingPredicate == null) {
            final ArrayList<V> values = new ArrayList<V>(result.size());
            for (Object data : result) {
                V value = toObject(data);
                values.add(value);
            }
            return values;
        }

        List<Entry<Object, V>> valueEntryList = new ArrayList<Entry<Object, V>>(result.size());
        final Iterator<Entry> iterator = result.rawIterator();
        while (iterator.hasNext()) {
            final Entry entry = iterator.next();
            K key = toObject(entry.getKey());
            V value = toObject(entry.getValue());
            valueEntryList.add(new AbstractMap.SimpleImmutableEntry<Object, V>(key, value));
        }

        Collections.sort(valueEntryList, SortingUtil.newComparator(pagingPredicate.getComparator(), IterationType.VALUE));
        if (valueEntryList.size() > pagingPredicate.getPageSize()) {
            valueEntryList = valueEntryList.subList(0, pagingPredicate.getPageSize());
        }
        Entry anchor = null;
        if (valueEntryList.size() != 0) {
            anchor = valueEntryList.get(valueEntryList.size() - 1);
        }
        PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, anchor);

        final ArrayList<V> values = new ArrayList<V>(valueEntryList.size());
        for (Entry<Object, V> objectVEntry : valueEntryList) {
            values.add(objectVEntry.getValue());
        }
        return values;
    }

    @Override
    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public Set<K> localKeySet(Predicate predicate) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public void addIndex(String attribute, boolean ordered) {
        MapAddIndexRequest request = new MapAddIndexRequest(name, attribute, ordered);
        invoke(request);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        initNearCache();
        LocalMapStatsImpl localMapStats = new LocalMapStatsImpl();
        if (nearCache != null) {
            localMapStats.setNearCacheStats(nearCache.getNearCacheStats());
        }
        return localMapStats;
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        final Data keyData = toData(key);
        MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);
        return invoke(request, keyData);
    }

    public void submitToKey(K key, EntryProcessor entryProcessor, final ExecutionCallback callback) {
        final Data keyData = toData(key);
        final MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);
        try {
            final ClientCallFuture future = (ClientCallFuture) getContext().getInvocationService().
                    invokeOnKeyOwner(request, keyData);
            future.andThen(callback);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public Future submitToKey(K key, EntryProcessor entryProcessor) {
        final Data keyData = toData(key);
        final MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);
        try {
            final ICompletableFuture future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
        MapExecuteOnAllKeysRequest request = new MapExecuteOnAllKeysRequest(name, entryProcessor);
        MapEntrySet entrySet = invoke(request);
        Map<K, Object> result = new HashMap<K, Object>();
        for (Entry<Data, Data> dataEntry : entrySet.getEntrySet()) {
            final Data keyData = dataEntry.getKey();
            final Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            result.put(key, toObject(valueData));
        }
        return result;
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
        MapExecuteWithPredicateRequest request = new MapExecuteWithPredicateRequest(name, entryProcessor, predicate);
        MapEntrySet entrySet = invoke(request);
        Map<K, Object> result = new HashMap<K, Object>();
        for (Entry<Data, Data> dataEntry : entrySet.getEntrySet()) {
            final Data keyData = dataEntry.getKey();
            final Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            result.put(key, toObject(valueData));
        }
        return result;
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {

        HazelcastInstance hazelcastInstance = getContext().getHazelcastInstance();
        JobTracker jobTracker = hazelcastInstance.getJobTracker("hz::aggregation-map-" + getName());
        return aggregate(supplier, aggregation, jobTracker);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation,
                                                    JobTracker jobTracker) {

        try {
            ValidationUtil.isNotNull(jobTracker, "jobTracker");
            KeyValueSource<K, V> keyValueSource = KeyValueSource.fromMap(this);
            Job<K, V> job = jobTracker.newJob(keyValueSource);
            Mapper mapper = aggregation.getMapper(supplier);
            CombinerFactory combinerFactory = aggregation.getCombinerFactory();
            ReducerFactory reducerFactory = aggregation.getReducerFactory();
            Collator collator = aggregation.getCollator();

            MappingJob mappingJob = job.mapper(mapper);
            ReducingSubmittableJob reducingJob;
            if (combinerFactory != null) {
                reducingJob = mappingJob.combiner(combinerFactory).reducer(reducerFactory);
            } else {
                reducingJob = mappingJob.reducer(reducerFactory);
            }

            ICompletableFuture<Result> future = reducingJob.submit(collator);
            return future.get();
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

    @Override
    public Map<K, Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor) {
        Set<Data> dataKeys = new HashSet<Data>(keys.size());
        for (K key : keys) {
            dataKeys.add(toData(key));
        }

        MapExecuteOnKeysRequest request = new MapExecuteOnKeysRequest(name, entryProcessor, dataKeys);
        MapEntrySet entrySet = invoke(request);
        Map<K, Object> result = new HashMap<K, Object>();
        for (Entry<Data, Data> dataEntry : entrySet.getEntrySet()) {
            final Data keyData = dataEntry.getKey();
            final Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            result.put(key, toObject(valueData));
        }
        return result;

    }

    @Override
    public void set(K key, V value) {
        set(key, value, -1, null);
    }

    @Override
    public int size() {
        MapSizeRequest request = new MapSizeRequest(name);
        Integer result = invoke(request);
        return result;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        MapEntrySet entrySet = new MapEntrySet();
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            final Data keyData = toData(entry.getKey());
            invalidateNearCache(keyData);
            entrySet.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(keyData, toData(entry.getValue())));
        }
        MapPutAllRequest request = new MapPutAllRequest(name, entrySet);
        invoke(request);
    }

    @Override
    public void clear() {
        MapClearRequest request = new MapClearRequest(name);
        invoke(request);
    }

    @Override
    protected void onDestroy() {
        destroyNearCache();
    }

    private void destroyNearCache() {
        if (nearCache != null) {
            nearCache.destroy();
        }
    }

    @Override
    protected void onShutdown() {
        destroyNearCache();
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private EventHandler<EventData> createHandler(final EntryListener<K, V> listener, final boolean includeValue) {
        return new EventHandler<EventData>() {
            public void handle(EventData eventData) {
                IMapEvent event = null;
                if (eventData instanceof PortableEntryEventData) {
                    event = handlePortableEntryEvent((PortableEntryEventData) eventData);
                } else if (eventData instanceof PortableMapEventData) {
                    event = handlePortableMapEvent((PortableMapEventData) eventData);
                }
                dispatchEvent(event, eventData);
            }

            private EntryEvent<K, V> handlePortableEntryEvent(PortableEntryEventData event) {
                V value = null;
                V oldValue = null;
                if (includeValue) {
                    value = toObject(event.getValue());
                    oldValue = toObject(event.getOldValue());
                }
                K key = toObject(event.getKey());
                Member member = getContext().getClusterService().getMember(event.getUuid());
                return new EntryEvent<K, V>(name, member,
                        event.getEventType().getType(), key, oldValue, value);
            }

            private MapEvent handlePortableMapEvent(PortableMapEventData event) {
                final Member member = getContext().getClusterService().getMember(event.getUuid());
                return new MapEvent(name, member, event.getEventType().getType(), event.getNumberOfEntriesAffected());
            }

            private void dispatchEvent(IMapEvent event, EventData eventData) {
                final AbstractPortableEventData portableEvent = (AbstractPortableEventData) eventData;
                switch (portableEvent.getEventType()) {
                    case ADDED:
                        listener.entryAdded((EntryEvent) event);
                        break;
                    case REMOVED:
                        listener.entryRemoved((EntryEvent) event);
                        break;
                    case UPDATED:
                        listener.entryUpdated((EntryEvent) event);
                        break;
                    case EVICTED:
                        listener.entryEvicted((EntryEvent) event);
                        break;
                    case EVICT_ALL:
                        listener.mapEvicted((MapEvent) event);
                        break;
                    default:
                        throw new IllegalArgumentException("Not a known event type " + portableEvent.getEventType());
                }
            }

            @Override
            public void onListenerRegister() {

            }
        };
    }

    private void invalidateNearCache(Data key) {
        if (nearCache != null) {
            nearCache.invalidate(key);
        }
    }

    private void invalidateNearCache(Collection<Data> keys) {
        if (nearCache != null) {
            nearCache.invalidate(keys);
        }
    }

    private void clearNearCache() {
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    private void initNearCache() {
        if (nearCacheInitialized.compareAndSet(false, true)) {
            final NearCacheConfig nearCacheConfig = getContext().getClientConfig().getNearCacheConfig(name);
            if (nearCacheConfig == null) {
                return;
            }
            ClientNearCache<Data> nearCacheInternal = new ClientNearCache<Data>(
                    name, ClientNearCacheType.Map, getContext(), nearCacheConfig);
            nearCache = nearCacheInternal;
        }
    }

    @Override
    public String toString() {
        return "IMap{" + "name='" + getName() + '\'' + '}';
    }

}
