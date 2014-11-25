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

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.ClientNearCache;
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
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapKeySet;
import com.hazelcast.map.impl.MapValueCollection;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.client.MapAddEntryListenerRequest;
import com.hazelcast.map.impl.client.MapAddIndexRequest;
import com.hazelcast.map.impl.client.MapAddInterceptorRequest;
import com.hazelcast.map.impl.client.MapClearRequest;
import com.hazelcast.map.impl.client.MapContainsKeyRequest;
import com.hazelcast.map.impl.client.MapContainsValueRequest;
import com.hazelcast.map.impl.client.MapDeleteRequest;
import com.hazelcast.map.impl.client.MapEntrySetRequest;
import com.hazelcast.map.impl.client.MapEvictAllRequest;
import com.hazelcast.map.impl.client.MapEvictRequest;
import com.hazelcast.map.impl.client.MapExecuteOnAllKeysRequest;
import com.hazelcast.map.impl.client.MapExecuteOnKeyRequest;
import com.hazelcast.map.impl.client.MapExecuteOnKeysRequest;
import com.hazelcast.map.impl.client.MapExecuteWithPredicateRequest;
import com.hazelcast.map.impl.client.MapFlushRequest;
import com.hazelcast.map.impl.client.MapGetAllRequest;
import com.hazelcast.map.impl.client.MapGetEntryViewRequest;
import com.hazelcast.map.impl.client.MapGetRequest;
import com.hazelcast.map.impl.client.MapIsEmptyRequest;
import com.hazelcast.map.impl.client.MapIsLockedRequest;
import com.hazelcast.map.impl.client.MapKeySetRequest;
import com.hazelcast.map.impl.client.MapLoadAllKeysRequest;
import com.hazelcast.map.impl.client.MapLoadGivenKeysRequest;
import com.hazelcast.map.impl.client.MapLockRequest;
import com.hazelcast.map.impl.client.MapAddNearCacheEntryListenerRequest;
import com.hazelcast.map.impl.client.MapPutAllRequest;
import com.hazelcast.map.impl.client.MapPutIfAbsentRequest;
import com.hazelcast.map.impl.client.MapPutRequest;
import com.hazelcast.map.impl.client.MapPutTransientRequest;
import com.hazelcast.map.impl.client.MapQueryRequest;
import com.hazelcast.map.impl.client.MapRemoveEntryListenerRequest;
import com.hazelcast.map.impl.client.MapRemoveIfSameRequest;
import com.hazelcast.map.impl.client.MapRemoveInterceptorRequest;
import com.hazelcast.map.impl.client.MapRemoveRequest;
import com.hazelcast.map.impl.client.MapReplaceIfSameRequest;
import com.hazelcast.map.impl.client.MapReplaceRequest;
import com.hazelcast.map.impl.client.MapSetRequest;
import com.hazelcast.map.impl.client.MapSizeRequest;
import com.hazelcast.map.impl.client.MapTryPutRequest;
import com.hazelcast.map.impl.client.MapTryRemoveRequest;
import com.hazelcast.map.impl.client.MapUnlockRequest;
import com.hazelcast.map.impl.client.MapValuesRequest;
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
import com.hazelcast.spi.impl.PortableEntryEvent;
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

import static com.hazelcast.util.ValidationUtil.checkNotNull;

public final class ClientMapProxy<K, V> extends ClientProxy implements IMap<K, V> {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private final String name;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();
    private volatile ClientHeapNearCache<Data> nearCache;

    public ClientMapProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

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
        MapContainsKeyRequest request = new MapContainsKeyRequest(name, keyData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        MapContainsValueRequest request = new MapContainsValueRequest(name, valueData);
        Boolean result = invoke(request);
        return result;
    }

    @Override
    public V get(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        initNearCache();

        final Data keyData = toData(key);
        if (nearCache != null) {
            Object cached = nearCache.get(keyData);
            if (cached != null) {
                if (cached.equals(ClientHeapNearCache.NULL_OBJECT)) {
                    return null;
                }
                return (V) cached;
            }
        }
        MapGetRequest request = new MapGetRequest(name, keyData, ThreadUtil.getThreadId());
        final V result = invoke(request, keyData);
        if (nearCache != null) {
            nearCache.put(keyData, result);
        }
        return result;
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        MapRemoveRequest request = new MapRemoveRequest(name, keyData, ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        // I do not why but findbugs does not like this null check:
        // value must be nonnull but is marked as nullable ["com.hazelcast.client.proxy.ClientMapProxy"]
        // At ClientMapProxy.java:[lines 131-1253]
        // checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapRemoveIfSameRequest request = new MapRemoveIfSameRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public void delete(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
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
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        initNearCache();
        final Data keyData = toData(key);
        if (nearCache != null) {
            Object cached = nearCache.get(keyData);
            if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
                return new CompletedFuture(getContext().getSerializationService(),
                        cached, getContext().getExecutionService().getAsyncExecutor());
            }
        }

        final MapGetRequest request = new MapGetRequest(name, keyData, ThreadUtil.getThreadId());
        request.setAsAsync();
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
        return putAsync(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public Future<V> putAsync(final K key, final V value, final long ttl, final TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapPutRequest request = new MapPutRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        request.setAsAsync();
        try {
            final ICompletableFuture future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture<V>(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Future<V> removeAsync(final K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        MapRemoveRequest request = new MapRemoveRequest(name, keyData, ThreadUtil.getThreadId());
        request.setAsAsync();
        try {
            final ICompletableFuture future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture<V>(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        MapTryRemoveRequest request = new MapTryRemoveRequest(name, keyData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
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
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapPutRequest request = new MapPutRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapPutTransientRequest request = new MapPutTransientRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapPutIfAbsentRequest request = new MapPutIfAbsentRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(oldValue, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(newValue, NULL_VALUE_IS_NOT_ALLOWED);

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
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapReplaceRequest request = new MapReplaceRequest(name, keyData, valueData,
                ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        MapSetRequest request = new MapSetRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request, keyData);
    }

    @Override
    public void lock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData,
                ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit), -1);
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
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
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData,
                ThreadUtil.getThreadId(), Long.MAX_VALUE, getTimeInMillis(time, timeunit));
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        MapUnlockRequest request = new MapUnlockRequest(name, keyData, ThreadUtil.getThreadId(), false);
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
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
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
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
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        final Data keyData = toData(key);
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, keyData, includeValue, predicate);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, boolean includeValue) {
        MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(name, null, includeValue, predicate);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, null, handler);
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        MapGetEntryViewRequest request = new MapGetEntryViewRequest(name, keyData, ThreadUtil.getThreadId());
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
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
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
        checkNotNull(keys, "Parameter keys should not be null.");
        if (keys.isEmpty()) {
            return;
        }
        final List<Data> dataKeys = convertKeysToData(keys);
        if (replaceExistingValues) {
            invalidateNearCache(dataKeys);
        }
        final MapLoadGivenKeysRequest request = new MapLoadGivenKeysRequest(name, dataKeys, replaceExistingValues);
        invoke(request);
    }

    // todo duplicate code.
    private <K> List<Data> convertKeysToData(Set<K> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Data> dataKeys = new ArrayList<Data>(keys.size());
        for (K key : keys) {
            checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

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
                if (cached != null && !ClientHeapNearCache.NULL_OBJECT.equals(cached)) {
                    result.put((K) toObject(key), (V) cached);
                    iterator.remove();
                }
            }
        }
        if (keySet.isEmpty()) {
            return result;
        }
        MapGetAllRequest request = new MapGetAllRequest(name, keySet);
        MapEntrySet mapEntrySet = invoke(request);
        Set<Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
        for (Entry<Data, Data> dataEntry : entrySet) {
            final V value = toObject(dataEntry.getValue());
            final K key = toObject(dataEntry.getKey());
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
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);
        return invoke(request, keyData);
    }

    public void submitToKey(K key, EntryProcessor entryProcessor, final ExecutionCallback callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);
        request.setAsSubmitToKey();
        try {
            final ClientCallFuture future = (ClientCallFuture) getContext().getInvocationService().
                    invokeOnKeyOwner(request, keyData);
            future.andThen(callback);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public Future submitToKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData);
        request.setAsSubmitToKey();
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
        set(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public int size() {
        MapSizeRequest request = new MapSizeRequest(name);
        Integer result = invoke(request);
        return result;
    }

    @Override
    public boolean isEmpty() {
        MapIsEmptyRequest request = new MapIsEmptyRequest(name);
        Boolean result = invoke(request);
        return result;
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
        invalidateNearCache();
        invoke(request);
    }

    @Override
    protected void onDestroy() {
        destroyNearCache();
    }

    private void destroyNearCache() {
        if (nearCache != null) {
            removeNearCacheInvalidationListener();
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

    private EventHandler<PortableEntryEvent> createHandler(final EntryListener<K, V> listener, final boolean includeValue) {
        return new EventHandler<PortableEntryEvent>() {
            public void handle(PortableEntryEvent event) {
                Member member = getContext().getClusterService().getMember(event.getUuid());
                switch (event.getEventType()) {
                    case ADDED:
                        listener.entryAdded(createEntryEvent(event, member));
                        break;
                    case REMOVED:
                        listener.entryRemoved(createEntryEvent(event, member));
                        break;
                    case UPDATED:
                        listener.entryUpdated(createEntryEvent(event, member));
                        break;
                    case EVICTED:
                        listener.entryEvicted(createEntryEvent(event, member));
                        break;
                    case EVICT_ALL:
                        listener.mapEvicted(createMapEvent(event, member));
                        break;
                    case CLEAR_ALL:
                        listener.mapCleared(createMapEvent(event, member));
                        break;
                    default:
                        throw new IllegalArgumentException("Not a known event type " + event.getEventType());
                }
            }

            private MapEvent createMapEvent(PortableEntryEvent event, Member member) {
                return new MapEvent(name, member, event.getEventType().getType(), event.getNumberOfAffectedEntries());
            }

            private EntryEvent<K, V> createEntryEvent(PortableEntryEvent event, Member member) {
                V value = null;
                V oldValue = null;
                if (includeValue) {
                    value = toObject(event.getValue());
                    oldValue = toObject(event.getOldValue());
                }
                K key = toObject(event.getKey());
                return new EntryEvent<K, V>(name, member,
                        event.getEventType().getType(), key, oldValue, value);
            }

            @Override
            public void beforeListenerRegister() {
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

    private void invalidateNearCache() {
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    private void invalidateNearCache(Collection<Data> keys) {
        if (nearCache != null) {
            if (keys == null || keys.isEmpty()) {
                return;
            }
            for (Data key : keys) {
                nearCache.invalidate(key);
            }
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

            nearCache = new ClientHeapNearCache<Data>(name, getContext(), nearCacheConfig);
            if (nearCache.isInvalidateOnChange()) {
                addNearCacheInvalidateListener();
            }
        }
    }

    private void addNearCacheInvalidateListener() {
        try {
            ClientRequest request = new MapAddNearCacheEntryListenerRequest(name, false);
            EventHandler handler = new EventHandler<PortableEntryEvent>() {
                @Override
                public void handle(PortableEntryEvent event) {
                    switch (event.getEventType()) {
                        case ADDED:
                        case REMOVED:
                        case UPDATED:
                        case EVICTED:
                            final Data key = event.getKey();
                            nearCache.remove(key);
                            break;
                        case CLEAR_ALL:
                        case EVICT_ALL:
                            nearCache.clear();
                            break;
                        default:
                            throw new IllegalArgumentException("Not a known event type " + event.getEventType());
                    }
                }

                @Override
                public void beforeListenerRegister() {
                    nearCache.clear();
                }

                @Override
                public void onListenerRegister() {
                    nearCache.clear();
                }
            };

            String registrationId = getContext().getListenerService().listen(request, null, handler);
            nearCache.setId(registrationId);
        } catch (Exception e) {
            Logger.getLogger(ClientHeapNearCache.class).severe(
                    "-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }

    private void removeNearCacheInvalidationListener() {
        if (nearCache != null && nearCache.getId() != null) {
            String registrationId = nearCache.getId();
            BaseClientRemoveListenerRequest request = new MapRemoveEntryListenerRequest(name, registrationId);
            getContext().getListenerService().stopListening(request, registrationId);
        }
    }

    @Override
    public String toString() {
        return "IMap{" + "name='" + getName() + '\'' + '}';
    }

}
