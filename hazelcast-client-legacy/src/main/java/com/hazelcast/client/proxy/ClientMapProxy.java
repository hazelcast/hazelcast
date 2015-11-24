/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.client.MapAddEntryListenerRequest;
import com.hazelcast.map.impl.client.MapAddIndexRequest;
import com.hazelcast.map.impl.client.MapAddInterceptorRequest;
import com.hazelcast.map.impl.client.MapAddPartitionLostListenerRequest;
import com.hazelcast.map.impl.client.MapClearNearCacheRequest;
import com.hazelcast.map.impl.client.MapClearRequest;
import com.hazelcast.map.impl.client.MapContainsKeyRequest;
import com.hazelcast.map.impl.client.MapContainsValueRequest;
import com.hazelcast.map.impl.client.MapDeleteRequest;
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
import com.hazelcast.map.impl.client.MapLoadAllKeysRequest;
import com.hazelcast.map.impl.client.MapLoadGivenKeysRequest;
import com.hazelcast.map.impl.client.MapLockRequest;
import com.hazelcast.map.impl.client.MapPutAllRequest;
import com.hazelcast.map.impl.client.MapPutIfAbsentRequest;
import com.hazelcast.map.impl.client.MapPutRequest;
import com.hazelcast.map.impl.client.MapPutTransientRequest;
import com.hazelcast.map.impl.client.MapQueryRequest;
import com.hazelcast.map.impl.client.MapRemoveEntryListenerRequest;
import com.hazelcast.map.impl.client.MapRemoveIfSameRequest;
import com.hazelcast.map.impl.client.MapRemoveInterceptorRequest;
import com.hazelcast.map.impl.client.MapRemovePartitionLostListenerRequest;
import com.hazelcast.map.impl.client.MapRemoveRequest;
import com.hazelcast.map.impl.client.MapReplaceIfSameRequest;
import com.hazelcast.map.impl.client.MapReplaceRequest;
import com.hazelcast.map.impl.client.MapSetRequest;
import com.hazelcast.map.impl.client.MapSizeRequest;
import com.hazelcast.map.impl.client.MapTryPutRequest;
import com.hazelcast.map.impl.client.MapTryRemoveRequest;
import com.hazelcast.map.impl.client.MapUnlockRequest;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
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
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.spi.impl.PortableMapPartitionLostEvent;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.collection.InflatableSet;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.map.impl.MapListenerFlagOperator.setAndGetListenerFlags;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.SortingUtil.getSortedQueryResultSet;

public class ClientMapProxy<K, V> extends ClientProxy implements IMap<K, V> {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    protected final String name;

    public ClientMapProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        return containsKeyInternal(keyData);
    }

    protected boolean containsKeyInternal(Data keyData) {
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

        Data keyData = toData(key);
        return getInternal(keyData);

    }

    protected V getInternal(Data keyData) {
        MapGetRequest request = new MapGetRequest(name, keyData, ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1L, TimeUnit.MILLISECONDS);
    }

    @Override
    public V remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        return removeInternal(keyData);
    }

    protected V removeInternal(Data keyData) {
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

        Data keyData = toData(key);
        Data valueData = toData(value);
        return removeInternal(keyData, valueData);
    }

    protected boolean removeInternal(Data keyData, Data valueData) {
        MapRemoveIfSameRequest request = new MapRemoveIfSameRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    @Override
    public void delete(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        deleteInternal(keyData);
    }

    protected void deleteInternal(Data keyData) {
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

        Data keyData = toData(key);
        return getAsyncInternal(keyData);
    }

    protected ICompletableFuture<V> getAsyncInternal(Data keyData) {
        MapGetRequest request = new MapGetRequest(name, keyData, ThreadUtil.getThreadId());
        request.setAsAsync();
        try {
            ICompletableFuture future = invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture<V>(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private ICompletableFuture invokeOnKeyOwner(ClientRequest request, Data keyData) {
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);
        ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, partitionId);
        return clientInvocation.invoke();
    }

    @Override
    public Future<V> putAsync(final K key, final V value) {
        return putAsync(key, value, -1L, TimeUnit.MILLISECONDS);
    }

    @Override
    public Future<V> putAsync(final K key, final V value, final long ttl, final TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);

        return putAsyncInternal(ttl, timeunit, keyData, valueData);
    }

    protected Future<V> putAsyncInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        MapPutRequest request = new MapPutRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        request.setAsAsync();
        try {
            ICompletableFuture future = invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture<V>(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Future<V> removeAsync(final K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        return removeAsyncInternal(keyData);
    }

    protected Future<V> removeAsyncInternal(Data keyData) {
        MapRemoveRequest request = new MapRemoveRequest(name, keyData, ThreadUtil.getThreadId());
        request.setAsAsync();
        try {
            ICompletableFuture future = invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture<V>(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        return tryRemoveInternal(timeout, timeunit, keyData);
    }

    protected Boolean tryRemoveInternal(long timeout, TimeUnit timeunit, Data keyData) {
        MapTryRemoveRequest request = new MapTryRemoveRequest(name, keyData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        return invoke(request, keyData);
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        return tryPutInternal(timeout, timeunit, keyData, valueData);
    }

    protected Boolean tryPutInternal(long timeout, TimeUnit timeunit, Data keyData, Data valueData) {
        MapTryPutRequest request = new MapTryPutRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        return invoke(request, keyData);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);

        return putInternal(ttl, timeunit, keyData, valueData);
    }

    protected V putInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        MapPutRequest request = new MapPutRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);

        putTransientInternal(ttl, timeunit, keyData, valueData);
    }

    protected void putTransientInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
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

        Data keyData = toData(key);
        Data valueData = toData(value);
        return putIfAbsentInternal(ttl, timeunit, keyData, valueData);
    }

    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        MapPutIfAbsentRequest request = new MapPutIfAbsentRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        return invoke(request, keyData);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(oldValue, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(newValue, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);
        return replaceIfSameInternal(keyData, oldValueData, newValueData);
    }

    protected Boolean replaceIfSameInternal(Data keyData, Data oldValueData, Data newValueData) {
        MapReplaceIfSameRequest request = new MapReplaceIfSameRequest(name, keyData, oldValueData, newValueData,
                ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public V replace(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);

        return replaceInternal(keyData, valueData);
    }

    protected V replaceInternal(Data keyData, Data valueData) {
        MapReplaceRequest request = new MapReplaceRequest(name, keyData, valueData,
                ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);

        setInternal(ttl, timeunit, keyData, valueData);
    }

    protected void setInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        MapSetRequest request = new MapSetRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request, keyData);
    }

    @Override
    public void lock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        MapLockRequest request = new MapLockRequest(name, keyData,
                ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit), -1);
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
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
        return tryLock(key, time, timeunit, -1, null);
    }

    @Override
    public boolean tryLock(K key, long timeout, TimeUnit timeunit,
                           long leaseTime, TimeUnit leaseTimeunit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        long timeoutInMillis = getTimeInMillis(timeout, timeunit);
        long leaseTimeInMillis = getTimeInMillis(leaseTime, leaseTimeunit);
        MapLockRequest request = new MapLockRequest(name, keyData, ThreadUtil.getThreadId(), leaseTimeInMillis, timeoutInMillis);
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
    public String addLocalEntryListener(MapListener listener) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String addLocalEntryListener(EntryListener listener) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String addLocalEntryListener(MapListener listener,
                                        Predicate<K, V> predicate, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String addLocalEntryListener(MapListener listener,
                                        Predicate<K, V> predicate, K key, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
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
    public String addEntryListener(MapListener listener, boolean includeValue) {
        return addEntryListenerInternal(listener, null, includeValue, null);
    }

    @Override
    public String addEntryListener(EntryListener listener, boolean includeValue) {
        return addEntryListenerInternal(listener, null, includeValue, null);
    }

    @Override
    public boolean removeEntryListener(String id) {
        return deregisterListener(id);
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener) {
        MapAddPartitionLostListenerRequest addRequest = new MapAddPartitionLostListenerRequest(name);
        MapRemovePartitionLostListenerRequest removeRequest = new MapRemovePartitionLostListenerRequest(name);
        EventHandler<PortableMapPartitionLostEvent> handler = new ClientMapPartitionLostEventHandler(listener);
        return registerListener(addRequest, removeRequest, handler);
    }

    @Override
    public boolean removePartitionLostListener(String id) {
        return deregisterListener(id);
    }

    @Override
    public String addEntryListener(MapListener listener, K key, boolean includeValue) {
        return addEntryListenerInternal(listener, null, includeValue, key);
    }

    @Override
    public String addEntryListener(EntryListener listener, K key, boolean includeValue) {
        return addEntryListenerInternal(listener, null, includeValue, key);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return addEntryListenerInternal(listener, predicate, includeValue, key);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return addEntryListenerInternal(listener, predicate, includeValue, key);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return addEntryListenerInternal(listener, predicate, includeValue, null);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return addEntryListenerInternal(listener, predicate, includeValue, null);
    }

    private String addEntryListenerInternal(Object listener, Predicate<K, V> predicate, boolean includeValue, K key) {
        Data keyData = toData(key);
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        MapAddEntryListenerRequest addRequest
                = new MapAddEntryListenerRequest(name, keyData, includeValue, predicate, listenerFlags);
        EventHandler<PortableEntryEvent> handler = createHandler(listenerAdaptor);
        MapRemoveEntryListenerRequest removeRequest = new MapRemoveEntryListenerRequest(name);

        return registerListener(addRequest, removeRequest, handler);
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

        Data keyData = toData(key);
        return evictInternal(keyData);
    }

    protected Boolean evictInternal(Data keyData) {
        MapEvictRequest request = new MapEvictRequest(name, keyData, ThreadUtil.getThreadId());
        return invoke(request);
    }

    @Override
    public void evictAll() {
        MapEvictAllRequest request = new MapEvictAllRequest(name);
        invoke(request);

        clearNearCachesOnLiteMembers();
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        MapLoadAllKeysRequest request = new MapLoadAllKeysRequest(name, replaceExistingValues);
        invoke(request);
    }

    @Override
    public void loadAll(Set<K> keys, boolean replaceExistingValues) {
        checkNotNull(keys, "Parameter keys should not be null.");

        if (keys.isEmpty()) {
            return;
        }
        List<Data> dataKeys = convertKeysToData(keys);
        loadAllInternal(replaceExistingValues, dataKeys);
    }

    protected void loadAllInternal(boolean replaceExistingValues, List<Data> dataKeys) {
        MapLoadGivenKeysRequest request = new MapLoadGivenKeysRequest(name, dataKeys, replaceExistingValues);
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
    @SuppressWarnings("unchecked")
    public Map<K, V> getAll(Set<K> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            return Collections.emptyMap();
        }

        Map<Integer, List<Data>> partitionToKeyData = new HashMap<Integer, List<Data>>();
        ClientPartitionService partitionService = getContext().getPartitionService();

        for (Object key : keys) {
            Data keyData = toData(key);
            int partitionId = partitionService.getPartitionId(keyData);
            List<Data> keyList = partitionToKeyData.get(partitionId);
            if (keyList == null) {
                keyList = new ArrayList<Data>();
                partitionToKeyData.put(partitionId, keyList);
            }
            keyList.add(keyData);
        }

        Map<K, V> result = new HashMap<K, V>();
        getAllInternal(partitionToKeyData, result);
        return result;
    }

    // This method is overriden.
    protected List<MapEntries> getAllInternal(Map<Integer, List<Data>> partitionToKeyData, Map<K, V> result) {
        List<Future<Data>> futures = new ArrayList<Future<Data>>(partitionToKeyData.size());
        List<MapEntries> responses = new ArrayList<MapEntries>(partitionToKeyData.size());

        for (final Map.Entry<Integer, List<Data>> entry : partitionToKeyData.entrySet()) {
            int partitionId = entry.getKey();
            List<Data> keyList = entry.getValue();
            MapGetAllRequest request = new MapGetAllRequest(name, keyList, partitionId);
            futures.add(new ClientInvocation(getClient(), request, partitionId).invoke());
        }

        for (Future<Data> future : futures) {
            try {
                MapEntries entries = toObject(future.get());
                for (Entry<Data, Data> entry : entries.entries()) {
                    final V value = toObject(entry.getValue());
                    final K key = toObject(entry.getKey());
                    result.put(key, value);
                }
                responses.add(entries);
            } catch (Exception e) {
                ExceptionUtil.rethrow(e);
            }
        }
        return responses;
    }

    @Override
    public Collection<V> values() {
        // we pass null instead of TruePredicate.INSTANCE due to security. But null will be interpreted as TruePredicate.
        return values(null);
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            return valuesForPagingPredicate((PagingPredicate) predicate);
        }

        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.VALUE);
        QueryResult result = invoke(request);

        List<V> values = new ArrayList<V>(result.size());
        for (QueryResultRow row : result) {
            values.add((V) toObject(row.getValue()));
        }
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        // we pass null instead of TruePredicate.INSTANCE due to security. But null will be interpreted as TruePredicate.
        return entrySet(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Entry<K, V>> entrySet(Predicate predicate) {
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(IterationType.ENTRY);
        }

        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.ENTRY);
        QueryResult result = invoke(request);
        if (pagingPredicate == null) {
            SerializationService serializationService = getContext().getSerializationService();
            InflatableSet.Builder<Entry<K, V>> setBuilder = InflatableSet.newBuilder(result.size());
            for (QueryResultRow row : result) {
                LazyMapEntry entry = new LazyMapEntry(row.getKey(), row.getValue(), serializationService);
                setBuilder.add(entry);
            }
            return setBuilder.build();
        }
        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        for (QueryResultRow data : result) {
            K key = toObject(data.getKey());
            V value = toObject(data.getValue());
            resultList.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return (Set) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.ENTRY);
    }

    @Override
    public Set<K> keySet() {
        // we pass null instead of TruePredicate.INSTANCE due to security. But null will be interpreted as TruePredicate.
        return keySet(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate predicate) {
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(IterationType.KEY);
        }
        MapQueryRequest request = new MapQueryRequest(name, predicate, IterationType.KEY);
        QueryResult result = invoke(request);
        if (pagingPredicate == null) {
            InflatableSet.Builder<K> setBuilder = InflatableSet.newBuilder(result.size());
            for (QueryResultRow row : result) {
                K key = toObject(row.getKey());
                setBuilder.add(key);
            }
            return setBuilder.build();
        }

        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>(result.size());
        for (QueryResultRow row : result) {
            K key = toObject(row.getKey());
            resultList.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, null));
        }
        return (Set<K>) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.KEY);
    }

    private Collection<V> valuesForPagingPredicate(PagingPredicate pagingPredicate) {
        pagingPredicate.setIterationType(IterationType.ENTRY);

        MapQueryRequest request = new MapQueryRequest(name, pagingPredicate, IterationType.ENTRY);
        QueryResult result = invoke(request);

        List<Entry> resultList = new ArrayList<Entry>(result.size());
        for (QueryResultRow row : result) {
            K key = toObject(row.getKey());
            V value = toObject(row.getValue());
            resultList.add(new AbstractMap.SimpleImmutableEntry<Object, V>(key, value));
        }

        return (Collection) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.VALUE);
    }

    @Override
    public Set<K> localKeySet() {
        return localKeySet(TruePredicate.INSTANCE);
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
        return new LocalMapStatsImpl();
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData, ThreadUtil.getThreadId());
        return invoke(request, keyData);
    }

    public void submitToKey(K key, EntryProcessor entryProcessor, final ExecutionCallback callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData, ThreadUtil.getThreadId());
        request.setAsSubmitToKey();
        try {
            final ICompletableFuture future = invokeOnKeyOwner(request, keyData);
            future.andThen(callback);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public Future submitToKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final MapExecuteOnKeyRequest request = new MapExecuteOnKeyRequest(name, entryProcessor, keyData, ThreadUtil.getThreadId());
        request.setAsSubmitToKey();
        try {
            final ICompletableFuture future = invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
        MapExecuteOnAllKeysRequest request = new MapExecuteOnAllKeysRequest(name, entryProcessor);
        MapEntries mapEntries = invoke(request);
        return prepareResult(mapEntries);
    }

    protected Map<K, Object> prepareResult(MapEntries mapEntries) {
        Map<K, Object> result = createHashMap(mapEntries.size());
        for (Entry<Data, Data> dataEntry : mapEntries) {
            Data keyData = dataEntry.getKey();
            Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            result.put(key, toObject(valueData));
        }
        return result;
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
        MapExecuteWithPredicateRequest request = new MapExecuteWithPredicateRequest(name, entryProcessor, predicate);
        MapEntries mapEntries = invoke(request);
        return prepareResult(mapEntries);
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
            Preconditions.isNotNull(jobTracker, "jobTracker");
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
        MapEntries entrySet = invoke(request);
        return prepareResult(entrySet);

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
        ClientPartitionService partitionService = getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        List<Future<?>> futures = new ArrayList<Future<?>>(partitionCount);
        MapEntries[] entriesPerPartition = new MapEntries[partitionCount];

        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
            checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

            Data keyData = toData(entry.getKey());

            int partitionId = partitionService.getPartitionId(keyData);
            MapEntries entries = entriesPerPartition[partitionId];
            if (entries == null) {
                entries = new MapEntries();
                entriesPerPartition[partitionId] = entries;
            }
            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(keyData, toData(entry.getValue())));
        }

        putAllInternal(futures, entriesPerPartition);
    }

    protected void putAllInternal(List<Future<?>> futures, MapEntries[] entriesPerPartition) {
        for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
            MapEntries entries = entriesPerPartition[partitionId];
            if (entries != null) {
                //If there is only one entry, consider how we can use MapPutRequest
                //without having to get back the return value.
                MapPutAllRequest request = new MapPutAllRequest(name, entries, partitionId);
                futures.add(new ClientInvocation(getClient(), request, partitionId).invoke());
            }
        }

        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public void clear() {
        MapClearRequest request = new MapClearRequest(name);
        invoke(request);

        clearNearCachesOnLiteMembers();
    }

    private void clearNearCachesOnLiteMembers() {
        final ClientClusterService clusterService = getClient().getClientClusterService();
        for (Member member : clusterService.getMembers(LITE_MEMBER_SELECTOR)) {
            final MapClearNearCacheRequest request = new MapClearNearCacheRequest(name, member.getAddress());
            invoke(request, member.getAddress());
        }
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private EventHandler<PortableEntryEvent> createHandler(final ListenerAdapter listenerAdapter) {
        return new ClientMapEventHandler(listenerAdapter);
    }

    @Override
    public String toString() {
        return "IMap{" + "name='" + getName() + '\'' + '}';
    }

    private class ClientMapEventHandler implements EventHandler<PortableEntryEvent> {

        private final ListenerAdapter listenerAdapter;

        public ClientMapEventHandler(ListenerAdapter listenerAdapter) {
            this.listenerAdapter = listenerAdapter;
        }

        public void handle(PortableEntryEvent event) {
            Member member = getContext().getClusterService().getMember(event.getUuid());
            final IMapEvent iMapEvent = createIMapEvent(event, member);
            listenerAdapter.onEvent(iMapEvent);
        }

        private IMapEvent createIMapEvent(PortableEntryEvent event, Member member) {
            IMapEvent iMapEvent;
            switch (event.getEventType()) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                case EXPIRED:
                case MERGED:
                    iMapEvent = createEntryEvent(event, member);
                    break;
                case EVICT_ALL:
                case CLEAR_ALL:
                    iMapEvent = createMapEvent(event, member);
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + event.getEventType());
            }

            return iMapEvent;
        }

        private MapEvent createMapEvent(PortableEntryEvent event, Member member) {
            return new MapEvent(name, member, event.getEventType().getType(), event.getNumberOfAffectedEntries());
        }

        private EntryEvent<K, V> createEntryEvent(PortableEntryEvent event, Member member) {
            EntryEventType eventType = event.getEventType();
            Data keyData = event.getKey();
            Data valueData = event.getValue();
            Data oldValueData = event.getOldValue();
            Data mergingValueData = event.getMergingValue();
            return new DataAwareEntryEvent(member, eventType.getType(), name, keyData, valueData,
                    oldValueData, mergingValueData, getContext().getSerializationService());
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    private class ClientMapPartitionLostEventHandler implements EventHandler<PortableMapPartitionLostEvent> {

        private MapPartitionLostListener listener;

        public ClientMapPartitionLostEventHandler(MapPartitionLostListener listener) {
            this.listener = listener;
        }

        @Override
        public void handle(PortableMapPartitionLostEvent event) {
            final Member member = getContext().getClusterService().getMember(event.getUuid());
            listener.partitionLost(new MapPartitionLostEvent(name, member, -1, event.getPartitionId()));
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {

        }
    }

}
