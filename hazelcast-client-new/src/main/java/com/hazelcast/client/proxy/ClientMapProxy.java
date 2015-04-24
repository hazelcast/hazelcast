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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.DataCollectionResultParameters;
import com.hazelcast.client.impl.protocol.parameters.DataEntryListResultParameters;
import com.hazelcast.client.impl.protocol.parameters.EntryEventParameters;
import com.hazelcast.client.impl.protocol.parameters.EntryViewParameters;
import com.hazelcast.client.impl.protocol.parameters.GenericResultParameters;
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddEntryListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddEntryListenerToKeyParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddEntryListenerToKeyWithPredicateParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddEntryListenerWithPredicateParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddIndexParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddInterceptorParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddNearCacheEntryListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.MapAddPartitionLostListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.MapClearParameters;
import com.hazelcast.client.impl.protocol.parameters.MapContainsKeyParameters;
import com.hazelcast.client.impl.protocol.parameters.MapContainsValueParameters;
import com.hazelcast.client.impl.protocol.parameters.MapDeleteParameters;
import com.hazelcast.client.impl.protocol.parameters.MapEntriesWithPredicateParameters;
import com.hazelcast.client.impl.protocol.parameters.MapEntrySetParameters;
import com.hazelcast.client.impl.protocol.parameters.MapEvictAllParameters;
import com.hazelcast.client.impl.protocol.parameters.MapEvictParameters;
import com.hazelcast.client.impl.protocol.parameters.MapExecuteOnAllKeysParameters;
import com.hazelcast.client.impl.protocol.parameters.MapExecuteOnKeyParameters;
import com.hazelcast.client.impl.protocol.parameters.MapExecuteOnKeysParameters;
import com.hazelcast.client.impl.protocol.parameters.MapExecuteWithPredicateParameters;
import com.hazelcast.client.impl.protocol.parameters.MapFlushParameters;
import com.hazelcast.client.impl.protocol.parameters.MapForceUnlockParameters;
import com.hazelcast.client.impl.protocol.parameters.MapGetAllParameters;
import com.hazelcast.client.impl.protocol.parameters.MapGetAsyncParameters;
import com.hazelcast.client.impl.protocol.parameters.MapGetEntryViewParameters;
import com.hazelcast.client.impl.protocol.parameters.MapGetParameters;
import com.hazelcast.client.impl.protocol.parameters.MapIsEmptyParameters;
import com.hazelcast.client.impl.protocol.parameters.MapIsLockedParameters;
import com.hazelcast.client.impl.protocol.parameters.MapKeySetParameters;
import com.hazelcast.client.impl.protocol.parameters.MapKeySetWithPredicateParameters;
import com.hazelcast.client.impl.protocol.parameters.MapLoadAllParameters;
import com.hazelcast.client.impl.protocol.parameters.MapLoadGivenKeysParameters;
import com.hazelcast.client.impl.protocol.parameters.MapLockParameters;
import com.hazelcast.client.impl.protocol.parameters.MapPutAllParameters;
import com.hazelcast.client.impl.protocol.parameters.MapPutAsyncParameters;
import com.hazelcast.client.impl.protocol.parameters.MapPutIfAbsentParameters;
import com.hazelcast.client.impl.protocol.parameters.MapPutParameters;
import com.hazelcast.client.impl.protocol.parameters.MapPutTransientParameters;
import com.hazelcast.client.impl.protocol.parameters.MapRemoveAsyncParameters;
import com.hazelcast.client.impl.protocol.parameters.MapRemoveEntryListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.MapRemoveIfSameParameters;
import com.hazelcast.client.impl.protocol.parameters.MapRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.MapReplaceIfSameParameters;
import com.hazelcast.client.impl.protocol.parameters.MapReplaceParameters;
import com.hazelcast.client.impl.protocol.parameters.MapSetParameters;
import com.hazelcast.client.impl.protocol.parameters.MapSizeParameters;
import com.hazelcast.client.impl.protocol.parameters.MapSubmitToKeyParameters;
import com.hazelcast.client.impl.protocol.parameters.MapTryLockParameters;
import com.hazelcast.client.impl.protocol.parameters.MapTryPutParameters;
import com.hazelcast.client.impl.protocol.parameters.MapTryRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.MapUnlockParameters;
import com.hazelcast.client.impl.protocol.parameters.MapValuesParameters;
import com.hazelcast.client.impl.protocol.parameters.MapValuesWithPredicateParameters;
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.config.NearCacheConfig;
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
import com.hazelcast.logging.Logger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.SimpleEntryView;
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
import com.hazelcast.query.PagingPredicateAccessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.PortableMapPartitionLostEvent;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.SortedQueryResultSet;
import com.hazelcast.util.SortingUtil;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.Preconditions;
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

import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.util.Preconditions.checkNotNull;

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
        ClientMessage message = MapContainsKeyParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage result = invoke(message, keyData);
        BooleanResultParameters resultParameters =
                BooleanResultParameters.decode((ClientMessage) result);
        return resultParameters.result;
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        ClientMessage request = MapContainsValueParameters.encode(name, valueData);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
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
        ClientMessage request = MapGetParameters.encode(name, keyData, ThreadUtil.getThreadId());

        ClientMessage response = invoke(request, keyData);
        GenericResultParameters resultParameters = GenericResultParameters.decode(response);
        V result = toObject(resultParameters.result);
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

        ClientMessage request = MapRemoveParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        GenericResultParameters resultParameters = GenericResultParameters.decode(response);
        return toObject(resultParameters.result);
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

        final ClientMessage request = MapRemoveIfSameParameters.encode(name, keyData,
                valueData, ThreadUtil.getThreadId());

        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    @Override
    public void delete(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        final ClientMessage request = MapDeleteParameters.encode(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void flush() {
        ClientMessage request = MapFlushParameters.encode(name);
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

        final ClientMessage request = MapGetAsyncParameters.encode(name, keyData, ThreadUtil.getThreadId());
        try {
            final ICompletableFuture future = invokeOnKeyOwner(request, keyData);
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

    private ICompletableFuture invokeOnKeyOwner(ClientMessage request, Data keyData) {
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);
        final ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, partitionId);
        return clientInvocation.invoke();
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
        ClientMessage request = MapPutAsyncParameters.encode(name, keyData,
                valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        try {
            final ICompletableFuture future = invokeOnKeyOwner(request, keyData);
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
        ClientMessage request = MapRemoveAsyncParameters.encode(name, keyData, ThreadUtil.getThreadId());
        try {
            final ICompletableFuture future = invokeOnKeyOwner(request, keyData);
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
        ClientMessage request = MapTryRemoveParameters.encode(name, keyData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);

        ClientMessage request = MapTryPutParameters.encode(name, keyData, valueData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);

        ClientMessage putMessage = MapPutParameters.encode(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        ClientMessage request = invoke(putMessage, keyData);
        GenericResultParameters resultParameters = GenericResultParameters.decode(request);
        return toObject(resultParameters.result);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        ClientMessage request = MapPutTransientParameters.encode(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        invoke(request, keyData);
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
        ClientMessage request = MapPutIfAbsentParameters.encode(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        ClientMessage result = invoke(request, keyData);
        GenericResultParameters resultParameters = GenericResultParameters.decode(result);
        return toObject(resultParameters.result);
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

        ClientMessage request = MapReplaceIfSameParameters.encode(name, keyData, oldValueData, newValueData,
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    @Override
    public V replace(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        ClientMessage request = MapReplaceParameters.encode(name, keyData, valueData,
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        GenericResultParameters resultParameters = GenericResultParameters.decode(response);
        return toObject(resultParameters.result);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        ClientMessage request = MapSetParameters.encode(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));

        invoke(request, keyData);
    }

    @Override
    public void lock(K key) {
        lock(key, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapLockParameters.encode(name, keyData,
                ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit));
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapIsLockedParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
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
        ClientMessage request = MapTryLockParameters.encode(name, keyData,
                ThreadUtil.getThreadId(), getTimeInMillis(time, timeunit));

        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    @Override
    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapUnlockParameters.encode(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapForceUnlockParameters.encode(name, keyData);
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

    @Override
    public String addInterceptor(MapInterceptor interceptor) {
        Data data = toData(interceptor);
        ClientMessage request = MapAddInterceptorParameters.encode(name, data);
        ClientMessage response = invoke(request);
        AddListenerResultParameters resultParameters = AddListenerResultParameters.decode(response);
        return resultParameters.registrationId;
    }

    @Override
    public void removeInterceptor(String id) {
        ClientMessage request = MapRemoveEntryListenerParameters.encode(name, id);
        invoke(request);
    }

    @Override
    public String addEntryListener(MapListener listener, boolean includeValue) {
        ClientMessage request = MapAddEntryListenerParameters.encode(name, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, handler);
    }

    @Override
    public String addEntryListener(EntryListener listener, boolean includeValue) {
        ClientMessage request = MapAddEntryListenerParameters.encode(name, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, handler);
    }

    @Override
    public boolean removeEntryListener(String id) {
        ClientMessage request = MapRemoveEntryListenerParameters.encode(name, id);
        return stopListening(request, id);
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener) {
        ClientMessage request = MapAddPartitionLostListenerParameters.encode(name);
        final EventHandler<PortableMapPartitionLostEvent> handler = new ClientMapPartitionLostEventHandler(listener);
        return listen(request, handler);
    }

    @Override
    public boolean removePartitionLostListener(String id) {
        ClientMessage request = MapRemoveEntryListenerParameters.encode(name, id);
        return stopListening(request, id);
    }

    @Override
    public String addEntryListener(MapListener listener, K key, boolean includeValue) {
        Data keyData = toData(key);
        ClientMessage request = MapAddEntryListenerToKeyParameters.encode(name, keyData, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener listener, K key, boolean includeValue) {
        final Data keyData = toData(key);
        ClientMessage request = MapAddEntryListenerToKeyParameters.encode(name, keyData, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        final Data keyData = toData(key);
        final Data predicateData = toData(predicate);
        ClientMessage request =
                MapAddEntryListenerToKeyWithPredicateParameters.encode(name, keyData, predicateData, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        final Data keyData = toData(key);
        final Data predicateData = toData(predicate);
        ClientMessage request =
                MapAddEntryListenerToKeyWithPredicateParameters.encode(name, keyData, predicateData, includeValue);

        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        final Data predicateData = toData(predicate);
        ClientMessage request =
                MapAddEntryListenerWithPredicateParameters.encode(name, predicateData, includeValue);

        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, null, handler);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        final Data predicateData = toData(predicate);
        ClientMessage request =
                MapAddEntryListenerWithPredicateParameters.encode(name, predicateData, includeValue);

        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, null, handler);
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapGetEntryViewParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        EntryViewParameters parameters = EntryViewParameters.decode(response);

        if (parameters == null) {
            return null;
        }
        SimpleEntryView<K, V> entryView = new SimpleEntryView<K, V>();
        entryView.setKey(key);
        entryView.setValue((V) toObject(parameters.value));
        entryView.setCost(parameters.cost);
        entryView.setCreationTime(parameters.creationTime);
        entryView.setExpirationTime(parameters.expirationTime);
        entryView.setHits(parameters.hits);
        entryView.setLastAccessTime(parameters.lastAccessTime);
        entryView.setLastStoredTime(parameters.lastStoredTime);
        entryView.setLastUpdateTime(parameters.lastUpdateTime);
        entryView.setVersion(parameters.version);
        entryView.setEvictionCriteriaNumber(parameters.evictionCriteriaNumber);
        entryView.setTtl(parameters.ttl);
        //TODO putCache
        return entryView;
    }

    @Override
    public boolean evict(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapEvictParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    @Override
    public void evictAll() {
        clearNearCache();
        ClientMessage request = MapEvictAllParameters.encode(name);
        invoke(request);
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        if (replaceExistingValues) {
            clearNearCache();
        }
        ClientMessage request = MapLoadAllParameters.encode(name, replaceExistingValues);
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
        ClientMessage request = MapLoadGivenKeysParameters.encode(name, dataKeys, replaceExistingValues);
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
        ClientMessage request = MapKeySetParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> result = resultParameters.result;
        Set<K> keySet = new HashSet<K>(result.size());
        for (Data data : result) {
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
        ClientMessage request = MapGetAllParameters.encode(name, keySet);
        ClientMessage response = invoke(request);
        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(response);
        int size = resultParameters.keys.size();

        for (int i = 0; i < size; i++) {
            Data dataKey = resultParameters.keys.get(i);
            final V value = toObject(resultParameters.values.get(i));
            final K key = toObject(dataKey);
            result.put(key, value);
            if (nearCache != null) {
                nearCache.put(dataKey, value);
            }
        }
        return result;
    }

    @Override
    public Collection<V> values() {
        ClientMessage request = MapValuesParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> collectionData = resultParameters.result;
        Collection<V> collection = new ArrayList<V>(collectionData.size());
        for (Data data : collectionData) {
            V value = toObject(data);
            collection.add(value);
        }
        return collection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        ClientMessage request = MapEntrySetParameters.encode(name);
        ClientMessage response = invoke(request);
        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(response);
        Set<Entry<K, V>> entrySet = new HashSet<Entry<K, V>>();

        int size = resultParameters.keys.size();

        for (int i = 0; i < size; i++) {
            Data keyData = resultParameters.keys.get(i);
            Data valueData = resultParameters.values.get(i);
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
        ClientMessage request = MapKeySetWithPredicateParameters.encode(name, toData(predicate));

        if (pagingPredicate == null) {
            ClientMessage response = invoke(request);
            DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);

            final HashSet<K> keySet = new HashSet<K>();
            for (Data o : resultParameters.result) {
                final K key = toObject(o);
                keySet.add(key);
            }
            return keySet;
        }


        ClientMessage response = invoke(request);
        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(response);

        final Comparator<Entry> comparator = SortingUtil.newComparator(pagingPredicate.getComparator(), IterationType.KEY);
        final SortedQueryResultSet sortedResult = new SortedQueryResultSet(comparator, IterationType.KEY,
                pagingPredicate.getPageSize());


        int size = resultParameters.keys.size();

        for (int i = 0; i < size; i++) {
            Data keyData = resultParameters.keys.get(i);
            Data valueData = resultParameters.values.get(i);

            final K key = toObject(keyData);
            final V value = toObject(valueData);
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

        ClientMessage request = MapEntriesWithPredicateParameters.encode(name, toData(predicate));

        ClientMessage response = invoke(request);
        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(response);

        Set entrySet;
        if (pagingPredicate == null) {
            entrySet = new HashSet<Entry<K, V>>(resultParameters.keys.size());
        } else {
            entrySet = new SortedQueryResultSet(pagingPredicate.getComparator(), IterationType.ENTRY,
                    pagingPredicate.getPageSize());
        }

        for (int i = 0; i < resultParameters.keys.size(); i++) {
            Data keyData = resultParameters.keys.get(i);
            Data valueData = resultParameters.values.get(i);
            K key = toObject(keyData);
            V value = toObject(valueData);
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        if (pagingPredicate != null) {
            PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) entrySet).last());
        }
        return entrySet;
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            return valuesForPagingPredicate((PagingPredicate) predicate);
        }

        ClientMessage request = MapValuesWithPredicateParameters.encode(name, toData(predicate));
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);

        Collection<Data> result = resultParameters.result;
        List<V> values = new ArrayList<V>(result.size());
        for (Data data : result) {
            V value = toObject(data);
            values.add(value);
        }
        return values;
    }

    private Collection<V> valuesForPagingPredicate(PagingPredicate pagingPredicate) {
        pagingPredicate.setIterationType(IterationType.VALUE);

        if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
            pagingPredicate.previousPage();
            values(pagingPredicate);
            pagingPredicate.nextPage();
        }


        ClientMessage request = MapValuesWithPredicateParameters.encode(name, toData(pagingPredicate));
        ClientMessage response = invoke(request);
        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(response);


        int size = resultParameters.keys.size();
        List<Entry<K, V>> valueEntryList = new ArrayList<Entry<K, V>>(size);
        for (int i = 0; i < size; i++) {
            Data keyData = resultParameters.keys.get(i);
            Data valueData = resultParameters.values.get(i);

            K key = toObject(keyData);
            V value = toObject(valueData);
            valueEntryList.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
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

        ArrayList<V> values = new ArrayList<V>(valueEntryList.size());
        for (Entry<K, V> entry : valueEntryList) {
            values.add(entry.getValue());
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
        ClientMessage request = MapAddIndexParameters.encode(name, attribute, ordered);
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
        ClientMessage request = MapExecuteOnKeyParameters.encode(name, toData(entryProcessor), keyData);
        return invoke(request, keyData);
    }

    public void submitToKey(K key, EntryProcessor entryProcessor, final ExecutionCallback callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapSubmitToKeyParameters.encode(name, toData(entryProcessor), keyData);
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
        ClientMessage request = MapSubmitToKeyParameters.encode(name, toData(entryProcessor), keyData);

        try {
            final ICompletableFuture future = invokeOnKeyOwner(request, keyData);
            return new DelegatingFuture(future, getContext().getSerializationService());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
        ClientMessage request = MapExecuteOnAllKeysParameters.encode(name, toData(entryProcessor));

        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(request);

        int size = resultParameters.keys.size();
        Map<K, Object> result = new HashMap<K, Object>();
        for (int i = 0; i < size; i++) {
            Data keyData = resultParameters.keys.get(i);
            Data valueData = resultParameters.values.get(i);
            K key = toObject(keyData);
            result.put(key, toObject(valueData));
        }
        return result;
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {

        ClientMessage request = MapExecuteWithPredicateParameters.encode(name,
                toData(entryProcessor), toData(predicate));
        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(request);


        int size = resultParameters.keys.size();
        Map<K, Object> result = new HashMap<K, Object>();
        for (int i = 0; i < size; i++) {
            Data keyData = resultParameters.keys.get(i);
            Data valueData = resultParameters.values.get(i);
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

        ClientMessage request = MapExecuteOnKeysParameters.encode(name, toData(entryProcessor), dataKeys);
        ClientMessage response = invoke(request);
        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(response);

        int size = resultParameters.keys.size();
        Map<K, Object> result = new HashMap<K, Object>();
        for (int i = 0; i < size; i++) {
            Data keyData = resultParameters.keys.get(i);
            Data valueData = resultParameters.values.get(i);
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
        ClientMessage request = MapSizeParameters.encode(name);
        ClientMessage response = invoke(request);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = MapIsEmptyParameters.encode(name);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        int len = m.size();
        List<Data> keys = new ArrayList<Data>(len);
        List<Data> values = new ArrayList<Data>(len);

        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            final Data keyData = toData(entry.getKey());
            invalidateNearCache(keyData);
            keys.add(keyData);
            values.add(toData(entry.getValue()));
        }

        ClientMessage request = MapPutAllParameters.encode(name, keys, values);
        invoke(request);
    }

    @Override
    public void clear() {
        ClientMessage request = MapClearParameters.encode(name);
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

    private EventHandler<ClientMessage> createHandler(final Object listener, final boolean includeValue) {
        final ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return new ClientMapEventHandler(listenerAdaptor, includeValue);
    }

    private class ClientMapEventHandler implements EventHandler<ClientMessage> {

        private final ListenerAdapter listenerAdapter;
        private final boolean includeValue;

        public ClientMapEventHandler(ListenerAdapter listenerAdapter, boolean includeValue) {
            this.listenerAdapter = listenerAdapter;
            this.includeValue = includeValue;
        }


        public void handle(ClientMessage clientMessage) {
            EntryEventParameters event = EntryEventParameters.decode(clientMessage);

            Member member = getContext().getClusterService().getMember(event.uuid);
            final IMapEvent iMapEvent = createIMapEvent(event, member);
            listenerAdapter.onEvent(iMapEvent);
        }

        private IMapEvent createIMapEvent(EntryEventParameters event, Member member) {
            IMapEvent iMapEvent;
            EntryEventType entryEventType = EntryEventType.getByType(event.eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                case MERGED:
                    iMapEvent = createEntryEvent(event, member);
                    break;
                case EVICT_ALL:
                case CLEAR_ALL:
                    iMapEvent = createMapEvent(event, member);
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + entryEventType);
            }

            return iMapEvent;
        }

        private MapEvent createMapEvent(EntryEventParameters event, Member member) {
            return new MapEvent(name, member, event.eventType, event.numberOfAffectedEntries);
        }

        private EntryEvent<K, V> createEntryEvent(EntryEventParameters event, Member member) {
            V value = null;
            V oldValue = null;
            V mergingValue = null;
            if (includeValue) {
                value = toObject(event.value);
                oldValue = toObject(event.oldValue);
                mergingValue = toObject(event.mergingValue);
            }
            K key = toObject(event.key);
            return new EntryEvent<K, V>(name, member, event.eventType, key, oldValue, value, mergingValue);
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
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
            ClientMessage request = MapAddNearCacheEntryListenerParameters.encode(name, false);
            EventHandler handler = new EventHandler<ClientMessage>() {
                @Override
                public void handle(ClientMessage eventMessage) {
                    EntryEventParameters event = EntryEventParameters.decode(eventMessage);

                    EntryEventType entryEventType = EntryEventType.getByType(event.eventType);
                    switch (entryEventType) {
                        case ADDED:
                        case REMOVED:
                        case UPDATED:
                        case MERGED:
                        case EVICTED:
                            nearCache.remove(event.key);
                            break;
                        case CLEAR_ALL:
                        case EVICT_ALL:
                            nearCache.clear();
                            break;
                        default:
                            throw new IllegalArgumentException("Not a known event type " + entryEventType);
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

            String registrationId = getContext().getListenerService().startListening(request, null, handler);
            nearCache.setId(registrationId);
        } catch (Exception e) {
            Logger.getLogger(ClientHeapNearCache.class).severe(
                    "-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }

    private void removeNearCacheInvalidationListener() {
        if (nearCache != null && nearCache.getId() != null) {
            String registrationId = nearCache.getId();
            ClientMessage request = MapRemoveEntryListenerParameters.encode(name, registrationId);
            getContext().getListenerService().stopListening(request, registrationId);
        }
    }

    @Override
    public String toString() {
        return "IMap{" + "name='" + getName() + '\'' + '}';
    }

}
