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

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddIndexCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddInterceptorCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapClearCodec;
import com.hazelcast.client.impl.protocol.codec.MapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapContainsValueCodec;
import com.hazelcast.client.impl.protocol.codec.MapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntriesWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntriesWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.MapEvictAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapEvictCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteOnKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapExecuteWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapFlushCodec;
import com.hazelcast.client.impl.protocol.codec.MapForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetAsyncCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetEntryViewCodec;
import com.hazelcast.client.impl.protocol.codec.MapIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.MapIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.MapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.MapKeySetWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapKeySetWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapLoadAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapLoadGivenKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapLockCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutAsyncCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveAsyncCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveInterceptorCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.MapSetCodec;
import com.hazelcast.client.impl.protocol.codec.MapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.MapSubmitToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryPutCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesWithPredicateCodec;
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerRemoveCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
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
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import static com.hazelcast.util.SortingUtil.getSortedQueryResultSet;

public class ClientMapProxy<K, V> extends ClientProxy implements IMap<K, V> {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private final String name;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();
    private volatile ClientHeapNearCache<Data> nearCache;

    private static final ClientMessageDecoder getAsyncResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapGetAsyncCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder putAsyncResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapPutAsyncCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder removeAsyncResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapRemoveAsyncCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder submitToKeyResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapSubmitToKeyCodec.decodeResponse(clientMessage).response;
        }
    };

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
        ClientMessage message = MapContainsKeyCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage result = invoke(message, keyData);
        MapContainsKeyCodec.ResponseParameters resultParameters = MapContainsKeyCodec.decodeResponse(result);
        return resultParameters.response;
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        ClientMessage request = MapContainsValueCodec.encodeRequest(name, valueData);
        ClientMessage response = invoke(request);
        MapContainsValueCodec.ResponseParameters resultParameters = MapContainsValueCodec.decodeResponse(response);
        return resultParameters.response;
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
        ClientMessage request = MapGetCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());

        ClientMessage response = invoke(request, keyData);
        MapGetCodec.ResponseParameters resultParameters = MapGetCodec.decodeResponse(response);
        V result = toObject(resultParameters.response);
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

        ClientMessage request = MapRemoveCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapRemoveCodec.ResponseParameters resultParameters = MapRemoveCodec.decodeResponse(response);
        return toObject(resultParameters.response);
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

        final ClientMessage request = MapRemoveIfSameCodec.encodeRequest(name, keyData,
                valueData, ThreadUtil.getThreadId());

        ClientMessage response = invoke(request, keyData);
        MapRemoveIfSameCodec.ResponseParameters resultParameters = MapRemoveIfSameCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void delete(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        final ClientMessage request = MapDeleteCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void flush() {
        ClientMessage request = MapFlushCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public Future<V> getAsync(final K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        initNearCache();
        final Data keyData = toData(key);
        SerializationService serializationService = getContext().getSerializationService();
        if (nearCache != null) {
            Object cached = nearCache.get(keyData);
            if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
                return new CompletedFuture<V>(serializationService,
                        cached, getContext().getExecutionService().getAsyncExecutor());
            }
        }

        final ClientMessage request = MapGetAsyncCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        try {
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            ClientDelegatingFuture<V> delegatingFuture =
                    new ClientDelegatingFuture<V>(future, serializationService, getAsyncResponseDecoder);
            if (nearCache != null) {
                delegatingFuture.andThenInternal(new ExecutionCallback<Data>() {
                    @Override
                    public void onResponse(Data response) {
                        if (nearCache != null) {
                            nearCache.put(keyData, response);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {

                    }
                });
            }
            return delegatingFuture;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private ClientInvocationFuture invokeOnKeyOwner(ClientMessage request, Data keyData) {
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
        ClientMessage request = MapPutAsyncCodec.encodeRequest(name, keyData,
                valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        try {
            final ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<V>(future, getContext().getSerializationService()
                    , putAsyncResponseDecoder);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Future<V> removeAsync(final K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        ClientMessage request = MapRemoveAsyncCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        try {
            final ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<V>(future, getContext().getSerializationService(),
                    removeAsyncResponseDecoder);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        invalidateNearCache(keyData);
        ClientMessage request = MapTryRemoveCodec.encodeRequest(name, keyData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        ClientMessage response = invoke(request, keyData);
        MapTryRemoveCodec.ResponseParameters resultParameters = MapTryRemoveCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);

        ClientMessage request = MapTryPutCodec.encodeRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), timeunit.toMillis(timeout));
        ClientMessage response = invoke(request, keyData);
        MapTryPutCodec.ResponseParameters resultParameters = MapTryPutCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);

        ClientMessage request = MapPutCodec.encodeRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        ClientMessage response = invoke(request, keyData);
        MapPutCodec.ResponseParameters resultParameters = MapPutCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        ClientMessage request = MapPutTransientCodec.encodeRequest(name, keyData, valueData,
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
        ClientMessage request = MapPutIfAbsentCodec.encodeRequest(name, keyData, valueData,
                ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        ClientMessage result = invoke(request, keyData);
        MapPutIfAbsentCodec.ResponseParameters resultParameters = MapPutIfAbsentCodec.decodeResponse(result);
        return toObject(resultParameters.response);
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

        ClientMessage request = MapReplaceIfSameCodec.encodeRequest(name, keyData, oldValueData, newValueData,
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapReplaceIfSameCodec.ResponseParameters resultParameters = MapReplaceIfSameCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public V replace(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        ClientMessage request = MapReplaceCodec.encodeRequest(name, keyData, valueData,
                ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapReplaceCodec.ResponseParameters resultParameters = MapReplaceCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        invalidateNearCache(keyData);
        ClientMessage request = MapSetCodec.encodeRequest(name, keyData, valueData,
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
        ClientMessage request = MapLockCodec.encodeRequest(name, keyData,
                ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit));
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapIsLockedCodec.encodeRequest(name, keyData);
        ClientMessage response = invoke(request, keyData);
        MapIsLockedCodec.ResponseParameters resultParameters = MapIsLockedCodec.decodeResponse(response);
        return resultParameters.response;
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
        ClientMessage request = MapTryLockCodec.encodeRequest(name, keyData,
                ThreadUtil.getThreadId(), getTimeInMillis(time, timeunit));

        ClientMessage response = invoke(request, keyData);
        MapTryLockCodec.ResponseParameters resultParameters = MapTryLockCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapUnlockCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapForceUnlockCodec.encodeRequest(name, keyData);
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
        ClientMessage request = MapAddInterceptorCodec.encodeRequest(name, data);
        ClientMessage response = invoke(request);
        MapAddInterceptorCodec.ResponseParameters resultParameters = MapAddInterceptorCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void removeInterceptor(String id) {
        ClientMessage request = MapRemoveInterceptorCodec.encodeRequest(name, id);
        invoke(request);
    }

    @Override
    public String addEntryListener(MapListener listener, boolean includeValue) {
        ClientMessage request = MapAddEntryListenerCodec.encodeRequest(name, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(EntryListener listener, boolean includeValue) {
        ClientMessage request = MapAddEntryListenerCodec.encodeRequest(name, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, handler, responseDecoder);
    }

    @Override
    public boolean removeEntryListener(String registrationId) {
        return stopListening(registrationId, new ListenerRemoveCodec() {
            @Override
            public ClientMessage encodeRequest(String realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        });
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener) {
        ClientMessage request = MapAddPartitionLostListenerCodec.encodeRequest(name);
        final EventHandler<ClientMessage> handler = new ClientMapPartitionLostEventHandler(listener);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddPartitionLostListenerCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, handler, responseDecoder);
    }

    @Override
    public boolean removePartitionLostListener(String registrationId) {
        return stopListening(registrationId, new ListenerRemoveCodec() {
            @Override
            public ClientMessage encodeRequest(String realRegistrationId) {
                return MapRemovePartitionLostListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeResponse(ClientMessage clientMessage) {
                return MapRemovePartitionLostListenerCodec.decodeResponse(clientMessage).response;
            }
        });
    }

    @Override
    public String addEntryListener(MapListener listener, K key, boolean includeValue) {
        Data keyData = toData(key);
        ClientMessage request = MapAddEntryListenerToKeyCodec.encodeRequest(name, keyData, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddEntryListenerToKeyCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, keyData, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(EntryListener listener, K key, boolean includeValue) {
        final Data keyData = toData(key);
        ClientMessage request = MapAddEntryListenerToKeyCodec.encodeRequest(name, keyData, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddEntryListenerToKeyCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, keyData, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        final Data keyData = toData(key);
        final Data predicateData = toData(predicate);
        ClientMessage request =
                MapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(name, keyData, predicateData, includeValue);
        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, keyData, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        final Data keyData = toData(key);
        final Data predicateData = toData(predicate);
        ClientMessage request =
                MapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(name, keyData, predicateData, includeValue);

        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, keyData, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        final Data predicateData = toData(predicate);
        ClientMessage request =
                MapAddEntryListenerWithPredicateCodec.encodeRequest(name, predicateData, includeValue);

        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddEntryListenerWithPredicateCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, null, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        final Data predicateData = toData(predicate);
        ClientMessage request =
                MapAddEntryListenerWithPredicateCodec.encodeRequest(name, predicateData, includeValue);

        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) MapAddEntryListenerWithPredicateCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, null, handler, responseDecoder);
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapGetEntryViewCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);

        MapGetEntryViewCodec.ResponseParameters parameters = MapGetEntryViewCodec.decodeResponse(response);
        SimpleEntryView<K, V> entryView = new SimpleEntryView<K, V>();
        SimpleEntryView<Data, Data> dataEntryView = parameters.dataEntryView;

        if (dataEntryView == null) {
            return null;
        }
        entryView.setKey((K) toObject(dataEntryView.getKey()));
        entryView.setValue((V) toObject(dataEntryView.getValue()));
        entryView.setCost(dataEntryView.getCost());
        entryView.setCreationTime(dataEntryView.getCreationTime());
        entryView.setExpirationTime(dataEntryView.getExpirationTime());
        entryView.setHits(dataEntryView.getHits());
        entryView.setLastAccessTime(dataEntryView.getLastAccessTime());
        entryView.setLastStoredTime(dataEntryView.getLastStoredTime());
        entryView.setLastUpdateTime(dataEntryView.getLastUpdateTime());
        entryView.setVersion(dataEntryView.getVersion());
        entryView.setEvictionCriteriaNumber(dataEntryView.getEvictionCriteriaNumber());
        entryView.setTtl(dataEntryView.getTtl());
        //TODO putCache
        return entryView;
    }

    @Override
    public boolean evict(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapEvictCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapEvictCodec.ResponseParameters resultParameters = MapEvictCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void evictAll() {
        invalidateNearCache();
        ClientMessage request = MapEvictAllCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        if (replaceExistingValues) {
            invalidateNearCache();
        }
        ClientMessage request = MapLoadAllCodec.encodeRequest(name, replaceExistingValues);
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
        ClientMessage request = MapLoadGivenKeysCodec.encodeRequest(name, dataKeys, replaceExistingValues);
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
        ClientMessage request = MapKeySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MapKeySetCodec.ResponseParameters resultParameters = MapKeySetCodec.decodeResponse(response);
        Collection<Data> result = resultParameters.list;
        Set<K> keySet = new HashSet<K>(result.size());
        for (Data data : result) {
            final K key = toObject(data);
            keySet.add(key);
        }
        return keySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<K, V> getAll(Set<K> keys) {
        initNearCache();
        Set<Data> keySet = new HashSet<Data>(keys.size());
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
        ClientMessage request = MapGetAllCodec.encodeRequest(name, keySet);
        ClientMessage response = invoke(request);
        MapGetAllCodec.ResponseParameters resultParameters = MapGetAllCodec.decodeResponse(response);

        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {

            final V value = toObject(entry.getValue());
            final K key = toObject(entry.getKey());
            result.put(key, value);
            if (nearCache != null) {
                nearCache.put(entry.getKey(), value);
            }
        }
        return result;
    }

    @Override
    public Collection<V> values() {
        ClientMessage request = MapValuesCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MapValuesCodec.ResponseParameters resultParameters = MapValuesCodec.decodeResponse(response);
        Collection<Data> collectionData = resultParameters.list;
        Collection<V> collection = new ArrayList<V>(collectionData.size());
        for (Data data : collectionData) {
            V value = toObject(data);
            collection.add(value);
        }
        return collection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        ClientMessage request = MapEntrySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MapEntrySetCodec.ResponseParameters resultParameters = MapEntrySetCodec.decodeResponse(response);
        Set<Entry<K, V>> entrySet = new HashSet<Entry<K, V>>();


        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {
            K key = toObject(entry.getKey());
            V value = toObject(entry.getValue());
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return entrySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            return keySetWithPagingPredicate((PagingPredicate) predicate);
        }

        ClientMessage request = MapKeySetWithPredicateCodec.encodeRequest(name, toData(predicate));
        ClientMessage response = invoke(request);
        MapKeySetWithPredicateCodec.ResponseParameters resultParameters = MapKeySetWithPredicateCodec.decodeResponse(response);

        final HashSet<K> keySet = new HashSet<K>();
        for (Data o : resultParameters.list) {
            final K key = toObject(o);
            keySet.add(key);
        }
        return keySet;
    }

    private Set<K> keySetWithPagingPredicate(PagingPredicate pagingPredicate) {
        pagingPredicate.setIterationType(IterationType.KEY);
        ClientMessage request = MapKeySetWithPagingPredicateCodec.encodeRequest(name, toData(pagingPredicate));

        ClientMessage response = invoke(request);
        MapKeySetWithPagingPredicateCodec.ResponseParameters resultParameters = MapKeySetWithPagingPredicateCodec.decodeResponse(response);

        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {
            K key = toObject(entry.getKey());
            resultList.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, null));
        }
        return (Set<K>) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.KEY);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Set<Entry<K, V>> entrySet(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            return entrySetWithPagingPredicate((PagingPredicate) predicate);
        }
        ClientMessage request = MapEntriesWithPredicateCodec.encodeRequest(name, toData(predicate));

        ClientMessage response = invoke(request);
        MapEntriesWithPredicateCodec.ResponseParameters resultParameters = MapEntriesWithPredicateCodec.decodeResponse(response);

        Set entrySet = new HashSet<Entry<K, V>>(resultParameters.map.size());

        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {
            K key = toObject(entry.getKey());
            V value = toObject(entry.getValue());
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return entrySet;
    }

    public Set<Entry<K, V>> entrySetWithPagingPredicate(PagingPredicate pagingPredicate) {
        pagingPredicate.setIterationType(IterationType.ENTRY);

        ClientMessage request = MapEntriesWithPagingPredicateCodec.encodeRequest(name, toData(pagingPredicate));

        ClientMessage response = invoke(request);
        MapEntriesWithPagingPredicateCodec.ResponseParameters resultParameters = MapEntriesWithPagingPredicateCodec.decodeResponse(response);

        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {
            K key = toObject(entry.getKey());
            V value = toObject(entry.getValue());
            resultList.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return (Set) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.ENTRY);
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            return valuesForPagingPredicate((PagingPredicate) predicate);
        }

        ClientMessage request = MapValuesWithPredicateCodec.encodeRequest(name, toData(predicate));
        ClientMessage response = invoke(request);
        MapValuesWithPredicateCodec.ResponseParameters resultParameters = MapValuesWithPredicateCodec.decodeResponse(response);

        Collection<Data> result = resultParameters.list;
        List<V> values = new ArrayList<V>(result.size());
        for (Data data : result) {
            V value = toObject(data);
            values.add(value);
        }
        return values;
    }

    private Collection<V> valuesForPagingPredicate(PagingPredicate pagingPredicate) {
        pagingPredicate.setIterationType(IterationType.VALUE);

        ClientMessage request = MapValuesWithPagingPredicateCodec.encodeRequest(name, toData(pagingPredicate));
        ClientMessage response = invoke(request);
        MapValuesWithPagingPredicateCodec.ResponseParameters resultParameters = MapValuesWithPagingPredicateCodec.decodeResponse(response);

        List<Entry> resultList = new ArrayList<Entry>(resultParameters.map.size());
        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {
            K key = toObject(entry.getKey());
            V value = toObject(entry.getValue());
            resultList.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
        }

        return (Collection) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.VALUE);
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
        ClientMessage request = MapAddIndexCodec.encodeRequest(name, attribute, ordered);
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
        ClientMessage request = MapExecuteOnKeyCodec.encodeRequest(name, toData(entryProcessor), keyData);
        ClientMessage response = invoke(request, keyData);
        return toObject(MapExecuteOnKeyCodec.decodeResponse(response).response);
    }

    public void submitToKey(K key, EntryProcessor entryProcessor, final ExecutionCallback callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapSubmitToKeyCodec.encodeRequest(name, toData(entryProcessor), keyData);
        try {
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            SerializationService serializationService = getContext().getSerializationService();
            ClientDelegatingFuture clientDelegatingFuture =
                    new ClientDelegatingFuture(future, serializationService, submitToKeyResponseDecoder);
            clientDelegatingFuture.andThen(callback);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public Future submitToKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapSubmitToKeyCodec.encodeRequest(name, toData(entryProcessor), keyData);

        try {
            final ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture(future, getContext().getSerializationService()
                    , submitToKeyResponseDecoder);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
        ClientMessage request = MapExecuteOnAllKeysCodec.encodeRequest(name, toData(entryProcessor));
        ClientMessage response = invoke(request);
        MapExecuteOnAllKeysCodec.ResponseParameters resultParameters = MapExecuteOnAllKeysCodec.decodeResponse(response);

        Map<K, Object> result = new HashMap<K, Object>();
        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {
            K key = toObject(entry.getKey());
            result.put(key, toObject(entry.getValue()));
        }
        return result;
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {

        ClientMessage request = MapExecuteWithPredicateCodec.encodeRequest(name,
                toData(entryProcessor), toData(predicate));
        ClientMessage response = invoke(request);

        MapExecuteWithPredicateCodec.ResponseParameters resultParameters = MapExecuteWithPredicateCodec.decodeResponse(response);


        Map<K, Object> result = new HashMap<K, Object>();
        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {
            K key = toObject(entry.getKey());
            result.put(key, toObject(entry.getValue()));
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

        ClientMessage request = MapExecuteOnKeysCodec.encodeRequest(name, toData(entryProcessor), dataKeys);
        ClientMessage response = invoke(request);
        MapExecuteOnKeysCodec.ResponseParameters resultParameters = MapExecuteOnKeysCodec.decodeResponse(response);

        Map<K, Object> result = new HashMap<K, Object>();
        for (Entry<Data, Data> entry : resultParameters.map.entrySet()) {
            K key = toObject(entry.getKey());
            result.put(key, toObject(entry.getValue()));
        }
        return result;

    }

    @Override
    public void set(K key, V value) {
        set(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public int size() {
        ClientMessage request = MapSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MapSizeCodec.ResponseParameters resultParameters = MapSizeCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = MapIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MapIsEmptyCodec.ResponseParameters resultParameters = MapIsEmptyCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Map<Data, Data> map = new HashMap<Data, Data>();
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            final Data keyData = toData(entry.getKey());
            invalidateNearCache(keyData);
            map.put(keyData, toData(entry.getValue()));
        }

        ClientMessage request = MapPutAllCodec.encodeRequest(name, map);
        invoke(request);
    }

    @Override
    public void clear() {
        ClientMessage request = MapClearCodec.encodeRequest(name);
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
            ClientMessage request = MapAddNearCacheEntryListenerCodec.encodeRequest(name, false);
            EventHandler handler = new ClientMapAddNearCacheEventHandler();
            String registrationId = getContext().getListenerService().startListening(request, null, handler,
                    new ClientMessageDecoder() {
                        @Override
                        public <T> T decodeClientMessage(ClientMessage clientMessage) {
                            return (T) MapAddNearCacheEntryListenerCodec.decodeResponse(clientMessage).response;
                        }
                    });
            nearCache.setId(registrationId);
        } catch (Exception e) {
            Logger.getLogger(ClientHeapNearCache.class).severe(
                    "-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }

    private void removeNearCacheInvalidationListener() {
        if (nearCache != null && nearCache.getId() != null) {
            String registrationId = nearCache.getId();

            ClientListenerService listenerService = getContext().getListenerService();

            listenerService.stopListening(registrationId, new ListenerRemoveCodec() {
                @Override
                public ClientMessage encodeRequest(String realRegistrationId) {
                    return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
                }

                @Override
                public boolean decodeResponse(ClientMessage clientMessage) {
                    return MapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
                }
            });
        }
    }

    @Override
    public String toString() {
        return "IMap{" + "name='" + getName() + '\'' + '}';
    }

    private class ClientMapEventHandler extends MapAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final ListenerAdapter listenerAdapter;
        private final boolean includeValue;

        public ClientMapEventHandler(ListenerAdapter listenerAdapter, boolean includeValue) {
            this.listenerAdapter = listenerAdapter;
            this.includeValue = includeValue;
        }

        @Override
        public void handle(Data key, Data value, Data oldValue, Data mergingValue,
                           int eventType, String uuid, int numberOfAffectedEntries) {
            Member member = getContext().getClusterService().getMember(uuid);
            final IMapEvent iMapEvent = createIMapEvent(key, value, oldValue,
                    mergingValue, eventType, numberOfAffectedEntries, member);
            listenerAdapter.onEvent(iMapEvent);
        }

        private IMapEvent createIMapEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                          int eventType, int numberOfAffectedEntries, Member member) {
            IMapEvent iMapEvent;
            EntryEventType entryEventType = EntryEventType.getByType(eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                case MERGED:
                    iMapEvent = createEntryEvent(key, value, oldValue, mergingValue, eventType, member);
                    break;
                case EVICT_ALL:
                case CLEAR_ALL:
                    iMapEvent = createMapEvent(eventType, numberOfAffectedEntries, member);
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + entryEventType);
            }

            return iMapEvent;
        }

        private MapEvent createMapEvent(int eventType, int numberOfAffectedEntries, Member member) {
            return new MapEvent(name, member, eventType, numberOfAffectedEntries);
        }

        private EntryEvent<K, V> createEntryEvent(Data keyData, Data valueData, Data oldValueData,
                                                  Data mergingValueData, int eventType, Member member) {
            V value = null;
            V oldValue = null;
            V mergingValue = null;
            if (includeValue) {
                value = toObject(valueData);
                oldValue = toObject(oldValueData);
                mergingValue = toObject(mergingValueData);
            }
            K key = toObject(keyData);
            return new EntryEvent<K, V>(name, member, eventType, key, oldValue, value, mergingValue);
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    private class ClientMapPartitionLostEventHandler extends MapAddPartitionLostListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private MapPartitionLostListener listener;

        public ClientMapPartitionLostEventHandler(MapPartitionLostListener listener) {
            this.listener = listener;
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {

        }

        @Override
        public void handle(int partitionId, String uuid) {
            final Member member = getContext().getClusterService().getMember(uuid);
            listener.partitionLost(new MapPartitionLostEvent(name, member, -1, partitionId));
        }
    }

    private class ClientMapAddNearCacheEventHandler extends MapAddNearCacheEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        @Override
        public void beforeListenerRegister() {
            invalidateNearCache();
        }

        @Override
        public void onListenerRegister() {
            invalidateNearCache();
        }

        @Override
        public void handle(Data key, Data value, Data oldValue, Data mergingValue,
                           int eventType, String uuid, int numberOfAffectedEntries) {
            EntryEventType entryEventType = EntryEventType.getByType(eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case MERGED:
                case EVICTED:
                    nearCache.remove(key);
                    break;
                case CLEAR_ALL:
                case EVICT_ALL:
                    nearCache.clear();
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + entryEventType);
            }
        }
    }

}
