/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.client.impl.ClientLockReferenceIdGenerator;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerToKeyWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddIndexCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddInterceptorCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAggregateCodec;
import com.hazelcast.client.impl.protocol.codec.MapAggregateWithPredicateCodec;
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
import com.hazelcast.client.impl.protocol.codec.MapProjectCodec;
import com.hazelcast.client.impl.protocol.codec.MapProjectWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveAllCodec;
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
import com.hazelcast.client.impl.querycache.ClientQueryCacheContext;
import com.hazelcast.client.impl.querycache.subscriber.ClientQueryCacheEndToEndConstructor;
import com.hazelcast.client.map.impl.ClientMapPartitionIterator;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
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
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.collection.InflatableSet;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.map.impl.MapListenerFlagOperator.setAndGetListenerFlags;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequests.newQueryCacheRequest;
import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.SortingUtil.getSortedQueryResultSet;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static java.util.Collections.emptyMap;

/**
 * Proxy implementation of {@link IMap}.
 *
 * @param <K> key
 * @param <V> value
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ClientMapProxy<K, V> extends ClientProxy implements IMap<K, V> {

    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Predicate should not be null!";
    protected static final String NULL_AGGREGATOR_IS_NOT_ALLOWED = "Aggregator should not be null!";

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder GET_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapGetCodec.decodeResponse(clientMessage).response;
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder PUT_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapPutCodec.decodeResponse(clientMessage).response;
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder SET_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder REMOVE_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapRemoveCodec.decodeResponse(clientMessage).response;
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder SUBMIT_TO_KEY_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapSubmitToKeyCodec.decodeResponse(clientMessage).response;
        }
    };

    private ClientLockReferenceIdGenerator lockReferenceIdGenerator;
    private ClientQueryCacheContext queryCacheContext;

    public ClientMapProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        lockReferenceIdGenerator = getClient().getLockReferenceIdGenerator();
        queryCacheContext = getContext().getQueryCacheContext();
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return containsKeyInternal(key);
    }

    protected boolean containsKeyInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage message = MapContainsKeyCodec.encodeRequest(name, keyData, getThreadId());
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

        return toObject(getInternal(key));
    }

    protected Object getInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapGetCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapGetCodec.ResponseParameters resultParameters = MapGetCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        MapRemoveCodec.ResponseParameters resultParameters = removeInternal(key);
        return toObject(resultParameters.response);
    }

    protected MapRemoveCodec.ResponseParameters removeInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapRemoveCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MapRemoveCodec.decodeResponse(response);
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return removeInternal(key, value);
    }

    protected boolean removeInternal(Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MapRemoveIfSameCodec.encodeRequest(name, keyData, valueData, getThreadId());

        ClientMessage response = invoke(request, keyData);
        MapRemoveIfSameCodec.ResponseParameters resultParameters = MapRemoveIfSameCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void removeAll(Predicate<K, V> predicate) {
        checkNotNull(predicate, "predicate cannot be null");

        removeAllInternal(predicate);
    }

    protected void removeAllInternal(Predicate predicate) {
        ClientMessage request = MapRemoveAllCodec.encodeRequest(name, toData(predicate));
        invoke(request);
    }

    @Override
    public void delete(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        deleteInternal(key);
    }

    protected void deleteInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapDeleteCodec.encodeRequest(name, keyData, getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void flush() {
        ClientMessage request = MapFlushCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return getAsyncInternal(key);
    }

    protected ICompletableFuture<V> getAsyncInternal(Object key) {
        try {
            Data keyData = toData(key);
            ClientMessage request = MapGetCodec.encodeRequest(name, keyData, getThreadId());
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<V>(future, getSerializationService(), GET_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private ClientInvocationFuture invokeOnKeyOwner(ClientMessage request, Data keyData) {
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);
        ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, partitionId);
        return clientInvocation.invoke();
    }

    @Override
    public ICompletableFuture<V> putAsync(K key, V value) {
        return putAsync(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture<V> putAsync(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return putAsyncInternal(ttl, timeunit, key, value);
    }

    protected ICompletableFuture<V> putAsyncInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        try {
            Data keyData = toData(key);
            Data valueData = toData(value);
            long ttlMillis = getTimeInMillis(ttl, timeunit);
            ClientMessage request = MapPutCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<V>(future, getSerializationService(), PUT_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public ICompletableFuture<Void> setAsync(K key, V value) {
        return setAsync(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return setAsyncInternal(ttl, timeunit, key, value);
    }

    protected ICompletableFuture<Void> setAsyncInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        try {
            Data keyData = toData(key);
            Data valueData = toData(value);
            long ttlMillis = getTimeInMillis(ttl, timeunit);
            ClientMessage request = MapSetCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<Void>(future, getSerializationService(), SET_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public ICompletableFuture<V> removeAsync(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        return removeAsyncInternal(key);
    }

    protected ICompletableFuture<V> removeAsyncInternal(Object key) {
        try {
            Data keyData = toData(key);
            ClientMessage request = MapRemoveCodec.encodeRequest(name, keyData, getThreadId());
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<V>(future, getSerializationService(), REMOVE_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        return tryRemoveInternal(timeout, timeunit, key);
    }

    protected boolean tryRemoveInternal(long timeout, TimeUnit timeunit, Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapTryRemoveCodec.encodeRequest(name, keyData, getThreadId(), timeunit.toMillis(timeout));
        ClientMessage response = invoke(request, keyData);
        MapTryRemoveCodec.ResponseParameters resultParameters = MapTryRemoveCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return tryPutInternal(timeout, timeunit, key, value);
    }

    protected boolean tryPutInternal(long timeout, TimeUnit timeunit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long timeoutMillis = getTimeInMillis(timeout, timeunit);
        ClientMessage request = MapTryPutCodec.encodeRequest(name, keyData, valueData, getThreadId(), timeoutMillis);
        ClientMessage response = invoke(request, keyData);
        MapTryPutCodec.ResponseParameters resultParameters = MapTryPutCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return putInternal(ttl, timeunit, key, value);
    }

    protected V putInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long ttlMillis = getTimeInMillis(ttl, timeunit);
        ClientMessage request = MapPutCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
        ClientMessage response = invoke(request, keyData);
        MapPutCodec.ResponseParameters resultParameters = MapPutCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        putTransientInternal(ttl, timeunit, key, value);
    }

    protected void putTransientInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long ttlMillis = getTimeInMillis(ttl, timeunit);
        ClientMessage request = MapPutTransientCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
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

        return putIfAbsentInternal(ttl, timeunit, key, value);
    }

    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long ttlMillis = getTimeInMillis(ttl, timeunit);
        ClientMessage request = MapPutIfAbsentCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
        ClientMessage result = invoke(request, keyData);
        MapPutIfAbsentCodec.ResponseParameters resultParameters = MapPutIfAbsentCodec.decodeResponse(result);
        return toObject(resultParameters.response);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(oldValue, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(newValue, NULL_VALUE_IS_NOT_ALLOWED);

        return replaceIfSameInternal(key, oldValue, newValue);
    }

    protected boolean replaceIfSameInternal(Object key, Object oldValue, Object newValue) {
        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);
        ClientMessage request = MapReplaceIfSameCodec.encodeRequest(name, keyData, oldValueData, newValueData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapReplaceIfSameCodec.ResponseParameters resultParameters = MapReplaceIfSameCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public V replace(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return replaceInternal(key, value);
    }

    protected V replaceInternal(Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MapReplaceCodec.encodeRequest(name, keyData, valueData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapReplaceCodec.ResponseParameters resultParameters = MapReplaceCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        setInternal(ttl, timeunit, key, value);
    }

    protected void setInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long ttlMillis = getTimeInMillis(ttl, timeunit);
        ClientMessage request = MapSetCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);

        invoke(request, keyData);
    }

    @Override
    public void lock(K key) {
        lock(key, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MapLockCodec.encodeRequest(name, keyData, getThreadId(), getTimeInMillis(leaseTime, timeUnit),
                lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
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
        return tryLock(key, time, timeunit, Long.MAX_VALUE, null);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        long leaseTimeMillis = getTimeInMillis(leaseTime, leaseUnit);
        long timeoutMillis = getTimeInMillis(time, timeunit);
        ClientMessage request = MapTryLockCodec.encodeRequest(name, keyData, getThreadId(), leaseTimeMillis, timeoutMillis,
                lockReferenceIdGenerator.getNextReferenceId());

        ClientMessage response = invoke(request, keyData);
        MapTryLockCodec.ResponseParameters resultParameters = MapTryLockCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MapUnlockCodec.encodeRequest(name, keyData, getThreadId(),
                lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MapForceUnlockCodec.encodeRequest(name, keyData, lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public String addLocalEntryListener(MapListener listener) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String addLocalEntryListener(EntryListener listener) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
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
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, includeValue);
    }

    private String addEntryListenerInternal(ListenerAdapter<IMapEvent> listenerAdaptor, boolean includeValue) {
        int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        EventHandler<ClientMessage> handler = createHandler(listenerAdaptor);
        return registerListener(createMapEntryListenerCodec(includeValue, listenerFlags), handler);
    }

    private ListenerMessageCodec createMapEntryListenerCodec(final boolean includeValue, final int listenerFlags) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddEntryListenerCodec.encodeRequest(name, includeValue, listenerFlags, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return MapAddEntryListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public boolean removeEntryListener(String registrationId) {
        return deregisterListener(registrationId);
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        EventHandler<ClientMessage> handler = new ClientMapPartitionLostEventHandler(listener);
        return registerListener(createMapPartitionListenerCodec(), handler);
    }

    private ListenerMessageCodec createMapPartitionListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddPartitionLostListenerCodec.encodeRequest(name, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return MapAddPartitionLostListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return MapRemovePartitionLostListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemovePartitionLostListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public boolean removePartitionLostListener(String registrationId) {
        return deregisterListener(registrationId);
    }

    @Override
    public String addEntryListener(MapListener listener, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, key, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, key, includeValue);
    }

    private String addEntryListenerInternal(ListenerAdapter<IMapEvent> listenerAdaptor, K key, boolean includeValue) {
        int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        Data keyData = toData(key);
        EventHandler<ClientMessage> handler = createHandler(listenerAdaptor);
        return registerListener(createMapEntryListenerToKeyCodec(includeValue, listenerFlags, keyData), handler);
    }

    private ListenerMessageCodec createMapEntryListenerToKeyCodec(final boolean includeValue, final int listenerFlags,
                                                                  final Data keyData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddEntryListenerToKeyCodec.encodeRequest(name, keyData, includeValue, listenerFlags, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return MapAddEntryListenerToKeyCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, key, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, key, includeValue);
    }

    private String addEntryListenerInternal(ListenerAdapter<IMapEvent> listenerAdaptor, Predicate<K, V> predicate, K key,
                                            boolean includeValue) {
        int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        Data keyData = toData(key);
        Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = createHandler(listenerAdaptor);
        ListenerMessageCodec codec = createEntryListenerToKeyWithPredicateCodec(includeValue, listenerFlags, keyData,
                predicateData);
        return registerListener(codec, handler);
    }

    private ListenerMessageCodec createEntryListenerToKeyWithPredicateCodec(final boolean includeValue, final int listenerFlags,
                                                                            final Data keyData, final Data predicateData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(name, keyData, predicateData, includeValue,
                        listenerFlags, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return MapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, includeValue);
    }

    private String addEntryListenerInternal(ListenerAdapter<IMapEvent> listenerAdaptor, Predicate<K, V> predicate,
                                            boolean includeValue) {
        int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = createHandler(listenerAdaptor);
        return registerListener(createEntryListenerWithPredicateCodec(includeValue, listenerFlags, predicateData), handler);
    }

    private ListenerMessageCodec createEntryListenerWithPredicateCodec(final boolean includeValue, final int listenerFlags,
                                                                       final Data predicateData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddEntryListenerWithPredicateCodec.encodeRequest(name, predicateData, includeValue, listenerFlags,
                        localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return MapAddEntryListenerWithPredicateCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public EntryView<K, V> getEntryView(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MapGetEntryViewCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);

        MapGetEntryViewCodec.ResponseParameters parameters = MapGetEntryViewCodec.decodeResponse(response);
        SimpleEntryView<K, V> entryView = new SimpleEntryView<K, V>();
        SimpleEntryView<Data, Data> dataEntryView = parameters.response;

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
        entryView.setHits(dataEntryView.getHits());
        entryView.setTtl(dataEntryView.getTtl());
        // TODO: putCache
        return entryView;
    }

    @Override
    public boolean evict(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return evictInternal(key);
    }

    protected boolean evictInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapEvictCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapEvictCodec.ResponseParameters resultParameters = MapEvictCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void evictAll() {
        ClientMessage request = MapEvictAllCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        ClientMessage request = MapLoadAllCodec.encodeRequest(name, replaceExistingValues);
        invoke(request);
    }

    @Override
    public void loadAll(Set<K> keys, boolean replaceExistingValues) {
        checkNotNull(keys, "Parameter keys should not be null.");
        if (keys.isEmpty()) {
            return;
        }

        loadAllInternal(replaceExistingValues, keys);
    }

    protected void loadAllInternal(boolean replaceExistingValues, Collection<?> keys) {
        Collection<Data> dataKeys = objectToDataCollection(keys, getSerializationService());
        ClientMessage request = MapLoadGivenKeysCodec.encodeRequest(name, dataKeys, replaceExistingValues);
        invoke(request);
    }

    @Override
    public Set<K> keySet() {
        ClientMessage request = MapKeySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MapKeySetCodec.ResponseParameters resultParameters = MapKeySetCodec.decodeResponse(response);

        InflatableSet.Builder<K> setBuilder = InflatableSet.newBuilder(resultParameters.response.size());
        for (Data data : resultParameters.response) {
            K key = toObject(data);
            setBuilder.add(key);
        }
        return setBuilder.build();
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            return emptyMap();
        }
        Map<Integer, List<Data>> partitionToKeyData = new HashMap<Integer, List<Data>>();
        Map<K, V> result = new HashMap<K, V>();
        getAllInternal(keys, partitionToKeyData, result);
        return result;
    }

    protected List<MapGetAllCodec.ResponseParameters> getAllInternal(Set<K> keys, Map<Integer, List<Data>> partitionToKeyData,
                                                                     Map<K, V> result) {
        if (partitionToKeyData.isEmpty()) {
            fillPartitionToKeyData(keys, partitionToKeyData);
        }
        List<Future<ClientMessage>> futures = new ArrayList<Future<ClientMessage>>(partitionToKeyData.size());
        List<MapGetAllCodec.ResponseParameters> responses = new ArrayList<MapGetAllCodec.ResponseParameters>(
                partitionToKeyData.size());

        for (Map.Entry<Integer, List<Data>> entry : partitionToKeyData.entrySet()) {
            int partitionId = entry.getKey();
            List<Data> keyList = entry.getValue();
            if (!keyList.isEmpty()) {
                ClientMessage request = MapGetAllCodec.encodeRequest(name, keyList);
                futures.add(new ClientInvocation(getClient(), request, partitionId).invoke());
            }
        }

        for (Future<ClientMessage> future : futures) {
            try {
                ClientMessage response = future.get();
                MapGetAllCodec.ResponseParameters resultParameters = MapGetAllCodec.decodeResponse(response);
                for (Entry<Data, Data> entry : resultParameters.response) {
                    V value = toObject(entry.getValue());
                    K key = toObject(entry.getKey());
                    result.put(key, value);
                }
                responses.add(resultParameters);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
        return responses;
    }

    protected void fillPartitionToKeyData(Set<K> keys, Map<Integer, List<Data>> partitionToKeyData) {
        ClientPartitionService partitionService = getContext().getPartitionService();
        for (K key : keys) {
            Data keyData = toData(key);
            int partitionId = partitionService.getPartitionId(keyData);
            List<Data> keyList = partitionToKeyData.get(partitionId);
            if (keyList == null) {
                keyList = new ArrayList<Data>();
                partitionToKeyData.put(partitionId, keyList);
            }
            keyList.add(keyData);
        }
    }

    @Override
    public Collection<V> values() {
        ClientMessage request = MapValuesCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MapValuesCodec.ResponseParameters resultParameters = MapValuesCodec.decodeResponse(response);
        return new UnmodifiableLazyList<V>(resultParameters.response, getSerializationService());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        ClientMessage request = MapEntrySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MapEntrySetCodec.ResponseParameters resultParameters = MapEntrySetCodec.decodeResponse(response);

        InflatableSet.Builder<Entry<K, V>> setBuilder = InflatableSet.newBuilder(resultParameters.response.size());
        InternalSerializationService serializationService = ((InternalSerializationService) getContext()
                .getSerializationService());
        for (Entry<Data, Data> row : resultParameters.response) {
            LazyMapEntry<K, V> entry = new LazyMapEntry<K, V>(row.getKey(), row.getValue(), serializationService);
            setBuilder.add(entry);
        }
        return setBuilder.build();
    }

    @Override
    public Set<K> keySet(Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        if (predicate instanceof PagingPredicate) {
            return keySetWithPagingPredicate((PagingPredicate) predicate);
        }

        ClientMessage request = MapKeySetWithPredicateCodec.encodeRequest(name, toData(predicate));
        ClientMessage response = invoke(request);
        MapKeySetWithPredicateCodec.ResponseParameters resultParameters = MapKeySetWithPredicateCodec.decodeResponse(response);

        InflatableSet.Builder<K> setBuilder = InflatableSet.newBuilder(resultParameters.response.size());
        for (Data data : resultParameters.response) {
            K key = toObject(data);
            setBuilder.add(key);
        }
        return setBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private Set<K> keySetWithPagingPredicate(PagingPredicate pagingPredicate) {
        pagingPredicate.setIterationType(IterationType.KEY);
        ClientMessage request = MapKeySetWithPagingPredicateCodec.encodeRequest(name, toData(pagingPredicate));

        ClientMessage response = invoke(request);
        MapKeySetWithPagingPredicateCodec.ResponseParameters resultParameters = MapKeySetWithPagingPredicateCodec
                .decodeResponse(response);

        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        for (Data keyData : resultParameters.response) {
            K key = toObject(keyData);
            resultList.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, null));
        }
        return (Set<K>) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.KEY);
    }

    @Override
    public Set<Entry<K, V>> entrySet(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            return entrySetWithPagingPredicate((PagingPredicate) predicate);
        }
        ClientMessage request = MapEntriesWithPredicateCodec.encodeRequest(name, toData(predicate));

        ClientMessage response = invoke(request);
        MapEntriesWithPredicateCodec.ResponseParameters resultParameters = MapEntriesWithPredicateCodec.decodeResponse(response);

        InflatableSet.Builder<Entry<K, V>> setBuilder = InflatableSet.newBuilder(resultParameters.response.size());
        InternalSerializationService serializationService = ((InternalSerializationService) getContext()
                .getSerializationService());
        for (Entry<Data, Data> row : resultParameters.response) {
            LazyMapEntry<K, V> entry = new LazyMapEntry<K, V>(row.getKey(), row.getValue(), serializationService);
            setBuilder.add(entry);
        }
        return setBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private Set<Entry<K, V>> entrySetWithPagingPredicate(PagingPredicate pagingPredicate) {
        pagingPredicate.setIterationType(IterationType.ENTRY);

        ClientMessage request = MapEntriesWithPagingPredicateCodec.encodeRequest(name, toData(pagingPredicate));

        ClientMessage response = invoke(request);
        MapEntriesWithPagingPredicateCodec.ResponseParameters resultParameters = MapEntriesWithPagingPredicateCodec
                .decodeResponse(response);

        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        for (Entry<Data, Data> entry : resultParameters.response) {
            K key = toObject(entry.getKey());
            V value = toObject(entry.getValue());
            resultList.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return (Set<Entry<K, V>>) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.ENTRY);
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        if (predicate instanceof PagingPredicate) {
            return valuesForPagingPredicate((PagingPredicate) predicate);
        }

        ClientMessage request = MapValuesWithPredicateCodec.encodeRequest(name, toData(predicate));
        ClientMessage response = invoke(request);
        MapValuesWithPredicateCodec.ResponseParameters resultParameters = MapValuesWithPredicateCodec.decodeResponse(response);

        return new UnmodifiableLazyList<V>(resultParameters.response, getSerializationService());
    }

    @SuppressWarnings("unchecked")
    private Collection<V> valuesForPagingPredicate(PagingPredicate pagingPredicate) {
        pagingPredicate.setIterationType(IterationType.VALUE);

        ClientMessage request = MapValuesWithPagingPredicateCodec.encodeRequest(name, toData(pagingPredicate));
        ClientMessage response = invoke(request);
        MapValuesWithPagingPredicateCodec.ResponseParameters resultParameters = MapValuesWithPagingPredicateCodec
                .decodeResponse(response);

        List<Entry> resultList = new ArrayList<Entry>(resultParameters.response.size());
        for (Entry<Data, Data> entry : resultParameters.response) {
            K key = toObject(entry.getKey());
            V value = toObject(entry.getValue());
            resultList.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
        }
        return (Collection<V>) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.VALUE);
    }

    @Override
    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public Set<K> localKeySet(Predicate predicate) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public void addIndex(String attribute, boolean ordered) {
        ClientMessage request = MapAddIndexCodec.encodeRequest(name, attribute, ordered);
        invoke(request);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return new LocalMapStatsImpl();
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        return executeOnKeyInternal(key, entryProcessor);
    }

    public Object executeOnKeyInternal(Object key, EntryProcessor entryProcessor) {
        validateEntryProcessorForSingleKeyProcessing(entryProcessor);
        Data keyData = toData(key);
        ClientMessage request = MapExecuteOnKeyCodec.encodeRequest(name, toData(entryProcessor), keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapExecuteOnKeyCodec.ResponseParameters resultParameters = MapExecuteOnKeyCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void submitToKey(K key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        submitToKeyInternal(key, entryProcessor, callback);
    }

    @SuppressWarnings("unchecked")
    public void submitToKeyInternal(Object key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        try {
            Data keyData = toData(key);
            ClientMessage request = MapSubmitToKeyCodec.encodeRequest(name, toData(entryProcessor), keyData, getThreadId());
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            new ClientDelegatingFuture(future, getSerializationService(), SUBMIT_TO_KEY_RESPONSE_DECODER)
                    .andThen(callback);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public ICompletableFuture submitToKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        return submitToKeyInternal(key, entryProcessor);
    }

    public ICompletableFuture submitToKeyInternal(Object key, EntryProcessor entryProcessor) {
        try {
            Data keyData = toData(key);
            ClientMessage request = MapSubmitToKeyCodec.encodeRequest(name, toData(entryProcessor), keyData, getThreadId());
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture(future, getSerializationService(), SUBMIT_TO_KEY_RESPONSE_DECODER);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
        ClientMessage request = MapExecuteOnAllKeysCodec.encodeRequest(name, toData(entryProcessor));
        ClientMessage response = invoke(request);
        MapExecuteOnAllKeysCodec.ResponseParameters resultParameters = MapExecuteOnAllKeysCodec.decodeResponse(response);
        return prepareResult(resultParameters.response);
    }

    protected Map<K, Object> prepareResult(Collection<Entry<Data, Data>> entries) {
        if (CollectionUtil.isEmpty(entries)) {
            return emptyMap();
        }

        Map<K, Object> result = createHashMap(entries.size());
        for (Entry<Data, Data> entry : entries) {
            K key = toObject(entry.getKey());
            result.put(key, toObject(entry.getValue()));
        }
        return result;
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
        ClientMessage request = MapExecuteWithPredicateCodec.encodeRequest(name, toData(entryProcessor), toData(predicate));
        ClientMessage response = invoke(request);

        MapExecuteWithPredicateCodec.ResponseParameters resultParameters = MapExecuteWithPredicateCodec.decodeResponse(response);
        return prepareResult(resultParameters.response);
    }

    @Override
    public <R> R aggregate(Aggregator<Map.Entry<K, V>, R> aggregator) {
        checkNotNull(aggregator, NULL_AGGREGATOR_IS_NOT_ALLOWED);

        ClientMessage request = MapAggregateCodec.encodeRequest(name, toData(aggregator));
        ClientMessage response = invoke(request);

        MapAggregateCodec.ResponseParameters resultParameters = MapAggregateCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public <R> R aggregate(Aggregator<Map.Entry<K, V>, R> aggregator, Predicate<K, V> predicate) {
        checkNotNull(aggregator, NULL_AGGREGATOR_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        ClientMessage request = MapAggregateWithPredicateCodec.encodeRequest(name, toData(aggregator), toData(predicate));
        ClientMessage response = invoke(request);

        MapAggregateWithPredicateCodec.ResponseParameters resultParameters =
                MapAggregateWithPredicateCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public <R> Collection<R> project(Projection<Entry<K, V>, R> projection) {
        ClientMessage request = MapProjectCodec.encodeRequest(name, toData(projection));
        ClientMessage response = invoke(request);

        MapProjectCodec.ResponseParameters resultParameters =
                MapProjectCodec.decodeResponse(response);

        return new UnmodifiableLazyList<R>(resultParameters.response, getSerializationService());
    }

    @Override
    public <R> Collection<R> project(Projection<Entry<K, V>, R> projection, Predicate<K, V> predicate) {
        ClientMessage request = MapProjectWithPredicateCodec.encodeRequest(name, toData(projection), toData(predicate));
        ClientMessage response = invoke(request);

        MapProjectWithPredicateCodec.ResponseParameters resultParameters =
                MapProjectWithPredicateCodec.decodeResponse(response);

        return new UnmodifiableLazyList<R>(resultParameters.response, getSerializationService());
    }


    @Override
    @SuppressWarnings({"deprecation"})
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {
        JobTracker jobTracker = getClient().getJobTracker("hz::aggregation-map-" + name);
        return aggregate(supplier, aggregation, jobTracker);
    }

    @Override
    @SuppressWarnings({"deprecation", "unchecked"})
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation, JobTracker jobTracker) {
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
    public QueryCache<K, V> getQueryCache(String name) {
        checkNotNull(name, "name cannot be null");

        return getQueryCacheInternal(name, null, null, null, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        return getQueryCacheInternal(name, null, predicate, includeValue, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name, MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        return getQueryCacheInternal(name, listener, predicate, includeValue, this);
    }

    private QueryCache<K, V> getQueryCacheInternal(String name, MapListener listener, Predicate predicate, Boolean includeValue,
                                                   IMap map) {
        QueryCacheRequest request = newQueryCacheRequest()
                .withUserGivenCacheName(name)
                .withCacheName(UuidUtil.newUnsecureUuidString())
                .withListener(listener)
                .withPredicate(predicate)
                .withIncludeValue(includeValue)
                .forMap(map)
                .withContext(queryCacheContext);

        return createQueryCache(request);
    }

    @SuppressWarnings("unchecked")
    private QueryCache<K, V> createQueryCache(QueryCacheRequest request) {
        ConstructorFunction<String, InternalQueryCache> constructorFunction
                = new ClientQueryCacheEndToEndConstructor(request);
        SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
        QueryCacheEndToEndProvider queryCacheEndToEndProvider = subscriberContext.getEndToEndQueryCacheProvider();
        return queryCacheEndToEndProvider.getOrCreateQueryCache(request.getMapName(),
                request.getUserGivenCacheName(), constructorFunction);
    }

    @Override
    public Map<K, Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor) {
        checkNotNull(keys, NULL_KEY_IS_NOT_ALLOWED);
        if (keys.isEmpty()) {
            return emptyMap();
        }
        Collection<Data> dataCollection = objectToDataCollection(keys, getSerializationService());

        ClientMessage request = MapExecuteOnKeysCodec.encodeRequest(name, toData(entryProcessor), dataCollection);
        ClientMessage response = invoke(request);
        MapExecuteOnKeysCodec.ResponseParameters resultParameters = MapExecuteOnKeysCodec.decodeResponse(response);
        return prepareResult(resultParameters.response);
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
    public void putAll(Map<? extends K, ? extends V> map) {
        ClientPartitionService partitionService = getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Map<Integer, List<Map.Entry<Data, Data>>> entryMap = new HashMap<Integer, List<Map.Entry<Data, Data>>>(partitionCount);

        for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
            checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
            checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

            Data keyData = toData(entry.getKey());
            int partitionId = partitionService.getPartitionId(keyData);
            List<Map.Entry<Data, Data>> partition = entryMap.get(partitionId);
            if (partition == null) {
                partition = new ArrayList<Map.Entry<Data, Data>>();
                entryMap.put(partitionId, partition);
            }
            partition.add(new AbstractMap.SimpleEntry<Data, Data>(keyData, toData(entry.getValue())));
        }
        putAllInternal(map, entryMap);
    }

    protected void putAllInternal(Map<? extends K, ? extends V> map, Map<Integer, List<Map.Entry<Data, Data>>> entryMap) {
        List<Future<?>> futures = new ArrayList<Future<?>>(entryMap.size());
        for (Entry<Integer, List<Map.Entry<Data, Data>>> entry : entryMap.entrySet()) {
            Integer partitionId = entry.getKey();
            // if there is only one entry, consider how we can use MapPutRequest
            // without having to get back the return value
            ClientMessage request = MapPutAllCodec.encodeRequest(name, entry.getValue());
            futures.add(new ClientInvocation(getClient(), request, partitionId).invoke());
        }
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void clear() {
        ClientMessage request = MapClearCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public String toString() {
        return "IMap{" + "name='" + name + '\'' + '}';
    }

    // used for testing
    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        return new ClientMapPartitionIterator<K, V>(this, getContext(), fetchSize, partitionId, prefetchValues);
    }

    // used for testing
    public ClientQueryCacheContext getQueryCacheContext() {
        return queryCacheContext;
    }

    private long getTimeInMillis(long time, TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private EventHandler<ClientMessage> createHandler(ListenerAdapter<IMapEvent> listenerAdapter) {
        return new ClientMapEventHandler(listenerAdapter);
    }

    private static void validateEntryProcessorForSingleKeyProcessing(EntryProcessor entryProcessor) {
        if (entryProcessor instanceof ReadOnly) {
            EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
            if (backupProcessor != null) {
                throw new IllegalArgumentException(
                        "EntryProcessor.getBackupProcessor() should be null for a read-only EntryProcessor");
            }
        }
    }

    private class ClientMapEventHandler
            extends MapAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private ListenerAdapter<IMapEvent> listenerAdapter;

        ClientMapEventHandler(ListenerAdapter<IMapEvent> listenerAdapter) {
            this.listenerAdapter = listenerAdapter;
        }

        @Override
        public void handle(Data key, Data value, Data oldValue, Data mergingValue, int eventType, String uuid,
                           int numberOfAffectedEntries) {
            Member member = getContext().getClusterService().getMember(uuid);
            listenerAdapter.onEvent(createIMapEvent(key, value, oldValue, mergingValue, eventType, numberOfAffectedEntries,
                    member));
        }

        private IMapEvent createIMapEvent(Data key, Data value, Data oldValue, Data mergingValue, int eventType,
                                          int numberOfAffectedEntries, Member member) {
            EntryEventType entryEventType = EntryEventType.getByType(eventType);
            checkNotNull(entryEventType, "Unknown eventType: " + eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                case EXPIRED:
                case MERGED:
                    return createEntryEvent(key, value, oldValue, mergingValue, eventType, member);
                case EVICT_ALL:
                case CLEAR_ALL:
                    return createMapEvent(eventType, numberOfAffectedEntries, member);
                default:
                    throw new IllegalArgumentException("Not a known event type: " + entryEventType);
            }
        }

        private MapEvent createMapEvent(int eventType, int numberOfAffectedEntries, Member member) {
            return new MapEvent(name, member, eventType, numberOfAffectedEntries);
        }

        private EntryEvent<K, V> createEntryEvent(Data keyData, Data valueData, Data oldValueData, Data mergingValueData,
                                                  int eventType, Member member) {
            return new DataAwareEntryEvent<K, V>(member, eventType, name, keyData, valueData, oldValueData, mergingValueData,
                    getSerializationService());
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    private class ClientMapPartitionLostEventHandler
            extends MapAddPartitionLostListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private MapPartitionLostListener listener;

        ClientMapPartitionLostEventHandler(MapPartitionLostListener listener) {
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
            Member member = getContext().getClusterService().getMember(uuid);
            listener.partitionLost(new MapPartitionLostEvent(name, member, -1, partitionId));
        }
    }
}
