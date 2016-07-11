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

package com.hazelcast.client.proxy;

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
import com.hazelcast.client.impl.protocol.codec.MapClearCodec;
import com.hazelcast.client.impl.protocol.codec.MapClearNearCacheCodec;
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
import com.hazelcast.client.impl.protocol.codec.MapPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec;
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
import com.hazelcast.client.map.impl.ClientMapPartitionIterator;
import com.hazelcast.client.spi.ClientClusterService;
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.LazyMapEntry;
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
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.MapUtil;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.ThreadUtil;
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

import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.map.impl.MapListenerFlagOperator.setAndGetListenerFlags;
import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.SortingUtil.getSortedQueryResultSet;
import static java.util.Collections.emptyMap;

/**
 * Proxy implementation of {@link IMap}.
 *
 * @param <K> key
 * @param <V> value
 */
public class ClientMapProxy<K, V>
        extends ClientProxy
        implements IMap<K, V> {

    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Predicate should not be null!";

    private static final ClientMessageDecoder GET_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder PUT_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapPutCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder SET_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private static final ClientMessageDecoder REMOVE_ASYNC_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapRemoveCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder SUBMIT_TO_KEY_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) MapSubmitToKeyCodec.decodeResponse(clientMessage).response;
        }
    };

    private ClientLockReferenceIdGenerator lockReferenceIdGenerator;

    public ClientMapProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        return containsKeyInternal(keyData);
    }

    protected boolean containsKeyInternal(Data keyData) {
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

        Data keyData = toData(key);
        return getInternal(keyData);
    }

    protected V getInternal(Data keyData) {
        ClientMessage request = MapGetCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapGetCodec.ResponseParameters resultParameters = MapGetCodec.decodeResponse(response);
        V result = toObject(resultParameters.response);
        return result;
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);

        MapRemoveCodec.ResponseParameters resultParameters = removeInternal(keyData);
        return toObject(resultParameters.response);
    }

    protected MapRemoveCodec.ResponseParameters removeInternal(Data keyData) {
        ClientMessage request = MapRemoveCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MapRemoveCodec.decodeResponse(response);
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
        ClientMessage request = MapRemoveIfSameCodec.encodeRequest(name, keyData, valueData, ThreadUtil.getThreadId());

        ClientMessage response = invoke(request, keyData);
        MapRemoveIfSameCodec.ResponseParameters resultParameters = MapRemoveIfSameCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void delete(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        deleteInternal(keyData);
    }

    protected void deleteInternal(Data keyData) {
        ClientMessage request = MapDeleteCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Override
    public void flush() {
        ClientMessage request = MapFlushCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public ICompletableFuture<V> getAsync(final K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);

        return getAsyncInternal(keyData);
    }

    protected ICompletableFuture<V> getAsyncInternal(Data keyData) {

        SerializationService serializationService = getContext().getSerializationService();

        ClientMessage request = MapGetCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        try {
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<V>(future, serializationService, GET_ASYNC_RESPONSE_DECODER);

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
    public ICompletableFuture<V> putAsync(final K key, final V value) {
        return putAsync(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture<V> putAsync(final K key, final V value, final long ttl, final TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        final Data valueData = toData(value);
        return putAsyncInternal(ttl, timeunit, keyData, valueData);
    }

    protected ICompletableFuture<V> putAsyncInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        ClientMessage request = MapPutCodec.encodeRequest(name, keyData,
                valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        try {
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<V>(future, getContext().getSerializationService(),
                    PUT_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public ICompletableFuture<Void> setAsync(final K key, final V value) {
        return setAsync(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture<Void> setAsync(final K key, final V value, final long ttl, final TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        final Data valueData = toData(value);
        return setAsyncInternal(ttl, timeunit, keyData, valueData);
    }

    protected ICompletableFuture<Void> setAsyncInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        ClientMessage request = MapSetCodec.encodeRequest(name, keyData,
                valueData, ThreadUtil.getThreadId(), getTimeInMillis(ttl, timeunit));
        try {
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<Void>(future, getContext().getSerializationService(),
                    SET_ASYNC_RESPONSE_DECODER);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public ICompletableFuture<V> removeAsync(final K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        return removeAsyncInternal(keyData);
    }

    protected ICompletableFuture<V> removeAsyncInternal(Data keyData) {
        ClientMessage request = MapRemoveCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        try {
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<V>(future, getContext().getSerializationService(),
                    REMOVE_ASYNC_RESPONSE_DECODER);
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

    protected boolean tryRemoveInternal(long timeout, TimeUnit timeunit, Data keyData) {
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

        return tryPutInternal(timeout, timeunit, keyData, valueData);
    }

    protected boolean tryPutInternal(long timeout, TimeUnit timeunit, Data keyData, Data valueData) {
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

        return putInternal(ttl, timeunit, keyData, valueData);
    }

    protected V putInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
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

        Data keyData = toData(key);
        Data valueData = toData(value);

        putTransientInternal(ttl, timeunit, keyData, valueData);
    }

    protected void putTransientInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
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

        Data keyData = toData(key);
        Data valueData = toData(value);
        return putIfAbsentInternal(ttl, timeunit, keyData, valueData);
    }

    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
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

        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);

        return replaceIfSameInternal(keyData, oldValueData, newValueData);
    }

    protected boolean replaceIfSameInternal(Data keyData, Data oldValueData, Data newValueData) {
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

        Data keyData = toData(key);
        Data valueData = toData(value);

        return replaceInternal(keyData, valueData);
    }

    protected V replaceInternal(Data keyData, Data valueData) {
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

        Data keyData = toData(key);
        Data valueData = toData(value);

        setInternal(ttl, timeunit, keyData, valueData);
    }

    protected void setInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
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
                ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit), lockReferenceIdGenerator.getNextReferenceId());
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
        return tryLock(key, time, timeunit, Long.MAX_VALUE, null);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        long timeoutInMillis = getTimeInMillis(time, timeunit);
        long leaseTimeInMillis = getTimeInMillis(leaseTime, leaseUnit);
        long threadId = ThreadUtil.getThreadId();
        ClientMessage request = MapTryLockCodec.encodeRequest(name, keyData, threadId, leaseTimeInMillis, timeoutInMillis,
                lockReferenceIdGenerator.getNextReferenceId());

        ClientMessage response = invoke(request, keyData);
        MapTryLockCodec.ResponseParameters resultParameters = MapTryLockCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapUnlockCodec
                .encodeRequest(name, keyData, ThreadUtil.getThreadId(), lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapForceUnlockCodec.encodeRequest(name, keyData, lockReferenceIdGenerator.getNextReferenceId());
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
    public String addEntryListener(MapListener listener, final boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, final boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, includeValue);
    }

    private String addEntryListenerInternal(ListenerAdapter listenerAdaptor, final boolean includeValue) {
        final int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
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
        final EventHandler<ClientMessage> handler = new ClientMapPartitionLostEventHandler(listener);
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
    public String addEntryListener(MapListener listener, K key, final boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, key, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, key, includeValue);
    }

    private String addEntryListenerInternal(ListenerAdapter listenerAdaptor, K key, final boolean includeValue) {
        final int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        final Data keyData = toData(key);
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
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, key, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, key, includeValue);
    }

    private String addEntryListenerInternal(ListenerAdapter listenerAdaptor, Predicate<K, V> predicate, K key,
                                            final boolean includeValue) {
        final int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        final Data keyData = toData(key);
        final Data predicateData = toData(predicate);
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
                return MapAddEntryListenerToKeyWithPredicateCodec
                        .encodeRequest(name, keyData, predicateData, includeValue, listenerFlags, localOnly);
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
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, includeValue);
    }

    private String addEntryListenerInternal(ListenerAdapter listenerAdaptor, Predicate<K, V> predicate,
                                            final boolean includeValue) {
        final int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        final Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = createHandler(listenerAdaptor);
        return registerListener(createEntryListenerWithPredicateCodec(includeValue, listenerFlags, predicateData), handler);
    }

    private ListenerMessageCodec createEntryListenerWithPredicateCodec(final boolean includeValue, final int listenerFlags,
                                                                       final Data predicateData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddEntryListenerWithPredicateCodec
                        .encodeRequest(name, predicateData, includeValue, listenerFlags, localOnly);
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
    public EntryView<K, V> getEntryView(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ClientMessage request = MapGetEntryViewCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
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
        //TODO putCache
        return entryView;
    }

    @Override
    public boolean evict(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);

        return evictInternal(keyData);
    }

    protected boolean evictInternal(Data keyData) {
        ClientMessage request = MapEvictCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapEvictCodec.ResponseParameters resultParameters = MapEvictCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void evictAll() {
        ClientMessage request = MapEvictAllCodec.encodeRequest(name);
        invoke(request);

        clearNearCachesOnLiteMembers();
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

        Collection<Data> dataKeys = CollectionUtil.objectToDataCollection(keys, getSerializationService());
        loadAllInternal(replaceExistingValues, dataKeys);
    }

    protected void loadAllInternal(boolean replaceExistingValues, Collection<Data> dataKeys) {
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
    @SuppressWarnings("unchecked")
    public Map<K, V> getAll(Set<K> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            return emptyMap();
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

    protected List<MapGetAllCodec.ResponseParameters> getAllInternal(Map<Integer, List<Data>> partitionToKeyData,
                                                                     Map<K, V> result) {

        List<Future<ClientMessage>> futures = new ArrayList<Future<ClientMessage>>(partitionToKeyData.size());
        List<MapGetAllCodec.ResponseParameters> responses = new ArrayList<MapGetAllCodec.ResponseParameters>(
                partitionToKeyData.size());

        for (final Map.Entry<Integer, List<Data>> entry : partitionToKeyData.entrySet()) {
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
                    final V value = toObject(entry.getValue());
                    final K key = toObject(entry.getKey());
                    result.put(key, value);
                }

                responses.add(resultParameters);
            } catch (Exception e) {
                ExceptionUtil.rethrow(e);
            }
        }
        return responses;
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
            LazyMapEntry entry = new LazyMapEntry(row.getKey(), row.getValue(), serializationService);
            setBuilder.add(entry);
        }
        return setBuilder.build();
    }

    @Override
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
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
            LazyMapEntry entry = new LazyMapEntry(row.getKey(), row.getValue(), serializationService);
            setBuilder.add(entry);
        }
        return setBuilder.build();

    }

    public Set<Entry<K, V>> entrySetWithPagingPredicate(PagingPredicate pagingPredicate) {
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
        return (Set) getSortedQueryResultSet(resultList, pagingPredicate, IterationType.ENTRY);
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
        return new LocalMapStatsImpl();
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        return executeOnKeyInternal(keyData, entryProcessor);
    }

    public Object executeOnKeyInternal(Data keyData, EntryProcessor entryProcessor) {
        ClientMessage request = MapExecuteOnKeyCodec
                .encodeRequest(name, toData(entryProcessor), keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MapExecuteOnKeyCodec.ResponseParameters resultParameters = MapExecuteOnKeyCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public void submitToKey(K key, EntryProcessor entryProcessor, final ExecutionCallback callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        submitToKeyInternal(keyData, entryProcessor, callback);
    }

    public void submitToKeyInternal(Data keyData, EntryProcessor entryProcessor, final ExecutionCallback callback) {
        ClientMessage request = MapSubmitToKeyCodec
                .encodeRequest(name, toData(entryProcessor), keyData, ThreadUtil.getThreadId());
        try {
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            SerializationService serializationService = getContext().getSerializationService();
            ClientDelegatingFuture clientDelegatingFuture = new ClientDelegatingFuture(future, serializationService,
                    SUBMIT_TO_KEY_RESPONSE_DECODER);
            clientDelegatingFuture.andThen(callback);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public ICompletableFuture submitToKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        return submitToKeyInternal(keyData, entryProcessor);
    }

    public ICompletableFuture submitToKeyInternal(Data keyData, EntryProcessor entryProcessor) {
        ClientMessage request = MapSubmitToKeyCodec
                .encodeRequest(name, toData(entryProcessor), keyData, ThreadUtil.getThreadId());

        try {
            final ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture(future, getContext().getSerializationService(), SUBMIT_TO_KEY_RESPONSE_DECODER);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
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

        Map<K, Object> result = MapUtil.createHashMap(entries.size());
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
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {

        HazelcastInstance hazelcastInstance = getContext().getHazelcastInstance();
        JobTracker jobTracker = hazelcastInstance.getJobTracker("hz::aggregation-map-" + name);
        return aggregate(supplier, aggregation, jobTracker);
    }

    @Override
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
    public void putAll(Map<? extends K, ? extends V> m) {
        ClientPartitionService partitionService = getContext().getPartitionService();
        Map<Integer, List<Map.Entry<Data, Data>>> entryMap = new HashMap<Integer, List<Map.Entry<Data, Data>>>(
                partitionService.getPartitionCount());

        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
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

        putAllInternal(entryMap);
    }

    protected void putAllInternal(Map<Integer, List<Map.Entry<Data, Data>>> entryMap) {
        List<Future<?>> futures = new ArrayList<Future<?>>(entryMap.size());
        for (final Entry<Integer, List<Map.Entry<Data, Data>>> entry : entryMap.entrySet()) {
            final Integer partitionId = entry.getKey();
            //If there is only one entry, consider how we can use MapPutRequest
            //without having to get back the return value.
            ClientMessage request = MapPutAllCodec.encodeRequest(name, entry.getValue());
            futures.add(new ClientInvocation(getClient(), request, partitionId).invoke());
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
        ClientMessage request = MapClearCodec.encodeRequest(name);
        invoke(request);

        clearNearCachesOnLiteMembers();
    }

    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        return new ClientMapPartitionIterator<K, V>(this, getContext(), fetchSize, partitionId, prefetchValues);
    }

    private void clearNearCachesOnLiteMembers() {
        final ClientClusterService clusterService = getClient().getClientClusterService();
        for (Member member : clusterService.getMembers(LITE_MEMBER_SELECTOR)) {
            final ClientMessage request = MapClearNearCacheCodec.encodeRequest(name, member.getAddress());
            invoke(request, member.getAddress());
        }
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private EventHandler<ClientMessage> createHandler(final ListenerAdapter listenerAdapter) {
        return new ClientMapEventHandler(listenerAdapter);
    }

    @Override
    public String toString() {
        return "IMap{" + "name='" + name + '\'' + '}';
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        lockReferenceIdGenerator = getClient().getLockReferenceIdGenerator();
    }

    private class ClientMapEventHandler
            extends MapAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final ListenerAdapter listenerAdapter;

        public ClientMapEventHandler(ListenerAdapter listenerAdapter) {
            this.listenerAdapter = listenerAdapter;
        }

        @Override
        public void handle(Data key, Data value, Data oldValue, Data mergingValue, int eventType, String uuid,
                           int numberOfAffectedEntries) {
            Member member = getContext().getClusterService().getMember(uuid);
            final IMapEvent iMapEvent = createIMapEvent(key, value, oldValue, mergingValue, eventType, numberOfAffectedEntries,
                    member);
            listenerAdapter.onEvent(iMapEvent);
        }

        private IMapEvent createIMapEvent(Data key, Data value, Data oldValue, Data mergingValue, int eventType,
                                          int numberOfAffectedEntries, Member member) {
            IMapEvent iMapEvent;
            EntryEventType entryEventType = EntryEventType.getByType(eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                case EXPIRED:
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

        private EntryEvent<K, V> createEntryEvent(Data keyData, Data valueData, Data oldValueData, Data mergingValueData,
                                                  int eventType, Member member) {
            return new DataAwareEntryEvent(member, eventType, name, keyData, valueData, oldValueData, mergingValueData,
                    getContext().getSerializationService());
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
}
