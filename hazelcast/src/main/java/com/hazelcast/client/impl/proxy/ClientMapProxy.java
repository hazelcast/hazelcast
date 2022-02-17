/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientLockReferenceIdGenerator;
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
import com.hazelcast.client.impl.protocol.codec.MapEventJournalReadCodec;
import com.hazelcast.client.impl.protocol.codec.MapEventJournalSubscribeCodec;
import com.hazelcast.client.impl.protocol.codec.MapEventJournalSubscribeCodec.ResponseParameters;
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
import com.hazelcast.client.impl.protocol.codec.MapPutIfAbsentWithMaxIdleCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutTransientCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutTransientWithMaxIdleCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutWithMaxIdleCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveInterceptorCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceIfSameCodec;
import com.hazelcast.client.impl.protocol.codec.MapSetCodec;
import com.hazelcast.client.impl.protocol.codec.MapSetTtlCodec;
import com.hazelcast.client.impl.protocol.codec.MapSetWithMaxIdleCodec;
import com.hazelcast.client.impl.protocol.codec.MapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.MapSubmitToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryPutCodec;
import com.hazelcast.client.impl.protocol.codec.MapTryRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesCodec;
import com.hazelcast.client.impl.protocol.codec.MapReplaceAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesWithPagingPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.MapValuesWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.holder.PagingPredicateHolder;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.map.impl.iterator.ClientMapIterable;
import com.hazelcast.client.map.impl.iterator.ClientMapIterator;
import com.hazelcast.client.map.impl.iterator.ClientMapPartitionIterable;
import com.hazelcast.client.map.impl.iterator.ClientMapPartitionIterator;
import com.hazelcast.client.map.impl.iterator.ClientMapQueryIterable;
import com.hazelcast.client.map.impl.iterator.ClientMapQueryPartitionIterable;
import com.hazelcast.client.map.impl.iterator.ClientMapQueryPartitionIterator;
import com.hazelcast.client.map.impl.querycache.ClientQueryCacheContext;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.IMapEvent;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.spi.impl.UnmodifiableLazySet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.internal.util.TimeUtil.timeInMsOrOneIfResultIsZero;
import static com.hazelcast.internal.util.TimeUtil.timeInMsOrTimeIfNullUnit;
import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.map.impl.MapListenerFlagOperator.setAndGetListenerFlags;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest.newQueryCacheRequest;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static com.hazelcast.query.impl.predicates.PredicateUtils.unwrapPagingPredicate;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Proxy implementation of {@link IMap}.
 *
 * @param <K> key
 * @param <V> value
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ClientMapProxy<K, V> extends ClientProxy
        implements IMap<K, V>, EventJournalReader<EventJournalMapEvent<K, V>> {

    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Predicate should not be null!";
    protected static final String NULL_AGGREGATOR_IS_NOT_ALLOWED = "Aggregator should not be null!";
    protected static final String NULL_PROJECTION_IS_NOT_ALLOWED = "Projection should not be null!";
    protected static final String NULL_ENTRY_PROCESSOR_IS_NOT_ALLOWED = "Null entry processor is not allowed!";
    protected static final String NULL_TTL_UNIT_IS_NOT_ALLOWED = "Null ttlUnit is not allowed!";
    protected static final String NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED = "Null maxIdleUnit is not allowed!";
    protected static final String NULL_TIMEUNIT_IS_NOT_ALLOWED = "Null timeunit is not allowed!";
    protected static final String NULL_BIFUNCTION_IS_NOT_ALLOWED = "Null BiFunction is not allowed!";
    protected static final String NULL_FUNCTION_IS_NOT_ALLOWED = "Null Function is not allowed!";

    private ClientLockReferenceIdGenerator lockReferenceIdGenerator;
    private ClientQueryCacheContext queryCacheContext;
    private boolean useDefaultReplaceAllOperation;

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
    public boolean containsKey(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return containsKeyInternal(key);
    }

    protected boolean containsKeyInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage message = MapContainsKeyCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage result = invoke(message, keyData);
        return MapContainsKeyCodec.decodeResponse(result);
    }

    @Override
    public boolean containsValue(@Nonnull Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        ClientMessage request = MapContainsValueCodec.encodeRequest(name, valueData);
        ClientMessage response = invoke(request);
        return MapContainsValueCodec.decodeResponse(response);
    }

    @Override
    public V get(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return toObject(getInternal(key));
    }

    protected Object getInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapGetCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MapGetCodec.decodeResponse(response);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return putInternal(UNSET, MILLISECONDS, null, null, key, value);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value,
                 long ttl, @Nonnull TimeUnit ttlUnit,
                 long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        return putInternal(ttl, ttlUnit, maxIdle, maxIdleUnit, key, value);
    }

    @Override
    public V remove(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return toObject(removeInternal(key));
    }

    protected Data removeInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapRemoveCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MapRemoveCodec.decodeResponse(response);
    }

    @Override
    public boolean remove(@Nonnull Object key, @Nonnull Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return removeInternal(key, value);
    }

    protected boolean removeInternal(Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MapRemoveIfSameCodec.encodeRequest(name, keyData, valueData, getThreadId());

        ClientMessage response = invoke(request, keyData);
        return MapRemoveIfSameCodec.decodeResponse(response);
    }

    @Override
    public void removeAll(@Nonnull Predicate<K, V> predicate) {
        checkNotNull(predicate, "predicate cannot be null");

        removeAllInternal(predicate);
    }

    protected void removeAllInternal(Predicate predicate) {
        ClientMessage request = MapRemoveAllCodec.encodeRequest(name, toData(predicate));
        invokeWithPredicate(request, predicate);
    }

    @Override
    public void delete(@Nonnull Object key) {
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
    public InternalCompletableFuture<V> getAsync(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return new ClientDelegatingFuture<>(getAsyncInternal(key),
                getSerializationService(), MapGetCodec::decodeResponse);
    }

    protected ClientInvocationFuture getAsyncInternal(Object key) {
        try {
            Data keyData = toData(key);
            ClientMessage request = MapGetCodec.encodeRequest(name, keyData, getThreadId());
            return invokeOnKeyOwner(request, keyData);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private ClientInvocationFuture invokeOnKeyOwner(ClientMessage request, Data keyData) {
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);
        ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, getName(), partitionId);
        return clientInvocation.invoke();
    }

    @Override
    public InternalCompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return putAsyncInternal(UNSET, MILLISECONDS, null, null, key, value);
    }

    @Override
    public InternalCompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        return putAsyncInternal(ttl, timeunit, null, null, key, value);
    }

    @Override
    public InternalCompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value,
                                                 long ttl, @Nonnull TimeUnit ttlUnit,
                                                 long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        return putAsyncInternal(ttl, ttlUnit, maxIdle, maxIdleUnit, key, value);
    }

    protected InternalCompletableFuture<V> putAsyncInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                                            Object key, Object value) {
        try {
            Data keyData = toData(key);
            Data valueData = toData(value);
            long ttlMillis = timeInMsOrOneIfResultIsZero(ttl, timeunit);
            ClientMessage request;
            if (maxIdle != null) {
                request = MapPutWithMaxIdleCodec.encodeRequest(name, keyData, valueData, getThreadId(),
                        ttlMillis, timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
            } else {
                request = MapPutCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
            }
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            SerializationService ss = getSerializationService();
            return new ClientDelegatingFuture<>(future, ss, MapPutCodec::decodeResponse);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(@Nonnull K key, @Nonnull V value) {
        return setAsync(key, value, UNSET, MILLISECONDS);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        return setAsyncInternal(ttl, timeunit, null, null, key, value);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(@Nonnull K key, @Nonnull V value,
                                                    long ttl, @Nonnull TimeUnit ttlUnit,
                                                    long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        return setAsyncInternal(ttl, ttlUnit, maxIdle, maxIdleUnit, key, value);
    }

    protected InternalCompletableFuture<Void> setAsyncInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                                               Object key, Object value) {
        try {
            Data keyData = toData(key);
            Data valueData = toData(value);
            long ttlMillis = timeInMsOrOneIfResultIsZero(ttl, timeunit);
            ClientMessage request;
            if (maxIdle != null) {
                request = MapSetWithMaxIdleCodec.encodeRequest(name, keyData, valueData, getThreadId(),
                        ttlMillis, timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
            } else {
                request = MapSetCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
            }

            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            return new ClientDelegatingFuture<>(future, getSerializationService(), clientMessage -> null);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public InternalCompletableFuture<V> removeAsync(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        return removeAsyncInternal(key);
    }

    protected InternalCompletableFuture<V> removeAsyncInternal(Object key) {
        try {
            Data keyData = toData(key);
            ClientMessage request = MapRemoveCodec.encodeRequest(name, keyData, getThreadId());
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            SerializationService ss = getSerializationService();
            return new ClientDelegatingFuture<>(future, ss, MapRemoveCodec::decodeResponse);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean tryRemove(@Nonnull K key, long timeout, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);
        return tryRemoveInternal(timeout, timeunit, key);
    }

    protected boolean tryRemoveInternal(long timeout, TimeUnit timeunit, Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapTryRemoveCodec.encodeRequest(name, keyData, getThreadId(), timeunit.toMillis(timeout));
        ClientMessage response = invoke(request, keyData);
        return MapTryRemoveCodec.decodeResponse(response);
    }

    @Override
    public boolean tryPut(@Nonnull K key, @Nonnull V value, long timeout, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        return tryPutInternal(timeout, timeunit, key, value);
    }

    protected boolean tryPutInternal(long timeout, TimeUnit timeunit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long timeoutMillis = timeInMsOrOneIfResultIsZero(timeout, timeunit);
        ClientMessage request = MapTryPutCodec.encodeRequest(name, keyData, valueData, getThreadId(), timeoutMillis);
        ClientMessage response = invoke(request, keyData);
        return MapTryPutCodec.decodeResponse(response);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        return putInternal(ttl, timeunit, null, null, key, value);
    }

    protected V putInternal(long ttl, TimeUnit ttlUnit, Long maxIdle, TimeUnit maxIdleUnit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long ttlMillis = timeInMsOrOneIfResultIsZero(ttl, ttlUnit);
        ClientMessage request;
        if (maxIdle != null) {
            request = MapPutWithMaxIdleCodec.encodeRequest(name, keyData, valueData,
                    getThreadId(), ttlMillis, timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
        } else {
            request = MapPutCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
        }
        ClientMessage response = invoke(request, keyData);
        return toObject(MapPutCodec.decodeResponse(response));
    }

    @Override
    public void putTransient(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        putTransientInternal(ttl, timeunit, null, null, key, value);
    }

    @Override
    public void putTransient(@Nonnull K key, @Nonnull V value,
                             long ttl, @Nonnull TimeUnit ttlUnit,
                             long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        putTransientInternal(ttl, ttlUnit, maxIdle, maxIdleUnit, key, value);
    }

    protected void putTransientInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                        Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long ttlMillis = timeInMsOrOneIfResultIsZero(ttl, timeunit);
        ClientMessage request;
        if (maxIdle != null) {
            request = MapPutTransientWithMaxIdleCodec.encodeRequest(name, keyData, valueData,
                    getThreadId(), ttlMillis, timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
        } else {
            request = MapPutTransientCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
        }

        invoke(request, keyData);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return putIfAbsentInternal(UNSET, MILLISECONDS, null, null, key, value);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        return putIfAbsentInternal(ttl, timeunit, null, null, key, value);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value,
                         long ttl, @Nonnull TimeUnit ttlUnit,
                         long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        return putIfAbsentInternal(ttl, ttlUnit, maxIdle, maxIdleUnit, key, value);
    }

    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long ttlMillis = timeInMsOrOneIfResultIsZero(ttl, timeunit);
        ClientMessage request;
        if (maxIdle != null) {
            request = MapPutIfAbsentWithMaxIdleCodec.encodeRequest(name, keyData, valueData,
                    getThreadId(), ttlMillis, timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
        } else {
            request = MapPutIfAbsentCodec.encodeRequest(name, keyData, valueData,
                    getThreadId(), ttlMillis);
        }

        ClientMessage result = invoke(request, keyData);
        return toObject(MapPutIfAbsentCodec.decodeResponse(result));
    }

    @Override
    public boolean replace(@Nonnull K key, @Nonnull V oldValue, @Nonnull V newValue) {
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
        return MapReplaceIfSameCodec.decodeResponse(response);
    }

    @Override
    public V replace(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        return replaceInternal(key, value);
    }

    protected V replaceInternal(Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MapReplaceCodec.encodeRequest(name, keyData, valueData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        return toObject(MapReplaceCodec.decodeResponse(response));
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit ttlUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);

        setInternal(ttl, ttlUnit, null, null, key, value);
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value,
                    long ttl, @Nonnull TimeUnit ttlUnit,
                    long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        setInternal(ttl, ttlUnit, maxIdle, maxIdleUnit, key, value);
    }

    protected void setInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit, Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        long ttlMillis = timeInMsOrOneIfResultIsZero(ttl, timeunit);
        ClientMessage request;
        if (maxIdle != null) {
            request = MapSetWithMaxIdleCodec.encodeRequest(name, keyData, valueData, getThreadId(),
                    ttlMillis, timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
        } else {
            request = MapSetCodec.encodeRequest(name, keyData, valueData, getThreadId(), ttlMillis);
        }
        invoke(request, keyData);
    }

    @Override
    public void lock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        lockInternal(key, timeInMsOrTimeIfNullUnit(UNSET, MILLISECONDS));
    }

    @Override
    public void lock(@Nonnull K key, long leaseTime, @Nullable TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkPositive("leaseTime", leaseTime);
        lockInternal(key, timeInMsOrTimeIfNullUnit(leaseTime, timeUnit));
    }

    private void lockInternal(@Nonnull K key, long leaseTimeMs) {
        Data keyData = toData(key);
        ClientMessage request = MapLockCodec.encodeRequest(name, keyData, getThreadId(),
                leaseTimeMs, lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData, Long.MAX_VALUE);
    }

    private <T> T invoke(ClientMessage clientMessage, Object key, long invocationTimeoutSeconds) {
        final int partitionId = getContext().getPartitionService().getPartitionId(key);
        try {
            ClientInvocation clientInvocation = new ClientInvocation(getClient(), clientMessage, getName(), partitionId);
            clientInvocation.setInvocationTimeoutMillis(invocationTimeoutSeconds);
            final Future future = clientInvocation.invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean isLocked(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MapIsLockedCodec.encodeRequest(name, keyData);
        ClientMessage response = invoke(request, keyData);
        return MapIsLockedCodec.decodeResponse(response);
    }

    @Override
    public boolean tryLock(@Nonnull K key) {
        try {
            return tryLock(key, 0, null);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean tryLock(@Nonnull K key, long time, @Nullable TimeUnit timeunit) throws InterruptedException {
        return tryLock(key, time, timeunit, UNSET, null);
    }

    @Override
    public boolean tryLock(@Nonnull K key,
                           long time, @Nullable TimeUnit timeunit,
                           long leaseTime, @Nullable TimeUnit leaseUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        long leaseTimeMillis = timeInMsOrTimeIfNullUnit(leaseTime, leaseUnit);
        long timeoutMillis = timeInMsOrTimeIfNullUnit(time, timeunit);
        ClientMessage request = MapTryLockCodec.encodeRequest(name, keyData, getThreadId(), leaseTimeMillis, timeoutMillis,
                lockReferenceIdGenerator.getNextReferenceId());

        ClientMessage response = invoke(request, keyData, Long.MAX_VALUE);
        return MapTryLockCodec.decodeResponse(response);
    }

    @Override
    public void unlock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MapUnlockCodec.encodeRequest(name, keyData, getThreadId(),
                lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MapForceUnlockCodec.encodeRequest(name, keyData, lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public UUID addLocalEntryListener(@Nonnull MapListener listener) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public UUID addLocalEntryListener(@Nonnull MapListener listener,
                                      @Nonnull Predicate<K, V> predicate,
                                      boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public UUID addLocalEntryListener(@Nonnull MapListener listener,
                                      @Nonnull Predicate<K, V> predicate,
                                      @Nullable K key,
                                      boolean includeValue) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String addInterceptor(@Nonnull MapInterceptor interceptor) {
        checkNotNull(interceptor, "Interceptor should not be null!");
        Data data = toData(interceptor);
        ClientMessage request = MapAddInterceptorCodec.encodeRequest(name, data);
        ClientMessage response = invoke(request);
        return MapAddInterceptorCodec.decodeResponse(response);
    }

    @Override
    public boolean removeInterceptor(@Nonnull String id) {
        checkNotNull(id, "Interceptor ID should not be null!");

        ClientMessage request = MapRemoveInterceptorCodec.encodeRequest(name, id);
        ClientMessage response = invoke(request);
        return MapRemoveInterceptorCodec.decodeResponse(response);
    }

    @Override
    public UUID addEntryListener(@Nonnull MapListener listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, includeValue);
    }

    private UUID addEntryListenerInternal(ListenerAdapter<IMapEvent> listenerAdaptor, boolean includeValue) {
        int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        EventHandler<ClientMessage> handler = new ClientMapEventHandler(listenerAdaptor);
        return registerListener(createMapEntryListenerCodec(includeValue, listenerFlags), handler);
    }

    private ListenerMessageCodec createMapEntryListenerCodec(final boolean includeValue, final int listenerFlags) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddEntryListenerCodec.encodeRequest(name, includeValue, listenerFlags, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MapAddEntryListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    public boolean removeEntryListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "Listener ID should not be null!");
        return deregisterListener(registrationId);
    }

    @Override
    public UUID addPartitionLostListener(@Nonnull MapPartitionLostListener listener) {
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
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MapAddPartitionLostListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MapRemovePartitionLostListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemovePartitionLostListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    public boolean removePartitionLostListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "Listener ID should not be null!");
        return deregisterListener(registrationId);
    }

    @Override
    public UUID addEntryListener(@Nonnull MapListener listener, @Nonnull K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, key, includeValue);
    }

    private UUID addEntryListenerInternal(ListenerAdapter<IMapEvent> listenerAdaptor, K key, boolean includeValue) {
        int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        Data keyData = toData(key);
        EventHandler<ClientMessage> handler = new ClientMapToKeyEventHandler(listenerAdaptor);
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
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MapAddEntryListenerToKeyCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    public UUID addEntryListener(@Nonnull MapListener listener,
                                 @Nonnull Predicate<K, V> predicate,
                                 @Nullable K key,
                                 boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return key == null
                ? addEntryListenerInternal(listenerAdaptor, predicate, includeValue)
                : addEntryListenerInternal(listenerAdaptor, predicate, key, includeValue);
    }

    private UUID addEntryListenerInternal(@Nonnull ListenerAdapter<IMapEvent> listenerAdaptor,
                                          @Nonnull Predicate<K, V> predicate,
                                          @Nullable K key,
                                          boolean includeValue) {
        int listenerFlags = setAndGetListenerFlags(listenerAdaptor);
        Data keyData = toData(key);
        Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = new ClientMapToKeyWithPredicateEventHandler(listenerAdaptor);
        ListenerMessageCodec codec = createEntryListenerToKeyWithPredicateCodec(
                includeValue, listenerFlags, keyData, predicateData);
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
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    public UUID addEntryListener(@Nonnull MapListener listener,
                                 @Nonnull Predicate<K, V> predicate,
                                 boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        ListenerAdapter<IMapEvent> listenerAdaptor = createListenerAdapter(listener);
        return addEntryListenerInternal(listenerAdaptor, predicate, includeValue);
    }

    private UUID addEntryListenerInternal(ListenerAdapter<IMapEvent> listenerAdapter,
                                          Predicate<K, V> predicate,
                                          boolean includeValue) {
        int listenerFlags = setAndGetListenerFlags(listenerAdapter);
        Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = new ClientMapWithPredicateEventHandler(listenerAdapter);
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
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MapAddEntryListenerWithPredicateCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public EntryView<K, V> getEntryView(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MapGetEntryViewCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);

        MapGetEntryViewCodec.ResponseParameters parameters = MapGetEntryViewCodec.decodeResponse(response);
        SimpleEntryView<Data, Data> dataEntryView = parameters.response;

        if (dataEntryView == null) {
            return null;
        }
        // TODO: putCache
        return new SimpleEntryView<K, V>()
                .withKey(toObject(dataEntryView.getKey()))
                .withValue(toObject(dataEntryView.getValue()))
                .withCost(dataEntryView.getCost())
                .withCreationTime(dataEntryView.getCreationTime())
                .withExpirationTime(dataEntryView.getExpirationTime())
                .withHits(dataEntryView.getHits())
                .withLastAccessTime(dataEntryView.getLastAccessTime())
                .withLastStoredTime(dataEntryView.getLastStoredTime())
                .withLastUpdateTime(dataEntryView.getLastUpdateTime())
                .withVersion(dataEntryView.getVersion())
                .withHits(dataEntryView.getHits())
                .withTtl(dataEntryView.getTtl())
                .withMaxIdle(parameters.maxIdle);
    }

    @Override
    public boolean evict(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return evictInternal(key);
    }

    protected boolean evictInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = MapEvictCodec.encodeRequest(name, keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MapEvictCodec.decodeResponse(response);
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
    public void loadAll(@Nonnull Set<K> keys, boolean replaceExistingValues) {
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

    @Nonnull
    @Override
    public Set<K> keySet() {
        ClientMessage request = MapKeySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);

        return (Set<K>) new UnmodifiableLazySet(MapKeySetCodec.decodeResponse(response), getSerializationService());
    }

    @Override
    public Map<K, V> getAll(@Nullable Set<K> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            // Wrap emptyMap() into unmodifiableMap to make sure put/putAll methods throw UnsupportedOperationException
            return Collections.unmodifiableMap(emptyMap());
        }

        int keysSize = keys.size();
        Map<Integer, List<Data>> partitionToKeyData = new HashMap<>();
        List<Object> resultingKeyValuePairs = new ArrayList<>(keysSize * 2);
        getAllInternal(keys, partitionToKeyData, resultingKeyValuePairs);

        Map<K, V> result = createHashMap(keysSize);
        for (int i = 0; i < resultingKeyValuePairs.size(); ) {
            K key = toObject(resultingKeyValuePairs.get(i++));
            V value = toObject(resultingKeyValuePairs.get(i++));
            result.put(key, value);
        }
        return Collections.unmodifiableMap(result);
    }

    protected void getAllInternal(Set<K> keys, Map<Integer, List<Data>> partitionToKeyData, List<Object> resultingKeyValuePairs) {
        if (partitionToKeyData.isEmpty()) {
            fillPartitionToKeyData(keys, partitionToKeyData, null, null);
        }
        List<Future<ClientMessage>> futures = new ArrayList<>(partitionToKeyData.size());
        for (Map.Entry<Integer, List<Data>> entry : partitionToKeyData.entrySet()) {
            int partitionId = entry.getKey();
            List<Data> keyList = entry.getValue();
            if (!keyList.isEmpty()) {
                ClientMessage request = MapGetAllCodec.encodeRequest(name, keyList);
                futures.add(new ClientInvocation(getClient(), request, getName(), partitionId).invoke());
            }
        }

        for (Future<ClientMessage> future : futures) {
            try {
                ClientMessage response = future.get();
                List<Entry<Data, Data>> entries = MapGetAllCodec.decodeResponse(response);
                for (Entry<Data, Data> entry : entries) {
                    resultingKeyValuePairs.add(entry.getKey());
                    resultingKeyValuePairs.add(entry.getValue());
                }
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }

    protected void fillPartitionToKeyData(Set<K> keys,
                                          Map<Integer, List<Data>> partitionToKeyData,
                                          Map<Object, Data> keyMap,
                                          Map<Data, Object> reverseKeyMap) {
        ClientPartitionService partitionService = getContext().getPartitionService();
        for (K key : keys) {
            Data keyData = toData(key);
            int partitionId = partitionService.getPartitionId(keyData);
            List<Data> keyList = partitionToKeyData.get(partitionId);
            if (keyList == null) {
                keyList = new ArrayList<>();
                partitionToKeyData.put(partitionId, keyList);
            }
            keyList.add(keyData);
            if (keyMap != null) {
                keyMap.put(key, keyData);
            }
            if (reverseKeyMap != null) {
                reverseKeyMap.put(keyData, key);
            }
        }
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        ClientMessage request = MapValuesCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return new UnmodifiableLazyList(MapValuesCodec.decodeResponse(response), getSerializationService());
    }

    @Nonnull
    @Override
    public Set<Entry<K, V>> entrySet() {
        ClientMessage request = MapEntrySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return getEntriesAsImmutableLazySet(MapEntrySetCodec.decodeResponse(response));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<K> keySet(@Nonnull Predicate<K, V> predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        if (containsPagingPredicate(predicate)) {
            return keySetWithPagingPredicate(predicate);
        }

        ClientMessage request = MapKeySetWithPredicateCodec.encodeRequest(name, toData(predicate));
        ClientMessage response = invokeWithPredicate(request, predicate);

        return (Set<K>) new UnmodifiableLazySet(MapKeySetWithPredicateCodec.decodeResponse(response), getSerializationService());
    }

    @SuppressWarnings("unchecked")
    private Set keySetWithPagingPredicate(Predicate predicate) {
        PagingPredicateImpl pagingPredicate = unwrapPagingPredicate(predicate);
        pagingPredicate.setIterationType(IterationType.KEY);

        PagingPredicateHolder pagingPredicateHolder = PagingPredicateHolder.of(predicate, getSerializationService());
        ClientMessage request = MapKeySetWithPagingPredicateCodec.encodeRequest(name, pagingPredicateHolder);
        ClientMessage response = invokeWithPredicate(request, predicate);
        MapKeySetWithPagingPredicateCodec.ResponseParameters resultParameters = MapKeySetWithPagingPredicateCodec
                .decodeResponse(response);

        SerializationService serializationService = getSerializationService();

        pagingPredicate.setAnchorList(resultParameters.anchorDataList.asAnchorList(serializationService));

        return new UnmodifiableLazySet(resultParameters.response, serializationService);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Entry<K, V>> entrySet(@Nonnull Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        if (containsPagingPredicate(predicate)) {
            return entrySetWithPagingPredicate(predicate);
        }
        ClientMessage request = MapEntriesWithPredicateCodec.encodeRequest(name, toData(predicate));

        ClientMessage response = invokeWithPredicate(request, predicate);

        return getEntriesAsImmutableLazySet(MapEntriesWithPredicateCodec.decodeResponse(response));
    }

    private Set getEntriesAsImmutableLazySet(List<Entry<Data, Data>> entryDataList) {
        return new UnmodifiableLazySet(entryDataList, getSerializationService());
    }

    private Set entrySetWithPagingPredicate(Predicate predicate) {
        PagingPredicateImpl pagingPredicate = unwrapPagingPredicate(predicate);
        pagingPredicate.setIterationType(IterationType.ENTRY);

        PagingPredicateHolder pagingPredicateHolder = PagingPredicateHolder.of(predicate, getSerializationService());
        ClientMessage request = MapEntriesWithPagingPredicateCodec.encodeRequest(name, pagingPredicateHolder);

        ClientMessage response = invokeWithPredicate(request, predicate);
        MapEntriesWithPagingPredicateCodec.ResponseParameters resultParameters = MapEntriesWithPagingPredicateCodec
                .decodeResponse(response);

        pagingPredicate.setAnchorList(resultParameters.anchorDataList.asAnchorList(getSerializationService()));

        return getEntriesAsImmutableLazySet(resultParameters.response);
    }

    @Override
    public Collection<V> values(@Nonnull Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        if (containsPagingPredicate(predicate)) {
            return valuesForPagingPredicate(predicate);
        }

        ClientMessage request = MapValuesWithPredicateCodec.encodeRequest(name, toData(predicate));
        ClientMessage response = invokeWithPredicate(request, predicate);
        List<Data> dataList = MapValuesWithPredicateCodec.decodeResponse(response);
        return (Collection<V>) new UnmodifiableLazyList(dataList, getSerializationService());
    }

    private ClientMessage invokeWithPredicate(ClientMessage request, Predicate predicate) {
        ClientMessage response;
        if (predicate instanceof PartitionPredicate) {
            PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
            response = invoke(request, partitionPredicate.getPartitionKey());
        } else {
            response = invoke(request);
        }
        return response;
    }

    @SuppressWarnings("unchecked")
    private Collection<V> valuesForPagingPredicate(Predicate predicate) {
        PagingPredicateImpl pagingPredicate = unwrapPagingPredicate(predicate);
        pagingPredicate.setIterationType(IterationType.VALUE);

        PagingPredicateHolder pagingPredicateHolder = PagingPredicateHolder.of(predicate, getSerializationService());
        ClientMessage request = MapValuesWithPagingPredicateCodec.encodeRequest(name, pagingPredicateHolder);

        ClientMessage response = invokeWithPredicate(request, predicate);
        MapValuesWithPagingPredicateCodec.ResponseParameters resultParameters = MapValuesWithPagingPredicateCodec
                .decodeResponse(response);

        SerializationService serializationService = getSerializationService();

        pagingPredicate.setAnchorList(resultParameters.anchorDataList.asAnchorList(serializationService));

        return (Collection<V>) new UnmodifiableLazyList(resultParameters.response, serializationService);
    }

    @Override
    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public Set<K> localKeySet(@Nonnull Predicate predicate) {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public void addIndex(IndexConfig indexConfig) {
        checkNotNull(indexConfig, "Index config cannot be null.");

        IndexConfig indexConfig0 = IndexUtils.validateAndNormalize(name, indexConfig);

        ClientMessage request = MapAddIndexCodec.encodeRequest(name, indexConfig0);
        invoke(request);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return new LocalMapStatsImpl();
    }

    @Override
    public boolean setTtl(@Nonnull K key, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);
        return setTtlInternal(key, ttl, timeunit);
    }

    protected boolean setTtlInternal(Object key, long ttl, TimeUnit timeUnit) {
        long ttlMillis = timeUnit.toMillis(ttl);
        Data keyData = toData(key);
        ClientMessage request = MapSetTtlCodec.encodeRequest(getName(), keyData, ttlMillis);
        ClientMessage result = invoke(request, keyData);
        return MapSetTtlCodec.decodeResponse(result);
    }

    @Override
    public <R> R executeOnKey(@Nonnull K key,
                              @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(entryProcessor, NULL_ENTRY_PROCESSOR_IS_NOT_ALLOWED);
        return executeOnKeyInternal(key, entryProcessor);
    }

    public <R> R executeOnKeyInternal(Object key,
                                      EntryProcessor<K, V, R> entryProcessor) {
        validateEntryProcessorForSingleKeyProcessing(entryProcessor);
        Data keyData = toData(key);
        ClientMessage request = MapExecuteOnKeyCodec.encodeRequest(name, toData(entryProcessor), keyData, getThreadId());
        ClientMessage response = invoke(request, keyData);
        return toObject(MapExecuteOnKeyCodec.decodeResponse(response));
    }

    @Override
    public <R> InternalCompletableFuture<R> submitToKey(@Nonnull K key,
                                                        @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        return submitToKeyInternal(key, entryProcessor);
    }

    public <R> InternalCompletableFuture<R> submitToKeyInternal(Object key,
                                                                EntryProcessor<K, V, R> entryProcessor) {
        try {
            Data keyData = toData(key);
            ClientMessage request = MapSubmitToKeyCodec.encodeRequest(name, toData(entryProcessor), keyData, getThreadId());
            ClientInvocationFuture future = invokeOnKeyOwner(request, keyData);
            SerializationService ss = getSerializationService();
            return new ClientDelegatingFuture(future, ss, MapSubmitToKeyCodec::decodeResponse);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public <R> Map<K, R> executeOnEntries(@Nonnull EntryProcessor<K, V, R> entryProcessor) {
        ClientMessage request = MapExecuteOnAllKeysCodec.encodeRequest(name, toData(entryProcessor));
        ClientMessage response = invoke(request);
        return prepareResult(MapExecuteOnAllKeysCodec.decodeResponse(response));
    }

    protected <R> Map<K, R> prepareResult(Collection<Entry<Data, Data>> entries) {
        if (CollectionUtil.isEmpty(entries)) {
            return emptyMap();
        }

        Map<K, R> result = createHashMap(entries.size());
        for (Entry<Data, Data> entry : entries) {
            K key = toObject(entry.getKey());
            result.put(key, toObject(entry.getValue()));
        }
        return result;
    }

    @Override
    public <R> Map<K, R> executeOnEntries(@Nonnull EntryProcessor<K, V, R> entryProcessor,
                                          @Nonnull Predicate<K, V> predicate) {
        ClientMessage request = MapExecuteWithPredicateCodec.encodeRequest(name, toData(entryProcessor), toData(predicate));
        ClientMessage response = invokeWithPredicate(request, predicate);

        return prepareResult(MapExecuteWithPredicateCodec.decodeResponse(response));
    }

    @Override
    public <R> R aggregate(@Nonnull Aggregator<? super Map.Entry<K, V>, R> aggregator) {
        checkNotNull(aggregator, NULL_AGGREGATOR_IS_NOT_ALLOWED);

        ClientMessage request = MapAggregateCodec.encodeRequest(name, toData(aggregator));
        ClientMessage response = invoke(request);

        return toObject(MapAggregateCodec.decodeResponse(response));
    }

    @Override
    public <R> R aggregate(@Nonnull Aggregator<? super Map.Entry<K, V>, R> aggregator,
                           @Nonnull Predicate<K, V> predicate) {
        checkNotNull(aggregator, NULL_AGGREGATOR_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        checkNotPagingPredicate(predicate, "aggregate");

        ClientMessage request = MapAggregateWithPredicateCodec.encodeRequest(name, toData(aggregator), toData(predicate));
        ClientMessage response = invokeWithPredicate(request, predicate);

        return toObject(MapAggregateWithPredicateCodec.decodeResponse(response));
    }

    @Override
    public <R> Collection<R> project(@Nonnull Projection<? super Entry<K, V>, R> projection) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        ClientMessage request = MapProjectCodec.encodeRequest(name, toData(projection));
        ClientMessage response = invoke(request);

        return new UnmodifiableLazyList(MapProjectCodec.decodeResponse(response), getSerializationService());
    }

    @Override
    public <R> Collection<R> project(@Nonnull Projection<? super Entry<K, V>, R> projection,
                                     @Nonnull Predicate<K, V> predicate) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        checkNotPagingPredicate(predicate, "project");

        ClientMessage request = MapProjectWithPredicateCodec.encodeRequest(name, toData(projection), toData(predicate));
        ClientMessage response = invokeWithPredicate(request, predicate);

        return new UnmodifiableLazyList(MapProjectWithPredicateCodec.decodeResponse(response), getSerializationService());
    }

    @Override
    public QueryCache<K, V> getQueryCache(@Nonnull String name) {
        checkNotNull(name, "name cannot be null");

        return getQueryCacheInternal(name, null, null, null, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(@Nonnull String name,
                                          @Nonnull Predicate<K, V> predicate,
                                          boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        return getQueryCacheInternal(name, null, predicate, includeValue, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(@Nonnull String name,
                                          @Nonnull MapListener listener,
                                          @Nonnull Predicate<K, V> predicate,
                                          boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(listener, "listener cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        return getQueryCacheInternal(name, listener, predicate, includeValue, this);
    }

    private QueryCache<K, V> getQueryCacheInternal(String name, MapListener listener, Predicate predicate, Boolean includeValue,
                                                   IMap map) {
        QueryCacheRequest request = newQueryCacheRequest()
                .withCacheName(name)
                .withListener(listener)
                .withPredicate(predicate)
                .withIncludeValue(includeValue)
                .forMap(map)
                .withContext(queryCacheContext);

        return createQueryCache(request);
    }

    @SuppressWarnings("unchecked")
    private QueryCache<K, V> createQueryCache(QueryCacheRequest request) {
        SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
        QueryCacheEndToEndProvider queryCacheEndToEndProvider = subscriberContext.getEndToEndQueryCacheProvider();
        return queryCacheEndToEndProvider.getOrCreateQueryCache(request.getMapName(), request.getCacheName(),
                subscriberContext.newEndToEndConstructor(request));
    }

    @Override
    public <R> Map<K, R> executeOnKeys(@Nonnull Set<K> keys,
                                       @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        try {
            return submitToKeys(keys, entryProcessor).get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public <R> InternalCompletableFuture<Map<K, R>> submitToKeys(@Nonnull Set<K> keys,
                                                                 @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        checkNotNull(keys, NULL_KEY_IS_NOT_ALLOWED);
        if (keys.isEmpty()) {
            return InternalCompletableFuture.newCompletedFuture(Collections.emptyMap());
        }
        Collection<Data> dataKeys = objectToDataCollection(keys, getSerializationService());
        return submitToKeysInternal(keys, dataKeys, entryProcessor);
    }

    /**
     * @param objectKeys not serialized key
     * @param dataKeys   serialized keys
     */
    @Nonnull
    protected <R> InternalCompletableFuture<Map<K, R>> submitToKeysInternal(@Nonnull Set<K> objectKeys,
                                                                            @Nonnull Collection<Data> dataKeys,
                                                                            @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        ClientMessage request = MapExecuteOnKeysCodec.encodeRequest(name, toData(entryProcessor), dataKeys);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, getName()).invoke();
        return new ClientDelegatingFuture<>(
                future, getSerializationService(),
                message -> prepareResult(MapExecuteOnKeysCodec.decodeResponse(message)));
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value) {
        set(key, value, UNSET, MILLISECONDS);
    }

    @Override
    public int size() {
        ClientMessage request = MapSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return MapSizeCodec.decodeResponse(response);
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = MapIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return MapIsEmptyCodec.decodeResponse(response);
    }

    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends V> m) {
        putAllInternal(m, null, true);
    }

    @Override
    public InternalCompletableFuture<Void> putAllAsync(@Nonnull Map<? extends K, ? extends V> m) {
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        putAllInternal(m, future, true);
        return future;
    }

    @Override
    public void setAll(@Nonnull Map<? extends K, ? extends V> m) {
        putAllInternal(m, null, false);
    }

    @Override
    public InternalCompletableFuture<Void> setAllAsync(@Nonnull Map<? extends K, ? extends V> m) {
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        putAllInternal(m, future, false);
        return future;
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void putAllInternal(@Nonnull Map<? extends K, ? extends V> map,
                                @Nullable InternalCompletableFuture<Void> future,
                                boolean triggerMapLoader) {
        if (map.isEmpty()) {
            if (future != null) {
                future.complete(null);
            }
            return;
        }
        checkNotNull(map, "Null argument map is not allowed");
        ClientPartitionService partitionService = getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Map<Integer, List<Map.Entry<Data, Data>>> entryMap = new HashMap<>(partitionCount);

        for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
            checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
            checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

            Data keyData = toData(entry.getKey());
            int partitionId = partitionService.getPartitionId(keyData);
            List<Map.Entry<Data, Data>> partition = entryMap.get(partitionId);
            if (partition == null) {
                partition = new ArrayList<>();
                entryMap.put(partitionId, partition);
            }
            partition.add(new AbstractMap.SimpleEntry<>(keyData, toData(entry.getValue())));
        }
        assert entryMap.size() > 0;
        AtomicInteger counter = new AtomicInteger(entryMap.size());
        InternalCompletableFuture<Void> resultFuture =
                future != null ? future : new InternalCompletableFuture<>();
        BiConsumer<ClientMessage, Throwable> callback = (response, t) -> {
            if (t != null) {
                resultFuture.completeExceptionally(t);
            }
            if (counter.decrementAndGet() == 0) {
                finalizePutAll(map, entryMap);
                if (!resultFuture.isDone()) {
                    resultFuture.complete(null);
                }
            }
        };
        for (Entry<Integer, List<Map.Entry<Data, Data>>> entry : entryMap.entrySet()) {
            Integer partitionId = entry.getKey();
            // if there is only one entry, consider how we can use MapPutRequest
            // without having to get back the return value
            ClientMessage request = MapPutAllCodec.encodeRequest(name, entry.getValue(), triggerMapLoader);
            new ClientInvocation(getClient(), request, getName(), partitionId)
                    .invoke()
                    .whenCompleteAsync(callback);
        }
        // if executing in sync mode, block for the responses
        if (future == null) {
            try {
                resultFuture.get();
            } catch (Throwable e) {
                throw rethrow(e);
            }
        }
    }

    protected void finalizePutAll(Map<? extends K, ? extends V> map, Map<Integer, List<Entry<Data, Data>>> entryMap) {
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

    @Override
    @Nonnull
    public Iterator<Entry<K, V>> iterator() {
        ClientPartitionService partitionService = getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        return new ClientMapIterator<>(this, partitionCount, false);
    }

    @Override
    @Nonnull
    public Iterator<Entry<K, V>> iterator(int fetchSize) {
        ClientPartitionService partitionService = getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        return new ClientMapIterator<>(this, fetchSize, partitionCount, false);
    }

    /**
     * Returns an iterator for iterating entries in the {@code partitionId}. If {@code prefetchValues} is
     * {@code true}, all values will be sent along with the keys and no additional data will be fetched when
     * iterating. If {@code false}, the values will be fetched when iterating the entries.
     * <p>
     * The values are not fetched one-by-one but rather in batches.
     * You may control the size of the batch by changing the {@code fetchSize} parameter.
     * A too small {@code fetchSize} can affect performance since more data will have to be sent to and from the partition owner.
     * A too high {@code fetchSize} means that more data will be sent which can block other operations from being sent,
     * including internal operations.
     * The underlying implementation may send more values in one batch than {@code fetchSize} if it needs to get to
     * a "safepoint" to later resume iteration.
     * <b>NOTE</b>
     * The iteration may be done when the map is being mutated or when there are
     * membership changes. The iterator does not reflect the state when it has
     * been constructed - it may return some entries that were added after the
     * iteration has started and may not return some entries that were removed
     * after iteration has started.
     * The iterator will not, however, skip an entry if it has not been changed
     * and will not return an entry twice.
     *
     * @param fetchSize   the size of the batches which will be sent when iterating the data
     * @param partitionId the partition ID which is being iterated
     * @return the iterator for the projected entries
     */
    @Nonnull
    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        return new ClientMapPartitionIterator<>(this, getContext(), fetchSize, partitionId, prefetchValues);
    }

    /**
     * Returns an iterator for iterating the result of the projection on entries in the {@code partitionId} which
     * satisfy the {@code predicate}. The {@link Iterator#remove()} method is not supported and will throw an
     * {@link UnsupportedOperationException}.
     * <p>
     * The values are not fetched one-by-one but rather in batches.
     * You may control the size of the batch by changing the {@code fetchSize} parameter.
     * A too small {@code fetchSize} can affect performance since more data will have to be sent to and from the partition owner.
     * A too high {@code fetchSize} means that more data will be sent which can block other operations from being sent,
     * including internal operations.
     * The underlying implementation may send more values in one batch than {@code fetchSize} if it needs to get to
     * a "safepoint" to later resume iteration.
     * Predicates of type {@link PagingPredicate} are not supported.
     * <b>NOTE</b>
     * The iteration may be done when the map is being mutated or when there are
     * membership changes. The iterator does not reflect the state when it has
     * been constructed - it may return some entries that were added after the
     * iteration has started and may not return some entries that were removed
     * after iteration has started.
     * The iterator will not, however, skip an entry if it has not been changed
     * and will not return an entry twice.
     *
     * @param fetchSize   the size of the batches which will be sent when iterating the data
     * @param partitionId the partition ID which is being iterated
     * @param projection  the projection to apply before returning the value. {@code null} value is not allowed
     * @param predicate   the predicate which the entries must match. {@code null} value is not allowed
     * @param <R>         the return type
     * @return the iterator for the projected entries
     * @throws UnsupportedOperationException if {@link Iterator#remove()} is invoked
     * @throws IllegalArgumentException      if the predicate is of type {@link PagingPredicate}
     * @since 3.9
     */
    @Nonnull
    public <R> Iterator<R> iterator(int fetchSize, int partitionId,
                                    @Nonnull Projection<? super Map.Entry<K, V>, R> projection,
                                    @Nonnull Predicate<K, V> predicate) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        checkNotPagingPredicate(predicate, "iterator");
        return new ClientMapQueryPartitionIterator<>(this, getContext(), fetchSize, partitionId,
                predicate, projection);
    }

    /**
     * Returns an iterable that provides a client-side iterable for the
     * result of the projection on entries in the {@code partitionId} which
     * satisfy the {@code predicate}.
     *
     * @param fetchSize   the size of the batches which will be sent when iterating the data
     * @param partitionId the partition ID which is being iterated
     * @param projection  the projection to apply before returning the value. A null value is not allowed
     * @param predicate   the predicate which the entries must match. A null value is not allowed
     * @param <R>         the return type
     * @return an iterable for the projected entries of the specified partition
     */
    @Nonnull
    public <R> Iterable<R> iterable(
            int fetchSize,
            int partitionId,
            @Nonnull Projection<? super Map.Entry<K, V>, R> projection,
            @Nonnull Predicate<K, V> predicate
    ) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        return new ClientMapQueryPartitionIterable<>(this, fetchSize, partitionId, projection, predicate);
    }

    /**
     * Returns an iterable that provides a client-side iterable for the
     * entries in the {@code partitionId}.
     * If {@code prefetchValues} is {@code true}, values will be sent along with
     * the keys and no additional data will be fetched when iterating. If
     * {@code false}, only keys will be sent and values will be fetched when
     * calling {@code Map.Entry.getValue()} lazily.
     *
     * @param fetchSize      the size of the batches which will be sent when iterating the data
     * @param partitionId    the partition ID which is being iterated
     * @param prefetchValues whether to send values along with keys (if true) or to fetch them lazily when iterating (if false)
     * @return an iterable of the specified partition
     */
    @Nonnull
    public Iterable<Entry<K, V>> iterable(int fetchSize, int partitionId, boolean prefetchValues) {
        return new ClientMapPartitionIterable<>(this, fetchSize, partitionId, prefetchValues);
    }

    /**
     * Returns an iterable for iterating the result of the projection on entries
     * in all of the partitions which satisfy the {@code predicate}.
     *
     * @param fetchSize  the size of the batches which will be sent when iterating the data
     * @param projection the projection to apply before returning the value. null value is not allowed
     * @param predicate  the predicate which the entries must match. null value is not allowed
     * @param <R>        the return type
     * @return an iterable for the projected entries
     */
    @Nonnull
    public <R> Iterable<R> iterable(int fetchSize,
                                    @Nonnull Projection<? super Map.Entry<K, V>, R> projection,
                                    @Nonnull Predicate<K, V> predicate) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        int partitionCount = getContext().getPartitionService().getPartitionCount();
        return new ClientMapQueryIterable<>(this, fetchSize, partitionCount, projection, predicate);
    }

    /**
     * Returns an iterable for iterating entries in the all of the partitions. If
     * {@code prefetchValues} is {@code true}, values will be sent along with
     * the keys and no additional data will be fetched when iterating. If
     * {@code false}, only keys will be sent and values will be fetched when
     * calling {@code Map.Entry.getValue()} lazily.
     *
     * @param fetchSize      the size of the batches which will be sent when iterating the data
     * @param prefetchValues whether to send values along with keys (if true) or to fetch them lazily when iterating (if false)
     * @return an iterable for the entries
     */
    @Nonnull
    public Iterable<Entry<K, V>> iterable(int fetchSize, boolean prefetchValues) {
        int partitionCount = getContext().getPartitionService().getPartitionCount();
        return new ClientMapIterable<>(this, fetchSize, partitionCount, prefetchValues);
    }

    @Override
    public InternalCompletableFuture<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId) {
        final ClientMessage request = MapEventJournalSubscribeCodec.encodeRequest(name);
        final ClientInvocationFuture fut = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
        return new ClientDelegatingFuture<>(fut, getSerializationService(), message -> {
            ResponseParameters resp = MapEventJournalSubscribeCodec.decodeResponse(message);
            return new EventJournalInitialSubscriberState(resp.oldestSequence, resp.newestSequence);
        });
    }

    @Override
    public <T> InternalCompletableFuture<ReadResultSet<T>> readFromEventJournal(
            long startSequence,
            int minSize,
            int maxSize,
            int partitionId,
            java.util.function.Predicate<? super EventJournalMapEvent<K, V>> predicate,
            java.util.function.Function<? super EventJournalMapEvent<K, V>, ? extends T> projection
    ) {
        if (maxSize < minSize) {
            throw new IllegalArgumentException("maxSize " + maxSize
                    + " must be greater or equal to minSize " + minSize);
        }
        final SerializationService ss = getSerializationService();
        final ClientMessage request = MapEventJournalReadCodec.encodeRequest(
                name, startSequence, minSize, maxSize, ss.toData(predicate), ss.toData(projection));
        final ClientInvocationFuture fut = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
        return new ClientDelegatingFuture<>(fut, ss, message -> {
            MapEventJournalReadCodec.ResponseParameters params = MapEventJournalReadCodec.decodeResponse(message);
            ReadResultSetImpl resultSet = new ReadResultSetImpl<>(
                    params.readCount, params.items, params.itemSeqs, params.nextSeq);
            resultSet.setSerializationService(getSerializationService());
            return resultSet;
        });
    }

    // used for testing
    public ClientQueryCacheContext getQueryCacheContext() {
        return queryCacheContext;
    }

    private static void validateEntryProcessorForSingleKeyProcessing(EntryProcessor entryProcessor) {
        if (entryProcessor instanceof ReadOnly) {
            EntryProcessor backupProcessor = entryProcessor.getBackupProcessor();
            if (backupProcessor != null) {
                throw new IllegalArgumentException(
                        "EntryProcessor.getBackupProcessor() should be null for a read-only EntryProcessor");
            }
        }
    }

    private static void checkNotPagingPredicate(Predicate predicate, String method) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("PagingPredicate not supported in " + method + " method");
        }
    }

    private static boolean containsPagingPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicateImpl) {
            return true;
        }
        if (!(predicate instanceof PartitionPredicate)) {
            return false;
        }
        PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
        return partitionPredicate.getTarget() instanceof PagingPredicateImpl;
    }

    private class ClientMapToKeyWithPredicateEventHandler extends AbstractClientMapEventHandler {

        private MapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler handler;

        ClientMapToKeyWithPredicateEventHandler(ListenerAdapter<IMapEvent> listenerAdapter) {
            super(listenerAdapter);
            handler = new MapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ClientMapToKeyWithPredicateEventHandler.this.handleEntryEvent(key, value, oldValue,
                            mergingValue, eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private class ClientMapWithPredicateEventHandler extends AbstractClientMapEventHandler {
        private MapAddEntryListenerWithPredicateCodec.AbstractEventHandler handler;

        ClientMapWithPredicateEventHandler(ListenerAdapter<IMapEvent> listenerAdapter) {
            super(listenerAdapter);
            handler = new MapAddEntryListenerWithPredicateCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ClientMapWithPredicateEventHandler.this.handleEntryEvent(key, value, oldValue,
                            mergingValue, eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private class ClientMapToKeyEventHandler extends AbstractClientMapEventHandler {

        private MapAddEntryListenerToKeyCodec.AbstractEventHandler handler;

        ClientMapToKeyEventHandler(ListenerAdapter<IMapEvent> listenerAdapter) {
            super(listenerAdapter);
            handler = new MapAddEntryListenerToKeyCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ClientMapToKeyEventHandler.this.handleEntryEvent(key, value, oldValue,
                            mergingValue, eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private class ClientMapEventHandler extends AbstractClientMapEventHandler {

        private MapAddEntryListenerCodec.AbstractEventHandler handler;

        ClientMapEventHandler(ListenerAdapter<IMapEvent> listenerAdapter) {
            super(listenerAdapter);
            handler = new MapAddEntryListenerCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ClientMapEventHandler.this.handleEntryEvent(key, value, oldValue,
                            mergingValue, eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private abstract class AbstractClientMapEventHandler implements EventHandler<ClientMessage> {

        private ListenerAdapter<IMapEvent> listenerAdapter;

        AbstractClientMapEventHandler(ListenerAdapter<IMapEvent> listenerAdapter) {
            this.listenerAdapter = listenerAdapter;
        }

        public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                     int eventType, UUID uuid, int numberOfAffectedEntries) {
            Member member = getContext().getClusterService().getMember(uuid);
            listenerAdapter.onEvent(createIMapEvent(key, value, oldValue,
                    mergingValue, eventType, numberOfAffectedEntries, member));
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
                case LOADED:
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

        private EntryEvent<K, V> createEntryEvent(Data keyData, Data valueData, Data oldValueData,
                                                  Data mergingValueData, int eventType, Member member) {
            return new DataAwareEntryEvent<>(member, eventType, name, keyData, valueData, oldValueData,
                    mergingValueData, getSerializationService());
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
        public void handleMapPartitionLostEvent(int partitionId, UUID uuid) {
            Member member = getContext().getClusterService().getMember(uuid);
            listener.partitionLost(new MapPartitionLostEvent(name, member, -1, partitionId));
        }
    }

    @Override
    protected void onDestroy() {
        try {
            SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
            QueryCacheEndToEndProvider provider = subscriberContext.getEndToEndQueryCacheProvider();
            provider.destroyAllQueryCaches(name);
        } finally {
            super.onDestroy();
        }
    }

    @Override
    public V computeIfPresent(@Nonnull K key, @Nonnull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_BIFUNCTION_IS_NOT_ALLOWED);

        return computeIfPresentLocally(key, remappingFunction);
    }


    private V computeIfPresentLocally(K key,
                                      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {

        while (true) {
            Data oldValueAsData = toData(getInternal(key));
            if (oldValueAsData == null) {
                return null;
            }

            V oldValueClone = toObject(oldValueAsData);
            V newValue = remappingFunction.apply(key, oldValueClone);
            if (newValue != null) {
                if (replaceIfSameInternal(key, oldValueAsData, toData(newValue))) {
                    return newValue;
                }
            } else if (removeInternal(key, oldValueAsData)) {
                return null;
            }
        }
    }

    @Override
    public V computeIfAbsent(@Nonnull K key, @Nonnull Function<? super K, ? extends V> mappingFunction) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(mappingFunction, NULL_FUNCTION_IS_NOT_ALLOWED);

        return computeIfAbsentLocally(key, mappingFunction);
    }

    private V computeIfAbsentLocally(K key, Function<? super K, ? extends V> mappingFunction) {
        V oldValue = toObject(getInternal(key));
        if (oldValue != null) {
            return oldValue;
        }

        V newValue = mappingFunction.apply(key);
        if (newValue == null) {
            return null;
        }

        V result = putIfAbsentInternal(UNSET, MILLISECONDS, null, null, key, toData(newValue));
        if (result == null) {
            return newValue;
        } else {
            return result;
        }
    }

    public V compute(@Nonnull K key, @Nonnull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_BIFUNCTION_IS_NOT_ALLOWED);

        return computeLocally(key, remappingFunction);
    }

    private V computeLocally(K key,
                             BiFunction<? super K, ? super V, ? extends V> remappingFunction) {

        while (true) {
            Data oldValueAsData = toData(getInternal(key));
            V oldValueClone = toObject(oldValueAsData);
            V newValue = remappingFunction.apply(key, oldValueClone);

            if (oldValueAsData != null) {
                if (newValue != null) {
                    if (replaceIfSameInternal(key, oldValueAsData, toData(newValue))) {
                        return newValue;
                    }
                } else if (removeInternal(key, oldValueAsData)) {
                    return null;
                }
            } else {
                if (newValue != null) {
                    V result = putIfAbsentInternal(UNSET, MILLISECONDS, null, null, key, toData(newValue));
                    if (result == null) {
                        return newValue;
                    }
                } else {
                    return null;
                }
            }
        }
    }

    public V merge(@Nonnull K key, @Nonnull V value,
                   @Nonnull BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(remappingFunction, NULL_BIFUNCTION_IS_NOT_ALLOWED);

        return mergeLocally(key, value, remappingFunction);
    }

    private V mergeLocally(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        Data keyAsData = toData(key);

        while (true) {
            Data oldValueAsData = toData(getInternal(keyAsData));
            if (oldValueAsData != null) {
                V oldValueClone = toObject(oldValueAsData);
                V newValue = remappingFunction.apply(oldValueClone, value);
                if (newValue != null) {
                    if (replaceIfSameInternal(keyAsData, oldValueAsData, toData(newValue))) {
                        return newValue;
                    }
                } else if (removeInternal(keyAsData, oldValueAsData)) {
                    return null;
                }
            } else {
                V result = putIfAbsentInternal(UNSET, MILLISECONDS, null, null, keyAsData, toData(value));
                if (result == null) {
                    return value;
                }
            }
        }
    }

    @Override
    public void replaceAll(@Nonnull BiFunction<? super K, ? super V, ? extends V> function) {
        checkNotNull(function, NULL_BIFUNCTION_IS_NOT_ALLOWED);
        replaceAllInternal(function);
    }

    /**
     * Orchestrator method that invokes all the paritition in parallel to replace each entry's
     * value with the result of invoking the given function on that entry
     *
     * @param function  remappingfunction
     */
    protected void replaceAllInternal(BiFunction<? super K, ? super V, ? extends V> function) {
        if (SerializationUtil.isClassStaticAndSerializable(function) && !useDefaultReplaceAllOperation) {
            try {
                int partitionCount = getContext().getPartitionService().getPartitionCount();
                List<Future<ClientMessage>> futures = new ArrayList<>(partitionCount);
                Data functionAsData = toData(function);
                for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                    ClientMessage request = MapReplaceAllCodec.encodeRequest(name, functionAsData);
                    futures.add(new ClientInvocation(getClient(), request, getName(), partitionId).invoke());
                }
                for (Future<ClientMessage> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        throw rethrow(e);
                    }
                }
            } catch (UnsupportedOperationException e) {
                //handle if the server version is less than client version
                IMap.super.replaceAll(function);
                useDefaultReplaceAllOperation = true;
            }
        } else {
            IMap.super.replaceAll(function);
        }
    }
}
