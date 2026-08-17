/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.client.impl;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionClearCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionGetCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionOptimizeCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSearchNearVectorCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSetCodec;
import com.hazelcast.client.impl.protocol.codec.VectorCollectionSizeCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.internal.impl.SearchResultsImpl;
import com.hazelcast.vector.internal.impl.VectorUtil;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class ClientVectorCollectionProxyImpl<K, V> extends ClientProxy implements VectorCollection<K, V> {
    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_MAP_IS_NOT_ALLOWED = "Null documents map is not allowed!";

    public ClientVectorCollectionProxyImpl(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    public CompletionStage<VectorDocument<V>> getAsync(@Nonnull K key) {
        return invokeSingleKeyOperationAsync(key,
                VectorCollectionGetCodec::encodeRequest,
                r -> toObject(VectorCollectionGetCodec.decodeResponse(r)));
    }

    @Override
    public CompletionStage<VectorDocument<V>> putAsync(@Nonnull K key, @Nonnull VectorDocument<V> value) {
        return invokeSingleKeyValueOperationAsync(key, value,
                VectorCollectionPutCodec::encodeRequest,
                r -> toObject(VectorCollectionPutCodec.decodeResponse(r)));
    }

    @Override
    public CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull VectorDocument<V> value) {
        return invokeSingleKeyValueOperationAsync(key, value,
                VectorCollectionSetCodec::encodeRequest, r -> null);
    }

    @Override
    public CompletionStage<VectorDocument<V>> putIfAbsentAsync(@Nonnull K key, @Nonnull VectorDocument<V> value) {
        return invokeSingleKeyValueOperationAsync(key, value,
                VectorCollectionPutIfAbsentCodec::encodeRequest,
                r -> toObject(VectorCollectionPutIfAbsentCodec.decodeResponse(r)));
    }

    @Override
    public CompletionStage<Void> putAllAsync(Map<? extends K, VectorDocument<V>> documents) {
        Objects.requireNonNull(documents, NULL_MAP_IS_NOT_ALLOWED);
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        putAllInternalAsync(documents, future);
        return future;
    }

    @Override
    public CompletionStage<VectorDocument<V>> removeAsync(K key) {
        return invokeSingleKeyOperationAsync(key,
                VectorCollectionRemoveCodec::encodeRequest,
                r -> toObject(VectorCollectionRemoveCodec.decodeResponse(r)));
    }

    @Override
    public CompletionStage<Void> deleteAsync(K key) {
        return invokeSingleKeyOperationAsync(key,
                VectorCollectionDeleteCodec::encodeRequest,
                r -> null);
    }

    @Override
    public CompletionStage<Void> optimizeAsync(String indexName) {
        var request = VectorCollectionOptimizeCodec.encodeRequest(name, indexName, UuidUtil.newUnsecureUUID());
        ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, getName());
        clientInvocation.setInvocationTimeoutMillis(Long.MAX_VALUE);
        ClientInvocationFuture future = clientInvocation.invoke();
        return future.thenAcceptAsync(response -> {}, CALLER_RUNS);
    }

    @Override
    public CompletionStage<Void> clearAsync() {
        var request = VectorCollectionClearCodec.encodeRequest(name);
        ClientInvocationFuture future = invokeAsync(request);
        return future.thenAcceptAsync(response -> {}, CALLER_RUNS);
    }

    @Override
    public long size() {
        ClientMessage request = VectorCollectionSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return VectorCollectionSizeCodec.decodeResponse(response);
    }

    @Override
    public CompletionStage<SearchResults<K, V>> searchAsync(VectorValues vectors, SearchOptions searchOptions) {
        checkNotNull(searchOptions);
        try {
            ClientMessage request;
            request = VectorCollectionSearchNearVectorCodec.encodeRequest(name, vectors, searchOptions);
            ClientInvocationFuture future = invokeAsync(request);
            SerializationService ss = getSerializationService();
            return new ClientDelegatingFuture<>(future, ss, VectorCollectionSearchNearVectorCodec::decodeResponse)
                    .thenApplyAsync(resultsList -> VectorUtil.deserialize(
                            new SearchResultsImpl<>((List<SearchResult<Data, Data>>) resultsList),
                            getSerializationService()), CALLER_RUNS);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void putAllInternalAsync(@Nonnull Map<? extends K, ? extends VectorDocument<V>> map,
                                     @Nonnull InternalCompletableFuture<Void> future) {
        if (map.isEmpty()) {
            future.complete(null);
            return;
        }
        ClientPartitionService partitionService = getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Map<Integer, List<Map.Entry<Data, DataVectorDocument>>> entryMap = new HashMap<>(partitionCount);

        for (Map.Entry<? extends K, ? extends VectorDocument<V>> entry : map.entrySet()) {
            checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
            checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

            Data keyData = toData(entry.getKey());
            int partitionId = partitionService.getPartitionId(keyData);
            var partitionEntries = entryMap.computeIfAbsent(partitionId, x -> new ArrayList<>());
            partitionEntries.add(new AbstractMap.SimpleEntry<>(keyData, toData(entry.getValue())));
        }
        assert !entryMap.isEmpty();
        AtomicInteger counter = new AtomicInteger(entryMap.size());
        BiConsumer<ClientMessage, Throwable> callback = (response, t) -> {
            if (t != null) {
                future.completeExceptionally(t);
            }
            if (counter.decrementAndGet() == 0) {
                if (!future.isDone()) {
                    future.complete(null);
                }
            }
        };
        for (var entry : entryMap.entrySet()) {
            Integer partitionId = entry.getKey();
            List<Map.Entry<Data, DataVectorDocument>> partitionEntries = entry.getValue();
            // if there is only one entry, use simpler `set` message
            ClientMessage request = partitionEntries.size() > 1
                    ? VectorCollectionPutAllCodec.encodeRequest(name, partitionEntries)
                    : VectorCollectionSetCodec.encodeRequest(name,
                            partitionEntries.get(0).getKey(), partitionEntries.get(0).getValue());
            invokeOnPartitionAsync(request, partitionId)
                    .whenCompleteAsync(callback, ConcurrencyUtil.getDefaultAsyncExecutor());
        }
    }

    private <R> CompletionStage<R> invokeSingleKeyOperationAsync(@Nonnull K key,
                                                                      BiFunction<String, Data, ClientMessage> encode,
                                                                      ClientMessageDecoder<R> decode) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        try {
            Data keyData = toData(key);
            ClientMessage request;
            request = encode.apply(name, keyData);
            ClientInvocationFuture future = invokeOnKeyOwnerAsync(request, keyData);
            SerializationService ss = getSerializationService();
            // Extra thenCompose with dummy future is a workaround for chained invocations hanging due to HZOLD-4709
            return CompletableFuture.completedFuture(null)
                    .thenComposeAsync(v -> new ClientDelegatingFuture<R>(future, ss, decode), CALLER_RUNS);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private <R> CompletionStage<R> invokeSingleKeyValueOperationAsync(@Nonnull K key, @Nonnull VectorDocument<V> value,
                                                          TriFunction<String, Data, DataVectorDocument, ClientMessage> encode,
                                                          ClientMessageDecoder<R> decode) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        try {
            Data keyData = toData(key);
            DataVectorDocument valueData = toData(value);
            ClientMessage request;
            request = encode.apply(name, keyData, valueData);
            ClientInvocationFuture future = invokeOnKeyOwnerAsync(request, keyData);
            SerializationService ss = getSerializationService();
            // Extra thenCompose with dummy future is a workaround for chained invocations hanging due to HZOLD-4709
            return CompletableFuture.completedFuture(null)
                    .thenComposeAsync(v -> new ClientDelegatingFuture<R>(future, ss, decode), CALLER_RUNS);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private DataVectorDocument toData(VectorDocument o) {
        return VectorUtil.serialize(o, getSerializationService());
    }

    private <T> VectorDocument<T> toObject(DataVectorDocument o) {
        return VectorUtil.deserialize(o, getSerializationService());
    }

    private ClientInvocationFuture invokeOnKeyOwnerAsync(ClientMessage request, Data keyData) {
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);
        return invokeOnPartitionAsync(request, partitionId);
    }

    private ClientInvocationFuture invokeOnPartitionAsync(ClientMessage request, int partitionId) {
        ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, getName(), partitionId);
        return clientInvocation.invoke();
    }
}
