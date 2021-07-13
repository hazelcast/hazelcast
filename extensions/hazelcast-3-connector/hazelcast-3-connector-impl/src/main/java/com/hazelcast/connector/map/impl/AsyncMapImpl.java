/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector.map.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapPutAllCodec;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.connector.map.AsyncMap;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

class AsyncMapImpl<K, V> implements AsyncMap<K, V> {

    private final IMap<K, V> map;

    AsyncMapImpl(IMap<K, V> map) {
        this.map = map;
    }

    @Override
    public CompletionStage<V> getAsync(@Nonnull K key) {
        return Hz3ImplUtil.toCompletableFuture(map.getAsync(key));
    }

    @SuppressWarnings("unchecked")
    public CompletionStage<Void> putAllAsync(
            Map<? extends K, ? extends V> items
    ) {
        ClientMapProxy<K, V> targetMap = (ClientMapProxy<K, V>) map;
        if (items.isEmpty()) {
            return completedFuture(null);
        }
        checkNotNull(targetMap, "Null argument map is not allowed");
        ClientPartitionService partitionService = targetMap.getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Map<Integer, List<Entry<Data, Data>>> entryMap = new HashMap<>(partitionCount);
        InternalSerializationService serializationService = targetMap.getContext().getSerializationService();

        for (Entry<? extends K, ? extends V> entry : items.entrySet()) {
            checkNotNull(entry.getKey(), "Null key is not allowed");
            checkNotNull(entry.getValue(), "Null value is not allowed");

            Data keyData = serializationService.toData(entry.getKey());
            int partitionId = partitionService.getPartitionId(keyData);
            entryMap
                    .computeIfAbsent(partitionId, k -> new ArrayList<>())
                    .add(new AbstractMap.SimpleEntry<>(keyData, serializationService.toData(entry.getValue())));
        }

        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) targetMap.getContext().getHazelcastInstance();
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        ExecutionCallback callback = createPutAllCallback(
                entryMap.size(),
                targetMap instanceof NearCachedClientMapProxy ? ((NearCachedClientMapProxy) targetMap).getNearCache()
                        : null,
                items.keySet(),
                entryMap.values().stream().flatMap(List::stream).map(Entry::getKey),
                resultFuture);

        for (Entry<Integer, List<Entry<Data, Data>>> partitionEntries : entryMap.entrySet()) {
            Integer partitionId = partitionEntries.getKey();
            // use setAsync if there's only one entry
            if (partitionEntries.getValue().size() == 1) {
                Entry<Data, Data> onlyEntry = partitionEntries.getValue().get(0);
                // cast to raw so that we can pass serialized key and value
                ((IMap) targetMap).setAsync(onlyEntry.getKey(), onlyEntry.getValue())
                        .andThen(callback);
            } else {
                ClientMessage request = MapPutAllCodec.encodeRequest(targetMap.getName(), partitionEntries.getValue());
                new ClientInvocation(client, request, targetMap.getName(), partitionId).invoke()
                        .andThen(callback);
            }
        }
        return resultFuture;
    }

    private ExecutionCallback<Object> createPutAllCallback(
            int participantCount,
            @Nullable NearCache<Object, Object> nearCache,
            @Nonnull Set<?> nonSerializedKeys,
            @Nonnull Stream<Data> serializedKeys,
            CompletableFuture<Void> resultFuture
    ) {
        AtomicInteger completionCounter = new AtomicInteger(participantCount);

        return new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                if (completionCounter.decrementAndGet() > 0) {
                    return;
                }

                if (nearCache != null) {
                    if (nearCache.isSerializeKeys()) {
                        serializedKeys.forEach(nearCache::invalidate);
                    } else {
                        for (Object key : nonSerializedKeys) {
                            nearCache.invalidate(key);
                        }
                    }
                }
                resultFuture.complete(null);
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.completeExceptionally(t);
            }
        };
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }
}
