/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConfigXmlGenerator;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapPutAllCodec;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.toCompletableFuture;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.ceil;
import static java.lang.Math.log10;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

public final class ImdgUtil {
    private static final float PUT_ALL_INITIAL_SIZE_MAGIC = 20f;

    private ImdgUtil() {
    }

    public static boolean existsDistributedObject(NodeEngine nodeEngine, String serviceName, String objectName) {
        return nodeEngine.getProxyService()
                  .getDistributedObjectNames(serviceName)
                  .contains(objectName);
    }

    public static <K, V> EntryProcessor<K, V> entryProcessor(
            BiFunctionEx<? super K, ? super V, ? extends V> remappingFunction
    ) {
        return new AbstractEntryProcessor<K, V>() {
            @Override
            public Object process(Entry<K, V> entry) {
                V newValue = remappingFunction.apply(entry.getKey(), entry.getValue());
                entry.setValue(newValue);
                return newValue;
            }
        };
    }

    public static boolean isMemberInstance(HazelcastInstance instance) {
        return instance.getLocalEndpoint() instanceof Member;
    }

    /**
     * Converts {@link ClientConfig} to xml representation using {@link
     * ClientConfigXmlGenerator}.
     */
    public static String asXmlString(ClientConfig clientConfig) {
        return clientConfig == null ? null : ClientConfigXmlGenerator.generate(clientConfig);
    }

    /**
     * Converts client-config xml string to {@link ClientConfig} using {@link
     * XmlClientConfigBuilder}.
     */
    public static ClientConfig asClientConfig(String xml) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        return new XmlClientConfigBuilder(inputStream).build();
    }

    public static <T> PredicateEx<T> wrapImdgPredicate(Predicate<T> predicate) {
        return new ImdgPredicateWrapper(predicate);
    }

    public static <T> Predicate<T> maybeUnwrapImdgPredicate(PredicateEx<T> predicate) {
        if (predicate instanceof ImdgPredicateWrapper) {
            return ((ImdgPredicateWrapper<T>) predicate).wrapped;
        }
        return predicate;
    }

    public static FunctionEx wrapImdgFunction(com.hazelcast.util.function.Function function) {
        return new ImdgFunctionWrapper(function);
    }

    public static <T, R> com.hazelcast.util.function.Function<T, R> maybeUnwrapImdgFunction(FunctionEx<T, R> function) {
        if (function instanceof ImdgFunctionWrapper) {
            return ((ImdgFunctionWrapper<T, R>) function).wrapped;
        }
        return function;
    }

    /**
     * Async version of {@code IMap.putAll()}. This is based on IMDG's code and
     * currently does not invalidate the near cache.
     *
     * TODO remove this method once https://github.com/hazelcast/hazelcast/pull/15463 is released

     * @param targetIMap imap to write to
     * @param items      items to add
     */
    public static <K, V> CompletionStage<Void> mapPutAllAsync(
            @Nonnull IMap<K, V> targetIMap, Map<? extends K, ? extends V> items
    ) {
        if (items.isEmpty()) {
            return completedFuture(null);
        }
        if (items.size() == 1) {
            Entry<? extends K, ? extends V> onlyEntry = items.entrySet().iterator().next();
            return toCompletableFuture(targetIMap.setAsync(onlyEntry.getKey(), onlyEntry.getValue()));
        }

        if (targetIMap instanceof MapProxyImpl) {
            return mapPutAllAsync((MapProxyImpl<K, V>) targetIMap, items);
        } else if (targetIMap instanceof ClientMapProxy) {
            return mapPutAllAsync((ClientMapProxy<K, V>) targetIMap, items);
        } else {
            throw new RuntimeException("Unexpected map class: " + targetIMap.getClass().getName());
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> CompletionStage<Void> mapPutAllAsync(
            @Nonnull MapProxyImpl<K, V> targetMap,
            @Nonnull Map<? extends K, ? extends V> items
    ) {
        NodeEngine nodeEngine = targetMap.getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();

        MapEntries[] entries = new MapEntries[partitionService.getPartitionCount()];
        // this is an educated guess for the initial size of the entries per partition, depending on the map size
        int initialSize = (int) ceil(PUT_ALL_INITIAL_SIZE_MAGIC * items.size() /
                partitionService.getPartitionCount() / log10(items.size()));

        for (Entry<? extends K, ? extends V> entry : items.entrySet()) {
            checkNotNull(entry.getKey(), "Null key is not allowed");
            checkNotNull(entry.getValue(), "Null value is not allowed");

            Data keyData = serializationService.toData(entry.getKey(), targetMap.getPartitionStrategy());
            int partitionId = partitionService.getPartitionId(keyData);
            MapEntries partitionEntries = entries[partitionId];
            if (partitionEntries == null) {
                partitionEntries = new MapEntries(initialSize);
                entries[partitionId] = partitionEntries;
            }

            partitionEntries.add(keyData, serializationService.toData(entry.getValue()));
        }

        int[] subPartitions = new int[memberPartitionsMap.values().stream().mapToInt(List::size).max().orElse(0)];
        MapEntries[] subEntries = new MapEntries[subPartitions.length];
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        ExecutionCallback callback = createPutAllCallback(
                memberPartitionsMap.size(),
                targetMap instanceof NearCachedMapProxyImpl ? ((NearCachedMapProxyImpl) targetMap).getNearCache() : null,
                items.keySet(),
                Stream.of(entries).filter(Objects::nonNull).flatMap(e -> e.entries().stream()).map(Entry::getKey),
                resultFuture);

        for (Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
            List<Integer> memberPartitions = entry.getValue();
            int count = 0;
            for (int partitionId : memberPartitions) {
                if (entries[partitionId] != null) {
                    subPartitions[count] = partitionId;
                    subEntries[count] = entries[partitionId];
                    count++;
                }
            }
            if (count == 0) {
                callback.onResponse(null);
                continue;
            }
            int[] subPartitionsTrimmed = Arrays.copyOf(subPartitions, count);
            MapEntries[] subEntriesTrimmed = Arrays.copyOf(subEntries, count);

            if (count > 0) {
                OperationFactory factory = targetMap.getOperationProvider().createPutAllOperationFactory(
                        targetMap.getName(), subPartitionsTrimmed, subEntriesTrimmed);
                targetMap.getOperationService().invokeOnPartitionsAsync(SERVICE_NAME, factory,
                        asIntegerList(subPartitionsTrimmed))
                         .andThen(callback);
            }
        }

        return resultFuture;
    }

    @SuppressWarnings("unchecked")
    private static <K, V> CompletionStage<Void> mapPutAllAsync(
            ClientMapProxy<K, V> targetMap,
            Map<? extends K, ? extends V> items
    ) {
        if (items.isEmpty()) {
            return completedFuture(null);
        }
        checkNotNull(targetMap, "Null argument map is not allowed");
        ClientPartitionService partitionService = targetMap.getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Map<Integer, List<Map.Entry<Data, Data>>> entryMap = new HashMap<>(partitionCount);
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

        for (Entry<Integer, List<Map.Entry<Data, Data>>> partitionEntries : entryMap.entrySet()) {
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

    private static ExecutionCallback<Object> createPutAllCallback(
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

    private static List<Integer> asIntegerList(int[] array) {
        return new AbstractList<Integer>() {
            @Override
            public Integer get(int index) {
                return array[index];
            }

            @Override
            public int size() {
                return array.length;
            }
        };
    }

    @Nonnull
    public static List<Address> getRemoteMembers(@Nonnull NodeEngine engine) {
        final Member localMember = engine.getLocalMember();
        return engine.getClusterService().getMembers().stream()
                     .filter(m -> !m.equals(localMember))
                     .map(Member::getAddress)
                     .collect(toList());
    }

    public static Connection getMemberConnection(@Nonnull NodeEngine engine, @Nonnull Address memberAddr) {
        return ((NodeEngineImpl) engine).getNode().getEndpointManager().getConnection(memberAddr);
    }

    /**
     * This method will generate an {@link ExecutionCallback} which
     * allows to asynchronously get notified when the execution is completed,
     * either successfully or with error by calling {@code onResponse} on success
     * and {@code onError} on error respectively.
     *
     * @param onResponse function to call when execution is completed successfully
     * @param onError function to call when execution is completed with error
     * @param <T> type of the response
     * @return {@link ExecutionCallback}
     */
    public static <T> ExecutionCallback<T> callbackOf(@Nonnull Consumer<T> onResponse,
                                                      @Nonnull Consumer<Throwable> onError) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T o) {
                onResponse.accept(o);
            }

            @Override
            public void onFailure(Throwable throwable) {
                onError.accept(throwable);
            }
        };
    }

    /**
     * This method will generate an {@link ExecutionCallback} which allows to
     * asynchronously get notified when the execution is completed, either
     * successfully or with an error.
     *
     * @param callback BiConsumer to call when execution is completed. Only one
     *                of the passed values will be non-null, except for the
     *                case the normal result is null, in which case both values
     *                will be null
     * @param <T> type of the response
     * @return {@link ExecutionCallback}
     */
    public static <T> ExecutionCallback<T> callbackOf(BiConsumer<T, Throwable> callback) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T o) {
                callback.accept(o, null);
            }

            @Override
            public void onFailure(Throwable throwable) {
                callback.accept(null, throwable);
            }
        };
    }

    @Nonnull
    public static BufferObjectDataOutput createObjectDataOutput(@Nonnull NodeEngine engine) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataOutput(Util.BUFFER_SIZE);
    }

    @Nonnull
    public static BufferObjectDataInput createObjectDataInput(@Nonnull NodeEngine engine, @Nonnull byte[] buf) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataInput(buf);
    }

    public static void writeList(@Nonnull ObjectDataOutput output, @Nonnull List list) throws IOException {
        output.writeInt(list.size());
        for (Object o : list) {
            output.writeObject(o);
        }
    }

    @Nonnull
    public static <E> List<E> readList(@Nonnull ObjectDataInput output) throws IOException {
        int length = output.readInt();
        List<E> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(output.readObject());
        }
        return list;
    }

    private static final class ImdgPredicateWrapper<T> implements PredicateEx<T> {
        private final com.hazelcast.util.function.Predicate<T> wrapped;

        ImdgPredicateWrapper(Predicate<T> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean testEx(T t) {
            return wrapped.test(t);
        }
    }

    private static final class ImdgFunctionWrapper<T, R> implements FunctionEx<T, R> {
        private final com.hazelcast.util.function.Function<T, R> wrapped;

        ImdgFunctionWrapper(com.hazelcast.util.function.Function<T, R> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public R applyEx(T t) {
            return wrapped.apply(t);
        }
    }
}
