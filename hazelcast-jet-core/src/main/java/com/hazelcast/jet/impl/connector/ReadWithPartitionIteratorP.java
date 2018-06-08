/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.processorToPartitions;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Private API, see methods in {@link SourceProcessors}.
 * <p>
 * The number of Hazelcast partitions should be configured to at least
 * {@code localParallelism * clusterSize}, otherwise some processors will
 * have no partitions assigned to them.
 */
@SuppressWarnings("unchecked")
public final class ReadWithPartitionIteratorP<T> extends AbstractProcessor {

    private static final boolean PREFETCH_VALUES = true;

    private static final int FETCH_SIZE = 16384;

    private final Traverser<T> outputTraverser;

    ReadWithPartitionIteratorP(Function<Integer, Iterator<T>> partitionToIterator,
                               List<Integer> partitions) {
        final CircularListCursor<Iterator<T>> iteratorCursor = new CircularListCursor<>(
                partitions.stream().map(partitionToIterator).collect(toList())
        );
        this.outputTraverser = () -> {
            do {
                final Iterator<T> currIterator = iteratorCursor.value();
                while (currIterator.hasNext()) {
                    T next = currIterator.next();
                    if (next != null) {
                        iteratorCursor.advance();
                        return next;
                    }
                }
                iteratorCursor.remove();
            } while (iteratorCursor.advance());
            return null;
        };
    }

    public static <T> ProcessorMetaSupplier readMapP(@Nonnull String mapName) {
        return new LocalClusterMetaSupplier<T>(
                instance -> partition -> ((MapProxyImpl) instance.getMap(mapName))
                        .iterator(FETCH_SIZE, partition, PREFETCH_VALUES));
    }

    public static <T> ProcessorMetaSupplier readRemoteMapP(
            @Nonnull String mapName, @Nonnull ClientConfig clientConfig
    ) {
        return new RemoteClusterMetaSupplier<T>(clientConfig,
                instance -> partition -> ((ClientMapProxy) instance.getMap(mapName))
                        .iterator(FETCH_SIZE, partition, PREFETCH_VALUES));
    }

    public static <K, V, T> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Map.Entry<K, V>, T> projection
    ) {
        checkSerializable(predicate, "predicate");
        checkSerializable(projection, "projection");

        return new LocalClusterMetaSupplier<T>(
                instance -> partition -> {
                    MapProxyImpl map = (MapProxyImpl) instance.<K, V>getMap(mapName);
                    return map.<T>iterator(FETCH_SIZE, partition, projection, predicate);
                });
    }

    public static <K, V, T> ProcessorMetaSupplier readRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Projection<Entry<K, V>, T> projection,
            @Nonnull Predicate<K, V> predicate
    ) {
        checkSerializable(projection, "projection");
        checkSerializable(predicate, "predicate");

        return new RemoteClusterMetaSupplier<T>(clientConfig,
                instance -> partition -> ((ClientMapProxy) instance.getMap(mapName))
                        .iterator(FETCH_SIZE, partition, projection, predicate));
    }

    public static ProcessorMetaSupplier readCacheP(@Nonnull String cacheName) {
        return new LocalClusterMetaSupplier<>(
                instance -> partition -> ((CacheProxy) instance.getCacheManager().getCache(cacheName))
                        .iterator(FETCH_SIZE, partition, PREFETCH_VALUES));
    }

    public static ProcessorMetaSupplier readRemoteCacheP(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return new RemoteClusterMetaSupplier<>(clientConfig,
                instance -> partition -> ((ClientCacheProxy) instance.getCacheManager().getCache(cacheName))
                        .iterator(FETCH_SIZE, partition, PREFETCH_VALUES));
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(outputTraverser);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private static <T> List<Processor> getProcessors(int count, List<Integer> ownedPartitions,
                                                     Function<Integer, Iterator<T>> partitionToIterator) {

        return processorToPartitions(count, ownedPartitions)
                .values().stream()
                .map(partitions -> !partitions.isEmpty()
                        ? new ReadWithPartitionIteratorP<>(partitionToIterator, partitions)
                        : Processors.noopP().get()
                )
                .collect(toList());
    }

    private static class RemoteClusterMetaSupplier<T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig serializableConfig;
        private final DistributedFunction<HazelcastInstance, Function<Integer, Iterator<T>>> iteratorSupplier;

        private transient int remotePartitionCount;

        RemoteClusterMetaSupplier(
                ClientConfig clientConfig,
                DistributedFunction<HazelcastInstance, Function<Integer, Iterator<T>>> iteratorSupplier
        ) {
            this.serializableConfig = new SerializableClientConfig(clientConfig);
            this.iteratorSupplier = iteratorSupplier;
        }

        @Override
        public int preferredLocalParallelism() {
            return 1;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance client = newHazelcastClient(serializableConfig.asClientConfig());
            try {
                HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
                remotePartitionCount = clientProxy.client.getClientPartitionService().getPartitionCount();
            } finally {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            // assign each remote partition to a member
            Map<Address, List<Integer>> membersToPartitions =
                    IntStream.range(0, remotePartitionCount)
                             .boxed()
                             .collect(groupingBy(partition -> addresses.get(partition % addresses.size())));

            return address -> new RemoteClusterProcessorSupplier<>(membersToPartitions.get(address),
                    serializableConfig, iteratorSupplier);
        }
    }

    private static class RemoteClusterProcessorSupplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final List<Integer> ownedPartitions;
        private final SerializableClientConfig serializableClientConfig;
        private final DistributedFunction<HazelcastInstance, Function<Integer, Iterator<T>>> iteratorSupplier;

        private transient HazelcastInstance client;
        private transient Function<Integer, Iterator<T>> partitionToIterator;

        RemoteClusterProcessorSupplier(
                List<Integer> ownedPartitions,
                SerializableClientConfig serializableClientConfig,
                DistributedFunction<HazelcastInstance, Function<Integer, Iterator<T>>> iteratorSupplier
        ) {
            this.ownedPartitions = ownedPartitions;
            this.serializableClientConfig = serializableClientConfig;
            this.iteratorSupplier = iteratorSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            client = newHazelcastClient(serializableClientConfig.asClientConfig());
            partitionToIterator = iteratorSupplier.apply(client);
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return getProcessors(count, ownedPartitions, partitionToIterator);
        }
    }

    private static class LocalClusterMetaSupplier<T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final DistributedFunction<HazelcastInstance, Function<Integer, Iterator<T>>> iteratorSupplier;

        private transient Map<Address, List<Integer>> addrToPartitions;

        LocalClusterMetaSupplier(
                DistributedFunction<HazelcastInstance, Function<Integer, Iterator<T>>> iteratorSupplier
        ) {
            this.iteratorSupplier = iteratorSupplier;
        }

        @Override
        public int preferredLocalParallelism() {
            return 2;
        }

        @Override
        public void init(@Nonnull Context context) {
            addrToPartitions = context.jetInstance().getHazelcastInstance().getPartitionService().getPartitions()
                                      .stream()
                                      .collect(groupingBy(p -> p.getOwner().getAddress(),
                                              mapping(Partition::getPartitionId, toList())));
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new LocalClusterProcessorSupplier<>(addrToPartitions.get(address), iteratorSupplier);
        }
    }

    private static class LocalClusterProcessorSupplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final List<Integer> ownedPartitions;
        private final DistributedFunction<HazelcastInstance, Function<Integer, Iterator<T>>> iteratorSupplier;

        private transient Function<Integer, Iterator<T>> partitionToIterator;

        LocalClusterProcessorSupplier(
                List<Integer> ownedPartitions,
                DistributedFunction<HazelcastInstance, Function<Integer, Iterator<T>>> iteratorSupplier
        ) {
            this.ownedPartitions = ownedPartitions != null ? ownedPartitions : Collections.emptyList();
            this.iteratorSupplier = iteratorSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            partitionToIterator = iteratorSupplier.apply(context.jetInstance().getHazelcastInstance());
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return getProcessors(count, ownedPartitions, partitionToIterator);
        }

    }
}
